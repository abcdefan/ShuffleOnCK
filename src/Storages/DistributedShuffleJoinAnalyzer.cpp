#include <Storages/DistributedShuffleJoinAnalyzer.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageSnapshot.h>

#include <algorithm>
#include <unordered_map>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
}

namespace
{

const QueryNode * getSingleSelectQueryNode(const QueryTreeNodePtr & query_tree)
{
    if (!query_tree)
        return nullptr;

    if (const auto * query_node = query_tree->as<QueryNode>())
        return query_node;

    const auto * union_node = query_tree->as<UnionNode>();
    if (!union_node)
        return nullptr;

    const auto & queries = union_node->getQueries().getNodes();
    if (queries.size() != 1)
        return nullptr;

    return queries.front()->as<QueryNode>();
}

bool hasUnsupportedSelectClauses(const QueryNode & query_node)
{
    return query_node.hasWith()
        || query_node.isDistinct()
        || query_node.hasPrewhere()
        || query_node.hasGroupBy()
        || query_node.isGroupByWithTotals()
        || query_node.isGroupByWithRollup()
        || query_node.isGroupByWithCube()
        || query_node.isGroupByWithGroupingSets()
        || query_node.isGroupByAll()
        || query_node.hasHaving()
        || query_node.hasWindow()
        || query_node.hasQualify()
        || query_node.isOrderByAll()
        || query_node.hasInterpolate()
        || query_node.hasLimitByLimit()
        || query_node.hasLimitByOffset()
        || query_node.hasLimitBy()
        || query_node.isLimitByAll();
}

class CollectColumnSourceToColumnsVisitor : public InDepthQueryTreeVisitor<CollectColumnSourceToColumnsVisitor>
{
public:
    struct Columns
    {
        NameSet column_names;
        NamesAndTypes columns;

        void addColumn(NameAndTypePair column)
        {
            if (column_names.contains(column.name))
                return;

            column_names.insert(column.name);
            columns.push_back(std::move(column));
        }
    };

    const std::unordered_map<QueryTreeNodePtr, Columns> & getColumnSourceToColumns() const
    {
        return column_source_to_columns;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            return;

        auto it = column_source_to_columns.find(column_source);
        if (it == column_source_to_columns.end())
        {
            auto [insert_it, _] = column_source_to_columns.emplace(column_source, Columns{});
            it = insert_it;
        }

        it->second.addColumn(column_node->getColumn());
    }

private:
    std::unordered_map<QueryTreeNodePtr, Columns> column_source_to_columns;
};

bool isSameQueryTreeNode(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
{
    return lhs && rhs && lhs->isEqual(*rhs);
}

bool containsColumnSource(const std::vector<QueryTreeNodePtr> & sources, const QueryTreeNodePtr & source)
{
    for (const auto & candidate : sources)
        if (isSameQueryTreeNode(candidate, source))
            return true;

    return false;
}

bool isLeftColumnSource(const DistributedShuffleJoinInfo & info, const QueryTreeNodePtr & source)
{
    if (!info.left_column_sources.empty())
        return containsColumnSource(info.left_column_sources, source);

    return isSameQueryTreeNode(source, info.left_table_expression);
}

bool isRightColumnSource(const DistributedShuffleJoinInfo & info, const QueryTreeNodePtr & source)
{
    if (!info.right_column_sources.empty())
        return containsColumnSource(info.right_column_sources, source);

    return isSameQueryTreeNode(source, info.right_table_expression);
}

String getMappedColumnName(
    const std::vector<DistributedShuffleJoinColumnNameMapping> & mappings,
    const QueryTreeNodePtr & source,
    const String & column_name)
{
    for (const auto & mapping : mappings)
    {
        if (mapping.source_column_name == column_name && isSameQueryTreeNode(mapping.source, source))
            return mapping.mapped_column_name;
    }

    return column_name;
}

String getMappedColumnName(
    const DistributedShuffleJoinInfo & info,
    const QueryTreeNodePtr & source,
    const String & column_name,
    bool left_side)
{
    return getMappedColumnName(
        left_side ? info.left_column_name_mappings : info.right_column_name_mappings,
        source,
        column_name);
}

void initializeBinaryColumnSources(DistributedShuffleJoinInfo & info)
{
    if (info.left_column_sources.empty() && info.left_table_expression)
        info.left_column_sources.push_back(info.left_table_expression);

    if (info.right_column_sources.empty() && info.right_table_expression)
        info.right_column_sources.push_back(info.right_table_expression);
}

bool isSupportedJoinStrictness(JoinKind kind, JoinStrictness strictness)
{
    /// `Unspecified` can appear before defaults are fully normalized and means
    /// the regular `ALL` strictness for plain `JOIN`.
    if (kind == JoinKind::Full)
        return strictness == JoinStrictness::All || strictness == JoinStrictness::Unspecified;

    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
        return kind == JoinKind::Left || kind == JoinKind::Right;

    return strictness == JoinStrictness::All
        || strictness == JoinStrictness::Any
        || strictness == JoinStrictness::RightAny
        || strictness == JoinStrictness::Unspecified;
}

bool isSupportedJoinKind(JoinKind kind)
{
    return kind == JoinKind::Inner
        || kind == JoinKind::Left
        || kind == JoinKind::Right
        || kind == JoinKind::Full;
}

bool collectUsingJoinKeys(DistributedShuffleJoinInfo & info, const JoinNode & join_node)
{
    const auto * list_node = join_node.getJoinExpression()->as<ListNode>();
    if (!list_node || list_node->getNodes().size() != 1)
        return false;

    const auto * using_column_node = list_node->getNodes().front()->as<ColumnNode>();
    if (!using_column_node)
        return false;

    info.left_key_column_name = using_column_node->getColumnName();
    info.right_key_column_name = using_column_node->getColumnName();

    const auto create_key_column = [&](const std::vector<QueryTreeNodePtr> & sources) -> QueryTreeNodePtr
    {
        QueryTreeNodePtr result_source;
        std::optional<NameAndTypePair> result_column;

        for (const auto & source : sources)
        {
            const auto * table_node = source ? source->as<TableNode>() : nullptr;
            if (!table_node || !table_node->getStorageSnapshot())
                continue;

            auto column = table_node->getStorageSnapshot()->tryGetColumn(
                GetColumnsOptions(GetColumnsOptions::All),
                using_column_node->getColumnName());
            if (!column)
                continue;

            result_source = source;
            result_column = std::move(column);
            break;
        }

        if (!result_column || !result_source)
            return {};

        return std::make_shared<ColumnNode>(*result_column, result_source);
    };

    info.left_key_expression = create_key_column(info.left_column_sources);
    info.right_key_expression = create_key_column(info.right_column_sources);
    if (!info.left_key_expression || !info.right_key_expression)
        return false;

    return true;
}

const ColumnNode * getDirectKeyColumnFromSide(
    const QueryTreeNodePtr & node,
    const DistributedShuffleJoinInfo & info,
    bool left_side)
{
    const auto * column_node = node->as<ColumnNode>();
    if (!column_node)
        return nullptr;

    auto column_source = column_node->getColumnSourceOrNull();
    if (!column_source)
        return nullptr;

    if (left_side)
    {
        if (!isLeftColumnSource(info, column_source))
            return nullptr;
    }
    else if (!isRightColumnSource(info, column_source))
    {
        return nullptr;
    }

    return column_node;
}

bool collectOnJoinKeys(DistributedShuffleJoinInfo & info, const JoinNode & join_node)
{
    const auto * equals_function = join_node.getJoinExpression()->as<FunctionNode>();
    if (!equals_function || equals_function->getFunctionName() != "equals")
        return false;

    const auto & arguments = equals_function->getArguments().getNodes();
    if (arguments.size() != 2)
        return false;

    auto [first_source, first_source_valid] = getExpressionSource(arguments[0]);
    auto [second_source, second_source_valid] = getExpressionSource(arguments[1]);
    if (!first_source_valid || !second_source_valid || !first_source || !second_source)
        return false;

    const bool left_to_right = isLeftColumnSource(info, first_source) && isRightColumnSource(info, second_source);
    const bool right_to_left = isRightColumnSource(info, first_source) && isLeftColumnSource(info, second_source);
    if (!left_to_right && !right_to_left)
        return false;

    info.left_key_expression = left_to_right ? arguments[0] : arguments[1];
    info.right_key_expression = left_to_right ? arguments[1] : arguments[0];

    const auto * left_key_column = getDirectKeyColumnFromSide(info.left_key_expression, info, true);
    const auto * right_key_column = getDirectKeyColumnFromSide(info.right_key_expression, info, false);
    if (!left_key_column || !right_key_column)
        return false;

    info.left_key_column_name = left_key_column->getColumnName();
    info.right_key_column_name = right_key_column->getColumnName();
    return true;
}

bool isSemiOrAntiJoin(JoinStrictness strictness)
{
    return strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti;
}

bool isExpressionFromSemiAntiOutputSide(
    const QueryTreeNodePtr & node,
    const DistributedShuffleJoinInfo & info)
{
    if (!isSemiOrAntiJoin(info.join_strictness))
        return true;

    auto [source, valid] = getExpressionSource(node);
    if (!valid)
        return false;

    if (!source)
        return true;

    if (info.join_kind == JoinKind::Left)
        return isLeftColumnSource(info, source);

    if (info.join_kind == JoinKind::Right)
        return isRightColumnSource(info, source);

    return false;
}

void collectRequiredColumns(DistributedShuffleJoinInfo & info, QueryTreeNodePtr query_tree)
{
    CollectColumnSourceToColumnsVisitor visitor;
    visitor.visit(query_tree);

    const auto & columns = visitor.getColumnSourceToColumns();

    for (const auto & source : info.left_column_sources)
    {
        auto left_columns_it = columns.find(source);
        if (left_columns_it != columns.end())
        {
            for (const auto & column : left_columns_it->second.columns)
                info.left_required_columns.push_back(column);
        }
    }

    for (const auto & source : info.right_column_sources)
    {
        auto right_columns_it = columns.find(source);
        if (right_columns_it != columns.end())
        {
            for (const auto & column : right_columns_it->second.columns)
                info.right_required_columns.push_back(column);
        }
    }
}

std::optional<String> formatPostJoinExpression(
    const QueryTreeNodePtr & node,
    const DistributedShuffleJoinInfo & info);

bool isDeterministicFilterExpression(const QueryTreeNodePtr & node);

bool collectProjectionColumns(DistributedShuffleJoinInfo & info, const QueryNode & query_node)
{
    const auto & projection_nodes = query_node.getProjection().getNodes();
    const auto & projection_columns = query_node.getProjectionColumns();

    if (projection_nodes.size() != projection_columns.size())
        return false;

    info.projection_columns.clear();
    info.projection_columns.reserve(projection_nodes.size());

    for (size_t i = 0; i < projection_nodes.size(); ++i)
    {
        if (!isExpressionFromSemiAntiOutputSide(projection_nodes[i], info))
            return false;

        if (const auto * column_node = projection_nodes[i]->as<ColumnNode>())
        {
            auto column_source = column_node->getColumnSourceOrNull();
            if (!column_source)
                return false;

            const bool is_left = isLeftColumnSource(info, column_source);
            const bool is_right = isRightColumnSource(info, column_source);
            if (!is_left && !is_right)
                return false;

            info.projection_columns.push_back(DistributedShuffleJoinProjectionColumn{
                .is_left = is_left,
                .is_hidden = false,
                .source_column_name = getMappedColumnName(info, column_source, column_node->getColumnName(), is_left),
                .expression = {},
                .result_column_name = projection_columns[i].name});
            continue;
        }

        if (!isDeterministicFilterExpression(projection_nodes[i]))
            return false;

        auto expression = formatPostJoinExpression(projection_nodes[i], info);
        if (!expression)
            return false;

        info.projection_columns.push_back(DistributedShuffleJoinProjectionColumn{
            .is_left = false,
            .is_hidden = false,
            .source_column_name = {},
            .expression = std::move(*expression),
            .result_column_name = projection_columns[i].name});
    }

    return true;
}

bool hasDuplicateProjectionColumnName(const NamesAndTypes & projection_columns, size_t projection_index)
{
    if (projection_index >= projection_columns.size())
        return false;

    size_t count = 0;
    for (const auto & projection_column : projection_columns)
        if (projection_column.name == projection_columns[projection_index].name)
            ++count;

    return count > 1;
}

bool isLogicalAndFunction(const FunctionNode & function_node)
{
    if (function_node.getFunctionName() == "and")
        return true;

    auto ast = function_node.toAST({});
    const auto * function_ast = ast ? ast->as<ASTFunction>() : nullptr;
    return function_ast && function_ast->name == "and";
}

void collectWhereConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & conjuncts)
{
    const auto * function_node = node->as<FunctionNode>();
    if (function_node && isLogicalAndFunction(*function_node))
    {
        for (const auto & argument : function_node->getArguments().getNodes())
            collectWhereConjuncts(argument, conjuncts);

        return;
    }

    conjuncts.push_back(node);
}

bool isDeterministicFilterExpression(const QueryTreeNodePtr & node)
{
    if (node->as<ColumnNode>())
        return true;

    if (const auto * constant_node = node->as<ConstantNode>())
        return constant_node->isDeterministic();

    if (const auto * function_node = node->as<FunctionNode>())
    {
        auto function = function_node->getFunction();
        if (!function || !function->isDeterministicInScopeOfQuery())
            return false;

        for (const auto & argument : function_node->getArguments().getNodes())
            if (!isDeterministicFilterExpression(argument))
                return false;

        return true;
    }

    return false;
}

String formatFilterCondition(const QueryTreeNodePtr & node)
{
    ConvertToASTOptions options;
    options.fully_qualified_identifiers = false;
    return node->toAST(options)->formatWithSecretsOneLine();
}

std::optional<String> formatSourceFilterExpression(
    const QueryTreeNodePtr & node,
    const DistributedShuffleJoinInfo & info,
    bool left_side)
{
    if (const auto * column_node = node->as<ColumnNode>())
    {
        auto source = column_node->getColumnSourceOrNull();
        if (!source)
            return {};

        if (left_side)
        {
            if (!isLeftColumnSource(info, source))
                return {};
        }
        else if (!isRightColumnSource(info, source))
        {
            return {};
        }

        return backQuoteIfNeed(getMappedColumnName(info, source, column_node->getColumnName(), left_side));
    }

    if (node->as<ConstantNode>())
        return formatFilterCondition(node);

    if (const auto * function_node = node->as<FunctionNode>())
    {
        String result = function_node->getFunctionName();
        result += "(";

        bool first = true;
        for (const auto & argument : function_node->getArguments().getNodes())
        {
            auto formatted_argument = formatSourceFilterExpression(argument, info, left_side);
            if (!formatted_argument)
                return {};

            if (!first)
                result += ", ";

            result += *formatted_argument;
            first = false;
        }

        result += ")";
        return result;
    }

    return {};
}

String combineFilterConditions(const std::vector<String> & conditions)
{
    String result;
    for (const auto & condition : conditions)
    {
        if (condition.empty())
            return {};

        if (!result.empty())
            result += " AND ";

        result += "(";
        result += condition;
        result += ")";
    }

    return result;
}

struct FilterSourceMask
{
    bool has_left = false;
    bool has_right = false;

    bool isConstant() const
    {
        return !has_left && !has_right;
    }

    bool isLeftOnly() const
    {
        return has_left && !has_right;
    }

    bool isRightOnly() const
    {
        return has_right && !has_left;
    }

    bool isPostJoin() const
    {
        return has_left && has_right;
    }
};

bool collectFilterSourceMask(
    const QueryTreeNodePtr & node,
    const DistributedShuffleJoinInfo & info,
    FilterSourceMask & result)
{
    if (const auto * column_node = node->as<ColumnNode>())
    {
        auto source = column_node->getColumnSourceOrNull();
        if (!source)
            return false;

        if (isLeftColumnSource(info, source))
        {
            result.has_left = true;
            return true;
        }

        if (isRightColumnSource(info, source))
        {
            result.has_right = true;
            return true;
        }

        return false;
    }

    if (node->as<ConstantNode>())
        return true;

    if (const auto * function_node = node->as<FunctionNode>())
    {
        for (const auto & argument : function_node->getArguments().getNodes())
            if (!collectFilterSourceMask(argument, info, result))
                return false;

        return true;
    }

    return false;
}

std::optional<String> formatPostJoinExpression(
    const QueryTreeNodePtr & node,
    const DistributedShuffleJoinInfo & info)
{
    if (const auto * column_node = node->as<ColumnNode>())
    {
        auto source = column_node->getColumnSourceOrNull();
        if (!source)
            return {};

        String result;
        if (isLeftColumnSource(info, source))
            result = "_shuffle_left.";
        else if (isRightColumnSource(info, source))
            result = "_shuffle_right.";
        else
            return {};

        const bool left_side = isLeftColumnSource(info, source);
        result += backQuoteIfNeed(getMappedColumnName(info, source, column_node->getColumnName(), left_side));
        return result;
    }

    if (node->as<ConstantNode>())
        return formatFilterCondition(node);

    if (const auto * function_node = node->as<FunctionNode>())
    {
        String result = function_node->getFunctionName();
        result += "(";

        bool first = true;
        for (const auto & argument : function_node->getArguments().getNodes())
        {
            auto formatted_argument = formatPostJoinExpression(argument, info);
            if (!formatted_argument)
                return {};

            if (!first)
                result += ", ";

            result += *formatted_argument;
            first = false;
        }

        result += ")";
        return result;
    }

    return {};
}

bool collectWhereFilters(DistributedShuffleJoinInfo & info, const QueryTreeNodes & conjuncts)
{
    std::vector<String> left_conditions;
    std::vector<String> right_conditions;
    std::vector<String> post_join_conditions;

    for (const auto & conjunct : conjuncts)
    {
        if (!isDeterministicFilterExpression(conjunct))
            return false;

        FilterSourceMask source_mask;
        if (!collectFilterSourceMask(conjunct, info, source_mask))
            return false;

        if (isSemiOrAntiJoin(info.join_strictness))
        {
            auto filter_condition = source_mask.isConstant()
                ? std::optional<String>(formatFilterCondition(conjunct))
                : formatSourceFilterExpression(conjunct, info, info.join_kind != JoinKind::Right);
            if (!filter_condition)
                return false;

            if (source_mask.isConstant())
            {
                if (info.join_kind == JoinKind::Right)
                    right_conditions.push_back(std::move(*filter_condition));
                else
                    left_conditions.push_back(std::move(*filter_condition));
            }
            else if (info.join_kind == JoinKind::Left && source_mask.isLeftOnly())
                left_conditions.push_back(std::move(*filter_condition));
            else if (info.join_kind == JoinKind::Right && source_mask.isRightOnly())
                right_conditions.push_back(std::move(*filter_condition));
            else
                return false;

            continue;
        }

        if (info.join_kind == JoinKind::Full
            || source_mask.isPostJoin()
            || (info.join_kind == JoinKind::Left && source_mask.isRightOnly())
            || (info.join_kind == JoinKind::Right && source_mask.isLeftOnly()))
        {
            auto filter_condition = formatPostJoinExpression(conjunct, info);
            if (!filter_condition)
                return false;

            post_join_conditions.push_back(std::move(*filter_condition));
            continue;
        }

        if (source_mask.isConstant())
        {
            auto filter_condition = formatFilterCondition(conjunct);
            if (info.join_kind == JoinKind::Right)
                right_conditions.push_back(std::move(filter_condition));
            else
                left_conditions.push_back(std::move(filter_condition));
        }
        else if (source_mask.isLeftOnly())
        {
            auto filter_condition = formatSourceFilterExpression(conjunct, info, true);
            if (!filter_condition)
                return false;

            left_conditions.push_back(std::move(*filter_condition));
        }
        else if (source_mask.isRightOnly())
        {
            auto filter_condition = formatSourceFilterExpression(conjunct, info, false);
            if (!filter_condition)
                return false;

            right_conditions.push_back(std::move(*filter_condition));
        }
        else
            return false;
    }

    info.left_filter_condition = combineFilterConditions(left_conditions);
    info.right_filter_condition = combineFilterConditions(right_conditions);
    info.post_join_filter_condition = combineFilterConditions(post_join_conditions);
    return true;
}

bool collectWhereFilters(DistributedShuffleJoinInfo & info, const QueryNode & query_node)
{
    if (!query_node.hasWhere())
        return true;

    QueryTreeNodes conjuncts;
    collectWhereConjuncts(query_node.getWhere(), conjuncts);
    return collectWhereFilters(info, conjuncts);
}

bool collectExpressionTableIndexes(
    const QueryTreeNodePtr & node,
    const std::vector<QueryTreeNodePtr> & table_expressions,
    std::vector<size_t> & table_indexes)
{
    if (const auto * column_node = node->as<ColumnNode>())
    {
        auto source = column_node->getColumnSourceOrNull();
        if (!source)
            return false;

        for (size_t table_index = 0; table_index < table_expressions.size(); ++table_index)
        {
            if (!isSameQueryTreeNode(source, table_expressions[table_index]))
                continue;

            if (std::find(table_indexes.begin(), table_indexes.end(), table_index) == table_indexes.end())
                table_indexes.push_back(table_index);

            return true;
        }

        return false;
    }

    if (node->as<ConstantNode>())
        return true;

    if (const auto * function_node = node->as<FunctionNode>())
    {
        for (const auto & argument : function_node->getArguments().getNodes())
            if (!collectExpressionTableIndexes(argument, table_expressions, table_indexes))
                return false;

        return true;
    }

    return false;
}

std::optional<size_t> tryGetEarliestLeftDeepFilterStage(
    const QueryTreeNodePtr & node,
    const std::vector<QueryTreeNodePtr> & table_expressions)
{
    std::vector<size_t> table_indexes;
    if (!collectExpressionTableIndexes(node, table_expressions, table_indexes))
        return {};

    if (table_indexes.empty())
        return 0;

    const auto max_table_index = *std::max_element(table_indexes.begin(), table_indexes.end());
    if (max_table_index == 0)
        return 0;

    return max_table_index - 1;
}

bool collectLeftDeepWhereFilters(
    std::vector<DistributedShuffleJoinInfo> & stage_infos,
    const std::vector<QueryTreeNodePtr> & table_expressions,
    const QueryNode & query_node)
{
    if (!query_node.hasWhere())
        return true;

    QueryTreeNodes conjuncts;
    collectWhereConjuncts(query_node.getWhere(), conjuncts);

    std::vector<QueryTreeNodes> stage_conjuncts(stage_infos.size());

    for (const auto & conjunct : conjuncts)
    {
        if (!isDeterministicFilterExpression(conjunct))
            return false;

        auto stage_index = tryGetEarliestLeftDeepFilterStage(conjunct, table_expressions);
        if (!stage_index || *stage_index >= stage_infos.size())
            return false;

        stage_conjuncts[*stage_index].push_back(conjunct);
    }

    for (size_t stage_index = 0; stage_index < stage_infos.size(); ++stage_index)
    {
        if (!stage_conjuncts[stage_index].empty() && !collectWhereFilters(stage_infos[stage_index], stage_conjuncts[stage_index]))
            return false;
    }

    return true;
}

bool collectLimit(DistributedShuffleJoinInfo & info, const QueryNode & query_node)
{
    if (!query_node.hasLimit())
        return !query_node.hasOffset();

    if (query_node.isLimitWithTies())
    {
        if (info.order_by.empty())
            return false;

        info.limit_with_ties = true;
    }

    const auto * limit_node = query_node.getLimit()->as<ConstantNode>();
    if (!limit_node || limit_node->getValue().getType() != Field::Types::UInt64)
        return false;

    info.limit_length = limit_node->getValue().safeGet<UInt64>();
    if (!query_node.hasOffset())
        return true;

    const auto * offset_node = query_node.getOffset()->as<ConstantNode>();
    if (!offset_node || offset_node->getValue().getType() != Field::Types::UInt64)
        return false;

    info.limit_offset = offset_node->getValue().safeGet<UInt64>();
    return true;
}

bool collectOrderBy(DistributedShuffleJoinInfo & info, const QueryNode & query_node)
{
    if (!query_node.hasOrderBy())
        return true;

    const auto & projection_nodes = query_node.getProjection().getNodes();
    const auto & projection_columns = query_node.getProjectionColumns();
    if (projection_nodes.size() != projection_columns.size())
        return false;

    auto make_hidden_order_by_column_name = [&]() -> String
    {
        for (size_t index = 0;; ++index)
        {
            String name = "_shuffle_order_by_" + std::to_string(index);
            bool exists = false;
            for (const auto & projection_column : info.projection_columns)
            {
                if (projection_column.result_column_name == name)
                {
                    exists = true;
                    break;
                }
            }

            if (!exists)
                return name;
        }
    };

    for (const auto & order_by_node : query_node.getOrderBy().getNodes())
    {
        const auto * sort_node = order_by_node->as<SortNode>();
        if (!sort_node || sort_node->withFill() || sort_node->getCollator())
            return false;

        size_t projection_index = projection_nodes.size();
        for (size_t i = 0; i < projection_nodes.size(); ++i)
        {
            if (projection_nodes[i]->isEqual(*sort_node->getExpression(), {.compare_aliases = false}))
            {
                projection_index = i;
                break;
            }
        }

        String order_by_column_name;
        const auto append_hidden_order_by_column = [&]() -> bool
        {
            if (!isExpressionFromSemiAntiOutputSide(sort_node->getExpression(), info))
                return false;

            if (!isDeterministicFilterExpression(sort_node->getExpression()))
                return false;

            auto expression = formatPostJoinExpression(sort_node->getExpression(), info);
            if (!expression)
                return false;

            order_by_column_name = make_hidden_order_by_column_name();
            info.projection_columns.push_back(DistributedShuffleJoinProjectionColumn{
                .is_left = false,
                .is_hidden = true,
                .source_column_name = {},
                .expression = std::move(*expression),
                .result_column_name = order_by_column_name});
            return true;
        };

        if (projection_index != projection_nodes.size()
            && !hasDuplicateProjectionColumnName(projection_columns, projection_index))
        {
            order_by_column_name = projection_columns[projection_index].name;
        }
        else
        {
            if (!append_hidden_order_by_column())
                return false;
        }

        const int direction = sort_node->getSortDirection() == SortDirection::ASCENDING ? 1 : -1;
        const auto nulls_sort_direction = sort_node->getNullsSortDirection();
        const int nulls_direction = nulls_sort_direction
            ? (*nulls_sort_direction == SortDirection::ASCENDING ? 1 : -1)
            : direction;

        info.order_by.emplace_back(std::move(order_by_column_name), direction, nulls_direction);
    }

    return true;
}

void addRequiredColumnIfMissing(NamesAndTypes & required_columns, NameAndTypePair column)
{
    for (const auto & required_column : required_columns)
        if (required_column.name == column.name)
            return;

    required_columns.push_back(std::move(column));
}

void ensureKeyColumnsAreRequired(DistributedShuffleJoinInfo & info)
{
    if (const auto * left_key_column = info.left_key_expression->as<ColumnNode>())
        addRequiredColumnIfMissing(info.left_required_columns, left_key_column->getColumn());

    if (const auto * right_key_column = info.right_key_expression->as<ColumnNode>())
        addRequiredColumnIfMissing(info.right_required_columns, right_key_column->getColumn());
}

bool hasOneReplicaPerShard(const Cluster & cluster)
{
    for (const auto & shard_info : cluster.getShardsInfo())
        if (shard_info.getAllNodeCount() != 1)
            return false;

    return true;
}

bool isSupportedLeftDeepStageJoin(const JoinNode & join_node)
{
    if (join_node.getKind() != JoinKind::Inner)
        return false;

    const auto strictness = join_node.getStrictness() == JoinStrictness::Unspecified ? JoinStrictness::All : join_node.getStrictness();
    if (strictness != JoinStrictness::All && strictness != JoinStrictness::Any)
        return false;

    return join_node.getLocality() != JoinLocality::Global && join_node.hasJoinExpression();
}

const StorageDistributed * getDistributedStorage(const TableNode * table_node)
{
    if (!table_node)
        return nullptr;

    return typeid_cast<const StorageDistributed *>(table_node->getStorage().get());
}

bool appendSourceColumns(
    NamesAndTypes & result,
    const std::unordered_map<QueryTreeNodePtr, CollectColumnSourceToColumnsVisitor::Columns> & columns,
    const QueryTreeNodePtr & source,
    std::vector<DistributedShuffleJoinColumnNameMapping> * mappings = nullptr)
{
    auto it = columns.find(source);
    if (it == columns.end())
        return true;

    for (const auto & column : it->second.columns)
    {
        result.push_back(column);
        if (mappings)
        {
            mappings->push_back(DistributedShuffleJoinColumnNameMapping{
                .source = source,
                .source_column_name = column.name,
                .mapped_column_name = column.name});
        }
    }

    return true;
}

void collectColumnsFromNode(
    CollectColumnSourceToColumnsVisitor & visitor,
    const QueryTreeNodePtr & node)
{
    auto node_copy = node;
    visitor.visit(node_copy);
}

bool collectColumnsRequiredAfterStage(
    CollectColumnSourceToColumnsVisitor & visitor,
    const QueryNode & query_node,
    const std::vector<const JoinNode *> & joins,
    const std::vector<QueryTreeNodePtr> & table_expressions,
    size_t first_later_join_index)
{
    for (const auto & projection_node : query_node.getProjection().getNodes())
        collectColumnsFromNode(visitor, projection_node);

    for (size_t join_index = first_later_join_index; join_index < joins.size(); ++join_index)
        collectColumnsFromNode(visitor, joins[join_index]->getJoinExpression());

    if (query_node.hasWhere())
    {
        const auto current_stage_index = first_later_join_index - 1;
        QueryTreeNodes conjuncts;
        collectWhereConjuncts(query_node.getWhere(), conjuncts);
        for (const auto & conjunct : conjuncts)
        {
            auto filter_stage_index = tryGetEarliestLeftDeepFilterStage(conjunct, table_expressions);
            if (filter_stage_index && *filter_stage_index < current_stage_index)
                continue;

            collectColumnsFromNode(visitor, conjunct);
        }
    }

    if (query_node.hasOrderBy())
    {
        for (const auto & order_by_node : query_node.getOrderBy().getNodes())
        {
            const auto * sort_node = order_by_node->as<SortNode>();
            if (!sort_node)
                return false;

            collectColumnsFromNode(visitor, sort_node->getExpression());
        }
    }

    return true;
}

bool hasDuplicateColumnNames(const NamesAndTypes & columns)
{
    NameSet names;
    for (const auto & column : columns)
    {
        if (!names.insert(column.name).second)
            return true;
    }

    return false;
}

String makeInternalLeftDeepColumnName(size_t column_index, const String & column_name)
{
    return "_shuffle_stage_col_" + std::to_string(column_index) + "_" + column_name;
}

void assignUniqueRequiredColumnNames(
    NamesAndTypes & columns,
    std::vector<DistributedShuffleJoinColumnNameMapping> & mappings)
{
    if (columns.size() != mappings.size())
        return;

    NameSet used_names;
    for (size_t column_index = 0; column_index < columns.size(); ++column_index)
    {
        auto column_name = columns[column_index].name;
        if (used_names.contains(column_name))
        {
            column_name = makeInternalLeftDeepColumnName(column_index, columns[column_index].name);
            while (used_names.contains(column_name))
                column_name = "_" + column_name;

            columns[column_index].name = column_name;
        }

        mappings[column_index].mapped_column_name = column_name;
        used_names.insert(column_name);
    }
}

std::optional<String> tryGetMappedColumnName(
    const std::vector<DistributedShuffleJoinColumnNameMapping> & mappings,
    const QueryTreeNodePtr & source,
    const String & column_name)
{
    for (const auto & mapping : mappings)
    {
        if (mapping.source_column_name == column_name && isSameQueryTreeNode(mapping.source, source))
            return mapping.mapped_column_name;
    }

    return {};
}

void ensureKeyColumnMapping(
    std::vector<DistributedShuffleJoinColumnNameMapping> & mappings,
    const QueryTreeNodePtr & key_expression)
{
    const auto * key_column = key_expression ? key_expression->as<ColumnNode>() : nullptr;
    if (!key_column)
        return;

    auto source = key_column->getColumnSourceOrNull();
    if (!source)
        return;

    if (tryGetMappedColumnName(mappings, source, key_column->getColumnName()))
        return;

    mappings.push_back(DistributedShuffleJoinColumnNameMapping{
        .source = source,
        .source_column_name = key_column->getColumnName(),
        .mapped_column_name = key_column->getColumnName()});
}

void updateMappedKeyColumnNames(DistributedShuffleJoinInfo & info)
{
    if (const auto * left_key_column = info.left_key_expression ? info.left_key_expression->as<ColumnNode>() : nullptr)
    {
        auto source = left_key_column->getColumnSourceOrNull();
        if (source)
            info.left_key_column_name = getMappedColumnName(info, source, left_key_column->getColumnName(), true);
    }

    if (const auto * right_key_column = info.right_key_expression ? info.right_key_expression->as<ColumnNode>() : nullptr)
    {
        auto source = right_key_column->getColumnSourceOrNull();
        if (source)
            info.right_key_column_name = getMappedColumnName(info, source, right_key_column->getColumnName(), false);
    }
}

bool buildStageOutputProjection(
    DistributedShuffleJoinInfo & stage_info,
    const DistributedShuffleJoinInfo & next_stage_info)
{
    stage_info.projection_columns.clear();
    stage_info.projection_columns.reserve(next_stage_info.left_column_name_mappings.size());

    for (const auto & output_column : next_stage_info.left_column_name_mappings)
    {
        if (isLeftColumnSource(stage_info, output_column.source))
        {
            auto source_column_name = tryGetMappedColumnName(
                stage_info.left_column_name_mappings,
                output_column.source,
                output_column.source_column_name);
            if (!source_column_name)
                source_column_name = output_column.source_column_name;

            stage_info.projection_columns.push_back(DistributedShuffleJoinProjectionColumn{
                .is_left = true,
                .is_hidden = false,
                .source_column_name = std::move(*source_column_name),
                .expression = {},
                .result_column_name = output_column.mapped_column_name});
            continue;
        }

        if (isRightColumnSource(stage_info, output_column.source))
        {
            auto source_column_name = tryGetMappedColumnName(
                stage_info.right_column_name_mappings,
                output_column.source,
                output_column.source_column_name);
            if (!source_column_name)
                source_column_name = output_column.source_column_name;

            stage_info.projection_columns.push_back(DistributedShuffleJoinProjectionColumn{
                .is_left = false,
                .is_hidden = false,
                .source_column_name = std::move(*source_column_name),
                .expression = {},
                .result_column_name = output_column.mapped_column_name});
            continue;
        }

        return false;
    }

    return true;
}

bool collectLeftDeepJoinTree(
    const QueryTreeNodePtr & node,
    std::vector<const JoinNode *> & joins,
    std::vector<QueryTreeNodePtr> & table_expressions,
    std::vector<const StorageDistributed *> & storages)
{
    const auto * join_node = node->as<JoinNode>();
    if (!join_node || !isSupportedLeftDeepStageJoin(*join_node))
        return false;

    const auto & left = join_node->getLeftTableExpression();
    if (left->as<JoinNode>())
    {
        if (!collectLeftDeepJoinTree(left, joins, table_expressions, storages))
            return false;
    }
    else
    {
        const auto * left_table_node = left->as<TableNode>();
        const auto * left_storage = getDistributedStorage(left_table_node);
        if (!left_storage)
            return false;

        table_expressions.push_back(left);
        storages.push_back(left_storage);
    }

    const auto * right_table_node = join_node->getRightTableExpression()->as<TableNode>();
    const auto * right_storage = getDistributedStorage(right_table_node);
    if (!right_storage)
        return false;

    joins.push_back(join_node);
    table_expressions.push_back(join_node->getRightTableExpression());
    storages.push_back(right_storage);
    return true;
}

std::vector<QueryTreeNodePtr> makeLeftSideSources(
    const std::vector<QueryTreeNodePtr> & table_expressions,
    size_t stage_index)
{
    std::vector<QueryTreeNodePtr> result;
    result.reserve(stage_index + 1);
    for (size_t table_index = 0; table_index <= stage_index; ++table_index)
        result.push_back(table_expressions[table_index]);

    return result;
}

}

bool isDistributedShuffleJoinClusterLayoutSupported(
    const Cluster & left_cluster,
    const Cluster & right_cluster)
{
    if (left_cluster.getName().empty() || left_cluster.getName() != right_cluster.getName())
        return false;

    if (left_cluster.getShardCount() < 2 || left_cluster.getShardCount() != right_cluster.getShardCount())
        return false;

    return hasOneReplicaPerShard(left_cluster) && hasOneReplicaPerShard(right_cluster);
}

std::optional<DistributedShuffleJoinInfo> tryAnalyzeDistributedShuffleJoin(
    const QueryTreeNodePtr & query_tree,
    ContextPtr context)
{
    if (!context)
        return {};

    if (context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas] > 0)
        return {};

    const auto * query_node = getSingleSelectQueryNode(query_tree);
    if (!query_node)
        return {};

    if (hasUnsupportedSelectClauses(*query_node))
        return {};

    const auto * join_node = query_node->getJoinTree()->as<JoinNode>();
    if (!join_node)
        return {};

    if (!isSupportedJoinKind(join_node->getKind())
        || !isSupportedJoinStrictness(join_node->getKind(), join_node->getStrictness())
        || join_node->getLocality() == JoinLocality::Global
        || !join_node->hasJoinExpression())
    {
        return {};
    }

    const auto * left_table_node = join_node->getLeftTableExpression()->as<TableNode>();
    const auto * right_table_node = join_node->getRightTableExpression()->as<TableNode>();
    if (!left_table_node || !right_table_node)
        return {};

    const auto * left_storage = typeid_cast<const StorageDistributed *>(left_table_node->getStorage().get());
    const auto * right_storage = typeid_cast<const StorageDistributed *>(right_table_node->getStorage().get());
    if (!left_storage || !right_storage)
        return {};

    auto left_cluster = left_storage->getCluster();
    auto right_cluster = right_storage->getCluster();
    if (!left_cluster || !right_cluster)
        return {};

    if (!isDistributedShuffleJoinClusterLayoutSupported(*left_cluster, *right_cluster))
        return {};

    DistributedShuffleJoinInfo info;
    info.left_table_expression = join_node->getLeftTableExpression();
    info.right_table_expression = join_node->getRightTableExpression();
    initializeBinaryColumnSources(info);
    info.left_storage = left_storage;
    info.right_storage = right_storage;
    info.left_source_database = left_storage->getRemoteDatabaseName();
    info.left_source_table = left_storage->getRemoteTableName();
    info.right_source_database = right_storage->getRemoteDatabaseName();
    info.right_source_table = right_storage->getRemoteTableName();
    info.join_kind = join_node->getKind();
    info.join_strictness = join_node->getStrictness() == JoinStrictness::Unspecified ? JoinStrictness::All : join_node->getStrictness();
    info.cluster_name = left_cluster->getName();
    info.shuffle_database = left_storage->getStorageID().database_name;
    info.shard_count = left_cluster->getShardCount();

    bool keys_collected = false;
    if (join_node->isUsingJoinExpression())
        keys_collected = collectUsingJoinKeys(info, *join_node);
    else if (join_node->isOnJoinExpression())
        keys_collected = collectOnJoinKeys(info, *join_node);

    if (!keys_collected)
        return {};

    collectRequiredColumns(info, query_tree);
    ensureKeyColumnsAreRequired(info);

    if (!collectWhereFilters(info, *query_node))
        return {};

    if (!collectProjectionColumns(info, *query_node))
        return {};

    if (!collectOrderBy(info, *query_node))
        return {};

    if (!collectLimit(info, *query_node))
        return {};

    return info;
}

std::optional<DistributedShuffleJoinLeftDeepInfo> tryAnalyzeDistributedShuffleJoinLeftDeep(
    const QueryTreeNodePtr & query_tree,
    ContextPtr context)
{
    if (!context)
        return {};

    if (context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas] > 0)
        return {};

    const auto * query_node = getSingleSelectQueryNode(query_tree);
    if (!query_node)
        return {};

    if (hasUnsupportedSelectClauses(*query_node))
        return {};

    std::vector<const JoinNode *> joins;
    std::vector<QueryTreeNodePtr> table_expressions;
    std::vector<const StorageDistributed *> storages;
    if (!collectLeftDeepJoinTree(query_node->getJoinTree(), joins, table_expressions, storages))
        return {};

    if (joins.size() < 2 || table_expressions.size() != joins.size() + 1 || storages.size() != table_expressions.size())
        return {};

    const auto * first_storage = storages.front();
    auto first_cluster = first_storage->getCluster();
    if (!first_cluster)
        return {};

    for (size_t table_index = 1; table_index < storages.size(); ++table_index)
    {
        auto cluster = storages[table_index]->getCluster();
        if (!cluster || !isDistributedShuffleJoinClusterLayoutSupported(*first_cluster, *cluster))
            return {};
    }

    std::vector<DistributedShuffleJoinInfo> stage_infos;
    stage_infos.reserve(joins.size());
    for (size_t stage_index = 0; stage_index < joins.size(); ++stage_index)
    {
        const auto * join = joins[stage_index];
        auto & stage_info = stage_infos.emplace_back();
        stage_info.left_table_expression = join->getLeftTableExpression();
        stage_info.right_table_expression = join->getRightTableExpression();
        stage_info.left_column_sources = stage_index == 0
            ? std::vector<QueryTreeNodePtr>{table_expressions[0]}
            : makeLeftSideSources(table_expressions, stage_index);
        stage_info.right_column_sources = {table_expressions[stage_index + 1]};
        stage_info.join_kind = join->getKind();
        stage_info.join_strictness = join->getStrictness() == JoinStrictness::Unspecified ? JoinStrictness::All : join->getStrictness();
        stage_info.cluster_name = first_cluster->getName();
        stage_info.shuffle_database = first_storage->getStorageID().database_name;
        stage_info.shard_count = first_cluster->getShardCount();

        if (stage_index == 0)
        {
            stage_info.left_storage = storages[0];
            stage_info.left_source_database = storages[0]->getRemoteDatabaseName();
            stage_info.left_source_table = storages[0]->getRemoteTableName();
        }

        stage_info.right_storage = storages[stage_index + 1];
        stage_info.right_source_database = storages[stage_index + 1]->getRemoteDatabaseName();
        stage_info.right_source_table = storages[stage_index + 1]->getRemoteTableName();

        bool keys_collected = false;
        if (join->isUsingJoinExpression())
            keys_collected = collectUsingJoinKeys(stage_info, *join);
        else if (join->isOnJoinExpression())
            keys_collected = collectOnJoinKeys(stage_info, *join);

        if (!keys_collected)
            return {};
    }

    for (size_t stage_index = 0; stage_index < stage_infos.size(); ++stage_index)
    {
        CollectColumnSourceToColumnsVisitor visitor;
        if (!collectColumnsRequiredAfterStage(visitor, *query_node, joins, table_expressions, stage_index + 1))
            return {};

        const auto & source_columns = visitor.getColumnSourceToColumns();

        for (size_t table_index = 0; table_index <= stage_index; ++table_index)
        {
            if (!appendSourceColumns(
                    stage_infos[stage_index].left_required_columns,
                    source_columns,
                    table_expressions[table_index],
                    &stage_infos[stage_index].left_column_name_mappings))
            {
                return {};
            }
        }

        if (!appendSourceColumns(
                stage_infos[stage_index].right_required_columns,
                source_columns,
                table_expressions[stage_index + 1],
                &stage_infos[stage_index].right_column_name_mappings))
        {
            return {};
        }

        ensureKeyColumnsAreRequired(stage_infos[stage_index]);
        ensureKeyColumnMapping(stage_infos[stage_index].left_column_name_mappings, stage_infos[stage_index].left_key_expression);
        ensureKeyColumnMapping(stage_infos[stage_index].right_column_name_mappings, stage_infos[stage_index].right_key_expression);
        assignUniqueRequiredColumnNames(stage_infos[stage_index].left_required_columns, stage_infos[stage_index].left_column_name_mappings);
        assignUniqueRequiredColumnNames(stage_infos[stage_index].right_required_columns, stage_infos[stage_index].right_column_name_mappings);
        updateMappedKeyColumnNames(stage_infos[stage_index]);
    }

    for (size_t stage_index = 0; stage_index + 1 < stage_infos.size(); ++stage_index)
    {
        if (hasDuplicateColumnNames(stage_infos[stage_index + 1].left_required_columns))
            return {};

        if (!buildStageOutputProjection(stage_infos[stage_index], stage_infos[stage_index + 1]))
            return {};
    }

    if (!collectLeftDeepWhereFilters(stage_infos, table_expressions, *query_node))
        return {};

    auto & final_stage = stage_infos.back();
    if (!collectProjectionColumns(final_stage, *query_node))
        return {};

    if (!collectOrderBy(final_stage, *query_node))
        return {};

    if (!collectLimit(final_stage, *query_node))
        return {};

    DistributedShuffleJoinLeftDeepInfo result;
    result.cluster_name = first_cluster->getName();
    result.shuffle_database = first_storage->getStorageID().database_name;
    result.shard_count = first_cluster->getShardCount();
    result.stages.reserve(stage_infos.size());
    for (size_t stage_index = 0; stage_index < stage_infos.size(); ++stage_index)
    {
        result.stages.push_back(DistributedShuffleJoinLeftDeepStageInfo{
            .info = std::move(stage_infos[stage_index]),
            .output_columns = stage_index + 1 < stage_infos.size() ? stage_infos[stage_index + 1].left_required_columns : NamesAndTypes{}});
    }

    return result;
}

}
