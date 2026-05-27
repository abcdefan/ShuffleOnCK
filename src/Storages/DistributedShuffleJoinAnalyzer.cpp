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
#include <Storages/StorageDistributed.h>

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
        || query_node.isLimitByAll()
        || query_node.isLimitWithTies();
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

bool isSupportedJoinStrictness(JoinStrictness strictness)
{
    /// `Unspecified` can appear before defaults are fully normalized and means
    /// the regular `ALL` strictness for plain `JOIN`.
    return strictness == JoinStrictness::All || strictness == JoinStrictness::Unspecified;
}

bool collectUsingJoinKeys(DistributedShuffleJoinInfo & info, const JoinNode & join_node)
{
    const auto * list_node = join_node.getJoinExpression()->as<ListNode>();
    if (!list_node || list_node->getNodes().size() != 1)
        return false;

    const auto * using_column_node = list_node->getNodes().front()->as<ColumnNode>();
    if (!using_column_node)
        return false;

    info.left_key_expression = list_node->getNodes().front();
    info.right_key_expression = list_node->getNodes().front();
    info.left_key_column_name = using_column_node->getColumnName();
    info.right_key_column_name = using_column_node->getColumnName();
    return true;
}

const ColumnNode * getDirectKeyColumn(const QueryTreeNodePtr & node, const QueryTreeNodePtr & expected_source)
{
    const auto * column_node = node->as<ColumnNode>();
    if (!column_node)
        return nullptr;

    auto column_source = column_node->getColumnSourceOrNull();
    if (!column_source || !column_source->isEqual(*expected_source))
        return nullptr;

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

    const bool left_to_right = first_source->isEqual(*info.left_table_expression) && second_source->isEqual(*info.right_table_expression);
    const bool right_to_left = first_source->isEqual(*info.right_table_expression) && second_source->isEqual(*info.left_table_expression);
    if (!left_to_right && !right_to_left)
        return false;

    info.left_key_expression = left_to_right ? arguments[0] : arguments[1];
    info.right_key_expression = left_to_right ? arguments[1] : arguments[0];

    const auto * left_key_column = getDirectKeyColumn(info.left_key_expression, info.left_table_expression);
    const auto * right_key_column = getDirectKeyColumn(info.right_key_expression, info.right_table_expression);
    if (!left_key_column || !right_key_column)
        return false;

    info.left_key_column_name = left_key_column->getColumnName();
    info.right_key_column_name = right_key_column->getColumnName();
    return true;
}

void collectRequiredColumns(DistributedShuffleJoinInfo & info, QueryTreeNodePtr query_tree)
{
    CollectColumnSourceToColumnsVisitor visitor;
    visitor.visit(query_tree);

    const auto & columns = visitor.getColumnSourceToColumns();

    auto left_columns_it = columns.find(info.left_table_expression);
    if (left_columns_it != columns.end())
        info.left_required_columns = left_columns_it->second.columns;

    auto right_columns_it = columns.find(info.right_table_expression);
    if (right_columns_it != columns.end())
        info.right_required_columns = right_columns_it->second.columns;
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
        if (const auto * column_node = projection_nodes[i]->as<ColumnNode>())
        {
            auto column_source = column_node->getColumnSourceOrNull();
            if (!column_source)
                return false;

            const bool is_left = column_source->isEqual(*info.left_table_expression);
            const bool is_right = column_source->isEqual(*info.right_table_expression);
            if (!is_left && !is_right)
                return false;

            info.projection_columns.push_back(DistributedShuffleJoinProjectionColumn{is_left, column_node->getColumnName(), {}, projection_columns[i].name});
            continue;
        }

        if (!isDeterministicFilterExpression(projection_nodes[i]))
            return false;

        auto expression = formatPostJoinExpression(projection_nodes[i], info);
        if (!expression)
            return false;

        info.projection_columns.push_back(DistributedShuffleJoinProjectionColumn{false, {}, std::move(*expression), projection_columns[i].name});
    }

    return true;
}

void collectWhereConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & conjuncts)
{
    const auto * function_node = node->as<FunctionNode>();
    if (function_node && function_node->getFunctionName() == "and")
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

        if (source->isEqual(*info.left_table_expression))
        {
            result.has_left = true;
            return true;
        }

        if (source->isEqual(*info.right_table_expression))
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
        if (source->isEqual(*info.left_table_expression))
            result = "_shuffle_left.";
        else if (source->isEqual(*info.right_table_expression))
            result = "_shuffle_right.";
        else
            return {};

        result += backQuoteIfNeed(column_node->getColumnName());
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

bool collectWhereFilters(DistributedShuffleJoinInfo & info, const QueryNode & query_node)
{
    if (!query_node.hasWhere())
        return true;

    QueryTreeNodes conjuncts;
    collectWhereConjuncts(query_node.getWhere(), conjuncts);

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

        if (source_mask.isPostJoin())
        {
            auto filter_condition = formatPostJoinExpression(conjunct, info);
            if (!filter_condition)
                return false;

            post_join_conditions.push_back(std::move(*filter_condition));
            continue;
        }

        auto filter_condition = formatFilterCondition(conjunct);
        if (source_mask.isConstant() || source_mask.isLeftOnly())
            left_conditions.push_back(std::move(filter_condition));
        else if (source_mask.isRightOnly())
            right_conditions.push_back(std::move(filter_condition));
        else
            return false;
    }

    info.left_filter_condition = combineFilterConditions(left_conditions);
    info.right_filter_condition = combineFilterConditions(right_conditions);
    info.post_join_filter_condition = combineFilterConditions(post_join_conditions);
    return true;
}

bool collectLimit(DistributedShuffleJoinInfo & info, const QueryNode & query_node)
{
    if (!query_node.hasLimit())
        return !query_node.hasOffset();

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

        if (projection_index == projection_nodes.size())
            return false;

        const int direction = sort_node->getSortDirection() == SortDirection::ASCENDING ? 1 : -1;
        const auto nulls_sort_direction = sort_node->getNullsSortDirection();
        const int nulls_direction = nulls_sort_direction
            ? (*nulls_sort_direction == SortDirection::ASCENDING ? 1 : -1)
            : direction;

        info.order_by.emplace_back(projection_columns[projection_index].name, direction, nulls_direction);
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

    if (join_node->getKind() != JoinKind::Inner
        || !isSupportedJoinStrictness(join_node->getStrictness())
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
    info.left_storage = left_storage;
    info.right_storage = right_storage;
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

}
