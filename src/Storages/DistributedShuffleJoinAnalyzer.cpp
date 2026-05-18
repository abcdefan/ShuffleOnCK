#include <Storages/DistributedShuffleJoinAnalyzer.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageDistributed.h>

#include <unordered_map>

namespace DB
{

namespace
{

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

}

std::optional<DistributedShuffleJoinInfo> tryAnalyzeDistributedShuffleJoin(
    const QueryTreeNodePtr & query_tree,
    ContextPtr)
{
    const auto * query_node = query_tree ? query_tree->as<QueryNode>() : nullptr;
    if (!query_node)
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

    if (left_cluster->getName() != right_cluster->getName())
        return {};

    if (left_cluster->getShardCount() < 2)
        return {};

    DistributedShuffleJoinInfo info;
    info.left_table_expression = join_node->getLeftTableExpression();
    info.right_table_expression = join_node->getRightTableExpression();
    info.left_storage = left_storage;
    info.right_storage = right_storage;
    info.cluster_name = left_cluster->getName();
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
    return info;
}

}
