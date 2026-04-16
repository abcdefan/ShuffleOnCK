#include <Storages/buildQueryTreeDistributedForShuffle.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Identifier.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Planner/Utils.h>
#include <Storages/StorageDistributed.h>

namespace DB
{

namespace
{

/// 这个 visitor 用来收集 query tree 中每个 table expression 实际被引用到了哪些列。
/// 后面在把左右表包装成子查询时，只读取真正需要的列，避免无意义地全列读取。
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

/// 这份结构体保存一次 `shuffle join` rewrite 所需的核心信息：
/// - 左右两边原始 table expression 是谁
/// - 左右两边底层是不是 `StorageDistributed`
/// - join key 是什么
/// 当前第一版只支持：
/// - 单个 `USING (k)`
/// - 单个 `ON left_key = right_key`
struct DistributedShuffleJoinRewriteInfo
{
    QueryTreeNodePtr left_table_expression;
    QueryTreeNodePtr right_table_expression;
    const StorageDistributed * left_storage = nullptr;
    const StorageDistributed * right_storage = nullptr;
    String using_column_name;
    QueryTreeNodePtr left_key_expression;
    QueryTreeNodePtr right_key_expression;
};

/// 对 `USING (k)` 的场景，需要基于左右各自的 table expression 单独重新解析列引用。
/// 不能简单复用同一个 `IdentifierNode`，因为左右两边的列来源不同。
QueryTreeNodePtr buildResolvedColumnExpressionForTableExpression(
    const String & column_name,
    const QueryTreeNodePtr & table_expression,
    ContextPtr context)
{
    QueryTreeNodePtr node = std::make_shared<IdentifierNode>(Identifier{column_name});
    QueryAnalysisPass query_analysis_pass(table_expression);
    query_analysis_pass.run(node, context);
    return node;
}

/// 构造当前 shard 对应的 bucket 过滤条件：
/// `cityHash64(key_expression) % shard_count = shard_index`
/// 这是方案 1 里每个 shard 只负责一个 bucket 的核心表达式。
QueryTreeNodePtr buildBucketFilter(
    QueryTreeNodePtr key_expression,
    size_t shard_index,
    size_t shard_count,
    ContextPtr context)
{
    /// 先构造 `cityHash64(key_expression)`。
    auto city_hash_function = std::make_shared<FunctionNode>("cityHash64");
    city_hash_function->getArguments().getNodes().push_back(std::move(key_expression));
    resolveOrdinaryFunctionNodeByName(*city_hash_function, "cityHash64", context);

    /// 构造常量 `shard_count`，作为 `% N` 里的 `N`。
    auto shard_count_node = std::make_shared<ConstantNode>(static_cast<UInt64>(shard_count), std::make_shared<DataTypeUInt64>());

    /// 构造 `cityHash64(key_expression) % shard_count`。
    auto modulo_function = std::make_shared<FunctionNode>("modulo");
    modulo_function->getArguments().getNodes().push_back(city_hash_function);
    modulo_function->getArguments().getNodes().push_back(shard_count_node);
    resolveOrdinaryFunctionNodeByName(*modulo_function, "modulo", context);

    /// 构造当前 shard 对应的 bucket 常量 `shard_index`。
    auto shard_index_node = std::make_shared<ConstantNode>(static_cast<UInt64>(shard_index), std::make_shared<DataTypeUInt64>());

    /// 最后构造完整过滤条件：
    /// `cityHash64(key_expression) % shard_count = shard_index`
    auto equals_function = std::make_shared<FunctionNode>("equals");
    equals_function->getArguments().getNodes().push_back(modulo_function);
    equals_function->getArguments().getNodes().push_back(shard_index_node);
    resolveOrdinaryFunctionNodeByName(*equals_function, "equals", context);

    return equals_function;
}

/// 基于当前 rewrite 信息，构造某一侧表在当前 table expression 语义下的 join key 表达式。
/// - `USING` 需要重新解析列名
/// - `ON` 场景可以直接 clone 之前提取出来的左右 key 表达式
QueryTreeNodePtr buildJoinKeyExpressionForTableExpression(
    const DistributedShuffleJoinRewriteInfo & rewrite_info,
    bool is_left,
    ContextPtr context)
{
    if (!rewrite_info.using_column_name.empty())
    {
        return buildResolvedColumnExpressionForTableExpression(
            rewrite_info.using_column_name,
            is_left ? rewrite_info.left_table_expression : rewrite_info.right_table_expression,
            context);
    }

    return (is_left ? rewrite_info.left_key_expression : rewrite_info.right_key_expression)->clone();
}

/// 尝试从 top-level query 中提取一次 `shuffle join` rewrite 所需的信息。
/// 这个函数同时承担两件事：
/// - 先做资格判断，确认当前 query 形态是否落在第一版支持范围内
/// - 如果资格判断通过，再构造后续 rewrite 要用到的 `rewrite_info`
///
/// 当前整体流程按下面顺序执行：
/// 1. 检查 root 是否为 `QueryNode`
/// 2. 检查 top-level join tree 是否为 `JoinNode`
/// 3. 检查是否是受支持的 join 类型：
///    - 只支持 `INNER JOIN`
///    - 不支持 `GLOBAL JOIN`
///    - 必须存在 join 条件
/// 4. 检查左右两边是否都是直接的 `TableNode`
/// 5. 检查左右底层存储是否都是 `StorageDistributed`
/// 6. 检查左右是否属于同一个 `cluster`
/// 7. 检查该 `cluster` 的 `shard` 数是否至少为 2
/// 8. 在上述公共前置条件都通过后，先构造一份基础的 `rewrite_info`
///    其中先保存左右 table expression 和左右 `StorageDistributed`
/// 9. 最后再解析 join key：
///    - 要么是单个 `USING (k)`
///    - 要么是单个 `ON equals(left_key, right_key)`
///
/// 只有当以上步骤全部成功时，才返回完整的 `rewrite_info`。
std::optional<DistributedShuffleJoinRewriteInfo> tryGetDistributedShuffleJoinRewriteInfo(
    const QueryTreeNodePtr & query_tree)
{
    /// 当前只支持 `SELECT` 顶层 query。
    const auto * query_node = query_tree->as<QueryNode>();
    if (!query_node)
        return {};

    /// 当前只支持 top-level `JoinNode`。
    const auto * join_node = query_node->getJoinTree()->as<JoinNode>();
    if (!join_node)
        return {};

    /// 第一版只支持：
    /// - `INNER JOIN`
    /// - 非 `GLOBAL JOIN`
    /// - 必须存在 join 条件
    if (join_node->getKind() != JoinKind::Inner || join_node->getLocality() == JoinLocality::Global || !join_node->hasJoinExpression())
        return {};

    /// 左右两边必须都是直接的 `TableNode`，暂时不支持更复杂的 join tree 形态。
    const auto * left_table_node = join_node->getLeftTableExpression()->as<TableNode>();
    const auto * right_table_node = join_node->getRightTableExpression()->as<TableNode>();
    if (!left_table_node || !right_table_node)
        return {};

    /// 左右底层存储都必须是 `StorageDistributed`，否则不属于方案 1 的适用范围。
    const auto * left_storage = typeid_cast<const StorageDistributed *>(left_table_node->getStorage().get());
    const auto * right_storage = typeid_cast<const StorageDistributed *>(right_table_node->getStorage().get());
    if (!left_storage || !right_storage)
        return {};

    /// 当前实现只支持左右两边属于同一个 `cluster`。
    /// 这样后面才可以直接定义：
    /// `N = cluster.shard_count`
    /// `bucket i -> shard i`
    if (left_storage->getCluster()->getName() != right_storage->getCluster()->getName())
        return {};

    /// 至少需要 2 个 `shard` 才有进入 `shuffle` 路径的意义。
    if (left_storage->getCluster()->getShardCount() < 2)
        return {};

    DistributedShuffleJoinRewriteInfo rewrite_info
    {
        .left_table_expression = join_node->getLeftTableExpression(),
        .right_table_expression = join_node->getRightTableExpression(),
        .left_storage = left_storage,
        .right_storage = right_storage,
        .using_column_name = {},
        .left_key_expression = {},
        .right_key_expression = {},
    };

    /// `USING` 场景下，当前只支持单个 key，例如 `USING (id)`。
    /// 这里先把列名保存下来，后面会基于左右各自 table expression 重新解析。
    if (join_node->isUsingJoinExpression())
    {
        const auto * list_node = join_node->getJoinExpression()->as<ListNode>();
        if (!list_node || list_node->getNodes().size() != 1)
            return {};

        const auto * using_column_node = list_node->getNodes().front()->as<ColumnNode>();
        if (!using_column_node)
            return {};

        rewrite_info.using_column_name = using_column_node->getColumnName();
        return rewrite_info;
    }

    if (join_node->isOnJoinExpression())
    {
        /// 第一版只支持单个 `equals(left_key, right_key)`。
        const auto * equals_function = join_node->getJoinExpression()->as<FunctionNode>();
        if (!equals_function || equals_function->getFunctionName() != "equals")
            return {};

        const auto & arguments = equals_function->getArguments().getNodes();
        if (arguments.size() != 2)
            return {};

        /// 分析 `equals` 两侧表达式分别来自哪一边的 table expression。
        /// 只有在一侧来自左表、另一侧来自右表时，才是当前实现支持的 join key 形态。
        auto [first_source, first_source_valid] = getExpressionSource(arguments[0]);
        auto [second_source, second_source_valid] = getExpressionSource(arguments[1]);
        if (!first_source_valid || !second_source_valid || !first_source || !second_source)
            return {};

        // on a.id = b.id
        const bool left_to_right = first_source->isEqual(*rewrite_info.left_table_expression) && second_source->isEqual(*rewrite_info.right_table_expression);
        // on b.id = a.id
        const bool right_to_left = first_source->isEqual(*rewrite_info.right_table_expression) && second_source->isEqual(*rewrite_info.left_table_expression);

        // 两种都为false会让if为true
        if (!left_to_right && !right_to_left)
            return {};

        /// 统一整理成：
        /// - `left_key_expression`
        /// - `right_key_expression`
        /// 后面构造左右 bucket 过滤条件时，直接按这个标准方向使用。
        rewrite_info.left_key_expression = left_to_right ? arguments[0] : arguments[1];
        rewrite_info.right_key_expression = left_to_right ? arguments[1] : arguments[0];

        return rewrite_info;
    }

    return {};
}

}

/// 外层入口只需要知道“这条 query 能不能进入 `shuffle join` rewrite”。
/// 这里直接复用上面的提取逻辑；能成功提取，就说明当前 query 形态满足第一版支持范围。
bool shouldRewriteDistributedShuffleJoinQuery(const QueryTreeNodePtr & query_tree, ContextPtr)
{
    return tryGetDistributedShuffleJoinRewriteInfo(query_tree).has_value();
}

/// 对某一个 shard 的 query tree 做真正的 rewrite。
/// 输入是原始 query tree 的一份 clone，输出是当前 shard 专属的 bucket query：
///
/// `FROM left_dist JOIN right_dist`
/// 会被替换成
/// `FROM (SELECT ... FROM left_dist WHERE bucket = i)
///       JOIN
///       (SELECT ... FROM right_dist WHERE bucket = i)`
///
/// 这里做的是“每个 shard 一份不同 query tree”的 rewrite，不负责分发本身。
bool rewriteDistributedShuffleJoinQueryForShard(
    QueryTreeNodePtr & query_tree,
    size_t shard_index,
    size_t shard_count,
    ContextPtr context)
{
    auto rewrite_info = tryGetDistributedShuffleJoinRewriteInfo(query_tree);
    if (!rewrite_info)
        return false;

    if (rewrite_info->left_storage->getCluster()->getShardCount() != shard_count)
        return false;

    /// 先收集左右两边实际被引用的列，后面构造子查询时只读这些列。
    CollectColumnSourceToColumnsVisitor collect_column_source_to_columns_visitor;
    collect_column_source_to_columns_visitor.visit(query_tree);
    const auto & column_source_to_columns = collect_column_source_to_columns_visitor.getColumnSourceToColumns();

    auto left_columns_it = column_source_to_columns.find(rewrite_info->left_table_expression);
    auto right_columns_it = column_source_to_columns.find(rewrite_info->right_table_expression);

    const NamesAndTypes left_columns = left_columns_it != column_source_to_columns.end() ? left_columns_it->second.columns : NamesAndTypes{};
    const NamesAndTypes right_columns = right_columns_it != column_source_to_columns.end() ? right_columns_it->second.columns : NamesAndTypes{};

    /// 把左右 table expression 分别包装成独立子查询。
    /// 这一步之后，左边和右边都会变成：
    /// `SELECT needed_columns FROM original_table_expression`
    auto left_subquery = buildSubqueryToReadColumnsFromTableExpression(left_columns, rewrite_info->left_table_expression, context);
    auto right_subquery = buildSubqueryToReadColumnsFromTableExpression(right_columns, rewrite_info->right_table_expression, context);

    auto & left_subquery_node = left_subquery->as<QueryNode &>();
    auto & right_subquery_node = right_subquery->as<QueryNode &>();

    /// 给左右子查询各自追加当前 shard 对应的 bucket 过滤条件。
    /// 这就是方案 1 里“每个 shard 只拉属于自己 bucket 的左右数据”的核心实现。
    left_subquery_node.getWhere() = buildBucketFilter(
        buildJoinKeyExpressionForTableExpression(*rewrite_info, true, left_subquery_node.getContext()),
        shard_index,
        shard_count,
        left_subquery_node.getContext());

    right_subquery_node.getWhere() = buildBucketFilter(
        buildJoinKeyExpressionForTableExpression(*rewrite_info, false, right_subquery_node.getContext()),
        shard_index,
        shard_count,
        right_subquery_node.getContext());

    /// 保留原始 alias，避免 rewrite 之后上层列引用失效。
    left_subquery->setAlias(rewrite_info->left_table_expression->getAlias());
    right_subquery->setAlias(rewrite_info->right_table_expression->getAlias());

    /// 用新的左右子查询替换掉原来的左右 table expression。
    IQueryTreeNode::ReplacementMap replacement_map;
    replacement_map.emplace(rewrite_info->left_table_expression.get(), left_subquery);
    replacement_map.emplace(rewrite_info->right_table_expression.get(), right_subquery);

    /// internal query 的 settings 由 remote `Context` 传递，这里清掉 query tree 上的 settings changes，
    /// 避免再次把外层 settings 文本化地保留在 rewritten query 里。
    query_tree = query_tree->cloneAndReplace(replacement_map);

    if (auto * query_node = query_tree->as<QueryNode>())
        query_node->clearSettingsChanges();

    /// 仅靠 `cloneAndReplace` 还不够，因为外层已经解析好的 `ColumnNode`
    /// 仍可能保留旧的 column source 绑定。
    /// 这里把 rewrite 后的 query tree 重新转成 AST，再重新 build + resolve 一次，
    /// 强制所有列引用都基于新的左右子查询重新绑定。
    auto rewritten_query_ast = queryNodeToDistributedSelectQuery(query_tree);
    query_tree = buildQueryTree(rewritten_query_ast, context);

    QueryAnalysisPass query_analysis_pass;
    query_analysis_pass.run(query_tree, context);

    return true;
}

}
