#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <optional>
#include <vector>

namespace DB
{

class Cluster;
class StorageDistributed;

struct DistributedShuffleJoinProjectionColumn
{
    bool is_left = false;
    String source_column_name;
    String expression;
    String result_column_name;
};

struct DistributedShuffleJoinInfo
{
    QueryTreeNodePtr left_table_expression;
    QueryTreeNodePtr right_table_expression;

    const StorageDistributed * left_storage = nullptr;
    const StorageDistributed * right_storage = nullptr;

    QueryTreeNodePtr left_key_expression;
    QueryTreeNodePtr right_key_expression;
    String left_key_column_name;
    String right_key_column_name;

    NamesAndTypes left_required_columns;
    NamesAndTypes right_required_columns;
    std::vector<DistributedShuffleJoinProjectionColumn> projection_columns;
    String left_filter_condition;
    String right_filter_condition;
    String post_join_filter_condition;
    SortDescription order_by;
    std::optional<UInt64> limit_length;
    UInt64 limit_offset = 0;

    String cluster_name;
    String shuffle_database;
    size_t shard_count = 0;
};

/// Return information required by the MVP distributed `shuffle join` path if
/// the query shape is eligible. This function must not modify the query tree.
std::optional<DistributedShuffleJoinInfo> tryAnalyzeDistributedShuffleJoin(
    const QueryTreeNodePtr & query_tree,
    ContextPtr context);

/// Return true if both sides use a cluster layout supported by the first
/// distributed `shuffle join` MVP.
bool isDistributedShuffleJoinClusterLayoutSupported(
    const Cluster & left_cluster,
    const Cluster & right_cluster);

}
