#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Joins.h>
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
    bool is_hidden = false;
    String source_column_name;
    String expression;
    String result_column_name;
};

struct DistributedShuffleJoinColumnNameMapping
{
    QueryTreeNodePtr source;
    String source_column_name;
    String mapped_column_name;
};

struct DistributedShuffleJoinInfo
{
    QueryTreeNodePtr left_table_expression;
    QueryTreeNodePtr right_table_expression;
    std::vector<QueryTreeNodePtr> left_column_sources;
    std::vector<QueryTreeNodePtr> right_column_sources;

    const StorageDistributed * left_storage = nullptr;
    const StorageDistributed * right_storage = nullptr;
    String left_source_database;
    String left_source_table;
    String right_source_database;
    String right_source_table;

    QueryTreeNodePtr left_key_expression;
    QueryTreeNodePtr right_key_expression;
    String left_key_column_name;
    String right_key_column_name;
    JoinKind join_kind = JoinKind::Inner;
    JoinStrictness join_strictness = JoinStrictness::All;

    NamesAndTypes left_required_columns;
    NamesAndTypes right_required_columns;
    std::vector<DistributedShuffleJoinColumnNameMapping> left_column_name_mappings;
    std::vector<DistributedShuffleJoinColumnNameMapping> right_column_name_mappings;
    std::vector<DistributedShuffleJoinProjectionColumn> projection_columns;
    String left_filter_condition;
    String right_filter_condition;
    String post_join_filter_condition;
    SortDescription order_by;
    std::optional<UInt64> limit_length;
    UInt64 limit_offset = 0;
    bool limit_with_ties = false;

    String cluster_name;
    String shuffle_database;
    size_t shard_count = 0;
};

struct DistributedShuffleJoinLeftDeepStageInfo
{
    DistributedShuffleJoinInfo info;
    NamesAndTypes output_columns;
};

struct DistributedShuffleJoinLeftDeepInfo
{
    std::vector<DistributedShuffleJoinLeftDeepStageInfo> stages;
    String cluster_name;
    String shuffle_database;
    size_t shard_count = 0;
};

/// Return information required by the MVP distributed `shuffle join` path if
/// the query shape is eligible. This function must not modify the query tree.
std::optional<DistributedShuffleJoinInfo> tryAnalyzeDistributedShuffleJoin(
    const QueryTreeNodePtr & query_tree,
    ContextPtr context);

/// Return information required by the conservative left-deep multi-stage
/// distributed `shuffle join` path if the query shape is eligible. This function
/// must not modify the query tree.
std::optional<DistributedShuffleJoinLeftDeepInfo> tryAnalyzeDistributedShuffleJoinLeftDeep(
    const QueryTreeNodePtr & query_tree,
    ContextPtr context);

/// Return true if both sides use a cluster layout supported by the first
/// distributed `shuffle join` MVP.
bool isDistributedShuffleJoinClusterLayoutSupported(
    const Cluster & left_cluster,
    const Cluster & right_cluster);

}
