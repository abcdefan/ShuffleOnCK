#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <optional>

namespace DB
{

class StorageDistributed;

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

    String cluster_name;
    size_t shard_count = 0;
};

/// Return information required by the MVP distributed `shuffle join` path if
/// the query shape is eligible. This function must not modify the query tree.
std::optional<DistributedShuffleJoinInfo> tryAnalyzeDistributedShuffleJoin(
    const QueryTreeNodePtr & query_tree,
    ContextPtr context);

}
