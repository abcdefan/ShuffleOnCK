#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
// StorageDistributed中的资格判断
bool shouldRewriteDistributedShuffleJoinQuery(const QueryTreeNodePtr & query_tree, ContextPtr context);

// executeQuery中的真正改写
bool rewriteDistributedShuffleJoinQueryForShard(
    QueryTreeNodePtr & query_tree,
    size_t shard_index,
    size_t shard_count,
    ContextPtr context);

}
