#pragma once

#include <Core/Names.h>
#include <Storages/DistributedShuffleJoinSink.h>

namespace DB
{

class Cluster;
struct DistributedShuffleJoinInfo;
using ClusterPtr = std::shared_ptr<Cluster>;

DistributedShuffleJoinSelector createDistributedShuffleJoinSelector(ClusterPtr cluster, String key_column_name);
DistributedShuffleJoinSelector createDistributedShuffleJoinSelector(
    ClusterPtr cluster,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side);

}
