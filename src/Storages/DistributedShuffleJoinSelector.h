#pragma once

#include <Core/Names.h>
#include <Storages/DistributedShuffleJoinSink.h>

namespace DB
{

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

DistributedShuffleJoinSelector createDistributedShuffleJoinSelector(ClusterPtr cluster, String key_column_name);

}
