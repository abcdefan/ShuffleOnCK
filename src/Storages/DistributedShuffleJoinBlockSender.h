#pragma once

#include <Client/ConnectionPool.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DistributedShuffleJoinSink.h>

#include <array>
#include <memory>
#include <vector>

namespace DB
{

class Cluster;
class PushingPipelineExecutor;
class QueryPipeline;

using ClusterPtr = std::shared_ptr<Cluster>;

class ClusterDistributedShuffleJoinBlockSender final : public DistributedShuffleJoinBlockSender
{
public:
    ClusterDistributedShuffleJoinBlockSender(ClusterPtr cluster_, ContextPtr context_);
    ~ClusterDistributedShuffleJoinBlockSender() override;

    void sendBlock(
        const DistributedShuffleJoinTableNames & table_names,
        size_t target_shard_index,
        DistributedShuffleJoinTableSide side,
        Block block) override;

    void finish(
        const DistributedShuffleJoinTableNames & table_names,
        size_t target_shard_index,
        DistributedShuffleJoinTableSide side) override;

    void cancel(const DistributedShuffleJoinTableNames & table_names, String reason) noexcept override;

private:
    struct WritingJob;

    static size_t sideIndex(DistributedShuffleJoinTableSide side);

    WritingJob & getJob(size_t target_shard_index, DistributedShuffleJoinTableSide side);
    WritingJob & getOrCreateJob(
        const DistributedShuffleJoinTableNames & table_names,
        size_t target_shard_index,
        DistributedShuffleJoinTableSide side);
    void cancelJobs() noexcept;

    void initializeLocalJob(
        WritingJob & job,
        const DistributedShuffleJoinTableNames & table_names,
        DistributedShuffleJoinTableSide side);
    void initializeRemoteJob(
        WritingJob & job,
        const DistributedShuffleJoinTableNames & table_names,
        size_t target_shard_index,
        DistributedShuffleJoinTableSide side);

    ClusterPtr cluster;
    ContextPtr context;
    std::vector<std::array<std::unique_ptr<WritingJob>, 2>> jobs;
};

}
