#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DistributedShuffleJoinTables.h>

#include <memory>

namespace DB
{

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

class IDistributedShuffleJoinQueryExecutor
{
public:
    virtual ~IDistributedShuffleJoinQueryExecutor() = default;

    virtual void executeOnShard(size_t shard_index, const String & query) = 0;
};

class ClusterDistributedShuffleJoinQueryExecutor final : public IDistributedShuffleJoinQueryExecutor
{
public:
    ClusterDistributedShuffleJoinQueryExecutor(ClusterPtr cluster_, ContextPtr context_);

    void executeOnShard(size_t shard_index, const String & query) override;

private:
    String makeQueryId(size_t shard_index);
    void executeLocal(const String & query, size_t shard_index);
    void executeRemote(const String & query, size_t shard_index);

    ClusterPtr cluster;
    ContextPtr context;
    UInt64 query_index = 0;
};

class DistributedShuffleJoinCoordinator final
{
public:
    DistributedShuffleJoinCoordinator(
        DistributedShuffleJoinTableNames table_names_,
        Block left_header_,
        Block right_header_,
        size_t shard_count_,
        IDistributedShuffleJoinQueryExecutor & executor_);

    ~DistributedShuffleJoinCoordinator();

    DistributedShuffleJoinCoordinator(const DistributedShuffleJoinCoordinator &) = delete;
    DistributedShuffleJoinCoordinator & operator=(const DistributedShuffleJoinCoordinator &) = delete;

    const DistributedShuffleJoinTableNames & getTableNames() const
    {
        return table_names;
    }

    void prepareShuffleTables();
    void cleanupShuffleTables() noexcept;

    bool hasPreparedShuffleTables() const
    {
        return prepared;
    }

    bool hasCleanedUpShuffleTables() const
    {
        return cleaned_up;
    }

private:
    void executeForAllShards(const String & query);

    const DistributedShuffleJoinTableNames table_names;
    const Block left_header;
    const Block right_header;
    const size_t shard_count;
    IDistributedShuffleJoinQueryExecutor & executor;

    bool prepare_started = false;
    bool prepared = false;
    bool cleaned_up = false;
};

}
