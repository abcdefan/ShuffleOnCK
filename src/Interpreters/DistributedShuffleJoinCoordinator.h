#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DistributedShuffleJoinTables.h>

#include <atomic>
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

    virtual void executeCleanupOnShard(size_t shard_index, const String & query)
    {
        executeOnShard(shard_index, query);
    }
};

class IDistributedShuffleJoinExchangeExecutor
{
public:
    virtual ~IDistributedShuffleJoinExchangeExecutor() = default;

    virtual void executeOnShard(size_t shard_index, const DistributedShuffleJoinTableNames & table_names) = 0;
};

class IDistributedShuffleJoinLocalJoinExecutor
{
public:
    virtual ~IDistributedShuffleJoinLocalJoinExecutor() = default;

    virtual void executeOnShard(size_t shard_index, const String & query) = 0;
};

class ClusterDistributedShuffleJoinQueryExecutor final
    : public IDistributedShuffleJoinQueryExecutor
    , public IDistributedShuffleJoinLocalJoinExecutor
{
public:
    ClusterDistributedShuffleJoinQueryExecutor(ClusterPtr cluster_, ContextPtr context_);

    void executeOnShard(size_t shard_index, const String & query) override;
    void executeCleanupOnShard(size_t shard_index, const String & query) override;

private:
    void executeOnShard(size_t shard_index, const String & query, bool observe_parent_cancellation);
    String makeQueryId(size_t shard_index);
    void executeLocal(const String & query, size_t shard_index, bool observe_parent_cancellation);
    void executeRemote(const String & query, size_t shard_index, bool observe_parent_cancellation);

    ClusterPtr cluster;
    ContextPtr context;
    String base_query_id;
    std::atomic<UInt64> query_index = 0;
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
    void exchangeShuffleTables(IDistributedShuffleJoinExchangeExecutor & exchange_executor);
    void joinShuffleTables(const String & local_join_query, IDistributedShuffleJoinLocalJoinExecutor & local_join_executor);
    void cleanupShuffleTables() noexcept;

    bool hasPreparedShuffleTables() const
    {
        return prepared;
    }

    bool hasCleanedUpShuffleTables() const
    {
        return cleaned_up;
    }

    bool hasExchangedShuffleTables() const
    {
        return exchanged;
    }

    bool hasJoinedShuffleTables() const
    {
        return joined;
    }

private:
    void executeForAllShards(const String & query);
    void cleanupForAllShards(const String & query, const String & table_side) noexcept;
    void exchangeForAllShards(IDistributedShuffleJoinExchangeExecutor & exchange_executor);
    void joinForAllShards(const String & local_join_query, IDistributedShuffleJoinLocalJoinExecutor & local_join_executor);

    const DistributedShuffleJoinTableNames table_names;
    const Block left_header;
    const Block right_header;
    const size_t shard_count;
    IDistributedShuffleJoinQueryExecutor & executor;

    bool prepare_started = false;
    bool prepared = false;
    bool exchanged = false;
    bool joined = false;
    bool cleaned_up = false;
};

}
