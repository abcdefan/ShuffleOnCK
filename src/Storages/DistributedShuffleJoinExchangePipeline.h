#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedShuffleJoinCoordinator.h>
#include <Storages/DistributedShuffleJoinAnalyzer.h>
#include <Storages/DistributedShuffleJoinSink.h>

#include <memory>

namespace DB
{

class Cluster;

using ClusterPtr = std::shared_ptr<Cluster>;

class IDistributedShuffleJoinExchangeSideExecutor
{
public:
    virtual ~IDistributedShuffleJoinExchangeSideExecutor() = default;

    virtual void executeSide(
        DistributedShuffleJoinTableSide side,
        const DistributedShuffleJoinTableNames & table_names) = 0;
};

class LocalDistributedShuffleJoinExchangeSideExecutor final : public IDistributedShuffleJoinExchangeSideExecutor
{
public:
    LocalDistributedShuffleJoinExchangeSideExecutor(
        ContextPtr context_,
        ClusterPtr cluster_,
        DistributedShuffleJoinInfo info_,
        DistributedShuffleJoinBlockSenderPtr sender_);

    void executeSide(
        DistributedShuffleJoinTableSide side,
        const DistributedShuffleJoinTableNames & table_names) override;

private:
    ContextPtr context;
    ClusterPtr cluster;
    DistributedShuffleJoinInfo info;
    DistributedShuffleJoinBlockSenderPtr sender;
};

class CurrentShardDistributedShuffleJoinExchangeExecutor final : public IDistributedShuffleJoinExchangeExecutor
{
public:
    CurrentShardDistributedShuffleJoinExchangeExecutor(
        size_t current_shard_index_,
        IDistributedShuffleJoinExchangeSideExecutor & side_executor_,
        DistributedShuffleJoinBlockSenderPtr sender_);
    CurrentShardDistributedShuffleJoinExchangeExecutor(
        size_t current_shard_index_,
        ContextPtr context_,
        ClusterPtr cluster_,
        DistributedShuffleJoinInfo info_,
        DistributedShuffleJoinBlockSenderPtr sender_);

    void executeOnShard(
        size_t shard_index,
        const DistributedShuffleJoinTableNames & table_names) override;

private:
    size_t current_shard_index;
    DistributedShuffleJoinBlockSenderPtr sender;
    std::unique_ptr<IDistributedShuffleJoinExchangeSideExecutor> owned_side_executor;
    IDistributedShuffleJoinExchangeSideExecutor * side_executor;
};

std::unique_ptr<CurrentShardDistributedShuffleJoinExchangeExecutor> createCurrentShardDistributedShuffleJoinExchangeExecutor(
    size_t current_shard_index,
    ContextPtr context,
    ClusterPtr cluster,
    DistributedShuffleJoinInfo info);

Block createDistributedShuffleJoinExchangeHeader(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side);

String createDistributedShuffleJoinExchangeSourceQuery(
    String database,
    String table,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side);
String createDistributedShuffleJoinExchangeSourceQuery(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side);
String createDistributedShuffleJoinLocalJoinQuery(
    const DistributedShuffleJoinTableNames & table_names,
    const DistributedShuffleJoinInfo & info);
String createDistributedShuffleJoinRemoteExchangeQuery(String payload);

std::shared_ptr<DistributedShuffleJoinSink> createDistributedShuffleJoinExchangeSink(
    ClusterPtr cluster,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableNames table_names,
    DistributedShuffleJoinTableSide side,
    DistributedShuffleJoinBlockSenderPtr sender);

void executeDistributedShuffleJoinExchangeQuery(
    String query,
    ContextPtr context,
    ClusterPtr cluster,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableNames table_names,
    DistributedShuffleJoinTableSide side,
    DistributedShuffleJoinBlockSenderPtr sender);
void executeDistributedShuffleJoinExchangeSide(
    ContextPtr context,
    ClusterPtr cluster,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableNames table_names,
    DistributedShuffleJoinTableSide side,
    DistributedShuffleJoinBlockSenderPtr sender);
void executeDistributedShuffleJoinExchangeSource(
    const DistributedShuffleJoinTableNames & table_names,
    IDistributedShuffleJoinExchangeSideExecutor & side_executor,
    DistributedShuffleJoinBlockSenderPtr sender);

}
