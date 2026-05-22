#include <Interpreters/DistributedShuffleJoinCoordinator.h>

#include <Client/Connection.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Interpreters/ClientInfo.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

ClusterDistributedShuffleJoinQueryExecutor::ClusterDistributedShuffleJoinQueryExecutor(
    ClusterPtr cluster_,
    ContextPtr context_)
    : cluster(std::move(cluster_))
    , context(std::move(context_))
{
    if (!cluster)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` query executor cannot be created without cluster");

    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` query executor cannot be created without context");

    if (cluster->getShardCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` query executor cannot be created with empty cluster");

    base_query_id = context->getCurrentQueryId();
    if (base_query_id.empty())
        base_query_id = fmt::format("distributed_shuffle_join_{}", UUIDHelpers::generateV4());
}

void ClusterDistributedShuffleJoinQueryExecutor::executeOnShard(size_t shard_index, const String & query)
{
    if (shard_index >= cluster->getShardCount())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` shard index {} is out of range for cluster with {} shards",
            shard_index,
            cluster->getShardCount());
    }

    const auto & shard_info = cluster->getShardsInfo()[shard_index];
    if (shard_info.isLocal())
        executeLocal(query, shard_index);
    else
        executeRemote(query, shard_index);
}

String ClusterDistributedShuffleJoinQueryExecutor::makeQueryId(size_t shard_index)
{
    return fmt::format("{}:shuffle:{}:{}", base_query_id, shard_index, query_index++);
}

void ClusterDistributedShuffleJoinQueryExecutor::executeLocal(const String & query, size_t shard_index)
{
    auto query_context = Context::createCopy(context);
    query_context->setCurrentQueryId(makeQueryId(shard_index));
    query_context->setInternalQuery(true);

    auto block_io = executeQuery(query, query_context, QueryFlags{.internal = true}, QueryProcessingStage::Complete).second;
    executeTrivialBlockIO(block_io, query_context);
}

void ClusterDistributedShuffleJoinQueryExecutor::executeRemote(const String & query, size_t shard_index)
{
    const auto & shard_info = cluster->getShardsInfo()[shard_index];
    if (!shard_info.pool)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed `shuffle join` remote shard {} does not have a connection pool",
            shard_index);
    }

    const auto & settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    auto connection = shard_info.pool->get(timeouts, settings, true);
    auto client_info = context->getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    ++client_info.distributed_depth;

    connection->sendQuery(
        timeouts,
        query,
        {},
        makeQueryId(shard_index),
        QueryProcessingStage::Complete,
        &settings,
        &client_info,
        false,
        {},
        {});

    while (true)
    {
        Packet packet = connection->receivePacket();

        if (packet.type == Protocol::Server::EndOfStream)
            return;

        if (packet.type == Protocol::Server::Exception)
            packet.exception->rethrow();

        if (packet.type == Protocol::Server::Log
            || packet.type == Protocol::Server::Progress
            || packet.type == Protocol::Server::ProfileEvents
            || packet.type == Protocol::Server::TimezoneUpdate)
            continue;

        throw NetException(
            ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Unexpected packet from server while executing distributed `shuffle join` query on shard {} "
            "(expected EndOfStream or Exception, got {})",
            shard_index,
            Protocol::Server::toString(packet.type));
    }
}

DistributedShuffleJoinCoordinator::DistributedShuffleJoinCoordinator(
    DistributedShuffleJoinTableNames table_names_,
    Block left_header_,
    Block right_header_,
    size_t shard_count_,
    IDistributedShuffleJoinQueryExecutor & executor_)
    : table_names(std::move(table_names_))
    , left_header(std::move(left_header_))
    , right_header(std::move(right_header_))
    , shard_count(shard_count_)
    , executor(executor_)
{
    if (shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` coordinator must have at least one shard");
}

DistributedShuffleJoinCoordinator::~DistributedShuffleJoinCoordinator()
{
    cleanupShuffleTables();
}

void DistributedShuffleJoinCoordinator::prepareShuffleTables()
{
    if (prepared)
        return;

    prepare_started = true;

    try
    {
        executeForAllShards(createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Left, left_header));
        executeForAllShards(createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Right, right_header));
        prepared = true;
        cleaned_up = false;
    }
    catch (...)
    {
        cleanupShuffleTables();
        throw;
    }
}

void DistributedShuffleJoinCoordinator::exchangeShuffleTables(IDistributedShuffleJoinExchangeExecutor & exchange_executor)
{
    if (!prepared)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` tables must be prepared before exchange");

    if (exchanged)
        return;

    try
    {
        exchangeForAllShards(exchange_executor);
        exchanged = true;
    }
    catch (...)
    {
        cleanupShuffleTables();
        throw;
    }
}

void DistributedShuffleJoinCoordinator::joinShuffleTables(
    const String & local_join_query,
    IDistributedShuffleJoinLocalJoinExecutor & local_join_executor)
{
    if (!exchanged)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` tables must be exchanged before local join");

    if (local_join_query.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join query cannot be empty");

    if (joined)
        return;

    try
    {
        joinForAllShards(local_join_query, local_join_executor);
        joined = true;
    }
    catch (...)
    {
        cleanupShuffleTables();
        throw;
    }
}

void DistributedShuffleJoinCoordinator::cleanupShuffleTables() noexcept
{
    if (!prepare_started || cleaned_up)
        return;

    try
    {
        executeForAllShards(dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Left));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to drop left distributed `shuffle join` table");
    }

    try
    {
        executeForAllShards(dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Right));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to drop right distributed `shuffle join` table");
    }

    cleaned_up = true;
    prepared = false;
    exchanged = false;
    joined = false;
}

void DistributedShuffleJoinCoordinator::executeForAllShards(const String & query)
{
    for (size_t shard_index = 0; shard_index < shard_count; ++shard_index)
        executor.executeOnShard(shard_index, query);
}

void DistributedShuffleJoinCoordinator::exchangeForAllShards(IDistributedShuffleJoinExchangeExecutor & exchange_executor)
{
    for (size_t shard_index = 0; shard_index < shard_count; ++shard_index)
        exchange_executor.executeOnShard(shard_index, table_names);
}

void DistributedShuffleJoinCoordinator::joinForAllShards(
    const String & local_join_query,
    IDistributedShuffleJoinLocalJoinExecutor & local_join_executor)
{
    for (size_t shard_index = 0; shard_index < shard_count; ++shard_index)
        local_join_executor.executeOnShard(shard_index, local_join_query);
}

}
