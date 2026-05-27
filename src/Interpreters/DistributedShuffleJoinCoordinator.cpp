#include <Interpreters/DistributedShuffleJoinCoordinator.h>

#include <Client/Connection.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Interpreters/ClientInfo.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>

#include <fmt/format.h>

namespace CurrentMetrics
{
    extern const Metric StorageDistributedThreads;
    extern const Metric StorageDistributedThreadsActive;
    extern const Metric StorageDistributedThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace
{

template <typename Callback>
void executeForShardsInParallel(size_t shard_count, Callback && callback)
{
    if (shard_count == 1)
    {
        callback(0);
        return;
    }

    ThreadPool pool(
        CurrentMetrics::StorageDistributedThreads,
        CurrentMetrics::StorageDistributedThreadsActive,
        CurrentMetrics::StorageDistributedThreadsScheduled,
        shard_count);
    auto thread_group = CurrentThread::getGroup();

    for (size_t shard_index = 0; shard_index < shard_count; ++shard_index)
    {
        pool.scheduleOrThrowOnError([&, shard_index, thread_group]()
        {
            ThreadGroupSwitcher switcher(thread_group, ThreadName::DISTRIBUTED_SINK);
            callback(shard_index);
        });
    }

    pool.wait();
}

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
    executeOnShard(shard_index, query, true);
}

void ClusterDistributedShuffleJoinQueryExecutor::executeCleanupOnShard(size_t shard_index, const String & query)
{
    executeOnShard(shard_index, query, false);
}

void ClusterDistributedShuffleJoinQueryExecutor::executeOnShard(
    size_t shard_index,
    const String & query,
    bool observe_parent_cancellation)
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
        executeLocal(query, shard_index, observe_parent_cancellation);
    else
        executeRemote(query, shard_index, observe_parent_cancellation);
}

String ClusterDistributedShuffleJoinQueryExecutor::makeQueryId(size_t shard_index)
{
    return fmt::format("{}:shuffle:{}:{}", base_query_id, shard_index, query_index.fetch_add(1, std::memory_order_relaxed));
}

void ClusterDistributedShuffleJoinQueryExecutor::executeLocal(
    const String & query,
    size_t shard_index,
    bool observe_parent_cancellation)
{
    auto query_context = Context::createCopy(context);
    query_context->setCurrentQueryId(makeQueryId(shard_index));
    query_context->setInternalQuery(true);

    if (observe_parent_cancellation)
    {
        auto parent_query_status = context->getProcessListElementSafe();
        auto parent_cancel_callback = context->getInteractiveCancelCallback();

        if (parent_query_status || parent_cancel_callback)
        {
            query_context->setInteractiveCancelCallback(
                [parent_query_status, upstream_cancel_callback = std::move(parent_cancel_callback), query_context]() mutable
                {
                    try
                    {
                        if (upstream_cancel_callback && upstream_cancel_callback())
                        {
                            query_context->killCurrentQuery();
                            return true;
                        }
                    }
                    catch (...)
                    {
                        query_context->killCurrentQuery();
                        throw;
                    }

                    if (!parent_query_status || !parent_query_status->isKilled())
                        return false;

                    query_context->killCurrentQuery();
                    return true;
                });
        }
    }

    auto block_io = executeQuery(query, query_context, QueryFlags{.internal = true}, QueryProcessingStage::Complete).second;
    executeTrivialBlockIO(block_io, query_context);
}

void ClusterDistributedShuffleJoinQueryExecutor::executeRemote(
    const String & query,
    size_t shard_index,
    bool observe_parent_cancellation)
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

    auto parent_query_status = observe_parent_cancellation ? context->getProcessListElementSafe() : nullptr;
    auto parent_cancel_callback = context->getInteractiveCancelCallback();
    if (!observe_parent_cancellation)
        parent_cancel_callback = {};
    bool cancel_sent = false;
    const auto send_cancel = [&]()
    {
        if (!cancel_sent)
        {
            connection->sendCancel();
            cancel_sent = true;
        }
    };

    while (true)
    {
        if (parent_query_status || parent_cancel_callback)
        {
            try
            {
                if ((parent_query_status && parent_query_status->isKilled())
                    || (parent_cancel_callback && parent_cancel_callback()))
                {
                    send_cancel();
                }
            }
            catch (...)
            {
                try
                {
                    send_cancel();
                }
                catch (...)
                {
                }
                throw;
            }

            if (!connection->checkPacket(100000))
                continue;
        }

        Packet packet = connection->receivePacket();

        if (packet.type == Protocol::Server::EndOfStream)
        {
            if (parent_query_status)
                parent_query_status->throwIfKilled();

            return;
        }

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

    cleanupForAllShards(dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Left), "left");
    cleanupForAllShards(dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Right), "right");

    cleaned_up = true;
    prepared = false;
    exchanged = false;
    joined = false;
}

void DistributedShuffleJoinCoordinator::executeForAllShards(const String & query)
{
    executeForShardsInParallel(shard_count, [&](size_t shard_index)
    {
        executor.executeOnShard(shard_index, query);
    });
}

void DistributedShuffleJoinCoordinator::cleanupForAllShards(const String & query, const String & table_side) noexcept
{
    try
    {
        executeForShardsInParallel(shard_count, [&](size_t shard_index)
        {
            try
            {
                executor.executeCleanupOnShard(shard_index, query);
            }
            catch (...)
            {
                tryLogCurrentException(
                    __PRETTY_FUNCTION__,
                    fmt::format("Failed to drop {} distributed `shuffle join` table on shard {}", table_side, shard_index));
            }
        });
    }
    catch (...)
    {
        tryLogCurrentException(
            __PRETTY_FUNCTION__,
            fmt::format("Failed to schedule {} distributed `shuffle join` table cleanup", table_side));
    }
}

void DistributedShuffleJoinCoordinator::exchangeForAllShards(IDistributedShuffleJoinExchangeExecutor & exchange_executor)
{
    executeForShardsInParallel(shard_count, [&](size_t shard_index)
    {
        exchange_executor.executeOnShard(shard_index, table_names);
    });
}

void DistributedShuffleJoinCoordinator::joinForAllShards(
    const String & local_join_query,
    IDistributedShuffleJoinLocalJoinExecutor & local_join_executor)
{
    executeForShardsInParallel(shard_count, [&](size_t shard_index)
    {
        local_join_executor.executeOnShard(shard_index, local_join_query);
    });
}

}
