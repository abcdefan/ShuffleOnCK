#include <Storages/DistributedShuffleJoinBlockSender.h>

#include <Client/ConnectionPoolWithFailover.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/RemoteSink.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

ASTPtr createInsertQuery(const DistributedShuffleJoinTableNames & table_names, DistributedShuffleJoinTableSide side)
{
    auto query = make_intrusive<ASTInsertQuery>();
    query->table_id = StorageID(table_names.database, table_names.getTableName(side));
    return query;
}

}

struct ClusterDistributedShuffleJoinBlockSender::WritingJob
{
    QueryPipeline pipeline;
    std::unique_ptr<PushingPipelineExecutor> executor;
    ConnectionPool::Entry connection_entry;
    String qualified_table_name;
    bool finished = false;
};

ClusterDistributedShuffleJoinBlockSender::ClusterDistributedShuffleJoinBlockSender(
    ClusterPtr cluster_,
    ContextPtr context_)
    : cluster(std::move(cluster_))
    , context(std::move(context_))
{
    if (!cluster)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` block sender cannot be created without cluster");

    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` block sender cannot be created without context");

    if (cluster->getShardCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` block sender cannot be created with empty cluster");

    jobs.resize(cluster->getShardCount());
}

ClusterDistributedShuffleJoinBlockSender::~ClusterDistributedShuffleJoinBlockSender()
{
    cancelJobs();
}

void ClusterDistributedShuffleJoinBlockSender::sendBlock(
    const DistributedShuffleJoinTableNames & table_names,
    size_t target_shard_index,
    DistributedShuffleJoinTableSide side,
    Block block)
{
    if (!block.rows())
        return;

    auto & job = getOrCreateJob(table_names, target_shard_index, side);
    if (job.finished)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed `shuffle join` block sender received data for already finished target shard {} and side {}",
            target_shard_index,
            toString(side));
    }

    job.executor->push(std::move(block));
}

void ClusterDistributedShuffleJoinBlockSender::finish(
    const DistributedShuffleJoinTableNames & table_names,
    size_t target_shard_index,
    DistributedShuffleJoinTableSide side)
{
    auto & job = getJob(target_shard_index, side);
    if (!job.executor)
        return;

    const auto qualified_table_name = table_names.getQualifiedTableName(side);
    if (job.qualified_table_name != qualified_table_name)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed `shuffle join` block sender tried to finish {} but active target is {}",
            qualified_table_name,
            job.qualified_table_name);
    }

    if (job.finished)
        return;

    job.executor->finish();
    job.finished = true;
}

void ClusterDistributedShuffleJoinBlockSender::cancel(const DistributedShuffleJoinTableNames &, String) noexcept
{
    cancelJobs();
}

void ClusterDistributedShuffleJoinBlockSender::cancelJobs() noexcept
{
    for (auto & shard_jobs : jobs)
    {
        for (auto & job : shard_jobs)
        {
            if (!job || !job->executor || job->finished)
                continue;

            try
            {
                job->executor->cancel();
                job->finished = true;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to cancel distributed `shuffle join` block sender job");
            }
        }
    }
}

size_t ClusterDistributedShuffleJoinBlockSender::sideIndex(DistributedShuffleJoinTableSide side)
{
    return side == DistributedShuffleJoinTableSide::Left ? 0 : 1;
}

ClusterDistributedShuffleJoinBlockSender::WritingJob &
ClusterDistributedShuffleJoinBlockSender::getJob(size_t target_shard_index, DistributedShuffleJoinTableSide side)
{
    if (target_shard_index >= jobs.size())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` target shard index {} is out of range for cluster with {} shards",
            target_shard_index,
            jobs.size());
    }

    auto & job = jobs[target_shard_index][sideIndex(side)];
    if (!job)
        job = std::make_unique<WritingJob>();

    return *job;
}

ClusterDistributedShuffleJoinBlockSender::WritingJob &
ClusterDistributedShuffleJoinBlockSender::getOrCreateJob(
    const DistributedShuffleJoinTableNames & table_names,
    size_t target_shard_index,
    DistributedShuffleJoinTableSide side)
{
    auto & job = getJob(target_shard_index, side);
    const auto qualified_table_name = table_names.getQualifiedTableName(side);

    if (job.executor)
    {
        if (job.qualified_table_name != qualified_table_name)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Distributed `shuffle join` block sender tried to reuse target shard {} and side {} "
                "for both {} and {}",
                target_shard_index,
                toString(side),
                job.qualified_table_name,
                qualified_table_name);
        }

        return job;
    }

    job.qualified_table_name = qualified_table_name;
    const auto & shard_info = cluster->getShardsInfo()[target_shard_index];
    if (shard_info.isLocal())
        initializeLocalJob(job, table_names, side);
    else
        initializeRemoteJob(job, table_names, target_shard_index, side);

    return job;
}

void ClusterDistributedShuffleJoinBlockSender::initializeLocalJob(
    WritingJob & job,
    const DistributedShuffleJoinTableNames & table_names,
    DistributedShuffleJoinTableSide side)
{
    auto query_context = Context::createCopy(context);
    auto insert_query = createInsertQuery(table_names, side);

    InterpreterInsertQuery interpreter(
        insert_query,
        query_context,
        /* allow_materialized_ = */ false,
        /* no_squash_ = */ false,
        /* no_destination = */ false,
        /* async_insert_ = */ false);

    auto block_io = interpreter.execute();
    job.pipeline = std::move(block_io.pipeline);
    job.executor = std::make_unique<PushingPipelineExecutor>(job.pipeline);
    job.executor->start();
}

void ClusterDistributedShuffleJoinBlockSender::initializeRemoteJob(
    WritingJob & job,
    const DistributedShuffleJoinTableNames & table_names,
    size_t target_shard_index,
    DistributedShuffleJoinTableSide side)
{
    const auto & shard_info = cluster->getShardsInfo()[target_shard_index];
    if (!shard_info.pool)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed `shuffle join` remote shard {} does not have a connection pool",
            target_shard_index);
    }

    const auto & settings = context->getSettingsRef();
    const auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    job.connection_entry = shard_info.pool->get(timeouts, settings, true);

    if (job.connection_entry.isNull())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed `shuffle join` failed to get a connection for remote shard {}",
            target_shard_index);
    }

    const auto query = createInsertQuery(table_names, side)->formatWithSecretsOneLine();
    job.pipeline = QueryPipeline(std::make_shared<RemoteSink>(
        *job.connection_entry,
        timeouts,
        query,
        settings,
        context->getClientInfo()));
    job.executor = std::make_unique<PushingPipelineExecutor>(job.pipeline);
    job.executor->start();
}

}
