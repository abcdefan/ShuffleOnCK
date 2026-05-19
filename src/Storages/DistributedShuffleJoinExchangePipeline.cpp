#include <Storages/DistributedShuffleJoinExchangePipeline.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Storages/DistributedShuffleJoinAnalyzer.h>
#include <Storages/DistributedShuffleJoinBlockSender.h>
#include <Storages/DistributedShuffleJoinSelector.h>
#include <Storages/DistributedShuffleJoinTables.h>
#include <Storages/StorageDistributed.h>
#include <Core/QueryProcessingStage.h>

#include <fmt/format.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

const NamesAndTypes & getRequiredColumns(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return info.left_required_columns;

    return info.right_required_columns;
}

const StorageDistributed * getSourceStorage(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    const auto * storage = side == DistributedShuffleJoinTableSide::Left
        ? info.left_storage
        : info.right_storage;

    if (!storage)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` exchange source cannot be created without {} distributed storage",
            toString(side));
    }

    return storage;
}

String formatColumnList(const NamesAndTypes & columns)
{
    String result;
    for (const auto & column : columns)
    {
        if (column.name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange source column has empty name");

        if (!column.type)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed `shuffle join` exchange source column {} does not have a type",
                column.name);

        if (!result.empty())
            result += ", ";

        result += backQuoteIfNeed(column.name);
    }

    return result;
}

void validateExchangeInfo(const DistributedShuffleJoinInfo & info, DistributedShuffleJoinTableSide side)
{
    if (info.shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange sink cannot be created without target shards");

    const auto & key_column_name = side == DistributedShuffleJoinTableSide::Left
        ? info.left_key_column_name
        : info.right_key_column_name;

    if (key_column_name.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` exchange sink cannot be created without {} key column",
            toString(side));

    if (getRequiredColumns(info, side).empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` exchange sink cannot be created without {} required columns",
            toString(side));
}

}

LocalDistributedShuffleJoinExchangeSideExecutor::LocalDistributedShuffleJoinExchangeSideExecutor(
    ContextPtr context_,
    ClusterPtr cluster_,
    DistributedShuffleJoinInfo info_,
    DistributedShuffleJoinBlockSenderPtr sender_)
    : context(std::move(context_))
    , cluster(std::move(cluster_))
    , info(std::move(info_))
    , sender(std::move(sender_))
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local exchange side executor cannot be created without context");

    if (!cluster)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local exchange side executor cannot be created without cluster");

    if (!sender)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local exchange side executor cannot be created without sender");
}

void LocalDistributedShuffleJoinExchangeSideExecutor::executeSide(
    DistributedShuffleJoinTableSide side,
    const DistributedShuffleJoinTableNames & table_names)
{
    executeDistributedShuffleJoinExchangeSide(
        context,
        cluster,
        info,
        table_names,
        side,
        sender);
}

CurrentShardDistributedShuffleJoinExchangeExecutor::CurrentShardDistributedShuffleJoinExchangeExecutor(
    size_t current_shard_index_,
    IDistributedShuffleJoinExchangeSideExecutor & side_executor_,
    DistributedShuffleJoinBlockSenderPtr sender_)
    : current_shard_index(current_shard_index_)
    , sender(std::move(sender_))
    , side_executor(&side_executor_)
{
    if (!sender)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange executor cannot be created without sender");
}

CurrentShardDistributedShuffleJoinExchangeExecutor::CurrentShardDistributedShuffleJoinExchangeExecutor(
    size_t current_shard_index_,
    ContextPtr context_,
    ClusterPtr cluster_,
    DistributedShuffleJoinInfo info_,
    DistributedShuffleJoinBlockSenderPtr sender_)
    : current_shard_index(current_shard_index_)
    , sender(std::move(sender_))
{
    if (!sender)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange executor cannot be created without sender");

    owned_side_executor = std::make_unique<LocalDistributedShuffleJoinExchangeSideExecutor>(
        std::move(context_),
        std::move(cluster_),
        std::move(info_),
        sender);
    side_executor = owned_side_executor.get();
}

void CurrentShardDistributedShuffleJoinExchangeExecutor::executeOnShard(
    size_t shard_index,
    const DistributedShuffleJoinTableNames & table_names)
{
    if (shard_index != current_shard_index)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Distributed `shuffle join` cannot execute source exchange for remote shard {} from current shard {} yet",
            shard_index,
            current_shard_index);
    }

    executeDistributedShuffleJoinExchangeSource(table_names, *side_executor, sender);
}

std::unique_ptr<CurrentShardDistributedShuffleJoinExchangeExecutor> createCurrentShardDistributedShuffleJoinExchangeExecutor(
    size_t current_shard_index,
    ContextPtr context,
    ClusterPtr cluster,
    DistributedShuffleJoinInfo info)
{
    auto sender = std::make_shared<ClusterDistributedShuffleJoinBlockSender>(cluster, context);
    return std::make_unique<CurrentShardDistributedShuffleJoinExchangeExecutor>(
        current_shard_index,
        std::move(context),
        std::move(cluster),
        std::move(info),
        std::move(sender));
}

Block createDistributedShuffleJoinExchangeHeader(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    validateExchangeInfo(info, side);
    return createDistributedShuffleJoinTableHeader(getRequiredColumns(info, side));
}

String createDistributedShuffleJoinExchangeSourceQuery(
    String database,
    String table,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    validateExchangeInfo(info, side);

    if (database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange source database cannot be empty");

    if (table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange source table cannot be empty");

    return fmt::format(
        "SELECT {} FROM {}.{}",
        formatColumnList(getRequiredColumns(info, side)),
        backQuoteIfNeed(database),
        backQuoteIfNeed(table));
}

String createDistributedShuffleJoinExchangeSourceQuery(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    const auto * storage = getSourceStorage(info, side);
    return createDistributedShuffleJoinExchangeSourceQuery(
        storage->getRemoteDatabaseName(),
        storage->getRemoteTableName(),
        info,
        side);
}

String createDistributedShuffleJoinLocalJoinQuery(
    const DistributedShuffleJoinTableNames & table_names,
    const DistributedShuffleJoinInfo & info)
{
    validateExchangeInfo(info, DistributedShuffleJoinTableSide::Left);
    validateExchangeInfo(info, DistributedShuffleJoinTableSide::Right);

    return fmt::format(
        "SELECT * FROM {} AS _shuffle_left INNER ALL JOIN {} AS _shuffle_right ON _shuffle_left.{} = _shuffle_right.{}",
        table_names.getQualifiedTableName(DistributedShuffleJoinTableSide::Left),
        table_names.getQualifiedTableName(DistributedShuffleJoinTableSide::Right),
        backQuoteIfNeed(info.left_key_column_name),
        backQuoteIfNeed(info.right_key_column_name));
}

String createDistributedShuffleJoinRemoteExchangeQuery(String payload)
{
    if (payload.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange request payload cannot be empty");

    return fmt::format("SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE {}", quoteString(payload));
}

std::shared_ptr<DistributedShuffleJoinSink> createDistributedShuffleJoinExchangeSink(
    ClusterPtr cluster,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableNames table_names,
    DistributedShuffleJoinTableSide side,
    DistributedShuffleJoinBlockSenderPtr sender)
{
    if (!cluster)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange sink cannot be created without cluster");

    if (cluster->getShardCount() != info.shard_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` exchange sink shard count {} does not match cluster shard count {}",
            info.shard_count,
            cluster->getShardCount());
    }

    auto header = std::make_shared<const Block>(createDistributedShuffleJoinExchangeHeader(info, side));
    auto selector = createDistributedShuffleJoinSelector(std::move(cluster), info, side);

    return std::make_shared<DistributedShuffleJoinSink>(
        std::move(header),
        std::move(table_names),
        info.shard_count,
        side,
        std::move(selector),
        std::move(sender));
}

void executeDistributedShuffleJoinExchangeQuery(
    String query,
    ContextPtr context,
    ClusterPtr cluster,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableNames table_names,
    DistributedShuffleJoinTableSide side,
    DistributedShuffleJoinBlockSenderPtr sender)
{
    if (query.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange query cannot be empty");

    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange query cannot be executed without context");

    auto query_context = Context::createCopy(context);
    auto block_io = executeQuery(std::move(query), query_context, QueryFlags{.internal = true}, QueryProcessingStage::Complete).second;
    block_io.pipeline.complete(createDistributedShuffleJoinExchangeSink(
        std::move(cluster),
        info,
        std::move(table_names),
        side,
        std::move(sender)));

    CompletedPipelineExecutor executor(block_io.pipeline);
    executor.execute();
}

void executeDistributedShuffleJoinExchangeSide(
    ContextPtr context,
    ClusterPtr cluster,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableNames table_names,
    DistributedShuffleJoinTableSide side,
    DistributedShuffleJoinBlockSenderPtr sender)
{
    executeDistributedShuffleJoinExchangeQuery(
        createDistributedShuffleJoinExchangeSourceQuery(info, side),
        std::move(context),
        std::move(cluster),
        info,
        std::move(table_names),
        side,
        std::move(sender));
}

void executeDistributedShuffleJoinExchangeSource(
    const DistributedShuffleJoinTableNames & table_names,
    IDistributedShuffleJoinExchangeSideExecutor & side_executor,
    DistributedShuffleJoinBlockSenderPtr sender)
{
    if (!sender)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange source cannot be executed without sender");

    try
    {
        side_executor.executeSide(DistributedShuffleJoinTableSide::Left, table_names);
        side_executor.executeSide(DistributedShuffleJoinTableSide::Right, table_names);
    }
    catch (...)
    {
        sender->cancel(table_names, "Distributed `shuffle join` source exchange failed");
        throw;
    }
}

}
