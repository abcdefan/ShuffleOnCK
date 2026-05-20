#include <Storages/DistributedShuffleJoinExchangePipeline.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
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
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <sstream>
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

const DistributedShuffleJoinExchangeSource & getPayloadSource(
    const DistributedShuffleJoinExchangePayload & payload,
    DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return payload.left_source;

    return payload.right_source;
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

Poco::JSON::Object::Ptr serializeColumns(const NamesAndTypes & columns)
{
    Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
    Poco::JSON::Array::Ptr names = new Poco::JSON::Array;
    Poco::JSON::Array::Ptr types = new Poco::JSON::Array;

    for (const auto & column : columns)
    {
        if (column.name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload column has empty name");

        if (!column.type)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed `shuffle join` exchange payload column {} does not have a type",
                column.name);

        names->add(column.name);
        types->add(column.type->getName());
    }

    result->set("names", names);
    result->set("types", types);
    return result;
}

NamesAndTypes parseColumns(const Poco::JSON::Object::Ptr & object, const String & side)
{
    if (!object)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload does not have {} columns", side);

    auto names = object->getArray("names");
    auto types = object->getArray("types");
    if (!names || !types)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload does not have {} column names or types", side);

    if (names->size() != types->size())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` exchange payload has {} {} column names but {} types",
            names->size(),
            side,
            types->size());
    }

    NamesAndTypes result;
    result.reserve(names->size());

    const auto & data_type_factory = DataTypeFactory::instance();
    for (size_t i = 0; i < names->size(); ++i)
    {
        const auto index = static_cast<unsigned int>(i);
        auto name = names->getElement<String>(index);
        auto type_name = types->getElement<String>(index);

        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload {} column {} has empty name", side, i);

        if (type_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload {} column {} has empty type", side, name);

        result.emplace_back(std::move(name), data_type_factory.get(type_name));
    }

    if (result.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload has empty {} columns", side);

    return result;
}

Poco::JSON::Object::Ptr serializeSource(const DistributedShuffleJoinExchangeSource & source)
{
    if (source.database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload source database cannot be empty");

    if (source.table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload source table cannot be empty");

    Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
    result->set("database", source.database);
    result->set("table", source.table);
    return result;
}

DistributedShuffleJoinExchangeSource parseSource(const Poco::JSON::Object::Ptr & object, const String & side)
{
    if (!object)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload does not have {} source", side);

    DistributedShuffleJoinExchangeSource result
    {
        .database = object->getValue<String>("database"),
        .table = object->getValue<String>("table"),
    };

    if (result.database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload {} source database cannot be empty", side);

    if (result.table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload {} source table cannot be empty", side);

    return result;
}

void validatePayload(const DistributedShuffleJoinExchangePayload & payload)
{
    if (payload.cluster_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload cluster name cannot be empty");

    if (payload.shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload shard count cannot be zero");

    if (payload.left_key_column_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload left key column cannot be empty");

    if (payload.right_key_column_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload right key column cannot be empty");

    (void)payload.table_names.getQualifiedTableName(DistributedShuffleJoinTableSide::Left);
    (void)payload.table_names.getQualifiedTableName(DistributedShuffleJoinTableSide::Right);
    (void)serializeSource(payload.left_source);
    (void)serializeSource(payload.right_source);
    (void)serializeColumns(payload.left_required_columns);
    (void)serializeColumns(payload.right_required_columns);
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

SystemQueryDistributedShuffleJoinExchangeExecutor::SystemQueryDistributedShuffleJoinExchangeExecutor(
    IDistributedShuffleJoinQueryExecutor & query_executor_,
    DistributedShuffleJoinInfo info_)
    : query_executor(query_executor_)
    , info(std::move(info_))
{
    validateExchangeInfo(info, DistributedShuffleJoinTableSide::Left);
    validateExchangeInfo(info, DistributedShuffleJoinTableSide::Right);
}

void SystemQueryDistributedShuffleJoinExchangeExecutor::executeOnShard(
    size_t shard_index,
    const DistributedShuffleJoinTableNames & table_names)
{
    auto payload = createDistributedShuffleJoinExchangePayload(table_names, info);
    auto query = createDistributedShuffleJoinRemoteExchangeQuery(serializeDistributedShuffleJoinExchangePayload(payload));
    query_executor.executeOnShard(shard_index, query);
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

String createDistributedShuffleJoinExchangeSourceQuery(
    const DistributedShuffleJoinExchangePayload & payload,
    DistributedShuffleJoinTableSide side)
{
    const auto info = createDistributedShuffleJoinInfoFromExchangePayload(payload);
    const auto & source = getPayloadSource(payload, side);
    return createDistributedShuffleJoinExchangeSourceQuery(
        source.database,
        source.table,
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

String serializeDistributedShuffleJoinExchangePayload(const DistributedShuffleJoinExchangePayload & payload)
{
    validatePayload(payload);

    Poco::JSON::Object::Ptr tables = new Poco::JSON::Object;
    tables->set("database", payload.table_names.database);
    tables->set("left", payload.table_names.left_table);
    tables->set("right", payload.table_names.right_table);

    Poco::JSON::Object::Ptr object = new Poco::JSON::Object;
    object->set("cluster", payload.cluster_name);
    object->set("shard_count", payload.shard_count);
    object->set("tables", tables);
    object->set("left_source", serializeSource(payload.left_source));
    object->set("right_source", serializeSource(payload.right_source));
    object->set("left_key", payload.left_key_column_name);
    object->set("right_key", payload.right_key_column_name);
    object->set("left_columns", serializeColumns(payload.left_required_columns));
    object->set("right_columns", serializeColumns(payload.right_required_columns));

    std::ostringstream out;
    Poco::JSON::Stringifier::stringify(object, out);
    return out.str();
}

DistributedShuffleJoinExchangePayload parseDistributedShuffleJoinExchangePayload(const String & payload)
{
    if (payload.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload cannot be empty");

    Poco::JSON::Parser parser;
    auto object = parser.parse(payload).extract<Poco::JSON::Object::Ptr>();

    auto tables = object->getObject("tables");
    if (!tables)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload does not have shuffle table names");

    DistributedShuffleJoinExchangePayload result;
    result.cluster_name = object->getValue<String>("cluster");
    result.shard_count = object->getValue<size_t>("shard_count");
    result.table_names.database = tables->getValue<String>("database");
    result.table_names.left_table = tables->getValue<String>("left");
    result.table_names.right_table = tables->getValue<String>("right");
    result.left_source = parseSource(object->getObject("left_source"), "left");
    result.right_source = parseSource(object->getObject("right_source"), "right");
    result.left_key_column_name = object->getValue<String>("left_key");
    result.right_key_column_name = object->getValue<String>("right_key");
    result.left_required_columns = parseColumns(object->getObject("left_columns"), "left");
    result.right_required_columns = parseColumns(object->getObject("right_columns"), "right");

    validatePayload(result);
    return result;
}

DistributedShuffleJoinInfo createDistributedShuffleJoinInfoFromExchangePayload(
    const DistributedShuffleJoinExchangePayload & payload)
{
    validatePayload(payload);

    DistributedShuffleJoinInfo info;
    info.cluster_name = payload.cluster_name;
    info.shard_count = payload.shard_count;
    info.left_key_column_name = payload.left_key_column_name;
    info.right_key_column_name = payload.right_key_column_name;
    info.left_required_columns = payload.left_required_columns;
    info.right_required_columns = payload.right_required_columns;
    return info;
}

DistributedShuffleJoinExchangePayload createDistributedShuffleJoinExchangePayload(
    const DistributedShuffleJoinTableNames & table_names,
    const DistributedShuffleJoinInfo & info)
{
    const auto * left_storage = getSourceStorage(info, DistributedShuffleJoinTableSide::Left);
    const auto * right_storage = getSourceStorage(info, DistributedShuffleJoinTableSide::Right);

    DistributedShuffleJoinExchangePayload payload;
    payload.cluster_name = info.cluster_name;
    payload.shard_count = info.shard_count;
    payload.table_names = table_names;
    payload.left_source = DistributedShuffleJoinExchangeSource
    {
        .database = left_storage->getRemoteDatabaseName(),
        .table = left_storage->getRemoteTableName(),
    };
    payload.right_source = DistributedShuffleJoinExchangeSource
    {
        .database = right_storage->getRemoteDatabaseName(),
        .table = right_storage->getRemoteTableName(),
    };
    payload.left_key_column_name = info.left_key_column_name;
    payload.right_key_column_name = info.right_key_column_name;
    payload.left_required_columns = info.left_required_columns;
    payload.right_required_columns = info.right_required_columns;

    validatePayload(payload);
    return payload;
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

void executeDistributedShuffleJoinExchangePayload(
    String payload,
    ContextPtr context)
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange payload cannot be executed without context");

    auto exchange_payload = parseDistributedShuffleJoinExchangePayload(payload);
    auto cluster = context->getCluster(exchange_payload.cluster_name);
    if (!cluster)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` exchange payload refers to unknown cluster {}",
            exchange_payload.cluster_name);
    }

    if (cluster->getShardCount() != exchange_payload.shard_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` exchange payload shard count {} does not match cluster shard count {}",
            exchange_payload.shard_count,
            cluster->getShardCount());
    }

    auto info = createDistributedShuffleJoinInfoFromExchangePayload(exchange_payload);
    auto sender = std::make_shared<ClusterDistributedShuffleJoinBlockSender>(cluster, context);

    try
    {
        executeDistributedShuffleJoinExchangeQuery(
            createDistributedShuffleJoinExchangeSourceQuery(exchange_payload, DistributedShuffleJoinTableSide::Left),
            context,
            cluster,
            info,
            exchange_payload.table_names,
            DistributedShuffleJoinTableSide::Left,
            sender);
        executeDistributedShuffleJoinExchangeQuery(
            createDistributedShuffleJoinExchangeSourceQuery(exchange_payload, DistributedShuffleJoinTableSide::Right),
            context,
            cluster,
            info,
            exchange_payload.table_names,
            DistributedShuffleJoinTableSide::Right,
            sender);
    }
    catch (...)
    {
        sender->cancel(exchange_payload.table_names, "Distributed `shuffle join` exchange payload execution failed");
        throw;
    }
}

}
