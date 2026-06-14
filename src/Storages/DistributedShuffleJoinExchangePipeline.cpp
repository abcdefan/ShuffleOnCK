#include <Storages/DistributedShuffleJoinExchangePipeline.h>

#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <Databases/IDatabase.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/DistributedShuffleJoinAnalyzer.h>
#include <Storages/DistributedShuffleJoinBlockSender.h>
#include <Storages/DistributedShuffleJoinSelector.h>
#include <Storages/DistributedShuffleJoinTables.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDistributed.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>

#include <fmt/format.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <sstream>
#include <utility>
#include <vector>

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
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsUInt64 distributed_shuffle_join_table_ttl_ms;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace
{

template <typename Callback>
void executeShuffleJoinQueryForShardsInParallel(size_t shard_count, Callback && callback)
{
    if (shard_count == 0)
        return;

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

const NamesAndTypes & getRequiredColumns(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return info.left_required_columns;

    return info.right_required_columns;
}

const String & getFilterCondition(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return info.left_filter_condition;

    return info.right_filter_condition;
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

DistributedShuffleJoinExchangeSource getExchangeSource(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    const auto & database = side == DistributedShuffleJoinTableSide::Left
        ? info.left_source_database
        : info.right_source_database;
    const auto & table = side == DistributedShuffleJoinTableSide::Left
        ? info.left_source_table
        : info.right_source_table;

    if (!database.empty() && !table.empty())
        return DistributedShuffleJoinExchangeSource{.database = database, .table = table};

    const auto * storage = getSourceStorage(info, side);
    return DistributedShuffleJoinExchangeSource{
        .database = storage->getRemoteDatabaseName(),
        .table = storage->getRemoteTableName(),
    };
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

String getQualifiedTableName(String database, String table)
{
    if (database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` table database cannot be empty");

    if (table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` table name cannot be empty");

    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}

String formatLocalJoinProjectionList(const DistributedShuffleJoinInfo & info)
{
    if (info.projection_columns.empty())
        return "*";

    String result;
    for (const auto & column : info.projection_columns)
    {
        if (column.expression.empty() && column.source_column_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` projection expression cannot be empty");

        if (column.result_column_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` projection result column cannot be empty");

        if (!result.empty())
            result += ", ";

        if (!column.expression.empty())
        {
            result += column.expression;
        }
        else
        {
            result += column.is_left ? "_shuffle_left." : "_shuffle_right.";
            result += backQuoteIfNeed(column.source_column_name);
        }

        result += " AS ";
        result += backQuoteIfNeed(column.result_column_name);
    }

    return result;
}

Names getVisibleResultColumns(const DistributedShuffleJoinInfo & info)
{
    Names result;
    for (const auto & column : info.projection_columns)
    {
        if (!column.is_hidden)
            result.push_back(column.result_column_name);
    }

    return result;
}

bool hasHiddenProjectionColumns(const DistributedShuffleJoinInfo & info)
{
    for (const auto & column : info.projection_columns)
        if (column.is_hidden)
            return true;

    return false;
}

bool isSemiOrAntiJoin(JoinStrictness strictness)
{
    return strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti;
}

bool isSupportedLocalJoinCombination(JoinKind kind, JoinStrictness strictness)
{
    if (kind == JoinKind::Full)
        return strictness == JoinStrictness::All || strictness == JoinStrictness::Unspecified;

    if (isSemiOrAntiJoin(strictness))
        return kind == JoinKind::Left || kind == JoinKind::Right;

    return strictness == JoinStrictness::All
        || strictness == JoinStrictness::Any
        || strictness == JoinStrictness::RightAny
        || strictness == JoinStrictness::Unspecified;
}

String formatJoinStrictness(JoinStrictness strictness)
{
    if (strictness == JoinStrictness::All || strictness == JoinStrictness::Unspecified)
        return "ALL";

    if (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny)
        return "ANY";

    if (strictness == JoinStrictness::Semi)
        return "SEMI";

    if (strictness == JoinStrictness::Anti)
        return "ANTI";

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Distributed `shuffle join` local `JOIN` strictness {} is not supported",
        toString(strictness));
}

String formatJoinKind(JoinKind kind)
{
    if (kind == JoinKind::Inner)
        return "INNER";

    if (kind == JoinKind::Left)
        return "LEFT";

    if (kind == JoinKind::Right)
        return "RIGHT";

    if (kind == JoinKind::Full)
        return "FULL";

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Distributed `shuffle join` local `JOIN` kind {} is not supported",
        toString(kind));
}

ActionsDAG createFinalProjectionActions(const Block & header, const Names & visible_result_columns)
{
    ActionsDAG actions;
    std::vector<const ActionsDAG::Node *> inputs;
    inputs.reserve(header.columns());

    for (const auto & column : header.getColumnsWithTypeAndName())
    {
        const auto * input = &actions.addInput(column);
        inputs.push_back(input);
    }

    // Match by position after each name lookup so duplicate visible names are preserved.
    size_t header_position = 0;
    for (const auto & column_name : visible_result_columns)
    {
        while (header_position < header.columns()
            && header.getByPosition(header_position).name != column_name)
        {
            ++header_position;
        }

        if (header_position == header.columns())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed `shuffle join` final projection column {} is missing from local `JOIN` header",
                column_name);
        }

        actions.getOutputs().push_back(inputs[header_position]);
        ++header_position;
    }

    return actions;
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

String getShuffleJoinInitialQueryId(ContextPtr context)
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` execution plan cannot be created without context");

    auto query_id = context->getCurrentQueryId();
    if (query_id.empty())
        query_id = fmt::format("distributed_shuffle_join_{}", UUIDHelpers::generateV4());

    return query_id;
}

UInt64 getCurrentTimeMilliseconds()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

UInt64 getShuffleJoinExpirationTimeMilliseconds(ContextPtr context)
{
    const UInt64 ttl_ms = context->getSettingsRef()[Setting::distributed_shuffle_join_table_ttl_ms];
    if (ttl_ms == 0)
        return 0;

    const UInt64 current_time_ms = getCurrentTimeMilliseconds();
    if (ttl_ms > std::numeric_limits<UInt64>::max() - current_time_ms)
        return std::numeric_limits<UInt64>::max();

    return current_time_ms + ttl_ms;
}

void cleanupExpiredDistributedShuffleJoinTables(
    const DistributedShuffleJoinTableNames & current_table_names,
    const std::vector<String> & protected_table_names,
    ContextPtr context)
{
    auto database = DatabaseCatalog::instance().tryGetDatabase(current_table_names.database);
    if (!database)
        return;

    const auto now_ms = getCurrentTimeMilliseconds();
    const auto is_protected_table = [&](const String & table_name)
    {
        if (table_name == current_table_names.left_table || table_name == current_table_names.right_table)
            return true;

        return std::find(protected_table_names.begin(), protected_table_names.end(), table_name) != protected_table_names.end();
    };

    std::vector<String> expired_table_names;
    {
        auto tables = database->getTablesIterator(
            context,
            [](const String & table_name)
            {
                return table_name.starts_with("_shuffle_");
            });

        for (; tables->isValid(); tables->next())
        {
            const auto & table_name = tables->name();
            const auto & storage = tables->table();
            if (!storage || storage->getName() != "Memory")
                continue;

            if (is_protected_table(table_name))
                continue;

            const auto expiration_time_ms = tryGetDistributedShuffleJoinTableExpirationTimeMs(table_name);
            if (!expiration_time_ms || *expiration_time_ms > now_ms)
                continue;

            expired_table_names.push_back(table_name);
        }
    }

    for (const auto & table_name : expired_table_names)
    {
        try
        {
            InterpreterDropQuery::executeDropQuery(
                ASTDropQuery::Kind::Drop,
                context->getGlobalContext(),
                context,
                StorageID(current_table_names.database, table_name),
                /* sync= */ true,
                /* ignore_sync_setting= */ false,
                /* need_ddl_guard= */ false);
        }
        catch (...)
        {
            tryLogCurrentException(
                __PRETTY_FUNCTION__,
                fmt::format("Failed to drop expired distributed `shuffle join` table {}.{}", current_table_names.database, table_name));
        }
    }
}

ContextMutablePtr createDistributedShuffleJoinLocalJoinContext(ContextPtr context)
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join cannot be executed without context");

    auto query_context = Context::createCopy(context);
    query_context->setInternalQuery(true);

    /// This helper owns a simple generated `SELECT` over ordinary `Memory` tables. Using the AST path keeps
    /// `ClusterProxy::executeQuery` independent from analyzer query-tree plumbing until the full SELECT path is integrated.
    query_context->setSetting("allow_experimental_analyzer", Field(false));
    return query_context;
}

ASTPtr parseDistributedShuffleJoinLocalJoinQuery(
    const DistributedShuffleJoinExecutionPlan & plan,
    const ContextPtr & context)
{
    if (plan.local_join_query.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join query cannot be empty");

    ParserSelectQuery parser;
    const auto & settings = context->getSettingsRef();
    return parseQuery(
        parser,
        plan.local_join_query,
        "distributed `shuffle join` local join query",
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
}

class DistributedShuffleJoinCoordinatorResource final : public ICustomResourceHolder
{
public:
    explicit DistributedShuffleJoinCoordinatorResource(std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator_)
        : coordinator(std::move(coordinator_))
    {
        if (!coordinator)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` coordinator resource cannot be created without coordinator");
    }

    DistributedShuffleJoinCoordinatorResource(
        std::unique_ptr<IDistributedShuffleJoinQueryExecutor> query_executor_,
        std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator_)
        : query_executor(std::move(query_executor_))
        , coordinator(std::move(coordinator_))
    {
        if (!query_executor)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` coordinator resource cannot be created without query executor");

        if (!coordinator)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` coordinator resource cannot be created without coordinator");
    }

private:
    std::unique_ptr<IDistributedShuffleJoinQueryExecutor> query_executor;
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator;
};

struct DistributedShuffleJoinExecutionLifetime
{
    std::unique_ptr<IDistributedShuffleJoinQueryExecutor> query_executor;
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator;
};

void executeDistributedShuffleJoinQueryForAllShards(
    size_t shard_count,
    const String & query,
    IDistributedShuffleJoinQueryExecutor & query_executor)
{
    if (shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` query cannot run without shards");

    executeShuffleJoinQueryForShardsInParallel(shard_count, [&](size_t shard_index)
    {
        query_executor.executeOnShard(shard_index, query);
    });
}

void cleanupDistributedShuffleJoinQueryForAllShards(
    size_t shard_count,
    const String & query,
    IDistributedShuffleJoinQueryExecutor & query_executor) noexcept
{
    try
    {
        executeShuffleJoinQueryForShardsInParallel(shard_count, [&](size_t shard_index)
        {
            try
            {
                query_executor.executeCleanupOnShard(shard_index, query);
            }
            catch (...)
            {
                tryLogCurrentException(
                    __PRETTY_FUNCTION__,
                    fmt::format("Failed to execute distributed `shuffle join` cleanup query on shard {}", shard_index));
            }
        });
    }
    catch (...)
    {
        tryLogCurrentException(
            __PRETTY_FUNCTION__,
            "Failed to schedule distributed `shuffle join` cleanup queries");
    }
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

    auto query = fmt::format(
        "SELECT {} FROM {}.{}",
        formatColumnList(getRequiredColumns(info, side)),
        backQuoteIfNeed(database),
        backQuoteIfNeed(table));

    const auto & filter_condition = getFilterCondition(info, side);
    if (!filter_condition.empty())
        query += " WHERE " + filter_condition;

    return query;
}

String createDistributedShuffleJoinExchangeSourceQuery(
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    const auto source = getExchangeSource(info, side);
    return createDistributedShuffleJoinExchangeSourceQuery(
        source.database,
        source.table,
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

    if (!isSupportedLocalJoinCombination(info.join_kind, info.join_strictness))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed `shuffle join` local `JOIN` combination {} {} is not supported",
            toString(info.join_kind),
            toString(info.join_strictness));
    }

    auto query = fmt::format(
        "SELECT {} FROM {} AS _shuffle_left {} {} JOIN {} AS _shuffle_right ON _shuffle_left.{} = _shuffle_right.{}",
        formatLocalJoinProjectionList(info),
        table_names.getQualifiedTableName(DistributedShuffleJoinTableSide::Left),
        formatJoinKind(info.join_kind),
        formatJoinStrictness(info.join_strictness),
        table_names.getQualifiedTableName(DistributedShuffleJoinTableSide::Right),
        backQuoteIfNeed(info.left_key_column_name),
        backQuoteIfNeed(info.right_key_column_name));

    if (!info.post_join_filter_condition.empty())
        query += " WHERE " + info.post_join_filter_condition;

    return query;
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
    object->set("left_filter", payload.left_filter_condition);
    object->set("right_filter", payload.right_filter_condition);

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
    if (object->has("left_filter"))
        result.left_filter_condition = object->getValue<String>("left_filter");
    if (object->has("right_filter"))
        result.right_filter_condition = object->getValue<String>("right_filter");

    validatePayload(result);
    return result;
}

DistributedShuffleJoinInfo createDistributedShuffleJoinInfoFromExchangePayload(
    const DistributedShuffleJoinExchangePayload & payload)
{
    validatePayload(payload);

    DistributedShuffleJoinInfo info;
    info.cluster_name = payload.cluster_name;
    info.shuffle_database = payload.table_names.database;
    info.shard_count = payload.shard_count;
    info.left_key_column_name = payload.left_key_column_name;
    info.right_key_column_name = payload.right_key_column_name;
    info.left_required_columns = payload.left_required_columns;
    info.right_required_columns = payload.right_required_columns;
    info.left_filter_condition = payload.left_filter_condition;
    info.right_filter_condition = payload.right_filter_condition;
    return info;
}

DistributedShuffleJoinExchangePayload createDistributedShuffleJoinExchangePayload(
    const DistributedShuffleJoinTableNames & table_names,
    const DistributedShuffleJoinInfo & info)
{
    DistributedShuffleJoinExchangePayload payload;
    payload.cluster_name = info.cluster_name;
    payload.shard_count = info.shard_count;
    payload.table_names = table_names;
    payload.left_source = getExchangeSource(info, DistributedShuffleJoinTableSide::Left);
    payload.right_source = getExchangeSource(info, DistributedShuffleJoinTableSide::Right);
    payload.left_key_column_name = info.left_key_column_name;
    payload.right_key_column_name = info.right_key_column_name;
    payload.left_required_columns = info.left_required_columns;
    payload.right_required_columns = info.right_required_columns;
    payload.left_filter_condition = info.left_filter_condition;
    payload.right_filter_condition = info.right_filter_condition;

    validatePayload(payload);
    return payload;
}

DistributedShuffleJoinExecutionPlan createDistributedShuffleJoinExecutionPlan(
    String shuffle_database,
    DistributedShuffleJoinExchangeId exchange_id,
    const DistributedShuffleJoinInfo & info)
{
    if (shuffle_database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` execution plan cannot be created without shuffle database");

    validateExchangeInfo(info, DistributedShuffleJoinTableSide::Left);
    validateExchangeInfo(info, DistributedShuffleJoinTableSide::Right);

    DistributedShuffleJoinExecutionPlan plan;
    plan.table_names = createDistributedShuffleJoinTableNames(std::move(shuffle_database), std::move(exchange_id));
    plan.left_header = createDistributedShuffleJoinExchangeHeader(info, DistributedShuffleJoinTableSide::Left);
    plan.right_header = createDistributedShuffleJoinExchangeHeader(info, DistributedShuffleJoinTableSide::Right);
    plan.local_join_query = createDistributedShuffleJoinLocalJoinQuery(plan.table_names, info);
    plan.order_by = info.order_by;
    plan.visible_result_columns = getVisibleResultColumns(info);
    plan.has_hidden_projection_columns = hasHiddenProjectionColumns(info);
    plan.limit_length = info.limit_length;
    plan.limit_offset = info.limit_offset;
    plan.limit_with_ties = info.limit_with_ties;
    return plan;
}

DistributedShuffleJoinExecutionPlan createDistributedShuffleJoinExecutionPlan(
    String shuffle_database,
    ContextPtr context,
    size_t join_id,
    const DistributedShuffleJoinInfo & info)
{
    return createDistributedShuffleJoinExecutionPlan(
        std::move(shuffle_database),
        DistributedShuffleJoinExchangeId{
            .initial_query_id = getShuffleJoinInitialQueryId(context),
            .join_id = join_id,
            .expiration_time_ms = getShuffleJoinExpirationTimeMilliseconds(context)},
        info);
}

DistributedShuffleJoinExecutionPlan createDistributedShuffleJoinExecutionPlan(
    ContextPtr context,
    size_t join_id,
    const DistributedShuffleJoinInfo & info)
{
    return createDistributedShuffleJoinExecutionPlan(
        info.shuffle_database,
        std::move(context),
        join_id,
        info);
}

std::vector<DistributedShuffleJoinLeftDeepStagePlan> createDistributedShuffleJoinLeftDeepStagePlans(
    const DistributedShuffleJoinLeftDeepInfo & info,
    const String & initial_query_id,
    size_t join_id,
    UInt64 expiration_time_ms)
{
    if (info.shuffle_database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` execution plan cannot be created without shuffle database");

    if (initial_query_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` execution plan cannot be created without initial query id");

    if (info.shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` execution plan cannot be created without shards");

    if (info.stages.size() < 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` execution plan expects at least two stages");

    std::vector<DistributedShuffleJoinLeftDeepStagePlan> stage_plans;
    stage_plans.reserve(info.stages.size());

    std::optional<String> previous_output_table;
    for (size_t stage_index = 0; stage_index < info.stages.size(); ++stage_index)
    {
        auto stage_info = info.stages[stage_index].info;
        if (stage_info.shard_count != info.shard_count)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed left-deep `shuffle join` stage {} shard count {} does not match top-level shard count {}",
                stage_index,
                stage_info.shard_count,
                info.shard_count);
        }

        if (!info.cluster_name.empty() && stage_info.cluster_name != info.cluster_name)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed left-deep `shuffle join` stage {} cluster {} does not match top-level cluster {}",
                stage_index,
                stage_info.cluster_name,
                info.cluster_name);
        }

        stage_info.shuffle_database = info.shuffle_database;

        if (previous_output_table)
        {
            stage_info.left_source_database = info.shuffle_database;
            stage_info.left_source_table = *previous_output_table;
        }

        const auto stage_id = DistributedShuffleJoinExchangeId{
            .initial_query_id = fmt::format("{}_stage_{}", initial_query_id, stage_index),
            .join_id = join_id,
            .expiration_time_ms = expiration_time_ms};

        DistributedShuffleJoinLeftDeepStagePlan stage_plan;
        stage_plan.info = std::move(stage_info);
        stage_plan.execution_plan = createDistributedShuffleJoinExecutionPlan(info.shuffle_database, stage_id, stage_plan.info);

        const bool is_final_stage = stage_index + 1 == info.stages.size();
        if (!is_final_stage)
        {
            stage_plan.output_table = makeDistributedShuffleJoinTableNamePrefix(stage_id) + "_output";
            stage_plan.qualified_output_table = getQualifiedTableName(info.shuffle_database, *stage_plan.output_table);
            stage_plan.output_header = createDistributedShuffleJoinTableHeader(info.stages[stage_index].output_columns);
            previous_output_table = stage_plan.output_table;
        }

        stage_plans.push_back(std::move(stage_plan));
    }

    return stage_plans;
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
    query_context->setInternalQuery(true);

    if (!context->getCurrentQueryId().empty())
        query_context->setCurrentQueryId(fmt::format("{}:source:{}", context->getCurrentQueryId(), toString(side)));

    auto block_io = executeQuery(std::move(query), query_context, QueryFlags{.internal = true}, QueryProcessingStage::Complete).second;
    block_io.pipeline.complete(createDistributedShuffleJoinExchangeSink(
        std::move(cluster),
        info,
        std::move(table_names),
        side,
        std::move(sender)));

    if (auto parent_cancel_callback = context->getInteractiveCancelCallback())
    {
        query_context->setInteractiveCancelCallback(
            [upstream_cancel_callback = std::move(parent_cancel_callback), query_context]() mutable
            {
                try
                {
                    if (!upstream_cancel_callback())
                        return false;
                }
                catch (...)
                {
                    query_context->killCurrentQuery();
                    throw;
                }

                query_context->killCurrentQuery();
                return true;
            });
    }

    CompletedPipelineExecutor executor(block_io.pipeline);

    if (auto callback = query_context->getInteractiveCancelCallback())
    {
        executor.setCancelCallback(
            std::move(callback),
            query_context->getSettingsRef()[Setting::interactive_delay] / 1000);
    }

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

    cleanupExpiredDistributedShuffleJoinTables(
        exchange_payload.table_names,
        {exchange_payload.left_source.table, exchange_payload.right_source.table},
        context);

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

std::unique_ptr<DistributedShuffleJoinCoordinator> prepareDistributedShuffleJoinExchange(
    const DistributedShuffleJoinExecutionPlan & plan,
    size_t shard_count,
    IDistributedShuffleJoinQueryExecutor & query_executor,
    IDistributedShuffleJoinExchangeExecutor & exchange_executor)
{
    if (shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange cannot be prepared without shards");

    auto coordinator = std::make_unique<DistributedShuffleJoinCoordinator>(
        plan.table_names,
        plan.left_header,
        plan.right_header,
        shard_count,
        query_executor);

    coordinator->prepareShuffleTables();
    coordinator->exchangeShuffleTables(exchange_executor);
    return coordinator;
}

QueryPlanResourceHolder holdDistributedShuffleJoinCoordinator(
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator)
{
    QueryPlanResourceHolder holder;
    holder.custom_resources.push_back(std::make_shared<DistributedShuffleJoinCoordinatorResource>(std::move(coordinator)));
    return holder;
}

QueryPlanResourceHolder holdDistributedShuffleJoinCoordinator(
    std::unique_ptr<IDistributedShuffleJoinQueryExecutor> query_executor,
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator)
{
    QueryPlanResourceHolder holder;
    holder.custom_resources.push_back(std::make_shared<DistributedShuffleJoinCoordinatorResource>(
        std::move(query_executor),
        std::move(coordinator)));
    return holder;
}

void attachDistributedShuffleJoinCoordinator(
    QueryPipeline & pipeline,
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator)
{
    pipeline.addResources(holdDistributedShuffleJoinCoordinator(std::move(coordinator)));
}

void attachDistributedShuffleJoinCoordinator(
    QueryPipeline & pipeline,
    std::unique_ptr<IDistributedShuffleJoinQueryExecutor> query_executor,
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator)
{
    pipeline.addResources(holdDistributedShuffleJoinCoordinator(
        std::move(query_executor),
        std::move(coordinator)));
}

BlockIO executeDistributedShuffleJoinLocalJoinPipeline(
    const DistributedShuffleJoinExecutionPlan & plan,
    ContextPtr context,
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator)
{
    if (plan.local_join_query.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join pipeline cannot be created without query");

    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join pipeline cannot be created without context");

    if (!coordinator)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join pipeline cannot be created without coordinator");

    auto query_context = Context::createCopy(context);
    auto block_io = executeQuery(
        plan.local_join_query,
        query_context,
        QueryFlags{.internal = true},
        QueryProcessingStage::Complete).second;

    attachDistributedShuffleJoinCoordinator(block_io.pipeline, std::move(coordinator));
    return block_io;
}

BlockIO executeDistributedShuffleJoinClusterLocalJoinPipeline(
    const DistributedShuffleJoinExecutionPlan & plan,
    ContextPtr context,
    ClusterPtr cluster,
    std::unique_ptr<IDistributedShuffleJoinQueryExecutor> query_executor,
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator)
{
    if (!query_executor)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join pipeline cannot be created without query executor");

    if (!coordinator)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join pipeline cannot be created without coordinator");

    DistributedShuffleJoinExecutionLifetime lifetime
    {
        .query_executor = std::move(query_executor),
        .coordinator = std::move(coordinator),
    };

    auto query_context = createDistributedShuffleJoinLocalJoinContext(context);
    QueryPlan query_plan;
    buildDistributedShuffleJoinClusterLocalJoinQueryPlan(query_plan, plan, query_context, std::move(cluster));

    auto builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(query_context),
        BuildQueryPipelineSettings(query_context));

    BlockIO block_io;
    block_io.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    attachDistributedShuffleJoinCoordinator(
        block_io.pipeline,
        std::move(lifetime.query_executor),
        std::move(lifetime.coordinator));
    return block_io;
}

void buildDistributedShuffleJoinClusterLocalJoinQueryPlan(
    QueryPlan & query_plan,
    const DistributedShuffleJoinExecutionPlan & plan,
    ContextPtr context,
    ClusterPtr cluster)
{
    if (!cluster)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join cannot be executed without cluster");

    if (cluster->getShardCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join cannot be executed without target shards");

    auto query_context = createDistributedShuffleJoinLocalJoinContext(std::move(context));
    auto query_ast = parseDistributedShuffleJoinLocalJoinQuery(plan, query_context);

    const auto processed_stage = QueryProcessingStage::Complete;
    auto header = InterpreterSelectQuery(query_ast, query_context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    SelectQueryInfo query_info;
    query_info.query = query_ast;
    query_info.cluster = std::move(cluster);
    query_info.is_internal = true;
    query_info.storage_limits = std::make_shared<const StorageLimitsList>();

    ClusterProxy::SelectStreamFactory stream_factory(
        header,
        /*storage_snapshot=*/nullptr,
        processed_stage);

    const StorageID main_table(plan.table_names.database, plan.table_names.left_table);
    const DistributedSettings distributed_settings;

    ClusterProxy::executeQuery(
        query_plan,
        header,
        processed_stage,
        main_table,
        /*table_func_ptr=*/nullptr,
        stream_factory,
        getLogger("DistributedShuffleJoinLocalJoin"),
        query_context,
        query_info,
        /*sharding_key_expr=*/nullptr,
        /*sharding_key_column_name=*/"",
        distributed_settings,
        /*shard_filter_generator=*/{},
        /*is_remote_function=*/false);

    if (!query_plan.isInitialized())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join did not create a query plan");

    if (!plan.order_by.empty())
    {
        auto sorting_step = std::make_unique<SortingStep>(
            query_plan.getCurrentHeader(),
            plan.order_by,
            /* limit= */ 0,
            SortingStep::Settings(query_context->getSettingsRef()));
        sorting_step->setStepDescription("Global sorting for distributed shuffle join");
        query_plan.addStep(std::move(sorting_step));
    }

    if (plan.limit_length)
    {
        auto limit_step = std::make_unique<LimitStep>(
            query_plan.getCurrentHeader(),
            *plan.limit_length,
            plan.limit_offset,
            /* always_read_till_end= */ false,
            plan.limit_with_ties,
            plan.limit_with_ties ? plan.order_by : SortDescription{});
        if (plan.limit_with_ties)
            limit_step->setStepDescription("LIMIT WITH TIES for distributed shuffle join");

        query_plan.addStep(std::move(limit_step));
    }

    if (plan.has_hidden_projection_columns)
    {
        auto final_projection_step = std::make_unique<ExpressionStep>(
            query_plan.getCurrentHeader(),
            createFinalProjectionActions(*query_plan.getCurrentHeader(), plan.visible_result_columns));
        final_projection_step->setStepDescription("Remove hidden distributed shuffle join columns");
        query_plan.addStep(std::move(final_projection_step));
    }
}

BlockIO executeDistributedShuffleJoinClusterLocalJoinPipeline(
    const DistributedShuffleJoinExecutionPlan & plan,
    ContextPtr context,
    ClusterPtr cluster,
    std::unique_ptr<DistributedShuffleJoinCoordinator> coordinator)
{
    if (!coordinator)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local join pipeline cannot be created without coordinator");

    auto query_context = createDistributedShuffleJoinLocalJoinContext(context);
    QueryPlan query_plan;
    buildDistributedShuffleJoinClusterLocalJoinQueryPlan(query_plan, plan, query_context, std::move(cluster));

    auto builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(query_context),
        BuildQueryPipelineSettings(query_context));

    BlockIO block_io;
    block_io.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    attachDistributedShuffleJoinCoordinator(block_io.pipeline, std::move(coordinator));
    return block_io;
}

BlockIO executeDistributedShuffleJoinPipeline(
    const DistributedShuffleJoinInfo & info,
    ContextPtr context,
    size_t join_id)
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` pipeline cannot be created without context");

    if (!info.left_storage)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` pipeline cannot be created without left distributed storage");

    auto cluster = info.left_storage->getCluster();
    if (!cluster)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` pipeline cannot be created without cluster");

    auto plan = createDistributedShuffleJoinExecutionPlan(context, join_id, info);
    auto query_executor = std::make_unique<ClusterDistributedShuffleJoinQueryExecutor>(cluster, context);
    SystemQueryDistributedShuffleJoinExchangeExecutor exchange_executor(*query_executor, info);
    auto coordinator = prepareDistributedShuffleJoinExchange(
        plan,
        info.shard_count,
        *query_executor,
        exchange_executor);

    return executeDistributedShuffleJoinClusterLocalJoinPipeline(
        plan,
        std::move(context),
        std::move(cluster),
        std::move(query_executor),
        std::move(coordinator));
}

BlockIO executeDistributedShuffleJoinLeftDeepPipeline(
    const DistributedShuffleJoinLeftDeepInfo & info,
    ContextPtr context,
    size_t join_id)
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` pipeline cannot be created without context");

    if (info.stages.size() < 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` pipeline expects at least two stages");

    const auto & first_stage = info.stages[0];
    if (!first_stage.info.left_storage)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` pipeline cannot be created without first input storage");

    auto cluster = first_stage.info.left_storage->getCluster();
    if (!cluster)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` pipeline cannot be created without cluster");

    if (info.cluster_name.empty() || cluster->getName() != info.cluster_name)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed left-deep `shuffle join` pipeline cluster {} does not match metadata cluster {}",
            cluster->getName(),
            info.cluster_name);
    }

    if (cluster->getShardCount() != info.shard_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Distributed left-deep `shuffle join` pipeline shard count {} does not match cluster shard count {}",
            info.shard_count,
            cluster->getShardCount());
    }

    const auto initial_query_id = getShuffleJoinInitialQueryId(context);
    const auto expiration_time_ms = getShuffleJoinExpirationTimeMilliseconds(context);

    auto query_executor = std::make_unique<ClusterDistributedShuffleJoinQueryExecutor>(cluster, context);
    std::optional<String> previous_qualified_output_table;
    std::vector<String> created_output_tables;
    const auto stage_plans = createDistributedShuffleJoinLeftDeepStagePlans(
        info,
        initial_query_id,
        join_id,
        expiration_time_ms);

    const auto cleanup_output_table = [&](const String & qualified_table_name) noexcept
    {
        cleanupDistributedShuffleJoinQueryForAllShards(
            info.shard_count,
            dropDistributedShuffleJoinTableQuery(qualified_table_name),
            *query_executor);
    };

    try
    {
        for (size_t stage_index = 0; stage_index < stage_plans.size(); ++stage_index)
        {
            const auto & stage_plan = stage_plans[stage_index];
            const auto & plan = stage_plan.execution_plan;

            SystemQueryDistributedShuffleJoinExchangeExecutor exchange_executor(*query_executor, stage_plan.info);
            auto coordinator = prepareDistributedShuffleJoinExchange(
                plan,
                info.shard_count,
                *query_executor,
                exchange_executor);

            if (previous_qualified_output_table)
            {
                cleanup_output_table(*previous_qualified_output_table);
                std::erase(created_output_tables, *previous_qualified_output_table);
                previous_qualified_output_table.reset();
            }

            const bool is_final_stage = stage_index + 1 == info.stages.size();
            if (is_final_stage)
            {
                return executeDistributedShuffleJoinClusterLocalJoinPipeline(
                    plan,
                    std::move(context),
                    std::move(cluster),
                    std::move(query_executor),
                    std::move(coordinator));
            }

            if (!stage_plan.qualified_output_table)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` non-final stage does not have output table");

            created_output_tables.push_back(*stage_plan.qualified_output_table);
            executeDistributedShuffleJoinQueryForAllShards(
                info.shard_count,
                createDistributedShuffleJoinMemoryTableQuery(
                    *stage_plan.qualified_output_table,
                    stage_plan.output_header),
                *query_executor);

            coordinator->joinShuffleTables(
                fmt::format("INSERT INTO {} {}", *stage_plan.qualified_output_table, plan.local_join_query),
                *query_executor);
            coordinator->cleanupShuffleTables();

            previous_qualified_output_table = stage_plan.qualified_output_table;
        }
    }
    catch (...)
    {
        for (const auto & qualified_output_table : created_output_tables)
            cleanup_output_table(qualified_output_table);
        throw;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed left-deep `shuffle join` pipeline did not create a final stage");
}

void executeDistributedShuffleJoinLocalJoinAndCleanup(
    DistributedShuffleJoinCoordinator & coordinator,
    const DistributedShuffleJoinExecutionPlan & plan,
    IDistributedShuffleJoinLocalJoinExecutor & local_join_executor)
{
    coordinator.joinShuffleTables(plan.local_join_query, local_join_executor);
    coordinator.cleanupShuffleTables();
}

void executeDistributedShuffleJoinStages(
    const DistributedShuffleJoinExecutionPlan & plan,
    size_t shard_count,
    IDistributedShuffleJoinQueryExecutor & query_executor,
    IDistributedShuffleJoinExchangeExecutor & exchange_executor,
    IDistributedShuffleJoinLocalJoinExecutor & local_join_executor)
{
    if (shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` execution stages cannot run without shards");

    auto coordinator = prepareDistributedShuffleJoinExchange(
        plan,
        shard_count,
        query_executor,
        exchange_executor);
    executeDistributedShuffleJoinLocalJoinAndCleanup(
        *coordinator,
        plan,
        local_join_executor);
}

}
