#include <Storages/DistributedShuffleJoinAnalyzer.h>
#include <Storages/DistributedShuffleJoinExchangePipeline.h>
#include <Storages/DistributedShuffleJoinSelector.h>
#include <Storages/DistributedShuffleJoinSink.h>
#include <Storages/DistributedShuffleJoinTables.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Interpreters/DistributedShuffleJoinCoordinator.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <mutex>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

Block makeBlock(const std::vector<UInt64> & ids)
{
    auto column = ColumnUInt64::create();
    auto & data = column->getData();
    data.assign(ids.begin(), ids.end());

    return Block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeUInt64>(), "id")};
}

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(makeBlock({}).cloneEmpty());
}

Chunk makeChunk(const std::vector<UInt64> & ids)
{
    auto block = makeBlock(ids);
    return Chunk(block.getColumns(), block.rows());
}

ClusterPtr makeCluster(
    const String & cluster_name,
    const std::vector<std::vector<String>> & shard_names)
{
    static const String username = "default";
    static const String password;
    static const String bind_host;
    static const String cluster_secret;

    Settings settings;
    ClusterConnectionParameters params
    {
        .username = username,
        .password = password,
        .clickhouse_port = 9000,
        .treat_local_as_remote = true,
        .treat_local_port_as_remote = true,
        .bind_host = bind_host,
        .cluster_name = cluster_name,
        .cluster_secret = cluster_secret,
    };

    return std::make_shared<Cluster>(
        settings,
        shard_names,
        params);
}

ClusterPtr makeTwoShardCluster()
{
    return makeCluster(
        "distributed_shuffle_join_test",
        {
            {"127.0.0.1:9000"},
            {"127.0.0.2:9000"},
        });
}

ClusterPtr makeConfiguredCluster(
    const String & cluster_name,
    const std::vector<std::vector<String>> & shard_names)
{
    Settings settings;
    Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration;

    const String cluster_prefix = "remote_servers." + cluster_name;
    for (size_t shard_index = 0; shard_index < shard_names.size(); ++shard_index)
    {
        const String shard_prefix = cluster_prefix + ".shard" + std::to_string(shard_index + 1);
        for (size_t replica_index = 0; replica_index < shard_names[shard_index].size(); ++replica_index)
        {
            const auto [host, port] = Cluster::Address::fromString(shard_names[shard_index][replica_index]);
            const String replica_prefix = shard_prefix + ".replica" + std::to_string(replica_index + 1);
            config->setString(replica_prefix + ".host", host);
            config->setString(replica_prefix + ".port", std::to_string(port));
        }
    }

    return std::make_shared<Cluster>(*config, settings, "remote_servers", cluster_name);
}

std::vector<UInt64> collectIds(const std::vector<Block> & blocks)
{
    std::vector<UInt64> ids;

    for (const auto & block : blocks)
    {
        const auto & id_column = typeid_cast<const ColumnUInt64 &>(*block.getByName("id").column);
        const auto & data = id_column.getData();
        ids.insert(ids.end(), data.begin(), data.end());
    }

    std::sort(ids.begin(), ids.end());
    return ids;
}

class CapturingShuffleTableBlockSender final : public DistributedShuffleJoinBlockSender
{
public:
    explicit CapturingShuffleTableBlockSender(size_t target_shard_count_)
        : target_shards(target_shard_count_)
    {
    }

    void sendBlock(
        const DistributedShuffleJoinTableNames & table_names,
        size_t target_shard_index,
        DistributedShuffleJoinTableSide side,
        Block block) override
    {
        auto & target = getTarget(target_shard_index);
        target.table_names = table_names;
        target.blocks[sideIndex(side)].push_back(std::move(block));
    }

    void finish(
        const DistributedShuffleJoinTableNames & table_names,
        size_t target_shard_index,
        DistributedShuffleJoinTableSide side) override
    {
        auto & target = getTarget(target_shard_index);
        target.table_names = table_names;
        ++target.finish_counts[sideIndex(side)];
    }

    void cancel(const DistributedShuffleJoinTableNames &, String) noexcept override
    {
        cancelled = true;
    }

    const std::vector<Block> & getBlocks(size_t target_shard_index, DistributedShuffleJoinTableSide side) const
    {
        return getTarget(target_shard_index).blocks[sideIndex(side)];
    }

    size_t getFinishCount(size_t target_shard_index, DistributedShuffleJoinTableSide side) const
    {
        return getTarget(target_shard_index).finish_counts[sideIndex(side)];
    }

    bool wasCancelled() const
    {
        return cancelled;
    }

private:
    struct TargetShardData
    {
        DistributedShuffleJoinTableNames table_names;
        std::array<std::vector<Block>, 2> blocks;
        std::array<size_t, 2> finish_counts = {};
    };

    static size_t sideIndex(DistributedShuffleJoinTableSide side)
    {
        return side == DistributedShuffleJoinTableSide::Left ? 0 : 1;
    }

    TargetShardData & getTarget(size_t target_shard_index)
    {
        if (target_shard_index >= target_shards.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target shard index {} is out of range", target_shard_index);

        return target_shards[target_shard_index];
    }

    const TargetShardData & getTarget(size_t target_shard_index) const
    {
        if (target_shard_index >= target_shards.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target shard index {} is out of range", target_shard_index);

        return target_shards[target_shard_index];
    }

    std::vector<TargetShardData> target_shards;
    bool cancelled = false;
};

class RecordingShuffleQueryExecutor final : public IDistributedShuffleJoinQueryExecutor
{
public:
    struct ExecutedQuery
    {
        size_t shard_index;
        String query;
    };

    void executeOnShard(size_t shard_index, const String & query) override
    {
        std::lock_guard lock(mutex);

        if (fail_at_query_index && queries.size() == *fail_at_query_index)
        {
            fail_at_query_index.reset();
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Injected distributed `shuffle join` query executor failure");
        }

        queries.push_back(ExecutedQuery{.shard_index = shard_index, .query = query});
    }

    void failAtQueryIndex(size_t query_index)
    {
        std::lock_guard lock(mutex);
        fail_at_query_index = query_index;
    }

    const std::vector<ExecutedQuery> & getQueries() const
    {
        return queries;
    }

private:
    std::mutex mutex;
    std::vector<ExecutedQuery> queries;
    std::optional<size_t> fail_at_query_index;
};

class RecordingShuffleExchangeExecutor final : public IDistributedShuffleJoinExchangeExecutor
{
public:
    struct ExecutedExchange
    {
        size_t shard_index;
        DistributedShuffleJoinTableNames table_names;
    };

    void executeOnShard(size_t shard_index, const DistributedShuffleJoinTableNames & table_names) override
    {
        std::lock_guard lock(mutex);

        if (fail_at_exchange_index && exchanges.size() == *fail_at_exchange_index)
        {
            fail_at_exchange_index.reset();
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Injected distributed `shuffle join` exchange failure");
        }

        exchanges.push_back(ExecutedExchange{.shard_index = shard_index, .table_names = table_names});
    }

    void failAtExchangeIndex(size_t exchange_index)
    {
        std::lock_guard lock(mutex);
        fail_at_exchange_index = exchange_index;
    }

    const std::vector<ExecutedExchange> & getExchanges() const
    {
        return exchanges;
    }

private:
    std::mutex mutex;
    std::vector<ExecutedExchange> exchanges;
    std::optional<size_t> fail_at_exchange_index;
};

class RecordingShuffleLocalJoinExecutor final : public IDistributedShuffleJoinLocalJoinExecutor
{
public:
    struct ExecutedLocalJoin
    {
        size_t shard_index;
        String query;
    };

    void executeOnShard(size_t shard_index, const String & query) override
    {
        std::lock_guard lock(mutex);

        if (fail_at_join_index && joins.size() == *fail_at_join_index)
        {
            fail_at_join_index.reset();
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Injected distributed `shuffle join` local join failure");
        }

        joins.push_back(ExecutedLocalJoin{.shard_index = shard_index, .query = query});
    }

    void failAtJoinIndex(size_t join_index)
    {
        std::lock_guard lock(mutex);
        fail_at_join_index = join_index;
    }

    const std::vector<ExecutedLocalJoin> & getJoins() const
    {
        return joins;
    }

private:
    std::mutex mutex;
    std::vector<ExecutedLocalJoin> joins;
    std::optional<size_t> fail_at_join_index;
};

class RecordingShuffleSideExecutor final : public IDistributedShuffleJoinExchangeSideExecutor
{
public:
    void executeSide(DistributedShuffleJoinTableSide side, const DistributedShuffleJoinTableNames &) override
    {
        if (fail_on_side && side == *fail_on_side)
        {
            fail_on_side.reset();
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Injected distributed `shuffle join` side exchange failure");
        }

        sides.push_back(side);
    }

    void failOnSide(DistributedShuffleJoinTableSide side)
    {
        fail_on_side = side;
    }

    const std::vector<DistributedShuffleJoinTableSide> & getSides() const
    {
        return sides;
    }

private:
    std::vector<DistributedShuffleJoinTableSide> sides;
    std::optional<DistributedShuffleJoinTableSide> fail_on_side;
};

void consumeOneBlock(DistributedShuffleJoinSink & sink, const std::vector<UInt64> & ids)
{
    auto chunk = makeChunk(ids);
    sink.consume(chunk);
    sink.onFinish();
}

DistributedShuffleJoinInfo makeExchangeInfo()
{
    DistributedShuffleJoinInfo info;
    info.left_key_column_name = "id";
    info.right_key_column_name = "id";
    info.left_required_columns = {{"id", std::make_shared<DataTypeUInt64>()}};
    info.right_required_columns = {{"id", std::make_shared<DataTypeUInt64>()}};
    info.shuffle_database = "default";
    info.shard_count = 2;
    return info;
}

}

TEST(DistributedShuffleJoin, SinkPartitionsBlocksForShuffleTables)
{
    auto sender = std::make_shared<CapturingShuffleTableBlockSender>(2);
    auto selector = createDistributedShuffleJoinSelector(makeTwoShardCluster(), "id");

    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "local-shuffle-test", .join_id = 0};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);
    constexpr size_t target_shard_count = 2;

    auto make_sink = [&](DistributedShuffleJoinTableSide side)
    {
        return DistributedShuffleJoinSink(
            makeHeader(),
            table_names,
            target_shard_count,
            side,
            selector,
            sender);
    };

    auto left_source0 = make_sink(DistributedShuffleJoinTableSide::Left);
    auto left_source1 = make_sink(DistributedShuffleJoinTableSide::Left);
    auto right_source0 = make_sink(DistributedShuffleJoinTableSide::Right);
    auto right_source1 = make_sink(DistributedShuffleJoinTableSide::Right);

    consumeOneBlock(left_source0, {1, 2});
    consumeOneBlock(left_source1, {3, 4});
    consumeOneBlock(right_source0, {2, 3});
    consumeOneBlock(right_source1, {1, 4});

    EXPECT_EQ(collectIds(sender->getBlocks(0, DistributedShuffleJoinTableSide::Left)), std::vector<UInt64>({2, 4}));
    EXPECT_EQ(collectIds(sender->getBlocks(0, DistributedShuffleJoinTableSide::Right)), std::vector<UInt64>({2, 4}));
    EXPECT_EQ(collectIds(sender->getBlocks(1, DistributedShuffleJoinTableSide::Left)), std::vector<UInt64>({1, 3}));
    EXPECT_EQ(collectIds(sender->getBlocks(1, DistributedShuffleJoinTableSide::Right)), std::vector<UInt64>({1, 3}));

    EXPECT_EQ(sender->getFinishCount(0, DistributedShuffleJoinTableSide::Left), 2);
    EXPECT_EQ(sender->getFinishCount(1, DistributedShuffleJoinTableSide::Left), 2);
    EXPECT_EQ(sender->getFinishCount(0, DistributedShuffleJoinTableSide::Right), 2);
    EXPECT_EQ(sender->getFinishCount(1, DistributedShuffleJoinTableSide::Right), 2);
    EXPECT_FALSE(sender->wasCancelled());
}

TEST(DistributedShuffleJoin, ClusterLayoutEligibilityMatchesMvpScope)
{
    auto supported_left = makeConfiguredCluster(
        "distributed_shuffle_join_test",
        {
            {"127.0.0.1:9000"},
            {"127.0.0.2:9000"},
        });
    auto supported_right = makeConfiguredCluster(
        "distributed_shuffle_join_test",
        {
            {"127.0.0.1:9000"},
            {"127.0.0.2:9000"},
        });
    EXPECT_TRUE(isDistributedShuffleJoinClusterLayoutSupported(*supported_left, *supported_right));

    auto different_cluster = makeConfiguredCluster(
        "another_distributed_shuffle_join_test",
        {
            {"127.0.0.1:9000"},
            {"127.0.0.2:9000"},
        });
    EXPECT_FALSE(isDistributedShuffleJoinClusterLayoutSupported(*supported_left, *different_cluster));

    auto anonymous_cluster = makeCluster(
        "",
        {
            {"127.0.0.1:9000"},
            {"127.0.0.2:9000"},
        });
    EXPECT_FALSE(isDistributedShuffleJoinClusterLayoutSupported(*anonymous_cluster, *anonymous_cluster));

    auto one_shard_cluster = makeConfiguredCluster(
        "distributed_shuffle_join_test",
        {
            {"127.0.0.1:9000"},
        });
    EXPECT_FALSE(isDistributedShuffleJoinClusterLayoutSupported(*supported_left, *one_shard_cluster));

    auto multi_replica_cluster = makeConfiguredCluster(
        "distributed_shuffle_join_test",
        {
            {"127.0.0.1:9000", "127.0.0.3:9000"},
            {"127.0.0.2:9000"},
        });
    EXPECT_FALSE(isDistributedShuffleJoinClusterLayoutSupported(*multi_replica_cluster, *supported_right));
}

TEST(DistributedShuffleJoin, ExchangeSinkUsesAnalyzerInfo)
{
    const auto info = makeExchangeInfo();
    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "exchange-pipeline-test", .join_id = 0});

    auto sender = std::make_shared<CapturingShuffleTableBlockSender>(info.shard_count);
    auto left_sink = createDistributedShuffleJoinExchangeSink(
        makeTwoShardCluster(),
        info,
        table_names,
        DistributedShuffleJoinTableSide::Left,
        sender);

    EXPECT_EQ(left_sink->getHeader().getByPosition(0).name, "id");

    consumeOneBlock(*left_sink, {1, 2, 3, 4});

    EXPECT_EQ(collectIds(sender->getBlocks(0, DistributedShuffleJoinTableSide::Left)), std::vector<UInt64>({2, 4}));
    EXPECT_EQ(collectIds(sender->getBlocks(1, DistributedShuffleJoinTableSide::Left)), std::vector<UInt64>({1, 3}));
    EXPECT_EQ(sender->getFinishCount(0, DistributedShuffleJoinTableSide::Left), 1);
    EXPECT_EQ(sender->getFinishCount(1, DistributedShuffleJoinTableSide::Left), 1);
}

TEST(DistributedShuffleJoin, ExchangeSinkChecksShardCount)
{
    auto info = makeExchangeInfo();
    info.shard_count = 1;

    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "exchange-pipeline-shards-test", .join_id = 0});

    auto sender = std::make_shared<CapturingShuffleTableBlockSender>(1);

    EXPECT_THROW(
        createDistributedShuffleJoinExchangeSink(
            makeTwoShardCluster(),
            info,
            table_names,
            DistributedShuffleJoinTableSide::Left,
            sender),
        Exception);
}

TEST(DistributedShuffleJoin, CreatesExchangeSourceQuery)
{
    auto info = makeExchangeInfo();
    info.left_required_columns = {
        {"id", std::make_shared<DataTypeUInt64>()},
        {"value", std::make_shared<DataTypeUInt64>()},
    };

    EXPECT_EQ(
        createDistributedShuffleJoinExchangeSourceQuery(
            "default",
            "left local",
            info,
            DistributedShuffleJoinTableSide::Left),
        "SELECT id, value FROM default.`left local`");
}

TEST(DistributedShuffleJoin, CreatesExchangeSourceQueryWithSideFilter)
{
    auto info = makeExchangeInfo();
    info.left_required_columns = {
        {"id", std::make_shared<DataTypeUInt64>()},
        {"value", std::make_shared<DataTypeUInt64>()},
    };
    info.left_filter_condition = "(id >= 10) AND (value < 20)";
    info.right_filter_condition = "id != 0";

    EXPECT_EQ(
        createDistributedShuffleJoinExchangeSourceQuery(
            "default",
            "left local",
            info,
            DistributedShuffleJoinTableSide::Left),
        "SELECT id, value FROM default.`left local` WHERE (id >= 10) AND (value < 20)");

    EXPECT_EQ(
        createDistributedShuffleJoinExchangeSourceQuery(
            "default",
            "right local",
            info,
            DistributedShuffleJoinTableSide::Right),
        "SELECT id FROM default.`right local` WHERE id != 0");
}

TEST(DistributedShuffleJoin, ExchangeSourceQueryNeedsDistributedStorage)
{
    const auto info = makeExchangeInfo();

    EXPECT_THROW(
        createDistributedShuffleJoinExchangeSourceQuery(
            info,
        DistributedShuffleJoinTableSide::Left),
        Exception);
}

TEST(DistributedShuffleJoin, CreatesLocalJoinQuery)
{
    auto info = makeExchangeInfo();
    info.left_key_column_name = "left key";
    info.right_key_column_name = "right key";

    const auto table_names = createDistributedShuffleJoinTableNames(
        "shuffle db",
        DistributedShuffleJoinExchangeId{.initial_query_id = "local-join-query-test", .join_id = 0});

    EXPECT_EQ(
        createDistributedShuffleJoinLocalJoinQuery(table_names, info),
        "SELECT * FROM `shuffle db`._shuffle_local_join_query_test_0_left AS _shuffle_left "
        "INNER ALL JOIN `shuffle db`._shuffle_local_join_query_test_0_right AS _shuffle_right "
        "ON _shuffle_left.`left key` = _shuffle_right.`right key`");
}

TEST(DistributedShuffleJoin, CreatesLocalJoinQueryWithProjection)
{
    auto info = makeExchangeInfo();
    info.left_required_columns = {
        {"id", std::make_shared<DataTypeUInt64>()},
        {"left value", std::make_shared<DataTypeUInt64>()},
    };
    info.right_required_columns = {
        {"id", std::make_shared<DataTypeUInt64>()},
        {"right value", std::make_shared<DataTypeUInt64>()},
    };
    info.projection_columns = {
        DistributedShuffleJoinProjectionColumn{true, "id", "", "id"},
        DistributedShuffleJoinProjectionColumn{true, "left value", "", "value"},
        DistributedShuffleJoinProjectionColumn{false, "right value", "", "right value"},
    };

    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "local-join-projection-test", .join_id = 20});

    EXPECT_EQ(
        createDistributedShuffleJoinLocalJoinQuery(table_names, info),
        "SELECT _shuffle_left.id AS id, _shuffle_left.`left value` AS value, _shuffle_right.`right value` AS `right value` "
        "FROM default._shuffle_local_join_projection_test_20_left AS _shuffle_left "
        "INNER ALL JOIN default._shuffle_local_join_projection_test_20_right AS _shuffle_right "
        "ON _shuffle_left.id = _shuffle_right.id");
}

TEST(DistributedShuffleJoin, CreatesLocalJoinQueryWithExpressionProjection)
{
    auto info = makeExchangeInfo();
    info.projection_columns = {
        DistributedShuffleJoinProjectionColumn{false, "", "_shuffle_left.id", "id"},
        DistributedShuffleJoinProjectionColumn{false, "", "plus(_shuffle_left.id, _shuffle_right.id)", "sum id"},
    };

    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "local-join-expression-projection-test", .join_id = 0});

    EXPECT_EQ(
        createDistributedShuffleJoinLocalJoinQuery(table_names, info),
        "SELECT _shuffle_left.id AS id, plus(_shuffle_left.id, _shuffle_right.id) AS `sum id` "
        "FROM default._shuffle_local_join_expression_projection_test_0_left AS _shuffle_left "
        "INNER ALL JOIN default._shuffle_local_join_expression_projection_test_0_right AS _shuffle_right "
        "ON _shuffle_left.id = _shuffle_right.id");
}

TEST(DistributedShuffleJoin, CreatesLocalJoinQueryWithPostJoinFilter)
{
    auto info = makeExchangeInfo();
    info.post_join_filter_condition = "greaterOrEquals(plus(_shuffle_left.id, _shuffle_right.id), 6)";

    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "local-join-filter-test", .join_id = 0});

    EXPECT_EQ(
        createDistributedShuffleJoinLocalJoinQuery(table_names, info),
        "SELECT * FROM default._shuffle_local_join_filter_test_0_left AS _shuffle_left "
        "INNER ALL JOIN default._shuffle_local_join_filter_test_0_right AS _shuffle_right "
        "ON _shuffle_left.id = _shuffle_right.id "
        "WHERE greaterOrEquals(plus(_shuffle_left.id, _shuffle_right.id), 6)");
}

TEST(DistributedShuffleJoin, CreatesExecutionPlan)
{
    auto info = makeExchangeInfo();
    info.left_required_columns = {
        {"id", std::make_shared<DataTypeUInt64>()},
        {"left value", std::make_shared<DataTypeUInt64>()},
    };
    info.right_required_columns = {
        {"id", std::make_shared<DataTypeUInt64>()},
        {"right value", std::make_shared<DataTypeUInt64>()},
    };

    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "shuffle db",
        DistributedShuffleJoinExchangeId{.initial_query_id = "execution-plan-test", .join_id = 12},
        info);

    EXPECT_EQ(plan.table_names.left_table, "_shuffle_execution_plan_test_12_left");
    EXPECT_EQ(plan.table_names.right_table, "_shuffle_execution_plan_test_12_right");
    ASSERT_EQ(plan.left_header.columns(), 2);
    EXPECT_EQ(plan.left_header.getByPosition(0).name, "id");
    EXPECT_EQ(plan.left_header.getByPosition(1).name, "left value");
    ASSERT_EQ(plan.right_header.columns(), 2);
    EXPECT_EQ(plan.right_header.getByPosition(0).name, "id");
    EXPECT_EQ(plan.right_header.getByPosition(1).name, "right value");
    EXPECT_EQ(
        plan.local_join_query,
        "SELECT * FROM `shuffle db`._shuffle_execution_plan_test_12_left AS _shuffle_left "
        "INNER ALL JOIN `shuffle db`._shuffle_execution_plan_test_12_right AS _shuffle_right "
        "ON _shuffle_left.id = _shuffle_right.id");
}

TEST(DistributedShuffleJoin, ExecutionPlanUsesUniqueFallbackQueryId)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    const auto plan = createDistributedShuffleJoinExecutionPlan(context, 7, makeExchangeInfo());

    EXPECT_TRUE(plan.table_names.left_table.starts_with("_shuffle_distributed_shuffle_join_"));
    EXPECT_TRUE(plan.table_names.left_table.ends_with("_left"));
    EXPECT_NE(plan.table_names.left_table, "_shuffle_distributed_shuffle_join_7_left");
    EXPECT_TRUE(tryGetDistributedShuffleJoinTableExpirationTimeMs(plan.table_names.left_table).has_value());
}

TEST(DistributedShuffleJoin, ExecutionPlanCarriesGlobalLimit)
{
    auto info = makeExchangeInfo();
    info.limit_length = 2;
    info.limit_offset = 1;

    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "limit-plan-test", .join_id = 0},
        info);

    ASSERT_TRUE(plan.limit_length.has_value());
    EXPECT_EQ(*plan.limit_length, 2);
    EXPECT_EQ(plan.limit_offset, 1);
}

TEST(DistributedShuffleJoin, ExecutionPlanCarriesGlobalOrderBy)
{
    auto info = makeExchangeInfo();
    info.order_by.emplace_back("id", -1, -1);
    info.order_by.emplace_back("value", 1, 1);

    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "order-plan-test", .join_id = 0},
        info);

    ASSERT_EQ(plan.order_by.size(), 2);
    EXPECT_EQ(plan.order_by[0].column_name, "id");
    EXPECT_EQ(plan.order_by[0].direction, -1);
    EXPECT_EQ(plan.order_by[0].nulls_direction, -1);
    EXPECT_EQ(plan.order_by[1].column_name, "value");
    EXPECT_EQ(plan.order_by[1].direction, 1);
    EXPECT_EQ(plan.order_by[1].nulls_direction, 1);
}

TEST(DistributedShuffleJoin, CreatesRemoteExchangeQuery)
{
    EXPECT_EQ(
        createDistributedShuffleJoinRemoteExchangeQuery("query=q123;join=0"),
        "SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE 'query=q123;join=0'");

    EXPECT_THROW(createDistributedShuffleJoinRemoteExchangeQuery(""), Exception);
}

TEST(DistributedShuffleJoin, SerializesAndParsesRemoteExchangePayload)
{
    const auto table_names = createDistributedShuffleJoinTableNames(
        "shuffle db",
        DistributedShuffleJoinExchangeId{.initial_query_id = "payload-test", .join_id = 9});

    DistributedShuffleJoinExchangePayload payload;
    payload.cluster_name = "test_cluster";
    payload.shard_count = 2;
    payload.table_names = table_names;
    payload.left_source = DistributedShuffleJoinExchangeSource{.database = "default", .table = "left local"};
    payload.right_source = DistributedShuffleJoinExchangeSource{.database = "default", .table = "right local"};
    payload.left_key_column_name = "left key";
    payload.right_key_column_name = "right key";
    payload.left_required_columns = {
        {"left key", std::make_shared<DataTypeUInt64>()},
        {"left value", std::make_shared<DataTypeUInt64>()},
    };
    payload.right_required_columns = {
        {"right key", std::make_shared<DataTypeUInt64>()},
        {"right value", std::make_shared<DataTypeUInt64>()},
    };
    payload.left_filter_condition = "`left value` > 10";
    payload.right_filter_condition = "`right value` < 20";

    const auto serialized = serializeDistributedShuffleJoinExchangePayload(payload);
    const auto parsed = parseDistributedShuffleJoinExchangePayload(serialized);

    EXPECT_EQ(parsed.cluster_name, payload.cluster_name);
    EXPECT_EQ(parsed.shard_count, payload.shard_count);
    EXPECT_EQ(parsed.table_names.database, payload.table_names.database);
    EXPECT_EQ(parsed.table_names.left_table, payload.table_names.left_table);
    EXPECT_EQ(parsed.table_names.right_table, payload.table_names.right_table);
    EXPECT_EQ(parsed.left_source.database, payload.left_source.database);
    EXPECT_EQ(parsed.left_source.table, payload.left_source.table);
    EXPECT_EQ(parsed.right_source.database, payload.right_source.database);
    EXPECT_EQ(parsed.right_source.table, payload.right_source.table);
    EXPECT_EQ(parsed.left_key_column_name, payload.left_key_column_name);
    EXPECT_EQ(parsed.right_key_column_name, payload.right_key_column_name);
    EXPECT_EQ(parsed.left_filter_condition, payload.left_filter_condition);
    EXPECT_EQ(parsed.right_filter_condition, payload.right_filter_condition);

    ASSERT_EQ(parsed.left_required_columns.size(), 2);
    EXPECT_EQ(parsed.left_required_columns[0].name, "left key");
    EXPECT_EQ(parsed.left_required_columns[0].type->getName(), "UInt64");
    EXPECT_EQ(parsed.left_required_columns[1].name, "left value");
    EXPECT_EQ(parsed.left_required_columns[1].type->getName(), "UInt64");

    ASSERT_EQ(parsed.right_required_columns.size(), 2);
    EXPECT_EQ(parsed.right_required_columns[0].name, "right key");
    EXPECT_EQ(parsed.right_required_columns[0].type->getName(), "UInt64");
    EXPECT_EQ(parsed.right_required_columns[1].name, "right value");
    EXPECT_EQ(parsed.right_required_columns[1].type->getName(), "UInt64");

    EXPECT_EQ(
        createDistributedShuffleJoinExchangeSourceQuery(parsed, DistributedShuffleJoinTableSide::Left),
        "SELECT `left key`, `left value` FROM default.`left local` WHERE `left value` > 10");
    EXPECT_EQ(
        createDistributedShuffleJoinExchangeSourceQuery(parsed, DistributedShuffleJoinTableSide::Right),
        "SELECT `right key`, `right value` FROM default.`right local` WHERE `right value` < 20");
}

TEST(DistributedShuffleJoin, ParsesRemoteExchangeSystemQuery)
{
    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "system-payload-test", .join_id = 10});

    DistributedShuffleJoinExchangePayload payload;
    payload.cluster_name = "test_cluster";
    payload.shard_count = 2;
    payload.table_names = table_names;
    payload.left_source = DistributedShuffleJoinExchangeSource{.database = "default", .table = "left_local"};
    payload.right_source = DistributedShuffleJoinExchangeSource{.database = "default", .table = "right_local"};
    payload.left_key_column_name = "id";
    payload.right_key_column_name = "id";
    payload.left_required_columns = {{"id", std::make_shared<DataTypeUInt64>()}};
    payload.right_required_columns = {{"id", std::make_shared<DataTypeUInt64>()}};

    const auto serialized_payload = serializeDistributedShuffleJoinExchangePayload(payload);
    const auto query = createDistributedShuffleJoinRemoteExchangeQuery(serialized_payload);

    ParserSystemQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0, 0);

    const auto * system_query = ast->as<ASTSystemQuery>();
    ASSERT_NE(system_query, nullptr);
    EXPECT_EQ(system_query->type, ASTSystemQuery::Type::DISTRIBUTED_SHUFFLE_JOIN_EXCHANGE);
    EXPECT_EQ(system_query->distributed_shuffle_join_exchange_payload, serialized_payload);
    EXPECT_EQ(system_query->formatWithSecretsOneLine(), query);

    const auto parsed = parseDistributedShuffleJoinExchangePayload(system_query->distributed_shuffle_join_exchange_payload);
    EXPECT_EQ(parsed.cluster_name, payload.cluster_name);
    EXPECT_EQ(parsed.table_names.left_table, payload.table_names.left_table);
    EXPECT_EQ(parsed.left_source.table, payload.left_source.table);
}

TEST(DistributedShuffleJoin, RemoteExchangeSystemQueryRejectsUserQuery)
{
    const String query = "SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE 'payload'";

    ParserSystemQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0, 0);

    auto context = Context::createCopy(getContext().context);
    EXPECT_THROW(InterpreterSystemQuery(ast, context).execute(), Exception);
}

TEST(DistributedShuffleJoin, ExchangeSourceRunsBothSides)
{
    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "exchange-source-test", .join_id = 0});

    RecordingShuffleSideExecutor side_executor;
    auto sender = std::make_shared<CapturingShuffleTableBlockSender>(2);

    executeDistributedShuffleJoinExchangeSource(table_names, side_executor, sender);

    ASSERT_EQ(side_executor.getSides().size(), 2);
    EXPECT_EQ(side_executor.getSides()[0], DistributedShuffleJoinTableSide::Left);
    EXPECT_EQ(side_executor.getSides()[1], DistributedShuffleJoinTableSide::Right);
    EXPECT_FALSE(sender->wasCancelled());
}

TEST(DistributedShuffleJoin, ExchangeSourceCancelsSenderAfterFailure)
{
    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "exchange-source-failure-test", .join_id = 0});

    RecordingShuffleSideExecutor side_executor;
    side_executor.failOnSide(DistributedShuffleJoinTableSide::Right);
    auto sender = std::make_shared<CapturingShuffleTableBlockSender>(2);

    EXPECT_THROW(executeDistributedShuffleJoinExchangeSource(table_names, side_executor, sender), Exception);

    ASSERT_EQ(side_executor.getSides().size(), 1);
    EXPECT_EQ(side_executor.getSides()[0], DistributedShuffleJoinTableSide::Left);
    EXPECT_TRUE(sender->wasCancelled());
}

TEST(DistributedShuffleJoin, CurrentShardExchangeExecutorRunsSourceExchange)
{
    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "current-shard-exchange-test", .join_id = 0});

    RecordingShuffleSideExecutor side_executor;
    auto sender = std::make_shared<CapturingShuffleTableBlockSender>(2);
    CurrentShardDistributedShuffleJoinExchangeExecutor exchange_executor(1, side_executor, sender);

    exchange_executor.executeOnShard(1, table_names);

    ASSERT_EQ(side_executor.getSides().size(), 2);
    EXPECT_EQ(side_executor.getSides()[0], DistributedShuffleJoinTableSide::Left);
    EXPECT_EQ(side_executor.getSides()[1], DistributedShuffleJoinTableSide::Right);
    EXPECT_FALSE(sender->wasCancelled());
}

TEST(DistributedShuffleJoin, CurrentShardExchangeExecutorRejectsRemoteShard)
{
    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "current-shard-exchange-remote-test", .join_id = 0});

    RecordingShuffleSideExecutor side_executor;
    auto sender = std::make_shared<CapturingShuffleTableBlockSender>(2);
    CurrentShardDistributedShuffleJoinExchangeExecutor exchange_executor(1, side_executor, sender);

    EXPECT_THROW(exchange_executor.executeOnShard(0, table_names), Exception);
    EXPECT_TRUE(side_executor.getSides().empty());
    EXPECT_FALSE(sender->wasCancelled());
}

TEST(DistributedShuffleJoin, CurrentShardExchangeExecutorFactoryValidatesInputs)
{
    EXPECT_THROW(
        createCurrentShardDistributedShuffleJoinExchangeExecutor(
            0,
            nullptr,
            nullptr,
            makeExchangeInfo()),
        Exception);

    EXPECT_THROW(
        createCurrentShardDistributedShuffleJoinExchangeExecutor(
            0,
            nullptr,
            makeTwoShardCluster(),
            makeExchangeInfo()),
        Exception);
}

TEST(DistributedShuffleJoin, SystemQueryExchangeExecutorNeedsDistributedStorages)
{
    const auto table_names = createDistributedShuffleJoinTableNames(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "system-query-exchange-test", .join_id = 11});

    RecordingShuffleQueryExecutor query_executor;
    SystemQueryDistributedShuffleJoinExchangeExecutor exchange_executor(query_executor, makeExchangeInfo());

    EXPECT_THROW(exchange_executor.executeOnShard(0, table_names), Exception);
    EXPECT_TRUE(query_executor.getQueries().empty());
}

TEST(DistributedShuffleJoin, CreatesMemoryTableNamesAndQueries)
{
    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "query-with-dashes", .join_id = 7};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);

    EXPECT_EQ(table_names.left_table, "_shuffle_query_with_dashes_7_left");
    EXPECT_EQ(table_names.right_table, "_shuffle_query_with_dashes_7_right");
    EXPECT_EQ(table_names.getQualifiedTableName(DistributedShuffleJoinTableSide::Left), "default._shuffle_query_with_dashes_7_left");

    EXPECT_EQ(
        createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Left, *makeHeader()),
        "CREATE TABLE IF NOT EXISTS default._shuffle_query_with_dashes_7_left (id UInt64) ENGINE = Memory");
    EXPECT_EQ(
        dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Right),
        "DROP TABLE IF EXISTS default._shuffle_query_with_dashes_7_right");
}

TEST(DistributedShuffleJoin, EncodesAndParsesMemoryTableExpiration)
{
    const DistributedShuffleJoinExchangeId exchange_id{
        .initial_query_id = "expiring-query",
        .join_id = 2,
        .expiration_time_ms = 123456};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);

    EXPECT_EQ(table_names.left_table, "_shuffle_expiring_query_2_expires_123456_left");
    EXPECT_EQ(table_names.right_table, "_shuffle_expiring_query_2_expires_123456_right");
    EXPECT_EQ(tryGetDistributedShuffleJoinTableExpirationTimeMs(table_names.left_table), 123456);
    EXPECT_EQ(tryGetDistributedShuffleJoinTableExpirationTimeMs(table_names.right_table), 123456);
    EXPECT_FALSE(tryGetDistributedShuffleJoinTableExpirationTimeMs("_shuffle_expiring_query_2_left").has_value());
    EXPECT_FALSE(tryGetDistributedShuffleJoinTableExpirationTimeMs("_shuffle_expiring_query_2_expires_invalid_left").has_value());
}

TEST(DistributedShuffleJoin, CreatesMemoryTableHeaderFromRequiredColumns)
{
    const NamesAndTypes required_columns
    {
        {"id", std::make_shared<DataTypeUInt64>()},
        {"value", std::make_shared<DataTypeUInt64>()},
    };

    const auto header = createDistributedShuffleJoinTableHeader(required_columns);
    ASSERT_EQ(header.columns(), 2);
    EXPECT_EQ(header.getByPosition(0).name, "id");
    EXPECT_EQ(header.getByPosition(1).name, "value");
    EXPECT_EQ(header.getByPosition(0).type->getName(), "UInt64");
    EXPECT_EQ(header.getByPosition(1).type->getName(), "UInt64");

    EXPECT_THROW(createDistributedShuffleJoinTableHeader({}), Exception);
}

TEST(DistributedShuffleJoin, CoordinatorPreparesAndCleansMemoryTables)
{
    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "coordinator-test", .join_id = 3};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);

    RecordingShuffleQueryExecutor executor;
    DistributedShuffleJoinCoordinator coordinator(table_names, *makeHeader(), *makeHeader(), 2, executor);

    coordinator.prepareShuffleTables();
    EXPECT_TRUE(coordinator.hasPreparedShuffleTables());

    const auto create_left = createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Left, *makeHeader());
    const auto create_right = createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Right, *makeHeader());
    const auto drop_left = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(executor.getQueries().size(), 4);
    EXPECT_EQ(executor.getQueries()[0].query, create_left);
    EXPECT_EQ(executor.getQueries()[1].query, create_left);
    EXPECT_EQ(executor.getQueries()[2].query, create_right);
    EXPECT_EQ(executor.getQueries()[3].query, create_right);
    EXPECT_NE(executor.getQueries()[0].shard_index, executor.getQueries()[1].shard_index);
    EXPECT_NE(executor.getQueries()[2].shard_index, executor.getQueries()[3].shard_index);

    coordinator.cleanupShuffleTables();
    EXPECT_TRUE(coordinator.hasCleanedUpShuffleTables());

    ASSERT_EQ(executor.getQueries().size(), 8);
    EXPECT_EQ(executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(executor.getQueries()[7].query, drop_right);
}

TEST(DistributedShuffleJoin, ExecutionStagesRunPrepareExchangeJoinAndCleanup)
{
    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "execution-stages-test", .join_id = 13},
        makeExchangeInfo());

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;
    RecordingShuffleLocalJoinExecutor local_join_executor;

    executeDistributedShuffleJoinStages(
        plan,
        2,
        query_executor,
        exchange_executor,
        local_join_executor);

    const auto create_left = createDistributedShuffleJoinMemoryTableQuery(
        plan.table_names,
        DistributedShuffleJoinTableSide::Left,
        plan.left_header);
    const auto create_right = createDistributedShuffleJoinMemoryTableQuery(
        plan.table_names,
        DistributedShuffleJoinTableSide::Right,
        plan.right_header);
    const auto drop_left = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(query_executor.getQueries().size(), 8);
    EXPECT_EQ(query_executor.getQueries()[0].query, create_left);
    EXPECT_EQ(query_executor.getQueries()[1].query, create_left);
    EXPECT_EQ(query_executor.getQueries()[2].query, create_right);
    EXPECT_EQ(query_executor.getQueries()[3].query, create_right);
    EXPECT_EQ(query_executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(query_executor.getQueries()[7].query, drop_right);

    ASSERT_EQ(exchange_executor.getExchanges().size(), 2);
    EXPECT_NE(exchange_executor.getExchanges()[0].shard_index, exchange_executor.getExchanges()[1].shard_index);

    ASSERT_EQ(local_join_executor.getJoins().size(), 2);
    EXPECT_EQ(local_join_executor.getJoins()[0].query, plan.local_join_query);
    EXPECT_EQ(local_join_executor.getJoins()[1].query, plan.local_join_query);
}

TEST(DistributedShuffleJoin, PrepareExchangeKeepsTablesUntilCoordinatorCleanup)
{
    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "prepare-exchange-lifecycle-test", .join_id = 14},
        makeExchangeInfo());

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;

    auto coordinator = prepareDistributedShuffleJoinExchange(
        plan,
        2,
        query_executor,
        exchange_executor);

    EXPECT_TRUE(coordinator->hasPreparedShuffleTables());
    EXPECT_TRUE(coordinator->hasExchangedShuffleTables());
    EXPECT_FALSE(coordinator->hasCleanedUpShuffleTables());

    ASSERT_EQ(query_executor.getQueries().size(), 4);
    ASSERT_EQ(exchange_executor.getExchanges().size(), 2);

    coordinator->cleanupShuffleTables();

    const auto drop_left = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(query_executor.getQueries().size(), 8);
    EXPECT_EQ(query_executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(query_executor.getQueries()[7].query, drop_right);
}

TEST(DistributedShuffleJoin, CoordinatorResourceCleansTablesOnDestruction)
{
    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "coordinator-resource-test", .join_id = 15},
        makeExchangeInfo());

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;

    {
        auto coordinator = prepareDistributedShuffleJoinExchange(
            plan,
            2,
            query_executor,
            exchange_executor);
        auto holder = holdDistributedShuffleJoinCoordinator(std::move(coordinator));
        EXPECT_EQ(holder.custom_resources.size(), 1);

        ASSERT_EQ(query_executor.getQueries().size(), 4);
        ASSERT_EQ(exchange_executor.getExchanges().size(), 2);
    }

    const auto drop_left = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(query_executor.getQueries().size(), 8);
    EXPECT_EQ(query_executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(query_executor.getQueries()[7].query, drop_right);

    EXPECT_THROW(holdDistributedShuffleJoinCoordinator(nullptr), Exception);
}

TEST(DistributedShuffleJoin, OwnedCoordinatorResourceValidatesInputs)
{
    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "owned-coordinator-resource-test", .join_id = 19},
        makeExchangeInfo());

    auto query_executor = std::make_unique<RecordingShuffleQueryExecutor>();
    auto coordinator = std::make_unique<DistributedShuffleJoinCoordinator>(
        plan.table_names,
        plan.left_header,
        plan.right_header,
        2,
        *query_executor);

    EXPECT_THROW(
        holdDistributedShuffleJoinCoordinator(
            nullptr,
            std::move(coordinator)),
        Exception);

    query_executor = std::make_unique<RecordingShuffleQueryExecutor>();
    EXPECT_THROW(
        holdDistributedShuffleJoinCoordinator(
            std::move(query_executor),
            nullptr),
        Exception);
}

TEST(DistributedShuffleJoin, AttachedCoordinatorResourceCleansTablesWithPipeline)
{
    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "attached-coordinator-resource-test", .join_id = 16},
        makeExchangeInfo());

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;

    {
        QueryPipeline pipeline;
        auto coordinator = prepareDistributedShuffleJoinExchange(
            plan,
            2,
            query_executor,
            exchange_executor);
        attachDistributedShuffleJoinCoordinator(pipeline, std::move(coordinator));

        ASSERT_EQ(query_executor.getQueries().size(), 4);
        ASSERT_EQ(exchange_executor.getExchanges().size(), 2);
    }

    const auto drop_left = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(query_executor.getQueries().size(), 8);
    EXPECT_EQ(query_executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(query_executor.getQueries()[7].query, drop_right);
}

TEST(DistributedShuffleJoin, LocalJoinPipelineFailureCleansCoordinator)
{
    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "local-join-pipeline-failure-test", .join_id = 17},
        makeExchangeInfo());

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;

    auto coordinator = prepareDistributedShuffleJoinExchange(
        plan,
        2,
        query_executor,
        exchange_executor);

    EXPECT_THROW(
        executeDistributedShuffleJoinLocalJoinPipeline(
            plan,
            nullptr,
            std::move(coordinator)),
        Exception);

    const auto drop_left = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(query_executor.getQueries().size(), 8);
    EXPECT_EQ(query_executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(query_executor.getQueries()[7].query, drop_right);
}

TEST(DistributedShuffleJoin, ClusterLocalJoinPipelineFailureCleansCoordinator)
{
    const auto plan = createDistributedShuffleJoinExecutionPlan(
        "default",
        DistributedShuffleJoinExchangeId{.initial_query_id = "cluster-local-join-pipeline-failure-test", .join_id = 18},
        makeExchangeInfo());

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;

    auto coordinator = prepareDistributedShuffleJoinExchange(
        plan,
        2,
        query_executor,
        exchange_executor);

    EXPECT_THROW(
        executeDistributedShuffleJoinClusterLocalJoinPipeline(
            plan,
            nullptr,
            makeTwoShardCluster(),
            std::move(coordinator)),
        Exception);

    const auto drop_left = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(plan.table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(query_executor.getQueries().size(), 8);
    EXPECT_EQ(query_executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(query_executor.getQueries()[7].query, drop_right);
}

TEST(DistributedShuffleJoin, FullPipelineEntryValidatesInputs)
{
    EXPECT_THROW(
        executeDistributedShuffleJoinPipeline(
            makeExchangeInfo(),
            nullptr,
            0),
        Exception);
}

TEST(DistributedShuffleJoin, CoordinatorCleansMemoryTablesAfterPrepareFailure)
{
    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "coordinator-failure-test", .join_id = 4};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);

    RecordingShuffleQueryExecutor executor;
    executor.failAtQueryIndex(2);

    DistributedShuffleJoinCoordinator coordinator(table_names, *makeHeader(), *makeHeader(), 2, executor);

    EXPECT_THROW(coordinator.prepareShuffleTables(), Exception);
    EXPECT_FALSE(coordinator.hasPreparedShuffleTables());
    EXPECT_TRUE(coordinator.hasCleanedUpShuffleTables());

    const auto drop_left = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Right);
    const auto create_left = createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Left, *makeHeader());
    const auto create_right = createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Right, *makeHeader());
    const auto count_query = [&](const String & query)
    {
        return std::count_if(executor.getQueries().begin(), executor.getQueries().end(), [&](const auto & executed)
        {
            return executed.query == query;
        });
    };

    EXPECT_EQ(count_query(create_left), 2);
    EXPECT_LE(count_query(create_right), 1);
    EXPECT_EQ(count_query(drop_left), 2);
    EXPECT_EQ(count_query(drop_right), 2);
}

TEST(DistributedShuffleJoin, CoordinatorContinuesCleanupAfterDropFailure)
{
    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "coordinator-drop-failure-test", .join_id = 20};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);

    RecordingShuffleQueryExecutor executor;
    DistributedShuffleJoinCoordinator coordinator(table_names, *makeHeader(), *makeHeader(), 2, executor);
    coordinator.prepareShuffleTables();

    executor.failAtQueryIndex(4);
    coordinator.cleanupShuffleTables();

    EXPECT_TRUE(coordinator.hasCleanedUpShuffleTables());

    const auto drop_left = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Right);
    const auto count_query = [&](const String & query)
    {
        return std::count_if(executor.getQueries().begin(), executor.getQueries().end(), [&](const auto & executed)
        {
            return executed.query == query;
        });
    };

    EXPECT_EQ(count_query(drop_left), 1);
    EXPECT_EQ(count_query(drop_right), 2);
}

TEST(DistributedShuffleJoin, CoordinatorRunsExchangeAfterPrepare)
{
    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "coordinator-exchange-test", .join_id = 5};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;
    DistributedShuffleJoinCoordinator coordinator(table_names, *makeHeader(), *makeHeader(), 2, query_executor);

    EXPECT_THROW(coordinator.exchangeShuffleTables(exchange_executor), Exception);

    coordinator.prepareShuffleTables();
    coordinator.exchangeShuffleTables(exchange_executor);

    EXPECT_TRUE(coordinator.hasExchangedShuffleTables());
    ASSERT_EQ(exchange_executor.getExchanges().size(), 2);
    EXPECT_EQ(exchange_executor.getExchanges()[0].table_names.left_table, table_names.left_table);
    EXPECT_EQ(exchange_executor.getExchanges()[1].table_names.right_table, table_names.right_table);
    EXPECT_NE(exchange_executor.getExchanges()[0].shard_index, exchange_executor.getExchanges()[1].shard_index);

    coordinator.exchangeShuffleTables(exchange_executor);
    EXPECT_EQ(exchange_executor.getExchanges().size(), 2);
}

TEST(DistributedShuffleJoin, CoordinatorCleansMemoryTablesAfterExchangeFailure)
{
    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "coordinator-exchange-failure-test", .join_id = 6};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;
    exchange_executor.failAtExchangeIndex(1);

    DistributedShuffleJoinCoordinator coordinator(table_names, *makeHeader(), *makeHeader(), 2, query_executor);
    coordinator.prepareShuffleTables();

    EXPECT_THROW(coordinator.exchangeShuffleTables(exchange_executor), Exception);
    EXPECT_FALSE(coordinator.hasPreparedShuffleTables());
    EXPECT_FALSE(coordinator.hasExchangedShuffleTables());
    EXPECT_TRUE(coordinator.hasCleanedUpShuffleTables());

    const auto drop_left = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(exchange_executor.getExchanges().size(), 1);
    ASSERT_EQ(query_executor.getQueries().size(), 8);
    EXPECT_EQ(query_executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(query_executor.getQueries()[7].query, drop_right);
}

TEST(DistributedShuffleJoin, CoordinatorRunsLocalJoinAfterExchange)
{
    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "coordinator-local-join-test", .join_id = 7};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);
    const auto local_join_query = createDistributedShuffleJoinLocalJoinQuery(table_names, makeExchangeInfo());

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;
    RecordingShuffleLocalJoinExecutor local_join_executor;
    DistributedShuffleJoinCoordinator coordinator(table_names, *makeHeader(), *makeHeader(), 2, query_executor);

    EXPECT_THROW(coordinator.joinShuffleTables(local_join_query, local_join_executor), Exception);

    coordinator.prepareShuffleTables();
    EXPECT_THROW(coordinator.joinShuffleTables(local_join_query, local_join_executor), Exception);

    coordinator.exchangeShuffleTables(exchange_executor);
    coordinator.joinShuffleTables(local_join_query, local_join_executor);

    EXPECT_TRUE(coordinator.hasJoinedShuffleTables());
    ASSERT_EQ(local_join_executor.getJoins().size(), 2);
    EXPECT_EQ(local_join_executor.getJoins()[0].query, local_join_query);
    EXPECT_EQ(local_join_executor.getJoins()[1].query, local_join_query);
    EXPECT_NE(local_join_executor.getJoins()[0].shard_index, local_join_executor.getJoins()[1].shard_index);

    coordinator.joinShuffleTables(local_join_query, local_join_executor);
    EXPECT_EQ(local_join_executor.getJoins().size(), 2);
}

TEST(DistributedShuffleJoin, CoordinatorCleansMemoryTablesAfterLocalJoinFailure)
{
    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "coordinator-local-join-failure-test", .join_id = 8};
    const auto table_names = createDistributedShuffleJoinTableNames("default", exchange_id);
    const auto local_join_query = createDistributedShuffleJoinLocalJoinQuery(table_names, makeExchangeInfo());

    RecordingShuffleQueryExecutor query_executor;
    RecordingShuffleExchangeExecutor exchange_executor;
    RecordingShuffleLocalJoinExecutor local_join_executor;
    local_join_executor.failAtJoinIndex(1);

    DistributedShuffleJoinCoordinator coordinator(table_names, *makeHeader(), *makeHeader(), 2, query_executor);
    coordinator.prepareShuffleTables();
    coordinator.exchangeShuffleTables(exchange_executor);

    EXPECT_THROW(coordinator.joinShuffleTables(local_join_query, local_join_executor), Exception);
    EXPECT_FALSE(coordinator.hasPreparedShuffleTables());
    EXPECT_FALSE(coordinator.hasExchangedShuffleTables());
    EXPECT_FALSE(coordinator.hasJoinedShuffleTables());
    EXPECT_TRUE(coordinator.hasCleanedUpShuffleTables());

    const auto drop_left = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Left);
    const auto drop_right = dropDistributedShuffleJoinTableQuery(table_names, DistributedShuffleJoinTableSide::Right);

    ASSERT_EQ(local_join_executor.getJoins().size(), 1);
    ASSERT_EQ(query_executor.getQueries().size(), 8);
    EXPECT_EQ(query_executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(query_executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(query_executor.getQueries()[7].query, drop_right);
}

TEST(DistributedShuffleJoin, SelectorUsesJoinKeyColumn)
{
    auto selector = createDistributedShuffleJoinSelector(makeTwoShardCluster(), "id");

    const auto left_selector = selector(makeBlock({1, 2, 7}));
    const auto right_selector = selector(makeBlock({7, 1, 2}));

    ASSERT_EQ(left_selector.size(), 3);
    ASSERT_EQ(right_selector.size(), 3);

    EXPECT_EQ(left_selector[0], right_selector[1]);
    EXPECT_EQ(left_selector[1], right_selector[2]);
    EXPECT_EQ(left_selector[2], right_selector[0]);
}

TEST(DistributedShuffleJoin, SelectorCanBeCreatedFromAnalyzerInfo)
{
    auto info = makeExchangeInfo();

    auto left_selector = createDistributedShuffleJoinSelector(
        makeTwoShardCluster(),
        info,
        DistributedShuffleJoinTableSide::Left);
    auto right_selector = createDistributedShuffleJoinSelector(
        makeTwoShardCluster(),
        info,
        DistributedShuffleJoinTableSide::Right);

    const auto left_targets = left_selector(makeBlock({1, 2}));
    const auto right_targets = right_selector(makeBlock({2, 1}));

    ASSERT_EQ(left_targets.size(), 2);
    ASSERT_EQ(right_targets.size(), 2);
    EXPECT_EQ(left_targets[0], right_targets[1]);
    EXPECT_EQ(left_targets[1], right_targets[0]);
}

}
