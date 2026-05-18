#include <Storages/DistributedShuffleJoinAnalyzer.h>
#include <Storages/DistributedShuffleJoinSelector.h>
#include <Storages/DistributedShuffleJoinSink.h>
#include <Storages/DistributedShuffleJoinTables.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Interpreters/DistributedShuffleJoinCoordinator.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Processors/Chunk.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
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

ClusterPtr makeTwoShardCluster()
{
    static const String username = "default";
    static const String password;
    static const String bind_host;
    static const String cluster_name = "distributed_shuffle_join_test";
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
        std::vector<std::vector<String>>
        {
            {"127.0.0.1:9000"},
            {"127.0.0.2:9000"},
        },
        params);
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
        if (fail_at_query_index && queries.size() == *fail_at_query_index)
        {
            fail_at_query_index.reset();
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Injected distributed `shuffle join` query executor failure");
        }

        queries.push_back(ExecutedQuery{.shard_index = shard_index, .query = query});
    }

    void failAtQueryIndex(size_t query_index)
    {
        fail_at_query_index = query_index;
    }

    const std::vector<ExecutedQuery> & getQueries() const
    {
        return queries;
    }

private:
    std::vector<ExecutedQuery> queries;
    std::optional<size_t> fail_at_query_index;
};

void consumeOneBlock(DistributedShuffleJoinSink & sink, const std::vector<UInt64> & ids)
{
    auto chunk = makeChunk(ids);
    sink.consume(chunk);
    sink.onFinish();
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
    EXPECT_EQ(executor.getQueries()[0].shard_index, 0);
    EXPECT_EQ(executor.getQueries()[0].query, create_left);
    EXPECT_EQ(executor.getQueries()[1].shard_index, 1);
    EXPECT_EQ(executor.getQueries()[1].query, create_left);
    EXPECT_EQ(executor.getQueries()[2].shard_index, 0);
    EXPECT_EQ(executor.getQueries()[2].query, create_right);
    EXPECT_EQ(executor.getQueries()[3].shard_index, 1);
    EXPECT_EQ(executor.getQueries()[3].query, create_right);

    coordinator.cleanupShuffleTables();
    EXPECT_TRUE(coordinator.hasCleanedUpShuffleTables());

    ASSERT_EQ(executor.getQueries().size(), 8);
    EXPECT_EQ(executor.getQueries()[4].query, drop_left);
    EXPECT_EQ(executor.getQueries()[5].query, drop_left);
    EXPECT_EQ(executor.getQueries()[6].query, drop_right);
    EXPECT_EQ(executor.getQueries()[7].query, drop_right);
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

    ASSERT_EQ(executor.getQueries().size(), 6);
    EXPECT_EQ(executor.getQueries()[0].query, createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Left, *makeHeader()));
    EXPECT_EQ(executor.getQueries()[1].query, createDistributedShuffleJoinMemoryTableQuery(table_names, DistributedShuffleJoinTableSide::Left, *makeHeader()));
    EXPECT_EQ(executor.getQueries()[2].query, drop_left);
    EXPECT_EQ(executor.getQueries()[3].query, drop_left);
    EXPECT_EQ(executor.getQueries()[4].query, drop_right);
    EXPECT_EQ(executor.getQueries()[5].query, drop_right);
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
    DistributedShuffleJoinInfo info;
    info.left_key_column_name = "id";
    info.right_key_column_name = "id";

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
