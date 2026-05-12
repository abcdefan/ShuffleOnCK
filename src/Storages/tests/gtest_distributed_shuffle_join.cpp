#include <Storages/DistributedShuffleJoinExchange.h>
#include <Storages/DistributedShuffleJoinSelector.h>
#include <Storages/DistributedShuffleJoinSink.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Processors/Chunk.h>

#include <gtest/gtest.h>

#include <algorithm>

namespace DB
{
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

void consumeOneBlock(DistributedShuffleJoinSink & sink, const std::vector<UInt64> & ids)
{
    auto chunk = makeChunk(ids);
    sink.consume(chunk);
    sink.onFinish();
}

}

TEST(DistributedShuffleJoin, LocalSinkReceiverExchangeRoundTrip)
{
    DistributedShuffleJoinExchangeRegistry target0_registry;
    DistributedShuffleJoinExchangeRegistry target1_registry;

    DistributedShuffleJoinExchangeReceiver target0_receiver(target0_registry);
    DistributedShuffleJoinExchangeReceiver target1_receiver(target1_registry);

    auto sender = std::make_shared<LocalDistributedShuffleJoinBlockSender>(
        std::vector<DistributedShuffleJoinExchangeReceiver *>{&target0_receiver, &target1_receiver});
    auto selector = createDistributedShuffleJoinSelector(makeTwoShardCluster(), "id");

    const DistributedShuffleJoinExchangeId exchange_id{.initial_query_id = "local-shuffle-test", .join_id = 0};
    constexpr size_t source_shard_count = 2;
    constexpr size_t target_shard_count = 2;

    auto make_sink = [&](DistributedShuffleJoinTableSide side, size_t source_shard_index)
    {
        return DistributedShuffleJoinSink(
            makeHeader(),
            exchange_id,
            source_shard_count,
            source_shard_index,
            target_shard_count,
            side,
            selector,
            sender);
    };

    auto left_source0 = make_sink(DistributedShuffleJoinTableSide::Left, 0);
    auto left_source1 = make_sink(DistributedShuffleJoinTableSide::Left, 1);
    auto right_source0 = make_sink(DistributedShuffleJoinTableSide::Right, 0);
    auto right_source1 = make_sink(DistributedShuffleJoinTableSide::Right, 1);

    consumeOneBlock(left_source0, {1, 2});
    consumeOneBlock(left_source1, {3, 4});
    consumeOneBlock(right_source0, {2, 3});
    consumeOneBlock(right_source1, {1, 4});

    auto target0_exchange = target0_registry.find(exchange_id);
    auto target1_exchange = target1_registry.find(exchange_id);

    ASSERT_NE(target0_exchange, nullptr);
    ASSERT_NE(target1_exchange, nullptr);

    EXPECT_TRUE(target0_exchange->waitReadyFor(std::chrono::milliseconds(0)));
    EXPECT_TRUE(target1_exchange->waitReadyFor(std::chrono::milliseconds(0)));

    EXPECT_EQ(collectIds(target0_exchange->getBlocks(DistributedShuffleJoinTableSide::Left)), std::vector<UInt64>({2, 4}));
    EXPECT_EQ(collectIds(target0_exchange->getBlocks(DistributedShuffleJoinTableSide::Right)), std::vector<UInt64>({2, 4}));
    EXPECT_EQ(collectIds(target1_exchange->getBlocks(DistributedShuffleJoinTableSide::Left)), std::vector<UInt64>({1, 3}));
    EXPECT_EQ(collectIds(target1_exchange->getBlocks(DistributedShuffleJoinTableSide::Right)), std::vector<UInt64>({1, 3}));

    const auto target0_left_stats = target0_exchange->getStats(DistributedShuffleJoinTableSide::Left);
    const auto target1_right_stats = target1_exchange->getStats(DistributedShuffleJoinTableSide::Right);

    EXPECT_EQ(target0_left_stats.blocks, 2);
    EXPECT_EQ(target0_left_stats.rows, 2);
    EXPECT_EQ(target1_right_stats.blocks, 2);
    EXPECT_EQ(target1_right_stats.rows, 2);
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

}
