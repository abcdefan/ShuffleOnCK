#include <Storages/DistributedShuffleJoinExchange.h>
#include <Storages/DistributedShuffleJoinSink.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
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

IColumn::Selector selectByIdParity(const Block & block)
{
    const auto & id_column = typeid_cast<const ColumnUInt64 &>(*block.getByName("id").column);
    const auto & ids = id_column.getData();

    IColumn::Selector selector(ids.size());
    for (size_t i = 0; i < ids.size(); ++i)
        selector[i] = ids[i] % 2;

    return selector;
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
            selectByIdParity,
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

}
