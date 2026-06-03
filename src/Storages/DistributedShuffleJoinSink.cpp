#include <Storages/DistributedShuffleJoinSink.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>

namespace ProfileEvents
{
    extern const Event DistributedShuffleJoinExchangeInputBlocks;
    extern const Event DistributedShuffleJoinExchangeInputRows;
    extern const Event DistributedShuffleJoinExchangeInputBytes;
    extern const Event DistributedShuffleJoinExchangeOutputBlocks;
    extern const Event DistributedShuffleJoinExchangeOutputRows;
    extern const Event DistributedShuffleJoinExchangeOutputBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

DistributedShuffleJoinSink::DistributedShuffleJoinSink(
    SharedHeader header,
    DistributedShuffleJoinTableNames table_names_,
    size_t target_shard_count_,
    DistributedShuffleJoinTableSide side_,
    DistributedShuffleJoinSelector selector_,
    DistributedShuffleJoinBlockSenderPtr sender_)
    : SinkToStorage(std::move(header))
    , table_names(std::move(table_names_))
    , target_shard_count(target_shard_count_)
    , side(side_)
    , selector(std::move(selector_))
    , sender(std::move(sender_))
{
    if (target_shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` sink must have at least one target shard");

    if (!selector)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` sink selector is empty");

    if (!sender)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` sink sender is empty");
}

void DistributedShuffleJoinSink::consume(Chunk & chunk)
{
    const auto input_rows = chunk.getNumRows();
    const auto input_bytes = chunk.bytes();
    auto block = getHeader().cloneWithColumns(chunk.getColumns());
    auto split_blocks = splitBlock(block);

    if (input_rows)
    {
        ProfileEvents::increment(ProfileEvents::DistributedShuffleJoinExchangeInputBlocks);
        ProfileEvents::increment(ProfileEvents::DistributedShuffleJoinExchangeInputRows, input_rows);
        ProfileEvents::increment(ProfileEvents::DistributedShuffleJoinExchangeInputBytes, input_bytes);
    }

    for (size_t target_shard_index = 0; target_shard_index < split_blocks.size(); ++target_shard_index)
    {
        if (!split_blocks[target_shard_index].rows())
            continue;

        ProfileEvents::increment(ProfileEvents::DistributedShuffleJoinExchangeOutputBlocks);
        ProfileEvents::increment(ProfileEvents::DistributedShuffleJoinExchangeOutputRows, split_blocks[target_shard_index].rows());
        ProfileEvents::increment(ProfileEvents::DistributedShuffleJoinExchangeOutputBytes, split_blocks[target_shard_index].bytes());

        sender->sendBlock(
            table_names,
            target_shard_index,
            side,
            std::move(split_blocks[target_shard_index]));
    }
}

void DistributedShuffleJoinSink::onFinish()
{
    finishAllTargets();
}

void DistributedShuffleJoinSink::onCancel() noexcept
{
    try
    {
        sender->cancel(table_names, "Distributed `shuffle join` sink was cancelled");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to cancel distributed `shuffle join` exchange from sink");
    }
}

Blocks DistributedShuffleJoinSink::splitBlock(const Block & block) const
{
    const auto row_count = block.rows();
    auto block_selector = selector(block);
    checkSelector(block_selector, row_count);

    Blocks split_blocks(target_shard_count);
    for (size_t target_shard_index = 0; target_shard_index < target_shard_count; ++target_shard_index)
        split_blocks[target_shard_index] = block.cloneEmpty();

    const size_t columns_in_block = block.columns();
    for (size_t column_index = 0; column_index < columns_in_block; ++column_index)
    {
        auto split_columns = block.getByPosition(column_index).column->scatter(target_shard_count, block_selector);
        for (size_t target_shard_index = 0; target_shard_index < target_shard_count; ++target_shard_index)
            split_blocks[target_shard_index].getByPosition(column_index).column = std::move(split_columns[target_shard_index]);
    }

    return split_blocks;
}

void DistributedShuffleJoinSink::checkSelector(const IColumn::Selector & block_selector, size_t rows) const
{
    if (block_selector.size() != rows)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed `shuffle join` selector size {} does not match block row count {}",
            block_selector.size(),
            rows);
    }

    for (size_t row_index = 0; row_index < block_selector.size(); ++row_index)
    {
        if (block_selector[row_index] >= target_shard_count)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Distributed `shuffle join` selector chose target shard {} for row {}, but target shard count is {}",
                block_selector[row_index],
                row_index,
                target_shard_count);
        }
    }
}

void DistributedShuffleJoinSink::finishAllTargets()
{
    if (finished)
        return;

    for (size_t target_shard_index = 0; target_shard_index < target_shard_count; ++target_shard_index)
    {
        sender->finish(
            table_names,
            target_shard_index,
            side);
    }

    finished = true;
}

}
