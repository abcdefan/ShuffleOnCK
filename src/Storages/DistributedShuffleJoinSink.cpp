#include <Storages/DistributedShuffleJoinSink.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

LocalDistributedShuffleJoinBlockSender::LocalDistributedShuffleJoinBlockSender(
    std::vector<DistributedShuffleJoinExchangeReceiver *> receivers_)
    : receivers(std::move(receivers_))
{
    if (receivers.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local sender must have at least one receiver");

    for (size_t i = 0; i < receivers.size(); ++i)
    {
        if (!receivers[i])
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` local sender receiver {} is null", i);
    }
}

void LocalDistributedShuffleJoinBlockSender::sendBlock(
    const DistributedShuffleJoinExchangeId & id,
    size_t target_shard_index,
    size_t source_shard_count,
    DistributedShuffleJoinTableSide side,
    size_t source_shard_index,
    Block block)
{
    getReceiver(target_shard_index).receiveBlock(id, source_shard_count, side, source_shard_index, std::move(block));
}

void LocalDistributedShuffleJoinBlockSender::finish(
    const DistributedShuffleJoinExchangeId & id,
    size_t target_shard_index,
    size_t source_shard_count,
    DistributedShuffleJoinTableSide side,
    size_t source_shard_index)
{
    getReceiver(target_shard_index).finishSource(id, source_shard_count, side, source_shard_index);
}

void LocalDistributedShuffleJoinBlockSender::cancel(const DistributedShuffleJoinExchangeId & id, String reason) noexcept
{
    for (auto * receiver : receivers)
    {
        try
        {
            receiver->cancelExchange(id, reason);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to cancel distributed `shuffle join` exchange");
        }
    }
}

DistributedShuffleJoinExchangeReceiver & LocalDistributedShuffleJoinBlockSender::getReceiver(size_t target_shard_index) const
{
    if (target_shard_index >= receivers.size())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Target shard index {} is out of range for distributed `shuffle join` local sender with {} receivers",
            target_shard_index,
            receivers.size());
    }

    return *receivers[target_shard_index];
}

DistributedShuffleJoinSink::DistributedShuffleJoinSink(
    SharedHeader header,
    DistributedShuffleJoinExchangeId exchange_id_,
    size_t source_shard_count_,
    size_t source_shard_index_,
    size_t target_shard_count_,
    DistributedShuffleJoinTableSide side_,
    DistributedShuffleJoinSelector selector_,
    DistributedShuffleJoinBlockSenderPtr sender_)
    : SinkToStorage(std::move(header))
    , exchange_id(std::move(exchange_id_))
    , source_shard_count(source_shard_count_)
    , source_shard_index(source_shard_index_)
    , target_shard_count(target_shard_count_)
    , side(side_)
    , selector(std::move(selector_))
    , sender(std::move(sender_))
{
    if (source_shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` sink must have at least one source shard");

    if (source_shard_index >= source_shard_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Source shard index {} is out of range for distributed `shuffle join` sink with {} source shards",
            source_shard_index,
            source_shard_count);
    }

    if (target_shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` sink must have at least one target shard");

    if (!selector)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` sink selector is empty");

    if (!sender)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` sink sender is empty");
}

void DistributedShuffleJoinSink::consume(Chunk & chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.getColumns());
    auto split_blocks = splitBlock(block);

    for (size_t target_shard_index = 0; target_shard_index < split_blocks.size(); ++target_shard_index)
    {
        if (!split_blocks[target_shard_index].rows())
            continue;

        sender->sendBlock(
            exchange_id,
            target_shard_index,
            source_shard_count,
            side,
            source_shard_index,
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
        sender->cancel(exchange_id, "Distributed `shuffle join` sink was cancelled");
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
            exchange_id,
            target_shard_index,
            source_shard_count,
            side,
            source_shard_index);
    }

    finished = true;
}

}
