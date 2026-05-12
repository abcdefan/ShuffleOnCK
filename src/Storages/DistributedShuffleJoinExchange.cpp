#include <Storages/DistributedShuffleJoinExchange.h>

#include <Common/Exception.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

String DistributedShuffleJoinExchangeId::toString() const
{
    return fmt::format("{}:{}", initial_query_id, join_id);
}

const char * toString(DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return "left";

    return "right";
}

DistributedShuffleJoinExchange::DistributedShuffleJoinExchange(
    DistributedShuffleJoinExchangeId id_,
    size_t source_shard_count_)
    : id(std::move(id_))
    , source_shard_count(source_shard_count_)
    , left_finished_sources(source_shard_count, false)
    , right_finished_sources(source_shard_count, false)
{
    if (id.initial_query_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange id cannot have empty initial query id");

    if (source_shard_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` exchange must have at least one source shard");
}

void DistributedShuffleJoinExchange::setHeader(DistributedShuffleJoinTableSide side, const Block & header)
{
    std::lock_guard lock(mutex);
    checkNotCancelledLocked();

    auto & data = getTableSideData(side);
    auto empty_header = header.cloneEmpty();

    if (data.has_header)
    {
        assertBlocksHaveEqualStructure(
            empty_header,
            data.header,
            fmt::format("Distributed `shuffle join` exchange {} {} side header", id.toString(), toString(side)));
        return;
    }

    data.header = std::move(empty_header);
    data.has_header = true;
}

Block DistributedShuffleJoinExchange::getHeader(DistributedShuffleJoinTableSide side) const
{
    std::lock_guard lock(mutex);
    checkNotCancelledLocked();

    const auto & data = getTableSideData(side);

    if (!data.has_header)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed `shuffle join` exchange {} does not have header for {} side",
            id.toString(),
            toString(side));
    }

    return data.header;
}

void DistributedShuffleJoinExchange::writeBlock(DistributedShuffleJoinTableSide side, size_t source_shard_index, Block block)
{
    std::lock_guard lock(mutex);
    checkSourceShardIndex(source_shard_index);
    checkNotCancelledLocked();

    if (getFinishedSources(side)[source_shard_index])
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot write {} side block from already finished source shard {} into distributed `shuffle join` exchange {}",
            toString(side),
            source_shard_index,
            id.toString());
    }

    block.checkNumberOfRows();

    auto & data = getTableSideData(side);
    auto block_header = block.cloneEmpty();

    if (data.has_header)
    {
        assertBlocksHaveEqualStructure(
            block_header,
            data.header,
            fmt::format("Distributed `shuffle join` exchange {} {} side block", id.toString(), toString(side)));
    }
    else
    {
        data.header = std::move(block_header);
        data.has_header = true;
    }

    const auto rows = block.rows();
    if (rows == 0)
        return;

    data.rows += rows;
    data.bytes += block.bytes();
    data.blocks.emplace_back(std::move(block));
}

std::vector<Block> DistributedShuffleJoinExchange::getBlocks(DistributedShuffleJoinTableSide side) const
{
    std::lock_guard lock(mutex);
    checkNotCancelledLocked();

    return getTableSideData(side).blocks;
}

DistributedShuffleJoinExchangeStats DistributedShuffleJoinExchange::getStats(DistributedShuffleJoinTableSide side) const
{
    std::lock_guard lock(mutex);
    checkNotCancelledLocked();

    const auto & data = getTableSideData(side);

    return DistributedShuffleJoinExchangeStats
    {
        .blocks = data.blocks.size(),
        .rows = data.rows,
        .bytes = data.bytes,
    };
}

void DistributedShuffleJoinExchange::markFinished(DistributedShuffleJoinTableSide side, size_t source_shard_index)
{
    std::lock_guard lock(mutex);
    checkSourceShardIndex(source_shard_index);

    checkNotCancelledLocked();

    auto & finished_sources = getFinishedSources(side);
    if (finished_sources[source_shard_index])
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Source shard {} has already finished sending {} side for distributed `shuffle join` exchange {}",
            source_shard_index,
            toString(side),
            id.toString());
    }

    finished_sources[source_shard_index] = true;
    ++getFinishedCount(side);

    if (isReadyLocked())
        ready_or_cancelled.notify_all();
}

bool DistributedShuffleJoinExchange::isSourceFinished(DistributedShuffleJoinTableSide side, size_t source_shard_index) const
{
    std::lock_guard lock(mutex);
    checkSourceShardIndex(source_shard_index);
    return getFinishedSources(side)[source_shard_index];
}

bool DistributedShuffleJoinExchange::isReady() const
{
    std::lock_guard lock(mutex);
    return isReadyLocked();
}

void DistributedShuffleJoinExchange::waitReady() const
{
    std::unique_lock lock(mutex);
    ready_or_cancelled.wait(lock, [this] { return cancelled || isReadyLocked(); });

    checkNotCancelledLocked();
}

bool DistributedShuffleJoinExchange::waitReadyFor(std::chrono::milliseconds timeout) const
{
    std::unique_lock lock(mutex);
    if (!ready_or_cancelled.wait_for(lock, timeout, [this] { return cancelled || isReadyLocked(); }))
        return false;

    checkNotCancelledLocked();
    return true;
}

void DistributedShuffleJoinExchange::cancel(String reason)
{
    std::lock_guard lock(mutex);
    if (cancelled)
        return;

    cancelled = true;
    cancel_reason = std::move(reason);
    clearDataLocked();
    ready_or_cancelled.notify_all();
}

bool DistributedShuffleJoinExchange::isCancelled() const
{
    std::lock_guard lock(mutex);
    return cancelled;
}

String DistributedShuffleJoinExchange::getCancelReason() const
{
    std::lock_guard lock(mutex);
    return cancel_reason;
}

void DistributedShuffleJoinExchange::clearData()
{
    std::lock_guard lock(mutex);
    clearDataLocked();
}

void DistributedShuffleJoinExchange::checkSourceShardIndex(size_t source_shard_index) const
{
    if (source_shard_index >= source_shard_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Source shard index {} is out of range for distributed `shuffle join` exchange {} with {} source shards",
            source_shard_index,
            id.toString(),
            source_shard_count);
    }
}

void DistributedShuffleJoinExchange::checkNotCancelledLocked() const
{
    if (!cancelled)
        return;

    if (cancel_reason.empty())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Distributed `shuffle join` exchange {} was cancelled", id.toString());

    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Distributed `shuffle join` exchange {} was cancelled: {}", id.toString(), cancel_reason);
}

bool DistributedShuffleJoinExchange::isReadyLocked() const
{
    return !cancelled && left_finished_count == source_shard_count && right_finished_count == source_shard_count;
}

void DistributedShuffleJoinExchange::clearDataLocked()
{
    left_data = TableSideData{};
    right_data = TableSideData{};
}

std::vector<bool> & DistributedShuffleJoinExchange::getFinishedSources(DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return left_finished_sources;

    return right_finished_sources;
}

const std::vector<bool> & DistributedShuffleJoinExchange::getFinishedSources(DistributedShuffleJoinTableSide side) const
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return left_finished_sources;

    return right_finished_sources;
}

size_t & DistributedShuffleJoinExchange::getFinishedCount(DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return left_finished_count;

    return right_finished_count;
}

DistributedShuffleJoinExchange::TableSideData & DistributedShuffleJoinExchange::getTableSideData(DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return left_data;

    return right_data;
}

const DistributedShuffleJoinExchange::TableSideData & DistributedShuffleJoinExchange::getTableSideData(DistributedShuffleJoinTableSide side) const
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return left_data;

    return right_data;
}

DistributedShuffleJoinExchangePtr DistributedShuffleJoinExchangeRegistry::getOrCreate(
    const DistributedShuffleJoinExchangeId & id,
    size_t source_shard_count)
{
    std::lock_guard lock(mutex);
    const auto key = id.toString();

    auto it = exchanges.find(key);
    if (it != exchanges.end())
    {
        if (it->second->getSourceShardCount() != source_shard_count)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Distributed `shuffle join` exchange {} already exists with {} source shards, requested {} source shards",
                key,
                it->second->getSourceShardCount(),
                source_shard_count);
        }

        return it->second;
    }

    auto exchange = std::make_shared<DistributedShuffleJoinExchange>(id, source_shard_count);
    exchanges.emplace(key, exchange);
    return exchange;
}

DistributedShuffleJoinExchangePtr DistributedShuffleJoinExchangeRegistry::find(const DistributedShuffleJoinExchangeId & id) const
{
    std::lock_guard lock(mutex);
    auto it = exchanges.find(id.toString());
    if (it == exchanges.end())
        return nullptr;

    return it->second;
}

bool DistributedShuffleJoinExchangeRegistry::remove(const DistributedShuffleJoinExchangeId & id)
{
    std::lock_guard lock(mutex);
    return exchanges.erase(id.toString()) != 0;
}

bool DistributedShuffleJoinExchangeRegistry::cancel(const DistributedShuffleJoinExchangeId & id, String reason) const
{
    auto exchange = find(id);
    if (!exchange)
        return false;

    exchange->cancel(std::move(reason));
    return true;
}

size_t DistributedShuffleJoinExchangeRegistry::size() const
{
    std::lock_guard lock(mutex);
    return exchanges.size();
}

DistributedShuffleJoinExchangeReceiver::DistributedShuffleJoinExchangeReceiver(DistributedShuffleJoinExchangeRegistry & registry_)
    : registry(registry_)
{
}

DistributedShuffleJoinExchangePtr DistributedShuffleJoinExchangeReceiver::prepareExchange(
    const DistributedShuffleJoinExchangeId & id,
    size_t source_shard_count,
    const Block & left_header,
    const Block & right_header)
{
    auto exchange = registry.getOrCreate(id, source_shard_count);
    exchange->setHeader(DistributedShuffleJoinTableSide::Left, left_header);
    exchange->setHeader(DistributedShuffleJoinTableSide::Right, right_header);
    return exchange;
}

void DistributedShuffleJoinExchangeReceiver::receiveBlock(
    const DistributedShuffleJoinExchangeId & id,
    size_t source_shard_count,
    DistributedShuffleJoinTableSide side,
    size_t source_shard_index,
    Block block)
{
    auto exchange = registry.getOrCreate(id, source_shard_count);
    exchange->writeBlock(side, source_shard_index, std::move(block));
}

void DistributedShuffleJoinExchangeReceiver::finishSource(
    const DistributedShuffleJoinExchangeId & id,
    size_t source_shard_count,
    DistributedShuffleJoinTableSide side,
    size_t source_shard_index)
{
    auto exchange = registry.getOrCreate(id, source_shard_count);
    exchange->markFinished(side, source_shard_index);
}

bool DistributedShuffleJoinExchangeReceiver::cancelExchange(const DistributedShuffleJoinExchangeId & id, String reason) const
{
    return registry.cancel(id, std::move(reason));
}

}
