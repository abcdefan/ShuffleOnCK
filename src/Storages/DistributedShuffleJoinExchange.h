#pragma once

#include <Core/Block.h>
#include <base/types.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace DB
{

struct DistributedShuffleJoinExchangeId
{
    String initial_query_id;
    size_t join_id = 0;

    String toString() const;

    bool operator==(const DistributedShuffleJoinExchangeId & rhs) const
    {
        return initial_query_id == rhs.initial_query_id && join_id == rhs.join_id;
    }
};

enum class DistributedShuffleJoinTableSide : uint8_t
{
    Left,
    Right,
};

const char * toString(DistributedShuffleJoinTableSide side);

struct DistributedShuffleJoinExchangeStats
{
    size_t blocks = 0;
    size_t rows = 0;
    size_t bytes = 0;
};

class DistributedShuffleJoinExchange;
using DistributedShuffleJoinExchangePtr = std::shared_ptr<DistributedShuffleJoinExchange>;

/// Query-local state for one target shard of one distributed `shuffle join`.
///
/// The same `exchange_id` is used on every target shard, but each target shard
/// stores only the buckets assigned to itself. This object tracks when all
/// source shards have finished sending both table sides to this target.
class DistributedShuffleJoinExchange final
{
public:
    DistributedShuffleJoinExchange(DistributedShuffleJoinExchangeId id_, size_t source_shard_count_);

    const DistributedShuffleJoinExchangeId & getId() const { return id; }
    size_t getSourceShardCount() const { return source_shard_count; }

    void setHeader(DistributedShuffleJoinTableSide side, const Block & header);
    Block getHeader(DistributedShuffleJoinTableSide side) const;

    void writeBlock(DistributedShuffleJoinTableSide side, size_t source_shard_index, Block block);
    std::vector<Block> getBlocks(DistributedShuffleJoinTableSide side) const;
    DistributedShuffleJoinExchangeStats getStats(DistributedShuffleJoinTableSide side) const;

    void markFinished(DistributedShuffleJoinTableSide side, size_t source_shard_index);
    bool isSourceFinished(DistributedShuffleJoinTableSide side, size_t source_shard_index) const;

    bool isReady() const;
    void waitReady() const;
    bool waitReadyFor(std::chrono::milliseconds timeout) const;

    void cancel(String reason);
    bool isCancelled() const;
    String getCancelReason() const;

    void clearData();

private:
    struct TableSideData
    {
        Block header;
        bool has_header = false;
        std::vector<Block> blocks;
        size_t rows = 0;
        size_t bytes = 0;
    };

    void checkSourceShardIndex(size_t source_shard_index) const;
    void checkNotCancelledLocked() const;
    bool isReadyLocked() const;
    void clearDataLocked();

    std::vector<bool> & getFinishedSources(DistributedShuffleJoinTableSide side);
    const std::vector<bool> & getFinishedSources(DistributedShuffleJoinTableSide side) const;

    size_t & getFinishedCount(DistributedShuffleJoinTableSide side);
    TableSideData & getTableSideData(DistributedShuffleJoinTableSide side);
    const TableSideData & getTableSideData(DistributedShuffleJoinTableSide side) const;

    const DistributedShuffleJoinExchangeId id;
    const size_t source_shard_count;

    TableSideData left_data;
    TableSideData right_data;

    std::vector<bool> left_finished_sources;
    std::vector<bool> right_finished_sources;
    size_t left_finished_count = 0;
    size_t right_finished_count = 0;

    bool cancelled = false;
    String cancel_reason;

    mutable std::mutex mutex;
    mutable std::condition_variable ready_or_cancelled;
};

class DistributedShuffleJoinExchangeRegistry final
{
public:
    DistributedShuffleJoinExchangePtr getOrCreate(const DistributedShuffleJoinExchangeId & id, size_t source_shard_count);
    DistributedShuffleJoinExchangePtr find(const DistributedShuffleJoinExchangeId & id) const;

    bool remove(const DistributedShuffleJoinExchangeId & id);
    bool cancel(const DistributedShuffleJoinExchangeId & id, String reason) const;

    size_t size() const;

private:
    mutable std::mutex mutex;
    std::unordered_map<String, DistributedShuffleJoinExchangePtr> exchanges;
};

/// Local receiver facade for shuffle exchange messages.
///
/// Network/protocol code should call this class after decoding an incoming
/// exchange message instead of touching the registry directly.
class DistributedShuffleJoinExchangeReceiver final
{
public:
    explicit DistributedShuffleJoinExchangeReceiver(DistributedShuffleJoinExchangeRegistry & registry_);

    DistributedShuffleJoinExchangePtr prepareExchange(
        const DistributedShuffleJoinExchangeId & id,
        size_t source_shard_count,
        const Block & left_header,
        const Block & right_header);

    void receiveBlock(
        const DistributedShuffleJoinExchangeId & id,
        size_t source_shard_count,
        DistributedShuffleJoinTableSide side,
        size_t source_shard_index,
        Block block);

    void finishSource(
        const DistributedShuffleJoinExchangeId & id,
        size_t source_shard_count,
        DistributedShuffleJoinTableSide side,
        size_t source_shard_index);

    bool cancelExchange(const DistributedShuffleJoinExchangeId & id, String reason) const;

private:
    DistributedShuffleJoinExchangeRegistry & registry;
};

}
