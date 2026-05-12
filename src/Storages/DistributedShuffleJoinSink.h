#pragma once

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Columns/IColumn.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/DistributedShuffleJoinExchange.h>

#include <functional>
#include <memory>
#include <vector>

namespace DB
{

using DistributedShuffleJoinSelector = std::function<IColumn::Selector(const Block &)>;

class DistributedShuffleJoinBlockSender;
using DistributedShuffleJoinBlockSenderPtr = std::shared_ptr<DistributedShuffleJoinBlockSender>;

class DistributedShuffleJoinBlockSender
{
public:
    virtual ~DistributedShuffleJoinBlockSender() = default;

    virtual void sendBlock(
        const DistributedShuffleJoinExchangeId & id,
        size_t target_shard_index,
        size_t source_shard_count,
        DistributedShuffleJoinTableSide side,
        size_t source_shard_index,
        Block block) = 0;

    virtual void finish(
        const DistributedShuffleJoinExchangeId & id,
        size_t target_shard_index,
        size_t source_shard_count,
        DistributedShuffleJoinTableSide side,
        size_t source_shard_index) = 0;

    virtual void cancel(const DistributedShuffleJoinExchangeId & id, String reason) noexcept = 0;
};

class LocalDistributedShuffleJoinBlockSender final : public DistributedShuffleJoinBlockSender
{
public:
    explicit LocalDistributedShuffleJoinBlockSender(std::vector<DistributedShuffleJoinExchangeReceiver *> receivers_);

    void sendBlock(
        const DistributedShuffleJoinExchangeId & id,
        size_t target_shard_index,
        size_t source_shard_count,
        DistributedShuffleJoinTableSide side,
        size_t source_shard_index,
        Block block) override;

    void finish(
        const DistributedShuffleJoinExchangeId & id,
        size_t target_shard_index,
        size_t source_shard_count,
        DistributedShuffleJoinTableSide side,
        size_t source_shard_index) override;

    void cancel(const DistributedShuffleJoinExchangeId & id, String reason) noexcept override;

private:
    DistributedShuffleJoinExchangeReceiver & getReceiver(size_t target_shard_index) const;

    std::vector<DistributedShuffleJoinExchangeReceiver *> receivers;
};

class DistributedShuffleJoinSink final : public SinkToStorage
{
public:
    DistributedShuffleJoinSink(
        SharedHeader header,
        DistributedShuffleJoinExchangeId exchange_id_,
        size_t source_shard_count_,
        size_t source_shard_index_,
        size_t target_shard_count_,
        DistributedShuffleJoinTableSide side_,
        DistributedShuffleJoinSelector selector_,
        DistributedShuffleJoinBlockSenderPtr sender_);

    String getName() const override { return "DistributedShuffleJoinSink"; }

    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    void onCancel() noexcept override;

    Blocks splitBlock(const Block & block) const;
    void checkSelector(const IColumn::Selector & selector, size_t rows) const;
    void finishAllTargets();

    const DistributedShuffleJoinExchangeId exchange_id;
    const size_t source_shard_count;
    const size_t source_shard_index;
    const size_t target_shard_count;
    const DistributedShuffleJoinTableSide side;
    const DistributedShuffleJoinSelector selector;
    const DistributedShuffleJoinBlockSenderPtr sender;

    bool finished = false;
};

}
