#pragma once

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Columns/IColumn.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/DistributedShuffleJoinTables.h>

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
        const DistributedShuffleJoinTableNames & table_names,
        size_t target_shard_index,
        DistributedShuffleJoinTableSide side,
        Block block) = 0;

    virtual void finish(
        const DistributedShuffleJoinTableNames & table_names,
        size_t target_shard_index,
        DistributedShuffleJoinTableSide side) = 0;

    virtual void cancel(const DistributedShuffleJoinTableNames & table_names, String reason) noexcept = 0;
};

class DistributedShuffleJoinSink final : public SinkToStorage
{
public:
    DistributedShuffleJoinSink(
        SharedHeader header,
        DistributedShuffleJoinTableNames table_names_,
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

    const DistributedShuffleJoinTableNames table_names;
    const size_t target_shard_count;
    const DistributedShuffleJoinTableSide side;
    const DistributedShuffleJoinSelector selector;
    const DistributedShuffleJoinBlockSenderPtr sender;

    bool finished = false;
};

}
