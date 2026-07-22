# Distributed Shuffle Join Memory Risk

## 结论

当前 `distributed_shuffle_join` 在没有专用中间数据限制时，主要有两个内存风险点：

- `Exchange` 阶段会把左右两侧数据写入每个 target shard 的 `_shuffle_*` `Memory` 表。如果某个 target shard 对应的 bucket 很大，或者存在明显 key skew，这个阶段可能在 Local `JOIN` 开始前就消耗大量内存。
- 即使 target shard 能存放完整的 `_shuffle_*` 表，后续 Local `JOIN` 仍然需要构建本地 `JOIN` 执行所需的数据结构，也可能再次超过内存限制。

本文先记录 `Exchange` 阶段的风险和第一版预防措施。Local `JOIN` 阶段如何处理 `max_bytes_in_join`、`max_memory_usage` 以及后续更精确的保护策略，后面单独讨论。

## 当前流程中的问题

当前 push-based `shuffle join` 的执行流程是：

```text
Prepare
  -> 在所有 shard 创建 left/right 两张 `_shuffle_*` `Memory` 表

Exchange
  -> 每个 source shard 读取本地 left/right 表
  -> 按 `JOIN` key 计算 target shard
  -> 把 block 写入 target shard 的 `_shuffle_*` 表

Barrier
  -> 等所有 source shard 的 `Exchange` 完成

Local JOIN
  -> 每个 target shard 读取本地 `_shuffle_*` 表做本地 `JOIN`

Cleanup
  -> 删除 `_shuffle_*` 表
```

如果没有 `Exchange` 阶段的专用限制，某个 target shard 上 `_shuffle_*` 表能累计多少数据主要取决于输入数据规模和 hash 分布。现有 ClickHouse 的 `max_bytes_in_join` / `max_rows_in_join` 主要约束 Local `JOIN` 构建 hash table 的阶段，不能直接约束 `_shuffle_*` `Memory` 表在 `Exchange` 阶段的累计大小。

这意味着失败点可能出现在两个不同阶段：

- `Exchange` 期间：target shard 持续接收来自多个 source shard 的 block，`_shuffle_*` `Memory` 表先把内存吃满。
- Local `JOIN` 期间：`Exchange` 已经成功，`_shuffle_*` 表也能完整存下，但本地执行 `JOIN` 时又需要额外内存，最终由现有 `JOIN` 限制或 query memory tracker 抛出 exception。

第一版需要先让 `Exchange` 阶段从“可能在任意内存分配点失败”变成“按明确预算提前抛 exception”。

## 设计口径

`shard` 本身不是一个可配置的数据大小单位。集群配置只描述拓扑；它不会告诉 `distributed_shuffle_join` 某个 shard 最多应该承载多少本次查询的 shuffle 中间数据。

因此这里要加的是本次 query 的 `shuffle join` 中间数据预算，而不是配置 shard 大小。

第一版只做 bytes 保护：

```text
distributed_shuffle_join_max_bytes_per_shard
```

语义：

- 限制单个 target shard 上，本次 query 的 left/right 两张 `_shuffle_*` 表合计占用的最大 bytes。
- `0` 表示不启用专用限制，保持当前实验行为。
- 非 `0` 时，target shard 在写入 `_shuffle_*` 表前做检查；超过限制就抛 exception。
- 触发 exception 后，coordinator 取消剩余 `Exchange` 请求，并清理已经创建或写入的 `_shuffle_*` 表。

暂时不做 rows 限制。bytes 是当前防止爆内存的主保护手段；rows 限制可以后续根据测试需要再加。

## 检查位置

检查必须发生在 target shard 的写入路径上，而不是 source shard 拆分 block 后。

原因是一个 target shard 会同时接收多个 source shard 的数据。source shard 只知道自己要发送的 block 大小，不知道同一时间其他 source shard 已经向同一个 target shard 写入了多少数据。

目标检查逻辑是：

```text
left_current_bytes
+ right_current_bytes
+ incoming_block_bytes
<= distributed_shuffle_join_max_bytes_per_shard
```

无论当前写入的是 left 表还是 right 表，都按同一个 target shard 上两张 `_shuffle_*` 表的合计 bytes 判断。

如果超限，抛出类似下面语义的 exception：

```text
Distributed shuffle join exceeded max bytes per shard
```

## 并发要求

这个检查需要考虑并发写入。

多个 source shard 可能同时向同一个 target shard 的同一张或不同 `_shuffle_*` 表写入。如果实现只是先读取当前 `totalBytes`，再在没有同步保护的情况下写入 block，就可能出现多个写入同时检查通过，最终合计超过限制很多。

理想实现应该保证：

- target shard 上同一个 query 的 left/right `_shuffle_*` 表共享同一个 bytes quota。
- 检查当前合计 bytes 和提交当前 block 在受保护路径内完成。
- 超限时当前 block 不应被写入 `_shuffle_*` 表。

MVP 可以先实现 block 级 hard fail，不做更细粒度的行级或列级控制。

## 和现有 ClickHouse 限制的关系

现有 ClickHouse 设置仍然有用，但覆盖的是不同阶段：

- `max_memory_usage`：query 级通用 memory tracker 限制。
- `max_bytes_in_join`：Local `JOIN` hash table 的 bytes 限制。
- `max_rows_in_join`：Local `JOIN` hash table 的 rows 限制。
- `join_overflow_mode = 'throw'`：超过 `JOIN` 限制时抛 exception。

这些设置不能替代 `Exchange` 阶段的 `_shuffle_*` 表限制。`Exchange` 阶段的专用限制用于更早、更明确地拒绝过大的 shuffle bucket。

## TODO

- 新增 setting：`distributed_shuffle_join_max_bytes_per_shard`，默认 `0`。
- 在 target shard 写入 `_shuffle_*` 表前检查 left/right 两张 shuffle 表的合计 bytes。
- 超限时抛 exception，确保 coordinator 取消剩余 `Exchange` 并清理 `_shuffle_*` 表。
- 增加测试覆盖：
  - 单个 block 超过 `distributed_shuffle_join_max_bytes_per_shard`。
  - left 表未超限，right 表写入时因为合计 bytes 超限而失败。
  - 超限后 `_shuffle_*` 表被清理。
  - `distributed_shuffle_join_max_bytes_per_shard = 0` 时保持当前行为。

Local `JOIN` 阶段的内存保护单独设计，不在本文展开。
