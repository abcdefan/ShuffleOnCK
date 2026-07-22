# Distributed Shuffle Join Memory Budget

## 背景

当前 `distributed_shuffle_join` 在每个 target shard 上先物化两张 `_shuffle_*` `Memory` 表，然后再用这两张表执行本地 `JOIN`。因此单个 shard 的内存压力不是来自一个点，而是多个执行步骤叠加。

可以先把单个 target shard 的内存账本理解成：

```text
总可用内存
  - `_shuffle_left` `Memory` 表
  - `_shuffle_right` `Memory` 表
  - Local `JOIN` 构建 hash table 的额外内存
  - probe/output block
  - 其他 query / 后台任务 / server 自身开销
```

如果 `_shuffle_left` 和 `_shuffle_right` 已经占用了大量内存，即使还没有超过机器内存或 query memory tracker，留给后续 Local `JOIN` 的空间也会明显变小。Local `JOIN` 开始构建 hash table 时，仍然可能触发内存限制。

下面按执行步骤整理：每一步先说明内存来源和风险，再说明当前阶段对应的保护策略。

## 步骤一：`Exchange` 写入 `_shuffle_left`

### 内存来源和风险

`Exchange` 阶段会把每个 source shard 的左侧数据按 `JOIN` key 分桶，然后写入 target shard 的 `_shuffle_left` 表。

这部分内存基本对应左侧 bucket 的物化数据。它包含 `JOIN` key、输出列、单表过滤需要的列，以及 post-join filter 仍需要的列。列裁剪和单表 `WHERE` 下推越充分，这部分越小。

主要风险：

- 输入左表很大。
- hash 分布不均匀，某个 target shard 收到过多左侧行。
- 多个 source shard 同时向同一个 target shard 写入，累计速度很快。

### 当前保护策略

当前实现还没有针对 `_shuffle_left` 的专用保护。需要新增：

```text
distributed_shuffle_join_max_bytes_per_shard
```

在 target shard 写入 `_shuffle_left` 前检查：

```text
left_current_bytes
+ right_current_bytes
+ incoming_block_bytes
<= distributed_shuffle_join_max_bytes_per_shard
```

虽然当前写入的是 left 表，也要按 left/right 两张 `_shuffle_*` 表的合计 bytes 判断。原因是限制目标是“单个 target shard 上本次 query 的 shuffle 中间数据总量”，不是单张表大小。

超限时直接抛 exception，整个 `shuffle join` 失败，并触发 cleanup。

这个策略是当前阶段的 hard limit。它不会让超大数据自动跑完，只是把不可控的内存增长变成可控失败。

## 步骤二：`Exchange` 写入 `_shuffle_right`

### 内存来源和风险

`Exchange` 阶段同样会把右侧数据写入 target shard 的 `_shuffle_right` 表。

这部分内存对应右侧 bucket 的物化数据。对于常见 hash join，右侧 bucket 后续还会被 Local `JOIN` 用来构建 hash table，因此它不仅自己占内存，还会影响下一阶段的额外内存。

主要风险：

- 右表很大。
- 右侧 key skew 导致单个 bucket 过大。
- `ALL JOIN` 下重复 key 多，后续 hash table 结构也会更重。
- left 表已经占用大量内存，right 表继续写入时虽然单张表不大，但 left/right 合计已经接近内存上限。

### 当前保护策略

和 `_shuffle_left` 一样，使用同一个 setting：

```text
distributed_shuffle_join_max_bytes_per_shard
```

写入 `_shuffle_right` 前仍然检查：

```text
left_current_bytes
+ right_current_bytes
+ incoming_block_bytes
<= distributed_shuffle_join_max_bytes_per_shard
```

这能覆盖一个重要场景：left 表单独写入时没有超限，但 right 表写入后 left/right 合计超限。此时应该在 right block 写入前抛 exception，而不是等 Local `JOIN` 开始后再由通用内存限制兜底。

`distributed_shuffle_join_max_bytes_per_shard = 0` 表示不启用专用限制，保持当前实验行为。

## 步骤三：Local `JOIN` 构建 hash table

### 内存来源和风险

`Exchange` 成功后，每个 target shard 会执行类似下面的本地查询：

```sql
SELECT ...
FROM `_shuffle_left` AS `_shuffle_left`
... JOIN `_shuffle_right` AS `_shuffle_right`
ON ...
```

Local `JOIN` 不是直接在两张 `Memory` 表上原地比较。通常它需要读取一侧数据并构建 hash table，里面会包含 bucket、指针、行引用、重复 key 链表、nullable 标记等执行结构。

因此，即使 `_shuffle_right` 已经在 `Memory` 表里占了一份内存，Local `JOIN` 阶段仍然可能再消耗一份额外内存。这个额外开销可能比原始右侧数据更大，具体取决于 key 类型、列宽、重复 key 数量和 `JOIN` strictness。

主要风险：

- `_shuffle_right` bucket 本身很大。
- hash table 元数据和行引用带来额外放大。
- `ALL JOIN` 下重复 key 多，hash table 保存的匹配链更重。
- `_shuffle_left` + `_shuffle_right` 已经占用了很多内存，留给 hash table 的空间不足。

### 当前保护策略

这一步不需要我们重新实现一套 Local `JOIN` 专用限制。当前 Local `JOIN` 是生成普通本地 `JOIN` SQL，再走 ClickHouse 标准 `SELECT ... JOIN` 执行路径，因此会复用已有设置：

```text
max_bytes_in_join
max_rows_in_join
join_overflow_mode = 'throw'
```

这些设置的含义：

- `max_bytes_in_join`：限制 Local `JOIN` hash table 的 bytes。
- `max_rows_in_join`：限制 Local `JOIN` hash table 的 rows。
- `join_overflow_mode = 'throw'`：超限时抛 exception。

需要注意：

- `max_bytes_in_join = 0` 表示不限制。
- `max_rows_in_join = 0` 表示不限制。
- 因此测试和使用时要显式设置非零的 `max_bytes_in_join` 或 `max_rows_in_join`，否则 Local `JOIN` 专用限制不会触发。

如果任一 target shard 在 Local `JOIN` 阶段抛 exception，initiator 应该让整个 `shuffle join` 失败，并通过 coordinator 清理 `_shuffle_*` 表。

## 步骤四：probe 和 output block

### 内存来源和风险

Local `JOIN` probe 阶段会读取左侧 block，查 hash table，然后生成输出 block。

这部分通常是流式的，不会把完整结果一次性放进内存。但下面场景仍然可能放大内存压力：

- `ALL JOIN` 多对多匹配导致输出行数膨胀。
- 输出列很宽。
- 后续有 `ORDER BY`、全局排序、聚合等阻塞型算子。
- 下游消费慢，pipeline 中同时保留较多 block。

### 当前保护策略

probe/output 阶段主要依赖 ClickHouse 的通用 query memory tracker：

```text
max_memory_usage
```

`max_memory_usage` 不是在固定流程点检查一次，而是在执行过程中随着内存分配和记账持续检查。它可以覆盖 block 读取、表达式计算、输出 block、排序、聚合等没有被更专用限制覆盖的内存增长。

如果后续算子有自己的专用限制，也会继续按 ClickHouse 原有机制生效。例如排序或聚合相关限制不属于本文重点，但不会因为 `distributed_shuffle_join` 绕开普通 pipeline 保护。

## 步骤五：其他 server 内存

### 内存来源和风险

单个 shard 上不只有这一条 `shuffle join` query。还要给其他部分留空间：

- 其他并发 query。
- 后台 merge、mutation、fetch 等任务。
- server 自身数据结构和缓存。
- 网络收发、压缩解压、表达式计算中的临时内存。

因此不能把 `_shuffle_*` 表限制设置得接近机器总内存，也不能只按 `max_memory_usage` 的上限来估算 `_shuffle_*` 可用空间。

### 当前保护策略

这部分主要依赖 server 级配置、用户级限制和 query 级 `max_memory_usage` 共同兜底。对于 `distributed_shuffle_join` 自己，当前最实际的要求是：

```text
distributed_shuffle_join_max_bytes_per_shard
    < max_memory_usage
    < shard/server 可用内存
```

例如：

```sql
SET distributed_shuffle_join_max_bytes_per_shard = '4Gi';
SET max_memory_usage = '10Gi';
SET max_bytes_in_join = '4Gi';
SET join_overflow_mode = 'throw';
```

这里只是说明关系，不是通用推荐值。

含义是：

- `_shuffle_left` + `_shuffle_right` 最多占 `4 GiB`。
- 整个 query 在该 server 上最多占 `10 GiB`。
- Local `JOIN` hash table 也有自己的 `4 GiB` 上限。
- 超过任一阶段限制，query 抛 exception 并清理 `_shuffle_*` 表。

实际值需要根据机器内存、并发数、数据宽度、key 分布和 `JOIN` 类型调整。

## 真正的优化方向：spill 到磁盘

`distributed_shuffle_join_max_bytes_per_shard` 本质上只是保护措施。它让过大的 query 尽早失败，但不会让 query 在内存不足时继续完成。

真正的优化方向是 spill 到磁盘。这里也要区分两个层面。

### `Exchange` spill

目标是解决 `_shuffle_*` 表本身占用大量内存的问题。

可能方案：

- 不再使用纯 `Memory` 表，而是使用磁盘 backed 的临时 shuffle 表。
- 或者写临时 Native 格式文件，Local `JOIN` 阶段再读取。
- 或者先写内存，超过阈值后 flush 到磁盘。

这个方向可以减少 `Exchange` 阶段对内存的依赖，但它不自动解决 Local `JOIN` hash table 过大的问题。

### Local `JOIN` spill

目标是解决单个 target bucket 仍然太大，无法一次性构建 hash table 的问题。

典型思路是把本地 bucket 再按 `JOIN` key 切成多个 partition：

```text
partition 0:
  read right_0 -> build hash table
  read left_0  -> probe -> output
  free hash table

partition 1:
  read right_1 -> build hash table
  read left_1  -> probe -> output
  free hash table
```

这类方案才真正允许超大 bucket 分批执行。ClickHouse 现有的 external / grace hash join 能力可以作为后续参考，但接入 `distributed_shuffle_join` 需要单独设计。

## 当前阶段建议

第一版不做 spill，先做可控失败：

- 补 `distributed_shuffle_join_max_bytes_per_shard`，保护 `Exchange` 阶段。
- Local `JOIN` 继续复用 ClickHouse 的 `max_bytes_in_join`、`max_rows_in_join` 和 `join_overflow_mode`。
- 用 `max_memory_usage` 做整条 query 的通用兜底。
- 增加失败路径测试，确保 `Exchange` 超限、Local `JOIN` 超限和 query memory tracker 超限后都会清理 `_shuffle_*` 表。

后续如果要支持更大数据集，再分阶段设计 `Exchange` spill 和 Local `JOIN` spill。
