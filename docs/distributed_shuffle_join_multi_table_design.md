# Distributed Shuffle Join Multi-Table Design Notes

## 背景

当前 `exchange` 分支的 `distributed_shuffle_join` MVP 只支持一次二元分布式 `JOIN`：

```sql
SELECT ...
FROM a_dist AS a
INNER ALL JOIN b_dist AS b ON a.user_id = b.user_id
SETTINGS distributed_shuffle_join = 1
```

现有实现里的核心数据结构也是二元的：

- `DistributedShuffleJoinInfo` 只有 `left_storage` / `right_storage`。
- `_shuffle_*` 中间表只有 `left` / `right` 两侧。
- `DistributedShuffleJoinExchangePayload` 只有 `left_source` / `right_source`。
- `createDistributedShuffleJoinLocalJoinQuery` 只生成两表本地 `JOIN` SQL。
- `tryAnalyzeDistributedShuffleJoin` 要求 `QueryNode::getJoinTree` 直接是一个 `JoinNode`，且左右两边都是直接 `TableNode`。

因此，多分布式表查询目前会回退普通 planner，例如：

```sql
SELECT ...
FROM a_dist AS a
INNER JOIN b_dist AS b ON a.user_id = b.user_id
INNER JOIN c_dist AS c ON a.user_id = c.user_id
SETTINGS distributed_shuffle_join = 1
```

要让它走 `shuffle join`，需要先明确多表 `JOIN` 的分桶模型。主要有两类情况：同一 join key 的一次性 multi-way shuffle，以及不同 join key 的多阶段 shuffle。

## 情况一：同一 join key 的一次性 multi-way shuffle

### 示例

```sql
SELECT
    u.user_id,
    u.name,
    o.order_id,
    p.payment_id
FROM users_dist AS u
INNER JOIN orders_dist AS o ON u.user_id = o.user_id
INNER JOIN payments_dist AS p ON u.user_id = p.user_id
SETTINGS distributed_shuffle_join = 1
```

这个查询里三张表的 join key 实际是同一个等价类：

```text
u.user_id = o.user_id = p.user_id
```

因此可以一次性把三张表都按自己的 `user_id` 分桶：

```text
target_shard = hash(user_id) % shard_count
```

每个 source shard 执行：

```text
read users_local
  -> partition by users.user_id
  -> write target shard _shuffle_<query>_<join>_users

read orders_local
  -> partition by orders.user_id
  -> write target shard _shuffle_<query>_<join>_orders

read payments_local
  -> partition by payments.user_id
  -> write target shard _shuffle_<query>_<join>_payments
```

每个 target shard 上都有多张 `Memory` 中间表，但每张表只保存该 shard 负责的 key bucket：

```text
_shuffle_*_users
_shuffle_*_orders
_shuffle_*_payments
```

Exchange barrier 之后，target shard 本地执行多表 `JOIN`：

```sql
SELECT ...
FROM _shuffle_users AS u
INNER JOIN _shuffle_orders AS o ON u.user_id = o.user_id
INNER JOIN _shuffle_payments AS p ON u.user_id = p.user_id
```

最后 initiator 汇总所有 target shard 的结果，并继续执行全局 `ORDER BY` / `LIMIT` 等后处理。

### 关键难点：识别同一个 key 等价类

一次性 multi-way shuffle 的关键不是“让 `c` 发向 `a`”，而是识别出所有表都能按同一个等价 key 分桶。

analyzer 需要从 join tree 中提取等值条件，并构造 column equivalence class。例如：

```sql
a JOIN b ON a.user_id = b.user_id
JOIN c ON b.user_id = c.uid
```

需要推导出：

```text
a.user_id = b.user_id = c.uid
```

这说明 `a`、`b`、`c` 可以一次性 shuffle，只是每张表使用自己的 key 列：

```text
a -> hash(a.user_id)
b -> hash(b.user_id)
c -> hash(c.uid)
```

第一版可以只支持非常保守的等价类：

- join tree 是左深树。
- 所有输入都是直接 `StorageDistributed`。
- 所有 join condition 都是直接列等值。
- 每张表必须恰好有一个列落在同一个等价类中。
- 所有表属于同一个 cluster，且 shard count 一致。
- 每个 shard 只使用一个 replica。
- 先只支持 `INNER ALL JOIN` 和 `INNER ANY JOIN`，outer / semi / anti 后续再逐步放开。

### 设计改动

当前二元结构可以扩展成 N-way 结构。

现有结构：

```text
left table
right table
```

目标结构：

```text
input[0]
input[1]
input[2]
...
input[n]
```

建议新增：

```text
DistributedShuffleJoinInput
  table_expression
  storage
  alias
  source_database
  source_table
  shuffle_key_expression
  shuffle_key_column_name
  required_columns
  filter_condition
  shuffle_table_name
```

然后把 `DistributedShuffleJoinInfo` 改造成：

```text
DistributedShuffleJoinInfo
  inputs: vector<DistributedShuffleJoinInput>
  joins: vector<DistributedShuffleJoinOperator>
  projection_columns
  post_join_filter_condition
  order_by
  limit
  cluster_name
  shard_count
```

`DistributedShuffleJoinOperator` 记录本地 join SQL 的结构：

```text
left_input_or_intermediate
right_input
join_kind
join_strictness
left_key
right_key
```

对于同 key multi-way shuffle，`Prepare` 阶段创建 N 张 `_shuffle_*` 表；`Exchange` 阶段每个 source shard 读取 N 个 local source；`Local JOIN` 阶段生成一条 N 表本地 `JOIN` SQL。

### 优点

- 数据只 shuffle 一次。
- 没有中间 join result 再次落表的成本。
- 对星型同 key 查询很合适，例如多张事实/维度表都按 `user_id` 关联。

### 局限

它只能处理所有表共享同一个分桶 key 的情况。下面这种不适合一次性 multi-way shuffle：

```sql
SELECT ...
FROM users_dist AS u
INNER JOIN orders_dist AS o ON u.user_id = o.user_id
INNER JOIN products_dist AS p ON o.product_id = p.product_id
```

这里有两个等价类：

```text
u.user_id = o.user_id
o.product_id = p.product_id
```

`orders_dist` 同时参与两个不同 key。一次 shuffle 不能同时让 `orders_dist` 按 `user_id` 和 `product_id` 分布。

## 情况二：不同 join key 的多阶段 shuffle

### 示例

```sql
SELECT
    u.user_id,
    o.order_id,
    p.product_name
FROM users_dist AS u
INNER JOIN orders_dist AS o ON u.user_id = o.user_id
INNER JOIN products_dist AS p ON o.product_id = p.product_id
SETTINGS distributed_shuffle_join = 1
```

这个查询不能一次性按一个 key shuffle，因为第一段 join 需要 `user_id`，第二段 join 需要 `product_id`。

更自然的执行方式是多阶段：

```text
stage 0:
  shuffle users_dist by user_id
  shuffle orders_dist by user_id
  local join -> intermediate users_orders

stage 1:
  shuffle users_orders by product_id
  shuffle products_dist by product_id
  local join -> final result
```

### 设计方案

把一次二元 `shuffle join` 抽象成一个 `ShuffleJoinStage`：

```text
ShuffleJoinStage
  stage_id
  left_input
  right_input
  left_key
  right_key
  join_kind
  join_strictness
  left_shuffle_table
  right_shuffle_table
  output_table
  output_columns
```

其中 `left_input` / `right_input` 可以是：

```text
original distributed table
previous stage output table
```

执行流程变成：

```text
for each stage:
  Prepare:
    create left/right shuffle input tables
    if this stage needs materialized output, create output Memory table

  Exchange:
    read left input and partition by left_key
    read right input and partition by right_key
    write to target shard left/right shuffle tables

  Barrier:
    wait all source shards complete

  Local JOIN:
    if this is not final stage:
      INSERT INTO stage output table
      SELECT ...
      FROM stage left shuffle table
      JOIN stage right shuffle table
    else:
      return result pipeline to initiator

  Cleanup:
    drop stage input shuffle tables
    keep output table until next stage completes
```

### 中间结果表

多阶段方案需要新增“stage output table”。它和当前 left/right `_shuffle_*` 表不一样：

- left/right `_shuffle_*` 表是某一阶段的输入 bucket。
- output table 是某一阶段 `JOIN` 后的中间结果，可能要作为下一阶段的 source。

例如：

```text
_shuffle_q_stage0_left
_shuffle_q_stage0_right
_shuffle_q_stage0_output

_shuffle_q_stage1_left
_shuffle_q_stage1_right
```

`stage1_left` 可以来自 `stage0_output`，但如果 `stage0_output` 已经按 `stage1` 的 key 分布，也可以短路；否则需要重新 shuffle。

### 难点

1. 中间结果 schema

   每个 stage 的 output 需要包含后续 stage join key、最终 projection、后续 filter / order by 所需列。列裁剪会比二表版本复杂。

2. 列名和 alias 管理

   多表场景里不同表可能都有 `id`、`name`。中间结果必须生成稳定且不冲突的内部列名，同时最终输出要恢复用户可见 alias。

3. join tree 和 stage 规划

   第一版可以只支持左深树，例如：

   ```text
   ((a JOIN b) JOIN c) JOIN d
   ```

   bushy tree、join reorder 和 cost-based planning 暂时不做。

4. 重新分桶判断

   如果上一阶段 output 已经按下一阶段 key 分布，可以避免一次 shuffle；否则必须重新 Exchange。这个判断需要记录每个 intermediate 当前的 partition key。

5. cleanup 生命周期

   多阶段会产生更多中间表。任一 stage 出 exception 时，要清理：

   - 当前 stage 的 left/right shuffle input 表。
   - 已经创建但不再需要的 previous stage output 表。
   - 已经创建的后续 stage 表。

6. 错误传播和取消

   当前二元版本已经有 cancellation 和 cleanup 基础。多阶段需要保证 stage 之间的 output table 生命周期与 query pipeline 生命周期一致。

### 优点

- 可以支持不同 join key 的链式 join。
- 更接近生产查询形态。
- 可以复用当前二元 `Prepare -> Exchange -> Barrier -> Local JOIN -> Cleanup` 框架，把它提升成 stage primitive。

### 缺点

- 可能 shuffle 多次。
- 需要 materialize intermediate result。
- 中间结果可能比原始表更大，必须有内存上限、spill 或 fallback。
- cost model 更重要，否则错误选择 shuffle stage 可能比现有路径更慢。

## 推荐演进顺序

如果目标是尽快走向通用多分布式表 `JOIN`，推荐先实现左深树多阶段二元 shuffle，而不是先实现同 key multi-way shuffle。multi-way shuffle 更像性能优化路径；多阶段 shuffle 才是覆盖不同 join key 和普通生产查询形态的主线。

### 第一阶段：左深树多阶段二元 shuffle

目标查询：

```sql
SELECT ...
FROM a_dist AS a
INNER ALL JOIN b_dist AS b ON a.k1 = b.k1
INNER ALL JOIN c_dist AS c ON b.k2 = c.k2
SETTINGS distributed_shuffle_join = 1
```

限制：

- 只支持左深 join tree，按用户 SQL 的原始 join 顺序执行。
- 只支持 `INNER ALL JOIN` / `INNER ANY JOIN`。
- 所有原始输入都是直接 `StorageDistributed`。
- 每个 stage 都可以重新 shuffle，先不做 partitioning property 复用。
- stage output 使用普通 `Memory` 表。
- 不支持 aggregation、subquery、outer join、semi / anti join。

这一步目标是先让多个分布式表的 `shuffle join` 正确跑通：

```text
stage 0:
  a shuffle join b -> ab

stage 1:
  ab shuffle join c -> abc
```

这一步要优先解决 stage output 表、intermediate schema、内部列名、跨 stage cleanup 和取消传播。

### 第二阶段：N 表左深树多阶段 shuffle

把第一阶段从 3 表泛化到 N 表：

```text
(((a JOIN b) JOIN c) JOIN d) ...
```

仍然按 SQL 原始顺序执行，不做 join reorder。核心是把 `ShuffleJoinStage`、stage output schema 和 cleanup 生命周期稳定下来。

### 第三阶段：partitioning property 传递和 reshuffle 规避

记录每个 stage output 当前按哪个 key 分布。如果下一阶段需要同一个 key，可以避免对上一阶段 output 重新 shuffle。

```sql
a JOIN b ON a.user_id = b.user_id
JOIN c ON a.user_id = c.user_id
```

即使第一版仍按二元 stage 执行，也可以变成：

```text
stage 0:
  shuffle a and b by user_id -> ab partitioned by user_id

stage 1:
  keep ab partitioning
  shuffle c by user_id
  local join ab and c
```

这一步不改变语义，只减少不必要的网络传输。

### 第四阶段：cost model 和 join reorder

在多阶段路径正确后，再引入 cost model，选择更便宜的 join 顺序：

```text
(a JOIN b) JOIN c
(a JOIN c) JOIN b
(b JOIN c) JOIN a
```

cost model 至少需要考虑：

- 输入表大小。
- filter 后行数。
- join key cardinality。
- join 选择率。
- 输出列宽度。
- 中间结果大小。
- 是否有小表适合 broadcast。

第一版没有统计信息时可以继续使用 SQL 原始顺序。

### 第五阶段：同 key multi-way shuffle 优化

当 analyzer 能识别同一个 key equivalence class 时，可以把多个二元 stage 优化成一次 N-way shuffle：

```sql
SELECT ...
FROM a_dist AS a
INNER ALL JOIN b_dist AS b ON a.k = b.k
INNER ALL JOIN c_dist AS c ON a.k = c.k
SETTINGS distributed_shuffle_join = 1
```

优化后：

```text
a / b / c all shuffle by k
target shard local multi-way join
```

这一步是性能优化，不是多表正确性的前置条件。

### 第六阶段：outer / semi / anti

在多表路径稳定后，再把当前二元路径已经支持的 `LEFT` / `RIGHT` / `FULL` / `SEMI` / `ANTI` 逐步放开到多表。这个阶段必须和 cost model 一起做，避免所有可识别查询都强行进入 shuffle。

## 结论

多分布式表 `JOIN` 不能简单理解成“多建几张 `Memory` 表”。

通用多分布式表 `JOIN` 应先按左深树多阶段二元 shuffle 实现：一个 stage 做一个当前已有的二元 shuffle join，非最终 stage 把结果 materialize 成中间 `Memory` 表，再作为下一 stage 的输入。第一版按 SQL 原始顺序执行，不做 join reorder。

后续优化分两条线：

- cost model / join reorder：估算不同 join 顺序的网络传输、中间结果和内存代价，选择更便宜的 stage 顺序。
- 同 key multi-way shuffle：识别 `a.k = b.k = c.k` 这类 key equivalence class，将多个同 key stage 合并为一次 N-way shuffle，避免中间结果落表和二次 shuffle。

因此，当前框架可以作为基础继续演进，但从二表 MVP 到生产可用的多分布式表 `JOIN`，主线工作应集中在 stage planning、stage output 表、中间结果 schema、跨 stage cleanup、partitioning property 传递和 cost model 上；key 等价类和 N-way source 管理可以作为后续优化能力逐步加入。
