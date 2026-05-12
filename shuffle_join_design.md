# Shuffle Join 设计文档

## 一、背景

### 问题

ClickHouse 两个分布式表做 JOIN 时，现有方案（`distributed_product_mode='allow'`）的做法是：左表替换成本地表，右表保持分布式表，每个节点从所有节点拉取完整的右表数据做 JOIN。当右表数据量很大时，这会导致：
- 每个节点都存一份完整右表，内存开销大
- 全量右表要在网络上传输 N 次（N = 节点数），带宽压力大
- 节点数越多，重复传输越严重

### 之前的 Demo

做了一个 Shuffle Join 的 demo，思路是在 SQL 层加 WHERE 条件（`cityHash64(join_key) % N = shardNum()`），让每个节点只从其他节点拉取属于自己分桶的数据。验证了"按 join key hash 分桶后做本地 JOIN"的正确性。

但 demo 本质还是**拉模式**：每个节点向所有其他节点发查询，拉回数据后在本地过滤。这意味着数据在网络上还是走了一遍全量，只是接收端丢弃了不属于自己的行。网络传输没有真正减少。

### 目标

现在要做真正的 Shuffle Join：**从"拉"变成"推"**。每个节点读本地数据后，按 Join Key 的 Hash 值，只把对应的行推送到目标节点。这样网络上只传输必要的数据，没有浪费。

具体来说，一行数据只会被发到一个目标节点（由 `hash(join_key) % N` 决定），不会被广播。等所有节点完成数据分发后，每个节点手上就有了完整的一个分桶的左右表数据，可以做本地 JOIN。

## 二、整体流程

### 触发条件

- 两个分布式表做 JOIN
- `enable_shuffle_join = true`

### 执行流程（Initiator 协调）

```
步骤0：Prepare
  Initiator 向所有 shard 发送 CREATE TABLE
  等待所有 shard 确认 → shuffle 表就绪

步骤1：Exchange
  Initiator 向所有 shard 发送 Exchange 请求
  每个 shard：读本地数据 → hash分桶 → push到目标节点的 shuffle 表

步骤2：Barrier
  等待所有 shard 的 Exchange 返回

步骤3：Local JOIN
  Initiator 向所有 shard 发送 JOIN SQL
  每个 shard 从本地 shuffle 表做 JOIN，返回结果

步骤4：清理
  Initiator 向所有 shard 发送 DROP TABLE（异步）
```

示意图（3 节点）：

```
        节点0             节点1             节点2
        ┌───┐            ┌───┐            ┌───┐
        │读取│            │读取│            │读取│
        │本地│            │本地│            │本地│
        └─┬─┘            └─┬─┘            └─┬─┘
          │                │                │
       hash分桶         hash分桶         hash分桶
       ┌──┼──┐         ┌──┼──┐         ┌──┼──┐
       │  │  └─push──→ │  │  └─push──→ │  │  │
       │  └───push───→ │  │            │  │  │
       │          ←push─┘  │       ←push─┘  │  │
       ↓               ↓               ↓
  shuffle表L/R    shuffle表L/R    shuffle表L/R
       │               │               │
       ↓               ↓               ↓
  Local JOIN       Local JOIN       Local JOIN
       │               │               │
       └───────────────┼───────────────┘
                       ↓
                 Initiator 汇总
```

### 端到端示例

假设有 3 个节点，执行以下查询：

```sql
SELECT a.name, b.salary
FROM dist_users a JOIN dist_salaries b ON a.id = b.id
WHERE a.status = 'active'
```

左表 `users` 数据分布（本地表）：

| 节点 | id | name | status |
|------|----|------|--------|
| 节点0 | 1 | Alice | active |
| 节点0 | 4 | Dave | inactive |
| 节点1 | 2 | Bob | active |
| 节点1 | 5 | Eve | active |
| 节点2 | 3 | Carol | active |
| 节点2 | 6 | Frank | active |

右表 `salaries` 数据分布（本地表）：

| 节点 | id | salary |
|------|----|--------|
| 节点0 | 2 | 8000 |
| 节点0 | 3 | 9000 |
| 节点1 | 1 | 7000 |
| 节点1 | 6 | 6000 |
| 节点2 | 4 | 5000 |
| 节点2 | 5 | 10000 |

**Prepare**：Initiator 在所有节点创建 `_shuffle_{qid}_left(id, name)` 和 `_shuffle_{qid}_right(id, salary)` 表。

**Exchange（左表）**：各节点读本地 users，应用 WHERE `status = 'active'` 过滤，按 `cityHash64(id) % 3` 分桶后推送。假设 hash 结果是：id=1→节点1, id=2→节点2, id=3→节点0, id=5→节点2, id=6→节点0（id=4 被 WHERE 过滤掉了）：

- 节点0：Alice(id=1) 推到节点1
- 节点1：Bob(id=2) 推到节点2，Eve(id=5) 推到节点2
- 节点2：Carol(id=3) 推到节点0，Frank(id=6) 推到节点0

**Exchange（右表）**：各节点读本地 salaries，按同样 hash 分桶推送：

- 节点0：(id=2,8000) 推到节点2，(id=3,9000) 推到节点0（本地短路）
- 节点1：(id=1,7000) 推到节点1（本地短路），(id=6,6000) 推到节点0
- 节点2：(id=4,5000) 推到节点1，(id=5,10000) 推到节点2（本地短路）

**Barrier**：等所有节点 Exchange 完成。

**Local JOIN**：各节点用本地 shuffle 表做 JOIN：

- 节点0：左表有 Carol(3), Frank(6)；右表有 (3,9000), (6,6000) → 输出 Carol/9000, Frank/6000
- 节点1：左表有 Alice(1)；右表有 (1,7000), (4,5000) → 输出 Alice/7000（id=4 左表没有匹配）
- 节点2：左表有 Bob(2), Eve(5)；右表有 (2,8000), (5,10000) → 输出 Bob/8000, Eve/10000

**Initiator 汇总**：收集 3 个节点的结果，返回完整结果集。

**清理**：DROP 所有 shuffle 表。

## 三、数据分发方案

用 **RemoteSink + INSERT** 方案：每个节点通过 ClickHouse 原生协议 Connection 向目标节点执行 INSERT，写入 shuffle 表。

### 连接管理

每个 shard 在 Exchange 阶段需要向 N-1 个其他 shard 推送数据。连接策略：

- 每个 shard 启动 Exchange 时，对每个目标 shard 建立一个 Connection（通过 `ConnectionPool` 获取）
- 左表和右表分开处理（先 shuffle 左表，再 shuffle 右表，或两表并行），连接可复用
- 连接在整个 Exchange 阶段保持，所有 Block 发完后关闭（`RemoteInserter` 析构时会发送结束信号）

### 并发度

- **shard 间并行**：所有 shard 同时开始 Exchange（Initiator 同时发请求给所有 shard）
- **单 shard 内部**：对 N-1 个目标节点的发送可以并行（Pipeline 中 N 个 Sink 可并发执行）
- **左右表**：可以串行（先左后右）也可以并行，第一版先串行简化实现

### 为什么选 RemoteSink 而不是 InterserverIOEndpoint

`InterserverIOEndpoint` 用于副本间数据同步（如 `DataPartsExchange`），是 HTTP 接口，传输的是文件/part 粒度的数据。不太适合我们的场景，原因：
- 我们需要的是按行粒度 hash 后推送 Block，不是传文件
- RemoteSink 基于原生协议，天然支持 Block 级别传输和流式写入
- RemoteSink 已经是 Pipeline 里的标准 Sink 组件，可以直接嵌入 Pipeline

### 分桶逻辑详细设计

分桶的目标：给 Block 中每一行计算目标 shard，然后把 Block 拆成 N 个子 Block，分别发往对应 shard。

#### 分桶公式

```
target_shard_index = cityHash64(join_key_1, join_key_2, ...) % N
```

其中 N = shard 数量。对于等权重集群，直接取模即可。`cityHash64` 支持多参数，复合 Join Key 直接传入多列。

> 注意：`shardNum()` 从 1 开始（1~N），但内部 shard index 从 0 开始（0~N-1）。我们在内部计算统一用 0-based index，发送时通过 shard 地址数组映射到实际节点。

#### 计算流程（参考 `DistributedSink` 的实现）

```
输入：一个 Block（若干行数据）
输出：N 个子 Block，分别对应 N 个目标 shard

步骤1：计算 hash 列
  对 Block 执行表达式 cityHash64(join_key)，生成一列 UInt64 结果追加到 Block 中

步骤2：生成 selector 数组
  对 hash 列的每一行计算 hash_value % N，得到目标 shard index
  结果存入 IColumn::Selector（本质是 PaddedPODArray<UInt64>），长度等于行数
  selector[i] 表示第 i 行应该发往哪个 shard

步骤3：按 selector 拆分 Block
  对 Block 中的每一列，调用 column->scatter(N, selector)
  scatter 内部：先预统计每个 shard 会分到多少行（用于预分配内存），
  然后遍历所有行，根据 selector[i] 把第 i 行插入到对应的目标子列中
  最终得到 N 个子 Block，每个只包含属于该 shard 的行
```

整个过程只遍历数据两次（一次算 hash，一次做 scatter），内存预分配避免了扩容开销。

#### 我们的 ShufflePartitionTransform 与 DistributedSink 的区别

| | DistributedSink | 我们的 ShufflePartitionTransform |
|---|---|---|
| hash 表达式 | 用分布式表定义的 sharding_key | 用 JOIN 条件中的 join_key |
| 分桶公式 | `hash % total_weight → slot_to_shard` 映射（支持不等权重） | 简单 `cityHash64(join_key) % N`（等权重） |
| 输出形式 | `splitBlock` 返回 N 个 Block，后续写本地文件 | 作为 Processor，N 个输出端口分别连接 Sink |
| 目标 | 各 shard 的本地文件或远端节点 | 各 shard 的 shuffle 表 |

我们复用 `scatter` 的拆分逻辑，但 hash 表达式和分桶公式用自己的（基于 join key 而非 sharding key）。

### 参考代码

- `src/Storages/Distributed/DistributedSink.cpp` — `createSelector`（第660行）+ `splitBlock`（第671行）
- `src/Interpreters/createBlockSelector.cpp` — hash 值到 shard 的映射模板函数
- `src/Columns/IColumn.cpp` — `scatter` 实现（列级别拆分）
- `src/Processors/Sinks/RemoteSink.h` — 远端写入的 pipeline sink
- `src/QueryPipeline/RemoteInserter.h` — 通过 Connection 执行 INSERT 并发送 Block

## 四、Shuffle 表

### 为什么不用临时表

ClickHouse 的 `CREATE TEMPORARY TABLE` 是 session 级别的，只在创建它的那个 session（TCP 连接）内可见。当另一个 session 查询时，完全看不到该表。

我们的场景里，Exchange 阶段是 shard A 通过一个 Connection（session X）向 shard B INSERT 数据。后续 JOIN 阶段是 Initiator 通过另一个 Connection（session Y）向 shard B 发查询。session X 和 session Y 是两个独立 session，所以如果用临时表：
- session X 创建的临时表，session Y 看不到 → JOIN 阶段找不到表 → 查询失败

所以必须用**普通表**（server 级别可见，所有 session 都能访问）。

### 方案

用带 `query_id` 的普通 Memory 表，创建在分布式表所在的 database 里：

```sql
{database}._shuffle_{query_id}_left
{database}._shuffle_{query_id}_right
```

`query_id` 保证并发查询之间互不冲突。

### Schema 确定

Shuffle 表的列 = JOIN Key 列 + SELECT 中涉及该表的列（去重后）。

具体确定流程：
1. 从原始查询的 AST 中解析出 JOIN Key（ON 子句中的列）
2. 从 SELECT 和 WHERE 中收集涉及左/右表的所有列名
3. 合并去重，作为 shuffle 表的 schema
4. 列类型从原始本地表的 `StorageInMemoryMetadata` 中获取

示例：

```sql
-- 原始查询
SELECT a.name, a.age, b.salary
FROM dist_a a JOIN dist_b b ON a.id = b.id
WHERE a.status = 1

-- 左 shuffle 表的列：id（JOIN key）, name, age, status（WHERE 用）
-- 右 shuffle 表的列：id（JOIN key）, salary
```

### Memory 引擎的特点

- 数据纯内存存储，读写速度快
- 不持久化，server 重启数据丢失（但 shuffle 表本来就是临时用途）
- 无索引、无排序，适合全表扫描的 JOIN 场景
- 写入是 append，并发写入时有内部锁保护，不会数据错乱（但可能有锁竞争）

### 创建时序（防竞态）

Initiator **先统一向所有 shard 建表，等全部确认后再开始 Exchange**。

为什么要这样：Initiator 同时向所有 shard 发 Exchange 请求，但网络延迟不同。如果节点 A 先收到请求马上推数据给节点 B，而节点 B 还没建好表，INSERT 就会失败。分两步走（先建表 → 再 Exchange）解决这个问题。

### 清理

- **正常路径**：JOIN 完成后 Initiator 向所有 shard 发 DROP TABLE
- **异常路径**：Initiator 用 RAII scope guard，析构时发 DROP TABLE；后台 cleanup 线程定期扫描 `_shuffle_*` 前缀的表，超时（如 1 小时）直接 DROP，作为兜底

## 五、每个 Shard 的 Exchange Pipeline

### Pipeline 结构

```
Source: ReadFromLocal(本地表) [带 WHERE 下推 + 列裁剪，只读取需要的列]
  ↓
Transform: ExpressionTransform(计算 cityHash64(join_key) % N)
  ↓
Transform: ShufflePartitionTransform(按目标 shard 拆分 Block 到 N 个输出端口)
  ↓
Sink[self]:  直接写本地 shuffle 表（本地短路，不走网络）
Sink[其他]: SquashingTransform → RemoteSink(连接目标节点, INSERT INTO shuffle表)
```

左右表各执行一遍。

### Pipeline 怎么构建

Pipeline 的构建在 Exchange 请求的处理函数中完成，大致步骤：

1. **创建 Source**：用本地表的 `read` 方法创建 ReadFromMergeTree（或其他引擎）的 Source。需要传入列裁剪后的列列表和下推的 WHERE 条件。

2. **添加 ExpressionTransform**：在 Pipeline 中插入一个 Expression 步骤，用于计算 `cityHash64(join_key)` 表达式，将结果追加为新列 `_shuffle_hash`。

3. **添加 ShufflePartitionTransform**：这是一个 1-to-N 的 Processor。一个输入端口、N 个输出端口。它读取 `_shuffle_hash` 列，对每行计算 `hash_value % N`，然后用 `scatter` 拆分 Block 到 N 个输出端口。

4. **为每个输出端口连接 Sink**：
   - 如果目标是自己（`target_shard_index == my_shard_index`）：连接一个 `MemoryTableSink`，直接写本地 shuffle 表
   - 如果目标是其他节点：先连一个 `SquashingTransform`（合并小 Block），再连 `RemoteSink`（发往目标节点）

5. **Pipeline 类型**：这是一个 **push pipeline**（数据从 Source 流向 Sink）。使用 `QueryPipeline` 的 `setSinks` 方法设置多个 Sink。

### ShufflePartitionTransform 的 1-to-N 实现

ClickHouse 的 Processor 框架支持多输出端口（`OutputPort`），类似于 `CopyTransform`（1 输入 → N 输出复制同一份数据）。`ShufflePartitionTransform` 不同之处在于每个输出端口得到的是不同子集：

- 输入：一个 Block
- 处理：计算 selector → scatter 拆分成 N 个子 Block
- 输出：第 k 个输出端口输出 selector==k 的子 Block

当某个子 Block 为空（本批数据没有发往该 shard 的行）时，该输出端口不产出数据。

### 关于 addCompletedPipeline

原来考虑过用 `addCompletedPipeline` 把 Exchange 和 JOIN 合并到一个 Pipeline 里，但实际上不可行：JOIN 需要等**所有** shard 的 Exchange 都完成后才能开始（否则 shuffle 表数据不完整）。这个 Barrier 语义跨越了多个节点，无法在单个 Pipeline 内表达。所以拆成两个独立阶段是必要的。

## 六、多阶段协调的具体实现

Initiator 端的 `ShuffleExchangeCoordinator` 负责整个流程编排。所有阶段都通过向各 shard 发送 SQL 实现（复用现有的 `ClusterProxy::executeQuery` 或直接用 `Connection::sendQuery`）。

### 第零阶段：Prepare

Initiator 向所有 shard 发送建表 SQL：

```sql
CREATE TABLE IF NOT EXISTS {database}._shuffle_{query_id}_left (columns...) ENGINE = Memory;
CREATE TABLE IF NOT EXISTS {database}._shuffle_{query_id}_right (columns...) ENGINE = Memory;
```

**实现方式**：对每个 shard 获取一个 Connection，发送 CREATE TABLE 语句，等待返回。可以对所有 shard 并行发送。

等所有 shard 返回成功后进入下一阶段。如果某个 shard 失败则整个查询失败。

### 第一阶段：Exchange

Initiator 向所有 shard 发送 Exchange 请求。Exchange 请求的实现有两种思路：

**方案 A：发送特殊 SQL（推荐，第一版用这个）**

向每个 shard 发送一条 INSERT ... SELECT 风格的内部 SQL，由接收节点本地解析并执行 Exchange Pipeline：

```sql
-- 伪 SQL，实际可能是内部命令
SYSTEM SHUFFLE EXCHANGE
    query_id = '{query_id}'
    left_table = '{database}.{local_left_table}'
    right_table = '{database}.{local_right_table}'
    join_keys = 'id'
    left_columns = 'id, name, age, status'
    right_columns = 'id, salary'
    left_where = 'status = 1'
    right_where = ''
    num_shards = 3
    my_shard_index = {shard_index}
    shard_addresses = 'host1:9000,host2:9000,host3:9000'
```

每个 shard 收到这条命令后，在本地构建 Exchange Pipeline（参考第五节），执行完毕后返回给 Initiator。

**方案 B：通过 settings 携带参数，发送 INSERT SELECT**

更简单但更 hack 的方式，第一版也可以考虑：

```sql
INSERT INTO {database}._shuffle_{query_id}_left
SELECT id, name, age, status FROM {local_table} WHERE status = 1
SETTINGS _shuffle_mode = 1, _shuffle_num_shards = 3, _shuffle_my_index = 0, ...
```

通过自定义 settings 传递 shuffle 上下文，在 Pipeline 构建阶段识别并插入 ShufflePartitionTransform。

**实现方式**：使用 `Connection::sendQuery`，设置为不期望结果集（`Protocol::Server::EndOfStream` 表示完成）。所有 shard 并行发送，异步等待全部完成。

每个 shard 收到后执行：
1. 读取本地表数据（带谓词下推的 WHERE 条件）
2. 计算 `cityHash64(join_key) % N`，得到每行的目标 shard
3. 按目标 shard 拆分 Block
4. 通过 RemoteSink 发送到各目标节点的 shuffle 表（本地部分直接写）
5. 左右表各执行一遍
6. 全部完成后返回给 Initiator

### Barrier

Barrier 就是「等所有 shard 的 Exchange 返回」。不需要额外的同步机制。Initiator 对每个 shard 持有一个 Connection，当所有 Connection 都收到了 EndOfStream 响应时，说明所有 shard 的 Exchange 都完成了，shuffle 表数据完整。

### 第二阶段：Local JOIN

Initiator 等所有 shard Exchange 返回后，向所有 shard 发送 JOIN 查询：

```sql
SELECT {columns}
FROM {database}._shuffle_{query_id}_left AS a
JOIN {database}._shuffle_{query_id}_right AS b
ON a.{join_key} = b.{join_key}
{post_join_where}  -- 涉及两表的 WHERE 条件在这里应用
```

**实现方式**：用 `ClusterProxy::executeQuery` 向所有 shard 发送，拉取结果流。这跟普通的分布式查询一样——只不过查的是 shuffle 表而不是原始表。

每个 shard 数据已完整（Barrier 已过），直接做本地 JOIN 返回结果。Initiator 汇总各 shard 的结果返回给客户端。

### 第三阶段：清理

```sql
DROP TABLE IF EXISTS {database}._shuffle_{query_id}_left;
DROP TABLE IF EXISTS {database}._shuffle_{query_id}_right;
```

异步执行，不需要等待返回。即使清理失败也不影响查询结果的正确性。

### 超时处理

- Exchange 阶段设置超时（如 `shuffle_exchange_timeout_ms`），超过则查询失败并清理
- 如果某个 shard 无响应，Connection 层面的 socket timeout 会触发异常
- 异常触发后 scope guard 负责清理所有 shard 上的 shuffle 表

## 七、WHERE 条件下推

### 为什么需要下推

如果原始查询有 `WHERE a.status = 1`，不做下推的话，Exchange 阶段会把左表的**所有行**都读出来做 hash 分桶后发出去，然后在 JOIN 后再过滤。这意味着很多不满足条件的行白白走了网络。

下推就是在 Exchange 阶段读本地表时就把过滤条件加上，只读出满足条件的行，减少 shuffle 的数据量。

### 条件分类规则

原始查询的 WHERE 子句可能包含多种条件，需要根据条件涉及的表来分类：

| 条件类型 | 示例 | 处理方式 |
|----------|------|----------|
| 只涉及左表 | `a.status = 1` | 下推到左表的本地读取 |
| 只涉及右表 | `b.type = 'foo'` | 下推到右表的本地读取 |
| 涉及两个表 | `a.price > b.price` | **不能下推**，留到 JOIN 之后处理 |
| 涉及常量/函数 | `now() > '2024-01-01'` | 两边都可以下推（或由优化器决定） |

### 完整示例

```sql
-- 原始查询
SELECT a.name, b.salary
FROM dist_a a JOIN dist_b b ON a.id = b.id
WHERE a.status = 1 AND b.type = 'foo' AND a.age + b.score > 100
```

拆分后：

- **左表下推条件**：`status = 1`
- **右表下推条件**：`type = 'foo'`
- **post_join_where**：`a.age + b.score > 100`（涉及两表，留到 JOIN 后）

Exchange 阶段：
```sql
-- 节点读左表时
SELECT id, name, age FROM local_a WHERE status = 1
-- 注意 age 也要读出来，因为 post_join_where 用到了

-- 节点读右表时
SELECT id, salary, score FROM local_b WHERE type = 'foo'
-- score 也要读出来，因为 post_join_where 用到了
```

JOIN 阶段：
```sql
SELECT a.name, b.salary
FROM _shuffle_{query_id}_left AS a
JOIN _shuffle_{query_id}_right AS b ON a.id = b.id
WHERE a.age + b.score > 100  -- post_join_where 在这里执行
```

### 实现流程

1. 遍历 WHERE 子句中的所有 AND 连接的条件表达式
2. 对每个条件，收集它引用的所有列名
3. 根据列名的表归属判断该条件属于哪个表：
   - 所有列都属于左表 → 左表下推条件
   - 所有列都属于右表 → 右表下推条件
   - 混合使用两表的列 → post_join_where
4. 左表/右表下推条件分别传给 Exchange 阶段的本地读取
5. post_join_where 拼接到 JOIN 阶段的 SQL 中

### 注意事项

- ON 子句中的条件（JOIN 条件本身）不需要下推，它在 JOIN 时自然处理
- OR 连接的复合条件要整体判断，不能拆开：`a.x = 1 OR b.y = 2` 涉及两表，不能下推
- 如果 WHERE 里有子查询，暂时不下推，留到 JOIN 后处理

## 八、错误处理

### 整体原则

Shuffle Join 涉及多个节点、多个阶段，任何一步都可能失败。处理策略是：**一旦某个阶段出错，立即终止后续阶段，向客户端返回错误，同时清理已创建的 shuffle 表**。不做重试（第一版），失败就失败。

### 各阶段的错误场景

#### Prepare 阶段（建表）失败

- **可能原因**：某个 shard 宕机、网络不通、磁盘满了
- **处理流程**：
  1. Initiator 向所有 shard 发 CREATE TABLE
  2. 某个 shard 返回错误（或超时无响应）
  3. Initiator 停止等待其他 shard
  4. 向已成功建表的 shard 发 DROP TABLE（清理）
  5. 向客户端返回错误信息

#### Exchange 阶段（数据分发）失败

- **可能原因**：推送过程中目标节点挂了、网络中断、内存不足（Memory 表放不下）
- **处理流程**：
  1. 某个 shard 在执行 Exchange Pipeline 时遇到异常
  2. 该 shard 将错误返回给 Initiator
  3. Initiator 收到第一个错误后，向其他还在运行的 shard 发送 cancel 信号
  4. 等待所有 shard 停止后，统一清理 shuffle 表
  5. 向客户端返回错误信息

  注意：Exchange 阶段中，shard A 可能正在向 shard B 推数据，此时 shard A 自己出错了。那么 shard B 的 shuffle 表里可能已经有一部分 shard A 推过来的数据。这些"残留数据"不影响，因为整个查询已经失败，最后统一 DROP TABLE 时会清掉。

#### JOIN 阶段失败

- **可能原因**：内存不足（JOIN 的 hash table 太大）、被 KILL QUERY 杀掉
- **处理流程**：跟普通查询失败一样，Initiator 收到错误后返回给客户端。之后触发清理。

#### Initiator 自己挂了

- **问题**：Initiator 是协调者，如果它挂了，没人来发 DROP TABLE
- **处理**：每个 shard 上有后台 cleanup 线程，定期扫描本地的 `_shuffle_*` 前缀表。如果某张 shuffle 表创建时间超过阈值（如 1 小时）且没有被访问，直接 DROP。这是兜底机制。

### 清理的实现

`ShuffleExchangeCoordinator` 内部用 RAII 保证清理：

```cpp
class ShuffleExchangeCoordinator
{
    // 构造时记录 query_id 和所有 shard 的连接信息
    // 无论 execute() 是正常返回还是异常退出，析构时都会触发清理

    ~ShuffleExchangeCoordinator()
    {
        // 向所有 shard 异步发送 DROP TABLE
        // 不等待结果（best-effort），即使发送失败也不抛异常
        // 后台 cleanup 线程会兜底
        cleanupShuffleTables();
    }
};
```

### Cancel 信号传播

当用户执行 `KILL QUERY` 或客户端断开连接时：
1. Initiator 的查询上下文被标记为 cancelled
2. Initiator 检测到 cancel 后，向所有 shard 发送 cancel 信号（通过 Connection 的 cancel 机制）
3. 各 shard 停止 Exchange Pipeline 执行
4. Initiator 析构时清理 shuffle 表

## 九、需要新增的组件

| 组件 | 说明 |
|------|------|
| `ShufflePartitionTransform` | Processor，按 hash % N 将 Block 拆分路由到 N 个输出端口 |
| `ShuffleExchangeCoordinator` | Initiator 端，协调多阶段执行（Prepare → Exchange → JOIN → 清理） |
| `ShuffleTableCleanup` | 后台线程，定期清理残留的 shuffle 表 |

### 需要修改的文件

| 文件 | 改动 |
|------|------|
| `src/Core/Settings.cpp` | 新增 `enable_shuffle_join` 配置 |
| `src/Storages/StorageDistributed.cpp` | 在 `read` 方法中识别 Shuffle Join 场景 |
| `src/Interpreters/ClusterProxy/executeQuery.cpp` | 增加 Shuffle Join 多阶段路径 |
| `src/Storages/buildQueryTreeForShard.cpp` | 识别 Shuffle Join 场景时改变查询下发逻辑 |

### 关键依赖的现有代码

| 现有代码 | 用途 |
|----------|------|
| `DistributedSink::createSelector` | hash 分桶逻辑 |
| `RemoteSink` / `RemoteInserter` | 向远端节点发送数据 |
| `ClusterProxy::executeQuery` | 向各 shard 分发查询 |

## 十、性能分析：为什么比直接 JOIN 快

### 现有分布式表直接 JOIN 的开销

当 `distributed_product_mode='allow'` 时，执行 `SELECT * FROM dist_a JOIN dist_b ON ...`：

- 左表替换为本地表（不走网络），右表保持分布式表
- 每个节点都要从**所有节点**拉取完整的右表数据
- 网络传输量 = **N × 右表全量数据**（每个节点各拉一份完整右表）
- 每个节点内存中存一份**完整右表**用于构建 Hash Table

例子（左表 10GB、右表 10GB、3 节点，数据均匀分布）：
- 每个节点拉取右表全量 10GB → 集群总网络传输 = 3 × 10GB = 30GB
- 每个节点内存占用 = 10GB（完整右表的 Hash Table）

### Shuffle Join 的开销

- 左右表都按 hash 分桶后推送到目标节点
- 每行数据只发送到 1 个目标节点
- 每个节点本地数据中，约 1/N 留在本地（不走网络），(N-1)/N 需要发送出去
- 集群总网络传输 = (左表 + 右表) × (N-1)/N
- 每个节点内存占用 ≈ (左表 + 右表) / N

同样的例子（左表 10GB、右表 10GB、3 节点）：
- 总网络传输 = (10 + 10) × 2/3 ≈ 13.3GB
- 每个节点内存 ≈ (10 + 10) / 3 ≈ 6.6GB

### 对比

| | 直接 JOIN（广播右表） | Shuffle Join |
|---|---|---|
| 网络传输（3节点，左右表各10GB） | 30GB（每节点拉全量右表） | ~13.3GB（左右表各传 2/3） |
| 单节点内存 | 10GB（完整右表 Hash Table） | ~6.6GB（左右各 1/N） |
| 网络交互轮次 | 1 轮 | 3 轮（建表 + Exchange + JOIN） |
| 固定开销 | 低 | 较高（建表/清理/多轮协调） |
| 适合场景 | 右表小（广播成本低） | 两表都大（广播太贵） |

**核心优势**：当两个表都大时，Shuffle Join 的网络传输和内存占用都显著更低。虽然多了几轮交互的固定开销，但当数据量大到一定程度时，减少的数据传输量远超固定开销。

### Shuffle Join 必须满足的性能条件

Shuffle Join 引入了额外开销（多轮网络交互、临时表读写），要保证整体性能优于直接 JOIN，以下是**必要的**措施（不做就可能更慢）：

**1. 本地短路：目标 == 自己的数据不走网络**

每个节点 hash 分桶后，约 1/N 的数据目标就是自己。如果这部分也走 RemoteSink 发给自己，就白白浪费了网络+序列化开销。**必须**直接写本地 shuffle 表。

```
if (target_shard == my_shard)
    直接写本地 shuffle 表
else
    RemoteSink → 目标节点
```

**2. WHERE 条件下推：减少 shuffle 数据量**

如果原始查询有过滤条件（`WHERE a.status = 1`），不下推的话就要先 shuffle 全量数据再过滤，网络传输量白白变大。WHERE 下推在 shuffle 之前过滤掉不需要的行，是保证性能的基本要求。

**3. 列裁剪：只传必要的列**

如果原始表有 100 列但 JOIN 只用到 3 列，shuffle 时应该只传这 3 列（+ JOIN key），不传整行。否则网络传输量会膨胀几十倍。

```sql
-- 原始查询只用了 id, name, type
SELECT a.name, b.type FROM dist_a a JOIN dist_b b ON a.id = b.id

-- Exchange 阶段只读取需要的列
SELECT id, name FROM local_a  -- 而不是 SELECT *
SELECT id, type FROM local_b
```

**4. Block 批量发送：减少网络 round-trip**

不能每行数据单独发一次 INSERT，必须攒够一个 Block（比如 65536 行或到达一定字节数）后再发送。`RemoteSink` 本身就是按 Block 粒度发送的，符合要求，但 `ShufflePartitionTransform` 拆分后可能产生很小的 Block，需要在发送前做 squash（合并小 Block）。

### 什么时候不应该用 Shuffle Join

当右表很小（比如 < 100MB）时，直接 JOIN（广播右表）更快，因为：
- 广播一个小表开销很小
- Shuffle Join 的多轮交互 + 临时表创建/删除有固定开销

所以实际使用时应该有一个判断逻辑：如果能判断出某个表足够小，走广播（现有方案）；两个表都大时才走 Shuffle Join。第一版可以不做自动判断，靠 `enable_shuffle_join` 手动开关。

## 十一、跟 Demo 的区别

| | Demo（SQL 改写） | 真正的 Shuffle Join |
|---|---|---|
| 数据流向 | 每个节点从所有节点拉取 | 每个节点主动推送到目标节点 |
| 网络效率 | N×N 连接，每个连接查全量数据后过滤 | 只传需要的数据到目标节点 |
| 过滤时机 | 在接收端过滤 | 在发送端过滤（谓词下推） |
| 执行方式 | SQL 改写，利用现有分布式查询 | Exchange 算子 + 多阶段协调 |
| 中转存储 | 无 | shuffle 表缓存分桶后的数据 |

## 十二、实现计划

### 阶段一：基础框架（让数据能跑通）

1. **加配置开关**
   - 在 `src/Core/Settings.cpp` 中新增 `enable_shuffle_join`（Bool，默认 false）
   - 新增 `shuffle_exchange_timeout_ms`（UInt64，默认 300000，即 5 分钟）

2. **实现 ShufflePartitionTransform**
   - 位置：`src/Processors/Transforms/ShufflePartitionTransform.h/.cpp`
   - 继承 `IProcessor`，1 个 InputPort、N 个 OutputPort
   - 核心逻辑：接收 Block → 计算 hash 列 → 生成 selector → scatter 拆分 → 输出到各端口
   - 初始实现不做 squash，先跑通再优化

3. **实现 ShuffleExchangeCoordinator**
   - 位置：`src/Interpreters/ShuffleExchangeCoordinator.h/.cpp`
   - 职责：Prepare → Exchange → Barrier → JOIN → 清理的完整编排
   - 对外暴露一个 `execute(原始查询 AST, Cluster 信息)` 接口
   - 内部持有各 shard 的 ConnectionPool，管理各阶段的并发执行

4. **实现 Exchange 命令的处理**
   - 新增一个 SYSTEM 命令（`SYSTEM SHUFFLE EXCHANGE ...`）或者用 settings 方式携带参数的 INSERT SELECT
   - 在目标节点的处理函数中构建 Exchange Pipeline 并执行

5. **修改分布式查询入口**
   - 在 `src/Interpreters/InterpreterSelectQuery.cpp` 或 `src/Planner/Planner.cpp` 中
   - 判断 `enable_shuffle_join = true` 且两表都是分布式表时，走 ShuffleExchangeCoordinator 路径

### 阶段二：保证正确性

6. **Shuffle 表 Schema 提取**
   - 从 AST 中收集 JOIN key、SELECT 列、WHERE 涉及的列
   - 从本地表 metadata 获取列类型
   - 生成 CREATE TABLE 语句

7. **WHERE 条件下推**
   - 从原始 WHERE 中拆分出只涉及单表的条件
   - 传给 Exchange 阶段的本地读取

8. **列裁剪**
   - 确定每个表需要读取的最小列集合
   - 传给 Exchange 阶段的 Source

9. **清理机制**
   - Scope guard 在 Coordinator 析构时清理
   - 后台 cleanup 线程（可后续再加，第一版 scope guard 足够）

### 阶段三：验证和测试

10. **手动验证**
    - 手动分步执行各阶段 SQL，验证每步结果正确
    - 对比 Shuffle Join 和直接 JOIN 的查询结果，确保一致

11. **集成测试**
    - 在 `tests/queries/0_stateless/` 下新增测试
    - 覆盖：基本 JOIN、多列 JOIN Key、带 WHERE、NULL 值、空表等场景

### 各步骤之间的依赖关系

```
步骤1（配置）──→ 步骤5（入口修改）
步骤2（Transform）──→ 步骤4（Exchange 处理）──→ 步骤3（Coordinator）──→ 步骤5
步骤6（Schema）──→ 步骤3
步骤7（WHERE 下推）──→ 步骤4
步骤8（列裁剪）──→ 步骤4
步骤9（清理）──→ 步骤3
```

建议先实现步骤 1、2、4，再串起来实现 3 和 5，最后补 6、7、8、9 完善细节。
