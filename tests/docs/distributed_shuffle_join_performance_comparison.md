# Distributed Shuffle Join Performance Comparison

## 结论

本次在右表明显大于左表的三 shard 本地集群中，对比了 ClickHouse 的 `GLOBAL INNER ALL JOIN`、`distributed_product_mode = 'allow'`、当前 `distributed_shuffle_join` 实现，以及旧 `shuffle` 分支 SQL 分桶方案的等价复现。

以三次测量的中位数为准，当前 push-based `distributed_shuffle_join` 最快：

| 方案 | 中位耗时 | 相对当前 `distributed_shuffle_join` | 当前 `distributed_shuffle_join` 的优势 |
| --- | ---: | ---: | ---: |
| 当前 `distributed_shuffle_join` | 1.270 s | 1.000x | - |
| 旧 SQL 分桶方案复现 | 1.336 s | 1.052x | 快 5.0% |
| `GLOBAL INNER ALL JOIN` | 2.073 s | 1.633x | 快 38.7% |
| `distributed_product_mode = 'allow'` | 2.229 s | 1.755x | 快 43.0% |

这说明在本测试数据分布下，当前 Exchange 方案已经能够避免大右表在普通分布式 `JOIN` 路径中的重复传输代价，并且比仅通过 SQL 增加 bucket filter 的旧方案略快。

## 测试环境

| 项目 | 值 |
| --- | --- |
| 分支/实现 | 当前 `exchange` 工作树，包含未提交的 `distributed_shuffle_join` 实现 |
| ClickHouse 二进制 | `26.4.1.1` |
| 构建类型 | `Debug`，`SANITIZE = OFF` |
| 集群布局 | 本地 Docker，3 shards，每 shard 1 replica |
| CPU | Intel Core i5-14600KF，20 logical CPUs |
| 内存 | 15 GiB |
| 公共 query settings | `enable_analyzer = 1`, `max_threads = 4`, `use_query_cache = 0`, `join_algorithm = 'hash'` |

绝对耗时只能代表该本地 `Debug` 构建环境，不能直接视为 release 构建或真实多机网络的容量结论。四种路径使用同一个二进制、同一份数据和相同公共 settings，因此相对差距可用于当前 MVP 方向判断。

## 数据集

测试在每个节点创建 `left_local`、`right_local` 以及对应的 `Distributed` 表 `left_dist`、`right_dist`。

| 表侧 | 行数 | 列 | 说明 |
| --- | ---: | --- | --- |
| 左表 | 30,000 | `id`, `a_payload` | 全部 key 可在右表命中 |
| 右表 | 6,000,000 | `id`, `b_payload_1`, `b_payload_2`, `b_payload_3` | 三列宽字符串 payload，使右表成为主要传输/构建开销 |

左右表的物理落位分别使用带不同 seed 的 `cityHash64`，刻意不按 join bucket 预分布。因此，查询必须处理跨 shard 的匹配关系，而不能仅靠本地已有数据完成结果。

测量查询输出 `a.id`、左侧 payload 和三列右侧 payload，最终使用 `FORMAT Null` 消除客户端打印结果的干扰。四种方案均先校验返回的 `id` 集合，结果均为相同的 30,000 行。

## 对比方案

### `GLOBAL INNER ALL JOIN`

```sql
SELECT a.id, a.a_payload, b.b_payload_1, b.b_payload_2, b.b_payload_3
FROM default.left_dist AS a
GLOBAL INNER ALL JOIN default.right_dist AS b USING (id)
SETTINGS enable_analyzer = 1, max_threads = 4, use_query_cache = 0, join_algorithm = 'hash'
FORMAT Null
```

该路径需要将右侧数据提供给参与左侧处理的节点。在右表较大时，广播/复制成本明显。

### `distributed_product_mode = 'allow'`

```sql
SELECT a.id, a.a_payload, b.b_payload_1, b.b_payload_2, b.b_payload_3
FROM default.left_dist AS a
INNER ALL JOIN default.right_dist AS b USING (id)
SETTINGS enable_analyzer = 1, max_threads = 4, use_query_cache = 0, join_algorithm = 'hash',
         distributed_product_mode = 'allow', prefer_global_in_and_join = 0
FORMAT Null
```

该路径允许 distributed-over-distributed 的普通执行，每个执行 shard 会读取右侧分布式输入，产生大右表的重复工作。

### 当前 `distributed_shuffle_join`

```sql
SELECT a.id, a.a_payload, b.b_payload_1, b.b_payload_2, b.b_payload_3
FROM default.left_dist AS a
INNER ALL JOIN default.right_dist AS b USING (id)
SETTINGS enable_analyzer = 1, max_threads = 4, use_query_cache = 0, join_algorithm = 'hash',
         distributed_shuffle_join = 1
FORMAT Null
```

该路径在 source shard 按 join key 对左右输入分桶，写入 target shard 的 `_shuffle_*` `Memory` 表；barrier 完成后，每个 target shard 对完整 bucket 执行本地 `JOIN`，initiator 汇总结果。

### 旧 `shuffle` 分支 SQL 分桶方案复现

旧 `shuffle` 分支提交 `e5c716ce909` 的 `buildQueryTreeDistributedForShuffle.cpp` 将每个执行 shard 的左右输入改写为带有下列谓词的子查询：

```sql
modulo(cityHash64(id), shard_count) = shard_index
```

为避免切换当前带未提交实现的工作树，本次没有运行旧分支构建出的二进制，而是在当前二进制上按上述源码语义等价复现：分别在三个 target 节点并发执行 bucket 为 `0`、`1`、`2` 的查询，左右 `Distributed` 输入均加对应 bucket filter。

```sql
SELECT a.id, a.a_payload, b.b_payload_1, b.b_payload_2, b.b_payload_3
FROM
(
    SELECT id, a_payload
    FROM default.left_dist
    WHERE modulo(cityHash64(id), 3) = {bucket}
) AS a
INNER ALL JOIN
(
    SELECT id, b_payload_1, b_payload_2, b_payload_3
    FROM default.right_dist
    WHERE modulo(cityHash64(id), 3) = {bucket}
) AS b USING (id)
SETTINGS enable_analyzer = 1, max_threads = 4, use_query_cache = 0, join_algorithm = 'hash',
         distributed_product_mode = 'allow', prefer_global_in_and_join = 0
FORMAT Null
```

该结果代表旧方案的 SQL 执行模型复现，不等同于对旧分支二进制做逐提交性能回归。

## 测量方法

基准测试文件为 `tests/integration/test_distributed_shuffle_join_performance/test.py`，使用单独的三 shard 配置 `tests/integration/test_distributed_shuffle_join_performance/configs/remote_servers.xml`。

1. 创建表并插入相同数据。
2. 对四种方案返回的 `id` 集合排序后比对，先确认结果一致。
3. 每种方案预热 1 次。
4. 每种方案测量 3 次，每轮轮转执行顺序以降低固定顺序偏差。
5. 使用客户端观测到的 wall-clock time 作为指标。

## 结果明细

| 方案 | Run 1 | Run 2 | Run 3 | Mean | Median | Min | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `GLOBAL INNER ALL JOIN` | 2.225 s | 2.073 s | 1.971 s | 2.090 s | 2.073 s | 1.971 s | 2.225 s |
| `distributed_product_mode = 'allow'` | 2.230 s | 2.178 s | 2.229 s | 2.212 s | 2.229 s | 2.178 s | 2.230 s |
| 旧 SQL 分桶方案复现 | 1.336 s | 1.329 s | 1.377 s | 1.347 s | 1.336 s | 1.329 s | 1.377 s |
| 当前 `distributed_shuffle_join` | 1.220 s | 1.272 s | 1.270 s | 1.254 s | 1.270 s | 1.220 s | 1.272 s |

原始 JSON 输出保存在本地构建产物 `build/distributed_shuffle_join_performance_results.json`，运行日志保存在 `build/test_distributed_shuffle_join_performance_comparison_recorded.log`。

## 解读与后续

在右表 600 万行、输出仅命中 3 万行的场景中，`GLOBAL INNER ALL JOIN` 和 `allow` 都明显落后于两个按 key 切分工作的方案。这符合设计目标：大表不应被每个消费节点重复完整处理。

当前 `distributed_shuffle_join` 相对旧 SQL 分桶复现提升约 5.0%。该差距在本地 Docker 和三次测量下仍较小，需要在 release 构建、多机网络、更高数据量和更多轮次下继续验证稳定性。后续基准还应增加峰值内存、网络字节数、读行数及异常/cleanup 开销指标，以确认性能收益来自预期的数据移动模型。

## 复现

从仓库根目录执行 integration benchmark，并显式启用手工性能用例：

```bash
python -m ci.praktika run "integration" \
    --test test_distributed_shuffle_join_performance \
    --workers 1 \
    --param CLICKHOUSE_RUN_SHUFFLE_PERFORMANCE_COMPARISON=1,CLICKHOUSE_SHUFFLE_PERF_LEFT_ROWS=30000,CLICKHOUSE_SHUFFLE_PERF_RIGHT_ROWS=6000000,CLICKHOUSE_SHUFFLE_PERF_WARMUP_RUNS=1,CLICKHOUSE_SHUFFLE_PERF_MEASURED_RUNS=3,CLICKHOUSE_SHUFFLE_PERF_RESULT_PATH="$PWD/build/distributed_shuffle_join_performance_results.json" \
    > build/test_distributed_shuffle_join_performance_comparison_recorded.log 2>&1
```

该 benchmark 默认通过 `skipif` 跳过，不会进入常规 integration test 成本；只有设置 `CLICKHOUSE_RUN_SHUFFLE_PERFORMANCE_COMPARISON=1` 时才执行大数据测量。
