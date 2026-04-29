# `distributed_shuffle_join` 性能测试说明

## 目的

这组 benchmark 用来对比 `test_distributed_shuffle_join` 目录下三种分布式 `JOIN` 模式的性能：

- `GLOBAL JOIN`
- 普通 `JOIN`，配合 `distributed_product_mode = 'allow'`
- 普通 `JOIN`，配合 `distributed_shuffle_join = 1`

本次测试主要覆盖四种场景：

1. 左表按 `join key` 落位，右表不过滤
2. 左表按 `join key` 落位，右表高选择性过滤
3. 左表随机落位，右表不过滤
4. 左表随机落位，右表高选择性过滤

核心问题是：当右表足够大，并且左右两侧都真实读取 payload 列时，`distributed_shuffle_join` 相比 `allow` 和 `GLOBAL JOIN` 的性能表现如何。

## 相关文件

- benchmark 入口：[test_benchmark.py](/home/zhangzhifan/projects/ClickHouse/tests/integration/test_distributed_shuffle_join/test_benchmark.py)
- benchmark 辅助逻辑：[benchmark_helpers.py](/home/zhangzhifan/projects/ClickHouse/tests/integration/test_distributed_shuffle_join/benchmark_helpers.py)
- 集群配置：[remote_servers.xml](/home/zhangzhifan/projects/ClickHouse/tests/integration/test_distributed_shuffle_join/configs/remote_servers.xml)

## 集群拓扑

测试使用 3 个 shard 的集群：

- `node1`
- `node2`
- `node3`

对应：

- `SHARD_COUNT = 3`

## 使用的表

本次 benchmark 会创建以下本地表：

- `a_bench_local`
- `b_bench_local`
- `b_bench_filtered_local`

以及以下分布式表：

- `a_bench_dist`
- `b_bench_dist`
- `b_bench_filtered_dist`

其中 `b_bench_filtered_dist` 用于右表高选择性过滤场景。

之所以额外建这张过滤后的右表，是因为当前 `distributed_shuffle_join` 的 rewrite 只支持直接 `TableNode`。如果把右侧写成带 `WHERE` 的子查询，可能会绕开当前的 `shuffle` rewrite 路径，因此这里采用“预先物化过滤后右表”的方式来保证三种模式能够公平对比。

## 数据规模

默认参数定义在 [benchmark_helpers.py](/home/zhangzhifan/projects/ClickHouse/tests/integration/test_distributed_shuffle_join/benchmark_helpers.py) 中：

- 左表总行数：`30000`
- 右表总行数：`6000000`
- 过滤阈值：`2`
- 过滤分母：`100`
- 预热轮数：`1`
- 正式测量轮数：`3`

因此，高选择性右表场景中，右表大约保留 `2%` 的数据，也就是大约 `120000` 行。

本文记录了两组测试结果：

- 默认规模：左表 `30000` 行，右表 `6000000` 行，覆盖全部四个场景。
- 补充规模：左表 `6000000` 行，右表 `6000000` 行，只覆盖第 3 个场景 `left_random_right_unfiltered`。

这些值也可以通过环境变量覆盖：

- `CLICKHOUSE_SHUFFLE_BENCHMARK_LEFT_ROWS`
- `CLICKHOUSE_SHUFFLE_BENCHMARK_RIGHT_ROWS`
- `CLICKHOUSE_SHUFFLE_BENCHMARK_FILTER_THRESHOLD`
- `CLICKHOUSE_SHUFFLE_BENCHMARK_WARMUP_RUNS`
- `CLICKHOUSE_SHUFFLE_BENCHMARK_MEASURED_RUNS`

## 数据分布方式

### 左表

左表测试了两种分布：

- `aligned`
- `random`

对于 `aligned`，左表行的落位表达式为：

```sql
modulo(cityHash64(id), 3)
```

也就是说，左表按 `id` 对应的 bucket 落到对应 shard。

对于 `random`，左表行的落位表达式为：

```sql
modulo(cityHash64(id, toUInt64(20260420)), 3)
```

也就是说，左表使用另一套哈希方式打散，从而模拟“左表没有按 `join key` 自然落位”的场景。

### 右表

右表使用独立的落位表达式：

```sql
modulo(cityHash64(id, toUInt64(20260421)), 3)
```

这样右表与左表的分布方式是独立的，能够更清楚地体现不同 `JOIN` 策略的代价差异。

## 表结构

### 左表

`a_bench_local`：

- `id UInt64`
- `a_metric UInt64`
- `a_payload String`
- `a_tag UInt16`

### 右表

`b_bench_local` 和 `b_bench_filtered_local`：

- `id UInt64`
- `b_metric UInt64`
- `b_filter_bucket UInt16`
- `b_payload_text String`
- `b_payload_text_extra_1 String`
- `b_payload_text_extra_2 String`
- `b_tag UInt16`

右表明显比左表更宽，用来放大右侧数据搬运和读取成本。

## 查询形态

三种模式执行的是同一类逻辑查询：

```sql
SELECT
    count(),
    sum(a.a_metric),
    sum(b.b_metric),
    sum(cityHash64(a.a_payload)),
    sum(cityHash64(b.b_payload_text, b.b_payload_text_extra_1, b.b_payload_text_extra_2))
FROM default.a_bench_dist AS a
<join_mode> default.<right_table> AS b USING (id)
SETTINGS ...
```

这里使用 `cityHash64` 去聚合左右两侧的 payload 列，而不是用 `length`。这样做的目的，是强制 ClickHouse 真实读取 payload 内容，而不是仅仅读取字符串的 `.size` 子列。

三种模式对应的设置如下：

- `GLOBAL JOIN`：`enable_analyzer = 1`
- `allow`：`enable_analyzer = 1, distributed_product_mode = 'allow', prefer_global_in_and_join = 0`
- `shuffle`：`enable_analyzer = 1, distributed_shuffle_join = 1`

## 采集的指标

每次正式测量时，benchmark 会从 `system.query_log` 中提取以下指标：

- 整体 wall-clock 耗时
- 初始查询的 `query_duration_ms`
- 初始查询的 `read_rows`
- 初始查询的 `read_bytes`
- 所有 worker 查询累计的 `read_rows`
- 所有 worker 查询累计的 `read_bytes`
- worker 查询总数

## 测试场景

本次 benchmark 的四个场景如下：

| 场景名 | 左表布局 | 右表过滤 |
| --- | --- | --- |
| `left_aligned_right_unfiltered` | `aligned` | `none` |
| `left_aligned_right_selective` | `aligned` | `selective` |
| `left_random_right_unfiltered` | `random` | `none` |
| `left_random_right_selective` | `random` | `selective` |

## 运行方式

在仓库根目录、并进入 integration tests 对应的 Python 环境后，可以这样运行：

```bash
CLICKHOUSE_RUN_SHUFFLE_BENCHMARK=1 \
PYTEST_ADDOPTS='-p no:cacheprovider' \
python -m pytest tests/integration/test_distributed_shuffle_join/test_benchmark.py -s \
> build/test_distributed_shuffle_join_benchmark_full_both_payload.log 2>&1
```

这里加 `-p no:cacheprovider`，是为了避免当前环境下 `.pytest_cache` 的权限告警干扰输出。

如果只复测第 3 个场景，并把左表和右表都设为 `6000000` 行，可以这样运行：

```bash
CLICKHOUSE_RUN_SHUFFLE_BENCHMARK=1 \
CLICKHOUSE_SHUFFLE_BENCHMARK_LEFT_ROWS=6000000 \
CLICKHOUSE_SHUFFLE_BENCHMARK_RIGHT_ROWS=6000000 \
PYTEST_ADDOPTS='-p no:cacheprovider' \
python -m pytest \
'tests/integration/test_distributed_shuffle_join/test_benchmark.py::test_distributed_shuffle_join_benchmark[left_random_right_unfiltered]' -s \
> build/test_distributed_shuffle_join_benchmark_left6m_right6m_random_unfiltered.log 2>&1
```

## 测试结果

### 默认规模：左表 `30000` 行，右表 `6000000` 行

本次完整日志文件为：

- [test_distributed_shuffle_join_benchmark_full_both_payload.log](/home/zhangzhifan/projects/ClickHouse/build/test_distributed_shuffle_join_benchmark_full_both_payload.log#L1252)

测试参数为：

- 左表总行数：`30000`
- 右表总行数：`6000000`
- 右表过滤比例：约 `2%`
- 预热轮数：`1`
- 正式测量轮数：`3`

四个场景的平均耗时如下：

| 场景 | `GLOBAL JOIN` | `allow` | `distributed_shuffle_join` |
| --- | ---: | ---: | ---: |
| `left_aligned_right_unfiltered` | `6.725s` | `3.498s` | `3.126s` |
| `left_aligned_right_selective` | `0.349s` | `0.181s` | `0.198s` |
| `left_random_right_unfiltered` | `6.814s` | `3.488s` | `3.043s` |
| `left_random_right_selective` | `0.365s` | `0.265s` | `0.165s` |

### 补充规模：左表 `6000000` 行，右表 `6000000` 行

补充测试只跑第 3 个场景 `left_random_right_unfiltered`，也就是左表随机落位、右表不过滤。

本次完整日志文件为：

- [test_distributed_shuffle_join_benchmark_left6m_right6m_random_unfiltered_uv_escalated.log](/home/zhangzhifan/projects/ClickHouse/build/test_distributed_shuffle_join_benchmark_left6m_right6m_random_unfiltered_uv_escalated.log)

测试参数为：

- 左表总行数：`6000000`
- 右表总行数：`6000000`
- 右表过滤：无
- 预热轮数：`1`
- 正式测量轮数：`3`

测试结果为：

| 模式 | 平均 wall-clock 耗时 | 初始查询 `query_duration_ms` 平均值 | 初始查询 `read_rows` 平均值 | 初始查询 `read_bytes` 平均值 | worker 查询 `read_rows` 平均值 | worker 查询 `read_bytes` 平均值 | worker 查询数平均值 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `GLOBAL JOIN` | `7.123s` | `7033.00ms` | `30000000.00` | `12854332202.00` | `20001152.00` | `8569630229.00` | `4.00` |
| `allow` | `4.912s` | `4832.33ms` | `24000000.00` | `9515068312.00` | `28001116.00` | `12481963764.00` | `8.00` |
| `distributed_shuffle_join` | `3.608s` | `3548.00ms` | `36000000.00` | `10129621596.00` | `48000000.00` | `13506162128.00` | `14.00` |

同一个 `left_random_right_unfiltered` 场景下，左表从 `30000` 行增加到 `6000000` 行后的平均 wall-clock 耗时对比如下：

| 左表行数 | `GLOBAL JOIN` | `allow` | `distributed_shuffle_join` |
| --- | ---: | ---: | ---: |
| `30000` | `6.814s` | `3.488s` | `3.043s` |
| `6000000` | `7.123s` | `4.912s` | `3.608s` |

## 结果解读

从这次测试看：

- 在右表不过滤的两个场景里，`distributed_shuffle_join` 都是最快的。
- 在 `left_aligned_right_unfiltered` 这个主场景中，`distributed_shuffle_join` 明显快于 `allow` 和 `GLOBAL JOIN`。
- 当右表高选择性过滤后，`GLOBAL JOIN` 和 `allow` 都会明显变快。
- 在 `left_aligned_right_selective` 场景中，`allow` 略快于 `distributed_shuffle_join`。
- 在 `left_random_right_selective` 场景中，当前数据形态下 `distributed_shuffle_join` 仍然是最快的。
- 在补充的 `left_random_right_unfiltered` 测试中，左表扩大到 `6000000` 行后，`distributed_shuffle_join` 仍然是最快的：平均耗时 `3.608s`，相比 `allow` 快约 `26.6%`，相比 `GLOBAL JOIN` 快约 `49.4%`。
- 左表扩大后，`allow` 的耗时从 `3.488s` 增加到 `4.912s`，增幅比 `distributed_shuffle_join` 更明显；`distributed_shuffle_join` 从 `3.043s` 增加到 `3.608s`。

因此，这组 benchmark 支持一个比较实用的结论：

- 当右表很大且不过滤时，`distributed_shuffle_join` 的优势最明显。
- 当右表过滤很强时，三种模式的差距会缩小，`allow` 在某些场景下会变得更有竞争力，甚至略优于 `distributed_shuffle_join`。
- 当左右表都达到 `6000000` 行，且左表随机落位、右表不过滤时，当前数据形态下 `distributed_shuffle_join` 的优势仍然存在。
