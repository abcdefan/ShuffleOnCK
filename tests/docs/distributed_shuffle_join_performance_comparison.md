# Distributed Shuffle Join Performance Comparison

## 结论

本报告只比较当前 `exchange` 分支中能够从普通用户查询触发的三条真实执行路径：

- 当前 push-based `distributed_shuffle_join`。
- ClickHouse 内置 `GLOBAL INNER ALL JOIN`。
- `distributed_product_mode = 'allow'` 的普通 distributed-over-distributed `JOIN`。

在同一套受控三 shard 测试环境中，结果如下：

| 场景 | 当前 `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | `allow` | 结论 |
| --- | ---: | ---: | ---: | --- |
| 小表 `JOIN` 小表 | 0.316 s | 0.115 s | 0.115 s | shuffle 明显不适合小表 |
| 中表 `JOIN` 中表 | 0.315 s | 0.215 s | 0.215 s | shuffle 的固定成本仍未摊薄 |
| 大表 `JOIN` 大表（`2,000,000 x 2,000,000`） | 0.818 s | 0.918 s | 0.971 s | shuffle 快 `11.0-15.8%` |
| 小表 `JOIN` 大表（`30,000 x 6,000,000`） | 1.269 s | 2.223 s | 1.822 s | shuffle 快 `30.4-42.9%` |

当前实现已经在两组更大输入场景中表现出收益，但并不是普遍优于现有路径。尤其在小表或中表场景中，创建 `_shuffle_*` `Memory` 表、进行 Exchange、再执行 Local `JOIN` 和 cleanup 的成本明显高于收益。后续启用策略需要 cost model，至少纳入左右输入规模与候选 broadcast/ordinary distributed 路径成本。

## 范围与限制

三条路径的结果都由当前 `exchange` 二进制接受真实用户 SQL 并在三节点 ClickHouse 集群上执行。

为保护本地 WSL 环境，benchmark 仍默认拒绝匹配输出超过 `1,000,000` 行的 workload。本轮按请求额外测量了右表 `6,000,000` 行但输出仅 `30,000` 行的偏斜场景，并在明确 override 后运行了输出 `2,000,000` 行的对等场景。这些规模是本机受控测量值，不代表生产场景中的大表边界。绝对耗时只适用于本机 `Debug` build；结论重点是相同场景内三种路径的相对差异。

## 测试环境

| 项目 | 值 |
| --- | --- |
| 分支/实现 | `exchange`，基线 commit `2ffa0c9ea88`，附带本轮 benchmark 保护改动 |
| ClickHouse 二进制 | `26.4.1.1` |
| 构建类型 | `Debug`，`SANITIZE = OFF` |
| 集群布局 | 本地 Docker，3 shards，每 shard 1 replica |
| CPU | Intel Core i5-14600KF，20 logical CPUs |
| 内存 | 15 GiB |
| query settings | `enable_analyzer = 1`, `max_threads = 2`, `use_query_cache = 0`, `join_algorithm = 'hash'` |
| 预热 | 每条路径 1 次 |
| 测量 | 每条路径 5 次，按轮次轮转执行顺序 |

## 数据集

测试在每个 shard 创建本地表 `left_local`、`right_local`，并创建读取三 shard 的 `Distributed` 表 `left_dist`、`right_dist`。左右本地数据分别按照不同 seed 的 `cityHash64` 落位，刻意不按照 `JOIN` key 的目标 bucket 预分布。

| 规模名称 | 行数 |
| --- | ---: |
| 小表 | 30,000 |
| 中表 | 250,000 |
| 大表（对等场景单侧） | 2,000,000 |
| 大表（偏斜场景右侧） | 6,000,000 |

| 表侧 | 列 | 说明 |
| --- | --- | --- |
| 左表 | `id`, `a_payload` | `id` 为连续 `UInt64`；payload 为短字符串 |
| 右表 | `id`, `b_payload_1`, `b_payload_2`, `b_payload_3` | 三列由 hash 十六进制文本生成的宽 `String` payload |

四个场景如下：

| 场景 | 左表行数 | 右表行数 | 匹配输出行数 |
| --- | ---: | ---: | ---: |
| 小表 `JOIN` 小表 | 30,000 | 30,000 | 30,000 |
| 中表 `JOIN` 中表 | 250,000 | 250,000 | 250,000 |
| 大表 `JOIN` 大表 | 2,000,000 | 2,000,000 | 2,000,000 |
| 小表 `JOIN` 大表 | 30,000 | 6,000,000 | 30,000 |

## 执行 SQL

### 当前 `distributed_shuffle_join`

```sql
SELECT a.id, a.a_payload, b.b_payload_1, b.b_payload_2, b.b_payload_3
FROM default.left_dist AS a
INNER ALL JOIN default.right_dist AS b USING (id)
SETTINGS
    enable_analyzer = 1,
    max_threads = 2,
    use_query_cache = 0,
    join_algorithm = 'hash',
    distributed_shuffle_join = 1
FORMAT Null
```

### `GLOBAL INNER ALL JOIN`

```sql
SELECT a.id, a.a_payload, b.b_payload_1, b.b_payload_2, b.b_payload_3
FROM default.left_dist AS a
GLOBAL INNER ALL JOIN default.right_dist AS b USING (id)
SETTINGS
    enable_analyzer = 1,
    max_threads = 2,
    use_query_cache = 0,
    join_algorithm = 'hash'
FORMAT Null
```

### `distributed_product_mode = 'allow'`

```sql
SELECT a.id, a.a_payload, b.b_payload_1, b.b_payload_2, b.b_payload_3
FROM default.left_dist AS a
INNER ALL JOIN default.right_dist AS b USING (id)
SETTINGS
    enable_analyzer = 1,
    max_threads = 2,
    use_query_cache = 0,
    join_algorithm = 'hash',
    distributed_product_mode = 'allow',
    prefer_global_in_and_join = 0
FORMAT Null
```

## 正确性与安全控制

性能 test 位于 `tests/integration/test_distributed_shuffle_join_performance/test.py`。本轮先使用每个场景的完整表构造 join 输入，再执行以下限制：

- 三条路径都对 `id <= 10000` 的确定性匹配样本返回结果并排序比较，确认输出一致。
- 计时阶段仍对完整输入执行完整 projection，但使用 `FORMAT Null`，避免 Python 客户端持有完整输出。
- 默认 `max_threads = 2`。
- 默认拒绝预计匹配输出超过 `1,000,000` 行的 workload；需要明确 override 才能提高规模。
- `2,000,000 x 2,000,000` 场景使用 `CLICKHOUSE_SHUFFLE_PERF_ALLOW_LARGE_OUTPUT=1` 显式放行；默认保护未被放宽。

新增两次大规模测试全部结束后，本机可用内存回到约 `13 GiB`。

## 中位数对比

| 场景 | `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | 相对 `GLOBAL` | `allow` | 相对 `allow` |
| --- | ---: | ---: | ---: | ---: | ---: |
| 小表 `JOIN` 小表 | 0.316 s | 0.115 s | 慢 `174.9%` | 0.115 s | 慢 `174.9%` |
| 中表 `JOIN` 中表 | 0.315 s | 0.215 s | 慢 `46.6%` | 0.215 s | 慢 `46.8%` |
| 大表 `JOIN` 大表 | 0.818 s | 0.918 s | 快 `11.0%` | 0.971 s | 快 `15.8%` |
| 小表 `JOIN` 大表 | 1.269 s | 2.223 s | 快 `42.9%` | 1.822 s | 快 `30.4%` |

## 五轮计时明细

### 小表 `JOIN` 小表

| 路径 | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Mean | Median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `distributed_shuffle_join` | 0.416 s | 0.315 s | 0.315 s | 0.316 s | 0.365 s | 0.345 s | 0.316 s |
| `GLOBAL INNER ALL JOIN` | 0.115 s | 0.115 s | 0.115 s | 0.115 s | 0.115 s | 0.115 s | 0.115 s |
| `allow` | 0.115 s | 0.115 s | 0.115 s | 0.115 s | 0.115 s | 0.115 s | 0.115 s |

### 中表 `JOIN` 中表

| 路径 | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Mean | Median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `distributed_shuffle_join` | 0.315 s | 0.315 s | 0.416 s | 0.265 s | 0.416 s | 0.345 s | 0.315 s |
| `GLOBAL INNER ALL JOIN` | 0.215 s | 0.215 s | 0.215 s | 0.165 s | 0.215 s | 0.205 s | 0.215 s |
| `allow` | 0.215 s | 0.215 s | 0.216 s | 0.166 s | 0.166 s | 0.195 s | 0.215 s |

### 大表 `JOIN` 大表

| 路径 | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Mean | Median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `distributed_shuffle_join` | 0.818 s | 0.868 s | 0.868 s | 0.818 s | 0.817 s | 0.838 s | 0.818 s |
| `GLOBAL INNER ALL JOIN` | 1.018 s | 0.917 s | 0.919 s | 0.918 s | 0.867 s | 0.928 s | 0.918 s |
| `allow` | 0.971 s | 0.922 s | 0.975 s | 1.019 s | 0.969 s | 0.971 s | 0.971 s |

### 小表 `JOIN` 大表

| 路径 | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Mean | Median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `distributed_shuffle_join` | 1.470 s | 1.218 s | 1.269 s | 1.219 s | 1.269 s | 1.289 s | 1.269 s |
| `GLOBAL INNER ALL JOIN` | 2.324 s | 2.223 s | 2.124 s | 2.120 s | 2.274 s | 2.213 s | 2.223 s |
| `allow` | 2.074 s | 2.175 s | 1.822 s | 1.772 s | 1.774 s | 1.923 s | 1.822 s |

## 结果解读

小表和中表场景中，当前 shuffle 的 `Prepare -> Exchange -> Local JOIN -> Cleanup` 过程存在约 `0.3s` 的固定成本区间，因此普通路径更合适。

大表对大表时，双方都需要处理 `2,000,000` 行输入和 `2,000,000` 行输出，当前 shuffle 已能够抵消协调与物化开销，比 `GLOBAL INNER ALL JOIN` 快 `11.0%`，比 `allow` 快 `15.8%`。这个差距仍需在 release build 和更多样本中复核。

小表对大表时，右侧增大到 `6,000,000` 行后，当前 shuffle 比 `GLOBAL INNER ALL JOIN` 快 `42.9%`，也比 `allow` 快 `30.4%`。这说明在右侧物化或重复读取成本足够大时，Exchange 的固定成本会被摊薄；但小表和中表结果仍说明启用条件不能只依赖语义匹配。

## 结果文件

| 场景 | JSON 结果 | Praktika 日志 | 测试结果 |
| --- | --- | --- | --- |
| 小表 `JOIN` 小表 | `build/distributed_shuffle_join_three_modes_small_small_results.json` | `build/test_distributed_shuffle_join_three_modes_small_small.log` | `1 passed in 24.37s` |
| 中表 `JOIN` 中表 | `build/distributed_shuffle_join_three_modes_medium_medium_results.json` | `build/test_distributed_shuffle_join_three_modes_medium_medium.log` | `1 passed in 25.94s` |
| 大表 `JOIN` 大表 | `build/distributed_shuffle_join_three_modes_large_large_2m_results.json` | `build/test_distributed_shuffle_join_three_modes_large_large_2m.log` | `1 passed in 55.92s` |
| 小表 `JOIN` 大表 | `build/distributed_shuffle_join_three_modes_small_large_6m_results.json` | `build/test_distributed_shuffle_join_three_modes_small_large_6m.log` | `1 passed in 76.05s` |

## 复现

例如复现本轮显式放行的大表 `JOIN` 大表场景：

```bash
python -m ci.praktika run "integration" \
    --test test_distributed_shuffle_join_performance \
    --workers 1 \
    --param CLICKHOUSE_RUN_SHUFFLE_PERFORMANCE_COMPARISON=1,CLICKHOUSE_SHUFFLE_PERF_LEFT_ROWS=2000000,CLICKHOUSE_SHUFFLE_PERF_RIGHT_ROWS=2000000,CLICKHOUSE_SHUFFLE_PERF_CORRECTNESS_ROWS=10000,CLICKHOUSE_SHUFFLE_PERF_ALLOW_LARGE_OUTPUT=1,CLICKHOUSE_SHUFFLE_PERF_MAX_THREADS=2,CLICKHOUSE_SHUFFLE_PERF_WARMUP_RUNS=1,CLICKHOUSE_SHUFFLE_PERF_MEASURED_RUNS=5,CLICKHOUSE_SHUFFLE_PERF_RESULT_PATH="$PWD/build/distributed_shuffle_join_three_modes_large_large_2m_results.json" \
    > build/test_distributed_shuffle_join_three_modes_large_large_2m.log 2>&1
```

`CLICKHOUSE_SHUFFLE_PERF_ALLOW_LARGE_OUTPUT=1` 会绕过默认输出保护；除非先独立评估机器内存限制，不应增加该设置或继续提高数据规模。
