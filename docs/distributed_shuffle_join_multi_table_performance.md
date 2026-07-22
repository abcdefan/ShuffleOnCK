# Distributed Shuffle Join Multi-Table Performance

## 结论

本轮对比三条多分布式表 `INNER ALL JOIN` 执行路径：

- 当前 push-based `distributed_shuffle_join`。
- ClickHouse 内置 `GLOBAL INNER ALL JOIN`。
- `distributed_product_mode = 'allow'` 的默认 distributed-over-distributed `JOIN`。

在本地三 shard `Debug` build 上，当前 `distributed_shuffle_join` 只在“小表 `JOIN` 大表”场景明显更快；在小表、中表和大表同规模 `JOIN` 中均慢于 `GLOBAL INNER ALL JOIN` 和 `allow`。这说明当前多阶段左深 shuffle 的固定阶段成本和中间表物化成本仍然偏高，启用策略不能只看查询是否满足语义条件。

| 场景 | `n` | `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | 相对 `GLOBAL` | `allow` | 相对 `allow` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 小表 `JOIN` 小表 | 2 | 0.315 s | 0.064 s | 慢 `390.0%` | 0.064 s | 慢 `389.4%` |
| 小表 `JOIN` 小表 | 3 | 0.415 s | 0.114 s | 慢 `263.4%` | 0.115 s | 慢 `261.4%` |
| 小表 `JOIN` 小表 | 4 | 0.416 s | 0.114 s | 慢 `263.5%` | 0.114 s | 慢 `263.3%` |
| 小表 `JOIN` 小表 | 5 | 0.565 s | 0.114 s | 慢 `394.9%` | 0.114 s | 慢 `395.2%` |
| 中表 `JOIN` 中表 | 2 | 0.365 s | 0.114 s | 慢 `219.6%` | 0.114 s | 慢 `219.5%` |
| 中表 `JOIN` 中表 | 3 | 0.415 s | 0.215 s | 慢 `93.4%` | 0.165 s | 慢 `151.4%` |
| 中表 `JOIN` 中表 | 4 | 0.565 s | 0.264 s | 慢 `113.7%` | 0.214 s | 慢 `163.9%` |
| 中表 `JOIN` 中表 | 5 | 0.667 s | 0.365 s | 慢 `82.8%` | 0.265 s | 慢 `152.0%` |
| 大表 `JOIN` 大表 | 2 | 0.415 s | 0.315 s | 慢 `31.7%` | 0.215 s | 慢 `93.3%` |
| 大表 `JOIN` 大表 | 3 | 0.666 s | 0.515 s | 慢 `29.2%` | 0.366 s | 慢 `82.0%` |
| 大表 `JOIN` 大表 | 4 | 0.866 s | 0.766 s | 慢 `13.1%` | 0.516 s | 慢 `68.0%` |
| 大表 `JOIN` 大表 | 5 | 1.167 s | 1.067 s | 慢 `9.4%` | 0.667 s | 慢 `74.9%` |
| 小表 `JOIN` 大表 | 2 | 0.415 s | 0.616 s | 快 `32.6%` | 0.516 s | 快 `19.7%` |
| 小表 `JOIN` 大表 | 3 | 0.565 s | 1.218 s | 快 `53.6%` | 0.971 s | 快 `41.8%` |
| 小表 `JOIN` 大表 | 4 | 0.716 s | 1.969 s | 快 `63.7%` | 1.423 s | 快 `49.7%` |
| 小表 `JOIN` 大表 | 5 | 0.715 s | 2.921 s | 快 `75.5%` | 1.870 s | 快 `61.7%` |

## 测试环境

| 项目 | 值 |
| --- | --- |
| 分支 | `exchange` |
| commit | `65ba3b7d8b6` |
| ClickHouse 版本 | `26.4.1.1` |
| 构建类型 | `Debug` |
| sanitizer | 空，`SANITIZE` 未启用 |
| 集群 | 本地 Docker，3 shards，每 shard 1 replica |
| 平台 | `Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39` |
| CPU | 20 logical CPUs |
| 内存 | `16266416 kB` |
| Python | `3.14.0` |
| query settings | `enable_analyzer = 1`, `max_threads = 2`, `use_query_cache = 0`, `join_algorithm = 'hash'` |
| 预热 | 每条路径每组 1 次 |
| 测量 | 每条路径每组 3 次，按轮次轮转执行顺序 |

## 数据集

每个场景创建 `n` 张本地 `MergeTree` 表和对应的 `Distributed` 表：

```sql
CREATE TABLE default.bench_t0_local
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
```

每张 `Distributed` 表使用 `cityHash64(id)` 作为 sharding key。插入本地表时使用不同 seed 的 `cityHash64` 分布到三个 shard，刻意不让原始数据按 `JOIN` key 预落位。每张表对同一个 `id` 只生成一行，因此 `INNER ALL JOIN` 输出行数等于各表行数最小值。

| 规模 | 行数 |
| --- | ---: |
| 小表 | 30,000 |
| 中表 | 250,000 |
| 大表 | 1,000,000 |
| 小表 `JOIN` 大表中的大表 | 3,000,000 |

四类场景定义如下：

| 场景 | 表行数 |
| --- | --- |
| 小表 `JOIN` 小表 | 所有 `n` 张表都是 `30,000` 行 |
| 中表 `JOIN` 中表 | 所有 `n` 张表都是 `250,000` 行 |
| 大表 `JOIN` 大表 | 所有 `n` 张表都是 `1,000,000` 行 |
| 小表 `JOIN` 大表 | 第 1 张表 `30,000` 行，其余 `n - 1` 张表各 `3,000,000` 行 |

每组计时前，三条路径都先执行 `id <= 1000 ORDER BY id` 的正确性检查，确认返回 `1..1000`。

## SQL 形态

### 当前 `distributed_shuffle_join`

```sql
SELECT id, t0.payload AS p0, t1.payload AS p1, ...
FROM default.bench_t0_dist AS t0
INNER ALL JOIN default.bench_t1_dist AS t1 USING (id)
INNER ALL JOIN default.bench_t2_dist AS t2 USING (id)
...
SETTINGS
    enable_analyzer = 1,
    distributed_shuffle_join = 1,
    max_threads = 2,
    use_query_cache = 0,
    join_algorithm = 'hash'
FORMAT Null
```

### `GLOBAL INNER ALL JOIN`

```sql
SELECT id, t0.payload AS p0, t1.payload AS p1, ...
FROM default.bench_t0_dist AS t0
GLOBAL INNER ALL JOIN default.bench_t1_dist AS t1 USING (id)
GLOBAL INNER ALL JOIN default.bench_t2_dist AS t2 USING (id)
...
SETTINGS
    enable_analyzer = 1,
    max_threads = 2,
    use_query_cache = 0,
    join_algorithm = 'hash'
FORMAT Null
```

### `distributed_product_mode = 'allow'`

```sql
SELECT id, t0.payload AS p0, t1.payload AS p1, ...
FROM default.bench_t0_dist AS t0
INNER ALL JOIN default.bench_t1_dist AS t1 USING (id)
INNER ALL JOIN default.bench_t2_dist AS t2 USING (id)
...
SETTINGS
    enable_analyzer = 1,
    distributed_product_mode = 'allow',
    prefer_global_in_and_join = 0,
    max_threads = 2,
    use_query_cache = 0,
    join_algorithm = 'hash'
FORMAT Null
```

## 中位数结果

### 小表 `JOIN` 小表

| `n` | `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | 相对 `GLOBAL` | `allow` | 相对 `allow` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 0.315 s | 0.064 s | 慢 `390.0%` | 0.064 s | 慢 `389.4%` |
| 3 | 0.415 s | 0.114 s | 慢 `263.4%` | 0.115 s | 慢 `261.4%` |
| 4 | 0.416 s | 0.114 s | 慢 `263.5%` | 0.114 s | 慢 `263.3%` |
| 5 | 0.565 s | 0.114 s | 慢 `394.9%` | 0.114 s | 慢 `395.2%` |

### 中表 `JOIN` 中表

| `n` | `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | 相对 `GLOBAL` | `allow` | 相对 `allow` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 0.365 s | 0.114 s | 慢 `219.6%` | 0.114 s | 慢 `219.5%` |
| 3 | 0.415 s | 0.215 s | 慢 `93.4%` | 0.165 s | 慢 `151.4%` |
| 4 | 0.565 s | 0.264 s | 慢 `113.7%` | 0.214 s | 慢 `163.9%` |
| 5 | 0.667 s | 0.365 s | 慢 `82.8%` | 0.265 s | 慢 `152.0%` |

### 大表 `JOIN` 大表

| `n` | `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | 相对 `GLOBAL` | `allow` | 相对 `allow` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 0.415 s | 0.315 s | 慢 `31.7%` | 0.215 s | 慢 `93.3%` |
| 3 | 0.666 s | 0.515 s | 慢 `29.2%` | 0.366 s | 慢 `82.0%` |
| 4 | 0.866 s | 0.766 s | 慢 `13.1%` | 0.516 s | 慢 `68.0%` |
| 5 | 1.167 s | 1.067 s | 慢 `9.4%` | 0.667 s | 慢 `74.9%` |

### 小表 `JOIN` 大表

| `n` | `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | 相对 `GLOBAL` | `allow` | 相对 `allow` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 0.415 s | 0.616 s | 快 `32.6%` | 0.516 s | 快 `19.7%` |
| 3 | 0.565 s | 1.218 s | 快 `53.6%` | 0.971 s | 快 `41.8%` |
| 4 | 0.716 s | 1.969 s | 快 `63.7%` | 1.423 s | 快 `49.7%` |
| 5 | 0.715 s | 2.921 s | 快 `75.5%` | 1.870 s | 快 `61.7%` |

## 平均值结果

| 场景 | `n` | `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | `allow` |
| --- | ---: | ---: | ---: | ---: |
| 小表 `JOIN` 小表 | 2 | 0.315 s | 0.081 s | 0.081 s |
| 小表 `JOIN` 小表 | 3 | 0.399 s | 0.114 s | 0.115 s |
| 小表 `JOIN` 小表 | 4 | 0.482 s | 0.114 s | 0.114 s |
| 小表 `JOIN` 小表 | 5 | 0.616 s | 0.114 s | 0.114 s |
| 中表 `JOIN` 中表 | 2 | 0.366 s | 0.131 s | 0.114 s |
| 中表 `JOIN` 中表 | 3 | 0.448 s | 0.215 s | 0.165 s |
| 中表 `JOIN` 中表 | 4 | 0.633 s | 0.264 s | 0.214 s |
| 中表 `JOIN` 中表 | 5 | 0.700 s | 0.365 s | 0.248 s |
| 大表 `JOIN` 大表 | 2 | 0.433 s | 0.315 s | 0.231 s |
| 大表 `JOIN` 大表 | 3 | 0.649 s | 0.515 s | 0.366 s |
| 大表 `JOIN` 大表 | 4 | 0.833 s | 0.750 s | 0.516 s |
| 大表 `JOIN` 大表 | 5 | 1.184 s | 1.050 s | 0.667 s |
| 小表 `JOIN` 大表 | 2 | 0.398 s | 0.616 s | 0.503 s |
| 小表 `JOIN` 大表 | 3 | 0.532 s | 1.251 s | 0.985 s |
| 小表 `JOIN` 大表 | 4 | 0.716 s | 1.985 s | 1.520 s |
| 小表 `JOIN` 大表 | 5 | 0.732 s | 2.954 s | 1.869 s |

## 解读

当前左深多表 `distributed_shuffle_join` 会把 `n` 表 `JOIN` 拆成 `n - 1` 个二元 shuffle stage。每增加一张表，都会增加一轮 `Prepare -> Exchange -> Local JOIN -> Cleanup`，非最终 stage 还要把中间结果写入普通 `Memory` 表。

小表和中表场景里，`GLOBAL INNER ALL JOIN` 与 `allow` 的绝对耗时很低，当前 shuffle 的固定阶段成本无法摊薄，因此明显更慢。

大表同规模场景里，当前 shuffle 随 `n` 增加接近 `GLOBAL INNER ALL JOIN`，但仍慢于两条默认路径，尤其慢于 `allow`。这组数据说明在当前本地 `Debug` build 和这一类等值链式数据分布下，多阶段物化和调度成本仍然较高。

小表 `JOIN` 大表是当前 shuffle 的优势场景。第一张表只有 `30,000` 行，第一阶段后中间结果也保持小规模；而 `GLOBAL INNER ALL JOIN` 和 `allow` 需要继续处理每张大表的分布式读取/广播或重复 distributed-over-distributed 执行。`n = 5` 时当前 shuffle 比 `GLOBAL INNER ALL JOIN` 快 `75.5%`，比 `allow` 快 `61.7%`。

## 结果文件

| 文件 | 说明 |
| --- | --- |
| `build/distributed_shuffle_join_multi_table_performance_comparison_results.json` | 三模式结构化结果 |
| `build/test_distributed_shuffle_join_multi_table_performance_comparison.log` | 三模式 benchmark 日志 |
| `build/distributed_shuffle_join_multi_table_performance_comparison_smoke_results.json` | 三模式 smoke 结果 |
| `build/test_distributed_shuffle_join_multi_table_performance_comparison_smoke.log` | 三模式 smoke 日志 |
| `tmp/distributed_shuffle_join_multi_table_perf.py` | 本轮使用的临时 benchmark 脚本 |

运行期间 `ClickHouseCluster` 的后置 cleanup 在 Docker 容器已经删除后没有自然退出；本轮在 JSON 结果完整写入且 `docker ps` 确认没有残留 benchmark 容器后终止了 Python 收尾进程。查询计时、正确性检查和结果文件均已完成。
