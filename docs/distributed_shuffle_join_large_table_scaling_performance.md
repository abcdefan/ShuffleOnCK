# Distributed Shuffle Join Large-Table Scaling Performance

## 结论

本轮只测试等规模大表 `INNER ALL JOIN`，对比三条执行路径：

- 当前 push-based `distributed_shuffle_join`。
- ClickHouse 内置 `GLOBAL INNER ALL JOIN`。
- `distributed_product_mode = 'allow'` 的默认 distributed-over-distributed `JOIN`。

集群固定为 3 shards；分布式表数量 `n` 取 `2/3/4/5`，每张表行数取 `1M/2M/3M/4M`，组成 16 个场景。

结果验证了此前的判断：`1M` 和 `2M` 窄表多数仍不足以摊薄 shuffle 的固定成本；从 `3M` 开始，所有 `n=2..5` 场景中，当前 `distributed_shuffle_join` 都同时跑赢 `GLOBAL INNER ALL JOIN` 和 `allow`。

| `n` | 每表行数 | `distributed_shuffle_join` | `GLOBAL INNER ALL JOIN` | 相对 `GLOBAL` | `allow` | 相对 `allow` |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 1M | 0.465 s | 0.315 s | 慢 `47.8%` | 0.215 s | 慢 `116.2%` |
| 2 | 2M | 0.565 s | 0.415 s | 慢 `36.3%` | 0.365 s | 慢 `55.0%` |
| 2 | 3M | 0.515 s | 0.716 s | 快 `28.0%` | 0.617 s | 快 `16.5%` |
| 2 | 4M | 0.616 s | 0.866 s | 快 `28.9%` | 0.817 s | 快 `24.6%` |
| 3 | 1M | 0.615 s | 0.515 s | 慢 `19.4%` | 0.371 s | 慢 `66.1%` |
| 3 | 2M | 0.816 s | 0.767 s | 慢 `6.5%` | 0.616 s | 慢 `32.6%` |
| 3 | 3M | 0.967 s | 1.418 s | 快 `31.8%` | 1.217 s | 快 `20.6%` |
| 3 | 4M | 1.117 s | 1.819 s | 快 `38.6%` | 1.472 s | 快 `24.1%` |
| 4 | 1M | 1.017 s | 0.766 s | 慢 `32.8%` | 0.516 s | 慢 `97.1%` |
| 4 | 2M | 1.367 s | 1.167 s | 慢 `17.2%` | 0.818 s | 慢 `67.0%` |
| 4 | 3M | 1.571 s | 2.420 s | 快 `35.1%` | 1.769 s | 快 `11.2%` |
| 4 | 4M | 1.869 s | 2.871 s | 快 `34.9%` | 2.120 s | 快 `11.9%` |
| 5 | 1M | 1.067 s | 1.116 s | 快 `4.4%` | 0.666 s | 慢 `60.2%` |
| 5 | 2M | 1.668 s | 1.768 s | 快 `5.6%` | 1.067 s | 慢 `56.3%` |
| 5 | 3M | 1.970 s | 3.573 s | 快 `44.9%` | 2.379 s | 快 `17.2%` |
| 5 | 4M | 2.420 s | 3.975 s | 快 `39.1%` | 2.622 s | 快 `7.7%` |

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
| query settings | `enable_analyzer = 1`, `max_threads = 2`, `use_query_cache = 0`, `join_algorithm = 'hash'` |
| 预热 | 每条路径每组 1 次 |
| 测量 | 每条路径每组 3 次，按轮次轮转执行顺序 |

绝对耗时只适用于当前本地 `Debug` build 和 Docker 网络环境；本报告关注相同场景内三条路径的相对关系。

## 数据集

每个场景创建 `n` 张等 schema 的本地 `MergeTree` 表和对应的 `Distributed` 表：

```sql
CREATE TABLE default.bench_t0_local
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
```

每张表包含连续的 `id = 1..N`，每个 `id` 只有一行。`payload` 是一个包含表编号、`id` 和 hash 文本的短 `String`。最终查询输出所有表的 `payload`，因此性能测量包含实际 payload 的读取、传输和 join 成本。

每张 `Distributed` 表使用 `cityHash64(id)` 作为 sharding key。插入本地表时使用不同 seed 的 `cityHash64` 将行分布到三个 shard，刻意不让数据按 `JOIN` key 预落位。

所有表具有相同的行数，因此每组 `INNER ALL JOIN` 的输出行数等于单表行数。每组计时前，三条路径都执行 `id <= 1000 ORDER BY id` 的正确性检查，确认返回 `1..1000`。

## 中位数矩阵

### 当前 `distributed_shuffle_join`

| `n` | 1M | 2M | 3M | 4M |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 0.465 s | 0.565 s | 0.515 s | 0.616 s |
| 3 | 0.615 s | 0.816 s | 0.967 s | 1.117 s |
| 4 | 1.017 s | 1.367 s | 1.571 s | 1.869 s |
| 5 | 1.067 s | 1.668 s | 1.970 s | 2.420 s |

### `GLOBAL INNER ALL JOIN`

| `n` | 1M | 2M | 3M | 4M |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 0.315 s | 0.415 s | 0.716 s | 0.866 s |
| 3 | 0.515 s | 0.767 s | 1.418 s | 1.819 s |
| 4 | 0.766 s | 1.167 s | 2.420 s | 2.871 s |
| 5 | 1.116 s | 1.768 s | 3.573 s | 3.975 s |

### `distributed_product_mode = 'allow'`

| `n` | 1M | 2M | 3M | 4M |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 0.215 s | 0.365 s | 0.617 s | 0.817 s |
| 3 | 0.371 s | 0.616 s | 1.217 s | 1.472 s |
| 4 | 0.516 s | 0.818 s | 1.769 s | 2.120 s |
| 5 | 0.666 s | 1.067 s | 2.379 s | 2.622 s |

## 三轮计时明细

| `n` | 每表行数 | 路径 | Run 1 | Run 2 | Run 3 | Median |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 2 | 1M | `distributed_shuffle_join` | 0.465 s | 0.465 s | 0.465 s | 0.465 s |
| 2 | 1M | `GLOBAL INNER ALL JOIN` | 0.315 s | 0.315 s | 0.315 s | 0.315 s |
| 2 | 1M | `allow` | 0.265 s | 0.215 s | 0.215 s | 0.215 s |
| 2 | 2M | `distributed_shuffle_join` | 0.565 s | 0.566 s | 0.565 s | 0.565 s |
| 2 | 2M | `GLOBAL INNER ALL JOIN` | 0.415 s | 0.415 s | 0.415 s | 0.415 s |
| 2 | 2M | `allow` | 0.365 s | 0.365 s | 0.315 s | 0.365 s |
| 2 | 3M | `distributed_shuffle_join` | 0.616 s | 0.515 s | 0.465 s | 0.515 s |
| 2 | 3M | `GLOBAL INNER ALL JOIN` | 0.716 s | 0.768 s | 0.665 s | 0.716 s |
| 2 | 3M | `allow` | 0.617 s | 0.666 s | 0.617 s | 0.617 s |
| 2 | 4M | `distributed_shuffle_join` | 0.616 s | 0.616 s | 0.767 s | 0.616 s |
| 2 | 4M | `GLOBAL INNER ALL JOIN` | 0.866 s | 0.866 s | 0.817 s | 0.866 s |
| 2 | 4M | `allow` | 0.867 s | 0.768 s | 0.817 s | 0.817 s |
| 3 | 1M | `distributed_shuffle_join` | 0.666 s | 0.615 s | 0.615 s | 0.615 s |
| 3 | 1M | `GLOBAL INNER ALL JOIN` | 0.515 s | 0.515 s | 0.516 s | 0.515 s |
| 3 | 1M | `allow` | 0.371 s | 0.415 s | 0.365 s | 0.371 s |
| 3 | 2M | `distributed_shuffle_join` | 0.866 s | 0.816 s | 0.766 s | 0.816 s |
| 3 | 2M | `GLOBAL INNER ALL JOIN` | 0.766 s | 0.767 s | 0.816 s | 0.767 s |
| 3 | 2M | `allow` | 0.618 s | 0.616 s | 0.616 s | 0.616 s |
| 3 | 3M | `distributed_shuffle_join` | 1.167 s | 0.967 s | 0.917 s | 0.967 s |
| 3 | 3M | `GLOBAL INNER ALL JOIN` | 1.417 s | 1.468 s | 1.418 s | 1.418 s |
| 3 | 3M | `allow` | 1.217 s | 1.219 s | 1.168 s | 1.217 s |
| 3 | 4M | `distributed_shuffle_join` | 1.117 s | 1.117 s | 1.117 s | 1.117 s |
| 3 | 4M | `GLOBAL INNER ALL JOIN` | 1.668 s | 1.821 s | 1.819 s | 1.819 s |
| 3 | 4M | `allow` | 1.418 s | 1.476 s | 1.472 s | 1.472 s |
| 4 | 1M | `distributed_shuffle_join` | 0.916 s | 1.017 s | 1.067 s | 1.017 s |
| 4 | 1M | `GLOBAL INNER ALL JOIN` | 0.766 s | 0.766 s | 0.766 s | 0.766 s |
| 4 | 1M | `allow` | 0.516 s | 0.516 s | 0.566 s | 0.516 s |
| 4 | 2M | `distributed_shuffle_join` | 1.417 s | 1.367 s | 1.367 s | 1.367 s |
| 4 | 2M | `GLOBAL INNER ALL JOIN` | 1.167 s | 1.167 s | 1.217 s | 1.167 s |
| 4 | 2M | `allow` | 0.826 s | 0.818 s | 0.818 s | 0.818 s |
| 4 | 3M | `distributed_shuffle_join` | 1.718 s | 1.571 s | 1.518 s | 1.571 s |
| 4 | 3M | `GLOBAL INNER ALL JOIN` | 2.269 s | 2.420 s | 2.520 s | 2.420 s |
| 4 | 3M | `allow` | 1.769 s | 2.020 s | 1.769 s | 1.769 s |
| 4 | 4M | `distributed_shuffle_join` | 2.070 s | 1.869 s | 1.768 s | 1.869 s |
| 4 | 4M | `GLOBAL INNER ALL JOIN` | 3.072 s | 2.871 s | 2.722 s | 2.871 s |
| 4 | 4M | `allow` | 2.470 s | 2.120 s | 2.070 s | 2.120 s |
| 5 | 1M | `distributed_shuffle_join` | 1.066 s | 1.067 s | 1.117 s | 1.067 s |
| 5 | 1M | `GLOBAL INNER ALL JOIN` | 1.116 s | 1.066 s | 1.117 s | 1.116 s |
| 5 | 1M | `allow` | 0.666 s | 0.716 s | 0.666 s | 0.666 s |
| 5 | 2M | `distributed_shuffle_join` | 1.669 s | 1.668 s | 1.568 s | 1.668 s |
| 5 | 2M | `GLOBAL INNER ALL JOIN` | 1.769 s | 1.768 s | 1.768 s | 1.768 s |
| 5 | 2M | `allow` | 1.067 s | 1.119 s | 1.067 s | 1.067 s |
| 5 | 3M | `distributed_shuffle_join` | 2.020 s | 1.970 s | 1.869 s | 1.970 s |
| 5 | 3M | `GLOBAL INNER ALL JOIN` | 3.573 s | 3.875 s | 3.270 s | 3.573 s |
| 5 | 3M | `allow` | 2.932 s | 2.320 s | 2.379 s | 2.379 s |
| 5 | 4M | `distributed_shuffle_join` | 2.320 s | 2.420 s | 2.420 s | 2.420 s |
| 5 | 4M | `GLOBAL INNER ALL JOIN` | 3.975 s | 3.975 s | 4.025 s | 3.975 s |
| 5 | 4M | `allow` | 2.622 s | 2.521 s | 2.873 s | 2.622 s |

## 结果解读

### crossover 位于约 3M 行

对于 `n=2/3/4`，`1M` 和 `2M` 时 shuffle 都未跑赢两条基线；在 `3M` 时统一反转，并在 `4M` 继续保持优势。`n=5` 因默认路径随表数量增长更快，shuffle 在 `1M/2M` 已略快于 `GLOBAL INNER ALL JOIN`，但仍要到 `3M` 才同时跑赢 `allow`。

在当前窄 payload、3 shards、本地 Docker 和 `Debug` build 条件下，可以把“同时跑赢两条基线”的经验阈值暂时记为每表约 `3M` 行。它不是通用阈值；payload 更宽、网络更慢或 shard 更多时，crossover 预计会提前。

### 表数量越多，`GLOBAL INNER ALL JOIN` 增长越快

`4M` 场景下，`GLOBAL INNER ALL JOIN` 从 `n=2` 的 `0.866s` 增长到 `n=5` 的 `3.975s`；当前 shuffle 从 `0.616s` 增长到 `2.420s`。多张大表需要重复广播或处理更多右侧数据，shuffle 的单目标分桶优势逐渐显现。

### 当前 shuffle 仍有多阶段成本

虽然 `3M/4M` 已经跑赢基线，但当前左深实现仍会执行 `n - 1` 个二元 stage，并物化非最终 stage 的中间结果。后续如果实现同 key multi-way shuffle 或复用上一 stage 的 partitioning，`n=3..5` 的结果仍有优化空间。

## 结果文件

| 文件 | 说明 |
| --- | --- |
| `build/distributed_shuffle_join_large_matrix_comparison_results.json` | 16 组三模式结构化结果 |
| `build/test_distributed_shuffle_join_large_matrix_comparison.log` | 完整 benchmark 日志 |
| `tmp/distributed_shuffle_join_multi_table_perf.py` | 本轮使用的临时 benchmark 脚本 |

`ClickHouseCluster` 的后置 cleanup 在 Docker 容器已经删除后没有自然退出；本轮在 JSON 与实时日志完整写入且 `docker ps` 确认没有残留 benchmark 容器后终止了 Python 收尾进程。查询计时、正确性检查和结果文件均已完成。
