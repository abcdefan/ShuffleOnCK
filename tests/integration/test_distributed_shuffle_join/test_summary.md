# `distributed_shuffle_join` 测试总结

## 1. 测试环境

- **测试文件**: `tests/integration/test_distributed_shuffle_join/test.py`
- **运行方式**: ClickHouse `integration test`
- **操作系统**: Linux
- **ClickHouse 二进制**: 本地源码编译产物 `build/programs/clickhouse`
- **集群拓扑**: `3 shard`，每个 `shard` `2 replica`
  - `shard 1`: `node1`, `node2`
  - `shard 2`: `node3`, `node4`
  - `shard 3`: `node5`, `node6`
- **集群名称**: `test_cluster`
- **配置文件**: `tests/integration/test_distributed_shuffle_join/configs/remote_servers.xml`

## 2. 测试目标

验证 `distributed_shuffle_join` 在双分布式表 `JOIN` 场景下的正确性，以及与普通 `allow` / `GLOBAL JOIN` 的结果和简单耗时对比。

重点验证：

- 默认双分布式 `JOIN` 是否仍然被拒绝
- 打开 `distributed_shuffle_join = 1` 后结果是否正确
- 宽表场景下列绑定和投影是否正确
- 数据跨 `shard` 打散时是否还能得到完整结果
- 测试数据是否确实依赖 internal query 的 distributed 子查询跨 `shard` 拉取数据
- `GLOBAL JOIN`、普通 `allow`、`distributed_shuffle_join` 三条路径的结果和简单耗时对比

## 3. 测试数据设计

### 3.1 表结构

测试中构造了两张本地表和两张分布式表：

- `a_local`
- `b_local`
- `a_dist`
- `b_dist`

其中：

- `a_local` / `b_local` 使用 `ReplicatedMergeTree`
- 同一个 `shard` 的两个 `replica` 保存同一份本地数据
- `a_dist` / `b_dist` 基于 `test_cluster` 访问整个集群

左表 `a_local` 字段：

- `id`
- `a_val`
- `a_group`
- `a_metric`
- `a_note`
- `a_created_at`

右表 `b_local` 字段：

- `id`
- `b_val`
- `b_group`
- `b_metric`
- `b_note`
- `b_updated_at`

测试不是最小两列表，而是宽表，用来验证 rewrite 后的列绑定和输出头部是否正确。

### 3.2 数据分布

左右表数据按“逻辑 `shard`”分布，同一个 `shard` 的两个 `replica` 保存同一份本地数据。

同时，这批匹配数据**不是**预先按 `cityHash64(id) % shard_count` 对应的目标执行 `shard` 摆放好的，而是故意打散到不同 `shard`。这样如果最终 `distributed_shuffle_join` 仍然能返回完整正确结果，就说明 worker 上的 internal query 确实通过 distributed 子查询跨 `shard` 拉取了同 `bucket` 数据。

左表分布（按逻辑 `shard`）：

- `shard 1`: `1, 5, 8, 10, 14`，外加噪声行 `201`
- `shard 2`: `3, 6, 7, 13, 15`，外加噪声行 `202`
- `shard 3`: `2, 4, 9, 11, 12`，外加噪声行 `203`

右表分布（按逻辑 `shard`）：

- `shard 1`: `2, 6, 9, 10, 12`，外加噪声行 `301`
- `shard 2`: `3, 7, 8, 13, 15`，外加噪声行 `302`
- `shard 3`: `1, 4, 5, 11, 14`，外加噪声行 `303`

能够成功匹配的 `id` 为：

- `1..15`

额外噪声行只存在单边，用于验证结果中不会错误出现不匹配数据。

### 3.3 目标 `bucket shard` 预期结果

测试中还通过纯 Python 逻辑构造了“每个目标 `bucket shard` 最终应该拿到哪些匹配结果行”的预期结果，用于人工核对日志输出。

预期分布如下：

- `bucket shard 1`: `3, 4, 11, 13, 15`
- `bucket shard 2`: `1, 2, 5, 9, 12, 14`
- `bucket shard 3`: `6, 7, 8, 10`

这里的 `bucket shard` 是按 `cityHash64(id) % 3` 计算后，再映射为人类可读的 `1..3` 编号。

## 4. 测试用例

### Case 1: 默认双分布式 `JOIN` 被拒绝

SQL 形态：

```sql
SELECT ...
FROM a_dist AS a
JOIN b_dist AS b USING (id)
SETTINGS enable_analyzer = 1
```

验证点：

- 默认情况下，双分布式 `JOIN` 仍然走原有保护逻辑
- 预期抛出：
  - `Double-distributed IN/JOIN subqueries is denied`

实际结果：

- 通过

### Case 2: 测试结果依赖子查询跨 `shard` 拉取数据

验证点：

- 测试数据没有预先放到各自 `bucket` 对应的目标执行 `shard` 上
- 如果 `distributed_shuffle_join` 最终仍然返回完整正确结果，说明 worker 上的 internal query 确实通过 distributed 子查询跨 `shard` 拉取了同 `bucket` 数据

实际结果：

- 通过

### Case 3: `distributed_shuffle_join` 返回正确结果

SQL 形态：

```sql
SELECT
    a.id,
    a.a_val,
    a.a_group,
    a.a_metric,
    b.b_val,
    b.b_group,
    b.b_metric
FROM a_dist AS a
JOIN b_dist AS b USING (id)
ORDER BY id
SETTINGS
    enable_analyzer = 1,
    distributed_shuffle_join = 1
```

验证点：

- 打开 `distributed_shuffle_join = 1` 后，双分布式 `JOIN` 可以执行
- 返回结果应覆盖全部匹配 `id = 1..15`
- 不应出现重复行
- 不应出现噪声行
- 宽表列输出应正确

实际结果：

- 通过

### Case 4: 手动性能对比

该测试为手动触发，需设置环境变量：

```bash
CLICKHOUSE_RUN_SHUFFLE_RUNTIME_COMPARISON=1
```

对比三条路径：

1. `GLOBAL JOIN`
2. 普通 `allow`
3. `distributed_shuffle_join`

#### 4.1 `GLOBAL JOIN`

SQL 形态：

```sql
... GLOBAL JOIN ...
SETTINGS enable_analyzer = 1
```

#### 4.2 普通 `allow`

SQL 形态：

```sql
... JOIN ...
SETTINGS
    enable_analyzer = 1,
    distributed_product_mode = 'allow',
    prefer_global_in_and_join = 0
```

这里显式关闭 `prefer_global_in_and_join`，避免被隐式改写成 `GLOBAL JOIN`。

#### 4.3 `distributed_shuffle_join`

SQL 形态：

```sql
... JOIN ...
SETTINGS
    enable_analyzer = 1,
    distributed_shuffle_join = 1
```

验证点：

- 三种模式结果都应与预期结果一致
- 打印简单 wall-clock 耗时，做功能验证级别的对比
- 打印三条 SQL 的完整结果，方便人工核对

实际结果：

- 三条路径都返回了正确结果
- 测试通过

## 5. 测试结果

### 5.1 普通功能验证

执行结果：

- `test_default_double_distributed_join_is_denied`: `PASSED`
- `test_shuffle_join_data_requires_subquery_fetch`: `PASSED`
- `test_distributed_shuffle_join_returns_expected_result`: `PASSED`
- `test_distributed_shuffle_join_runtime_comparison`: `SKIPPED`

汇总：

- `3 passed, 1 skipped`

其中 `runtime comparison` 被跳过属于预期行为，因为未设置 `CLICKHOUSE_RUN_SHUFFLE_RUNTIME_COMPARISON=1`。

### 5.2 手动性能对比结果

执行结果：

- `test_distributed_shuffle_join_runtime_comparison`: `PASSED`

耗时数据：

- `GLOBAL JOIN average`: `0.064094s`
- `allow average`: `0.114673s`
- `distributed_shuffle_join average`: `0.114305s`

单次执行耗时：

- `GLOBAL JOIN`: `0.114390s`
- `allow`: `0.114497s`
- `distributed_shuffle_join`: `0.114466s`

本次样本下：

- `GLOBAL JOIN` 更快
- 普通 `allow` 与 `distributed_shuffle_join` 基本一致

说明：

- 这是当前这组本地小规模样本上的结果
- 只能作为功能验证级别的性能观测
- 不能直接当成稳定性能结论

## 6. 测试结论

基于 `test.py` 的当前 `integration` 测试结果，可以确认：

1. 默认双分布式 `JOIN` 仍然会被拒绝。
2. 打开 `distributed_shuffle_join = 1` 后，双分布式 `JOIN` 可以正确执行。
3. 在宽表和跨 `shard` 打散数据场景下，结果正确，无重复、无缺失。
4. 测试数据没有预先放到各自 `bucket` 对应的目标执行 `shard` 上，因此当前结果正确性说明 worker 上的 internal query 确实通过 distributed 子查询跨 `shard` 拉回了同 `bucket` 数据。
5. 结果中不会错误包含只存在单边的噪声行。
6. `GLOBAL JOIN`、普通 `allow`、`distributed_shuffle_join` 三条路径都能返回相同正确结果。
7. 当前这组小规模样本下，`allow` 和 `distributed_shuffle_join` 的耗时接近，`GLOBAL JOIN` 更快。

## 7. 当前覆盖范围

当前 `test.py` 已覆盖：

- 默认拒绝路径
- `INNER JOIN`
- `USING`
- 宽表输出
- 打散数据分布
- `3 shard x 2 replica` 多副本场景
- 测试数据不预先落在目标 `bucket shard`
- 手动 `runtime comparison`
- `GLOBAL JOIN`
- 普通 `allow`
- `distributed_shuffle_join`

当前 `test.py` 不包含：

- `LEFT JOIN`
- 非等值 `JOIN`
- 更大规模性能压测
