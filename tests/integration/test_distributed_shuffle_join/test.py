import os
import statistics
import time

import pytest

from helpers.cluster import ClickHouseCluster


# 起 6 个 ClickHouse 实例，模拟一个 3-shard、每个 shard 2-replica 的分布式集群。
cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 2, "replica": 1},
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 2, "replica": 2},
)
node5 = cluster.add_instance(
    "node5",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 3, "replica": 1},
)
node6 = cluster.add_instance(
    "node6",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 3, "replica": 2},
)

NODES = {
    "node1": node1,
    "node2": node2,
    "node3": node3,
    "node4": node4,
    "node5": node5,
    "node6": node6,
}

SHARD_REPLICAS = {
    1: (node1, node2),
    2: (node3, node4),
    3: (node5, node6),
}

PRIMARY_REPLICA_BY_SHARD = {
    shard: replicas[0] for shard, replicas in SHARD_REPLICAS.items()
}

MATCHED_IDS = list(range(1, 16))
SHARD_COUNT = 3

# 左右表数据按“逻辑 shard”存放，而不是按 `cityHash64(id) % shard_count` 对应的 bucket shard 预先摆好。
# 因此，worker 上执行的 internal query 必须通过 distributed 子查询跨 shard 拉取同 bucket 数据，才能拼出完整结果。
LEFT_IDS_BY_SHARD = {
    1: [1, 5, 8, 10, 14, 201],
    2: [3, 6, 7, 13, 15, 202],
    3: [2, 4, 9, 11, 12, 203],
}

RIGHT_IDS_BY_SHARD = {
    1: [2, 6, 9, 10, 12, 301],
    2: [3, 7, 8, 13, 15, 302],
    3: [1, 4, 5, 11, 14, 303],
}

# 这里显式写出当前测试数据中，每个匹配 `id` 最终应当落到哪个 bucket shard。
# 这个映射不通过 SQL 现算，而是作为测试数据的一部分固定下来，方便直接做人工核对。
MATCHED_IDS_BY_BUCKET_SHARD = {
    1: [3, 4, 11, 13, 15],
    2: [1, 2, 5, 9, 12, 14],
    3: [6, 7, 8, 10],
}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def build_left_row(row_id):
    # 构造左表的一行宽表数据，除了 join key 之外再带几列业务字段。
    day = (row_id - 1) % 28 + 1
    return (
        row_id,
        f"a{row_id}",
        f"group_{row_id % 4}",
        row_id * 10,
        f"a_note_{row_id}",
        f"2026-01-{day:02d} 00:00:00",
    )


def build_right_row(row_id):
    # 构造右表的一行宽表数据，字段和左表不完全相同，方便验证投影和列绑定。
    day = (row_id - 1) % 28 + 1
    return (
        row_id,
        f"b{row_id}",
        f"bucket_{row_id % 5}",
        row_id * 100,
        f"b_note_{row_id}",
        f"2026-02-{day:02d} 00:00:00",
    )


def format_sql_value(value):
    # 把 Python 值转成可以直接拼进 `INSERT VALUES` 的 SQL 字面量。
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    return str(value)


def insert_rows(node, table_name, rows):
    # 直接往指定 replica 的本地表插数据，明确控制这些行属于哪个逻辑 shard。
    values = ", ".join(
        "(" + ", ".join(format_sql_value(value) for value in row) + ")"
        for row in rows
    )
    node.query(f"INSERT INTO default.{table_name} VALUES {values}")


def create_replicated_local_table(table_name, columns, shard, replica_name):
    columns_sql = ",\n            ".join(columns)
    return f"""
        CREATE TABLE default.{table_name}
        (
            {columns_sql}
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_distributed_shuffle_join/shard_{shard}/{table_name}', '{replica_name}')
        ORDER BY id
    """


def create_tables():
    # 每个节点都建本地表和 `Distributed` 表。
    # 本地表使用 `ReplicatedMergeTree`，这样每个 shard 的两个 replica 持有同一份数据。
    a_columns = [
        "id UInt64",
        "a_val String",
        "a_group String",
        "a_metric Int64",
        "a_note String",
        "a_created_at DateTime",
    ]

    b_columns = [
        "id UInt64",
        "b_val String",
        "b_group String",
        "b_metric Int64",
        "b_note String",
        "b_updated_at DateTime",
    ]

    ddl_a_dist = """
        CREATE TABLE default.a_dist AS default.a_local
        ENGINE = Distributed('test_cluster', 'default', 'a_local', cityHash64(id))
    """

    ddl_b_dist = """
        CREATE TABLE default.b_dist AS default.b_local
        ENGINE = Distributed('test_cluster', 'default', 'b_local', cityHash64(id))
    """

    for node in NODES.values():
        node.query("DROP TABLE IF EXISTS default.a_dist SYNC")
        node.query("DROP TABLE IF EXISTS default.b_dist SYNC")
        node.query("DROP TABLE IF EXISTS default.a_local SYNC")
        node.query("DROP TABLE IF EXISTS default.b_local SYNC")

    for shard, replicas in SHARD_REPLICAS.items():
        for node in replicas:
            node.query(create_replicated_local_table("a_local", a_columns, shard, node.name))
            node.query(create_replicated_local_table("b_local", b_columns, shard, node.name))

    for node in NODES.values():
        node.query(ddl_a_dist)
        node.query(ddl_b_dist)


def sync_replicas():
    for node in NODES.values():
        node.query("SYSTEM SYNC REPLICA default.a_local")
        node.query("SYSTEM SYNC REPLICA default.b_local")


def insert_data():
    # 左右表只先写每个 shard 的一个主 replica，再同步到另一个 replica。
    # 这样既能覆盖多副本场景，也能保持“数据并不是预先按 bucket 存到目标执行 shard 上”的性质。
    for shard, row_ids in LEFT_IDS_BY_SHARD.items():
        insert_rows(PRIMARY_REPLICA_BY_SHARD[shard], "a_local", [build_left_row(row_id) for row_id in row_ids])

    for shard, row_ids in RIGHT_IDS_BY_SHARD.items():
        insert_rows(PRIMARY_REPLICA_BY_SHARD[shard], "b_local", [build_right_row(row_id) for row_id in row_ids])

    sync_replicas()


def reset_state():
    # 每个测试开始前都重建表并重新插数，避免测试之间互相污染。
    create_tables()
    insert_data()


def count_prepositioned_rows(placement_by_shard):
    # 统计测试数据里，有多少匹配行恰好被放到了它们自己的 bucket shard 上。
    # 这里应该为 0，避免测试退化成“本地刚好就有数据”。
    values = ", ".join(
        f"({row_id}, {shard})"
        for shard, row_ids in placement_by_shard.items()
        for row_id in row_ids
        if row_id in MATCHED_IDS
    )

    result = node1.query(
        f"""
            SELECT count()
            FROM values('id UInt64, storage_shard UInt8', {values})
            WHERE storage_shard = cityHash64(id) % {SHARD_COUNT} + 1
        """
    )

    return int(result.strip())


def build_expected_result():
    # 构造 join 之后的期望结果，只包含真正能匹配上的那批 id。
    lines = []

    for row_id in MATCHED_IDS:
        left_row = build_left_row(row_id)
        right_row = build_right_row(row_id)
        lines.append(
            "\t".join(
                [
                    str(row_id),
                    left_row[1],
                    left_row[2],
                    str(left_row[3]),
                    right_row[1],
                    right_row[2],
                    str(right_row[3]),
                ]
            )
        )

    return "\n".join(lines) + "\n"


def build_expected_rows_by_bucket_shard():
    # 构造“每个目标 bucket shard 最终应该拿到哪些 join 结果行”的纯 Python 版本。
    result = {}

    for shard, row_ids in MATCHED_IDS_BY_BUCKET_SHARD.items():
        lines = []

        for row_id in row_ids:
            left_row = build_left_row(row_id)
            right_row = build_right_row(row_id)
            lines.append(
                "\t".join(
                    [
                        str(row_id),
                        left_row[1],
                        left_row[2],
                        str(left_row[3]),
                        right_row[1],
                        right_row[2],
                        str(right_row[3]),
                    ]
                )
            )

        result[shard] = "\n".join(lines)

    return result


def build_join_query(*, join_keyword, settings_clause):
    # 统一生成测试 SQL。
    # `join_keyword` 用来切换普通 `JOIN` 和 `GLOBAL JOIN`；
    # `settings_clause` 用来切换默认、`allow`、`distributed_shuffle_join` 等模式。
    return f"""
        SELECT
            a.id,
            a.a_val,
            a.a_group,
            a.a_metric,
            b.b_val,
            b.b_group,
            b.b_metric
        FROM default.a_dist AS a
        {join_keyword} default.b_dist AS b USING (id)
        ORDER BY id
        SETTINGS {settings_clause}
    """


def measure_query_time(query, runs=3):
    # 简单做 3 次 wall-clock 计时，用来做人工性能对比。
    # 这里只做功能验证级别的对比，不作为严格性能结论。
    result = None
    durations = []

    for _ in range(runs):
        started_at = time.perf_counter()
        result = node1.query(query)
        durations.append(time.perf_counter() - started_at)

    return result, durations


def execute_query_once_with_timing(query):
    started_at = time.perf_counter()
    result = node1.query(query)
    duration = time.perf_counter() - started_at
    return result, duration


def test_default_double_distributed_join_is_denied(started_cluster):
    # Case 1:
    # 默认模式下，double-distributed `JOIN` 应该被拒绝。
    reset_state()

    query = build_join_query(
        join_keyword="JOIN",
        settings_clause="enable_analyzer = 1",
    )

    with pytest.raises(Exception, match="Double-distributed IN/JOIN subqueries is denied"):
        node1.query(query)


def test_shuffle_join_data_requires_subquery_fetch(started_cluster):
    # Case 2:
    # 测试数据没有预先放到 bucket 对应的目标执行 shard 上。
    # 如果 `shuffle join` 最终还能给出完整结果，就说明 internal query 的 distributed 子查询确实跨 shard 拉回了同 bucket 数据。
    assert count_prepositioned_rows(LEFT_IDS_BY_SHARD) == 0
    assert count_prepositioned_rows(RIGHT_IDS_BY_SHARD) == 0


def test_distributed_shuffle_join_returns_expected_result(started_cluster):
    # Case 3:
    # 打开 `distributed_shuffle_join = 1` 后，应该能得到完整正确的 join 结果。
    reset_state()

    query = build_join_query(
        join_keyword="JOIN",
        settings_clause="enable_analyzer = 1, distributed_shuffle_join = 1",
    )

    assert node1.query(query) == build_expected_result()


@pytest.mark.skipif(
    os.environ.get("CLICKHOUSE_RUN_SHUFFLE_RUNTIME_COMPARISON") != "1",
    reason="Set CLICKHOUSE_RUN_SHUFFLE_RUNTIME_COMPARISON=1 to run manual runtime comparison",
)
def test_distributed_shuffle_join_runtime_comparison(started_cluster):
    # Case 4:
    # 手动性能对比：
    # 1. `GLOBAL JOIN`
    # 2. 普通 `allow`
    # 3. `distributed_shuffle_join`
    #
    # 三种模式都先校验结果正确，再打印简单耗时，方便人工对比。
    reset_state()

    global_query = build_join_query(
        join_keyword="GLOBAL JOIN",
        settings_clause="enable_analyzer = 1",
    )

    allow_query = build_join_query(
        join_keyword="JOIN",
        settings_clause="enable_analyzer = 1, distributed_product_mode = 'allow', prefer_global_in_and_join = 0",
    )

    shuffle_query = build_join_query(
        join_keyword="JOIN",
        settings_clause="enable_analyzer = 1, distributed_shuffle_join = 1",
    )

    expected_result = build_expected_result()
    expected_rows_by_bucket_shard = build_expected_rows_by_bucket_shard()

    # 先预热一次，避免第一次执行把建连接等一次性开销也算进去。
    node1.query(global_query)
    node1.query(allow_query)
    node1.query(shuffle_query)

    global_result, global_durations = measure_query_time(global_query)
    allow_result, allow_durations = measure_query_time(allow_query)
    shuffle_result, shuffle_durations = measure_query_time(shuffle_query)

    assert global_result == expected_result
    assert allow_result == expected_result
    assert shuffle_result == expected_result

    print("Expected rows by bucket shard:")
    for shard in sorted(expected_rows_by_bucket_shard):
        print(f"[bucket shard {shard}]")
        print(expected_rows_by_bucket_shard[shard])

    print(
        "GLOBAL JOIN average: "
        f"{statistics.mean(global_durations):.6f}s "
        f"(runs={', '.join(f'{value:.6f}' for value in global_durations)})"
    )
    print(
        "allow average: "
        f"{statistics.mean(allow_durations):.6f}s "
        f"(runs={', '.join(f'{value:.6f}' for value in allow_durations)})"
    )
    print(
        "distributed_shuffle_join average: "
        f"{statistics.mean(shuffle_durations):.6f}s "
        f"(runs={', '.join(f'{value:.6f}' for value in shuffle_durations)})"
    )

    print("Actual query results and single-run timings:")
    for label, query in [
        ("GLOBAL JOIN", global_query),
        ("allow", allow_query),
        ("distributed_shuffle_join", shuffle_query),
    ]:
        result, duration = execute_query_once_with_timing(query)
        print(f"[{label}] single run: {duration:.6f}s")
        print(result)
