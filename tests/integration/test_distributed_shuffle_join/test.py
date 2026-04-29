import json
import os

import pytest

from helpers.cluster import ClickHouseCluster


# 起 3 个 ClickHouse 实例，模拟一个 3-shard、每个 shard 1-replica 的分布式集群。
cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": 2},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": 3},
)

NODES = {
    "node1": node1,
    "node2": node2,
    "node3": node3,
}

SHARD_NODES = {
    1: node1,
    2: node2,
    3: node3,
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
    # 直接往指定 shard 的本地表插数据，明确控制这些行属于哪个逻辑 shard。
    values = ", ".join(
        "(" + ", ".join(format_sql_value(value) for value in row) + ")"
        for row in rows
    )
    node.query(f"INSERT INTO default.{table_name} VALUES {values}")


def create_local_table(table_name, columns):
    columns_sql = ",\n            ".join(columns)
    return f"""
        CREATE TABLE default.{table_name}
        (
            {columns_sql}
        )
        ENGINE = MergeTree()
        ORDER BY id
    """


def create_tables():
    # 每个节点都建本地表和 `Distributed` 表。
    # 本地表使用 `MergeTree`，每个 shard 只有一个副本。
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

    for node in SHARD_NODES.values():
        node.query(create_local_table("a_local", a_columns))
        node.query(create_local_table("b_local", b_columns))

    for node in NODES.values():
        node.query(ddl_a_dist)
        node.query(ddl_b_dist)


def sync_replicas():
    return None


def insert_data():
    # 左右表只写到每个 shard 的唯一副本上。
    # 这样依然保持“数据并不是预先按 bucket 存到目标执行 shard 上”的性质。
    for shard, row_ids in LEFT_IDS_BY_SHARD.items():
        insert_rows(SHARD_NODES[shard], "a_local", [build_left_row(row_id) for row_id in row_ids])

    for shard, row_ids in RIGHT_IDS_BY_SHARD.items():
        insert_rows(SHARD_NODES[shard], "b_local", [build_right_row(row_id) for row_id in row_ids])

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


def build_shuffle_join_query():
    # 统一生成当前测试唯一关注的 `shuffle join` SQL。
    return """
        SELECT
            a.id,
            a.a_val,
            a.a_group,
            a.a_metric,
            b.b_val,
            b.b_group,
            b.b_metric
        FROM default.a_dist AS a
        JOIN default.b_dist AS b USING (id)
        ORDER BY id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """


def build_trace_query_id():
    return f"distributed_shuffle_join_trace_{os.getpid()}"


def flush_query_logs():
    for node in NODES.values():
        node.query("SYSTEM FLUSH LOGS")


def collect_query_log_rows(node, initial_query_id):
    result = node.query(
        f"""
            SELECT
                query_id,
                initial_query_id,
                is_initial_query,
                read_rows,
                result_rows,
                query
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND initial_query_id = '{initial_query_id}'
            ORDER BY event_time_microseconds, query_id
            FORMAT JSONEachRow
        """
    )

    return [json.loads(line) for line in result.splitlines() if line.strip()]


def collect_query_log_rows_by_node(initial_query_id):
    return {
        node_name: collect_query_log_rows(node, initial_query_id)
        for node_name, node in NODES.items()
    }


def print_query_log_rows_by_node(rows_by_node):
    print("Observed query_log rows by node:")

    for node_name in sorted(rows_by_node):
        print(f"[{node_name}]")

        for row in rows_by_node[node_name]:
            print(f"query_id={row['query_id']}")
            print(f"initial_query_id={row['initial_query_id']}")
            print(f"is_initial_query={row['is_initial_query']}")
            print(f"read_rows={row['read_rows']}")
            print(f"result_rows={row['result_rows']}")
            print(f"query={row['query']}")
            print()


def collect_non_initial_query_rows(rows_by_node):
    return [
        row
        for rows in rows_by_node.values()
        for row in rows
        if not row["is_initial_query"]
    ]


def collect_local_fetch_rows(rows_by_node):
    local_table_markers = (
        "FROM `default`.`a_local`",
        "FROM `default`.`b_local`",
    )

    return [
        row
        for row in collect_non_initial_query_rows(rows_by_node)
        if any(marker in row["query"] for marker in local_table_markers)
    ]


def test_shuffle_join_data_requires_subquery_fetch(started_cluster):
    # Case 1:
    # 测试数据没有预先放到 bucket 对应的目标执行 shard 上。
    # 如果 `shuffle join` 最终还能给出完整结果，就说明 internal query 的 distributed 子查询确实跨 shard 拉回了同 bucket 数据。
    assert count_prepositioned_rows(LEFT_IDS_BY_SHARD) == 0
    assert count_prepositioned_rows(RIGHT_IDS_BY_SHARD) == 0


def test_distributed_shuffle_join_returns_expected_result(started_cluster):
    # Case 2:
    # 打开 `distributed_shuffle_join = 1` 后，应该能得到完整正确的 join 结果。
    reset_state()

    assert node1.query(build_shuffle_join_query()) == build_expected_result()


@pytest.mark.skipif(
    os.environ.get("CLICKHOUSE_RUN_SHUFFLE_RUNTIME_COMPARISON") != "1",
    reason="Set CLICKHOUSE_RUN_SHUFFLE_RUNTIME_COMPARISON=1 to print manual shuffle trace",
)
def test_distributed_shuffle_join_runtime_comparison(started_cluster):
    # Case 3:
    # 手动 trace：
    # 1. 只跑一次 `distributed_shuffle_join`
    # 2. 用 `system.query_log` 打印各节点实际收到的 SQL
    # 3. 用于人工核对 `shuffle` 内部查询是否按 bucket 下发
    reset_state()

    shuffle_query = build_shuffle_join_query()
    trace_query_id = build_trace_query_id()
    expected_result = build_expected_result()
    expected_rows_by_bucket_shard = build_expected_rows_by_bucket_shard()

    shuffle_result = node1.query(
        shuffle_query,
        query_id=trace_query_id,
        settings={"log_queries": 1},
    )
    flush_query_logs()
    rows_by_node = collect_query_log_rows_by_node(trace_query_id)

    assert shuffle_result == expected_result
    assert any(
        row["is_initial_query"] and "distributed_shuffle_join = 1" in row["query"]
        for row in rows_by_node["node1"]
    )
    assert any(not row["is_initial_query"] for row in rows_by_node["node2"])
    assert any(not row["is_initial_query"] for row in rows_by_node["node3"])

    worker_rows = collect_non_initial_query_rows(rows_by_node)
    local_fetch_rows = collect_local_fetch_rows(rows_by_node)

    for bucket in range(SHARD_COUNT):
        assert any(
            "modulo(cityHash64" in row["query"] and f"_CAST({bucket}, 'UInt64')" in row["query"]
            for row in worker_rows
        )
        assert any(
            f"FROM `default`.`a_local`" in row["query"] and f"_CAST({bucket}, 'UInt64')" in row["query"]
            for row in local_fetch_rows
        )
        assert any(
            f"FROM `default`.`b_local`" in row["query"] and f"_CAST({bucket}, 'UInt64')" in row["query"]
            for row in local_fetch_rows
        )

    assert local_fetch_rows
    assert all("modulo(cityHash64" in row["query"] for row in local_fetch_rows)

    print("Expected rows by bucket shard:")
    for shard in sorted(expected_rows_by_bucket_shard):
        print(f"[bucket shard {shard} / modulo {shard - 1}]")
        print(expected_rows_by_bucket_shard[shard])
    print(f"Trace query_id: {trace_query_id}")
    print_query_log_rows_by_node(rows_by_node)
