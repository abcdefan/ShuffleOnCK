import pytest

from helpers.cluster import ClickHouseCluster


# 复用当前目录下的 `remote_servers.xml`，起 6 个节点构造一个 3-shard、每个 shard 2-replica 的集群。
cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"])
node4 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"])
node5 = cluster.add_instance("node5", main_configs=["configs/remote_servers.xml"])
node6 = cluster.add_instance("node6", main_configs=["configs/remote_servers.xml"])

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

DATABASE = "test_lwh"

# 左右表都包含 1..10 这批能够成功匹配的 id，但故意分布在不同 shard 上。
# 同一个 shard 的两个 replica 写入完全相同的数据，保证任意 replica 被选中时结果一致。
LEFT_ROWS_BY_SHARD = {
    1: [(1, "a1"), (4, "a4"), (7, "a7"), (10, "a10"), (201, "left_noise_201")],
    2: [(2, "a2"), (5, "a5"), (8, "a8"), (202, "left_noise_202")],
    3: [(3, "a3"), (6, "a6"), (9, "a9"), (203, "left_noise_203")],
}

RIGHT_ROWS_BY_SHARD = {
    1: [(2, "n2"), (5, "n5"), (8, "n8"), (301, "right_noise_301")],
    2: [(3, "n3"), (6, "n6"), (9, "n9"), (302, "right_noise_302")],
    3: [(1, "n1"), (4, "n4"), (7, "n7"), (10, "n10"), (303, "right_noise_303")],
}

MATCHED_IDS = list(range(1, 11))


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def format_sql_value(value):
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    return str(value)


def insert_rows(node, table_name, rows):
    values = ", ".join(
        "(" + ", ".join(format_sql_value(value) for value in row) + ")"
        for row in rows
    )
    node.query(f"INSERT INTO {DATABASE}.{table_name} VALUES {values}")


def create_tables():
    for node in NODES.values():
        node.query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    ddl_local_t1 = f"""
        CREATE TABLE {DATABASE}.local_t1
        (
            id UInt64,
            value String
        )
        ENGINE = MergeTree
        ORDER BY id
    """

    ddl_local_t2 = f"""
        CREATE TABLE {DATABASE}.local_t2
        (
            id UInt64,
            name String
        )
        ENGINE = MergeTree
        ORDER BY id
    """

    ddl_dist_t1 = f"""
        CREATE TABLE {DATABASE}.dist_t1 AS {DATABASE}.local_t1
        ENGINE = Distributed('test_cluster', '{DATABASE}', 'local_t1', cityHash64(id))
    """

    ddl_dist_t2 = f"""
        CREATE TABLE {DATABASE}.dist_t2 AS {DATABASE}.local_t2
        ENGINE = Distributed('test_cluster', '{DATABASE}', 'local_t2', cityHash64(id))
    """

    for node in NODES.values():
        node.query(f"DROP TABLE IF EXISTS {DATABASE}.dist_t1 SYNC")
        node.query(f"DROP TABLE IF EXISTS {DATABASE}.dist_t2 SYNC")
        node.query(f"DROP TABLE IF EXISTS {DATABASE}.local_t1 SYNC")
        node.query(f"DROP TABLE IF EXISTS {DATABASE}.local_t2 SYNC")

        node.query(ddl_local_t1)
        node.query(ddl_local_t2)
        node.query(ddl_dist_t1)
        node.query(ddl_dist_t2)


def insert_data():
    for shard, rows in LEFT_ROWS_BY_SHARD.items():
        for node in SHARD_REPLICAS[shard]:
            insert_rows(node, "local_t1", rows)

    for shard, rows in RIGHT_ROWS_BY_SHARD.items():
        for node in SHARD_REPLICAS[shard]:
            insert_rows(node, "local_t2", rows)


def reset_state():
    create_tables()
    insert_data()


def build_expected_join_result():
    return "".join(f"{row_id}\ta{row_id}\tn{row_id}\n" for row_id in MATCHED_IDS)


def build_expected_left_join_result():
    return build_expected_join_result()


def build_expected_where_result():
    return "".join(f"{row_id}\ta{row_id}\tn{row_id}\n" for row_id in range(6, 11))


def build_on_query(*, join_keyword="JOIN", where_clause="", settings_clause="enable_analyzer = 1"):
    return f"""
        SELECT a.id, a.value, b.name
        FROM {DATABASE}.dist_t1 AS a
        {join_keyword} {DATABASE}.dist_t2 AS b ON a.id = b.id
        {where_clause}
        ORDER BY a.id
        SETTINGS {settings_clause}
    """


def build_using_query(*, join_keyword="JOIN", where_clause="", settings_clause="enable_analyzer = 1"):
    return f"""
        SELECT a.id, a.value, b.name
        FROM {DATABASE}.dist_t1 AS a
        {join_keyword} {DATABASE}.dist_t2 AS b USING (id)
        {where_clause}
        ORDER BY a.id
        SETTINGS {settings_clause}
    """


def test_lwh_basic_distributed_table_query(started_cluster):
    # 对应文档 Test 1：先确认分布式表基础读是通的。
    reset_state()

    result = node1.query(
        f"""
            SELECT count()
            FROM {DATABASE}.dist_t1
            SETTINGS enable_analyzer = 1
        """
    )

    assert result == "13\n"


def test_lwh_allow_join_baseline(started_cluster):
    # 对应文档 Test 2：普通 `allow` 作为基准对照。
    reset_state()

    query = build_on_query(
        settings_clause="enable_analyzer = 1, distributed_product_mode = 'allow', prefer_global_in_and_join = 0",
    )

    assert node1.query(query) == build_expected_join_result()


def test_lwh_shuffle_join_core(started_cluster):
    # 对应文档 Test 3：核心 `shuffle join` 场景。
    reset_state()

    query = build_on_query(
        settings_clause="enable_analyzer = 1, distributed_shuffle_join = 1",
    )

    assert node1.query(query) == build_expected_join_result()


def test_lwh_shuffle_join_with_using(started_cluster):
    # 对应文档 Test 4：`USING` 语法。
    reset_state()

    query = build_using_query(
        settings_clause="enable_analyzer = 1, distributed_shuffle_join = 1",
    )

    assert node1.query(query) == build_expected_join_result()


def test_lwh_shuffle_left_join(started_cluster):
    # 对应文档 Test 5：`LEFT JOIN`。
    # 当前匹配 id 1..10 在两边都存在，所以结果应与普通 inner join 相同。
    reset_state()

    query = build_on_query(
        join_keyword="LEFT JOIN",
        settings_clause="enable_analyzer = 1, distributed_shuffle_join = 1",
    )

    assert node1.query(query) == build_expected_left_join_result()


def test_lwh_shuffle_join_count(started_cluster):
    # 对应文档 Test 6：聚合场景。
    reset_state()

    result = node1.query(
        f"""
            SELECT count()
            FROM {DATABASE}.dist_t1 AS a
            JOIN {DATABASE}.dist_t2 AS b ON a.id = b.id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """
    )

    assert result == "10\n"


def test_lwh_shuffle_join_with_where(started_cluster):
    # 对应文档 Test 7：带额外 `WHERE` 条件的 join。
    reset_state()

    query = build_on_query(
        where_clause="WHERE a.id > 5",
        settings_clause="enable_analyzer = 1, distributed_shuffle_join = 1",
    )

    assert node1.query(query) == build_expected_where_result()
