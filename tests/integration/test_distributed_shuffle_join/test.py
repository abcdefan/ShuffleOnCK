import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS left_dist")
            node.query("DROP TABLE IF EXISTS right_dist")
            node.query("DROP TABLE IF EXISTS left_local")
            node.query("DROP TABLE IF EXISTS right_local")

            node.query(
                "CREATE TABLE left_local (id UInt64, left_value String) ENGINE = Memory"
            )
            node.query(
                "CREATE TABLE right_local (id UInt64, right_value String) ENGINE = Memory"
            )
            node.query(
                "CREATE TABLE left_dist AS left_local "
                "ENGINE = Distributed(shuffle_join_cluster, default, left_local, rand())"
            )
            node.query(
                "CREATE TABLE right_dist AS right_local "
                "ENGINE = Distributed(shuffle_join_cluster, default, right_local, rand())"
            )

        node1.query("INSERT INTO left_local VALUES (1, 'l1_node1'), (2, 'l2_node1')")
        node2.query("INSERT INTO left_local VALUES (3, 'l3_node2'), (4, 'l4_node2')")

        node1.query("INSERT INTO right_local VALUES (3, 'r3_node1'), (4, 'r4_node1')")
        node2.query("INSERT INTO right_local VALUES (1, 'r1_node2'), (2, 'r2_node2')")

        yield cluster
    finally:
        cluster.shutdown()


def sorted_tsv(result):
    return sorted(line for line in result.strip().splitlines() if line)


def assert_no_shuffle_tables():
    for node in (node1, node2):
        assert node.query("SHOW TABLES LIKE '_shuffle_%'") == ""


def test_inner_all_join_uses_push_based_shuffle(started_cluster):
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    result = node1.query(query)

    assert sorted_tsv(result) == [
        "1\tl1_node1\tr1_node2",
        "2\tl2_node1\tr2_node2",
        "3\tl3_node2\tr3_node1",
        "4\tl4_node2\tr4_node1",
    ]
    assert_no_shuffle_tables()


def test_inner_all_join_pushes_single_side_where_filters(started_cluster):
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        WHERE l.id >= 2 AND r.id <= 3
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    result = node1.query(query)

    assert sorted_tsv(result) == [
        "2\tl2_node1\tr2_node2",
        "3\tl3_node2\tr3_node1",
    ]
    assert_no_shuffle_tables()
