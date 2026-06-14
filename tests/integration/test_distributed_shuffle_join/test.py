import pytest
import threading
import time
import uuid

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], stay_alive=True
)


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


def query_log_profile_event_sum(node, query_id, event_name):
    return int(
        node.query(
            f"""
            SELECT sum(ProfileEvents['{event_name}'])
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND startsWith(query_id, '{query_id}:shuffle:')
            """
        ).strip()
    )


def query_log_source_query_count(node, query_id, side, query_condition):
    return int(
        node.query(
            f"""
            SELECT count()
            FROM system.query_log
            WHERE type = 'QueryStart'
              AND startsWith(query_id, '{query_id}:shuffle:')
              AND endsWith(query_id, ':source:{side}')
              AND {query_condition}
            """
        ).strip()
    )


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


def test_inner_all_join_using_key_uses_push_based_shuffle(started_cluster):
    query = """
        SELECT id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r USING (id)
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


def test_left_deep_three_table_join_uses_multi_stage_shuffle(started_cluster):
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS left_deep_a_dist")
        node.query("DROP TABLE IF EXISTS left_deep_b_dist")
        node.query("DROP TABLE IF EXISTS left_deep_c_dist")
        node.query("DROP TABLE IF EXISTS left_deep_a_local")
        node.query("DROP TABLE IF EXISTS left_deep_b_local")
        node.query("DROP TABLE IF EXISTS left_deep_c_local")

        node.query(
            "CREATE TABLE left_deep_a_local "
            "(id UInt64, a_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep_b_local "
            "(id UInt64, bucket UInt64, b_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep_c_local "
            "(bucket UInt64, c_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep_a_dist AS left_deep_a_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep_a_local, rand())"
        )
        node.query(
            "CREATE TABLE left_deep_b_dist AS left_deep_b_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep_b_local, rand())"
        )
        node.query(
            "CREATE TABLE left_deep_c_dist AS left_deep_c_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep_c_local, rand())"
        )

    try:
        node1.query(
            "INSERT INTO left_deep_a_local VALUES "
            "(1, 'a1_node1'), (2, 'a2_node1')"
        )
        node2.query(
            "INSERT INTO left_deep_a_local VALUES "
            "(3, 'a3_node2'), (4, 'a4_node2')"
        )

        node1.query(
            "INSERT INTO left_deep_b_local VALUES "
            "(3, 30, 'b3_node1'), (4, 40, 'b4_node1')"
        )
        node2.query(
            "INSERT INTO left_deep_b_local VALUES "
            "(1, 10, 'b1_node2'), (2, 20, 'b2_node2')"
        )

        node1.query(
            "INSERT INTO left_deep_c_local VALUES "
            "(20, 'c20_node1'), (40, 'c40_node1')"
        )
        node2.query(
            "INSERT INTO left_deep_c_local VALUES "
            "(10, 'c10_node2'), (30, 'c30_node2')"
        )

        query_id = f"shuffle_left_deep_scan_{uuid.uuid4().hex}"
        query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
                log_queries = 1, log_queries_min_type = 'QUERY_START'
        """

        assert node1.query(query, query_id=query_id).splitlines() == [
            "1\ta1_node1\tb1_node2\tc10_node2",
            "2\ta2_node1\tb2_node2\tc20_node1",
            "3\ta3_node2\tb3_node1\tc30_node2",
            "4\ta4_node2\tb4_node1\tc40_node1",
        ]

        for node in (node1, node2):
            node.query("SYSTEM FLUSH LOGS query_log")
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "left",
                    "position(query, 'FROM default.left_deep_a_local') > 0",
                )
                == 1
            )
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "right",
                    "position(query, 'FROM default.left_deep_b_local') > 0",
                )
                == 1
            )
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "left",
                    "position(query, '_output') > 0",
                )
                == 1
            )
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "right",
                    "position(query, 'FROM default.left_deep_c_local') > 0",
                )
                == 1
            )
        assert_no_shuffle_tables()

        filtered_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            WHERE a.id >= 2 AND c.bucket != 30 AND a.id + c.bucket >= 30
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(filtered_query).splitlines() == [
            "4\ta4_node2\tb4_node1\tc40_node1",
        ]
        assert_no_shuffle_tables()

        previous_stage_right_filter_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            WHERE b.b_value = 'b2_node2'
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(previous_stage_right_filter_query).splitlines() == [
            "2\ta2_node1\tb2_node2\tc20_node1",
        ]
        assert_no_shuffle_tables()

        filter_pushdown_query_id = f"shuffle_left_deep_filter_pushdown_{uuid.uuid4().hex}"
        filter_pushdown_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            WHERE a.id >= 2 AND b.b_value = 'b2_node2' AND c.bucket = 20
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
                log_queries = 1, log_queries_min_type = 'QUERY_START'
        """

        assert node1.query(
            filter_pushdown_query, query_id=filter_pushdown_query_id
        ).splitlines() == [
            "2\ta2_node1\tb2_node2\tc20_node1",
        ]

        for node in (node1, node2):
            node.query("SYSTEM FLUSH LOGS query_log")
            assert (
                query_log_source_query_count(
                    node,
                    filter_pushdown_query_id,
                    "right",
                    "position(query, 'FROM default.left_deep_b_local') > 0 AND position(query, 'WHERE') > 0",
                )
                == 1
            )
            assert (
                query_log_source_query_count(
                    node,
                    filter_pushdown_query_id,
                    "right",
                    "position(query, 'FROM default.left_deep_c_local') > 0 AND position(query, 'WHERE') > 0",
                )
                == 1
            )
        assert_no_shuffle_tables()

        pruned_filter_column_query = """
            SELECT a.id AS id
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            WHERE b.b_value = 'b2_node2'
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(pruned_filter_column_query).splitlines() == [
            "2",
        ]
        assert_no_shuffle_tables()

        plain_join_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(plain_join_query).splitlines() == [
            "1\ta1_node1\tb1_node2\tc10_node2",
            "2\ta2_node1\tb2_node2\tc20_node1",
            "3\ta3_node2\tb3_node1\tc30_node2",
            "4\ta4_node2\tb4_node1\tc40_node1",
        ]
        assert_no_shuffle_tables()

        expression_projection_query = """
            SELECT
                a.id AS id,
                concat(a.a_value, ':', b.b_value, ':', c.c_value) AS joined_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(expression_projection_query).splitlines() == [
            "1\ta1_node1:b1_node2:c10_node2",
            "2\ta2_node1:b2_node2:c20_node1",
            "3\ta3_node2:b3_node1:c30_node2",
            "4\ta4_node2:b4_node1:c40_node1",
        ]
        assert_no_shuffle_tables()

        global_order_limit_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id DESC
            LIMIT 2 OFFSET 1
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(global_order_limit_query).splitlines() == [
            "3\ta3_node2\tb3_node1\tc30_node2",
            "2\ta2_node1\tb2_node2\tc20_node1",
        ]
        assert_no_shuffle_tables()

        hidden_order_limit_query = """
            SELECT
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY a.id DESC
            LIMIT 2 OFFSET 1
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(hidden_order_limit_query).splitlines() == [
            "a3_node2\tb3_node1\tc30_node2",
            "a2_node1\tb2_node2\tc20_node1",
        ]
        assert_no_shuffle_tables()

        limit_with_ties_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY modulo(a.id, 2) ASC
            LIMIT 1 WITH TIES
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert sorted_tsv(node1.query(limit_with_ties_query)) == [
            "2\ta2_node1\tb2_node2\tc20_node1",
            "4\ta4_node2\tb4_node1\tc40_node1",
        ]
        assert_no_shuffle_tables()

        duplicate_column_query = """
            SELECT
                a.id AS a_id,
                b.id AS b_id,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY a_id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(duplicate_column_query).splitlines() == [
            "1\t1\tc10_node2",
            "2\t2\tc20_node1",
            "3\t3\tc30_node2",
            "4\t4\tc40_node1",
        ]
        assert_no_shuffle_tables()

        duplicate_column_filter_query = """
            SELECT
                a.id AS a_id,
                b.id AS b_id,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            WHERE a.id + b.id = 4
            ORDER BY a_id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(duplicate_column_filter_query).splitlines() == [
            "2\t2\tc20_node1",
        ]
        assert_no_shuffle_tables()

        early_post_join_filter_pruned_query = """
            SELECT c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            WHERE a.id + b.id = 4
            ORDER BY c_value
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(early_post_join_filter_pruned_query).splitlines() == [
            "c20_node1",
        ]
        assert_no_shuffle_tables()

        using_join_query = """
            SELECT
                id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b USING (id)
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(using_join_query).splitlines() == [
            "1\ta1_node1\tb1_node2\tc10_node2",
            "2\ta2_node1\tb2_node2\tc20_node1",
            "3\ta3_node2\tb3_node1\tc30_node2",
            "4\ta4_node2\tb4_node1\tc40_node1",
        ]
        assert_no_shuffle_tables()

        later_stage_using_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c USING (bucket)
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(later_stage_using_query).splitlines() == [
            "1\ta1_node1\tb1_node2\tc10_node2",
            "2\ta2_node1\tb2_node2\tc20_node1",
            "3\ta3_node2\tb3_node1\tc30_node2",
            "4\ta4_node2\tb4_node1\tc40_node1",
        ]
        assert_no_shuffle_tables()

        first_stage_local_join_failure_query = """
            SELECT c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            WHERE throwIf(a.id = b.id, 'injected left-deep first local join failure') = 0
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        error = node1.query_and_get_error(first_stage_local_join_failure_query)

        assert "injected left-deep first local join failure" in error
        assert_no_shuffle_tables()

        final_stage_local_join_failure_query = """
            SELECT throwIf(c.bucket = b.bucket, 'injected left-deep final local join failure') AS must_fail
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        error = node1.query_and_get_error(final_stage_local_join_failure_query)

        assert "injected left-deep final local join failure" in error
        assert_no_shuffle_tables()

        expired_stage_output_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
                distributed_shuffle_join_table_ttl_ms = 1
        """

        assert node1.query(expired_stage_output_query).splitlines() == [
            "1\ta1_node1\tb1_node2\tc10_node2",
            "2\ta2_node1\tb2_node2\tc20_node1",
            "3\ta3_node2\tb3_node1\tc30_node2",
            "4\ta4_node2\tb4_node1\tc40_node1",
        ]
        assert_no_shuffle_tables()

        node1.query("INSERT INTO left_deep_b_local VALUES (1, 10, 'b1_dup_node1')")

        any_join_query = """
            SELECT
                a.id AS id
            FROM left_deep_a_dist AS a
            ANY INNER JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(any_join_query).splitlines() == [
            "1",
            "2",
            "3",
            "4",
        ]
        assert_no_shuffle_tables()

        node1.query("INSERT INTO left_deep_c_local VALUES (10, 'c10_dup_node1')")

        second_stage_any_join_query = """
            SELECT
                a.id AS id
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            ANY INNER JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
                any_join_distinct_right_table_keys = 0
        """

        assert node1.query(second_stage_any_join_query).splitlines() == [
            "1",
            "2",
            "3",
            "4",
        ]
        assert_no_shuffle_tables()

        node2.query("DROP TABLE left_deep_c_local")

        second_stage_exchange_failure_query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_a_dist AS a
            INNER ALL JOIN left_deep_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep_c_dist AS c ON b.bucket = c.bucket
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        error = node1.query_and_get_error(second_stage_exchange_failure_query)

        assert "left_deep_c_local" in error
        assert_no_shuffle_tables()
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS left_deep_a_dist")
            node.query("DROP TABLE IF EXISTS left_deep_b_dist")
            node.query("DROP TABLE IF EXISTS left_deep_c_dist")
            node.query("DROP TABLE IF EXISTS left_deep_a_local")
            node.query("DROP TABLE IF EXISTS left_deep_b_local")
            node.query("DROP TABLE IF EXISTS left_deep_c_local")


def test_left_deep_using_chain_join_uses_multi_stage_shuffle(started_cluster):
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS left_deep_using_a_dist")
        node.query("DROP TABLE IF EXISTS left_deep_using_b_dist")
        node.query("DROP TABLE IF EXISTS left_deep_using_c_dist")
        node.query("DROP TABLE IF EXISTS left_deep_using_a_local")
        node.query("DROP TABLE IF EXISTS left_deep_using_b_local")
        node.query("DROP TABLE IF EXISTS left_deep_using_c_local")

        node.query(
            "CREATE TABLE left_deep_using_a_local "
            "(id UInt64, a_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep_using_b_local "
            "(id UInt64, b_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep_using_c_local "
            "(id UInt64, c_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep_using_a_dist AS left_deep_using_a_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep_using_a_local, rand())"
        )
        node.query(
            "CREATE TABLE left_deep_using_b_dist AS left_deep_using_b_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep_using_b_local, rand())"
        )
        node.query(
            "CREATE TABLE left_deep_using_c_dist AS left_deep_using_c_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep_using_c_local, rand())"
        )

    try:
        node1.query(
            "INSERT INTO left_deep_using_a_local VALUES "
            "(1, 'a1_node1'), (2, 'a2_node1')"
        )
        node2.query(
            "INSERT INTO left_deep_using_a_local VALUES "
            "(3, 'a3_node2'), (4, 'a4_node2')"
        )

        node1.query(
            "INSERT INTO left_deep_using_b_local VALUES "
            "(3, 'b3_node1'), (4, 'b4_node1')"
        )
        node2.query(
            "INSERT INTO left_deep_using_b_local VALUES "
            "(1, 'b1_node2'), (2, 'b2_node2')"
        )

        node1.query(
            "INSERT INTO left_deep_using_c_local VALUES "
            "(2, 'c2_node1'), (4, 'c4_node1')"
        )
        node2.query(
            "INSERT INTO left_deep_using_c_local VALUES "
            "(1, 'c1_node2'), (3, 'c3_node2')"
        )

        query = """
            SELECT
                id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value
            FROM left_deep_using_a_dist AS a
            INNER ALL JOIN left_deep_using_b_dist AS b USING (id)
            INNER ALL JOIN left_deep_using_c_dist AS c USING (id)
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(query).splitlines() == [
            "1\ta1_node1\tb1_node2\tc1_node2",
            "2\ta2_node1\tb2_node2\tc2_node1",
            "3\ta3_node2\tb3_node1\tc3_node2",
            "4\ta4_node2\tb4_node1\tc4_node1",
        ]
        assert_no_shuffle_tables()
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS left_deep_using_a_dist")
            node.query("DROP TABLE IF EXISTS left_deep_using_b_dist")
            node.query("DROP TABLE IF EXISTS left_deep_using_c_dist")
            node.query("DROP TABLE IF EXISTS left_deep_using_a_local")
            node.query("DROP TABLE IF EXISTS left_deep_using_b_local")
            node.query("DROP TABLE IF EXISTS left_deep_using_c_local")


def test_left_deep_outer_join_preserves_regular_distributed_join_exception(
    started_cluster,
):
    query = """
        SELECT l.id AS id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        LEFT ALL JOIN left_dist AS x ON r.id = x.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    error = node1.query_and_get_error(query)

    assert "Double-distributed IN/JOIN subqueries is denied" in error
    assert_no_shuffle_tables()


def test_left_deep_semi_join_preserves_regular_distributed_join_exception(
    started_cluster,
):
    query = """
        SELECT l.id AS id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        LEFT SEMI JOIN left_dist AS x ON r.id = x.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    error = node1.query_and_get_error(query)

    assert "Double-distributed IN/JOIN subqueries is denied" in error
    assert_no_shuffle_tables()


def test_left_deep_expression_key_preserves_regular_distributed_join_exception(
    started_cluster,
):
    query = """
        SELECT l.id AS id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        INNER ALL JOIN left_dist AS x ON r.id + 0 = x.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    error = node1.query_and_get_error(query)

    assert "Double-distributed IN/JOIN subqueries is denied" in error
    assert_no_shuffle_tables()


def test_left_deep_duplicate_projection_hidden_order_uses_shuffle_join(
    started_cluster,
):
    query_id = f"shuffle_left_deep_duplicate_projection_hidden_order_{uuid.uuid4().hex}"
    query = """
        SELECT l.id, x.id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        INNER ALL JOIN left_dist AS x ON r.id = x.id
        ORDER BY modulo(r.id + 1, 3) ASC, l.id ASC
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
            log_queries = 1, log_queries_min_type = 'QUERY_START'
    """

    assert node1.query(query, query_id=query_id).splitlines() == [
        "2\t2",
        "3\t3",
        "1\t1",
        "4\t4",
    ]

    for node in (node1, node2):
        node.query("SYSTEM FLUSH LOGS query_log")
        assert (
            query_log_source_query_count(
                node,
                query_id,
                "left",
                "position(query, 'FROM default.left_local') > 0",
            )
            == 1
        )
        assert (
            query_log_source_query_count(
                node,
                query_id,
                "right",
                "position(query, 'FROM default.left_local') > 0",
            )
            == 1
        )
        assert (
            query_log_source_query_count(
                node,
                query_id,
                "right",
                "position(query, 'FROM default.right_local') > 0",
            )
            == 1
        )
        assert (
            query_log_source_query_count(
                node,
                query_id,
                "left",
                "position(query, '_output') > 0",
            )
            == 1
        )
    assert_no_shuffle_tables()

    ties_query = """
        SELECT l.id, x.id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        INNER ALL JOIN left_dist AS x ON r.id = x.id
        ORDER BY modulo(r.id + 1, 3) ASC
        LIMIT 3 WITH TIES
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    assert sorted_tsv(node1.query(ties_query)) == [
        "1\t1",
        "2\t2",
        "3\t3",
        "4\t4",
    ]
    assert_no_shuffle_tables()


def test_left_deep_compound_key_preserves_regular_distributed_join_exception(
    started_cluster,
):
    query = """
        SELECT l.id AS id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        INNER ALL JOIN left_dist AS x ON r.id = x.id AND l.id = x.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    error = node1.query_and_get_error(query)

    assert "Double-distributed IN/JOIN subqueries is denied" in error
    assert_no_shuffle_tables()


def test_left_deep_aggregation_preserves_regular_distributed_join_exception(
    started_cluster,
):
    query = """
        SELECT l.id AS id, count() AS count
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        INNER ALL JOIN left_dist AS x ON r.id = x.id
        GROUP BY l.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    error = node1.query_and_get_error(query)

    assert "Double-distributed IN/JOIN subqueries is denied" in error
    assert_no_shuffle_tables()


def test_left_deep_distinct_preserves_regular_distributed_join_exception(
    started_cluster,
):
    query = """
        SELECT DISTINCT l.id AS id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        INNER ALL JOIN left_dist AS x ON r.id = x.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    error = node1.query_and_get_error(query)

    assert "Double-distributed IN/JOIN subqueries is denied" in error
    assert_no_shuffle_tables()


def test_left_deep_parallel_replicas_preserves_regular_distributed_join_exception(
    started_cluster,
):
    query = """
        SELECT l.id AS id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        INNER ALL JOIN left_dist AS x ON r.id = x.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
            allow_experimental_parallel_reading_from_replicas = 1
    """

    error = node1.query_and_get_error(query)

    assert "Double-distributed IN/JOIN subqueries is denied" in error
    assert_no_shuffle_tables()


def test_left_deep_four_table_join_uses_n_stage_shuffle(started_cluster):
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS left_deep4_a_dist")
        node.query("DROP TABLE IF EXISTS left_deep4_b_dist")
        node.query("DROP TABLE IF EXISTS left_deep4_c_dist")
        node.query("DROP TABLE IF EXISTS left_deep4_d_dist")
        node.query("DROP TABLE IF EXISTS left_deep4_a_local")
        node.query("DROP TABLE IF EXISTS left_deep4_b_local")
        node.query("DROP TABLE IF EXISTS left_deep4_c_local")
        node.query("DROP TABLE IF EXISTS left_deep4_d_local")

        node.query(
            "CREATE TABLE left_deep4_a_local "
            "(id UInt64, a_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep4_b_local "
            "(id UInt64, bucket UInt64, b_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep4_c_local "
            "(bucket UInt64, group_id UInt64, c_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep4_d_local "
            "(group_id UInt64, d_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_deep4_a_dist AS left_deep4_a_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep4_a_local, rand())"
        )
        node.query(
            "CREATE TABLE left_deep4_b_dist AS left_deep4_b_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep4_b_local, rand())"
        )
        node.query(
            "CREATE TABLE left_deep4_c_dist AS left_deep4_c_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep4_c_local, rand())"
        )
        node.query(
            "CREATE TABLE left_deep4_d_dist AS left_deep4_d_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_deep4_d_local, rand())"
        )

    try:
        node1.query(
            "INSERT INTO left_deep4_a_local VALUES "
            "(1, 'a1_node1'), (2, 'a2_node1')"
        )
        node2.query(
            "INSERT INTO left_deep4_a_local VALUES "
            "(3, 'a3_node2'), (4, 'a4_node2')"
        )

        node1.query(
            "INSERT INTO left_deep4_b_local VALUES "
            "(3, 30, 'b3_node1'), (4, 40, 'b4_node1')"
        )
        node2.query(
            "INSERT INTO left_deep4_b_local VALUES "
            "(1, 10, 'b1_node2'), (2, 20, 'b2_node2')"
        )

        node1.query(
            "INSERT INTO left_deep4_c_local VALUES "
            "(20, 200, 'c20_node1'), (40, 400, 'c40_node1')"
        )
        node2.query(
            "INSERT INTO left_deep4_c_local VALUES "
            "(10, 100, 'c10_node2'), (30, 300, 'c30_node2')"
        )

        node1.query(
            "INSERT INTO left_deep4_d_local VALUES "
            "(100, 'd100_node1'), (300, 'd300_node1')"
        )
        node2.query(
            "INSERT INTO left_deep4_d_local VALUES "
            "(200, 'd200_node2'), (400, 'd400_node2')"
        )

        query_id = f"shuffle_left_deep4_scan_{uuid.uuid4().hex}"
        query = """
            SELECT
                a.id AS id,
                a.a_value AS a_value,
                b.b_value AS b_value,
                c.c_value AS c_value,
                d.d_value AS d_value
            FROM left_deep4_a_dist AS a
            INNER ALL JOIN left_deep4_b_dist AS b ON a.id = b.id
            INNER ALL JOIN left_deep4_c_dist AS c ON b.bucket = c.bucket
            INNER ALL JOIN left_deep4_d_dist AS d ON c.group_id = d.group_id
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
                log_queries = 1, log_queries_min_type = 'QUERY_START'
        """

        assert node1.query(query, query_id=query_id).splitlines() == [
            "1\ta1_node1\tb1_node2\tc10_node2\td100_node1",
            "2\ta2_node1\tb2_node2\tc20_node1\td200_node2",
            "3\ta3_node2\tb3_node1\tc30_node2\td300_node1",
            "4\ta4_node2\tb4_node1\tc40_node1\td400_node2",
        ]

        for node in (node1, node2):
            node.query("SYSTEM FLUSH LOGS query_log")
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "left",
                    "position(query, 'FROM default.left_deep4_a_local') > 0",
                )
                == 1
            )
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "right",
                    "position(query, 'FROM default.left_deep4_b_local') > 0",
                )
                == 1
            )
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "left",
                    "position(query, '_output') > 0",
                )
                == 2
            )
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "right",
                    "position(query, 'FROM default.left_deep4_c_local') > 0",
                )
                == 1
            )
            assert (
                query_log_source_query_count(
                    node,
                    query_id,
                    "right",
                    "position(query, 'FROM default.left_deep4_d_local') > 0",
                )
                == 1
            )
        assert_no_shuffle_tables()
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS left_deep4_a_dist")
            node.query("DROP TABLE IF EXISTS left_deep4_b_dist")
            node.query("DROP TABLE IF EXISTS left_deep4_c_dist")
            node.query("DROP TABLE IF EXISTS left_deep4_d_dist")
            node.query("DROP TABLE IF EXISTS left_deep4_a_local")
            node.query("DROP TABLE IF EXISTS left_deep4_b_local")
            node.query("DROP TABLE IF EXISTS left_deep4_c_local")
            node.query("DROP TABLE IF EXISTS left_deep4_d_local")


def test_inner_any_join_uses_push_based_shuffle(started_cluster):
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS any_left_dist")
        node.query("DROP TABLE IF EXISTS any_right_dist")
        node.query("DROP TABLE IF EXISTS any_left_local")
        node.query("DROP TABLE IF EXISTS any_right_local")

        node.query(
            "CREATE TABLE any_left_local (id UInt64, left_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE any_right_local (id UInt64, right_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE any_left_dist AS any_left_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, any_left_local, rand())"
        )
        node.query(
            "CREATE TABLE any_right_dist AS any_right_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, any_right_local, rand())"
        )

    try:
        node1.query("INSERT INTO any_left_local VALUES (1, 'l1_node1'), (2, 'l2_node1')")
        node2.query("INSERT INTO any_left_local VALUES (3, 'l3_node2')")

        node1.query(
            "INSERT INTO any_right_local VALUES "
            "(1, 'r1a_node1'), (1, 'r1b_node1'), (3, 'r3_node1')"
        )
        node2.query(
            "INSERT INTO any_right_local VALUES "
            "(2, 'r2a_node2'), (2, 'r2b_node2')"
        )

        query = """
            SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
            FROM any_left_dist AS l
            ANY INNER JOIN any_right_dist AS r ON l.id = r.id
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
                any_join_distinct_right_table_keys = 0
        """

        rows = node1.query(query).strip().splitlines()

        assert len(rows) == 3
        assert rows[0].startswith("1\tl1_node1\tr1")
        assert rows[1].startswith("2\tl2_node1\tr2")
        assert rows[2] == "3\tl3_node2\tr3_node1"
        assert_no_shuffle_tables()
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS any_left_dist")
            node.query("DROP TABLE IF EXISTS any_right_dist")
            node.query("DROP TABLE IF EXISTS any_left_local")
            node.query("DROP TABLE IF EXISTS any_right_local")


def test_left_all_join_uses_push_based_shuffle(started_cluster):
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS left_join_left_dist")
        node.query("DROP TABLE IF EXISTS left_join_right_dist")
        node.query("DROP TABLE IF EXISTS left_join_left_local")
        node.query("DROP TABLE IF EXISTS left_join_right_local")

        node.query(
            "CREATE TABLE left_join_left_local (id UInt64, left_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_join_right_local (id UInt64, right_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE left_join_left_dist AS left_join_left_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_join_left_local, rand())"
        )
        node.query(
            "CREATE TABLE left_join_right_dist AS left_join_right_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, left_join_right_local, rand())"
        )

    try:
        node1.query(
            "INSERT INTO left_join_left_local VALUES "
            "(1, 'l1_node1'), (2, 'l2_node1')"
        )
        node2.query("INSERT INTO left_join_left_local VALUES (3, 'l3_node2')")

        node1.query("INSERT INTO left_join_right_local VALUES (2, 'r2_node1')")
        node2.query("INSERT INTO left_join_right_local VALUES (1, 'r1_node2')")

        query = """
            SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
            FROM left_join_left_dist AS l
            LEFT ALL JOIN left_join_right_dist AS r ON l.id = r.id
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(query).splitlines() == [
            "1\tl1_node1\tr1_node2",
            "2\tl2_node1\tr2_node1",
            "3\tl3_node2\t",
        ]
        assert_no_shuffle_tables()

        right_filter_query = """
            SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
            FROM left_join_left_dist AS l
            LEFT ALL JOIN left_join_right_dist AS r ON l.id = r.id
            WHERE r.right_value = ''
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(right_filter_query).splitlines() == [
            "3\tl3_node2\t",
        ]
        assert_no_shuffle_tables()
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS left_join_left_dist")
            node.query("DROP TABLE IF EXISTS left_join_right_dist")
            node.query("DROP TABLE IF EXISTS left_join_left_local")
            node.query("DROP TABLE IF EXISTS left_join_right_local")


def test_right_all_join_uses_push_based_shuffle(started_cluster):
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS right_join_left_dist")
        node.query("DROP TABLE IF EXISTS right_join_right_dist")
        node.query("DROP TABLE IF EXISTS right_join_left_local")
        node.query("DROP TABLE IF EXISTS right_join_right_local")

        node.query(
            "CREATE TABLE right_join_left_local (id UInt64, left_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE right_join_right_local (id UInt64, right_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE right_join_left_dist AS right_join_left_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, right_join_left_local, rand())"
        )
        node.query(
            "CREATE TABLE right_join_right_dist AS right_join_right_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, right_join_right_local, rand())"
        )

    try:
        node1.query("INSERT INTO right_join_left_local VALUES (1, 'l1_node1')")
        node2.query("INSERT INTO right_join_left_local VALUES (2, 'l2_node2')")

        node1.query(
            "INSERT INTO right_join_right_local VALUES "
            "(2, 'r2_node1'), (3, 'r3_node1')"
        )
        node2.query("INSERT INTO right_join_right_local VALUES (1, 'r1_node2')")

        query = """
            SELECT r.id AS id, l.left_value AS left_value, r.right_value AS right_value
            FROM right_join_left_dist AS l
            RIGHT ALL JOIN right_join_right_dist AS r ON l.id = r.id
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(query).splitlines() == [
            "1\tl1_node1\tr1_node2",
            "2\tl2_node2\tr2_node1",
            "3\t\tr3_node1",
        ]
        assert_no_shuffle_tables()

        left_filter_query = """
            SELECT r.id AS id, l.left_value AS left_value, r.right_value AS right_value
            FROM right_join_left_dist AS l
            RIGHT ALL JOIN right_join_right_dist AS r ON l.id = r.id
            WHERE l.left_value = ''
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(left_filter_query).splitlines() == [
            "3\t\tr3_node1",
        ]
        assert_no_shuffle_tables()
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS right_join_left_dist")
            node.query("DROP TABLE IF EXISTS right_join_right_dist")
            node.query("DROP TABLE IF EXISTS right_join_left_local")
            node.query("DROP TABLE IF EXISTS right_join_right_local")


def test_semi_and_anti_join_use_push_based_shuffle(started_cluster):
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS semi_anti_left_dist")
        node.query("DROP TABLE IF EXISTS semi_anti_right_dist")
        node.query("DROP TABLE IF EXISTS semi_anti_left_local")
        node.query("DROP TABLE IF EXISTS semi_anti_right_local")

        node.query(
            "CREATE TABLE semi_anti_left_local (id UInt64, left_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE semi_anti_right_local (id UInt64, right_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE semi_anti_left_dist AS semi_anti_left_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, semi_anti_left_local, rand())"
        )
        node.query(
            "CREATE TABLE semi_anti_right_dist AS semi_anti_right_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, semi_anti_right_local, rand())"
        )

    try:
        node1.query(
            "INSERT INTO semi_anti_left_local VALUES "
            "(1, 'l1_node1'), (2, 'l2_node1')"
        )
        node2.query("INSERT INTO semi_anti_left_local VALUES (3, 'l3_node2')")

        node1.query("INSERT INTO semi_anti_right_local VALUES (2, 'r2_node1')")
        node2.query("INSERT INTO semi_anti_right_local VALUES (4, 'r4_node2')")

        left_semi_query = """
            SELECT l.id AS id, l.left_value AS left_value
            FROM semi_anti_left_dist AS l
            LEFT SEMI JOIN semi_anti_right_dist AS r ON l.id = r.id
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(left_semi_query).splitlines() == [
            "2\tl2_node1",
        ]
        assert_no_shuffle_tables()

        left_anti_query = """
            SELECT l.id AS id, l.left_value AS left_value
            FROM semi_anti_left_dist AS l
            LEFT ANTI JOIN semi_anti_right_dist AS r ON l.id = r.id
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(left_anti_query).splitlines() == [
            "1\tl1_node1",
            "3\tl3_node2",
        ]
        assert_no_shuffle_tables()

        right_semi_query = """
            SELECT r.id AS id, r.right_value AS right_value
            FROM semi_anti_left_dist AS l
            RIGHT SEMI JOIN semi_anti_right_dist AS r ON l.id = r.id
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(right_semi_query).splitlines() == [
            "2\tr2_node1",
        ]
        assert_no_shuffle_tables()

        right_anti_query = """
            SELECT r.id AS id, r.right_value AS right_value
            FROM semi_anti_left_dist AS l
            RIGHT ANTI JOIN semi_anti_right_dist AS r ON l.id = r.id
            ORDER BY id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(right_anti_query).splitlines() == [
            "4\tr4_node2",
        ]
        assert_no_shuffle_tables()
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS semi_anti_left_dist")
            node.query("DROP TABLE IF EXISTS semi_anti_right_dist")
            node.query("DROP TABLE IF EXISTS semi_anti_left_local")
            node.query("DROP TABLE IF EXISTS semi_anti_right_local")


def test_full_all_join_uses_push_based_shuffle(started_cluster):
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS full_join_left_dist")
        node.query("DROP TABLE IF EXISTS full_join_right_dist")
        node.query("DROP TABLE IF EXISTS full_join_left_local")
        node.query("DROP TABLE IF EXISTS full_join_right_local")

        node.query(
            "CREATE TABLE full_join_left_local (id UInt64, left_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE full_join_right_local (id UInt64, right_value String) ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE full_join_left_dist AS full_join_left_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, full_join_left_local, rand())"
        )
        node.query(
            "CREATE TABLE full_join_right_dist AS full_join_right_local "
            "ENGINE = Distributed(shuffle_join_cluster, default, full_join_right_local, rand())"
        )

    try:
        node1.query("INSERT INTO full_join_left_local VALUES (1, 'l1_node1')")
        node2.query("INSERT INTO full_join_left_local VALUES (2, 'l2_node2')")

        node1.query("INSERT INTO full_join_right_local VALUES (2, 'r2_node1')")
        node2.query("INSERT INTO full_join_right_local VALUES (3, 'r3_node2')")

        query = """
            SELECT l.id AS left_id, l.left_value AS left_value,
                   r.id AS right_id, r.right_value AS right_value
            FROM full_join_left_dist AS l
            FULL ALL JOIN full_join_right_dist AS r ON l.id = r.id
            ORDER BY left_id, right_id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(query).splitlines() == [
            "0\t\t3\tr3_node2",
            "1\tl1_node1\t0\t",
            "2\tl2_node2\t2\tr2_node1",
        ]
        assert_no_shuffle_tables()

        left_filter_query = """
            SELECT l.id AS left_id, l.left_value AS left_value,
                   r.id AS right_id, r.right_value AS right_value
            FROM full_join_left_dist AS l
            FULL ALL JOIN full_join_right_dist AS r ON l.id = r.id
            WHERE l.left_value = ''
            ORDER BY left_id, right_id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(left_filter_query).splitlines() == [
            "0\t\t3\tr3_node2",
        ]
        assert_no_shuffle_tables()

        right_filter_query = """
            SELECT l.id AS left_id, l.left_value AS left_value,
                   r.id AS right_id, r.right_value AS right_value
            FROM full_join_left_dist AS l
            FULL ALL JOIN full_join_right_dist AS r ON l.id = r.id
            WHERE r.right_value = ''
            ORDER BY left_id, right_id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
        """

        assert node1.query(right_filter_query).splitlines() == [
            "1\tl1_node1\t0\t",
        ]
        assert_no_shuffle_tables()
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS full_join_left_dist")
            node.query("DROP TABLE IF EXISTS full_join_right_dist")
            node.query("DROP TABLE IF EXISTS full_join_left_local")
            node.query("DROP TABLE IF EXISTS full_join_right_local")


def test_exchange_profile_events_are_logged(started_cluster):
    query_id = f"shuffle_profile_events_{uuid.uuid4().hex}"
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
            log_queries = 1
    """

    result = node1.query(query, query_id=query_id)

    assert sorted_tsv(result) == [
        "1\tl1_node1\tr1_node2",
        "2\tl2_node1\tr2_node2",
        "3\tl3_node2\tr3_node1",
        "4\tl4_node2\tr4_node1",
    ]

    for node in (node1, node2):
        node.query("SYSTEM FLUSH LOGS query_log")

    exchange_input_rows = sum(
        query_log_profile_event_sum(
            node, query_id, "DistributedShuffleJoinExchangeInputRows"
        )
        for node in (node1, node2)
    )
    exchange_output_rows = sum(
        query_log_profile_event_sum(
            node, query_id, "DistributedShuffleJoinExchangeOutputRows"
        )
        for node in (node1, node2)
    )
    local_output_rows = sum(
        query_log_profile_event_sum(
            node, query_id, "DistributedShuffleJoinLocalOutputRows"
        )
        for node in (node1, node2)
    )
    remote_output_rows = sum(
        query_log_profile_event_sum(
            node, query_id, "DistributedShuffleJoinRemoteOutputRows"
        )
        for node in (node1, node2)
    )
    exchange_input_bytes = sum(
        query_log_profile_event_sum(
            node, query_id, "DistributedShuffleJoinExchangeInputBytes"
        )
        for node in (node1, node2)
    )
    exchange_output_bytes = sum(
        query_log_profile_event_sum(
            node, query_id, "DistributedShuffleJoinExchangeOutputBytes"
        )
        for node in (node1, node2)
    )

    assert exchange_input_rows > 0
    assert exchange_output_rows == exchange_input_rows
    assert local_output_rows > 0
    assert remote_output_rows > 0
    assert local_output_rows + remote_output_rows == exchange_output_rows
    assert exchange_input_bytes > 0
    assert exchange_output_bytes > 0
    assert_no_shuffle_tables()


def test_each_source_shard_scans_each_side_once(started_cluster):
    query_id = f"shuffle_source_scan_{uuid.uuid4().hex}"
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
            log_queries = 1, log_queries_min_type = 'QUERY_START'
    """

    result = node1.query(query, query_id=query_id)

    assert sorted_tsv(result) == [
        "1\tl1_node1\tr1_node2",
        "2\tl2_node1\tr2_node2",
        "3\tl3_node2\tr3_node1",
        "4\tl4_node2\tr4_node1",
    ]

    for node in (node1, node2):
        node.query("SYSTEM FLUSH LOGS query_log")

        left_scans = node.query(
            f"""
            SELECT count()
            FROM system.query_log
            WHERE type = 'QueryStart'
              AND startsWith(query_id, '{query_id}:shuffle:')
              AND endsWith(query_id, ':source:left')
              AND position(query, 'FROM default.left_local') > 0
            """
        ).strip()
        right_scans = node.query(
            f"""
            SELECT count()
            FROM system.query_log
            WHERE type = 'QueryStart'
              AND startsWith(query_id, '{query_id}:shuffle:')
              AND endsWith(query_id, ':source:right')
              AND position(query, 'FROM default.right_local') > 0
            """
        ).strip()

        assert left_scans == "1"
        assert right_scans == "1"

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


def test_setting_off_preserves_regular_distributed_join_exception(started_cluster):
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 0
    """

    error = node1.query_and_get_error(query)

    assert "Double-distributed IN/JOIN subqueries is denied" in error
    assert_no_shuffle_tables()


def test_post_join_filter_uses_shuffle_join(started_cluster):
    query = """
        SELECT l.id AS id, l.id + r.id AS sum_id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        WHERE l.id + r.id >= 6
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    result = node1.query(query)

    assert sorted_tsv(result) == [
        "3\t6\tl3_node2\tr3_node1",
        "4\t8\tl4_node2\tr4_node1",
    ]
    assert_no_shuffle_tables()


def test_global_limit_offset_uses_shuffle_join(started_cluster):
    query = """
        SELECT toUInt64(1) AS selected
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        LIMIT 2 OFFSET 1
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    result = node1.query(query)

    assert sorted_tsv(result) == ["1", "1"]
    assert_no_shuffle_tables()


def test_global_order_by_limit_offset_uses_shuffle_join(started_cluster):
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        ORDER BY id DESC
        LIMIT 2 OFFSET 1
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    result = node1.query(query)

    assert result.strip().splitlines() == [
        "3\tl3_node2\tr3_node1",
        "2\tl2_node1\tr2_node2",
    ]
    assert_no_shuffle_tables()


def test_hidden_global_order_by_uses_shuffle_join(started_cluster):
    query = """
        SELECT l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        ORDER BY l.id DESC
        LIMIT 2 OFFSET 1
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    result = node1.query(query)

    assert result.strip().splitlines() == [
        "l3_node2\tr3_node1",
        "l2_node1\tr2_node2",
    ]
    assert_no_shuffle_tables()


def test_global_limit_with_ties_uses_shuffle_join(started_cluster):
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        ORDER BY modulo(l.id, 2) ASC
        LIMIT 1 WITH TIES
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    result = node1.query(query)

    assert sorted_tsv(result) == [
        "2\tl2_node1\tr2_node2",
        "4\tl4_node2\tr4_node1",
    ]
    assert_no_shuffle_tables()


def test_exchange_source_failure_cleans_shuffle_tables(started_cluster):
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    node2.query("DROP TABLE right_local")
    try:
        error = node1.query_and_get_error(query)

        assert "right_local" in error
        assert_no_shuffle_tables()
    finally:
        node2.query(
            "CREATE TABLE IF NOT EXISTS right_local (id UInt64, right_value String) ENGINE = Memory"
        )
        node2.query("TRUNCATE TABLE right_local")
        node2.query("INSERT INTO right_local VALUES (1, 'r1_node2'), (2, 'r2_node2')")


def test_local_join_failure_cleans_shuffle_tables(started_cluster):
    query = """
        SELECT throwIf(l.id = r.id, 'injected shuffle local join failure') AS must_fail
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
    """

    error = node1.query_and_get_error(query)

    assert "injected shuffle local join failure" in error
    assert_no_shuffle_tables()


def test_query_cancellation_cleans_shuffle_tables(started_cluster):
    query_id = f"shuffle_cancel_{uuid.uuid4().hex}"
    query = """
        SELECT sleepEachRow(1) AS stalled, l.id AS id
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
            function_sleep_max_microseconds_per_block = 0
    """
    result = {}

    def execute_query():
        result["error"] = node1.query_and_get_error(query, query_id=query_id)

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    for _ in range(100):
        local_join_running = any(
            node.query(
                """
                SELECT count()
                FROM system.processes
                WHERE position(query, 'sleepEachRow') > 0
                  AND position(query, '_shuffle_') > 0
                  AND position(query, 'system.processes') = 0
                """
            ).strip()
            != "0"
            for node in (node1, node2)
        )
        if local_join_running:
            break
        time.sleep(0.05)
    else:
        pytest.fail("Shuffle local join did not start before cancellation")

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
    query_thread.join(timeout=10)

    assert not query_thread.is_alive()
    assert "QUERY_WAS_CANCELLED" in result["error"]
    assert_no_shuffle_tables()


def test_query_cancellation_during_exchange_cleans_shuffle_tables(started_cluster):
    query_id = f"shuffle_exchange_cancel_{uuid.uuid4().hex}"
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        WHERE sleepEachRow(30) = 0 AND l.id > 0
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
            function_sleep_max_microseconds_per_block = 0
    """
    result = {}

    def execute_query():
        result["error"] = node1.query_and_get_error(query, query_id=query_id)

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    for _ in range(100):
        exchange_running = any(
            node.query(
                """
                SELECT count()
                FROM system.processes
                WHERE position(query, 'sleepEachRow') > 0
                  AND position(query, 'FROM default.left_local') > 0
                  AND position(query, 'system.processes') = 0
                """
            ).strip()
            != "0"
            for node in (node1, node2)
        )
        if exchange_running:
            break
        time.sleep(0.05)
    else:
        pytest.fail("Shuffle source exchange did not start before cancellation")

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
    query_thread.join(timeout=10)

    assert not query_thread.is_alive()
    assert "QUERY_WAS_CANCELLED" in result["error"]
    assert_no_shuffle_tables()


def test_client_disconnect_during_exchange_cleans_shuffle_tables(started_cluster):
    query_id = f"shuffle_disconnect_{uuid.uuid4().hex}"
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        WHERE sleepEachRow(30) = 0 AND l.id > 0
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
            function_sleep_max_microseconds_per_block = 0
    """
    request = node1.get_query_request(query, query_id=query_id)

    for _ in range(100):
        exchange_running = any(
            node.query(
                """
                SELECT count()
                FROM system.processes
                WHERE position(query, 'sleepEachRow') > 0
                  AND position(query, 'FROM default.left_local') > 0
                  AND position(query, 'system.processes') = 0
                """
            ).strip()
            != "0"
            for node in (node1, node2)
        )
        if exchange_running:
            break
        time.sleep(0.05)
    else:
        request.process.kill()
        request.get_answer_and_error()
        pytest.fail("Shuffle source exchange did not start before client disconnect")

    request.process.kill()
    request.get_answer_and_error()

    for _ in range(100):
        initial_query_finished = (
            node1.query(
                f"""
                SELECT count()
                FROM system.processes
                WHERE query_id = '{query_id}'
                """
            ).strip()
            == "0"
        )
        shuffle_tables_removed = all(
            node.query("SHOW TABLES LIKE '_shuffle_%'") == ""
            for node in (node1, node2)
        )
        if initial_query_finished and shuffle_tables_removed:
            return
        time.sleep(0.1)

    pytest.fail("Client disconnect did not finish shuffle query and cleanup in time")


def test_cleanup_continues_when_remote_shard_is_unavailable(started_cluster):
    query_id = f"shuffle_cleanup_remote_down_{uuid.uuid4().hex}"
    shuffle_table_pattern = f"_shuffle_{query_id}_%"
    query = """
        SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
        FROM left_dist AS l
        INNER ALL JOIN right_dist AS r ON l.id = r.id
        WHERE sleepEachRow(30) = 0 AND l.id > 0
        SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1,
            function_sleep_max_microseconds_per_block = 0,
            distributed_shuffle_join_table_ttl_ms = 100
    """
    result = {}

    def execute_query():
        result["error"] = node1.query_and_get_error(query, query_id=query_id)

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    for _ in range(100):
        exchange_running = (
            node1.query(
                """
                SELECT count()
                FROM system.processes
                WHERE position(query, 'sleepEachRow') > 0
                  AND position(query, 'FROM default.left_local') > 0
                  AND position(query, 'system.processes') = 0
                """
            ).strip()
            != "0"
        )
        tables_created = all(
            node.query(f"SHOW TABLES LIKE '{shuffle_table_pattern}'") != ""
            for node in (node1, node2)
        )
        if exchange_running and tables_created:
            break
        time.sleep(0.05)
    else:
        node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
        query_thread.join(timeout=10)
        pytest.fail("Shuffle exchange did not prepare both shards before remote stop")

    node2.stop_clickhouse(kill=True)
    node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
    query_thread.join(timeout=10)

    assert not query_thread.is_alive()
    assert result["error"]
    assert node1.query(f"SHOW TABLES LIKE '{shuffle_table_pattern}'") == ""

    node2.start_clickhouse()
    remote_tables = node2.query(
        f"SHOW TABLES LIKE '{shuffle_table_pattern}'"
    ).strip().splitlines()
    assert len(remote_tables) == 2

    node2.query("INSERT INTO left_local VALUES (3, 'l3_node2'), (4, 'l4_node2')")
    node2.query("INSERT INTO right_local VALUES (1, 'r1_node2'), (2, 'r2_node2')")

    protected_table = f"_shuffle_user_table_{uuid.uuid4().hex}_0_expires_1_left"
    node2.query(f"CREATE TABLE {protected_table} (id UInt64) ENGINE = MergeTree ORDER BY id")
    node2.query(f"INSERT INTO {protected_table} VALUES (42)")
    try:
        time.sleep(0.2)
        recovery_result = node1.query(
            """
            SELECT l.id AS id, l.left_value AS left_value, r.right_value AS right_value
            FROM left_dist AS l
            INNER ALL JOIN right_dist AS r ON l.id = r.id
            SETTINGS enable_analyzer = 1, distributed_shuffle_join = 1
            """
        )

        assert sorted_tsv(recovery_result) == [
            "1\tl1_node1\tr1_node2",
            "2\tl2_node1\tr2_node2",
            "3\tl3_node2\tr3_node1",
            "4\tl4_node2\tr4_node1",
        ]
        assert node2.query(f"SHOW TABLES LIKE '{shuffle_table_pattern}'") == ""
        assert node2.query(f"SELECT count() FROM {protected_table}").strip() == "1"
    finally:
        node2.query(f"DROP TABLE IF EXISTS {protected_table}")

    assert_no_shuffle_tables()
