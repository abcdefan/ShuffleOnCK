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
