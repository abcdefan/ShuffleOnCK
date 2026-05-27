import json
import os
import shutil
import statistics
import time

from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"])
nodes = (node1, node2, node3)

SHARD_COUNT = 3
LEFT_ROWS = int(os.environ.get("CLICKHOUSE_SHUFFLE_PERF_LEFT_ROWS", "30000"))
RIGHT_ROWS = int(os.environ.get("CLICKHOUSE_SHUFFLE_PERF_RIGHT_ROWS", "6000000"))
WARMUP_RUNS = int(os.environ.get("CLICKHOUSE_SHUFFLE_PERF_WARMUP_RUNS", "1"))
MEASURED_RUNS = int(os.environ.get("CLICKHOUSE_SHUFFLE_PERF_MEASURED_RUNS", "3"))
REPOSITORY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
RESULT_PATH = os.environ.get(
    "CLICKHOUSE_SHUFFLE_PERF_RESULT_PATH",
    os.path.join(REPOSITORY_ROOT, "build", "distributed_shuffle_join_performance_results.json"),
)

COMMON_SETTINGS = """
    enable_analyzer = 1,
    max_threads = 4,
    use_query_cache = 0,
    join_algorithm = 'hash'
"""


def distributed_query(mode, output_columns, output_format):
    if mode == "global":
        join = "GLOBAL INNER ALL JOIN default.right_dist AS b USING (id)"
        settings = COMMON_SETTINGS
    elif mode == "allow":
        join = "INNER ALL JOIN default.right_dist AS b USING (id)"
        settings = COMMON_SETTINGS + ", distributed_product_mode = 'allow', prefer_global_in_and_join = 0"
    elif mode == "exchange":
        join = "INNER ALL JOIN default.right_dist AS b USING (id)"
        settings = COMMON_SETTINGS + ", distributed_shuffle_join = 1"
    else:
        raise AssertionError(f"Unknown mode {mode}")

    return f"""
        SELECT {output_columns}
        FROM default.left_dist AS a
        {join}
        SETTINGS {settings}
        FORMAT {output_format}
    """


def sql_bucket_query(bucket, output_columns, output_format):
    return f"""
        SELECT {output_columns}
        FROM
        (
            SELECT id, a_payload
            FROM default.left_dist
            WHERE modulo(cityHash64(id), {SHARD_COUNT}) = {bucket}
        ) AS a
        INNER ALL JOIN
        (
            SELECT id, b_payload_1, b_payload_2, b_payload_3
            FROM default.right_dist
            WHERE modulo(cityHash64(id), {SHARD_COUNT}) = {bucket}
        ) AS b USING (id)
        SETTINGS {COMMON_SETTINGS}, distributed_product_mode = 'allow', prefer_global_in_and_join = 0
        FORMAT {output_format}
    """


def run_sql_bucket_mode(output_columns, output_format):
    with ThreadPoolExecutor(max_workers=SHARD_COUNT) as executor:
        futures = [
            executor.submit(node.query, sql_bucket_query(bucket, output_columns, output_format), timeout=600)
            for bucket, node in enumerate(nodes)
        ]
        return "".join(future.result() for future in futures)


def run_mode(mode, output_columns, output_format):
    if mode == "sql_bucket":
        return run_sql_bucket_mode(output_columns, output_format)
    return node1.query(distributed_query(mode, output_columns, output_format), timeout=600)


def create_tables():
    for node in nodes:
        for table in ("left_dist", "right_dist", "left_local", "right_local"):
            node.query(f"DROP TABLE IF EXISTS default.{table} SYNC")

        node.query(
            """
            CREATE TABLE default.left_local
            (
                id UInt64,
                a_payload String
            )
            ENGINE = MergeTree
            ORDER BY id
            """
        )
        node.query(
            """
            CREATE TABLE default.right_local
            (
                id UInt64,
                b_payload_1 String,
                b_payload_2 String,
                b_payload_3 String
            )
            ENGINE = MergeTree
            ORDER BY id
            """
        )
        node.query(
            """
            CREATE TABLE default.left_dist AS default.left_local
            ENGINE = Distributed(shuffle_join_cluster, default, left_local, cityHash64(id))
            """
        )
        node.query(
            """
            CREATE TABLE default.right_dist AS default.right_local
            ENGINE = Distributed(shuffle_join_cluster, default, right_local, cityHash64(id))
            """
        )


def insert_data():
    for bucket, node in enumerate(nodes):
        node.query(
            f"""
            INSERT INTO default.left_local
            SELECT
                id,
                concat('left-', toString(id), '-', hex(cityHash64(id, toUInt64(99)))) AS a_payload
            FROM
            (
                SELECT toUInt64(number + 1) AS id
                FROM numbers({LEFT_ROWS})
            )
            WHERE modulo(cityHash64(id, toUInt64(20260420)), {SHARD_COUNT}) = {bucket}
            """,
            timeout=600,
        )
        node.query(
            f"""
            INSERT INTO default.right_local
            SELECT
                id,
                concat(hex(cityHash64(id, toUInt64(1))), hex(cityHash64(id, toUInt64(2))),
                       hex(cityHash64(id, toUInt64(3))), hex(cityHash64(id, toUInt64(4)))) AS b_payload_1,
                concat(hex(cityHash64(id, toUInt64(5))), hex(cityHash64(id, toUInt64(6))),
                       hex(cityHash64(id, toUInt64(7))), hex(cityHash64(id, toUInt64(8)))) AS b_payload_2,
                concat(hex(cityHash64(id, toUInt64(9))), hex(cityHash64(id, toUInt64(10))),
                       hex(cityHash64(id, toUInt64(11))), hex(cityHash64(id, toUInt64(12)))) AS b_payload_3
            FROM
            (
                SELECT toUInt64(number + 1) AS id
                FROM numbers({RIGHT_ROWS})
            )
            WHERE modulo(cityHash64(id, toUInt64(20260421)), {SHARD_COUNT}) = {bucket}
            """,
            timeout=600,
        )


@pytest.fixture(scope="module")
def started_cluster():
    shutil.rmtree(cluster.instances_dir, ignore_errors=True)
    try:
        cluster.start()
        create_tables()
        insert_data()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.skipif(
    os.environ.get("CLICKHOUSE_RUN_SHUFFLE_PERFORMANCE_COMPARISON") != "1",
    reason="Manual benchmark; set CLICKHOUSE_RUN_SHUFFLE_PERFORMANCE_COMPARISON=1",
)
def test_large_right_table_join_modes(started_cluster):
    modes = ("global", "allow", "sql_bucket", "exchange")

    expected_ids = run_mode("global", "a.id", "TSV").splitlines()
    expected_ids.sort()
    for mode in modes[1:]:
        ids = run_mode(mode, "a.id", "TSV").splitlines()
        ids.sort()
        assert ids == expected_ids

    measured_projection = "a.id, a.a_payload, b.b_payload_1, b.b_payload_2, b.b_payload_3"
    for _ in range(WARMUP_RUNS):
        for mode in modes:
            run_mode(mode, measured_projection, "Null")

    elapsed_by_mode = {mode: [] for mode in modes}
    for round_index in range(MEASURED_RUNS):
        order = modes[round_index % len(modes) :] + modes[: round_index % len(modes)]
        for mode in order:
            started_at = time.perf_counter()
            run_mode(mode, measured_projection, "Null")
            elapsed = time.perf_counter() - started_at
            elapsed_by_mode[mode].append(elapsed)
            print("SHUFFLE_PERF_RUN " + json.dumps({"mode": mode, "round": round_index, "elapsed_seconds": elapsed}, sort_keys=True))

    print(
        "SHUFFLE_PERF_CONTEXT "
        + json.dumps(
            {
                "left_rows": LEFT_ROWS,
                "right_rows": RIGHT_ROWS,
                "shards": SHARD_COUNT,
                "warmup_runs": WARMUP_RUNS,
                "measured_runs": MEASURED_RUNS,
                "output_rows": len(expected_ids),
            },
            sort_keys=True,
        )
    )
    summaries = {}
    for mode in modes:
        values = elapsed_by_mode[mode]
        summaries[mode] = {
            "mean_seconds": statistics.mean(values),
            "median_seconds": statistics.median(values),
            "min_seconds": min(values),
            "max_seconds": max(values),
            "runs_seconds": values,
        }
        print("SHUFFLE_PERF_SUMMARY " + json.dumps({"mode": mode, **summaries[mode]}, sort_keys=True))

    result = {
        "context": {
            "left_rows": LEFT_ROWS,
            "right_rows": RIGHT_ROWS,
            "shards": SHARD_COUNT,
            "warmup_runs": WARMUP_RUNS,
            "measured_runs": MEASURED_RUNS,
            "output_rows": len(expected_ids),
        },
        "summaries": summaries,
    }
    os.makedirs(os.path.dirname(os.path.abspath(RESULT_PATH)), exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as result_file:
        json.dump(result, result_file, indent=2, sort_keys=True)
