import json
import os
import time

from dataclasses import dataclass

from helpers.cluster import ClickHouseCluster


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

SHARD_COUNT = 3


BENCHMARK_LEFT_LOCAL_TABLE = "a_bench_local"
BENCHMARK_RIGHT_LOCAL_TABLE = "b_bench_local"
BENCHMARK_RIGHT_FILTERED_LOCAL_TABLE = "b_bench_filtered_local"
BENCHMARK_LEFT_DISTRIBUTED_TABLE = "a_bench_dist"
BENCHMARK_RIGHT_DISTRIBUTED_TABLE = "b_bench_dist"
BENCHMARK_RIGHT_FILTERED_DISTRIBUTED_TABLE = "b_bench_filtered_dist"

BENCHMARK_LEFT_ROWS = int(os.environ.get("CLICKHOUSE_SHUFFLE_BENCHMARK_LEFT_ROWS", "30000"))
BENCHMARK_RIGHT_ROWS = int(os.environ.get("CLICKHOUSE_SHUFFLE_BENCHMARK_RIGHT_ROWS", "6000000"))
BENCHMARK_FILTER_THRESHOLD = int(os.environ.get("CLICKHOUSE_SHUFFLE_BENCHMARK_FILTER_THRESHOLD", "2"))
BENCHMARK_FILTER_DIVISOR = 100
BENCHMARK_WARMUP_RUNS = int(os.environ.get("CLICKHOUSE_SHUFFLE_BENCHMARK_WARMUP_RUNS", "1"))
BENCHMARK_MEASURED_RUNS = int(os.environ.get("CLICKHOUSE_SHUFFLE_BENCHMARK_MEASURED_RUNS", "3"))


@dataclass(frozen=True)
class BenchmarkScenario:
    name: str
    left_layout: str
    right_filter: str
    left_row_count: int = BENCHMARK_LEFT_ROWS
    right_row_count: int = BENCHMARK_RIGHT_ROWS


@dataclass(frozen=True)
class BenchmarkMode:
    name: str
    label: str


@dataclass
class BenchmarkMeasurement:
    mode_name: str
    query_id: str
    result: str
    elapsed_seconds: float
    query_duration_ms: int
    initial_read_rows: int
    initial_read_bytes: int
    total_worker_read_rows: int
    total_worker_read_bytes: int
    worker_query_count: int


BENCHMARK_MODES = [
    BenchmarkMode(name="global", label="GLOBAL JOIN"),
    BenchmarkMode(name="allow", label="JOIN + allow"),
    BenchmarkMode(name="shuffle", label="JOIN + distributed_shuffle_join"),
]


def flush_query_logs():
    for node in NODES.values():
        node.query("SYSTEM FLUSH LOGS")


def build_left_local_table_ddl():
    return f"""
        CREATE TABLE default.{BENCHMARK_LEFT_LOCAL_TABLE}
        (
            id UInt64,
            a_metric UInt64,
            a_payload String,
            a_tag UInt16
        )
        ENGINE = MergeTree()
        ORDER BY id
    """


def build_right_local_table_ddl():
    return f"""
        CREATE TABLE default.{BENCHMARK_RIGHT_LOCAL_TABLE}
        (
            id UInt64,
            b_metric UInt64,
            b_filter_bucket UInt16,
            b_payload_text String,
            b_payload_text_extra_1 String,
            b_payload_text_extra_2 String,
            b_tag UInt16
        )
        ENGINE = MergeTree()
        ORDER BY id
    """


def build_right_filtered_local_table_ddl():
    return f"""
        CREATE TABLE default.{BENCHMARK_RIGHT_FILTERED_LOCAL_TABLE}
        (
            id UInt64,
            b_metric UInt64,
            b_filter_bucket UInt16,
            b_payload_text String,
            b_payload_text_extra_1 String,
            b_payload_text_extra_2 String,
            b_tag UInt16
        )
        ENGINE = MergeTree()
        ORDER BY id
    """


def build_left_distributed_table_ddl():
    return f"""
        CREATE TABLE default.{BENCHMARK_LEFT_DISTRIBUTED_TABLE} AS default.{BENCHMARK_LEFT_LOCAL_TABLE}
        ENGINE = Distributed('test_cluster', 'default', '{BENCHMARK_LEFT_LOCAL_TABLE}', cityHash64(id))
    """


def build_right_distributed_table_ddl():
    return f"""
        CREATE TABLE default.{BENCHMARK_RIGHT_DISTRIBUTED_TABLE} AS default.{BENCHMARK_RIGHT_LOCAL_TABLE}
        ENGINE = Distributed('test_cluster', 'default', '{BENCHMARK_RIGHT_LOCAL_TABLE}', cityHash64(id))
    """


def build_right_filtered_distributed_table_ddl():
    return f"""
        CREATE TABLE default.{BENCHMARK_RIGHT_FILTERED_DISTRIBUTED_TABLE} AS default.{BENCHMARK_RIGHT_FILTERED_LOCAL_TABLE}
        ENGINE = Distributed('test_cluster', 'default', '{BENCHMARK_RIGHT_FILTERED_LOCAL_TABLE}', cityHash64(id))
    """


def build_left_storage_bucket_expression(id_expression, left_layout):
    if left_layout == "aligned":
        return f"modulo(cityHash64({id_expression}), _CAST({SHARD_COUNT}, 'UInt64'))"

    if left_layout == "random":
        return f"modulo(cityHash64({id_expression}, toUInt64(20260420)), _CAST({SHARD_COUNT}, 'UInt64'))"

    raise ValueError(f"Unknown left layout: {left_layout}")


def build_right_storage_bucket_expression(id_expression):
    return f"modulo(cityHash64({id_expression}, toUInt64(20260421)), _CAST({SHARD_COUNT}, 'UInt64'))"


def drop_benchmark_tables():
    for node in NODES.values():
        node.query(f"DROP TABLE IF EXISTS default.{BENCHMARK_LEFT_DISTRIBUTED_TABLE} SYNC")
        node.query(f"DROP TABLE IF EXISTS default.{BENCHMARK_RIGHT_DISTRIBUTED_TABLE} SYNC")
        node.query(f"DROP TABLE IF EXISTS default.{BENCHMARK_RIGHT_FILTERED_DISTRIBUTED_TABLE} SYNC")
        node.query(f"DROP TABLE IF EXISTS default.{BENCHMARK_LEFT_LOCAL_TABLE} SYNC")
        node.query(f"DROP TABLE IF EXISTS default.{BENCHMARK_RIGHT_LOCAL_TABLE} SYNC")
        node.query(f"DROP TABLE IF EXISTS default.{BENCHMARK_RIGHT_FILTERED_LOCAL_TABLE} SYNC")


def create_benchmark_tables():
    drop_benchmark_tables()

    for node in SHARD_NODES.values():
        node.query(build_left_local_table_ddl())
        node.query(build_right_local_table_ddl())
        node.query(build_right_filtered_local_table_ddl())

    for node in NODES.values():
        node.query(build_left_distributed_table_ddl())
        node.query(build_right_distributed_table_ddl())
        node.query(build_right_filtered_distributed_table_ddl())


def insert_left_data(scenario):
    for shard_index, node in SHARD_NODES.items():
        target_bucket = shard_index - 1
        node.query(
            f"""
                INSERT INTO default.{BENCHMARK_LEFT_LOCAL_TABLE}
                SELECT
                    id,
                    id * 10 AS a_metric,
                    concat(
                        'left_',
                        toString(id),
                        '_',
                        toString(cityHash64(id))
                    ) AS a_payload,
                    toUInt16(modulo(id, 1024)) AS a_tag
                FROM
                (
                    SELECT toUInt64(number + 1) AS id
                    FROM numbers({scenario.left_row_count})
                )
                WHERE {build_left_storage_bucket_expression('id', scenario.left_layout)} = _CAST({target_bucket}, 'UInt64')
            """
        )


def insert_right_data(scenario):
    for shard_index, node in SHARD_NODES.items():
        target_bucket = shard_index - 1
        base_right_rows_sql = f"""
            SELECT
                id,
                id * 100 AS b_metric,
                toUInt16(modulo(id, {BENCHMARK_FILTER_DIVISOR})) AS b_filter_bucket,
                concat(
                    toString(cityHash64(id, toUInt64(1))),
                    ':',
                    toString(cityHash64(id, toUInt64(2))),
                    ':',
                    toString(cityHash64(id, toUInt64(3))),
                    ':',
                    toString(cityHash64(id, toUInt64(4))),
                    ':',
                    toString(cityHash64(id, toUInt64(5))),
                    ':',
                    toString(cityHash64(id, toUInt64(6))),
                    ':',
                    toString(cityHash64(id, toUInt64(7))),
                    ':',
                    toString(cityHash64(id, toUInt64(8)))
                ) AS b_payload_text,
                concat(
                    toString(cityHash64(id, toUInt64(9))),
                    ':',
                    toString(cityHash64(id, toUInt64(10))),
                    ':',
                    toString(cityHash64(id, toUInt64(11))),
                    ':',
                    toString(cityHash64(id, toUInt64(12))),
                    ':',
                    toString(cityHash64(id, toUInt64(13))),
                    ':',
                    toString(cityHash64(id, toUInt64(14))),
                    ':',
                    toString(cityHash64(id, toUInt64(15))),
                    ':',
                    toString(cityHash64(id, toUInt64(16)))
                ) AS b_payload_text_extra_1,
                concat(
                    toString(cityHash64(id, toUInt64(17))),
                    ':',
                    toString(cityHash64(id, toUInt64(18))),
                    ':',
                    toString(cityHash64(id, toUInt64(19))),
                    ':',
                    toString(cityHash64(id, toUInt64(20))),
                    ':',
                    toString(cityHash64(id, toUInt64(21))),
                    ':',
                    toString(cityHash64(id, toUInt64(22))),
                    ':',
                    toString(cityHash64(id, toUInt64(23))),
                    ':',
                    toString(cityHash64(id, toUInt64(24)))
                ) AS b_payload_text_extra_2,
                toUInt16(modulo(cityHash64(id), 2048)) AS b_tag
            FROM
            (
                SELECT toUInt64(number + 1) AS id
                FROM numbers({scenario.right_row_count})
            )
            WHERE {build_right_storage_bucket_expression('id')} = _CAST({target_bucket}, 'UInt64')
        """
        node.query(f"INSERT INTO default.{BENCHMARK_RIGHT_LOCAL_TABLE} {base_right_rows_sql}")
        node.query(
            f"""
                -- `distributed_shuffle_join` 现在只支持直接 `TableNode`。
                -- 因此高选择性场景用一张单独的右表来物化过滤后的结果，
                -- 避免把右侧写成带 `WHERE` 的子查询后绕开 `shuffle` rewrite。
                INSERT INTO default.{BENCHMARK_RIGHT_FILTERED_LOCAL_TABLE}
                {base_right_rows_sql}
                AND modulo(id, {BENCHMARK_FILTER_DIVISOR}) < {BENCHMARK_FILTER_THRESHOLD}
            """
        )


def reset_benchmark_state(scenario):
    create_benchmark_tables()
    insert_left_data(scenario)
    insert_right_data(scenario)


def get_right_table_name(scenario):
    if scenario.right_filter == "none":
        return BENCHMARK_RIGHT_DISTRIBUTED_TABLE

    if scenario.right_filter == "selective":
        return BENCHMARK_RIGHT_FILTERED_DISTRIBUTED_TABLE

    raise ValueError(f"Unknown right filter mode: {scenario.right_filter}")


def build_benchmark_query(mode, scenario):
    right_table_name = get_right_table_name(scenario)

    if mode.name == "global":
        join_sql = f"GLOBAL INNER JOIN default.{right_table_name} AS b USING (id)"
        settings_sql = "enable_analyzer = 1"
    elif mode.name == "allow":
        join_sql = f"INNER JOIN default.{right_table_name} AS b USING (id)"
        settings_sql = "enable_analyzer = 1, distributed_product_mode = 'allow', prefer_global_in_and_join = 0"
    elif mode.name == "shuffle":
        join_sql = f"INNER JOIN default.{right_table_name} AS b USING (id)"
        settings_sql = "enable_analyzer = 1, distributed_shuffle_join = 1"
    else:
        raise ValueError(f"Unknown benchmark mode: {mode.name}")

    return f"""
        SELECT
            count() AS joined_rows,
            sum(a.a_metric) AS left_metric_sum,
            sum(b.b_metric) AS right_metric_sum,
            sum(cityHash64(a.a_payload)) AS left_payload_hash_sum,
            sum(
                cityHash64(
                    b.b_payload_text,
                    b.b_payload_text_extra_1,
                    b.b_payload_text_extra_2
                )
            ) AS right_payload_hash_sum
        FROM default.{BENCHMARK_LEFT_DISTRIBUTED_TABLE} AS a
        {join_sql}
        SETTINGS {settings_sql}
    """


def build_benchmark_query_id(scenario, mode, round_index):
    return (
        f"distributed_shuffle_benchmark_"
        f"{scenario.name}_{mode.name}_"
        f"{os.getpid()}_{round_index}"
    )


def collect_query_log_rows(node, initial_query_id):
    result = node.query(
        f"""
            SELECT
                query_id,
                initial_query_id,
                is_initial_query,
                query_duration_ms,
                read_rows,
                read_bytes,
                result_rows,
                query
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND initial_query_id = '{initial_query_id}'
            ORDER BY event_time_microseconds, query_id
            FORMAT JSONEachRow
        """
    )

    rows = []
    for line in result.splitlines():
        if line.strip():
            rows.append(json.loads(line))

    return rows


def collect_query_log_rows_by_node(initial_query_id):
    return {
        node_name: collect_query_log_rows(node, initial_query_id)
        for node_name, node in NODES.items()
    }


def run_measurement(mode, scenario, round_index):
    query = build_benchmark_query(mode, scenario)
    query_id = build_benchmark_query_id(scenario, mode, round_index)

    started_at = time.perf_counter()
    result = node1.query(
        query,
        query_id=query_id,
        settings={"log_queries": 1},
    )
    elapsed_seconds = time.perf_counter() - started_at

    flush_query_logs()
    rows_by_node = collect_query_log_rows_by_node(query_id)

    initial_rows = [
        row
        for row in rows_by_node["node1"]
        if row["query_id"] == query_id and row["is_initial_query"]
    ]
    assert len(initial_rows) == 1

    worker_rows = [
        row
        for rows in rows_by_node.values()
        for row in rows
        if not row["is_initial_query"]
    ]

    initial_row = initial_rows[0]

    return BenchmarkMeasurement(
        mode_name=mode.name,
        query_id=query_id,
        result=result,
        elapsed_seconds=elapsed_seconds,
        query_duration_ms=initial_row["query_duration_ms"],
        initial_read_rows=initial_row["read_rows"],
        initial_read_bytes=initial_row["read_bytes"],
        total_worker_read_rows=sum(row["read_rows"] for row in worker_rows),
        total_worker_read_bytes=sum(row["read_bytes"] for row in worker_rows),
        worker_query_count=len(worker_rows),
    )


def run_warmup(mode, scenario, round_index):
    node1.query(build_benchmark_query(mode, scenario), settings={"log_queries": 0})


def summarize_measurements(measurements):
    assert measurements

    return {
        "elapsed_seconds_avg": sum(item.elapsed_seconds for item in measurements) / len(measurements),
        "query_duration_ms_avg": sum(item.query_duration_ms for item in measurements) / len(measurements),
        "initial_read_rows_avg": sum(item.initial_read_rows for item in measurements) / len(measurements),
        "initial_read_bytes_avg": sum(item.initial_read_bytes for item in measurements) / len(measurements),
        "worker_read_rows_avg": sum(item.total_worker_read_rows for item in measurements) / len(measurements),
        "worker_read_bytes_avg": sum(item.total_worker_read_bytes for item in measurements) / len(measurements),
        "worker_query_count_avg": sum(item.worker_query_count for item in measurements) / len(measurements),
    }


def print_scenario_summary(scenario, mode_to_measurements):
    print(f"Benchmark scenario: {scenario.name}")
    print(f"left_layout={scenario.left_layout}")
    print(f"right_filter={scenario.right_filter}")
    print(f"left_row_count={scenario.left_row_count}")
    print(f"right_row_count={scenario.right_row_count}")
    print(f"warmup_runs={BENCHMARK_WARMUP_RUNS}")
    print(f"measured_runs={BENCHMARK_MEASURED_RUNS}")

    for mode in BENCHMARK_MODES:
        summary = summarize_measurements(mode_to_measurements[mode.name])
        print(
            f"{mode.label}: "
            f"elapsed_avg={summary['elapsed_seconds_avg']:.6f}s, "
            f"query_duration_avg={summary['query_duration_ms_avg']:.2f}ms, "
            f"initial_read_rows_avg={summary['initial_read_rows_avg']:.2f}, "
            f"initial_read_bytes_avg={summary['initial_read_bytes_avg']:.2f}, "
            f"worker_read_rows_avg={summary['worker_read_rows_avg']:.2f}, "
            f"worker_read_bytes_avg={summary['worker_read_bytes_avg']:.2f}, "
            f"worker_query_count_avg={summary['worker_query_count_avg']:.2f}"
        )
