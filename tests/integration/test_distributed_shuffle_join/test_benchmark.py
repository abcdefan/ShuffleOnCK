import os
import shutil

import pytest

from .benchmark_helpers import (
    BENCHMARK_MEASURED_RUNS,
    BENCHMARK_MODES,
    BENCHMARK_WARMUP_RUNS,
    BenchmarkScenario,
    cluster,
    print_scenario_summary,
    reset_benchmark_state,
    run_measurement,
    run_warmup,
)


BENCHMARK_SCENARIOS = [
    BenchmarkScenario(
        name="left_aligned_right_unfiltered",
        left_layout="aligned",
        right_filter="none",
    ),
    BenchmarkScenario(
        name="left_aligned_right_selective",
        left_layout="aligned",
        right_filter="selective",
    ),
    BenchmarkScenario(
        name="left_random_right_unfiltered",
        left_layout="random",
        right_filter="none",
    ),
    BenchmarkScenario(
        name="left_random_right_selective",
        left_layout="random",
        right_filter="selective",
    ),
]


@pytest.mark.skipif(
    os.environ.get("CLICKHOUSE_RUN_SHUFFLE_BENCHMARK") != "1",
    reason="Set CLICKHOUSE_RUN_SHUFFLE_BENCHMARK=1 to run manual `shuffle join` benchmarks",
)
@pytest.mark.parametrize("scenario", BENCHMARK_SCENARIOS, ids=lambda scenario: scenario.name)
def test_distributed_shuffle_join_benchmark(started_cluster, scenario):
    reset_benchmark_state(scenario)

    for warmup_round in range(BENCHMARK_WARMUP_RUNS):
        for mode in BENCHMARK_MODES:
            run_warmup(mode, scenario, warmup_round)

    mode_to_measurements = {mode.name: [] for mode in BENCHMARK_MODES}
    baseline_result = None

    for measured_round in range(BENCHMARK_MEASURED_RUNS):
        for mode in BENCHMARK_MODES:
            measurement = run_measurement(mode, scenario, measured_round)
            mode_to_measurements[mode.name].append(measurement)

            if baseline_result is None:
                baseline_result = measurement.result
            else:
                assert measurement.result == baseline_result

    print_scenario_summary(scenario, mode_to_measurements)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    shutil.rmtree(cluster.instances_dir, ignore_errors=True)

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()
