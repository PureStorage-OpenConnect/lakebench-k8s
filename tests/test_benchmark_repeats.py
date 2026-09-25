"""Each benchmark round times every query N times and scores the median (LB-150).

A single sample per query could not tell a change from noise: a live AML s10
post-maintenance round read every query 10-80% slower with nothing to judge
that against.
"""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from lakebench.benchmark.queries import BenchmarkQuery
from lakebench.benchmark.result import QueryExecutorResult
from lakebench.benchmark.runner import BenchmarkRunner, round_spread
from lakebench.benchmark.spread import query_samples, samples_per_query, spread

_Q = [
    BenchmarkQuery(name=f"Q{i}", display_name=f"Q{i}", query_class="scan", sql="select 1")
    for i in (1, 2)
]


def _runner(durations):
    """A runner whose executor returns *durations* in order (None = failure)."""
    runner = BenchmarkRunner.__new__(BenchmarkRunner)
    runner.catalog = "c"
    runner.silver_table = "s"
    runner.gold_table = "g"
    runner._extra_tables = {}
    it = iter(durations)

    def _exec(sql, timeout=300):
        d = next(it)
        if d is None:
            return QueryExecutorResult(sql, "trino", 300.0, 0, "", error="timeout")
        return QueryExecutorResult(sql, "trino", d, 1, "")

    runner.executor = MagicMock()
    runner.executor.adapt_query.side_effect = lambda s: s
    runner.executor.execute_query.side_effect = _exec
    return runner


def test_median_scores_and_every_sample_is_kept():
    r = _runner([3.0, 1.0, 2.0, 10.0, 12.0, 11.0])
    results = r._run_query_stream(_Q, "hot", 3)
    assert [q.elapsed_seconds for q in results] == [2.0, 11.0]
    assert results[0].samples == [3.0, 1.0, 2.0]
    d = results[0].to_dict()
    assert d["samples"] == [3.0, 1.0, 2.0]
    assert (d["min_seconds"], d["max_seconds"]) == (1.0, 3.0)
    assert d["relative_range"] == pytest.approx(1.0)


def test_first_failure_stops_the_repeats_and_fails_the_query():
    # Q1 fails on its second sample: no third attempt, no median of a timeout.
    r = _runner([1.0, None, 5.0, 5.0, 5.0])
    results = r._run_query_stream(_Q, "hot", 3)
    assert not results[0].success and results[0].error_message == "timeout"
    assert results[0].samples == [1.0, 300.0]
    assert results[1].success and results[1].elapsed_seconds == 5.0
    assert r.executor.execute_query.call_count == 5


def test_single_iteration_reads_like_the_old_record():
    r = _runner([4.0, 6.0])
    results = r._run_query_stream(_Q, "hot", 1)
    assert [q.samples for q in results] == [[4.0], [6.0]]
    assert round_spread(results)["samples_per_query"] == 1


def test_power_qph_uses_medians():
    r = _runner([3.0, 1.0, 2.0, 2.0, 2.0, 2.0])
    r.config = MagicMock()
    r.config.architecture.workload.schema_type = "customer360"
    r.config.architecture.workload.datagen.get_effective_scale.return_value = 1
    from unittest.mock import patch

    with patch("lakebench.benchmark.runner.get_benchmark_queries", return_value=_Q):
        result = r.run_power(iterations=3)
    assert result.qph == pytest.approx(2 / 4.0 * 3600)
    d = result.to_dict()
    assert d["spread"]["samples_per_query"] == 3
    assert d["spread"]["qph_low"] == pytest.approx(round(2 / 5.0 * 3600, 1))
    assert d["spread"]["qph_high"] == pytest.approx(round(2 / 3.0 * 3600, 1))


def test_spread_helpers_treat_old_records_as_one_sample():
    old = {"name": "Q1", "elapsed_seconds": 2.5, "success": True}
    assert query_samples(old) == [2.5]
    assert samples_per_query([old]) == 1
    assert samples_per_query([{"name": "Q1", "success": False, "samples": [1.0]}]) is None
    s = spread([old])
    assert s["samples_per_query"] == 1 and s["relative_range"] == 0.0
    # Failed queries do not set the count: they stop at their first failure.
    new = {"name": "Q2", "elapsed_seconds": 2.0, "success": True, "samples": [1.0, 2.0, 3.0]}
    failed = {"name": "Q3", "elapsed_seconds": 9.0, "success": False, "samples": [9.0]}
    assert samples_per_query([new, failed]) == 3


def test_metrics_json_round_trip_keeps_samples_and_reason(tmp_path):
    from lakebench.metrics import BenchmarkMetrics, PipelineMetrics
    from lakebench.metrics.collector import build_pipeline_benchmark
    from lakebench.metrics.storage import MetricsStorage

    pm = PipelineMetrics(
        run_id="20260925-100000-abcdef",
        deployment_name="x",
        start_time=datetime(2026, 9, 25, 10, 0, 0),
        success=True,
    )
    pm.benchmark = BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1,
        qph=1800.0,
        total_seconds=2.0,
        queries=[{"name": "Q1", "elapsed_seconds": 2.0, "success": True, "samples": [1, 2, 4]}],
        iterations=3,
    )
    pm.end_time = datetime(2026, 9, 25, 10, 5, 0)
    pb = build_pipeline_benchmark(pm)
    pb.pre_compaction_qph = 1700.0
    pb.post_compaction_qph = 1800.0
    pb.maintenance_value_reason = "within noise: +5.9% is inside the within-round spread"
    pb.pre_compaction_benchmark = {"qph": 1700.0, "queries": []}
    pm.pipeline_benchmark = pb
    path = MetricsStorage(tmp_path).save_run(pm)
    raw = json.loads(path.read_text())
    scores = raw["pipeline_benchmark"]["scores"]
    assert scores["maintenance_value_pct"] is None
    assert scores["maintenance_value_reason"].startswith("within noise")
    assert scores["benchmark_samples_per_query"] == 3
    assert scores["qph_spread"]["low"] == pytest.approx(round(3600 / 4, 1))
    assert raw["pipeline_benchmark"]["query_benchmark"]["spread"]["samples_per_query"] == 3
    loaded = MetricsStorage(tmp_path).load_run(pm.run_id)
    assert loaded.pipeline_benchmark.maintenance_value_reason.startswith("within noise")
    assert loaded.pipeline_benchmark.pre_compaction_benchmark == {"qph": 1700.0, "queries": []}
