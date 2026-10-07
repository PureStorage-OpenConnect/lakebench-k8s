"""Every continuous QpH figure carries the number of rounds behind it.

lb16-cs (2026-09-27): Spark Thrift completed 4 in-stream rounds against 5 on
Trino and DuckDB (runs 20260927-073533-9de9c9 and -500d2c), so the QpH
medians were over different n. DESIGN 2.4 lists benchmark iterations as an
execution condition: runs with different round counts are comparable but not
like-for-like.
"""

from __future__ import annotations

from datetime import datetime

from lakebench.metrics import experiment as ex
from lakebench.metrics.collector import (
    BenchmarkMetrics,
    MetricsCollector,
    StreamingJobMetrics,
    build_config_snapshot,
    build_pipeline_benchmark,
)
from tests.conftest import make_config, stub_experiment


def _round(qph: float) -> BenchmarkMetrics:
    return BenchmarkMetrics(mode="power", cache="hot", scale=1, qph=qph, total_seconds=30.0)


def _continuous_run(qphs):
    cfg = make_config(architecture={"pipeline": {"mode": "continuous"}})
    run = MetricsCollector().start_run(
        "20260927-120000-aaaaaa", cfg.name, build_config_snapshot(cfg, run_mode="continuous")
    )
    run.start_time = datetime(2026, 9, 27, 12)
    run.benchmark_rounds = [_round(q) for q in qphs]
    run.streaming = [
        StreamingJobMetrics(
            job_name="lakebench-bronze-ingest",
            job_type="bronze-ingest",
            total_batches=10,
            total_rows_processed=1000,
            elapsed_seconds=1800.0,
            success=True,
        )
    ]
    run.pipeline_benchmark = build_pipeline_benchmark(run)
    return run


def test_scores_record_the_rounds_behind_the_median():
    # A round with no QpH (every query failed) is not in the median or the count.
    scores = _continuous_run([200.0, 0.0, 250.0, 300.0]).pipeline_benchmark.to_dict()["scores"]
    assert scores["composite_qph"] == 250.0
    assert scores["composite_qph_rounds"] == 3
    assert scores["benchmark_rounds_count"] == 4


def test_post_stream_fallback_records_zero_rounds():
    run = _continuous_run([])
    run.benchmark = _round(260.0)
    scores = build_pipeline_benchmark(run).to_dict()["scores"]
    assert scores["composite_qph"] == 260.0
    assert scores["composite_qph_rounds"] == 0


def test_experiment_limits_carry_the_round_count():
    exp = _continuous_run([200.0, 0.0, 250.0]).to_dict()["experiment"]
    assert exp["limits"]["benchmark_rounds"] == 2
    assert ex.identity(exp)["benchmark rounds"] == 2


def _exp(mode: str, rounds: int | None) -> dict:
    e = stub_experiment(["Q1"], mode=mode)
    e["limits"] = {"benchmark_iterations": 1, "benchmark_rounds": rounds}
    return e


def test_different_round_counts_are_not_like_for_like():
    a, b = _exp("sustained", 4), _exp("sustained", 5)
    assert ex.identity_differences(a, b) == []
    assert ex.condition_differences(a, b) == ["benchmark rounds differs (4 vs 5)"]
    assert ex.condition_differences(a, _exp("sustained", 4)) == []


def test_zero_rounds_never_gates_against_an_in_stream_median():
    """Fix-pass finding: with 0 rounds composite_qph is the post-stream
    benchmark, a different estimator from the reference's in-stream median."""
    for ref, run in ((5, 0), (0, 3)):
        baseline, current = _exp("sustained", ref), _exp("sustained", run)
        reasons = ex.stored_identity_refusals(
            ex.identity(baseline), ex.result_fingerprints(baseline), current, "baseline"
        )
        assert any("continuous QpH estimator differs" in r for r in reasons), reasons
