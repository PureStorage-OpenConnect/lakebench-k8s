"""Every continuous QpH figure carries the number of rounds behind it, and
runs with different round counts are not like-for-like."""

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
    run = _continuous_run([200.0, 0.0, 250.0, 300.0])
    scores = run.pipeline_benchmark.to_dict()["scores"]
    assert scores["composite_qph"] == 250.0
    assert scores["composite_qph_rounds"] == 3
    assert scores["benchmark_rounds_count"] == 4
    exp = run.to_dict()["experiment"]
    assert exp["limits"]["benchmark_rounds"] == 3
    assert ex.identity(exp)["benchmark rounds"] == 3


def test_post_stream_fallback_records_zero_rounds():
    run = _continuous_run([])
    run.benchmark = _round(260.0)
    scores = build_pipeline_benchmark(run).to_dict()["scores"]
    assert scores["composite_qph"] == 260.0
    assert scores["composite_qph_rounds"] == 0


def _exp(mode: str, rounds: int | None) -> dict:
    e = stub_experiment(["Q1"], mode=mode)
    e["limits"] = {"benchmark_iterations": 1, "benchmark_rounds": rounds}
    return e


def test_different_round_counts_are_not_like_for_like():
    a, b = _exp("sustained", 4), _exp("sustained", 5)
    assert ex.identity_differences(a, b) == []
    assert ex.condition_differences(a, b) == ["benchmark rounds differs (4 vs 5)"]
    assert ex.condition_differences(a, _exp("sustained", 4)) == []
