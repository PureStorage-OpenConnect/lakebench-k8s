"""Every continuous QpH figure carries the number of rounds behind it.

lb16-cs (2026-09-27): Spark Thrift completed 4 in-stream rounds against 5 on
Trino and DuckDB (runs 20260927-073533-9de9c9 and -500d2c), so the QpH
medians were over different n. DESIGN 2.4 lists benchmark iterations as an
execution condition: runs with different round counts are comparable but not
like-for-like.
"""

from __future__ import annotations

from datetime import datetime

from lakebench.cli._compare import _build_comparison
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


def test_batch_identity_has_no_round_key():
    """Batch baselines keep their stored identity keys."""
    assert "benchmark rounds" not in ex.identity(_exp("batch", None))


def _record(rounds: int, qph: float) -> dict:
    return {
        "run_id": f"r{rounds}",
        "success": True,
        "experiment": _exp("sustained", rounds),
        "pipeline_benchmark": {
            "pipeline_mode": "sustained",
            "scores": {"composite_qph": qph, "composite_qph_rounds": rounds},
            "query_benchmark": {"query_set_id": "qs8-32043638dbc4"},
        },
    }


def test_compare_shows_the_counts_and_withholds_like_for_like():
    c = _build_comparison("thrift", _record(4, 456.2), "trino", _record(5, 1239.5))
    assert c["like_for_like"] is False
    assert "benchmark rounds differs (4 vs 5)" in c["condition_differences"]
    assert any("median of 4 in-stream round(s) and for B of 5" in w for w in c["warnings"])
    rows = {r["metric"]: r for r in c["metrics"]}
    assert (rows["composite_qph_rounds"]["config_a"], rows["composite_qph_rounds"]["config_b"]) == (
        4,
        5,
    )


def test_report_shows_the_round_count(tmp_path):
    from lakebench.reports.generator import ReportGenerator

    html = ReportGenerator(output_dir=tmp_path)._generate_experiment_section(
        _continuous_run([200.0, 250.0])
    )
    assert "In-stream QpH rounds" in html


def test_perf_gate_and_reproduce_do_not_refuse_on_the_round_count():
    """Review finding: the count is an outcome (a slower build fits fewer
    rounds), so refusing on it would turn a regression into "not comparable"."""
    baseline = _exp("sustained", 5)
    run = _exp("sustained", 4)
    reasons = ex.stored_identity_refusals(
        ex.identity(baseline), ex.result_fingerprints(baseline), run, "baseline"
    )
    assert not any("benchmark rounds" in r for r in reasons), reasons
    # A reference stored before the key existed is not "older" for it.
    old = {k: v for k, v in ex.identity(baseline).items() if k != "benchmark rounds"}
    reasons = ex.stored_identity_refusals(old, ex.result_fingerprints(baseline), run, "package")
    assert not any("older experiment identity" in r for r in reasons), reasons
