"""AML continuous: time to detect reaches the scorecard, and a short
ingest_ratio says what bounded intake.

run-20260925-104452-21bf3a (AML, continuous, scale 10) reported no time to
detect at all: gold_refresh_financial logged no such measurement and the
collector had no field for it. The same run read pipeline_saturated=True
with no way to tell a slow bronze from a trickle rate that a 1800 s window
could never drain; its bronze ran 16 back-to-back 110 s micro-batches under a
30 s trigger, so bronze's own processing was the limit.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from lakebench.metrics.collector import (
    MetricsCollector,
    PipelineBenchmark,
    StageMetrics,
    StreamingJobMetrics,
    build_pipeline_benchmark,
    ttd_percentile,
)

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
_T0 = datetime(2026, 9, 25, 10, 48)


def _common():
    spec = importlib.util.spec_from_file_location("lb_common_ttd", SCRIPTS / "common.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _prefixed(lines):
    return "\n".join(f"[lb] 2026-09-25T11:00:00.000000 - {ln}" for ln in lines)


def _gold_log():
    c = _common()
    return _prefixed(
        [
            "Cycle 1: aggregating 19,431,000 Silver records",
            "Cycle 1: refreshed gold.alerts in 250.0s",
            "Cycle 1: data freshness 400s",
            c.ttd_line(
                1,
                {
                    "alerts": 10,
                    "late": 0,
                    "unmatched": 1,
                    "max_s": 318.0,
                    "bin_s": 10,
                    "bins": {25: 4, 31: 6},
                },
            ),
            "Cycle 2: aggregating 48,577,500 Silver records",
            "Cycle 2: refreshed gold.alerts in 260.0s",
            c.ttd_line(
                2,
                {
                    "alerts": 10,
                    "late": 1,
                    "unmatched": 0,
                    "max_s": 612.4,
                    "bin_s": 10,
                    "bins": {40: 9, 61: 1},
                },
            ),
            # A cycle with nothing new still logs its (empty) line.
            "Cycle 3: aggregating 58,000,000 Silver records",
            c.ttd_line(
                3,
                {"alerts": 0, "late": 0, "unmatched": 0, "max_s": None, "bin_s": 10, "bins": {}},
            ),
            # A cycle whose measurement failed logs no line.
            "Cycle 4: aggregating 68,000,000 Silver records",
            "[metrics] time to detect unavailable on cycle 4: boom",
        ]
    )


def test_time_to_detect_lines_parse_and_merge():
    m = MetricsCollector().parse_streaming_logs(_gold_log(), "gold-refresh")
    assert m.ttd_alerts == 20
    assert m.ttd_unmatched == 1
    assert m.ttd_late == 1
    assert m.ttd_unmeasured_cycles == 1
    assert m.ttd_max_seconds == pytest.approx(612.4)
    # Merged bins 25:4, 31:6, 40:9, 61:1. Median = 10th value, in bin 31.
    assert m.ttd_p50_seconds == pytest.approx(320.0)
    # 95th percentile = 19th value, in bin 40.
    assert m.ttd_p95_seconds == pytest.approx(410.0)
    # The existing gold lines still parse alongside.
    assert m.freshness_seconds == pytest.approx(400)
    assert m.total_batches == 4


def test_time_to_detect_reaches_the_scores():
    run_start = _T0
    collector = MetricsCollector()
    run = collector.start_run("r", "d", {"workload_schema": "financial", "scale": 10})
    gold = collector.parse_streaming_logs(_gold_log(), "gold-refresh")
    gold.elapsed_seconds = 1800
    gold.success = True
    run.streaming.append(gold)
    run.start_time = run_start
    run.end_time = run_start + timedelta(seconds=1800)
    pb = build_pipeline_benchmark(run)
    scores = pb.to_dict()["scores"]
    assert scores["time_to_detect_seconds"] == pytest.approx(320.0)
    assert scores["time_to_detect_p95_seconds"] == pytest.approx(410.0)
    assert scores["time_to_detect_max_seconds"] == pytest.approx(612.4)
    assert scores["time_to_detect_alerts"] == 20
    assert scores["time_to_detect_late_alerts"] == 1
    assert scores["time_to_detect_unmeasured_cycles"] == 1


def test_financial_run_without_a_measurement_says_none():
    pb = _pb(bronze_batches=16, bronze_ms=109_737.5, schema="financial")
    scores = pb.to_dict()["scores"]
    assert "time_to_detect_seconds" in scores
    assert scores["time_to_detect_seconds"] is None


def test_c360_scores_carry_no_time_to_detect_keys():
    pb = _pb(bronze_batches=16, bronze_ms=109_737.5, schema="customer360")
    assert "time_to_detect_seconds" not in pb.to_dict()["scores"]


def test_percentile_is_the_bin_upper_edge_capped_at_max():
    assert ttd_percentile({3: 1}, 10, 0.5, 34.2) == pytest.approx(34.2)
    assert ttd_percentile({3: 1, 9: 1}, 10, 0.5, 95.0) == pytest.approx(40.0)


def _pb(bronze_batches, bronze_ms, bronze_rows=155_610_788, schema="financial"):
    stages = [
        StageMetrics(
            stage_name="bronze",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=1800,
            input_rows=bronze_rows,
            latency_ms=bronze_ms,
            total_batches=bronze_batches,
        ),
        StageMetrics(
            stage_name="gold",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=1800,
            freshness_seconds=826.0,
        ),
    ]
    pb = PipelineBenchmark(
        run_id="t",
        deployment_name="t",
        pipeline_mode="sustained",
        start_time=_T0,
        end_time=_T0 + timedelta(seconds=1800),
        success=True,
        stages=stages,
        config_snapshot={"datagen_output_rows": 266_666_400, "workload_schema": schema},
    )
    pb.compute_aggregates()
    return pb


def test_back_to_back_bronze_is_a_capacity_limit():
    """The live run: 16 batches x 109.7 s in an 1800 s window."""
    pb = _pb(bronze_batches=16, bronze_ms=109_737.5)
    assert pb.ingest_ratio == pytest.approx(0.5835, abs=1e-4)
    assert pb.bronze_busy_fraction == pytest.approx(0.975, abs=1e-3)
    assert pb.intake_limit == "bronze_capacity"
    assert pb.pipeline_saturated is True


def test_idle_bronze_is_not_called_a_capacity_limit():
    """Same rows, but bronze was inside a batch for 27% of the window: the
    limit was not bronze's processing (a trigger cap, a late start or a
    stall). The ratio verdict is kept: idle time does not prove the
    pipeline kept pace."""
    pb = _pb(bronze_batches=60, bronze_ms=8_000.0)
    assert pb.intake_limit == "below_bronze_capacity"
    assert pb.pipeline_saturated is True
    assert pb.to_dict()["scores"]["intake_limit"] == "below_bronze_capacity"


def test_kept_up_intake_has_no_limit():
    pb = _pb(bronze_batches=16, bronze_ms=5_000.0, bronze_rows=266_666_400)
    assert pb.intake_limit == "none"
    assert pb.pipeline_saturated is False


def test_unknown_bronze_timing_keeps_the_ratio_verdict():
    pb = _pb(bronze_batches=0, bronze_ms=None)
    assert pb.bronze_busy_fraction is None
    assert pb.intake_limit is None
    assert pb.pipeline_saturated is True


def test_gold_refresh_logs_time_to_detect():
    src = (SCRIPTS / "gold_refresh_financial.py").read_text()
    assert "if _log_time_to_detect(spark, cycle, ttd_base, detection_end_s):" in src
    assert "ttd_baseline.measured()" in src
    assert "log(ttd_line(cycle, stats))" in src


def test_an_unmeasured_tick_is_carried_not_dropped():
    c = _common()
    b = c.TtdBaseline(max_carry=3)
    assert b.begin(10, None) == (10, None)
    b.measured()
    # Tick 2 starts from its own snapshot, then fails to measure.
    assert b.begin(20, 100.0) == (20, 100.0)
    # Tick 3 is measured against tick 2's baseline, so tick 2's new alerts count.
    assert b.begin(30, 200.0) == (20, 100.0)
    b.measured()
    assert b.begin(40, 300.0) == (40, 300.0)


def test_a_failed_lookup_is_not_carried():
    c = _common()
    b = c.TtdBaseline()
    assert b.begin(c.TTD_SNAPSHOT_UNKNOWN, 1.0) == (c.TTD_SNAPSHOT_UNKNOWN, 1.0)
    assert b.begin(7, 2.0) == (7, 2.0)


def test_a_carried_baseline_gives_way_after_max_carry():
    """The carried snapshot may be expired by in-stream maintenance."""
    c = _common()
    b = c.TtdBaseline(max_carry=2)
    assert b.begin(1, None) == (1, None)
    assert b.begin(2, 5.0) == (1, None)
    assert b.begin(3, 6.0) == (3, 6.0)


def test_streaming_job_metrics_default_to_unmeasured():
    m = StreamingJobMetrics(job_name="x", job_type="gold-refresh")
    assert m.ttd_alerts is None and m.ttd_p50_seconds is None
