"""AML continuous: time to detect parses into the scorecard, and a short
ingest_ratio names what bounded intake."""

from __future__ import annotations

import functools
import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from lakebench.metrics.collector import (
    MetricsCollector,
    PipelineBenchmark,
    StageMetrics,
    build_pipeline_benchmark,
    ttd_percentile,
)

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
_T0 = datetime(2026, 9, 25, 10, 48)


@functools.cache
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


@pytest.mark.parametrize(("schema", "has_key"), [("financial", True), ("customer360", False)])
def test_unmeasured_time_to_detect_is_none_for_aml_and_absent_for_c360(schema, has_key):
    scores = _pb(bronze_batches=16, bronze_ms=109_737.5, schema=schema).to_dict()["scores"]
    assert ("time_to_detect_seconds" in scores) is has_key
    if has_key:
        assert scores["time_to_detect_seconds"] is None


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


_UNKNOWN = "unknown snapshot"


@pytest.mark.parametrize(
    ("max_carry", "steps"),
    [
        # an unmeasured tick is carried, not dropped: tick 3 is measured against
        # tick 2's baseline, so tick 2's new alerts count
        (
            3,
            [
                ("begin", (10, None), (10, None)),
                ("measured", (), None),
                ("begin", (20, 100.0), (20, 100.0)),
                ("begin", (30, 200.0), (20, 100.0)),
                ("measured", (), None),
                ("begin", (40, 300.0), (40, 300.0)),
            ],
        ),
        # a failed lookup falls back to the snapshot read after the last
        # measured tick, which is the same point in history
        (
            None,
            [
                ("begin", (5, 1.0), (5, 1.0)),
                ("measured", (6,), None),
                ("begin", (_UNKNOWN, 2.0), (6, 2.0)),
            ],
        ),
        # a failed lookup with no fallback is not carried and uses no carry budget
        (
            None,
            [
                ("begin", (_UNKNOWN, 1.0), (_UNKNOWN, 1.0)),
                ("begin", (7, 2.0), (7, 2.0)),
                ("begin", (8, 3.0), (7, 2.0)),
            ],
        ),
        # the carried snapshot may be expired by in-stream maintenance, so it
        # gives way after max_carry
        (
            2,
            [
                ("begin", (1, None), (1, None)),
                ("begin", (2, 5.0), (1, None)),
                ("begin", (3, 6.0), (1, None)),
                ("begin", (4, 7.0), (4, 7.0)),
            ],
        ),
        # after max_carry the fallback snapshot is as old as the carried one
        (
            1,
            [
                ("begin", (1, None), (1, None)),
                ("measured", (2,), None),
                ("begin", (3, 1.0), (3, 1.0)),
                ("begin", (4, 2.0), (3, 1.0)),
                ("begin", (_UNKNOWN, 3.0), (_UNKNOWN, 3.0)),
            ],
        ),
    ],
    ids=["carried", "fallback", "no-fallback", "carry-exhausted", "fallback-dropped"],
)
def test_ttd_baseline_never_drops_an_unmeasured_tick(max_carry, steps):
    c = _common()
    b = c.TtdBaseline() if max_carry is None else c.TtdBaseline(max_carry=max_carry)

    def real(args):
        return tuple(c.TTD_SNAPSHOT_UNKNOWN if a == _UNKNOWN else a for a in args)

    for action, args, expected in steps:
        if action == "measured":
            b.measured(*args)
        else:
            assert b.begin(*real(args)) == real(expected)


def test_every_cycle_unmeasured_is_counted():
    log = _prefixed(
        [
            "Cycle 1: aggregating 10 Silver records",
            "Cycle 2: aggregating 20 Silver records",
        ]
    )
    m = MetricsCollector().parse_streaming_logs(log, "gold-refresh")
    assert m.ttd_alerts is None
    assert m.ttd_unmeasured_cycles == 2
    stage = StageMetrics(
        stage_name="gold",
        stage_type="streaming",
        engine="spark",
        elapsed_seconds=1800,
        ttd_unmeasured_cycles=2,
    )
    pb = PipelineBenchmark(
        run_id="t",
        deployment_name="t",
        pipeline_mode="sustained",
        start_time=_T0,
        end_time=_T0 + timedelta(seconds=1800),
        stages=[stage],
        config_snapshot={"workload_schema": "financial"},
    )
    pb.compute_aggregates()
    scores = pb.to_dict()["scores"]
    assert scores["time_to_detect_unmeasured_cycles"] == 2
    assert scores["time_to_detect_seconds"] is None


def test_stage_capacity_is_in_the_autosizer_units():
    """Raw MB/s per core at full busy, from window rows, busy time, datagen
    bytes per row and the stage's cores."""

    def stage(name, batches, ms, cores):
        return StageMetrics(
            stage_name=name,
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=900,
            input_rows=9_000_000,
            window_input_rows=9_000_000,
            latency_ms=ms,
            total_batches=batches,
            executor_count=cores // 4,
            executor_cores=4,
        )

    pb = PipelineBenchmark(
        run_id="t",
        deployment_name="t",
        pipeline_mode="sustained",
        start_time=_T0,
        end_time=_T0 + timedelta(seconds=900),
        success=True,
        stages=[stage("bronze", 20, 30_000.0, 20), stage("silver", 3, 300_000.0, 40)],
        config_snapshot={"datagen_bytes_per_row": 400.0, "datagen_mb_s_per_core": 31.0},
    )
    pb.compute_aggregates()
    cap = pb.to_dict()["stage_capacity"]
    assert cap["bronze"]["busy_fraction"] == pytest.approx(0.667, abs=1e-3)
    assert cap["bronze"]["mb_s_per_core"] == pytest.approx(0.3)
    assert cap["silver"]["mb_s_per_core"] == pytest.approx(0.1)
