"""LB-145: continuous freshness must not score the idle tail of a drained corpus.

A gold cycle whose silver input has not moved since the previous cycle logs
"(silver idle)". Only the trailing idle run can be a drained corpus, and only
when every datagen row reached bronze and silver committed all of it. An idle
stretch that new data later ends is a stall and keeps its staleness.
"""

from datetime import datetime, timedelta, timezone

import pytest

from lakebench.cli._reproduce import _extract_expected_numbers
from lakebench.metrics.collector import (
    MetricsCollector,
    PipelineBenchmark,
    PipelineMetrics,
    StageMetrics,
)
from lakebench.metrics.storage import MetricsStorage

_T0 = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _gold(values):
    """Gold log with one freshness line per (seconds, idle) pair."""
    lines = []
    for i, (v, idle) in enumerate(values, start=1):
        lines.append(f"[lb] 2026-02-01T12:00:00.000Z - Cycle {i}: aggregating 1,000 Silver records")
        tag = " (silver idle)" if idle else ""
        lines.append(f"[lb] 2026-02-01T12:00:05.000Z - Cycle {i}: data freshness {v}s{tag}")
    return "\n".join(lines) + "\n"


def _parse(log, job="gold-refresh"):
    return MetricsCollector().parse_streaming_logs(log, job)


def test_trailing_idle_run_is_split_off():
    m = _parse(_gold([(40, False), (55, False), (355, True), (655, True)]))
    assert m.freshness_seconds == pytest.approx(655.0)
    assert m.freshness_active_seconds == pytest.approx(55.0)
    assert m.trailing_idle_cycles == 2


def test_mid_run_idle_stretch_stays_in_active_freshness():
    # Silver stalls for three cycles, then recovers: the stall is real.
    m = _parse(_gold([(40, False), (350, True), (650, True), (950, True), (50, False), (60, True)]))
    assert m.freshness_active_seconds == pytest.approx(950.0)
    assert m.trailing_idle_cycles == 1


def test_untagged_log_is_unchanged():
    m = _parse(_gold([(40, False), (655, False)]))
    assert m.freshness_active_seconds == pytest.approx(655.0)
    assert m.trailing_idle_cycles == 0


def test_all_idle_after_first_falls_back_to_first_cycle():
    m = _parse(_gold([(40, False), (400, True)]))
    assert m.freshness_active_seconds == pytest.approx(40.0)


SILVER_LOG = """\
[lb] 2026-02-01T12:00:01.000Z - Batch 0: transforming 600 rows
[lb] 2026-02-01T12:00:09.000Z - Batch 0: committed to ice.silver.t in 8.0s
[lb] 2026-02-01T12:01:01.000Z - Batch 1: transforming 400 rows
"""


def test_silver_committed_rows_exclude_an_unlogged_commit():
    m = _parse(SILVER_LOG, "silver-stream")
    assert m.total_rows_processed == 1000
    assert m.committed_rows == 600


def _pb(bronze_rows, silver_committed, datagen_rows, trailing_idle=2):
    stages = [
        StageMetrics(
            stage_name="bronze",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=1800,
            input_rows=bronze_rows,
        ),
        StageMetrics(
            stage_name="silver",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=1800,
            input_rows=bronze_rows,
            committed_rows=silver_committed,
        ),
        StageMetrics(
            stage_name="gold",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=1800,
            input_rows=bronze_rows * 4,
            freshness_seconds=655.0,
            freshness_active_seconds=55.0,
            trailing_idle_cycles=trailing_idle,
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
        config_snapshot={"datagen_output_rows": datagen_rows},
    )
    pb.compute_aggregates()
    return pb


def test_drained_corpus_scores_active_cycles_only():
    pb = _pb(1_000_000, 1_000_000, 1_000_000)
    assert pb.corpus_drained is True
    assert pb.data_freshness_seconds == pytest.approx(55.0)
    assert pb.to_dict()["scores"]["corpus_drained"] is True


def test_missing_rows_is_a_stall():
    pb = _pb(600_000, 600_000, 1_000_000)
    assert pb.corpus_drained is False
    assert pb.data_freshness_seconds == pytest.approx(655.0)


def test_bronze_short_by_under_one_percent_is_a_stall():
    pb = _pb(995_000, 995_000, 1_000_000)
    assert pb.corpus_drained is False
    assert pb.data_freshness_seconds == pytest.approx(655.0)


def test_single_trailing_idle_cycle_is_not_drained():
    pb = _pb(1_000_000, 1_000_000, 1_000_000, trailing_idle=1)
    assert pb.corpus_drained is False


def test_uncommitted_silver_is_a_stall():
    pb = _pb(1_000_000, 700_000, 1_000_000)
    assert pb.corpus_drained is False
    assert pb.data_freshness_seconds == pytest.approx(655.0)


def test_unknown_silver_commits_is_not_drained():
    pb = _pb(1_000_000, None, 1_000_000)
    assert pb.corpus_drained is False
    assert pb.data_freshness_seconds == pytest.approx(655.0)


def test_no_trailing_idle_is_not_drained():
    pb = _pb(1_000_000, 1_000_000, 1_000_000, trailing_idle=0)
    assert pb.corpus_drained is False
    assert pb.data_freshness_seconds == pytest.approx(655.0)


def test_unknown_denominator_is_unknown():
    pb = _pb(1_000_000, 1_000_000, 0)
    assert pb.corpus_drained is None
    assert pb.data_freshness_seconds == pytest.approx(655.0)


def test_drained_flag_and_stage_fields_survive_save_and_load(tmp_path):
    run = PipelineMetrics(
        run_id="lb145",
        deployment_name="t",
        start_time=_T0,
        end_time=_T0 + timedelta(seconds=1800),
        success=True,
    )
    run.pipeline_benchmark = _pb(1_000_000, 1_000_000, 1_000_000)
    storage = MetricsStorage(tmp_path)
    storage.save_run(run)
    loaded = storage.load_run("lb145").pipeline_benchmark
    assert loaded.corpus_drained is True
    gold = next(s for s in loaded.stages if s.stage_name == "gold")
    assert gold.trailing_idle_cycles == 2
    assert gold.freshness_active_seconds == pytest.approx(55.0)
    silver = next(s for s in loaded.stages if s.stage_name == "silver")
    assert silver.committed_rows == 1_000_000


def test_reproduce_leaves_out_bounded_rps_when_drained():
    run = PipelineMetrics(run_id="r", deployment_name="t", start_time=_T0, success=True)
    run.pipeline_benchmark = _pb(1_000_000, 1_000_000, 1_000_000)
    numbers = _extract_expected_numbers(run)
    assert "sustained_throughput_rps" not in numbers
    assert numbers["data_freshness_seconds"] == pytest.approx(55.0)
