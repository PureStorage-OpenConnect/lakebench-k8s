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


@pytest.mark.parametrize(
    ("cycles", "freshness", "active", "idle"),
    [
        ([(40, False), (55, False), (355, True), (655, True)], 655.0, 55.0, 2),
        # a mid-run stall that recovers is real and stays in active freshness
        (
            [(40, False), (350, True), (650, True), (950, True), (50, False), (60, True)],
            None,
            950.0,
            1,
        ),
        ([(40, False), (655, False)], None, 655.0, 0),  # untagged log unchanged
        ([(40, False), (400, True)], None, 40.0, None),  # all idle after the first
    ],
)
def test_trailing_idle_cycles_are_split_off(cycles, freshness, active, idle):
    m = _parse(_gold(cycles))
    if freshness is not None:
        assert m.freshness_seconds == pytest.approx(freshness)
    assert m.freshness_active_seconds == pytest.approx(active)
    if idle is not None:
        assert m.trailing_idle_cycles == idle


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


@pytest.mark.parametrize(
    ("bronze", "silver", "datagen", "idle", "drained", "freshness"),
    [
        # drained: every datagen row in bronze and committed by silver
        (1_000_000, 1_000_000, 1_000_000, None, True, 55.0),
        (1_000_000, 1_000_000, 1_000_000, 1, True, None),  # does not wait on idle gold
        (1_000_000, 1_000_000, 1_000_000, 0, True, 55.0),  # no idle cycle: every cycle
        (600_000, 600_000, 1_000_000, None, False, 655.0),  # rows missing
        (995_000, 995_000, 1_000_000, None, False, 655.0),  # short by under 1%
        (1_000_000, 700_000, 1_000_000, None, False, 655.0),  # silver uncommitted
        (1_000_000, None, 1_000_000, None, False, 655.0),  # silver commits unknown
        (1_000_000, 1_000_000, 0, None, None, 655.0),  # denominator unknown
    ],
)
def test_corpus_drained_and_freshness(bronze, silver, datagen, idle, drained, freshness):
    """A drained corpus scores freshness over its active cycles only; a stall
    or an unknown is never called drained."""
    pb = _pb(bronze, silver, datagen, **({} if idle is None else {"trailing_idle": idle}))
    assert pb.corpus_drained is drained
    if freshness is not None:
        assert pb.data_freshness_seconds == pytest.approx(freshness)
    if drained:
        assert pb.to_dict()["scores"]["corpus_drained"] is True


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
