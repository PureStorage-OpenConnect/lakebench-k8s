"""Time to value leaves a multi-cycle run's datagen out (CD-20, H3).

A multi-cycle run generates cycles 2+ between one cycle's gold and the next
bronze, inside the span time to value measures, so ``time_to_value_seconds``
counted datagen as pipeline time. Each cycle's datagen interval
(``cycles[].datagen_start``/``datagen_end``) overlapping the span is now
subtracted and recorded as ``time_to_value_datagen_excluded_seconds``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lakebench.metrics.collector import CycleMetrics, PipelineBenchmark, StageMetrics

T0 = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def _at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def _bench(cycles: list[CycleMetrics]) -> PipelineBenchmark:
    """Four cycles of three 100 s stages; datagen before each cycle takes 50 s
    (cycle 0's before the first stage, cycles 1-3 between gold and bronze)."""
    stages = []
    t = 0.0
    for c in range(4):
        if c:
            t += 50.0  # this cycle's datagen
        for name in ("bronze", "silver", "gold"):
            stages.append(
                StageMetrics(
                    stage_name=name,
                    stage_type="batch",
                    engine="spark",
                    start_time=_at(t),
                    end_time=_at(t + 100),
                    elapsed_seconds=100.0,
                    success=True,
                )
            )
            t += 100.0
    pb = PipelineBenchmark(
        run_id="r",
        deployment_name="d",
        pipeline_mode="batch",
        start_time=T0,
        stages=stages,
        cycles=cycles,
    )
    pb.compute_aggregates()
    return pb


def _cycle(i: int, skipped: bool = False) -> CycleMetrics:
    if skipped:
        return CycleMetrics(cycle_index=i, datagen_skipped=True)
    start = -50.0 + i * 350.0  # 50 s before the cycle's bronze
    return CycleMetrics(
        cycle_index=i,
        datagen_start=_at(start).isoformat(),
        datagen_end=_at(start + 50).isoformat(),
    )


def test_ttv_subtracts_cycle_datagen():
    pb = _bench([_cycle(i) for i in range(4)])
    # Span 1350 s (12 stages and 3 gaps); cycle 0's datagen is before it.
    assert pb.time_to_value_datagen_excluded_seconds == 150.0
    assert pb.time_to_value_seconds == 1200.0
    scores = pb.to_dict()["scores"]
    assert scores["time_to_value_seconds"] == 1200.0
    assert scores["time_to_value_datagen_excluded_seconds"] == 150.0


def test_reused_corpus_excludes_nothing():
    pb = _bench([_cycle(i, skipped=True) for i in range(4)])
    assert pb.time_to_value_datagen_excluded_seconds == 0.0
    assert pb.time_to_value_seconds == 1350.0


def test_single_cycle_and_unreadable_times_keep_the_span():
    assert _bench([]).time_to_value_datagen_excluded_seconds is None
    assert _bench([]).time_to_value_seconds == 1350.0
    legacy = [CycleMetrics(cycle_index=i) for i in range(4)]  # a record before the fields
    pb = _bench(legacy)
    assert pb.time_to_value_datagen_excluded_seconds is None
    assert pb.time_to_value_seconds == 1350.0
    assert "time_to_value_datagen_excluded_seconds" not in pb.to_dict()["scores"]


def test_cycle_datagen_times_round_trip_through_storage():
    from lakebench.metrics.storage import _deserialize_cycles

    c = _cycle(2)
    (back,) = _deserialize_cycles([c.to_dict()])
    assert (back.datagen_start, back.datagen_end, back.datagen_skipped) == (
        c.datagen_start,
        c.datagen_end,
        False,
    )


def test_a_generating_multicycle_run_records_each_cycles_datagen(tmp_path, monkeypatch):
    """Through the real run (QA-9 harness): each cycle's datagen interval is
    recorded and the score says how much of the span it was."""
    from tests.harness.run_harness import saved_record
    from tests.test_multicycle_generate_bronze import CYCLES, _run

    result, _keys, _calls = _run(tmp_path, monkeypatch, owned=True, argv=["--yes"])
    assert result.exit_code == 0, result.output
    record = saved_record(tmp_path)
    cycles = record["cycles"]
    assert len(cycles) == CYCLES
    for c in cycles:
        lo, hi = (
            datetime.fromisoformat(c["datagen_start"]),
            datetime.fromisoformat(c["datagen_end"]),
        )
        assert lo <= hi and lo.tzinfo is not None and not c["datagen_skipped"]
    scores = record["pipeline_benchmark"]["scores"]
    assert scores["time_to_value_datagen_excluded_seconds"] >= 0.0
