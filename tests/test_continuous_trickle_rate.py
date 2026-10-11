"""Continuous intake held to the trickle rate is labelled trickle_rate, not saturation."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from lakebench.metrics.collector import PipelineBenchmark, PipelineMetrics, StageMetrics

_T0 = datetime(2026, 9, 25, 19, 14)
_WINDOW = 1800
_SUSTAINED = {
    "bronze_trigger_interval": "30 seconds",
    "silver_trigger_interval": "60 seconds",
    "gold_refresh_interval": "5 minutes",
    "run_duration": _WINDOW,
    "max_files_per_trigger": 50,
}
# c360 corpus rows per scale.
_CORPUS = {1: 2_477_011, 10: 24_770_109, 100: 247_856_000}


def _pb(
    *,
    corpus_rows,
    bronze_rows,
    bronze_batches,
    bronze_ms,
    silver_committed,
    silver_ms=24_563.3,
    schema="customer360",
    sustained=None,
    trailing_idle=0,
    bronze_span=None,
    window=_WINDOW,
):
    # A trickle-held bronze logs one batch per trigger: first to last is
    # (batches - 1) triggers apart.
    if bronze_span is None and bronze_batches > 1:
        bronze_span = (bronze_batches - 1) * 30.0
    stages = [
        StageMetrics(
            stage_name="bronze",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=window,
            input_rows=bronze_rows,
            latency_ms=bronze_ms,
            total_batches=bronze_batches,
            batch_span_seconds=bronze_span,
        ),
        StageMetrics(
            stage_name="silver",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=window,
            input_rows=silver_committed or 0,
            latency_ms=silver_ms,
            total_batches=30,
            committed_rows=silver_committed,
            trailing_idle_cycles=trailing_idle,
        ),
        StageMetrics(
            stage_name="gold",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=window,
            freshness_seconds=136.0,
            freshness_active_seconds=136.0,
            trailing_idle_cycles=trailing_idle,
        ),
    ]
    pb = PipelineBenchmark(
        run_id="t",
        deployment_name="t",
        pipeline_mode="sustained",
        start_time=_T0,
        end_time=_T0 + timedelta(seconds=_WINDOW),
        success=True,
        stages=stages,
        config_snapshot={
            "datagen_output_rows": corpus_rows,
            "workload_schema": schema,
            "sustained": _SUSTAINED if sustained is None else sustained,
        },
    )
    pb.compute_aggregates()
    return pb


def _live_s100(**overrides):
    kwargs = {
        "corpus_rows": _CORPUS[100],
        "bronze_rows": 46_473_000,
        "bronze_batches": 60,
        "bronze_ms": 20_463.3,
        "silver_committed": 44_923_900,
    }
    kwargs.update(overrides)
    return _pb(**kwargs)


# -- scale 100: the live run ---------------------------------------------------


def test_scale_100_is_held_to_the_trickle_rate_not_saturated():
    pb = _live_s100()
    assert pb.ingest_ratio == pytest.approx(0.1875, abs=1e-4)
    assert pb.bronze_busy_fraction == pytest.approx(0.682, abs=1e-3)
    assert pb.intake_limit == "trickle_rate"
    assert pb.pipeline_saturated is False
    assert pb.corpus_drained is False
    # 50 files x 15,491 rows per 30 s trigger.
    assert pb.sustained_throughput_rps == pytest.approx(25_818.3, abs=0.1)
    # The corpus needs 9,600 s at that rate.
    assert pb.corpus_drain_seconds == pytest.approx(9_600, abs=1)
    scores = pb.to_dict()["scores"]
    assert scores["intake_limit"] == "trickle_rate"
    assert scores["pipeline_saturated"] is False
    assert scores["corpus_drain_seconds"] == 9_600
    # ingest_ratio keeps its meaning: the share of the corpus consumed.
    assert scores["ingest_ratio"] == pytest.approx(0.1875, abs=1e-4)


# -- scale 100: runs that did not keep pace stay saturated ---------------------


_UNSET = object()


@pytest.mark.parametrize(
    ("over", "limit", "saturated", "no_note"),
    [
        # silver at 600 s a batch on a 60 s trigger, 15.5M rows behind: its batch
        # time must not widen the lag it may have
        (
            {"silver_ms": 600_000.0, "silver_committed": 46_473_000 - 15_500_000},
            "trickle_rate",
            True,
            True,
        ),
        ({"silver_ms": 65_000.0}, _UNSET, True, False),
        # a 4-minute bronze stall mid-window, hidden by a tail of batches
        ({"bronze_span": 59 * 30.0 + 240.0}, "below_bronze_capacity", True, False),
        ({"bronze_span": 60 * 30.0}, "trickle_rate", _UNSET, False),  # one missed trigger
        # a late start: 40 of 60 triggers
        (
            {"bronze_rows": 40 * 774_550, "bronze_batches": 40, "silver_committed": 40 * 774_550},
            "below_bronze_capacity",
            True,
            True,
        ),
        # 50 files take 45 s on a 30 s trigger
        (
            {
                "bronze_rows": 40 * 774_550,
                "bronze_batches": 40,
                "bronze_ms": 45_000.0,
                "silver_committed": 40 * 774_550,
            },
            "bronze_capacity",
            True,
            False,
        ),
        ({"bronze_ms": 31_000.0}, "bronze_capacity", True, False),  # each batch over the trigger
        # silver committed 5M fewer rows than bronze took: past one trigger of lag
        ({"silver_committed": 46_473_000 - 5_000_000}, "trickle_rate", True, True),
        # silver stuck in its first batch is the worst silver, not unmeasured
        ({"silver_committed": None, "silver_ms": None}, "trickle_rate", True, False),
        (
            {"sustained": {**_SUSTAINED, "bronze_trigger_interval": "every so often"}},
            "below_bronze_capacity",
            True,
            False,
        ),
        ({"sustained": {}}, "below_bronze_capacity", True, False),
        # AML at scale 100, bronze sized to keep the trigger (25 s of a 30 s
        # trigger, busy above the busy bound): finishing inside the trigger is
        # the trickle, not bronze capacity
        (
            {
                "corpus_rows": 2_666_664_000,
                "bronze_rows": 60 * 9_725_000,
                "bronze_ms": 25_000.0,
                "silver_committed": 58 * 9_725_000,
                "silver_ms": 40_000.0,
                "schema": "financial",
                "busy": 0.833,
            },
            "trickle_rate",
            False,
            False,
        ),
        # one missed trigger in a 330 s window: 10 batches over 11 triggers
        (
            {
                "bronze_rows": 10 * 774_550,
                "bronze_batches": 10,
                "bronze_span": 300.0,
                "silver_committed": 10 * 774_550,
                "window": 330,
            },
            "trickle_rate",
            False,
            False,
        ),
        # a single batch has no span, which proves nothing about the trigger
        ({"bronze_span": None, "bronze_batches": 1}, "below_bronze_capacity", True, False),
        # financial, no sustained block: 16 batches x 109.7 s in an 1800 s window
        (
            {
                "corpus_rows": 266_666_400,
                "schema": "financial",
                "sustained": {},
                "bronze_rows": 155_610_788,
                "bronze_batches": 16,
                "bronze_ms": 109_737.5,
                "silver_committed": 155_610_788,
                "busy": 0.975,
                "ratio": 0.5835,
            },
            "bronze_capacity",
            True,
            False,
        ),
        # bronze in a batch 27% of the window: idle time does not prove the
        # pipeline kept pace
        (
            {
                "corpus_rows": 266_666_400,
                "schema": "financial",
                "sustained": {},
                "bronze_rows": 155_610_788,
                "bronze_batches": 60,
                "bronze_ms": 8_000.0,
                "silver_committed": 155_610_788,
            },
            "below_bronze_capacity",
            True,
            False,
        ),
        # corpus fully ingested
        (
            {
                "corpus_rows": 266_666_400,
                "schema": "financial",
                "sustained": {},
                "bronze_rows": 266_666_400,
                "bronze_batches": 16,
                "bronze_ms": 5_000.0,
                "silver_committed": 266_666_400,
            },
            "none",
            False,
            False,
        ),
        # no batch logged
        (
            {
                "corpus_rows": 266_666_400,
                "schema": "financial",
                "sustained": {},
                "bronze_rows": 155_610_788,
                "bronze_batches": 0,
                "bronze_ms": None,
                "silver_committed": 155_610_788,
                "busy": None,
            },
            None,
            True,
            False,
        ),
    ],
)
def test_intake_verdict(over, limit, saturated, no_note):
    over = dict(over)
    busy = over.pop("busy", _UNSET)
    ratio = over.pop("ratio", _UNSET)
    pb = _live_s100(**over)
    if limit is not _UNSET:
        assert pb.intake_limit == limit
        assert pb.to_dict()["scores"]["intake_limit"] == limit
    if busy is None:
        assert pb.bronze_busy_fraction is None
    elif busy is not _UNSET:
        assert pb.bronze_busy_fraction == pytest.approx(busy, abs=1e-3)
    if ratio is not _UNSET:
        assert pb.ingest_ratio == pytest.approx(ratio, abs=1e-4)
    if saturated is not _UNSET:
        assert pb.pipeline_saturated is saturated
    if no_note:
        assert pb.trickle_note() is None


def _silver_unmeasured():
    """Silver logged no batch: its pace is unknown, not lagging."""
    pb = _live_s100(silver_committed=None)
    for st in pb.stages:
        if st.stage_name == "silver":
            st.total_batches = 0
            st.input_rows = 0
    pb.compute_aggregates()
    return pb


def test_silver_with_no_log_lines_leaves_saturation_unknown():
    pb = _silver_unmeasured()
    assert pb.intake_limit == "trickle_rate"
    assert pb.pipeline_saturated is None
    assert pb.trickle_note() is None


# -- scale 1 and 10: the same trickle drains the corpus -------------------------


@pytest.mark.parametrize("scale", [1, 10])
def test_small_scales_drain_and_have_no_intake_limit(scale):
    rows = _CORPUS[scale]
    # 774,550 rows per trigger: scale 1 drains in 4 triggers, scale 10 in 32.
    batches = -(-rows // 774_550)
    pb = _pb(
        corpus_rows=rows,
        bronze_rows=rows,
        bronze_batches=batches,
        bronze_ms=20_463.3,
        silver_committed=rows,
        trailing_idle=3,
    )
    assert pb.ingest_ratio == pytest.approx(1.0)
    assert pb.intake_limit == "none"
    assert pb.pipeline_saturated is False
    assert pb.corpus_drained is True
    assert pb.corpus_drain_seconds is None
    assert pb.trickle_note() is None


# -- the report and the stored scorecard ---------------------------------------


def _metrics(pb):
    pm = PipelineMetrics(
        run_id="t",
        deployment_name="t",
        start_time=_T0,
        end_time=_T0 + timedelta(seconds=_WINDOW),
        success=True,
    )
    pm.pipeline_benchmark = pb
    return pm


def test_report_does_not_fail_a_trickle_bound_run(tmp_path):
    from lakebench.reports.generator import ReportGenerator

    gen = ReportGenerator(metrics_dir=tmp_path)
    _, reasons, _ = gen._compute_overall_status(_metrics(_live_s100()))
    assert not any("Ingest ratio" in r for r in reasons), reasons


def test_report_still_fails_a_stalled_run(tmp_path):
    from lakebench.reports.generator import ReportGenerator

    gen = ReportGenerator(metrics_dir=tmp_path)
    stalled = _live_s100(bronze_rows=40 * 774_550, bronze_batches=40)
    _, reasons, _ = gen._compute_overall_status(_metrics(stalled))
    # A stall leaves bronze idle: named as such, not as saturation.
    assert any("Ingest ratio" in r and "without being at capacity" in r for r in reasons), reasons
    silver_behind = _live_s100(silver_committed=40_000_000)
    _, reasons, _ = gen._compute_overall_status(_metrics(silver_behind))
    assert any("silver did not keep pace" in r for r in reasons)


def test_report_does_not_fail_a_run_whose_silver_is_unmeasured(tmp_path):
    from lakebench.reports.generator import ReportGenerator

    gen = ReportGenerator(metrics_dir=tmp_path)
    _, reasons, _ = gen._compute_overall_status(_metrics(_silver_unmeasured()))
    assert not any("Ingest ratio" in r for r in reasons), reasons


# -- the parser path: driver log text to the verdict -----------------------------


def _log(t, msg):
    return f"[lb] {t.isoformat()} - {msg}"


def _bronze_log(batches, rows_per_batch, seconds, start=_T0 + timedelta(seconds=40), first=0):
    lines = []
    for b in range(first, first + batches):
        t = start + timedelta(seconds=30 * (b - first))
        lines.append(_log(t, f"Batch {b}: writing {rows_per_batch:,} rows to lakehouse.bronze"))
        lines.append(
            _log(t + timedelta(seconds=seconds), f"Batch {b}: committed in {seconds:.1f}s")
        )
    return "\n".join(lines)


def _silver_log(batches, rows_per_batch, seconds):
    lines = []
    for b in range(batches):
        t = _T0 + timedelta(seconds=90 + 60 * b)
        lines.append(_log(t, f"Batch {b}: transforming {rows_per_batch:,} rows"))
        lines.append(
            _log(
                t,
                f"Batch {b}: committed to lakehouse.silver.customer_interactions_enriched in {seconds:.1f}s",
            )
        )
    return "\n".join(lines)


def _run_from_logs(bronze_log):
    from lakebench.metrics.collector import MetricsCollector, build_pipeline_benchmark

    collector = MetricsCollector()
    run = collector.start_run(
        "r",
        "d",
        {
            "workload_schema": "customer360",
            "scale": 100,
            "datagen_output_rows": _CORPUS[100],
            "sustained": _SUSTAINED,
        },
    )
    for log, job in (
        (bronze_log, "bronze-ingest"),
        (_silver_log(29, 1_549_100, 24.6), "silver-stream"),
    ):
        m = collector.parse_streaming_logs(log, job)
        m.elapsed_seconds = _WINDOW
        m.success = True
        run.streaming.append(m)
    run.start_time = _T0
    run.end_time = _T0 + timedelta(seconds=_WINDOW)
    return build_pipeline_benchmark(run)


def test_live_shaped_logs_parse_to_the_trickle_verdict():
    pb = _run_from_logs(_bronze_log(60, 774_550, 20.5))
    bronze = next(s for s in pb.stages if s.stage_name == "bronze")
    assert bronze.total_batches == 60
    assert bronze.batch_span_seconds == pytest.approx(59 * 30)
    assert pb.intake_limit == "trickle_rate"
    assert pb.pipeline_saturated is False


def test_logs_with_a_stall_parse_to_saturated():
    """30 batches, a 5-minute stall, 30 more: 60 of the window's 60
    triggers, so only the span shows the stall."""
    stalled = (
        _bronze_log(30, 774_550, 20.5)
        + "\n"
        + _bronze_log(
            30, 774_550, 20.5, start=_T0 + timedelta(seconds=40 + 30 * 30 + 300), first=30
        )
    )
    pb = _run_from_logs(stalled)
    bronze = next(s for s in pb.stages if s.stage_name == "bronze")
    assert bronze.total_batches == 60
    assert pb.intake_limit == "below_bronze_capacity"
    assert pb.pipeline_saturated is True
