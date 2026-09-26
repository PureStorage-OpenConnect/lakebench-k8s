"""Continuous intake held to the trickle rate is not saturation (LB-156).

Continuous mode trickles a finite corpus into bronze at a configured rate:
max_files_per_trigger files per bronze trigger (50 per 30 s by default), the
same for every scale and both workloads. The corpus grows with scale, the
rate does not. run-20260925-191402-bc5b57 (c360, scale 100, 1800 s) took
50 files on each of 60 triggers, 20.5 s per batch, and ingested 46,473,000 of
the corpus's ~247.9M rows: ingest_ratio 0.19 with bronze idle 32% of the
window and silver 1.55M rows behind. It read pipeline_saturated=True and the
report marked the run failed, although every stage kept pace with what the
trickle offered.

The fixtures use that run's numbers at scale 100 and scale-shaped corpora at
scale 1 and 10 (both drain at the same trickle rate).
"""

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
# c360 corpus rows: scale 100 is the live run (46,473,000 rows at 0.1875);
# scale 10 is run-20260924-131446-fc32ee; scale 1 is a tenth of that.
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
):
    stages = [
        StageMetrics(
            stage_name="bronze",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=_WINDOW,
            input_rows=bronze_rows,
            latency_ms=bronze_ms,
            total_batches=bronze_batches,
        ),
        StageMetrics(
            stage_name="silver",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=_WINDOW,
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
            elapsed_seconds=_WINDOW,
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


def test_trickle_note_names_the_rate_and_the_drain_time():
    note = _live_s100().trickle_note()
    assert note is not None
    assert "50 files per 30 seconds bronze trigger" in note
    assert "19% of the corpus" in note
    assert "not saturated" in note
    assert "9,600 s" in note
    assert chr(0x2014) not in note


def test_aml_uses_the_same_trickle_rule():
    """AML at scale 100 with bronze sized to keep the trigger: 50 files of
    ~194K rows each per 30 s, 25 s per batch (busy 83%, above the busy
    bound), the corpus ~2.67B rows. Finishing inside the trigger is what
    counts, so it is the trickle, not bronze capacity."""
    per_batch = 9_725_000
    pb = _pb(
        corpus_rows=2_666_664_000,
        bronze_rows=60 * per_batch,
        bronze_batches=60,
        bronze_ms=25_000.0,
        silver_committed=60 * per_batch - 2 * per_batch,
        silver_ms=40_000.0,
        schema="financial",
    )
    assert pb.bronze_busy_fraction == pytest.approx(0.833, abs=1e-3)
    assert pb.intake_limit == "trickle_rate"
    assert pb.pipeline_saturated is False


# -- scale 100: runs that did not keep pace stay saturated ---------------------


def test_late_start_is_not_the_trickle():
    """Bronze reached its first batch 10 minutes in: 40 of 60 triggers."""
    pb = _live_s100(bronze_rows=40 * 774_550, bronze_batches=40, silver_committed=40 * 774_550)
    assert pb.intake_limit == "below_bronze_capacity"
    assert pb.pipeline_saturated is True
    assert pb.corpus_drain_seconds is None
    assert pb.trickle_note() is None


def test_bronze_overrunning_its_trigger_is_bronze_capacity():
    """50 files take 45 s on a 30 s trigger: 40 back-to-back batches."""
    pb = _live_s100(
        bronze_rows=40 * 774_550,
        bronze_batches=40,
        bronze_ms=45_000.0,
        silver_committed=40 * 774_550,
    )
    assert pb.intake_limit == "bronze_capacity"
    assert pb.pipeline_saturated is True


def test_batches_at_the_trigger_count_but_each_over_it_is_not_the_trickle():
    """Every trigger ran a batch, but the mean batch overran the trigger."""
    pb = _live_s100(bronze_ms=31_000.0)
    assert pb.intake_limit == "bronze_capacity"
    assert pb.pipeline_saturated is True


def test_silver_behind_the_trickle_is_saturated():
    """Bronze held the trickle, but silver committed 5M fewer rows than
    bronze took: more than one silver trigger, batch and bronze trigger of
    lag (25.8K rows/s x 114.6 s = 2.96M)."""
    pb = _live_s100(silver_committed=46_473_000 - 5_000_000)
    assert pb.intake_limit == "trickle_rate"
    assert pb.pipeline_saturated is True
    assert pb.trickle_note() is None


def test_silver_commits_unlogged_cannot_prove_pace():
    pb = _live_s100(silver_committed=None)
    assert pb.intake_limit == "trickle_rate"
    assert pb.pipeline_saturated is True


def test_unparseable_trigger_config_proves_nothing():
    pb = _live_s100(sustained={**_SUSTAINED, "bronze_trigger_interval": "every so often"})
    assert pb.intake_limit == "below_bronze_capacity"
    assert pb.pipeline_saturated is True


def test_unknown_trigger_config_keeps_lane_r_semantics():
    pb = _live_s100(sustained={})
    assert pb.intake_limit == "below_bronze_capacity"
    assert pb.pipeline_saturated is True


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


def test_the_offered_rate_is_the_same_at_every_scale():
    """The trickle is config, not scale: the per-trigger rows at scale 10
    and 100 are the same 50 files, so the rate a scale-100 run holds is the
    rate a scale-10 run drained at."""
    s100 = _live_s100()
    per_trigger = s100.sustained_throughput_rps * 30
    assert per_trigger == pytest.approx(774_550, rel=1e-4)
    # Scale 10 drains its corpus in about 32 triggers (960 s) at that rate.
    assert _CORPUS[10] / per_trigger * 30 == pytest.approx(959, abs=2)


# -- the report and the stored scorecard ---------------------------------------


def _metrics(pb):
    pm = PipelineMetrics(
        run_id="20260925-191402-bc5b57",
        deployment_name="t",
        start_time=_T0,
        end_time=_T0 + timedelta(seconds=_WINDOW),
        success=True,
    )
    pm.pipeline_benchmark = pb
    return pm


def test_report_warns_instead_of_failing_a_trickle_bound_run():
    from lakebench.reports.generator import ReportGenerator

    gen = ReportGenerator(metrics_dir="/tmp/unused-rg")
    passed, reasons, warnings = gen._compute_overall_status(_metrics(_live_s100()))
    assert not any("Ingest ratio" in r for r in reasons), reasons
    assert any("trickle rate" in w for w in warnings)
    cards = gen._generate_sustained_detail_cards(_live_s100())
    assert "SATURATED" not in cards
    assert "Held to trickle rate" in cards


def test_report_still_fails_a_stalled_run():
    from lakebench.reports.generator import ReportGenerator

    gen = ReportGenerator(metrics_dir="/tmp/unused-rg")
    stalled = _live_s100(bronze_rows=40 * 774_550, bronze_batches=40)
    _, reasons, _ = gen._compute_overall_status(_metrics(stalled))
    assert any("pipeline saturated" in r for r in reasons)
    silver_behind = _live_s100(silver_committed=40_000_000)
    _, reasons, _ = gen._compute_overall_status(_metrics(silver_behind))
    assert any("silver did not keep pace" in r for r in reasons)


def test_corpus_drain_seconds_survives_save_and_load(tmp_path):
    from lakebench.metrics.storage import MetricsStorage

    pm = _metrics(_live_s100())
    MetricsStorage(tmp_path).save_run(pm)
    loaded = MetricsStorage(tmp_path).load_run(pm.run_id).pipeline_benchmark
    assert loaded.intake_limit == "trickle_rate"
    assert loaded.pipeline_saturated is False
    assert loaded.corpus_drain_seconds == 9_600
