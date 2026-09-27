"""Measurement precision fixes from the lb16-checks2 live run (v1.6).

1. The continuous headline printed event-date age (~54.9M s) as freshness.
2. Batch stage times were quantized to the 15 s job-monitor poll.
3. The default continuous retention_threshold warned on every run.
4. Metrics timestamps were naive local time beside UTC window times.
5. ``lakebench info`` labelled a continuous config by its datagen mode and
   showed the scale default date range instead of the configured one.
6. Destroy did not name the tables whose files stay in a refused bucket.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from lakebench.metrics.collector import (
    BenchmarkMetrics,
    BenchmarkRoundMeta,
    PipelineBenchmark,
)

# ---------------------------------------------------------------------------
# 1. Continuous freshness headline
# ---------------------------------------------------------------------------

LIVE_EVENT_AGE = 54_890_938.0  # lb16-checks2 run 20260927-011043-e338c5


def _continuous_pb(**kw) -> PipelineBenchmark:
    rounds = [
        BenchmarkMetrics(
            mode="power",
            cache="hot",
            scale=1,
            qph=1243.4,
            total_seconds=20.0,
            round_meta=BenchmarkRoundMeta(round_index=i, gold_event_age_seconds=LIVE_EVENT_AGE + i),
        )
        for i in range(1, 4)
    ]
    pb = PipelineBenchmark(
        run_id="r",
        deployment_name="d",
        pipeline_mode="sustained",
        start_time=datetime(2026, 9, 27, 7, 10, 43),
        benchmark_rounds=rounds,
        **kw,
    )
    pb.compute_aggregates()
    return pb


class TestContinuousFreshnessHeadline:
    def test_pipeline_score_prints_the_scored_freshness(self):
        from lakebench.cli._sustained import pipeline_score_freshness

        pb = _continuous_pb()
        pb.data_freshness_seconds = 71.0
        pb.corpus_drained = False
        assert pb.query_time_event_age_seconds > 5e7
        assert pipeline_score_freshness(pb) == "71.0s"

    def test_pipeline_score_without_a_scored_freshness_is_na(self):
        from lakebench.cli._sustained import pipeline_score_freshness

        pb = _continuous_pb()
        pb.data_freshness_seconds = None
        assert pipeline_score_freshness(pb) == "n/a"

    def test_event_age_is_labelled_in_days(self):
        from lakebench.cli._sustained import event_age_label

        assert event_age_label(LIVE_EVENT_AGE) == "635.3 d"
        assert event_age_label(None) == "n/a"

    def test_scores_carry_event_age_under_its_own_name(self):
        pb = _continuous_pb()
        scores = pb.to_dict()["scores"]
        assert "query_time_freshness_seconds" not in scores
        assert scores["query_time_event_age_seconds"] == pytest.approx(LIVE_EVENT_AGE + 2)

    def test_description_says_it_is_not_freshness(self):
        desc = PipelineBenchmark._SCORE_DESCRIPTIONS["query_time_event_age_seconds"]
        assert "not freshness" in desc

    def test_legacy_round_meta_loads_as_event_age(self, tmp_path):
        from lakebench.metrics.storage import MetricsStorage

        raw = {
            "run_id": "20260927-011043-e338c5",
            "deployment_name": "d",
            "start_time": "2026-09-27T01:10:43",
            "benchmark_rounds": [
                {
                    "mode": "power",
                    "qph": 1235.9,
                    "round_meta": {"round_index": 5, "gold_freshness_seconds": 54891607.0},
                }
            ],
        }
        m = MetricsStorage.__new__(MetricsStorage)._dict_to_metrics(raw)
        assert m.benchmark_rounds[0].round_meta.gold_event_age_seconds == 54891607.0
        d = m.benchmark_rounds[0].round_meta.to_dict()
        assert "gold_freshness_seconds" not in d
        json.dumps(d)

    def test_report_does_not_present_event_age_as_freshness(self):
        from lakebench.metrics.collector import PipelineMetrics
        from lakebench.reports.generator import ReportGenerator

        pb = _continuous_pb()
        pb.data_freshness_seconds = 71.0
        metrics = PipelineMetrics(
            run_id="r",
            deployment_name="d",
            start_time=datetime(2026, 9, 27, 7, 10, 43),
            success=True,
            pipeline_benchmark=pb,
            benchmark_rounds=list(pb.benchmark_rounds),
            config_snapshot={"pipeline_mode": "continuous"},
        )
        html = ReportGenerator(metrics_dir="/tmp/unused-lb16")._generate_html(metrics)
        assert "Query-Time Freshness" not in html
        assert "54890" not in html
        assert "Median freshness" not in html


# ---------------------------------------------------------------------------
# 4. Timestamps are UTC with a zone
# ---------------------------------------------------------------------------


class TestUtcTimestamps:
    def test_run_start_and_end_are_aware_utc(self):
        from lakebench.metrics.collector import MetricsCollector

        c = MetricsCollector()
        run = c.start_run("20260927-011043-e338c5", "d", {})
        assert run.start_time.utcoffset() == timedelta(0)
        done = c.end_run(success=True)
        assert done is not None and done.end_time is not None
        assert done.end_time.utcoffset() == timedelta(0)
        d = done.to_dict()
        assert d["start_time"].endswith("+00:00")
        assert d["end_time"].endswith("+00:00")

    def test_old_naive_metrics_still_load_and_score(self):
        from lakebench.metrics.storage import MetricsStorage

        raw = {
            "run_id": "20260927-011123-497f02",
            "deployment_name": "d",
            "start_time": "2026-09-27T01:11:23",
            "end_time": "2026-09-27T01:40:23",
            "jobs": [
                {
                    "job_name": "lakebench-bronze-verify",
                    "job_type": "bronze-verify",
                    "start_time": "2026-09-27T01:11:25",
                    "end_time": "2026-09-27T01:12:55",
                    "elapsed_seconds": 90.1,
                    "success": True,
                }
            ],
        }
        m = MetricsStorage.__new__(MetricsStorage)._dict_to_metrics(raw)
        assert m.jobs[0].start_time.tzinfo is None
        from lakebench.metrics import build_pipeline_benchmark

        pb = build_pipeline_benchmark(m)
        assert pb.time_to_value_seconds == pytest.approx(90.0)

    def test_perf_gate_ttv_reads_aware_stage_times(self, tmp_path):
        from lakebench.metrics import perf_gate

        run_dir = tmp_path / "run-x"
        run_dir.mkdir()
        stages = [
            {
                "stage_name": "bronze",
                "stage_type": "batch",
                "start_time": "2026-09-27T07:11:25.500000+00:00",
                "end_time": "2026-09-27T07:12:56.250000+00:00",
                "elapsed_seconds": 90.75,
                "input_size_gb": 9.0,
            }
        ]
        (run_dir / "metrics.json").write_text(
            json.dumps(
                {
                    "run_id": "x",
                    "deployment_name": "d",
                    "start_time": "2026-09-27T07:11:20+00:00",
                    "pipeline_benchmark": {"stages": stages, "scores": {}},
                }
            )
        )
        rec = perf_gate.load_run(run_dir)
        ttv = perf_gate._pipeline_ttv(rec)
        assert ttv is not None and ttv[0] == pytest.approx(90.75)
