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
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

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


# ---------------------------------------------------------------------------
# 2. Batch stage times from the Spark application, not the poll
# ---------------------------------------------------------------------------

T0 = datetime(2026, 9, 27, 7, 11, 25, 250000, tzinfo=timezone.utc)


def _timing(**kw):
    from lakebench.modules.pipeline_engines.spark.monitor import stage_timing

    args = {
        "submitted_at": T0,
        # lb16: every stage ended on a 15 s poll tick
        "observed_end": T0 + timedelta(seconds=90.09),
        "cluster_end": None,
        "cluster_source": "driver_container",
        "clock_offset_seconds": 4.5,
        "poll_interval": 15,
    }
    args.update(kw)
    return stage_timing(**args)


class TestStageTiming:
    def test_cluster_end_replaces_the_poll_tick(self):
        # True end 83.7 s after submission on this host; the cluster clock is
        # 4.5 s ahead and the kubelet truncates to the second.
        true_end = T0 + timedelta(seconds=83.7)
        recorded = (true_end + timedelta(seconds=4.5)).replace(microsecond=0)
        t = _timing(cluster_end=recorded)
        assert t.source == "driver_container"
        assert t.resolution_seconds == 2.0
        assert abs(t.elapsed_seconds - 83.7) <= 1.0
        assert t.start == T0

    def test_no_cluster_end_uses_the_poll_and_says_so(self):
        t = _timing()
        assert t.source == "poll"
        assert t.resolution_seconds == 15.0
        assert t.elapsed_seconds == pytest.approx(90.09)

    def test_unknown_clock_offset_uses_the_poll(self):
        t = _timing(cluster_end=T0 + timedelta(seconds=80), clock_offset_seconds=None)
        assert t.source == "poll" and "offset unknown" in t.note

    @pytest.mark.parametrize("secs", [-30.0, 200.0])
    def test_end_outside_the_observed_run_is_skew_not_a_time(self, secs):
        t = _timing(cluster_end=T0 + timedelta(seconds=secs + 4.5))
        assert t.source == "poll"
        assert "outside the observed run" in t.note

    def test_end_before_the_last_submission_is_a_stale_status(self):
        """A retried application: terminationTime left from the first attempt."""
        t = _timing(
            cluster_end=T0 + timedelta(seconds=20),
            last_submission=T0 + timedelta(seconds=40),
        )
        assert t.source == "poll" and "stale" in t.note

    def test_rounding_past_the_observed_end_is_clamped(self):
        t = _timing(cluster_end=T0 + timedelta(seconds=90.09 + 4.5 + 1.0))
        assert t.source == "driver_container"
        assert t.end == T0 + timedelta(seconds=90.09)

    def test_parse_k8s_time(self):
        from lakebench.modules.pipeline_engines.spark.monitor import parse_k8s_time

        assert parse_k8s_time("2026-09-27T07:12:55Z") == datetime(
            2026, 9, 27, 7, 12, 55, tzinfo=timezone.utc
        )
        naive = datetime(2026, 9, 27, 7, 12, 55)
        assert parse_k8s_time(naive).tzinfo is timezone.utc
        assert parse_k8s_time("") is None
        assert parse_k8s_time("not a time") is None


def _driver_pod(name, finished):
    term = SimpleNamespace(finished_at=finished)
    cs = SimpleNamespace(name="spark-kubernetes-driver", state=SimpleNamespace(terminated=term))
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name), status=SimpleNamespace(container_statuses=[cs])
    )


class TestApplicationEnd:
    def _monitor(self):
        from lakebench.modules.pipeline_engines.spark.monitor import SparkJobMonitor

        m = SparkJobMonitor.__new__(SparkJobMonitor)
        m.namespace = "ns"
        return m

    def _status(self, **kw):
        from lakebench.modules.pipeline_engines.spark.job import JobState, JobStatus

        return JobStatus(name="lakebench-silver-build", state=JobState.COMPLETED, message="", **kw)

    def test_prefers_the_current_driver_container(self):
        finished = datetime(2026, 9, 27, 7, 15, 1, tzinfo=timezone.utc)
        pods = [
            _driver_pod("old-driver", finished - timedelta(hours=1)),
            _driver_pod("d", finished),
        ]
        status = self._status(driver_pod="d", completion_time="2026-09-27T07:15:09Z")
        with patch("kubernetes.client.CoreV1Api") as core:
            core.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            end, source = self._monitor().application_end("lakebench-silver-build", status)
        assert (end, source) == (finished, "driver_container")

    def test_falls_back_to_termination_time(self):
        status = self._status(driver_pod="d", completion_time="2026-09-27T07:15:09Z")
        with patch("kubernetes.client.CoreV1Api") as core:
            core.return_value.list_namespaced_pod.side_effect = RuntimeError("no cluster")
            end, source = self._monitor().application_end("lakebench-silver-build", status)
        assert source == "spark_application"
        assert end == datetime(2026, 9, 27, 7, 15, 9, tzinfo=timezone.utc)

    def test_nothing_readable(self):
        with patch("kubernetes.client.CoreV1Api") as core:
            core.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=[])
            assert self._monitor().application_end("x", None) == (None, "")


class TestRunStageTiming:
    def test_run_loop_helper_uses_the_cluster_end(self):
        from lakebench.cli import _run

        result = SimpleNamespace(final_status=None)
        monitor = SimpleNamespace(
            application_end=lambda _n, _s: (T0 + timedelta(seconds=64.0), "spark_application")
        )
        with patch("lakebench.cli._sustained.cluster_clock_offset_seconds", return_value=0.0):
            t = _run._stage_timing(
                monitor, "lakebench-bronze-verify", result, T0, T0 + timedelta(seconds=75.1)
            )
        assert t.source == "spark_application"
        # +0.5 s centres the second-truncated cluster timestamp
        assert t.elapsed_seconds == pytest.approx(64.5, abs=0.01)

    def test_run_loop_helper_survives_an_unreadable_cluster(self):
        from lakebench.cli import _run

        def boom(_n, _s):
            raise RuntimeError("api down")

        t = _run._stage_timing(
            SimpleNamespace(application_end=boom),
            "x",
            SimpleNamespace(final_status=None),
            T0,
            T0 + timedelta(seconds=30),
        )
        assert t.source == "poll" and t.resolution_seconds == _run._STAGE_POLL_S

    def test_poll_is_no_longer_15s(self):
        from lakebench.cli import _run

        assert _run._STAGE_POLL_S <= 5

    def test_timing_source_reaches_the_stage_and_survives_a_reload(self):
        from lakebench.metrics import build_pipeline_benchmark
        from lakebench.metrics.collector import JobMetrics, PipelineMetrics
        from lakebench.metrics.storage import MetricsStorage

        job = JobMetrics(
            job_name="lakebench-bronze-verify",
            job_type="bronze-verify",
            start_time=T0,
            end_time=T0 + timedelta(seconds=64.25),
            elapsed_seconds=64.25,
            timing_source="driver_container",
            timing_resolution_seconds=1.0,
            success=True,
        )
        run = PipelineMetrics(run_id="r", deployment_name="d", start_time=T0, jobs=[job])
        run.pipeline_benchmark = build_pipeline_benchmark(run)
        stage = run.pipeline_benchmark.stages[0]
        assert (stage.timing_source, stage.timing_resolution_seconds) == ("driver_container", 1.0)
        back = MetricsStorage.__new__(MetricsStorage)._dict_to_metrics(
            json.loads(json.dumps(run.to_dict()))
        )
        assert back.jobs[0].timing_source == "driver_container"
        assert back.pipeline_benchmark.stages[0].timing_resolution_seconds == 1.0
        assert back.pipeline_benchmark.time_to_value_seconds == pytest.approx(64.25)


# ---------------------------------------------------------------------------
# 5. lakebench info shows the pipeline mode and the configured date range
# ---------------------------------------------------------------------------


def _continuous_c360(tmp_path, **datagen):
    import yaml

    from tests.conftest import make_config

    base = make_config().model_dump(mode="json")
    data = {
        "name": "lb16-cont2",
        "platform": base["platform"],
        "architecture": {"pipeline": {"mode": "continuous"}},
        "workload": {"schema": "customer360", "datagen": {"scale": 1, **datagen}},
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


class TestInfoLabels:
    def test_continuous_config_is_not_labelled_batch(self, tmp_path):
        from typer.testing import CliRunner

        from lakebench.cli import app

        path = _continuous_c360(tmp_path, timestamp_start="2024-12-18", timestamp_end="2025-01-01")
        out = CliRunner().invoke(app, ["info", str(path)])
        assert out.exit_code == 0, out.output
        assert "customer360-continuous" in out.output
        assert "customer360-batch" not in out.output
        assert "14 days (2024-12-18 to 2025-01-01)" in out.output
        assert "365 days" not in out.output

    def test_continuous_datagen_mode_says_how_the_corpus_arrives(self, tmp_path):
        """lb16-cs: "Datagen mode: batch" beside "Pipeline mode: continuous"."""
        from typer.testing import CliRunner

        from lakebench.cli import app

        out = CliRunner().invoke(app, ["info", str(_continuous_c360(tmp_path))])
        assert out.exit_code == 0, out.output
        text = " ".join(out.output.replace("\u2502", " ").split())
        assert "batch generator (auto for scale 1); corpus written up front" in text
        assert "trickled to bronze by the pipeline" in text

    def test_datagen_mode_line_batch_and_explicit(self):
        from lakebench.cli import info_datagen_mode
        from tests.conftest import make_config

        cfg = make_config()
        assert info_datagen_mode(cfg).startswith("batch generator (auto for scale")
        assert "trickle" not in info_datagen_mode(cfg)
        cfg = make_config(workload={"datagen": {"mode": "continuous"}})
        assert info_datagen_mode(cfg) == "continuous generator (set in config)"

    def test_date_range_defaults_and_non_c360(self):
        from lakebench.cli import info_date_range
        from tests.conftest import make_config

        cfg = make_config()
        assert info_date_range(cfg, 365) == "365 days"
        cfg.architecture.workload.datagen.timestamp_end = "2024-01-15"
        assert info_date_range(cfg, 365) == "14 days (2024-01-01 to 2024-01-15)"
        cfg.architecture.workload.datagen.timestamp_start = "bad"
        assert info_date_range(cfg, 365) == "365 days"
        fin = make_config(workload={"schema": "financial"})
        fin.architecture.workload.datagen.timestamp_start = "2024-12-18"
        assert info_date_range(fin, 1826) == "1826 days"


# ---------------------------------------------------------------------------
# 6. Destroy names the tables whose files stay in a refused bucket
# ---------------------------------------------------------------------------


class TestDestroyNamesTablesLeftInRefusedBuckets:
    def _engine(self):
        from tests.conftest import make_config

        cfg = make_config(workload={"schema": "financial"})
        cfg.platform.storage.s3.buckets.bronze = "lb16-lb186-bronze"
        cfg.platform.storage.s3.buckets.silver = "lb16-lb186-silver"
        cfg.platform.storage.s3.buckets.gold = "lb16-lb186-gold"
        return SimpleNamespace(config=cfg)

    def test_refused_bronze_names_its_tables_only(self):
        from lakebench.deploy import destroy as destroy_mod

        engine = self._engine()
        tables = engine.config.architecture.tables
        bronze = [f"lakehouse.{t}" for t in tables.workload_tables("financial", layers=("bronze",))]
        silver = [f"lakehouse.{t}" for t in tables.workload_tables("financial", layers=("silver",))]
        notes = destroy_mod._files_left_in_refused_buckets(
            engine, {"lb16-lb186-bronze"}, bronze + silver
        )
        assert len(notes) == 1
        assert "remain in lb16-lb186-bronze" in notes[0]
        for t in bronze:
            assert t.split(".", 1)[1] in notes[0]
        for t in silver:
            assert t.split(".", 1)[1] not in notes[0]

    def test_nothing_refused_or_nothing_unregistered(self):
        from lakebench.deploy import destroy as destroy_mod

        engine = self._engine()
        assert destroy_mod._files_left_in_refused_buckets(engine, set(), ["lakehouse.a.b"]) == []
        assert destroy_mod._files_left_in_refused_buckets(engine, {"lb16-lb186-bronze"}, []) == []

    def test_end_to_end_the_bucket_step_says_where_the_files_stay(self):
        """LB-186 live: bronze refused (tagless, not on the record), Trino
        unregistered the tables, and the refused bucket kept their files."""
        from tests.test_destroy_bucket_delete import FakeBoto, TestDestroyAllBuckets

        h = TestDestroyAllBuckets()
        boto = FakeBoto({"a-bronze": ["warehouse/t/data.parquet"], "a-silver": [], "a-gold": []})
        r = h._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "UNSUPPORTED"),
            created={"a-silver", "a-gold"},
            maint=("trino", "trino-coordinator-0", "lakehouse"),
        )
        assert boto.buckets == {"a-bronze": ["warehouse/t/data.parquet"]}
        assert "their files remain in a-bronze" in r.message
        assert "silver.t" in r.message
        tables = [x for x in h._results if x.component == "table-cleanup"][-1]
        assert "silver.t, gold.t" in tables.message


class TestReviewFixes:
    def test_slow_node_clock_is_not_taken_as_the_end(self):
        """Review: a driver node 120 s slow on a 470 s stage read as 345 s."""
        t = _timing(
            observed_end=T0 + timedelta(seconds=470.0),
            cluster_end=T0 + timedelta(seconds=350.0 + 4.5),
        )
        assert t.source == "poll" and "outside the observed run" in t.note

    def test_end_within_poll_and_operator_lag_is_accepted(self):
        t = _timing(
            observed_end=T0 + timedelta(seconds=470.0),
            cluster_end=T0 + timedelta(seconds=440.0 + 4.5),
        )
        assert t.source == "driver_container"

    def test_clock_offset_is_read_once_per_monitor(self):
        from lakebench.cli import _run

        calls = []

        def offset():
            calls.append(1)
            return 0.0

        monitor = SimpleNamespace(
            application_end=lambda _n, _s: (T0 + timedelta(seconds=60), "spark_application")
        )
        result = SimpleNamespace(final_status=None)
        with patch("lakebench.cli._sustained.cluster_clock_offset_seconds", side_effect=offset):
            for _ in range(3):
                _run._stage_timing(monitor, "x", result, T0, T0 + timedelta(seconds=65))
        assert len(calls) == 1

    def test_compare_maps_the_old_freshness_key(self):
        from lakebench.cli._compare import _renamed_scores

        assert _renamed_scores({"query_time_freshness_seconds": 5.0, "x": 1}) == {
            "query_time_event_age_seconds": 5.0,
            "x": 1,
        }


class TestRunOrdering:
    def test_latest_run_is_by_instant_not_string(self, tmp_path, monkeypatch):
        """Review: on a UTC+5:30 host an old naive run at 10:00 local sorted
        after a new UTC run at 05:30Z (11:00 local)."""
        import time

        from lakebench.metrics.storage import MetricsStorage

        monkeypatch.setenv("TZ", "Asia/Kolkata")
        time.tzset()
        try:
            for rid, start in (
                ("20260927-100000-aaaaaa", "2026-09-27T10:00:00.000001"),
                ("20260927-110000-bbbbbb", "2026-09-27T05:30:00.000001+00:00"),
            ):
                d = tmp_path / f"run-{rid}"
                d.mkdir()
                (d / "metrics.json").write_text(
                    json.dumps({"run_id": rid, "deployment_name": "d", "start_time": start})
                )
            runs = MetricsStorage(metrics_dir=tmp_path).list_runs()
        finally:
            monkeypatch.delenv("TZ")
            time.tzset()
        assert [r["run_id"] for r in runs][0] == "20260927-110000-bbbbbb"


class TestLagAnchor:
    def test_end_anchored_on_the_last_running_poll_survives_a_slow_api(self):
        """Fix pass: under API load the observed end can trail the driver by
        more than poll + lag; the last running poll is the real bound."""
        t = _timing(
            observed_end=T0 + timedelta(seconds=470.0),
            cluster_end=T0 + timedelta(seconds=380.0 + 4.5),
            last_running=T0 + timedelta(seconds=375.0),
        )
        assert t.source == "driver_container"

    def test_end_well_before_the_last_running_poll_is_skew(self):
        t = _timing(
            observed_end=T0 + timedelta(seconds=470.0),
            cluster_end=T0 + timedelta(seconds=300.0 + 4.5),
            last_running=T0 + timedelta(seconds=465.0),
        )
        assert t.source == "poll"
