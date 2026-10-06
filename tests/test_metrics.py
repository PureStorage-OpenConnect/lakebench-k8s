"""Tests for metrics collection, storage, and report generation."""

import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from lakebench.metrics import (
    BenchmarkMetrics,
    BenchmarkRoundMeta,
    JobMetrics,
    MetricsCollector,
    MetricsStorage,
    PipelineBenchmark,
    PipelineMetrics,
    QueryMetrics,
    StageMetrics,
    StreamingJobMetrics,
    aggregate_benchmark_rounds,
    build_pipeline_benchmark,
)
from lakebench.reports.generator import ReportGenerator

# ---------------------------------------------------------------------------
# JobMetrics
# ---------------------------------------------------------------------------


class TestJobMetrics:
    """Tests for JobMetrics dataclass."""


# ---------------------------------------------------------------------------
# QueryMetrics
# ---------------------------------------------------------------------------


class TestQueryMetrics:
    """Tests for QueryMetrics dataclass."""


# ---------------------------------------------------------------------------
# PipelineMetrics
# ---------------------------------------------------------------------------


class TestPipelineMetrics:
    """Tests for PipelineMetrics dataclass."""


# ---------------------------------------------------------------------------
# MetricsCollector
# ---------------------------------------------------------------------------


class TestMetricsCollector:
    """Tests for MetricsCollector lifecycle."""


# ---------------------------------------------------------------------------
# MetricsStorage
# ---------------------------------------------------------------------------


class TestMetricsStorage:
    """Tests for metrics persistence."""

    def test_save_and_load_roundtrip(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")

        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="test-run-001",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=300),
            total_elapsed_seconds=300.0,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    success=True,
                    elapsed_seconds=53.0,
                    input_size_gb=1.0,
                    output_rows=100_000,
                ),
            ],
            queries=[
                QueryMetrics(
                    query_name="rfm",
                    query_text="SELECT * FROM gold.customer_executive_dashboard",
                    elapsed_seconds=3.42,
                    rows_returned=100,
                    success=True,
                ),
            ],
            config_snapshot={"name": "test"},
        )

        filepath = storage.save_run(metrics)
        assert filepath.exists()
        assert filepath.name == "metrics.json"
        assert "test-run-001" in str(filepath.parent.name)

        loaded = storage.load_run("test-run-001")
        assert loaded is not None
        assert loaded.run_id == "test-run-001"
        assert loaded.success is True
        assert len(loaded.jobs) == 1
        assert loaded.jobs[0].job_name == "lakebench-bronze-verify"
        assert len(loaded.queries) == 1
        assert loaded.queries[0].query_name == "rfm"
        assert loaded.queries[0].elapsed_seconds == 3.42

    def test_save_and_load_financial_scoring_and_detection_fields(self, tmp_path):
        """LB-123: financial_scoring AND the JobMetrics detection dicts must
        survive save -> load. The report is always rendered from the disk
        reload, so a field dropped here shows the scorecard zero alerts /
        no recall even though the live run had them."""
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="aml-001",
            deployment_name="test",
            start_time=now,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-gold-finalize",
                    job_type="gold-finalize",
                    success=True,
                    alerts_by_rule={"W2_structuring": 12, "W3_round_tripping": 5},
                    rules_skipped={"W1_connected_components": "vertex-cap"},
                    rule_errors={"W7_cross_border_high_risk": "boom"},
                ),
            ],
            financial_scoring={
                "typologies": [
                    {
                        "typology_type": "micro_structuring",
                        "recall": 0.83,
                        "instance_count": 6,
                        "detection_status": "scored",
                    }
                ],
                "total_alerts": 17,
                "fp_alerts": 2,
                "fp_rate": 0.1176,
                "run_id": "aml-001",
            },
        )
        storage.save_run(metrics)
        loaded = storage.load_run("aml-001")
        assert loaded is not None
        # financial_scoring round-trips.
        assert loaded.financial_scoring is not None
        assert loaded.financial_scoring["typologies"][0]["recall"] == 0.83
        assert loaded.financial_scoring["total_alerts"] == 17
        # JobMetrics detection dicts round-trip (the HIGH finding: previously
        # serialized but dropped on load).
        job = loaded.jobs[0]
        assert job.alerts_by_rule == {"W2_structuring": 12, "W3_round_tripping": 5}
        assert job.rules_skipped == {"W1_connected_components": "vertex-cap"}
        assert job.rule_errors == {"W7_cross_border_high_risk": "boom"}

    def test_detection_dicts_parse_record_and_roundtrip(self, tmp_path):
        """LB-123 re-review F1 guard: the detection dicts must survive the
        FULL cluster path shape -- parse_driver_logs (the only producer) ->
        JobMetrics fields the cluster run copies -> save_run -> load_run.
        The old suite injected dicts straight into JobMetrics, hiding that
        the cluster run never copied parse_driver_logs' detection fields."""
        from lakebench.metrics import MetricsCollector

        logs = (
            "[detection] W2_structuring: alerts=12 elapsed=3.1s\n"
            "[detection] W3_round_tripping: alerts=5 elapsed=2.0s\n"
            "[detection] W1_connected_components: skipped=vertex-cap "
            "detail=vertices=111111 max=100 elapsed=3.5s\n"
        )
        parsed = MetricsCollector().parse_driver_logs(logs, "gold-finalize")
        # Source of truth populates the three dicts.
        assert parsed.alerts_by_rule == {"W2_structuring": 12, "W3_round_tripping": 5}
        assert parsed.rules_skipped == {"W1_connected_components": "vertex-cap"}
        assert "W1_connected_components" not in parsed.alerts_by_rule

        # Mirror the cluster run copy (_run.py) onto a fresh JobMetrics, then
        # round-trip through storage.
        storage = MetricsStorage(tmp_path / "metrics")
        job = JobMetrics(
            job_name="lakebench-gold-finalize",
            job_type="gold-finalize",
            success=True,
            alerts_by_rule=parsed.alerts_by_rule,
            rule_errors=parsed.rule_errors,
            rules_skipped=parsed.rules_skipped,
        )
        metrics = PipelineMetrics(
            run_id="det-001",
            deployment_name="test",
            start_time=datetime.now(),
            jobs=[job],
        )
        storage.save_run(metrics)
        loaded = storage.load_run("det-001")
        assert loaded.jobs[0].alerts_by_rule == {"W2_structuring": 12, "W3_round_tripping": 5}
        assert loaded.jobs[0].rules_skipped == {"W1_connected_components": "vertex-cap"}


# ---------------------------------------------------------------------------
# ReportGenerator
# ---------------------------------------------------------------------------


class TestReportGenerator:
    """Tests for HTML report generation."""

    def _make_sample_metrics(self) -> PipelineMetrics:
        now = datetime.now()
        return PipelineMetrics(
            run_id="report-test-001",
            deployment_name="test-deploy",
            start_time=now,
            end_time=now + timedelta(seconds=300),
            total_elapsed_seconds=300.0,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    success=True,
                    elapsed_seconds=53.0,
                    input_size_gb=1.0,
                    output_rows=100_000,
                    throughput_gb_per_second=0.019,
                ),
                JobMetrics(
                    job_name="lakebench-silver-build",
                    job_type="silver-build",
                    success=True,
                    elapsed_seconds=107.0,
                    input_size_gb=1.0,
                    output_rows=500_000,
                    throughput_gb_per_second=0.009,
                ),
            ],
            queries=[
                QueryMetrics(
                    query_name="rfm",
                    query_text="SELECT customer_id ...",
                    elapsed_seconds=3.42,
                    rows_returned=100,
                    success=True,
                ),
                QueryMetrics(
                    query_name="revenue",
                    query_text="SELECT interaction_date ...",
                    elapsed_seconds=1.87,
                    rows_returned=30,
                    success=True,
                ),
            ],
            config_snapshot={"name": "test-deploy"},
        )


class TestSustainedReport:
    """Tests for sustained-mode HTML report rendering."""

    @staticmethod
    def _make_sustained_metrics() -> PipelineMetrics:
        """Build a minimal sustained PipelineMetrics with streaming + pipeline benchmark."""
        now = datetime.now()
        return PipelineMetrics(
            run_id="cont-report-001",
            deployment_name="cont-deploy",
            start_time=now,
            end_time=now + timedelta(seconds=1800),
            total_elapsed_seconds=1800.0,
            success=True,
            streaming=[
                StreamingJobMetrics(
                    job_name="lb-bronze-ingest",
                    job_type="bronze-ingest",
                    throughput_rps=5200.0,
                    freshness_seconds=12.0,
                    micro_batch_duration_ms=850.0,
                    total_batches=2100,
                    total_rows_processed=9_400_000,
                    unique_rows_processed=9_400_000,
                    elapsed_seconds=1800.0,
                    success=True,
                ),
                StreamingJobMetrics(
                    job_name="lb-silver-stream",
                    job_type="silver-stream",
                    throughput_rps=4800.0,
                    freshness_seconds=18.0,
                    micro_batch_duration_ms=1200.0,
                    total_batches=1500,
                    total_rows_processed=8_600_000,
                    unique_rows_processed=8_600_000,
                    elapsed_seconds=1800.0,
                    success=True,
                ),
                StreamingJobMetrics(
                    job_name="lb-gold-refresh",
                    job_type="gold-refresh",
                    throughput_rps=3000.0,
                    freshness_seconds=25.0,
                    micro_batch_duration_ms=5000.0,
                    total_batches=360,
                    total_rows_processed=5_400_000,
                    unique_rows_processed=2_700_000,
                    elapsed_seconds=1800.0,
                    success=True,
                ),
            ],
            pipeline_benchmark=PipelineBenchmark(
                run_id="cont-report-001",
                deployment_name="cont-deploy",
                pipeline_mode="sustained",
                start_time=now,
                end_time=now + timedelta(seconds=1800),
                success=True,
                stages=[
                    StageMetrics(
                        stage_name="bronze",
                        stage_type="streaming",
                        engine="spark",
                        elapsed_seconds=1800.0,
                        input_size_gb=85.0,
                        input_rows=9_400_000,
                        output_rows=9_400_000,
                        throughput_rows_per_second=5222.0,
                        latency_ms=850.0,
                        freshness_seconds=12.0,
                        executor_count=4,
                        executor_cores=2,
                        executor_memory_gb=4.0,
                        success=True,
                    ),
                    StageMetrics(
                        stage_name="silver",
                        stage_type="streaming",
                        engine="spark",
                        elapsed_seconds=1800.0,
                        input_size_gb=80.0,
                        input_rows=8_600_000,
                        output_rows=8_600_000,
                        throughput_rows_per_second=4778.0,
                        latency_ms=1200.0,
                        freshness_seconds=18.0,
                        executor_count=8,
                        executor_cores=4,
                        executor_memory_gb=48.0,
                        success=True,
                    ),
                    StageMetrics(
                        stage_name="gold",
                        stage_type="streaming",
                        engine="spark",
                        elapsed_seconds=1800.0,
                        input_size_gb=50.0,
                        input_rows=5_400_000,
                        output_rows=5_400_000,
                        throughput_rows_per_second=3000.0,
                        latency_ms=5000.0,
                        freshness_seconds=25.0,
                        executor_count=4,
                        executor_cores=4,
                        executor_memory_gb=32.0,
                        success=True,
                    ),
                ],
                data_freshness_seconds=25.0,
                sustained_throughput_rps=5200.0,
                total_data_processed_gb=215.0,
                pipeline_throughput_gb_per_second=0.119,
                stage_latency_profile=[850.0, 1200.0, 5000.0],
                ingest_ratio=0.98,
                compute_efficiency_gb_per_core_hour=0.42,
                total_rows_processed=23_400_000,
                total_elapsed_seconds=1800.0,
                query_time_event_age_seconds=22.0,
            ),
            config_snapshot={
                "name": "cont-deploy",
                "scale": 50,
                "catalog": "hive",
                "table_format": "iceberg",
                "pipeline_engine": "spark",
                "query_engine": "trino",
            },
        )

    def _render(self, metrics: PipelineMetrics | None = None) -> str:
        gen = ReportGenerator(metrics_dir="/tmp/unused-cont-rg")
        return gen._generate_html(metrics or self._make_sustained_metrics())


# ---------------------------------------------------------------------------
# StreamingJobMetrics
# ---------------------------------------------------------------------------


class TestStreamingJobMetrics:
    """Tests for StreamingJobMetrics dataclass."""


# ---------------------------------------------------------------------------
# Streaming log parsing
# ---------------------------------------------------------------------------


class TestStreamingLogParsing:
    """Tests for streaming driver log parsing."""

    def test_parse_streaming_logs_captures_per_batch_silver_labels(self):
        """D-full-simple + E1 + I7 labels emitted per micro-batch land in
        StreamingJobMetrics.extra_metrics. Live validation on
        lb-silver-live-v16c showed these labels in driver logs but the
        narrow silver_stream_scale_(cap|admission) regex would have
        dropped them, so metrics.json would ship without the per-batch
        parity_mode / dim_merge_elapsed / kyc_refresh evidence."""
        c = MetricsCollector()
        logs = """
[lb] 2026-09-28T15:54:53.883921 - silver_stream_scale_cap: measured_up_to_scale_10
[lb] 2026-09-28T15:54:53.883921 - silver_stream_scale_admission: ok
[lb] 2026-09-28T15:55:13.041566 - [batch 0] 189335 txns
[lb] 2026-09-28T15:56:14.522310 - silver_statements_parity_mode: strict_monotone
[lb] 2026-09-28T15:56:14.522514 - silver_statements_late_arrivals_this_batch: 0
[lb] 2026-09-28T15:56:14.522514 - silver_statements_batch_id: 0
[lb] 2026-09-28T15:55:53.790921 - dim_merge_elapsed_ms_entities: 12345
[lb] 2026-09-28T15:55:53.790921 - dim_merge_elapsed_ms_accounts: 6789
[lb] 2026-09-28T15:55:25.028644 - kyc_refreshed_at: 1717029325
[lb] 2026-09-28T15:55:25.028644 - kyc_refresh_kind: initial
"""
        metrics = c.parse_streaming_logs(logs, "silver-stream")
        e = metrics.extra_metrics
        assert e.get("silver_stream_scale_cap") == "measured_up_to_scale_10"
        assert e.get("silver_stream_scale_admission") == "ok"
        assert e.get("silver_statements_parity_mode") == "strict_monotone"
        assert e.get("silver_statements_late_arrivals_this_batch") == "0"
        assert e.get("silver_statements_batch_id") == "0"
        assert e.get("dim_merge_elapsed_ms_entities") == "12345"
        assert e.get("dim_merge_elapsed_ms_accounts") == "6789"
        assert e.get("kyc_refreshed_at") == "1717029325"
        assert e.get("kyc_refresh_kind") == "initial"

    def test_parse_streaming_logs_captures_new_labels_under_accepted_prefixes(self):
        """Class-level guarantee: any new label under an accepted prefix
        family (silver_/dim_merge_/kyc_/data_clock_) is picked up without
        editing the regex. Two live-caught omissions in silver-plan r3
        (silver_tables adapter + narrow streaming regex) were both the
        same shape: a hand-enumerated list drifting behind the code that
        emits into it. A hypothetical future label with any of these
        prefixes must land in extra_metrics with no code change here."""
        c = MetricsCollector()
        logs = """
[lb] 2026-09-28T15:00:00.000000 - silver_frobnicate_rows: 42
[lb] 2026-09-28T15:00:00.000000 - silver_stream_backpressure_ms: 137
[lb] 2026-09-28T15:00:00.000000 - dim_merge_conflicts: 3
[lb] 2026-09-28T15:00:00.000000 - kyc_pending_refreshes: 11
[lb] 2026-09-28T15:00:00.000000 - data_clock_skew_ms: -250
26/09/28 15:00:00 INFO SparkContext: unrelated_something: should_not_match
"""
        metrics = c.parse_streaming_logs(logs, "silver-stream")
        e = metrics.extra_metrics
        assert e.get("silver_frobnicate_rows") == "42"
        assert e.get("silver_stream_backpressure_ms") == "137"
        assert e.get("dim_merge_conflicts") == "3"
        assert e.get("kyc_pending_refreshes") == "11"
        assert e.get("data_clock_skew_ms") == "-250"
        # Spark's own log lines never match; they carry the timestamp
        # prefix before any word, so the anchored regex ignores them.
        assert "unrelated_something" not in e

    def test_parse_streaming_logs_rejects_multi_label_per_line(self):
        """Reviewer-caught (2026-09-28): pre-fix, greedy `\\S.*` value
        pattern silently swallowed the second label on lines like
        `silver_statements_parity_mode: X silver_statements_late_arrivals_this_batch: Y`.
        The four emitter sites in silver_stream_financial.py have been
        split to one-per-line, and the regex value pattern (single token,
        `\\S+\\s*$`) refuses to swallow. A multi-label line now matches
        the regex NOT AT ALL -- so both labels are dropped, but LOUDLY:
        the user notices the intended metric is missing rather than
        seeing a garbage value they might trust. Loud loss is better
        than silent corruption.
        """
        c = MetricsCollector()
        logs = (
            "[lb] 2026-09-28T15:00:00 - silver_statements_parity_mode: "
            "strict_monotone silver_statements_late_arrivals_this_batch: 0\n"
        )
        metrics = c.parse_streaming_logs(logs, "silver-stream")
        e = metrics.extra_metrics
        # The whole line is refused: neither label lands. That is the
        # loud failure mode; a regression in the emitter now shows up
        # as a missing metric rather than a garbage-valued metric.
        assert "silver_statements_parity_mode" not in e
        assert "silver_statements_late_arrivals_this_batch" not in e

    def test_parse_streaming_logs_rejects_uppercase_and_hyphen_keys(self):
        """Design choice locked in (2026-09-28 adversarial-review):
        key tail is lowercase snake_case ([a-z0-9_]+). An emitter that
        used `silver_TxScale` or `dim_merge_ok-count` would fail the
        prefix-family match and never land, by design -- lane-specific
        typos should not become metric names. This test documents that
        so the rejection is intentional, not accidental."""
        c = MetricsCollector()
        logs = (
            "[lb] 2026-09-28T15:00:00 - silver_TxScale: 5\n"
            "[lb] 2026-09-28T15:00:00 - dim_merge_ok-count: 3\n"
            "[lb] 2026-09-28T15:00:00 - silver_valid_lowercase: 7\n"
        )
        metrics = c.parse_streaming_logs(logs, "silver-stream")
        e = metrics.extra_metrics
        assert "silver_TxScale" not in e
        assert "dim_merge_ok-count" not in e
        # Only the strictly-lowercase key is captured.
        assert e.get("silver_valid_lowercase") == "7"


# ---------------------------------------------------------------------------
# Streaming storage roundtrip
# ---------------------------------------------------------------------------


class TestStreamingStorageRoundtrip:
    """Tests for streaming metrics persistence."""


# ---------------------------------------------------------------------------
# Streaming report generation
# ---------------------------------------------------------------------------


class TestStreamingReportGeneration:
    """Tests for streaming section in HTML reports."""


# ---------------------------------------------------------------------------
# Phase 1+2: Driver log parsing with [lb] prefix
# ---------------------------------------------------------------------------


class TestDriverLogParsingWithLbPrefix:
    """Tests for parse_driver_logs with realistic [lb]-prefixed output."""


class TestDetectionRulesMetrics:
    """LB-116: per-rule AML alert counts surfaced from the driver log
    into JobMetrics.alerts_by_rule / rule_errors so a metrics parser
    sees one row per rule attempted, not a single total."""

    def test_parse_success_rules(self):
        c = MetricsCollector()
        logs = """\
[lb] 2026-09-22T10:00:00 - [detection] W2_structuring: alerts=1234 prior=0 elapsed=15.2s
[lb] 2026-09-22T10:00:16 - [detection] W3_round_tripping: alerts=42 prior=42 elapsed=8.9s
[lb] 2026-09-22T10:00:25 - [detection] W1_connected_components: alerts=17 prior=17 elapsed=120.4s
[lb] 2026-09-22T10:02:26 - [detection] total alerts written: 1293
"""
        metrics = c.parse_driver_logs(logs, "gold-finalize")
        assert metrics.alerts_by_rule == {
            "W2_structuring": 1234,
            "W3_round_tripping": 42,
            "W1_connected_components": 17,
        }
        assert metrics.rule_errors == {}

    def test_parse_crashed_rule(self):
        c = MetricsCollector()
        logs = """\
[lb] 2026-09-22T10:00:00 - [detection] W2_structuring: alerts=1234 prior=0 elapsed=15.2s
[lb] 2026-09-22T10:00:16 - [detection] W7_cross_border_high_risk: alerts=0 error=AnalysisException: silver.entities not found elapsed=0.4s
"""
        metrics = c.parse_driver_logs(logs, "gold-finalize")
        assert metrics.alerts_by_rule["W2_structuring"] == 1234
        assert metrics.alerts_by_rule["W7_cross_border_high_risk"] == 0
        assert "AnalysisException" in metrics.rule_errors["W7_cross_border_high_risk"]
        assert "silver.entities not found" in metrics.rule_errors["W7_cross_border_high_risk"]

    def test_parse_skipped_rule_is_third_state(self):
        """LB-119: a structural skip (W1 above vertex cap) is recorded in
        rules_skipped and kept OUT of alerts_by_rule, so a skip is never
        read as a 0-alert / 0-recall result. The other rules on the same
        run still parse normally."""
        c = MetricsCollector()
        logs = """\
[lb] 2026-09-22T10:00:00 - [detection] W2_structuring: alerts=1234 prior=0 elapsed=15.2s
[lb] 2026-09-22T10:00:16 - [detection] W1_connected_components: skipped=vertex-cap detail=vertices=50000000 max=8000000 (raise financial.w1_max_vertices to run W1 at this scale) elapsed=0.6s
[lb] 2026-09-22T10:00:20 - [detection] total alerts written: 1234
"""
        metrics = c.parse_driver_logs(logs, "gold-finalize")
        assert metrics.alerts_by_rule == {"W2_structuring": 1234}
        assert "W1_connected_components" not in metrics.alerts_by_rule
        assert metrics.rules_skipped == {"W1_connected_components": "vertex-cap"}
        assert metrics.rule_errors == {}

    def test_skip_and_error_and_success_coexist(self):
        """All three detection outcomes on one run parse into their own
        maps without cross-contamination."""
        c = MetricsCollector()
        logs = """\
[lb] 2026-09-22T10:00:00 - [detection] W2_structuring: alerts=10 prior=0 elapsed=1.0s
[lb] 2026-09-22T10:00:01 - [detection] W1_connected_components: skipped=vertex-cap detail=vertices=9000000 max=8000000 elapsed=0.2s
[lb] 2026-09-22T10:00:02 - [detection] W7_cross_border_high_risk: alerts=0 error=AnalysisException: boom elapsed=0.4s
"""
        metrics = c.parse_driver_logs(logs, "gold-finalize")
        assert metrics.alerts_by_rule == {"W2_structuring": 10, "W7_cross_border_high_risk": 0}
        assert metrics.rules_skipped == {"W1_connected_components": "vertex-cap"}
        assert "AnalysisException" in metrics.rule_errors["W7_cross_border_high_risk"]


# ---------------------------------------------------------------------------
# Phase 1+2: Streaming timing and freshness parsing
# ---------------------------------------------------------------------------


class TestStreamingTimingAndFreshness:
    """Tests for new streaming timing and freshness log patterns."""

    def test_gold_freshness_parsing(self):
        """Gold 'data freshness Xs' should populate freshness_seconds."""
        c = MetricsCollector()
        logs = """\
[lb] 2026-02-01T12:00:01.000Z - Cycle 1: aggregating 150,000 Silver records
[lb] 2026-02-01T12:00:07.000Z - Cycle 1: data freshness 30s
[lb] 2026-02-01T12:00:08.000Z - Cycle 1: refreshed ice.gold.customer_executive_dashboard in 8.2s (30 KPI records)
[lb] 2026-02-01T12:05:01.000Z - Cycle 2: aggregating 300,000 Silver records
[lb] 2026-02-01T12:05:11.000Z - Cycle 2: data freshness 60s
[lb] 2026-02-01T12:05:14.000Z - Cycle 2: refreshed ice.gold.customer_executive_dashboard in 13.5s (30 KPI records)
"""
        metrics = c.parse_streaming_logs(logs, "gold-refresh")
        assert metrics.total_batches == 2
        assert metrics.total_rows_processed == 450_000
        # freshness: max(30, 60) = 60 (worst-case staleness)
        assert metrics.freshness_seconds == pytest.approx(60.0)
        # batch durations from "refreshed ... in Xs"
        assert metrics.micro_batch_duration_ms == pytest.approx(10850.0, abs=1.0)


# ---------------------------------------------------------------------------
# Phase 1: build_config_snapshot
# ---------------------------------------------------------------------------


class TestBuildConfigSnapshot:
    """Tests for build_config_snapshot function."""


# ---------------------------------------------------------------------------
# Phase 1: record_actual_sizes warning on missing bucket
# ---------------------------------------------------------------------------


class TestRecordActualSizesBucketWarning:
    """Test that record_actual_sizes logs a warning for missing buckets."""

    def test_warns_on_missing_bucket(self, caplog):
        c = MetricsCollector()
        c.start_run("run-1", "test", {})

        # Mock S3 client that returns a bucket-not-found response
        mock_s3 = MagicMock()
        info = MagicMock()
        info.size_bytes = None
        info.exists = False
        mock_s3.get_bucket_size.return_value = info

        with caplog.at_level(logging.WARNING, logger="lakebench.metrics.collector"):
            total = c.record_actual_sizes(mock_s3, "lb-bronze", "lb-silver", "lb-gold")

        # Should have 3 warnings, one per bucket
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 3
        assert "lb-bronze" in warnings[0].message
        assert total == 0  # Missing buckets contribute 0 objects


# ---------------------------------------------------------------------------
# Phase 1: throughput_rps computation
# ---------------------------------------------------------------------------


class TestThroughputRpsComputation:
    """Tests for throughput_rps computation logic."""


# ---------------------------------------------------------------------------
# Phase 3: Cross-run comparison
# ---------------------------------------------------------------------------


class TestListRunsEnriched:
    """Tests for enriched list_runs() fields."""


class TestExportCsvEnriched:
    """Tests for enriched export_csv() with per-job columns."""


# ---------------------------------------------------------------------------
# StageMetrics
# ---------------------------------------------------------------------------


class TestStageMetrics:
    """Tests for the universal per-stage measurement dataclass."""


# ---------------------------------------------------------------------------
# PipelineBenchmark
# ---------------------------------------------------------------------------


class TestPipelineBenchmark:
    """Tests for the pipeline-level benchmark dataclass."""

    def _make_stages(self) -> list[StageMetrics]:
        now = datetime.now()
        return [
            StageMetrics(
                stage_name="bronze",
                stage_type="batch",
                engine="spark",
                start_time=now,
                end_time=now + timedelta(seconds=50),
                elapsed_seconds=50.0,
                success=True,
                input_size_gb=10.0,
                output_size_gb=10.0,
                input_rows=5_000_000,
                output_rows=5_000_000,
            ),
            StageMetrics(
                stage_name="silver",
                stage_type="batch",
                engine="spark",
                start_time=now + timedelta(seconds=50),
                end_time=now + timedelta(seconds=150),
                elapsed_seconds=100.0,
                success=True,
                input_size_gb=10.0,
                output_size_gb=3.5,
                input_rows=5_000_000,
                output_rows=4_900_000,
            ),
            StageMetrics(
                stage_name="gold",
                stage_type="batch",
                engine="spark",
                start_time=now + timedelta(seconds=150),
                end_time=now + timedelta(seconds=200),
                elapsed_seconds=50.0,
                success=True,
                input_size_gb=3.5,
                output_size_gb=0.01,
                input_rows=4_900_000,
                output_rows=92,
            ),
        ]

    def test_throughput_uses_ttv_not_summed_time(self):
        """Throughput divides by wall-clock TTV, not sum of stage durations."""
        now = datetime.now()
        # Two stages that overlap: both run from t=0 to t=100
        # Summed elapsed = 200s, but TTV = 100s
        stages = [
            StageMetrics(
                stage_name="bronze",
                stage_type="batch",
                engine="spark",
                start_time=now,
                end_time=now + timedelta(seconds=100),
                elapsed_seconds=100.0,
                success=True,
                input_size_gb=10.0,
                output_size_gb=10.0,
            ),
            StageMetrics(
                stage_name="silver",
                stage_type="batch",
                engine="spark",
                start_time=now,
                end_time=now + timedelta(seconds=100),
                elapsed_seconds=100.0,
                success=True,
                input_size_gb=10.0,
                output_size_gb=3.5,
            ),
        ]
        pb = PipelineBenchmark(
            run_id="ttv-test",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
            stages=stages,
            success=True,
        )
        pb.compute_aggregates()

        assert pb.total_elapsed_seconds == 200.0  # summed
        assert pb.time_to_value_seconds == 100.0  # wall-clock
        # Throughput uses TTV: 20 GB / 100s = 0.2, not 20/200 = 0.1
        assert pb.pipeline_throughput_gb_per_second == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# build_pipeline_benchmark
# ---------------------------------------------------------------------------


class TestBuildPipelineBenchmark:
    """Tests for the PipelineMetrics → PipelineBenchmark conversion."""

    def _make_run(self) -> PipelineMetrics:
        now = datetime.now()
        return PipelineMetrics(
            run_id="build-test",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=300),
            total_elapsed_seconds=300.0,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=3.5,
            gold_size_gb=0.01,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    start_time=now,
                    end_time=now + timedelta(seconds=50),
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                    input_rows=5_000_000,
                    output_rows=5_000_000,
                    executor_count=4,
                    executor_cores=2,
                    executor_memory_gb=4.0,
                ),
                JobMetrics(
                    job_name="lakebench-silver-build",
                    job_type="silver-build",
                    start_time=now + timedelta(seconds=50),
                    end_time=now + timedelta(seconds=200),
                    elapsed_seconds=150.0,
                    success=True,
                    input_size_gb=10.0,
                    input_rows=5_000_000,
                    output_rows=4_900_000,
                    executor_count=12,
                    executor_cores=4,
                    executor_memory_gb=48.0,
                ),
                JobMetrics(
                    job_name="lakebench-gold-finalize",
                    job_type="gold-finalize",
                    start_time=now + timedelta(seconds=200),
                    end_time=now + timedelta(seconds=260),
                    elapsed_seconds=60.0,
                    success=True,
                    input_size_gb=3.5,
                    input_rows=4_900_000,
                    output_rows=92,
                    executor_count=4,
                    executor_cores=4,
                    executor_memory_gb=32.0,
                ),
            ],
            benchmark=BenchmarkMetrics(
                mode="power",
                cache="hot",
                scale=100,
                qph=820.0,
                total_seconds=40.0,
                queries=[{"name": "Q1", "success": True}],
            ),
        )

    def test_s3_size_fallback(self):
        """output_size_gb uses S3 layer size when job reports 0."""
        run = self._make_run()
        # Zero out output_size_gb on jobs (simulating no driver log reporting)
        for j in run.jobs:
            j.output_size_gb = 0.0

        pb = build_pipeline_benchmark(run)

        assert pb.stages[0].output_size_gb == pytest.approx(10.0)  # bronze_size_gb
        assert pb.stages[1].output_size_gb == pytest.approx(3.5)  # silver_size_gb
        assert pb.stages[2].output_size_gb == pytest.approx(0.01)  # gold_size_gb

    def test_gold_input_size_fallback(self):
        """Gold stage uses silver_size_gb when input_size_gb is 0."""
        run = self._make_run()
        # Zero out gold's input_size_gb (simulating Iceberg metadata failure)
        run.jobs[2].input_size_gb = 0.0
        run.silver_size_gb = 101.3

        pb = build_pipeline_benchmark(run)
        gold_stage = [s for s in pb.stages if s.stage_name == "gold"][0]
        assert gold_stage.input_size_gb == pytest.approx(101.3)
        assert gold_stage.throughput_gb_per_second > 0

    def test_empty_fleet_dict_does_not_append_zero_stage(self):
        """A fleet with pods_reported=0 (all pods failed to emit) is still
        a truthy dict; the datagen stage must NOT be appended, otherwise
        core-hour aggregation double-counts a zero-length stage."""
        run = self._make_run()
        empty_fleet = {
            "schema": "customer360",
            "pods_expected": 8,
            "pods_reported": 0,
            "pods_missing": 8,
            "data_quality": "empty",
            "total_bytes_written": 0,
            "total_files_written": 0,
            "total_rows_written": 0,
            "aggregate_mbps": 0.0,
            "wall_elapsed_max_s": 0.0,
            "wall_elapsed_min_s": 0.0,
            "cores_total": 0.0,
            "cpu_seconds_total": 0.0,
            "cpu_hr_per_tb": None,
            "phase_pct": {"build_batch": 0, "encode_parquet": 0, "s3_put": 0},
            "phase_p50_s": {"build_batch": 0, "encode_parquet": 0, "s3_put": 0},
            "phase_p95_s": {"build_batch": 0, "encode_parquet": 0, "s3_put": 0},
            "worst_pod_elapsed_s": 0.0,
            "best_pod_elapsed_s": 0.0,
            "per_pod": [],
        }
        pb = build_pipeline_benchmark(run, datagen_elapsed=0.0, datagen_fleet=empty_fleet)
        stage_names = [s.stage_name for s in pb.stages]
        assert "datagen" not in stage_names

    def test_fleet_enriches_datagen_stage_with_executors(self):
        run = self._make_run()
        fleet = {
            "schema": "customer360",
            "pods_expected": 4,
            "pods_reported": 4,
            "pods_missing": 0,
            "data_quality": "complete",
            "total_bytes_written": 100_000_000_000,
            "total_files_written": 200,
            "total_rows_written": 40_000_000,
            "aggregate_mbps": 833.0,
            "wall_elapsed_max_s": 120.0,
            "wall_elapsed_min_s": 118.0,
            "cores_total": 32.0,  # 4 pods * 8 cores
            "cpu_seconds_total": 3840.0,
            "cpu_hr_per_tb": 10.66,
            "phase_pct": {"build_batch": 55, "encode_parquet": 40, "s3_put": 5},
            "phase_p50_s": {"build_batch": 200, "encode_parquet": 150, "s3_put": 15},
            "phase_p95_s": {"build_batch": 220, "encode_parquet": 165, "s3_put": 20},
            "worst_pod_elapsed_s": 120.0,
            "best_pod_elapsed_s": 118.0,
            "per_pod": [],
        }
        pb = build_pipeline_benchmark(run, datagen_elapsed=120.0, datagen_fleet=fleet)
        dg = next(s for s in pb.stages if s.stage_name == "datagen")
        assert dg.executor_count == 4
        assert dg.executor_cores == 8  # 32 / 4


# ---------------------------------------------------------------------------
# Pipeline benchmark storage roundtrip
# ---------------------------------------------------------------------------


class TestPipelineBenchmarkStorageRoundtrip:
    """Tests for pipeline benchmark save/load through MetricsStorage."""


# ---------------------------------------------------------------------------
# Pipeline benchmark report generation
# ---------------------------------------------------------------------------


class TestPipelineBenchmarkReport:
    """Tests for pipeline benchmark section in HTML reports."""


# ---------------------------------------------------------------------------
# Sustained pipeline scoring
# ---------------------------------------------------------------------------


class TestSustainedPipelineScoring:
    """Tests for sustained (streaming) pipeline scoring."""

    def _make_streaming_run(self) -> PipelineMetrics:
        """Helper: build a 3-stage streaming PipelineMetrics."""
        now = datetime.now()
        return PipelineMetrics(
            run_id="cont-test",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=5.0,
            gold_size_gb=0.5,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=100,
                    total_rows_processed=500_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=277.8,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=5.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=95,
                    total_rows_processed=480_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=266.7,
                    micro_batch_duration_ms=350.0,
                    freshness_seconds=8.5,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=90,
                    total_rows_processed=450_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=250.0,
                    micro_batch_duration_ms=500.0,
                    freshness_seconds=15.0,
                    batch_size=5000,
                ),
            ],
        )

    def test_sustained_scores_computed(self):
        """Sustained scores are computed from streaming stage data."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)

        assert pb.pipeline_mode == "sustained"
        assert len(pb.stages) == 3

        # data_freshness_seconds = max freshness across stages (worst case)
        assert pb.data_freshness_seconds == 15.0

        # sustained_throughput_rps = bronze input_rows / run_duration
        # bronze has 500_000 rows, run_duration = max(1800, 1800, 1800) = 1800
        assert pb.sustained_throughput_rps == pytest.approx(500_000 / 1800.0, rel=1e-2)

        # stage_latency_profile = [bronze_ms, silver_ms, gold_ms]
        assert pb.stage_latency_profile == [200.0, 350.0, 500.0]

        # total_rows_processed = sum of input_rows
        assert pb.total_rows_processed == 500_000 + 480_000 + 450_000

        # total_elapsed_seconds: sustained mode uses wall-clock (max), not sum
        assert pb.total_elapsed_seconds == pytest.approx(1800.0)

    def test_sustained_batch_scores_zero(self):
        """Batch-only scores remain zero for a sustained pipeline."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)

        # time_to_value is batch-only (based on start/end wall-clock span)
        assert pb.time_to_value_seconds == 0.0
        # total_data_processed_gb and pipeline_throughput are now shared
        assert pb.total_data_processed_gb > 0
        assert pb.pipeline_throughput_gb_per_second > 0

    def test_batch_sustained_scores_zero(self):
        """Sustained scores remain zero for a batch pipeline."""
        now = datetime.now()
        run = PipelineMetrics(
            run_id="batch-only",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=5.0,
            gold_size_gb=0.5,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                    output_size_gb=10.0,
                    start_time=now,
                    end_time=now + timedelta(seconds=50),
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)

        assert pb.pipeline_mode == "batch"
        assert pb.data_freshness_seconds is None
        assert pb.sustained_throughput_rps == 0.0
        assert pb.stage_latency_profile == []
        assert pb.total_rows_processed == 0
        # Batch scores are populated
        assert pb.time_to_value_seconds > 0

    def test_streaming_input_size_gb_backfill(self):
        """Streaming stages get input_size_gb from measured S3 bucket sizes."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)

        bronze = next(s for s in pb.stages if s.stage_name == "bronze")
        silver = next(s for s in pb.stages if s.stage_name == "silver")
        gold = next(s for s in pb.stages if s.stage_name == "gold")

        # Bronze reads from landing zone (bronze bucket)
        assert bronze.input_size_gb == pytest.approx(10.0)
        # Silver: measured silver bucket size during monitoring window
        assert silver.input_size_gb == pytest.approx(5.0)
        # Gold reads from silver Iceberg table (silver bucket)
        assert gold.input_size_gb == pytest.approx(5.0)

        # throughput_gb_per_second should now be non-zero (computed by compute_derived)
        assert bronze.throughput_gb_per_second > 0
        assert silver.throughput_gb_per_second > 0
        assert gold.throughput_gb_per_second > 0

    def test_streaming_input_size_gb_no_overwrite(self):
        """Backfill does not overwrite a non-zero input_size_gb."""
        now = datetime.now()
        run = PipelineMetrics(
            run_id="no-overwrite",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=5.0,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=10,
                    total_rows_processed=100_000,
                    elapsed_seconds=600.0,
                    success=True,
                    throughput_rps=166.7,
                    micro_batch_duration_ms=100.0,
                    # input_size_gb is on StageMetrics, not StreamingJobMetrics --
                    # the stage defaults to 0 and gets backfilled
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)
        bronze = next(s for s in pb.stages if s.stage_name == "bronze")
        # Backfilled from run.bronze_size_gb
        assert bronze.input_size_gb == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# BenchmarkRoundMeta
# ---------------------------------------------------------------------------


class TestBenchmarkRoundMeta:
    """Tests for the in-stream benchmark round metadata dataclass."""


# ---------------------------------------------------------------------------
# BenchmarkMetrics with round_meta
# ---------------------------------------------------------------------------


class TestBenchmarkMetricsRoundMeta:
    """Tests for BenchmarkMetrics with round_meta field."""


# ---------------------------------------------------------------------------
# aggregate_benchmark_rounds
# ---------------------------------------------------------------------------


class TestAggregateBenchmarkRounds:
    """Tests for the aggregate_benchmark_rounds function."""

    def _make_round(
        self,
        round_index: int,
        qph: float,
        total_seconds: float,
        q1_elapsed: float = 5.0,
        q2_elapsed: float = 3.0,
        freshness: float = 0.0,
    ) -> BenchmarkMetrics:
        meta = BenchmarkRoundMeta(
            round_index=round_index,
            timestamp=datetime.now(),
            gold_event_age_seconds=freshness,
        )
        return BenchmarkMetrics(
            mode="power",
            cache="hot",
            scale=10,
            qph=qph,
            total_seconds=total_seconds,
            queries=[
                {
                    "name": "q1",
                    "display_name": "Full Scan",
                    "class": "scan",
                    "elapsed_seconds": q1_elapsed,
                    "rows_returned": 100,
                    "success": True,
                },
                {
                    "name": "q2",
                    "display_name": "Date Filter",
                    "class": "filter",
                    "elapsed_seconds": q2_elapsed,
                    "rows_returned": 50,
                    "success": True,
                },
            ],
            round_meta=meta,
        )

    def test_median_of_three(self):
        rounds = [
            self._make_round(1, qph=200.0, total_seconds=30.0, q1_elapsed=5.0),
            self._make_round(2, qph=300.0, total_seconds=25.0, q1_elapsed=4.0),
            self._make_round(3, qph=250.0, total_seconds=28.0, q1_elapsed=4.5),
        ]
        result = aggregate_benchmark_rounds(rounds)
        # Median of [200, 300, 250] = 250
        assert result.qph == 250.0
        # Median of [30, 25, 28] = 28
        assert result.total_seconds == 28.0
        # q1 elapsed median of [5.0, 4.0, 4.5] = 4.5
        q1 = next(q for q in result.queries if q["name"] == "q1")
        assert q1["elapsed_seconds"] == 4.5

    def test_even_count_median(self):
        rounds = [
            self._make_round(1, qph=200.0, total_seconds=30.0),
            self._make_round(2, qph=300.0, total_seconds=20.0),
        ]
        result = aggregate_benchmark_rounds(rounds)
        # Median of [200, 300] = 250
        assert result.qph == 250.0

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Cannot aggregate zero"):
            aggregate_benchmark_rounds([])


# ---------------------------------------------------------------------------
# PipelineMetrics with benchmark_rounds
# ---------------------------------------------------------------------------


class TestPipelineMetricsRounds:
    """Tests for PipelineMetrics benchmark_rounds field."""


# ---------------------------------------------------------------------------
# MetricsCollector record_round
# ---------------------------------------------------------------------------


class TestRecordBenchmarkRound:
    """Tests for MetricsCollector.record_round."""


# ---------------------------------------------------------------------------
# PipelineBenchmark with benchmark_rounds and query_time_event_age
# ---------------------------------------------------------------------------


class TestPipelineBenchmarkRounds:
    """Tests for PipelineBenchmark in-stream benchmark round integration."""

    def _make_streaming_run_with_rounds(self) -> PipelineMetrics:
        now = datetime.now()
        rounds = []
        for i in range(3):
            meta = BenchmarkRoundMeta(
                round_index=i + 1,
                timestamp=now + timedelta(seconds=300 * (i + 1)),
                gold_event_age_seconds=30.0 + i * 10,  # 30, 40, 50
            )
            rounds.append(
                BenchmarkMetrics(
                    mode="power",
                    cache="hot",
                    scale=10,
                    qph=200.0 + i * 25,  # 200, 225, 250
                    total_seconds=30.0,
                    round_meta=meta,
                )
            )

        return PipelineMetrics(
            run_id="rounds-test",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=5.0,
            gold_size_gb=0.5,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=100,
                    total_rows_processed=500_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=277.8,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=5.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=6,
                    total_rows_processed=450_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    freshness_seconds=15.0,
                ),
            ],
            benchmark=BenchmarkMetrics(
                mode="power",
                cache="hot",
                scale=10,
                qph=260.0,
                total_seconds=25.0,
            ),
            benchmark_rounds=rounds,
        )

    def test_query_time_event_age_computed(self):
        run = self._make_streaming_run_with_rounds()
        pb = build_pipeline_benchmark(run)
        # Freshness values: 30, 40, 50 -- median = 40
        assert pb.query_time_event_age_seconds == pytest.approx(40.0)


# ---------------------------------------------------------------------------
# parse_streaming_logs freshness uses max (not average)
# ---------------------------------------------------------------------------


class TestStreamingLogsFreshnessMax:
    """Tests that parse_streaming_logs uses max (worst-case) freshness."""

    def test_max_freshness_not_average(self):
        c = MetricsCollector()
        logs = """\
[lb] 2026-02-01T12:00:01.000Z - Cycle 1: aggregating 100,000 Silver records
[lb] 2026-02-01T12:00:07.000Z - Cycle 1: data freshness 10s
[lb] 2026-02-01T12:00:08.000Z - Cycle 1: refreshed tbl in 7.0s (30 KPI records)
[lb] 2026-02-01T12:05:01.000Z - Cycle 2: aggregating 100,000 Silver records
[lb] 2026-02-01T12:05:11.000Z - Cycle 2: data freshness 50s
[lb] 2026-02-01T12:05:14.000Z - Cycle 2: refreshed tbl in 13.0s (30 KPI records)
[lb] 2026-02-01T12:10:01.000Z - Cycle 3: aggregating 100,000 Silver records
[lb] 2026-02-01T12:10:05.000Z - Cycle 3: data freshness 20s
[lb] 2026-02-01T12:10:09.000Z - Cycle 3: refreshed tbl in 8.0s (30 KPI records)
"""
        metrics = c.parse_streaming_logs(logs, "gold-refresh")
        # max(10, 50, 20) = 50, NOT average (10+50+20)/3 = 26.67
        assert metrics.freshness_seconds == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# Sustained scoring edge cases
# ---------------------------------------------------------------------------


class TestSustainedScoringEdgeCases:
    """Edge-case tests for sustained (streaming) pipeline scoring."""

    def _make_streaming_run(
        self,
        streaming: list[StreamingJobMetrics] | None = None,
        bronze_size_gb: float = 10.0,
        silver_size_gb: float = 5.0,
        gold_size_gb: float = 0.5,
    ) -> PipelineMetrics:
        """Helper: build a streaming PipelineMetrics with customizable stages."""
        now = datetime.now()
        if streaming is None:
            streaming = [
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=100,
                    total_rows_processed=500_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=277.8,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=5.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=95,
                    total_rows_processed=480_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=266.7,
                    micro_batch_duration_ms=350.0,
                    freshness_seconds=8.5,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=90,
                    total_rows_processed=450_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=250.0,
                    micro_batch_duration_ms=500.0,
                    freshness_seconds=15.0,
                    batch_size=5000,
                ),
            ]
        return PipelineMetrics(
            run_id="edge-test",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=bronze_size_gb,
            silver_size_gb=silver_size_gb,
            gold_size_gb=gold_size_gb,
            streaming=streaming,
        )

    def test_sustained_zero_batch_freshness(self):
        """All stages with freshness_seconds=0.0 produce unmeasurable freshness."""
        run = self._make_streaming_run(
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=50,
                    total_rows_processed=200_000,
                    elapsed_seconds=600.0,
                    success=True,
                    throughput_rps=333.3,
                    micro_batch_duration_ms=150.0,
                    freshness_seconds=0.0,
                    batch_size=4000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=48,
                    total_rows_processed=190_000,
                    elapsed_seconds=600.0,
                    success=True,
                    throughput_rps=316.7,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=0.0,
                    batch_size=4000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=45,
                    total_rows_processed=180_000,
                    elapsed_seconds=600.0,
                    success=True,
                    throughput_rps=300.0,
                    micro_batch_duration_ms=250.0,
                    freshness_seconds=0.0,
                    batch_size=4000,
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)

        assert pb.pipeline_mode == "sustained"
        # freshness_seconds=0.0 is falsy, so stages get freshness_seconds=None.
        # _compute_sustained_scores filters for > 0, finds nothing --
        # data_freshness_seconds stays None (unmeasurable).
        assert pb.data_freshness_seconds is None
        # All stages should have freshness_seconds=None (0.0 converted to None)
        for stage in pb.stages:
            if stage.stage_type == "streaming":
                assert stage.freshness_seconds is None

    def test_sustained_saturation_detection(self):
        """Pipeline is saturated when ingest_ratio < 0.95."""
        run = self._make_streaming_run(
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=80,
                    total_rows_processed=400_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=222.2,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=10.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=75,
                    total_rows_processed=380_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=211.1,
                    micro_batch_duration_ms=350.0,
                    freshness_seconds=12.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=70,
                    total_rows_processed=350_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=194.4,
                    micro_batch_duration_ms=500.0,
                    freshness_seconds=20.0,
                    batch_size=5000,
                ),
            ],
        )
        # Bronze processed 400,000 rows but datagen produced 500,000
        pb = build_pipeline_benchmark(run, datagen_output_rows=500_000)

        # ingest_ratio = 400_000 / 500_000 = 0.8 < 0.95 -> saturated
        assert pb.pipeline_saturated is True
        assert pb.ingest_ratio == pytest.approx(0.8, rel=1e-2)

    def test_sustained_missing_stage(self):
        """Streaming run with only bronze and silver (no gold) builds gracefully."""
        run = self._make_streaming_run(
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=100,
                    total_rows_processed=500_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=277.8,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=5.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=95,
                    total_rows_processed=480_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=266.7,
                    micro_batch_duration_ms=350.0,
                    freshness_seconds=12.0,
                    batch_size=5000,
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)

        assert pb.pipeline_mode == "sustained"
        assert len(pb.stages) == 2
        # data_freshness_seconds = max(5.0, 12.0) = 12.0
        assert pb.data_freshness_seconds == pytest.approx(12.0)


# ---------------------------------------------------------------------------
# CycleMetrics (v1.1.0)
# ---------------------------------------------------------------------------


class TestCycleMetrics:
    """Tests for the CycleMetrics dataclass."""


# ---------------------------------------------------------------------------
# BenchmarkRoundMeta table health fields (v1.1.0)
# ---------------------------------------------------------------------------


class TestBenchmarkRoundMetaTableHealth:
    """Tests for table health fields on BenchmarkRoundMeta."""


# ---------------------------------------------------------------------------
# QpH degradation metric (v1.1.0)
# ---------------------------------------------------------------------------


class TestQphDegradation:
    """Tests for qph_degradation_pct computation in sustained scoring."""

    def _make_sustained_pb(self, qph_values):
        """Create a PipelineBenchmark with benchmark rounds at given QpH values."""
        now = datetime.now()
        rounds = []
        for i, qph in enumerate(qph_values):
            rounds.append(
                BenchmarkMetrics(
                    mode="power",
                    cache="hot",
                    scale=10,
                    qph=qph,
                    total_seconds=30.0,
                    round_meta=BenchmarkRoundMeta(
                        round_index=i + 1,
                        timestamp=now + timedelta(minutes=i * 10),
                    ),
                )
            )
        stage = StageMetrics(
            stage_name="bronze-ingest",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=3600.0,
            success=True,
            input_rows=1_000_000,
            freshness_seconds=5.0,
            latency_ms=100.0,
        )
        stage.start_time = now
        stage.end_time = now + timedelta(hours=1)
        pb = PipelineBenchmark(
            run_id="test-deg",
            deployment_name="test",
            pipeline_mode="sustained",
            start_time=now,
            stages=[stage],
            benchmark_rounds=rounds,
            config_snapshot={"datagen_output_rows": 1_000_000},
        )
        pb.compute_aggregates()
        return pb

    def test_degradation_with_4_rounds(self):
        """4 rounds: first half median 200, second half median 100 = 50% degradation."""
        pb = self._make_sustained_pb([200, 200, 100, 100])
        assert pb.qph_degradation_pct is not None
        assert pb.qph_degradation_pct == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# CycleMetrics storage roundtrip (v1.1.0)
# ---------------------------------------------------------------------------


class TestCycleMetricsStorage:
    """Tests for CycleMetrics serialization and deserialization."""
