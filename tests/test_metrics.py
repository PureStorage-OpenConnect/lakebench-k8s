"""Tests for metrics collection, storage, and report generation."""

import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from lakebench.metrics import (
    BenchmarkMetrics,
    JobMetrics,
    MetricsCollector,
    MetricsStorage,
    PipelineBenchmark,
    PipelineMetrics,
    QueryMetrics,
    StageMetrics,
    StreamingJobMetrics,
    build_config_snapshot,
    build_pipeline_benchmark,
)
from lakebench.reports.generator import ReportGenerator

# ---------------------------------------------------------------------------
# JobMetrics
# ---------------------------------------------------------------------------


class TestJobMetrics:
    """Tests for JobMetrics dataclass."""

    def test_basic_job_metrics(self):
        m = JobMetrics(job_name="lakebench-bronze-verify", job_type="bronze-verify")
        assert m.job_name == "lakebench-bronze-verify"
        assert m.success is False
        assert m.elapsed_seconds == 0.0
        assert m.executor_cores == 0
        assert m.executor_memory_gb == 0.0
        assert m.cpu_seconds_requested == 0.0
        assert m.memory_gb_requested == 0.0

    def test_job_metrics_to_dict(self):
        m = JobMetrics(
            job_name="lakebench-silver-build",
            job_type="silver-build",
            elapsed_seconds=107.0,
            success=True,
            input_size_gb=1.0,
            output_rows=500_000,
            throughput_gb_per_second=0.009,
        )
        d = m.to_dict()
        assert d["job_name"] == "lakebench-silver-build"
        assert d["success"] is True
        assert d["output_rows"] == 500_000

    def test_job_metrics_datetime_serialisation(self):
        now = datetime.now()
        m = JobMetrics(
            job_name="test",
            job_type="bronze-verify",
            start_time=now,
            end_time=now + timedelta(seconds=60),
        )
        d = m.to_dict()
        assert d["start_time"] == now.isoformat()
        assert d["end_time"] == (now + timedelta(seconds=60)).isoformat()

    def test_job_metrics_none_times(self):
        m = JobMetrics(job_name="test", job_type="bronze-verify")
        d = m.to_dict()
        assert d["start_time"] is None
        assert d["end_time"] is None

    def test_job_metrics_resource_allocation(self):
        m = JobMetrics(
            job_name="lakebench-silver-build",
            job_type="silver-build",
            elapsed_seconds=200.0,
            success=True,
            executor_count=8,
            executor_cores=4,
            executor_memory_gb=48.0,
            cpu_seconds_requested=6400.0,  # 8 * 4 * 200
            memory_gb_requested=480.0,  # 8 * (48 + 12)
        )
        d = m.to_dict()
        assert d["executor_count"] == 8
        assert d["executor_cores"] == 4
        assert d["executor_memory_gb"] == 48.0
        assert d["cpu_seconds_requested"] == 6400.0
        assert d["memory_gb_requested"] == 480.0
        assert "stages" not in d


# ---------------------------------------------------------------------------
# QueryMetrics
# ---------------------------------------------------------------------------


class TestQueryMetrics:
    """Tests for QueryMetrics dataclass."""

    def test_basic_query_metrics(self):
        q = QueryMetrics(
            query_name="rfm",
            query_text="SELECT * FROM gold.customer_executive_dashboard",
            elapsed_seconds=3.42,
            rows_returned=100,
            success=True,
        )
        assert q.query_name == "rfm"
        assert q.elapsed_seconds == 3.42
        assert q.error_message == ""

    def test_query_metrics_to_dict(self):
        q = QueryMetrics(
            query_name="count",
            query_text="SELECT count(*) FROM silver.interactions",
            elapsed_seconds=0.42,
            rows_returned=1,
            success=True,
        )
        d = q.to_dict()
        assert d["query_name"] == "count"
        assert d["rows_returned"] == 1
        assert d["success"] is True
        assert d["error_message"] == ""

    def test_failed_query_metrics(self):
        q = QueryMetrics(
            query_name="bad-query",
            query_text="SELECT * FROM nonexistent",
            elapsed_seconds=0.1,
            rows_returned=0,
            success=False,
            error_message="Table not found",
        )
        d = q.to_dict()
        assert d["success"] is False
        assert d["error_message"] == "Table not found"


# ---------------------------------------------------------------------------
# PipelineMetrics
# ---------------------------------------------------------------------------


class TestPipelineMetrics:
    """Tests for PipelineMetrics dataclass."""

    def test_basic_pipeline_metrics(self):
        m = PipelineMetrics(
            run_id="20260129-120000-abc123",
            deployment_name="test-deploy",
            start_time=datetime.now(),
        )
        assert m.run_id == "20260129-120000-abc123"
        assert m.success is False
        assert m.jobs == []
        assert m.queries == []

    def test_pipeline_to_dict(self):
        now = datetime.now()
        m = PipelineMetrics(
            run_id="test-run",
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
                ),
            ],
            queries=[
                QueryMetrics(
                    query_name="rfm",
                    query_text="SELECT ...",
                    elapsed_seconds=3.42,
                    rows_returned=100,
                    success=True,
                ),
            ],
        )
        d = m.to_dict()
        assert d["run_id"] == "test-run"
        assert d["success"] is True
        assert len(d["jobs"]) == 1
        assert len(d["queries"]) == 1
        assert d["jobs"][0]["job_name"] == "lakebench-bronze-verify"
        assert d["queries"][0]["query_name"] == "rfm"


# ---------------------------------------------------------------------------
# MetricsCollector
# ---------------------------------------------------------------------------


class TestMetricsCollector:
    """Tests for MetricsCollector lifecycle."""

    def test_start_run(self):
        c = MetricsCollector()
        run = c.start_run("run-1", "test-deploy", {"name": "test"})

        assert run.run_id == "run-1"
        assert run.deployment_name == "test-deploy"
        assert run.config_snapshot == {"name": "test"}
        assert c.current_run is run

    def test_end_run(self):
        c = MetricsCollector()
        c.start_run("run-1", "test", {})
        result = c.end_run(success=True)

        assert result is not None
        assert result.success is True
        assert result.end_time is not None
        assert result.total_elapsed_seconds >= 0

    def test_end_run_without_start(self):
        c = MetricsCollector()
        result = c.end_run()
        assert result is None

    def test_record_job(self):
        c = MetricsCollector()
        c.start_run("run-1", "test", {})

        job = JobMetrics(
            job_name="lakebench-bronze-verify",
            job_type="bronze-verify",
            success=True,
            elapsed_seconds=53.0,
        )
        c.record_job(job)

        assert len(c.current_run.jobs) == 1
        assert c.current_run.jobs[0].job_name == "lakebench-bronze-verify"

    def test_record_query(self):
        c = MetricsCollector()
        c.start_run("run-1", "test", {})

        q = QueryMetrics(
            query_name="rfm",
            query_text="SELECT ...",
            elapsed_seconds=3.42,
            rows_returned=100,
            success=True,
        )
        c.record_query(q)

        assert len(c.current_run.queries) == 1
        assert c.current_run.queries[0].query_name == "rfm"

    def test_record_without_run_is_noop(self):
        c = MetricsCollector()
        job = JobMetrics(job_name="test", job_type="bronze-verify")
        c.record_job(job)  # should not raise
        q = QueryMetrics(query_name="test", query_text="SELECT 1")
        c.record_query(q)  # should not raise

    def test_get_summary(self):
        c = MetricsCollector()
        c.start_run("run-1", "test", {})

        c.record_job(
            JobMetrics(
                job_name="bronze",
                job_type="bronze-verify",
                success=True,
                elapsed_seconds=50,
                input_size_gb=1.0,
                output_rows=100_000,
            )
        )
        c.record_job(
            JobMetrics(
                job_name="silver",
                job_type="silver-build",
                success=True,
                elapsed_seconds=100,
                input_size_gb=1.0,
                output_rows=500_000,
            )
        )
        c.record_job(
            JobMetrics(
                job_name="gold",
                job_type="gold-finalize",
                success=False,
                elapsed_seconds=60,
                input_size_gb=0.5,
                output_rows=50_000,
            )
        )

        summary = c.get_summary()
        assert summary["total_jobs"] == 3
        assert summary["successful_jobs"] == 2
        assert summary["total_input_gb"] == 2.5
        assert summary["total_output_rows"] == 650_000
        assert summary["total_job_time_seconds"] == 210

    def test_record_benchmark(self):
        c = MetricsCollector()
        c.start_run("run-1", "test", {})

        bm = BenchmarkMetrics(
            mode="standard",
            cache="hot",
            scale=102,
            qph=820.4,
            total_seconds=43.88,
            queries=[{"name": "Q1", "success": True}],
        )
        c.record_benchmark(bm)

        assert c.current_run.benchmark is not None
        assert c.current_run.benchmark.qph == 820.4

    def test_record_benchmark_without_run_is_noop(self):
        c = MetricsCollector()
        bm = BenchmarkMetrics(
            mode="standard",
            cache="hot",
            scale=10,
            qph=100.0,
            total_seconds=10.0,
        )
        c.record_benchmark(bm)  # should not raise

    def test_get_summary_empty(self):
        c = MetricsCollector()
        assert c.get_summary() == {}

    def test_parse_driver_logs(self):
        c = MetricsCollector()
        logs = """
2026-01-29 12:00:00 INFO Starting job
=== JOB METRICS: bronze-verify ===
total_size_gb: 1.5
estimated_rows: 1000000
output_rows: 999500
========================================
2026-01-29 12:00:53 INFO completed in 53.2s
"""
        metrics = c.parse_driver_logs(logs, "bronze-verify")
        assert metrics.job_type == "bronze-verify"
        assert metrics.input_size_gb == 1.5
        assert metrics.input_rows == 1_000_000
        assert metrics.output_rows == 999_500
        assert metrics.elapsed_seconds == 53.2
        assert metrics.throughput_gb_per_second > 0

    def test_parse_driver_logs_no_metrics(self):
        c = MetricsCollector()
        metrics = c.parse_driver_logs("No metrics here", "bronze-verify")
        assert metrics.input_size_gb == 0.0
        assert metrics.elapsed_seconds == 0.0


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

    def test_load_nonexistent(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        assert storage.load_run("nonexistent") is None

    def test_list_runs(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()

        for i in range(3):
            m = PipelineMetrics(
                run_id=f"run-{i:03d}",
                deployment_name="test",
                start_time=now,
                success=True,
                jobs=[],
            )
            storage.save_run(m)

        runs = storage.list_runs()
        assert len(runs) == 3
        # Should be sorted (most recent first by filename)
        run_ids = [r["run_id"] for r in runs]
        assert "run-000" in run_ids
        assert "run-002" in run_ids

    def test_get_latest_run(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()

        for i in range(3):
            m = PipelineMetrics(
                run_id=f"run-{i:03d}",
                deployment_name="test",
                start_time=now,
                success=i == 2,
            )
            storage.save_run(m)

        latest = storage.get_latest_run()
        assert latest is not None

    def test_get_latest_run_empty(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        assert storage.get_latest_run() is None

    def test_save_and_load_with_benchmark(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")

        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="bench-roundtrip",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=100),
            total_elapsed_seconds=100.0,
            success=True,
            benchmark=BenchmarkMetrics(
                mode="standard",
                cache="hot",
                scale=102,
                qph=820.4,
                total_seconds=43.88,
                queries=[
                    {
                        "name": "Q1_full_aggregation_scan",
                        "class": "scan",
                        "elapsed_seconds": 4.23,
                        "rows_returned": 1,
                        "success": True,
                    }
                ],
                iterations=1,
            ),
        )

        storage.save_run(metrics)
        loaded = storage.load_run("bench-roundtrip")
        assert loaded is not None
        assert loaded.benchmark is not None
        assert loaded.benchmark.qph == 820.4
        assert loaded.benchmark.cache == "hot"
        assert len(loaded.benchmark.queries) == 1

    def test_save_and_load_without_benchmark(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")

        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="no-bench",
            deployment_name="test",
            start_time=now,
            success=True,
        )

        storage.save_run(metrics)
        loaded = storage.load_run("no-bench")
        assert loaded is not None
        assert loaded.benchmark is None

    def test_export_csv(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()

        m = PipelineMetrics(
            run_id="csv-test",
            deployment_name="test",
            start_time=now,
            success=True,
        )
        storage.save_run(m)

        csv_path = storage.export_csv(tmp_path / "export.csv")
        assert csv_path.exists()

        content = csv_path.read_text()
        assert "csv-test" in content
        assert "run_id" in content  # header


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

    def test_generate_report(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        storage = MetricsStorage(metrics_dir)
        storage.save_run(self._make_sample_metrics())

        generator = ReportGenerator(
            metrics_dir=metrics_dir,
            output_dir=output_dir,
        )

        report_path = generator.generate_report()
        assert report_path.exists()
        assert report_path.suffix == ".html"

        html = report_path.read_text()
        assert "Lakebench Benchmark Report" in html
        assert "test-deploy" in html

    def test_report_contains_jobs_table(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        storage = MetricsStorage(metrics_dir)
        storage.save_run(self._make_sample_metrics())

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Job Performance" in html
        assert "lakebench-bronze-verify" in html
        assert "lakebench-silver-build" in html

    def test_report_contains_queries_table(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        storage = MetricsStorage(metrics_dir)
        storage.save_run(self._make_sample_metrics())

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Query Performance" in html
        assert "rfm" in html
        assert "revenue" in html
        assert "3.42s" in html

    def test_report_no_queries_section_when_empty(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        # Metrics with no queries
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="no-queries",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=60),
            total_elapsed_seconds=60.0,
            success=True,
        )

        storage = MetricsStorage(metrics_dir)
        storage.save_run(metrics)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        # Query Performance section should NOT appear
        assert "Query Performance" not in html

    def test_generate_report_nonexistent_run(self, tmp_path):
        generator = ReportGenerator(
            metrics_dir=tmp_path / "metrics",
            output_dir=tmp_path / "reports",
        )

        with pytest.raises(ValueError, match="No runs found"):
            generator.generate_report()

    def test_report_contains_benchmark_section(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        metrics = self._make_sample_metrics()
        metrics.benchmark = BenchmarkMetrics(
            mode="standard",
            cache="hot",
            scale=102,
            qph=820.4,
            total_seconds=43.88,
            queries=[
                {
                    "name": "Q1_full_aggregation_scan",
                    "display_name": "Full aggregation scan",
                    "class": "scan",
                    "elapsed_seconds": 4.23,
                    "rows_returned": 1,
                    "success": True,
                },
            ],
        )

        storage = MetricsStorage(metrics_dir)
        storage.save_run(metrics)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Trino Query Benchmark" in html
        assert "820.4" in html
        assert "Q1_full_aggregation_scan" in html
        assert "QpH" in html

    def test_report_no_benchmark_section_when_absent(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        metrics = self._make_sample_metrics()
        # No benchmark attribute set

        storage = MetricsStorage(metrics_dir)
        storage.save_run(metrics)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Trino Query Benchmark" not in html

    def test_generate_report_specific_nonexistent(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir(parents=True)

        generator = ReportGenerator(
            metrics_dir=metrics_dir,
            output_dir=tmp_path / "reports",
        )

        with pytest.raises(ValueError, match="Run not found"):
            generator.generate_report(run_id="nonexistent")


# ---------------------------------------------------------------------------
# StreamingJobMetrics
# ---------------------------------------------------------------------------


class TestStreamingJobMetrics:
    """Tests for StreamingJobMetrics dataclass."""

    def test_basic_streaming_metrics(self):
        m = StreamingJobMetrics(
            job_name="lakebench-bronze-ingest",
            job_type="bronze-ingest",
        )
        assert m.job_name == "lakebench-bronze-ingest"
        assert m.success is False
        assert m.total_batches == 0
        assert m.total_rows_processed == 0
        assert m.micro_batch_duration_ms == 0.0

    def test_streaming_metrics_to_dict(self):
        m = StreamingJobMetrics(
            job_name="lakebench-silver-stream",
            job_type="silver-stream",
            elapsed_seconds=1800.0,
            total_batches=30,
            total_rows_processed=450_000,
            success=True,
        )
        d = m.to_dict()
        assert d["job_name"] == "lakebench-silver-stream"
        assert d["total_batches"] == 30
        assert d["total_rows_processed"] == 450_000
        assert d["success"] is True


# ---------------------------------------------------------------------------
# Streaming log parsing
# ---------------------------------------------------------------------------


class TestStreamingLogParsing:
    """Tests for streaming driver log parsing."""

    def test_parse_streaming_logs_bronze(self):
        c = MetricsCollector()
        logs = """
[lb] 2026-02-01T12:00:30.000Z - Batch 0: writing 150,000 rows to lakehouse.default.bronze_raw
[lb] 2026-02-01T12:00:31.000Z - Batch 0: committed
[lb] 2026-02-01T12:01:00.000Z - Batch 1: writing 175,000 rows to lakehouse.default.bronze_raw
[lb] 2026-02-01T12:01:01.000Z - Batch 1: committed
[lb] 2026-02-01T12:01:30.000Z - Batch 2: empty, skipping
[lb] 2026-02-01T12:02:00.000Z - Batch 3: writing 200,000 rows to lakehouse.default.bronze_raw
[lb] 2026-02-01T12:02:01.000Z - Batch 3: committed
"""
        metrics = c.parse_streaming_logs(logs, "bronze-ingest")
        assert metrics.job_type == "bronze-ingest"
        assert metrics.total_batches == 3  # batches 0, 1, 3 (2 was empty)
        assert metrics.total_rows_processed == 525_000
        assert metrics.batch_size == 175_000  # 525000 // 3

    def test_parse_streaming_logs_silver(self):
        c = MetricsCollector()
        logs = """
[lb] 2026-02-01T12:00:00.000Z - Batch 0: transforming 150,000 rows
[lb] 2026-02-01T12:00:10.000Z - Batch 0: 148,500 rows after transforms (filtered ~1%)
[lb] 2026-02-01T12:00:10.000Z - Batch 0: committed to ice.silver.customer_interactions_enriched
[lb] 2026-02-01T12:01:00.000Z - Batch 1: transforming 200,000 rows
[lb] 2026-02-01T12:01:12.000Z - Batch 1: 198,000 rows after transforms (filtered ~1%)
[lb] 2026-02-01T12:01:12.000Z - Batch 1: committed to ice.silver.customer_interactions_enriched
"""
        metrics = c.parse_streaming_logs(logs, "silver-stream")
        assert metrics.job_type == "silver-stream"
        assert metrics.total_batches == 2
        assert metrics.total_rows_processed == 350_000

    def test_parse_streaming_logs_gold(self):
        c = MetricsCollector()
        logs = """
[lb] 2026-02-01T12:00:00.000Z - Refresh cycle 1 (batch 0)
[lb] 2026-02-01T12:00:01.000Z - Cycle 1: aggregating 150,000 Silver records
[lb] 2026-02-01T12:00:08.000Z - Cycle 1: generated 30 daily KPI records
[lb] 2026-02-01T12:00:08.000Z - Cycle 1: refreshed ice.gold.customer_executive_dashboard in 8.2s (30 KPI records)
[lb] 2026-02-01T12:05:00.000Z - Refresh cycle 2 (batch 1)
[lb] 2026-02-01T12:05:01.000Z - Cycle 2: aggregating 300,000 Silver records
[lb] 2026-02-01T12:05:14.000Z - Cycle 2: generated 30 daily KPI records
[lb] 2026-02-01T12:05:14.000Z - Cycle 2: refreshed ice.gold.customer_executive_dashboard in 13.5s (30 KPI records)
"""
        metrics = c.parse_streaming_logs(logs, "gold-refresh")
        assert metrics.job_type == "gold-refresh"
        assert metrics.total_batches == 2  # cycles 1 and 2
        assert metrics.total_rows_processed == 450_000
        # avg batch duration: (8.2 + 13.5) / 2 = 10.85s = 10850ms
        assert metrics.micro_batch_duration_ms == pytest.approx(10850.0, abs=1.0)

    def test_parse_streaming_logs_empty(self):
        c = MetricsCollector()
        metrics = c.parse_streaming_logs("No streaming output here", "bronze-ingest")
        assert metrics.total_batches == 0
        assert metrics.total_rows_processed == 0
        assert metrics.micro_batch_duration_ms == 0.0

    def test_record_streaming(self):
        c = MetricsCollector()
        c.start_run("run-1", "test", {})

        m = StreamingJobMetrics(
            job_name="lakebench-bronze-ingest",
            job_type="bronze-ingest",
            total_batches=5,
            total_rows_processed=100_000,
            success=True,
        )
        c.record_streaming(m)

        assert len(c.current_run.streaming) == 1
        assert c.current_run.streaming[0].total_rows_processed == 100_000

    def test_record_streaming_without_run_is_noop(self):
        c = MetricsCollector()
        m = StreamingJobMetrics(job_name="test", job_type="bronze-ingest")
        c.record_streaming(m)  # should not raise


# ---------------------------------------------------------------------------
# Streaming storage roundtrip
# ---------------------------------------------------------------------------


class TestStreamingStorageRoundtrip:
    """Tests for streaming metrics persistence."""

    def test_save_and_load_with_streaming(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")

        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="streaming-roundtrip",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=1800),
            total_elapsed_seconds=1800.0,
            success=True,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=60,
                    total_rows_processed=900_000,
                    elapsed_seconds=1800.0,
                    success=True,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=30,
                    total_rows_processed=850_000,
                    micro_batch_duration_ms=3500.0,
                    elapsed_seconds=1800.0,
                    success=True,
                ),
            ],
        )

        storage.save_run(metrics)
        loaded = storage.load_run("streaming-roundtrip")

        assert loaded is not None
        assert len(loaded.streaming) == 2
        assert loaded.streaming[0].job_type == "bronze-ingest"
        assert loaded.streaming[0].total_batches == 60
        assert loaded.streaming[0].total_rows_processed == 900_000
        assert loaded.streaming[1].job_type == "silver-stream"
        assert loaded.streaming[1].micro_batch_duration_ms == 3500.0


# ---------------------------------------------------------------------------
# Streaming report generation
# ---------------------------------------------------------------------------


class TestStreamingReportGeneration:
    """Tests for streaming section in HTML reports."""

    def test_report_contains_streaming_section(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="streaming-report",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=1800),
            total_elapsed_seconds=1800.0,
            success=True,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=60,
                    total_rows_processed=900_000,
                    elapsed_seconds=1800.0,
                    throughput_rps=500.0,
                    success=True,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=30,
                    total_rows_processed=850_000,
                    elapsed_seconds=1800.0,
                    throughput_rps=472.2,
                    success=True,
                ),
            ],
        )

        storage = MetricsStorage(metrics_dir)
        storage.save_run(metrics)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Streaming Pipeline" in html
        assert "lakebench-bronze-ingest" in html
        assert "lakebench-silver-stream" in html
        assert "900,000" in html
        assert "rows/s" in html

    def test_report_no_streaming_section_when_empty(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="batch-only",
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
                ),
            ],
        )

        storage = MetricsStorage(metrics_dir)
        storage.save_run(metrics)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Streaming Pipeline" not in html

    def test_report_streaming_freshness_column(self, tmp_path):
        """Streaming table should show Freshness column when data exists."""
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="freshness-report",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=1800),
            total_elapsed_seconds=1800.0,
            success=True,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=5,
                    total_rows_processed=50_000,
                    elapsed_seconds=1800.0,
                    freshness_seconds=45.0,
                    throughput_rps=27.8,
                    success=True,
                ),
            ],
        )

        storage = MetricsStorage(metrics_dir)
        storage.save_run(metrics)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Freshness" in html
        assert "45s" in html

    def test_report_config_shows_scale(self, tmp_path):
        """Config section should show scale, not deprecated target_size."""
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="config-report",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=300),
            total_elapsed_seconds=300.0,
            success=True,
            config_snapshot={
                "name": "test",
                "scale": 100,
                "approx_bronze_gb": 1000.0,
                "processing_pattern": "streaming",
                "s3": {"endpoint": "http://test-s3-endpoint:80"},
                "spark": {
                    "executor": {"instances": 8, "cores": 4, "memory": "48g"},
                },
                "catalog": "hive",
                "table_format": "iceberg",
                "query_engine": "trino",
                "trino": {"worker": {"replicas": 3, "cpu": "4", "memory": "16Gi"}},
                "images": {"datagen": "lakebench/datagen:latest"},
            },
        )

        storage = MetricsStorage(metrics_dir)
        storage.save_run(metrics)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Scale" in html
        assert "100" in html
        assert "Target Size" not in html


# ---------------------------------------------------------------------------
# Phase 1+2: Driver log parsing with [lb] prefix
# ---------------------------------------------------------------------------


class TestDriverLogParsingWithLbPrefix:
    """Tests for parse_driver_logs with realistic [lb]-prefixed output."""

    def test_parse_bronze_verify_logs(self):
        c = MetricsCollector()
        logs = """\
[lb] 2026-02-01T10:00:00.000000 - ============================================================
[lb] 2026-02-01T10:00:00.000000 - Bronze Data Verification
[lb] 2026-02-01T10:00:00.000000 - ============================================================
[lb] 2026-02-01T10:00:00.000000 - Reading from: s3a://lb-bronze/customer/interactions/
[lb] 2026-02-01T10:00:30.000000 - Rows: 5,000,000
[lb] 2026-02-01T10:00:30.000000 - Columns: 21
[lb] 2026-02-01T10:00:45.000000 - Total rows verified: 5,000,000
[lb] 2026-02-01T10:00:45.000000 - Total time: 45.2s (0.8 min)
[lb] 2026-02-01T10:00:45.000000 - === JOB METRICS: bronze-verify ===
[lb] 2026-02-01T10:00:45.000000 - input_size_gb: 9.523
[lb] 2026-02-01T10:00:45.000000 - estimated_rows: 5000000
[lb] 2026-02-01T10:00:45.000000 - output_rows: 5000000
[lb] 2026-02-01T10:00:45.000000 - elapsed_seconds: 45.2
[lb] 2026-02-01T10:00:45.000000 - ========================================
"""
        metrics = c.parse_driver_logs(logs, "bronze-verify")
        assert metrics.input_size_gb == pytest.approx(9.523)
        assert metrics.input_rows == 5_000_000
        assert metrics.output_rows == 5_000_000
        assert metrics.elapsed_seconds == pytest.approx(45.2)
        assert metrics.throughput_gb_per_second > 0

    def test_parse_silver_build_logs(self):
        c = MetricsCollector()
        logs = """\
[lb] 2026-02-01T10:01:00.000000 - ============================================================
[lb] 2026-02-01T10:01:00.000000 - Silver Layer Build
[lb] 2026-02-01T10:01:00.000000 - Duration: 107.3s
[lb] 2026-02-01T10:01:00.000000 - === JOB METRICS: silver-build ===
[lb] 2026-02-01T10:01:00.000000 - input_size_gb: 9.523
[lb] 2026-02-01T10:01:00.000000 - estimated_rows: 5000000
[lb] 2026-02-01T10:01:00.000000 - output_rows: 4850000
[lb] 2026-02-01T10:01:00.000000 - elapsed_seconds: 107.3
[lb] 2026-02-01T10:01:00.000000 - ========================================
"""
        metrics = c.parse_driver_logs(logs, "silver-build")
        assert metrics.input_size_gb == pytest.approx(9.523)
        assert metrics.input_rows == 5_000_000
        assert metrics.output_rows == 4_850_000
        assert metrics.elapsed_seconds == pytest.approx(107.3)

    def test_parse_gold_finalize_logs(self):
        c = MetricsCollector()
        logs = """\
[lb] 2026-02-01T10:03:00.000000 - ============================================================
[lb] 2026-02-01T10:03:00.000000 - Gold Layer Finalization
[lb] 2026-02-01T10:03:00.000000 - Duration: 22.5s
[lb] 2026-02-01T10:03:00.000000 - === JOB METRICS: gold-finalize ===
[lb] 2026-02-01T10:03:00.000000 - input_size_gb: 3.210
[lb] 2026-02-01T10:03:00.000000 - estimated_rows: 4850000
[lb] 2026-02-01T10:03:00.000000 - output_rows: 92
[lb] 2026-02-01T10:03:00.000000 - elapsed_seconds: 22.5
[lb] 2026-02-01T10:03:00.000000 - ========================================
"""
        metrics = c.parse_driver_logs(logs, "gold-finalize")
        assert metrics.input_size_gb == pytest.approx(3.21)
        assert metrics.input_rows == 4_850_000
        assert metrics.output_rows == 92
        assert metrics.elapsed_seconds == pytest.approx(22.5)


# ---------------------------------------------------------------------------
# Phase 1+2: Streaming timing and freshness parsing
# ---------------------------------------------------------------------------


class TestStreamingTimingAndFreshness:
    """Tests for new streaming timing and freshness log patterns."""

    def test_bronze_committed_timing(self):
        """Bronze 'committed in X.Xs' should populate batch_durations."""
        c = MetricsCollector()
        logs = """\
[lb] 2026-02-01T12:00:30.000Z - Batch 0: writing 150,000 rows to lakehouse.default.bronze_raw
[lb] 2026-02-01T12:00:35.000Z - Batch 0: committed in 5.2s
[lb] 2026-02-01T12:01:00.000Z - Batch 1: writing 175,000 rows to lakehouse.default.bronze_raw
[lb] 2026-02-01T12:01:08.000Z - Batch 1: committed in 7.8s
"""
        metrics = c.parse_streaming_logs(logs, "bronze-ingest")
        assert metrics.total_batches == 2
        assert metrics.total_rows_processed == 325_000
        # avg batch duration: (5.2 + 7.8) / 2 = 6.5s = 6500ms
        assert metrics.micro_batch_duration_ms == pytest.approx(6500.0, abs=1.0)

    def test_silver_committed_timing(self):
        """Silver 'committed to <table> in X.Xs' should populate batch_durations."""
        c = MetricsCollector()
        logs = """\
[lb] 2026-02-01T12:00:00.000Z - Batch 0: transforming 150,000 rows
[lb] 2026-02-01T12:00:10.000Z - Batch 0: committed to ice.silver.customer_interactions_enriched in 10.3s
[lb] 2026-02-01T12:01:00.000Z - Batch 1: transforming 200,000 rows
[lb] 2026-02-01T12:01:12.000Z - Batch 1: committed to ice.silver.customer_interactions_enriched in 12.1s
"""
        metrics = c.parse_streaming_logs(logs, "silver-stream")
        assert metrics.total_batches == 2
        assert metrics.total_rows_processed == 350_000
        # avg: (10.3 + 12.1) / 2 = 11.2s = 11200ms
        assert metrics.micro_batch_duration_ms == pytest.approx(11200.0, abs=1.0)

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
        # freshness: (30 + 60) / 2 = 45
        assert metrics.freshness_seconds == pytest.approx(45.0)
        # batch durations from "refreshed ... in Xs"
        assert metrics.micro_batch_duration_ms == pytest.approx(10850.0, abs=1.0)

    def test_no_freshness_when_absent(self):
        """Freshness stays 0 when no freshness lines present."""
        c = MetricsCollector()
        logs = """\
[lb] 2026-02-01T12:00:01.000Z - Cycle 1: aggregating 150,000 Silver records
[lb] 2026-02-01T12:00:08.000Z - Cycle 1: refreshed ice.gold.dashboard in 8.2s (30 KPI records)
"""
        metrics = c.parse_streaming_logs(logs, "gold-refresh")
        assert metrics.freshness_seconds == 0.0


# ---------------------------------------------------------------------------
# Phase 1: build_config_snapshot
# ---------------------------------------------------------------------------


class TestBuildConfigSnapshot:
    """Tests for build_config_snapshot function."""

    def test_captures_expected_fields(self):
        from lakebench.config import LakebenchConfig

        cfg = LakebenchConfig(
            name="snapshot-test",
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://test-s3-endpoint:80",
                        "access_key": "testkey",
                        "secret_key": "testsecret",
                        "buckets": {
                            "bronze": "lb-bronze",
                            "silver": "lb-silver",
                            "gold": "lb-gold",
                        },
                    },
                    "scratch": {
                        "enabled": True,
                        "storage_class": "px-csi-scratch",
                        "size": "150Gi",
                    },
                },
            },
            architecture={
                "workload": {"datagen": {"scale": 50}},
            },
        )
        snapshot = build_config_snapshot(cfg)

        assert snapshot["name"] == "snapshot-test"
        assert snapshot["scale"] == 50
        assert snapshot["approx_bronze_gb"] > 0
        assert snapshot["processing_pattern"] == "medallion"  # default
        assert snapshot["s3"]["endpoint"] == "http://test-s3-endpoint:80"
        assert snapshot["s3"]["buckets"]["bronze"] == "lb-bronze"
        assert snapshot["scratch"]["enabled"] is True
        assert snapshot["spark"]["executor"]["instances"] == 8  # default
        assert snapshot["spark"]["executor"]["cores"] == 4  # default
        assert snapshot["catalog"] == "hive"  # default
        assert snapshot["table_format"] == "iceberg"  # default
        assert snapshot["query_engine"] == "trino"  # default
        assert snapshot["datagen"]["scale"] == 50
        assert "datagen" in snapshot["images"]
        assert "spark" in snapshot["images"]
        assert snapshot["trino"]["worker"]["replicas"] == 2  # default
        assert snapshot["continuous"]["run_duration"] == 1800  # default
        assert snapshot["continuous"]["max_files_per_trigger"] == 50  # default
        assert snapshot["continuous"]["bronze_target_file_size_mb"] == 512
        assert snapshot["continuous"]["silver_target_file_size_mb"] == 512
        assert snapshot["continuous"]["gold_target_file_size_mb"] == 128

    def test_captures_executor_overrides(self):
        from lakebench.config import LakebenchConfig

        cfg = LakebenchConfig(
            name="override-test",
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://test:80",
                        "access_key": "ak",
                        "secret_key": "sk",
                    },
                },
                "compute": {
                    "spark": {
                        "silver_executors": 20,
                        "gold_refresh_executors": 5,
                    },
                },
            },
        )
        snapshot = build_config_snapshot(cfg)

        assert snapshot["spark"]["executor_overrides"]["silver"] == 20
        assert snapshot["spark"]["executor_overrides"]["gold_refresh"] == 5
        assert snapshot["spark"]["executor_overrides"]["bronze"] is None


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
            c.record_actual_sizes(mock_s3, "lb-bronze", "lb-silver", "lb-gold")

        # Should have 3 warnings, one per bucket
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 3
        assert "lb-bronze" in warnings[0].message

    def test_no_warning_on_successful_measurement(self, caplog):
        c = MetricsCollector()
        c.start_run("run-1", "test", {})

        mock_s3 = MagicMock()
        info = MagicMock()
        info.size_bytes = 1024 * 1024 * 1024  # 1 GB
        info.object_count = 100
        mock_s3.get_bucket_size.return_value = info

        with caplog.at_level(logging.WARNING, logger="lakebench.metrics.collector"):
            c.record_actual_sizes(mock_s3, "lb-bronze", "lb-silver", "lb-gold")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 0

        # Check sizes were recorded
        assert c.current_run.bronze_size_gb == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Phase 1: throughput_rps computation
# ---------------------------------------------------------------------------


class TestThroughputRpsComputation:
    """Tests for throughput_rps computation logic."""

    def test_throughput_rps_computed_when_data_exists(self):
        """throughput_rps should be total_rows / elapsed_seconds."""
        m = StreamingJobMetrics(
            job_name="lakebench-bronze-ingest",
            job_type="bronze-ingest",
            total_rows_processed=900_000,
            elapsed_seconds=1800.0,
        )
        # Simulate the computation from cli.py
        if m.elapsed_seconds > 0 and m.total_rows_processed > 0:
            m.throughput_rps = m.total_rows_processed / m.elapsed_seconds

        assert m.throughput_rps == pytest.approx(500.0)

    def test_throughput_rps_zero_when_no_rows(self):
        m = StreamingJobMetrics(
            job_name="test",
            job_type="bronze-ingest",
            total_rows_processed=0,
            elapsed_seconds=1800.0,
        )
        # No computation when no rows
        assert m.throughput_rps == 0.0

    def test_throughput_rps_zero_when_no_elapsed(self):
        m = StreamingJobMetrics(
            job_name="test",
            job_type="bronze-ingest",
            total_rows_processed=1000,
            elapsed_seconds=0.0,
        )
        assert m.throughput_rps == 0.0


# ---------------------------------------------------------------------------
# Phase 3: Cross-run comparison
# ---------------------------------------------------------------------------


class TestListRunsEnriched:
    """Tests for enriched list_runs() fields."""

    def test_list_runs_includes_new_fields(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="enriched-001",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=10.5,
            silver_size_gb=3.2,
            gold_size_gb=0.01,
            config_snapshot={"scale": 100, "processing_pattern": "streaming"},
            benchmark=BenchmarkMetrics(
                mode="standard",
                cache="hot",
                scale=100,
                qph=820.4,
                total_seconds=43.88,
            ),
            streaming=[
                StreamingJobMetrics(job_name="s1", job_type="bronze-ingest"),
                StreamingJobMetrics(job_name="s2", job_type="silver-stream"),
            ],
        )
        storage.save_run(metrics)

        runs = storage.list_runs()
        assert len(runs) == 1
        r = runs[0]
        assert r["scale"] == 100
        assert r["processing_pattern"] == "streaming"
        assert r["qph"] == 820.4
        assert r["bronze_size_gb"] == pytest.approx(10.5)
        assert r["silver_size_gb"] == pytest.approx(3.2)
        assert r["gold_size_gb"] == pytest.approx(0.01)
        assert r["streaming_count"] == 2

    def test_list_runs_missing_config_fields(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="old-format",
            deployment_name="test",
            start_time=now,
            success=True,
            config_snapshot={"continuous": True, "duration": 1800},
        )
        storage.save_run(metrics)

        runs = storage.list_runs()
        r = runs[0]
        assert r["scale"] is None
        assert r["processing_pattern"] is None
        assert r["qph"] is None
        assert r["streaming_count"] == 0

    def test_list_runs_no_benchmark(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="no-bench",
            deployment_name="test",
            start_time=now,
            success=True,
        )
        storage.save_run(metrics)

        runs = storage.list_runs()
        assert runs[0]["qph"] is None


class TestExportCsvEnriched:
    """Tests for enriched export_csv() with per-job columns."""

    def test_csv_has_enriched_headers(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="csv-enriched",
            deployment_name="test",
            start_time=now,
            success=True,
            config_snapshot={"scale": 50, "processing_pattern": "medallion"},
        )
        storage.save_run(metrics)

        csv_path = storage.export_csv(tmp_path / "export.csv")
        header = csv_path.read_text().split("\n")[0]

        assert "scale" in header
        assert "processing_pattern" in header
        assert "qph" in header
        assert "bronze_verify_seconds" in header
        assert "silver_build_seconds" in header
        assert "gold_finalize_seconds" in header
        assert "bronze_ingest_total_batches" in header
        assert "gold_refresh_freshness_seconds" in header

    def test_csv_batch_job_columns(self, tmp_path):
        import csv as csv_mod

        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="csv-batch",
            deployment_name="test",
            start_time=now,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    elapsed_seconds=45.2,
                    input_size_gb=9.5,
                    output_rows=5_000_000,
                    success=True,
                ),
                JobMetrics(
                    job_name="lakebench-silver-build",
                    job_type="silver-build",
                    elapsed_seconds=107.3,
                    input_size_gb=9.5,
                    output_rows=4_850_000,
                    success=True,
                ),
            ],
        )
        storage.save_run(metrics)

        csv_path = storage.export_csv(tmp_path / "export.csv")
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            row = next(reader)

        assert row["bronze_verify_seconds"] == "45.2"
        assert row["silver_build_seconds"] == "107.3"
        assert row["bronze_verify_output_rows"] == "5000000"
        assert row["gold_finalize_seconds"] == ""

    def test_csv_streaming_job_columns(self, tmp_path):
        import csv as csv_mod

        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="csv-stream",
            deployment_name="test",
            start_time=now,
            success=True,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=59,
                    total_rows_processed=47_135_100,
                    throughput_rps=26186.17,
                    freshness_seconds=0.0,
                    success=True,
                ),
            ],
        )
        storage.save_run(metrics)

        csv_path = storage.export_csv(tmp_path / "export.csv")
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            row = next(reader)

        assert row["bronze_ingest_total_batches"] == "59"
        assert row["bronze_ingest_total_rows_processed"] == "47135100"


# ---------------------------------------------------------------------------
# StageMetrics
# ---------------------------------------------------------------------------


class TestStageMetrics:
    """Tests for the universal per-stage measurement dataclass."""

    def test_basic_construction(self):
        s = StageMetrics(stage_name="bronze", stage_type="batch", engine="spark")
        assert s.stage_name == "bronze"
        assert s.elapsed_seconds == 0.0
        assert s.success is False
        assert s.throughput_gb_per_second == 0.0

    def test_to_dict(self):
        now = datetime.now()
        s = StageMetrics(
            stage_name="silver",
            stage_type="batch",
            engine="spark",
            start_time=now,
            end_time=now + timedelta(seconds=100),
            elapsed_seconds=100.0,
            success=True,
            input_size_gb=10.0,
            output_size_gb=3.5,
            input_rows=5_000_000,
            output_rows=4_900_000,
            executor_count=8,
        )
        d = s.to_dict()
        assert d["stage_name"] == "silver"
        assert d["engine"] == "spark"
        assert d["elapsed_seconds"] == 100.0
        assert d["input_size_gb"] == 10.0
        assert d["executor_count"] == 8
        assert "start_time" in d
        assert "end_time" in d

    def test_to_dict_omits_none_times(self):
        s = StageMetrics(stage_name="gold", stage_type="batch", engine="spark")
        d = s.to_dict()
        assert "start_time" not in d
        assert "end_time" not in d

    def test_compute_derived(self):
        s = StageMetrics(
            stage_name="bronze",
            stage_type="batch",
            engine="spark",
            elapsed_seconds=50.0,
            input_size_gb=10.0,
            input_rows=5_000_000,
        )
        s.compute_derived()
        assert s.throughput_gb_per_second == pytest.approx(0.2)
        assert s.throughput_rows_per_second == pytest.approx(100_000.0)

    def test_compute_derived_zero_elapsed(self):
        s = StageMetrics(
            stage_name="bronze",
            stage_type="batch",
            engine="spark",
            elapsed_seconds=0.0,
            input_size_gb=10.0,
        )
        s.compute_derived()
        assert s.throughput_gb_per_second == 0.0

    def test_query_stage(self):
        s = StageMetrics(
            stage_name="query",
            stage_type="query",
            engine="trino",
            elapsed_seconds=44.0,
            queries_executed=10,
            queries_per_hour=818.2,
        )
        d = s.to_dict()
        assert d["queries_per_hour"] == 818.2
        assert d["queries_executed"] == 10


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

    def test_compute_aggregates(self):
        now = datetime.now()
        stages = self._make_stages()
        pb = PipelineBenchmark(
            run_id="agg-test",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
            stages=stages,
            success=True,
        )
        pb.compute_aggregates()

        assert pb.total_elapsed_seconds == 200.0
        assert pb.total_data_processed_gb == pytest.approx(23.5)
        assert pb.pipeline_throughput_gb_per_second == pytest.approx(23.5 / 200.0)
        assert pb.time_to_value_seconds == pytest.approx(200.0)

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

    def test_to_matrix(self):
        now = datetime.now()
        stages = self._make_stages()
        pb = PipelineBenchmark(
            run_id="matrix-test",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
            stages=stages,
            success=True,
        )
        matrix = pb.to_matrix()

        assert "bronze" in matrix
        assert "silver" in matrix
        assert "gold" in matrix
        assert matrix["bronze"]["elapsed_seconds"] == 50.0
        assert matrix["silver"]["input_size_gb"] == 10.0
        assert matrix["gold"]["output_size_gb"] == 0.01

    def test_compute_efficiency(self):
        """compute_efficiency = total_gb / total_core_hours."""
        now = datetime.now()
        stages = [
            StageMetrics(
                stage_name="bronze",
                stage_type="batch",
                engine="spark",
                start_time=now,
                end_time=now + timedelta(seconds=3600),
                elapsed_seconds=3600.0,
                success=True,
                input_size_gb=100.0,
                executor_count=4,
                executor_cores=2,
            ),
        ]
        pb = PipelineBenchmark(
            run_id="eff-test",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
            stages=stages,
            success=True,
        )
        pb.compute_aggregates()
        # core_hours = 4 * 2 * 3600 / 3600 = 8
        # efficiency = 100 / 8 = 12.5
        assert pb.compute_efficiency_gb_per_core_hour == pytest.approx(12.5)

    def test_scale_verified_ratio(self):
        """scale_verified_ratio = total_gb / expected_gb from config."""
        now = datetime.now()
        stages = [
            StageMetrics(
                stage_name="bronze",
                stage_type="batch",
                engine="spark",
                start_time=now,
                end_time=now + timedelta(seconds=100),
                elapsed_seconds=100.0,
                success=True,
                input_size_gb=95.0,
            ),
        ]
        pb = PipelineBenchmark(
            run_id="scale-test",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
            stages=stages,
            success=True,
            config_snapshot={"approx_bronze_gb": 100.0},
        )
        pb.compute_aggregates()
        assert pb.scale_verified_ratio == pytest.approx(0.95)

    def test_to_dict(self):
        now = datetime.now()
        stages = self._make_stages()
        pb = PipelineBenchmark(
            run_id="dict-test",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
            stages=stages,
            success=True,
        )
        pb.compute_aggregates()
        d = pb.to_dict()

        assert d["run_id"] == "dict-test"
        assert d["pipeline_mode"] == "batch"
        assert "scorecard" in d
        assert "scores" in d  # backward compat alias
        assert d["scorecard"]["total_elapsed_seconds"] == 200.0
        assert "compute_efficiency_gb_per_core_hour" in d["scorecard"]
        assert "scale_verified_ratio" in d["scorecard"]
        assert len(d["stages"]) == 3
        assert "stage_matrix" in d
        assert d["stage_matrix"]["bronze"]["engine"] == "spark"

    def test_empty_pipeline(self):
        now = datetime.now()
        pb = PipelineBenchmark(
            run_id="empty",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
        )
        pb.compute_aggregates()

        assert pb.total_elapsed_seconds == 0.0
        assert pb.time_to_value_seconds == 0.0
        assert pb.to_matrix() == {}

    def test_with_query_benchmark(self):
        now = datetime.now()
        qb = BenchmarkMetrics(
            mode="power",
            cache="hot",
            scale=100,
            qph=820.0,
            total_seconds=44.0,
        )
        pb = PipelineBenchmark(
            run_id="qb-test",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
            query_benchmark=qb,
            success=True,
        )
        d = pb.to_dict()
        assert "query_benchmark" in d
        assert d["query_benchmark"]["qph"] == 820.0


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

    def test_batch_jobs_converted(self):
        run = self._make_run()
        pb = build_pipeline_benchmark(run)

        assert len(pb.stages) == 4  # bronze, silver, gold, query
        assert pb.stages[0].stage_name == "bronze"
        assert pb.stages[0].engine == "spark"
        assert pb.stages[0].executor_count == 4
        assert pb.stages[0].executor_cores == 2
        assert pb.stages[0].executor_memory_gb == 4.0
        assert pb.stages[1].stage_name == "silver"
        assert pb.stages[1].executor_count == 12
        assert pb.stages[1].executor_cores == 4
        assert pb.stages[1].executor_memory_gb == 48.0
        assert pb.stages[2].stage_name == "gold"
        assert pb.stages[2].executor_cores == 4
        assert pb.stages[3].stage_name == "query"
        assert pb.stages[3].engine == "trino"

    def test_query_stage_from_benchmark(self):
        run = self._make_run()
        pb = build_pipeline_benchmark(run)

        query_stage = pb.stages[3]
        assert query_stage.stage_type == "query"
        assert query_stage.queries_per_hour == 820.0
        assert query_stage.elapsed_seconds == 40.0
        assert query_stage.queries_executed == 1

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

    def test_executor_cores_memory_propagation(self):
        """executor_cores and executor_memory_gb propagate to pipeline stages."""
        run = self._make_run()
        pb = build_pipeline_benchmark(run)

        bronze = pb.stages[0]
        assert bronze.executor_cores == 2
        assert bronze.executor_memory_gb == 4.0

        silver = pb.stages[1]
        assert silver.executor_cores == 4
        assert silver.executor_memory_gb == 48.0

        # stage_matrix includes the new fields
        matrix = pb.to_matrix()
        assert matrix["bronze"]["executor_cores"] == 2
        assert matrix["bronze"]["executor_memory_gb"] == 4.0
        assert matrix["silver"]["executor_cores"] == 4
        assert matrix["silver"]["executor_memory_gb"] == 48.0

    def test_with_datagen(self):
        run = self._make_run()
        pb = build_pipeline_benchmark(
            run,
            datagen_elapsed=600.0,
            datagen_output_gb=10.0,
            datagen_output_rows=5_000_000,
        )

        assert len(pb.stages) == 5  # datagen + bronze + silver + gold + query
        assert pb.stages[0].stage_name == "datagen"
        assert pb.stages[0].elapsed_seconds == 600.0
        assert pb.stages[0].output_size_gb == 10.0

    def test_no_datagen_when_zero(self):
        run = self._make_run()
        pb = build_pipeline_benchmark(run, datagen_elapsed=0.0)

        assert pb.stages[0].stage_name != "datagen"

    def test_pipeline_mode_detection(self):
        run = self._make_run()
        pb = build_pipeline_benchmark(run)
        assert pb.pipeline_mode == "batch"

    def test_streaming_mode_detection(self):
        now = datetime.now()
        run = PipelineMetrics(
            run_id="stream-mode",
            deployment_name="test",
            start_time=now,
            success=True,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=10,
                    total_rows_processed=100_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=55.6,
                    micro_batch_duration_ms=450.0,
                    freshness_seconds=12.5,
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)
        assert pb.pipeline_mode == "continuous"
        assert len(pb.stages) == 1
        assert pb.stages[0].stage_name == "bronze"
        assert pb.stages[0].stage_type == "streaming"
        # Continuous scores are computed from streaming data
        assert pb.data_freshness_seconds == 12.5
        # sustained = bronze input_rows / run_duration = 100_000 / 1800
        assert pb.sustained_throughput_rps == pytest.approx(100_000 / 1800.0)
        assert pb.stage_latency_profile == [450.0, 0.0, 0.0]
        assert pb.total_rows_processed == 100_000
        # Batch scores remain zero for continuous
        assert pb.time_to_value_seconds == 0.0
        assert pb.total_data_processed_gb == 0.0
        assert pb.pipeline_throughput_gb_per_second == 0.0

    def test_aggregates_computed(self):
        run = self._make_run()
        pb = build_pipeline_benchmark(run)

        assert pb.total_elapsed_seconds > 0
        assert pb.time_to_value_seconds > 0
        assert pb.success is True

    def test_empty_run(self):
        now = datetime.now()
        run = PipelineMetrics(
            run_id="empty",
            deployment_name="test",
            start_time=now,
            success=True,
        )
        pb = build_pipeline_benchmark(run)

        assert len(pb.stages) == 0
        assert pb.total_elapsed_seconds == 0.0

    def test_query_benchmark_preserved(self):
        run = self._make_run()
        pb = build_pipeline_benchmark(run)

        assert pb.query_benchmark is not None
        assert pb.query_benchmark.qph == 820.0


# ---------------------------------------------------------------------------
# Pipeline benchmark storage roundtrip
# ---------------------------------------------------------------------------


class TestPipelineBenchmarkStorageRoundtrip:
    """Tests for pipeline benchmark save/load through MetricsStorage."""

    def test_roundtrip(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()

        run = PipelineMetrics(
            run_id="pb-roundtrip",
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
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                    executor_count=4,
                    executor_cores=2,
                    executor_memory_gb=4.0,
                    cpu_seconds_requested=400.0,
                    memory_gb_requested=24.0,
                ),
            ],
            benchmark=BenchmarkMetrics(
                mode="power",
                cache="hot",
                scale=100,
                qph=820.0,
                total_seconds=40.0,
            ),
        )
        pb = build_pipeline_benchmark(run)
        run.pipeline_benchmark = pb

        storage.save_run(run)
        loaded = storage.load_run("pb-roundtrip")

        assert loaded is not None
        assert loaded.pipeline_benchmark is not None
        lpb = loaded.pipeline_benchmark
        assert lpb.run_id == "pb-roundtrip"
        assert lpb.pipeline_mode == "batch"
        assert len(lpb.stages) == 2  # bronze + query
        assert lpb.stages[0].stage_name == "bronze"
        assert lpb.stages[0].executor_count == 4
        assert lpb.stages[0].executor_cores == 2
        assert lpb.stages[0].executor_memory_gb == 4.0
        assert lpb.stages[1].stage_name == "query"
        assert lpb.stages[1].queries_per_hour == 820.0
        assert lpb.total_elapsed_seconds > 0
        assert lpb.query_benchmark is not None
        assert lpb.query_benchmark.qph == 820.0

    def test_backward_compat_no_pipeline_benchmark(self, tmp_path):
        """Old runs without pipeline_benchmark load without error."""
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()

        run = PipelineMetrics(
            run_id="old-run",
            deployment_name="test",
            start_time=now,
            success=True,
        )
        storage.save_run(run)
        loaded = storage.load_run("old-run")

        assert loaded is not None
        assert loaded.pipeline_benchmark is None

    def test_list_runs_includes_pipeline_scores(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()

        run = PipelineMetrics(
            run_id="pb-list",
            deployment_name="test",
            start_time=now,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)
        run.pipeline_benchmark = pb
        storage.save_run(run)

        runs = storage.list_runs()
        assert len(runs) == 1
        assert runs[0]["time_to_value_seconds"] is not None or runs[0]["time_to_value_seconds"] == 0

    def test_csv_includes_pipeline_benchmark_columns(self, tmp_path):
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()

        run = PipelineMetrics(
            run_id="pb-csv",
            deployment_name="test",
            start_time=now,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)
        run.pipeline_benchmark = pb
        storage.save_run(run)

        csv_path = storage.export_csv(tmp_path / "export.csv")
        header = csv_path.read_text().split("\n")[0]

        assert "time_to_value_seconds" in header
        assert "pipeline_throughput_gb_per_second" in header
        assert "pb_bronze_seconds" in header
        assert "pb_silver_seconds" in header
        assert "pb_gold_seconds" in header
        assert "pb_query_seconds" in header

    def test_job_resource_fields_roundtrip(self, tmp_path):
        """New resource fields survive save/load."""
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()

        run = PipelineMetrics(
            run_id="res-roundtrip",
            deployment_name="test",
            start_time=now,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-silver-build",
                    job_type="silver-build",
                    elapsed_seconds=200.0,
                    success=True,
                    executor_count=8,
                    executor_cores=4,
                    executor_memory_gb=48.0,
                    cpu_seconds_requested=6400.0,
                    memory_gb_requested=480.0,
                ),
            ],
        )
        storage.save_run(run)
        loaded = storage.load_run("res-roundtrip")

        assert loaded is not None
        j = loaded.jobs[0]
        assert j.executor_count == 8
        assert j.executor_cores == 4
        assert j.executor_memory_gb == 48.0
        assert j.cpu_seconds_requested == 6400.0
        assert j.memory_gb_requested == 480.0

    def test_backward_compat_old_field_names(self, tmp_path):
        """Old JSON with total_cpu_time_seconds/peak_memory_gb loads into new fields."""
        import json

        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()
        old_json = {
            "run_id": "old-fields",
            "deployment_name": "test",
            "start_time": now.isoformat() if (now := datetime.now()) else "",
            "success": True,
            "total_elapsed_seconds": 60.0,
            "bronze_size_gb": 0,
            "silver_size_gb": 0,
            "gold_size_gb": 0,
            "jobs": [
                {
                    "job_name": "lakebench-bronze-verify",
                    "job_type": "bronze-verify",
                    "elapsed_seconds": 50.0,
                    "success": True,
                    "total_cpu_time_seconds": 123.4,
                    "peak_memory_gb": 56.7,
                }
            ],
            "queries": [],
            "streaming": [],
        }
        with open(metrics_dir / "run-old-fields.json", "w") as f:
            json.dump(old_json, f)

        storage = MetricsStorage(metrics_dir)
        loaded = storage.load_run("old-fields")

        assert loaded is not None
        j = loaded.jobs[0]
        assert j.cpu_seconds_requested == 123.4
        assert j.memory_gb_requested == 56.7


# ---------------------------------------------------------------------------
# Pipeline benchmark report generation
# ---------------------------------------------------------------------------


class TestPipelineBenchmarkReport:
    """Tests for pipeline benchmark section in HTML reports."""

    def test_report_contains_pipeline_benchmark(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        now = datetime.now()
        run = PipelineMetrics(
            run_id="pb-report",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=300),
            total_elapsed_seconds=300.0,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                ),
                JobMetrics(
                    job_name="lakebench-silver-build",
                    job_type="silver-build",
                    elapsed_seconds=100.0,
                    success=True,
                    input_size_gb=10.0,
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)
        run.pipeline_benchmark = pb

        storage = MetricsStorage(metrics_dir)
        storage.save_run(run)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Pipeline Benchmark" in html
        assert "Time to Value" in html
        assert "bronze" in html
        assert "silver" in html

    def test_report_no_pipeline_benchmark_when_absent(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        now = datetime.now()
        run = PipelineMetrics(
            run_id="no-pb",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=60),
            total_elapsed_seconds=60.0,
            success=True,
        )

        storage = MetricsStorage(metrics_dir)
        storage.save_run(run)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Pipeline Benchmark" not in html


# ---------------------------------------------------------------------------
# Continuous pipeline scoring
# ---------------------------------------------------------------------------


class TestContinuousPipelineScoring:
    """Tests for continuous (streaming) pipeline scoring."""

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

    def test_continuous_scores_computed(self):
        """Continuous scores are computed from streaming stage data."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)

        assert pb.pipeline_mode == "continuous"
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

        # total_elapsed_seconds is universal
        assert pb.total_elapsed_seconds == pytest.approx(5400.0)

    def test_continuous_batch_scores_zero(self):
        """Batch-oriented scores remain zero for a continuous pipeline."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)

        assert pb.time_to_value_seconds == 0.0
        assert pb.total_data_processed_gb == 0.0
        assert pb.pipeline_throughput_gb_per_second == 0.0

    def test_batch_continuous_scores_zero(self):
        """Continuous scores remain zero for a batch pipeline."""
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
        assert pb.data_freshness_seconds == 0.0
        assert pb.sustained_throughput_rps == 0.0
        assert pb.stage_latency_profile == []
        assert pb.total_rows_processed == 0
        # Batch scores are populated
        assert pb.time_to_value_seconds > 0

    def test_continuous_to_dict_scores(self):
        """to_dict() outputs continuous score keys for continuous mode."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)
        d = pb.to_dict()

        scores = d["scores"]
        assert "data_freshness_seconds" in scores
        assert "sustained_throughput_rps" in scores
        assert "stage_latency_profile" in scores
        assert "ingestion_completeness_ratio" in scores
        assert "pipeline_saturated" in scores
        assert "total_rows_processed" in scores
        assert "total_elapsed_seconds" in scores
        assert "compute_efficiency_gb_per_core_hour" in scores
        # Batch keys should NOT appear
        assert "time_to_value_seconds" not in scores
        assert "total_data_processed_gb" not in scores
        assert "pipeline_throughput_gb_per_second" not in scores

        # Values match computed scores
        assert scores["data_freshness_seconds"] == pytest.approx(15.0)
        assert scores["total_rows_processed"] == 500_000 + 480_000 + 450_000

    def test_batch_to_dict_scores(self):
        """to_dict() outputs batch score keys for batch mode."""
        now = datetime.now()
        run = PipelineMetrics(
            run_id="batch-dict",
            deployment_name="test",
            start_time=now,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)
        d = pb.to_dict()

        scores = d["scores"]
        assert "time_to_value_seconds" in scores
        assert "total_data_processed_gb" in scores
        assert "pipeline_throughput_gb_per_second" in scores
        # Continuous keys should NOT appear
        assert "data_freshness_seconds" not in scores
        assert "sustained_throughput_rps" not in scores

    def test_continuous_storage_roundtrip(self, tmp_path):
        """Continuous pipeline benchmark survives save/load."""
        storage = MetricsStorage(tmp_path / "metrics")
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)
        run.pipeline_benchmark = pb
        storage.save_run(run)

        loaded = storage.load_run("cont-test")
        assert loaded is not None
        assert loaded.pipeline_benchmark is not None

        lpb = loaded.pipeline_benchmark
        assert lpb.pipeline_mode == "continuous"
        assert lpb.data_freshness_seconds == 15.0
        assert lpb.sustained_throughput_rps == pytest.approx(500_000 / 1800.0, rel=1e-2)
        assert lpb.stage_latency_profile == [200.0, 350.0, 500.0]
        assert lpb.total_rows_processed == 1_430_000
        assert lpb.total_elapsed_seconds == pytest.approx(5400.0)
        assert len(lpb.stages) == 3

    def test_continuous_report_cards(self, tmp_path):
        """Report contains continuous score cards for streaming pipeline."""
        metrics_dir = tmp_path / "metrics"
        output_dir = tmp_path / "reports"

        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)
        run.pipeline_benchmark = pb

        storage = MetricsStorage(metrics_dir)
        storage.save_run(run)

        generator = ReportGenerator(metrics_dir=metrics_dir, output_dir=output_dir)
        report_path = generator.generate_report()
        html = report_path.read_text()

        assert "Data Freshness" in html
        assert "Sustained Throughput" in html
        assert "Stage Latency" in html
        assert "continuous pipeline" in html
        # Batch cards should NOT appear
        assert "Time to Value" not in html

    def test_streaming_input_size_gb_backfill(self):
        """Streaming stages get input_size_gb from measured S3 bucket sizes."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)

        bronze = next(s for s in pb.stages if s.stage_name == "bronze")
        silver = next(s for s in pb.stages if s.stage_name == "silver")
        gold = next(s for s in pb.stages if s.stage_name == "gold")

        # Bronze reads from landing zone (bronze bucket)
        assert bronze.input_size_gb == pytest.approx(10.0)
        # Silver reads from bronze Iceberg table (bronze bucket)
        assert silver.input_size_gb == pytest.approx(10.0)
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

    def test_ingestion_completeness_ratio(self):
        """ingestion_completeness_ratio computed when datagen_output_rows provided."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run, datagen_output_rows=600_000)

        # bronze processed 500_000 out of 600_000 datagen rows
        assert pb.ingestion_completeness_ratio == pytest.approx(500_000 / 600_000, rel=1e-3)
        assert pb.pipeline_saturated is True  # 0.833 < 0.95

    def test_ingestion_completeness_not_saturated(self):
        """pipeline_saturated=False when completeness >= 0.95."""
        run = self._make_streaming_run()
        # 500_000 bronze rows / 500_000 datagen rows = 1.0 completeness
        pb = build_pipeline_benchmark(run, datagen_output_rows=500_000)

        assert pb.ingestion_completeness_ratio == pytest.approx(1.0)
        assert pb.pipeline_saturated is False
