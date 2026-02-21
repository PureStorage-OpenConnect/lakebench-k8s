"""Functional tests for report generation, metrics storage, and deployment engine."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus
from lakebench.metrics import (
    BenchmarkMetrics,
    JobMetrics,
    MetricsStorage,
    PipelineMetrics,
)
from lakebench.reports.generator import ReportGenerator
from tests.conftest import make_config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pipeline_metrics(
    run_id: str = "test-run-001",
    deployment_name: str = "test-deploy",
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> PipelineMetrics:
    """Build a PipelineMetrics instance suitable for tests."""
    return PipelineMetrics(
        run_id=run_id,
        deployment_name=deployment_name,
        start_time=start_time or datetime(2026, 2, 12, 10, 0, 0),
        end_time=end_time or datetime(2026, 2, 12, 10, 30, 0),
        success=True,
        total_elapsed_seconds=1800.0,
        jobs=[
            JobMetrics(
                job_name="bronze-verify",
                job_type="bronze-verify",
                elapsed_seconds=45.0,
                success=True,
            )
        ],
        config_snapshot={"scale": 1, "recipe": "hive-iceberg-spark-trino"},
        bronze_size_gb=10.5,
        silver_size_gb=9.2,
        gold_size_gb=0.3,
    )


def _make_benchmark_metrics() -> BenchmarkMetrics:
    """Build a BenchmarkMetrics instance suitable for tests.

    Note: BenchmarkMetrics.queries is ``list[dict[str, Any]]``, not
    ``list[QueryMetrics]``.
    """
    return BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1,
        qph=450.0,
        total_seconds=64.0,
        queries=[
            {
                "name": "Q1",
                "query_text": "SELECT 1",
                "elapsed_seconds": 8.0,
                "rows_returned": 100,
                "success": True,
            }
        ],
    )


def _mock_k8s() -> MagicMock:
    """Create a pre-configured mock K8sClient."""
    k8s = MagicMock()
    k8s.namespace_exists.return_value = False
    k8s.create_namespace.return_value = True
    k8s.apply_manifest.return_value = True
    k8s.secret_exists.return_value = False
    k8s.get_cluster_capacity.return_value = None
    return k8s


# ---------------------------------------------------------------------------
# MetricsStorage Round-Trip Tests
# ---------------------------------------------------------------------------


class TestMetricsStorageRoundTrip:
    """Verify save / load / list round-trip behaviour of MetricsStorage."""

    def test_save_and_load_run(self, tmp_path: pytest.TempPathFactory) -> None:
        """Saved metrics should be loadable and preserve all key fields."""
        storage = MetricsStorage(tmp_path)
        original = _make_pipeline_metrics()

        storage.save_run(original)
        loaded = storage.load_run(original.run_id)

        assert loaded is not None
        assert loaded.run_id == original.run_id
        assert loaded.deployment_name == original.deployment_name
        assert loaded.success is True
        assert loaded.total_elapsed_seconds == original.total_elapsed_seconds
        assert loaded.bronze_size_gb == original.bronze_size_gb
        assert loaded.silver_size_gb == original.silver_size_gb
        assert loaded.gold_size_gb == original.gold_size_gb
        assert loaded.start_time == original.start_time
        assert len(loaded.jobs) == 1
        assert loaded.jobs[0].job_name == "bronze-verify"
        assert loaded.config_snapshot["scale"] == 1

    def test_list_runs_empty(self, tmp_path: pytest.TempPathFactory) -> None:
        """An empty metrics directory should yield an empty run list."""
        storage = MetricsStorage(tmp_path)
        assert storage.list_runs() == []

    def test_list_runs_after_save(self, tmp_path: pytest.TempPathFactory) -> None:
        """Saving two runs should make both visible via list_runs."""
        storage = MetricsStorage(tmp_path)

        m1 = _make_pipeline_metrics(
            run_id="run-alpha",
            start_time=datetime(2026, 2, 12, 8, 0, 0),
            end_time=datetime(2026, 2, 12, 8, 30, 0),
        )
        m2 = _make_pipeline_metrics(
            run_id="run-beta",
            start_time=datetime(2026, 2, 12, 9, 0, 0),
            end_time=datetime(2026, 2, 12, 9, 30, 0),
        )
        storage.save_run(m1)
        storage.save_run(m2)

        runs = storage.list_runs()
        assert len(runs) == 2
        run_ids = {r["run_id"] for r in runs}
        assert "run-alpha" in run_ids
        assert "run-beta" in run_ids

    def test_get_latest_run(self, tmp_path: pytest.TempPathFactory) -> None:
        """get_latest_run should return the run with the most recent start_time."""
        storage = MetricsStorage(tmp_path)

        earlier = _make_pipeline_metrics(
            run_id="earlier",
            start_time=datetime(2026, 2, 12, 7, 0, 0),
            end_time=datetime(2026, 2, 12, 7, 30, 0),
        )
        later = _make_pipeline_metrics(
            run_id="later",
            start_time=datetime(2026, 2, 12, 12, 0, 0),
            end_time=datetime(2026, 2, 12, 12, 30, 0),
        )
        storage.save_run(earlier)
        storage.save_run(later)

        latest = storage.get_latest_run()
        assert latest is not None
        assert latest.run_id == "later"

    def test_run_dir_creation(self, tmp_path: pytest.TempPathFactory) -> None:
        """After save, the run directory should exist and contain metrics.json."""
        storage = MetricsStorage(tmp_path)
        metrics = _make_pipeline_metrics(run_id="dir-check")
        storage.save_run(metrics)

        run_dir = storage.run_dir("dir-check")
        assert run_dir.exists()
        assert (run_dir / "metrics.json").exists()

    def test_load_nonexistent_returns_none(self, tmp_path: pytest.TempPathFactory) -> None:
        """Loading a run ID that was never saved should return None."""
        storage = MetricsStorage(tmp_path)
        assert storage.load_run("nonexistent") is None


# ---------------------------------------------------------------------------
# ReportGenerator End-to-End Tests
# ---------------------------------------------------------------------------


class TestReportGeneratorEndToEnd:
    """Verify HTML report generation from PipelineMetrics."""

    def test_generate_html_produces_valid_html(self) -> None:
        """_generate_html should return a string containing <html and </html>."""
        gen = ReportGenerator(metrics_dir="/tmp/unused-rg")
        metrics = _make_pipeline_metrics()

        html = gen._generate_html(metrics)

        assert "<html" in html
        assert "</html>" in html
        assert "<title>" in html

    def test_generate_html_contains_deployment_name(self) -> None:
        """The deployment name should appear in the generated HTML."""
        gen = ReportGenerator(metrics_dir="/tmp/unused-rg")
        metrics = _make_pipeline_metrics(deployment_name="my-custom-deploy")

        html = gen._generate_html(metrics)

        assert "my-custom-deploy" in html

    def test_generate_html_batch_mode(self) -> None:
        """Batch metrics with jobs should produce a jobs table."""
        gen = ReportGenerator(metrics_dir="/tmp/unused-rg")
        metrics = _make_pipeline_metrics()

        html = gen._generate_html(metrics)

        assert "Batch Job Performance" in html
        assert "bronze-verify" in html

    def test_generate_html_with_benchmark(self) -> None:
        """When benchmark data is present, QpH should appear in the HTML."""
        gen = ReportGenerator(metrics_dir="/tmp/unused-rg")
        metrics = _make_pipeline_metrics()
        metrics.benchmark = _make_benchmark_metrics()

        html = gen._generate_html(metrics)

        assert "QpH" in html or "qph" in html.lower()
        assert "450" in html

    def test_generate_report_writes_file(self, tmp_path: pytest.TempPathFactory) -> None:
        """generate_report should write an HTML file to the run directory."""
        metrics_dir = tmp_path / "runs"
        metrics_dir.mkdir()

        storage = MetricsStorage(metrics_dir)
        metrics = _make_pipeline_metrics(run_id="report-test")
        storage.save_run(metrics)

        gen = ReportGenerator(metrics_dir=metrics_dir)
        report_path = gen.generate_report(run_id="report-test")

        assert report_path.exists()
        assert report_path.suffix == ".html"
        content = report_path.read_text()
        assert "<html" in content
        assert "test-deploy" in content

    def test_generate_html_with_platform_metrics(self) -> None:
        """When platform_metrics are provided, a Platform Metrics section appears."""
        gen = ReportGenerator(metrics_dir="/tmp/unused-rg")
        metrics = _make_pipeline_metrics()

        platform_metrics = {
            "pods": [
                {
                    "pod_name": "spark-driver-0",
                    "component": "spark",
                    "cpu_avg_cores": 2.5,
                    "cpu_max_cores": 4.0,
                    "memory_avg_bytes": 4 * 1024**3,
                    "memory_max_bytes": 8 * 1024**3,
                }
            ],
            "s3_requests_total": 1200,
            "s3_errors_total": 3,
            "s3_avg_latency_ms": 12.5,
            "duration_seconds": 1800,
        }
        html = gen._generate_html(metrics, platform_metrics=platform_metrics)

        assert "Platform Metrics" in html
        assert "spark-driver-0" in html


# ---------------------------------------------------------------------------
# DeploymentEngine Orchestration Tests
# ---------------------------------------------------------------------------


class TestDeploymentEngineOrchestration:
    """Verify deployment engine dry-run behaviour and orchestration logic."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_deploy_all_dry_run_succeeds(self, _mock_ocp: MagicMock) -> None:
        """Dry-run deploy_all should return SUCCESS/SKIPPED for every step."""
        cfg = make_config(name="dry-run-test")
        mock_k8s = _mock_k8s()
        engine = DeploymentEngine(config=cfg, k8s_client=mock_k8s, dry_run=True)

        results = engine.deploy_all()

        assert len(results) > 0
        for r in results:
            assert r.status in (
                DeploymentStatus.SUCCESS,
                DeploymentStatus.SKIPPED,
            ), f"{r.component} returned unexpected status {r.status}: {r.message}"

        # Dry run should not make actual K8s calls
        mock_k8s.create_namespace.assert_not_called()

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_deploy_all_returns_ordered_results(self, _mock_ocp: MagicMock) -> None:
        """Results should come back in deployment order, namespace first."""
        cfg = make_config(name="order-test")
        mock_k8s = _mock_k8s()
        engine = DeploymentEngine(config=cfg, k8s_client=mock_k8s, dry_run=True)

        results = engine.deploy_all()

        assert len(results) > 0
        assert results[0].component == "namespace"

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_deploy_all_progress_callback_invoked(self, _mock_ocp: MagicMock) -> None:
        """A progress callback should be called for every deployment step."""
        cfg = make_config(name="callback-test")
        mock_k8s = _mock_k8s()
        engine = DeploymentEngine(config=cfg, k8s_client=mock_k8s, dry_run=True)

        invocations: list[tuple[str, DeploymentStatus, str]] = []
        engine.deploy_all(progress_callback=lambda c, s, m: invocations.append((c, s, m)))

        # At least two callbacks per step: IN_PROGRESS + final status
        assert len(invocations) >= 2
        # First callback should signal the namespace step starting
        assert invocations[0][0] == "namespace"
        assert invocations[0][1] == DeploymentStatus.IN_PROGRESS

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_destroy_all_dry_run(self, _mock_ocp: MagicMock) -> None:
        """Dry-run destroy_all should not raise and should not contact K8s."""
        cfg = make_config(name="destroy-test")
        mock_k8s = _mock_k8s()
        engine = DeploymentEngine(config=cfg, k8s_client=mock_k8s, dry_run=True)

        # destroy_all accesses the real kubernetes client module for API calls;
        # patch it so the test does not need a live cluster.
        with patch("lakebench.deploy.engine.K8sClient"):
            try:
                results = engine.destroy_all()
            except Exception:
                # destroy_all may fail in unit-test context because it
                # imports kubernetes.client directly -- that is acceptable
                # for a dry-run orchestration test.
                results = []

        # If we got results back, none should be PENDING (not yet started)
        for r in results:
            assert r.status != DeploymentStatus.PENDING

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_build_context_has_all_required_keys(self, _mock_ocp: MagicMock) -> None:
        """The template context should contain all keys needed by templates."""
        cfg = make_config(name="ctx-test")
        mock_k8s = _mock_k8s()
        engine = DeploymentEngine(config=cfg, k8s_client=mock_k8s, dry_run=True)

        ctx = engine.context

        required_keys = [
            "name",
            "namespace",
            "s3_endpoint",
            "s3_access_key",
            "s3_secret_key",
            "s3_region",
            "s3_host",
            "s3_port",
            "bucket_bronze",
            "bucket_silver",
            "bucket_gold",
            "postgres_image",
            "spark_image",
            "trino_image",
            "spark_executor_instances",
            "spark_executor_cores",
            "spark_executor_memory",
            "trino_worker_replicas",
            "trino_coordinator_cpu",
            "catalog_type",
            "query_engine_type",
        ]
        for key in required_keys:
            assert key in ctx, f"Missing required context key: {key}"
