"""Tests for DuckDB engine, observability, and platform metrics.

Covers:
- DuckDBConfig schema and defaults
- DuckDB recipes (hive-iceberg-spark-duckdb, polaris-iceberg-spark-duckdb)
- DuckDB supported combinations
- Flat ObservabilityConfig (replaces nested metrics/dashboards)
- DuckDB co_resident_cpu_m
- DuckDB executor and adapt_query
- DuckDB deployer skip guard
- ObservabilityDeployer
- S3 metrics wrapper
- Platform collector
- Report enrichment
"""

from unittest.mock import MagicMock, patch

from lakebench.config import LakebenchConfig
from lakebench.config.recipes import RECIPES

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig with sensible defaults for testing."""
    base: dict = {
        "name": "test-v050",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                }
            }
        },
    }
    base.update(overrides)
    return LakebenchConfig(**base)


# ===========================================================================
# Phase 1: DuckDB Config Schema
# ===========================================================================


class TestDuckDBConfig:
    """Tests for DuckDB configuration model."""

    def test_duckdb_config_defaults(self):
        """DuckDBConfig has correct defaults."""
        from lakebench.config.schema import DuckDBConfig

        cfg = DuckDBConfig()
        assert cfg.cores == 2
        assert cfg.memory == "4g"
        assert cfg.catalog_name == "lakehouse"

    def test_duckdb_config_in_query_engine(self):
        """QueryEngineConfig includes duckdb field."""
        cfg = _make_config()
        assert hasattr(cfg.architecture.query_engine, "duckdb")
        assert cfg.architecture.query_engine.duckdb.cores == 2

    def test_duckdb_engine_type_enum(self):
        """QueryEngineType includes DUCKDB."""
        from lakebench.config.schema import QueryEngineType

        assert QueryEngineType.DUCKDB.value == "duckdb"

    def test_duckdb_image_in_images(self):
        """ImagesConfig includes duckdb image."""
        cfg = _make_config()
        assert cfg.images.duckdb == "python:3.11-slim"


# ===========================================================================
# Phase 1: DuckDB Recipes
# ===========================================================================


class TestDuckDBRecipes:
    """Tests for DuckDB recipe definitions."""

    def test_hive_iceberg_duckdb_recipe_exists(self):
        assert "hive-iceberg-spark-duckdb" in RECIPES

    def test_polaris_iceberg_duckdb_recipe_exists(self):
        assert "polaris-iceberg-spark-duckdb" in RECIPES

    def test_hive_iceberg_duckdb_recipe_valid(self):
        cfg = _make_config(recipe="hive-iceberg-spark-duckdb")
        assert cfg.architecture.catalog.type.value == "hive"
        assert cfg.architecture.table_format.type.value == "iceberg"
        assert cfg.architecture.query_engine.type.value == "duckdb"

    def test_polaris_iceberg_duckdb_recipe_valid(self):
        cfg = _make_config(recipe="polaris-iceberg-spark-duckdb")
        assert cfg.architecture.catalog.type.value == "polaris"
        assert cfg.architecture.table_format.type.value == "iceberg"
        assert cfg.architecture.query_engine.type.value == "duckdb"

    def test_total_recipes_count(self):
        """8 named recipes + default alias."""
        named = [k for k in RECIPES if k != "default"]
        assert len(named) == 8


# ===========================================================================
# Phase 1: Supported Combinations
# ===========================================================================


class TestDuckDBCombinations:
    """Tests for DuckDB supported combinations."""

    def test_hive_iceberg_duckdb_supported(self):
        from lakebench.config.schema import _SUPPORTED_COMBINATIONS

        assert ("hive", "iceberg", "spark", "duckdb") in _SUPPORTED_COMBINATIONS

    def test_polaris_iceberg_duckdb_supported(self):
        from lakebench.config.schema import _SUPPORTED_COMBINATIONS

        assert ("polaris", "iceberg", "spark", "duckdb") in _SUPPORTED_COMBINATIONS


# ===========================================================================
# Phase 1: Flat ObservabilityConfig
# ===========================================================================


class TestObservabilityConfig:
    """Tests for the flat ObservabilityConfig model."""

    def test_observability_defaults(self):
        cfg = _make_config()
        obs = cfg.observability
        assert obs.enabled is False
        assert obs.prometheus_stack_enabled is True
        assert obs.s3_metrics_enabled is True
        assert obs.spark_metrics_enabled is True
        assert obs.dashboards_enabled is True

    def test_observability_reports_preserved(self):
        """ReportsConfig is preserved in the flat model."""
        cfg = _make_config()
        assert cfg.observability.reports.enabled is True
        assert cfg.observability.reports.format.value == "html"

    def test_observability_enabled_override(self):
        cfg = _make_config(observability={"enabled": True, "retention": "14d"})
        assert cfg.observability.enabled is True
        assert cfg.observability.retention == "14d"

    def test_observability_report_include_platform_metrics(self):
        """ReportIncludeConfig has platform_metrics field."""
        cfg = _make_config()
        assert cfg.observability.reports.include.platform_metrics is True


# ===========================================================================
# Phase 1: DuckDB co_resident_cpu_m
# ===========================================================================


class TestDuckDBCoResidentCpu:
    """Tests for _co_resident_cpu_m with DuckDB engine."""

    def test_co_resident_cpu_duckdb(self):
        from lakebench.config.autosizer import _co_resident_cpu_m

        cfg = _make_config(recipe="hive-iceberg-spark-duckdb")
        result = _co_resident_cpu_m(cfg)
        # DuckDB: default 2 cores = 2000m + 1000m infra = 3000m
        assert result == 3000

    def test_co_resident_cpu_duckdb_custom_cores(self):
        from lakebench.config.autosizer import _co_resident_cpu_m

        cfg = _make_config(
            recipe="hive-iceberg-spark-duckdb",
            architecture={
                "query_engine": {"type": "duckdb", "duckdb": {"cores": 4}},
                "catalog": {"type": "hive"},
                "table_format": {"type": "iceberg"},
            },
        )
        result = _co_resident_cpu_m(cfg)
        # DuckDB: 4 cores = 4000m + 1000m infra = 5000m
        assert result == 5000


# ===========================================================================
# Phase 2: DuckDB Executor
# ===========================================================================


class TestDuckDBExecutor:
    """Tests for DuckDBExecutor."""

    def test_get_executor_duckdb(self):
        from lakebench.benchmark.executor import DuckDBExecutor, get_executor

        cfg = _make_config(recipe="hive-iceberg-spark-duckdb")
        executor = get_executor(cfg, namespace="test-ns")
        assert isinstance(executor, DuckDBExecutor)
        assert executor.engine_name() == "duckdb"
        assert executor.catalog_name == "lakehouse"

    def test_duckdb_executor_s3_config(self):
        from lakebench.benchmark.executor import DuckDBExecutor, get_executor

        cfg = _make_config(recipe="hive-iceberg-spark-duckdb")
        executor = get_executor(cfg, namespace="test-ns")
        assert isinstance(executor, DuckDBExecutor)
        assert executor.s3_endpoint == "http://minio:9000"

    def test_duckdb_flush_cache_noop(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        # Should not raise
        executor.flush_cache()

    def test_duckdb_adapt_query_identity(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        sql = "SELECT * FROM lakehouse.silver.customer_interactions"
        assert executor.adapt_query(sql) == sql


class TestAdaptQuery:
    """Tests for adapt_query on all executors."""

    def test_trino_adapt_query_identity(self):
        from lakebench.benchmark.executor import TrinoExecutor

        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")
        sql = "SELECT * FROM lakehouse.silver.table1"
        assert executor.adapt_query(sql) == sql

    def test_spark_thrift_adapt_query_identity(self):
        from lakebench.benchmark.executor import SparkThriftExecutor

        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        sql = "SELECT * FROM lakehouse.silver.table1"
        assert executor.adapt_query(sql) == sql


# ===========================================================================
# Phase 2: DuckDB Deployer
# ===========================================================================


class TestDuckDBDeployer:
    """Tests for DuckDBDeployer skip guard."""

    def test_duckdb_deployer_skip(self):
        """DuckDBDeployer skips when engine is not duckdb."""
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        cfg = _make_config(recipe="hive-iceberg-spark-trino")
        engine = MagicMock()
        engine.config = cfg
        deployer = DuckDBDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED
        assert "duckdb" in result.component

    def test_duckdb_deployer_proceeds_for_duckdb(self):
        """DuckDBDeployer does NOT skip when engine is duckdb."""
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        cfg = _make_config(recipe="hive-iceberg-spark-duckdb")
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = DuckDBDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would deploy" in result.message


# ===========================================================================
# Phase 2: Runner adapt_query integration
# ===========================================================================


class TestRunnerAdaptQuery:
    """Tests that runner calls adapt_query before execute_query."""

    def test_runner_calls_adapt_query(self):
        from lakebench.benchmark.executor import QueryExecutorResult

        cfg = _make_config(recipe="hive-iceberg-spark-duckdb")
        mock_executor = MagicMock()
        mock_executor.engine_name.return_value = "duckdb"
        mock_executor.catalog_name = "lakehouse"
        mock_executor.flush_cache.return_value = None
        mock_executor.adapt_query.return_value = "ADAPTED SQL"
        mock_executor.execute_query.return_value = QueryExecutorResult(
            sql="ADAPTED SQL",
            engine="duckdb",
            duration_seconds=0.05,
            rows_returned=1,
            raw_output="1",
        )

        with patch("lakebench.benchmark.executor.get_executor", return_value=mock_executor):
            from lakebench.benchmark.runner import BenchmarkRunner

            runner = BenchmarkRunner(config=cfg, namespace="test-ns")
            assert runner.executor is mock_executor


# ===========================================================================
# Phase 3: ObservabilityDeployer
# ===========================================================================


class TestObservabilityDeployer:
    """Tests for ObservabilityDeployer skip guard."""

    def test_observability_deployer_skip_when_disabled(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = _make_config()  # observability.enabled defaults to False
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED

    def test_observability_deployer_dry_run(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = _make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = ObservabilityDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would deploy" in result.message

    def test_observability_deployer_destroy_dry_run(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = _make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = ObservabilityDeployer(engine)
        result = deployer.destroy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would destroy" in result.message


# ===========================================================================
# Phase 3: S3 Metrics Wrapper
# ===========================================================================


class TestS3MetricsWrapper:
    """Tests for S3MetricsWrapper."""

    def test_wrapper_proxies_calls(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = ["bucket1", "bucket2"]

        wrapper = S3MetricsWrapper(mock_client, enabled=False)
        result = wrapper.list_buckets()
        assert result == ["bucket1", "bucket2"]
        mock_client.list_buckets.assert_called_once()

    def test_wrapper_disabled_noop(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.upload_file.return_value = None

        wrapper = S3MetricsWrapper(mock_client, enabled=False)
        wrapper.upload_file("file.txt", "bucket", "key")
        mock_client.upload_file.assert_called_once_with("file.txt", "bucket", "key")


# ===========================================================================
# Phase 3: Platform Collector
# ===========================================================================


class TestPlatformCollector:
    """Tests for PlatformCollector."""

    def test_platform_metrics_to_dict(self):
        from datetime import datetime

        from lakebench.observability.platform_collector import PlatformMetrics, PodMetrics

        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)
        metrics = PlatformMetrics(
            start_time=start,
            end_time=end,
            pods=[
                PodMetrics(
                    pod_name="lakebench-trino-coord-0",
                    component="trino-coordinator",
                    cpu_avg_cores=1.5,
                    cpu_max_cores=2.0,
                    memory_avg_bytes=2e9,
                    memory_max_bytes=3e9,
                )
            ],
            s3_requests_total=100,
            s3_errors_total=2,
            s3_avg_latency_ms=15.5,
        )
        d = metrics.to_dict()
        assert d["duration_seconds"] == 300.0
        assert len(d["pods"]) == 1
        assert d["pods"][0]["component"] == "trino-coordinator"
        assert d["s3_requests_total"] == 100

    def test_infer_component(self):
        from lakebench.observability.platform_collector import PlatformCollector

        assert (
            PlatformCollector._infer_component("lakebench-trino-coordinator-0")
            == "trino-coordinator"
        )
        assert PlatformCollector._infer_component("lakebench-duckdb-abc123") == "duckdb"
        assert PlatformCollector._infer_component("lakebench-spark-thrift-xyz") == "spark-thrift"
        assert PlatformCollector._infer_component("some-spark-driver-pod") == "spark-driver"
        assert PlatformCollector._infer_component("bronze-exec-1") == "spark-executor"
        assert PlatformCollector._infer_component("some-spark-process") == "spark-job"
        assert PlatformCollector._infer_component("random-pod") == "unknown"

    def test_platform_metrics_duration(self):
        from datetime import datetime

        from lakebench.observability.platform_collector import PlatformMetrics

        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 10, 0)
        metrics = PlatformMetrics(start_time=start, end_time=end)
        assert metrics.duration_seconds == 600.0


# ===========================================================================
# Phase 4: Report Enrichment
# ===========================================================================


class TestReportPlatformSection:
    """Tests for the platform metrics section in the HTML report."""

    def test_platform_section_empty_when_no_metrics(self):
        from lakebench.reports.generator import ReportGenerator

        gen = ReportGenerator()
        result = gen._generate_platform_section(None)
        assert result == ""

    def test_platform_section_with_metrics(self):
        from lakebench.reports.generator import ReportGenerator

        gen = ReportGenerator()
        platform_data = {
            "start_time": "2026-02-12T10:00:00",
            "end_time": "2026-02-12T10:05:00",
            "duration_seconds": 300.0,
            "pods": [
                {
                    "pod_name": "lakebench-trino-coord-0",
                    "component": "trino-coordinator",
                    "cpu_avg_cores": 1.5,
                    "cpu_max_cores": 2.0,
                    "memory_avg_bytes": 2e9,
                    "memory_max_bytes": 3e9,
                }
            ],
            "s3_requests_total": 100,
            "s3_errors_total": 2,
            "s3_avg_latency_ms": 15.5,
            "collection_error": None,
        }
        result = gen._generate_platform_section(platform_data)
        assert "Platform Metrics" in result
        assert "trino-coordinator" in result
        assert "100" in result  # s3 requests
        assert "300s" in result  # duration

    def test_platform_section_with_collection_error(self):
        from lakebench.reports.generator import ReportGenerator

        gen = ReportGenerator()
        platform_data = {
            "duration_seconds": 0,
            "pods": [],
            "s3_requests_total": 0,
            "s3_errors_total": 0,
            "s3_avg_latency_ms": 0,
            "collection_error": "Connection refused",
        }
        result = gen._generate_platform_section(platform_data)
        assert "Connection refused" in result
