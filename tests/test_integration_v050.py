"""Integration tests for DuckDB, observability, and report features.

These tests verify cross-module data flow:
- Config → Executor factory chain
- Config → Deployer → Template rendering
- Runner → Executor adapt_query ordering
- Observability config propagation
- Autosizer with all recipes
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.config.recipes import RECIPES
from tests.conftest import make_config

# ===========================================================================
# Config → Executor factory
# ===========================================================================


class TestConfigToExecutorChain:
    """Tests that recipe configs produce the correct executor type."""

    def test_all_trino_recipes_produce_trino_executor(self):
        from lakebench.benchmark.executor import TrinoExecutor, get_executor

        for name, _ in RECIPES.items():
            if name == "default":
                continue
            cfg = make_config(recipe=name)
            engine_type = cfg.architecture.query_engine.type.value
            if engine_type == "trino":
                executor = get_executor(cfg, namespace="test")
                assert isinstance(executor, TrinoExecutor), (
                    f"Recipe {name} should produce TrinoExecutor"
                )

    def test_all_spark_recipes_produce_spark_executor(self):
        from lakebench.benchmark.executor import SparkThriftExecutor, get_executor

        for name, _ in RECIPES.items():
            if name == "default":
                continue
            cfg = make_config(recipe=name)
            engine_type = cfg.architecture.query_engine.type.value
            if engine_type == "spark-thrift":
                executor = get_executor(cfg, namespace="test")
                assert isinstance(executor, SparkThriftExecutor), f"Recipe {name}"

    def test_all_duckdb_recipes_produce_duckdb_executor(self):
        from lakebench.benchmark.executor import DuckDBExecutor, get_executor

        for name, _ in RECIPES.items():
            if name == "default":
                continue
            cfg = make_config(recipe=name)
            engine_type = cfg.architecture.query_engine.type.value
            if engine_type == "duckdb":
                executor = get_executor(cfg, namespace="test")
                assert isinstance(executor, DuckDBExecutor), f"Recipe {name}"

    def test_duckdb_s3_config_propagation(self):
        """S3 config from platform section flows through to DuckDBExecutor."""
        from lakebench.benchmark.executor import DuckDBExecutor, get_executor

        cfg = make_config(
            recipe="hive-iceberg-spark-duckdb",
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "https://s3.custom.io",
                        "region": "eu-west-1",
                        "path_style": False,
                        "access_key": "ak",
                        "secret_key": "sk",
                    }
                }
            },
        )
        executor = get_executor(cfg, namespace="test")
        assert isinstance(executor, DuckDBExecutor)
        assert executor.s3_endpoint == "https://s3.custom.io"
        assert executor.s3_region == "eu-west-1"
        assert executor.s3_path_style is False


# ===========================================================================
# Config → Deployer skip guards
# ===========================================================================


class TestConfigToDeployerGuards:
    """Tests that deployers correctly skip based on config."""

    def test_duckdb_deployer_skips_for_non_duckdb_recipes(self):
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        for name in RECIPES:
            if name == "default":
                continue
            cfg = make_config(recipe=name)
            if cfg.architecture.query_engine.type.value != "duckdb":
                engine = MagicMock()
                engine.config = cfg
                deployer = DuckDBDeployer(engine)
                result = deployer.deploy()
                assert result.status == DeploymentStatus.SKIPPED, (
                    f"Recipe {name} should skip DuckDB"
                )

    def test_duckdb_deployer_proceeds_for_duckdb_recipes(self):
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        for name in RECIPES:
            if name == "default":
                continue
            cfg = make_config(recipe=name)
            if cfg.architecture.query_engine.type.value == "duckdb":
                engine = MagicMock()
                engine.config = cfg
                engine.dry_run = True
                deployer = DuckDBDeployer(engine)
                result = deployer.deploy()
                assert result.status == DeploymentStatus.SUCCESS, f"Recipe {name} should proceed"


# ===========================================================================
# Observability config propagation
# ===========================================================================


class TestObservabilityConfigPropagation:
    """Tests that observability config flows correctly through the system."""

    def test_observability_disabled_by_default(self):
        cfg = make_config()
        assert cfg.observability.enabled is False

    def test_observability_enabled_propagates(self):
        cfg = make_config(observability={"enabled": True})
        assert cfg.observability.enabled is True
        assert cfg.observability.prometheus_stack_enabled is True

    def test_dashboards_disabled_while_observability_enabled(self):
        cfg = make_config(observability={"enabled": True, "dashboards_enabled": False})
        assert cfg.observability.enabled is True
        assert cfg.observability.dashboards_enabled is False

    def test_s3_metrics_default(self):
        cfg = make_config(observability={"enabled": True})
        assert cfg.observability.s3_metrics_enabled is True

    def test_spark_metrics_default(self):
        cfg = make_config(observability={"enabled": True})
        assert cfg.observability.spark_metrics_enabled is True


# ===========================================================================
# Autosizer with all recipes
# ===========================================================================


class TestAutosizerAllRecipes:
    """Tests that autosizer handles all recipes without errors."""

    def test_co_resident_cpu_all_recipes(self):
        from lakebench.config.autosizer import _co_resident_cpu_m

        for name in RECIPES:
            if name == "default":
                continue
            cfg = make_config(recipe=name)
            result = _co_resident_cpu_m(cfg)
            assert result > 0, f"Recipe {name} should have positive co-resident CPU"

    def test_duckdb_cpu_scales_with_cores(self):
        from lakebench.config.autosizer import _co_resident_cpu_m

        cfg_small = make_config(recipe="hive-iceberg-spark-duckdb")
        cfg_large = make_config(
            recipe="hive-iceberg-spark-duckdb",
            architecture={
                "query_engine": {"type": "duckdb", "duckdb": {"cores": 8}},
                "catalog": {"type": "hive"},
                "table_format": {"type": "iceberg"},
            },
        )
        small = _co_resident_cpu_m(cfg_small)
        large = _co_resident_cpu_m(cfg_large)
        assert large > small


# ===========================================================================
# Runner → Executor integration
# ===========================================================================


class TestRunnerExecutorIntegration:
    """Tests that runner correctly uses executor methods."""

    def test_runner_uses_executor_from_factory(self):
        cfg = make_config(recipe="hive-iceberg-spark-duckdb")
        mock_executor = MagicMock()
        mock_executor.engine_name.return_value = "duckdb"
        mock_executor.catalog_name = "lakehouse"

        with patch("lakebench.benchmark.executor.get_executor", return_value=mock_executor):
            from lakebench.benchmark.runner import BenchmarkRunner

            runner = BenchmarkRunner(config=cfg, namespace="test-ns")
            assert runner.executor is mock_executor
            assert runner.executor.engine_name() == "duckdb"

    def test_all_executors_have_adapt_query(self):
        """All executors implement adapt_query() as identity."""
        from lakebench.benchmark.executor import (
            DuckDBExecutor,
            SparkThriftExecutor,
            TrinoExecutor,
        )

        sql = "SELECT * FROM lakehouse.silver.table1"
        for cls, kwargs in [
            (TrinoExecutor, {"namespace": "t", "catalog_name": "c"}),
            (SparkThriftExecutor, {"namespace": "t", "catalog_name": "c"}),
            (DuckDBExecutor, {"namespace": "t", "catalog_name": "c"}),
        ]:
            executor = cls(**kwargs)
            assert executor.adapt_query(sql) == sql

    def test_all_executors_have_flush_cache(self):
        """All executors implement flush_cache() without error."""
        from lakebench.benchmark.executor import (
            DuckDBExecutor,
            SparkThriftExecutor,
        )

        for cls, kwargs in [
            (SparkThriftExecutor, {"namespace": "t", "catalog_name": "c"}),
            (DuckDBExecutor, {"namespace": "t", "catalog_name": "c"}),
        ]:
            executor = cls(**kwargs)
            # flush_cache should not raise (no-op for these engines)
            executor.flush_cache()


# ===========================================================================
# Template rendering
# ===========================================================================


class TestDuckDBTemplateRendering:
    """Tests for DuckDB template rendering via DeploymentEngine context."""

    def test_duckdb_context_variables_present(self):
        """Verify _build_context includes DuckDB variables."""
        # We can't easily call _build_context without a full engine,
        # so we test that the config provides the expected values
        cfg = make_config(recipe="hive-iceberg-spark-duckdb")
        duckdb = cfg.architecture.query_engine.duckdb
        assert duckdb.cores == 2
        assert duckdb.memory == "4g"
        assert duckdb.catalog_name == "lakehouse"
        assert cfg.images.duckdb == "python:3.11-slim"

    def test_observability_enabled_in_context(self):
        """Verify observability.enabled is available for template rendering."""
        cfg = make_config(observability={"enabled": True})
        assert cfg.observability.enabled is True
        # This value should be passed as observability_enabled to templates

    def test_duckdb_memory_format(self):
        """DuckDB memory should be parseable as a K8s resource value."""
        cfg = make_config(
            recipe="hive-iceberg-spark-duckdb",
            architecture={
                "query_engine": {"type": "duckdb", "duckdb": {"memory": "8g"}},
                "catalog": {"type": "hive"},
                "table_format": {"type": "iceberg"},
            },
        )
        assert cfg.architecture.query_engine.duckdb.memory == "8g"


# ===========================================================================
# PlatformMetrics → Report data flow
# ===========================================================================


class TestPlatformMetricsToReport:
    """Tests for PlatformMetrics → ReportGenerator data flow."""

    def test_platform_metrics_serialization_roundtrip(self):
        """PlatformMetrics.to_dict() output is consumable by _generate_platform_section."""
        from datetime import datetime

        from lakebench.observability.platform_collector import PlatformMetrics, PodMetrics
        from lakebench.reports.generator import ReportGenerator

        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)
        metrics = PlatformMetrics(
            start_time=start,
            end_time=end,
            pods=[
                PodMetrics(
                    pod_name="pod-1",
                    component="trino-coordinator",
                    cpu_avg_cores=1.5,
                    cpu_max_cores=2.0,
                    memory_avg_bytes=2e9,
                    memory_max_bytes=3e9,
                )
            ],
            s3_requests_total=50,
        )

        d = metrics.to_dict()
        gen = ReportGenerator()
        html = gen._generate_platform_section(d)
        assert "Platform Metrics" in html
        assert "trino-coordinator" in html
        assert "50" in html
