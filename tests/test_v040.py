"""Tests for recipe system, Spark Thrift Server, and pipeline rename.

Covers:
- Spark 4.x compatibility (_parse_spark_major, _spark_compat)
- Pipeline rename (processing → pipeline backward compat)
- Recipe system (expansion, overrides, unknown recipe, default alias, _deep_setdefault)
- QueryExecutor factory (get_executor)
- SparkThriftDeployer skip guard
- Autosizer _co_resident_cpu_m branching on engine type
- BenchmarkRunner with mock executor
"""

import warnings
from unittest.mock import MagicMock, patch

import pytest

from lakebench.config import LakebenchConfig
from lakebench.config.recipes import RECIPES, _deep_setdefault
from lakebench.spark.job import _parse_spark_major, _spark_compat

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig with sensible defaults for testing."""
    base: dict = {
        "name": "test-v040",
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
# Phase 1: Spark 4.x Compatibility
# ===========================================================================


class TestSpark4xCompat:
    """Tests for _parse_spark_major and _spark_compat."""

    def test_parse_spark_major_3x(self):
        assert _parse_spark_major("apache/spark:3.5.4-python3") == 3

    def test_parse_spark_major_3x_plain(self):
        assert _parse_spark_major("apache/spark:3.5.4") == 3

    def test_parse_spark_major_4x(self):
        assert _parse_spark_major("apache/spark:4.0.0-python3") == 4

    def test_parse_spark_major_4x_custom_registry(self):
        assert _parse_spark_major("my-registry.io/spark:4.1.0-java17") == 4

    def test_parse_spark_major_unparseable(self):
        """Unparseable tags fall back to 3 with a warning."""
        assert _parse_spark_major("custom-image:latest") == 3

    def test_spark_compat_3x(self):
        scala_suffix, hadoop_version = _spark_compat("apache/spark:3.5.4-python3")
        assert scala_suffix == "_2.12"
        assert hadoop_version == "3.3.4"

    def test_spark_compat_4x(self):
        scala_suffix, hadoop_version = _spark_compat("apache/spark:4.0.0-python3")
        assert scala_suffix == "_2.13"
        assert hadoop_version == "3.4.1"


# ===========================================================================
# Phase 2: Pipeline Rename
# ===========================================================================


class TestPipelineRename:
    """Tests for processing → pipeline backward compat."""

    def test_pipeline_field_works(self):
        """New 'pipeline' field is accepted."""
        cfg = _make_config(architecture={"pipeline": {"pattern": "streaming"}})
        assert cfg.architecture.pipeline.pattern.value == "streaming"

    def test_processing_backward_compat(self):
        """Old 'processing' key is accepted with DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cfg = _make_config(architecture={"processing": {"pattern": "streaming"}})
            assert cfg.architecture.pipeline.pattern.value == "streaming"
            deprecation_msgs = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecation_msgs) >= 1
            assert "processing" in str(deprecation_msgs[0].message).lower()

    def test_pipeline_takes_precedence(self):
        """When both 'processing' and 'pipeline' are present, pipeline wins."""
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            cfg = _make_config(
                architecture={
                    "processing": {"pattern": "streaming"},
                    "pipeline": {"pattern": "medallion"},
                }
            )
            assert cfg.architecture.pipeline.pattern.value == "medallion"


# ===========================================================================
# Phase 4: Recipe System
# ===========================================================================


class TestRecipeSystem:
    """Tests for recipe expansion, overrides, and _deep_setdefault."""

    def test_recipe_expansion(self):
        """Recipe defaults are applied correctly."""
        cfg = _make_config(recipe="hive-iceberg-spark-trino")
        assert cfg.architecture.catalog.type.value == "hive"
        assert cfg.architecture.table_format.type.value == "iceberg"
        assert cfg.architecture.query_engine.type.value == "trino"

    def test_recipe_user_override(self):
        """User explicit values win over recipe defaults."""
        cfg = _make_config(
            recipe="hive-iceberg-spark-trino",
            images={"spark": "my-registry/spark:3.5.3-python3"},
        )
        # User override wins
        assert "3.5.3" in cfg.images.spark
        # Recipe still sets architecture defaults
        assert cfg.architecture.catalog.type.value == "hive"

    def test_recipe_unknown_raises(self):
        """Unknown recipe name raises ValueError."""
        with pytest.raises(Exception, match="Unknown recipe"):
            _make_config(recipe="nonexistent-recipe")

    def test_recipe_default_alias(self):
        """'default' alias maps to hive-iceberg-spark-trino."""
        assert RECIPES["default"] is RECIPES["hive-iceberg-spark-trino"]

    def test_recipe_default_alias_expansion(self):
        """'default' recipe expands the same as hive-iceberg-spark-trino."""
        cfg = _make_config(recipe="default")
        assert cfg.architecture.catalog.type.value == "hive"
        assert cfg.architecture.table_format.type.value == "iceberg"
        assert cfg.architecture.query_engine.type.value == "trino"

    def test_deep_setdefault_basic(self):
        """_deep_setdefault merges without overwriting existing keys."""
        target = {"a": 1, "b": {"x": 10}}
        defaults = {"a": 99, "b": {"x": 99, "y": 20}, "c": 3}
        _deep_setdefault(target, defaults)
        assert target["a"] == 1  # not overwritten
        assert target["b"]["x"] == 10  # not overwritten
        assert target["b"]["y"] == 20  # added
        assert target["c"] == 3  # added

    def test_deep_setdefault_nested(self):
        """Deeply nested values merge correctly."""
        target = {"l1": {"l2": {"l3": "keep"}}}
        defaults = {"l1": {"l2": {"l3": "discard", "new": "added"}, "new2": "also"}}
        _deep_setdefault(target, defaults)
        assert target["l1"]["l2"]["l3"] == "keep"
        assert target["l1"]["l2"]["new"] == "added"
        assert target["l1"]["new2"] == "also"

    def test_all_recipes_valid(self):
        """Every recipe expands to a valid LakebenchConfig."""
        named_recipes = [k for k in RECIPES if k != "default"]
        assert len(named_recipes) == 8
        for name in named_recipes:
            cfg = _make_config(recipe=name)
            assert cfg.architecture is not None, f"Recipe {name} failed"

    def test_recipe_polaris_iceberg_spark(self):
        """Polaris + Iceberg + Spark Thrift recipe."""
        cfg = _make_config(recipe="polaris-iceberg-spark-thrift")
        assert cfg.architecture.catalog.type.value == "polaris"
        assert cfg.architecture.table_format.type.value == "iceberg"
        assert cfg.architecture.query_engine.type.value == "spark-thrift"


# ===========================================================================
# Phase 5A: QueryExecutor
# ===========================================================================


class TestQueryExecutor:
    """Tests for get_executor factory."""

    def test_get_executor_trino(self):
        from lakebench.benchmark.executor import TrinoExecutor, get_executor

        cfg = _make_config(recipe="hive-iceberg-spark-trino")
        executor = get_executor(cfg, namespace="test-ns")
        assert isinstance(executor, TrinoExecutor)
        assert executor.engine_name() == "trino"
        assert executor.catalog_name == "lakehouse"

    def test_get_executor_spark_thrift(self):
        from lakebench.benchmark.executor import SparkThriftExecutor, get_executor

        cfg = _make_config(recipe="hive-iceberg-spark-thrift")
        executor = get_executor(cfg, namespace="test-ns")
        assert isinstance(executor, SparkThriftExecutor)
        assert executor.engine_name() == "spark-thrift"
        assert executor.catalog_name == "lakehouse"

    def test_get_executor_none_raises(self):
        from lakebench.benchmark.executor import get_executor

        cfg = _make_config(recipe="hive-iceberg-spark-none")
        with pytest.raises(ValueError, match="Cannot run queries"):
            get_executor(cfg, namespace="test-ns")

    def test_executor_result_success_property(self):
        from lakebench.benchmark.executor import QueryExecutorResult

        result = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=0.1,
            rows_returned=1,
            raw_output="1",
        )
        assert result.success is True

    def test_executor_result_error_property(self):
        from lakebench.benchmark.executor import QueryExecutorResult

        result = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=0.1,
            rows_returned=0,
            raw_output="",
            error="connection refused",
        )
        assert result.success is False


# ===========================================================================
# Phase 5D: SparkThriftDeployer
# ===========================================================================


class TestSparkThriftDeployer:
    """Tests for SparkThriftDeployer skip guard."""

    def test_spark_thrift_deployer_skip(self):
        """SparkThriftDeployer skips when engine is not spark-thrift."""
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.spark_thrift import SparkThriftDeployer

        cfg = _make_config(recipe="hive-iceberg-spark-trino")
        engine = MagicMock()
        engine.config = cfg
        deployer = SparkThriftDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED
        assert "spark-thrift" in result.component

    def test_spark_thrift_deployer_proceeds_for_spark(self):
        """SparkThriftDeployer does NOT skip when engine is spark-thrift."""
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.spark_thrift import SparkThriftDeployer

        cfg = _make_config(recipe="hive-iceberg-spark-thrift")
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = SparkThriftDeployer(engine)
        result = deployer.deploy()
        # dry_run → SUCCESS (not SKIPPED)
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would deploy" in result.message


# ===========================================================================
# Phase 5E: Autosizer co_resident_cpu_m
# ===========================================================================


class TestCoResidentCpu:
    """Tests for _co_resident_cpu_m branching on engine type."""

    def test_co_resident_cpu_trino(self):
        from lakebench.config.autosizer import _co_resident_cpu_m

        cfg = _make_config(recipe="hive-iceberg-spark-trino")
        result = _co_resident_cpu_m(cfg)
        # Should include Trino coordinator + workers + infra
        # Default: coordinator 2 CPU + 2 workers * 2 CPU = 6000m + 1000m infra = 7000m
        assert result > 1000  # More than just infra

    def test_co_resident_cpu_spark_thrift(self):
        from lakebench.config.autosizer import _co_resident_cpu_m

        cfg = _make_config(recipe="hive-iceberg-spark-thrift")
        result = _co_resident_cpu_m(cfg)
        # Spark thrift: default 2 cores = 2000m + 1000m infra = 3000m
        assert result == 3000

    def test_co_resident_cpu_none(self):
        from lakebench.config.autosizer import _co_resident_cpu_m

        cfg = _make_config(recipe="hive-iceberg-spark-none")
        result = _co_resident_cpu_m(cfg)
        # No engine: just infra = 1000m
        assert result == 1000


# ===========================================================================
# BenchmarkRunner with mock executor
# ===========================================================================


class TestBenchmarkRunnerMockExecutor:
    """Tests for BenchmarkRunner using a mock QueryExecutor."""

    def test_benchmark_runner_creates_executor(self):
        """BenchmarkRunner should use get_executor to create its executor."""
        from lakebench.benchmark.executor import QueryExecutorResult

        cfg = _make_config(recipe="hive-iceberg-spark-trino")
        mock_executor = MagicMock()
        mock_executor.engine_name.return_value = "trino"
        mock_executor.catalog_name = "lakehouse"
        mock_executor.flush_cache.return_value = None
        mock_executor.execute_query.return_value = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=0.05,
            rows_returned=1,
            raw_output="1",
        )

        with patch(
            "lakebench.benchmark.executor.get_executor",
            return_value=mock_executor,
        ):
            from lakebench.benchmark.runner import BenchmarkRunner

            runner = BenchmarkRunner(cfg)
            assert runner.executor is mock_executor
            assert runner.catalog == "lakehouse"
