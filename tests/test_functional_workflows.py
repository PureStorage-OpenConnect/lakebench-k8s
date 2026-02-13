"""Functional workflow tests.

Exercises the DuckDB, observability, and report generation workflows
end-to-end with mocked infrastructure (no real K8s/S3 required).

Covers gaps identified in test coverage audit:
- DuckDB benchmark workflow (deploy -> executor -> runner -> report)
- Observability deployer lifecycle (deploy -> collect -> report)
- Report generation with platform metrics integration
- DuckDB health_check coverage
- Runner adapt_query ordering verification
- Per-engine benchmark workflows (trino, spark-thrift, duckdb)
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from lakebench.benchmark.executor import (
    DuckDBExecutor,
    QueryExecutorResult,
    SparkThriftExecutor,
    TrinoExecutor,
    get_executor,
)
from lakebench.benchmark.runner import BenchmarkRunner
from lakebench.deploy.engine import DeploymentStatus
from lakebench.observability.platform_collector import PlatformMetrics, PodMetrics
from lakebench.reports.generator import ReportGenerator
from tests.conftest import make_config

# ===========================================================================
# 1. DuckDB Benchmark Workflow
# ===========================================================================


class TestDuckDBBenchmarkWorkflow:
    """End-to-end DuckDB benchmark workflow: config -> executor -> runner -> results."""

    @pytest.fixture
    def duckdb_executor(self):
        """A DuckDB executor with mocked subprocess."""
        executor = DuckDBExecutor(
            namespace="test-ns",
            catalog_name="lakehouse",
            s3_endpoint="http://minio:9000",
            s3_region="us-east-1",
            s3_path_style=True,
        )
        executor._pod = "lakebench-duckdb-abc"  # skip discovery
        return executor

    @pytest.fixture
    def duckdb_runner(self):
        """BenchmarkRunner wired to a mocked DuckDB executor."""
        cfg = make_config(recipe="hive-iceberg-duckdb")
        mock_result = QueryExecutorResult(
            sql="SELECT 1",
            engine="duckdb",
            duration_seconds=0.3,
            rows_returned=10,
            raw_output='{"rows": 10, "data": []}',
        )
        with patch("lakebench.benchmark.executor.get_executor") as mock_factory:
            mock_exec = MagicMock()
            mock_exec.engine_name.return_value = "duckdb"
            mock_exec.catalog_name = "lakehouse"
            mock_exec.adapt_query.side_effect = lambda sql: sql
            mock_exec.execute_query.return_value = mock_result
            mock_factory.return_value = mock_exec
            runner = BenchmarkRunner(cfg)
            yield runner, mock_exec

    def test_duckdb_power_benchmark_runs_all_queries(self, duckdb_runner):
        runner, mock_exec = duckdb_runner
        result = runner.run_power()

        assert result.mode == "power"
        assert len(result.queries) == 8
        assert mock_exec.execute_query.call_count == 8

    def test_duckdb_benchmark_calls_adapt_query(self, duckdb_runner):
        """adapt_query must be called for each query before execute_query."""
        runner, mock_exec = duckdb_runner
        runner.run_power()

        assert mock_exec.adapt_query.call_count == 8

    def test_duckdb_benchmark_adapt_before_execute(self, duckdb_runner):
        """adapt_query must be called BEFORE execute_query for each query."""
        runner, mock_exec = duckdb_runner

        # Track call order
        call_log = []
        mock_exec.adapt_query.side_effect = lambda sql: call_log.append(("adapt", sql[:30])) or sql
        original_execute = mock_exec.execute_query.return_value

        def track_execute(sql, **kwargs):
            call_log.append(("execute", sql[:30]))
            return original_execute

        mock_exec.execute_query.side_effect = track_execute

        runner.run_power()

        # Verify interleaving: adapt always comes before its execute
        for i in range(0, len(call_log), 2):
            assert call_log[i][0] == "adapt"
            assert call_log[i + 1][0] == "execute"

    def test_duckdb_qph_calculation(self, duckdb_runner):
        runner, mock_exec = duckdb_runner
        mock_exec.execute_query.return_value = QueryExecutorResult(
            sql="SELECT 1",
            engine="duckdb",
            duration_seconds=1.0,
            rows_returned=10,
            raw_output="...",
        )
        result = runner.run_power()

        # 8 queries * 1s each = 8s total
        assert result.total_seconds == pytest.approx(8.0)
        expected_qph = (8 / 8.0) * 3600  # 3600.0
        assert result.qph == pytest.approx(expected_qph)

    def test_duckdb_throughput_benchmark(self, duckdb_runner):
        runner, mock_exec = duckdb_runner
        result = runner.run_throughput(streams=2)

        assert result.mode == "throughput"
        assert result.streams == 2
        assert len(result.stream_results) == 2
        assert mock_exec.execute_query.call_count == 16  # 2 * 8

    def test_duckdb_composite_benchmark(self, duckdb_runner):
        runner, mock_exec = duckdb_runner
        power, throughput, composite = runner.run_composite(streams=2)

        assert power.mode == "power"
        assert throughput.mode == "throughput"
        assert composite.mode == "composite"
        expected = math.sqrt(power.qph * throughput.qph)
        assert composite.qph == pytest.approx(expected, rel=1e-6)

    def test_duckdb_cold_cache_calls_flush(self, duckdb_runner):
        runner, mock_exec = duckdb_runner
        runner.run_power(cache="cold")
        assert mock_exec.flush_cache.call_count == 8

    def test_duckdb_result_serialization(self, duckdb_runner):
        runner, _ = duckdb_runner
        result = runner.run_power()
        d = result.to_dict()

        assert d["mode"] == "power"
        assert d["benchmark_type"] == "trino_query"
        assert len(d["queries"]) == 8
        assert isinstance(d["qph"], float)
        assert isinstance(d["category_qph"], dict)

    def test_duckdb_execute_query_json_output(self, duckdb_executor):
        """DuckDB executor parses JSON output from kubectl exec."""
        output = json.dumps({"rows": 42, "data": ["(1,)", "(2,)"]})
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=output, stderr="")
            result = duckdb_executor.execute_query("SELECT * FROM t")

        assert result.success
        assert result.rows_returned == 42
        assert result.engine == "duckdb"

    def test_duckdb_execute_query_builds_correct_command(self, duckdb_executor):
        """Verify kubectl exec command structure."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='{"rows": 0, "data": []}',
                stderr="",
            )
            duckdb_executor.execute_query("SELECT 1")

            cmd = mock_run.call_args[0][0]
            assert cmd[0] == "kubectl"
            assert cmd[1] == "exec"
            assert cmd[2] == "lakebench-duckdb-abc"
            assert "-n" in cmd
            assert "test-ns" in cmd
            assert "python" in cmd
            assert "-c" in cmd


# ===========================================================================
# 2. DuckDB Health Check
# ===========================================================================


class TestDuckDBHealthCheck:
    """Tests for DuckDBExecutor.health_check()."""

    def test_health_check_success(self):
        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='{"rows": 1, "data": ["(1,)"]}',
                stderr="",
            )
            assert executor.health_check() is True

    def test_health_check_failure_nonzero(self):
        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="module not found: duckdb",
            )
            assert executor.health_check() is False

    def test_health_check_failure_exception(self):
        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = OSError("kubectl not found")
            assert executor.health_check() is False

    def test_health_check_failure_timeout(self):
        import subprocess

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="kubectl", timeout=15)
            assert executor.health_check() is False


# ===========================================================================
# 3. Trino and SparkThrift Health Checks (parity)
# ===========================================================================


class TestTrinoHealthCheck:
    def test_health_check_success(self):
        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "trino-0"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="1", stderr="")
            assert executor.health_check() is True

    def test_health_check_failure(self):
        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "trino-0"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="err")
            assert executor.health_check() is False


class TestSparkThriftHealthCheck:
    def test_health_check_success(self):
        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "spark-0"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="col\n1", stderr="")
            assert executor.health_check() is True

    def test_health_check_failure(self):
        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "spark-0"
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = OSError("not found")
            assert executor.health_check() is False


# ===========================================================================
# 4. Per-Engine Benchmark Runner Workflows
# ===========================================================================


class TestPerEngineBenchmarkWorkflow:
    """Verify the runner works correctly with each engine type."""

    @pytest.fixture(params=["hive-iceberg-trino", "hive-iceberg-spark", "hive-iceberg-duckdb"])
    def engine_runner(self, request):
        """Parametrized runner for each engine type."""
        recipe = request.param
        cfg = make_config(recipe=recipe)

        engine_map = {
            "hive-iceberg-trino": "trino",
            "hive-iceberg-spark": "spark-thrift",
            "hive-iceberg-duckdb": "duckdb",
        }
        engine_name = engine_map[recipe]

        mock_result = QueryExecutorResult(
            sql="SELECT 1",
            engine=engine_name,
            duration_seconds=0.5,
            rows_returned=10,
            raw_output="...",
        )

        with patch("lakebench.benchmark.executor.get_executor") as mock_factory:
            mock_exec = MagicMock()
            mock_exec.engine_name.return_value = engine_name
            mock_exec.catalog_name = "lakehouse"
            mock_exec.adapt_query.side_effect = lambda sql: sql
            mock_exec.execute_query.return_value = mock_result
            mock_factory.return_value = mock_exec
            runner = BenchmarkRunner(cfg)
            yield runner, mock_exec, engine_name

    def test_power_mode_all_engines(self, engine_runner):
        runner, mock_exec, engine_name = engine_runner
        result = runner.run_power()

        assert result.mode == "power"
        assert len(result.queries) == 8
        assert mock_exec.execute_query.call_count == 8
        assert mock_exec.adapt_query.call_count == 8

    def test_query_failure_doesnt_abort(self, engine_runner):
        runner, mock_exec, engine_name = engine_runner

        success = QueryExecutorResult(
            sql="SELECT 1",
            engine=engine_name,
            duration_seconds=0.5,
            rows_returned=10,
            raw_output="...",
        )
        failure = QueryExecutorResult(
            sql="SELECT 1",
            engine=engine_name,
            duration_seconds=1.0,
            rows_returned=0,
            raw_output="",
            error="Table not found",
        )
        mock_exec.execute_query.side_effect = [failure, *[success] * 7]

        result = runner.run_power()
        assert len(result.queries) == 8
        assert result.queries[0].success is False
        assert all(q.success for q in result.queries[1:])


# ===========================================================================
# 5. DuckDB Deployer Workflow
# ===========================================================================


class TestDuckDBDeployerWorkflow:
    """Tests deployer -> template render -> K8s apply workflow."""

    def test_deploy_renders_and_applies_templates(self):
        from lakebench.deploy.duckdb import DuckDBDeployer

        cfg = make_config(recipe="hive-iceberg-duckdb")
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False

        # Mock renderer to return valid YAML
        engine.renderer.render.return_value = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lakebench-duckdb
spec:
  replicas: 1
"""
        # Mock K8s apply
        engine.k8s.apply_manifest.return_value = True

        deployer = DuckDBDeployer(engine)

        # Mock _wait_for_ready
        with patch.object(deployer, "_wait_for_ready"):
            result = deployer.deploy()

        assert result.status == DeploymentStatus.SUCCESS
        assert engine.renderer.render.call_count == 2  # deployment + service
        assert engine.k8s.apply_manifest.call_count >= 1

    def test_deploy_failure_returns_failed_status(self):
        from lakebench.deploy.duckdb import DuckDBDeployer

        cfg = make_config(recipe="hive-iceberg-duckdb")
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        engine.renderer.render.side_effect = Exception("Template error")

        deployer = DuckDBDeployer(engine)
        result = deployer.deploy()

        assert result.status == DeploymentStatus.FAILED
        assert "Template error" in result.message

    def test_full_recipe_to_deployer_skip_guard(self):
        """All 10 recipes: DuckDB deployer only proceeds for duckdb recipes."""
        from lakebench.deploy.duckdb import DuckDBDeployer

        duckdb_recipes = {"hive-iceberg-duckdb", "polaris-iceberg-duckdb"}
        all_recipes = [
            "hive-iceberg-trino",
            "hive-iceberg-spark",
            "hive-iceberg-duckdb",
            "hive-iceberg-none",
            "hive-delta-trino",
            "hive-delta-none",
            "polaris-iceberg-trino",
            "polaris-iceberg-spark",
            "polaris-iceberg-duckdb",
            "polaris-iceberg-none",
        ]

        for recipe in all_recipes:
            cfg = make_config(recipe=recipe)
            engine = MagicMock()
            engine.config = cfg
            deployer = DuckDBDeployer(engine)
            result = deployer.deploy()

            if recipe in duckdb_recipes:
                # Should proceed (dry_run mock returns SUCCESS)
                assert result.status in (DeploymentStatus.SUCCESS, DeploymentStatus.FAILED), (
                    f"Recipe {recipe} should not be skipped"
                )
            else:
                assert result.status == DeploymentStatus.SKIPPED, (
                    f"Recipe {recipe} should be skipped for DuckDB"
                )


# ===========================================================================
# 6. Observability Deployer Workflow
# ===========================================================================


class TestObservabilityDeployerWorkflow:
    """Tests for ObservabilityDeployer lifecycle."""

    def test_deploy_builds_correct_helm_values(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(
            observability={
                "enabled": True,
                "retention": "14d",
                "storage": "20Gi",
                "dashboards_enabled": False,
            }
        )
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)

        values = deployer._build_helm_values("test-ns")
        assert values["prometheus.prometheusSpec.retention"] == "14d"
        assert (
            "20Gi"
            in values[
                "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage"
            ]
        )
        assert values["grafana.enabled"] == "false"

    def test_deploy_calls_helm_upgrade_install(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False

        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
            result = deployer.deploy()

        assert result.status == DeploymentStatus.SUCCESS
        # Should have called helm repo add, helm repo update, helm upgrade --install
        assert mock_run.call_count == 3

    def test_deploy_helm_failure(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False

        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            # repo add/update succeed, install fails
            mock_run.side_effect = [
                MagicMock(returncode=0),  # repo add
                MagicMock(returncode=0),  # repo update
                MagicMock(returncode=1, stderr="chart not found", stdout=""),  # install
            ]
            result = deployer.deploy()

        assert result.status == DeploymentStatus.FAILED
        assert "chart not found" in result.message

    def test_destroy_calls_helm_uninstall(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False

        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = deployer.destroy()

        assert result.status == DeploymentStatus.SUCCESS
        cmd = mock_run.call_args[0][0]
        assert "helm" in cmd
        assert "uninstall" in cmd

    def test_destroy_release_not_found_is_success(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False

        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="Error: release not found",
            )
            result = deployer.destroy()

        # "not found" should be treated as success (already cleaned up)
        assert result.status == DeploymentStatus.SUCCESS


# ===========================================================================
# 7. Platform Metrics Collection Workflow
# ===========================================================================


class TestPlatformMetricsWorkflow:
    """Tests for PlatformCollector -> PlatformMetrics -> Report."""

    def test_platform_metrics_serialization_roundtrip(self):
        """PlatformMetrics -> to_dict -> report section."""
        now = datetime.utcnow()
        metrics = PlatformMetrics(
            start_time=now - timedelta(minutes=10),
            end_time=now,
            pods=[
                PodMetrics(
                    pod_name="lakebench-duckdb-abc",
                    component="duckdb",
                    cpu_avg_cores=1.5,
                    cpu_max_cores=2.0,
                    memory_avg_bytes=2 * 1024**3,
                    memory_max_bytes=3 * 1024**3,
                ),
                PodMetrics(
                    pod_name="lakebench-postgres-0",
                    component="postgres",
                    cpu_avg_cores=0.2,
                    cpu_max_cores=0.5,
                    memory_avg_bytes=512 * 1024**2,
                    memory_max_bytes=1024 * 1024**2,
                ),
            ],
            s3_requests_total=250,
            s3_errors_total=3,
            s3_avg_latency_ms=8.5,
        )

        d = metrics.to_dict()
        assert d["duration_seconds"] == pytest.approx(600.0)
        assert len(d["pods"]) == 2
        assert d["pods"][0]["component"] == "duckdb"
        assert d["s3_requests_total"] == 250

        # Feed into report generator
        gen = ReportGenerator()
        html = gen._generate_platform_section(d)
        assert "Platform Metrics" in html
        assert "duckdb" in html
        assert "postgres" in html
        assert "250" in html

    def test_platform_metrics_with_collection_error(self):
        now = datetime.utcnow()
        metrics = PlatformMetrics(
            start_time=now - timedelta(minutes=5),
            end_time=now,
            collection_error="Prometheus unreachable at http://localhost:9090",
        )

        d = metrics.to_dict()
        gen = ReportGenerator()
        html = gen._generate_platform_section(d)
        assert "Prometheus unreachable" in html

    def test_empty_platform_metrics_returns_empty_section(self):
        now = datetime.utcnow()
        metrics = PlatformMetrics(
            start_time=now,
            end_time=now,
        )

        d = metrics.to_dict()
        gen = ReportGenerator()
        html = gen._generate_platform_section(d)
        assert html == ""  # no pods, no error -> empty


# ===========================================================================
# 8. S3MetricsWrapper Workflow
# ===========================================================================


class TestS3MetricsWrapperWorkflow:
    """Functional tests for S3MetricsWrapper integration."""

    def test_wrapper_proxies_all_public_methods(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": [{"Name": "test"}]}
        mock_client.head_object.return_value = {"ContentLength": 100}

        wrapper = S3MetricsWrapper(mock_client, enabled=False)

        result = wrapper.list_buckets()
        assert result == {"Buckets": [{"Name": "test"}]}
        mock_client.list_buckets.assert_called_once()

        wrapper.head_object(Bucket="test", Key="file.txt")
        mock_client.head_object.assert_called_once_with(Bucket="test", Key="file.txt")

    def test_wrapper_reraises_exceptions(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.get_object.side_effect = RuntimeError("access denied")

        wrapper = S3MetricsWrapper(mock_client, enabled=False)

        with pytest.raises(RuntimeError, match="access denied"):
            wrapper.get_object(Bucket="test", Key="secret.txt")

    def test_wrapper_does_not_wrap_private_methods(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client._internal_method = "raw_value"

        wrapper = S3MetricsWrapper(mock_client, enabled=False)
        assert wrapper._internal_method == "raw_value"


# ===========================================================================
# 9. Full Config -> Executor -> Runner Pipeline
# ===========================================================================


class TestConfigToRunnerPipeline:
    """Tests that exercise the config -> get_executor -> BenchmarkRunner chain."""

    def test_duckdb_config_to_executor_to_runner(self):
        """Config with DuckDB recipe produces a working runner."""
        cfg = make_config(recipe="hive-iceberg-duckdb")

        with patch("lakebench.benchmark.executor.get_executor") as mock_factory:
            mock_exec = MagicMock()
            mock_exec.engine_name.return_value = "duckdb"
            mock_exec.catalog_name = "lakehouse"
            mock_exec.adapt_query.side_effect = lambda sql: sql
            mock_exec.execute_query.return_value = QueryExecutorResult(
                sql="SELECT 1",
                engine="duckdb",
                duration_seconds=0.1,
                rows_returned=1,
                raw_output='{"rows": 1, "data": []}',
            )
            mock_factory.return_value = mock_exec

            runner = BenchmarkRunner(cfg)
            assert runner.executor == mock_exec
            assert runner.catalog == "lakehouse"

            result = runner.run_power()
            assert result.mode == "power"
            assert len(result.queries) == 8

    def test_polaris_duckdb_config_creates_executor(self):
        """polaris-iceberg-duckdb recipe creates DuckDBExecutor with correct S3 config."""
        cfg = make_config(
            recipe="polaris-iceberg-duckdb",
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "https://fb.example.com",
                        "access_key": "ak",
                        "secret_key": "sk",
                        "region": "eu-west-1",
                        "path_style": False,
                    }
                }
            },
        )

        executor = get_executor(cfg, namespace="polaris-ns")
        assert isinstance(executor, DuckDBExecutor)
        assert executor.s3_endpoint == "https://fb.example.com"
        assert executor.s3_region == "eu-west-1"
        assert executor.s3_path_style is False

    def test_run_method_dispatches_to_mode(self):
        """runner.run() dispatches based on config benchmark mode."""
        cfg = make_config(recipe="hive-iceberg-duckdb")

        with patch("lakebench.benchmark.executor.get_executor") as mock_factory:
            mock_exec = MagicMock()
            mock_exec.engine_name.return_value = "duckdb"
            mock_exec.catalog_name = "lakehouse"
            mock_exec.adapt_query.side_effect = lambda sql: sql
            mock_exec.execute_query.return_value = QueryExecutorResult(
                sql="SELECT 1",
                engine="duckdb",
                duration_seconds=0.1,
                rows_returned=1,
                raw_output="...",
            )
            mock_factory.return_value = mock_exec

            runner = BenchmarkRunner(cfg)
            result = runner.run(mode="power")
            assert isinstance(result, type(runner.run_power()))


# ===========================================================================
# 10. DuckDB Template Context Propagation
# ===========================================================================


class TestDuckDBTemplateContext:
    """Verify DuckDB template context variables are set correctly."""

    def test_duckdb_context_in_engine(self):
        """DeploymentEngine._build_context includes duckdb fields for duckdb recipes."""
        from lakebench.deploy.engine import DeploymentEngine

        cfg = make_config(recipe="hive-iceberg-duckdb")

        with patch("lakebench.k8s.client.K8sClient"):
            engine = DeploymentEngine(cfg, dry_run=True)
            ctx = engine.context

        assert "duckdb_cores" in ctx
        assert "duckdb_memory" in ctx
        assert "duckdb_memory_k8s" in ctx
        assert "duckdb_image" in ctx
        assert ctx["duckdb_cores"] == 2
        assert ctx["duckdb_memory"] == "4g"

    def test_duckdb_context_custom_resources(self):
        """Custom DuckDB resources propagate to template context."""
        from lakebench.deploy.engine import DeploymentEngine

        cfg = make_config(
            recipe="hive-iceberg-duckdb",
            architecture={
                "query_engine": {
                    "type": "duckdb",
                    "duckdb": {"cores": 4, "memory": "8g"},
                },
                "catalog": {"type": "hive"},
                "table_format": {"type": "iceberg"},
            },
        )

        with patch("lakebench.k8s.client.K8sClient"):
            engine = DeploymentEngine(cfg, dry_run=True)
            ctx = engine.context

        assert ctx["duckdb_cores"] == 4
        assert ctx["duckdb_memory"] == "8g"
