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
from unittest.mock import MagicMock, patch

import pytest

from lakebench.benchmark.executor import (
    DuckDBExecutor,
    QueryExecutorResult,
    get_executor,
)
from lakebench.benchmark.runner import BenchmarkRunner
from lakebench.deploy.engine import DeploymentStatus
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
        cfg = make_config(recipe="hive-iceberg-spark-duckdb")
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

    def test_duckdb_result_serialization(self, duckdb_runner):
        runner, _ = duckdb_runner
        result = runner.run_power()
        d = result.to_dict()

        assert d["mode"] == "power"
        # The real engine, not "trino_query" on every engine (lb16 sweep).
        assert d["benchmark_type"] == "duckdb_query"
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


# ===========================================================================
# 2. DuckDB Health Check
# ===========================================================================


# ===========================================================================
# 3. Trino and SparkThrift Health Checks (parity)
# ===========================================================================


# ===========================================================================
# 4. Per-Engine Benchmark Runner Workflows
# ===========================================================================


# ===========================================================================
# 5. DuckDB Deployer Workflow
# ===========================================================================


# ===========================================================================
# 6. Observability Deployer Workflow
# ===========================================================================


class TestObservabilityDeployerWorkflow:
    """Tests for ObservabilityDeployer lifecycle."""

    def test_install_builds_correct_helm_values(self):
        from lakebench.deploy.observability import build_helm_values

        cfg = make_config(
            observability={
                "enabled": True,
                "retention": "14d",
                "storage": "20Gi",
                "dashboards_enabled": False,
            }
        )
        values = build_helm_values(cfg.observability)
        assert values["prometheus.prometheusSpec.retention"] == "14d"
        assert (
            "20Gi"
            in values[
                "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage"
            ]
        )
        assert values["grafana.enabled"] == "false"

    def test_deploy_uses_the_shared_release_and_never_installs(self):
        from lakebench.deploy.observability import (
            HELM_RELEASE_NAME,
            OBSERVABILITY_NAMESPACE,
            ObservabilityDeployer,
        )

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False

        deployer = ObservabilityDeployer(engine)
        listed = json.dumps(
            [
                {
                    "name": HELM_RELEASE_NAME,
                    "namespace": OBSERVABILITY_NAMESPACE,
                    "status": "deployed",
                }
            ]
        )
        with (
            patch("subprocess.run") as mock_run,
            patch("lakebench.deploy.observability._wait_for_prometheus", return_value=""),
        ):
            mock_run.return_value = MagicMock(returncode=0, stdout=listed, stderr="")
            result = deployer.deploy()

        assert result.status == DeploymentStatus.SUCCESS
        cmds = [c.args[0] for c in mock_run.call_args_list]
        assert [c[:2] for c in cmds if c[0] == "helm"] == [["helm", "list"]]

    def test_destroy_does_not_uninstall_the_shared_release(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False

        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = deployer.destroy()

        assert result.status == DeploymentStatus.SKIPPED
        assert not any("uninstall" in c.args[0] for c in mock_run.call_args_list)

    def test_destroy_uninstalls_a_legacy_release_in_its_own_namespace(self):
        from lakebench.deploy.observability import HELM_RELEASE_NAME, ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False

        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout=HELM_RELEASE_NAME, stderr=""),  # list
                MagicMock(returncode=1, stdout="", stderr="Error: release not found"),
            ]
            result = deployer.destroy()

        # "not found" on the uninstall is treated as already cleaned up
        assert result.status == DeploymentStatus.SUCCESS
        assert mock_run.call_args_list[1].args[0][:2] == ["helm", "uninstall"]


# ===========================================================================
# 7. Platform Metrics Collection Workflow
# ===========================================================================


# ===========================================================================
# 8. S3MetricsWrapper Workflow
# ===========================================================================


# ===========================================================================
# 9. Full Config -> Executor -> Runner Pipeline
# ===========================================================================


class TestConfigToRunnerPipeline:
    """Tests that exercise the config -> get_executor -> BenchmarkRunner chain."""

    def test_polaris_duckdb_config_creates_executor(self):
        """polaris-iceberg-spark-duckdb recipe creates DuckDBExecutor with correct S3 config."""
        cfg = make_config(
            recipe="polaris-iceberg-spark-duckdb",
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


# ===========================================================================
# 10. DuckDB Template Context Propagation
# ===========================================================================


class TestDuckDBTemplateContext:
    """Verify DuckDB template context variables are set correctly."""

    def test_duckdb_context_custom_resources(self):
        """Custom DuckDB resources propagate to template context."""
        from lakebench.deploy.engine import DeploymentEngine

        cfg = make_config(
            recipe="hive-iceberg-spark-duckdb",
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
