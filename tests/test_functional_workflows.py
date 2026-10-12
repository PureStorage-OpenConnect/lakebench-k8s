"""Functional workflow tests: DuckDB benchmark and observability lifecycle with mocked infrastructure."""

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
# 6. Observability Deployer Workflow (per-namespace)
# ===========================================================================


class TestObservabilityDeployerWorkflow:
    """Tests for ObservabilityDeployer lifecycle."""

    def _engine(self, cfg):
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        engine.context = {"namespace": cfg.get_namespace(), "openshift_mode": False}
        engine.k8s = MagicMock()
        engine.renderer = MagicMock()
        engine.renderer.render.return_value = (
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n"
        )
        return engine

    def test_deploy_applies_per_namespace_templates(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(
            observability={
                "enabled": True,
                "retention": "14d",
                "storage": "20Gi",
                "dashboards_enabled": False,
            }
        )
        engine = self._engine(cfg)
        with (
            patch(
                "lakebench.deploy.observability.kubelet_cluster_role_exists",
                return_value=True,
            ),
            patch(
                "lakebench.deploy.observability._wait_for_deployment_ready",
                return_value="",
            ),
            patch(
                "lakebench.deploy.observability.read_grafana_password",
                return_value=None,
            ),
        ):
            result = ObservabilityDeployer(engine).deploy()

        assert result.status == DeploymentStatus.SUCCESS, result.message
        rendered = [c.args[0] for c in engine.renderer.render.call_args_list]
        # Dashboards disabled: dashboard template not rendered.
        assert "grafana/dashboard-configmap.yaml.j2" not in rendered
        assert "prometheus/prometheus.yaml.j2" in rendered
        # One of the apply contexts carries the retention + storage from the config.
        kwargs_lists = [
            c
            for c in engine.renderer.render.call_args_list
            if c.args[0] == "prometheus/prometheus.yaml.j2"
        ]
        ctx = kwargs_lists[0].args[1]
        assert ctx["prometheus_retention"] == "14d"
        assert ctx["prometheus_storage"] == "20Gi"

    def test_deploy_succeeds_when_the_role_is_missing(self):
        """Missing kubelet role is a warning, not a failure; stack still deploys."""
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = self._engine(cfg)
        with (
            patch(
                "lakebench.deploy.observability.kubelet_cluster_role_exists",
                return_value=False,
            ),
            patch(
                "lakebench.deploy.observability._wait_for_deployment_ready",
                return_value="",
            ),
            patch(
                "lakebench.deploy.observability.read_grafana_password",
                return_value=None,
            ),
        ):
            result = ObservabilityDeployer(engine).deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "not collected" in result.message

    def test_destroy_removes_the_per_deployment_binding(self):

        from lakebench.deploy.observability import (
            KUBELET_METRICS_CLUSTER_ROLE,
            ObservabilityDeployer,
        )

        cfg = make_config(name="dep-x", observability={"enabled": True})
        engine = self._engine(cfg)
        deleted: list[str] = []

        class FakeRbac:
            def delete_cluster_role_binding(self, name):
                deleted.append(name)

        with patch("kubernetes.client.RbacAuthorizationV1Api", return_value=FakeRbac()):
            result = ObservabilityDeployer(engine).destroy()
        assert result.status == DeploymentStatus.SUCCESS
        assert deleted == [f"{KUBELET_METRICS_CLUSTER_ROLE}-dep-x"]

    def test_destroy_tolerates_a_404(self):
        from kubernetes.client.rest import ApiException

        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = self._engine(cfg)

        class FakeRbac:
            def delete_cluster_role_binding(self, name):
                raise ApiException(status=404, reason="Not Found")

        with patch("kubernetes.client.RbacAuthorizationV1Api", return_value=FakeRbac()):
            result = ObservabilityDeployer(engine).destroy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "No kubelet binding" in result.message


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
