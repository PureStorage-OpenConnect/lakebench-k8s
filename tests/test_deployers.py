"""Tests for deployer modules (P2).

Covers:
- DuckDBDeployer: skip guard, dry-run, template rendering, _wait_for_ready
- ObservabilityDeployer: skip, dry-run, helm values
- DatagenDeployer: _parse_size_to_bytes, dry-run
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import make_config

# ===========================================================================
# DuckDBDeployer
# ===========================================================================


class TestDuckDBDeployerSkip:
    """Tests for DuckDBDeployer skip guard."""

    def test_skip_when_trino(self):
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        cfg = make_config(recipe="hive-iceberg-trino")
        engine = MagicMock()
        engine.config = cfg
        deployer = DuckDBDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED
        assert "duckdb" in result.component

    def test_skip_when_spark_thrift(self):
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        cfg = make_config(recipe="hive-iceberg-spark")
        engine = MagicMock()
        engine.config = cfg
        deployer = DuckDBDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED


class TestDuckDBDeployerDryRun:
    """Tests for DuckDBDeployer dry-run mode."""

    def test_dry_run_success(self):
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        cfg = make_config(recipe="hive-iceberg-duckdb")
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = DuckDBDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would deploy" in result.message


class TestDuckDBDeployerTemplates:
    """Tests for DuckDBDeployer template list."""

    def test_template_names(self):
        from lakebench.deploy.duckdb import DuckDBDeployer

        assert "duckdb/deployment.yaml.j2" in DuckDBDeployer.TEMPLATES
        assert "duckdb/service.yaml.j2" in DuckDBDeployer.TEMPLATES
        assert len(DuckDBDeployer.TEMPLATES) == 2


class TestDuckDBDeployerWaitForReady:
    """Tests for DuckDBDeployer._wait_for_ready()."""

    def test_wait_success(self):
        from lakebench.deploy.duckdb import DuckDBDeployer

        cfg = make_config(recipe="hive-iceberg-duckdb")
        engine = MagicMock()
        engine.config = cfg
        deployer = DuckDBDeployer(engine)

        mock_dep = MagicMock()
        mock_dep.status.ready_replicas = 1
        mock_dep.spec.replicas = 1

        with patch("kubernetes.client.AppsV1Api") as mock_api:
            mock_api.return_value.read_namespaced_deployment.return_value = mock_dep
            # Should not raise
            deployer._wait_for_ready("test-ns", timeout_seconds=5)

    def test_wait_timeout(self):
        from lakebench.deploy.duckdb import DuckDBDeployer

        cfg = make_config(recipe="hive-iceberg-duckdb")
        engine = MagicMock()
        engine.config = cfg
        deployer = DuckDBDeployer(engine)

        mock_dep = MagicMock()
        mock_dep.status.ready_replicas = 0
        mock_dep.spec.replicas = 1

        with patch("kubernetes.client.AppsV1Api") as mock_api:
            mock_api.return_value.read_namespaced_deployment.return_value = mock_dep
            with patch("time.sleep"):
                with patch("time.time") as mock_time:
                    # Simulate immediate timeout
                    mock_time.side_effect = [0, 0, 1]
                    with pytest.raises(RuntimeError, match="did not become ready"):
                        deployer._wait_for_ready("test-ns", timeout_seconds=0)


# ===========================================================================
# ObservabilityDeployer
# ===========================================================================


class TestObservabilityDeployerSkip:
    """Tests for ObservabilityDeployer skip/deploy/destroy logic."""

    def test_skip_when_disabled(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config()  # observability.enabled = False
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED

    def test_dry_run(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = ObservabilityDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would deploy" in result.message

    def test_destroy_dry_run(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = ObservabilityDeployer(engine)
        result = deployer.destroy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would destroy" in result.message

    def test_helm_values_retention_and_storage(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True, "retention": "14d", "storage": "20Gi"})
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)
        values = deployer._build_helm_values("test-ns")
        assert values["prometheus.prometheusSpec.retention"] == "14d"
        assert (
            "20Gi"
            in values[
                "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate"
                ".spec.resources.requests.storage"
            ]
        )

    def test_helm_values_grafana_disabled(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True, "dashboards_enabled": False})
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)
        values = deployer._build_helm_values("test-ns")
        assert values["grafana.enabled"] == "false"


# ===========================================================================
# DatagenDeployer
# ===========================================================================


class TestDatagenDeployerParseSizeToBytes:
    """Tests for DatagenDeployer._parse_size_to_bytes()."""

    def test_parse_gb(self):
        from lakebench.deploy.datagen import DatagenDeployer

        engine = MagicMock()
        engine.config = make_config()
        deployer = DatagenDeployer(engine)
        assert deployer._parse_size_to_bytes("100GB") == 100 * 1024**3

    def test_parse_tb(self):
        from lakebench.deploy.datagen import DatagenDeployer

        engine = MagicMock()
        engine.config = make_config()
        deployer = DatagenDeployer(engine)
        assert deployer._parse_size_to_bytes("1TB") == 1024**4

    def test_parse_mb(self):
        from lakebench.deploy.datagen import DatagenDeployer

        engine = MagicMock()
        engine.config = make_config()
        deployer = DatagenDeployer(engine)
        assert deployer._parse_size_to_bytes("512MB") == 512 * 1024**2

    def test_parse_kb(self):
        from lakebench.deploy.datagen import DatagenDeployer

        engine = MagicMock()
        engine.config = make_config()
        deployer = DatagenDeployer(engine)
        assert deployer._parse_size_to_bytes("1024KB") == 1024 * 1024

    def test_parse_bytes(self):
        from lakebench.deploy.datagen import DatagenDeployer

        engine = MagicMock()
        engine.config = make_config()
        deployer = DatagenDeployer(engine)
        assert deployer._parse_size_to_bytes("1048576B") == 1048576

    def test_parse_plain_number(self):
        from lakebench.deploy.datagen import DatagenDeployer

        engine = MagicMock()
        engine.config = make_config()
        deployer = DatagenDeployer(engine)
        assert deployer._parse_size_to_bytes("4096") == 4096


class TestDatagenDeployerDryRun:
    """Tests for DatagenDeployer dry-run."""

    def test_deploy_dry_run(self):
        from lakebench.deploy.datagen import DatagenDeployer
        from lakebench.deploy.engine import DeploymentStatus

        cfg = make_config()
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = DatagenDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would deploy" in result.message
