"""Tests for deployer modules (P2).

Covers:
- HiveDeployer: auto-install Stackable operators, skip, fail
- DuckDBDeployer: skip guard, dry-run, template rendering, _wait_for_ready
- ObservabilityDeployer: skip, dry-run, helm values
- DatagenDeployer: _parse_size_to_bytes, dry-run
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import make_config

# ===========================================================================
# HiveDeployer -- Stackable auto-install
# ===========================================================================


class TestHiveDeployerAutoInstall:
    """Tests for HiveDeployer._install_stackable_operators()."""

    def _make_deployer(self, install: bool = True, version: str = "25.7.0"):
        from lakebench.deploy.hive import HiveDeployer

        cfg = make_config(
            architecture={
                "catalog": {"hive": {"operator": {"install": install, "version": version}}}
            }
        )
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        deployer = HiveDeployer(engine)
        return deployer

    @patch("lakebench.deploy.hive.time.sleep")
    @patch("subprocess.run")
    def test_install_runs_four_helm_commands(self, mock_run, _sleep):
        deployer = self._make_deployer()
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        deployer._is_stackable_available = MagicMock(side_effect=[True])

        result = deployer._install_stackable_operators()
        assert result is True
        assert mock_run.call_count == 4

        # Verify install order
        cmds = [c.args[0] for c in mock_run.call_args_list]
        assert cmds[0][2] == "commons-operator"
        assert cmds[1][2] == "listener-operator"
        assert cmds[2][2] == "secret-operator"
        assert cmds[3][2] == "hive-operator"

    @patch("lakebench.deploy.hive.time.sleep")
    @patch("subprocess.run")
    def test_install_first_command_has_create_namespace(self, mock_run, _sleep):
        deployer = self._make_deployer()
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        deployer._is_stackable_available = MagicMock(side_effect=[True])

        deployer._install_stackable_operators()

        first_cmd = mock_run.call_args_list[0].args[0]
        assert "--create-namespace" in first_cmd
        # Others should not have --create-namespace
        for c in mock_run.call_args_list[1:]:
            assert "--create-namespace" not in c.args[0]

    @patch("lakebench.deploy.hive.time.sleep")
    @patch("subprocess.run")
    def test_install_uses_configured_version(self, mock_run, _sleep):
        deployer = self._make_deployer(version="24.3.0")
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        deployer._is_stackable_available = MagicMock(side_effect=[True])

        deployer._install_stackable_operators()

        for c in mock_run.call_args_list:
            cmd = c.args[0]
            idx = cmd.index("--version")
            assert cmd[idx + 1] == "24.3.0"

    @patch("lakebench.deploy.hive.time.sleep")
    @patch("subprocess.run")
    def test_install_skips_already_installed(self, mock_run, _sleep):
        deployer = self._make_deployer()
        # First two succeed, third is already installed, fourth succeeds
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),
            MagicMock(returncode=0, stdout="", stderr=""),
            MagicMock(returncode=1, stdout="", stderr="cannot re-use a name that is still in use"),
            MagicMock(returncode=0, stdout="", stderr=""),
        ]
        deployer._is_stackable_available = MagicMock(side_effect=[True])

        result = deployer._install_stackable_operators()
        assert result is True

    @patch("lakebench.deploy.hive.time.sleep")
    @patch("subprocess.run")
    def test_install_fails_on_helm_error(self, mock_run, _sleep):
        deployer = self._make_deployer()
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Error: chart not found")

        result = deployer._install_stackable_operators()
        assert result is False

    @patch("lakebench.deploy.hive.time.sleep")
    @patch("subprocess.run")
    def test_install_fails_on_timeout(self, mock_run, _sleep):
        import subprocess as sp

        deployer = self._make_deployer()
        mock_run.side_effect = sp.TimeoutExpired(cmd="helm", timeout=120)

        result = deployer._install_stackable_operators()
        assert result is False

    @patch("lakebench.deploy.hive.time.sleep")
    @patch("subprocess.run")
    def test_install_fails_helm_not_found(self, mock_run, _sleep):
        deployer = self._make_deployer()
        mock_run.side_effect = FileNotFoundError("helm")

        result = deployer._install_stackable_operators()
        assert result is False


class TestHiveDeployerDeploy:
    """Tests for HiveDeployer.deploy() auto-install integration."""

    def _make_deployer(self, install: bool = False):
        from lakebench.deploy.hive import HiveDeployer

        cfg = make_config(architecture={"catalog": {"hive": {"operator": {"install": install}}}})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        deployer = HiveDeployer(engine)
        return deployer

    def test_deploy_skips_when_not_hive(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.hive import HiveDeployer

        cfg = make_config(recipe="polaris-iceberg-spark-trino")
        engine = MagicMock()
        engine.config = cfg
        deployer = HiveDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED

    def test_deploy_dry_run(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.hive import HiveDeployer

        cfg = make_config()
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = HiveDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would deploy" in result.message

    def test_deploy_fails_without_stackable_install_false(self):
        from lakebench.deploy.engine import DeploymentStatus

        deployer = self._make_deployer(install=False)
        deployer._is_stackable_available = MagicMock(return_value=False)
        deployer._check_stackable_crds = MagicMock(
            return_value={
                "hiveclusters.hive.stackable.tech": False,
                "secretclasses.secrets.stackable.tech": False,
            }
        )

        result = deployer.deploy()
        assert result.status == DeploymentStatus.FAILED
        assert "Option 1" in result.message
        assert "Option 2" in result.message

    @patch("lakebench.deploy.hive.time.sleep")
    @patch("subprocess.run")
    def test_deploy_auto_installs_when_install_true(self, mock_run, _sleep):
        from lakebench.deploy.engine import DeploymentStatus

        deployer = self._make_deployer(install=True)
        # First call: not available; after install: available
        deployer._is_stackable_available = MagicMock(side_effect=[False, True])
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        # Mock _deploy_stackable to avoid K8s calls
        deployer._deploy_stackable = MagicMock(
            return_value=MagicMock(
                status=DeploymentStatus.SUCCESS,
            )
        )

        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert mock_run.call_count == 4  # Four helm install calls


# ===========================================================================
# DuckDBDeployer
# ===========================================================================


class TestDuckDBDeployerSkip:
    """Tests for DuckDBDeployer skip guard."""

    def test_skip_when_trino(self):
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        cfg = make_config(recipe="hive-iceberg-spark-trino")
        engine = MagicMock()
        engine.config = cfg
        deployer = DuckDBDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED
        assert "duckdb" in result.component

    def test_skip_when_spark_thrift(self):
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deploy.engine import DeploymentStatus

        cfg = make_config(recipe="hive-iceberg-spark-thrift")
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

        cfg = make_config(recipe="hive-iceberg-spark-duckdb")
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

        cfg = make_config(recipe="hive-iceberg-spark-duckdb")
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

        cfg = make_config(recipe="hive-iceberg-spark-duckdb")
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
