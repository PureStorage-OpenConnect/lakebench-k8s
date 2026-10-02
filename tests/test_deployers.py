"""Tests for deployer modules (P2).

Covers:
- HiveDeployer: skip, and fail naming admin install when Stackable is missing
- DuckDBDeployer: skip guard, dry-run, template rendering, _wait_for_ready
- ObservabilityDeployer: skip, dry-run, helm values
- DatagenDeployer: _parse_size_to_bytes, dry-run
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import make_config

# ===========================================================================
# HiveDeployer -- deploy verifies Stackable, never installs it
# (the install moved to deploy/shared_components.py; tests in
# tests/test_shared_components.py)
# ===========================================================================


class TestHiveDeployerDeploy:
    """Tests for HiveDeployer.deploy()."""

    def _make_deployer(self):
        from lakebench.deploy.hive import HiveDeployer

        cfg = make_config()
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

    @patch("subprocess.run")
    def test_deploy_fails_naming_admin_install_and_runs_no_helm(self, mock_run):
        """DEP-3: a missing Stackable fails the step with the admin command,
        and deploy never installs it. Reverted (the v1.6 auto-install), the
        step ran four helm installs."""
        from lakebench.deploy.engine import DeploymentStatus

        deployer = self._make_deployer()
        deployer._is_stackable_available = MagicMock(return_value=False)
        deployer._check_stackable_crds = MagicMock(
            return_value={
                "hiveclusters.hive.stackable.tech": False,
                "secretclasses.secrets.stackable.tech": True,
            }
        )
        deployer._deploy_stackable = MagicMock()

        result = deployer.deploy()
        assert result.status == DeploymentStatus.FAILED
        assert "lakebench admin install --component stackable" in result.message
        assert "hive-operator" in result.message
        assert "helm install" not in result.message
        mock_run.assert_not_called()
        deployer._deploy_stackable.assert_not_called()

    def test_deploy_uses_stackable_when_present(self):
        from lakebench.deploy.engine import DeploymentStatus

        deployer = self._make_deployer()
        deployer._is_stackable_available = MagicMock(return_value=True)
        deployer._deploy_stackable = MagicMock(
            return_value=MagicMock(status=DeploymentStatus.SUCCESS)
        )
        assert deployer.deploy().status == DeploymentStatus.SUCCESS
        deployer._deploy_stackable.assert_called_once()


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
    """DuckDBDeployer._wait_for_ready(): rolled out, on this deploy's set."""

    def _seed(self, rec, *, ready: int, pinset: str):
        from kubernetes.client.models import V1DeploymentStatus

        labels = {"app.kubernetes.io/name": "lakebench", "app.kubernetes.io/component": "duckdb"}
        rec.add_namespace("test-ns")
        rec.add(
            "deployments",
            {
                "metadata": {"name": "lakebench-duckdb", "generation": 1},
                "spec": {"replicas": 1, "selector": {"matchLabels": labels}, "template": {}},
            },
            namespace="test-ns",
        )
        rec.store[("deployments", "test-ns", "lakebench-duckdb")].status = V1DeploymentStatus(
            observed_generation=1, replicas=1, updated_replicas=1, ready_replicas=ready
        )
        rec.add(
            "pods",
            {
                "metadata": {
                    "name": "lakebench-duckdb-1",
                    "labels": labels,
                    "annotations": {"lakebench.io/deps-set": pinset},
                },
                "status": {"phase": "Running"},
            },
            namespace="test-ns",
        )

    def _deployer(self):
        from lakebench.deploy.duckdb import DuckDBDeployer
        from lakebench.deps.manifest import placeholder_handle

        cfg = make_config(recipe="hive-iceberg-spark-duckdb")
        engine = MagicMock()
        engine.config = cfg
        engine.deps = placeholder_handle(cfg)
        return DuckDBDeployer(engine), engine.deps.pinset_sha256

    def test_wait_success(self, recording_k8s):
        deployer, pinset = self._deployer()
        self._seed(recording_k8s, ready=1, pinset=pinset)
        deployer._wait_for_ready("test-ns", timeout_seconds=5)

    def test_wait_timeout_on_another_set(self, recording_k8s):
        """A Ready pod on the old set is not the deploy's pod."""
        deployer, _ = self._deployer()
        self._seed(recording_k8s, ready=1, pinset="old" * 21 + "x")
        with patch("lakebench.k8s.wait.time.sleep"):
            with pytest.raises(RuntimeError, match="0/1 on the set"):
                deployer._wait_for_ready("test-ns", timeout_seconds=1)


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
        assert "Would check the shared observability stack" in result.message

    def test_destroy_dry_run(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = ObservabilityDeployer(engine)
        result = deployer.destroy()
        # The shared stack is never removed by destroy, dry run or not.
        assert result.status == DeploymentStatus.SKIPPED
        assert "left in place" in result.message

    def test_helm_values_retention_and_storage(self):
        from lakebench.deploy.observability import build_helm_values

        cfg = make_config(observability={"enabled": True, "retention": "14d", "storage": "20Gi"})
        values = build_helm_values(cfg.observability)
        assert values["prometheus.prometheusSpec.retention"] == "14d"
        assert (
            "20Gi"
            in values[
                "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate"
                ".spec.resources.requests.storage"
            ]
        )

    def test_helm_values_grafana_disabled(self):
        from lakebench.deploy.observability import build_helm_values

        cfg = make_config(observability={"enabled": True, "dashboards_enabled": False})
        values = build_helm_values(cfg.observability)
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


class TestDatagenDeployerSchemaWireThrough:
    """Confirms the workload.schema value reaches the K8s Job's argv.

    Regression: prior to this test, --schema was never wired through the
    template, so financial deploys silently ran the Customer 360 generator
    against financial S3 buckets. Caught in UAT.
    """

    def test_customer360_passes_customer360_schema(self):
        from lakebench.deploy.datagen import DatagenDeployer

        cfg = make_config()  # default schema = customer360
        engine = MagicMock()
        engine.config = cfg
        engine.context = {}
        deployer = DatagenDeployer(engine)
        context = deployer._build_datagen_context()
        assert context["datagen_schema"] == "customer360"
        assert context["datagen_path_prefix"] == "customer/interactions"

    def test_financial_passes_financial_schema_and_pacs008_prefix(self):
        from lakebench.deploy.datagen import DatagenDeployer

        cfg = make_config(
            architecture={
                "workload": {"schema": "financial"},
            }
        )
        engine = MagicMock()
        engine.config = cfg
        engine.context = {}
        deployer = DatagenDeployer(engine)
        context = deployer._build_datagen_context()
        assert context["datagen_schema"] == "financial"
        # Default path_template is the C360 default; when schema=financial
        # the deployer must substitute the pacs.008 prefix to match
        # bronze_verify_financial's LB_FINANCIAL_BRONZE_PREFIX default.
        assert context["datagen_path_prefix"] == "pacs008"

    def test_financial_custom_path_template_no_longer_applies(self):
        # v1.7 removed medallion.bronze.path_template: the layout is fixed per
        # workload (load_config refuses a custom one for the commands that
        # change data; built directly the block is dropped with a warning).
        from lakebench.deploy.datagen import DatagenDeployer

        with pytest.warns(DeprecationWarning, match="'medallion'"):
            cfg = make_config(
                architecture={
                    "workload": {"schema": "financial"},
                    "pipeline": {
                        "mode": "batch",
                        "medallion": {
                            "bronze": {"format": "parquet", "path_template": "custom/pacs"}
                        },
                    },
                }
            )
        engine = MagicMock()
        engine.config = cfg
        engine.context = {}
        deployer = DatagenDeployer(engine)
        context = deployer._build_datagen_context()
        assert context["datagen_path_prefix"] == "pacs008"
