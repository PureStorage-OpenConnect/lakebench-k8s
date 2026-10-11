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

    @patch("subprocess.run")
    def test_missing_stackable_fails_and_installs_nothing(self, mock_run):
        """A missing Stackable fails the step; deploy never installs it."""
        from lakebench.deploy.engine import DeploymentStatus

        deployer = self._make_deployer()
        deployer._check_stackable_crds_raw = MagicMock(
            return_value={
                "hiveclusters.hive.stackable.tech": False,
                "secretclasses.secrets.stackable.tech": True,
            }
        )

        result = deployer.deploy()
        assert result.status == DeploymentStatus.FAILED
        mock_run.assert_not_called()
        deployer.k8s.apply_manifest.assert_not_called()


# ===========================================================================
# DuckDBDeployer
# ===========================================================================


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

    @pytest.mark.parametrize("on_this_set", [True, False])
    def test_wait_requires_the_deploys_own_set(self, recording_k8s, on_this_set):
        """A Ready pod on another set is not the deploy's pod."""
        deployer, pinset = self._deployer()
        self._seed(recording_k8s, ready=1, pinset=pinset if on_this_set else "old" * 21 + "x")
        with patch("lakebench.k8s.wait.time.sleep"):
            if on_this_set:
                deployer._wait_for_ready("test-ns", timeout_seconds=1)
            else:
                with pytest.raises(RuntimeError):
                    deployer._wait_for_ready("test-ns", timeout_seconds=1)


# ===========================================================================
# ObservabilityDeployer
# ===========================================================================


class TestObservabilityDeployerSkip:
    """Tests for ObservabilityDeployer skip/deploy/destroy logic."""

    def test_destroy_never_removes_the_shared_stack(self, recording_k8s):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        recording_k8s.for_config(cfg)
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        result = ObservabilityDeployer(engine).destroy()
        assert result.status == DeploymentStatus.SKIPPED
        assert not recording_k8s.mutations()


# ===========================================================================
# DatagenDeployer
# ===========================================================================


class TestDatagenDeployerParseSizeToBytes:
    """Tests for DatagenDeployer._parse_size_to_bytes()."""

    @pytest.mark.parametrize(
        ("text", "size"),
        [
            ("100GB", 100 * 1024**3),
            ("1TB", 1024**4),
            ("512MB", 512 * 1024**2),
            ("1024KB", 1024 * 1024),
            ("1048576B", 1048576),
            ("4096", 4096),
        ],
    )
    def test_parse(self, text, size):
        from lakebench.deploy.datagen import DatagenDeployer

        engine = MagicMock()
        engine.config = make_config()
        assert DatagenDeployer(engine)._parse_size_to_bytes(text) == size


class TestDatagenDeployerSchemaWireThrough:
    """The workload.schema value reaches the K8s Job's argv."""

    @pytest.mark.parametrize(
        ("schema", "prefix"),
        [("customer360", "customer/interactions"), ("financial", "pacs008")],
    )
    def test_schema_and_bronze_prefix_reach_datagen(self, schema, prefix):
        """The financial prefix must match bronze_verify_financial's default."""
        from lakebench.deploy.datagen import DatagenDeployer

        cfg = make_config(architecture={"workload": {"schema": schema}})
        engine = MagicMock()
        engine.config = cfg
        engine.context = {}
        context = DatagenDeployer(engine)._build_datagen_context()
        assert context["datagen_schema"] == schema
        assert context["datagen_path_prefix"] == prefix
