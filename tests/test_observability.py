"""Tests for the observability package (P0 + P2).

Covers:
- PlatformCollector: collect(), _query_range(), _query_instant(), _infer_component()
- S3MetricsWrapper: enabled/disabled paths, error recording, proxy behavior
- ObservabilityDeployer deploy/destroy paths; build_helm_values() and the admin
  install component's prepare() and install()
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import make_config

# ===========================================================================
# PlatformCollector
# ===========================================================================


class TestPlatformCollectorCollect:
    """Tests for PlatformCollector.collect() method."""

    def test_collect_successful_cpu_memory(self):
        """collect() populates pods with CPU and memory from Prometheus."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)

        cpu_response = MagicMock()
        cpu_response.status_code = 200
        cpu_response.json.return_value = {
            "data": {
                "result": [
                    {
                        "metric": {"pod": "lakebench-trino-coordinator-0"},
                        "values": [[1, "1.5"], [2, "2.0"]],
                    }
                ]
            }
        }

        mem_response = MagicMock()
        mem_response.status_code = 200
        mem_response.json.return_value = {
            "data": {
                "result": [
                    {
                        "metric": {"pod": "lakebench-trino-coordinator-0"},
                        "values": [[1, "2000000000"], [2, "3000000000"]],
                    }
                ]
            }
        }

        s3_total_response = MagicMock()
        s3_total_response.status_code = 200
        s3_total_response.json.return_value = {"data": {"result": [{"value": [1, "42"]}]}}

        s3_errors_response = MagicMock()
        s3_errors_response.status_code = 200
        s3_errors_response.json.return_value = {"data": {"result": [{"value": [1, "2"]}]}}

        mock_httpx = MagicMock()
        mock_client = MagicMock()
        mock_httpx.Client.return_value = mock_client
        # Engine metrics queries (5 instant queries, all return empty)
        engine_empty = MagicMock()
        engine_empty.status_code = 200
        engine_empty.json.return_value = {"data": {"result": []}}

        mock_client.get.side_effect = [
            cpu_response,
            mem_response,
            s3_total_response,
            s3_errors_response,
            engine_empty,  # spark GC
            engine_empty,  # spark shuffle read
            engine_empty,  # spark shuffle write
            engine_empty,  # trino completed
            engine_empty,  # trino failed
        ]

        with patch.dict("sys.modules", {"httpx": mock_httpx}):
            metrics = collector.collect(start, end)

        assert metrics.collection_error is None
        assert len(metrics.pods) == 1
        assert metrics.pods[0].pod_name == "lakebench-trino-coordinator-0"
        assert metrics.pods[0].component == "trino-coordinator"
        assert metrics.pods[0].cpu_avg_cores == pytest.approx(1.75, rel=0.01)
        assert metrics.pods[0].cpu_max_cores == 2.0
        assert metrics.pods[0].memory_avg_bytes == pytest.approx(2.5e9, rel=0.01)
        assert metrics.pods[0].memory_max_bytes == 3e9
        assert metrics.s3_requests_total == 42
        assert metrics.s3_errors_total == 2


class TestInferComponent:
    """Tests for PlatformCollector._infer_component() static method."""

    @pytest.mark.parametrize(
        ("pod", "component"),
        [
            ("lakebench-trino-coordinator-0", "trino-coordinator"),
            ("lakebench-trino-worker-0", "trino-worker"),
            ("lakebench-spark-thrift-server-0", "spark-thrift"),
            ("lakebench-duckdb-abc123", "duckdb"),
            ("lakebench-hive-metastore-0", "hive"),
            ("lakebench-postgres-0", "postgres"),
            ("lakebench-polaris-0", "polaris"),
            ("lakebench-prometheus-0", "prometheus"),
            ("lakebench-grafana-xyz", "grafana"),
            ("some-spark-driver-pod", "spark-driver"),
            ("bronze-exec-1", "spark-executor"),
            ("some-spark-process", "spark-job"),
            ("random-pod-xyz", "unknown"),
        ],
    )
    def test_component(self, pod, component):
        from lakebench.observability.platform_collector import PlatformCollector

        assert PlatformCollector._infer_component(pod) == component


# ===========================================================================
# CLI _collect_platform_metrics helper
# ===========================================================================


# ===========================================================================
# S3MetricsWrapper
# ===========================================================================


# ===========================================================================
# ObservabilityDeployer
# ===========================================================================


class TestObservabilityBuildHelmValues:
    """Tests for observability.build_helm_values() (the admin install's values)."""

    def test_build_helm_values_structure(self):
        from lakebench.deploy.observability import build_helm_values

        cfg = make_config(observability={"enabled": True, "retention": "14d", "storage": "20Gi"})
        values = build_helm_values(cfg.observability)
        assert values["prometheus.prometheusSpec.retention"] == "14d"
        assert (
            "20Gi"
            in values[
                "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage"
            ]
        )
        # SAF-8: no fixed Grafana password; the chart generates one per install.
        assert "grafana.adminPassword" not in values

    def test_build_helm_values_grafana_disabled(self):
        from lakebench.deploy.observability import build_helm_values

        cfg = make_config(observability={"enabled": True, "dashboards_enabled": False})
        assert build_helm_values(cfg.observability)["grafana.enabled"] == "false"


class TestObservabilityComponentInstall:
    """The shared install, now run by admin install (shared_components)."""

    def _install(self, side_effect):
        from lakebench.deploy.shared_components import Observability, Settings

        cfg = make_config(observability={"enabled": True})
        with (
            patch("subprocess.run") as mock_run,
            patch("lakebench.deploy.observability.is_openshift", return_value=False),
            patch.object(Observability, "apply_dashboard") as dash,
        ):
            dash.return_value = MagicMock(ok=True, message="")
            mock_run.side_effect = side_effect
            result = Observability().install(
                Settings.from_config(cfg), cfg.observability.chart_version
            )
        return cfg, mock_run, result

    def test_install_pins_chart_version(self):
        """The install command must pin --version -- previously it
        carried no version flag at all and silently tracked whatever the
        Helm repo served at install time (unpinned equivalent of LB-070's
        --reuse-values gap, just with no pin to even reuse)."""
        from lakebench.deploy.observability import HELM_CHART, OBSERVABILITY_NAMESPACE

        # The chart repo is refreshed by prepare(), before the lease.
        cfg, mock_run, result = self._install([MagicMock(returncode=0, stdout="", stderr="")])
        assert result.ok, result.message
        assert mock_run.call_count == 1
        install_call = mock_run.call_args_list[0][0][0]
        assert install_call[:2] == ["helm", "install"]
        assert HELM_CHART in install_call
        idx = install_call.index("--version")
        assert install_call[idx + 1] == cfg.observability.chart_version
        assert install_call[install_call.index("--namespace") + 1] == OBSERVABILITY_NAMESPACE
        assert "--wait" not in install_call

    def test_install_helm_failure(self):
        _cfg, _run, result = self._install(
            [MagicMock(returncode=1, stderr="chart not found", stdout="")]
        )
        assert not result.ok
        assert "chart not found" in result.message

    def test_install_helm_timeout(self):
        import subprocess as sp

        _cfg, _run, result = self._install([sp.TimeoutExpired(cmd="helm", timeout=360)])
        assert not result.ok
        assert "timed out" in result.message


class TestObservabilityDeployerDeployDestroy:
    """Tests for ObservabilityDeployer deploy/destroy lifecycle."""

    @pytest.fixture(autouse=True)
    def _ready(self):
        with patch("lakebench.deploy.observability._wait_for_prometheus", return_value=""):
            yield

    def test_deploy_skip_when_disabled(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config()  # observability.enabled = False
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)
        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED

    def test_deploy_without_the_stack_fails_and_installs_nothing(self):
        """DEP-3: deploy only verifies. Reverted (v1.6), a missing stack was
        helm-installed from deploy."""
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="[]", stderr="")
            result = deployer.deploy()
        assert result.status == DeploymentStatus.FAILED
        assert "lakebench admin install --component observability" in result.message
        helm = [c.args[0][:2] for c in mock_run.call_args_list if c.args[0][0] == "helm"]
        assert helm == [["helm", "list"]]

    def test_destroy_legacy_release_not_found_is_success(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        deployer = ObservabilityDeployer(engine)

        from lakebench.deploy.observability import HELM_RELEASE_NAME

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout=HELM_RELEASE_NAME, stderr=""),  # legacy release
                MagicMock(returncode=1, stdout="", stderr="release: not found"),  # uninstall
            ]
            result = deployer.destroy()
            assert result.status == DeploymentStatus.SUCCESS
