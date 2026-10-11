"""Tests for the observability package (P0 + P2).

Covers:
- PlatformCollector: collect(), _query_range(), _query_instant(), _infer_component()
- S3MetricsWrapper: enabled/disabled paths, error recording, proxy behavior
- ObservabilityDeployer deploy/destroy paths; build_helm_values() and the admin
  install component's prepare() and install()
"""

from __future__ import annotations

import subprocess
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

        def _resp(result):
            r = MagicMock()
            r.status_code = 200
            r.json.return_value = {"data": {"result": result}}
            return r

        by_query = {
            "container_cpu_usage_seconds_total": _resp(
                [
                    {
                        "metric": {"pod": "lakebench-trino-coordinator-0"},
                        "values": [[1, "1.5"], [2, "2.0"]],
                    }
                ]
            ),
            "container_memory_working_set_bytes": _resp(
                [
                    {
                        "metric": {"pod": "lakebench-trino-coordinator-0"},
                        "values": [[1, "2000000000"], [2, "3000000000"]],
                    }
                ]
            ),
            # A series nobody should read: S3 request counts are not collected.
            "lakebench_s3_requests_total": _resp([{"value": [1, "42"]}]),
            "lakebench_s3_errors_total": _resp([{"value": [1, "2"]}]),
        }

        def fake_get(url, params=None, **_):
            q = params["query"]
            # Trino's counters are lifetime totals: 500 since the coordinator
            # started, 7 of them inside this run's window.
            if "completedqueries_totalcount" in q:
                return _resp([{"value": [1, "7" if "increase(" in q else "500"]}])
            for needle, resp in by_query.items():
                if needle in q:
                    return resp
            return _resp([])

        mock_httpx = MagicMock()
        mock_httpx.Client.return_value.get.side_effect = fake_get

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
        assert metrics.s3_requests_total is None
        assert metrics.s3_errors_total is None
        assert metrics.to_dict()["s3_avg_latency_ms"] is None
        assert metrics.engine.trino_completed_queries == 7


@pytest.mark.parametrize("failing", ["container_cpu_usage", "container_memory_working_set"])
def test_failed_query_leaves_its_values_not_collected(failing):
    """A query Prometheus refuses leaves its pod values None and names itself
    in collection_error; the other query's values are still recorded."""
    from lakebench.observability.platform_collector import PlatformCollector

    def fake_get(url, params=None, **_):
        q = params["query"]
        r = MagicMock()
        if failing in q:
            r.status_code, r.text = 503, "unavailable"
            return r
        r.status_code = 200
        series = [{"metric": {"pod": "bronze-driver"}, "values": [[1, "2.0"], [2, "4.0"]]}]
        r.json.return_value = {"data": {"result": series if "container_" in q else []}}
        return r

    mock_httpx = MagicMock()
    mock_httpx.Client.return_value.get.side_effect = fake_get
    with patch.dict("sys.modules", {"httpx": mock_httpx}):
        metrics = PlatformCollector("http://prom:9090", "ns").collect(
            datetime(2026, 2, 12, 10, 0), datetime(2026, 2, 12, 10, 5)
        )

    [pod] = metrics.to_dict()["pods"]
    cpu_failed = failing == "container_cpu_usage"
    assert "503" in metrics.collection_error
    assert ("CPU" if cpu_failed else "memory") in metrics.collection_error
    assert (pod["cpu_max_cores"] is None) is cpu_failed
    assert (pod["memory_max_bytes"] is None) is not cpu_failed
    assert (pod["memory_max_bytes"] if cpu_failed else pod["cpu_max_cores"]) == 4.0


def test_a_pod_missing_from_one_query_is_counted_as_not_collected():
    """Both queries answer, but one pod has a memory series and no CPU series
    (it lived under the rate window): its CPU is None and the record says how
    many pods lack it."""
    from lakebench.observability.platform_collector import PlatformCollector

    def fake_get(url, params=None, **_):
        q = params["query"]
        pods = ["a-driver"] if "cpu" in q else ["a-driver", "b-driver"]
        r = MagicMock()
        r.status_code = 200
        series = [{"metric": {"pod": p}, "values": [[1, "3.0"]]} for p in pods]
        r.json.return_value = {"data": {"result": series if "container_" in q else []}}
        return r

    mock_httpx = MagicMock()
    mock_httpx.Client.return_value.get.side_effect = fake_get
    with patch.dict("sys.modules", {"httpx": mock_httpx}):
        metrics = PlatformCollector("http://prom:9090", "ns").collect(
            datetime(2026, 2, 12, 10, 0), datetime(2026, 2, 12, 10, 5)
        )
    by_pod = {p["pod_name"]: p for p in metrics.to_dict()["pods"]}
    assert by_pod["b-driver"]["cpu_max_cores"] is None
    assert by_pod["b-driver"]["memory_max_bytes"] == 3
    assert "1 of 2 pods" in metrics.collection_error


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

    def test_build_helm_values_sets_no_fixed_grafana_password(self):
        from lakebench.deploy.observability import build_helm_values

        cfg = make_config(observability={"enabled": True})
        # The chart generates one password per install.
        assert "grafana.adminPassword" not in build_helm_values(cfg.observability)


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
        Helm repo served at install time (unpinned equivalent of the Spark Operator's
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

    @pytest.mark.parametrize(
        "side_effect",
        [
            [MagicMock(returncode=1, stderr="chart not found", stdout="")],
            [subprocess.TimeoutExpired(cmd="helm", timeout=360)],
        ],
        ids=["failure", "timeout"],
    )
    def test_install_helm_failure_is_not_ok(self, side_effect):
        _cfg, _run, result = self._install(side_effect)
        assert not result.ok


class TestObservabilityDeployerDeployDestroy:
    """Tests for ObservabilityDeployer deploy/destroy lifecycle."""

    @pytest.fixture(autouse=True)
    def _ready(self):
        with patch("lakebench.deploy.observability._wait_for_prometheus", return_value=""):
            yield

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
        from lakebench.deploy.observability import HELM_RELEASE_NAME, ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout=HELM_RELEASE_NAME, stderr=""),  # legacy release
                MagicMock(returncode=1, stdout="", stderr="release: not found"),  # uninstall
            ]
            result = deployer.destroy()
        assert result.status == DeploymentStatus.SUCCESS
        namespace = cfg.get_namespace()
        argvs = [c.args[0] for c in mock_run.call_args_list]
        assert len(argvs) == 2
        for argv in argvs:
            assert argv[argv.index("--namespace") + 1] == namespace
        uninstalls = [a for a in argvs if "uninstall" in a]
        assert len(uninstalls) == 1
        assert uninstalls[0][uninstalls[0].index("uninstall") + 1] == HELM_RELEASE_NAME
