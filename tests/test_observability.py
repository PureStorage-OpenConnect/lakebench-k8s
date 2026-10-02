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

    def test_collect_httpx_not_installed(self):
        """collect() returns error when httpx is missing."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)

        # Patch httpx to simulate ImportError
        with patch.dict("sys.modules", {"httpx": None}):
            # Force re-evaluation of the import inside collect

            import lakebench.observability.platform_collector as pc_mod

            def patched_collect(self, start_time, end_time):
                try:
                    raise ImportError("No module named 'httpx'")
                except ImportError:
                    return pc_mod.PlatformMetrics(
                        start_time=start_time,
                        end_time=end_time,
                        collection_error="httpx not installed; cannot query Prometheus",
                    )

            with patch.object(pc_mod.PlatformCollector, "collect", patched_collect):
                metrics = collector.collect(start, end)
                assert metrics.collection_error is not None
                assert "httpx" in metrics.collection_error

    def test_collect_prometheus_unreachable(self):
        """collect() records collection_error when Prometheus is down."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)

        mock_httpx = MagicMock()
        mock_client = MagicMock()
        mock_httpx.Client.return_value = mock_client
        mock_client.get.side_effect = ConnectionError("Connection refused")

        with patch.dict("sys.modules", {"httpx": mock_httpx}):
            metrics = collector.collect(start, end)
            assert metrics.collection_error is not None

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

    def test_collect_empty_prometheus_results(self):
        """collect() handles empty results from Prometheus gracefully."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)

        empty_response = MagicMock()
        empty_response.status_code = 200
        empty_response.json.return_value = {"data": {"result": []}}

        s3_empty = MagicMock()
        s3_empty.status_code = 200
        s3_empty.json.return_value = {"data": {"result": []}}

        mock_httpx = MagicMock()
        mock_client = MagicMock()
        mock_httpx.Client.return_value = mock_client
        mock_client.get.side_effect = [
            empty_response,
            empty_response,
            s3_empty,
            s3_empty,
            empty_response,  # spark GC
            empty_response,  # spark shuffle read
            empty_response,  # spark shuffle write
            empty_response,  # trino completed
            empty_response,  # trino failed
        ]

        with patch.dict("sys.modules", {"httpx": mock_httpx}):
            metrics = collector.collect(start, end)

        assert metrics.collection_error is None
        assert len(metrics.pods) == 0
        assert metrics.s3_requests_total == 0


class TestPlatformCollectorQueryRange:
    """Tests for PlatformCollector._query_range() method."""

    def test_query_range_non_200(self):
        """_query_range returns empty list on non-200 response."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_client.get.return_value = mock_response

        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)
        result = collector._query_range(mock_client, "up", start, end)
        assert result == []

    def test_query_range_valid_response(self):
        """_query_range parses valid Prometheus response."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"result": [{"metric": {"pod": "pod-1"}, "values": [[1, "1.0"]]}]}
        }
        mock_client.get.return_value = mock_response

        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)
        result = collector._query_range(mock_client, "up", start, end)
        assert len(result) == 1
        assert result[0]["metric"]["pod"] == "pod-1"


class TestPlatformCollectorQueryInstant:
    """Tests for PlatformCollector._query_instant() method."""

    def test_query_instant_non_200(self):
        """_query_instant returns None on non-200 response."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_client.get.return_value = mock_response

        t = datetime(2026, 2, 12, 10, 5, 0)
        assert collector._query_instant(mock_client, "up", t) is None

    def test_query_instant_empty_results(self):
        """_query_instant returns None when no results."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"result": []}}
        mock_client.get.return_value = mock_response

        t = datetime(2026, 2, 12, 10, 5, 0)
        assert collector._query_instant(mock_client, "up", t) is None

    def test_query_instant_valid_value(self):
        """_query_instant extracts float value from Prometheus response."""
        from lakebench.observability.platform_collector import PlatformCollector

        collector = PlatformCollector("http://prom:9090", "test-ns")
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"result": [{"value": [1707753600, "42.5"]}]}}
        mock_client.get.return_value = mock_response

        t = datetime(2026, 2, 12, 10, 5, 0)
        assert collector._query_instant(mock_client, "up", t) == 42.5


class TestInferComponent:
    """Tests for PlatformCollector._infer_component() static method."""

    def test_all_known_components(self):
        from lakebench.observability.platform_collector import PlatformCollector

        cases = {
            "lakebench-trino-coordinator-0": "trino-coordinator",
            "lakebench-trino-worker-0": "trino-worker",
            "lakebench-spark-thrift-server-0": "spark-thrift",
            "lakebench-duckdb-abc123": "duckdb",
            "lakebench-hive-metastore-0": "hive",
            "lakebench-postgres-0": "postgres",
            "lakebench-polaris-0": "polaris",
            "lakebench-prometheus-0": "prometheus",
            "lakebench-grafana-xyz": "grafana",
        }
        for pod_name, expected in cases.items():
            assert PlatformCollector._infer_component(pod_name) == expected, (
                f"Expected {expected} for {pod_name}"
            )

    def test_spark_driver_and_executor(self):
        from lakebench.observability.platform_collector import PlatformCollector

        assert PlatformCollector._infer_component("some-spark-driver-pod") == "spark-driver"
        assert PlatformCollector._infer_component("bronze-exec-1") == "spark-executor"

    def test_spark_job_fallback(self):
        from lakebench.observability.platform_collector import PlatformCollector

        assert PlatformCollector._infer_component("some-spark-process") == "spark-job"

    def test_unknown_fallback(self):
        from lakebench.observability.platform_collector import PlatformCollector

        assert PlatformCollector._infer_component("random-pod-xyz") == "unknown"


class TestPlatformMetricsToDict:
    """Tests for PlatformMetrics.to_dict() serialization."""

    def test_full_serialization(self):
        from lakebench.observability.platform_collector import PlatformMetrics, PodMetrics

        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 5, 0)
        metrics = PlatformMetrics(
            start_time=start,
            end_time=end,
            pods=[
                PodMetrics(
                    pod_name="pod-a",
                    component="trino-coordinator",
                    cpu_avg_cores=1.5555,
                    cpu_max_cores=2.1111,
                    memory_avg_bytes=1024 * 1024 * 1024,
                    memory_max_bytes=2 * 1024 * 1024 * 1024,
                )
            ],
            s3_requests_total=100,
            s3_errors_total=3,
            s3_avg_latency_ms=15.555,
        )
        d = metrics.to_dict()

        assert d["duration_seconds"] == 300.0
        assert d["collection_window_seconds"] == 300.0
        assert d["start_time"] == "2026-02-12T10:00:00"
        assert d["end_time"] == "2026-02-12T10:05:00"
        assert len(d["pods"]) == 1
        # Check rounding
        assert d["pods"][0]["cpu_avg_cores"] == 1.556
        assert d["pods"][0]["cpu_max_cores"] == 2.111
        assert d["pods"][0]["memory_avg_bytes"] == 1024 * 1024 * 1024
        assert d["s3_avg_latency_ms"] == 15.55
        assert d["collection_error"] is None

    def test_empty_pods(self):
        from lakebench.observability.platform_collector import PlatformMetrics

        start = datetime(2026, 2, 12, 10, 0, 0)
        end = datetime(2026, 2, 12, 10, 0, 30)
        metrics = PlatformMetrics(start_time=start, end_time=end)
        d = metrics.to_dict()
        assert d["pods"] == []
        assert d["duration_seconds"] == 30.0


# ===========================================================================
# CLI _collect_platform_metrics helper
# ===========================================================================


class TestCollectPlatformMetricsHelper:
    """Tests for cli._collect_platform_metrics()."""

    def test_skips_when_observability_disabled(self):
        from unittest.mock import MagicMock

        from lakebench.metrics import PipelineMetrics

        cfg = MagicMock()
        cfg.observability.enabled = False
        run_metrics = PipelineMetrics(
            run_id="test", deployment_name="test", start_time=datetime(2026, 1, 1)
        )

        from lakebench.cli import _collect_platform_metrics

        _collect_platform_metrics(cfg, run_metrics)
        assert run_metrics.platform_metrics is None

    def test_collects_when_observability_enabled(self):
        from unittest.mock import MagicMock, patch

        from lakebench.metrics import PipelineMetrics
        from lakebench.observability.platform_collector import EngineMetrics, PlatformMetrics

        cfg = MagicMock()
        cfg.observability.enabled = True
        cfg.get_namespace.return_value = "test-ns"

        start = datetime(2026, 2, 19, 10, 0, 0)
        end = datetime(2026, 2, 19, 10, 30, 0)
        run_metrics = PipelineMetrics(
            run_id="test", deployment_name="test", start_time=start, end_time=end
        )

        fake_pm = PlatformMetrics(
            start_time=start,
            end_time=end,
            s3_requests_total=50,
            engine=EngineMetrics(trino_completed_queries=41),
        )

        with (
            patch(
                "lakebench.cli._sustained._find_prometheus_svc",
                return_value="prom-svc",
            ),
            patch("httpx.get") as mock_httpx_get,
            patch(
                "lakebench.observability.platform_collector.PlatformCollector",
            ) as mock_cls,
        ):
            # Simulate in-cluster DNS success (httpx.get doesn't raise)
            mock_httpx_get.return_value = MagicMock(status_code=200)
            mock_cls.return_value.collect.return_value = fake_pm
            from lakebench.cli import _collect_platform_metrics

            _collect_platform_metrics(cfg, run_metrics)

        assert run_metrics.platform_metrics is not None
        assert run_metrics.platform_metrics["s3_requests_total"] == 50
        assert run_metrics.platform_metrics["engine"]["trino_completed_queries"] == 41

    def test_handles_exception_gracefully(self):
        from unittest.mock import MagicMock, patch

        from lakebench.metrics import PipelineMetrics

        cfg = MagicMock()
        cfg.observability.enabled = True
        cfg.get_namespace.return_value = "test-ns"

        run_metrics = PipelineMetrics(
            run_id="test",
            deployment_name="test",
            start_time=datetime(2026, 2, 19, 10, 0, 0),
            end_time=datetime(2026, 2, 19, 10, 30, 0),
        )

        with (
            patch(
                "lakebench.cli._sustained._find_prometheus_svc",
                return_value="prom-svc",
            ),
            patch("httpx.get") as mock_httpx_get,
            patch(
                "lakebench.observability.platform_collector.PlatformCollector",
            ) as mock_cls,
        ):
            mock_httpx_get.return_value = MagicMock(status_code=200)
            mock_cls.return_value.collect.side_effect = Exception("connection refused")
            from lakebench.cli import _collect_platform_metrics

            # Should not raise
            _collect_platform_metrics(cfg, run_metrics)

        assert run_metrics.platform_metrics is None


# ===========================================================================
# S3MetricsWrapper
# ===========================================================================


class TestS3MetricsWrapperDisabled:
    """Tests for S3MetricsWrapper when disabled (no prometheus_client)."""

    def test_proxy_calls_pass_through(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = ["b1", "b2"]
        wrapper = S3MetricsWrapper(mock_client, enabled=False)
        assert wrapper.list_buckets() == ["b1", "b2"]
        mock_client.list_buckets.assert_called_once()

    def test_proxy_with_args(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.put_object.return_value = {"ETag": "abc"}
        wrapper = S3MetricsWrapper(mock_client, enabled=False)
        result = wrapper.put_object(Bucket="b", Key="k", Body=b"data")
        assert result == {"ETag": "abc"}
        mock_client.put_object.assert_called_once_with(Bucket="b", Key="k", Body=b"data")

    def test_private_attrs_not_wrapped(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client._internal = "raw"
        wrapper = S3MetricsWrapper(mock_client, enabled=False)
        # Private attributes should pass through without wrapping
        assert wrapper._internal == "raw"

    def test_non_callable_attrs_pass_through(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.meta = "metadata_object"
        S3MetricsWrapper(mock_client, enabled=False)
        # Non-callable attributes shouldn't be wrapped
        # (MagicMock makes everything callable, so test with a real object)

        class FakeClient:
            region = "us-east-1"

        fc = FakeClient()
        w = S3MetricsWrapper(fc, enabled=False)
        assert w.region == "us-east-1"

    def test_exception_propagates(self):
        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.get_object.side_effect = Exception("NoSuchKey")
        wrapper = S3MetricsWrapper(mock_client, enabled=False)
        with pytest.raises(Exception, match="NoSuchKey"):
            wrapper.get_object(Bucket="b", Key="missing")


def _unregister_lakebench_s3_metrics() -> None:
    try:
        from prometheus_client import REGISTRY
    except ImportError:
        return
    collectors = {
        c
        for name, c in list(REGISTRY._names_to_collectors.items())
        if name.startswith("lakebench_s3_")
    }
    for collector in collectors:
        REGISTRY.unregister(collector)


class TestS3MetricsWrapperEnabled:
    """Tests for S3MetricsWrapper when enabled (mocked prometheus_client)."""

    @pytest.fixture(autouse=True)
    def _fresh_registry(self):
        # S3MetricsWrapper registers its metrics in the global registry, so
        # each test starts and ends without them, in any order.
        _unregister_lakebench_s3_metrics()
        yield
        _unregister_lakebench_s3_metrics()

    def test_enabled_records_metrics(self):
        """When enabled and prometheus_client available, metrics are recorded."""
        from lakebench.observability.s3_metrics import _prom_available

        if not _prom_available:
            pytest.skip("prometheus_client not installed")

        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = ["b1"]

        wrapper = S3MetricsWrapper(mock_client, enabled=True)
        result = wrapper.list_buckets()
        assert result == ["b1"]

    def test_enabled_records_errors(self):
        """When enabled, errors increment the error counter."""
        from lakebench.observability.s3_metrics import _prom_available

        if not _prom_available:
            pytest.skip("prometheus_client not installed")

        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.get_object.side_effect = Exception("Boom")

        wrapper = S3MetricsWrapper(mock_client, enabled=True)
        with pytest.raises(Exception, match="Boom"):
            wrapper.get_object(Bucket="b", Key="k")


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


class TestObservabilityPrepare:
    """The chart repo is refreshed before the lease, and a failure is seen."""

    def _prepare(self, results):
        from lakebench.deploy.shared_components import Observability, Settings

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = results
            problem = Observability().prepare(Settings.from_config(None))
        return mock_run, problem

    def test_prepare_adds_and_updates_the_repo(self):
        mock_run, problem = self._prepare([MagicMock(returncode=0), MagicMock(returncode=0)])
        assert problem is None
        cmds = [c.args[0] for c in mock_run.call_args_list]
        assert [c[:3] for c in cmds] == [["helm", "repo", "add"], ["helm", "repo", "update"]]

    def test_prepare_reports_a_failed_update(self):
        _run, problem = self._prepare(
            [MagicMock(returncode=0), MagicMock(returncode=1, stderr="no network")]
        )
        assert problem and "no network" in problem


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

    def test_deploy_dry_run(self):
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

    def test_destroy_dry_run(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = True
        deployer = ObservabilityDeployer(engine)
        result = deployer.destroy()
        # The shared stack is never removed, dry run or not.
        assert result.status == DeploymentStatus.SKIPPED
        assert "left in place" in result.message

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
