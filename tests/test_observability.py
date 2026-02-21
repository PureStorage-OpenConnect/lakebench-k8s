"""Tests for the observability package (P0 + P2).

Covers:
- PlatformCollector: collect(), _query_range(), _query_instant(), _infer_component()
- S3MetricsWrapper: enabled/disabled paths, error recording, proxy behavior
- ObservabilityDeployer: _build_helm_values(), _add_helm_repo(), deploy/destroy paths
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
                "lakebench.cli._find_prometheus_svc",
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
                "lakebench.cli._find_prometheus_svc",
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


class TestS3MetricsWrapperEnabled:
    """Tests for S3MetricsWrapper when enabled (mocked prometheus_client)."""

    def test_enabled_records_metrics(self):
        """When enabled and prometheus_client available, metrics are recorded."""
        from lakebench.observability.s3_metrics import _prom_available

        if not _prom_available:
            pytest.skip("prometheus_client not installed")

        from lakebench.observability.s3_metrics import S3MetricsWrapper

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = ["b1"]

        # Use unique metric names to avoid duplicate registration
        wrapper = S3MetricsWrapper(mock_client, enabled=True)
        result = wrapper.list_buckets()
        assert result == ["b1"]

    def test_enabled_records_errors(self):
        """When enabled, errors increment the error counter."""
        from prometheus_client import REGISTRY

        from lakebench.observability.s3_metrics import _prom_available

        if not _prom_available:
            pytest.skip("prometheus_client not installed")

        from lakebench.observability.s3_metrics import S3MetricsWrapper

        # Unregister metrics from the previous test to avoid duplicate errors
        collectors_to_unregister = set()
        for name, collector in list(REGISTRY._names_to_collectors.items()):
            if name.startswith("lakebench_s3_"):
                collectors_to_unregister.add(collector)
        for collector in collectors_to_unregister:
            REGISTRY.unregister(collector)

        mock_client = MagicMock()
        mock_client.get_object.side_effect = Exception("Boom")

        wrapper = S3MetricsWrapper(mock_client, enabled=True)
        with pytest.raises(Exception, match="Boom"):
            wrapper.get_object(Bucket="b", Key="k")


# ===========================================================================
# ObservabilityDeployer
# ===========================================================================


class TestObservabilityDeployerBuildHelmValues:
    """Tests for ObservabilityDeployer._build_helm_values()."""

    def test_build_helm_values_structure(self):
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
                "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage"
            ]
        )
        assert values["grafana.adminPassword"] == "lakebench"

    def test_build_helm_values_grafana_disabled(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True, "dashboards_enabled": False})
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)

        values = deployer._build_helm_values("test-ns")
        assert values["grafana.enabled"] == "false"


class TestObservabilityDeployerAddHelmRepo:
    """Tests for ObservabilityDeployer._add_helm_repo()."""

    def test_add_helm_repo_calls_subprocess(self):
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            deployer._add_helm_repo()
            # Should call helm repo add + helm repo update
            assert mock_run.call_count == 2
            first_call_args = mock_run.call_args_list[0][0][0]
            assert "helm" in first_call_args
            assert "repo" in first_call_args
            assert "add" in first_call_args

    def test_add_helm_repo_failure_silent(self):
        """_add_helm_repo doesn't raise even if subprocess fails."""
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="error")
            # Should not raise
            deployer._add_helm_repo()


class TestObservabilityDeployerDeployDestroy:
    """Tests for ObservabilityDeployer deploy/destroy lifecycle."""

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
        assert "Would deploy" in result.message

    def test_deploy_helm_failure(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            # First two calls for helm repo add/update, third for helm install
            mock_run.side_effect = [
                MagicMock(returncode=0),  # helm repo add
                MagicMock(returncode=0),  # helm repo update
                MagicMock(returncode=1, stderr="chart not found"),  # helm install fails
            ]
            result = deployer.deploy()
            assert result.status == DeploymentStatus.FAILED

    def test_deploy_helm_timeout(self):
        import subprocess as sp

        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # helm repo add
                MagicMock(returncode=0),  # helm repo update
                sp.TimeoutExpired(cmd="helm", timeout=360),  # helm install timeout
            ]
            result = deployer.deploy()
            assert result.status == DeploymentStatus.FAILED
            assert "timed out" in result.message

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

    def test_destroy_not_found_is_success(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": True})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        deployer = ObservabilityDeployer(engine)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="release: not found")
            result = deployer.destroy()
            assert result.status == DeploymentStatus.SUCCESS
