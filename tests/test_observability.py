"""Tests for the observability package.

Covers:
- PlatformCollector: collect(), failed and partial queries, _infer_component()
- ObservabilityDeployer deploy/destroy paths, the Grafana route and the
  provisioned dashboard
- The shared Observability component's status() and install()
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
            ("lakebench-spark-thrift-server-0", "spark-thrift"),
            ("lakebench-hive-metastore-0", "hive"),
            ("some-spark-driver-pod", "spark-driver"),
            ("bronze-exec-1", "spark-executor"),
            ("random-pod-xyz", "unknown"),
        ],
    )
    def test_component(self, pod, component):
        from lakebench.observability.platform_collector import PlatformCollector

        assert PlatformCollector._infer_component(pod) == component


# ===========================================================================
# ObservabilityDeployer (per-namespace stack)
# ===========================================================================


def _obs_engine(namespace: str = "lakebench-dep") -> MagicMock:
    """A minimal engine with a per-namespace renderer that returns one empty doc."""
    cfg = make_config(name=namespace, observability={"enabled": True})
    engine = MagicMock()
    engine.config = cfg
    engine.dry_run = False
    engine.context = {"namespace": namespace, "openshift_mode": False}
    engine.k8s = MagicMock()
    engine.renderer = MagicMock()
    engine.renderer.render.return_value = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n"
    return engine


class TestObservabilityDeployer:
    """Per-namespace deployer: apply templates, bind kubelet role, destroy binding."""

    def test_deploy_disabled_skips(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        cfg = make_config(observability={"enabled": False})
        engine = MagicMock()
        engine.config = cfg
        engine.dry_run = False
        result = ObservabilityDeployer(engine).deploy()
        assert result.status == DeploymentStatus.SKIPPED

    @pytest.mark.parametrize("dashboards", [True, False])
    def test_deploy_renders_and_applies_templates(self, dashboards):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        engine = _obs_engine()
        engine.config = make_config(
            name="lakebench-dep",
            observability={
                "enabled": True,
                "retention": "14d",
                "storage": "20Gi",
                "dashboards_enabled": dashboards,
            },
        )
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
        # Prometheus, Grafana, dashboard (unless disabled), Pushgateway,
        # kubelet binding.
        assert "prometheus/prometheus.yaml.j2" in rendered
        assert "grafana/grafana.yaml.j2" in rendered
        assert ("grafana/dashboard-configmap.yaml.j2" in rendered) is dashboards
        assert "pushgateway/pushgateway.yaml.j2" in rendered
        assert "prometheus/kubelet-metrics-binding.yaml.j2" in rendered
        # The config's retention and storage reach the Prometheus context.
        ctx = next(
            c.args[1]
            for c in engine.renderer.render.call_args_list
            if c.args[0] == "prometheus/prometheus.yaml.j2"
        )
        assert ctx["prometheus_retention"] == "14d"
        assert ctx["prometheus_storage"] == "20Gi"

    @pytest.mark.parametrize(
        ("openshift", "route_on", "expect_route"),
        [(True, True, True), (True, False, False), (False, True, False)],
    )
    def test_grafana_route_only_on_openshift_when_enabled(self, openshift, route_on, expect_route):
        from lakebench.deploy.observability import ObservabilityDeployer

        engine = _obs_engine()
        engine.context["openshift_mode"] = openshift
        engine.config = make_config(
            name="lakebench-dep", observability={"enabled": True, "grafana_route": route_on}
        )
        with (
            patch("lakebench.deploy.observability.kubelet_cluster_role_exists", return_value=True),
            patch("lakebench.deploy.observability._wait_for_deployment_ready", return_value=""),
            patch("lakebench.deploy.observability.read_grafana_password", return_value=None),
            patch(
                "lakebench.deploy.observability.grafana_route_host",
                return_value="grafana-dep.apps.example.com",
            ),
        ):
            result = ObservabilityDeployer(engine).deploy()
        rendered = [c.args[0] for c in engine.renderer.render.call_args_list]
        assert ("grafana/grafana-route.yaml.j2" in rendered) is expect_route
        url = (result.details or {}).get("grafana_url")
        assert url == ("https://grafana-dep.apps.example.com" if expect_route else None)

    def test_grafana_route_host_reads_the_admitted_ingress(self):
        from lakebench.deploy.observability import grafana_route_host

        route = {
            "spec": {"host": "spec.example.com"},
            "status": {"ingress": [{"host": "admitted.example.com"}]},
        }
        api = MagicMock()
        api.get_namespaced_custom_object.return_value = route
        with (
            patch("kubernetes.client.CustomObjectsApi", return_value=api),
            patch("lakebench.k8s.target.ClusterTarget.resolve") as resolve,
        ):
            resolve.return_value.activate.return_value = None
            assert grafana_route_host("ns") == "admitted.example.com"
            api.get_namespaced_custom_object.side_effect = RuntimeError("no route api")
            assert grafana_route_host("ns") is None

    def test_deploy_without_kubelet_role_still_succeeds_with_warning_detail(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        engine = _obs_engine()
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
        assert result.status == DeploymentStatus.SUCCESS, result.message
        # The binding template is skipped and the message says platform
        # metrics will read 'not collected'.
        rendered = [c.args[0] for c in engine.renderer.render.call_args_list]
        assert "prometheus/kubelet-metrics-binding.yaml.j2" not in rendered
        assert "not collected" in result.message

    def test_deploy_success_reports_failed_templates_and_omits_a_disabled_pushgateway(self):
        """A template that fails to apply is named on the SUCCESS result, and
        a disabled Pushgateway is not claimed as deployed."""
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        engine = _obs_engine()
        engine.config = make_config(
            name="lakebench-dep",
            observability={"enabled": True, "pushgateway_enabled": False},
        )

        def apply(doc, namespace=None):
            if namespace and engine.k8s.apply_manifest.call_count == 1:
                raise RuntimeError("quota exceeded")

        engine.k8s.apply_manifest.side_effect = apply
        with (
            patch("lakebench.deploy.observability.kubelet_cluster_role_exists", return_value=True),
            patch("lakebench.deploy.observability._wait_for_deployment_ready", return_value=""),
            patch("lakebench.deploy.observability.read_grafana_password", return_value=None),
        ):
            result = ObservabilityDeployer(engine).deploy()
        assert result.status == DeploymentStatus.SUCCESS, result.message
        assert "Pushgateway" not in result.message
        assert "apply prometheus/prometheus.yaml.j2/ConfigMap: quota exceeded" in result.message
        assert "quota exceeded" in result.detail

    def test_deploy_fails_when_prometheus_not_ready(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        engine = _obs_engine()

        def ready(_ns, selector, **_kw):
            if "prometheus" in selector:
                return "not Ready after 300s"
            return ""

        with (
            patch(
                "lakebench.deploy.observability.kubelet_cluster_role_exists",
                return_value=True,
            ),
            patch(
                "lakebench.deploy.observability._wait_for_deployment_ready",
                side_effect=ready,
            ),
            patch(
                "lakebench.deploy.observability.read_grafana_password",
                return_value=None,
            ),
        ):
            result = ObservabilityDeployer(engine).deploy()
        assert result.status == DeploymentStatus.FAILED
        assert "Prometheus not Ready" in result.message

    def test_destroy_deletes_the_kubelet_binding(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import (
            KUBELET_METRICS_CLUSTER_ROLE,
            ObservabilityDeployer,
        )

        engine = _obs_engine(namespace="dep-a")
        deleted: list[str] = []

        class FakeRbac:
            def delete_cluster_role_binding(self, name):
                deleted.append(name)

        with patch("kubernetes.client.RbacAuthorizationV1Api", return_value=FakeRbac()):
            result = ObservabilityDeployer(engine).destroy()
        assert result.status == DeploymentStatus.SUCCESS
        assert deleted == [f"{KUBELET_METRICS_CLUSTER_ROLE}-dep-a"]

    def test_destroy_tolerates_a_404(self):
        from lakebench.deploy.engine import DeploymentStatus
        from lakebench.deploy.observability import ObservabilityDeployer

        engine = _obs_engine()
        from kubernetes.client.rest import ApiException

        class FakeRbac:
            def delete_cluster_role_binding(self, name):
                raise ApiException(status=404, reason="Not Found")

        with patch("kubernetes.client.RbacAuthorizationV1Api", return_value=FakeRbac()):
            result = ObservabilityDeployer(engine).destroy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "No kubelet binding" in result.message


class TestObservabilityComponent:
    """The shared ClusterRole: admin install applies it, deploy reads it."""

    @pytest.mark.parametrize("present", [True, False])
    def test_component_status_reads_the_clusterrole(self, present):
        from lakebench.deploy.shared_components import Observability, Settings

        cfg = make_config(observability={"enabled": True})
        settings = Settings.from_config(cfg)
        with patch(
            "lakebench.deploy.observability.kubelet_cluster_role_exists",
            return_value=present,
        ):
            status = Observability().status(settings)
        assert status.installed is present
        if present:
            assert status.ready is True
        else:
            assert "not present" in status.detail

    def test_component_install_applies_the_clusterrole(self):
        from lakebench.deploy.shared_components import Observability, Settings

        cfg = make_config(observability={"enabled": True})
        settings = Settings.from_config(cfg)
        fake_k8s = MagicMock()
        with patch("lakebench.k8s.get_k8s_client", return_value=fake_k8s):
            result = Observability().install(settings, version=None)
        assert result.ok, result.message
        applied = fake_k8s.apply_manifest.call_args_list[0].args[0]
        assert applied["kind"] == "ClusterRole"
        assert applied["metadata"]["name"] == "lakebench-kubelet-metrics"


class TestDashboardTemplate:
    """The provisioned Grafana dashboard renders to valid JSON Grafana can load."""

    def _dash(self):
        import json

        import yaml

        from lakebench.deploy.engine import TemplateRenderer

        text = TemplateRenderer().render("grafana/dashboard-configmap.yaml.j2", {"namespace": "ns"})
        return json.loads(yaml.safe_load(text)["data"]["lakebench-overview.json"])

    def test_panels_are_well_formed(self):
        d = self._dash()
        assert d["uid"] == "lakebench-overview"
        ids = [p["id"] for p in d["panels"]]
        assert len(ids) == len(set(ids))
        for p in d["panels"]:
            g = p["gridPos"]
            assert 0 <= g["x"] and g["x"] + g["w"] <= 24
            for t in p.get("targets", []):
                assert t["expr"] and t["refId"]
                assert t["datasource"]["uid"] == "${datasource}"

    def test_grafana_legend_templates_survive_jinja(self):
        d = self._dash()
        legends = {t["legendFormat"] for p in d["panels"] for t in p.get("targets", [])}
        assert "{{pod}}" in legends and "pod {{node_id}}" in legends

    def test_resource_panels_cover_every_component(self):
        d = self._dash()
        cpu = next(p for p in d["panels"] if p["title"].startswith("CPU by component"))
        names = {t["legendFormat"] for t in cpu["targets"]}
        assert {"datagen", "bronze-ingest", "silver-stream", "gold-refresh"} <= names
        assert {"trino coordinator", "trino workers", "polaris", "postgres"} <= names
