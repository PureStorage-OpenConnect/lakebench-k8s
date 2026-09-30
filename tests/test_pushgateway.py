"""Unit tests for the per-deployment Pushgateway component and the cluster-wide
Grafana dashboard (LB-192).

Covers: the pushgateway Deployment/Service/PVC render; the PodMonitor carrying
release + honorLabels (review F2); the dashboard rendered once cluster-wide with
namespace + run_id template variables and cap-labeled panels (invariant 6); and
the deployer gating on observability.pushgateway_enabled.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
import yaml

from lakebench.deploy.engine import TemplateRenderer
from lakebench.deploy.observability import (
    DASHBOARD_TEMPLATE,
    PODMONITOR_TEMPLATES,
    PUSHGATEWAY_TEMPLATES,
    ObservabilityDeployer,
)


@pytest.fixture
def renderer() -> TemplateRenderer:
    return TemplateRenderer()


@pytest.fixture
def ctx() -> dict:
    return {
        "namespace": "lb-test",
        "prometheus_release": "lakebench-observability",
        "observability_namespace": "lakebench-observability",
        "pushgateway_image": "prom/pushgateway:v1.11.1",
        "pushgateway_storage": "1Gi",
        "pushgateway_storage_class": "px-csi-scratch",
    }


def _docs(rendered: str) -> list[dict]:
    return [d for d in yaml.safe_load_all(rendered) if d]


class TestPushgatewayManifests:
    def test_deployment_service_pvc_render(self, renderer, ctx):
        docs = _docs(renderer.render("pushgateway/pushgateway.yaml.j2", ctx))
        kinds = {d["kind"] for d in docs}
        assert kinds == {"PersistentVolumeClaim", "Deployment", "Service"}
        dep = next(d for d in docs if d["kind"] == "Deployment")
        container = dep["spec"]["template"]["spec"]["containers"][0]
        assert container["image"] == "prom/pushgateway:v1.11.1"
        assert container["ports"][0]["containerPort"] == 9091
        # RWO PVC needs Recreate, not RollingUpdate (two pods can't mount it).
        assert dep["spec"]["strategy"]["type"] == "Recreate"
        svc = next(d for d in docs if d["kind"] == "Service")
        assert svc["spec"]["ports"][0]["port"] == 9091
        pvc = next(d for d in docs if d["kind"] == "PersistentVolumeClaim")
        assert pvc["spec"]["storageClassName"] == "px-csi-scratch"
        for d in docs:
            assert d["metadata"]["namespace"] == "lb-test"

    def test_fsgroup_on_vanilla_k8s_absent_on_openshift(self, renderer, ctx):
        # Vanilla k8s (no auto fsGroup): the pod sets fsGroup so nobody can write
        # the PVC. OpenShift: SCC assigns one, so no explicit securityContext.
        vanilla = _docs(
            renderer.render("pushgateway/pushgateway.yaml.j2", {**ctx, "openshift_mode": False})
        )
        dep = next(d for d in vanilla if d["kind"] == "Deployment")
        assert dep["spec"]["template"]["spec"]["securityContext"]["fsGroup"] == 65534

        ocp = _docs(
            renderer.render("pushgateway/pushgateway.yaml.j2", {**ctx, "openshift_mode": True})
        )
        dep_ocp = next(d for d in ocp if d["kind"] == "Deployment")
        assert "securityContext" not in dep_ocp["spec"]["template"]["spec"]

    def test_podmonitor_has_honor_labels_and_release(self, renderer, ctx):
        docs = _docs(renderer.render("pushgateway/podmonitor-pushgateway.yaml.j2", ctx))
        assert len(docs) == 1
        pm = docs[0]
        assert pm["kind"] == "PodMonitor"
        # Review F2: honorLabels keeps the pushed job/run_id labels.
        ep = pm["spec"]["podMetricsEndpoints"][0]
        assert ep["honorLabels"] is True
        assert ep["targetPort"] == 9091
        assert ep["path"] == "/metrics"
        # Shared podMonitorSelector requires the release label.
        assert pm["metadata"]["labels"]["release"] == "lakebench-observability"


class TestDashboard:
    def test_removed_from_per_namespace_templates(self):
        # LB-192: the dashboard is NOT applied per-namespace anymore.
        assert DASHBOARD_TEMPLATE not in PODMONITOR_TEMPLATES

    def test_cluster_wide_with_template_variables(self, renderer, ctx):
        docs = _docs(renderer.render(DASHBOARD_TEMPLATE, ctx))
        cm = docs[0]
        assert cm["kind"] == "ConfigMap"
        # Rendered into the shared observability namespace, not the deployment ns.
        assert cm["metadata"]["namespace"] == "lakebench-observability"
        assert cm["metadata"]["labels"]["grafana_dashboard"] == "1"
        dash = json.loads(cm["data"]["lakebench-overview.json"])
        assert dash["uid"] == "lakebench-overview"
        var_names = {v["name"] for v in dash["templating"]["list"]}
        assert {"datasource", "namespace", "run_id"} <= var_names

    def test_datagen_panel_is_cap_labeled(self, renderer, ctx):
        docs = _docs(renderer.render(DASHBOARD_TEMPLATE, ctx))
        dash = json.loads(docs[0]["data"]["lakebench-overview.json"])
        titles = [p["title"] for p in dash["panels"]]
        # Invariant 6: the datagen throughput panel labels the cap.
        assert any("CAPPED" in t for t in titles)
        tp = next(p for p in dash["panels"] if "throughput" in p["title"].lower())
        # The label names the real sizing (8 CPU default, 16Gi memory cap),
        # not the retired 4 CPU / 4Gi / 24Gi lock.
        label = tp["title"] + " " + tp["description"]
        assert "16Gi" in label and "default 8" in label
        assert "4Gi" not in label and "24Gi" not in label
        expr = tp["targets"][0]["expr"]
        assert 'namespace=~"$namespace"' in expr and 'run_id=~"$run_id"' in expr


class TestDeployerGating:
    def _deployer(self, pushgateway_enabled: bool) -> ObservabilityDeployer:
        engine = MagicMock()
        engine.config.observability.pushgateway_enabled = pushgateway_enabled
        engine.config.observability.pushgateway_image = "prom/pushgateway:v1.11.1"
        engine.config.observability.pushgateway_storage = "1Gi"
        engine.config.observability.pushgateway_storage_class = "px-csi-scratch"
        engine.context = {"namespace": "lb-test"}
        return ObservabilityDeployer(engine)

    def _rendered_templates(self, dep: ObservabilityDeployer) -> list[str]:
        dep.renderer.render = MagicMock(return_value=None)  # None -> skip apply
        dep._apply_podmonitor_templates("lb-test")
        return [call.args[0] for call in dep.renderer.render.call_args_list]

    def test_pushgateway_applied_when_enabled(self):
        dep = self._deployer(pushgateway_enabled=True)
        rendered = self._rendered_templates(dep)
        for t in PUSHGATEWAY_TEMPLATES:
            assert t in rendered

    def test_pushgateway_excluded_when_disabled(self):
        dep = self._deployer(pushgateway_enabled=False)
        rendered = self._rendered_templates(dep)
        for t in PUSHGATEWAY_TEMPLATES:
            assert t not in rendered
        # The base monitors still render.
        assert "prometheus/podmonitor-trino.yaml.j2" in rendered
