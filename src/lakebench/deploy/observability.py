"""Observability stack for Lakebench.

Per-deployment Prometheus, Grafana and Pushgateway, each in the deployment
namespace (ownership category 1): torn down with the namespace, or by
``destroy``'s category1 step when the namespace survives. Two deployments
on the same cluster never collide, and no shared Helm release exists.

The one cluster-scoped grant is a ClusterRole (``lakebench-kubelet-metrics``)
that lets Prometheus read the kubelet's cAdvisor endpoint for per-pod CPU
and memory. ``lakebench admin install --component observability`` creates
it; ``deploy`` binds the deployment's service account to it, and
``destroy`` removes the binding. A deploy without permission to create the
binding logs the refusal and continues; platform CPU and memory then read
``not collected``, and the rest of the stack is unaffected.
"""

from __future__ import annotations

import logging
import secrets
import subprocess
import time
from typing import TYPE_CHECKING

import yaml

from lakebench.deploy import deadline as deploy_deadline
from lakebench.k8s import pinned_kubectl

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from .engine import DeploymentEngine

logger = logging.getLogger(__name__)

#: ClusterRole name created by ``admin install --component observability``.
KUBELET_METRICS_CLUSTER_ROLE = "lakebench-kubelet-metrics"

#: Status message used when the stack is deployed.
STACK_LABEL = "Observability"

_PROMETHEUS_READY_TIMEOUT_S = 300
_GRAFANA_READY_TIMEOUT_S = 120
_READY_JSONPATH = '{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\\n"}{end}'

#: Templates deploy applies into the deployment namespace, in order.
_NAMESPACE_TEMPLATES = [
    "prometheus/prometheus.yaml.j2",
    "grafana/grafana.yaml.j2",
    "grafana/dashboard-configmap.yaml.j2",
]

_GRAFANA_ROUTE_TEMPLATE = "grafana/grafana-route.yaml.j2"

_PUSHGATEWAY_TEMPLATES = [
    "pushgateway/pushgateway.yaml.j2",
]

#: ClusterRole + binding. The role is applied by admin install; the binding
#: is applied by deploy and deleted by destroy.
_KUBELET_ROLE_TEMPLATE = "prometheus/kubelet-metrics-clusterrole.yaml.j2"
_KUBELET_BINDING_TEMPLATE = "prometheus/kubelet-metrics-binding.yaml.j2"


def _wait_for_deployment_ready(
    namespace: str,
    label_selector: str,
    timeout_s: float,
    context: str | None = None,
) -> str:
    """Wait until a pod matching ``label_selector`` is Ready.

    Returns the empty string on success, or a short reason on timeout. Used
    for Prometheus (platform collector depends on it) and Grafana (the user
    expects to port-forward right after deploy).
    """
    deadline = time.time() + timeout_s
    last = ""
    while True:
        try:
            r = pinned_kubectl(
                context,
                [
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    label_selector,
                    "-o",
                    f"jsonpath={_READY_JSONPATH}",
                ],
                capture_output=True,
                text=True,
                timeout=max(1.0, min(30.0, deadline - time.time())),
            )
            if r.returncode == 0 and "True" in (r.stdout or "").split():
                return ""
            last = (r.stderr or "").strip()[:200] or "no Ready pod yet"
        except (OSError, subprocess.TimeoutExpired) as e:
            last = str(e)
        if time.time() >= deadline:
            deploy_deadline.check(f"{label_selector} Ready in namespace {namespace}", last)
            return (
                f"{label_selector} in namespace '{namespace}' was not Ready after "
                f"{timeout_s}s ({last})"
            )
        time.sleep(max(0.0, min(5.0, deadline - time.time())))


def _generate_grafana_password() -> str:
    """Generate a per-deployment admin password (24 URL-safe characters)."""
    return secrets.token_urlsafe(18)


def read_grafana_password(namespace: str, context: str | None = None) -> str | None:
    """Return the live Grafana admin password from the Secret, or None.

    Deploy's success message and ``docs/component-observability.md`` both
    point readers at the Secret for the password; this helper is used by
    anything that needs to show it without the user running kubectl.
    """
    try:
        from kubernetes import client as k8s_client

        from lakebench.k8s.target import ClusterTarget
    except Exception:
        return None
    try:
        ClusterTarget.resolve(context=context or "").activate()
        v1 = k8s_client.CoreV1Api()
        secret = v1.read_namespaced_secret("lakebench-grafana", namespace)
    except Exception:
        return None
    data = secret.data or {}
    import base64

    enc = data.get("admin-password")
    if not enc:
        return None
    try:
        return base64.b64decode(enc).decode("utf-8")
    except Exception:
        return None


def grafana_route_host(namespace: str, context: str | None = None) -> str | None:
    """The host of this deployment's Grafana Route, or None (no Route, not OpenShift,
    or not readable)."""
    try:
        from kubernetes import client as k8s_client

        from lakebench.k8s.target import ClusterTarget

        ClusterTarget.resolve(context=context or "").activate()
        route = k8s_client.CustomObjectsApi().get_namespaced_custom_object(
            "route.openshift.io", "v1", namespace, "routes", "lakebench-grafana"
        )
    except Exception:
        return None
    ingress = ((route.get("status") or {}).get("ingress")) or []
    host = (ingress[0].get("host") if ingress else None) or (route.get("spec") or {}).get("host")
    return host or None


def kubelet_cluster_role_exists(context: str | None = None) -> bool | None:
    """Whether the kubelet-metrics ClusterRole exists.

    ``True`` present, ``False`` absent, ``None`` unknown (403 or an API
    error). ``admin install`` creates it; ``deploy`` reads it to decide
    whether to apply the binding.
    """
    try:
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        from lakebench.k8s.target import ClusterTarget
    except Exception:
        return None
    try:
        ClusterTarget.resolve(context=context or "").activate()
        k8s_client.RbacAuthorizationV1Api().read_cluster_role(KUBELET_METRICS_CLUSTER_ROLE)
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        return None
    except Exception:
        return None


def kubelet_cluster_role_manifests() -> list[dict]:
    """The ClusterRole manifest(s) ``admin install`` applies."""
    from lakebench.deploy.engine import TemplateRenderer

    text = TemplateRenderer().render(_KUBELET_ROLE_TEMPLATE, {})
    return [d for d in yaml.safe_load_all(text) if d]


class ObservabilityDeployer:
    """Deploy and destroy the per-namespace Prometheus, Grafana and Pushgateway."""

    def __init__(self, engine: DeploymentEngine):
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    def _kube_context(self) -> str | None:
        return self.config.platform.kubernetes.context or None

    def deploy(self) -> DeploymentResult:
        """Apply this deployment's Prometheus, Grafana and Pushgateway.

        Skipped when ``observability.enabled`` is False. On success the
        caller may print how to reach Grafana; see
        ``docs/component-observability.md``.
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if not self.config.observability.enabled:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SKIPPED,
                message="Observability not enabled in config",
                elapsed_seconds=0,
            )

        if self.engine.dry_run:
            stack = "Prometheus, Grafana and the Pushgateway"
            if not self.config.observability.pushgateway_enabled:
                stack = "Prometheus and Grafana"
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SUCCESS,
                message=f"Would deploy {stack} in namespace '{namespace}'",
                elapsed_seconds=0,
            )

        try:
            return self._deploy_stack(namespace, start)
        except Exception as e:
            logger.exception("Observability deployment failed")
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Observability deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _deploy_stack(self, namespace: str, start: float) -> DeploymentResult:
        obs = self.config.observability

        base_context = dict(self.context)
        base_context["prometheus_image"] = obs.prometheus_image
        base_context["prometheus_storage"] = obs.storage
        base_context["prometheus_storage_class"] = obs.prometheus_storage_class
        base_context["prometheus_retention"] = obs.retention
        base_context["grafana_image"] = obs.grafana_image
        base_context["grafana_admin_password"] = self._admin_password(namespace)

        templates = list(_NAMESPACE_TEMPLATES)
        if not obs.dashboards_enabled:
            templates.remove("grafana/dashboard-configmap.yaml.j2")
        if obs.grafana_route and base_context.get("openshift_mode"):
            templates.append(_GRAFANA_ROUTE_TEMPLATE)
        if obs.pushgateway_enabled:
            templates += _PUSHGATEWAY_TEMPLATES
            base_context.update(
                pushgateway_image=obs.pushgateway_image,
                pushgateway_storage=obs.pushgateway_storage,
                pushgateway_storage_class=obs.pushgateway_storage_class,
            )

        apply_notes: list[str] = []
        for template_name in templates:
            try:
                yaml_text = self.renderer.render(template_name, base_context)
            except Exception as e:
                apply_notes.append(f"render {template_name}: {e}")
                logger.warning("Failed to render %s: %s", template_name, e)
                continue
            if not isinstance(yaml_text, str):
                continue
            for doc in yaml.safe_load_all(yaml_text):
                if not doc:
                    continue
                try:
                    self.k8s.apply_manifest(doc, namespace=namespace)
                except Exception as e:
                    apply_notes.append(f"apply {template_name}/{doc.get('kind')}: {e}")
                    logger.warning(
                        "Failed to apply %s/%s: %s",
                        template_name,
                        doc.get("kind"),
                        e,
                    )

        binding_note = self._apply_kubelet_binding(namespace)

        wait_notes: list[str] = []
        prom_wait = _wait_for_deployment_ready(
            namespace,
            "app.kubernetes.io/name=lakebench,app.kubernetes.io/component=prometheus",
            timeout_s=deploy_deadline.clamp(_PROMETHEUS_READY_TIMEOUT_S),
            context=self._kube_context(),
        )
        if prom_wait:
            wait_notes.append(prom_wait)
        grafana_wait = _wait_for_deployment_ready(
            namespace,
            "app.kubernetes.io/name=lakebench,app.kubernetes.io/component=grafana",
            timeout_s=deploy_deadline.clamp(_GRAFANA_READY_TIMEOUT_S),
            context=self._kube_context(),
        )
        if grafana_wait:
            wait_notes.append(grafana_wait)

        details = {
            "namespace": namespace,
            "prometheus_service": f"lakebench-prometheus.{namespace}.svc",
            "grafana_service": f"lakebench-grafana.{namespace}.svc",
            "kubelet_binding": binding_note or "applied",
        }
        if obs.grafana_route and base_context.get("openshift_mode"):
            host = grafana_route_host(namespace, context=self._kube_context())
            if host:
                details["grafana_url"] = f"https://{host}"

        if prom_wait:
            # Prometheus not Ready is a hard failure: without it the platform
            # collector reports nothing for this run.
            msg = f"Prometheus not Ready: {prom_wait}"
            if apply_notes:
                msg = f"{msg}; {'; '.join(apply_notes)}"
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=msg,
                elapsed_seconds=time.time() - start,
                details=details,
            )

        detail_parts = [f"in {namespace}"]
        if binding_note:
            detail_parts.append(binding_note)
        if grafana_wait:
            detail_parts.append(f"grafana: {grafana_wait}")
        detail_parts += apply_notes

        stack = (
            "Prometheus, Grafana and Pushgateway"
            if obs.pushgateway_enabled
            else "Prometheus and Grafana"
        )
        return DeploymentResult(
            component="observability",
            status=DeploymentStatus.SUCCESS,
            message=(
                f"Deployed {stack} in '{namespace}'"
                + (f" ({binding_note})" if binding_note else "")
                + (f"; {'; '.join(apply_notes)}" if apply_notes else "")
            ),
            elapsed_seconds=time.time() - start,
            details=details,
            label=STACK_LABEL,
            detail=", ".join(detail_parts),
        )

    def _admin_password(self, namespace: str) -> str:
        """Keep an existing Grafana password; generate one on first install."""
        existing = read_grafana_password(namespace, context=self._kube_context())
        return existing or _generate_grafana_password()

    def _apply_kubelet_binding(self, namespace: str) -> str:
        """Apply the ClusterRoleBinding; a short status string.

        Returns ``""`` on success, or a short reason (``role missing``,
        ``forbidden``, ...). The caller treats a non-empty string as a
        warning, not a failure.
        """
        role_present = kubelet_cluster_role_exists(context=self._kube_context())
        if role_present is False:
            return (
                "kubelet metrics role missing: platform CPU and memory not collected. "
                "A cluster admin runs 'lakebench admin install --component observability'"
            )
        if role_present is None:
            logger.warning(
                "cannot read ClusterRole %s; attempting to apply the binding",
                KUBELET_METRICS_CLUSTER_ROLE,
            )

        try:
            text = self.renderer.render(_KUBELET_BINDING_TEMPLATE, {"namespace": namespace})
        except Exception as e:
            logger.warning("Failed to render kubelet binding: %s", e)
            return f"kubelet binding render failed ({e})"
        for doc in yaml.safe_load_all(text):
            if not doc:
                continue
            try:
                self.k8s.apply_manifest(doc)
            except Exception as e:
                logger.warning("Failed to apply kubelet ClusterRoleBinding: %s", e)
                return "kubelet binding forbidden: platform CPU and memory not collected"
        return ""

    def destroy(self) -> DeploymentResult:
        """Delete the per-deployment ClusterRoleBinding; everything else goes
        with the namespace or by the category1 step.
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.engine.dry_run:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SKIPPED,
                message="Dry run: would delete the kubelet-metrics binding",
                elapsed_seconds=0,
            )

        binding_name = f"{KUBELET_METRICS_CLUSTER_ROLE}-{namespace}"
        try:
            from kubernetes import client as k8s_client
            from kubernetes.client.rest import ApiException
        except Exception as e:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SKIPPED,
                message=f"Kubernetes client not available: {e}",
                elapsed_seconds=time.time() - start,
            )

        try:
            k8s_client.RbacAuthorizationV1Api().delete_cluster_role_binding(binding_name)
        except ApiException as e:
            if e.status == 404:
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.SUCCESS,
                    message=f"No kubelet binding '{binding_name}' to remove",
                    elapsed_seconds=time.time() - start,
                )
            if e.status == 403:
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.SUCCESS,
                    message=(
                        f"Could not remove ClusterRoleBinding '{binding_name}' "
                        f"({e.reason}); a cluster admin can delete it"
                    ),
                    elapsed_seconds=time.time() - start,
                )
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Failed to delete ClusterRoleBinding '{binding_name}': {e.reason}",
                elapsed_seconds=time.time() - start,
            )
        except Exception as e:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Failed to delete ClusterRoleBinding '{binding_name}': {e}",
                elapsed_seconds=time.time() - start,
            )

        return DeploymentResult(
            component="observability",
            status=DeploymentStatus.SUCCESS,
            message=f"Removed ClusterRoleBinding '{binding_name}'",
            elapsed_seconds=time.time() - start,
        )
