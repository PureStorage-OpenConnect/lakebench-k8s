"""Observability stack for Lakebench.

kube-prometheus-stack (Prometheus, Grafana, node-exporter and
kube-state-metrics in one Helm chart) plus the PodMonitor CRDs each
deployment applies for Trino JMX and Spark PrometheusServlet scraping.

The stack is a shared cluster component (ownership category 3): the chart
installs CRDs, cluster roles and admission webhooks that exist once per
cluster. A cluster admin installs it once, in its own namespace
(``OBSERVABILITY_NAMESPACE``), with ``lakebench admin install --component
observability`` (``deploy/shared_components.py``), which installs only when no
release of it exists anywhere on the cluster. ``deploy`` only checks that it is
there and applies the deployment's own PodMonitors; it never installs,
upgrades or modifies the release. ``destroy`` never uninstalls the shared
release: deployment A's teardown must not remove deployment B's monitoring
(DESIGN.md invariant 6).
Each deployment's own PodMonitors and Pushgateway live in its namespace:
they go with it, and when ``create_namespace: false`` keeps it, destroy's
category1 step deletes them by name (``deploy/category1.py``).
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
import time
from typing import TYPE_CHECKING

import yaml

from lakebench.k8s import PlatformType, SecurityVerifier, pinned_helm, pinned_kubectl

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from lakebench.config.schema import ObservabilityConfig

    from .engine import DeploymentEngine

logger = logging.getLogger(__name__)

HELM_RELEASE_NAME = "lakebench-observability"
HELM_CHART = "prometheus-community/kube-prometheus-stack"
# Shared namespace for the one cluster-wide release. Before v1.6 each
# deployment installed the release into its own namespace.
OBSERVABILITY_NAMESPACE = "lakebench-observability"

SHARED_NOTICE = (
    "kube-prometheus-stack is a shared cluster component (CRDs, cluster roles, "
    "admission webhooks). A cluster admin installs it once with 'lakebench admin install "
    f"--component observability', in namespace '{OBSERVABILITY_NAMESPACE}'; deploy reuses it "
    "for every deployment, never upgrades it, and destroy never removes it. Remove it when "
    f"no deployment uses it: helm uninstall {HELM_RELEASE_NAME} -n {OBSERVABILITY_NAMESPACE}"
)


# Release states with nothing usable behind them: a first install still
# running or never finished, or a release being removed. Other states
# (deployed, superseded, pending-upgrade/rollback, a failed upgrade) keep a
# running revision and are reused; Prometheus readiness is checked separately.
_UNUSABLE_STATUSES = {"pending-install", "uninstalling", "uninstalled", "unknown", ""}
_PROMETHEUS_READY_TIMEOUT_S = 600
_READY_JSONPATH = '{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\\n"}{end}'


def _wait_for_prometheus(
    namespace: str,
    timeout_s: int = _PROMETHEUS_READY_TIMEOUT_S,
    context: str | None = None,
) -> str:
    """Wait until a Prometheus pod of the release is Ready; '' when it is, else why not."""
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
                    "app.kubernetes.io/name=prometheus",
                    "-o",
                    f"jsonpath={_READY_JSONPATH}",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode == 0 and "True" in (r.stdout or "").split():
                return ""
            last = (r.stderr or "").strip()[:200] or "no Ready Prometheus pod yet"
        except (OSError, subprocess.TimeoutExpired) as e:
            last = str(e)
        if time.time() >= deadline:
            return (
                f"Prometheus in namespace '{namespace}' was not Ready after {timeout_s}s "
                f"({last}); platform metrics would be empty"
            )
        time.sleep(10)


class ObservabilityLookupError(RuntimeError):
    """The cluster's Helm releases could not be listed."""


def find_observability_release(context: str | None = None) -> str | None:
    """Namespace of the cluster's observability release, or None if there is none."""
    return find_observability_release_status(context)[0]


def find_observability_release_status(
    context: str | None = None,
) -> tuple[str | None, str]:
    """(namespace, helm status) of the cluster's observability release.

    Any release of that name counts, whatever its status, so a failed or
    pending install is never installed over. ``(None, "")`` when there is none.

    Raises ObservabilityLookupError when helm cannot list releases: an
    unknown answer must not be read as "absent", or deploy would install a
    second copy over the cluster-scoped objects of the first.
    """
    import json

    try:
        result = pinned_helm(
            context,
            [
                "list",
                "--all-namespaces",
                "--all",
                "--filter",
                f"^{HELM_RELEASE_NAME}$",
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        raise ObservabilityLookupError(f"helm list failed: {e}") from e
    if result.returncode != 0:
        raise ObservabilityLookupError(
            f"helm list failed: {(result.stderr or '').strip()[:300] or 'unknown error'}"
        )
    try:
        releases = json.loads(result.stdout or "[]") or []
    except ValueError as e:
        raise ObservabilityLookupError(f"helm list returned unreadable output: {e}") from e
    by_ns = {
        r.get("namespace"): str(r.get("status") or "")
        for r in releases
        if r.get("name") == HELM_RELEASE_NAME and r.get("namespace")
    }
    if not by_ns:
        return None, ""
    # Prefer the shared namespace when an older per-deployment release also
    # exists; otherwise the first, deterministically.
    ns = OBSERVABILITY_NAMESPACE if OBSERVABILITY_NAMESPACE in by_ns else sorted(by_ns)[0]
    return ns, by_ns[ns]


def _find_helm_service(
    namespace: str,
    app_label: str,
    context: str | None = None,
) -> str | None:
    """Find a Helm-managed service by its app label.

    The kube-prometheus-stack chart truncates service names based on
    the release name length, making them unpredictable. This function
    looks up the actual service name by Helm release + app labels.

    Tries the K8s Python client first, then falls back to kubectl.
    """
    label = f"release={HELM_RELEASE_NAME},app={app_label}"

    # Attempt 1: K8s Python client, on the process's active cluster target
    # (or this context's, when none is active yet). Never reloads another
    # context; a conflicting one raises rather than reaching another cluster.
    from lakebench.k8s.target import ClusterTarget, ContextConflictError

    try:
        from kubernetes import client as k8s_client

        ClusterTarget.resolve(context=context or "").activate()
        v1 = k8s_client.CoreV1Api()
        svcs = v1.list_namespaced_service(namespace, label_selector=label)
        if svcs.items:
            return svcs.items[0].metadata.name
    except ContextConflictError:
        raise
    except Exception:
        pass

    # Attempt 2: kubectl fallback
    try:
        result = pinned_kubectl(
            context,
            [
                "get",
                "svc",
                "-n",
                namespace,
                "-l",
                label,
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        svc_name = result.stdout.strip()
        if result.returncode == 0 and svc_name:
            return svc_name
    except Exception:
        pass

    return None


# PodMonitor templates applied after the Helm install so Prometheus
# Operator can immediately reconcile them.
PODMONITOR_TEMPLATES = [
    "prometheus/podmonitor-trino.yaml.j2",
    "prometheus/podmonitor-spark.yaml.j2",
    "prometheus/configmap.yaml.j2",
]

# The Grafana dashboard is rendered ONCE into the shared observability namespace,
# not per-deployment (LB-192: a fixed uid rendered per-namespace collides in the
# shared Grafana). A namespace + run_id template variable makes the single
# dashboard serve every deployment/run.
DASHBOARD_TEMPLATE = "grafana/dashboard-configmap.yaml.j2"

# Per-deployment Prometheus Pushgateway (Deployment + Service + PVC) and the
# PodMonitor that scrapes it. Applied into the deployment namespace alongside
# the PodMonitors, only when observability.pushgateway_enabled. Ownership
# category 1: torn down with the namespace by `destroy`, or by its category1
# step when the namespace survives.
PUSHGATEWAY_TEMPLATES = [
    "pushgateway/pushgateway.yaml.j2",
    "pushgateway/podmonitor-pushgateway.yaml.j2",
]


class ObservabilityDeployer:
    """Deploys the observability stack via kube-prometheus-stack Helm chart."""

    def __init__(self, engine: DeploymentEngine):
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    def _kube_context(self) -> str | None:
        """Return the configured kubeconfig context, or None to use current."""
        return self.config.platform.kubernetes.context or None

    def deploy(self) -> DeploymentResult:
        """Check the shared observability stack, then apply this deployment's monitors.

        Skips if ``observability.enabled`` is False. Never installs, upgrades
        or modifies the shared release, and writes nothing outside this
        deployment's namespace, so it needs no cluster lease: a cluster admin
        installs the stack once with ``lakebench admin install --component
        observability``.
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
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SUCCESS,
                message="Would check the shared observability stack and apply this "
                "deployment's monitors",
                elapsed_seconds=0,
            )

        try:
            result = self._deploy_monitors(namespace, start)
            release_ns = (result.details or {}).get("release_namespace")
            if result.status == DeploymentStatus.SUCCESS and release_ns:
                problem = _wait_for_prometheus(release_ns, context=self._kube_context())
                if problem:
                    return DeploymentResult(
                        component="observability",
                        status=DeploymentStatus.FAILED,
                        message=f"{problem}. {SHARED_NOTICE}",
                        elapsed_seconds=time.time() - start,
                        details=result.details,
                    )
            return result
        except Exception as e:
            logger.exception("Observability deployment failed")
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Observability deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _deploy_monitors(self, namespace: str, start: float) -> DeploymentResult:
        """Verify the shared release, then apply this deployment's monitors."""
        try:
            existing_ns, status = find_observability_release_status(self._kube_context())
        except ObservabilityLookupError as e:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=(
                    f"Cannot tell whether the shared observability stack is installed "
                    f"({e}). {SHARED_NOTICE}"
                ),
                elapsed_seconds=time.time() - start,
            )

        if existing_ns is None:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=(
                    "The shared observability stack (kube-prometheus-stack) is not installed. "
                    f"A cluster admin installs it once: {INSTALL_COMMAND}. Or set "
                    "observability.enabled: false."
                ),
                elapsed_seconds=time.time() - start,
            )

        # Deploy never upgrades or modifies the release: another deployment
        # may depend on its current values. A release with no running revision
        # (an install still running or never finished) is not used.
        if status in _UNUSABLE_STATUSES:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=(
                    f"The shared observability release in namespace '{existing_ns}' has status "
                    f"'{status or 'unknown'}', with no running revision; not using it. An "
                    "admin install may still be running: check 'lakebench admin status', then "
                    "'lakebench admin doctor'."
                ),
                elapsed_seconds=time.time() - start,
            )
        self._apply_podmonitor_templates(namespace)
        if existing_ns == OBSERVABILITY_NAMESPACE:
            message = (
                f"Using the shared observability stack in namespace '{existing_ns}' "
                f"(left unchanged). {SHARED_NOTICE}"
            )
        else:
            message = (
                f"An observability release from an older lakebench exists in namespace "
                f"'{existing_ns}'. It is left unchanged and scrapes only that namespace, "
                f"so this deployment's pods are not monitored until it is removed and "
                f"the shared stack is installed. {SHARED_NOTICE}"
            )
        if status != "deployed":
            # An upgrade in progress or a failed upgrade still has a running
            # earlier revision; readiness is checked next.
            message = f"Release status is '{status}'. {message}"
        logger.warning(message)
        return DeploymentResult(
            component="observability",
            status=DeploymentStatus.SUCCESS,
            message=message,
            elapsed_seconds=time.time() - start,
            details={"helm_release": HELM_RELEASE_NAME, "release_namespace": existing_ns},
            label="Observability",
            detail=f"shared, in {existing_ns}",
        )

    def destroy(self) -> DeploymentResult:
        """Leave the shared observability stack in place.

        The kube-prometheus-stack release is shared by every deployment on
        the cluster; uninstalling it would remove other deployments'
        monitoring. This deployment's PodMonitors and Pushgateway are in its
        own namespace and are removed with it (or by destroy's category1 step
        when the namespace survives); the shared dashboard ConfigMap
        in the observability namespace (LB-192) is left in place. The one release destroy
        removes is a pre-v1.6 release installed into this deployment's own
        namespace: it scrapes only that namespace, and deleting the namespace
        without uninstalling it would orphan its cluster-scoped webhooks and
        cluster roles.
        """
        start = time.time()
        namespace = self.config.get_namespace()
        kept = (
            f"Shared observability stack left in place (namespace "
            f"'{OBSERVABILITY_NAMESPACE}'); this deployment's monitors go with its namespace."
        )

        if self.engine.dry_run:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SKIPPED,
                message=kept,
                elapsed_seconds=0,
            )
        if namespace == OBSERVABILITY_NAMESPACE:
            # The shared namespace itself: never uninstall from here.
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SKIPPED,
                message=kept,
                elapsed_seconds=time.time() - start,
            )

        try:
            listed = pinned_helm(
                self._kube_context(),
                [
                    "list",
                    "--namespace",
                    namespace,
                    "--all",
                    "--filter",
                    f"^{HELM_RELEASE_NAME}$",
                    "--short",
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if listed.returncode != 0:
                # Cannot tell whether a pre-v1.6 release sits in this
                # namespace; deleting the namespace would then orphan its
                # cluster-scoped objects. Say so rather than report success.
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.FAILED,
                    message=(
                        f"Could not list Helm releases in '{namespace}' "
                        f"({(listed.stderr or '').strip()[:200] or 'helm error'}). If an older "
                        f"lakebench installed {HELM_RELEASE_NAME} here, run 'helm uninstall "
                        f"{HELM_RELEASE_NAME} -n {namespace}' before the namespace is deleted. "
                        f"{kept}"
                    ),
                    elapsed_seconds=time.time() - start,
                )
            if HELM_RELEASE_NAME not in (listed.stdout or "").split():
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.SKIPPED,
                    message=kept,
                    elapsed_seconds=time.time() - start,
                )

            # A pre-v1.6 release in this deployment's own namespace.
            result = pinned_helm(
                self._kube_context(),
                ["uninstall", HELM_RELEASE_NAME, "--namespace", namespace],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode != 0 and "not found" not in (result.stderr or ""):
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.FAILED,
                    message=f"Helm uninstall of the pre-v1.6 release in {namespace} failed: "
                    f"{result.stderr}",
                    elapsed_seconds=time.time() - start,
                )
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SUCCESS,
                message=(
                    f"Removed the pre-v1.6 observability release from namespace '{namespace}'. "
                    f"{kept}"
                ),
                elapsed_seconds=time.time() - start,
            )
        except Exception as e:
            logger.exception("Observability destroy check failed")
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"{kept} Release check failed ({e}); a pre-v1.6 release in "
                f"'{namespace}' would be orphaned by the namespace delete.",
                elapsed_seconds=time.time() - start,
            )

    def _apply_podmonitor_templates(self, namespace: str) -> None:
        """Render and apply this deployment's per-namespace monitors.

        Always the Trino/Spark PodMonitors + the Prometheus configmap. Plus the
        per-deployment Pushgateway (Deployment + Service + PVC) and its PodMonitor
        when observability.pushgateway_enabled -- category 1, torn down with the
        namespace. All best-effort: a monitor that fails to apply must not fail
        the deploy (metrics.json stays authoritative).
        """
        obs = self.config.observability
        templates = list(PODMONITOR_TEMPLATES)
        context = dict(self.context)
        if obs.pushgateway_enabled:
            templates += PUSHGATEWAY_TEMPLATES
            context.update(
                pushgateway_image=obs.pushgateway_image,
                pushgateway_storage=obs.pushgateway_storage,
                pushgateway_storage_class=obs.pushgateway_storage_class,
            )
        for template_name in templates:
            try:
                yaml_content = self.renderer.render(template_name, context)
            except Exception as e:
                logger.warning("Failed to render %s: %s", template_name, e)
                continue
            if not isinstance(yaml_content, str):
                continue
            # Apply doc-by-doc: one failing doc must not suppress its siblings in
            # the same file (e.g. a PVC failure must not skip the Deployment).
            for doc in yaml.safe_load_all(yaml_content):
                if not doc:
                    continue
                try:
                    self.k8s.apply_manifest(doc, namespace=namespace)
                except Exception as e:
                    logger.warning("Failed to apply %s/%s: %s", template_name, doc.get("kind"), e)


# ---------------------------------------------------------------------------
# The shared install, run only by ``lakebench admin install --component
# observability`` (deploy/shared_components.py) under the cluster lease.
# ---------------------------------------------------------------------------

#: The command that installs the shared stack, named in deploy's failures.
INSTALL_COMMAND = "lakebench admin install --component observability <config>"


def is_openshift(context: str | None) -> bool:
    """Detect if running on OpenShift."""
    try:
        from lakebench.k8s import get_k8s_client

        verifier = SecurityVerifier(get_k8s_client(context=context or "", namespace="default"))
        return verifier.detect_platform() == PlatformType.OPENSHIFT
    except Exception:
        return False


def build_helm_values(observability: ObservabilityConfig) -> dict[str, str]:
    """Helm --set values for the shared kube-prometheus-stack install."""
    values: dict[str, str] = {
        "prometheus.prometheusSpec.retention": observability.retention,
        "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage": observability.storage,
        "grafana.enabled": str(observability.dashboards_enabled).lower(),
        # No grafana.adminPassword: the chart generates one per
        # install into the Secret <release>-grafana. An existing install
        # keeps the value it was installed with.
        # The dashboard ConfigMap lives in the shared observability
        # namespace (LB-192); ALL also picks up older per-namespace ones.
        "grafana.sidecar.dashboards.searchNamespace": "ALL",
    }
    # No namespace selector: the chart default ({}) watches every
    # namespace, so the one shared Prometheus scrapes the PodMonitors each
    # deployment applies in its own namespace. podMonitorSelector still
    # requires the release label those PodMonitors carry.
    return values


def write_openshift_values_file() -> str:
    """Write a temp values file that nulls out hardcoded securityContexts.

    OpenShift assigns UIDs from the namespace annotation range.
    The chart's hardcoded runAsUser/fsGroup values (e.g. 2000, 65534)
    are rejected by SCC. A values file is needed because ``--set key=null``
    passes the string literal "null", not YAML null.
    """
    import yaml

    # Shared securityContext override -- null out everything
    _null_sc: dict[str, None] = {
        "runAsUser": None,
        "runAsGroup": None,
        "fsGroup": None,
    }

    overrides = {
        "prometheusOperator": {
            "securityContext": _null_sc,
            "admissionWebhooks": {
                "patch": {
                    "securityContext": _null_sc,
                    "podSecurityContext": {
                        "runAsUser": None,
                        "runAsNonRoot": True,
                    },
                },
            },
        },
        "prometheus": {
            "prometheusSpec": {"securityContext": _null_sc},
        },
        "alertmanager": {
            "alertmanagerSpec": {"securityContext": _null_sc},
        },
        "grafana": {"securityContext": _null_sc},
        "kube-state-metrics": {"securityContext": _null_sc},
        # node-exporter needs hostNetwork/hostPID/hostPath -- blocked by SCC
        "nodeExporter": {"enabled": False},
        "prometheusNodeExporter": {"enabled": False},
    }

    fp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", prefix="lb-obs-", delete=False)
    yaml.safe_dump(overrides, fp, default_flow_style=False)
    fp.close()
    return fp.name
