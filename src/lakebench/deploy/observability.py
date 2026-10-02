"""Observability stack deployment for Lakebench.

Deploys kube-prometheus-stack via Helm. The Helm chart bundles
Prometheus, Grafana, node-exporter, and kube-state-metrics in a
single install. After Helm, renders and applies PodMonitor CRDs
for Trino JMX and Spark PrometheusServlet scraping.

The stack is a shared cluster component (ownership category 3): the chart
installs CRDs, cluster roles and admission webhooks that exist once per
cluster. It is installed once, in its own namespace
(``OBSERVABILITY_NAMESPACE``), only when no release of it exists anywhere
on the cluster, and an existing release is never upgraded or modified by
``deploy``. ``destroy`` never uninstalls the shared release: deployment A's
teardown must not remove deployment B's monitoring (DESIGN.md invariant 6).
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
    from .engine import DeploymentEngine

logger = logging.getLogger(__name__)

HELM_RELEASE_NAME = "lakebench-observability"
HELM_CHART = "prometheus-community/kube-prometheus-stack"
# Shared namespace for the one cluster-wide release. Before v1.6 each
# deployment installed the release into its own namespace.
OBSERVABILITY_NAMESPACE = "lakebench-observability"

SHARED_NOTICE = (
    "kube-prometheus-stack is a shared cluster component (CRDs, cluster roles, "
    "admission webhooks). lakebench installs it once, in namespace "
    f"'{OBSERVABILITY_NAMESPACE}', reuses it for every deployment, never upgrades an "
    "existing install, and never removes it on destroy. Remove it when no deployment "
    f"uses it: helm uninstall {HELM_RELEASE_NAME} -n {OBSERVABILITY_NAMESPACE}"
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
        """Deploy the observability stack.

        Skips if ``observability.enabled`` is False.
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
                message="Would deploy observability stack (kube-prometheus-stack)",
                elapsed_seconds=0,
            )

        try:
            from kubernetes import client as _kclient

            from lakebench.deploy.cluster_lock import ClusterLockError, cluster_lock

            # The existence check and the install happen under the cluster
            # lease, so two deploys cannot both see "absent" and both install.
            try:
                with cluster_lock(_kclient.CoreV1Api(), timeout=600):
                    result = self._deploy_locked(namespace, start)
            except ClusterLockError as e:
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.FAILED,
                    message=(
                        f"Could not take the cluster lease to check the shared "
                        f"observability stack: {e}. See 'lakebench admin status'."
                    ),
                    elapsed_seconds=time.time() - start,
                )
            # Readiness is waited for outside the lease, so a slow first
            # install does not stall other deployments' lease holders.
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
        except subprocess.TimeoutExpired as e:
            # Under the lease the timeout comes from the hold budget, and the
            # message (LeasedCommandTimeout) names the recovery.
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Helm install timed out ({e})",
                elapsed_seconds=time.time() - start,
            )
        except Exception as e:
            logger.exception("Observability deployment failed")
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Observability deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _deploy_locked(self, namespace: str, start: float) -> DeploymentResult:
        """Install the shared stack if absent, then apply this deployment's monitors."""
        try:
            existing_ns, status = find_observability_release_status(self._kube_context())
        except ObservabilityLookupError as e:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=(
                    f"Cannot tell whether the shared observability stack is installed "
                    f"({e}); not installing it. {SHARED_NOTICE}"
                ),
                elapsed_seconds=time.time() - start,
            )

        if existing_ns is not None:
            # Never upgrade or modify an existing release: another deployment
            # may depend on its current values. A release that is not
            # 'deployed' (a failed or interrupted install) is not reused as if
            # it worked, and is not repaired here either: that is an admin
            # decision for a shared component.
            if status in _UNUSABLE_STATUSES:
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.FAILED,
                    message=(
                        f"The shared observability release in namespace '{existing_ns}' "
                        f"has status '{status or 'unknown'}', with no running revision; not using or "
                        f"modifying it. A cluster admin can inspect it with 'helm status "
                        f"{HELM_RELEASE_NAME} -n {existing_ns}' and, if no deployment uses "
                        f"it, remove it with 'helm uninstall {HELM_RELEASE_NAME} -n "
                        f"{existing_ns}'."
                    ),
                    elapsed_seconds=time.time() - start,
                )
            self._apply_podmonitor_templates(namespace)
            self._apply_dashboard()
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
                # An upgrade in progress or a failed upgrade still has a
                # running earlier revision; readiness is checked after the lease.
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

        release_ns = OBSERVABILITY_NAMESPACE
        # Ensure helm repo is added
        self._add_helm_repo()

        # Build Helm values
        values = self._build_helm_values(namespace)

        # A fresh install, never an upgrade: this path runs only when no
        # release exists, and 'helm install' fails rather than modifying one
        # that appeared since the check.
        cmd = [
            "install",
            HELM_RELEASE_NAME,
            HELM_CHART,
            "--version",
            self.config.observability.chart_version,
            "--namespace",
            release_ns,
            "--create-namespace",
            # No --wait: the install runs under the cluster lease, and holding
            # it through image pulls would stall parallel watch-list changes.
            # The release record exists once this returns, so a concurrent
            # deploy sees it and does not install a second copy.
            "--timeout",
            "5m",
        ]
        for key, val in values.items():
            cmd.extend(["--set", f"{key}={val}"])

        # OpenShift: null out hardcoded securityContexts and disable
        # node-exporter (requires hostNetwork/hostPID/hostPath which SCC blocks)
        if self._is_openshift():
            openshift_values_file = self._write_openshift_values_file()
            cmd.extend(["-f", openshift_values_file])

        result = pinned_helm(
            self._kube_context(),
            cmd,
            capture_output=True,
            text=True,
            timeout=360,
        )

        if result.returncode != 0:
            # Filter out K8s API warnings (I0216... lines) to find real errors
            stderr_lines = (result.stderr or "").strip().splitlines()
            error_lines = [
                ln for ln in stderr_lines if not ln.lstrip().startswith(("I0", "W0", '"Warning'))
            ]
            error = (
                "\n".join(error_lines).strip()[:500]
                or result.stderr.strip()[:500]
                or "Unknown error"
            )
            recovery = (
                f"\nRecovery (cluster admin; the release is shared):\n"
                f"  helm status {HELM_RELEASE_NAME} -n {release_ns}\n"
                f"  helm uninstall {HELM_RELEASE_NAME} -n {release_ns}  # only if no deployment uses it\n"
                f"  lakebench deploy  # retry"
            )
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Helm install failed: {error}{recovery}",
                elapsed_seconds=time.time() - start,
            )

        # Apply PodMonitor and Prometheus ConfigMap templates
        self._apply_podmonitor_templates(namespace)
        self._apply_dashboard()

        prom_svc = _find_helm_service(
            release_ns, "kube-prometheus-stack-prometheus", context=self._kube_context()
        )
        grafana_svc = _find_helm_service(release_ns, "grafana", context=self._kube_context())
        prom_url = (
            f"http://{prom_svc}.{release_ns}.svc:9090"
            if prom_svc
            else f"http://{HELM_RELEASE_NAME}-prometheus.{release_ns}.svc:9090"
        )
        grafana_url = (
            f"http://{grafana_svc}.{release_ns}.svc:80"
            if grafana_svc
            else f"http://{HELM_RELEASE_NAME}-grafana.{release_ns}.svc:80"
        )
        logger.warning(SHARED_NOTICE)

        return DeploymentResult(
            component="observability",
            status=DeploymentStatus.SUCCESS,
            message=f"Observability stack installed (kube-prometheus-stack). {SHARED_NOTICE}",
            elapsed_seconds=time.time() - start,
            details={
                "helm_release": HELM_RELEASE_NAME,
                "release_namespace": release_ns,
                "prometheus_url": prom_url,
                "grafana_url": grafana_url,
                "retention": self.config.observability.retention,
            },
            label="Observability",
            detail="kube-prometheus-stack",
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

    def _apply_dashboard(self) -> None:
        """Apply the single cluster-wide Grafana dashboard (LB-192).

        Rendered once into OBSERVABILITY_NAMESPACE with a namespace + run_id
        template variable, idempotently on every deploy. Best-effort: a dashboard
        apply failure never fails the deploy.
        """
        if not self.config.observability.dashboards_enabled:
            return
        context = dict(self.context)
        context["observability_namespace"] = OBSERVABILITY_NAMESPACE
        try:
            yaml_content = self.renderer.render(DASHBOARD_TEMPLATE, context)
            if not isinstance(yaml_content, str):
                return
            for doc in yaml.safe_load_all(yaml_content):
                if doc:
                    self.k8s.apply_manifest(doc, namespace=OBSERVABILITY_NAMESPACE)
        except Exception as e:
            logger.warning("Failed to apply %s: %s", DASHBOARD_TEMPLATE, e)

    def _add_helm_repo(self) -> None:
        """Add the prometheus-community Helm repo if not present."""
        ctx = self._kube_context()
        pinned_helm(
            ctx,
            [
                "repo",
                "add",
                "prometheus-community",
                "https://prometheus-community.github.io/helm-charts",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        pinned_helm(
            ctx,
            ["repo", "update"],
            capture_output=True,
            text=True,
            timeout=60,
        )

    def _is_openshift(self) -> bool:
        """Detect if running on OpenShift."""
        try:
            verifier = SecurityVerifier(self.k8s)
            return verifier.detect_platform() == PlatformType.OPENSHIFT
        except Exception:
            return False

    def _build_helm_values(self, namespace: str) -> dict[str, str]:
        """Build Helm --set values for kube-prometheus-stack."""
        obs = self.config.observability
        values: dict[str, str] = {
            "prometheus.prometheusSpec.retention": obs.retention,
            "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage": obs.storage,
            "grafana.enabled": str(obs.dashboards_enabled).lower(),
            "grafana.adminPassword": "lakebench",
            # The dashboard ConfigMap lives in the shared observability
            # namespace (LB-192); ALL also picks up older per-namespace ones.
            "grafana.sidecar.dashboards.searchNamespace": "ALL",
        }
        # No namespace selector: the chart default ({}) watches every
        # namespace, so the one shared Prometheus scrapes the PodMonitors each
        # deployment applies in its own namespace. podMonitorSelector still
        # requires the release label those PodMonitors carry.

        return values

    def _write_openshift_values_file(self) -> str:
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
