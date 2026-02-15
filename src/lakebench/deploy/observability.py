"""Observability stack deployment for Lakebench.

Deploys kube-prometheus-stack via Helm. The Helm chart bundles
Prometheus, Grafana, node-exporter, and kube-state-metrics in a
single install.
"""

from __future__ import annotations

import logging
import subprocess
import time
from typing import TYPE_CHECKING

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from .engine import DeploymentEngine

logger = logging.getLogger(__name__)

HELM_RELEASE_NAME = "lakebench-observability"
HELM_CHART = "prometheus-community/kube-prometheus-stack"


class ObservabilityDeployer:
    """Deploys the observability stack via kube-prometheus-stack Helm chart."""

    def __init__(self, engine: DeploymentEngine):
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s

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
            # Ensure helm repo is added
            self._add_helm_repo()

            # Build Helm values
            values = self._build_helm_values(namespace)

            # Install/upgrade kube-prometheus-stack
            cmd = [
                "helm",
                "upgrade",
                "--install",
                HELM_RELEASE_NAME,
                HELM_CHART,
                "--namespace",
                namespace,
                "--create-namespace",
                "--wait",
                "--timeout",
                "5m",
            ]
            for key, val in values.items():
                cmd.extend(["--set", f"{key}={val}"])

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=360,
            )

            if result.returncode != 0:
                error = result.stderr.strip()[:300] if result.stderr else "Unknown error"
                recovery = (
                    f"\nRecovery:\n"
                    f"  helm status {HELM_RELEASE_NAME} -n {namespace}\n"
                    f"  helm uninstall {HELM_RELEASE_NAME} -n {namespace}\n"
                    f"  lakebench deploy  # retry"
                )
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.FAILED,
                    message=f"Helm install failed: {error}{recovery}",
                    elapsed_seconds=time.time() - start,
                )

            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SUCCESS,
                message="Observability stack deployed (kube-prometheus-stack)",
                elapsed_seconds=time.time() - start,
                details={
                    "helm_release": HELM_RELEASE_NAME,
                    "prometheus_url": f"http://{HELM_RELEASE_NAME}-prometheus.{namespace}.svc:9090",
                    "grafana_url": f"http://{HELM_RELEASE_NAME}-grafana.{namespace}.svc:80",
                    "retention": self.config.observability.retention,
                },
            )

        except subprocess.TimeoutExpired:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message="Helm install timed out (360s)",
                elapsed_seconds=time.time() - start,
            )
        except Exception as e:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Observability deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def destroy(self) -> DeploymentResult:
        """Remove the observability stack."""
        start = time.time()
        namespace = self.config.get_namespace()

        if self.engine.dry_run:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SUCCESS,
                message="Would destroy observability stack",
                elapsed_seconds=0,
            )

        try:
            result = subprocess.run(
                [
                    "helm",
                    "uninstall",
                    HELM_RELEASE_NAME,
                    "--namespace",
                    namespace,
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )

            if result.returncode != 0 and "not found" not in (result.stderr or ""):
                return DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.FAILED,
                    message=f"Helm uninstall failed: {result.stderr}",
                    elapsed_seconds=time.time() - start,
                )

            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.SUCCESS,
                message="Observability stack removed",
                elapsed_seconds=time.time() - start,
            )
        except Exception as e:
            return DeploymentResult(
                component="observability",
                status=DeploymentStatus.FAILED,
                message=f"Observability destroy failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _add_helm_repo(self) -> None:
        """Add the prometheus-community Helm repo if not present."""
        subprocess.run(
            [
                "helm",
                "repo",
                "add",
                "prometheus-community",
                "https://prometheus-community.github.io/helm-charts",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        subprocess.run(
            ["helm", "repo", "update"],
            capture_output=True,
            text=True,
            timeout=60,
        )

    def _build_helm_values(self, namespace: str) -> dict[str, str]:
        """Build Helm --set values for kube-prometheus-stack."""
        obs = self.config.observability
        return {
            "prometheus.prometheusSpec.retention": obs.retention,
            "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage": obs.storage,
            "grafana.enabled": str(obs.dashboards_enabled).lower(),
            "grafana.adminPassword": "lakebench",
            # Scrape lakebench namespace pods
            "prometheus.prometheusSpec.podMonitorNamespaceSelector.matchNames[0]": namespace,
            "prometheus.prometheusSpec.serviceMonitorNamespaceSelector.matchNames[0]": namespace,
        }
