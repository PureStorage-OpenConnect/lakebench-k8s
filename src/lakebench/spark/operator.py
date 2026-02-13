"""Spark Operator management for Lakebench.

Handles detection and installation of the Kubeflow Spark Operator.
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class OperatorStatus:
    """Status of Spark Operator."""

    installed: bool
    version: str | None
    namespace: str | None
    ready: bool
    message: str


class SparkOperatorManager:
    """Manages Spark Operator installation and status."""

    # Helm chart settings
    HELM_REPO_NAME = "spark-operator"
    HELM_REPO_URL = "https://kubeflow.github.io/spark-operator"
    HELM_CHART_NAME = "spark-operator/spark-operator"
    HELM_RELEASE_NAME = "spark-operator"
    DEFAULT_NAMESPACE = "spark-operator"

    def __init__(self, namespace: str | None = None, version: str | None = None):
        """Initialize Spark Operator manager.

        Args:
            namespace: Namespace for operator (default: spark-operator)
            version: Helm chart version to install (default: latest)
        """
        self.namespace = namespace or self.DEFAULT_NAMESPACE
        self.target_version = version  # Version to install if not present

    def check_status(self) -> OperatorStatus:
        """Check if Spark Operator is installed and ready.

        Returns:
            OperatorStatus with current state
        """
        try:
            # Check if CRD exists
            result = subprocess.run(
                ["kubectl", "get", "crd", "sparkapplications.sparkoperator.k8s.io"],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                return OperatorStatus(
                    installed=False,
                    version=None,
                    namespace=None,
                    ready=False,
                    message="SparkApplication CRD not found",
                )

            # Check if operator deployment exists
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    "-A",
                    "-l",
                    "app.kubernetes.io/name=spark-operator",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0 or "No resources" in result.stdout:
                return OperatorStatus(
                    installed=True,
                    version=None,
                    namespace=None,
                    ready=False,
                    message="CRD exists but operator deployment not found",
                )

            # Parse operator namespace
            lines = result.stdout.strip().split("\n")
            if len(lines) < 2:
                return OperatorStatus(
                    installed=True,
                    version=None,
                    namespace=None,
                    ready=False,
                    message="Could not parse operator deployment",
                )

            # First column is namespace
            parts = lines[1].split()
            operator_ns = parts[0] if parts else self.namespace

            # Check if operator is ready
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    "-n",
                    operator_ns,
                    "-l",
                    "app.kubernetes.io/name=spark-operator",
                    "-o",
                    "jsonpath={.items[0].status.readyReplicas}",
                ],
                capture_output=True,
                text=True,
            )

            ready_replicas = int(result.stdout.strip() or "0")
            is_ready = ready_replicas > 0

            # Get version from Helm release if possible
            version = self._get_helm_version()

            return OperatorStatus(
                installed=True,
                version=version,
                namespace=operator_ns,
                ready=is_ready,
                message="Spark Operator is ready" if is_ready else "Spark Operator not ready",
            )

        except Exception as e:
            return OperatorStatus(
                installed=False,
                version=None,
                namespace=None,
                ready=False,
                message=f"Error checking operator status: {e}",
            )

    def _get_helm_version(self) -> str | None:
        """Get Spark Operator version from Helm release.

        Returns:
            Version string or None if not found
        """
        try:
            result = subprocess.run(
                [
                    "helm",
                    "list",
                    "-A",
                    "-f",
                    "spark-operator",
                    "-o",
                    "json",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                import json

                releases = json.loads(result.stdout)
                if releases:
                    return releases[0].get("chart", "").replace("spark-operator-", "")
            return None
        except Exception:
            return None

    def install(
        self,
        version: str | None = None,
        values: dict[str, Any] | None = None,
    ) -> bool:
        """Install Spark Operator via Helm.

        Args:
            version: Specific chart version (default: latest)
            values: Custom Helm values

        Returns:
            True if installation succeeded
        """
        logger.info(f"Installing Spark Operator to namespace {self.namespace}")

        try:
            # Add Helm repo
            subprocess.run(
                ["helm", "repo", "add", self.HELM_REPO_NAME, self.HELM_REPO_URL],
                capture_output=True,
                check=True,
            )

            subprocess.run(
                ["helm", "repo", "update"],
                capture_output=True,
                check=True,
            )

            # Build Helm install command
            cmd = [
                "helm",
                "upgrade",
                "--install",
                self.HELM_RELEASE_NAME,
                self.HELM_CHART_NAME,
                "--namespace",
                self.namespace,
                "--create-namespace",
                "--set",
                "webhook.enable=true",
                "--set",
                "webhook.port=443",
            ]

            if version:
                cmd.extend(["--version", version])

            # Add custom values
            if values:
                for key, value in values.items():
                    cmd.extend(["--set", f"{key}={value}"])

            # Run install
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                logger.error(f"Helm install failed: {result.stderr}")
                return False

            # Wait for operator to be ready
            return self._wait_for_ready(timeout=120)

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install Spark Operator: {e}")
            return False

    def _wait_for_ready(self, timeout: int = 120) -> bool:
        """Wait for Spark Operator to become ready.

        Args:
            timeout: Maximum wait time in seconds

        Returns:
            True if operator becomes ready
        """
        start = time.time()

        while time.time() - start < timeout:
            status = self.check_status()
            if status.ready:
                logger.info("Spark Operator is ready")
                return True
            time.sleep(5)

        logger.error(f"Spark Operator not ready after {timeout}s")
        return False

    def ensure_installed(self) -> OperatorStatus:
        """Ensure Spark Operator is installed and ready.

        If not installed, installs it automatically.

        Returns:
            OperatorStatus after ensuring installation
        """
        status = self.check_status()

        if status.ready:
            return status

        if not status.installed:
            logger.info("Spark Operator not found, installing...")
            if self.install(version=self.target_version):
                return self.check_status()
            else:
                return OperatorStatus(
                    installed=False,
                    version=None,
                    namespace=None,
                    ready=False,
                    message="Failed to install Spark Operator",
                )

        # CRD exists but operator not ready, try reinstall
        logger.info("Spark Operator not ready, attempting reinstall...")
        if self.install():
            return self.check_status()

        return status
