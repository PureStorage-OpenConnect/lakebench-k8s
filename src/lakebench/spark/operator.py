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


class _DeploymentReadError(Exception):
    """Raised when the controller deployment spec cannot be read."""


@dataclass
class OperatorStatus:
    """Status of Spark Operator."""

    installed: bool
    version: str | None
    namespace: str | None
    ready: bool
    message: str
    watching_namespace: bool | None = None  # None = could not determine
    watched_namespaces: list[str] | None = None  # None = watches all


class SparkOperatorManager:
    """Manages Spark Operator installation and status."""

    # Helm chart settings
    HELM_REPO_NAME = "spark-operator"
    HELM_REPO_URL = "https://kubeflow.github.io/spark-operator"
    HELM_CHART_NAME = "spark-operator/spark-operator"
    HELM_RELEASE_NAME = "spark-operator"
    DEFAULT_NAMESPACE = "spark-operator"

    def __init__(
        self,
        namespace: str | None = None,
        version: str | None = None,
        job_namespace: str | None = None,
    ):
        """Initialize Spark Operator manager.

        Args:
            namespace: Namespace for operator (default: spark-operator)
            version: Helm chart version to install (default: latest)
            job_namespace: Namespace where SparkApplications will be created.
                Passed to the Helm chart as ``spark.jobNamespaces``.
                If not set, the chart default (``default``) is used.
        """
        self.namespace = namespace or self.DEFAULT_NAMESPACE
        self.target_version = version  # Version to install if not present
        self.job_namespace = job_namespace

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

            # Check namespace watching -- use the deployment spec args as
            # ground truth, NOT Helm values (which can be out of sync after
            # a failed or partial helm upgrade).
            watching_namespace = None
            watched_namespaces = None
            if is_ready and self.job_namespace:
                try:
                    watched = self._get_active_namespaces(operator_ns)
                except _DeploymentReadError:
                    # Could not read deployment spec, fall back to Helm values
                    watched = self._get_watched_namespaces()
                if watched is None:
                    # Watches all namespaces (empty or unset)
                    watching_namespace = True
                elif len(watched) == 0:
                    # Could not determine (helm error)
                    watching_namespace = None
                else:
                    watched_namespaces = watched
                    watching_namespace = self.job_namespace in watched

            if is_ready and watching_namespace is False:
                message = (
                    f"Spark Operator is ready but does NOT watch namespace "
                    f"'{self.job_namespace}'. Watched: {watched_namespaces}"
                )
            elif is_ready:
                message = "Spark Operator is ready"
            else:
                message = "Spark Operator not ready"

            return OperatorStatus(
                installed=True,
                version=version,
                namespace=operator_ns,
                ready=is_ready,
                message=message,
                watching_namespace=watching_namespace,
                watched_namespaces=watched_namespaces,
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

    def _get_active_namespaces(self, operator_ns: str | None = None) -> list[str] | None:
        """Get namespaces from the running controller deployment spec.

        Reads the ``--namespaces=...`` arg from the deployment's pod template.
        This is the ground truth -- what the controller will actually watch
        when its pods start.  Helm values can be out of sync after a failed
        or partial upgrade.

        Returns:
            List of namespace strings if ``--namespaces`` is set,
            None if the arg is absent or empty (watches all namespaces).
            Raises ``_DeploymentReadError`` on error so the caller can
            distinguish "watches all" from "could not read".
        """
        ns = operator_ns or self.namespace
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    "spark-operator-controller",
                    "-n",
                    ns,
                    "-o",
                    "jsonpath={.spec.template.spec.containers[0].args}",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0 or not result.stdout:
                raise _DeploymentReadError("kubectl failed or empty output")

            import json

            try:
                args = json.loads(result.stdout)
            except (json.JSONDecodeError, ValueError) as exc:
                raise _DeploymentReadError(f"bad JSON: {result.stdout!r}") from exc

            for arg in args:
                if isinstance(arg, str) and arg.startswith("--namespaces="):
                    ns_str = arg.split("=", 1)[1]
                    if not ns_str:
                        return None  # Empty -- watches all
                    return [n.strip() for n in ns_str.split(",") if n.strip()]

            return None  # No --namespaces arg -- watches all

        except _DeploymentReadError:
            raise
        except Exception as e:
            raise _DeploymentReadError(str(e)) from e

    def _get_watched_namespaces(self) -> list[str] | None:
        """Get the namespaces the Spark Operator is configured to watch.

        Uses ``--all`` to include chart defaults (the chart defaults
        ``spark.jobNamespaces`` to ``["default"]``, NOT "all namespaces").

        Returns:
            List of namespace strings if jobNamespaces is set,
            None if the operator watches all namespaces (empty list),
            or empty list ``[]`` if Helm values could not be retrieved.
        """
        try:
            import json

            result = subprocess.run(
                [
                    "helm",
                    "get",
                    "values",
                    self.HELM_RELEASE_NAME,
                    "-n",
                    self.namespace,
                    "--all",
                    "-o",
                    "json",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                logger.debug("Could not get Helm values: %s", result.stderr)
                return []

            values = json.loads(result.stdout)
            ns_value = values.get("spark", {}).get("jobNamespaces", None)

            if ns_value is None:
                return None  # Not set -- watches all
            if isinstance(ns_value, str):
                if ns_value == "":
                    return None  # Empty string -- watches all
                return [ns_value]
            if isinstance(ns_value, list):
                filtered = [ns for ns in ns_value if ns]
                return filtered if filtered else None
            return None  # Unknown type -- assume watches all

        except Exception as e:
            logger.debug("Error reading Helm values: %s", e)
            return []

    def _filter_existing_namespaces(self, namespaces: list[str]) -> list[str]:
        """Return only namespaces that exist on the cluster.

        Stale namespaces from previous test runs cause ``helm upgrade`` to
        fail when the operator tries to create resources in non-existent
        namespaces.
        """
        try:
            from kubernetes import client as k8s_client

            core_v1 = k8s_client.CoreV1Api()
            existing = {ns.metadata.name for ns in core_v1.list_namespace().items}
            live = [ns for ns in namespaces if ns in existing]
            removed = set(namespaces) - set(live)
            if removed:
                logger.info(
                    "Pruning stale namespaces from spark.jobNamespaces: %s",
                    removed,
                )
            return live
        except Exception as e:
            logger.debug("Could not list namespaces, keeping all: %s", e)
            return namespaces

    def _add_namespace_to_watch(self, namespace: str) -> bool:
        """Add a namespace to the Spark Operator's watched namespaces.

        Uses ``helm upgrade --reuse-values`` to preserve existing config.

        Args:
            namespace: The namespace to add.

        Returns:
            True if helm upgrade succeeded.
        """
        watched = self._get_watched_namespaces()
        if watched is None:
            # Already watches all namespaces
            return True
        if namespace in watched:
            return True

        # Filter out stale namespaces that no longer exist on the cluster
        live_namespaces = self._filter_existing_namespaces(watched)
        new_list = live_namespaces + [namespace]
        ns_set = ",".join(new_list)

        try:
            result = subprocess.run(
                [
                    "helm",
                    "upgrade",
                    self.HELM_RELEASE_NAME,
                    self.HELM_CHART_NAME,
                    "-n",
                    self.namespace,
                    "--reuse-values",
                    "--set",
                    f"spark.jobNamespaces={{{ns_set}}}",
                ],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            logger.error("helm not found on PATH -- cannot add namespace")
            return False

        if result.returncode != 0:
            logger.error(
                "helm upgrade failed to add namespace '%s': %s",
                namespace,
                result.stderr,
            )
            return False

        logger.info(
            "Added namespace '%s' to spark.jobNamespaces (now: %s)",
            namespace,
            new_list,
        )

        # On OpenShift, ``helm upgrade`` regenerates deployment manifests
        # from the chart template, which re-introduces the hardcoded fsGroup
        # and seccompProfile that were patched out during install.  Re-apply
        # the patches before restarting.
        if self._is_openshift():
            self._assign_openshift_scc()
            self._patch_openshift_deployments()

        # The Spark Operator reads jobNamespaces at startup and does not
        # watch for config changes.  Restart so it picks up the new list.
        if not self._restart_operator():
            logger.error("Operator restart failed after helm upgrade")
            return False

        # Verify the operator actually picked up the new namespace by
        # checking the running pod's command-line args.
        if not self._verify_namespace_watched(namespace, timeout=30):
            logger.error(
                "Operator restarted but is not watching namespace '%s'",
                namespace,
            )
            return False

        return True

    def _restart_operator(self) -> bool:
        """Restart the Spark Operator to pick up config changes.

        The operator reads ``spark.jobNamespaces`` at startup only, so a
        ``helm upgrade`` alone is not enough -- both the controller and
        webhook pods must be recycled.

        Returns:
            True if both deployments restarted and rolled out successfully.
        """
        deployments = ["spark-operator-controller", "spark-operator-webhook"]

        for deploy in deployments:
            result = subprocess.run(
                [
                    "kubectl",
                    "rollout",
                    "restart",
                    f"deployment/{deploy}",
                    "-n",
                    self.namespace,
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning("Failed to restart %s: %s", deploy, result.stderr)
                return False

        logger.info("Restarting Spark Operator deployments to apply namespace changes")

        for deploy in deployments:
            result = subprocess.run(
                [
                    "kubectl",
                    "rollout",
                    "status",
                    f"deployment/{deploy}",
                    "-n",
                    self.namespace,
                    "--timeout=120s",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning("Rollout of %s did not complete: %s", deploy, result.stderr)
                return False

        return True

    def _verify_namespace_watched(self, namespace: str, timeout: int = 30) -> bool:
        """Verify the running operator controller is watching a namespace.

        Checks the controller pod's command-line args for the target
        namespace in ``--namespaces=...``.

        Args:
            namespace: The namespace that should appear in the arg list.
            timeout: Seconds to wait for verification.

        Returns:
            True if the controller is confirmed watching the namespace.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    self.namespace,
                    "-l",
                    "app.kubernetes.io/component=controller",
                    "-o",
                    "jsonpath={.items[0].spec.containers[0].args}",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and result.stdout:
                # Args contain --namespaces=ns1,ns2,...
                for arg in result.stdout.split(","):
                    if namespace in arg:
                        logger.info(
                            "Verified operator controller is watching '%s'",
                            namespace,
                        )
                        return True
            time.sleep(3)

        logger.warning(
            "Could not verify operator is watching '%s' after %ds",
            namespace,
            timeout,
        )
        return False

    @staticmethod
    def _is_openshift() -> bool:
        """Detect whether we are running on an OpenShift cluster."""
        result = subprocess.run(
            ["kubectl", "api-resources", "--api-group=security.openshift.io"],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0 and "security.openshift.io" in result.stdout

    def _assign_openshift_scc(self) -> None:
        """Assign anyuid SCC to Spark Operator service accounts on OpenShift.

        The operator controller and webhook pods require fsGroup 185 which
        violates the default restricted-v2 SCC.  Granting anyuid allows the
        pods to start.
        """
        for sa in ("spark-operator-controller", "spark-operator-webhook"):
            subprocess.run(
                [
                    "oc",
                    "adm",
                    "policy",
                    "add-scc-to-user",
                    "anyuid",
                    "-z",
                    sa,
                    "-n",
                    self.namespace,
                ],
                capture_output=True,
                text=True,
            )
        logger.info("Assigned anyuid SCC to Spark Operator service accounts")

    def _patch_openshift_deployments(self) -> None:
        """Patch Spark Operator deployments for OpenShift compatibility.

        The Helm chart hardcodes fsGroup=185 and seccompProfile=RuntimeDefault
        in the pod/container security contexts.  These cannot be overridden via
        Helm values (deep merge behavior).  On OpenShift the restricted-v2 SCC
        rejects both, so we patch them out after install.
        """
        patch = [
            {"op": "remove", "path": "/spec/template/spec/securityContext/fsGroup"},
            {
                "op": "remove",
                "path": "/spec/template/spec/containers/0/securityContext/seccompProfile",
            },
        ]
        import json

        patch_json = json.dumps(patch)

        for deploy in ("spark-operator-controller", "spark-operator-webhook"):
            result = subprocess.run(
                [
                    "kubectl",
                    "patch",
                    "deployment",
                    deploy,
                    "-n",
                    self.namespace,
                    "--type=json",
                    f"-p={patch_json}",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning("Failed to patch %s for OpenShift: %s", deploy, result.stderr)
            else:
                logger.info("Patched %s for OpenShift compatibility", deploy)

    def install(
        self,
        version: str | None = None,
        values: dict[str, Any] | None = None,
    ) -> bool:
        """Install Spark Operator via Helm.

        On OpenShift, automatically:
        - Uses webhook port 9443 (non-root can't bind 443)
        - Assigns the anyuid SCC to operator service accounts
        - Patches deployments to remove fsGroup and seccompProfile
          (the Helm chart hardcodes these and they can't be overridden
          via values due to deep merge behavior)

        Args:
            version: Specific chart version (default: latest)
            values: Custom Helm values

        Returns:
            True if installation succeeded
        """
        logger.info(f"Installing Spark Operator to namespace {self.namespace}")

        is_openshift = self._is_openshift()
        if is_openshift:
            logger.info("OpenShift detected -- will assign anyuid SCC after install")

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

            # On OpenShift, use a non-privileged port for the webhook
            # (non-root can't bind to port 443).
            webhook_port = "9443" if is_openshift else "443"

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
                f"webhook.port={webhook_port}",
            ]

            # Tell the operator which namespace(s) to watch for SparkApplications
            if self.job_namespace:
                cmd.extend(["--set", f"spark.jobNamespaces={{{self.job_namespace}}}"])

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

            # On OpenShift, assign SCCs and patch security contexts so
            # operator pods can start under the restricted-v2 SCC.
            if is_openshift:
                self._assign_openshift_scc()
                self._patch_openshift_deployments()

            # Wait for operator to be ready
            return self._wait_for_ready(timeout=120)

        except (subprocess.CalledProcessError, FileNotFoundError) as e:
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
        If installed but not watching the target namespace, adds it.

        Returns:
            OperatorStatus after ensuring installation
        """
        status = self.check_status()

        if status.ready:
            # Operator is running -- ensure it watches our namespace
            if self.job_namespace and status.watching_namespace is False:
                logger.info(
                    "Spark Operator not watching '%s' -- adding via helm upgrade",
                    self.job_namespace,
                )
                if not self._add_namespace_to_watch(self.job_namespace):
                    return OperatorStatus(
                        installed=True,
                        version=status.version,
                        namespace=status.namespace,
                        ready=False,
                        message=(
                            f"Failed to add namespace '{self.job_namespace}' "
                            f"to spark.jobNamespaces via helm upgrade"
                        ),
                    )
                status = self.check_status()
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

    def ensure_namespace_watched(self, *, can_heal: bool = False) -> OperatorStatus:
        """Ensure the Spark Operator watches the target namespace.

        When ``can_heal`` is True and the operator is not watching the target
        namespace, attempts to add it via ``helm upgrade --reuse-values``.
        When False, returns status with the exact fix command for the user.

        Args:
            can_heal: If True, attempt to fix via helm upgrade.
                Should be True when ``install=True`` in config.

        Returns:
            OperatorStatus reflecting the namespace watching state.
        """
        if not self.job_namespace:
            return self.check_status()

        status = self.check_status()

        if not status.ready:
            return status

        # If watching or unknown, accept it
        if status.watching_namespace is not False:
            return status

        # Operator is NOT watching the target namespace
        if can_heal:
            logger.info(
                "Adding namespace '%s' to spark.jobNamespaces",
                self.job_namespace,
            )
            if self._add_namespace_to_watch(self.job_namespace):
                return self.check_status()
            # Heal failed -- fall through to provide fix command

        existing = status.watched_namespaces or []
        new_list = ",".join(existing + [self.job_namespace])
        fix_cmd = (
            f"helm upgrade {self.HELM_RELEASE_NAME} {self.HELM_CHART_NAME} "
            f"-n {self.namespace} --reuse-values "
            f"--set 'spark.jobNamespaces={{{new_list}}}'"
        )
        status.message = (
            f"Spark Operator does not watch namespace '{self.job_namespace}'. "
            f"Currently watching: {existing}. "
            f"SparkApplications will not be reconciled.\n"
            f"Fix with:\n  {fix_cmd}"
        )
        return status
