"""Hive Metastore deployment for Lakebench."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import yaml

from lakebench.k8s import WaitResult, WaitStatus

from .engine import DeploymentResult, DeploymentStatus

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .engine import DeploymentEngine


class HiveDeployer:
    """Deploys Hive Metastore using Stackable operator.

    Stackable HiveCluster provides:
    - Built-in S3 filesystem support (hadoop-aws JARs included)
    - Automatic credential injection via SecretClass
    - PostgreSQL backend integration
    - Resource management
    """

    # Stackable Hive templates
    STACKABLE_TEMPLATES = [
        "hive/stackable-secretclass.yaml.j2",
        "hive/stackable-hivecluster.yaml.j2",
        "hive/service.yaml.j2",  # ClusterIP service for stable DNS
    ]

    # Legacy raw Hive templates (deprecated)
    LEGACY_TEMPLATES = [
        "hive/configmap.yaml.j2",
        "hive/deployment.yaml.j2",
        "hive/service.yaml.j2",
    ]

    def __init__(self, engine: DeploymentEngine):
        """Initialize Hive deployer.

        Args:
            engine: Parent deployment engine
        """
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    # Required CRDs for a working Stackable Hive deployment
    _REQUIRED_CRDS: dict[str, str] = {
        "hiveclusters.hive.stackable.tech": "hive-operator",
        "secretclasses.secrets.stackable.tech": "secret-operator",
    }

    def _check_stackable_crds(self) -> dict[str, bool]:
        """Check which required Stackable CRDs are present.

        Returns:
            Dict mapping CRD name to availability boolean.
        """
        result = dict.fromkeys(self._REQUIRED_CRDS, False)
        try:
            from kubernetes import client as k8s_client

            api_ext = k8s_client.ApiextensionsV1Api()
            crds = api_ext.list_custom_resource_definition()
            for crd in crds.items:
                if crd.metadata.name in result:
                    result[crd.metadata.name] = True
        except Exception as e:
            logger.warning("Could not list CRDs to check Stackable availability: %s", e)
        return result

    def _is_stackable_available(self) -> bool:
        """Check if all required Stackable CRDs are available.

        Returns:
            True if HiveCluster and SecretClass CRDs both exist
        """
        return all(self._check_stackable_crds().values())

    def deploy(self) -> DeploymentResult:
        """Deploy Hive Metastore.

        Uses Stackable operator if available (recommended),
        falls back to raw deployment otherwise.

        Returns:
            DeploymentResult with status
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.config.architecture.catalog.type.value != "hive":
            return DeploymentResult(
                component="hive",
                status=DeploymentStatus.SKIPPED,
                message=f"Skipping Hive (catalog type is {self.config.architecture.catalog.type.value})",
                elapsed_seconds=0,
            )

        if self.engine.dry_run:
            return DeploymentResult(
                component="hive",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy Hive Metastore",
                elapsed_seconds=0,
            )

        use_stackable = self._is_stackable_available()

        if use_stackable:
            return self._deploy_stackable(namespace, start)
        else:
            # Stackable is required -- report which CRDs are missing
            crd_status = self._check_stackable_crds()
            missing = [
                self._REQUIRED_CRDS[crd] for crd, available in crd_status.items() if not available
            ]
            # Only list operators that are actually missing (plus prerequisites)
            operators_needed = set(missing)
            if operators_needed:
                operators_needed.update(["commons-operator", "listener-operator"])
            install_order = [
                "commons-operator",
                "listener-operator",
                "secret-operator",
                "hive-operator",
            ]
            install_cmds = "\n  ".join(
                f"helm install {op} oci://oci.stackable.tech/sdp-charts/{op} "
                "--version 25.7.0 --namespace stackable --create-namespace"
                for op in install_order
                if op in operators_needed
            )
            return DeploymentResult(
                component="hive",
                status=DeploymentStatus.FAILED,
                message=(
                    f"Stackable platform not fully installed (missing: {', '.join(missing)}). "
                    f"Install all required operators:\n  {install_cmds}"
                ),
                elapsed_seconds=time.time() - start,
            )

    def _deploy_stackable(self, namespace: str, start: float) -> DeploymentResult:
        """Deploy Hive using Stackable operator.

        Args:
            namespace: Target namespace
            start: Start timestamp

        Returns:
            DeploymentResult
        """
        try:
            # Render and apply Stackable templates
            for template_name in self.STACKABLE_TEMPLATES:
                yaml_content = self.renderer.render(template_name, self.context)
                # Handle multi-document YAML
                for doc in yaml.safe_load_all(yaml_content):
                    if doc:
                        self.k8s.apply_manifest(doc, namespace=namespace)

            # Wait for HiveCluster to be ready
            result = self._wait_for_hivecluster(namespace, timeout_seconds=300)

            if result.status != WaitStatus.READY:
                return DeploymentResult(
                    component="hive",
                    status=DeploymentStatus.FAILED,
                    message=f"Stackable HiveCluster not ready: {result.message}",
                    elapsed_seconds=time.time() - start,
                )

            pod_name = self._get_hive_pod_name(namespace)

            return DeploymentResult(
                component="hive",
                status=DeploymentStatus.SUCCESS,
                message="Stackable HiveCluster deployed with S3 support",
                elapsed_seconds=time.time() - start,
                details={
                    "type": "stackable",
                    "pod": pod_name,
                    "service": "lakebench-hive-metastore",
                    "port": 9083,
                    "thrift_uri": f"thrift://lakebench-hive-metastore.{namespace}.svc.cluster.local:9083",
                },
            )

        except Exception as e:
            return DeploymentResult(
                component="hive",
                status=DeploymentStatus.FAILED,
                message=f"Stackable HiveCluster deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _wait_for_hivecluster(self, namespace: str, timeout_seconds: int = 300) -> WaitResult:
        """Wait for Stackable HiveCluster to be ready.

        Args:
            namespace: Target namespace
            timeout_seconds: Timeout

        Returns:
            WaitResult with status
        """
        try:
            from kubernetes import client as k8s_client
            from kubernetes.client.rest import ApiException

            custom_api = k8s_client.CustomObjectsApi()

            deadline = time.time() + timeout_seconds
            while time.time() < deadline:
                try:
                    hc = custom_api.get_namespaced_custom_object(
                        group="hive.stackable.tech",
                        version="v1alpha1",
                        namespace=namespace,
                        plural="hiveclusters",
                        name="lakebench-hive",
                    )

                    # Check status conditions
                    status = hc.get("status", {})
                    conditions = status.get("conditions", [])

                    # Look for Available=True condition
                    for cond in conditions:
                        if cond.get("type") == "Available" and cond.get("status") == "True":
                            elapsed = time.time() - (deadline - timeout_seconds)
                            return WaitResult(WaitStatus.READY, "HiveCluster available", elapsed, 0)

                    # Also check if pods are running
                    core_v1 = k8s_client.CoreV1Api()
                    pods = core_v1.list_namespaced_pod(
                        namespace,
                        label_selector="app.kubernetes.io/name=hive,app.kubernetes.io/instance=lakebench-hive",
                    )

                    running_pods = [p for p in pods.items if p.status.phase == "Running"]
                    if running_pods:
                        # Check if container is ready
                        for pod in running_pods:
                            if pod.status.container_statuses:
                                all_ready = all(c.ready for c in pod.status.container_statuses)
                                if all_ready:
                                    elapsed = time.time() - (deadline - timeout_seconds)
                                    return WaitResult(
                                        WaitStatus.READY,
                                        f"HiveCluster pod ready: {pod.metadata.name}",
                                        elapsed,
                                        0,
                                    )

                except ApiException as e:
                    if e.status == 404:
                        pass  # HiveCluster not yet created, keep waiting
                    else:
                        raise

                time.sleep(10)

            elapsed = time.time() - (deadline - timeout_seconds)
            return WaitResult(
                WaitStatus.TIMEOUT, f"HiveCluster not ready after {timeout_seconds}s", elapsed, 0
            )

        except Exception as e:
            return WaitResult(WaitStatus.FAILED, str(e), 0.0, 0)

    def _get_hive_pod_name(self, namespace: str) -> str:
        """Get Stackable Hive pod name.

        Args:
            namespace: Target namespace

        Returns:
            Pod name or "unknown"
        """
        try:
            from kubernetes import client as k8s_client

            core_v1 = k8s_client.CoreV1Api()
            pods = core_v1.list_namespaced_pod(
                namespace,
                label_selector="app.kubernetes.io/name=hive,app.kubernetes.io/instance=lakebench-hive",
            )
            if pods.items:
                return str(pods.items[0].metadata.name)
            return "unknown"
        except Exception:
            return "unknown"

    def get_thrift_uri(self) -> str:
        """Get Hive Metastore thrift URI.

        Returns:
            Thrift URI string
        """
        namespace = self.config.get_namespace()
        return f"thrift://lakebench-hive-metastore.{namespace}.svc.cluster.local:9083"
