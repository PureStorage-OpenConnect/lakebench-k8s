"""Kubernetes client for Lakebench."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException


class K8sError(Exception):
    """Base exception for Kubernetes errors."""

    pass


class K8sConnectionError(K8sError):
    """Raised when Kubernetes cluster is unreachable."""

    pass


class K8sResourceError(K8sError):
    """Raised when resource operations fail."""

    pass


@dataclass
class K8sContext:
    """Kubernetes context information."""

    name: str
    cluster: str
    user: str
    namespace: str | None


@dataclass
class ResourceStatus:
    """Status of a Kubernetes resource."""

    kind: str
    name: str
    namespace: str | None
    exists: bool
    ready: bool
    message: str = ""


@dataclass
class ClusterCapacity:
    """Aggregate allocatable resources across worker nodes."""

    total_cpu_millicores: int
    total_memory_bytes: int
    node_count: int
    largest_node_cpu_millicores: int
    largest_node_memory_bytes: int


class K8sClient:
    """Kubernetes client for resource management.

    This client wraps the official kubernetes-client and provides
    high-level operations for Lakebench.
    """

    def __init__(self, context: str = "", namespace: str = ""):
        """Initialize Kubernetes client."""
        self.context_name = context
        self._namespace = namespace

        try:
            if context:
                config.load_kube_config(context=context)
            else:
                # Try in-cluster config first, fall back to kubeconfig
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()
        except Exception as e:
            raise K8sConnectionError(f"Failed to load Kubernetes config: {e}")  # noqa: B904

        self._core_v1 = client.CoreV1Api()
        self._apps_v1 = client.AppsV1Api()
        self._rbac_v1 = client.RbacAuthorizationV1Api()
        self._batch_v1 = client.BatchV1Api()
        self._custom = client.CustomObjectsApi()

    @property
    def namespace(self) -> str:
        """Get the default namespace."""
        if self._namespace:
            return self._namespace

        # Try to get from kubeconfig
        try:
            contexts, active = config.list_kube_config_contexts()
            if active and "namespace" in active.get("context", {}):
                return active["context"]["namespace"]
        except Exception:
            pass

        return "default"

    def get_current_context(self) -> K8sContext | None:
        """Get information about the current context.

        Returns:
            K8sContext with context details, or None if unavailable
        """
        try:
            contexts, active = config.list_kube_config_contexts()
            if active:
                ctx = active.get("context", {})
                return K8sContext(
                    name=active.get("name", ""),
                    cluster=ctx.get("cluster", ""),
                    user=ctx.get("user", ""),
                    namespace=ctx.get("namespace"),
                )
        except Exception:
            pass
        return None

    def test_connectivity(self) -> tuple[bool, str]:
        """Test connectivity to the Kubernetes cluster.

        Returns:
            Tuple of (success, message)
        """
        try:
            # Try to get API versions - lightweight call
            version = client.VersionApi().get_code()
            return True, f"Connected to Kubernetes {version.git_version}"
        except ApiException as e:
            return False, f"API error: {e.reason}"
        except Exception as e:
            return False, f"Connection error: {e}"

    def namespace_exists(self, name: str) -> bool:
        """Check if a namespace exists."""
        try:
            self._core_v1.read_namespace(name)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise K8sResourceError(f"Error checking namespace: {e}")  # noqa: B904

    def get_namespace_phase(self, name: str) -> str:
        """Get the phase of a namespace (Active, Terminating, etc)."""
        try:
            ns = self._core_v1.read_namespace(name)
            return ns.status.phase or ""
        except ApiException as e:
            if e.status == 404:
                return ""
            raise K8sResourceError(f"Error reading namespace phase: {e}")  # noqa: B904

    def wait_for_namespace_deleted(self, name: str, timeout: int = 120) -> None:
        """Wait for a namespace to be fully deleted."""
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.namespace_exists(name):
                return
            time.sleep(2)
        raise K8sResourceError(
            f"Namespace '{name}' still exists after {timeout}s (may still be terminating)"
        )

    def can_create_namespace(self, name: str) -> tuple[bool, str]:
        """Check if we have permission to create a namespace."""
        if self.namespace_exists(name):
            return True, f"Namespace '{name}' already exists"

        try:
            auth_v1 = client.AuthorizationV1Api()
            review = client.V1SelfSubjectAccessReview(
                spec=client.V1SelfSubjectAccessReviewSpec(
                    resource_attributes=client.V1ResourceAttributes(
                        verb="create",
                        resource="namespaces",
                    )
                )
            )
            result = auth_v1.create_self_subject_access_review(review)
            if result.status.allowed:
                return True, "Permission to create namespace"
            else:
                reason = result.status.reason or "insufficient permissions"
                return False, f"Cannot create namespace: {reason}"
        except ApiException as e:
            return False, f"Error checking permissions: {e.reason}"

    def create_namespace(self, name: str) -> bool:
        """Create a namespace.

        Args:
            name: Namespace name

        Returns:
            True if created, False if already existed
        """
        if self.namespace_exists(name):
            return False

        try:
            ns = client.V1Namespace(
                metadata=client.V1ObjectMeta(
                    name=name,
                    labels={
                        "app.kubernetes.io/managed-by": "lakebench",
                    },
                )
            )
            self._core_v1.create_namespace(ns)
            return True
        except ApiException as e:
            if e.status == 409:  # Already exists
                return False
            raise K8sResourceError(f"Failed to create namespace: {e}")  # noqa: B904

    def delete_namespace(self, name: str) -> bool:
        """Delete a namespace.

        Args:
            name: Namespace name

        Returns:
            True if deleted, False if didn't exist
        """
        if not self.namespace_exists(name):
            return False

        try:
            self._core_v1.delete_namespace(name)
            return True
        except ApiException as e:
            raise K8sResourceError(f"Failed to delete namespace: {e}")  # noqa: B904

    def secret_exists(self, name: str, namespace: str | None = None) -> bool:
        """Check if a secret exists.

        Args:
            name: Secret name
            namespace: Namespace (default: client's namespace)

        Returns:
            True if secret exists
        """
        ns = namespace or self.namespace
        try:
            self._core_v1.read_namespaced_secret(name, ns)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise K8sResourceError(f"Error checking secret: {e}")  # noqa: B904

    def apply_manifest(self, manifest: dict[str, Any], namespace: str | None = None) -> bool:
        """Apply a Kubernetes manifest (create or update).

        Args:
            manifest: Kubernetes manifest as dict
            namespace: Override namespace

        Returns:
            True if applied successfully
        """
        kind = manifest.get("kind", "")
        metadata = manifest.get("metadata", {})
        name = metadata.get("name", "")
        ns = namespace or metadata.get("namespace") or self.namespace

        try:
            if kind == "Namespace":
                return self._apply_namespace(manifest)
            elif kind == "Secret":
                return self._apply_secret(manifest, ns)
            elif kind == "ConfigMap":
                return self._apply_configmap(manifest, ns)
            elif kind == "ServiceAccount":
                return self._apply_serviceaccount(manifest, ns)
            elif kind == "Role":
                return self._apply_role(manifest, ns)
            elif kind == "RoleBinding":
                return self._apply_rolebinding(manifest, ns)
            elif kind == "ClusterRole":
                return self._apply_clusterrole(manifest)
            elif kind == "ClusterRoleBinding":
                return self._apply_clusterrolebinding(manifest)
            elif kind == "Service":
                return self._apply_service(manifest, ns)
            elif kind == "Deployment":
                return self._apply_deployment(manifest, ns)
            elif kind == "StatefulSet":
                return self._apply_statefulset(manifest, ns)
            elif kind == "Job":
                return self._apply_job(manifest, ns)
            else:
                # Try to apply as a CRD via CustomObjectsApi
                api_version = manifest.get("apiVersion", "")
                if "/" in api_version:
                    return self._apply_custom_resource(manifest, ns)
                raise K8sResourceError(f"Unsupported resource kind: {kind}")
        except ApiException as e:
            raise K8sResourceError(f"Failed to apply {kind}/{name}: {e}")  # noqa: B904

    def _apply_namespace(self, manifest: dict[str, Any]) -> bool:
        """Apply a Namespace manifest."""
        name = manifest["metadata"]["name"]
        if self.namespace_exists(name):
            # Update labels
            self._core_v1.patch_namespace(name, manifest)
            return True
        # Create namespace using proper V1Namespace object
        ns = client.V1Namespace(
            metadata=client.V1ObjectMeta(
                name=name,
                labels=manifest.get("metadata", {}).get("labels", {}),
            )
        )
        self._core_v1.create_namespace(ns)
        return True

    def _apply_secret(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a Secret manifest."""
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            self._core_v1.read_namespaced_secret(name, namespace)
            self._core_v1.replace_namespaced_secret(name, namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._core_v1.create_namespaced_secret(namespace, manifest)
            else:
                raise
        return True

    def _apply_configmap(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a ConfigMap manifest."""
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            self._core_v1.read_namespaced_config_map(name, namespace)
            self._core_v1.replace_namespaced_config_map(name, namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._core_v1.create_namespaced_config_map(namespace, manifest)
            else:
                raise
        return True

    def _apply_serviceaccount(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a ServiceAccount manifest."""
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            self._core_v1.read_namespaced_service_account(name, namespace)
            self._core_v1.replace_namespaced_service_account(name, namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._core_v1.create_namespaced_service_account(namespace, manifest)
            else:
                raise
        return True

    def _apply_role(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a Role manifest."""
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            self._rbac_v1.read_namespaced_role(name, namespace)
            self._rbac_v1.replace_namespaced_role(name, namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._rbac_v1.create_namespaced_role(namespace, manifest)
            else:
                raise
        return True

    def _apply_rolebinding(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a RoleBinding manifest."""
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            self._rbac_v1.read_namespaced_role_binding(name, namespace)
            self._rbac_v1.replace_namespaced_role_binding(name, namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._rbac_v1.create_namespaced_role_binding(namespace, manifest)
            else:
                raise
        return True

    def _apply_clusterrole(self, manifest: dict[str, Any]) -> bool:
        """Apply a ClusterRole manifest (cluster-scoped)."""
        name = manifest["metadata"]["name"]
        try:
            self._rbac_v1.read_cluster_role(name)
            self._rbac_v1.replace_cluster_role(name, manifest)
        except ApiException as e:
            if e.status == 404:
                self._rbac_v1.create_cluster_role(manifest)
            else:
                raise
        return True

    def _apply_clusterrolebinding(self, manifest: dict[str, Any]) -> bool:
        """Apply a ClusterRoleBinding manifest (cluster-scoped)."""
        name = manifest["metadata"]["name"]
        try:
            self._rbac_v1.read_cluster_role_binding(name)
            self._rbac_v1.replace_cluster_role_binding(name, manifest)
        except ApiException as e:
            if e.status == 404:
                self._rbac_v1.create_cluster_role_binding(manifest)
            else:
                raise
        return True

    def _apply_service(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a Service manifest."""
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            existing = self._core_v1.read_namespaced_service(name, namespace)
            # Preserve clusterIP for updates
            if "spec" in manifest and "clusterIP" not in manifest["spec"]:
                manifest["spec"]["clusterIP"] = existing.spec.cluster_ip
            self._core_v1.replace_namespaced_service(name, namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._core_v1.create_namespaced_service(namespace, manifest)
            else:
                raise
        return True

    def _apply_deployment(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a Deployment manifest."""
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            self._apps_v1.read_namespaced_deployment(name, namespace)
            self._apps_v1.replace_namespaced_deployment(name, namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._apps_v1.create_namespaced_deployment(namespace, manifest)
            else:
                raise
        return True

    def _apply_statefulset(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a StatefulSet manifest."""
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            self._apps_v1.read_namespaced_stateful_set(name, namespace)
            self._apps_v1.replace_namespaced_stateful_set(name, namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._apps_v1.create_namespaced_stateful_set(namespace, manifest)
            else:
                raise
        return True

    def _apply_job(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a Job manifest.

        Note: Jobs are immutable, so we delete and recreate if already exists.
        """
        name = manifest["metadata"]["name"]
        manifest["metadata"]["namespace"] = namespace
        try:
            self._batch_v1.read_namespaced_job(name, namespace)
            # Job exists - delete it first (jobs are immutable)
            self._batch_v1.delete_namespaced_job(
                name,
                namespace,
                body=client.V1DeleteOptions(propagation_policy="Background"),
            )
            # Wait briefly for deletion
            import time

            time.sleep(2)
            self._batch_v1.create_namespaced_job(namespace, manifest)
        except ApiException as e:
            if e.status == 404:
                self._batch_v1.create_namespaced_job(namespace, manifest)
            else:
                raise
        return True

    def _apply_custom_resource(self, manifest: dict[str, Any], namespace: str) -> bool:
        """Apply a Custom Resource via CustomObjectsApi.

        Supports any CRD including Stackable operators (HiveCluster, SecretClass, etc.)
        Handles both namespaced and cluster-scoped resources.
        """
        api_version = manifest.get("apiVersion", "")
        kind = manifest.get("kind", "")
        name = manifest.get("metadata", {}).get("name", "")

        # Parse group and version from apiVersion (e.g., "hive.stackable.tech/v1alpha1")
        if "/" not in api_version:
            raise K8sResourceError(f"Invalid apiVersion for CRD: {api_version}")

        group, version = api_version.split("/", 1)

        # Look up the actual plural form from the API
        plural = self._get_crd_plural(group, version, kind)

        # Check if resource is cluster-scoped
        is_cluster_scoped = self._is_cluster_scoped_crd(group, version, kind)

        custom_api = client.CustomObjectsApi()

        if is_cluster_scoped:
            # Cluster-scoped resource (like SecretClass)
            # Remove namespace from metadata if present
            if "namespace" in manifest.get("metadata", {}):
                del manifest["metadata"]["namespace"]

            try:
                custom_api.get_cluster_custom_object(
                    group=group,
                    version=version,
                    plural=plural,
                    name=name,
                )
                custom_api.patch_cluster_custom_object(
                    group=group,
                    version=version,
                    plural=plural,
                    name=name,
                    body=manifest,
                )
            except ApiException as e:
                if e.status == 404:
                    custom_api.create_cluster_custom_object(
                        group=group,
                        version=version,
                        plural=plural,
                        body=manifest,
                    )
                else:
                    raise
        else:
            # Namespaced resource (like HiveCluster)
            manifest["metadata"]["namespace"] = namespace

            try:
                custom_api.get_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    name=name,
                )
                custom_api.patch_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    name=name,
                    body=manifest,
                )
            except ApiException as e:
                if e.status == 404:
                    custom_api.create_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        body=manifest,
                    )
                else:
                    raise

        return True

    def delete_custom_resource(
        self,
        group: str,
        version: str,
        plural: str,
        name: str,
        namespace: str | None = None,
    ) -> bool:
        """Delete a namespaced Custom Resource.

        Args:
            group: API group (e.g., "sparkoperator.k8s.io")
            version: API version (e.g., "v1beta2")
            plural: Resource plural name (e.g., "sparkapplications")
            name: Resource name
            namespace: Namespace (default: client's namespace)

        Returns:
            True if deleted, False if not found
        """
        ns = namespace or self.namespace
        custom_api = client.CustomObjectsApi()

        try:
            custom_api.delete_namespaced_custom_object(
                group=group,
                version=version,
                namespace=ns,
                plural=plural,
                name=name,
            )
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise K8sResourceError(  # noqa: B904
                f"Failed to delete {group}/{version}/{plural}/{name}: {e}"
            )

    def _get_crd_plural(self, group: str, version: str, kind: str) -> str:
        """Get the plural name of a CRD.

        Args:
            group: API group
            version: API version
            kind: Resource kind

        Returns:
            Plural name for the resource
        """
        try:
            api_ext = client.ApiextensionsV1Api()
            crds = api_ext.list_custom_resource_definition()
            for crd in crds.items:
                if crd.spec.group == group and crd.spec.names.kind == kind:
                    return crd.spec.names.plural
        except Exception:
            pass

        # Fallback: simple pluralization
        kind_lower = kind.lower()
        if kind_lower.endswith("s"):
            return kind_lower + "es"
        return kind_lower + "s"

    def _is_cluster_scoped_crd(self, group: str, version: str, kind: str) -> bool:
        """Check if a CRD is cluster-scoped.

        Args:
            group: API group
            version: API version
            kind: Resource kind

        Returns:
            True if cluster-scoped, False if namespaced
        """
        try:
            api_ext = client.ApiextensionsV1Api()
            crds = api_ext.list_custom_resource_definition()
            for crd in crds.items:
                if crd.spec.group == group and crd.spec.names.kind == kind:
                    return crd.spec.scope == "Cluster"
        except Exception:
            pass

        # Default to namespaced
        return False

    def get_cluster_capacity(self) -> ClusterCapacity | None:
        """Get aggregate allocatable resources across worker nodes.

        Filters out control-plane nodes and sums allocatable CPU and memory
        across all remaining nodes.

        Returns:
            ClusterCapacity with totals, or None if node list unavailable
        """
        try:
            nodes = self._core_v1.list_node()
        except Exception:
            return None

        total_cpu = 0
        total_mem = 0
        max_cpu = 0
        max_mem = 0
        worker_count = 0

        control_plane_labels = (
            "node-role.kubernetes.io/control-plane",
            "node-role.kubernetes.io/master",
        )

        for node in nodes.items:
            labels = node.metadata.labels or {}
            if any(lbl in labels for lbl in control_plane_labels):
                continue

            allocatable = node.status.allocatable or {}
            cpu_str = allocatable.get("cpu", "0")
            mem_str = allocatable.get("memory", "0")

            cpu_m = self._parse_cpu_to_millicores(cpu_str)
            mem_b = self._parse_memory_to_bytes(mem_str)

            total_cpu += cpu_m
            total_mem += mem_b
            max_cpu = max(max_cpu, cpu_m)
            max_mem = max(max_mem, mem_b)
            worker_count += 1

        if worker_count == 0:
            return None

        return ClusterCapacity(
            total_cpu_millicores=total_cpu,
            total_memory_bytes=total_mem,
            node_count=worker_count,
            largest_node_cpu_millicores=max_cpu,
            largest_node_memory_bytes=max_mem,
        )

    @staticmethod
    def _parse_cpu_to_millicores(cpu: str) -> int:
        """Parse K8s CPU string to millicores."""
        cpu = cpu.strip()
        if cpu.endswith("m"):
            return int(cpu[:-1])
        return int(float(cpu) * 1000)

    @staticmethod
    def _parse_memory_to_bytes(mem: str) -> int:
        """Parse K8s memory string to bytes."""
        mem = mem.strip()
        units = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4}
        for suffix, multiplier in units.items():
            if mem.endswith(suffix):
                return int(mem[: -len(suffix)]) * multiplier
        # Plain bytes or with 'k', 'M', 'G' (SI units)
        si_units = {"k": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4}
        for suffix, multiplier in si_units.items():
            if mem.endswith(suffix):
                return int(mem[: -len(suffix)]) * multiplier
        return int(mem)

    def get_pod_status(self, name: str, namespace: str | None = None) -> ResourceStatus:
        """Get status of a pod.

        Args:
            name: Pod name
            namespace: Namespace

        Returns:
            ResourceStatus with pod details
        """
        ns = namespace or self.namespace
        try:
            pod = self._core_v1.read_namespaced_pod(name, ns)
            phase = pod.status.phase
            ready = phase == "Running" and all(
                c.ready for c in (pod.status.container_statuses or [])
            )
            return ResourceStatus(
                kind="Pod",
                name=name,
                namespace=ns,
                exists=True,
                ready=ready,
                message=phase,
            )
        except ApiException as e:
            if e.status == 404:
                return ResourceStatus(
                    kind="Pod",
                    name=name,
                    namespace=ns,
                    exists=False,
                    ready=False,
                )
            raise K8sResourceError(f"Error getting pod status: {e}")  # noqa: B904

    def exec_in_pod(
        self,
        name: str,
        command: list[str],
        namespace: str | None = None,
        container: str | None = None,
    ) -> tuple[int, str, str]:
        """Execute a command in a pod.

        Args:
            name: Pod name
            command: Command to execute
            namespace: Namespace
            container: Container name (optional)

        Returns:
            Tuple of (exit_code, stdout, stderr)
        """
        ns = namespace or self.namespace

        # Use kubectl exec as it's more reliable for exec operations
        cmd = ["kubectl", "exec", name, "-n", ns]
        if container:
            cmd.extend(["-c", container])
        cmd.append("--")
        cmd.extend(command)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return 1, "", "Command timed out"
        except Exception as e:
            return 1, "", str(e)


def get_k8s_client(context: str = "", namespace: str = "") -> K8sClient:
    """Create a Kubernetes client.

    Args:
        context: Kubernetes context (empty = current)
        namespace: Default namespace (empty = from context)

    Returns:
        K8sClient instance
    """
    return K8sClient(context=context, namespace=namespace)
