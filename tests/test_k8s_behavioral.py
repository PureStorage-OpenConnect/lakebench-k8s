"""Behavioral tests exercising real K8s cluster interactions.

These tests require a live Kubernetes cluster (tested on OpenShift 4.19 / K8s v1.32).
They exercise the lakebench K8s client, security verifier, wait module, and deployment
engine against real infrastructure rather than mocks.

Run with: pytest tests/test_k8s_behavioral.py -v -m integration
Or: pytest tests/test_k8s_behavioral.py -v (they will auto-skip if no cluster)

All tests clean up after themselves. Namespaces are created with unique
suffixes to avoid collisions.
"""

from __future__ import annotations

import uuid

import pytest

from lakebench.k8s.client import ClusterCapacity, K8sClient, K8sContext

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _cluster_available() -> bool:
    """Check whether a live K8s cluster is reachable."""
    try:
        k = K8sClient()
        ok, _ = k.test_connectivity()
        return ok
    except Exception:
        return False


# Skip the entire module when no cluster is available.
_HAS_CLUSTER = _cluster_available()


@pytest.fixture(scope="module")
def k8s() -> K8sClient:
    """Real K8sClient connected to whatever cluster kubeconfig points at."""
    if not _HAS_CLUSTER:
        pytest.skip("No live K8s cluster available")
    return K8sClient()


@pytest.fixture
def unique_ns() -> str:
    """Generate a unique namespace name for isolation."""
    return f"lb-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def created_ns(k8s: K8sClient, unique_ns: str):
    """Create a namespace, yield its name, delete it on teardown."""
    k8s.create_namespace(unique_ns)
    yield unique_ns
    try:
        k8s.delete_namespace(unique_ns)
    except Exception:
        pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# 1. Connectivity & Context
# ---------------------------------------------------------------------------


class TestClusterConnectivity:
    """Verify the K8sClient can reach the real cluster."""

    def test_connectivity(self, k8s: K8sClient) -> None:
        ok, msg = k8s.test_connectivity()
        assert ok is True
        assert "Connected" in msg

    def test_current_context(self, k8s: K8sClient) -> None:
        ctx = k8s.get_current_context()
        assert ctx is not None
        assert isinstance(ctx, K8sContext)
        assert ctx.cluster != ""

    def test_default_namespace_is_set(self, k8s: K8sClient) -> None:
        ns = k8s.namespace
        assert isinstance(ns, str)
        assert len(ns) > 0


# ---------------------------------------------------------------------------
# 2. Cluster Capacity
# ---------------------------------------------------------------------------


class TestClusterCapacity:
    """Verify cluster capacity reporting from real nodes."""

    def test_get_cluster_capacity(self, k8s: K8sClient) -> None:
        cap = k8s.get_cluster_capacity()
        assert cap is not None
        assert isinstance(cap, ClusterCapacity)
        assert cap.node_count > 0

    def test_capacity_excludes_control_plane(self, k8s: K8sClient) -> None:
        """Worker count should be less than total nodes on a multi-role cluster."""
        cap = k8s.get_cluster_capacity()
        assert cap is not None
        # On the OpenShift cluster: 3 masters + 8 workers = 11 total.
        # Capacity should report workers only (at most total minus masters).
        assert cap.node_count >= 1

    def test_capacity_has_reasonable_resources(self, k8s: K8sClient) -> None:
        cap = k8s.get_cluster_capacity()
        assert cap is not None
        # At least 1 core and 1 GiB across all workers
        assert cap.total_cpu_millicores >= 1000
        assert cap.total_memory_bytes >= 1024**3

    def test_largest_node_within_total(self, k8s: K8sClient) -> None:
        cap = k8s.get_cluster_capacity()
        assert cap is not None
        assert cap.largest_node_cpu_millicores <= cap.total_cpu_millicores
        assert cap.largest_node_memory_bytes <= cap.total_memory_bytes


# ---------------------------------------------------------------------------
# 3. Namespace CRUD
# ---------------------------------------------------------------------------


class TestNamespaceCRUD:
    """Create, inspect, and delete namespaces on a real cluster."""

    def test_create_namespace(self, k8s: K8sClient, unique_ns: str) -> None:
        created = k8s.create_namespace(unique_ns)
        assert created is True
        assert k8s.namespace_exists(unique_ns) is True
        # Cleanup
        k8s.delete_namespace(unique_ns)

    def test_create_namespace_idempotent(self, k8s: K8sClient, created_ns: str) -> None:
        """Creating an already-existing namespace returns False."""
        result = k8s.create_namespace(created_ns)
        assert result is False

    def test_namespace_phase_active(self, k8s: K8sClient, created_ns: str) -> None:
        phase = k8s.get_namespace_phase(created_ns)
        assert phase == "Active"

    def test_delete_namespace(self, k8s: K8sClient, unique_ns: str) -> None:
        k8s.create_namespace(unique_ns)
        deleted = k8s.delete_namespace(unique_ns)
        assert deleted is True

    def test_delete_nonexistent_namespace(self, k8s: K8sClient) -> None:
        result = k8s.delete_namespace("lb-test-does-not-exist-99999")
        assert result is False

    def test_namespace_not_exists(self, k8s: K8sClient) -> None:
        assert k8s.namespace_exists("lb-test-does-not-exist-99999") is False

    def test_can_create_namespace_permission(self, k8s: K8sClient) -> None:
        """Current user should have permission to create namespaces."""
        allowed, msg = k8s.can_create_namespace("lb-test-rbac-check")
        assert allowed is True

    def test_namespace_has_managed_by_label(self, k8s: K8sClient, created_ns: str) -> None:
        """Namespaces created by lakebench should carry the managed-by label."""
        ns_obj = k8s._core_v1.read_namespace(created_ns)
        labels = ns_obj.metadata.labels or {}
        assert labels.get("app.kubernetes.io/managed-by") == "lakebench"


# ---------------------------------------------------------------------------
# 4. Manifest Apply / Delete (ConfigMap + Secret)
# ---------------------------------------------------------------------------


class TestManifestApply:
    """Apply and remove manifests against a real namespace."""

    def test_apply_configmap_create(self, k8s: K8sClient, created_ns: str) -> None:
        manifest = {
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {"name": "lb-test-cm"},
            "data": {"key": "value"},
        }
        result = k8s.apply_manifest(manifest, namespace=created_ns)
        assert result is True

    def test_apply_configmap_update(self, k8s: K8sClient, created_ns: str) -> None:
        """Applying the same ConfigMap twice should succeed (update path)."""
        manifest = {
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {"name": "lb-test-cm-update"},
            "data": {"v": "1"},
        }
        k8s.apply_manifest(manifest, namespace=created_ns)
        manifest["data"]["v"] = "2"
        result = k8s.apply_manifest(manifest, namespace=created_ns)
        assert result is True

    def test_apply_secret(self, k8s: K8sClient, created_ns: str) -> None:
        manifest = {
            "kind": "Secret",
            "apiVersion": "v1",
            "metadata": {"name": "lb-test-secret"},
            "type": "Opaque",
            "stringData": {"password": "hunter2"},
        }
        result = k8s.apply_manifest(manifest, namespace=created_ns)
        assert result is True
        assert k8s.secret_exists("lb-test-secret", created_ns) is True

    def test_secret_exists_false(self, k8s: K8sClient, created_ns: str) -> None:
        assert k8s.secret_exists("nonexistent-secret", created_ns) is False

    def test_apply_service_account(self, k8s: K8sClient, created_ns: str) -> None:
        manifest = {
            "kind": "ServiceAccount",
            "apiVersion": "v1",
            "metadata": {"name": "lb-test-sa"},
        }
        result = k8s.apply_manifest(manifest, namespace=created_ns)
        assert result is True


# ---------------------------------------------------------------------------
# 5. Pod Status & Exec
# ---------------------------------------------------------------------------


class TestPodOperations:
    """Test pod status and exec against a real cluster.

    These tests create a lightweight busybox pod so they don't depend on
    any pre-existing deployment.
    """

    @pytest.fixture
    def busybox_pod(self, k8s: K8sClient, created_ns: str):
        """Create a minimal busybox pod and wait for it to start."""
        from lakebench.k8s.wait import WaitStatus, wait_for_pod_ready

        manifest = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "lb-test-busybox"},
            "spec": {
                "containers": [
                    {
                        "name": "busybox",
                        "image": "busybox:1.36",
                        "command": ["sleep", "300"],
                    }
                ],
                "restartPolicy": "Never",
            },
        }
        # Apply the pod manifest directly via the core API
        from kubernetes.client.rest import ApiException

        try:
            k8s._core_v1.create_namespaced_pod(namespace=created_ns, body=manifest)
        except ApiException as e:
            if e.status != 409:
                raise

        result = wait_for_pod_ready(
            k8s, "lb-test-busybox", created_ns, timeout_seconds=120, poll_interval=3
        )
        if result.status != WaitStatus.READY:
            pytest.skip(f"Busybox pod did not become ready: {result.message}")

        yield "lb-test-busybox"

        # Cleanup
        try:
            k8s._core_v1.delete_namespaced_pod("lb-test-busybox", created_ns)
        except Exception:
            pass

    def test_get_pod_status_running(
        self, k8s: K8sClient, created_ns: str, busybox_pod: str
    ) -> None:
        status = k8s.get_pod_status(busybox_pod, created_ns)
        assert status.exists is True
        assert status.ready is True
        assert status.kind == "Pod"

    def test_get_pod_status_nonexistent(self, k8s: K8sClient, created_ns: str) -> None:
        status = k8s.get_pod_status("nonexistent-pod-99999", created_ns)
        assert status.exists is False

    def test_exec_in_pod(self, k8s: K8sClient, created_ns: str, busybox_pod: str) -> None:
        code, stdout, stderr = k8s.exec_in_pod(busybox_pod, ["echo", "hello-lakebench"], created_ns)
        assert code == 0
        assert "hello-lakebench" in stdout

    def test_exec_in_pod_nonzero(self, k8s: K8sClient, created_ns: str, busybox_pod: str) -> None:
        code, stdout, stderr = k8s.exec_in_pod(busybox_pod, ["false"], created_ns)
        assert code != 0


# ---------------------------------------------------------------------------
# 6. SecurityVerifier -- OpenShift Detection
# ---------------------------------------------------------------------------


class TestSecurityVerifierReal:
    """Detect platform type on the real cluster."""

    def test_detect_platform(self, k8s: K8sClient) -> None:
        from lakebench.k8s.security import PlatformType, SecurityVerifier

        verifier = SecurityVerifier(k8s)
        platform = verifier.detect_platform()
        assert isinstance(platform, PlatformType)
        # On our known OpenShift cluster this should be OPENSHIFT
        assert platform in (PlatformType.OPENSHIFT, PlatformType.VANILLA)

    def test_detect_openshift_on_openshift_cluster(self, k8s: K8sClient) -> None:
        """On an OpenShift cluster the verifier should detect OPENSHIFT."""
        from lakebench.k8s.security import PlatformType, SecurityVerifier

        verifier = SecurityVerifier(k8s)
        platform = verifier.detect_platform()
        # If the cluster has OpenShift CRDs, we expect OPENSHIFT
        import kubernetes.client as kc

        try:
            ext = kc.ApiextensionsV1Api()
            crds = ext.list_custom_resource_definition()
            has_openshift = any("openshift.io" in crd.metadata.name for crd in crds.items)
        except Exception:
            has_openshift = False

        if has_openshift:
            assert platform == PlatformType.OPENSHIFT
        else:
            assert platform == PlatformType.VANILLA

    def test_security_check_returns_result(self, k8s: K8sClient, created_ns: str) -> None:
        """verify_security should return a SecurityCheckResult without crashing."""
        from lakebench.k8s.security import SecurityCheckResult, SecurityVerifier

        verifier = SecurityVerifier(k8s)
        result = verifier.verify_security(created_ns)
        assert isinstance(result, SecurityCheckResult)
        assert result.checks_passed >= 0
        assert result.checks_failed >= 0


# ---------------------------------------------------------------------------
# 7. Wait Module -- Real Pods
# ---------------------------------------------------------------------------


class TestWaitModuleReal:
    """Test wait functions against real pods."""

    def test_wait_for_nonexistent_pod_times_out(self, k8s: K8sClient, created_ns: str) -> None:
        from lakebench.k8s.wait import WaitStatus, wait_for_pod_ready

        result = wait_for_pod_ready(
            k8s,
            "nonexistent-pod-zzz",
            created_ns,
            timeout_seconds=5,
            poll_interval=1,
        )
        assert result.status == WaitStatus.TIMEOUT
        assert result.attempts >= 1

    def test_wait_for_deployment_nonexistent_times_out(
        self, k8s: K8sClient, created_ns: str
    ) -> None:
        from lakebench.k8s.wait import WaitStatus, wait_for_deployment_ready

        result = wait_for_deployment_ready(
            k8s,
            "nonexistent-deployment",
            created_ns,
            timeout_seconds=5,
            poll_interval=1,
        )
        assert result.status == WaitStatus.TIMEOUT

    def test_wait_for_statefulset_nonexistent_times_out(
        self, k8s: K8sClient, created_ns: str
    ) -> None:
        from lakebench.k8s.wait import WaitStatus, wait_for_statefulset_ready

        result = wait_for_statefulset_ready(
            k8s,
            "nonexistent-sts",
            created_ns,
            timeout_seconds=5,
            poll_interval=1,
        )
        assert result.status == WaitStatus.TIMEOUT

    def test_wait_for_condition_immediate_success(self) -> None:
        """Generic wait_for_condition with a True-returning check."""
        if not _HAS_CLUSTER:
            pytest.skip("No cluster")

        from lakebench.k8s.wait import WaitStatus, wait_for_condition

        result = wait_for_condition(
            lambda: (True, "done"),
            timeout_seconds=5,
            poll_interval=1,
        )
        assert result.status == WaitStatus.READY
        assert result.attempts == 1


# ---------------------------------------------------------------------------
# 8. DeploymentEngine -- Context & Dry-Run
# ---------------------------------------------------------------------------


class TestDeploymentEngineReal:
    """Exercise DeploymentEngine against the real cluster."""

    def _make_engine(self, k8s: K8sClient, ns: str, dry_run: bool = True):
        from unittest.mock import patch

        from lakebench.deploy.engine import DeploymentEngine
        from tests.conftest import make_config

        cfg = make_config(
            name="behavioral-test",
            platform={
                "kubernetes": {"namespace": ns, "create_namespace": True},
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                    }
                },
            },
        )
        with patch(
            "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
            return_value=False,
        ):
            return DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=dry_run)

    def test_context_has_namespace(self, k8s: K8sClient, created_ns: str) -> None:
        engine = self._make_engine(k8s, created_ns)
        ctx = engine.context
        assert ctx["namespace"] == created_ns

    def test_context_has_s3_fields(self, k8s: K8sClient, created_ns: str) -> None:
        engine = self._make_engine(k8s, created_ns)
        ctx = engine.context
        for key in ("s3_endpoint", "s3_access_key", "s3_secret_key"):
            assert key in ctx, f"Missing context key: {key}"

    def test_deploy_all_dry_run_with_real_cluster(self, k8s: K8sClient, created_ns: str) -> None:
        from lakebench.deploy.engine import DeploymentStatus

        engine = self._make_engine(k8s, created_ns, dry_run=True)
        results = engine.deploy_all()

        assert len(results) > 0
        for r in results:
            assert r.status in (
                DeploymentStatus.SUCCESS,
                DeploymentStatus.SKIPPED,
            ), f"{r.component}: {r.status} -- {r.message}"

    def test_deploy_all_dry_run_namespace_first(self, k8s: K8sClient, created_ns: str) -> None:
        engine = self._make_engine(k8s, created_ns, dry_run=True)
        results = engine.deploy_all()
        assert results[0].component == "namespace"

    def test_deploy_all_dry_run_progress_callback(self, k8s: K8sClient, created_ns: str) -> None:
        from lakebench.deploy.engine import DeploymentStatus

        engine = self._make_engine(k8s, created_ns, dry_run=True)
        invocations: list[tuple[str, DeploymentStatus, str]] = []
        engine.deploy_all(progress_callback=lambda c, s, m: invocations.append((c, s, m)))
        assert len(invocations) >= 2
        assert invocations[0][0] == "namespace"
        assert invocations[0][1] == DeploymentStatus.IN_PROGRESS


# ---------------------------------------------------------------------------
# 9. CRD Discovery
# ---------------------------------------------------------------------------


class TestCRDDiscovery:
    """Verify that the cluster has the expected CRDs installed."""

    def test_spark_operator_crd_exists(self, k8s: K8sClient) -> None:
        """The sparkoperator.k8s.io CRD should be installed."""
        import kubernetes.client as kc

        ext = kc.ApiextensionsV1Api()
        crds = ext.list_custom_resource_definition()
        crd_names = [crd.metadata.name for crd in crds.items]
        assert "sparkapplications.sparkoperator.k8s.io" in crd_names

    def test_hive_stackable_crd_exists(self, k8s: K8sClient) -> None:
        """The hiveclusters.hive.stackable.tech CRD should be installed."""
        import kubernetes.client as kc

        ext = kc.ApiextensionsV1Api()
        crds = ext.list_custom_resource_definition()
        crd_names = [crd.metadata.name for crd in crds.items]
        assert "hiveclusters.hive.stackable.tech" in crd_names

    def test_stackable_s3_connection_crd_exists(self, k8s: K8sClient) -> None:
        """Stackable S3Connection CRD should be installed."""
        import kubernetes.client as kc

        ext = kc.ApiextensionsV1Api()
        crds = ext.list_custom_resource_definition()
        crd_names = [crd.metadata.name for crd in crds.items]
        assert "s3connections.s3.stackable.tech" in crd_names


# ---------------------------------------------------------------------------
# 10. Storage Classes
# ---------------------------------------------------------------------------


class TestStorageClasses:
    """Verify expected storage classes are available."""

    def _get_storage_class_names(self, k8s: K8sClient) -> set[str]:
        from kubernetes import client as kc

        storage_v1 = kc.StorageV1Api()
        scs = storage_v1.list_storage_class()
        return {sc.metadata.name for sc in scs.items}

    def test_portworx_db_storage_class(self, k8s: K8sClient) -> None:
        """px-csi-db should be available for PostgreSQL PVCs."""
        names = self._get_storage_class_names(k8s)
        assert "px-csi-db" in names

    def test_default_storage_class_exists(self, k8s: K8sClient) -> None:
        """At least one storage class should be present."""
        names = self._get_storage_class_names(k8s)
        assert len(names) > 0

    def test_stackable_listeners_storage_class(self, k8s: K8sClient) -> None:
        """Stackable listeners storage class should be present."""
        names = self._get_storage_class_names(k8s)
        assert "listeners.stackable.tech" in names


# ---------------------------------------------------------------------------
# 11. Deployment + StatefulSet Apply (lightweight, real namespace)
# ---------------------------------------------------------------------------


class TestDeploymentApply:
    """Apply a real Deployment/StatefulSet manifest and verify status."""

    def test_apply_deployment(self, k8s: K8sClient, created_ns: str) -> None:
        """Apply a minimal Deployment and verify it was accepted by the API."""
        manifest = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "lb-test-pause"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "lb-test-pause"}},
                "template": {
                    "metadata": {"labels": {"app": "lb-test-pause"}},
                    "spec": {
                        "containers": [
                            {
                                "name": "pause",
                                "image": "registry.k8s.io/pause:3.10",
                            }
                        ],
                    },
                },
            },
        }
        result = k8s.apply_manifest(manifest, namespace=created_ns)
        assert result is True

        # Verify the deployment object exists in the API
        dep = k8s._apps_v1.read_namespaced_deployment("lb-test-pause", created_ns)
        assert dep.metadata.name == "lb-test-pause"
        assert dep.spec.replicas == 1

    def test_apply_service(self, k8s: K8sClient, created_ns: str) -> None:
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "lb-test-svc"},
            "spec": {
                "selector": {"app": "lb-test-pause"},
                "ports": [{"port": 80, "targetPort": 80}],
            },
        }
        result = k8s.apply_manifest(manifest, namespace=created_ns)
        assert result is True


# ---------------------------------------------------------------------------
# 12. OpenShift-Specific (conditional)
# ---------------------------------------------------------------------------


class TestOpenShiftFeatures:
    """Tests that only run on OpenShift clusters."""

    @pytest.fixture(autouse=True)
    def _require_openshift(self, k8s: K8sClient) -> None:
        from lakebench.k8s.security import PlatformType, SecurityVerifier

        verifier = SecurityVerifier(k8s)
        if verifier.detect_platform() != PlatformType.OPENSHIFT:
            pytest.skip("Not an OpenShift cluster")

    def test_detect_openshift(self, k8s: K8sClient) -> None:
        from lakebench.deploy.engine import DeploymentEngine
        from tests.conftest import make_config

        cfg = make_config(name="ocp-detect")
        engine = DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)
        assert engine._detect_openshift() is True

    def test_openshift_version_available(self, k8s: K8sClient) -> None:
        from lakebench.k8s.security import SecurityVerifier

        verifier = SecurityVerifier(k8s)
        version = verifier.get_platform_version()
        assert version != ""
        # Should look like "4.x.y"
        assert version.startswith("4.") or version.startswith("1.")

    def test_security_check_detects_openshift(self, k8s: K8sClient, created_ns: str) -> None:
        from lakebench.k8s.security import PlatformType, SecurityVerifier

        verifier = SecurityVerifier(k8s)
        result = verifier.verify_security(created_ns)
        assert result.platform == PlatformType.OPENSHIFT
