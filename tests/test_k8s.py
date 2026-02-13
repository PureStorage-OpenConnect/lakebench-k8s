"""Tests for the K8s client, security, and wait modules (P3).

All tests mock the kubernetes-client to avoid requiring a real cluster.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

# ===========================================================================
# K8sClient static helpers
# ===========================================================================


class TestK8sClientParsers:
    """Tests for K8sClient._parse_cpu_to_millicores and _parse_memory_to_bytes."""

    def test_parse_cpu_millicores(self):
        from lakebench.k8s.client import K8sClient

        assert K8sClient._parse_cpu_to_millicores("1000m") == 1000
        assert K8sClient._parse_cpu_to_millicores("500m") == 500
        assert K8sClient._parse_cpu_to_millicores("4") == 4000
        assert K8sClient._parse_cpu_to_millicores("0.5") == 500
        assert K8sClient._parse_cpu_to_millicores("2") == 2000

    def test_parse_memory_ki(self):
        from lakebench.k8s.client import K8sClient

        assert K8sClient._parse_memory_to_bytes("1024Ki") == 1024 * 1024
        assert K8sClient._parse_memory_to_bytes("4Mi") == 4 * 1024**2
        assert K8sClient._parse_memory_to_bytes("2Gi") == 2 * 1024**3
        assert K8sClient._parse_memory_to_bytes("1Ti") == 1024**4

    def test_parse_memory_si(self):
        from lakebench.k8s.client import K8sClient

        assert K8sClient._parse_memory_to_bytes("1000k") == 1000 * 1000
        assert K8sClient._parse_memory_to_bytes("2M") == 2 * 1000**2
        assert K8sClient._parse_memory_to_bytes("4G") == 4 * 1000**3

    def test_parse_memory_plain_bytes(self):
        from lakebench.k8s.client import K8sClient

        assert K8sClient._parse_memory_to_bytes("1048576") == 1048576


# ===========================================================================
# K8sClient connection and namespace
# ===========================================================================


class TestK8sClientInit:
    """Tests for K8sClient initialization."""

    def test_connection_error(self):
        from lakebench.k8s.client import K8sClient, K8sConnectionError

        with patch("lakebench.k8s.client.config") as mock_config:
            mock_config.load_incluster_config.side_effect = Exception("not in cluster")
            mock_config.load_kube_config.side_effect = Exception("no kubeconfig")
            mock_config.ConfigException = Exception
            with pytest.raises(K8sConnectionError):
                K8sClient()

    def test_with_context(self):
        from lakebench.k8s.client import K8sClient

        with patch("lakebench.k8s.client.config") as mock_config:
            with patch("lakebench.k8s.client.client"):
                K8sClient(context="my-context")
                mock_config.load_kube_config.assert_called_once_with(context="my-context")


class TestK8sClientNamespace:
    """Tests for K8sClient namespace operations."""

    def _make_client(self):
        with patch("lakebench.k8s.client.config"):
            with patch("lakebench.k8s.client.client"):
                return __import__("lakebench.k8s.client", fromlist=["K8sClient"]).K8sClient(
                    namespace="test-ns"
                )

    def test_namespace_property(self):
        k = self._make_client()
        assert k.namespace == "test-ns"

    def test_namespace_exists_true(self):
        k = self._make_client()
        k._core_v1.read_namespace.return_value = MagicMock()
        assert k.namespace_exists("test-ns") is True

    def test_namespace_exists_false(self):
        from kubernetes.client.rest import ApiException

        k = self._make_client()
        k._core_v1.read_namespace.side_effect = ApiException(status=404, reason="Not Found")
        assert k.namespace_exists("missing-ns") is False

    def test_create_namespace_already_exists(self):
        k = self._make_client()
        k._core_v1.read_namespace.return_value = MagicMock()  # exists
        result = k.create_namespace("test-ns")
        assert result is False  # Already existed

    def test_create_namespace_success(self):
        from kubernetes.client.rest import ApiException

        k = self._make_client()
        k._core_v1.read_namespace.side_effect = ApiException(status=404, reason="Not Found")
        k._core_v1.create_namespace.return_value = MagicMock()
        result = k.create_namespace("new-ns")
        assert result is True

    def test_delete_namespace_not_exists(self):
        from kubernetes.client.rest import ApiException

        k = self._make_client()
        k._core_v1.read_namespace.side_effect = ApiException(status=404, reason="Not Found")
        result = k.delete_namespace("missing-ns")
        assert result is False


class TestK8sClientSecrets:
    """Tests for K8sClient secret operations."""

    def _make_client(self):
        with patch("lakebench.k8s.client.config"):
            with patch("lakebench.k8s.client.client"):
                from lakebench.k8s.client import K8sClient

                return K8sClient(namespace="test-ns")

    def test_secret_exists_true(self):
        k = self._make_client()
        k._core_v1.read_namespaced_secret.return_value = MagicMock()
        assert k.secret_exists("my-secret", "test-ns") is True

    def test_secret_exists_false(self):
        from kubernetes.client.rest import ApiException

        k = self._make_client()
        k._core_v1.read_namespaced_secret.side_effect = ApiException(status=404, reason="Not Found")
        assert k.secret_exists("missing", "test-ns") is False


class TestK8sClientApplyManifest:
    """Tests for K8sClient.apply_manifest()."""

    def _make_client(self):
        with patch("lakebench.k8s.client.config"):
            with patch("lakebench.k8s.client.client"):
                from lakebench.k8s.client import K8sClient

                return K8sClient(namespace="test-ns")

    def test_apply_configmap_create(self):
        from kubernetes.client.rest import ApiException

        k = self._make_client()
        k._core_v1.read_namespaced_config_map.side_effect = ApiException(
            status=404, reason="Not Found"
        )
        manifest = {
            "kind": "ConfigMap",
            "metadata": {"name": "test-cm"},
            "data": {"key": "value"},
        }
        result = k.apply_manifest(manifest, namespace="test-ns")
        assert result is True
        k._core_v1.create_namespaced_config_map.assert_called_once()

    def test_apply_configmap_update(self):
        k = self._make_client()
        k._core_v1.read_namespaced_config_map.return_value = MagicMock()
        manifest = {
            "kind": "ConfigMap",
            "metadata": {"name": "test-cm"},
            "data": {"key": "value"},
        }
        result = k.apply_manifest(manifest, namespace="test-ns")
        assert result is True
        k._core_v1.replace_namespaced_config_map.assert_called_once()

    def test_apply_deployment_create(self):
        from kubernetes.client.rest import ApiException

        k = self._make_client()
        k._apps_v1.read_namespaced_deployment.side_effect = ApiException(
            status=404, reason="Not Found"
        )
        manifest = {
            "kind": "Deployment",
            "metadata": {"name": "test-dep"},
            "spec": {},
        }
        result = k.apply_manifest(manifest, namespace="test-ns")
        assert result is True
        k._apps_v1.create_namespaced_deployment.assert_called_once()

    def test_apply_unsupported_kind(self):
        from lakebench.k8s.client import K8sResourceError

        k = self._make_client()
        manifest = {
            "kind": "FancyWidget",
            "apiVersion": "v1",
            "metadata": {"name": "widget"},
        }
        with pytest.raises(K8sResourceError, match="Unsupported resource kind"):
            k.apply_manifest(manifest, namespace="test-ns")


class TestK8sClientExecInPod:
    """Tests for K8sClient.exec_in_pod()."""

    def _make_client(self):
        with patch("lakebench.k8s.client.config"):
            with patch("lakebench.k8s.client.client"):
                from lakebench.k8s.client import K8sClient

                return K8sClient(namespace="test-ns")

    def test_exec_success(self):
        k = self._make_client()
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="hello", stderr="")
            code, stdout, stderr = k.exec_in_pod("pod-0", ["echo", "hello"], "test-ns")
            assert code == 0
            assert stdout == "hello"

    def test_exec_timeout(self):
        k = self._make_client()
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="kubectl", timeout=30)
            code, stdout, stderr = k.exec_in_pod("pod-0", ["sleep", "999"], "test-ns")
            assert code == 1
            assert "timed out" in stderr

    def test_exec_with_container(self):
        k = self._make_client()
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
            k.exec_in_pod("pod-0", ["cmd"], "test-ns", container="sidecar")
            cmd = mock_run.call_args[0][0]
            assert "-c" in cmd
            assert "sidecar" in cmd


class TestK8sClientClusterCapacity:
    """Tests for K8sClient.get_cluster_capacity()."""

    def _make_client(self):
        with patch("lakebench.k8s.client.config"):
            with patch("lakebench.k8s.client.client"):
                from lakebench.k8s.client import K8sClient

                return K8sClient(namespace="test-ns")

    def test_cluster_capacity_filters_control_plane(self):
        k = self._make_client()

        worker = MagicMock()
        worker.metadata.labels = {"node-role.kubernetes.io/worker": ""}
        worker.status.allocatable = {"cpu": "4", "memory": "8Gi"}

        control = MagicMock()
        control.metadata.labels = {"node-role.kubernetes.io/control-plane": ""}
        control.status.allocatable = {"cpu": "2", "memory": "4Gi"}

        k._core_v1.list_node.return_value = MagicMock(items=[worker, control])
        cap = k.get_cluster_capacity()
        assert cap is not None
        assert cap.node_count == 1
        assert cap.total_cpu_millicores == 4000

    def test_cluster_capacity_no_workers(self):
        k = self._make_client()

        control = MagicMock()
        control.metadata.labels = {"node-role.kubernetes.io/control-plane": ""}
        control.status.allocatable = {"cpu": "2", "memory": "4Gi"}

        k._core_v1.list_node.return_value = MagicMock(items=[control])
        cap = k.get_cluster_capacity()
        assert cap is None


# ===========================================================================
# SecurityVerifier
# ===========================================================================


class TestSecurityVerifier:
    """Tests for SecurityVerifier platform detection."""

    def test_detect_vanilla(self):
        from lakebench.k8s.security import PlatformType, SecurityVerifier

        mock_k8s = MagicMock()
        verifier = SecurityVerifier(mock_k8s)

        with patch("lakebench.k8s.security.k8s_client", create=True):
            mock_api_ext = MagicMock()
            mock_crds = MagicMock()
            mock_crds.items = []  # No OpenShift CRDs
            mock_api_ext.list_custom_resource_definition.return_value = mock_crds
            with patch("kubernetes.client.ApiextensionsV1Api", return_value=mock_api_ext):
                result = verifier.detect_platform()
                assert result == PlatformType.VANILLA

    def test_security_check_result_passed(self):
        from lakebench.k8s.security import PlatformType, SecurityCheckResult

        result = SecurityCheckResult(
            platform=PlatformType.VANILLA, checks_passed=2, checks_failed=0
        )
        assert result.passed is True

    def test_security_check_result_failed(self):
        from lakebench.k8s.security import PlatformType, SecurityCheckResult

        result = SecurityCheckResult(
            platform=PlatformType.VANILLA, checks_passed=1, checks_failed=1
        )
        assert result.passed is False


# ===========================================================================
# Wait module
# ===========================================================================


class TestWaitForCondition:
    """Tests for wait_for_condition()."""

    def test_immediate_success(self):
        from lakebench.k8s.wait import WaitStatus, wait_for_condition

        result = wait_for_condition(
            lambda: (True, "done"),
            timeout_seconds=10,
            poll_interval=1,
        )
        assert result.status == WaitStatus.READY
        assert result.attempts == 1

    def test_timeout(self):
        from lakebench.k8s.wait import WaitStatus, wait_for_condition

        call_count = 0

        def always_false():
            nonlocal call_count
            call_count += 1
            return False, "not ready"

        with patch("lakebench.k8s.wait.time") as mock_time:
            # Simulate time progression: 0, 0.1, 0.2, ..., 11.0
            mock_time.time.side_effect = [0, 0, 0.1, 0.2, 11.0]
            mock_time.sleep = MagicMock()
            result = wait_for_condition(
                always_false,
                timeout_seconds=10,
                poll_interval=1,
            )
            assert result.status == WaitStatus.TIMEOUT

    def test_exception_in_check(self):
        from lakebench.k8s.wait import WaitStatus, wait_for_condition

        def exploding():
            raise ConnectionError("boom")

        with patch("lakebench.k8s.wait.time") as mock_time:
            mock_time.time.side_effect = [0, 0, 0.1, 0.2, 11.0]
            mock_time.sleep = MagicMock()
            result = wait_for_condition(
                exploding,
                timeout_seconds=10,
                poll_interval=1,
            )
            assert result.status == WaitStatus.TIMEOUT
            assert "boom" in result.message


class TestWaitDataClasses:
    """Tests for wait module data classes."""

    def test_wait_status_enum(self):
        from lakebench.k8s.wait import WaitStatus

        assert WaitStatus.PENDING.value == "pending"
        assert WaitStatus.READY.value == "ready"
        assert WaitStatus.FAILED.value == "failed"
        assert WaitStatus.TIMEOUT.value == "timeout"

    def test_wait_result(self):
        from lakebench.k8s.wait import WaitResult, WaitStatus

        r = WaitResult(status=WaitStatus.READY, message="ok", elapsed_seconds=1.5, attempts=3)
        assert r.status == WaitStatus.READY
        assert r.elapsed_seconds == 1.5
        assert r.attempts == 3

    def test_wait_error_hierarchy(self):
        from lakebench.k8s.client import K8sError
        from lakebench.k8s.wait import WaitError, WaitTimeout

        assert issubclass(WaitError, K8sError)
        assert issubclass(WaitTimeout, WaitError)
