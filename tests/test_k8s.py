"""Tests for the K8s client, security, and wait modules (P3).

All tests mock the kubernetes-client to avoid requiring a real cluster.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import point_kubeconfig_at, write_kubeconfig


@pytest.fixture(autouse=True)
def _fake_kubeconfig(tmp_path, monkeypatch):
    """A kubeconfig whose current context is ``my-context`` (no cluster)."""
    path = tmp_path / "kubeconfig"
    write_kubeconfig(path, {"my-context": "https://a.example:6443"}, current="my-context")
    point_kubeconfig_at(monkeypatch, path)


# ===========================================================================
# K8sClient static helpers
# ===========================================================================


# ===========================================================================
# K8sClient connection and namespace
# ===========================================================================


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


# ===========================================================================
# SecurityVerifier
# ===========================================================================


# ===========================================================================
# Wait module
# ===========================================================================
