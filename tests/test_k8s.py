"""Tests for K8sClient cluster capacity.

The kubernetes-client is mocked so no real cluster is required.
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
