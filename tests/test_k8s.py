"""Tests for K8sClient: cluster capacity, API retries, exec refusals.

The kubernetes-client is mocked so no real cluster is required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from lakebench.k8s.lease_state import LeaseHoldExceeded
from lakebench.k8s.target import ContextConflictError
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


def _make_client():
    with patch("lakebench.k8s.client.config"):
        with patch("lakebench.k8s.client.client"):
            from lakebench.k8s.client import K8sClient

            return K8sClient(namespace="test-ns")


_CM = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm"}}


class TestK8sClientRetry:
    """``retry_k8s_api`` retries transient API errors on methods that wrap them."""

    def test_apply_manifest_retries_503_then_succeeds(self):
        k = _make_client()
        k._core_v1.read_namespaced_config_map.side_effect = [ApiException(status=503), None]
        with patch("lakebench.k8s.client.time.sleep") as sleep:
            assert k.apply_manifest(dict(_CM)) is True
        assert k._core_v1.read_namespaced_config_map.call_count == 2
        assert sleep.call_count == 1

    def test_create_namespace_retries_503_then_succeeds(self):
        k = _make_client()
        k._core_v1.read_namespace.side_effect = ApiException(status=404)
        k._core_v1.create_namespace.side_effect = [ApiException(status=503), None]
        with patch("lakebench.k8s.client.time.sleep"):
            assert k.create_namespace("ns") is True
        assert k._core_v1.create_namespace.call_count == 2

    def test_non_transient_error_is_not_retried(self):
        from lakebench.k8s.client import K8sResourceError

        k = _make_client()
        k._core_v1.read_namespaced_config_map.side_effect = ApiException(status=403)
        with patch("lakebench.k8s.client.time.sleep") as sleep:
            with pytest.raises(K8sResourceError):
                k.apply_manifest(dict(_CM))
        assert k._core_v1.read_namespaced_config_map.call_count == 1
        sleep.assert_not_called()

    def test_exhausted_retries_raise_resource_error(self):
        from lakebench.k8s.client import _MAX_RETRIES, K8sResourceError

        k = _make_client()
        k._core_v1.read_namespaced_config_map.side_effect = ApiException(status=503)
        with patch("lakebench.k8s.client.time.sleep"):
            with pytest.raises(K8sResourceError, match="Failed to apply ConfigMap/cm"):
                k.apply_manifest(dict(_CM))
        assert k._core_v1.read_namespaced_config_map.call_count == _MAX_RETRIES + 1


class TestExecInPodRefusals:
    """Refusals from the pinned kubectl helper propagate; other errors do not."""

    @pytest.mark.parametrize("exc_cls", [ContextConflictError, LeaseHoldExceeded])
    def test_refusal_is_reraised(self, exc_cls):
        k = _make_client()
        with patch("lakebench.k8s._pinned.pinned_kubectl", side_effect=exc_cls("refused")):
            with pytest.raises(exc_cls):
                k.exec_in_pod("pod", ["true"])

    def test_other_error_becomes_failed_result(self):
        k = _make_client()
        with patch("lakebench.k8s._pinned.pinned_kubectl", side_effect=OSError("boom")):
            assert k.exec_in_pod("pod", ["true"]) == (1, "", "boom")
