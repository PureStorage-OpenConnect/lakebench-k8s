"""Mock Kubernetes client with a fixed, empty-cluster capacity."""

from __future__ import annotations

from unittest.mock import MagicMock

from lakebench.k8s.client import ClusterCapacity


def capacity_k8s(cores: int = 434) -> MagicMock:
    """A client whose total and free capacity are the same ``cores``-core cluster,
    so admission preflight passes."""
    k8s = MagicMock()
    capacity = ClusterCapacity(
        total_cpu_millicores=cores * 1000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=cores * 1000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    k8s.get_cluster_capacity.return_value = capacity
    k8s.get_free_capacity.return_value = capacity
    return k8s
