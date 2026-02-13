"""Kubernetes client module for Lakebench."""

from .client import (
    ClusterCapacity,
    K8sClient,
    K8sConnectionError,
    K8sContext,
    K8sError,
    K8sResourceError,
    ResourceStatus,
    get_k8s_client,
)
from .security import (
    PlatformType,
    SCCStatus,
    SecurityCheckResult,
    SecurityVerifier,
)
from .wait import (
    WaitError,
    WaitResult,
    WaitStatus,
    WaitTimeout,
    wait_for_condition,
    wait_for_deployment_ready,
    wait_for_hive_metastore,
    wait_for_pod_ready,
    wait_for_postgres_ready,
    wait_for_statefulset_ready,
    wait_for_tcp_port,
    wait_for_trino_catalog,
)

__all__ = [
    # Client
    "K8sClient",
    "K8sContext",
    "ClusterCapacity",
    "ResourceStatus",
    "get_k8s_client",
    # Errors
    "K8sError",
    "K8sConnectionError",
    "K8sResourceError",
    "WaitError",
    "WaitTimeout",
    # Wait
    "WaitResult",
    "WaitStatus",
    "wait_for_condition",
    "wait_for_pod_ready",
    "wait_for_postgres_ready",
    "wait_for_tcp_port",
    "wait_for_hive_metastore",
    "wait_for_trino_catalog",
    "wait_for_deployment_ready",
    "wait_for_statefulset_ready",
    # Security
    "PlatformType",
    "SCCStatus",
    "SecurityCheckResult",
    "SecurityVerifier",
]
