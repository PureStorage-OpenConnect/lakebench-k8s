"""Wait-for-ready logic for Kubernetes resources.

This module implements the wait-for-ready patterns documented in the
Lakebench spec addendum Section E.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from .client import K8sClient, K8sError


class WaitStatus(Enum):
    """Status of a wait operation."""

    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class WaitResult:
    """Result of a wait operation."""

    status: WaitStatus
    message: str
    elapsed_seconds: float
    attempts: int


class WaitError(K8sError):
    """Raised when a wait operation fails."""

    pass


class WaitTimeout(WaitError):
    """Raised when a wait operation times out."""

    pass


def wait_for_condition(
    check_fn: Callable[[], tuple[bool, str]],
    timeout_seconds: int = 300,
    poll_interval: int = 5,
    description: str = "condition",
) -> WaitResult:
    """Generic wait for a condition to be true.

    Args:
        check_fn: Function that returns (success, message)
        timeout_seconds: Maximum time to wait
        poll_interval: Seconds between checks
        description: Description for logging

    Returns:
        WaitResult with outcome
    """
    start_time = time.time()
    attempts = 0

    while True:
        attempts += 1
        elapsed = time.time() - start_time

        try:
            success, message = check_fn()
            if success:
                return WaitResult(
                    status=WaitStatus.READY,
                    message=message,
                    elapsed_seconds=elapsed,
                    attempts=attempts,
                )
        except Exception as e:
            message = str(e)

        if elapsed >= timeout_seconds:
            return WaitResult(
                status=WaitStatus.TIMEOUT,
                message=f"Timeout after {int(elapsed)}s waiting for {description}: {message}",
                elapsed_seconds=elapsed,
                attempts=attempts,
            )

        time.sleep(poll_interval)


def wait_for_pod_ready(
    client: K8sClient,
    name: str,
    namespace: str,
    timeout_seconds: int = 300,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for a pod to be ready.

    Args:
        client: K8sClient instance
        name: Pod name
        namespace: Namespace
        timeout_seconds: Maximum time to wait
        poll_interval: Seconds between checks

    Returns:
        WaitResult with outcome
    """

    def check() -> tuple[bool, str]:
        status = client.get_pod_status(name, namespace)
        if not status.exists:
            return False, f"Pod {name} not found"
        if status.ready:
            return True, f"Pod {name} is ready"
        return False, f"Pod {name} is {status.message}"

    return wait_for_condition(
        check,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
        description=f"pod {name}",
    )


def wait_for_postgres_ready(
    client: K8sClient,
    pod_name: str,
    namespace: str,
    database: str = "hive",
    user: str = "hive",
    timeout_seconds: int = 120,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for PostgreSQL to be ready using pg_isready.

    This implements the pattern from Addendum E.1:
    - First wait for pod to be running
    - Then verify pg_isready returns success

    Args:
        client: K8sClient instance
        pod_name: PostgreSQL pod name
        namespace: Namespace
        database: Database name to check
        user: Database user to check
        timeout_seconds: Maximum time to wait
        poll_interval: Seconds between checks

    Returns:
        WaitResult with outcome
    """
    # First wait for pod
    pod_result = wait_for_pod_ready(
        client,
        pod_name,
        namespace,
        timeout_seconds=timeout_seconds // 2,
        poll_interval=poll_interval,
    )
    if pod_result.status != WaitStatus.READY:
        return pod_result

    # Then check pg_isready
    def check() -> tuple[bool, str]:
        cmd = ["pg_isready", "-U", user, "-d", database, "-h", "127.0.0.1"]
        exit_code, stdout, stderr = client.exec_in_pod(pod_name, cmd, namespace)
        if exit_code == 0:
            return True, "PostgreSQL is accepting connections"
        return False, f"pg_isready failed: {stderr or stdout}"

    return wait_for_condition(
        check,
        timeout_seconds=timeout_seconds // 2,
        poll_interval=poll_interval,
        description="PostgreSQL ready",
    )


def wait_for_tcp_port(
    client: K8sClient,
    pod_name: str,
    namespace: str,
    port: int,
    timeout_seconds: int = 180,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for a TCP port to be accepting connections.

    This implements the pattern from Addendum E.2 for Hive Metastore.

    Args:
        client: K8sClient instance
        pod_name: Pod name to check
        namespace: Namespace
        port: TCP port number
        timeout_seconds: Maximum time to wait
        poll_interval: Seconds between checks

    Returns:
        WaitResult with outcome
    """
    start_time = time.time()

    # First wait for pod (use 2/3 of timeout for pod, 1/3 for port check)
    pod_timeout = (timeout_seconds * 2) // 3
    pod_result = wait_for_pod_ready(
        client,
        pod_name,
        namespace,
        timeout_seconds=pod_timeout,
        poll_interval=poll_interval,
    )
    if pod_result.status != WaitStatus.READY:
        return pod_result

    # Calculate remaining time for port check
    elapsed = time.time() - start_time
    remaining_timeout = max(60, timeout_seconds - int(elapsed))  # At least 60s for port

    # Then check TCP port using nc or bash
    def check() -> tuple[bool, str]:
        # Try nc first, fall back to bash /dev/tcp
        cmd = [
            "sh",
            "-c",
            f"nc -z localhost {port} 2>/dev/null || (echo >/dev/tcp/localhost/{port}) 2>/dev/null",
        ]
        exit_code, stdout, stderr = client.exec_in_pod(pod_name, cmd, namespace)
        if exit_code == 0:
            return True, f"Port {port} is accepting connections"
        return False, f"Port {port} not ready"

    return wait_for_condition(
        check,
        timeout_seconds=remaining_timeout,
        poll_interval=poll_interval,
        description=f"TCP port {port}",
    )


def wait_for_hive_metastore(
    client: K8sClient,
    pod_name: str,
    namespace: str,
    timeout_seconds: int = 180,
    poll_interval: int = 10,
) -> WaitResult:
    """Wait for Hive Metastore thrift port to be ready.

    Args:
        client: K8sClient instance
        pod_name: Hive Metastore pod name
        namespace: Namespace
        timeout_seconds: Maximum time to wait
        poll_interval: Seconds between checks

    Returns:
        WaitResult with outcome
    """
    return wait_for_tcp_port(
        client,
        pod_name,
        namespace,
        port=9083,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
    )


def wait_for_trino_catalog(
    client: K8sClient,
    pod_name: str,
    namespace: str,
    catalog_name: str = "lakehouse",
    timeout_seconds: int = 180,
    poll_interval: int = 10,
) -> WaitResult:
    """Wait for Trino to be ready with catalog registered.

    This implements the pattern from Addendum E.3:
    - Execute SHOW CATALOGS
    - Verify catalog is registered

    Args:
        client: K8sClient instance
        pod_name: Trino coordinator pod name
        namespace: Namespace
        catalog_name: Catalog name to verify
        timeout_seconds: Maximum time to wait
        poll_interval: Seconds between checks

    Returns:
        WaitResult with outcome
    """
    # First wait for pod
    pod_result = wait_for_pod_ready(
        client,
        pod_name,
        namespace,
        timeout_seconds=timeout_seconds // 2,
        poll_interval=poll_interval,
    )
    if pod_result.status != WaitStatus.READY:
        return pod_result

    # Then check catalog
    def check() -> tuple[bool, str]:
        cmd = ["trino", "--execute", "SHOW CATALOGS"]
        exit_code, stdout, stderr = client.exec_in_pod(pod_name, cmd, namespace)
        if exit_code != 0:
            return False, f"SHOW CATALOGS failed: {stderr}"

        # Trino CLI outputs catalog names with quotes: "lakehouse"
        # Strip quotes when comparing
        catalogs = [line.strip().strip('"') for line in stdout.split("\n") if line.strip()]
        if catalog_name in catalogs:
            return True, f"Catalog '{catalog_name}' is registered"
        return False, f"Catalog '{catalog_name}' not found in: {catalogs}"

    return wait_for_condition(
        check,
        timeout_seconds=timeout_seconds // 2,
        poll_interval=poll_interval,
        description=f"Trino catalog {catalog_name}",
    )


def wait_for_deployment_ready(
    client: K8sClient,
    name: str,
    namespace: str,
    timeout_seconds: int = 300,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for a Deployment to have all replicas ready.

    Args:
        client: K8sClient instance
        name: Deployment name
        namespace: Namespace
        timeout_seconds: Maximum time to wait
        poll_interval: Seconds between checks

    Returns:
        WaitResult with outcome
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    apps_v1 = k8s_client.AppsV1Api()

    def check() -> tuple[bool, str]:
        try:
            dep = apps_v1.read_namespaced_deployment(name, namespace)
            desired = dep.spec.replicas or 1
            ready = dep.status.ready_replicas or 0

            if ready >= desired:
                return True, f"Deployment {name} ready ({ready}/{desired} replicas)"
            return False, f"Deployment {name} not ready ({ready}/{desired} replicas)"
        except ApiException as e:
            if e.status == 404:
                return False, f"Deployment {name} not found"
            return False, f"Error: {e.reason}"

    return wait_for_condition(
        check,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
        description=f"deployment {name}",
    )


def wait_for_statefulset_ready(
    client: K8sClient,
    name: str,
    namespace: str,
    timeout_seconds: int = 300,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for a StatefulSet to have all replicas ready.

    Args:
        client: K8sClient instance
        name: StatefulSet name
        namespace: Namespace
        timeout_seconds: Maximum time to wait
        poll_interval: Seconds between checks

    Returns:
        WaitResult with outcome
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    apps_v1 = k8s_client.AppsV1Api()

    def check() -> tuple[bool, str]:
        try:
            sts = apps_v1.read_namespaced_stateful_set(name, namespace)
            desired = sts.spec.replicas or 1
            ready = sts.status.ready_replicas or 0

            if ready >= desired:
                return True, f"StatefulSet {name} ready ({ready}/{desired} replicas)"
            return False, f"StatefulSet {name} not ready ({ready}/{desired} replicas)"
        except ApiException as e:
            if e.status == 404:
                return False, f"StatefulSet {name} not found"
            return False, f"Error: {e.reason}"

    return wait_for_condition(
        check,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
        description=f"statefulset {name}",
    )
