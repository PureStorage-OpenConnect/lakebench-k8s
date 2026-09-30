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


class WaitTerminal(WaitError):
    """Raised from a check function to short-circuit ``wait_for_condition``.

    Regular exceptions from a check are swallowed so transient errors do
    not fail the whole wait. This one is the exception -- callers raise
    it when the observed state cannot recover without operator
    intervention (image pull failure, crash loop, admission rejection)
    and there is no point continuing to poll.
    """

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
        except WaitTerminal as e:
            return WaitResult(
                status=WaitStatus.FAILED,
                message=f"Terminal state waiting for {description}: {e}",
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


# Container Waiting reasons where continuing to poll is pointless -- the
# container will not become ready without operator intervention (image fix,
# config change, CR fix). Everything else (ContainerCreating, PodInitializing,
# ...) is transient and we keep waiting.
_TERMINAL_WAITING_REASONS = frozenset(
    {
        "ImagePullBackOff",
        "ErrImagePull",
        "InvalidImageName",
        "CreateContainerConfigError",
        "CreateContainerError",
        "RunContainerError",
    }
)

# CrashLoopBackOff is terminal only after this many observed restarts. A
# container that crashes once because a dependency is still coming up
# (Trino before Hive is ready, Polaris before Postgres is ready) can
# eventually pass; killing the wait on the first observation would turn
# every such normal cold-start race into a spurious deploy failure.
_CRASH_LOOP_TERMINAL_RESTART_COUNT = 3


def _describe_deployment_pods(namespace: str, label_selector: str | None) -> str:
    """Best-effort pod-state summary for a not-ready Deployment/StatefulSet.

    Returns a human-readable string listing each pod's Waiting reason /
    Terminated exit code / probe failure. Diagnostics must not raise, so any
    lookup failure degrades to a note.
    """
    from kubernetes import client as k8s_client

    if not label_selector:
        return "no label selector available to inspect pods"

    try:
        core_api = k8s_client.CoreV1Api()
        pods = core_api.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector,
        ).items
    except Exception as e:  # noqa: BLE001 - diagnostics only
        return f"could not inspect pods: {e}"

    if not pods:
        return (
            "no pods matched the Deployment's selector -- likely rejected by an "
            "SCC, quota, or admission webhook; check the Deployment events."
        )

    notes: list[str] = []
    for pod in pods:
        pod_name = pod.metadata.name
        phase = pod.status.phase
        found = False
        for cs in pod.status.container_statuses or []:
            waiting = cs.state.waiting if cs.state else None
            terminated = cs.state.terminated if cs.state else None
            if waiting and waiting.reason:
                notes.append(
                    f"{pod_name}: waiting ({waiting.reason}: {waiting.message or 'no detail'})"
                )
                found = True
            elif terminated and terminated.reason:
                notes.append(
                    f"{pod_name}: terminated ({terminated.reason}, exit {terminated.exit_code})"
                )
                found = True
            elif not cs.ready:
                notes.append(
                    f"{pod_name}: running but not ready after "
                    f"{cs.restart_count} restart(s) -- probe likely failing"
                )
                found = True
        if not found:
            notes.append(f"{pod_name}: phase {phase}")

    return " | ".join(notes)


def _selector_from_match_labels(match_labels: dict[str, str] | None) -> str | None:
    if not match_labels:
        return None
    return ",".join(f"{k}={v}" for k, v in sorted(match_labels.items()))


def _pod_has_terminal_waiting(namespace: str, label_selector: str | None) -> str | None:
    """Return a diagnostic string if any pod is in a terminal Waiting state,
    otherwise None. Failures reading pods degrade to None (keep waiting).
    """
    from kubernetes import client as k8s_client

    if not label_selector:
        return None
    try:
        core_api = k8s_client.CoreV1Api()
        pods = core_api.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector,
        ).items
    except Exception:  # noqa: BLE001 - diagnostics only
        return None

    for pod in pods:
        for cs in pod.status.container_statuses or []:
            waiting = cs.state.waiting if cs.state else None
            if not waiting or not waiting.reason:
                continue
            if waiting.reason in _TERMINAL_WAITING_REASONS:
                return f"{pod.metadata.name}: {waiting.reason} ({waiting.message or 'no detail'})"
            # CrashLoopBackOff is not terminal until it has crashed
            # `_CRASH_LOOP_TERMINAL_RESTART_COUNT` times. Below that
            # threshold the container may still recover once a
            # dependency (Hive metastore, Postgres, ...) comes up.
            if (
                waiting.reason == "CrashLoopBackOff"
                and (cs.restart_count or 0) >= _CRASH_LOOP_TERMINAL_RESTART_COUNT
            ):
                return (
                    f"{pod.metadata.name}: CrashLoopBackOff "
                    f"({waiting.message or 'no detail'}); "
                    f"restarts={cs.restart_count}"
                )
    return None


def wait_for_deployment_ready(
    client: K8sClient,
    name: str,
    namespace: str,
    timeout_seconds: int = 300,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for a Deployment to have all replicas ready.

    Fails fast when a pod enters a terminal Waiting state (ImagePullBackOff,
    CrashLoopBackOff, ...), rather than polling `ready_replicas` for the full
    timeout and returning a bare "not ready" (LB-070 shape: a Helm crash loop
    was masked as an SCC rollout timeout for 10 minutes because the only
    signal was `ready >= desired`). On timeout, the message names each pod's
    state so the caller can act.
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

            selector = _selector_from_match_labels(
                dep.spec.selector.match_labels if dep.spec.selector else None
            )
            terminal = _pod_has_terminal_waiting(namespace, selector)
            if terminal:
                raise WaitTerminal(
                    f"Deployment {name} pod entered terminal Waiting state: {terminal}"
                )

            return False, f"Deployment {name} not ready ({ready}/{desired} replicas)"
        except ApiException as e:
            if e.status == 404:
                return False, f"Deployment {name} not found"
            return False, f"Error: {e.reason}"

    result = wait_for_condition(
        check,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
        description=f"deployment {name}",
    )

    # FAILED already carries the terminal Waiting reason in its message;
    # only augment TIMEOUT, where the caller otherwise sees just
    # "not ready (0/1 replicas)".
    if result.status is WaitStatus.TIMEOUT:
        try:
            dep = apps_v1.read_namespaced_deployment(name, namespace)
            selector = _selector_from_match_labels(
                dep.spec.selector.match_labels if dep.spec.selector else None
            )
            diagnostic = _describe_deployment_pods(namespace, selector)
            result.message = f"{result.message}. Pod state: {diagnostic}"
        except Exception:  # noqa: BLE001 - diagnostics only
            pass

    return result


def wait_for_statefulset_ready(
    client: K8sClient,
    name: str,
    namespace: str,
    timeout_seconds: int = 300,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for a StatefulSet to have all replicas ready.

    Same LB-070 shape as ``wait_for_deployment_ready`` -- Postgres and
    Trino workers both run as StatefulSets, and a bad image tag or
    misconfigured probe used to burn the full timeout with a bare
    "not ready" message. Terminal-Waiting fast-fail and per-pod
    diagnostic on timeout apply here too.
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

            selector = _selector_from_match_labels(
                sts.spec.selector.match_labels if sts.spec.selector else None
            )
            terminal = _pod_has_terminal_waiting(namespace, selector)
            if terminal:
                raise WaitTerminal(
                    f"StatefulSet {name} pod entered terminal Waiting state: {terminal}"
                )

            return False, f"StatefulSet {name} not ready ({ready}/{desired} replicas)"
        except ApiException as e:
            if e.status == 404:
                return False, f"StatefulSet {name} not found"
            return False, f"Error: {e.reason}"

    result = wait_for_condition(
        check,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
        description=f"statefulset {name}",
    )

    if result.status is WaitStatus.TIMEOUT:
        try:
            sts = apps_v1.read_namespaced_stateful_set(name, namespace)
            selector = _selector_from_match_labels(
                sts.spec.selector.match_labels if sts.spec.selector else None
            )
            diagnostic = _describe_deployment_pods(namespace, selector)
            result.message = f"{result.message}. Pod state: {diagnostic}"
        except Exception:  # noqa: BLE001 - diagnostics only
            pass

    return result
