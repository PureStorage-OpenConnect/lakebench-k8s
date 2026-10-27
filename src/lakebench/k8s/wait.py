"""Wait-for-ready logic for Kubernetes resources."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

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

    Raises:
        DeployTimeout: the deploy deadline, not ``timeout_seconds``,
            ended the wait. Every ``wait_for_*`` here goes through this
            function, so each is bounded by ``lakebench deploy --timeout``.
    """
    from lakebench.deploy import deadline as deploy_deadline

    limit = deploy_deadline.clamp(timeout_seconds)
    start_time = time.time()
    attempts = 0
    message = ""

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

        if elapsed >= limit:
            if limit < timeout_seconds:
                # The deploy deadline cut this wait short.
                deploy_deadline.check(description, message)
            return WaitResult(
                status=WaitStatus.TIMEOUT,
                message=f"Timeout after {int(elapsed)}s waiting for {description}: {message}",
                elapsed_seconds=elapsed,
                attempts=attempts,
            )

        time.sleep(max(0.0, min(poll_interval, limit - elapsed)))


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
    """Wait for the pod to be ready, then for ``pg_isready`` to succeed.

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


def wait_for_trino_catalog(
    client: K8sClient,
    pod_name: str,
    namespace: str,
    catalog_name: str = "lakehouse",
    timeout_seconds: int = 180,
    poll_interval: int = 10,
) -> WaitResult:
    """Wait for the pod to be ready, then for ``SHOW CATALOGS`` to list the catalog.

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


def _wait_for_workload_ready(
    kind: str,
    read: Callable[[str, str], Any],
    name: str,
    namespace: str,
    timeout_seconds: int,
    poll_interval: int,
) -> WaitResult:
    """Wait for a Deployment or StatefulSet (``kind``) to have all replicas ready.

    Fails fast when a pod enters a terminal Waiting state (ImagePullBackOff,
    CrashLoopBackOff, ...) instead of polling ``ready_replicas`` for the full
    timeout, where a crash loop would read as a bare "not ready". On timeout,
    the message names each pod's state so the caller can act.
    """
    from kubernetes.client.rest import ApiException

    def selector(obj: Any) -> str | None:
        return _selector_from_match_labels(
            obj.spec.selector.match_labels if obj.spec.selector else None
        )

    def check() -> tuple[bool, str]:
        try:
            obj = read(name, namespace)
            desired = obj.spec.replicas or 1
            ready = obj.status.ready_replicas or 0

            if ready >= desired:
                return True, f"{kind} {name} ready ({ready}/{desired} replicas)"

            terminal = _pod_has_terminal_waiting(namespace, selector(obj))
            if terminal:
                raise WaitTerminal(f"{kind} {name} pod entered terminal Waiting state: {terminal}")

            return False, f"{kind} {name} not ready ({ready}/{desired} replicas)"
        except ApiException as e:
            if e.status == 404:
                return False, f"{kind} {name} not found"
            return False, f"Error: {e.reason}"

    result = wait_for_condition(
        check,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
        description=f"{kind.lower()} {name}",
    )

    # FAILED already carries the terminal Waiting reason in its message;
    # only augment TIMEOUT, where the caller otherwise sees just
    # "not ready (0/1 replicas)".
    if result.status is WaitStatus.TIMEOUT:
        try:
            diagnostic = _describe_deployment_pods(namespace, selector(read(name, namespace)))
            result.message = f"{result.message}. Pod state: {diagnostic}"
        except Exception:  # noqa: BLE001 - diagnostics only
            pass

    return result


def wait_for_deployment_ready(
    client: K8sClient,
    name: str,
    namespace: str,
    timeout_seconds: int = 300,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for a Deployment to have all replicas ready."""
    from kubernetes import client as k8s_client

    read = k8s_client.AppsV1Api().read_namespaced_deployment
    return _wait_for_workload_ready(
        "Deployment", read, name, namespace, timeout_seconds, poll_interval
    )


def wait_for_statefulset_ready(
    client: K8sClient,
    name: str,
    namespace: str,
    timeout_seconds: int = 300,
    poll_interval: int = 5,
) -> WaitResult:
    """Wait for a StatefulSet to have all replicas ready."""
    from kubernetes import client as k8s_client

    read = k8s_client.AppsV1Api().read_namespaced_stateful_set
    return _wait_for_workload_ready(
        "StatefulSet", read, name, namespace, timeout_seconds, poll_interval
    )
