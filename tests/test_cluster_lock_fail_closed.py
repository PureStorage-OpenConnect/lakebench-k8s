"""``acquire_cluster_lock`` fails closed under timeout.

Older revisions of ``deploy/cluster_lock.py`` had branches that treated
a timed-out lease acquire as "proceed unlocked". Under parallel UAT that
lets two writers both mutate the shared spark-operator watch list at the
same time, which is exactly the race the lease is here to close (F5 /
LB-shared-cluster-state-races).

The invariant we lock in here: whenever the lease cannot be acquired
within the caller's timeout budget, ``acquire_cluster_lock`` raises
``ClusterLockHeld`` -- it never returns a handle, and it never
silently proceeds. The context-manager form :func:`cluster_lock`
propagates the same exception so ``with cluster_lock(...):`` bodies
never run against unlocked shared state.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy.cluster_lock import (
    ClusterLockHeld,
    LeaseState,
    acquire_cluster_lock,
    cluster_lock,
)


def _held_state(
    holder: str = "other-host@ci@abc",
    # Far future so is_expired() is False, but well inside a POSIX time_t
    # so datetime.fromtimestamp does not overflow.
    expires_at_epoch: float = 4102444800.0,  # 2100-01-01 UTC
) -> LeaseState:
    return LeaseState(
        holder=holder,
        acquired_at="2026-09-29T00:00:00+00:00",
        ttl_seconds=3600,
        expires_at_epoch=expires_at_epoch,
        resource_version="42",
        uid="abcd-1234",
    )


def _core_v1() -> MagicMock:
    """Stand-in for a ``kubernetes.client.CoreV1Api``.

    Says the lease namespace already exists so acquire skips the
    bootstrap create, and gives ``_try_acquire_once`` a live state to
    read via the patched ``read_cluster_lock``.
    """
    m = MagicMock()
    m.read_namespace.return_value = SimpleNamespace(
        metadata=SimpleNamespace(name="lakebench-system")
    )
    return m


def test_acquire_raises_cluster_lock_held_on_timeout() -> None:
    """A live holder that never releases: acquire must raise, not return."""
    with patch(
        "lakebench.deploy.cluster_lock.read_cluster_lock",
        return_value=_held_state(),
    ):
        with pytest.raises(ClusterLockHeld) as exc_info:
            # Timeout=0 guarantees the poll loop exits on the first read.
            acquire_cluster_lock(_core_v1(), ttl_seconds=60, timeout=0)
    err = exc_info.value
    assert err.holder == "other-host@ci@abc"
    assert err.ttl_seconds == 3600
    # The message must include an actionable recovery hint.
    assert "release-lock" in str(err)


def test_cluster_lock_context_manager_reraises_on_timeout() -> None:
    """``with cluster_lock(...):`` propagates the refusal instead of running the body."""
    entered = False
    with patch(
        "lakebench.deploy.cluster_lock.read_cluster_lock",
        return_value=_held_state(),
    ):
        with pytest.raises(ClusterLockHeld):
            with cluster_lock(_core_v1(), ttl_seconds=60, timeout=0):
                entered = True  # pragma: no cover -- must not execute
    assert entered is False


def test_acquire_never_returns_unlocked_string() -> None:
    """Defensive: the function must not gain a 'proceeded unlocked' return type.

    A regression that returned a truthy sentinel instead of raising would
    let callers unlock ``with cluster_lock(...) as handle:`` bodies while
    the lease was held. Assert the raise semantics rather than probing
    for a sentinel that should not exist.
    """
    with patch(
        "lakebench.deploy.cluster_lock.read_cluster_lock",
        return_value=_held_state(),
    ):
        with pytest.raises(ClusterLockHeld):
            acquire_cluster_lock(_core_v1(), ttl_seconds=60, timeout=0)
