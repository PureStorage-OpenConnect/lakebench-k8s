"""Whether this process holds the cluster lease, and how long it may keep it.

``lakebench.deploy.cluster_lock.cluster_lock`` sets this state while it
holds the lease; the low-level helpers read it without importing the
deploy layer: ``lakebench.k8s._pinned`` (new session and timeouts for
kubectl, helm and oc) and ``lakebench.k8s.client.K8sClient`` (request
timeouts). DESIGN ch01 3.6 and 3.7.
"""

from __future__ import annotations

import time
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any

# (connect, read) seconds for every Kubernetes API call made while the lease
# is held or acquired, so a deferred signal never waits on a hung request.
LEASE_REQUEST_TIMEOUT: tuple[int, int] = (10, 60)


class LeaseHoldExceeded(RuntimeError):
    """A step inside the cluster lease would run past the hold budget.

    Raised before the step starts, so nothing was changed by it; the caller
    fails closed.
    """


@dataclass(frozen=True)
class HeldLease:
    holder: str
    max_hold_s: float
    deadline: float  # time.monotonic() at which the hold budget runs out


_LEASE: ContextVar[HeldLease | None] = ContextVar("lakebench_cluster_lease", default=None)


def enter(holder: str, max_hold_s: float) -> tuple[HeldLease, Token[HeldLease | None]]:
    """Mark the lease held in this context (``cluster_lock`` only)."""
    held = HeldLease(holder, max_hold_s, time.monotonic() + max_hold_s)
    return held, _LEASE.set(held)


def leave(token: Token[HeldLease | None]) -> None:
    """Undo ``enter`` (``cluster_lock`` only)."""
    _LEASE.reset(token)


def lease_held() -> bool:
    """True inside ``cluster_lock`` in the holding thread or task."""
    return _LEASE.get() is not None


def lease_hold_remaining() -> float | None:
    """Seconds left in the current hold budget, or None outside the lease."""
    held = _LEASE.get()
    if held is None:
        return None
    return held.deadline - time.monotonic()


def lease_clamp(t: float) -> float:
    """``t`` bounded by the hold budget left (``t`` itself outside the lease)."""
    remaining = lease_hold_remaining()
    if remaining is None:
        return t
    return max(0.0, min(t, remaining))


def request_timeout_kw() -> dict[str, Any]:
    """``{"_request_timeout": ...}`` while the lease is held, else ``{}``."""
    return {"_request_timeout": LEASE_REQUEST_TIMEOUT} if lease_held() else {}
