"""Cluster-wide lakebench lease.

Category 4 (shared mutable state) mutations -- the Spark Operator watch
list, the observability release, the SCC bindings on older OCP -- cannot
be run concurrently without serialising them across every ``lakebench``
process on the cluster. This module implements the serialisation
mechanism: a single ConfigMap named ``lakebench-cluster-lock`` in the
``lakebench-system`` namespace, acquired via optimistic-concurrency
create-or-replace, released in a ``finally`` block, and reclaimable by
``lakebench admin release-lock`` when a holder crashes.

Contract:

- ``acquire_cluster_lock`` creates the ConfigMap (409 on race) or
  replaces an expired one (resourceVersion CAS on race). It refuses to
  bump a lease still within its TTL, and returns the caller-visible
  holder line so the refusal is actionable.
- ``release_cluster_lock`` deletes the ConfigMap only if the current
  ``holder`` string matches the value the acquire returned. A stolen
  lease (someone forced release-lock while we still thought we held it)
  is treated as "already gone" rather than an error.
- ``cluster_lock`` is the context-manager form. Always prefer it over
  raw acquire/release so ``finally`` is not the author's responsibility.

The lease is NOT a distributed lock in the CAP sense: a partitioned
holder that cannot reach the API server can still complete its
mutation locally. What it does prevent is two ``lakebench`` processes
racing the same Helm upgrade or the same SCC install through the
apiserver -- which is what actually broke under parallel UAT.
"""

from __future__ import annotations

import getpass
import logging
import os
import socket
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


LOCK_NAMESPACE = "lakebench-system"
LOCK_CONFIGMAP_NAME = "lakebench-cluster-lock"

# Default acquire timeout. Callers may pass a shorter one for interactive
# commands (``admin doctor``) or a longer one for admin installs.
DEFAULT_ACQUIRE_TIMEOUT_SEC = 30

# Default lease TTL. Long enough to cover a slow ``helm upgrade`` +
# operator rollout; short enough that a crashed holder does not brick
# the cluster for a whole shift.
DEFAULT_TTL_SEC = 3600

_POLL_INITIAL_SEC = 1.0
_WAIT_NOTICE_SEC = 30.0
_POLL_MAX_SEC = 4.0


class ClusterLockError(RuntimeError):
    """Base class for lease errors."""


class ClusterLockHeld(ClusterLockError):
    """The lease is held by another process and its TTL has not expired."""

    def __init__(self, holder: str, acquired_at: str, ttl_seconds: int, expires_at: str):
        self.holder = holder
        self.acquired_at = acquired_at
        self.ttl_seconds = ttl_seconds
        self.expires_at = expires_at
        super().__init__(
            f"cluster lease held by {holder!r} since {acquired_at} "
            f"(ttl {ttl_seconds}s, expires {expires_at}); "
            f"pass --wait to block, or `lakebench admin release-lock "
            f"--expired-only` after the TTL"
        )


class ClusterLockNotHeld(ClusterLockError):
    """Release was called but the lease is not held by this handle."""


@dataclass(frozen=True)
class LeaseHandle:
    """Opaque token proving ownership of the lease.

    ``holder`` is the ``<hostname>@<user>@<git-sha>`` string written at
    acquire time. ``resource_version`` is the ConfigMap resourceVersion
    at acquire time. Release matches on holder and acquired-at, then
    deletes with a resourceVersion precondition taken from that same
    read, so a steal landing between the read and the delete makes the
    delete fail instead of removing the new holder's lease.
    """

    holder: str
    acquired_at: str
    ttl_seconds: int
    resource_version: str


@dataclass(frozen=True)
class LeaseState:
    """A read of the current lease. Read-only diagnostics."""

    holder: str
    acquired_at: str
    ttl_seconds: int
    expires_at_epoch: float
    resource_version: str
    uid: str | None = None

    def is_expired(self, now_epoch: float | None = None) -> bool:
        return (now_epoch if now_epoch is not None else time.time()) >= self.expires_at_epoch


# ---------------------------------------------------------------------------
# Holder identity
# ---------------------------------------------------------------------------


def _git_sha_short() -> str:
    """Best-effort short git SHA of the current lakebench checkout."""
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return "no-git"
    if r.returncode != 0:
        return "no-git"
    return r.stdout.strip() or "no-git"


def build_holder_id() -> str:
    """Return ``<hostname>@<user>@<git-sha>`` for the lease's holder field.

    All three components are best-effort so the holder line is never
    empty. This is a diagnostic string, not a security identifier.
    """
    try:
        host = socket.gethostname() or "unknown-host"
    except Exception:  # noqa: BLE001
        host = "unknown-host"
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001
        user = os.environ.get("USER", "unknown-user")
    return f"{host}@{user}@{_git_sha_short()}"


# ---------------------------------------------------------------------------
# Lease read / write
# ---------------------------------------------------------------------------


def _ensure_lock_namespace(core_v1: Any) -> None:
    """Create the ``lakebench-system`` namespace if it doesn't exist.

    A cluster with no prior lakebench admin activity has no such
    namespace. We create it lazily on first acquire rather than
    forcing a mandatory ``admin bootstrap`` step. The namespace holds
    the lease and nothing else.
    """
    try:
        from kubernetes.client.exceptions import ApiException
        from kubernetes.client.models import V1Namespace, V1ObjectMeta
    except ImportError as e:  # pragma: no cover
        raise ClusterLockError(f"kubernetes client not installed: {e}") from e

    try:
        core_v1.read_namespace(LOCK_NAMESPACE)
        return
    except ApiException as e:
        if e.status != 404:
            raise ClusterLockError(f"cannot read {LOCK_NAMESPACE}: {e}") from e

    body = V1Namespace(
        metadata=V1ObjectMeta(
            name=LOCK_NAMESPACE,
            labels={"lakebench.deployment/system": "cluster-lock"},
        )
    )
    try:
        core_v1.create_namespace(body)
    except ApiException as e:
        # Race with another process bootstrapping the same namespace is
        # benign; anything else is a real error.
        if e.status not in (409,):
            raise ClusterLockError(f"cannot create {LOCK_NAMESPACE}: {e}") from e


def _parse_iso8601(s: str) -> float:
    """Return epoch seconds. Robust to a bad string (returns 0 -> expired)."""
    try:
        # Python 3.11 handles both "Z" and "+00:00" via fromisoformat.
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:  # noqa: BLE001
        logger.warning("cluster_lock: unparseable acquired-at %r; treating as expired", s)
        return 0.0


def read_cluster_lock(core_v1: Any) -> LeaseState | None:
    """Return the current lease state, or None if not held."""
    try:
        from kubernetes.client.exceptions import ApiException
    except ImportError as e:  # pragma: no cover
        raise ClusterLockError(f"kubernetes client not installed: {e}") from e

    try:
        cm = core_v1.read_namespaced_config_map(LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            return None
        raise ClusterLockError(f"cannot read lease: {e}") from e

    data = cm.data or {}
    try:
        ttl = int(data.get("ttl-seconds", "0"))
    except ValueError:
        ttl = 0
    acquired_at = data.get("acquired-at", "")
    holder = data.get("holder", "")
    return LeaseState(
        holder=holder,
        acquired_at=acquired_at,
        ttl_seconds=ttl,
        expires_at_epoch=_parse_iso8601(acquired_at) + ttl,
        resource_version=cm.metadata.resource_version,
        uid=getattr(cm.metadata, "uid", None),
    )


def _delete_if_unchanged(core_v1: Any, resource_version: str | None, uid: str | None) -> None:
    """Delete the lease ConfigMap only if it is still the object we read.

    The API server rejects the delete with 409 when the preconditions no
    longer match, which is how a steal (replace bumps resourceVersion) or
    a delete-and-recreate (new uid) between our read and our delete is
    detected. Callers handle the ApiException.
    """
    from kubernetes.client.models import V1DeleteOptions, V1Preconditions

    if not resource_version:
        # Without a resourceVersion there is nothing to make the delete
        # conditional on; refusing is safer than deleting blind.
        raise ClusterLockError("lease read carried no resourceVersion; refusing blind delete")
    body = V1DeleteOptions(
        preconditions=V1Preconditions(resource_version=resource_version, uid=uid)
    )
    core_v1.delete_namespaced_config_map(LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE, body=body)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _build_configmap(holder: str, acquired_at: str, ttl_seconds: int) -> Any:
    from kubernetes.client.models import V1ConfigMap, V1ObjectMeta

    return V1ConfigMap(
        metadata=V1ObjectMeta(
            name=LOCK_CONFIGMAP_NAME,
            namespace=LOCK_NAMESPACE,
            labels={"lakebench.deployment/system": "cluster-lock"},
        ),
        data={
            "holder": holder,
            "acquired-at": acquired_at,
            "ttl-seconds": str(ttl_seconds),
        },
    )


def _try_acquire_once(
    core_v1: Any,
    holder: str,
    ttl_seconds: int,
) -> LeaseHandle | LeaseState:
    """Single acquire attempt.

    Returns a ``LeaseHandle`` on success, or a ``LeaseState`` describing
    the current holder if the lease is held with unexpired TTL.

    Raises on transport errors so the caller's poll loop stops on
    non-retryable conditions (missing namespace after we created it,
    RBAC denial, etc.).
    """
    from kubernetes.client.exceptions import ApiException

    now_epoch = time.time()
    state = read_cluster_lock(core_v1)

    if state is None:
        # No lease. Try create.
        acquired_at = _now_iso()
        body = _build_configmap(holder, acquired_at, ttl_seconds)
        try:
            created = core_v1.create_namespaced_config_map(LOCK_NAMESPACE, body)
        except ApiException as e:
            if e.status == 409:
                # Another process created it in the gap. Fall through by
                # re-reading; return whatever state is now visible.
                s2 = read_cluster_lock(core_v1)
                if s2 is None:
                    # Deleted again in the gap. Signal caller to retry.
                    raise ClusterLockError("lease vanished mid-create; retry") from e
                return s2
            raise ClusterLockError(f"cannot create lease: {e}") from e
        return LeaseHandle(
            holder=holder,
            acquired_at=acquired_at,
            ttl_seconds=ttl_seconds,
            resource_version=created.metadata.resource_version,
        )

    if not state.is_expired(now_epoch):
        return state

    # Expired. Steal via replace() keyed on resourceVersion. If someone
    # else steals it first, the replace 409s and we return their state.
    acquired_at = _now_iso()
    body = _build_configmap(holder, acquired_at, ttl_seconds)
    body.metadata.resource_version = state.resource_version
    try:
        replaced = core_v1.replace_namespaced_config_map(LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE, body)
    except ApiException as e:
        if e.status == 409:
            s2 = read_cluster_lock(core_v1)
            if s2 is None:
                raise ClusterLockError("lease vanished mid-steal; retry") from e
            return s2
        raise ClusterLockError(f"cannot steal expired lease: {e}") from e
    logger.info(
        "cluster_lock: stole expired lease from %r (was acquired %s, ttl %ss)",
        state.holder,
        state.acquired_at,
        state.ttl_seconds,
    )
    return LeaseHandle(
        holder=holder,
        acquired_at=acquired_at,
        ttl_seconds=ttl_seconds,
        resource_version=replaced.metadata.resource_version,
    )


def acquire_cluster_lock(
    core_v1: Any,
    *,
    ttl_seconds: int = DEFAULT_TTL_SEC,
    timeout: float = DEFAULT_ACQUIRE_TIMEOUT_SEC,
    holder: str | None = None,
) -> LeaseHandle:
    """Acquire the cluster lease or raise.

    Polls with backoff up to ``timeout`` seconds. If the lease remains
    held by an unexpired holder at the end of the window,
    ``ClusterLockHeld`` is raised carrying the holder identity so the
    caller can present it verbatim.
    """
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")
    _ensure_lock_namespace(core_v1)

    holder_id = holder or build_holder_id()
    deadline = time.time() + max(0.0, timeout)
    delay = _POLL_INITIAL_SEC
    last_state: LeaseState | None = None
    last_notice = time.time()

    while True:
        try:
            outcome = _try_acquire_once(core_v1, holder_id, ttl_seconds)
        except ClusterLockError as e:
            # ADR-F7: the internal "lease vanished mid-{create,steal}"
            # signals are benign contention: the winning holder released
            # (or the lease was force-released) between our conflict and
            # our re-read. Retry within the timeout budget rather than
            # bubbling a scary error to the CLI.
            msg = str(e).lower()
            if "vanished" in msg:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise
                time.sleep(min(_POLL_INITIAL_SEC, remaining))
                continue
            raise
        if isinstance(outcome, LeaseHandle):
            return outcome
        last_state = outcome
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        # Waits can last minutes behind a helm upgrade; say who we wait for
        # instead of going silent.
        now = time.time()
        if now - last_notice >= _WAIT_NOTICE_SEC:
            last_notice = now
            logger.warning(
                "cluster_lock: waiting for lease held by %s (up to %.0fs more)",
                getattr(outcome, "holder", "?"),
                remaining,
            )
        time.sleep(min(delay, remaining))
        delay = min(delay * 1.5, _POLL_MAX_SEC)

    assert last_state is not None
    expires_at_iso = datetime.fromtimestamp(last_state.expires_at_epoch, tz=timezone.utc).isoformat(
        timespec="seconds"
    )
    raise ClusterLockHeld(
        holder=last_state.holder,
        acquired_at=last_state.acquired_at,
        ttl_seconds=last_state.ttl_seconds,
        expires_at=expires_at_iso,
    )


_DELETE_ATTEMPTS = 3


def release_cluster_lock(core_v1: Any, handle: LeaseHandle) -> None:
    """Release the lease held by ``handle``.

    Idempotent: a lease that has been stolen or admin-released is
    treated as already-gone and returns cleanly. Any other error
    (RBAC, transport) raises.

    The delete is conditional on the resourceVersion and uid of the read
    that confirmed ownership. A 409 means the object changed in between.
    That is either a steal (holder or acquired-at now differ, so we
    leave it) or a write that kept our data, such as a label or
    annotation, in which case we re-read and try again rather than leak
    our own lease until its TTL.
    """
    from kubernetes.client.exceptions import ApiException

    for _ in range(_DELETE_ATTEMPTS):
        try:
            cm = core_v1.read_namespaced_config_map(LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE)
        except ApiException as e:
            if e.status == 404:
                return
            raise ClusterLockError(f"cannot read lease for release: {e}") from e

        data = cm.data or {}
        if data.get("holder") != handle.holder or data.get("acquired-at") != handle.acquired_at:
            logger.warning(
                "cluster_lock: release skipped; lease no longer ours "
                "(current holder=%r acquired_at=%r)",
                data.get("holder"),
                data.get("acquired-at"),
            )
            return

        try:
            _delete_if_unchanged(
                core_v1, cm.metadata.resource_version, getattr(cm.metadata, "uid", None)
            )
            return
        except ApiException as e:
            if e.status == 404:
                return
            if e.status != 409:
                raise ClusterLockError(f"cannot delete lease: {e}") from e
            logger.info(
                "cluster_lock: lease changed after it was read (resourceVersion %s); re-checking",
                cm.metadata.resource_version,
            )
    raise ClusterLockError(
        f"lease kept changing under release after {_DELETE_ATTEMPTS} attempts; "
        "it may still be held by this process until its TTL"
    )


def _held_error(state: LeaseState) -> ClusterLockHeld:
    return ClusterLockHeld(
        holder=state.holder,
        acquired_at=state.acquired_at,
        ttl_seconds=state.ttl_seconds,
        expires_at=datetime.fromtimestamp(state.expires_at_epoch, tz=timezone.utc).isoformat(
            timespec="seconds"
        ),
    )


def force_release_cluster_lock(core_v1: Any, *, expired_only: bool) -> LeaseState | None:
    """Admin recovery path. Returns the pre-delete state, or None if absent.

    ``expired_only=True`` refuses to delete a lease whose TTL has not
    yet elapsed, and deletes only the exact object it judged expired
    (resourceVersion and uid preconditions). A holder that stole the
    lease in the meantime has a fresh TTL and keeps it. That is the
    default for the ``admin release-lock`` command; callers who
    genuinely want to break a live lease must opt in with
    ``expired_only=False``, which deletes unconditionally.
    """
    from kubernetes.client.exceptions import ApiException

    state = read_cluster_lock(core_v1)
    if state is None:
        return None

    if not expired_only:
        try:
            core_v1.delete_namespaced_config_map(LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE)
        except ApiException as e:
            if e.status == 404:
                return state
            raise ClusterLockError(f"cannot force-delete lease: {e}") from e
        return state

    for _ in range(_DELETE_ATTEMPTS):
        if not state.is_expired():
            raise _held_error(state)
        try:
            _delete_if_unchanged(core_v1, state.resource_version, state.uid)
            return state
        except ApiException as e:
            if e.status == 404:
                return state
            if e.status != 409:
                raise ClusterLockError(f"cannot force-delete lease: {e}") from e
        # Changed since we judged it. Re-judge the current object: a steal
        # is live and raises above; a write that kept it expired is retried.
        current = read_cluster_lock(core_v1)
        if current is None:
            return state
        state = current
    if not state.is_expired():
        raise _held_error(state)
    raise ClusterLockError(
        f"lease kept changing after {_DELETE_ATTEMPTS} attempts; re-run release-lock"
    )


@contextmanager
def cluster_lock(
    core_v1: Any,
    *,
    ttl_seconds: int = DEFAULT_TTL_SEC,
    timeout: float = DEFAULT_ACQUIRE_TIMEOUT_SEC,
    holder: str | None = None,
) -> Iterator[LeaseHandle]:
    """Acquire on enter, release on exit (even under exception).

    The context manager is the preferred acquisition path -- it makes
    the release non-optional and localises the ``finally`` in one
    place, so future callers cannot forget it.
    """
    handle = acquire_cluster_lock(
        core_v1,
        ttl_seconds=ttl_seconds,
        timeout=timeout,
        holder=holder,
    )
    try:
        yield handle
    finally:
        try:
            release_cluster_lock(core_v1, handle)
        except Exception as e:  # noqa: BLE001
            # Log but do not shadow whatever the body raised. Transport
            # errors (urllib3 MaxRetryError and friends) are not
            # ApiException and would otherwise replace the body's error.
            logger.warning("cluster_lock: release failed on exit: %s", e)
