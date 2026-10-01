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

While the lease is held (cluster-safety 2, DESIGN ch01 3.6):

- ``lease_held()`` is true in the holding context, and
  ``lease_hold_remaining()``/``lease_clamp()`` give the hold budget
  (``max_hold_s``: ``LEASE_MAX_HOLD_S`` for deploy, destroy and run,
  ``ADMIN_MAX_HOLD_S`` for admin verbs). The state lives in
  ``lakebench.k8s.lease_state`` so the k8s helpers can read it.
- On the main thread, SIGINT, SIGTERM and SIGHUP are deferred
  (``_SignalDeferral``): the first is recorded and announced, the shared
  change finishes, the lease is released (signals during the release are
  only recorded), and then the first signal is re-delivered to the
  handler that was installed before the lease. A third signal raises
  ``LeaseAbort`` at once; the lease is still released.
- ``lakebench.k8s._pinned`` runs kubectl, helm and oc in a new session
  with a timeout from the hold budget, refuses a helm mutation that the
  budget cannot cover (``LeaseHoldExceeded``), and stops a child with
  SIGTERM before SIGKILL, so a helm upgrade is not left
  ``pending-upgrade`` by a Ctrl-C or a timeout.
- Every Kubernetes API call this module makes carries
  ``_request_timeout=LEASE_REQUEST_TIMEOUT``, so a deferred signal never
  waits on a request that does not return; a write whose reply timed out
  but which landed is kept (``_adopt_if_written``).

Not covered: SIGKILL and a lost host. The TTL reclaims the lease. An
interrupt inside the acquire, after the lease was written, releases it:
the holder id is unique to each acquire (LB-178).
"""

from __future__ import annotations

import getpass
import logging
import math
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from lakebench.k8s import lease_state
from lakebench.k8s.lease_state import (
    LEASE_REQUEST_TIMEOUT,
    LeaseHoldExceeded,
    lease_clamp,
    lease_held,
    lease_hold_remaining,
)

__all__ = [
    "ADMIN_MAX_HOLD_S",
    "LEASE_MAX_HOLD_S",
    "LEASE_REQUEST_TIMEOUT",
    "ClusterLockError",
    "ClusterLockHeld",
    "LeaseAbort",
    "LeaseHoldExceeded",
    "cluster_lock",
    "lease_clamp",
    "lease_held",
    "lease_hold_remaining",
]

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

# How long one holder may keep the lease (DESIGN ch01 3.7). The watch-list
# phases (helm upgrade, rollout waits, restart, the SAF-4 pod poll, the
# namespace delete) fit in LEASE_MAX_HOLD_S. The admin verbs
# (install-spark-operator with --wait, repair-operator's per-namespace
# restarts, migrate-deployment, reclaim-bucket) get ADMIN_MAX_HOLD_S. Both
# stay under the TTL so a crashed holder is still reclaimed. Subprocesses
# under the lease are bounded by the budget today (lakebench.k8s._pinned);
# SD-12 moves the remaining waits and sleeps onto lease_clamp.
LEASE_MAX_HOLD_S = 750
ADMIN_MAX_HOLD_S = 1800

# Signals deferred while the lease is held. SIGHUP is absent on Windows.
_DEFERRED_SIGNALS: tuple[int, ...] = tuple(
    s for s in (getattr(signal, n, None) for n in ("SIGINT", "SIGTERM", "SIGHUP")) if s is not None
)


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

    ``holder`` is the ``<hostname>@<user>@<git-sha>#<pid>-<8 hex>`` string
    written at acquire time (``build_holder_id``). ``resource_version`` is
    the ConfigMap resourceVersion at acquire time. ``write_nonce`` is the
    random ``write-nonce`` of this acquire's write. Release matches on
    holder, acquired-at and, when set, the write nonce, then deletes with a
    resourceVersion precondition taken from that same read, so a steal
    landing between the read and the delete makes the delete fail instead
    of removing the new holder's lease.
    """

    holder: str
    acquired_at: str
    ttl_seconds: int
    resource_version: str
    write_nonce: str = ""


@dataclass(frozen=True)
class LeaseState:
    """A read of the current lease. Read-only diagnostics."""

    holder: str
    acquired_at: str
    ttl_seconds: int
    expires_at_epoch: float
    resource_version: str
    uid: str | None = None
    # Random per write; tells this process's write from another's with the
    # same holder string and second (``_adopt_if_written``).
    write_nonce: str = ""

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
    """Return ``<hostname>@<user>@<git-sha>#<pid>-<8 hex>`` for the holder field.

    The first three components are best-effort so the holder line is never
    empty. The suffix (LB-178) names the process for ``admin status`` and
    makes the id unique to one acquire: two runs from the same tree on the
    same host, in the same second, no longer write the same holder, and
    ``cluster_lock`` can tell its own lease from anyone else's after an
    interrupt inside the acquire. A diagnostic string, not a security
    identifier.
    """
    try:
        host = socket.gethostname() or "unknown-host"
    except Exception:  # noqa: BLE001
        host = "unknown-host"
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001
        user = os.environ.get("USER", "unknown-user")
    return f"{host}@{user}@{_git_sha_short()}#{os.getpid()}-{uuid.uuid4().hex[:8]}"


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
        core_v1.read_namespace(LOCK_NAMESPACE, _request_timeout=LEASE_REQUEST_TIMEOUT)
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
        core_v1.create_namespace(body, _request_timeout=LEASE_REQUEST_TIMEOUT)
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
        cm = core_v1.read_namespaced_config_map(
            LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE, _request_timeout=LEASE_REQUEST_TIMEOUT
        )
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
        write_nonce=data.get("write-nonce", ""),
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
    core_v1.delete_namespaced_config_map(
        LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE, body=body, _request_timeout=LEASE_REQUEST_TIMEOUT
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _build_configmap(holder: str, acquired_at: str, ttl_seconds: int, write_nonce: str) -> Any:
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
            "write-nonce": write_nonce,
        },
    )


def _adopt_if_written(
    core_v1: Any,
    holder: str,
    acquired_at: str,
    ttl_seconds: int,
    write_nonce: str,
    error: Exception,
) -> LeaseHandle:
    """After a transport error on our create or replace, keep a lease we did write.

    The request timeout (``LEASE_REQUEST_TIMEOUT``) can fire after the API
    server committed the write. Without this the lease would stay held,
    by us, until its TTL. Only the random ``write-nonce`` of this write
    identifies it: the holder string and the acquired-at second can be
    identical for another lakebench run from the same tree on this host.
    Anything else re-raises the original error.
    """
    try:
        state = read_cluster_lock(core_v1)
    except Exception:  # noqa: BLE001
        raise error from None
    if (
        state is not None
        and state.write_nonce == write_nonce
        and state.holder == holder
        and state.acquired_at == acquired_at
    ):
        logger.warning("cluster_lock: the lease write timed out but landed; keeping it")
        return LeaseHandle(
            holder=holder,
            acquired_at=acquired_at,
            ttl_seconds=ttl_seconds,
            resource_version=state.resource_version,
            write_nonce=write_nonce,
        )
    raise error


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
        nonce = uuid.uuid4().hex
        body = _build_configmap(holder, acquired_at, ttl_seconds, nonce)
        try:
            created = core_v1.create_namespaced_config_map(
                LOCK_NAMESPACE, body, _request_timeout=LEASE_REQUEST_TIMEOUT
            )
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
        except Exception as e:  # noqa: BLE001 -- transport: the write may have landed
            return _adopt_if_written(core_v1, holder, acquired_at, ttl_seconds, nonce, e)
        return LeaseHandle(
            holder=holder,
            acquired_at=acquired_at,
            ttl_seconds=ttl_seconds,
            resource_version=created.metadata.resource_version,
            write_nonce=nonce,
        )

    if not state.is_expired(now_epoch):
        return state

    # Expired. Steal via replace() keyed on resourceVersion. If someone
    # else steals it first, the replace 409s and we return their state.
    acquired_at = _now_iso()
    nonce = uuid.uuid4().hex
    body = _build_configmap(holder, acquired_at, ttl_seconds, nonce)
    body.metadata.resource_version = state.resource_version
    try:
        replaced = core_v1.replace_namespaced_config_map(
            LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE, body, _request_timeout=LEASE_REQUEST_TIMEOUT
        )
    except ApiException as e:
        if e.status == 409:
            s2 = read_cluster_lock(core_v1)
            if s2 is None:
                raise ClusterLockError("lease vanished mid-steal; retry") from e
            return s2
        raise ClusterLockError(f"cannot steal expired lease: {e}") from e
    except Exception as e:  # noqa: BLE001 -- transport: the write may have landed
        return _adopt_if_written(core_v1, holder, acquired_at, ttl_seconds, nonce, e)
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
        write_nonce=nonce,
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
            cm = core_v1.read_namespaced_config_map(
                LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE, _request_timeout=LEASE_REQUEST_TIMEOUT
            )
        except ApiException as e:
            if e.status == 404:
                return
            raise ClusterLockError(f"cannot read lease for release: {e}") from e

        data = cm.data or {}
        if (
            data.get("holder") != handle.holder
            or data.get("acquired-at") != handle.acquired_at
            or (handle.write_nonce and data.get("write-nonce") != handle.write_nonce)
        ):
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
            core_v1.delete_namespaced_config_map(
                LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE, _request_timeout=LEASE_REQUEST_TIMEOUT
            )
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


# ---------------------------------------------------------------------------
# Signal deferral while the lease is held
# ---------------------------------------------------------------------------


class LeaseAbort(KeyboardInterrupt):
    """The third interrupt inside the lease: stop now, after the release.

    ``signum`` is the signal that aborted, for an interrupt record (CD-16).
    """

    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


_Handler = Callable[[int, Any], Any] | int | None


@dataclass
class _SignalDeferral:
    """Holds back SIGINT/SIGTERM/SIGHUP while the lease is held (main thread).

    The first signal is announced and held back; the second repeats the
    budget left; the third raises ``LeaseAbort`` at once. While the lease is
    being released every signal is only recorded, so no interrupt can stop
    the release half way and leave the lease held until its TTL. After the
    release the saved handlers are restored and the first recorded signal
    is sent again, so the handler that was installed before the lease
    (Python's ``KeyboardInterrupt`` for SIGINT, or a command's own SIGTERM
    handler) runs then. A signal its process ignores (``SIG_IGN``, as under
    ``nohup``) stays ignored.
    """

    held: lease_state.HeldLease
    saved: dict[int, _Handler] = field(default_factory=dict)
    received: list[int] = field(default_factory=list)
    aborted: bool = False

    def install(self) -> bool:
        if threading.current_thread() is not threading.main_thread():
            logger.warning(
                "cluster_lock: held off the main thread; an interrupt can stop the shared change"
            )
            return False
        try:
            for sig in _DEFERRED_SIGNALS:
                handler = signal.getsignal(sig)
                if handler == signal.SIG_IGN:
                    continue
                self.saved[sig] = handler
                signal.signal(sig, self._on_signal)
        except (ValueError, OSError) as e:  # not the main interpreter, or unsupported
            logger.warning("cluster_lock: cannot defer signals: %s", e)
            self.restore()
            return False
        return True

    def quiet(self) -> None:
        """Record, never raise, while the lease is released."""
        for sig in self.saved:
            try:
                signal.signal(sig, self._record_only)
            except (ValueError, OSError) as e:
                logger.debug("cluster_lock: could not quiet %s: %s", sig, e)

    def restore(self) -> None:
        for sig, handler in self.saved.items():
            try:
                signal.signal(sig, handler if handler is not None else signal.SIG_DFL)
            except (ValueError, OSError, TypeError) as e:
                logger.debug("cluster_lock: could not restore handler for %s: %s", sig, e)

    def _left(self) -> int:
        return max(0, math.ceil(self.held.deadline - time.monotonic()))

    def _record_only(self, signum: int, _frame: Any) -> None:
        self.received.append(signum)

    def _on_signal(self, signum: int, _frame: Any) -> None:
        self.received.append(signum)
        n = len(self.received)
        if n == 1:
            _say(
                "interrupt received while holding the cluster lease; finishing the "
                f"shared change (hold budget {self._left()} s left), then stopping. "
                "Interrupt twice more to abort now"
            )
        elif n == 2:
            _say(
                "still finishing the shared change under the cluster lease (hold "
                f"budget {self._left()} s left). Interrupt once more to abort now"
            )
        else:
            self.aborted = True
            self.quiet()
            _say(
                "aborting inside the cluster lease; the lease is released first. If a "
                "helm upgrade was running, check `helm history` for the release and "
                "roll back a pending-upgrade revision, then run "
                "`lakebench admin repair-operator`"
            )
            raise LeaseAbort(signum)

    def redeliver(self) -> None:
        if self.received and not self.aborted:
            signal.raise_signal(self.received[0])


def _say(message: str) -> None:
    try:
        sys.stderr.write(message + "\n")
        sys.stderr.flush()
    except Exception:  # noqa: BLE001 -- a closed stderr must not break the lease
        pass


@contextmanager
def cluster_lock(
    core_v1: Any,
    *,
    ttl_seconds: int = DEFAULT_TTL_SEC,
    timeout: float = DEFAULT_ACQUIRE_TIMEOUT_SEC,
    holder: str | None = None,
    max_hold_s: float = LEASE_MAX_HOLD_S,
) -> Iterator[LeaseHandle]:
    """Acquire on enter, release on exit (even under exception).

    The context manager is the preferred acquisition path -- it makes
    the release non-optional and localises the ``finally`` in one
    place, so future callers cannot forget it. While the body runs,
    ``lease_held()`` is true, the hold budget is ``max_hold_s``
    (``lease_hold_remaining``, ``lease_clamp``), and on the main thread
    SIGINT, SIGTERM and SIGHUP are deferred until the lease is released
    (``_SignalDeferral``).
    """
    if max_hold_s <= 0:
        raise ValueError("max_hold_s must be positive")
    # The hold budget never outlives the lease itself.
    max_hold_s = min(max_hold_s, ttl_seconds)
    holder_id = holder or build_holder_id()
    handle: LeaseHandle | None = None
    token = None
    deferral: _SignalDeferral | None = None
    try:
        try:
            handle = acquire_cluster_lock(
                core_v1,
                ttl_seconds=ttl_seconds,
                timeout=timeout,
                holder=holder_id,
            )
        except BaseException as e:
            # An interrupt can land after our write committed and before the
            # handle reaches this frame. The holder id is unique to this
            # acquire (LB-178), so a lease carrying it is ours to release.
            if holder is None and not isinstance(e, Exception):
                _release_interrupted_acquire(core_v1, holder_id)
            raise
        held, token = lease_state.enter(handle.holder, max_hold_s)
        deferral = _SignalDeferral(held)
        deferral.install()
        yield handle
    finally:
        if handle is not None:
            if deferral is not None:
                deferral.quiet()
            try:
                release_cluster_lock(core_v1, handle)
            except Exception as e:  # noqa: BLE001
                # Log but do not shadow whatever the body raised. Transport
                # errors (urllib3 MaxRetryError and friends) are not
                # ApiException and would otherwise replace the body's error.
                logger.warning("cluster_lock: release failed on exit: %s", e)
            finally:
                try:
                    if token is not None:
                        lease_state.leave(token)
                finally:
                    if deferral is not None:
                        deferral.restore()
                        # After the release: the saved handler now sees the signal.
                        deferral.redeliver()


def _release_interrupted_acquire(core_v1: Any, holder_id: str) -> None:
    """Delete the lease if this acquire wrote it before it was interrupted.

    Only a lease whose holder equals ``holder_id``, which ``build_holder_id``
    makes unique to one acquire, is deleted, with the resourceVersion and
    uid of the read as preconditions. Best effort: an error is logged and
    the TTL reclaims the lease.
    """
    try:
        state = read_cluster_lock(core_v1)
        if state is not None and state.holder == holder_id:
            _delete_if_unchanged(core_v1, state.resource_version, state.uid)
            logger.warning("cluster_lock: interrupted while acquiring; released the lease")
    except Exception as e:  # noqa: BLE001
        logger.warning("cluster_lock: interrupted while acquiring; lease left to its TTL: %s", e)
