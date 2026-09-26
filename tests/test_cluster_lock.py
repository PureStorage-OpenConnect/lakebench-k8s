"""Unit tests for the cluster-wide lakebench lease.

The lease guards Category 4 (shared mutable state) mutations under
concurrent lakebench invocations. See
``dev-artifacts/DESIGN-namespace-isolation.md``.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from kubernetes.client.exceptions import ApiException
from kubernetes.client.models import V1ConfigMap, V1ObjectMeta

from lakebench.deploy.cluster_lock import (
    LOCK_CONFIGMAP_NAME,
    LOCK_NAMESPACE,
    ClusterLockError,
    ClusterLockHeld,
    LeaseHandle,
    LeaseState,
    _now_iso,
    _parse_iso8601,
    acquire_cluster_lock,
    build_holder_id,
    cluster_lock,
    force_release_cluster_lock,
    read_cluster_lock,
    release_cluster_lock,
)


def _cm(holder: str, acquired_at: str, ttl: int, rv: str = "1") -> V1ConfigMap:
    return V1ConfigMap(
        metadata=V1ObjectMeta(
            name=LOCK_CONFIGMAP_NAME,
            namespace=LOCK_NAMESPACE,
            resource_version=rv,
        ),
        data={
            "holder": holder,
            "acquired-at": acquired_at,
            "ttl-seconds": str(ttl),
        },
    )


def _api_exc(status: int) -> ApiException:
    e = ApiException(status=status, reason="test")
    return e


class TestHolderId:
    def test_build_holder_id_returns_three_segments(self):
        h = build_holder_id()
        assert h.count("@") == 2


class TestReadClusterLock:
    def test_returns_none_when_absent(self):
        core = MagicMock()
        core.read_namespaced_config_map.side_effect = _api_exc(404)
        assert read_cluster_lock(core) is None

    def test_parses_valid_configmap(self):
        core = MagicMock()
        core.read_namespaced_config_map.return_value = _cm(
            "host@user@abc", "2026-09-21T12:00:00+00:00", 60, rv="42"
        )
        state = read_cluster_lock(core)
        assert isinstance(state, LeaseState)
        assert state.holder == "host@user@abc"
        assert state.ttl_seconds == 60
        assert state.resource_version == "42"

    def test_raises_on_non_404(self):
        core = MagicMock()
        core.read_namespaced_config_map.side_effect = _api_exc(500)
        with pytest.raises(ClusterLockError):
            read_cluster_lock(core)


class TestAcquireClusterLock:
    def test_creates_when_absent(self):
        core = MagicMock()
        core.read_namespace.return_value = MagicMock()
        # First read -> 404, then create -> new configmap.
        core.read_namespaced_config_map.side_effect = _api_exc(404)
        created = _cm("host@user@abc", _now_iso(), 60, rv="1")
        core.create_namespaced_config_map.return_value = created

        handle = acquire_cluster_lock(core, ttl_seconds=60, timeout=1, holder="host@user@abc")
        assert isinstance(handle, LeaseHandle)
        assert handle.holder == "host@user@abc"
        core.create_namespaced_config_map.assert_called_once()

    def test_refuses_when_held_within_ttl(self):
        core = MagicMock()
        core.read_namespace.return_value = MagicMock()
        core.read_namespaced_config_map.return_value = _cm(
            "other-host@other-user@xyz", _now_iso(), 3600, rv="1"
        )

        with pytest.raises(ClusterLockHeld) as ei:
            acquire_cluster_lock(core, ttl_seconds=60, timeout=0.5, holder="me@here@abc")
        assert ei.value.holder == "other-host@other-user@xyz"

    def test_steals_expired_lease(self):
        core = MagicMock()
        core.read_namespace.return_value = MagicMock()
        # Expired: acquired 2 hours ago with 60s TTL.
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        expired = _cm("crashed-host@ghost@000", past, 60, rv="7")
        core.read_namespaced_config_map.return_value = expired
        core.replace_namespaced_config_map.return_value = _cm("me@here@abc", _now_iso(), 60, rv="8")

        handle = acquire_cluster_lock(core, ttl_seconds=60, timeout=1, holder="me@here@abc")
        assert handle.resource_version == "8"

    def test_ttl_must_be_positive(self):
        core = MagicMock()
        with pytest.raises(ValueError):
            acquire_cluster_lock(core, ttl_seconds=0, timeout=1, holder="x@y@z")


class TestReleaseClusterLock:
    def test_release_when_still_ours(self):
        core = MagicMock()
        core.read_namespaced_config_map.return_value = _cm(
            "me@here@abc", "2026-09-21T12:00:00+00:00", 60, rv="1"
        )
        handle = LeaseHandle(
            holder="me@here@abc",
            acquired_at="2026-09-21T12:00:00+00:00",
            ttl_seconds=60,
            resource_version="1",
        )
        release_cluster_lock(core, handle)
        core.delete_namespaced_config_map.assert_called_once()
        args, kwargs = core.delete_namespaced_config_map.call_args
        assert args == (LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE)
        assert kwargs["body"].preconditions.resource_version == "1"

    def test_release_skipped_when_stolen(self):
        """A lease admin-released mid-run must not error at release time."""
        core = MagicMock()
        core.read_namespaced_config_map.return_value = _cm(
            "someone-else@host@xyz", "2026-09-21T14:00:00+00:00", 60, rv="9"
        )
        handle = LeaseHandle(
            holder="me@here@abc",
            acquired_at="2026-09-21T12:00:00+00:00",
            ttl_seconds=60,
            resource_version="1",
        )
        release_cluster_lock(core, handle)  # no exception
        core.delete_namespaced_config_map.assert_not_called()

    def test_release_when_already_gone(self):
        core = MagicMock()
        core.read_namespaced_config_map.side_effect = _api_exc(404)
        handle = LeaseHandle(
            holder="me@here@abc",
            acquired_at="2026-09-21T12:00:00+00:00",
            ttl_seconds=60,
            resource_version="1",
        )
        release_cluster_lock(core, handle)  # no exception


class _FakeLockApi:
    """The lock ConfigMap as the API server keeps it: resourceVersion bumps
    on every write and a delete whose preconditions no longer match is
    refused with 409. ``after_read`` runs once, right after the next read
    returns, to land a competing write inside the read-then-delete window.
    """

    def __init__(self) -> None:
        self.cm: V1ConfigMap | None = None
        self._rv = 0
        self.after_read = None

    def _stamp(self, body: V1ConfigMap, uid: str) -> V1ConfigMap:
        self._rv += 1
        body.metadata.resource_version = str(self._rv)
        body.metadata.uid = uid
        self.cm = body
        return body

    def put(self, holder: str, acquired_at: str, ttl: int, uid: str = "uid-1") -> V1ConfigMap:
        return self._stamp(_cm(holder, acquired_at, ttl), uid)

    def steal(self, holder: str) -> None:
        assert self.cm is not None
        self._stamp(_cm(holder, _now_iso(), 3600), self.cm.metadata.uid)

    def read_namespace(self, name):
        return MagicMock()

    def create_namespaced_config_map(self, namespace, body):
        if self.cm is not None:
            raise _api_exc(409)
        return self._stamp(body, "uid-1")

    def read_namespaced_config_map(self, name, namespace):
        if self.cm is None:
            raise _api_exc(404)
        snap = _cm(
            self.cm.data["holder"],
            self.cm.data["acquired-at"],
            int(self.cm.data["ttl-seconds"]),
            rv=self.cm.metadata.resource_version,
        )
        snap.metadata.uid = self.cm.metadata.uid
        hook, self.after_read = self.after_read, None
        if hook:
            hook()
        return snap

    def delete_namespaced_config_map(self, name, namespace, body=None):
        if self.cm is None:
            raise _api_exc(404)
        pre = getattr(body, "preconditions", None)
        if pre is not None:
            if pre.resource_version and pre.resource_version != self.cm.metadata.resource_version:
                raise _api_exc(409)
            if pre.uid and pre.uid != self.cm.metadata.uid:
                raise _api_exc(409)
        self.cm = None


class TestStealBetweenReadAndDelete:
    """A steal landing after the releaser's read must survive the delete."""

    def test_release_leaves_the_new_holders_lease(self):
        api = _FakeLockApi()
        cm = api.put("me@here@abc", "2026-09-21T12:00:00+00:00", 60)
        handle = LeaseHandle(
            holder="me@here@abc",
            acquired_at="2026-09-21T12:00:00+00:00",
            ttl_seconds=60,
            resource_version=cm.metadata.resource_version,
        )
        api.after_read = lambda: api.steal("thief@host@xyz")
        release_cluster_lock(api, handle)  # no exception
        assert api.cm is not None and api.cm.data["holder"] == "thief@host@xyz"

    def test_release_leaves_a_recreated_lease(self):
        api = _FakeLockApi()
        api.put("me@here@abc", "2026-09-21T12:00:00+00:00", 60)
        handle = LeaseHandle("me@here@abc", "2026-09-21T12:00:00+00:00", 60, "1")

        def _recreate():
            api.cm = None
            api.put("other@host@q", _now_iso(), 3600, uid="uid-2")

        api.after_read = _recreate
        release_cluster_lock(api, handle)
        assert api.cm is not None and api.cm.data["holder"] == "other@host@q"

    def test_context_manager_does_not_raise_when_stolen_before_delete(self):
        api = _FakeLockApi()
        with cluster_lock(api, ttl_seconds=60, timeout=1, holder="me@here@abc"):
            api.after_read = lambda: api.steal("thief@host@xyz")
        assert api.cm is not None and api.cm.data["holder"] == "thief@host@xyz"

    def test_release_still_deletes_when_unchanged(self):
        api = _FakeLockApi()
        api.put("me@here@abc", "2026-09-21T12:00:00+00:00", 60)
        release_cluster_lock(api, LeaseHandle("me@here@abc", "2026-09-21T12:00:00+00:00", 60, "1"))
        assert api.cm is None

    def test_expired_only_keeps_a_lease_stolen_after_the_expiry_check(self):
        api = _FakeLockApi()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        api.put("ghost@host@x", past, 60)
        api.after_read = lambda: api.steal("fresh@host@y")
        with pytest.raises(ClusterLockHeld) as ei:
            force_release_cluster_lock(api, expired_only=True)
        assert ei.value.holder == "fresh@host@y"
        assert api.cm is not None and api.cm.data["holder"] == "fresh@host@y"

    def test_expired_only_deletes_when_unchanged(self):
        api = _FakeLockApi()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        api.put("ghost@host@x", past, 60)
        state = force_release_cluster_lock(api, expired_only=True)
        assert state is not None and state.holder == "ghost@host@x"
        assert api.cm is None

    def test_force_is_unconditional(self):
        api = _FakeLockApi()
        api.put("live@host@a", _now_iso(), 3600)
        api.after_read = lambda: api.steal("thief@host@xyz")
        force_release_cluster_lock(api, expired_only=False)
        assert api.cm is None


class TestForceRelease:
    def test_expired_only_refuses_live(self):
        core = MagicMock()
        core.read_namespaced_config_map.return_value = _cm(
            "prod-host@sre@abc", _now_iso(), 3600, rv="1"
        )
        with pytest.raises(ClusterLockHeld):
            force_release_cluster_lock(core, expired_only=True)

    def test_expired_only_deletes_expired(self):
        core = MagicMock()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        core.read_namespaced_config_map.return_value = _cm("ghost@host@x", past, 60, rv="1")
        state = force_release_cluster_lock(core, expired_only=True)
        assert state is not None
        core.delete_namespaced_config_map.assert_called_once()

    def test_force_deletes_live(self):
        core = MagicMock()
        core.read_namespaced_config_map.return_value = _cm(
            "live-holder@host@abc", _now_iso(), 3600, rv="1"
        )
        state = force_release_cluster_lock(core, expired_only=False)
        assert state is not None
        core.delete_namespaced_config_map.assert_called_once()


def _stateful_lock_core() -> MagicMock:
    """A CoreV1Api mock whose lock ConfigMap is whatever ``create`` stored.

    The release path deletes only when the stored ``acquired-at`` equals
    the handle's, and both are second-resolution timestamps. Stamping the
    mocked read with a separate ``_now_iso()`` at setup time made the two
    differ whenever a second boundary fell between setup and acquire, so
    release correctly skipped the delete and the test failed (about 1 in
    1,000 runs under CPU load). Echoing back the created body is what a
    real API server does and removes the clock from the test.
    """
    core = MagicMock()
    core.read_namespace.return_value = MagicMock()
    stored: dict[str, V1ConfigMap] = {}

    def _read(name, namespace):
        if "cm" not in stored:
            raise _api_exc(404)
        return stored["cm"]

    def _create(namespace, body):
        body.metadata.resource_version = "1"
        stored["cm"] = body
        return body

    core.read_namespaced_config_map.side_effect = _read
    core.create_namespaced_config_map.side_effect = _create
    return core


class TestContextManager:
    def test_releases_on_normal_exit(self):
        core = _stateful_lock_core()

        with cluster_lock(core, ttl_seconds=60, timeout=1, holder="me@here@abc") as h:
            assert isinstance(h, LeaseHandle)
        core.delete_namespaced_config_map.assert_called_once()

    def test_releases_on_exception(self):
        core = _stateful_lock_core()

        class Boom(RuntimeError):
            pass

        with pytest.raises(Boom):
            with cluster_lock(core, ttl_seconds=60, timeout=1, holder="me@here@abc"):
                raise Boom("body failed")
        core.delete_namespaced_config_map.assert_called_once()


class TestParseIso:
    def test_parses_z_and_offset(self):
        a = _parse_iso8601("2026-09-21T12:00:00Z")
        b = _parse_iso8601("2026-09-21T12:00:00+00:00")
        assert abs(a - b) < 1

    def test_bad_string_treated_as_expired(self):
        assert _parse_iso8601("not-a-timestamp") == 0.0


class TestLeaseStateExpiry:
    def test_is_expired_true_when_past(self):
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        state = LeaseState(
            holder="x@y@z",
            acquired_at=past,
            ttl_seconds=60,
            expires_at_epoch=_parse_iso8601(past) + 60,
            resource_version="1",
        )
        assert state.is_expired() is True

    def test_is_expired_false_when_future(self):
        state = LeaseState(
            holder="x@y@z",
            acquired_at=_now_iso(),
            ttl_seconds=3600,
            expires_at_epoch=time.time() + 3600,
            resource_version="1",
        )
        assert state.is_expired() is False
