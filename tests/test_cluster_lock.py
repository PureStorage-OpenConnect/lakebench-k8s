"""Unit tests for the cluster-wide lakebench lease.

The lease guards Category 4 (shared mutable state) mutations under
concurrent lakebench invocations. See
``docs/internal/namespace-isolation.md``.
"""

from __future__ import annotations

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
    @pytest.mark.parametrize("zulu", [False, True])
    def test_refuses_when_held_within_ttl(self, zulu):
        """A live lease is refused, whichever way its acquired-at is written
        (a failed parse would read as expired and be stolen)."""
        core = MagicMock()
        core.read_namespace.return_value = MagicMock()
        now = datetime.now(timezone.utc)
        acquired = now.strftime("%Y-%m-%dT%H:%M:%SZ") if zulu else _now_iso()
        core.read_namespaced_config_map.return_value = _cm(
            "other-host@other-user@xyz", acquired, 3600, rv="1"
        )

        with pytest.raises(ClusterLockHeld) as ei:
            acquire_cluster_lock(core, ttl_seconds=60, timeout=0, holder="me@here@abc")
        assert ei.value.holder == "other-host@other-user@xyz"
        core.replace_namespaced_config_map.assert_not_called()

    @pytest.mark.parametrize("racer", [None, "b@host@2"])
    def test_steals_expired_lease(self, racer):
        """An expired lease is stolen by compare-and-swap: a second stealer
        that replaces it between our read and our write wins, and we get its
        lease back instead of overwriting it."""
        from lakebench.deploy.cluster_lock import _try_acquire_once

        api = _FakeLockApi()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        api.put("crashed-host@ghost@000", past, 60)
        won = []
        if racer:
            api.after_read = lambda: won.append(_try_acquire_once(api, racer, 3600))
            with pytest.raises(ClusterLockHeld) as ei:
                acquire_cluster_lock(api, ttl_seconds=60, timeout=0, holder="me@here@abc")
            assert ei.value.holder == racer
            assert isinstance(won[0], LeaseHandle)
            assert api.cm is not None and api.cm.data["holder"] == racer
        else:
            handle = acquire_cluster_lock(api, ttl_seconds=60, timeout=0, holder="me@here@abc")
            assert api.cm is not None and api.cm.data["holder"] == "me@here@abc"
            assert handle.resource_version == api.cm.metadata.resource_version


class TestReleaseClusterLock:
    def test_release_skipped_when_stolen(self):
        """A lease admin-released and re-taken mid-run is left alone."""
        api = _FakeLockApi()
        api.put("someone-else@host@xyz", "2026-09-21T14:00:00+00:00", 60)
        handle = LeaseHandle(
            holder="me@here@abc",
            acquired_at="2026-09-21T12:00:00+00:00",
            ttl_seconds=60,
            resource_version="1",
        )
        release_cluster_lock(api, handle)
        assert api.cm is not None and api.cm.data["holder"] == "someone-else@host@xyz"

    def test_release_when_already_gone(self):
        core = MagicMock()
        core.read_namespaced_config_map.side_effect = _api_exc(404)
        handle = LeaseHandle(
            holder="me@here@abc",
            acquired_at="2026-09-21T12:00:00+00:00",
            ttl_seconds=60,
            resource_version="1",
        )
        release_cluster_lock(core, handle)
        core.delete_namespaced_config_map.assert_not_called()


class _FakeLockApi:
    """The lock ConfigMap as the API server keeps it: resourceVersion bumps
    on every write and a delete whose preconditions no longer match is
    refused with 409. ``after_read`` runs once, right after the next read
    returns, to land a competing write inside the read-then-delete window.
    """

    def __init__(self) -> None:
        self.cm: V1ConfigMap | None = None
        self.last_delete_preconditions = None
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

    def touch(self) -> None:
        """A write that keeps the lease data (label, annotation, managedFields)."""
        assert self.cm is not None
        self._stamp(self.cm, self.cm.metadata.uid)

    def replace_namespaced_config_map(self, name, namespace, body, **_kw):
        if self.cm is None:
            raise _api_exc(404)
        # Without a resourceVersion the API server replaces unconditionally.
        rv = body.metadata.resource_version
        if rv and rv != self.cm.metadata.resource_version:
            raise _api_exc(409)
        return self._stamp(body, self.cm.metadata.uid)

    def read_namespace(self, name, **_kw):
        return MagicMock()

    def create_namespaced_config_map(self, namespace, body, **_kw):
        if self.cm is not None:
            raise _api_exc(409)
        return self._stamp(body, "uid-1")

    def read_namespaced_config_map(self, name, namespace, **_kw):
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

    def delete_namespaced_config_map(self, name, namespace, body=None, **_kw):
        if self.cm is None:
            raise _api_exc(404)
        pre = getattr(body, "preconditions", None)
        self.last_delete_preconditions = pre
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
        # the delete is conditional on the lease that was read
        assert api.last_delete_preconditions.resource_version == "1"

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

    def test_release_retries_after_a_write_that_kept_our_data(self):
        """A label or annotation bump must not leak our own lease for its TTL."""
        api = _FakeLockApi()
        api.put("me@here@abc", "2026-09-21T12:00:00+00:00", 60)
        api.after_read = api.touch
        release_cluster_lock(api, LeaseHandle("me@here@abc", "2026-09-21T12:00:00+00:00", 60, "1"))
        assert api.cm is None

    def test_release_leaves_a_real_acquire_steal(self):
        """B steals the expired lease through the real acquire CAS mid-release."""
        from lakebench.deploy.cluster_lock import _try_acquire_once

        api = _FakeLockApi()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        api.put("a@host@1", past, 60)
        stolen = []
        api.after_read = lambda: stolen.append(_try_acquire_once(api, "b@host@2", 3600))
        release_cluster_lock(api, LeaseHandle("a@host@1", past, 60, "1"))
        assert isinstance(stolen[0], LeaseHandle)
        assert api.cm is not None and api.cm.data["holder"] == "b@host@2"

    def test_expired_only_deletes_after_a_write_that_kept_it_expired(self):
        api = _FakeLockApi()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        api.put("ghost@host@x", past, 60)
        api.after_read = api.touch
        state = force_release_cluster_lock(api, expired_only=True)
        assert state is not None and state.holder == "ghost@host@x"
        assert api.cm is None

    def test_expired_only_names_a_holder_who_stole_on_the_last_attempt(self):
        api = _FakeLockApi()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
        api.put("ghost@host@x", past, 60)
        reads = {"n": 0}
        orig_read = api.read_namespaced_config_map

        def _read(name, namespace, **_kw):
            reads["n"] += 1
            # Keep the ghost expired but changed for two rounds, then steal.
            api.after_read = api.touch if reads["n"] < 3 else (lambda: api.steal("late@host@z"))
            return orig_read(name, namespace)

        api.read_namespaced_config_map = _read
        with pytest.raises(ClusterLockHeld) as ei:
            force_release_cluster_lock(api, expired_only=True)
        assert ei.value.holder == "late@host@z"
        assert api.cm is not None and api.cm.data["holder"] == "late@host@z"

    def test_transport_error_on_release_does_not_shadow_the_body(self):
        class Boom(RuntimeError):
            pass

        api = _FakeLockApi()
        with pytest.raises(Boom):
            with cluster_lock(api, ttl_seconds=60, timeout=1, holder="me@here@abc"):
                api.read_namespaced_config_map = MagicMock(side_effect=OSError("conn reset"))
                raise Boom("body failed")

    def test_force_is_unconditional(self):
        api = _FakeLockApi()
        api.put("live@host@a", _now_iso(), 3600)
        api.after_read = lambda: api.steal("thief@host@xyz")
        force_release_cluster_lock(api, expired_only=False)
        assert api.cm is None


class TestForceRelease:
    def test_expired_only_refuses_live(self):
        api = _FakeLockApi()
        api.put("prod-host@sre@abc", _now_iso(), 3600)
        with pytest.raises(ClusterLockHeld) as ei:
            force_release_cluster_lock(api, expired_only=True)
        assert ei.value.holder == "prod-host@sre@abc"
        assert api.cm is not None and api.cm.data["holder"] == "prod-host@sre@abc"


class TestParseIso:
    def test_parses_z_and_offset(self):
        expected = datetime(2026, 9, 21, 12, tzinfo=timezone.utc).timestamp()
        assert _parse_iso8601("2026-09-21T12:00:00Z") == expected
        assert _parse_iso8601("2026-09-21T12:00:00+00:00") == expected

    def test_bad_string_treated_as_expired(self):
        assert _parse_iso8601("not-a-timestamp") == 0.0
