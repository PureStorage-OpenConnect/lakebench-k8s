"""Tests for the strict watch-list mutation path.

The non-strict path preserves the historical bool-return contract for
older internal callers. The strict path is what destroy uses: it
lease-gates the mutation and raises ``WatchListMutationError`` naming
``admin repair-operator`` on failure.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy.cluster_lock import ClusterLockError, ClusterLockHeld
from lakebench.modules.pipeline_engines.spark.operator import (
    SparkOperatorManager,
    WatchListMutationError,
)


def _mgr() -> SparkOperatorManager:
    return SparkOperatorManager(
        namespace="spark-operator",
        version="2.5.1",
        job_namespace="my-ns",
    )


class TestStrictMode:
    def test_strict_dispatches_to_locked(self):
        mgr = _mgr()
        with patch.object(
            mgr, "_remove_namespace_from_watch_locked", return_value=True
        ) as mock_locked:
            r = mgr.remove_namespace_from_watch("my-ns", strict=True)
        assert r is True
        mock_locked.assert_called_once_with("my-ns", None, None)

    def test_nonstrict_does_not_engage_lock(self):
        mgr = _mgr()
        with (
            patch.object(mgr, "_remove_namespace_from_watch_locked") as mock_locked,
            patch.object(mgr, "_get_watched_namespaces", return_value=None),
        ):
            # Non-strict path returns True for "watches all namespaces".
            r = mgr.remove_namespace_from_watch("my-ns")
        assert r is True
        mock_locked.assert_not_called()

    def test_raises_on_helm_failure(self):
        """A False from the underlying helper becomes an error message
        naming ``admin repair-operator`` for recovery."""
        mgr = _mgr()
        cm_ctx = MagicMock()
        cm_ctx.__enter__ = MagicMock(return_value=MagicMock())
        cm_ctx.__exit__ = MagicMock(return_value=False)
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=cm_ctx),
            patch.object(mgr, "_remove_namespace_from_watch_impl", return_value=False),
        ):
            with pytest.raises(WatchListMutationError) as ei:
                mgr._remove_namespace_from_watch_locked("my-ns")
        assert "admin repair-operator" in str(ei.value)

    def test_raises_on_lease_held(self):
        mgr = _mgr()
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch(
                "lakebench.deploy.cluster_lock.cluster_lock",
                side_effect=ClusterLockHeld(
                    holder="other@host@abc",
                    acquired_at="2026-09-21T00:00:00+00:00",
                    ttl_seconds=3600,
                    expires_at="2026-09-21T01:00:00+00:00",
                ),
            ),
        ):
            with pytest.raises(WatchListMutationError) as ei:
                mgr._remove_namespace_from_watch_locked("my-ns")
        assert "other@host@abc" in str(ei.value)
        assert "release-lock" in str(ei.value)

    def test_raises_on_lease_error(self):
        mgr = _mgr()
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch(
                "lakebench.deploy.cluster_lock.cluster_lock",
                side_effect=ClusterLockError("transport blew up"),
            ),
        ):
            with pytest.raises(WatchListMutationError) as ei:
                mgr._remove_namespace_from_watch_locked("my-ns")
        assert "admin repair-operator" in str(ei.value)

    def test_success_returns_true(self):
        mgr = _mgr()
        cm_ctx = MagicMock()
        cm_ctx.__enter__ = MagicMock(return_value=MagicMock())
        cm_ctx.__exit__ = MagicMock(return_value=False)
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=cm_ctx),
            patch.object(mgr, "_remove_namespace_from_watch_impl", return_value=True),
        ):
            assert mgr._remove_namespace_from_watch_locked("my-ns") is True


class TestPrecondition:
    """Destroy re-checks the namespace UID inside the lease (lane Y finding 3)."""

    def _lock(self, events):
        cm_ctx = MagicMock()
        cm_ctx.__enter__ = MagicMock(side_effect=lambda *a: events.append("lock"))
        cm_ctx.__exit__ = MagicMock(side_effect=lambda *a: events.append("unlock") and False)
        return cm_ctx

    def test_precondition_runs_inside_the_lease_before_the_change(self):
        mgr = _mgr()
        events: list[str] = []
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=self._lock(events)),
            patch.object(
                mgr,
                "_remove_namespace_from_watch_impl",
                side_effect=lambda ns: events.append("helm") or True,
            ),
        ):
            mgr.remove_namespace_from_watch(
                "my-ns", strict=True, precondition=lambda: events.append("check")
            )
        assert events == ["lock", "check", "helm", "unlock"]

    def test_raising_precondition_leaves_the_watch_list_alone(self):
        class Replaced(Exception):
            pass

        def refuse():
            raise Replaced("newer incarnation")

        mgr = _mgr()
        events: list[str] = []
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=self._lock(events)),
            patch.object(mgr, "_remove_namespace_from_watch_impl") as impl,
        ):
            with pytest.raises(Replaced):
                mgr.remove_namespace_from_watch("my-ns", strict=True, precondition=refuse)
        impl.assert_not_called()
        assert events == ["lock", "unlock"], "the lease must still be released"

    def test_then_runs_under_the_lease_only_after_a_successful_removal(self):
        mgr = _mgr()
        events: list[str] = []
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=self._lock(events)),
            patch.object(
                mgr,
                "_remove_namespace_from_watch_impl",
                side_effect=lambda ns: events.append("helm") or True,
            ),
        ):
            mgr.remove_namespace_from_watch(
                "my-ns", strict=True, then=lambda: events.append("delete")
            )
        assert events == ["lock", "helm", "delete", "unlock"]

        events.clear()
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=self._lock(events)),
            patch.object(mgr, "_remove_namespace_from_watch_impl", return_value=False),
        ):
            with pytest.raises(WatchListMutationError):
                mgr.remove_namespace_from_watch(
                    "my-ns", strict=True, then=lambda: events.append("delete")
                )
        assert "delete" not in events


class TestAddRefusesTerminatingNamespace:
    """Race review finding 1: a deploy's add must not re-list a namespace a
    destroy has just un-watched and deleted (the operator would crash-loop)."""

    def _ns(self, deleting: bool):
        import datetime

        ns = MagicMock()
        ns.metadata.deletion_timestamp = (
            datetime.datetime(2026, 9, 25, tzinfo=datetime.timezone.utc) if deleting else None
        )
        return ns

    def test_terminating_namespace_is_not_added(self):
        mgr = _mgr()
        with (
            patch.object(mgr, "_get_watched_namespaces", return_value=["other"]),
            patch("kubernetes.client.CoreV1Api") as core,
            patch.object(mgr, "_run") as run,
        ):
            core.return_value.read_namespace.return_value = self._ns(deleting=True)
            assert mgr._add_namespace_to_watch_impl("my-ns") is False
        run.assert_not_called()

    def test_live_namespace_is_added(self):
        mgr = _mgr()
        with (
            patch.object(mgr, "_get_watched_namespaces", return_value=["other"]),
            patch.object(mgr, "_filter_existing_namespaces", return_value=["other"]),
            patch("kubernetes.client.CoreV1Api") as core,
            patch.object(mgr, "_run", return_value=MagicMock(returncode=1, stderr="boom")) as run,
        ):
            core.return_value.read_namespace.return_value = self._ns(deleting=False)
            mgr._add_namespace_to_watch_impl("my-ns")
        assert run.called, "a live namespace proceeds to the helm upgrade"

    def test_namespace_already_gone_is_not_added(self):
        """Fix review: destroy can finish deleting it while the deploy waits
        for the lease; a 404 must refuse, not fail open."""
        from kubernetes.client.rest import ApiException

        mgr = _mgr()
        with (
            patch.object(mgr, "_get_watched_namespaces", return_value=["other"]),
            patch("kubernetes.client.CoreV1Api") as core,
            patch.object(mgr, "_run") as run,
        ):
            core.return_value.read_namespace.side_effect = ApiException(status=404)
            assert mgr._add_namespace_to_watch_impl("my-ns") is False
        run.assert_not_called()

    @pytest.mark.parametrize("status", [429, 500, 503])
    def test_unreadable_namespace_is_not_added(self, status):
        """Third review: under API throttling a failed read must refuse the
        add, not treat the namespace as live (crash-loop route)."""
        from kubernetes.client.rest import ApiException

        mgr = _mgr()
        with (
            patch.object(mgr, "_get_watched_namespaces", return_value=["other"]),
            patch("kubernetes.client.CoreV1Api") as core,
            patch.object(mgr, "_run") as run,
        ):
            core.return_value.read_namespace.side_effect = ApiException(status=status)
            assert mgr._add_namespace_to_watch_impl("my-ns") is False
        run.assert_not_called()

    def test_transport_error_is_not_added(self):
        mgr = _mgr()
        with (
            patch.object(mgr, "_get_watched_namespaces", return_value=["other"]),
            patch("kubernetes.client.CoreV1Api") as core,
            patch.object(mgr, "_run") as run,
        ):
            core.return_value.read_namespace.side_effect = ConnectionError("reset")
            assert mgr._add_namespace_to_watch_impl("my-ns") is False
        run.assert_not_called()
