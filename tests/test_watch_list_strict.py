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
        mock_locked.assert_called_once_with("my-ns")

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
