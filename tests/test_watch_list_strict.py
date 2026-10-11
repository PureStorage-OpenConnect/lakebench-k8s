"""Tests for the strict watch-list mutation path.

The non-strict path preserves the historical bool-return contract for
older internal callers. The strict path is what destroy uses: it
lease-gates the mutation and raises ``WatchListMutationError`` naming
``admin repair-operator`` on failure.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lakebench.modules.pipeline_engines.spark import operator as mgr_mod
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

    @pytest.mark.parametrize(
        "read",
        ["terminating", 404, 429, 500, 503, "transport"],
    )
    def test_unproven_namespace_is_not_added(self, read, monkeypatch):
        """A terminating, gone (destroy finished while deploy waited for the
        lease), throttled or unreachable namespace read refuses the add: no
        helm upgrade (a watched deleted namespace crash-loops the operator)."""
        from kubernetes.client.rest import ApiException

        monkeypatch.setattr(mgr_mod.time, "sleep", lambda _s: None)
        mgr = _mgr()
        with (
            patch.object(mgr, "_get_watched_namespaces", return_value=["other"]),
            patch("kubernetes.client.CoreV1Api") as core,
            patch.object(mgr, "_run") as run,
        ):
            rn = core.return_value.read_namespace
            if read == "terminating":
                rn.return_value = self._ns(deleting=True)
            elif read == "transport":
                rn.side_effect = ConnectionError("reset")
            else:
                rn.side_effect = ApiException(status=read)
            assert mgr._add_namespace_to_watch_impl("my-ns") is False
        run.assert_not_called()

    @pytest.mark.parametrize("transient", [False, True])
    def test_live_namespace_is_added(self, transient, monkeypatch):
        """A live namespace, or one read after a single transient 429, is added
        to spark.jobNamespaces by the helm upgrade."""
        from kubernetes.client.rest import ApiException

        monkeypatch.setattr(mgr_mod.time, "sleep", lambda _s: None)
        mgr = _mgr()
        reads = [self._ns(deleting=False)]
        if transient:
            reads.insert(0, ApiException(status=429))
        chart = '[{"name": "spark-operator", "chart": "spark-operator-2.5.1"}]'

        upgraded: list[bool] = []

        def watched(*_a, **_kw):
            return ["other", "my-ns"] if upgraded else ["other"]

        def run_cmd(cmd, **_kw):
            is_list = cmd[:2] == ["helm", "list"]
            if cmd[:2] == ["helm", "upgrade"]:
                upgraded.append(True)
            return MagicMock(returncode=0, stdout=chart if is_list else "", stderr="")

        with (
            patch.object(mgr, "_get_watched_namespaces", side_effect=watched),
            patch.object(mgr, "_filter_existing_namespaces", return_value=["other"]),
            patch("kubernetes.client.CoreV1Api") as core,
            patch.object(mgr, "_run", side_effect=run_cmd) as run,
        ):
            core.return_value.read_namespace.side_effect = reads
            added = mgr._add_namespace_to_watch_impl("my-ns")
        assert added is True
        upgrades = [c.args[0] for c in run.call_args_list if c.args[0][:2] == ["helm", "upgrade"]]
        assert any(
            any(a.startswith("spark.jobNamespaces=") and "my-ns" in a and "other" in a for a in cmd)
            for cmd in upgrades
        )


class TestPinnedContext:
    """SAF-7: a kubeconfig rewritten under destroy surfaces as a
    WatchListMutationError, so destroy keeps the namespace and names the
    recovery, instead of an untyped error after a half-done upgrade."""

    def test_conflict_before_the_first_mutation_modifies_nothing(self):
        from lakebench.k8s.target import ContextConflictError

        mgr = _mgr()
        cm_ctx = MagicMock()
        cm_ctx.__enter__ = MagicMock(return_value=MagicMock())
        cm_ctx.__exit__ = MagicMock(return_value=False)
        precondition = MagicMock()
        with (
            patch(
                "lakebench.k8s.target.cli_args",
                side_effect=ContextConflictError("one cluster context per process"),
            ),
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=cm_ctx),
            patch.object(mgr, "_remove_namespace_from_watch_impl") as impl,
        ):
            with pytest.raises(WatchListMutationError, match="NOT modified"):
                mgr._remove_namespace_from_watch_locked("my-ns", precondition=precondition)
        # Checked under the lease, before the precondition and any mutation.
        cm_ctx.__enter__.assert_called_once()
        precondition.assert_not_called()
        impl.assert_not_called()

    def test_add_refuses_on_a_context_conflict(self):
        from lakebench.k8s.target import ContextConflictError

        mgr = _mgr()
        with (
            patch.object(mgr, "_acquire_watch_lease", return_value=(None, "unlocked")),
            patch(
                "lakebench.k8s.target.cli_args",
                side_effect=ContextConflictError("one cluster context per process"),
            ),
            patch.object(mgr, "_add_namespace_to_watch_impl") as impl,
        ):
            assert mgr._add_namespace_to_watch("my-ns") is False
        impl.assert_not_called()

    @pytest.mark.parametrize("raised", ["timeout", "lease_hold"])
    def test_add_fails_closed_after_the_pin_check_on_a_leased_timeout(self, raised):
        """The pinned-context check passes, then a command runs out of the
        lease's time: the add reports failure, not an exception, so deploy
        does not read the namespace as watched (merge of CC-7 and LEASE)."""
        import subprocess

        from lakebench.k8s.lease_state import LeaseHoldExceeded

        exc = (
            subprocess.TimeoutExpired(["helm"], 30)
            if raised == "timeout"
            else LeaseHoldExceeded("lease hold budget spent")
        )
        mgr = _mgr()
        with (
            patch.object(mgr, "_acquire_watch_lease", return_value=(None, "unlocked")),
            patch("lakebench.k8s.target.cli_args", return_value=[]) as pin,
            patch.object(mgr, "_add_namespace_to_watch_impl", side_effect=exc) as impl,
        ):
            assert mgr._add_namespace_to_watch("my-ns") is False
        pin.assert_called_once()
        impl.assert_called_once()

    def test_run_refuses_a_tool_given_as_a_path(self):
        mgr = _mgr()
        with patch.object(mgr_mod.subprocess, "run") as run:
            with pytest.raises(ValueError, match="by name"):
                mgr._run(["/usr/bin/kubectl", "get", "pods"])
        run.assert_not_called()
