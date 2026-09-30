"""Watch-list unchanged: SparkOperatorManager skips the helm upgrade.

The shared spark-operator release's ``spark.jobNamespaces`` value is
read, mutated and rewritten by every ``lakebench deploy`` and every
strict destroy. When the value we would write matches what is already
there, the ``helm upgrade`` and the controller restart it triggers are
pure churn: they take the release lock, roll the operator pods, and
contend with every other parallel deploy for no state change.

These tests lock in two behaviours:

* Under an unchanged desired list, the manager does not shell out to
  ``helm upgrade`` at all (zero helm invocations, no restart).
* The content hash used to compare lists is order-independent, so
  a reordered read is treated as equal.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

# ---------------------------------------------------------------------------
# Hash determinism
# ---------------------------------------------------------------------------


def test_watch_list_hash_is_order_independent() -> None:
    a = SparkOperatorManager._watch_list_hash(["ns-a", "ns-b", "ns-c"])
    b = SparkOperatorManager._watch_list_hash(["ns-c", "ns-a", "ns-b"])
    assert a == b


def test_watch_list_hash_ignores_duplicates_and_whitespace() -> None:
    a = SparkOperatorManager._watch_list_hash(["ns-a", "ns-b"])
    b = SparkOperatorManager._watch_list_hash([" ns-a ", "ns-b", "ns-a", ""])
    assert a == b


def test_watch_list_hash_differs_between_sets() -> None:
    a = SparkOperatorManager._watch_list_hash(["ns-a", "ns-b"])
    b = SparkOperatorManager._watch_list_hash(["ns-a", "ns-b", "ns-c"])
    assert a != b


def test_watch_list_hash_watch_all_is_sentinel() -> None:
    # None means "watch all namespaces". No concrete list can collide.
    watch_all = SparkOperatorManager._watch_list_hash(None)
    concrete = SparkOperatorManager._watch_list_hash([])
    assert watch_all != concrete


# ---------------------------------------------------------------------------
# Skip when the desired watch list matches the current one
# ---------------------------------------------------------------------------


def _mgr() -> SparkOperatorManager:
    """A manager whose lease and precondition checks are all no-ops."""
    m = SparkOperatorManager(namespace="spark-operator")
    m._bypass_cluster_lock = True  # type: ignore[attr-defined]
    return m


def _run_returning(returncode: int = 0, stderr: str = "") -> Any:
    class _R:
        pass

    r = _R()
    r.returncode = returncode
    r.stdout = ""
    r.stderr = stderr
    return r


def test_add_namespace_already_watched_makes_no_helm_calls() -> None:
    """Adding a namespace that is already in the list must not shell out."""
    mgr = _mgr()
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        calls.append(list(cmd))
        return _run_returning()

    with (
        patch.object(mgr, "_get_watched_namespaces", return_value=["ns-a", "ns-b"]),
        patch.object(mgr, "_run", side_effect=fake_run),
    ):
        assert mgr._add_namespace_to_watch_impl("ns-a") is True

    # helm upgrade would be the first tool spawned. Nothing at all should run.
    helm_calls = [c for c in calls if c and c[0] == "helm"]
    assert helm_calls == []


def test_remove_namespace_not_watched_makes_no_helm_calls() -> None:
    """Dropping a namespace that is not in the list is a no-op."""
    mgr = _mgr()
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        calls.append(list(cmd))
        return _run_returning()

    with (
        patch.object(mgr, "_get_watched_namespaces", return_value=["ns-a"]),
        patch.object(mgr, "_run", side_effect=fake_run),
    ):
        assert mgr._remove_namespace_from_watch_unlocked("ns-b") is True

    helm_calls = [c for c in calls if c and c[0] == "helm"]
    assert helm_calls == []


def test_remove_last_namespace_falling_back_to_default_skips_when_already_default() -> None:
    """If the watch list is exactly ["default"], removing "default" would rewrite
    the same value; the manager must recognise that and skip helm upgrade."""
    mgr = _mgr()
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        calls.append(list(cmd))
        return _run_returning()

    with (
        patch.object(mgr, "_get_watched_namespaces", return_value=["default"]),
        patch.object(mgr, "_run", side_effect=fake_run),
    ):
        # Ask to remove "default": the code recomputes desired = ["default"]
        # (the chart-default fallback) which matches the current list.
        assert mgr._remove_namespace_from_watch_unlocked("default") is True

    helm_calls = [c for c in calls if c and c[0] == "helm"]
    assert helm_calls == []


def test_add_first_namespace_still_runs_helm_upgrade() -> None:
    """Sanity: skipping is not "always skip"; a real change must still run.

    Regression guard so a broken hash comparison that always returns
    equal doesn't silently disable every watch-list edit.
    """
    mgr = _mgr()
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        calls.append(list(cmd))
        return _run_returning()

    with (
        patch.object(mgr, "_get_watched_namespaces", return_value=["ns-a"]),
        patch.object(mgr, "_filter_existing_namespaces", side_effect=lambda x: x),
        patch.object(mgr, "_namespace_is_terminating", return_value=False),
        patch.object(mgr, "_is_openshift", return_value=False),
        patch.object(mgr, "_restart_operator", return_value=True),
        patch.object(mgr, "_verify_namespace_watched", return_value=True),
        patch.object(mgr, "_run", side_effect=fake_run),
    ):
        assert mgr._add_namespace_to_watch_impl("ns-b") is True

    helm_upgrade_calls = [c for c in calls if c[:2] == ["helm", "upgrade"]]
    assert len(helm_upgrade_calls) == 1


# ---------------------------------------------------------------------------
# Lease refuses on timeout (C2 fail-closed at the call site)
# ---------------------------------------------------------------------------


def test_watch_lease_refuses_on_timeout() -> None:
    """A bare "timed out" ClusterLockError must NOT proceed unlocked.

    ADR-F5 / LB-ux-safety C2: with a *reachable* API server (no
    'connection refused' / 'not resolve' marker), a timed-out acquire
    means someone else likely holds the lease. Proceeding without it
    would race the watch-list mutation and re-open the crash-loop the
    lease exists to close. The call site must return ("refuse") so the
    caller aborts the mutation.
    """
    from lakebench.deploy.cluster_lock import ClusterLockError

    mgr = _mgr()
    mgr._bypass_cluster_lock = False  # exercise the real acquire path

    # Fake context manager whose __enter__ raises ClusterLockError("... timed out ...").
    class _TimeoutLease:
        def __enter__(self) -> Any:
            raise ClusterLockError("cannot read lease: request timed out")

        def __exit__(self, *args: object) -> None:
            return None

    with (
        patch("kubernetes.client.CoreV1Api", return_value=MagicMock()),
        patch(
            "lakebench.deploy.cluster_lock.cluster_lock",
            return_value=_TimeoutLease(),
        ),
    ):
        lease_cm, mode = mgr._acquire_watch_lease()

    assert lease_cm is None
    assert mode == "refuse"


def test_watch_lease_still_unlocks_on_connection_refused() -> None:
    """Workstation-with-no-cluster is still safe: no other writer can race."""
    from lakebench.deploy.cluster_lock import ClusterLockError

    mgr = _mgr()
    mgr._bypass_cluster_lock = False

    class _NoClusterLease:
        def __enter__(self) -> Any:
            raise ClusterLockError("cannot reach api: connection refused")

        def __exit__(self, *args: object) -> None:
            return None

    with (
        patch("kubernetes.client.CoreV1Api", return_value=MagicMock()),
        patch(
            "lakebench.deploy.cluster_lock.cluster_lock",
            return_value=_NoClusterLease(),
        ),
    ):
        lease_cm, mode = mgr._acquire_watch_lease()

    assert lease_cm is None
    assert mode == "unlocked"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-x"]))
