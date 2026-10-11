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


@pytest.mark.parametrize(
    ("list_a", "list_b", "equal"),
    [
        (["ns-a", "ns-b", "ns-c"], ["ns-c", "ns-a", "ns-b"], True),
        (["ns-a", "ns-b"], [" ns-a ", "ns-b", "ns-a", ""], True),
        (["ns-a", "ns-b"], ["ns-a", "ns-b", "ns-c"], False),
        # None means "watch all namespaces". No concrete list can collide.
        (None, [], False),
    ],
    ids=["order", "duplicates_and_whitespace", "differing_sets", "watch_all_sentinel"],
)
def test_watch_list_hash(list_a, list_b, equal) -> None:
    a = SparkOperatorManager._watch_list_hash(list_a)
    b = SparkOperatorManager._watch_list_hash(list_b)
    assert (a == b) is equal


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


@pytest.mark.parametrize(
    ("method", "arg", "watched"),
    [
        # Adding a namespace that is already in the list.
        ("_add_namespace_to_watch_impl", "ns-a", ["ns-a", "ns-b"]),
        # Dropping a namespace that is not in the list.
        ("_remove_namespace_from_watch_unlocked", "ns-b", ["ns-a"]),
        # Removing "default" from ["default"] recomputes the chart-default
        # fallback, which equals the current list.
        ("_remove_namespace_from_watch_unlocked", "default", ["default"]),
    ],
    ids=["add_already_watched", "remove_not_watched", "remove_last_falls_back_to_default"],
)
def test_unchanged_watch_list_makes_no_helm_calls(method, arg, watched) -> None:
    mgr = _mgr()
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        calls.append(list(cmd))
        return _run_returning()

    with (
        patch.object(mgr, "_get_watched_namespaces", return_value=watched),
        patch.object(mgr, "_run", side_effect=fake_run),
    ):
        assert getattr(mgr, method)(arg) is True

    assert [c for c in calls if c and c[0] == "helm"] == []


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
        patch.object(mgr, "_get_helm_version", return_value="2.5.1"),
        patch.object(mgr, "_run", side_effect=fake_run),
    ):
        assert mgr._add_namespace_to_watch_impl("ns-b") is True

    helm_upgrade_calls = [c for c in calls if c[:2] == ["helm", "upgrade"]]
    assert len(helm_upgrade_calls) == 1


# ---------------------------------------------------------------------------
# Lease refuses on timeout (C2 fail-closed at the call site)
# ---------------------------------------------------------------------------


class _FailingLease:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def __enter__(self) -> Any:
        raise self._exc

    def __exit__(self, *args: object) -> None:
        return None


def _lease_errors() -> list[Any]:
    from lakebench.deploy.cluster_lock import ClusterLockError, ClusterLockHeld

    held = ClusterLockHeld("other", "t0", 600, "t1")
    return [
        pytest.param(
            ClusterLockError("cannot read lease: request timed out"), "refuse", id="timeout"
        ),
        pytest.param(
            ClusterLockError("cannot reach api: connection refused"), "unlocked", id="refused"
        ),
        pytest.param(held, "refuse", id="held"),
        pytest.param(RuntimeError("timed out"), "refuse", id="bare_timeout"),
        pytest.param(RuntimeError("no route to host"), "unlocked", id="no_route"),
    ]


@pytest.mark.parametrize(("exc", "expected_mode"), _lease_errors())
def test_watch_lease_failure_mode(exc: BaseException, expected_mode: str) -> None:
    """A timed-out or held lease on a reachable API server must not proceed
    unlocked (it would race another writer of the shared watch list); a
    workstation with no cluster has no other writer, so it may."""
    mgr = _mgr()
    mgr._bypass_cluster_lock = False  # exercise the real acquire path

    with (
        patch("kubernetes.client.CoreV1Api", return_value=MagicMock()),
        patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=_FailingLease(exc)),
    ):
        lease_cm, mode = mgr._acquire_watch_lease()

    assert lease_cm is None
    assert mode == expected_mode
