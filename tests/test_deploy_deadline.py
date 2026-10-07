"""DEP-6: the deploy timeout inside every wait (ch01 s8)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy import deadline
from lakebench.deploy.engine import DeploymentEngine
from lakebench.k8s import wait as wait_mod

SRC = Path(__file__).resolve().parent.parent / "src" / "lakebench"


class FakeClock:
    """One clock for time.time, time.monotonic and time.sleep."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept = 0.0

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.now += max(0.0, s)
        self.slept += max(0.0, s)


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock()
    for mod in (deadline.time, wait_mod.time):
        for name in ("time", "monotonic", "sleep"):
            monkeypatch.setattr(mod, name, getattr(c, name))
    return c


# --- the deadline itself ---------------------------------------------------------


def test_no_deadline_outside_deploy():
    assert deadline.remaining() is None
    assert deadline.clamp(600) == 600
    deadline.check("anything")  # never raises
    assert not deadline.expired()


# --- waits ------------------------------------------------------------------------


def _engine():
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.results = []
    return eng


def test_post_upgrade_rollout_is_not_cut_by_the_deadline(clock):
    from lakebench.modules.pipeline_engines.spark import operator as op

    m = op.SparkOperatorManager.__new__(op.SparkOperatorManager)
    m.namespace = "spark-operator"
    m._run = MagicMock(return_value=MagicMock(returncode=0, stderr=""))
    with deadline.deploy_deadline(5):
        clock.sleep(10)
        assert m._rollout_status_after_upgrade()
    args = [c.args[0] for c in m._run.call_args_list]
    # Bounded by its phase (and the lease hold), never by the deploy deadline.
    assert len(args) == 2 and all(f"--timeout={op.WATCH_ROLLOUT_PHASE_S}s" in a for a in args)


# --- the shared watch list: gate before the mutation, never after ------------------


def _operator(clock):
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    m = SparkOperatorManager.__new__(SparkOperatorManager)
    m.namespace = "spark-operator"
    m._get_watched_namespaces = MagicMock(return_value=["other-ns"])
    m._namespace_is_terminating = MagicMock(return_value=False)
    m._filter_existing_namespaces = MagicMock(side_effect=lambda ns: list(ns))
    m._watch_list_pin = MagicMock(return_value=[])
    m._is_openshift = MagicMock(return_value=False)
    m._verify_namespace_watched = MagicMock(return_value=True)
    m._restart_operator = MagicMock(return_value=True)
    return m


def test_no_shared_upgrade_after_the_deadline(clock):
    m = _operator(clock)
    m._run = MagicMock()
    with deadline.deploy_deadline(10):
        clock.sleep(10)
        with pytest.raises(deadline.DeployTimeout):
            m._add_namespace_to_watch_impl("mine")
    m._run.assert_not_called()


def test_a_committed_upgrade_is_restarted_and_verified_past_the_deadline(clock):
    """The deadline passing during the helm upgrade must not release the
    lease with the shared operator mid-restart and unverified."""
    m = _operator(clock)

    def slow_helm(*a, **k):
        clock.sleep(20)  # the upgrade outlives the deadline
        return MagicMock(returncode=0, stderr="")

    m._run = MagicMock(side_effect=slow_helm)
    with deadline.deploy_deadline(10):
        assert m._add_namespace_to_watch_impl("mine") is True
    m._restart_operator.assert_called_once()
    m._verify_namespace_watched.assert_called_once()
    from lakebench.modules.pipeline_engines.spark import operator as op

    assert m._verify_namespace_watched.call_args.kwargs["timeout"] == op._POST_UPGRADE_VERIFY_S


def test_eviction_re_add_is_not_cut_by_the_deadline(clock):
    m = _operator(clock)
    m._verify_namespace_watched = MagicMock(side_effect=[False, True])  # evicted once

    def slow_helm(*a, **k):
        clock.sleep(20)
        return MagicMock(returncode=0, stderr="")

    m._run = MagicMock(side_effect=slow_helm)
    with deadline.deploy_deadline(10):
        assert m._add_namespace_to_watch_impl("mine") is True
    assert m._run.call_count == 2  # the re-add ran past the deadline


def test_rbac_recreate_re_adds_after_a_committed_removal(clock):
    m = _operator(clock)
    m._get_watched_namespaces = MagicMock(side_effect=[["other-ns", "mine"], ["other-ns"]])

    def slow_helm(*a, **k):
        clock.sleep(20)
        return MagicMock(returncode=0, stderr="")

    m._run = MagicMock(side_effect=slow_helm)
    with deadline.deploy_deadline(10):
        assert m._recreate_namespace_rbac_impl("mine") is True
    assert m._run.call_count == 2  # removal, then the re-add


def test_rbac_recreate_does_not_start_after_the_deadline(clock):
    m = _operator(clock)
    m._get_watched_namespaces = MagicMock(return_value=["other-ns", "mine"])
    m._run = MagicMock()
    with deadline.deploy_deadline(10):
        clock.sleep(10)
        with pytest.raises(deadline.DeployTimeout):
            m._recreate_namespace_rbac_impl("mine")
    m._run.assert_not_called()


def test_no_operator_install_after_the_deadline(clock):
    m = _operator(clock)
    m.target_version = "2.5.1"
    m.job_namespace = "mine"
    m._release_exists = MagicMock(return_value=False)

    def slow_repo(*a, **k):
        clock.sleep(20)  # helm repo update outlives the deadline
        return MagicMock(returncode=0, stderr="")

    m._run = MagicMock(side_effect=slow_repo)
    with deadline.deploy_deadline(10):
        with pytest.raises(deadline.DeployTimeout):
            m.install()
    assert all(c.args[0][:2] == ["helm", "repo"] for c in m._run.call_args_list)


def test_lease_wait_counts_against_the_deadline(clock, monkeypatch):
    from lakebench.deploy import cluster_lock as cl
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    seen = {}

    class Held:
        def __enter__(self):
            clock.sleep(seen["timeout"])
            raise cl.ClusterLockHeld("holder-b", "t0", 60, "t1")

        def __exit__(self, *a):
            return False

    def fake_lock(core, timeout):
        seen["timeout"] = timeout
        return Held()

    monkeypatch.setattr(cl, "cluster_lock", fake_lock)
    m = SparkOperatorManager.__new__(SparkOperatorManager)
    with patch("kubernetes.client.CoreV1Api"), deadline.deploy_deadline(30):
        clock.sleep(20)
        with pytest.raises(deadline.DeployTimeout) as e:
            m._acquire_watch_lease()
    assert seen["timeout"] == 10
    assert "held by holder-b" in str(e.value)


def test_operator_scc_check_is_not_cut_after_a_shared_change(clock, monkeypatch):
    """The Spark Operator's SCC check runs after a committed helm change; a
    deadline that passes during its poll must not skip the patch after it."""
    from kubernetes.client.rest import ApiException

    from lakebench.k8s import security
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    calls = []

    def fake_ensure(*a, **k):
        calls.append(k.get("cut_by_deadline"))
        rbac = MagicMock()
        rbac.read_namespaced_role_binding.side_effect = ApiException(status=404)
        authz = MagicMock()
        authz.create_namespaced_local_subject_access_review.return_value = {
            "status": {"allowed": False}
        }
        return real(rbac, a[1], a[2], a[3], authz_api=authz, **k)

    real = security.ensure_scc_rolebinding
    monkeypatch.setattr(security, "ensure_scc_rolebinding", fake_ensure)
    monkeypatch.setattr("kubernetes.client.RbacAuthorizationV1Api", MagicMock())
    mgr = SparkOperatorManager.__new__(SparkOperatorManager)
    mgr.namespace = "spark-operator"
    with deadline.deploy_deadline(5):
        clock.sleep(4)
        mgr._assign_openshift_scc()  # logs the SCCGrantError, no DeployTimeout
    assert calls == [False, False]


# --- static guard -------------------------------------------------------------------
