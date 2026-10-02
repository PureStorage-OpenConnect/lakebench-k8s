"""DEP-6: the deploy timeout inside every wait (ch01 s8)."""

from __future__ import annotations

import ast
import re
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy import deadline
from lakebench.deploy.engine import DeploymentEngine, DeploymentResult, DeploymentStatus
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


def test_clamp_and_check(clock):
    with deadline.deploy_deadline(100):
        assert deadline.clamp(600) == 100
        assert deadline.clamp(30) == 30
        clock.sleep(70)
        assert deadline.clamp(600) == 30
        deadline.check("x")
        clock.sleep(30)
        assert deadline.clamp(600) == 0
        with deadline.component("trino"), pytest.raises(deadline.DeployTimeout) as e:
            deadline.check("deployment lakebench-trino", "0/1 ready")
    assert str(e.value) == (
        "deploy timeout (100 s) reached after 100 s while waiting for trino: "
        "deployment lakebench-trino (0/1 ready)"
    )
    assert deadline.remaining() is None


def test_zero_timeout_means_no_deadline():
    with deadline.deploy_deadline(0):
        assert deadline.remaining() is None


def test_deploy_timeout_is_not_retried_as_transient():
    exc = deadline.DeployTimeout("c", "w", 1, 1)
    assert not isinstance(exc, TimeoutError)
    assert not DeploymentEngine._is_transient_error(exc)


def test_clamp_whole_seconds_never_passes_zero(clock):
    with deadline.deploy_deadline(10):
        clock.sleep(8.5)
        assert deadline.clamp_whole_seconds(120, "rollout") == 2
        clock.sleep(1.0)
        # kubectl and helm read --timeout=0 as "wait forever".
        with pytest.raises(deadline.DeployTimeout):
            deadline.clamp_whole_seconds(120, "rollout")


def test_deadline_is_per_thread():
    seen = []
    with deadline.deploy_deadline(100):
        t = threading.Thread(target=lambda: seen.append(deadline.remaining()))
        t.start()
        t.join()
    assert seen == [None]


# --- waits ------------------------------------------------------------------------


def test_wait_for_condition_raises_when_the_deadline_cuts_it(clock):
    with deadline.deploy_deadline(30), deadline.component("duckdb"):
        with pytest.raises(deadline.DeployTimeout) as e:
            wait_mod.wait_for_condition(
                lambda: (False, "0/1 ready"), timeout_seconds=900, description="pod duck"
            )
    assert "while waiting for duckdb: pod duck (0/1 ready)" in str(e.value)
    assert clock.slept <= 30


def test_wait_for_condition_keeps_its_own_timeout_when_shorter(clock):
    with deadline.deploy_deadline(3600):
        r = wait_mod.wait_for_condition(
            lambda: (False, "nope"), timeout_seconds=60, description="x"
        )
    assert r.status == wait_mod.WaitStatus.TIMEOUT


def _engine():
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.results = []
    return eng


def test_stuck_wait_exits_at_deploy_timeout_naming_component(clock):
    """The failing case of today's code: a 900 s wait inside one step ran
    past a 60 s deploy timeout, which was checked only between steps."""

    def stuck():
        wait_mod.wait_for_condition(
            lambda: (False, "0/1 ready"),
            timeout_seconds=900,
            description="deployment lakebench-duckdb",
        )
        return DeploymentResult(component="duckdb", status=DeploymentStatus.SUCCESS, message="ok")

    later = MagicMock()
    steps = [
        ("duckdb", "Deploying DuckDB", stuck),
        ("observability", "Deploying Observability", later),
    ]
    with deadline.deploy_deadline(60):
        results = _engine()._run_steps(steps, None)
    assert [r.status for r in results] == [DeploymentStatus.FAILED]
    assert results[0].message.startswith("deploy timeout (60 s) reached after 60 s")
    assert "duckdb: deployment lakebench-duckdb (0/1 ready)" in results[0].message
    later.assert_not_called()
    assert clock.now - 1000.0 <= 61


def test_between_steps_check_names_the_next_step(clock):
    def slow():
        clock.sleep(61)  # non-wait work, e.g. a helm call, finishes first
        return DeploymentResult(component="trino", status=DeploymentStatus.SUCCESS, message="ok")

    steps = [("trino", "Deploying Trino", slow), ("duckdb", "Deploying DuckDB", MagicMock())]
    with deadline.deploy_deadline(60):
        results = _engine()._run_steps(steps, None)
    assert [r.component for r in results] == ["trino", "duckdb"]
    assert results[1].status == DeploymentStatus.FAILED
    assert "while waiting for duckdb: Deploying DuckDB" in results[1].message


def test_deploy_all_sets_the_deadline(monkeypatch):
    from tests.test_deploy import _make_config, _mock_k8s

    seen = {}

    def fake_run_steps(self, steps, cb):
        seen["remaining"] = deadline.remaining()
        seen["components"] = [c for c, _, _ in steps]
        return []

    monkeypatch.setattr(DeploymentEngine, "_run_steps", fake_run_steps)
    with patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False):
        eng = DeploymentEngine(_make_config(), k8s_client=_mock_k8s())
        eng.deploy_all(timeout=1234)
        assert 1230 < seen["remaining"] <= 1234
        assert seen["components"][0] == "namespace"
        eng.deploy_all(timeout=0)
        assert seen["remaining"] is None
    assert deadline.remaining() is None


def test_thrift_wait_is_cut_by_the_deadline(clock, monkeypatch):
    from lakebench.modules.query_engines.spark_thrift import deployer as thrift

    for name in ("time", "sleep"):
        monkeypatch.setattr(thrift.time, name, getattr(clock, name))
    dep = MagicMock()
    dep.status.ready_replicas = 0
    dep.status.replicas = dep.status.updated_replicas = dep.status.observed_generation = 1
    dep.metadata.generation = 1
    dep.spec.replicas = 1
    dep.spec.selector.match_labels = {"app.kubernetes.io/component": "spark-thrift-server"}
    d = thrift.SparkThriftDeployer.__new__(thrift.SparkThriftDeployer)
    d.engine = MagicMock(deps=None)
    with (
        patch("kubernetes.client.AppsV1Api") as api,
        patch("kubernetes.client.CoreV1Api") as core,
        deadline.deploy_deadline(40),
        deadline.component("spark-thrift"),
    ):
        api.return_value.read_namespaced_deployment.return_value = dep
        core.return_value.list_namespaced_pod.return_value.items = []
        with pytest.raises(deadline.DeployTimeout) as e:
            d._wait_for_ready("ns", timeout_seconds=deadline.clamp(300))
    assert "spark-thrift: Spark Thrift Server (lakebench-spark-thrift) to roll out" in str(e.value)
    assert "0/1 ready" in str(e.value)
    assert clock.now - 1000.0 <= 50


def test_post_upgrade_rollout_is_not_cut_by_the_deadline(clock):
    from lakebench.modules.pipeline_engines.spark import operator as op

    m = op.SparkOperatorManager.__new__(op.SparkOperatorManager)
    m.namespace = "spark-operator"
    m._run = MagicMock(return_value=MagicMock(returncode=0, stderr=""))
    with deadline.deploy_deadline(5):
        clock.sleep(10)
        assert m._rollout_status_after_upgrade()
    args = [c.args[0] for c in m._run.call_args_list]
    assert len(args) == 2 and all(f"--timeout={op._POST_UPGRADE_ROLLOUT_S}s" in a for a in args)


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


def test_committed_install_reports_not_ready_not_a_timeout(clock, monkeypatch):
    """After a committed operator install the ready wait keeps its own bound
    and its own message; the deadline does not relabel it."""
    from lakebench.modules.pipeline_engines.spark import operator as op

    monkeypatch.setattr(op.time, "time", clock.time)
    monkeypatch.setattr(op.time, "sleep", clock.sleep)
    m = _operator(clock)
    m.target_version = "2.5.1"
    m.job_namespace = "mine"
    m._release_exists = MagicMock(return_value=False)
    m.check_status = MagicMock(return_value=MagicMock(ready=False, message="0/1 ready"))

    def run(cmd, **k):
        if cmd[:2] == ["helm", "upgrade"]:
            clock.sleep(20)  # the install outlives the deadline
        return MagicMock(returncode=0, stderr="")

    m._run = MagicMock(side_effect=run)
    with deadline.deploy_deadline(10):
        assert m.install() is False


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


def test_transient_error_is_not_retried_after_the_deadline(clock):
    from kubernetes.client.rest import ApiException

    calls = []

    def flaky():
        calls.append(1)
        clock.sleep(11)
        raise ApiException(status=503)

    with deadline.deploy_deadline(10):
        results = _engine()._run_steps([("trino", "Deploying Trino", flaky)], None)
    assert len(calls) == 1
    assert results[0].status == DeploymentStatus.FAILED


def test_no_stackable_install_after_the_deadline(clock):
    from lakebench.modules.catalogs.hive import deployer as hive

    d = hive.HiveDeployer.__new__(hive.HiveDeployer)
    d.config = MagicMock()
    with patch("lakebench.k8s.pinned_helm") as helm, deadline.deploy_deadline(5):
        clock.sleep(5)
        with pytest.raises(deadline.DeployTimeout) as e:
            d._install_stackable_operators()
    helm.assert_not_called()
    assert "helm install of Stackable commons-operator" in str(e.value)


def test_scc_verify_poll_stops_at_the_deadline(clock):
    """The post-grant review poll (15 s) is cut by the deploy deadline and
    reports the deadline, not "the grant did not take effect"."""
    from lakebench.k8s import security

    rbac = MagicMock()
    rbac.read_namespaced_role_binding.side_effect = __import__(
        "kubernetes.client.rest", fromlist=["ApiException"]
    ).ApiException(status=404)
    authz = MagicMock()
    authz.create_namespaced_local_subject_access_review.return_value = {
        "status": {"allowed": False}
    }
    with deadline.deploy_deadline(5):
        clock.sleep(3)
        with pytest.raises(deadline.DeployTimeout):
            security.ensure_scc_rolebinding(rbac, "ns", "sa", authz_api=authz)
    assert clock.now == pytest.approx(1005.0)  # stopped at the deadline, not 15 s later
    without = MagicMock()
    without.create_namespaced_local_subject_access_review.return_value = {
        "status": {"allowed": False}
    }
    with pytest.raises(security.SCCGrantError):
        security.ensure_scc_rolebinding(rbac, "ns", "sa", authz_api=without)


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

_TIMEOUT_KW = {"timeout", "timeout_s", "timeout_seconds"}
_CLAMPS = {"clamp", "lease_clamp", "clamp_whole_seconds"}
_CHECKS = _CLAMPS | {"check"}
# Waits after a committed shared mutation: bounded by their own timeout,
# never by the deploy deadline (operator.py _POST_UPGRADE_*).
_POST_UPGRADE = re.compile(r"^_POST_UPGRADE_\w+_S$")
# Files whose code never runs inside deploy_all, so no deploy deadline is set.
_OUTSIDE_DEPLOY = {"deploy/destroy.py", "deploy/datagen.py", "deploy/garage.py", "deploy/local.py"}
# Functions on the destroy path inside the scanned files (no deploy deadline).
_DESTROY_PATH = {
    ("modules/pipeline_engines/spark/operator.py", "_remove_namespace_from_watch_locked"),
    ("modules/pipeline_engines/spark/operator.py", "_remove_namespace_from_watch_unlocked"),
}
# Sleep loops in the scanned files that need not consult the deadline, and why.
_SLEEP_LOOP_ALLOWED = {
    (
        "deploy/cluster_lock.py",
        "acquire_cluster_lock",
    ): "its timeout is clamped at each deploy-path call site",
    ("deploy/ownership.py", "stamp_namespace"): "409 retry, 0.2 s steps, bounded attempts",
    (
        "modules/pipeline_engines/spark/operator.py",
        "_namespace_is_terminating",
    ): "fixed short read backoff before the gated upgrade",
    (
        "modules/pipeline_engines/spark/operator.py",
        "_remove_namespace_from_watch_unlocked",
    ): "destroy path",
    (
        "modules/pipeline_engines/spark/operator.py",
        "_verify_namespace_watched",
    ): "post-upgrade verify, deliberately not cut",
}


def _callee(call: ast.Call) -> str:
    f = call.func
    return f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else ""


def _wait_py_functions() -> set[str]:
    tree = ast.parse((SRC / "k8s" / "wait.py").read_text())
    return {
        n.name
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name.startswith("wait_for")
    }


def _scanned() -> list[Path]:
    files = sorted((SRC / "deploy").glob("*.py"))
    files += sorted((SRC / "modules").glob("*/*/deployer.py"))
    files.append(SRC / "modules" / "pipeline_engines" / "spark" / "operator.py")
    files.append(SRC / "k8s" / "security.py")
    return [f for f in files if str(f.relative_to(SRC)) not in _OUTSIDE_DEPLOY]


def _bounded(value: ast.expr) -> bool:
    if isinstance(value, ast.Call) and _callee(value) in _CLAMPS:
        return True
    return isinstance(value, ast.Name) and bool(_POST_UPGRADE.match(value.id))


def test_wait_py_waits_all_go_through_wait_for_condition():
    """So a call to any of them is clamped inside wait_for_condition."""
    tree = ast.parse((SRC / "k8s" / "wait.py").read_text())
    names = _wait_py_functions()
    for fn in tree.body:
        if isinstance(fn, ast.FunctionDef) and fn.name in names - {"wait_for_condition"}:
            called = {_callee(c) for c in ast.walk(fn) if isinstance(c, ast.Call)}
            assert called & names, f"{fn.name} waits without wait_for_condition"


def test_every_deploy_wait_is_clamped():
    """In the deploy path: a call to a wait function or to cluster_lock
    passes a clamped timeout (or a post-upgrade constant), or is a
    k8s/wait.py wait, which wait_for_condition clamps; a kubectl rollout
    status --timeout is clamped or post-upgrade; and every function with a
    sleep loop consults the deadline or is listed with its reason."""
    exempt = _wait_py_functions()
    bad = []
    for path in _scanned():
        tree = ast.parse(path.read_text())
        rel = str(path.relative_to(SRC))
        destroy_calls = {
            id(c)
            for fn in ast.walk(tree)
            if isinstance(fn, ast.FunctionDef) and (rel, fn.name) in _DESTROY_PATH
            for c in ast.walk(fn)
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and id(node) not in destroy_calls:
                name = _callee(node)
                if (
                    re.match(r"^_?wait_for", name) and name not in exempt
                ) or name == "cluster_lock":
                    if not any(
                        kw.arg in _TIMEOUT_KW and _bounded(kw.value) for kw in node.keywords
                    ):
                        bad.append(f"{rel}:{node.lineno} {name}(...) without a clamped timeout")
            if isinstance(node, ast.List):
                consts = [
                    e.value
                    for e in node.elts
                    if isinstance(e, ast.Constant) and isinstance(e.value, str)
                ]
                if "rollout" in consts and "status" in consts:
                    for e in node.elts:
                        text = [
                            c.value
                            for c in ast.walk(e)
                            if isinstance(c, ast.Constant) and isinstance(c.value, str)
                        ]
                        if any(t.startswith("--timeout") for t in text):
                            if not any(_bounded(c) for c in ast.walk(e) if isinstance(c, ast.expr)):
                                bad.append(
                                    f"{rel}:{e.lineno} kubectl rollout status with an unclamped --timeout"
                                )
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                loops = [n for n in ast.walk(node) if isinstance(n, (ast.While, ast.For))]
                sleeps = any(
                    isinstance(c, ast.Call) and _callee(c) == "sleep"
                    for lp in loops
                    for c in ast.walk(lp)
                )
                uses = any(
                    isinstance(c, ast.Call) and _callee(c) in _CHECKS for c in ast.walk(node)
                )
                if sleeps and not uses and (rel, node.name) not in _SLEEP_LOOP_ALLOWED:
                    bad.append(
                        f"{rel}:{node.lineno} {node.name} sleeps in a loop without the deploy deadline"
                    )
    assert not bad, "\n".join(bad)


def test_sleep_loop_allowlist_is_current():
    """An allowlisted function that is gone or now consults the deadline is
    removed from the list, so the list cannot hide a new loop by name."""
    present = set()
    for path in _scanned():
        rel = str(path.relative_to(SRC))
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                present.add((rel, node.name))
    assert set(_SLEEP_LOOP_ALLOWED) <= present
    assert _DESTROY_PATH <= present
