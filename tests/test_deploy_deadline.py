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
    dep.spec.replicas = 1
    d = thrift.SparkThriftDeployer.__new__(thrift.SparkThriftDeployer)
    with (
        patch("kubernetes.client.AppsV1Api") as api,
        deadline.deploy_deadline(40),
        deadline.component("spark-thrift"),
    ):
        api.return_value.read_namespaced_deployment.return_value = dep
        with pytest.raises(deadline.DeployTimeout) as e:
            d._wait_for_ready("ns", timeout_seconds=deadline.clamp(300))
    assert "spark-thrift: deployment lakebench-spark-thrift (0/1 ready)" in str(e.value)
    assert clock.now - 1000.0 <= 50


def test_operator_rollout_timeout_is_clamped(clock):
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    m = SparkOperatorManager.__new__(SparkOperatorManager)
    m.namespace = "spark-operator"
    m._run = MagicMock(return_value=MagicMock(returncode=0, stderr=""))
    with deadline.deploy_deadline(50):
        assert m._wait_for_rollout(timeout_s=deadline.clamp(180))
        args = [c.args[0] for c in m._run.call_args_list]
        assert all("--timeout=50s" in a for a in args), args
        clock.sleep(50)
        m._run.reset_mock()
        with pytest.raises(deadline.DeployTimeout):
            m._wait_for_rollout(timeout_s=180)
        m._run.assert_not_called()


# --- static guard -------------------------------------------------------------------

_TIMEOUT_KW = {"timeout", "timeout_s", "timeout_seconds"}
_CLAMPS = {"clamp", "lease_clamp", "clamp_whole_seconds"}


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
    return files


def test_wait_py_waits_all_go_through_wait_for_condition():
    """So a call to any of them is clamped inside wait_for_condition."""
    tree = ast.parse((SRC / "k8s" / "wait.py").read_text())
    names = _wait_py_functions()
    for fn in tree.body:
        if isinstance(fn, ast.FunctionDef) and fn.name in names - {"wait_for_condition"}:
            called = {_callee(c) for c in ast.walk(fn) if isinstance(c, ast.Call)}
            assert called & names, f"{fn.name} waits without wait_for_condition"


def test_every_deploy_wait_is_clamped():
    """A call to a wait function in the deploy path passes a timeout derived
    from clamp (or SD-12's lease_clamp), or is a k8s/wait.py wait, which
    wait_for_condition clamps. A kubectl rollout status never carries a
    literal --timeout."""
    exempt = _wait_py_functions()
    bad = []
    for path in _scanned():
        tree = ast.parse(path.read_text())
        rel = path.relative_to(SRC)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                name = _callee(node)
                if re.match(r"^_?wait_for", name) and name not in exempt:
                    ok = any(
                        kw.arg in _TIMEOUT_KW
                        and isinstance(kw.value, ast.Call)
                        and _callee(kw.value) in _CLAMPS
                        for kw in node.keywords
                    )
                    if not ok:
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
                            clamped = any(
                                isinstance(c, ast.Call) and _callee(c) in _CLAMPS
                                for c in ast.walk(e)
                            )
                            if not clamped:
                                bad.append(
                                    f"{rel}:{e.lineno} kubectl rollout status with an unclamped --timeout"
                                )
    assert not bad, "\n".join(bad)
