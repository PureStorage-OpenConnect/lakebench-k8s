"""Signals, children and API calls while the cluster lease is held (SD-22).

Cluster-safety 2 (DESIGN ch01 3.6): a Ctrl-C or SIGTERM inside the lease used
to stop the body between the helm upgrade and the operator restart, and a
terminal Ctrl-C reached the helm child directly, which leaves the shared
release ``pending-upgrade`` and blocks every watch-list change on the
cluster. These tests run the real ``cluster_lock`` and ``_pinned`` helpers
against the SD-9 recording fixture.
"""

from __future__ import annotations

import ast
import copy
import os
import signal
import subprocess
import threading
from pathlib import Path

import pytest

from lakebench.deploy import cluster_lock as cl
from lakebench.k8s import _pinned
from tests.fixtures.recording_k8s import K8sRecorder, recording

NS = "u01"
SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench"


def _core():
    from kubernetes import client

    return client.CoreV1Api()


def _cm(name: str):
    from kubernetes.client.models import V1ConfigMap, V1ObjectMeta

    return V1ConfigMap(metadata=V1ObjectMeta(name=name))


def _lease_deleted(rec: K8sRecorder) -> bool:
    return any(
        c.verb == "delete" and c.kind == "configmaps" and c.name == cl.LOCK_CONFIGMAP_NAME
        for c in rec.calls
    )


@pytest.fixture
def own_ns(monkeypatch):
    with recording(NS) as rec:
        rec.add_namespace(NS)
        yield rec


# ---------------------------------------------------------------------------
# Deferral
# ---------------------------------------------------------------------------


def test_sigint_inside_lease_is_deferred(own_ns, capsys):
    """Both mutations run, the lease is released, then KeyboardInterrupt.

    With the deferral removed the SIGINT raises at once and the second
    mutation never runs.
    """
    rec = own_ns
    reached_end = False
    with pytest.raises(KeyboardInterrupt):
        with cl.cluster_lock(_core(), timeout=0):
            _core().create_namespaced_config_map(NS, _cm("first"))
            os.kill(os.getpid(), signal.SIGINT)
            _core().create_namespaced_config_map(NS, _cm("second"))
            reached_end = True
            assert cl.lease_held()
    assert reached_end
    assert [c.name for c in rec.calls if c.verb == "create" and c.namespace == NS] == [
        "first",
        "second",
    ]
    assert _lease_deleted(rec) and not cl.lease_held()
    err = capsys.readouterr().err
    assert "interrupt received while holding the cluster lease" in err
    assert "hold budget" in err and "Interrupt twice more to abort now" in err


def test_sigterm_reaches_the_saved_handler_after_release(own_ns):
    """The CD-16 contract: a command's own SIGTERM handler runs after the release."""
    rec = own_ns
    seen: list[tuple[int, bool]] = []

    def handler(signum, _frame):
        seen.append((signum, _lease_deleted(rec)))

    previous = signal.signal(signal.SIGTERM, handler)
    try:
        with cl.cluster_lock(_core(), timeout=0):
            os.kill(os.getpid(), signal.SIGTERM)
            assert seen == []  # held back while the lease is held
        assert seen == [(signal.SIGTERM, True)]
        assert signal.getsignal(signal.SIGTERM) is handler
    finally:
        signal.signal(signal.SIGTERM, previous)


def test_second_signal_repeats_the_time_left(own_ns, capsys):
    with pytest.raises(KeyboardInterrupt):
        with cl.cluster_lock(_core(), timeout=0):
            os.kill(os.getpid(), signal.SIGINT)
            os.kill(os.getpid(), signal.SIGINT)
            _core().create_namespaced_config_map(NS, _cm("still-runs"))
    assert "Interrupt once more to abort now" in capsys.readouterr().err
    assert ("configmaps", NS, "still-runs") in own_ns.store


def test_third_signal_aborts_but_still_releases(own_ns, capsys):
    rec = own_ns
    before = signal.getsignal(signal.SIGINT)
    after_third = False
    with pytest.raises(KeyboardInterrupt):
        with cl.cluster_lock(_core(), timeout=0):
            for _ in range(3):
                os.kill(os.getpid(), signal.SIGINT)
            after_third = True
    assert not after_third
    assert _lease_deleted(rec)
    assert signal.getsignal(signal.SIGINT) is before
    assert "run `lakebench admin repair-operator`" in capsys.readouterr().err


def test_handlers_restored_after_a_clean_exit(own_ns):
    before = {s: signal.getsignal(s) for s in cl._DEFERRED_SIGNALS}
    with cl.cluster_lock(_core(), timeout=0):
        assert signal.getsignal(signal.SIGINT) is not before[signal.SIGINT]
    assert {s: signal.getsignal(s) for s in cl._DEFERRED_SIGNALS} == before


def test_body_error_still_releases_and_restores(own_ns):
    before = signal.getsignal(signal.SIGINT)
    with pytest.raises(RuntimeError, match="boom"):
        with cl.cluster_lock(_core(), timeout=0):
            raise RuntimeError("boom")
    assert _lease_deleted(own_ns) and signal.getsignal(signal.SIGINT) is before


def test_off_main_thread_the_lease_works_without_deferral(own_ns):
    before = signal.getsignal(signal.SIGINT)
    seen: dict[str, object] = {}

    def worker():
        with cl.cluster_lock(_core(), timeout=0):
            seen["held"] = cl.lease_held()
            seen["handler"] = signal.getsignal(signal.SIGINT)

    t = threading.Thread(target=worker)
    t.start()
    t.join(10)
    assert seen == {"held": True, "handler": before}
    assert _lease_deleted(own_ns)


def test_lease_held_and_budget_only_inside(own_ns):
    assert not cl.lease_held() and cl.lease_hold_remaining() is None
    assert cl.lease_clamp(999.0) == 999.0
    with cl.cluster_lock(_core(), timeout=0, max_hold_s=100):
        assert cl.lease_held()
        remaining = cl.lease_hold_remaining()
        assert remaining is not None and 0 < remaining <= 100
        assert cl.lease_clamp(999.0) <= 100 and cl.lease_clamp(5.0) == 5.0
    assert not cl.lease_held() and cl.lease_hold_remaining() is None


def test_lease_budget_fits_the_ttl():
    assert cl.LEASE_MAX_HOLD_S < cl.ADMIN_MAX_HOLD_S < cl.DEFAULT_TTL_SEC


# ---------------------------------------------------------------------------
# Children under the lease
# ---------------------------------------------------------------------------


def _kwargs(rec: K8sRecorder, api: str) -> list[dict]:
    return [c.kwargs for c in rec.calls if c.api == api]


def test_pinned_helm_new_session_under_lease(own_ns):
    """Outside the lease nothing changes; inside, a new session and a timeout."""
    rec = own_ns
    upgrade = ["upgrade", "spark-operator", "chart", "-n", "spark-operator"]
    _pinned.pinned_helm("c", upgrade)
    with cl.cluster_lock(_core(), timeout=0):
        _pinned.pinned_helm("c", upgrade)
        _pinned.pinned_kubectl("c", ["rollout", "restart", "deploy/x", "-n", "spark-operator"])
        _pinned.pinned_oc("c", ["adm", "policy", "add-scc-to-user", "anyuid", "-z", "s"])
    helm_out, helm_in = [c for c in rec.calls if c.api == "helm"]
    assert "start_new_session" not in helm_out.kwargs and "timeout" not in helm_out.kwargs
    assert "--timeout" not in helm_out.argv
    assert helm_in.kwargs["start_new_session"] is True
    sub_timeout = helm_in.kwargs["timeout"]
    # The recovery reserve is kept back from a mutating helm call.
    assert 0 < sub_timeout <= cl.LEASE_MAX_HOLD_S - _pinned.HELM_RECOVERY_RESERVE_S
    argv = list(helm_in.argv)
    helm_timeout = argv[argv.index("--timeout") + 1]
    # No caller --timeout: helm's 300 s default, which is under the limit.
    assert helm_timeout == "300s" < f"{int(sub_timeout) - _pinned.HELM_TIMEOUT_MARGIN_S}s"
    for api in ("kubectl", "oc"):
        [kw] = _kwargs(rec, api)
        assert kw["start_new_session"] is True and kw["timeout"] <= cl.LEASE_MAX_HOLD_S


def test_caller_choices_are_kept_when_tighter(own_ns):
    rec = own_ns
    with cl.cluster_lock(_core(), timeout=0):
        _pinned.pinned_helm(
            "c",
            ["upgrade", "r", "chart", "--timeout", "2m"],
            timeout=200,
            start_new_session=False,
        )
        _pinned.pinned_helm("c", ["install", "r2", "chart", "--timeout=1h"])
        _pinned.pinned_helm("c", ["get", "values", "spark-operator", "-o", "json"])
    first, second, read = [c for c in rec.calls if c.api == "helm"]
    assert first.kwargs["start_new_session"] is False and first.kwargs["timeout"] == 200
    assert "2m" in first.argv  # 120 s is under the 170 s limit: kept
    limit = int(second.kwargs["timeout"]) - _pinned.HELM_TIMEOUT_MARGIN_S
    assert f"--timeout={limit}s" in second.argv  # 1h is longer than the budget
    assert "--timeout" not in read.argv  # reads get a subprocess timeout only


def test_leased_timeout_stops_helm_gently_and_names_the_recovery(own_ns):
    """SIGTERM first (helm marks the release failed), not the SIGKILL of subprocess.run."""
    rec = own_ns
    rec.on_command("helm", "upgrade", raises=subprocess.TimeoutExpired(["helm"], 5))
    with cl.cluster_lock(_core(), timeout=0):
        with pytest.raises(subprocess.TimeoutExpired) as exc:
            _pinned.pinned_helm("c", ["upgrade", "spark-operator", "chart"])
    assert isinstance(exc.value, _pinned.LeasedCommandTimeout)
    text = str(exc.value)
    assert "helm rollback" in text and "repair-operator" in text
    assert rec.processes[-1].signals == [signal.SIGTERM]
    rec.on_command("helm", "status", raises=subprocess.TimeoutExpired(["helm"], 5))
    with pytest.raises(subprocess.TimeoutExpired) as exc2:
        _pinned.pinned_helm("c", ["status", "x"])
    assert not isinstance(exc2.value, _pinned.LeasedCommandTimeout)


def test_popen_under_lease_gets_a_new_session(own_ns):
    rec = own_ns
    with cl.cluster_lock(_core(), timeout=0):
        _pinned.pinned_kubectl_popen("c", ["logs", "-f", "x", "-n", NS])
    [kw] = _kwargs(rec, "kubectl")
    assert kw["start_new_session"] is True


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("5m", 300), ("300s", 300), ("1h2m3s", 3723), ("90", 90), ("1.5m", 90), ("bad", None)],
)
def test_helm_duration(text, seconds):
    assert _pinned._helm_duration_s(text) == seconds


# ---------------------------------------------------------------------------
# API calls under the lease
# ---------------------------------------------------------------------------


def test_lease_api_calls_carry_request_timeouts(own_ns):
    """Every call the lease itself makes, acquire to release, is bounded."""
    rec = own_ns
    rec.seed_lease(acquired_at="2020-01-01T00:00:00+00:00", ttl_seconds=60)  # steal path
    with cl.cluster_lock(_core(), timeout=0):
        pass
    lease_calls = [
        c
        for c in rec.calls
        if c.name in (cl.LOCK_CONFIGMAP_NAME, cl.LOCK_NAMESPACE) or c.namespace == cl.LOCK_NAMESPACE
    ]
    assert {c.verb for c in lease_calls} >= {"read", "replace", "delete"}
    assert all(c.kwargs.get("_request_timeout") == cl.LEASE_REQUEST_TIMEOUT for c in lease_calls)


def test_real_destroy_leased_api_calls_carry_request_timeouts():
    """Run unmodified destroy: every API call inside the lease has a timeout."""
    from tests.test_recording_k8s_fixture import _destroy_under_recorder

    with recording() as rec:
        _destroy_under_recorder(rec)
        leased_api = [c for c in rec.calls if c.lease_held and c.api.endswith("Api")]
        assert any(c.kind == "namespaces" and c.verb == "delete" for c in leased_api)
        missing = [c.describe() for c in leased_api if "_request_timeout" not in c.kwargs]
        assert missing == []
        leased_cli = [c for c in rec.calls if c.lease_held and c.api in ("helm", "kubectl")]
        assert leased_cli and all(c.kwargs.get("start_new_session") for c in leased_cli)


# Functions whose Kubernetes client calls run inside (or to take) the lease.
_LEASED_FUNCTIONS = {
    "deploy/cluster_lock.py": {
        "_ensure_lock_namespace",
        "read_cluster_lock",
        "_delete_if_unchanged",
        "_try_acquire_once",
        "release_cluster_lock",
        "force_release_cluster_lock",
    },
    # deploy's and run's heal path, inside the lease.
    "modules/pipeline_engines/spark/operator.py": {
        "_namespace_is_terminating",
        "_filter_existing_namespaces",
    },
    # destroy, inside the lease: the SAF-4 operator pod list (3.3) and the
    # legacy SecretClass refcount and deletes (3.2).
    "deploy/destroy.py": {
        "_operator_pods_listing",
        "_legacy_secretclass_cleanup_locked",
    },
    # destroy's _delete_in_lease reads and deletes the namespace through these.
    "k8s/client.py": {
        "namespace_exists",
        "get_namespace_phase",
        "get_namespace_uid",
        "get_namespace_annotation",
        "get_namespace_termination_status",
        "delete_namespace",
    },
}
_CLIENT_VERBS = ("read_", "create_", "replace_", "patch_", "delete_", "list_")


def test_leased_api_calls_have_timeouts():
    """[static] a client call in a leased function without a request timeout fails."""
    bad: list[str] = []
    for rel, names in _LEASED_FUNCTIONS.items():
        tree = ast.parse((SRC / rel).read_text(encoding="utf-8"))
        found = set()
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef) or fn.name not in names:
                continue
            found.add(fn.name)
            for call in ast.walk(fn):
                if not (
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and call.func.attr.startswith(_CLIENT_VERBS)
                ):
                    continue
                owner = ast.unparse(call.func.value)
                if not (
                    owner in ("core_v1", "self._core_v1", "custom_api") or owner.endswith("Api()")
                ):
                    continue
                kws = {k.arg for k in call.keywords}
                if "_request_timeout" not in kws and None not in kws:  # None: **kwargs
                    bad.append(f"{rel}:{call.lineno} {fn.name}: {ast.unparse(call.func)}")
        assert found == names, f"{rel}: functions renamed or removed: {names - found}"
    assert bad == []


def _starts_threads(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in (
            "threading",
            "concurrent.futures",
            "multiprocessing.pool",
        ):
            return True
        if isinstance(node, ast.Attribute) and node.attr in (
            "Thread",
            "ThreadPoolExecutor",
            "ProcessPoolExecutor",
            "to_thread",
            "run_in_executor",
            "start_new_thread",
        ):
            return True
    return False


def test_leased_paths_run_on_main_thread():
    """[static] no module that takes the lease, or that they import, starts threads.

    Signal deferral needs the main thread (signal.signal is main-thread
    only). The CLI calls deploy, destroy, run's heal and the admin verbs on
    its main thread; this guards the leased side: every module with a
    cluster_lock call site, and every lakebench module it imports, starts no
    thread or executor. Off the main thread cluster_lock also logs a warning.
    """
    trees = {
        p.relative_to(SRC).as_posix(): ast.parse(p.read_text(encoding="utf-8"))
        for p in SRC.rglob("*.py")
        if "/spark/scripts/" not in p.as_posix()
    }
    lease_modules = {
        rel
        for rel, tree in trees.items()
        if rel != "deploy/cluster_lock.py"
        and any(
            isinstance(n, ast.Call)
            and isinstance(n.func, (ast.Name, ast.Attribute))
            and getattr(n.func, "id", getattr(n.func, "attr", "")) == "cluster_lock"
            for n in ast.walk(tree)
        )
    }
    assert lease_modules >= {
        "cli/_admin.py",
        "deploy/observability.py",
        "modules/pipeline_engines/spark/operator.py",
    }

    def imported(rel: str) -> set[str]:
        out = set()
        for n in ast.walk(trees[rel]):
            mods = []
            if isinstance(n, ast.ImportFrom) and n.module and n.module.startswith("lakebench."):
                mods = [n.module]
            elif isinstance(n, ast.Import):
                mods = [a.name for a in n.names if a.name.startswith("lakebench.")]
            for m in mods:
                path = m.removeprefix("lakebench.").replace(".", "/")
                for cand in (f"{path}.py", f"{path}/__init__.py"):
                    if cand in trees:
                        out.add(cand)
        return out

    reach = set(lease_modules) | {"deploy/cluster_lock.py", "deploy/destroy.py"}
    for rel in list(reach):
        reach |= imported(rel)
    threaded = sorted(rel for rel in reach if _starts_threads(trees[rel]))
    assert threaded == [], f"modules on the leased paths start threads: {threaded}"


# ---------------------------------------------------------------------------
# Review round 1 (SD-22 Full review)
# ---------------------------------------------------------------------------


def test_mutating_helm_refused_without_enough_budget(own_ns):
    """3.7: a helm mutation is not started with under 60 s of its phase left."""
    rec = own_ns
    with cl.cluster_lock(_core(), timeout=0, max_hold_s=100):  # 100 - 60 reserve < 60
        with pytest.raises(cl.LeaseHoldExceeded):
            _pinned.pinned_helm("c", ["upgrade", "spark-operator", "chart"])
        _pinned.pinned_helm("c", ["get", "values", "spark-operator", "-o", "json"])  # a read runs
    assert [c.verb for c in rec.calls if c.api == "helm"] == ["get"]


def test_any_call_refused_once_the_budget_is_spent(own_ns):
    import time

    with cl.cluster_lock(_core(), timeout=0, max_hold_s=0.01):
        time.sleep(0.05)
        with pytest.raises(cl.LeaseHoldExceeded):
            _pinned.pinned_kubectl("c", ["get", "ns"])


def test_signals_during_the_release_cannot_leak_the_lease(own_ns, monkeypatch):
    """Ctrl-C mashed while the lease is released: the release still completes."""
    rec = own_ns
    real_release = cl.release_cluster_lock

    def interrupted_release(core_v1, handle):
        for _ in range(3):
            os.kill(os.getpid(), signal.SIGINT)
        real_release(core_v1, handle)

    monkeypatch.setattr(cl, "release_cluster_lock", interrupted_release)
    with pytest.raises(KeyboardInterrupt):
        with cl.cluster_lock(_core(), timeout=0):
            pass
    assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) not in rec.store


def test_a_signal_after_the_abort_cannot_leak_the_lease(own_ns, monkeypatch):
    rec = own_ns
    real_release = cl.release_cluster_lock

    def interrupted_release(core_v1, handle):
        os.kill(os.getpid(), signal.SIGINT)  # the fourth Ctrl-C
        real_release(core_v1, handle)

    monkeypatch.setattr(cl, "release_cluster_lock", interrupted_release)
    with pytest.raises(cl.LeaseAbort) as exc:
        with cl.cluster_lock(_core(), timeout=0):
            for _ in range(3):
                os.kill(os.getpid(), signal.SIGINT)
    assert exc.value.signum == signal.SIGINT
    assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) not in rec.store


def test_abort_carries_the_signal_for_an_interrupt_record(own_ns):
    with pytest.raises(cl.LeaseAbort) as exc:
        with cl.cluster_lock(_core(), timeout=0):
            for _ in range(3):
                os.kill(os.getpid(), signal.SIGTERM)
    assert exc.value.signum == signal.SIGTERM and isinstance(exc.value, KeyboardInterrupt)


def test_ignored_signal_stays_ignored(own_ns):
    previous = signal.signal(signal.SIGHUP, signal.SIG_IGN)
    try:
        finished = False
        with cl.cluster_lock(_core(), timeout=0):
            for _ in range(3):
                os.kill(os.getpid(), signal.SIGHUP)
            finished = True
        assert finished and signal.getsignal(signal.SIGHUP) == signal.SIG_IGN
    finally:
        signal.signal(signal.SIGHUP, previous)


def test_abort_stops_a_running_child_gently(own_ns):
    """The third interrupt lands while helm runs: SIGTERM, then the abort."""

    class Proc:
        returncode = None

        def __init__(self, *a, **k):
            self.signals = []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def communicate(self, data=None, timeout=None):
            if not self.signals:
                for _ in range(3):
                    os.kill(os.getpid(), signal.SIGINT)
            return "", ""

        def send_signal(self, sig):
            self.signals.append(sig)

        def kill(self):
            self.signals.append(signal.SIGKILL)

    procs = []

    def make(*a, **k):
        procs.append(Proc())
        return procs[-1]

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(subprocess, "Popen", make)
        with pytest.raises(cl.LeaseAbort):
            with cl.cluster_lock(_core(), timeout=0):
                _pinned.pinned_helm("c", ["upgrade", "spark-operator", "chart"])
    assert procs[0].signals == [signal.SIGTERM]


def test_a_landed_write_after_a_transport_error_is_kept(own_ns):
    """The 60 s read timeout fired but the create landed: hold it, then release it."""
    rec = own_ns
    import urllib3

    core = _core()

    class Flaky:
        def __getattr__(self, name):
            return getattr(core, name)

        def create_namespaced_config_map(self, *a, **k):
            core.create_namespaced_config_map(*a, **k)
            raise urllib3.exceptions.ReadTimeoutError(None, "/", "read timed out")

    with cl.cluster_lock(Flaky(), timeout=0):
        assert cl.lease_held()
    assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) not in rec.store


def test_destroy_keeps_the_namespace_when_a_leased_helm_times_out():
    """The strict remove maps a leased timeout to WatchListMutationError, fail closed."""
    from tests.test_recording_k8s_fixture import _destroy_under_recorder

    with recording() as rec:
        rec.on_command("helm", "upgrade", raises=subprocess.TimeoutExpired(["helm"], 5))
        results = {r.component: r for r in _destroy_under_recorder(rec)}
        assert results["spark-operator-watch"].status.value == "failed"
        assert "ran out of time" in results["spark-operator-watch"].message
        assert ("namespaces", None, NS) in rec.store
        assert not [c for c in rec.calls if c.verb == "delete" and c.kind == "namespaces"]


def test_heal_path_leased_api_calls_carry_request_timeouts():
    """deploy's and run's ensure_namespace_watched(can_heal=True) under the lease."""
    from lakebench.spark import SparkOperatorManager

    with recording(NS) as rec:
        rec.add_namespace(NS)
        rec.add_spark_operator(watched=["u02"])
        rec.add_namespace("u02")
        mgr = SparkOperatorManager(namespace="spark-operator", job_namespace=NS)
        status = mgr.ensure_namespace_watched(can_heal=True)
        assert status.ready, status
        rec.assert_recorded(api="helm", verb="upgrade", lease_held=True)
        leased_api = [c for c in rec.calls if c.lease_held and c.api.endswith("Api")]
        assert leased_api
        assert [c.describe() for c in leased_api if "_request_timeout" not in c.kwargs] == []
        assert mgr._get_active_namespaces() == ["u02", NS]


# ---------------------------------------------------------------------------
# Review round 2 (brief pass on the fix)
# ---------------------------------------------------------------------------


def test_a_same_holder_write_from_another_process_is_not_adopted(own_ns):
    """Holder and second can match another run from this tree; the nonce cannot."""
    import urllib3

    rec = own_ns
    core = _core()

    class Raced:
        def __getattr__(self, name):
            return getattr(core, name)

        def create_namespaced_config_map(self, namespace, body, **k):
            other = copy.deepcopy(body)
            other.data["write-nonce"] = "someone-else"
            core.create_namespaced_config_map(namespace, other, **k)  # B's write lands
            raise urllib3.exceptions.ReadTimeoutError(None, "/", "read timed out")

    with pytest.raises(urllib3.exceptions.ReadTimeoutError):
        cl.acquire_cluster_lock(Raced(), timeout=0, holder="host@user@sha")
    assert not cl.lease_held()
    assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) in rec.store  # B keeps it


def test_lease_state_cleared_before_handlers_return(own_ns, monkeypatch):
    """A signal right after the handlers are restored cannot leave lease_held() true."""
    seen = []
    real_restore = cl._SignalDeferral.restore

    def restore_then_check(self):
        seen.append(cl.lease_held())
        real_restore(self)

    monkeypatch.setattr(cl._SignalDeferral, "restore", restore_then_check)
    with cl.cluster_lock(_core(), timeout=0):
        pass
    assert seen and seen[-1] is False


def test_gentle_stop_is_bounded_when_pipes_stay_open(monkeypatch):
    """A grandchild holding the pipes open cannot keep the lease waiting."""
    import time

    monkeypatch.setattr(_pinned, "TERM_GRACE_S", 0.2)
    monkeypatch.setattr(_pinned, "_DRAIN_S", 0.2)
    proc = subprocess.Popen(
        ["sh", "-c", "trap '' TERM; sleep 30 & echo ready; wait"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        assert proc.stdout is not None and proc.stdout.readline() == b"ready\n"
        started = time.monotonic()
        _pinned._stop_gently(proc)  # SIGTERM is ignored; the sleep holds the pipes
        assert time.monotonic() - started < 3
        proc.wait(5)
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass


def test_signal_child_never_targets_our_own_group():
    """A child sharing our process group gets the signal alone, never the group."""
    got = []
    previous = signal.signal(signal.SIGUSR1, lambda *_: got.append(1))
    try:
        child = subprocess.Popen(["sleep", "5"], start_new_session=False)
        _pinned._signal_child(child, signal.SIGUSR1)
        assert child.wait(5) == -signal.SIGUSR1
        assert got == []
    finally:
        signal.signal(signal.SIGUSR1, previous)


def test_admin_verbs_report_a_leased_timeout_as_an_error():
    """[static] every admin lease block maps a leased timeout to a one-line exit."""
    text = (SRC / "cli/_admin.py").read_text(encoding="utf-8")
    blocks = text.count("max_hold_s=ADMIN_MAX_HOLD_S")
    assert blocks == 5
    assert text.count("except (subprocess.TimeoutExpired, LeaseHoldExceeded)") == blocks
