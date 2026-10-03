"""Release harness (scripts/release/): refusals, destroy by incarnation,
admission, resume and the ledger. Everything runs against fakes; nothing
reaches a cluster."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "scripts" / "release"


def _load(name: str) -> Any:
    key = f"lb_release_{name}"
    if key in sys.modules:
        return sys.modules[key]
    spec = importlib.util.spec_from_file_location(key, RELEASE / f"{name}.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[key] = mod
    spec.loader.exec_module(mod)
    return mod


H = _load("harness")
C = _load("cluster")
L = _load("ledger")

LEDGER_TEXT = """# Evidence

## Deployments ledger

Intro text.

| Namespace | Config path | Session or lane | Scale | Deployed |
|---|---|---|---|---|
closed lb17-old 2026-10-02T18:28:23Z destroy DONE
| lb17-other | /x/other.yaml | v17-run | 1 | 2026-10-03 |

Reconciled text after the table.

## Entries

- entry one
"""


# -- fakes -------------------------------------------------------------------


class ApiException(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(status)
        self.status = status


@pytest.fixture(autouse=True)
def _api_exception(monkeypatch):
    # read_namespace_identity catches kubernetes' ApiException by class.
    import kubernetes.client.rest as rest

    monkeypatch.setattr(rest, "ApiException", ApiException)
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")


class FakeCore:
    """A CoreV1Api with namespaces, nodes and pods in memory."""

    def __init__(self) -> None:
        self.namespaces: dict[str, dict[str, Any]] = {}
        self.nodes_fail = False
        self.nodes = [_node("w1", "40", "400Gi"), _node("w2", "40", "400Gi")]
        self.pods: list[Any] = []
        self.reads = 0

    def read_namespace(self, name, _request_timeout=None):
        self.reads += 1
        ns = self.namespaces.get(name)
        if ns is None:
            raise ApiException(404)
        return SimpleNamespace(
            metadata=SimpleNamespace(uid=ns["uid"], annotations=dict(ns["annotations"]), name=name)
        )

    def list_namespace(self, label_selector=None, _request_timeout=None):
        return SimpleNamespace(
            items=[
                SimpleNamespace(metadata=SimpleNamespace(name=n))
                for n, ns in self.namespaces.items()
                if ns.get("managed", True)
            ]
        )

    def list_node(self, _request_timeout=None):
        if self.nodes_fail:
            raise RuntimeError("forbidden")
        return SimpleNamespace(items=self.nodes)

    def list_pod_for_all_namespaces(self, field_selector=None, _request_timeout=None):
        return SimpleNamespace(items=self.pods)

    def add_ns(self, name: str, uid: str, nonce: str, managed: bool = True) -> None:
        self.namespaces[name] = {
            "uid": uid,
            "annotations": {"lakebench.deployment/deploy-nonce": nonce},
            "managed": managed,
        }


def _node(name: str, cpu: str, mem: str) -> Any:
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, labels={}),
        spec=SimpleNamespace(unschedulable=False, taints=[]),
        status=SimpleNamespace(
            allocatable={"cpu": cpu, "memory": mem},
            conditions=[SimpleNamespace(type="Ready", status="True")],
        ),
    )


def _pod(ns: str, cpu: str, mem: str) -> Any:
    c = SimpleNamespace(resources=SimpleNamespace(requests={"cpu": cpu, "memory": mem}))
    return SimpleNamespace(
        metadata=SimpleNamespace(namespace=ns, name=f"{ns}-pod"),
        spec=SimpleNamespace(
            containers=[c], init_containers=[], resources=None, overhead=None, node_name="w1"
        ),
    )


def write_state(config: Path, name: str, namespace: str, nonces: list[tuple[str, str]]) -> None:
    from lakebench.config.deploy_state import DeployState, NonceEntry, state_path, write_state

    state = DeployState(
        name=name,
        config_path=str(config),
        config_dir=str(config.parent),
        host="h",
        namespace=namespace,
        nonces=[NonceEntry(n, s, "t") for n, s in nonces],  # type: ignore[arg-type]
    )
    path = state_path(config, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_state(path, state)


class FakeRunner:
    """Plays the lakebench CLI against a FakeCore."""

    def __init__(self, core: FakeCore) -> None:
        self.core = core
        self.calls: list[tuple[str, ...]] = []
        self.deploy_code = 0
        self.deploy_writes = "confirmed"  # confirmed | pending | none | foreign
        self.run_code = 0
        self.run_records = 1
        self.destroy_override: tuple[int, list[str], str] | None = None
        self.on_deploy: Any = None
        self.on_destroy: Any = None
        self.counter = 0

    def __call__(self, args, *, cwd, log, interruptible=False, on_spawn=None):
        args = tuple(args)
        self.calls.append(args)
        log.parent.mkdir(parents=True, exist_ok=True)
        verb = args[0]
        if on_spawn is not None:
            on_spawn(os.getpid(), "0")
        if verb == "init":
            return self._init(args, log)
        cfg = Path(args[1])
        data = yaml.safe_load(cfg.read_text())
        name = data["name"]
        if verb == "deploy":
            if self.on_deploy is not None:
                self.on_deploy(cfg)
            self.counter += 1
            nonce, uid = f"n{self.counter}", f"u{self.counter}"
            if self.deploy_writes == "confirmed":
                self.core.add_ns(name, uid, nonce)
                write_state(cfg, name, name, [(nonce, "confirmed")])
            elif self.deploy_writes == "pending":
                self.core.add_ns(name, uid, "")
                write_state(cfg, name, name, [(nonce, "pending")])
            elif self.deploy_writes == "foreign":
                self.core.add_ns(name, uid, "someone-else")
                write_state(cfg, name, name, [(nonce, "confirmed")])
            return H.ChildResult(self.deploy_code, [], log)
        if verb == "run":
            runs = cfg.parent / "lakebench-output" / "runs"
            for _ in range(self.run_records):
                self.counter += 1
                d = runs / f"run-2026-{self.counter:06d}"
                d.mkdir(parents=True)
                (d / "metrics.json").write_text(json.dumps({"run_id": d.name}))
            return H.ChildResult(self.run_code, [], log)
        if verb == "report":
            return H.ChildResult(0, [], log)
        if verb == "destroy":
            assert "--force" not in args
            if self.destroy_override is not None:
                code, paths, text = self.destroy_override
                log.write_text(text)
                return H.ChildResult(code, paths, log)
            expected = args[args.index("--expect-incarnation") + 1]
            ns = self.core.namespaces.get(name)
            found = (
                f"{ns['uid']}#{ns['annotations']['lakebench.deployment/deploy-nonce']}"
                if ns
                else ""
            )
            if found != expected:
                return H.ChildResult(3, ["destroy.incarnation_mismatch"], log)
            del self.core.namespaces[name]
            if self.on_destroy is not None:
                self.on_destroy(cfg)
            return H.ChildResult(0, [], log)
        raise AssertionError(f"unexpected verb {verb}")

    def _init(self, args, log):
        get = lambda flag: args[args.index(flag) + 1]  # noqa: E731
        out = Path(get("-o"))
        out.write_text(
            yaml.safe_dump(
                {
                    "name": get("-n"),
                    "recipe": get("-r"),
                    "workload": {"schema": get("-w"), "datagen": {"scale": float(get("-s"))}},
                    "platform": {
                        "storage": {
                            "s3": {
                                "endpoint": get("--endpoint"),
                                "access_key": "${LAKEBENCH_S3_ACCESS_KEY}",
                                "secret_key": "${LAKEBENCH_S3_SECRET_KEY}",
                            }
                        }
                    },
                }
            )
        )
        return H.ChildResult(0, [], log)

    def verbs(self) -> list[str]:
        return [c[0] for c in self.calls]


ROW = H.Row("M01", "customer360", "batch", "hive-iceberg-spark-trino", 1.0, 42)


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(H, "record_verdict", lambda record, freeze, version: [])
    stub = SimpleNamespace(
        scrub_record=lambda rec: (rec, []), dump=lambda rec: json.dumps(rec) + "\n"
    )
    monkeypatch.setattr(H, "_scrub_module", lambda: stub)
    core = FakeCore()
    runner = FakeRunner(core)
    out = tmp_path / "out"
    ledger_path = tmp_path / "EVIDENCE.md"
    ledger_path.write_text(LEDGER_TEXT)
    rowlog = L.RowLog(out)
    rowlog.acquire()
    said: list[str] = []
    h = H.Harness(
        tree=ROOT,
        out=out,
        freeze="f" * 40,
        version="1.7.0",
        rehearsal=False,
        context="ctx",
        runner=runner,
        cluster=C.ClusterReader(core),
        rowlog=rowlog,
        ledger=L.MarkdownLedger(ledger_path, out / "ledger-backups"),
        sleep=lambda s: None,
        say=said.append,
    )
    h.rows = {ROW.id: ROW}
    yield SimpleNamespace(h=h, core=core, runner=runner, ledger=ledger_path, said=said, out=out)
    rowlog.release()


def _plan(env, row=ROW):
    plan = env.h.plan_row(row, env.h.row_dir(row))
    return plan


def _status(env, row="M01"):
    return env.h.rowlog.latest()[row]


# -- refusals ----------------------------------------------------------------


def _git(repo: Path, *args: str) -> str:
    environ = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    environ.update(
        GIT_AUTHOR_NAME="t",
        GIT_AUTHOR_EMAIL="t@t",
        GIT_COMMITTER_NAME="t",
        GIT_COMMITTER_EMAIL="t@t",
    )
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True, env=environ
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "tree"
    (r / "src" / "lakebench").mkdir(parents=True)
    (r / "src" / "lakebench" / "__init__.py").write_text("")
    (r / "scripts").mkdir()
    (r / "scripts" / "a.py").write_text("")
    (r / ".gitignore").write_text("*.local\n__pycache__/\n")
    _git(r, "init", "-q")
    _git(r, "add", "-A")
    _git(r, "commit", "-qm", "init")
    sha = _git(r, "rev-parse", "HEAD")
    _git(r, "checkout", "-q", "--detach")
    return SimpleNamespace(path=r, sha=sha, own=str(r / "src" / "lakebench" / "__init__.py"))


def _reasons(repo, **kw):
    args: dict[str, Any] = {
        "freeze": repo.sha,
        "rehearsal": False,
        "out": None,
        "live": False,
        "ledger": None,
        "context": None,
        "in_process_file": repo.own,
    }
    args.update(kw)
    return H.refuse_reasons(repo.path, **args)


def test_clean_detached_tree_at_freeze_has_no_refusal(repo):
    assert _reasons(repo) == []


def test_refuses_branch_checkout(repo):
    _git(repo.path, "checkout", "-q", "-b", "somebranch")
    assert any("on a branch" in r for r in _reasons(repo))


def test_refuses_foreign_sha(repo):
    reasons = _reasons(repo, freeze="0" * 40)
    assert any("not the freeze commit" in r for r in reasons)
    # --rehearsal waives only the sha check
    assert _reasons(repo, freeze="0" * 40, rehearsal=True) == []


def test_refuses_dirty_tree_tracked_edit(repo):
    (repo.path / "scripts" / "a.py").write_text("x = 1\n")
    assert any("tracked change" in r for r in _reasons(repo))


def test_refuses_dirty_tree_untracked_src_file(repo):
    (repo.path / "src" / "x.py").write_text("")
    assert any("untracked file under src/" in r for r in _reasons(repo))


def test_refuses_ignored_file_under_src_but_not_pycache(repo):
    cache = repo.path / "src" / "lakebench" / "__pycache__"
    cache.mkdir()
    (cache / "m.cpython-311.pyc").write_bytes(b"")
    assert _reasons(repo) == []
    (repo.path / "src" / "evil.local").write_text("")
    assert any("ignored file under src/" in r for r in _reasons(repo))


def test_refuses_outside_import(repo):
    # The child process imports lakebench from wherever PYTHONPATH points;
    # this tree has no lakebench package, so it resolves elsewhere.
    import shutil

    shutil.rmtree(repo.path / "src" / "lakebench")
    _git(repo.path, "add", "-A")
    _git(repo.path, "commit", "-qm", "drop")
    sha = _git(repo.path, "rev-parse", "HEAD")
    _git(repo.path, "checkout", "-q", "--detach", sha)
    reasons = _reasons(repo, freeze=sha, in_process_file="/elsewhere/lakebench/__init__.py")
    assert any("this process imports lakebench from /elsewhere" in r for r in reasons)
    assert any("a lakebench child imports from" in r for r in reasons)


def test_child_import_uses_tree_src(repo, tmp_path):
    assert H.imported_from(repo.path, tmp_path) == repo.own


def test_refuses_out_inside_worktree_or_tmp(repo):
    assert any("inside the release worktree" in r for r in _reasons(repo, out=repo.path / "o"))
    assert any("under /tmp" in r for r in _reasons(repo, out=Path("/tmp/x")))


def test_live_run_needs_ledger_context_and_credentials(repo, tmp_path, monkeypatch):
    for v in ("LAKEBENCH_S3_ACCESS_KEY", "LAKEBENCH_S3_SECRET_KEY", "LB_S3_ENDPOINT"):
        monkeypatch.delenv(v, raising=False)
    reasons = _reasons(repo, live=True, contexts=lambda: ["a"], context="b")
    assert any("--deployments-ledger is required" in r for r in reasons)
    assert any("--context b is not in the kubeconfig" in r for r in reasons)
    assert any("unset credential variables" in r for r in reasons)
    assert any("LB_S3_ENDPOINT" in r for r in reasons)


# -- matrix ------------------------------------------------------------------


def test_matrix_file_is_exactly_the_release_matrix():
    from lakebench.metrics.release_record import RELEASE_MATRIX

    version, rows = H.load_matrix(RELEASE / "matrix-1.7.yaml", H.KNOWN_STEPS)
    assert version == "1.7.0"
    assert sorted(r.key for r in rows) == sorted(
        (w, m, r, float(s)) for w, m, r, s in RELEASE_MATRIX
    )
    assert H.matrix_problems(rows) == []
    assert [r.id for r in rows if r.alone] == ["M16"]
    assert {r.seed for r in rows if r.workload == "financial"} == {43}


def test_matrix_rejects_spent_aml_seed(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text(
        "version: x\nrows:\n  - {id: A, workload: financial, mode: batch, "
        "recipe: hive-iceberg-spark-trino, scale: 1, seed: 42}\n"
    )
    with pytest.raises(H.Refused, match="seed 43"):
        H.load_matrix(p)


def test_every_matrix_row_config_resolves_to_its_release_versions(env):
    _, rows = H.load_matrix(RELEASE / "matrix-1.7.yaml", H.KNOWN_STEPS)
    for row in rows:
        plan = env.h.plan_row(row, env.out / "p" / row.id)
        data = yaml.safe_load(plan.config.read_text())
        assert data["platform"]["kubernetes"]["context"] == "ctx"
        assert data["workload"]["datagen"]["seed"] == row.seed
        assert plan.peak.cores > 0


def test_off_matrix_versions_refused(env):
    row = ROW

    def bad_init(args, **kw):
        res = FakeRunner._init(env.runner, tuple(args), kw["log"])
        out = Path(args[args.index("-o") + 1])
        data = yaml.safe_load(out.read_text())
        data["images"] = {"spark": "apache/spark:4.0.2-python3"}
        out.write_text(yaml.safe_dump(data))
        return res

    env.h.runner = bad_init
    with pytest.raises(H.Refused, match="release matrix runs"):
        env.h.plan_row(row, env.out / "bad")


def test_plan_peak_includes_trino(env):
    # The row's peak is plan_requirements' full request: Spark plus the
    # co-resident Trino, catalog and Postgres pods, not Spark alone.
    from lakebench.config.sizing import plan_requirements
    from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

    plan = _plan(env)
    p = plan_requirements(H.load_row_config(plan.config))
    assert "Trino" in p.co_resident.label or p.co_resident.cpu_cores >= 4, p.co_resident
    spark_only = compute_peak_requirements(1.0, "batch", "customer360")
    assert plan.peak.cores == p.full.cpu_cores
    assert plan.peak.cores >= spark_only.cpu_cores + p.co_resident.cpu_cores


# -- deploy, incarnation, destroy ----------------------------------------------


def test_ledger_row_written_before_deploy(env):
    plan = _plan(env)
    seen = {}

    def on_deploy(cfg):
        seen["ledger"] = plan.namespace in env.ledger.read_text()
        seen["status"] = _status(env)["status"]

    env.runner.on_deploy = on_deploy
    env.h.deploy(plan)
    assert seen == {"ledger": True, "status": "deploying"}
    states = [e["status"] for e in env.h.rowlog.entries()]
    assert states.index("ledgered") < states.index("deploying")


def test_full_row_destroys_by_incarnation_and_closes_ledger(env):
    plan = _plan(env)
    env.h.deploy(plan)
    st = _status(env)
    assert st["status"] == "destroyed", st
    assert st["verdict"] == "PASS"
    assert env.runner.verbs() == ["init", "deploy", "run", "report", "destroy"]
    deploy = env.runner.calls[1]
    assert deploy[2:] == ("--yes", "--require-new")
    text = env.ledger.read_text()
    assert f"closed {plan.namespace} " in text
    assert f"| {plan.namespace} |" not in text
    assert (env.out / "uat" / "runs").is_dir()


def test_destroy_passes_expect_incarnation(env):
    plan = _plan(env)
    env.h.deploy(plan)
    destroy = [c for c in env.runner.calls if c[0] == "destroy"][0]
    assert destroy[2:4] == ("--yes", "--expect-incarnation")
    assert destroy[4] == "u1#n1"
    assert "--force" not in destroy


def test_foreign_nonce_gets_no_destroy(env):
    env.runner.deploy_writes = "foreign"
    plan = _plan(env)
    env.h.deploy(plan)
    assert "destroy" not in env.runner.verbs()
    assert _status(env)["status"] == "failed"
    assert plan.namespace in env.core.namespaces


def test_nonce_read_from_named_state_file(env):
    plan = _plan(env)
    name = yaml.safe_load(plan.config.read_text())["name"]
    env.core.add_ns(plan.namespace, "u9", "n9")
    write_state(plan.config, name, plan.namespace, [("n9", "confirmed")])
    assert H.confirmed_incarnation(plan.config, env.core) == ("u9#n9", "")


def test_legacy_state_json_only_gives_no_incarnation(env):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u9", "n9")
    legacy = plan.config.parent / ".lakebench" / "state.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps({"nonce": "n9", "deploy_nonce": "n9"}))
    inc, why = H.confirmed_incarnation(plan.config, env.core)
    assert inc is None and "no deploy state" in why


def test_older_kept_nonce_is_not_the_rows_incarnation(env):
    plan = _plan(env)
    name = yaml.safe_load(plan.config.read_text())["name"]
    env.core.add_ns(plan.namespace, "u9", "old")
    write_state(plan.config, name, plan.namespace, [("new", "confirmed"), ("old", "confirmed")])
    inc, why = H.confirmed_incarnation(plan.config, env.core)
    assert inc is None and "older recorded nonce" in why


def test_pending_nonce_is_not_confirmed(env):
    plan = _plan(env)
    name = yaml.safe_load(plan.config.read_text())["name"]
    env.core.add_ns(plan.namespace, "u9", "n9")
    write_state(plan.config, name, plan.namespace, [("n9", "pending")])
    inc, why = H.confirmed_incarnation(plan.config, env.core)
    assert inc is None and "pending" in why


def test_failed_deploy_without_namespace_is_not_deployed_and_ledger_closed(env):
    env.runner.deploy_code = 4
    env.runner.deploy_writes = "none"
    plan = _plan(env)
    env.h.deploy(plan)
    assert _status(env)["status"] == "not-deployed"
    assert f"closed {plan.namespace} " in env.ledger.read_text()
    assert env.runner.verbs().count("deploy") == 1


def test_failed_deploy_with_confirmed_nonce_is_destroyed_by_incarnation(env):
    env.runner.deploy_code = 1
    plan = _plan(env)
    env.h.deploy(plan)
    st = _status(env)
    assert st["status"] == "destroyed" and st["verdict"] == "FAIL"
    assert env.runner.verbs().count("deploy") == 1
    assert "run" not in env.runner.verbs()


def test_failed_deploy_with_pending_nonce_is_left(env):
    env.runner.deploy_code = 1
    env.runner.deploy_writes = "pending"
    plan = _plan(env)
    env.h.deploy(plan)
    assert _status(env)["status"] == "left"
    assert "destroy" not in env.runner.verbs()
    assert f"| {plan.namespace} |" in env.ledger.read_text()


def test_missing_namespace_is_left_not_destroyed(env):
    plan = _plan(env)
    env.h.log("M01", "deployed", incarnation="u1#n1")
    env.h.destroy(plan, "u1#n1")
    assert _status(env)["status"] == "left"
    assert "destroy" not in env.runner.verbs()


def test_exit_6_polls_and_never_reinvokes_destroy(env):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u1", "n1")
    env.runner.destroy_override = (6, ["destroy.namespace_terminating"], "still terminating")
    polls = {"n": 0}
    real = env.h.cluster.namespace_exists

    def exists(ns):
        polls["n"] += 1
        if polls["n"] >= 4:
            env.core.namespaces.pop(ns, None)
        return real(ns)

    env.h.cluster.namespace_exists = exists
    env.h.destroy(plan, "u1#n1")
    assert env.runner.verbs().count("destroy") == 1
    assert _status(env)["status"] == "destroyed"


def test_exit_6_still_present_after_limit_is_failed(env):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u1", "n1")
    env.runner.destroy_override = (6, ["destroy.namespace_terminating"], "")
    clock = {"t": 0.0}
    env.h.monotonic = lambda: clock["t"]

    def sleep(s):
        clock["t"] += s

    env.h.sleep = sleep
    env.h.destroy(plan, "u1#n1")
    assert env.runner.verbs().count("destroy") == 1
    assert _status(env)["status"] == "failed"
    assert env.h.stopping


@pytest.mark.parametrize(
    ("paths", "status"),
    [
        (["destroy.unverified_cluster"], "failed"),
        (["lease.held"], "failed"),
        (["destroy.redeployed"], "failed"),
        ([], "failed"),
    ],
)
def test_destroy_refusals_stop_admission_and_never_retry(env, paths, status):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u1", "n1")
    env.h.ledger_add(plan)
    env.runner.destroy_override = (3, paths, "Destroy NOT completed")
    env.h.destroy(plan, "u1#n1")
    assert env.runner.verbs().count("destroy") == 1
    assert _status(env)["status"] == status
    assert env.h.stopping
    assert f"| {plan.namespace} |" in env.ledger.read_text()


def test_incarnation_mismatch_is_destroy_refused(env):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u2", "redeployed")
    env.h.destroy(plan, "u1#n1")
    assert _status(env)["status"] == "destroy-refused"
    assert env.h.stopping
    assert plan.namespace in env.core.namespaces


def test_destroy_exit_0_with_not_completed_text_is_failed(env):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u1", "n1")
    env.runner.destroy_override = (0, [], "Destroy NOT completed: something")
    env.h.destroy(plan, "u1#n1")
    assert _status(env)["status"] == "failed"


def test_verdict_comes_from_the_record_not_the_exit_code(env, monkeypatch):
    monkeypatch.setattr(H, "record_verdict", lambda r, f, v: ["no silver rows"])
    plan = _plan(env)
    env.h.deploy(plan)
    st = _status(env)
    assert st["verdict"] == "FAIL" and any("no silver rows" in p for p in st["problems"])
    assert st["status"] == "destroyed"


def test_two_run_records_fail_the_row(env):
    env.runner.run_records = 2
    plan = _plan(env)
    env.h.deploy(plan)
    assert _status(env)["verdict"] == "FAIL"


def test_stopping_after_run_leaves_row_recorded_for_resume(env):
    plan = _plan(env)
    real = env.runner.__call__

    def runner(args, **kw):
        res = real(args, **kw)
        if args[0] == "run":
            env.h.stop_admission("test interrupt")
        return res

    env.h.runner = runner
    env.h.deploy(plan)
    assert _status(env)["status"] == "recorded"
    assert "destroy" not in env.runner.verbs()


# -- resume --------------------------------------------------------------------


def _seed_row(env, status, **fields):
    plan = _plan(env)
    env.h.log(
        "M01",
        "planned",
        namespace=plan.namespace,
        config=str(plan.config),
        peak=[plan.peak.cores, plan.peak.gib],
    )
    env.h.log("M01", status, **fields)
    return plan


def test_resume_destroys_only_matching_nonce(env):
    plan = _seed_row(env, "recorded", incarnation="u1#n1", verdict="PASS")
    env.core.add_ns(plan.namespace, "u7", "redeployed-meanwhile")
    H.resume(env.h, {"M01": ROW})
    assert _status(env)["status"] == "destroy-refused"
    assert plan.namespace in env.core.namespaces


def test_resume_destroys_recorded_row_with_its_incarnation(env):
    plan = _seed_row(env, "recorded", incarnation="u1#n1", verdict="PASS")
    env.core.add_ns(plan.namespace, "u1", "n1")
    H.resume(env.h, {"M01": ROW})
    assert _status(env)["status"] == "destroyed"


def test_resume_refuses_a_row_whose_child_is_alive(env):
    _seed_row(
        env,
        "running",
        incarnation="u1#n1",
        child_pid=os.getpid(),
        child_start=H._start_time(os.getpid()),
    )
    with pytest.raises(H.Refused, match="live child"):
        H.resume(env.h, {"M01": ROW})


def test_resume_never_reruns_a_running_row(env):
    plan = _seed_row(
        env, "running", incarnation="u1#n1", runs_before=[], child_pid=1, child_start="x"
    )
    env.core.add_ns(plan.namespace, "u1", "n1")
    H.resume(env.h, {"M01": ROW})
    assert "run" not in env.runner.verbs()
    st = _status(env)
    assert st["verdict"] == "FAIL" and st["status"] == "destroyed"


def test_resume_never_redeploys_a_deploying_row(env):
    plan = _seed_row(env, "deploying", child_pid=1, child_start="x")
    name = yaml.safe_load(plan.config.read_text())["name"]
    env.core.add_ns(plan.namespace, "u1", "n1")
    write_state(plan.config, name, plan.namespace, [("n1", "pending")])
    H.resume(env.h, {"M01": ROW})
    assert "deploy" not in env.runner.verbs()
    assert _status(env)["status"] == "left"


def test_resume_destroying_polls_only(env):
    plan = _seed_row(env, "destroying", incarnation="u1#n1", child_pid=1, child_start="x")
    H.resume(env.h, {"M01": ROW})
    assert "destroy" not in env.runner.verbs()
    assert _status(env)["status"] == "destroyed"
    assert plan.namespace not in env.core.namespaces


def test_run_refuses_rows_already_in_the_log(env):
    _seed_row(env, "destroyed")
    with pytest.raises(H.Refused, match="use resume"):
        env.h.run([ROW])


# -- admission -----------------------------------------------------------------


def _snap(cores=80.0, gib=800.0, **req):
    return C.Snapshot(C.Peak(cores, gib), {k: C.Peak(*v) for k, v in req.items()})


def _cand(cores=10.0, gib=100.0, **kw):
    return C.Candidate("M01", "rel17-m01", C.Peak(cores, gib), **kw)


def _admit(cand, snap, **kw):
    args: dict[str, Any] = {
        "managed": [],
        "ledger_live": [],
        "own_active": [],
        "ledger_peaks": {},
        "fallback_peak": C.Peak(0, 0),
    }
    args.update(kw)
    return C.admit(cand, snap, **args)


def test_admission_counts_foreign_by_requests():
    # 80 cores * 0.8 = 64; foreign pods request 55 cores; the row needs 10.
    d = _admit(_cand(), _snap(**{"someone": (55.0, 10.0)}))
    assert not d.admit and "someone" in d.blocking
    assert _admit(_cand(), _snap(**{"someone": (50.0, 10.0)})).admit


def test_admission_fails_closed_on_unreadable_nodes():
    d = _admit(_cand(), C.Unknown("listing nodes failed"))
    assert not d.admit and "unreadable" in d.reasons[0]


def test_reader_fails_closed_on_unreadable_nodes():
    core = FakeCore()
    core.nodes_fail = True
    assert isinstance(C.ClusterReader(core).snapshot(), C.Unknown)


def test_reader_sums_requests_per_namespace():
    core = FakeCore()
    core.pods = [_pod("a", "2", "4Gi"), _pod("a", "500m", "1Gi"), _pod("b", "1", "2Gi")]
    snap = C.ClusterReader(core).snapshot()
    assert snap.allocatable == C.Peak(80.0, 800.0)
    assert snap.requests["a"] == C.Peak(2.5, 5.0)


def test_ledger_rows_count_toward_the_deployment_limit():
    d = _admit(_cand(), _snap(), managed=["a", "b"], ledger_live=["c", "d"])
    assert not d.admit and any("4 lakebench deployments plus 1" in r for r in d.reasons)
    assert _admit(_cand(), _snap(), managed=["a", "b"], ledger_live=["a", "c"]).admit


def test_ledger_namespace_counts_its_peak_not_its_idle_requests():
    snap = _snap(**{"idle": (1.0, 1.0)})
    d = _admit(_cand(), snap, ledger_live=["idle"], ledger_peaks={"idle": C.Peak(60.0, 100.0)})
    assert not d.admit
    d = _admit(
        _cand(), snap, ledger_live=["idle"], ledger_peaks={}, fallback_peak=C.Peak(60.0, 1.0)
    )
    assert not d.admit


def test_alone_row_needs_an_empty_cluster_and_blocks_others():
    assert not _admit(_cand(alone=True), _snap(), managed=["x"]).admit
    own = [C.ActiveRow("rel17-m16", C.Peak(1, 1), alone=True)]
    assert not _admit(_cand(), _snap(), own_active=own).admit


def test_slots_and_aml_continuous_cap():
    own = [C.ActiveRow(f"n{i}", C.Peak(1, 1), aml_continuous=True) for i in range(2)]
    assert not _admit(_cand(aml_continuous=True), _snap(), own_active=own).admit
    assert _admit(_cand(), _snap(), own_active=own).admit
    own3 = [C.ActiveRow(f"n{i}", C.Peak(1, 1)) for i in range(3)]
    assert not _admit(_cand(), _snap(), own_active=own3).admit


def test_schedule_waits_then_admits(env):
    plan = _plan(env)
    decisions = iter([C.Unknown("nodes"), C.Decision(True)])
    env.h.decide = lambda p, f: next(decisions)
    sleeps = []
    env.h.sleep = sleeps.append
    env.h.schedule([plan], [])
    assert sleeps and _status(env)["status"] == "destroyed"


# -- ledger --------------------------------------------------------------------


def test_ledger_add_and_close_keep_every_other_line(tmp_path):
    p = tmp_path / "E.md"
    p.write_text(LEDGER_TEXT)
    led = L.MarkdownLedger(p, tmp_path / "b")
    led.add(L.LedgerRow("rel17-m01-abc", "/o/M01.yaml", "release-harness x row M01", "1", "t"))
    lines = p.read_text().splitlines()
    i = lines.index("| lb17-other | /x/other.yaml | v17-run | 1 | 2026-10-03 |")
    assert lines[i + 1].startswith("| rel17-m01-abc |")
    assert led.live_namespaces() == {"lb17-other", "rel17-m01-abc"}
    led.close("rel17-m01-abc", "T")
    text = p.read_text()
    assert "closed rel17-m01-abc T destroy DONE" in text
    assert text.replace("closed rel17-m01-abc T destroy DONE\n", "") == LEDGER_TEXT
    assert len(list((tmp_path / "b").iterdir())) == 2


def test_ledger_refuses_without_header(tmp_path):
    p = tmp_path / "E.md"
    p.write_text("# nothing here\n")
    with pytest.raises(L.LedgerError, match="header"):
        L.MarkdownLedger(p, tmp_path / "b").add(L.LedgerRow("a", "b", "c", "1", "t"))


def test_ledger_refuses_duplicate_and_unknown_close(tmp_path):
    p = tmp_path / "E.md"
    p.write_text(LEDGER_TEXT)
    led = L.MarkdownLedger(p, tmp_path / "b")
    with pytest.raises(L.LedgerError, match="already has a row"):
        led.add(L.LedgerRow("lb17-other", "b", "c", "1", "t"))
    with pytest.raises(L.LedgerError, match="expected one ledger row"):
        led.close("nope")
    assert p.read_text() == LEDGER_TEXT


def test_ledger_retries_when_edited_underneath(tmp_path, monkeypatch):
    p = tmp_path / "E.md"
    p.write_text(LEDGER_TEXT)
    led = L.MarkdownLedger(p, tmp_path / "b")
    real_backup = led._backup
    hit = {"n": 0}

    def sneaky(raw):
        real_backup(raw)
        if hit["n"] == 0:
            hit["n"] += 1
            # another session appends an entry between our read and write
            with open(p, "a") as fh:
                fh.write("- entry two\n")

    led._backup = sneaky
    led.add(L.LedgerRow("rel17-x", "/c", "s", "1", "t"))
    text = p.read_text()
    assert "- entry two" in text and "| rel17-x |" in text


def test_ledger_refuses_a_shrunken_file(tmp_path):
    p = tmp_path / "E.md"
    p.write_text(LEDGER_TEXT + "x" * 10000 + "\n")
    led = L.MarkdownLedger(p, tmp_path / "b")
    led.add(L.LedgerRow("rel17-x", "/c", "s", "1", "t"))
    p.write_text(LEDGER_TEXT)
    with pytest.raises(L.LedgerError, match="shrank"):
        led.close("rel17-x")


def test_rowlog_lock_is_exclusive_and_appends_fold(tmp_path):
    a = L.RowLog(tmp_path / "o")
    a.acquire()
    b = L.RowLog(tmp_path / "o")
    with pytest.raises(L.LedgerError, match="another harness"):
        b.acquire()
    a.append("M01", "planned", namespace="ns")
    a.append("M01", "deployed", incarnation="u#n")
    st = a.latest()["M01"]
    assert st["status"] == "deployed" and st["namespace"] == "ns" and st["incarnation"] == "u#n"
    with pytest.raises(L.LedgerError, match="unknown row status"):
        a.append("M01", "bogus")
    a.release()


def test_results_table_lists_rows_with_verdicts(env):
    plan = _plan(env)
    env.h.deploy(plan)
    path = env.h.write_results()
    text = path.read_text()
    assert text.startswith("# UAT results 1.7.0\n")
    assert f"Freeze commit: {'f' * 40}" in text
    assert "| M01 | customer360 | batch | hive-iceberg-spark-trino | 4.1 / 1.11.0 | 1 |" in text
    assert "| PASS | - |" in text


def test_rehearsal_writes_its_own_results_file(env):
    env.h.rehearsal = True
    assert env.h.write_results().name == "results-rehearsal.md"


def test_sigint_reaches_only_interruptible_children(tmp_path, monkeypatch):
    sent = []
    monkeypatch.setattr(H.os, "killpg", lambda pid, sig: sent.append(pid))
    r = H.ProcessRunner(ROOT)
    r._children = {11: True, 12: False}
    r.interrupt()
    r.interrupt()
    assert sent == [11]


def test_process_runner_reads_exit_paths(tmp_path):
    r = H.ProcessRunner(ROOT)
    res = r(
        ["destroy", str(tmp_path / "missing.yaml"), "--yes", "--expect-incarnation", "bad"],
        cwd=tmp_path,
        log=tmp_path / "l" / "d.log",
    )
    assert res.code == 2
    assert res.paths == ["cli.bad_argument"]


def test_threads_finish_before_schedule_returns(env):
    plan = _plan(env)
    env.h.decide = lambda p, f: C.Decision(True)
    env.h.schedule([plan], [])
    assert not [t for t in threading.enumerate() if t.name == "M01"]


# -- scenarios (S-P1 to S-P6) ----------------------------------------------------

SCEN = RELEASE / "scenarios"


class FakeS3:
    def __init__(self) -> None:
        self.buckets: dict[str, dict[str, bytes]] = {}
        self.owner: dict[str, str] = {}
        self.raw_client = self

    def bucket_exists(self, b):
        return b in self.buckets

    def create_bucket(self, b):
        self.buckets.setdefault(b, {})
        return True

    def put_object(self, Bucket, Key, Body):  # noqa: N803 -- boto3 names
        self.buckets[Bucket][Key] = Body

    def get_paginator(self, _name):
        return self

    def paginate(self, Bucket):  # noqa: N803
        return [{"Contents": [{"Key": k} for k in self.buckets[Bucket]]}]

    def empty_bucket(self, b, keep_prefixes=()):
        self.buckets[b].clear()
        return 0

    def delete_bucket(self, b):
        return self.buckets.pop(b, None) is not None


@pytest.fixture
def senv(env):
    # a scenario admits two s1 deployments at once: give the fake room
    env.core.nodes = [_node(f"w{i}", "100", "1000Gi") for i in range(4)]
    s3 = FakeS3()
    env.h.s3_factory = lambda cfg: s3
    env.s3 = s3
    env.runner.on_destroy = lambda cfg: _destroy_fake(env, cfg)
    return env


def _deploy_fake(env, cfg: Path, nonce: str, uid: str) -> str:
    data = yaml.safe_load(cfg.read_text())
    name = data["name"]
    env.core.add_ns(name, uid, nonce)
    from lakebench.config.deploy_state import read_state

    old = read_state(cfg)
    kept = [(e.nonce, e.status) for e in (old.nonces if old else [])]
    write_state(cfg, name, name, [(nonce, "confirmed"), *kept])
    for b in data["platform"]["storage"]["s3"]["buckets"].values():
        if b not in env.s3.buckets:
            env.s3.create_bucket(b)
            env.s3.owner[b] = name
    return name


def _destroy_fake(env, cfg: Path) -> None:
    data = yaml.safe_load(cfg.read_text())
    env.core.namespaces.pop(data["name"], None)
    for b in data["platform"]["storage"]["s3"]["buckets"].values():
        if env.s3.owner.get(b) == data["name"]:
            env.s3.buckets.pop(b, None)


def _script(env, body, rc=0, pass_line=True):
    def run(argv, *, env: dict, cwd: Path, log: Path) -> int:
        assert argv[0] == "bash" and Path(argv[1]).parent == SCEN
        assert env["LB_EXIT_REFUSED"] == "3"
        body(env)
        log.parent.mkdir(parents=True, exist_ok=True)
        sid = Path(argv[1]).name.split("-")[1].upper()
        log.write_text(f"PASS: S-{sid} -- fake\n" if pass_line else "done\n")
        return rc

    return run


def _scen_states(env, sid):
    return {r: s for r, s in env.h.rowlog.latest().items() if r.startswith(sid)}


def test_scenario_shim_imports_release_tree(tmp_path):
    bin_dir = tmp_path / "bin"
    H.write_shim(ROOT, bin_dir)
    env = H.child_env(ROOT)
    env["PATH"] = f"{bin_dir}{os.pathsep}{os.environ['PATH']}"
    seen = H.shim_imports_from(bin_dir, env)
    assert seen == str(ROOT / "src" / "lakebench" / "__init__.py")


def test_scenario_shim_runs_the_cli(tmp_path):
    bin_dir = tmp_path / "bin"
    H.write_shim(ROOT, bin_dir)
    env = H.child_env(ROOT, {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"})
    pf = tmp_path / "p"
    env["LB_EXIT_PATH_FILE"] = str(pf)
    out = subprocess.run(
        ["lakebench", "destroy", str(tmp_path / "x.yaml"), "--yes", "--expect-incarnation", "bad"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 2
    assert pf.read_text().split() == ["2", "cli.bad_argument"]


def test_scenario_leftover_namespace_destroyed_by_incarnation(senv):
    def body(e):
        _deploy_fake(senv, Path(e["LB_CONFIG_A"]), "na", "ua")
        _deploy_fake(senv, Path(e["LB_CONFIG_B"]), "nb", "ub")
        _destroy_fake(senv, Path(e["LB_CONFIG_A"]))

    senv.h.script_runner = _script(senv, body)
    assert senv.h.scenario(H.SCENARIOS["S-P1"]) == 0
    st = _scen_states(senv, "S-P1")
    assert st["S-P1-A"]["status"] == "destroyed" and st["S-P1-B"]["status"] == "destroyed"
    destroys = [c for c in senv.runner.calls if c[0] == "destroy"]
    assert len(destroys) == 1 and destroys[0][4] == "ub#nb"
    text = senv.ledger.read_text()
    assert text.count("closed rel17-s-p1-") == 2
    assert st["S-P1-A"]["scenario_verdict"] == "PASS"
    assert (senv.out / "results-extra.md").read_text().count("| S-P1 |") == 1
    assert "S-P1" not in (senv.h.write_results().read_text())


def test_scenario_kept_namespace_missing_fails(senv):
    def body(e):
        _deploy_fake(senv, Path(e["LB_CONFIG_A"]), "na", "ua")
        _deploy_fake(senv, Path(e["LB_CONFIG_B"]), "nb", "ub")
        _destroy_fake(senv, Path(e["LB_CONFIG_A"]))
        _destroy_fake(senv, Path(e["LB_CONFIG_B"]))

    senv.h.script_runner = _script(senv, body)
    assert senv.h.scenario(H.SCENARIOS["S-P1"]) == 1
    assert any(
        "expected present" in p for p in _scen_states(senv, "S-P1")["S-P1-A"]["scenario_problems"]
    )


def test_scenario_exit_0_without_pass_line_fails(senv):
    senv.h.script_runner = _script(senv, lambda e: None, pass_line=False)
    assert senv.h.scenario(H.SCENARIOS["S-P4"]) == 1


def test_scenario_script_failure_still_cleans_up(senv):
    def body(e):
        _deploy_fake(senv, Path(e["LB_CONFIG_A"]), "na", "ua")

    senv.h.script_runner = _script(senv, body, rc=1)
    assert senv.h.scenario(H.SCENARIOS["S-P4"]) == 1
    assert _scen_states(senv, "S-P4")["S-P4-A"]["status"] == "destroyed"
    assert not senv.core.namespaces


def test_scenario_redeployed_config_uses_its_current_kept_nonce(senv):
    def body(e):
        cfg = Path(e["LB_CONFIG_A"])
        _deploy_fake(senv, cfg, "n1", "u1")
        _destroy_fake(senv, cfg)
        _deploy_fake(senv, cfg, "n2", "u2")

    senv.h.script_runner = _script(senv, body)
    senv.h.scenario(H.SCENARIOS["S-P4"])
    destroy = [c for c in senv.runner.calls if c[0] == "destroy"][0]
    assert destroy[4] == "u2#n2"


def test_scenario_cleanup_allows_its_designed_refusal(senv):
    def body(e):
        _deploy_fake(senv, Path(e["LB_CONFIG_A"]), "na", "ua")
        _deploy_fake(senv, Path(e["LB_CONFIG_B"]), "nb", "ub")

    real = senv.runner.__call__

    def runner(args, **kw):
        if args[0] == "destroy" and "s-p6-b" in args[1].lower():
            _destroy_fake(senv, Path(args[1]))
            return H.ChildResult(3, ["deploy.identity_foreign"], kw["log"])
        return real(args, **kw)

    senv.h.runner = runner
    senv.h.script_runner = _script(senv, body)
    assert senv.h.scenario(H.SCENARIOS["S-P6"]) == 0, senv.said
    st = _scen_states(senv, "S-P6")
    assert st["S-P6-B"]["status"] == "destroyed" and st["S-P6-A"]["status"] == "destroyed"
    order = [Path(c[1]).name for c in senv.runner.calls if c[0] == "destroy"]
    assert order == ["S-P6-A.yaml"]  # B went through the designed refusal first


def test_scenario_cleanup_unexpected_refusal_fails_and_stops(senv):
    def body(e):
        _deploy_fake(senv, Path(e["LB_CONFIG_A"]), "na", "ua")

    senv.runner.destroy_override = (3, ["deploy.identity_foreign"], "")
    senv.h.script_runner = _script(senv, body)
    assert senv.h.scenario(H.SCENARIOS["S-P4"]) == 1
    assert _scen_states(senv, "S-P4")["S-P4-A"]["status"] == "failed"
    assert senv.h.stopping


def test_scenario_buckets_left_keep_the_ledger_row(senv):
    def body(e):
        cfg = Path(e["LB_CONFIG_A"])
        _deploy_fake(senv, cfg, "na", "ua")
        senv.core.namespaces.clear()  # namespace gone, buckets not

    senv.h.script_runner = _script(senv, body)
    assert senv.h.scenario(H.SCENARIOS["S-P4"]) == 1
    st = _scen_states(senv, "S-P4")["S-P4-A"]
    assert st["status"] == "left"
    assert f"| {st['namespace']} |" in senv.ledger.read_text()


def test_legacy_bucket_created_checked_and_removed(senv):
    seen = {}

    def body(e):
        seen["legacy"] = e["LB_LEGACY_BUCKET"]
        seen["objects"] = dict(senv.s3.buckets[e["LB_LEGACY_BUCKET"]])
        cfg = Path(e["LB_CONFIG_A"])
        assert (
            yaml.safe_load(cfg.read_text())["platform"]["storage"]["s3"]["buckets"]["bronze"]
            == seen["legacy"]
        )

    senv.h.script_runner = _script(senv, body)
    assert senv.h.scenario(H.SCENARIOS["S-P5"]) == 0
    assert list(seen["objects"]) == ["harness-legacy/object.txt"]
    assert seen["legacy"] not in senv.s3.buckets


def test_legacy_bucket_changed_fails_the_scenario(senv):
    def body(e):
        senv.s3.buckets[e["LB_LEGACY_BUCKET"]]["extra"] = b"x"

    senv.h.script_runner = _script(senv, body)
    assert senv.h.scenario(H.SCENARIOS["S-P5"]) == 1


def test_shared_bucket_is_bronze_of_both_configs(senv):
    seen = {}

    def body(e):
        a = yaml.safe_load(Path(e["LB_CONFIG_A"]).read_text())
        b = yaml.safe_load(Path(e["LB_CONFIG_B"]).read_text())
        seen["a"] = a["platform"]["storage"]["s3"]["buckets"]["bronze"]
        seen["b"] = b["platform"]["storage"]["s3"]["buckets"]["bronze"]
        seen["env"] = e["LB_SHARED_BUCKET"]

    senv.h.script_runner = _script(senv, body)
    senv.h.scenario(H.SCENARIOS["S-P6"])
    assert seen["a"] == seen["b"] == seen["env"]


def test_scenario_admission_asks_for_all_its_deployments(senv):
    asked = {}

    def decide(plan, fallback, size=1, namespaces=()):
        asked["size"], asked["ns"] = size, namespaces
        return C.Decision(True)

    senv.h.decide = decide
    senv.h.script_runner = _script(senv, lambda e: None)
    senv.h.scenario(H.SCENARIOS["S-P2"])
    assert asked["size"] == 2 and len(asked["ns"]) == 2


def test_group_admission_counts_size():
    d = _admit(
        C.Candidate("S", "a", C.Peak(1, 1), size=2, namespaces=("a", "b")),
        _snap(),
        managed=["x", "y", "z"],
    )
    assert not d.admit
    assert _admit(
        C.Candidate("S", "a", C.Peak(1, 1), size=2, namespaces=("a", "b")),
        _snap(),
        managed=["x", "y"],
    ).admit


def test_scenario_resume_cleans_up_without_rerunning(senv):
    plans = senv.h.scenario_rows(H.SCENARIOS["S-P4"])
    plan = plans["A"]
    senv.h._set_buckets(plan)
    senv.h.log(
        "S-P4-A",
        "planned",
        namespace=plan.namespace,
        config=str(plan.config),
        peak=[1, 1],
        scenario="S-P4",
        owned_buckets=[],
    )
    senv.h.log("S-P4-A", "ledgered")
    _deploy_fake(senv, plan.config, "na", "ua")
    H.resume(senv.h, {})
    st = _scen_states(senv, "S-P4")["S-P4-A"]
    assert st["status"] == "destroyed" and st["scenario_verdict"] == "FAIL"
    assert [c[0] for c in senv.runner.calls if c[0] != "init"] == ["destroy"]


# -- the scripts themselves --------------------------------------------------------


SCRIPTS = sorted(SCEN.glob("s-p*.sh"))


def test_six_scenario_scripts_are_tracked_with_their_specs():
    assert [p.name for p in SCRIPTS] == sorted(s.script for s in H.SCENARIOS.values())


@pytest.mark.parametrize("path", [*SCRIPTS, SCEN / "lib-checks.sh"], ids=lambda p: p.name)
def test_scenario_script_bash_syntax(path):
    assert subprocess.run(["bash", "-n", str(path)], check=False).returncode == 0


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda p: p.name)
def test_scenario_scripts_use_the_v17_cli(path):
    """No removed flag, no forced destroy, no unpinned kubectl, no reworded
    message grep; every lakebench verb and flag exists in the release tree."""
    import re

    import click
    import typer

    from lakebench.cli import app

    text = path.read_text()
    assert 'source "$(dirname "$0")/lib-checks.sh"' in text
    assert not re.search(r"--force\b(?!-)", text)
    assert "--force-legacy" not in text and "--wait" not in text
    assert not re.search(r"(^|[^.\w])kubectl ", text), "use kc (context-pinned)"
    assert not re.search(r"\bpython3 ", text)
    for stale in (
        "owned by another lakebench deployment",
        "tag-mismatch",
        "verify_bucket_ownership",
    ):
        assert stale not in text
    root = typer.main.get_command(app)
    ctx = click.Context(root)
    code = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith(("#", "echo")))
    calls = re.findall(r"(?:^|[\s(])lakebench (\w[\w-]*)([^\n>&|]*)", code)
    calls += re.findall(r"lb_run \"[^\"]+\" (\w[\w-]*)([^\n>&|]*)", code)
    assert calls
    for verb, rest in calls:
        cmd = root.get_command(ctx, verb)
        assert cmd is not None, verb
        opts = {o for p in cmd.params for o in getattr(p, "opts", [])}
        for flag in re.findall(r"(?<![\w-])(--[\w-]+)", rest):
            assert flag in opts, (path.name, verb, flag)


def test_lib_checks_reads_credentials_from_the_environment():
    text = (SCEN / "lib-checks.sh").read_text()
    assert "os.path.expandvars" in text
    assert "print(key" not in text and "print(secret" not in text
