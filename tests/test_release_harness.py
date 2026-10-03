"""Release harness (scripts/release/): refusals, destroy by incarnation,
admission, resume, the ledger and the S-P scenarios. Everything runs against
fakes; nothing reaches a cluster or an object store."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "scripts" / "release"
SCEN = RELEASE / "scenarios"


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


class FakeCore:
    """A CoreV1Api with namespaces, nodes and pods in memory."""

    def __init__(self) -> None:
        self.namespaces: dict[str, dict[str, Any]] = {}
        self.nodes_fail = False
        self.read_fail = False
        self.nodes = [_node(f"w{i}", "100", "1000Gi") for i in range(4)]
        self.pods: list[Any] = []

    def read_namespace(self, name, _request_timeout=None):
        if self.read_fail:
            raise RuntimeError("apiserver unreachable")
        ns = self.namespaces.get(name)
        if ns is None:
            raise ApiException(404)
        return SimpleNamespace(
            metadata=SimpleNamespace(uid=ns["uid"], annotations=dict(ns["annotations"]), name=name)
        )

    def list_namespace(self, label_selector=None, _request_timeout=None):
        return SimpleNamespace(
            items=[SimpleNamespace(metadata=SimpleNamespace(name=n)) for n in self.namespaces]
        )

    def list_node(self, _request_timeout=None):
        if self.nodes_fail:
            raise RuntimeError("forbidden")
        return SimpleNamespace(items=self.nodes)

    def list_pod_for_all_namespaces(self, field_selector=None, _request_timeout=None):
        return SimpleNamespace(items=self.pods)

    def add_ns(self, name: str, uid: str, nonce: str) -> None:
        self.namespaces[name] = {
            "uid": uid,
            "annotations": {
                "lakebench.deployment/deploy-nonce": nonce,
                "lakebench.deployment/name": name,
            },
        }


class FakeS3:
    def __init__(self) -> None:
        self.buckets: dict[str, dict[str, bytes]] = {}
        self.owner: dict[str, str] = {}
        self.raw_client = self
        self.create_ok = True

    def bucket_exists(self, b):
        return b in self.buckets

    def create_bucket(self, b):
        if not self.create_ok:
            return False
        self.buckets.setdefault(b, {})
        return True

    def put_object(self, Bucket, Key, Body):  # noqa: N803 -- boto3 names
        self.buckets[Bucket][Key] = Body

    def get_paginator(self, _name):
        return self

    def paginate(self, Bucket, Prefix=""):  # noqa: N803
        keys = [k for k in self.buckets[Bucket] if k.startswith(Prefix)]
        return [
            {
                "Contents": [
                    {"Key": k, "Size": len(self.buckets[Bucket][k]), "ETag": "e"} for k in keys
                ]
            }
        ]

    def empty_bucket(self, b, keep_prefixes=()):
        self.buckets[b].clear()
        return 0

    def delete_bucket(self, b):
        return self.buckets.pop(b, None) is not None


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


def _buckets_of(cfg: Path) -> list[str]:
    data = yaml.safe_load(cfg.read_text())
    b = ((data.get("platform") or {}).get("storage") or {}).get("s3", {}).get("buckets") or {}
    return [b.get(k) or f"{data['name']}-{k}" for k in ("bronze", "silver", "gold")]


class FakeRunner:
    """Plays the lakebench CLI against a FakeCore and a FakeS3."""

    def __init__(self, core: FakeCore, s3: FakeS3) -> None:
        self.core = core
        self.s3 = s3
        self.calls: list[tuple[str, ...]] = []
        self.deploy_code = 0
        self.deploy_writes = "confirmed"  # confirmed | pending | none | foreign
        self.run_code = 0
        self.run_records = 1
        self.destroy_override: tuple[int, list[str], str] | None = None
        self.destroy_keeps_buckets = False
        self.continuous_output = "Continuous tables reset in 42s\n"
        self.reset_log = (
            "Continuous reset: deleted 12 data entries under s3a://{name}-silver/warehouse/t\n"
            "Continuous reset: DROP PURGE lakehouse.silver.t (iceberg)\n"
            "Continuous reset: deleted s3a://{name}-silver/warehouse/t\n"
        )
        self.on_deploy: Any = None
        self.counter = 0

    def __call__(self, args, *, cwd, log, interruptible=False, on_spawn=None, stdout=None):
        args = tuple(args)
        self.calls.append(args)
        log.parent.mkdir(parents=True, exist_ok=True)
        verb = args[0]
        if on_spawn is not None:
            on_spawn(os.getpid(), "0")
        if verb == "init":
            return self._init(args, log)
        cfg = Path(args[1])
        name = yaml.safe_load(cfg.read_text())["name"]
        if verb == "deploy":
            if self.on_deploy is not None:
                self.on_deploy(cfg)
            self.counter += 1
            nonce, uid = f"n{self.counter}", f"u{self.counter}"
            if self.deploy_writes in ("confirmed", "foreign"):
                self.core.add_ns(name, uid, nonce if self.deploy_writes == "confirmed" else "x")
                write_state(cfg, name, name, [(nonce, "confirmed")])
                for b in _buckets_of(cfg):
                    if b not in self.s3.buckets:
                        self.s3.create_bucket(b)
                        self.s3.owner[b] = name
            elif self.deploy_writes == "pending":
                self.core.add_ns(name, uid, "")
                write_state(cfg, name, name, [(nonce, "pending")])
            return H.ChildResult(self.deploy_code, [], log)
        if verb == "run":
            if "--continuous" in args:
                log.write_text(self.continuous_output)
            runs = cfg.parent / "lakebench-output" / "runs"
            for _ in range(self.run_records):
                self.counter += 1
                d = runs / f"run-20261003-{self.counter:06d}-abcdef"
                d.mkdir(parents=True)
                (d / "metrics.json").write_text(json.dumps({"run_id": d.name}))
            return H.ChildResult(self.run_code, [], log)
        if verb == "report":
            return H.ChildResult(0, [], log)
        if verb == "logs":
            stdout.write_text(self.reset_log.format(name=name))
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
            if not self.destroy_keeps_buckets:
                for b in _buckets_of(cfg):
                    if self.s3.owner.get(b) == name:
                        self.s3.buckets.pop(b, None)
            return H.ChildResult(0, [], log)
        raise AssertionError(f"unexpected verb {verb}")

    def _init(self, args, log):
        def get(flag):
            return args[args.index(flag) + 1]

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
    judged: list[Any] = []

    def verdict(record, freeze, version):
        judged.append((record, freeze))
        return []

    monkeypatch.setattr(H, "record_verdict", verdict)
    stub = SimpleNamespace(
        scrub_record=lambda rec: (
            dict(rec, scrubbed=True),
            [".config.platform.storage.s3.endpoint"],
        ),
        dump=lambda rec: json.dumps(rec) + "\n",
    )
    monkeypatch.setattr(H, "_scrub_module", lambda: stub)
    core = FakeCore()
    s3 = FakeS3()
    runner = FakeRunner(core, s3)
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
        s3_factory=lambda cfg: s3,
        judge_sha="f" * 40,
    )
    h.rows = {ROW.id: ROW}
    yield SimpleNamespace(
        h=h, core=core, runner=runner, ledger=ledger_path, said=said, out=out, s3=s3, judged=judged
    )
    rowlog.release()


def _plan(env, row=ROW):
    return env.h.plan_row(row, env.h.row_dir(row))


def _go(env, plan):
    """What schedule does for an admitted row: ledger row, then the thread body."""
    env.h.ledger_add_logged(plan)
    env.h._guarded(plan)


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
    _git(repo.path, "commit", "-q", "--allow-empty", "-m", "later")
    assert any("not the freeze commit" in r for r in _reasons(repo))
    # --rehearsal waives only the sha check
    assert _reasons(repo, rehearsal=True) == []


def test_refuses_a_freeze_that_is_not_a_commit(repo):
    assert any("is not a commit" in r for r in _reasons(repo, freeze="0" * 40))
    assert any("is not a commit" in r for r in _reasons(repo, freeze="0" * 40, rehearsal=True))


def test_freeze_resolves_to_the_full_sha(repo):
    assert H.resolve_commit(repo.path, "HEAD") == repo.sha
    assert H.resolve_commit(repo.path, repo.sha[:8]) == repo.sha
    assert H.resolve_commit(repo.path, "nope") is None


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


def test_live_run_needs_ledger_context_and_credentials(repo, monkeypatch):
    for v in ("LAKEBENCH_S3_ACCESS_KEY", "LAKEBENCH_S3_SECRET_KEY", "LB_S3_ENDPOINT"):
        monkeypatch.delenv(v, raising=False)
    reasons = _reasons(repo, live=True, contexts=lambda: ["a"], context="b")
    assert any("--deployments-ledger is required" in r for r in reasons)
    assert any("--context b is not in the kubeconfig" in r for r in reasons)
    assert any("unset credential variables" in r for r in reasons)
    assert any("LB_S3_ENDPOINT" in r for r in reasons)


def test_kubeconfig_without_contexts_refuses(repo):
    reasons = _reasons(repo, live=True, contexts=lambda: [], context="b")
    assert any("lists no contexts" in r for r in reasons)


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


def test_aml_rows_run_the_calibration_seed():
    assert H.aml_seed_problem(43) is None


def test_aml_seed_problem_never_names_the_seed(monkeypatch):
    import lakebench.config.datagen_seed as ds

    monkeypatch.setattr(ds, "protected_seeds", lambda: {12345: "evaluation"})
    msg = H.aml_seed_problem(12345)
    assert msg and "12345" not in msg


def test_every_matrix_row_config_resolves_to_its_release_versions(env):
    _, rows = H.load_matrix(RELEASE / "matrix-1.7.yaml", H.KNOWN_STEPS)
    for row in rows:
        plan = env.h.plan_row(row, env.out / "p" / row.id)
        data = yaml.safe_load(plan.config.read_text())
        assert data["platform"]["kubernetes"]["context"] == "ctx"
        assert data["workload"]["datagen"]["seed"] == row.seed
        assert plan.peak.cores > 0


def test_off_matrix_versions_refused(env):
    def bad_init(args, **kw):
        res = FakeRunner._init(env.runner, tuple(args), kw["log"])
        out = Path(args[args.index("-o") + 1])
        data = yaml.safe_load(out.read_text())
        data["images"] = {"spark": "apache/spark:4.0.2-python3"}
        out.write_text(yaml.safe_dump(data))
        return res

    env.h.runner = bad_init
    with pytest.raises(H.Refused, match="release matrix runs"):
        env.h.plan_row(ROW, env.out / "bad")


def test_plan_peak_includes_trino(env):
    # The row's peak is plan_requirements' full request: Spark plus the
    # co-resident Trino, catalog and Postgres pods, not Spark alone.
    from lakebench.config.sizing import plan_requirements
    from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

    plan = _plan(env)
    p = plan_requirements(H.load_row_config(plan.config))
    assert p.co_resident.cpu_cores >= 4, p.co_resident
    spark_only = compute_peak_requirements(1.0, "batch", "customer360")
    assert plan.peak.cores == p.full.cpu_cores
    assert plan.peak.cores >= spark_only.cpu_cores + p.co_resident.cpu_cores


# -- deploy, incarnation, destroy ----------------------------------------------


def test_ledger_row_written_before_deploy_under_the_admission_lock(env):
    plan = _plan(env)
    seen = {}

    def on_deploy(cfg):
        seen["ledger"] = plan.namespace in env.ledger.read_text()

    env.runner.on_deploy = on_deploy
    env.h.decide = lambda p, obs, f, **kw: C.Decision(True)
    env.h.schedule([plan], [])
    assert seen == {"ledger": True}
    states = [e["status"] for e in env.h.rowlog.entries()]
    assert states.index("ledgered") < states.index("deploying")
    assert any(e.get("ledger_intent") for e in env.h.rowlog.entries())


def test_full_row_destroys_by_incarnation_and_closes_ledger(env):
    plan = _plan(env)
    _go(env, plan)
    st = _status(env)
    assert st["status"] == "destroyed", st
    assert st["verdict"] == "PASS"
    assert env.runner.verbs() == ["init", "deploy", "run", "report", "destroy"]
    assert env.runner.calls[1][2:] == ("--yes", "--require-new")
    text = env.ledger.read_text()
    assert f"closed {plan.namespace} " in text
    assert f"| {plan.namespace} |" not in text
    assert (env.out / "uat" / "runs").is_dir()


def test_destroy_passes_expect_incarnation(env):
    plan = _plan(env)
    _go(env, plan)
    destroy = [c for c in env.runner.calls if c[0] == "destroy"][0]
    assert destroy[2:4] == ("--yes", "--expect-incarnation")
    assert destroy[4] == "u1#n1"
    assert "--force" not in destroy


def test_foreign_nonce_gets_no_destroy(env):
    env.runner.deploy_writes = "foreign"
    plan = _plan(env)
    _go(env, plan)
    assert "destroy" not in env.runner.verbs()
    assert _status(env)["status"] == "left"
    assert plan.namespace in env.core.namespaces
    assert env.h.admitting_stopped


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
    _go(env, plan)
    assert _status(env)["status"] == "not-deployed"
    assert f"closed {plan.namespace} " in env.ledger.read_text()
    assert env.runner.verbs().count("deploy") == 1


def test_failed_deploy_with_confirmed_nonce_is_destroyed_by_incarnation(env):
    env.runner.deploy_code = 1
    plan = _plan(env)
    _go(env, plan)
    st = _status(env)
    assert st["status"] == "destroyed" and st["verdict"] == "FAIL"
    assert env.runner.verbs().count("deploy") == 1
    assert "run" not in env.runner.verbs()


def test_failed_deploy_with_pending_nonce_is_left(env):
    env.runner.deploy_code = 1
    env.runner.deploy_writes = "pending"
    plan = _plan(env)
    _go(env, plan)
    assert _status(env)["status"] == "left"
    assert "destroy" not in env.runner.verbs()
    assert f"| {plan.namespace} |" in env.ledger.read_text()
    assert env.h.admitting_stopped


def test_missing_namespace_is_left_not_destroyed(env):
    plan = _plan(env)
    env.h.ledger_add_logged(plan)
    env.h.destroy(plan, "u1#n1")
    assert _status(env)["status"] == "left"
    assert "destroy" not in env.runner.verbs()


def test_exit_6_polls_and_never_reinvokes_destroy(env):
    plan = _plan(env)
    env.h.ledger_add_logged(plan)
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
    assert env.h.admitting_stopped


@pytest.mark.parametrize(
    "paths", [["destroy.unverified_cluster"], ["lease.held"], ["destroy.redeployed"], []]
)
def test_destroy_refusals_stop_admission_and_never_retry(env, paths):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u1", "n1")
    env.h.ledger_add_logged(plan)
    env.runner.destroy_override = (3, paths, "Destroy NOT completed")
    env.h.destroy(plan, "u1#n1")
    assert env.runner.verbs().count("destroy") == 1
    assert _status(env)["status"] == "failed"
    assert env.h.admitting_stopped
    assert f"| {plan.namespace} |" in env.ledger.read_text()


def test_incarnation_mismatch_is_destroy_refused(env):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u2", "redeployed")
    env.h.destroy(plan, "u1#n1")
    assert _status(env)["status"] == "destroy-refused"
    assert env.h.admitting_stopped
    assert plan.namespace in env.core.namespaces


def test_destroy_exit_0_with_not_completed_text_is_failed(env):
    plan = _plan(env)
    env.core.add_ns(plan.namespace, "u1", "n1")
    env.runner.destroy_override = (0, [], "Destroy NOT completed: something")
    env.h.destroy(plan, "u1#n1")
    assert _status(env)["status"] == "failed"


def test_destroy_text_of_an_earlier_invocation_is_not_read(tmp_path):
    log = tmp_path / "d.log"
    log.write_text("Destroy NOT completed: an earlier destroy\n")
    res = H.ChildResult(0, [], log, offset=log.stat().st_size)
    with open(log, "a") as fh:
        fh.write("Namespace x deleted\n")
    assert "NOT completed" not in res.text()


def test_destroy_exit_0_with_buckets_left_keeps_the_ledger_row(env):
    env.runner.destroy_keeps_buckets = True
    plan = _plan(env)
    _go(env, plan)
    st = _status(env)
    assert st["status"] == "left" and "buckets remain" in st["detail"]
    assert f"| {plan.namespace} |" in env.ledger.read_text()
    assert env.h.admitting_stopped


def test_verdict_comes_from_the_record_not_the_exit_code(env, monkeypatch):
    monkeypatch.setattr(H, "record_verdict", lambda r, f, v: ["no silver rows"])
    plan = _plan(env)
    _go(env, plan)
    st = _status(env)
    assert st["verdict"] == "FAIL" and any("no silver rows" in p for p in st["problems"])
    assert st["status"] == "destroyed"


def test_scrub_rewrites_are_not_refusals_and_the_scrubbed_copy_is_judged(env):
    plan = _plan(env)
    _go(env, plan)
    assert _status(env)["verdict"] == "PASS"
    record, freeze = env.judged[0]
    assert record["scrubbed"] is True and freeze == "f" * 40


def test_a_scrub_refusal_fails_the_row(env, monkeypatch):
    def refuse(rec):
        raise ValueError("bucket name 'silver' is a single word")

    monkeypatch.setattr(H, "_scrub_module", lambda: SimpleNamespace(scrub_record=refuse))
    plan = _plan(env)
    _go(env, plan)
    st = _status(env)
    assert st["verdict"] == "FAIL" and any("scrub refused" in p for p in st["problems"])
    assert not (env.out / "uat" / "runs").exists()


def test_a_record_that_cannot_be_judged_fails_and_the_row_is_destroyed(env, monkeypatch):
    def boom(r, f, v):
        raise KeyError("experiment")

    monkeypatch.setattr(H, "record_verdict", boom)
    plan = _plan(env)
    _go(env, plan)
    st = _status(env)
    assert st["verdict"] == "FAIL" and st["status"] == "destroyed"


def test_two_run_records_fail_the_row(env):
    env.runner.run_records = 2
    plan = _plan(env)
    _go(env, plan)
    assert _status(env)["verdict"] == "FAIL"


def test_admission_stop_lets_rows_in_flight_finish(env):
    plan = _plan(env)
    real = env.runner.__call__

    def runner(args, **kw):
        res = real(args, **kw)
        if args[0] == "run":
            env.h.stop_admission("another row failed")
        return res

    env.h.runner = runner
    _go(env, plan)
    assert _status(env)["status"] == "destroyed"


def test_interrupt_after_run_leaves_row_recorded_for_resume(env):
    plan = _plan(env)
    real = env.runner.__call__

    def runner(args, **kw):
        res = real(args, **kw)
        if args[0] == "run":
            env.h.interrupt()
        return res

    env.h.runner = runner
    _go(env, plan)
    assert _status(env)["status"] == "recorded"
    assert "destroy" not in env.runner.verbs()


def test_harness_error_keeps_the_row_resumable(env):
    plan = _plan(env)
    env.h.ledger_add_logged(plan)

    def boom(*a, **k):
        raise OSError("disk full")

    env.h.runner = boom
    env.h._guarded(plan)
    st = _status(env)
    assert st["status"] not in L.TERMINAL and "disk full" in st["harness_error"]
    assert env.h.admitting_stopped


def test_namespace_read_failure_before_destroy_keeps_status(env):
    plan = _plan(env)
    env.h.log("M01", "recorded", incarnation="u1#n1")
    env.core.read_fail = True
    env.h.destroy(plan, "u1#n1")
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


def test_resume_of_a_failed_deploy_goes_to_destroy_never_to_a_run(env):
    env.runner.deploy_code = 1
    plan = _plan(env)
    env.h.ledger_add_logged(plan)
    real = env.h.destroy
    env.h.destroy = lambda p, i: None  # the harness dies before its destroy
    env.h.deploy(plan)
    env.h.destroy = real
    assert _status(env)["status"] == "recorded" and _status(env)["verdict"] == "FAIL"
    H.resume(env.h, {"M01": ROW})
    assert "run" not in env.runner.verbs()
    st = _status(env)
    assert st["status"] == "destroyed" and st["verdict"] == "FAIL"


def test_resume_of_a_row_stopped_while_ledgering_creates_nothing(env):
    _seed_row(env, "planned", ledger_intent=True)
    H.resume(env.h, {"M01": ROW})
    assert "deploy" not in env.runner.verbs()
    assert _status(env)["status"] == "not-deployed"
    assert not env.h.admitting_stopped


def test_resume_destroying_polls_only(env):
    plan = _seed_row(env, "destroying", incarnation="u1#n1", child_pid=1, child_start="x")
    H.resume(env.h, {"M01": ROW})
    assert "destroy" not in env.runner.verbs()
    assert _status(env)["status"] == "destroyed"
    assert plan.namespace not in env.core.namespaces


def test_resume_refuses_unknown_rows_before_touching_anything(env):
    _seed_row(env, "recorded", incarnation="u1#n1")
    with pytest.raises(H.Refused, match="not in the matrix"):
        H.resume(env.h, {})
    assert _status(env)["status"] == "recorded"


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
    core.nodes = [_node("w1", "40", "400Gi"), _node("w2", "40", "400Gi")]
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


def test_the_candidates_own_ledger_row_is_counted_once():
    # a resumed row whose ledger row exists: its peak must not count twice
    d = _admit(
        _cand(cores=40.0),
        _snap(),
        ledger_live=["rel17-m01"],
        ledger_peaks={"rel17-m01": C.Peak(40.0, 1.0)},
    )
    assert d.admit, d.reasons


def test_ledger_alone_marker_of_another_harness_blocks_admission():
    d = _admit(_cand(), _snap(), ledger_live=["rel17-m16"], ledger_alone=["rel17-m16"])
    assert not d.admit and "rel17-m16" in d.blocking


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


def test_group_admission_counts_size():
    group = C.Candidate("S", "a", C.Peak(1, 1), size=2, namespaces=("a", "b"))
    assert not _admit(group, _snap(), managed=["x", "y", "z"]).admit
    assert _admit(group, _snap(), managed=["x", "y"]).admit


def test_unreadable_ledger_config_counts_the_worst_case_at_its_scale(env, tmp_path):
    worst = H.worst_case_peak(1.0)
    m14 = H.Row("M14", "financial", "continuous", "hive-iceberg-spark-trino", 1.0, 43)
    assert worst.cores >= env.h.plan_row(m14, tmp_path / "m14").peak.cores
    row = L.LedgerRow("lb17-x", str(tmp_path / "nowhere"), "v17-run", "1", "t")
    assert env.h._ledger_peak(row) == worst


def test_ledger_config_directory_resolves_to_its_one_yaml(tmp_path):
    d = tmp_path / "ledger-configs" / "lb17-x"
    d.mkdir(parents=True)
    (d / "lb17-x.yaml").write_text("name: x\n")
    assert H.ledger_config_path(str(d)) == d / "lb17-x.yaml"
    (d / "other.yaml").write_text("name: y\n")
    assert H.ledger_config_path(str(d)) is None


def test_decide_fails_closed_on_an_unexpected_error(env):
    plan = _plan(env)
    row = SimpleNamespace(namespace="n", config="c", session="s", scale="x")
    obs = H.Observation([row], set(), None)
    assert isinstance(env.h.decide(plan, obs, C.Peak(0, 0)), C.Unknown)


def test_schedule_waits_then_admits(env):
    plan = _plan(env)
    decisions = iter([C.Unknown("nodes"), C.Decision(True)])
    env.h.decide = lambda p, obs, f, **kw: next(decisions)
    sleeps: list[float] = []
    env.h.sleep = sleeps.append
    env.h.schedule([plan], [])
    assert sleeps and _status(env)["status"] == "destroyed"


def test_threads_finish_before_schedule_returns(env):
    plan = _plan(env)
    env.h.decide = lambda p, obs, f, **kw: C.Decision(True)
    env.h.schedule([plan], [])
    assert not [t for t in threading.enumerate() if t.name == "M01"]


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


def test_ledger_retries_when_edited_underneath(tmp_path):
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


def test_ledger_refuses_when_its_change_is_lost_right_after_writing(tmp_path, monkeypatch):
    p = tmp_path / "E.md"
    p.write_text(LEDGER_TEXT)
    led = L.MarkdownLedger(p, tmp_path / "b")
    real_replace = os.replace

    def replace_then_clobber(src, dst):
        real_replace(src, dst)
        Path(dst).write_text(LEDGER_TEXT)  # an unlocked writer's stale copy lands

    monkeypatch.setattr(L.os, "replace", replace_then_clobber)
    with pytest.raises(L.LedgerError, match="lost the harness's change"):
        led.add(L.LedgerRow("rel17-x", "/c", "s", "1", "t"))


def test_ledger_refuses_a_shrunken_file(tmp_path):
    p = tmp_path / "E.md"
    p.write_text(LEDGER_TEXT + "x" * 10000 + "\n")
    led = L.MarkdownLedger(p, tmp_path / "b")
    led.add(L.LedgerRow("rel17-x", "/c", "s", "1", "t"))
    p.write_text(LEDGER_TEXT)
    again = L.MarkdownLedger(p, tmp_path / "b")  # a new process: the backups remember
    with pytest.raises(L.LedgerError, match="less than 75% of its newest backup"):
        again.close("rel17-x")


def test_ledger_keeps_other_line_endings(tmp_path):
    p = tmp_path / "E.md"
    p.write_bytes(LEDGER_TEXT.replace("- entry one\n", "- entry one\r\n").encode())
    L.MarkdownLedger(p, tmp_path / "b").add(L.LedgerRow("rel17-x", "/c", "s", "1", "t"))
    assert b"- entry one\r\n" in p.read_bytes()


def test_ledger_symlink_is_edited_at_its_target(tmp_path):
    real = tmp_path / "real.md"
    real.write_text(LEDGER_TEXT)
    link = tmp_path / "link.md"
    link.symlink_to(real)
    L.MarkdownLedger(link, tmp_path / "b").add(L.LedgerRow("rel17-x", "/c", "s", "1", "t"))
    assert link.is_symlink() and "| rel17-x |" in real.read_text()


def test_ledger_lock_is_reentrant_and_excludes_other_threads(tmp_path):
    p = tmp_path / "E.md"
    p.write_text(LEDGER_TEXT)
    led = L.MarkdownLedger(p, tmp_path / "b", lock_path=tmp_path / "queue.lock")
    order: list[str] = []

    def other():
        led.close("rel17-x")
        order.append("closed")

    with led.transaction():
        led.add(L.LedgerRow("rel17-x", "/c", "s", "1", "t"))  # same thread: no deadlock
        t = threading.Thread(target=other)
        t.start()
        time.sleep(0.3)
        order.append("released")
    t.join(5)
    assert order == ["released", "closed"]


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


# -- results -------------------------------------------------------------------


def test_results_table_lists_rows_with_verdicts(env):
    plan = _plan(env)
    _go(env, plan)
    text = env.h.write_results().read_text()
    assert text.startswith("# UAT results 1.7.0\n")
    assert f"Freeze commit: {'f' * 40}" in text
    assert "| M01 | customer360 | batch | hive-iceberg-spark-trino | 4.1 / 1.11.0 | 1 |" in text
    assert "| PASS | not checked |" in text


def test_failed_rows_cite_no_run_id_in_the_table(env, monkeypatch):
    import re

    monkeypatch.setattr(H, "record_verdict", lambda r, f, v: ["bad"])
    plan = _plan(env)
    _go(env, plan)
    text = env.h.write_results().read_text()
    table = [ln for ln in text.splitlines() if ln.startswith("| M01")]
    assert table and not re.search(r"\d{8}-\d{6}-[0-9a-f]{6}", table[0])
    assert "Runs of rows that did not pass (not evidence):" in text


def test_rehearsal_results_are_not_a_uat_results_file(env):
    env.h.rehearsal = True
    path = env.h.write_results()
    assert path.name == "results-rehearsal.md"
    assert not path.read_text().startswith("# UAT results")


# -- children and signals --------------------------------------------------------


def test_sigint_level_1_reaches_runs_level_2_reaches_all(monkeypatch):
    sent: list[int] = []
    monkeypatch.setattr(H.os, "killpg", lambda pid, sig: sent.append(pid))
    r = H.ProcessRunner(ROOT)
    r._children = {11: True, 12: False}
    r.interrupt(1)
    r.interrupt(1)
    assert sent == [11]
    r.interrupt(2)
    assert sent == [11, 12]


def test_process_runner_reads_exit_paths(tmp_path):
    r = H.ProcessRunner(ROOT)
    res = r(
        ["destroy", str(tmp_path / "missing.yaml"), "--yes", "--expect-incarnation", "bad"],
        cwd=tmp_path,
        log=tmp_path / "l" / "d.log",
    )
    assert res.code == 2
    assert res.paths == ["cli.bad_argument"]


def test_process_runner_keeps_waiting_when_recording_the_child_fails(tmp_path):
    r = H.ProcessRunner(ROOT)

    def broken(pid, start):
        raise OSError("No space left on device")

    res = r(["version"], cwd=tmp_path, log=tmp_path / "v.log", on_spawn=broken)
    assert res.code == 0
    assert "could not record the child" in res.log.read_text()


def test_a_child_past_its_time_limit_is_stopped():
    proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
    start = time.monotonic()
    seq = ((H.signal.SIGINT, 2), (H.signal.SIGTERM, 2), (H.signal.SIGKILL, 2))
    assert H._wait_or_stop(proc, 0.2, seq) == H.TIMED_OUT
    assert time.monotonic() - start < 10 and proc.poll() is not None


def _gone(pid: int) -> bool:
    stat = Path(f"/proc/{pid}/stat")
    return not stat.exists() or stat.read_text().rsplit(")", 1)[1].split()[0] == "Z"


def test_script_runner_reaps_what_the_script_left_running(tmp_path, monkeypatch):
    monkeypatch.setattr(H, "SCRIPT_REAP_S", 0.5)
    script = tmp_path / "s.sh"
    pidfile = tmp_path / "bg.pid"
    script.write_text(f"sleep 120 & echo $! > {pidfile}\nexit 0\n")
    real = H.reap_group
    seq = ((H.signal.SIGTERM, 2), (H.signal.SIGKILL, 2))
    monkeypatch.setattr(H, "reap_group", lambda pgid, out: real(pgid, out, poll=0.1, sequence=seq))
    r = H.ProcessRunner(ROOT)
    rc = r.script(["bash", str(script)], env=dict(os.environ), cwd=tmp_path, log=tmp_path / "s.log")
    assert rc == 0
    bg = int(pidfile.read_text())
    deadline = time.monotonic() + 5
    while not _gone(bg) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert _gone(bg)


# -- scenarios (S-P1 to S-P6) ----------------------------------------------------


def _deploy_fake(env, cfg: Path, nonce: str, uid: str) -> str:
    name = yaml.safe_load(cfg.read_text())["name"]
    env.core.add_ns(name, uid, nonce)
    from lakebench.config.deploy_state import read_state

    old = read_state(cfg)
    kept = [(e.nonce, e.status) for e in (old.nonces if old else [])]
    write_state(cfg, name, name, [(nonce, "confirmed"), *kept])
    for b in _buckets_of(cfg):
        if b not in env.s3.buckets:
            env.s3.create_bucket(b)
            env.s3.owner[b] = name
    return name


def _destroy_fake(env, cfg: Path) -> None:
    name = yaml.safe_load(cfg.read_text())["name"]
    env.core.namespaces.pop(name, None)
    for b in _buckets_of(cfg):
        if env.s3.owner.get(b) == name:
            env.s3.buckets.pop(b, None)


def _script(env, body, rc=0, pass_line=True):
    def run(argv, *, env: dict, cwd: Path, log: Path, on_spawn=None) -> int:
        assert argv[0] == "bash" and Path(argv[1]).parent == SCEN
        assert env["LB_EXIT_REFUSED"] == "3"
        if on_spawn is not None:
            on_spawn(os.getpid(), "0")
        body(env)
        log.parent.mkdir(parents=True, exist_ok=True)
        sid = Path(argv[1]).name.split("-")[1].upper()
        with open(log, "a") as fh:
            fh.write(f"PASS: S-{sid} -- fake\n" if pass_line else "done\n")
        return rc

    return run


def _scen_states(env, sid):
    return {r: s for r, s in env.h.rowlog.latest().items() if r.startswith(sid)}


def test_scenario_shim_imports_release_tree(tmp_path):
    bin_dir = tmp_path / "bin"
    H.write_shim(ROOT, bin_dir)
    env = H.child_env(ROOT)
    env["PATH"] = f"{bin_dir}{os.pathsep}{os.environ['PATH']}"
    assert H.shim_imports_from(bin_dir, env) == str(ROOT / "src" / "lakebench" / "__init__.py")


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


def test_scenario_leftover_namespace_destroyed_by_incarnation(env):
    def body(e):
        _deploy_fake(env, Path(e["LB_CONFIG_A"]), "na", "ua")
        _deploy_fake(env, Path(e["LB_CONFIG_B"]), "nb", "ub")
        _destroy_fake(env, Path(e["LB_CONFIG_A"]))

    env.h.script_runner = _script(env, body)
    assert env.h.scenario(H.SCENARIOS["S-P1"]) == 0, env.said
    st = _scen_states(env, "S-P1")
    assert st["S-P1-A"]["status"] == "destroyed" and st["S-P1-B"]["status"] == "destroyed"
    destroys = [c for c in env.runner.calls if c[0] == "destroy"]
    assert len(destroys) == 1 and destroys[0][4] == "ub#nb"
    assert env.ledger.read_text().count("closed rel17-s-p1-") == 2
    assert st["S-P1-A"]["scenario_verdict"] == "PASS"
    assert (env.out / "results-extra.md").read_text().count("| S-P1 |") == 1
    assert "S-P1" not in env.h.write_results().read_text()


def test_scenario_kept_namespace_missing_fails(env):
    def body(e):
        for c in ("A", "B"):
            _deploy_fake(env, Path(e[f"LB_CONFIG_{c}"]), f"n{c}", f"u{c}")
            _destroy_fake(env, Path(e[f"LB_CONFIG_{c}"]))

    env.h.script_runner = _script(env, body)
    assert env.h.scenario(H.SCENARIOS["S-P1"]) == 1
    problems = _scen_states(env, "S-P1")["S-P1-A"]["scenario_problems"]
    assert any("expected present" in p for p in problems)


def test_scenario_exit_0_without_pass_line_fails(env):
    env.h.script_runner = _script(env, lambda e: None, pass_line=False)
    assert env.h.scenario(H.SCENARIOS["S-P4"]) == 1


def test_scenario_script_failure_still_cleans_up(env):
    env.h.script_runner = _script(
        env, lambda e: _deploy_fake(env, Path(e["LB_CONFIG_A"]), "na", "ua"), rc=1
    )
    assert env.h.scenario(H.SCENARIOS["S-P4"]) == 1
    assert _scen_states(env, "S-P4")["S-P4-A"]["status"] == "destroyed"
    assert not env.core.namespaces


def test_scenario_redeployed_config_uses_its_current_kept_nonce(env):
    def body(e):
        cfg = Path(e["LB_CONFIG_A"])
        _deploy_fake(env, cfg, "n1", "u1")
        _destroy_fake(env, cfg)
        _deploy_fake(env, cfg, "n2", "u2")

    env.h.script_runner = _script(env, body)
    env.h.scenario(H.SCENARIOS["S-P4"])
    assert [c for c in env.runner.calls if c[0] == "destroy"][0][4] == "u2#n2"


def test_scenario_cleanup_allows_its_designed_refusal(env):
    def body(e):
        _deploy_fake(env, Path(e["LB_CONFIG_A"]), "na", "ua")
        _deploy_fake(env, Path(e["LB_CONFIG_B"]), "nb", "ub")

    real = env.runner.__call__

    def runner(args, **kw):
        if args[0] == "destroy" and Path(args[1]).name == "S-P6-B.yaml":
            env.runner.calls.append(tuple(args))
            _destroy_fake(env, Path(args[1]))
            return H.ChildResult(3, ["deploy.identity_foreign"], kw["log"])
        return real(args, **kw)

    env.h.runner = runner
    env.h.script_runner = _script(env, body)
    assert env.h.scenario(H.SCENARIOS["S-P6"]) == 0, env.said
    st = _scen_states(env, "S-P6")
    assert st["S-P6-B"]["status"] == "destroyed" and st["S-P6-A"]["status"] == "destroyed"
    order = [Path(c[1]).name for c in env.runner.calls if c[0] == "destroy"]
    assert order == ["S-P6-B.yaml", "S-P6-A.yaml"]


def test_scenario_cleanup_unexpected_refusal_fails_and_stops(env):
    env.runner.destroy_override = (3, ["deploy.identity_foreign"], "")
    env.h.script_runner = _script(
        env, lambda e: _deploy_fake(env, Path(e["LB_CONFIG_A"]), "na", "ua")
    )
    assert env.h.scenario(H.SCENARIOS["S-P4"]) == 1
    assert _scen_states(env, "S-P4")["S-P4-A"]["status"] == "failed"
    assert env.h.admitting_stopped


def test_scenario_buckets_left_keep_the_ledger_row(env):
    def body(e):
        _deploy_fake(env, Path(e["LB_CONFIG_A"]), "na", "ua")
        env.core.namespaces.clear()  # namespace gone, buckets not

    env.h.script_runner = _script(env, body)
    assert env.h.scenario(H.SCENARIOS["S-P4"]) == 1
    st = _scen_states(env, "S-P4")["S-P4-A"]
    assert st["status"] == "left"
    assert f"| {st['namespace']} |" in env.ledger.read_text()


def test_scenario_script_pid_is_recorded_and_resume_refuses_while_it_lives(env):
    def body(e):
        _deploy_fake(env, Path(e["LB_CONFIG_A"]), "na", "ua")
        st = env.h.rowlog.latest()["S-P4-A"]
        assert st["script_pid"] == os.getpid()
        env.h.log("S-P4-A", st["status"], script_start=H._start_time(os.getpid()))
        with pytest.raises(H.Refused, match="live child"):
            H.resume(env.h, {})

    env.h.script_runner = _script(env, body)
    env.h.scenario(H.SCENARIOS["S-P4"])
    assert env.h.rowlog.latest()["S-P4-A"]["script_pid"] is None


def test_legacy_bucket_created_checked_and_removed(env):
    seen = {}

    def body(e):
        seen["legacy"] = e["LB_LEGACY_BUCKET"]
        seen["objects"] = dict(env.s3.buckets[e["LB_LEGACY_BUCKET"]])
        data = yaml.safe_load(Path(e["LB_CONFIG_A"]).read_text())
        seen["bronze"] = data["platform"]["storage"]["s3"]["buckets"]["bronze"]
        seen["name"] = data["name"]

    env.h.script_runner = _script(env, body)
    assert env.h.scenario(H.SCENARIOS["S-P5"]) == 0, env.said
    assert list(seen["objects"]) == ["harness-legacy/object.txt"]
    assert seen["bronze"] == seen["legacy"]
    assert not seen["legacy"].startswith(seen["name"])  # outside A's name prefix
    assert seen["legacy"] not in env.s3.buckets


def test_preexisting_legacy_bucket_is_never_emptied_or_deleted(env, monkeypatch):
    monkeypatch.setattr(H.secrets, "token_hex", lambda n: "abcdef")
    env.s3.buckets["rel17-s-p5-legacy-abcdef"] = {"theirs": b"x"}
    env.h.script_runner = _script(env, lambda e: None)
    assert env.h.scenario(H.SCENARIOS["S-P5"]) == 1
    assert env.s3.buckets["rel17-s-p5-legacy-abcdef"] == {"theirs": b"x"}


def test_legacy_bucket_not_created_is_never_removed(env):
    env.s3.create_ok = False
    env.h.script_runner = _script(env, lambda e: None)
    assert env.h.scenario(H.SCENARIOS["S-P5"]) == 1


def test_changed_legacy_bucket_is_left_for_a_person(env):
    def body(e):
        env.s3.buckets[e["LB_LEGACY_BUCKET"]]["extra"] = b"x"

    env.h.script_runner = _script(env, body)
    assert env.h.scenario(H.SCENARIOS["S-P5"]) == 1
    assert any(b.startswith("rel17-s-p5-legacy-") for b in env.s3.buckets)


def test_s_p6_b_names_as_bronze_bucket(env):
    seen = {}

    def body(e):
        a = yaml.safe_load(Path(e["LB_CONFIG_A"]).read_text())
        b = yaml.safe_load(Path(e["LB_CONFIG_B"]).read_text())
        seen["a"] = a["platform"]["storage"]["s3"]["buckets"]["bronze"]
        seen["b"] = b["platform"]["storage"]["s3"]["buckets"]["bronze"]
        seen["env"] = e["LB_SHARED_BUCKET"]
        seen["a_name"] = a["name"]

    env.h.script_runner = _script(env, body)
    env.h.scenario(H.SCENARIOS["S-P6"])
    assert seen["a"] == seen["b"] == seen["env"] == f"{seen['a_name']}-bronze"


def test_scenario_admission_asks_for_all_its_deployments(env):
    asked = {}

    def decide(plan, obs, fallback, size=1, namespaces=()):
        asked["size"], asked["ns"] = size, namespaces
        return C.Decision(True)

    env.h.decide = decide
    env.h.script_runner = _script(env, lambda e: None)
    env.h.scenario(H.SCENARIOS["S-P2"])
    assert asked["size"] == 2 and len(asked["ns"]) == 2


def test_scenario_resume_cleans_up_without_rerunning(env):
    plan = env.h.scenario_rows(H.SCENARIOS["S-P4"])["A"]
    env.h._set_buckets(plan)
    env.h.log(
        "S-P4-A",
        "planned",
        namespace=plan.namespace,
        config=str(plan.config),
        peak=[1, 1],
        scenario="S-P4",
        owned_buckets=[],
    )
    env.h.log("S-P4-A", "ledgered")
    _deploy_fake(env, plan.config, "na", "ua")
    H.resume(env.h, {})
    st = _scen_states(env, "S-P4")["S-P4-A"]
    assert st["status"] == "destroyed" and st["scenario_verdict"] == "FAIL"
    assert [c[0] for c in env.runner.calls if c[0] != "init"] == ["destroy"]


def test_scenario_resume_polls_a_destroy_that_was_running(env):
    plan = env.h.scenario_rows(H.SCENARIOS["S-P4"])["A"]
    env.h.log(
        "S-P4-A",
        "destroying",
        namespace=plan.namespace,
        config=str(plan.config),
        peak=[1, 1],
        scenario="S-P4",
        owned_buckets=[],
        child_pid=1,
        child_start="x",
    )
    H.resume(env.h, {})
    assert "destroy" not in env.runner.verbs()
    assert _scen_states(env, "S-P4")["S-P4-A"]["status"] == "destroyed"


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
    calls += re.findall(r'lb_run "[^"]+" (\w[\w-]*)([^\n>&|]*)', code)
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
    assert "trap _lb_reap EXIT" in text
    assert ".lakebench/owner.json" in text


def test_lib_checks_reaps_background_jobs_on_an_early_exit(tmp_path):
    pidfile = tmp_path / "bg.pid"
    script = tmp_path / "s.sh"
    script.write_text(
        f'LB_KUBE_CONTEXT=x LB_EXIT_REFUSED=3\nsource "{SCEN / "lib-checks.sh"}"\n'
        f"sleep 120 &\necho $! > {pidfile}\nexit 1\n"
    )
    rc = subprocess.run(["bash", str(script)], check=False, timeout=30).returncode
    assert rc == 1
    bg = int(pidfile.read_text())
    deadline = time.monotonic() + 5
    while not _gone(bg) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert _gone(bg)


# -- brief-pass fixes ------------------------------------------------------------


def test_stop_sequence_gives_a_lease_holder_three_terms_before_kill():
    sigs = [sig for sig, _ in H.STOP_SEQUENCE]
    assert sigs[-1] == H.signal.SIGKILL
    assert sigs.count(H.signal.SIGTERM) >= 3
    first_term = sigs.index(H.signal.SIGTERM)
    assert sum(g for _, g in H.STOP_SEQUENCE[first_term:-1]) >= 800
    text = (SCEN / "lib-checks.sh").read_text()
    assert "for grace in 900 300 300" in text and "jobs -pr" in text


def test_a_failed_ledger_close_keeps_the_row_open_for_resume(env, monkeypatch):
    plan = _plan(env)
    env.h.ledger_add_logged(plan)

    real_close = env.h.ledger.close
    broken = {"on": True}

    def flaky_close(ns, when=None):
        if broken["on"]:
            raise L.LedgerError("kept changing")
        return real_close(ns, when)

    monkeypatch.setattr(env.h.ledger, "close", flaky_close)
    env.h._guarded(plan)
    st = _status(env)
    assert st["status"] == "destroying" and "ledger close pending" in st["detail"]
    broken["on"] = False
    env.h._admission.clear()
    H.resume(env.h, {"M01": ROW})
    assert _status(env)["status"] == "destroyed"
    assert f"closed {plan.namespace} " in env.ledger.read_text()


def test_resume_refuses_while_a_process_names_the_rows_config(env):
    plan = _seed_row(env, "deploying", child_pid=None, child_start=None)
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", str(plan.config)])
    try:
        time.sleep(0.2)
        with pytest.raises(H.Refused, match="live child"):
            H.resume(env.h, {"M01": ROW})
    finally:
        proc.kill()
        proc.wait()


def test_after_a_second_ctrl_c_a_new_destroy_is_not_signalled(monkeypatch):
    sent: list[int] = []
    monkeypatch.setattr(H.os, "killpg", lambda pid, sig: sent.append(pid))
    r = H.ProcessRunner(ROOT)
    r.interrupt(1)
    r.interrupt(2)
    r._register(21, False)  # a cleanup destroy started afterwards
    r._register(22, True)  # a run started afterwards
    assert sent == [22]


def test_a_failed_deploy_after_ctrl_c_is_not_destroyed(env):
    env.runner.deploy_code = 1
    plan = _plan(env)
    env.h.ledger_add_logged(plan)
    env.h.interrupt()
    env.h.deploy(plan)
    assert _status(env)["status"] == "recorded"
    assert "destroy" not in env.runner.verbs()


def test_unreadable_ledger_scale_fails_admission_closed(env, tmp_path):
    plan = _plan(env)
    row = SimpleNamespace(namespace="n", config=str(tmp_path / "x"), session="s", scale="s10")
    obs = H.Observation([row], set(), C.Snapshot(C.Peak(400, 4000), {}))
    assert isinstance(env.h.decide(plan, obs, C.Peak(0, 0)), C.Unknown)


def test_alone_marker_is_the_harness_session_suffix_only():
    assert H.ALONE_SESSION.fullmatch("release-harness abcdef12 row M16 alone")
    assert not H.ALONE_SESSION.fullmatch("v17-run working alone today")


def test_scenario_stopped_destroy_is_polled_against_the_buckets_it_owns(env):
    plan = env.h.scenario_rows(H.SCENARIOS["S-P5"])["A"]
    owned = env.h._set_buckets(plan, "rel17-s-p5-legacy-abcdef")
    env.s3.buckets["rel17-s-p5-legacy-abcdef"] = {"harness-legacy/object.txt": b"x"}
    env.h.ledger_add_logged(plan)
    env.h.log(
        "S-P5-A",
        "destroying",
        namespace=plan.namespace,
        config=str(plan.config),
        peak=[1, 1],
        scenario="S-P5",
        owned_buckets=owned,
        legacy_bucket="rel17-s-p5-legacy-abcdef",
        legacy_keys=["harness-legacy/object.txt"],
        child_pid=1,
        child_start="x",
    )
    H.resume(env.h, {})
    st = _scen_states(env, "S-P5")["S-P5-A"]
    assert st["status"] == "destroyed", st
    assert "rel17-s-p5-legacy-abcdef" not in env.s3.buckets


def test_a_legacy_bucket_left_after_terminal_rows_is_still_removed(env):
    plan = env.h.scenario_rows(H.SCENARIOS["S-P5"])["A"]
    env.s3.buckets["rel17-s-p5-legacy-abcdef"] = {"harness-legacy/object.txt": b"x"}
    env.h.log(
        "S-P5-A",
        "destroyed",
        namespace=plan.namespace,
        config=str(plan.config),
        peak=[1, 1],
        scenario="S-P5",
        owned_buckets=[],
        legacy_bucket="rel17-s-p5-legacy-abcdef",
        legacy_keys=["harness-legacy/object.txt"],
    )
    H.resume(env.h, {})
    assert "rel17-s-p5-legacy-abcdef" not in env.s3.buckets


# -- upgrade from 1.6 ------------------------------------------------------------

DATAGEN_PREFIX = "customer/interactions/"
TABLES = ("silver.customer_interactions_enriched", "gold.customer_executive_dashboard")


class FakeV16(FakeRunner):
    """The 1.6 CLI: init writes a 1.6-style config, deploy stamps a nonce but
    writes no local state, run generates bronze and a PASSED record, query
    prints 1.6's JSON and then its 'N rows' line on stdout."""

    def __init__(self, core, s3, counts=None):
        super().__init__(core, s3)
        self.counts = counts or {TABLES[0]: 100, TABLES[1]: 5}
        self.stage_rows = 7

    def __call__(self, args, *, cwd, log, interruptible=False, on_spawn=None, stdout=None):
        args = tuple(args)
        self.calls.append(args)
        log.parent.mkdir(parents=True, exist_ok=True)
        if on_spawn is not None:
            on_spawn(os.getpid(), "0")
        verb = args[0]
        if verb == "init":
            assert "--no-interactive" in args
            return self._init(args, log)
        cfg = Path(args[1])
        name = yaml.safe_load(cfg.read_text())["name"]
        if verb == "deploy":
            self.core.add_ns(name, "u1", "n1")
            for b in _buckets_of(cfg):
                self.s3.create_bucket(b)
                self.s3.owner[b] = name
            return H.ChildResult(self.deploy_code, [], log)
        if verb == "run":
            assert "--generate" in args
            self.s3.buckets[f"{name}-bronze"][DATAGEN_PREFIX + "part-0.parquet"] = b"data"
            d = cfg.parent / "lakebench-output" / "runs" / "run-20261003-000001-a1b2c3"
            d.mkdir(parents=True)
            jobs = [{"job_name": j, "output_rows": self.stage_rows} for j in H.V16_STAGES]
            (d / "metrics.json").write_text(
                json.dumps({"verdict": {"status": "PASSED"}, "jobs": jobs})
            )
            return H.ChildResult(self.run_code, [], log)
        if verb == "query":
            assert args[-2:] == ("--format", "json")
            table = args[args.index("--sql") + 1].split("lakehouse.", 1)[1]
            body = json.dumps({"rows": [{"0": f'"{self.counts[table]}"'}], "count": 1}, indent=2)
            stdout.write_text(f"{body}\n\n1 rows in 0.20s\n")
            return H.ChildResult(0, [], log)
        raise AssertionError(f"1.6 runner got {verb}")


class FakeV17(FakeRunner):
    """This tree's CLI over a 1.6 deployment: init --from, deploy adopting
    the namespace, run without --generate, query --json."""

    def __init__(self, core, s3, counts=None):
        super().__init__(core, s3)
        self.counts = counts or {TABLES[0]: 100, TABLES[1]: 5}
        self.run_touches_bronze = False
        self.deploy_leaves_pending = False

    def __call__(self, args, *, cwd, log, interruptible=False, on_spawn=None, stdout=None):
        args = tuple(args)
        if args[0] == "init" and "--from" in args:
            self.calls.append(args)
            old = yaml.safe_load(Path(args[args.index("--from") + 1]).read_text())
            name = old["name"]
            new = {
                "name": name,
                "recipe": old.get("recipe", "hive-iceberg-spark-trino"),
                "workload": {"schema": "customer360", "datagen": {"scale": 1.0}},
                "platform": {
                    "kubernetes": {"context": old["platform"]["kubernetes"]["context"]},
                    "storage": {
                        "s3": {
                            **old["platform"]["storage"]["s3"],
                            "buckets": {k: f"{name}-{k}" for k in ("bronze", "silver", "gold")},
                        }
                    },
                },
            }
            Path(args[args.index("-o") + 1]).write_text(yaml.safe_dump(new))
            return H.ChildResult(0, [], log)
        if args[0] == "deploy":
            assert "--require-new" not in args
            self.calls.append(args)
            if on_spawn is not None:
                on_spawn(os.getpid(), "0")
            cfg = Path(args[1])
            name = yaml.safe_load(cfg.read_text())["name"]
            ns = self.core.namespaces[name]
            ns["annotations"]["lakebench.deployment/deploy-nonce"] = "n2"
            status = "pending" if self.deploy_leaves_pending else "confirmed"
            write_state(cfg, name, name, [("n2", status)])
            return H.ChildResult(self.deploy_code, [], log)
        if args[0] == "run":
            assert "--generate" not in args
            if self.run_touches_bronze:
                name = yaml.safe_load(Path(args[1]).read_text())["name"]
                self.s3.buckets[f"{name}-bronze"][DATAGEN_PREFIX + "part-0.parquet"] = b"other"
        if args[0] == "query":
            self.calls.append(args)
            assert args[-1] == "--json"
            table = args[args.index("--sql") + 1].split("lakehouse.", 1)[1]
            doc = {"schema": "lb-cli/1", "data": {"rows": [[str(self.counts[table])]]}}
            stdout.write_text(json.dumps(doc))
            return H.ChildResult(0, [], log)
        return super().__call__(
            args, cwd=cwd, log=log, interruptible=interruptible, on_spawn=on_spawn
        )


@pytest.fixture
def uenv(env, monkeypatch):
    env.h.runner = FakeV17(env.core, env.s3)
    env.runner = env.h.runner
    env.v16 = FakeV16(env.core, env.s3)
    env.h.v16_runner = env.v16
    collected: list[str] = []

    def collect(plan, run_id):
        collected.append(run_id)
        return []

    monkeypatch.setattr(env.h, "collect_upgrade", collect)
    env.collected = collected
    return env


def _up(env):
    return env.h.rowlog.latest()["UPGRADE"]


def test_upgrade_deploys_with_16_and_runs_and_destroys_with_this_tree(uenv):
    assert uenv.h.upgrade() == 0, uenv.said
    st = _up(uenv)
    assert st["status"] == "destroyed" and st["upgrade_verdict"] == "PASS"
    assert [c[0] for c in uenv.v16.calls] == ["init", "deploy", "run", "query", "query"]
    v17 = [c[0] for c in uenv.runner.calls]
    assert v17 == ["init", "deploy", "query", "query", "run", "destroy"]
    destroy = [c for c in uenv.runner.calls if c[0] == "destroy"][0]
    assert destroy[3:5] == ("--expect-incarnation", "u1#n2")
    assert st["v16_incarnation"] == "u1#n1"
    assert st["counts_v16"] == st["counts_after_v17_deploy"] == {TABLES[0]: 100, TABLES[1]: 5}
    assert f"closed {st['namespace']} " in uenv.ledger.read_text()
    assert len(uenv.collected) == 1
    assert "upgrade (1.6 to 1.7.0)" in (uenv.out / "results-extra.md").read_text()
    assert "UPGRADE" not in uenv.h.write_results().read_text()


def test_upgrade_writes_new_before_anything_is_deployed(uenv):
    seen = {}
    real = uenv.v16.__call__

    def v16(args, **kw):
        if args[0] == "deploy":
            led = uenv.ledger.read_text()
            st = uenv.h.rowlog.latest()["UPGRADE"]
            seen["new_exists"] = Path(st["config"]).is_file()
            seen["ledger_names_new"] = st["config"] in led
        return real(args, **kw)

    uenv.h.v16_runner = v16
    uenv.h.upgrade()
    assert seen == {"new_exists": True, "ledger_names_new": True}


def test_upgrade_old_config_pins_the_context_and_keeps_credentials_as_references(uenv):
    uenv.h.upgrade()
    old = yaml.safe_load(Path(_up(uenv)["v16_config"]).read_text())
    assert old["platform"]["kubernetes"]["context"] == "ctx"
    assert old["platform"]["storage"]["s3"]["access_key"] == "${LAKEBENCH_S3_ACCESS_KEY}"


def test_upgrade_refuses_a_namespace_that_exists_before_the_16_deploy(uenv, monkeypatch):
    monkeypatch.setattr(H.secrets, "token_hex", lambda n: "abcdef")
    uenv.core.add_ns("rel17-up-abcdef", "x", "y")
    with pytest.raises(H.Refused, match="exists before the 1.6 deploy"):
        uenv.h.upgrade()
    assert "deploy" not in [c[0] for c in uenv.v16.calls]


def test_upgrade_fails_when_this_tree_changes_the_16_bronze(uenv):
    uenv.runner.run_touches_bronze = True
    assert uenv.h.upgrade() == 1
    assert any("bronze changed" in p for p in _up(uenv)["upgrade_problems"])
    assert _up(uenv)["status"] == "destroyed"


def test_upgrade_fails_when_the_17_deploy_changes_the_16_tables(uenv):
    uenv.runner.counts = {TABLES[0]: 0, TABLES[1]: 5}
    assert uenv.h.upgrade() == 1
    assert any("changed through the 1.7 deploy" in p for p in _up(uenv)["upgrade_problems"])


def test_upgrade_fails_when_the_16_tables_are_empty(uenv):
    uenv.v16.counts = {TABLES[0]: 0, TABLES[1]: 0}
    uenv.runner.counts = dict(uenv.v16.counts)
    assert uenv.h.upgrade() == 1
    assert any("unreadable or empty" in p for p in _up(uenv)["upgrade_problems"])


def test_upgrade_fails_when_a_16_stage_wrote_no_rows(uenv):
    uenv.v16.stage_rows = 0
    assert uenv.h.upgrade() == 1
    assert any("no output rows" in p for p in _up(uenv)["upgrade_problems"])


def test_upgrade_leaves_a_namespace_redeployed_before_the_17_deploy(uenv, monkeypatch):
    real = uenv.h.datagen_listing

    def listing(config):
        name = yaml.safe_load(config.read_text())["name"]
        uenv.core.namespaces[name]["uid"] = "someone-else"
        return real(config)

    monkeypatch.setattr(uenv.h, "datagen_listing", listing)
    assert uenv.h.upgrade() == 1
    assert _up(uenv)["status"] == "left"
    assert "deploy" not in [c[0] for c in uenv.runner.calls]
    assert "destroy" not in [c[0] for c in uenv.runner.calls]


def test_a_failed_17_deploy_is_destroyed_by_an_incarnation_this_row_made(uenv):
    uenv.runner.deploy_code = 1
    assert uenv.h.upgrade() == 1
    destroy = [c for c in uenv.runner.calls if c[0] == "destroy"]
    assert destroy and destroy[0][4] == "u1#n2"
    assert _up(uenv)["status"] == "destroyed"
    assert "run" not in [c[0] for c in uenv.runner.calls]


def test_a_17_deploy_left_pending_is_destroyed_by_its_own_recorded_nonce(uenv):
    uenv.runner.deploy_code = 1
    uenv.runner.deploy_leaves_pending = True
    assert uenv.h.upgrade() == 1
    destroy = [c for c in uenv.runner.calls if c[0] == "destroy"]
    assert destroy and destroy[0][4] == "u1#n2"


def test_a_failed_16_deploy_with_its_identity_is_destroyed_by_the_16_nonce(uenv):
    uenv.v16.deploy_code = 1
    assert uenv.h.upgrade() == 1
    destroy = [c for c in uenv.runner.calls if c[0] == "destroy"]
    assert destroy and destroy[0][4] == "u1#n1"
    assert _up(uenv)["status"] == "destroyed"


def test_a_failed_16_deploy_that_created_nothing_is_not_deployed(uenv):
    def no_deploy(args, **kw):
        uenv.v16.calls.append(tuple(args))
        if args[0] == "deploy":
            return H.ChildResult(1, [], kw["log"])
        return FakeV16.__call__(uenv.v16, args, **kw)

    uenv.h.v16_runner = no_deploy
    assert uenv.h.upgrade() == 1
    assert _up(uenv)["status"] == "not-deployed"
    assert f"closed {_up(uenv)['namespace']} " in uenv.ledger.read_text()


def test_upgrade_stops_before_the_17_deploy_after_ctrl_c(uenv):
    real = uenv.v16.__call__

    def v16(args, **kw):
        res = real(args, **kw)
        if args[0] == "query":
            uenv.h.interrupt()
        return res

    uenv.h.v16_runner = v16
    uenv.h.upgrade()
    assert "deploy" not in [c[0] for c in uenv.runner.calls]


def _bystander(uenv, tmp_path):
    by = H.Row("BY", "customer360", "batch", "hive-iceberg-spark-trino", 1.0, 42)
    plan = uenv.h.plan_row(by, tmp_path / "by")
    name = _deploy_fake(uenv, plan.config, "nb", "ub")
    uenv.s3.buckets[f"{name}-bronze"][DATAGEN_PREFIX + "x"] = b"by"
    return plan, name


def test_upgrade_bystander_untouched_passes(uenv, tmp_path):
    plan, _name = _bystander(uenv, tmp_path)
    assert uenv.h.upgrade(plan.config) == 0, uenv.said
    assert _up(uenv)["bystander_checked"] is True


def test_upgrade_bystander_damage_is_caught(uenv, tmp_path):
    plan, name = _bystander(uenv, tmp_path)
    real = uenv.runner.__call__

    def runner(args, **kw):
        res = real(args, **kw)
        if args[0] == "destroy":
            uenv.s3.buckets[f"{name}-bronze"].clear()
            uenv.s3.buckets.pop(f"{name}-gold")
        return res

    uenv.h.runner = runner
    assert uenv.h.upgrade(plan.config) == 1
    problems = _up(uenv)["upgrade_problems"]
    assert any("bystander lost" in p for p in problems)
    assert any("bystander buckets gone" in p for p in problems)


def test_an_unreadable_bystander_never_skips_the_destroy(uenv, tmp_path):
    assert uenv.h.upgrade(tmp_path / "missing.yaml") == 1
    assert _up(uenv)["status"] == "destroyed"
    assert any("bystander: not read" in p for p in _up(uenv)["upgrade_problems"])


def test_upgrade_resume_destroys_by_the_16_incarnation_without_rerunning(uenv):
    name = "rel17-up-abcdef"
    v16dir = uenv.out / "upgrade" / "v16"
    v17dir = uenv.out / "upgrade" / "v17"
    v16dir.mkdir(parents=True)
    v17dir.mkdir(parents=True)
    old = v16dir / f"{name}.yaml"
    FakeRunner._init(
        uenv.v16,
        (
            "init",
            "-n",
            name,
            "-r",
            "hive-iceberg-spark-trino",
            "-w",
            "customer360",
            "-s",
            "1",
            "--endpoint",
            "http://10.0.1.50:80",
            "-o",
            str(old),
        ),
        uenv.out / "l.log",
    )
    data = yaml.safe_load(old.read_text())
    data["platform"]["kubernetes"] = {"context": "ctx"}
    old.write_text(yaml.safe_dump(data))
    new = v17dir / f"{name}.yaml"
    uenv.runner(["init", "--from", str(old), "-o", str(new)], cwd=v17dir, log=uenv.out / "l2.log")
    uenv.runner.calls.clear()
    uenv.core.add_ns(name, "u1", "n1")
    uenv.h.log(
        "UPGRADE",
        "running",
        namespace=name,
        config=str(new),
        v16_config=str(old),
        peak=[1, 1],
        upgrade=True,
        v16_incarnation="u1#n1",
        incarnation="u1#n1",
        owned_buckets=[f"{name}-{k}" for k in ("bronze", "silver", "gold")],
        child_pid=1,
        child_start="x",
    )
    H.resume(uenv.h, {})
    assert "run" not in [c[0] for c in uenv.runner.calls]
    destroy = [c for c in uenv.runner.calls if c[0] == "destroy"]
    assert destroy and destroy[0][4] == "u1#n1"
    st = _up(uenv)
    assert st["status"] == "destroyed" and st["upgrade_verdict"] == "FAIL"


def test_collect_upgrade_skips_only_the_image_and_lineage_checks(env, tmp_path, monkeypatch):
    rec = json.loads(
        (ROOT / "uat" / "runs" / "run-20260929-212900-5105a0" / "metrics.json").read_text()
    )
    monkeypatch.setattr(
        H, "_scrub_module", lambda: SimpleNamespace(scrub_record=lambda r: (r, []), dump=json.dumps)
    )
    plan = H.RowPlan(H.UPGRADE_ROW, "ns", tmp_path / "c" / "c.yaml", C.Peak(1, 1))
    run_dir = plan.config.parent / "lakebench-output" / "runs" / "run-x"
    run_dir.mkdir(parents=True)
    (run_dir / "metrics.json").write_text(json.dumps(rec))
    env.h.judge_sha = rec["provenance"]["git_sha"]
    problems = env.h.collect_upgrade(plan, "run-x")
    assert not any("datagen image" in p or "corpus" in p for p in problems), problems
    env.h.judge_sha = "0" * 40
    assert any("is not from" in p for p in env.h.collect_upgrade(plan, "run-x"))


def _fake_python(tmp_path, version, where):
    py = tmp_path / "venv" / "bin" / "python"
    py.parent.mkdir(parents=True)
    py.write_text(f"#!/bin/sh\nprintf '%s\\n%s\\n' '{version}' '{where}'\n")
    py.chmod(0o755)
    return str(py)


def test_v16_interpreter_must_be_16_and_isolated(tmp_path):
    inside = tmp_path / "venv" / "lib" / "lakebench" / "__init__.py"
    assert H.v16_python_problem(_fake_python(tmp_path, "1.6.0", inside)) is None


def test_v16_interpreter_refuses_another_version(tmp_path):
    inside = tmp_path / "venv" / "lib" / "lakebench" / "__init__.py"
    assert "not 1.6" in H.v16_python_problem(_fake_python(tmp_path, "1.7.0", inside))


def test_v16_interpreter_refuses_a_lakebench_outside_its_environment(tmp_path):
    where = ROOT / "src" / "lakebench" / "__init__.py"
    assert "outside its own environment" in H.v16_python_problem(
        _fake_python(tmp_path, "1.6.0", where)
    )


def test_counts_parse_from_16_json_with_its_trailing_line_and_from_the_17_envelope():
    v16 = json.dumps({"rows": [{"0": '"24274519"'}], "count": 1}, indent=2) + "\n\n1 rows in 0.2s\n"
    assert H.parse_count(v16) == 24274519
    v17 = json.dumps({"schema": "lb-cli/1", "data": {"rows": [["366"]]}})
    assert H.parse_count(v17) == 366
    with pytest.raises(ValueError):
        H.parse_count("\n0 rows in 0.1s\n")


def test_counts_parse_through_colour_codes():
    v16 = "\x1b[1m" + json.dumps({"rows": [{"0": '"7"'}], "count": 1}) + "\x1b[0m\n1 rows in 0.2s\n"
    assert H.parse_count(v16) == 7


def test_collect_upgrade_names_a_missing_experiment_block(env, tmp_path, monkeypatch):
    monkeypatch.setattr(
        H, "_scrub_module", lambda: SimpleNamespace(scrub_record=lambda r: (r, []), dump=json.dumps)
    )
    plan = H.RowPlan(H.UPGRADE_ROW, "ns", tmp_path / "c" / "c.yaml", C.Peak(1, 1))
    run_dir = plan.config.parent / "lakebench-output" / "runs" / "run-x"
    run_dir.mkdir(parents=True)
    (run_dir / "metrics.json").write_text(json.dumps({"verdict": {"status": "PASSED"}}))
    assert any("no experiment block" in p for p in env.h.collect_upgrade(plan, "run-x"))


def test_upgrade_keeps_a_copy_of_the_16_record_before_querying(uenv):
    uenv.h.upgrade()
    copies = list((uenv.out / "extra" / "v16-runs").glob("*/metrics.json"))
    assert len(copies) == 1 and json.loads(copies[0].read_text())["verdict"]["status"] == "PASSED"


def test_a_namespace_appearing_while_waiting_for_admission_is_not_deployed(uenv, monkeypatch):
    real = uenv.h.admit_together

    def admit(label, plans):
        real(label, plans)
        uenv.core.add_ns(plans[0].namespace, "zz", "other")

    monkeypatch.setattr(uenv.h, "admit_together", admit)
    assert uenv.h.upgrade() == 1
    assert "deploy" not in [c[0] for c in uenv.v16.calls]
    assert _up(uenv)["status"] == "not-deployed"


# -- extra steps -----------------------------------------------------------------

M01X = H.Row(
    "M01",
    "customer360",
    "batch",
    "hive-iceberg-spark-trino",
    1.0,
    42,
    extra_steps=("continuous-after-batch",),
)


@pytest.fixture
def xenv(env, monkeypatch):
    monkeypatch.setattr(H, "_judge_extra", lambda record, sha: [])
    env.h.rows = {"M01": M01X}
    return env


def test_m01_runs_continuous_after_batch_and_only_c360_batch_rows_may(tmp_path):
    _, rows = H.load_matrix(RELEASE / "matrix-1.7.yaml", H.KNOWN_STEPS)
    assert {r.id: r.extra_steps for r in rows if r.extra_steps} == {
        "M01": ("continuous-after-batch",)
    }
    p = tmp_path / "m.yaml"
    p.write_text(
        "version: x\nrows:\n  - {id: A, workload: financial, mode: batch, "
        "recipe: hive-iceberg-spark-trino, scale: 1, seed: 43, "
        "extra_steps: [continuous-after-batch]}\n"
    )
    with pytest.raises(H.Refused, match="Customer 360 batch row only"):
        H.load_matrix(p, H.KNOWN_STEPS)


def test_a_row_with_continuous_after_batch_is_admitted_at_the_larger_peak(xenv):
    plain = _plan(xenv)
    with_step = xenv.h.plan_row(M01X, xenv.out / "x")
    assert with_step.peak.cores > plain.peak.cores  # continuous s1 needs more cores


def test_continuous_after_batch_runs_before_the_destroy_and_passes(xenv):
    plan = xenv.h.plan_row(M01X, xenv.h.row_dir(M01X))
    _go(xenv, plan)
    verbs = xenv.runner.verbs()
    assert verbs == ["init", "deploy", "run", "report", "run", "logs", "destroy"]
    assert {"--continuous", "--force-reset", "--skip-deploy"} <= set(xenv.runner.calls[4])
    st = _status(xenv)
    assert st["status"] == "destroyed" and st["verdict"] == "PASS"
    step = st["extra"]["continuous-after-batch"]
    assert step["verdict"] == "PASS" and len(step["run_ids"]) == 1
    assert (xenv.out / "extra" / "runs" / step["run_ids"][0]).is_dir()
    assert step["run_ids"][0] not in [p.name for p in (xenv.out / "uat" / "runs").iterdir()]
    assert "continuous-after-batch" in (xenv.out / "results-extra.md").read_text()
    assert xenv.h.finish() == 0


def test_continuous_after_batch_fails_when_the_reset_was_refused(xenv):
    xenv.runner.continuous_output = "Refusing to reset continuous state: not owned\n"
    plan = xenv.h.plan_row(M01X, xenv.h.row_dir(M01X))
    _go(xenv, plan)
    step = _status(xenv)["extra"]["continuous-after-batch"]
    assert step["verdict"] == "FAIL"
    assert any("refused to reset" in p for p in step["problems"])
    assert _status(xenv)["status"] == "destroyed"
    assert xenv.h.finish() == 1


def test_extra_steps_are_skipped_when_the_row_failed(xenv, monkeypatch):
    monkeypatch.setattr(H, "record_verdict", lambda r, f, v: ["bad"])
    plan = xenv.h.plan_row(M01X, xenv.h.row_dir(M01X))
    _go(xenv, plan)
    assert _status(xenv)["extra"]["continuous-after-batch"]["verdict"] == "SKIPPED"
    assert xenv.runner.verbs().count("run") == 1


def test_a_step_stopped_midway_is_destroyed_on_resume_and_counts_as_missing(xenv):
    plan = xenv.h.plan_row(M01X, xenv.h.row_dir(M01X))
    xenv.h.log(
        "M01",
        "planned",
        namespace=plan.namespace,
        config=str(plan.config),
        peak=[plan.peak.cores, plan.peak.gib],
    )
    xenv.core.add_ns(plan.namespace, "u1", "n1")
    xenv.h.log(
        "M01",
        "recorded",
        incarnation="u1#n1",
        verdict="PASS",
        run_ids=["run-x"],
        extra_running="continuous-after-batch",
        child_pid=1,
        child_start="x",
    )
    H.resume(xenv.h, {"M01": M01X})
    assert _status(xenv)["status"] == "destroyed"
    assert xenv.h.finish() == 1


def test_reset_lines_inside_own_buckets_pass_and_foreign_or_kept_fail():
    own = ["n-bronze", "n-silver", "n-gold"]
    good = (
        "Continuous reset: deleted 3 data entries under s3a://n-gold/w/t\n"
        "Continuous reset: DROP PURGE lakehouse.gold.t (iceberg)\n"
    )
    assert H.reset_problems(good, own) == []
    assert any("outside" in p for p in H.reset_problems(good.replace("n-gold", "other"), own))
    kept = (
        good + "Continuous reset: kept s3a://other/x (outside this deployment or not its own dir)\n"
    )
    assert any("kept" in p for p in H.reset_problems(kept, own))
    no_purge = "Continuous reset: DROP lakehouse.gold.t (iceberg)\n"
    assert any("PURGE" in p for p in H.reset_problems(no_purge, own))
    assert H.reset_problems("nothing", own)


def test_the_step_fails_on_a_foreign_reset_location(xenv):
    xenv.runner.reset_log = (
        "Continuous reset: deleted s3a://someone-else/x\nContinuous reset: DROP PURGE t (iceberg)\n"
    )
    plan = xenv.h.plan_row(M01X, xenv.h.row_dir(M01X))
    _go(xenv, plan)
    step = _status(xenv)["extra"]["continuous-after-batch"]
    assert step["verdict"] == "FAIL" and any("outside" in p for p in step["problems"])


def test_the_step_quotes_the_refusal_it_saw(xenv):
    xenv.runner.continuous_output = (
        "Refusing to reset continuous state: this deployment already holds data\n"
    )
    plan = xenv.h.plan_row(M01X, xenv.h.row_dir(M01X))
    _go(xenv, plan)
    step = _status(xenv)["extra"]["continuous-after-batch"]
    assert any("already holds data" in p for p in step["problems"])


def test_the_step_never_runs_on_another_incarnation(xenv, monkeypatch):
    plan = xenv.h.plan_row(M01X, xenv.h.row_dir(M01X))
    real = xenv.runner.__call__

    def runner(args, **kw):
        res = real(args, **kw)
        if args[0] == "report":  # someone redeploys the namespace meanwhile
            name = yaml.safe_load(plan.config.read_text())["name"]
            xenv.core.namespaces[name]["uid"] = "other"
        return res

    xenv.h.runner = runner
    _go(xenv, plan)
    step = _status(xenv)["extra"]["continuous-after-batch"]
    assert step["verdict"] == "SKIPPED"
    assert xenv.runner.verbs().count("run") == 1


def test_results_extra_lists_a_missing_step(xenv):
    xenv.h.log("M01", "planned", namespace="n", config="c", peak=[1, 1])
    xenv.h.log("M01", "destroyed", verdict="PASS")
    assert (
        "| M01 continuous-after-batch | n | MISSING |" in xenv.h.write_extra_results().read_text()
    )
