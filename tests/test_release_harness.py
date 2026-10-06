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


def test_refuses_foreign_sha(repo):
    _git(repo.path, "commit", "-q", "--allow-empty", "-m", "later")
    assert any("not the freeze commit" in r for r in _reasons(repo))
    # --rehearsal waives only the sha check
    assert _reasons(repo, rehearsal=True) == []


def test_live_run_needs_ledger_context_and_credentials(repo, monkeypatch):
    for v in ("LAKEBENCH_S3_ACCESS_KEY", "LAKEBENCH_S3_SECRET_KEY", "LB_S3_ENDPOINT"):
        monkeypatch.delenv(v, raising=False)
    reasons = _reasons(repo, live=True, contexts=lambda: ["a"], context="b")
    assert any("--deployments-ledger is required" in r for r in reasons)
    assert any("--context b is not in the kubeconfig" in r for r in reasons)
    assert any("unset credential variables" in r for r in reasons)
    assert any("LB_S3_ENDPOINT" in r for r in reasons)


# -- matrix ------------------------------------------------------------------


def test_matrix_rejects_spent_aml_seed(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text(
        "version: x\nrows:\n  - {id: A, workload: financial, mode: batch, "
        "recipe: hive-iceberg-spark-trino, scale: 1, seed: 42}\n"
    )
    with pytest.raises(H.Refused, match="seed 43"):
        H.load_matrix(p)


def test_aml_seed_problem_never_names_the_seed(monkeypatch):
    import lakebench.config.datagen_seed as ds

    monkeypatch.setattr(ds, "seed_is_protected", lambda v, heldout=None: v == 12345)
    msg = H.aml_seed_problem(12345)
    assert msg and "12345" not in msg


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


def test_older_kept_nonce_is_not_the_rows_incarnation(env):
    plan = _plan(env)
    name = yaml.safe_load(plan.config.read_text())["name"]
    env.core.add_ns(plan.namespace, "u9", "old")
    write_state(plan.config, name, plan.namespace, [("new", "confirmed"), ("old", "confirmed")])
    inc, why = H.confirmed_incarnation(plan.config, env.core)
    assert inc is None and "older recorded nonce" in why


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


def test_destroy_exit_0_with_buckets_left_keeps_the_ledger_row(env):
    env.runner.destroy_keeps_buckets = True
    plan = _plan(env)
    _go(env, plan)
    st = _status(env)
    assert st["status"] == "left" and "buckets remain" in st["detail"]
    assert f"| {plan.namespace} |" in env.ledger.read_text()
    assert env.h.admitting_stopped


def test_scrub_rewrites_are_not_refusals_and_the_scrubbed_copy_is_judged(env):
    plan = _plan(env)
    _go(env, plan)
    assert _status(env)["verdict"] == "PASS"
    record, freeze = env.judged[0]
    assert record["scrubbed"] is True and freeze == "f" * 40


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


def test_admission_fails_closed_on_unreadable_nodes():
    d = _admit(_cand(), C.Unknown("listing nodes failed"))
    assert not d.admit and "unreadable" in d.reasons[0]


def test_reader_fails_closed_on_unreadable_nodes():
    core = FakeCore()
    core.nodes_fail = True
    assert isinstance(C.ClusterReader(core).snapshot(), C.Unknown)


def test_ledger_rows_count_toward_the_deployment_limit():
    d = _admit(_cand(), _snap(), managed=["a", "b"], ledger_live=["c", "d"])
    assert not d.admit and any("4 lakebench deployments plus 1" in r for r in d.reasons)
    assert _admit(_cand(), _snap(), managed=["a", "b"], ledger_live=["a", "c"]).admit


def test_ledger_alone_marker_of_another_harness_blocks_admission():
    d = _admit(_cand(), _snap(), ledger_live=["rel17-m16"], ledger_alone=["rel17-m16"])
    assert not d.admit and "rel17-m16" in d.blocking


def test_alone_row_needs_an_empty_cluster_and_blocks_others():
    assert not _admit(_cand(alone=True), _snap(), managed=["x"]).admit
    own = [C.ActiveRow("rel17-m16", C.Peak(1, 1), alone=True)]
    assert not _admit(_cand(), _snap(), own_active=own).admit


def test_unreadable_ledger_config_counts_the_worst_case_at_its_scale(env, tmp_path):
    worst = H.worst_case_peak(1.0)
    m14 = H.Row("M14", "financial", "continuous", "hive-iceberg-spark-trino", 1.0, 43)
    assert worst.cores >= env.h.plan_row(m14, tmp_path / "m14").peak.cores
    row = L.LedgerRow("lb17-x", str(tmp_path / "nowhere"), "v17-run", "1", "t")
    assert env.h._ledger_peak(row) == worst


def test_decide_fails_closed_on_an_unexpected_error(env):
    plan = _plan(env)
    row = SimpleNamespace(namespace="n", config="c", session="s", scale="x")
    obs = H.Observation([row], set(), None)
    assert isinstance(env.h.decide(plan, obs, C.Peak(0, 0)), C.Unknown)


# -- ledger --------------------------------------------------------------------


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


# -- results -------------------------------------------------------------------


# -- children and signals --------------------------------------------------------


def _gone(pid: int) -> bool:
    stat = Path(f"/proc/{pid}/stat")
    return not stat.exists() or stat.read_text().rsplit(")", 1)[1].split()[0] == "Z"


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


def test_scenario_redeployed_config_uses_its_current_kept_nonce(env):
    def body(e):
        cfg = Path(e["LB_CONFIG_A"])
        _deploy_fake(env, cfg, "n1", "u1")
        _destroy_fake(env, cfg)
        _deploy_fake(env, cfg, "n2", "u2")

    env.h.script_runner = _script(env, body)
    env.h.scenario(H.SCENARIOS["S-P4"])
    assert [c for c in env.runner.calls if c[0] == "destroy"][0][4] == "u2#n2"


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


def test_changed_legacy_bucket_is_left_for_a_person(env):
    def body(e):
        env.s3.buckets[e["LB_LEGACY_BUCKET"]]["extra"] = b"x"

    env.h.script_runner = _script(env, body)
    assert env.h.scenario(H.SCENARIOS["S-P5"]) == 1
    assert any(b.startswith("rel17-s-p5-legacy-") for b in env.s3.buckets)


# -- the scripts themselves --------------------------------------------------------


SCRIPTS = sorted(SCEN.glob("s-p*.sh"))


# -- brief-pass fixes ------------------------------------------------------------


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


def test_unreadable_ledger_scale_fails_admission_closed(env, tmp_path):
    plan = _plan(env)
    row = SimpleNamespace(namespace="n", config=str(tmp_path / "x"), session="s", scale="s10")
    obs = H.Observation([row], set(), C.Snapshot(C.Peak(400, 4000), {}))
    assert isinstance(env.h.decide(plan, obs, C.Peak(0, 0)), C.Unknown)


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


def _bystander(uenv, tmp_path):
    by = H.Row("BY", "customer360", "batch", "hive-iceberg-spark-trino", 1.0, 42)
    plan = uenv.h.plan_row(by, tmp_path / "by")
    name = _deploy_fake(uenv, plan.config, "nb", "ub")
    uenv.s3.buckets[f"{name}-bronze"][DATAGEN_PREFIX + "x"] = b"by"
    return plan, name


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


def _fake_python(tmp_path, version, where):
    py = tmp_path / "venv" / "bin" / "python"
    py.parent.mkdir(parents=True)
    py.write_text(f"#!/bin/sh\nprintf '%s\\n%s\\n' '{version}' '{where}'\n")
    py.chmod(0o755)
    return str(py)


def test_v16_interpreter_must_be_16_and_isolated(tmp_path):
    inside = tmp_path / "venv" / "lib" / "lakebench" / "__init__.py"
    assert H.v16_python_problem(_fake_python(tmp_path, "1.6.0", inside)) is None


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


def test_the_step_fails_when_the_cli_cleared_another_bucket(xenv):
    xenv.runner.continuous_output = (
        "Cleared 4 objects from someone-else-bronze/checkpoints\nContinuous tables reset in 9s\n"
    )
    plan = xenv.h.plan_row(M01X, xenv.h.row_dir(M01X))
    _go(xenv, plan)
    step = _status(xenv)["extra"]["continuous-after-batch"]
    assert any("cleared objects in someone-else-bronze" in p for p in step["problems"])


# -- the rehearsal's draft -------------------------------------------------------


def test_rehearsal_writes_a_draft_outside_the_tree(env, monkeypatch):
    built = {"version": "1.7.0", "entries": [], "continuous": []}
    seen: list = []

    def build(records, version, **_kw):
        seen.append(([rid for rid, _ in records], version))
        return built, []

    monkeypatch.setattr(H._expected, "build_expected", build)
    from lakebench.metrics import verdict

    monkeypatch.setattr(verdict, "passed", lambda record: True)
    env.h.rehearsal = True
    _go(env, _plan(env))
    env.h.finish()
    draft = env.out / "expected-results-1.7.0.draft.json"
    assert json.loads(draft.read_text()) == built
    run_ids = env.h.rowlog.latest()[ROW.id]["run_ids"]
    assert seen == [(run_ids, "1.7.0")] and run_ids
    assert not (H.TREE / "uat" / "expected-results-1.7.0.draft.json").exists()
    assert any("not reviewed, not evidence" in s for s in env.said)


def test_rehearsal_draft_refused_says_why_and_removes_an_old_one(env, monkeypatch):
    from lakebench.metrics import verdict

    monkeypatch.setattr(verdict, "passed", lambda record: True)
    env.h.rehearsal = True
    draft = env.out / "expected-results-1.7.0.draft.json"
    _go(env, _plan(env))
    draft.write_text("{}")
    env.h.finish()  # the fake runs' records have no experiment block
    assert not draft.exists()
    assert any("draft expected results not written" in s for s in env.said)
    assert any("no experiment block" in s for s in env.said)


