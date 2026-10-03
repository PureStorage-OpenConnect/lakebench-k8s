"""A registered (held-out) corpus's seed reaches the cluster only through a
Kubernetes Secret (owner decision 10-03): never in the datagen Job's
arguments, the SparkApplication spec, or the aml_gate.py command line.
Development seeds (43 and the rest) render exactly as before.

Every held-out value here is a test-only seed from
``tests/fixtures/heldout_test_seeds.py``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

from lakebench.config import datagen_seed as ds
from lakebench.config import seed_secret as ss
from tests.conftest import make_config
from tests.fixtures import heldout_test_seeds as ts

REPO = Path(__file__).resolve().parents[1]
EV, RB = ts.TEST_EVALUATION_SEED, ts.TEST_ROBUSTNESS_SEED


def _spellings(seed: int) -> list[str]:
    return [str(seed), f"{seed:_}", hex(seed)]


def _no_seed_in(text: str) -> bool:
    return not any(s in text for seed in (EV, RB) for s in _spellings(seed))


@pytest.fixture(autouse=True)
def _fixture_heldout(monkeypatch):
    ts.use_fixture(monkeypatch)
    opened = {
        **{
            k: v
            for k, v in ds._corpora().items()
            if k not in ("evaluation_seed", "robustness_seed")
        },
        "registered_looks_open": True,
    }
    monkeypatch.setattr(ds, "_corpora", lambda: opened)


def _cfg(**dg):
    return make_config(architecture={"workload": {"schema": "financial", "datagen": dg}})


def _registered():
    return _cfg(seed=EV, corpus_role="evaluation")


def _render(cfg):
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    engine = DeploymentEngine(cfg, dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    text = engine.renderer.render("datagen/job.yaml.j2", ctx)
    return text, yaml.safe_load(text)["spec"]["template"]["spec"]["containers"][0]


# ---------------------------------------------------------------------------
# Which configs use the Secret
# ---------------------------------------------------------------------------


def test_only_registered_financial_corpora_use_the_secret():
    assert ss.uses_seed_secret(_registered())
    assert ss.uses_seed_secret(
        _cfg(seed=RB, corpus_role="robustness", robustness_perturbation=True)
    )
    assert not ss.uses_seed_secret(_cfg(seed=43))
    assert not ss.uses_seed_secret(_cfg(seed=43, corpus_role="calibration"))
    assert not ss.uses_seed_secret(_cfg())
    assert not ss.uses_seed_secret(
        make_config(architecture={"workload": {"schema": "customer360", "datagen": {"seed": 7}}})
    )


def test_unreadable_record_uses_the_secret(monkeypatch):
    cfg = _cfg(seed=7777)

    def gone():
        raise FileNotFoundError("heldout_hashes.json not found")

    monkeypatch.setattr(ds, "_heldout", gone)
    assert ss.uses_seed_secret(cfg)


def test_secret_name_follows_the_seed_and_hides_it():
    a, b = (
        ss.seed_secret_name(_registered()),
        ss.seed_secret_name(_cfg(seed=RB, corpus_role="robustness", robustness_perturbation=True)),
    )
    assert a != b and a.startswith(ss.SEED_SECRET_PREFIX) and len(a) <= 63
    assert _no_seed_in(a + b)


# ---------------------------------------------------------------------------
# The datagen Job
# ---------------------------------------------------------------------------


def test_registered_job_has_no_seed_argument_and_reads_the_secret():
    cfg = _registered()
    text, c = _render(cfg)
    assert "--seed" not in c["args"]
    env = {e["name"]: e for e in c["env"]}
    assert env["LB_DATAGEN_SEED"] == ss.seed_secret_env(cfg)
    assert "value" not in env["LB_DATAGEN_SEED"]
    assert _no_seed_in(text)


def test_development_job_is_unchanged():
    # Seed 43 keeps --seed in the args and gets no secret env; nothing else
    # in the container moves. (The seed-43 render was also compared byte for
    # byte with the parent commit's template when this was written.)
    _, c = _render(_cfg(seed=43))
    assert c["args"][c["args"].index("--seed") + 1] == "43"
    assert "LB_DATAGEN_SEED" not in {e["name"] for e in c["env"]}
    _, reg = _render(_registered())
    dev_args = list(c["args"])
    i = dev_args.index("--seed")
    del dev_args[i : i + 2]
    assert reg["args"] == dev_args
    assert [e for e in reg["env"] if e["name"] != "LB_DATAGEN_SEED"] == c["env"]


# ---------------------------------------------------------------------------
# The Secret
# ---------------------------------------------------------------------------


class _ApiException(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status = status


def _obj(body):
    md = body["metadata"]
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=md["name"], labels=md.get("labels"), annotations=md.get("annotations")
        )
    )


@pytest.fixture
def core(monkeypatch):
    import kubernetes.client.rest as rest

    monkeypatch.setattr(rest, "ApiException", _ApiException)
    store: dict[str, dict] = {}
    c = MagicMock()

    def read(name, ns):
        if name not in store:
            raise _ApiException(404)
        return _obj(store[name])

    def create(ns, body):
        if body["metadata"]["name"] in store:
            raise _ApiException(409)
        store[body["metadata"]["name"]] = body

    def delete(name, ns):
        if store.pop(name, None) is None:
            raise _ApiException(404)

    def listed(ns, label_selector=""):
        return SimpleNamespace(items=[_obj(b) for b in store.values()])

    c.read_namespaced_secret.side_effect = read
    c.create_namespaced_secret.side_effect = create
    c.delete_namespaced_secret.side_effect = delete
    c.list_namespaced_secret.side_effect = listed
    c.store = store
    return c


def _foreign(name, owner="someone-else", ref="x"):
    return {
        "metadata": {
            "name": name,
            "labels": {
                "app.kubernetes.io/instance": owner,
                "app.kubernetes.io/component": ss.SEED_SECRET_COMPONENT,
            },
            "annotations": {ss.SEED_REF_ANNOTATION: ref},
        }
    }


def test_secret_holds_the_seed_and_its_ref(core):
    from lakebench.deploy.datagen import ensure_seed_secret

    cfg = _registered()
    ensure_seed_secret(cfg, core)
    body = core.store[ss.seed_secret_name(cfg)]
    assert body["immutable"] is True and body["type"] == "Opaque"
    assert body["stringData"] == {ss.SEED_SECRET_KEY: str(EV)}
    assert body["metadata"]["annotations"][ss.SEED_REF_ANNOTATION] == ds.seed_ref("financial", EV)
    labels = body["metadata"]["labels"]
    assert labels["app.kubernetes.io/instance"] == cfg.name
    assert labels["app.kubernetes.io/component"] == ss.SEED_SECRET_COMPONENT
    # The same seed again changes nothing.
    ensure_seed_secret(cfg, core)
    assert core.create_namespaced_secret.call_count == 1
    assert core.delete_namespaced_secret.call_count == 0


def test_another_seed_gets_another_secret_and_the_old_one_goes(core):
    # Names are never reused for another seed, so a kubelet still caching an
    # immutable Secret cannot serve its value to a new pod.
    from lakebench.deploy.datagen import ensure_seed_secret

    first = _registered()
    ensure_seed_secret(first, core)
    other = _cfg(seed=RB, corpus_role="robustness", robustness_perturbation=True)
    ensure_seed_secret(other, core)
    assert list(core.store) == [ss.seed_secret_name(other)]
    assert core.store[ss.seed_secret_name(other)]["stringData"] == {ss.SEED_SECRET_KEY: str(RB)}


def test_a_foreign_secret_is_never_used_or_replaced(core):
    from lakebench.deploy.datagen import SeedSecretError, ensure_seed_secret

    cfg = _registered()
    name = ss.seed_secret_name(cfg)
    core.store[name] = _foreign(name, ref=ds.seed_ref("financial", EV))
    with pytest.raises(SeedSecretError, match="not this deployment") as e:
        ensure_seed_secret(cfg, core)
    assert core.delete_namespaced_secret.call_count == 0
    assert _no_seed_in(str(e.value))
    # Ours by label but for another seed_ref under this name: refused too.
    core.store[name] = _foreign(name, owner=cfg.name, ref="0" * 64)
    with pytest.raises(SeedSecretError):
        ensure_seed_secret(cfg, core)


def test_secret_errors_never_hold_the_seed(core):
    from lakebench.deploy.datagen import SeedSecretError, ensure_seed_secret

    def boom(*a, **k):
        raise _ApiException(500)

    for call in ("read_namespaced_secret", "create_namespaced_secret", "list_namespaced_secret"):
        saved = getattr(core, call).side_effect
        getattr(core, call).side_effect = boom
        with pytest.raises(SeedSecretError) as e:
            ensure_seed_secret(_registered(), core)
        assert e.value.__cause__ is None and e.value.__suppress_context__, call
        assert _no_seed_in(str(e.value)), call
        getattr(core, call).side_effect = saved
        core.store.clear()


def test_a_development_generate_drops_this_deployments_secrets(core, caplog):
    from lakebench.deploy.datagen import drop_seed_secrets, ensure_seed_secret

    ensure_seed_secret(_registered(), core)
    core.store["lakebench-datagen-seed-other"] = _foreign("lakebench-datagen-seed-other")
    drop_seed_secrets(_cfg(seed=43), core)
    assert list(core.store) == ["lakebench-datagen-seed-other"]
    # A failure is logged, not raised, and names no seed.
    ensure_seed_secret(_registered(), core)

    def boom(*a, **k):
        raise _ApiException(403)

    core.delete_namespaced_secret.side_effect = boom
    drop_seed_secrets(_cfg(seed=43), core)
    assert "HTTP 403" in caplog.text and _no_seed_in(caplog.text)


def test_prepare_picks_ensure_or_drop(monkeypatch):
    import lakebench.deploy.datagen as dg

    calls = []
    monkeypatch.setattr(dg, "ensure_seed_secret", lambda cfg, c: calls.append("ensure"))
    monkeypatch.setattr(dg, "drop_seed_secrets", lambda cfg, c: calls.append("drop"))
    k8s = SimpleNamespace(_core_v1=object())
    dg.prepare_seed_secret(_registered(), k8s)
    dg.prepare_seed_secret(_cfg(seed=43), k8s)
    assert calls == ["ensure", "drop"]


def _deployer(cfg, applied):
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    k8s = MagicMock()
    k8s.apply_manifest.side_effect = lambda m, namespace=None: applied.append(m["kind"]) or True
    engine = DeploymentEngine(cfg, dry_run=True)
    engine.dry_run = False
    engine.k8s = k8s
    d = DatagenDeployer(engine)
    d.stop_previous_job = lambda: applied.append("stopped")
    d._clear_bronze_prefix_if_fresh = lambda *a, **k: applied.append("cleared")
    return d, k8s


@pytest.mark.parametrize("cycles", [False, True])
def test_secret_written_after_the_old_pods_stop_and_before_the_job(monkeypatch, cycles):
    import lakebench.deploy.datagen as dg

    applied: list[str] = []
    monkeypatch.setattr(dg, "prepare_seed_secret", lambda cfg, k8s: applied.append("secret"))
    d, _ = _deployer(_registered(), applied)
    r = d.deploy_cycle(0, 2) if cycles else d.deploy()
    assert r.status.value == "success", r.message
    assert applied == ["stopped", "secret", "cleared", "Job"]


def test_a_secret_failure_submits_no_job(monkeypatch, caplog):
    import lakebench.deploy.datagen as dg

    def fail(cfg, k8s):
        raise dg.SeedSecretError("could not create Secret x (HTTP 403)")

    monkeypatch.setattr(dg, "prepare_seed_secret", fail)
    applied: list[str] = []
    d, k8s = _deployer(_registered(), applied)
    r = d.deploy()
    assert r.status.value == "failed"
    assert "Job" not in applied and "cleared" not in applied
    assert _no_seed_in(r.message) and _no_seed_in(caplog.text)


def _progress(monkeypatch, waiting):
    import kubernetes.client as kc

    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    job = SimpleNamespace(
        status=SimpleNamespace(succeeded=0, failed=0, active=1),
        spec=SimpleNamespace(completions=1),
        metadata=SimpleNamespace(uid="u1"),
    )
    cs = SimpleNamespace(
        last_state=None,
        restart_count=0,
        state=SimpleNamespace(waiting=waiting),
    )
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            name="dg-0",
            annotations={},
            owner_references=[SimpleNamespace(uid="u1", kind="Job")],
        ),
        status=SimpleNamespace(phase="Pending", container_statuses=[cs]),
    )
    monkeypatch.setattr(
        kc, "BatchV1Api", lambda: SimpleNamespace(read_namespaced_job_status=lambda **k: job)
    )
    monkeypatch.setattr(
        kc,
        "CoreV1Api",
        lambda: SimpleNamespace(list_namespaced_pod=lambda *a, **k: SimpleNamespace(items=[pod])),
    )
    return DatagenDeployer(DeploymentEngine(_registered(), dry_run=True)).get_progress()


def test_missing_secret_fails_the_wait_fast(monkeypatch):
    missing = SimpleNamespace(
        reason="CreateContainerConfigError",
        message='secret "lakebench-datagen-seed-0123" not found',
    )
    p = _progress(monkeypatch, missing)
    assert p["crash_pods"] == ["dg-0"] and "missing" in p["crash_details"]["dg-0"]
    # A transient secret-cache timeout is left to recover.
    flaky = SimpleNamespace(
        reason="CreateContainerConfigError", message="failed to sync secret cache: timed out"
    )
    assert _progress(monkeypatch, flaky)["crash_pods"] == []


# ---------------------------------------------------------------------------
# The reference scorer
# ---------------------------------------------------------------------------


def _scorer_env(cfg):
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    mgr = SparkJobManager(cfg, MagicMock())
    return {e["name"]: e for e in mgr._build_env_vars(JobType.SCORE_FINANCIAL_REFERENCE)}


def test_registered_scorer_reads_the_seed_from_the_secret():
    env = _scorer_env(_registered())
    assert env["LB_DATAGEN_SEED"] == ss.seed_secret_env(_registered())
    assert _no_seed_in(json.dumps(env))
    assert "LB_SEED" not in env
    dev = _scorer_env(_cfg(seed=43))
    assert dev["LB_DATAGEN_SEED"] == {"name": "LB_DATAGEN_SEED", "value": "43"}
    assert dev["LB_SEED"] == {"name": "LB_SEED", "value": "43"}


def test_no_registered_spark_job_carries_the_seed():
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    mgr = SparkJobManager(_registered(), MagicMock())
    built = 0
    for jt in JobType:
        try:
            manifest = mgr._build_manifest(jt)
        except Exception:  # noqa: BLE001 -- a job type this config cannot build
            continue
        built += 1
        assert _no_seed_in(json.dumps(manifest, default=str)), jt
    assert built >= 5


# ---------------------------------------------------------------------------
# The entrypoint
# ---------------------------------------------------------------------------


def _entrypoint_argv(monkeypatch, argv, env_seed=None):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "datagen_entrypoint_secret", REPO / "datagen_rs" / "entrypoint.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    seen = {}

    def execvp(prog, cmd):
        seen["cmd"] = cmd
        raise SystemExit(0)

    monkeypatch.setattr(mod.os, "execvp", execvp)
    monkeypatch.setattr(mod, "detect_memory_limit_bytes", lambda: None)
    if env_seed is None:
        monkeypatch.delenv("LB_DATAGEN_SEED", raising=False)
    else:
        monkeypatch.setenv("LB_DATAGEN_SEED", env_seed)
    monkeypatch.setattr(sys, "argv", ["entrypoint", *argv])
    try:
        rc = mod.main()
    except SystemExit as e:
        rc = e.code
    return rc, seen.get("cmd")


BASE = ["--schema", "financial", "--bucket", "b", "--scale", "1"]


def test_entrypoint_passes_an_env_seed_on_in_the_environment(monkeypatch, capsys):
    rc, cmd = _entrypoint_argv(monkeypatch, BASE, env_seed=str(EV))
    assert rc == 0 and "--seed" not in cmd and str(EV) not in cmd
    out = capsys.readouterr()
    assert _no_seed_in(out.out + out.err)


def test_entrypoint_argv_seed_unchanged(monkeypatch):
    rc, cmd = _entrypoint_argv(monkeypatch, [*BASE, "--seed", "43"])
    assert rc == 0 and cmd[cmd.index("--seed") + 1] == "43"


@pytest.mark.parametrize(
    ("argv", "env"),
    [([*BASE, "--seed", "43"], "43"), (BASE, "4x3"), (BASE, ""), (BASE, "-5")],
)
def test_entrypoint_refuses_a_bad_or_doubled_seed(monkeypatch, capsys, argv, env):
    rc, cmd = _entrypoint_argv(monkeypatch, argv, env_seed=env)
    assert rc == 2 and cmd is None
    err = capsys.readouterr().err
    assert "LB_DATAGEN_SEED" in err
    assert not env or env not in err.replace("LB_DATAGEN_SEED", "")


# ---------------------------------------------------------------------------
# aml_gate.py
# ---------------------------------------------------------------------------


def _gate():
    from tests.conftest import exec_repo_script

    return exec_repo_script(REPO / "scripts/aml_gate.py", "aml_gate_seed_secret")


def test_seed_file_must_be_owner_only(tmp_path):
    g = _gate()
    f = tmp_path / "seed"
    f.write_text(f"{EV}\n")
    os.chmod(f, 0o644)
    with pytest.raises(ValueError, match="chmod 600") as e:
        g.read_seed_file(f)
    assert _no_seed_in(str(e.value))
    os.chmod(f, 0o600)
    assert g.read_seed_file(f) == EV
    f.write_text("not-a-seed")
    with pytest.raises(ValueError, match="content not shown"):
        g.read_seed_file(f)


@pytest.mark.parametrize("role", ["evaluation", "robustness"])
def test_registered_look_refuses_a_seed_on_the_command_line(tmp_path, role):
    corpus = tmp_path / "c"
    corpus.mkdir()
    r = subprocess.run(
        [
            sys.executable,
            str(REPO / "scripts/aml_gate.py"),
            str(corpus),
            "--registered",
            role,
            "--seed",
            "7777",
            "--out",
            str(tmp_path / "o.json"),
            "--generator-image",
            "repo@sha256:" + "0" * 64,
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(REPO / "src")},
        timeout=120,
    )
    assert r.returncode == 1
    assert "--seed-file" in r.stderr and "never from --seed" in r.stderr


def _run_gate(monkeypatch, tmp_path, argv, stop_at="predictions"):
    """aml_gate.main in-process with the test hash fixture; the look
    preconditions after the seed checks are stubbed so the test sees which
    seed reached them."""
    g = _gate()
    seen = {}
    monkeypatch.setattr(g, "clean_checkout_error", lambda: None)

    def recorded(seed):
        seen["seed"] = seed
        return None

    monkeypatch.setattr(g, "seed_ever_recorded", recorded)
    monkeypatch.setattr(g, "predictions_error", lambda image: f"stop at {stop_at}")
    corpus = tmp_path / "c"
    corpus.mkdir(exist_ok=True)
    rc = g.main([str(corpus), *argv])
    return rc, seen


def _seed_file(tmp_path, seed, mode=0o600):
    f = tmp_path / "seed"
    f.write_text(f"{seed}\n")
    os.chmod(f, mode)
    return f


LOOK = ["--generator-image", "repo@sha256:" + "0" * 64]


def test_registered_look_takes_its_seed_from_the_file(monkeypatch, tmp_path, capsys):
    f = _seed_file(tmp_path, EV)
    rc, seen = _run_gate(
        monkeypatch,
        tmp_path,
        ["--registered", "evaluation", "--seed-file", str(f), "--out", str(tmp_path / "o.json")]
        + LOOK,
    )
    err = capsys.readouterr().err
    assert rc == 1 and "stop at predictions" in err, err
    assert seen["seed"] == EV
    assert _no_seed_in(err)


def test_a_heldout_seed_is_refused_on_the_command_line_in_any_mode(monkeypatch, tmp_path, capsys):
    for extra in (["--counts-only"], [], ["--registered", "evaluation"]):
        rc, seen = _run_gate(monkeypatch, tmp_path, ["--seed", str(EV), *extra, *LOOK])
        err = capsys.readouterr().err
        assert rc == 1 and "--seed-file" in err and "seed" not in seen, (extra, err)
        assert _no_seed_in(err)


def test_seed_given_twice_or_badly_is_refused(monkeypatch, tmp_path, capsys):
    f = _seed_file(tmp_path, 43)
    rc, _ = _run_gate(monkeypatch, tmp_path, ["--seed", "43", "--seed-file", str(f)])
    assert rc == 1 and "give the seed once" in capsys.readouterr().err
    g = _gate()
    with pytest.raises(ValueError, match="not a regular file"):
        g.read_seed_file(tmp_path)
    big = _seed_file(tmp_path, 1 << 63)
    with pytest.raises(ValueError, match="outside"):
        g.read_seed_file(big)
