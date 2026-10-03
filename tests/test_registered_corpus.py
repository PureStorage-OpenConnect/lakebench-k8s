"""``generate --registered-corpus`` and the local corpus ledger (SAF-5).

A protected corpus is generated only with the flag; the flag refuses a config
that names none; the ``attempted`` entry is on disk before the first cluster
call, a handled failure appends ``failed``, success appends ``generated``,
and a crash leaves ``attempted`` alone. No output, ledger line, sidecar or
journal names the seed. TEST VALUES ONLY (the fixture held-out record).
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.config import datagen_seed as ds
from tests.fixtures import protected_corpus as pc


@pytest.fixture
def env(tmp_path, monkeypatch):
    """The fixture held-out record, ledgers under tmp_path, no git."""
    monkeypatch.chdir(tmp_path)
    held = pc.use_heldout(monkeypatch)
    monkeypatch.setenv("LB_AML_CORPORA_LEDGER", str(tmp_path / "corpora.jsonl"))
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "looks.jsonl"))
    # generate sets LB_RUN_ID when it is unset; keep it out of later tests.
    monkeypatch.setenv("LB_RUN_ID", "20261002-000000-test00")
    monkeypatch.setattr(ds, "seed_ever_recorded", lambda seed: None)
    from lakebench.metrics import provenance

    monkeypatch.setattr(provenance, "sample", lambda: {"git_sha": "abc1234", "git_dirty": False})
    return SimpleNamespace(held=held, tmp=tmp_path, ledger=tmp_path / "corpora.jsonl")


class _Calls(list):
    pass


@pytest.fixture
def cluster(monkeypatch):
    """The cluster, as calls that raise unless a test fakes them; each call
    records whether the ledger already held an ``attempted`` line."""
    import subprocess

    import boto3
    import kubernetes.config

    import lakebench.cli._generate as gen
    from lakebench.k8s.client import K8sClient
    from lakebench.s3 import S3Client

    calls = _Calls()

    def stop(name):
        def call(*a, **k):
            calls.append(name)
            raise AssertionError(f"cluster call: {name}")

        return call

    monkeypatch.setattr(kubernetes.config, "load_kube_config", stop("load_kube_config"))
    monkeypatch.setattr(K8sClient, "__init__", stop("K8sClient"))
    monkeypatch.setattr(S3Client, "__init__", stop("S3Client"))
    monkeypatch.setattr(boto3, "client", stop("boto3.client"))
    monkeypatch.setattr(subprocess, "run", stop("subprocess.run"))
    monkeypatch.setattr(gen, "get_k8s_client", stop("get_k8s_client"))
    return calls


def _lines(path: Path) -> list[dict]:
    return [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else []


def _gen(cfg, *extra):
    return CliRunner().invoke(app, ["generate", str(cfg), *extra])


def _no_seed(*texts):
    for t in texts:
        assert pc.seed_tokens(t) == [], "a held-out seed was written"


# -- refusals before any cluster call ----------------------------------------


def test_protected_config_without_the_flag_is_refused(env, cluster):
    cfg = pc.financial_config(env.tmp / "c.yaml", seed=pc.EV, role="evaluation")
    r = _gen(cfg, "--yes")
    assert r.exit_code == 2, r.output
    assert "only with --registered-corpus" in r.output
    assert cluster == [] and not env.ledger.exists()
    _no_seed(r.output)


def test_flag_on_a_config_that_names_no_protected_corpus_is_refused(env, cluster):
    cfg = pc.financial_config(env.tmp / "c.yaml", seed=pc.CALIBRATION)
    r = _gen(cfg, "--registered-corpus", "--yes")
    assert r.exit_code == 2 and "declares neither role" in r.output, r.output
    assert cluster == [] and not env.ledger.exists()


def test_flag_needs_yes(env, cluster):
    cfg = pc.financial_config(env.tmp / "c.yaml", seed=pc.EV, role="evaluation")
    r = _gen(cfg, "--registered-corpus")
    assert r.exit_code == 2 and "needs --yes" in r.output, r.output
    assert cluster == [] and not env.ledger.exists()


def test_flag_refuses_writing_over_stale_bronze(env, cluster):
    cfg = pc.financial_config(env.tmp / "c.yaml", seed=pc.EV, role="evaluation")
    r = _gen(cfg, "--registered-corpus", "--yes", "--allow-stale-bronze")
    assert r.exit_code == 2 and "never writes over objects" in r.output, r.output
    assert cluster == [] and not env.ledger.exists()


def test_a_development_generate_into_a_registered_prefix_is_refused(env, cluster):
    from lakebench.config import LoadPurpose, load_config
    from lakebench.deploy.datagen import bronze_datagen_prefix

    dev = pc.financial_config(env.tmp / "dev.yaml", seed=pc.CALIBRATION)
    cfg = load_config(dev, purpose=LoadPurpose.MUTATE)
    uri = f"s3://{cfg.platform.storage.s3.buckets.bronze}/{bronze_datagen_prefix(cfg)}/"
    ds.append_corpus_ledger(
        {
            "kind": "registered_corpus",
            "state": "generated",
            "role": "robustness",
            "bronze_uri": uri,
            "attempt": "a1",
        }
    )
    r = _gen(dev, "--yes", "--allow-stale-bronze")
    assert r.exit_code == 2 and "holds a registered robustness corpus" in r.output, r.output
    assert cluster == []


def test_a_seed_with_a_look_is_never_generated_again(env, cluster, monkeypatch):
    monkeypatch.setattr(
        ds, "seed_ever_recorded", lambda seed: "the registered evaluation seed is in the ledger"
    )
    cfg = pc.financial_config(env.tmp / "c.yaml", seed=pc.EV, role="evaluation")
    r = _gen(cfg, "--registered-corpus", "--yes")
    assert r.exit_code == 2 and "never generated again" in r.output, r.output
    assert cluster == [] and not env.ledger.exists()


def test_an_unreadable_look_history_refuses(env, cluster, monkeypatch):
    def broken(seed):
        raise OSError("git log over aml_registered_looks.json failed (exit 128)")

    monkeypatch.setattr(ds, "seed_ever_recorded", broken)
    cfg = pc.financial_config(env.tmp / "c.yaml", seed=pc.EV, role="evaluation")
    r = _gen(cfg, "--registered-corpus", "--yes")
    assert r.exit_code == 2 and "cannot be checked" in r.output, r.output
    assert cluster == [] and not env.ledger.exists()


# -- the ledger --------------------------------------------------------------


def _fake_generate(monkeypatch, env, *, deploy=None, complete=True):
    """Fake everything generate touches after the attempted entry; the first
    cluster-side call asserts the attempted entry is already on disk."""
    import lakebench.cli._generate as gen
    import lakebench.deploy as deploy_mod
    from lakebench.deploy import DeploymentStatus

    seen = []

    def first_cluster_call(*a, **k):
        seen.append([e["state"] for e in _lines(env.ledger)])
        raise RuntimeError("no capacity read in this test")

    monkeypatch.setattr(gen, "get_k8s_client", first_cluster_call)
    monkeypatch.setattr(gen, "enforce_bronze_gate", lambda *a, **k: None)
    monkeypatch.setattr("lakebench.metrics.datagen_aggregator.drop_sidecar", lambda ns: None)
    monkeypatch.setattr(deploy_mod, "DeploymentEngine", lambda cfg: SimpleNamespace())

    class Datagen:
        def __init__(self, engine, allow_stale_bronze=False):
            pass

        def deploy(self):
            if deploy is not None:
                return deploy()
            return SimpleNamespace(
                status=DeploymentStatus.SUCCESS, message="", details={"parallelism": 1}
            )

        def get_progress(self):
            return {"completions": 1, "running": False}

        def wait_for_completion(self, timeout_seconds):
            status = DeploymentStatus.SUCCESS if complete else DeploymentStatus.FAILED
            return SimpleNamespace(
                status=status,
                message="" if complete else "datagen pods failed",
                details={"succeeded": 1, "completions": 1, "failed": 0},
                elapsed_seconds=1.0,
            )

    monkeypatch.setattr(deploy_mod, "DatagenDeployer", Datagen)
    fleet = SimpleNamespace(
        to_dict=lambda: {"seed": pc.EV, "image_ids": ["repo@sha256:" + "a" * 64]},
        pods_reported=1,
        pods_expected=1,
        aggregate_mbps=1.0,
        cpu_hr_per_tb=None,
    )
    monkeypatch.setattr("lakebench.metrics.datagen_aggregator.collect_from_k8s", lambda **k: fleet)
    return seen


def _registered(env):
    return pc.financial_config(env.tmp / "c.yaml", seed=pc.EV, role="evaluation")


def test_attempted_is_written_before_the_first_cluster_call(env, monkeypatch):
    seen = _fake_generate(monkeypatch, env)
    r = _gen(_registered(env), "--registered-corpus", "--yes")
    assert r.exit_code == 0, r.output
    assert seen == [["attempted"]]


def test_success_appends_generated_with_the_image_digest(env, monkeypatch):
    _fake_generate(monkeypatch, env)
    r = _gen(_registered(env), "--registered-corpus", "--yes")
    assert r.exit_code == 0, r.output
    entries = _lines(env.ledger)
    assert [e["state"] for e in entries] == ["attempted", "submitting", "generated"]
    assert len({e["attempt"] for e in entries}) == 1
    assert entries[2]["image_ids"] == ["repo@sha256:" + "a" * 64]
    first = entries[0]
    assert first["kind"] == "registered_corpus" and first["role"] == "evaluation"
    assert first["seed_hash"] == ds.seed_hash(env.held.salt, pc.EV)
    assert first["lakebench_commit"] == "abc1234" and first["config_sha256"]
    assert first["bronze_uri"].endswith("/pacs008/")
    sidecar = next((env.tmp / "lakebench-output" / "datagen").glob("*-datagen-metrics.json"))
    side = json.loads(sidecar.read_text())
    assert side["seed"] is None and side["seed_ref"] == first["seed_hash"]
    journals = "".join(p.read_text() for p in (env.tmp / "lakebench-output").rglob("*.jsonl"))
    _no_seed(env.ledger.read_text(), r.output, sidecar.read_text(), journals)


def test_a_handled_failure_after_submit_appends_failed(env, monkeypatch):
    _fake_generate(monkeypatch, env, complete=False)
    r = _gen(_registered(env), "--registered-corpus", "--yes")
    assert r.exit_code == 1, r.output
    entries = _lines(env.ledger)
    assert [e["state"] for e in entries] == ["attempted", "submitting", "failed"]
    assert entries[2]["submitted"] is True and "may still be writing" in entries[2]["note"]


def test_a_refused_submit_appends_failed_not_submitted(env, monkeypatch):
    from lakebench.deploy import DeploymentStatus

    _fake_generate(
        monkeypatch,
        env,
        deploy=lambda: SimpleNamespace(status=DeploymentStatus.FAILED, message="no", details={}),
    )
    r = _gen(_registered(env), "--registered-corpus", "--yes")
    assert r.exit_code == 1, r.output
    assert [e["state"] for e in _lines(env.ledger)] == ["attempted", "submitting", "failed"]


def test_a_crash_after_submit_leaves_only_attempted(env, monkeypatch):
    def crash():
        raise RuntimeError("the process died after the Job was created")

    _fake_generate(monkeypatch, env, deploy=crash)
    r = _gen(_registered(env), "--registered-corpus", "--yes")
    assert r.exit_code != 0
    # The process "died" after the Job was created: the attempt and the
    # submit stand, with no outcome.
    entries = _lines(env.ledger)
    assert [e["state"] for e in entries] == ["attempted", "submitting"]
    _no_seed(env.ledger.read_text(), r.output)


def test_the_ledger_refuses_a_plaintext_integer():
    with pytest.raises(ValueError, match="hashes"):
        ds.append_corpus_ledger({"seed": 43})


def test_an_unwritable_ledger_submits_nothing(env, cluster, monkeypatch):
    def full(entry):
        raise OSError("No space left on device")

    monkeypatch.setattr(ds, "append_corpus_ledger", full)
    r = _gen(_registered(env), "--registered-corpus", "--yes")
    assert r.exit_code == 1 and "Could not record the attempt" in r.output, r.output
    assert cluster == []


def test_look_ledger_moved_into_datagen_seed(tmp_path, monkeypatch):
    """aml_gate's ledger helpers are datagen_seed's: one implementation."""
    from tests.conftest import exec_repo_script

    root = Path(__file__).resolve().parents[1]
    gate = exec_repo_script(root / "scripts/aml_gate.py", "aml_gate_ledger")
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "looks.jsonl"))
    assert gate.ledger_path() == ds.looks_ledger_path() == tmp_path / "looks.jsonl"
    gate.append_ledger({"role": "evaluation", "seed": 5})
    (tmp_path / "looks.jsonl").write_text((tmp_path / "looks.jsonl").read_text() + "garbage\n")
    with pytest.raises(ValueError, match="line 2 is not a look entry"):
        ds.seed_ever_recorded(6)


def _git(repo, *args):
    import subprocess

    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_seed_ever_recorded_reads_the_git_history_without_the_seed_in_argv(tmp_path, monkeypatch):
    """A look recorded on another branch and reverted on this one is found by
    parsing each commit's copy; git never gets the seed as an argument."""
    import subprocess

    repo = tmp_path / "repo"
    rec = repo / "aml_registered_looks.json"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    rec.write_text(json.dumps({"looks": []}))
    _git(repo, "add", rec.name)
    _git(repo, "commit", "-qm", "empty record")
    _git(repo, "checkout", "-qb", "side")
    rec.write_text(json.dumps({"looks": [{"role": "evaluation", "seed": pc.EV}]}))
    _git(repo, "commit", "-qam", "a look")
    _git(repo, "checkout", "-q", "main")
    monkeypatch.setattr(ds, "looks_path", lambda: rec)
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "none.jsonl"))
    pc.use_heldout(monkeypatch)
    argvs = []
    real = subprocess.run

    def spy(argv, *a, **k):
        argvs.append(list(argv))
        return real(argv, *a, **k)

    monkeypatch.setattr(subprocess, "run", spy)
    msg = ds.seed_ever_recorded(pc.EV)
    assert msg and "registered evaluation seed" in msg and "by commit" in msg
    assert ds.seed_ever_recorded(pc.RB) is None
    assert ds.seed_ever_recorded(int(str(pc.EV)[:7])) is None  # no substring match
    assert argvs and not any(str(pc.EV)[:7] in " ".join(a) for a in argvs)
    _no_seed(msg)


def _repo_with_record(tmp_path, monkeypatch, text):
    repo = tmp_path / "repo"
    rec = repo / "aml_registered_looks.json"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    rec.write_text(text)
    _git(repo, "add", rec.name)
    _git(repo, "commit", "-qm", "record")
    monkeypatch.setattr(ds, "looks_path", lambda: rec)
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "none.jsonl"))
    pc.use_heldout(monkeypatch)
    return repo, rec


def test_look_history_finds_amended_away_conflicted_and_text_seeds(tmp_path, monkeypatch):
    repo, rec = _repo_with_record(tmp_path, monkeypatch, json.dumps({"looks": []}))
    # A look committed, then amended away: only the reflog still has it.
    rec.write_text(json.dumps({"looks": [{"role": "evaluation", "seed": pc.EV}]}))
    _git(repo, "commit", "-qam", "look")
    rec.write_text(json.dumps({"looks": []}))
    _git(repo, "commit", "-q", "--amend", "--allow-empty", "-am", "no look")
    assert ds.seed_ever_recorded(pc.EV)
    # A copy left with conflict markers is searched as text; a seed recorded
    # as a string is matched.
    rec.write_text(f'<<<<<<< ours\n{{"looks": [{{"seed": "{pc.RB}"}}]}}\n>>>>>>> theirs\n')
    _git(repo, "commit", "-qam", "conflict")
    assert ds.seed_ever_recorded(pc.RB)


def test_a_shallow_history_refuses(tmp_path, monkeypatch):
    import subprocess

    src, _ = _repo_with_record(tmp_path, monkeypatch, json.dumps({"looks": []}))
    (src / "x").write_text("1")
    _git(src, "add", "x")
    _git(src, "commit", "-qm", "two")
    shallow = tmp_path / "shallow"
    subprocess.run(
        ["git", "clone", "-q", "--depth", "1", f"file://{src}", str(shallow)],
        check=True,
        capture_output=True,
    )
    monkeypatch.setattr(ds, "looks_path", lambda: shallow / "aml_registered_looks.json")
    with pytest.raises(OSError, match="shallow"):
        ds.seed_ever_recorded(pc.EV)
