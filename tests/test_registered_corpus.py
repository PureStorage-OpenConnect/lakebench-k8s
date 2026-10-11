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

    calls = []

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


def _broken_look_history(seed):
    raise OSError("git log over aml_registered_looks.json failed (exit 128)")


@pytest.mark.parametrize(
    ("seed", "role", "image", "flags", "look_history"),
    [
        ("EV", "evaluation", None, ("--yes",), None),  # protected config without the flag
        (
            "CALIBRATION",
            None,
            None,
            ("--registered-corpus", "--yes"),
            None,
        ),  # names no protected corpus
        ("EV", "evaluation", "IMAGE", ("--registered-corpus",), None),  # needs --yes
        (
            "EV",
            "evaluation",
            "IMAGE",
            ("--registered-corpus", "--yes", "--allow-stale-bronze"),
            None,
        ),
        ("EV", "evaluation", "IMAGE", ("--registered-corpus", "--yes"), "seen"),  # look recorded
        ("EV", "evaluation", "IMAGE", ("--registered-corpus", "--yes"), "unreadable"),
        (
            "EV",
            "evaluation",
            "docker.io/sillidata/lb-datagen:1.6.0",
            ("--registered-corpus", "--yes"),
            None,
        ),  # tag, not digest
    ],
)
def test_registered_generate_refusals_touch_nothing(
    env, cluster, monkeypatch, seed, role, image, flags, look_history
):
    """Each refusal exits 2 with zero cluster calls, no ledger written and no
    seed in the output."""
    if look_history == "seen":
        monkeypatch.setattr(
            ds, "seed_ever_recorded", lambda s: "the registered evaluation seed is in the ledger"
        )
    elif look_history == "unreadable":
        monkeypatch.setattr(ds, "seed_ever_recorded", _broken_look_history)
    kw = {}
    if role:
        kw["role"] = role
    if image:
        kw["image"] = pc.IMAGE if image == "IMAGE" else image
    cfg = pc.financial_config(env.tmp / "c.yaml", seed=getattr(pc, seed), **kw)
    r = _gen(cfg, *flags)
    assert r.exit_code == 2, r.output
    assert cluster == [] and not env.ledger.exists()
    _no_seed(r.output)


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
    assert r.exit_code == 2, r.output
    assert cluster == []


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
    monkeypatch.setattr(
        gen,
        "enforce_bronze_gate",
        lambda *a, **k: SimpleNamespace(stale_allowed=False, record=lambda: None),
    )
    # generate stops an earlier datagen Job before the gate (a cluster call).
    monkeypatch.setattr("lakebench.deploy.datagen.stop_previous_datagen", lambda c: None)
    monkeypatch.setattr("lakebench.metrics.datagen_aggregator.drop_sidecar", lambda ns: None)
    monkeypatch.setattr(deploy_mod, "DeploymentEngine", lambda cfg: SimpleNamespace())

    class Datagen:
        def __init__(self, engine, allow_stale_bronze=False, **kw):
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
    monkeypatch.setattr(
        "lakebench.s3.S3Client", lambda **k: SimpleNamespace(raw_client=FakeBronze())
    )
    return seen


#: The registered corpus's objects under the datagen prefix (test bytes).
CORPUS = {
    "pacs008/bronze/pacs008/part-00000.parquet": b"p" * 100,
    "pacs008/bronze/party.parquet": b"q" * 10,
    "pacs008/manifest/manifest.parquet": b"manifest-bytes",
    "pacs008/_corpus/c000/n000.json": b"{}",
}


class FakeBronze:
    def get_paginator(self, name):
        assert name == "list_objects_v2"

        class _Pages:
            def paginate(self, Bucket, Prefix, **kw):  # noqa: N803 -- boto3's names
                keys = sorted(k for k in CORPUS if k.startswith(Prefix))
                yield {"Contents": [{"Key": k, "Size": len(CORPUS[k])} for k in keys]}

        return _Pages()

    def get_object(self, Bucket, Key):  # noqa: N803
        import io

        return {"Body": io.BytesIO(CORPUS[Key])}


def _local_copy(root: Path) -> Path:
    for key, body in CORPUS.items():
        path = root / key.split("/", 1)[1]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    return root


def _registered(env):
    return pc.financial_config(env.tmp / "c.yaml", seed=pc.EV, role="evaluation", image=pc.IMAGE)


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


def test_a_torn_look_ledger_line_refuses(tmp_path, monkeypatch):
    from tests.conftest import exec_repo_script

    root = Path(__file__).resolve().parents[1]
    gate = exec_repo_script(root / "scripts/aml_gate.py", "aml_gate_ledger")
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "looks.jsonl"))
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


# -- owner, 10-03: the registered look scores only the generated corpus -------


def test_generated_entry_carries_the_corpus_fingerprint(env, monkeypatch):
    _fake_generate(monkeypatch, env)
    assert _gen(_registered(env), "--registered-corpus", "--yes").exit_code == 0
    gen = _lines(env.ledger)[-1]
    assert gen["state"] == "generated"
    assert gen["corpus_fingerprint"] == ds.local_corpus_fingerprint(_local_copy(env.tmp / "copy"))
    assert gen["corpus_fingerprint"]["files"] == 3  # the marker is not data


def _gate_problem(env, corpus, image=pc.IMAGE, role="evaluation", seed=pc.EV):
    return ds.registered_corpus_problem(role, seed, image, corpus)


def test_the_look_needs_a_matching_generated_entry(env, monkeypatch):
    corpus = _local_copy(env.tmp / "copy")
    assert "no generation" in _gate_problem(env, corpus)
    _fake_generate(monkeypatch, env)
    assert _gen(_registered(env), "--registered-corpus", "--yes").exit_code == 0
    assert _gate_problem(env, corpus) is None
    # Another seed, role or image digest: refused.
    assert _gate_problem(env, corpus, seed=pc.RB) is not None
    assert _gate_problem(env, corpus, role="robustness") is not None
    assert "digest" in _gate_problem(env, corpus, image="repo@sha256:" + "b" * 64)
    assert "digest" in _gate_problem(env, corpus, image="repo:latest")
    # A local copy that differs (a host-built corpus, a partial sync): refused.
    (corpus / "bronze" / "pacs008" / "part-00001.parquet").write_bytes(b"x")
    assert "not the one" in _gate_problem(env, corpus)
    _no_seed(env.ledger.read_text())


def _base(env, **kw):
    return {
        "kind": "registered_corpus",
        "role": "evaluation",
        "seed_hash": ds.seed_hash(env.held.salt, pc.EV),
        "bronze_uri": "s3://b/pacs008/",
        "image": pc.IMAGE,
        **kw,
    }


def _gen_ok(env, fp, attempt="a1"):
    return _base(
        env, attempt=attempt, state="generated", corpus_fingerprint=fp, image_ids=[pc.IMAGE]
    )


_OTHER_IMAGE = "docker.io/example/lb-datagen@sha256:" + "d" * 64
_PLATFORM_IMAGE = "docker.io/example/lb-datagen@sha256:" + "e" * 64


def _opened(attempt, submitted=True):
    rows = [(attempt, "attempted", {})]
    return rows + ([(attempt, "submitting", {})] if submitted else [])


_LEDGER_SEQUENCES = {
    "mixed-fleet": (
        lambda fp: [
            ("a1", "attempted", {}),
            ("a1", "generated", {"image_ids": [pc.IMAGE, "repo@sha256:" + "c" * 64]}),
        ],
        "one image digest",
    ),
    "unobserved-images": (
        lambda fp: [
            ("a2", "attempted", {}),
            ("a2", "generated", {"image_ids": "not_observed"}),
        ],
        "digest",
    ),
    "concurrent-submit": (
        lambda fp: [
            ("a1", "attempted", {}),
            ("a1", "submitting", {}),
            ("a2", "submitting", {}),
            "GEN_OK",
        ],
        "same bronze prefix",
    ),
    "earlier-unfinished-submit": (
        lambda fp: [*_opened("a0"), *_opened("a1"), "GEN_OK"],
        "same bronze prefix",
    ),
    "earlier-failure-after-submit": (
        lambda fp: [
            *_opened("a0"),
            ("a0", "failed", {"submitted": True}),
            *_opened("a1"),
            "GEN_OK",
        ],
        "same bronze prefix",
    ),
    "earlier-generated-attempt": (
        lambda fp: [
            *_opened("a0"),
            ("a0", "generated", {"corpus_fingerprint": None}),
            *_opened("a1"),
            "GEN_OK",
        ],
        None,
    ),
    "generation-without-attempted-line": (lambda fp: ["GEN_OK"], "no attempted entry"),
    "pinned-to-another-image": (
        lambda fp: [
            ("a1", "attempted", {}),
            ("a1", "generated", {"image": _OTHER_IMAGE, "image_ids": [_OTHER_IMAGE]}),
        ],
        "pinned",
    ),
    "uniform-platform-digest": (
        lambda fp: [
            ("a1", "attempted", {}),
            ("a1", "generated", {"image_ids": [_PLATFORM_IMAGE, _PLATFORM_IMAGE]}),
        ],
        None,
    ),
    "unfingerprinted-generation": (
        lambda fp: [
            ("a1", "attempted", {}),
            (
                "a1",
                "generated",
                {"corpus_fingerprint": None, "fingerprint_error": "ClientError"},
            ),
        ],
        "no corpus fingerprint (ClientError)",
    ),
}


@pytest.mark.parametrize("build,expected", _LEDGER_SEQUENCES.values(), ids=list(_LEDGER_SEQUENCES))
def test_gate_ledger_sequences(env, build, expected):
    corpus = _local_copy(env.tmp / "copy")
    fp = ds.local_corpus_fingerprint(corpus)
    for row in build(fp):
        if row == "GEN_OK":
            ds.append_corpus_ledger(_gen_ok(env, fp))
            continue
        attempt, state, extra = row
        fields = {"corpus_fingerprint": fp, "image_ids": [pc.IMAGE]} if state == "generated" else {}
        ds.append_corpus_ledger(_base(env, attempt=attempt, state=state, **{**fields, **extra}))
    problem = _gate_problem(env, corpus)
    if expected is None:
        assert problem is None
    else:
        assert problem is not None and expected in problem


def test_a_half_synced_file_is_part_of_the_fingerprint(env):
    corpus = _local_copy(env.tmp / "copy")
    before = ds.local_corpus_fingerprint(corpus)
    (corpus / "bronze" / "pacs008" / "part-00000.parquet.a1b2").write_bytes(b"p" * 100)
    (corpus / "_corpus").mkdir(exist_ok=True)
    (corpus / "_corpus" / "marker.json").write_bytes(b"{}")
    after = ds.local_corpus_fingerprint(corpus)
    assert after["files"] == before["files"] + 1 and after != before


def test_a_failed_fingerprint_fails_generate(env, monkeypatch):
    _fake_generate(monkeypatch, env)

    class Broken:
        def get_paginator(self, name):
            raise RuntimeError("503")

    monkeypatch.setattr("lakebench.s3.S3Client", lambda **k: SimpleNamespace(raw_client=Broken()))
    r = _gen(_registered(env), "--registered-corpus", "--yes")
    assert r.exit_code == 1 and "could not be fingerprinted" in r.output, r.output
    gen = _lines(env.ledger)[-1]
    assert gen["state"] == "generated" and gen["corpus_fingerprint"] is None


def test_a_torn_ledger_line_refuses(env):
    env.ledger.write_text("garbage\n")
    with pytest.raises(ValueError, match="line 1"):
        _gate_problem(env, _local_copy(env.tmp / "copy"))


def test_aml_gate_registered_preflight_requires_the_entry(env, monkeypatch, capsys):
    """scripts/aml_gate.py --registered refuses before Spark when no
    generation of the registered corpus matches (one small hunk there)."""
    from tests.conftest import exec_repo_script

    root = Path(__file__).resolve().parents[1]
    gate = exec_repo_script(root / "scripts/aml_gate.py", "aml_gate_registered")
    monkeypatch.setattr(gate, "clean_checkout_error", lambda: None)
    monkeypatch.setattr(gate, "seed_ever_recorded", lambda seed: None)
    looks = env.tmp / "looks.json"
    looks.write_text('{"looks": []}')
    monkeypatch.setattr(ds, "looks_path", lambda: looks)
    corpus = _local_copy(env.tmp / "copy")
    # A registered look reads a held-out seed only from an owner-only file.
    seed_file = env.tmp / "seed"
    seed_file.write_text(f"{pc.EV}\n")
    seed_file.chmod(0o600)
    rc = gate.main(
        [
            str(corpus),
            "--seed-file",
            str(seed_file),
            "--registered",
            "evaluation",
            "--out",
            str(env.tmp / "gate.json"),
            "--generator-image",
            pc.IMAGE,
        ]
    )
    err = capsys.readouterr().err
    assert rc == 1 and "no generation of the registered evaluation corpus" in err, err
    _no_seed(err)


def test_unread_pod_images_fail_generate(env, monkeypatch):
    _fake_generate(monkeypatch, env)

    def no_pods(**k):
        raise RuntimeError("pods gone")

    monkeypatch.setattr("lakebench.metrics.datagen_aggregator.collect_from_k8s", no_pods)
    r = _gen(_registered(env), "--registered-corpus", "--yes")
    assert r.exit_code == 1 and "image digests could not be read" in r.output, r.output


def test_a_directory_marker_key_is_not_a_file(env, monkeypatch):
    monkeypatch.setitem(CORPUS, "pacs008/bronze/", b"")
    _fake_generate(monkeypatch, env)
    assert _gen(_registered(env), "--registered-corpus", "--yes").exit_code == 0
    gen = _lines(env.ledger)[-1]
    assert gen["corpus_fingerprint"]["files"] == 3
