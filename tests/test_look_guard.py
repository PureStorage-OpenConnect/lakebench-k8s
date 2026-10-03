"""The protected AML corpus guard (SAF-5, LB-205).

Every command that reads or scores data refuses a protected corpus (the
evaluation or robustness one, by role or by a seed that hashes to a held-out
seed) with exit 2 on ``run.protected_corpus`` before any cluster call, and no
refusal prints a seed. TEST VALUES ONLY: the held-out record is the test
fixture (``tests/fixtures/heldout_test.json``), never a pre-registration value.
"""

from __future__ import annotations

import ast
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.aml import look_guard as lg
from lakebench.cli import app
from lakebench.config import datagen_seed as ds
from tests.fixtures import heldout_test_seeds as ts
from tests.fixtures import protected_corpus as pc

ROOT = Path(__file__).resolve().parents[1]
RECORD = ROOT / "tests/fixtures/records/run-20260927-011123-497f02/metrics.json"


@pytest.fixture
def held(monkeypatch):
    return pc.use_heldout(monkeypatch)


@pytest.fixture
def no_cluster(monkeypatch):
    """Every way a command reaches a cluster, S3 or a child process records
    the call and raises."""
    import subprocess

    import boto3
    import kubernetes.client
    import kubernetes.config

    from lakebench.k8s.client import K8sClient
    from lakebench.s3 import S3Client

    fired: list[str] = []

    def stop(name):
        def call(*a, **k):
            fired.append(name)
            raise AssertionError(f"cluster call: {name}")

        return call

    monkeypatch.setattr(kubernetes.config, "load_kube_config", stop("load_kube_config"))
    monkeypatch.setattr(kubernetes.config, "load_incluster_config", stop("load_incluster_config"))
    monkeypatch.setattr(kubernetes.client.ApiClient, "__init__", stop("ApiClient"))
    monkeypatch.setattr(K8sClient, "__init__", stop("K8sClient"))
    monkeypatch.setattr(S3Client, "__init__", stop("S3Client"))
    monkeypatch.setattr(subprocess, "run", stop("subprocess.run"))
    monkeypatch.setattr(subprocess, "Popen", stop("subprocess.Popen"))
    monkeypatch.setattr(boto3, "client", stop("boto3.client"))
    return fired


def _invoke(argv):
    return CliRunner().invoke(app, argv)


def _assert_refused(result, fired, verb):
    out = result.output
    assert result.exit_code == 2, out
    assert "never runs on a protected AML corpus" in out or "never reads a run" in out, out
    assert f"`{verb}`" in out, out
    assert fired == []
    assert pc.seed_tokens(out) == [], "a refusal printed a held-out seed"


# -- per verb: exit 2, zero cluster calls, no seed printed -------------------

PROTECTED = [
    pytest.param({"seed": pc.EV, "role": "evaluation"}, id="evaluation-role"),
    pytest.param({"seed": pc.RB, "role": "robustness", "perturbation": True}, id="robustness-role"),
]


def _verb_argv(verb: str, cfg: Path) -> list[str]:
    return {
        "run": ["run", str(cfg), "--yes"],
        "benchmark": ["benchmark", str(cfg)],
        "query": ["query", str(cfg), "--sql", "SELECT 1"],
        "financial score": [
            "financial",
            "score",
            str(cfg),
            "--manifest",
            "s3a://b/m.parquet",
            "--output",
            "s3a://g/o.parquet",
        ],
        "financial replay": ["financial", "replay", str(cfg), "--rule", "W2_structuring"],
        "financial reproduce": ["financial", "reproduce", str(cfg), "--alert-id", "a-1"],
        "financial reference-score": [
            "financial",
            "reference-score",
            str(cfg),
            "--manifest",
            "s3a://b/m.parquet",
            "--output-prefix",
            "s3a://g/out",
        ],
    }[verb]


VERBS = [
    "run",
    "benchmark",
    "query",
    "financial score",
    "financial replay",
    "financial reproduce",
    "financial reference-score",
]


@pytest.mark.parametrize("verb", VERBS)
@pytest.mark.parametrize("kw", PROTECTED)
def test_verb_refuses_a_protected_config(tmp_path, monkeypatch, held, no_cluster, verb, kw):
    monkeypatch.chdir(tmp_path)
    cfg = pc.financial_config(tmp_path / "c.yaml", **kw)
    _assert_refused(_invoke(_verb_argv(verb, cfg)), no_cluster, verb)


def test_reproduce_refuses_a_config_naming_a_protected_corpus(
    tmp_path, monkeypatch, held, no_cluster
):
    """An ordinary package with --config naming the evaluation corpus: exit 2
    before the run (the package-level held-out refusal stays exit 3)."""
    import lakebench.cli._reproduce as rep

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(rep, "_current_commit_sha", lambda: None)
    pkg = tmp_path / "pkg.yaml"
    pkg.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "reproduction_metadata": {
                    "commit_sha": "unknown",
                    "pipeline_mode": "batch",
                    "corpus_role": "calibration",
                    "expected_numbers": {"scale_ratio": 1.0},
                    "experiment_identity": {"workload": "financial", "seed": pc.CALIBRATION},
                },
            }
        )
    )
    cfg = pc.financial_config(tmp_path / "c.yaml", seed=pc.EV, role="evaluation")
    _assert_refused(_invoke(["reproduce", str(pkg), "--config", str(cfg)]), no_cluster, "reproduce")


def test_reproduce_pipeline_refuses_before_it_deploys(tmp_path, monkeypatch, held, no_cluster):
    from lakebench.cli._reproduce import _run_pipeline
    from lakebench.exit_codes import UsageError

    cfg = pc.financial_config(tmp_path / "c.yaml", seed=pc.EV, role="evaluation")
    with pytest.raises(UsageError) as info:
        _run_pipeline(cfg, 600, keep=True)
    assert info.value.path == lg.PATH and no_cluster == []


def _write_record(runs: Path, run_id: str, *, role=None, seed=None) -> None:
    rec = json.loads(RECORD.read_text())
    rec["run_id"] = run_id
    corpus = rec["experiment"]["corpus"]
    corpus["corpus_role"] = role
    if seed is not None:
        corpus["seed"] = seed
    d = runs / f"run-{run_id}"
    d.mkdir(parents=True)
    (d / "metrics.json").write_text(json.dumps(rec))


@pytest.mark.parametrize("kw", [{"role": "evaluation"}, {"seed": pc.RB}], ids=["role", "seed-hash"])
def test_compare_refuses_a_protected_record(tmp_path, monkeypatch, held, no_cluster, kw):
    monkeypatch.chdir(tmp_path)
    runs = tmp_path / "runs"
    _write_record(runs, "20261002-000001-aaaaaa", **kw)
    _write_record(runs, "20261002-000002-bbbbbb")
    result = _invoke(
        [
            "compare",
            "20261002-000001-aaaaaa",
            "20261002-000002-bbbbbb",
            "--runs-dir",
            str(runs),
        ]
    )
    _assert_refused(result, no_cluster, "compare")
    assert "20261002-000001-aaaaaa" in result.output


def test_compare_still_reads_ordinary_records(tmp_path, monkeypatch, held, no_cluster):
    monkeypatch.chdir(tmp_path)
    runs = tmp_path / "runs"
    _write_record(runs, "20261002-000001-aaaaaa")
    _write_record(runs, "20261002-000002-bbbbbb")
    result = _invoke(
        ["compare", "20261002-000001-aaaaaa", "20261002-000002-bbbbbb", "--runs-dir", str(runs)]
    )
    assert "protected AML corpus" not in result.output
    assert result.exit_code in (0, 10, 11, 12, 13), result.output


def test_compare_reads_records_when_the_held_out_record_is_unreadable(
    tmp_path, monkeypatch, no_cluster
):
    """compare spends nothing and hides every integer seed then: an ordinary
    AML record is not refused because the hash file or floor cannot be read."""
    pc.use_heldout(monkeypatch)

    def gone():
        raise ValueError("the compiled held-out floor (_HELDOUT_FLOOR) is not initialised")

    monkeypatch.setattr(ds, "_heldout", gone)
    monkeypatch.chdir(tmp_path)
    runs = tmp_path / "runs"
    _write_record(runs, "20261002-000001-aaaaaa")
    _write_record(runs, "20261002-000002-bbbbbb")
    result = _invoke(
        ["compare", "20261002-000001-aaaaaa", "20261002-000002-bbbbbb", "--runs-dir", str(runs)]
    )
    assert "protected AML corpus" not in result.output
    assert result.exit_code in (0, 10, 11, 12, 13), result.output
    # A record shown to be protected is still refused.
    _write_record(runs, "20261002-000003-cccccc", role="evaluation")
    result = _invoke(
        ["compare", "20261002-000003-cccccc", "20261002-000002-bbbbbb", "--runs-dir", str(runs)]
    )
    assert result.exit_code == 2 and no_cluster == []


# -- refused at load ---------------------------------------------------------


@pytest.mark.parametrize("role", [None, "calibration"], ids=["no-role", "calibration-role"])
@pytest.mark.parametrize("seed", [pc.EV, pc.RB], ids=["evaluation-seed", "robustness-seed"])
def test_development_config_with_a_held_out_seed_refused_at_load(
    tmp_path, monkeypatch, held, no_cluster, role, seed
):
    from lakebench.cli._exit import error_for
    from lakebench.config import ConfigProtectedCorpusError, LoadPurpose, load_config

    cfg = pc.financial_config(tmp_path / "c.yaml", seed=seed, role=role)
    with pytest.raises(ConfigProtectedCorpusError) as info:
        load_config(cfg, purpose=LoadPurpose.RUN)
    assert error_for(info.value).path == lg.PATH
    assert pc.seed_tokens(str(info.value)) == []
    result = _invoke(["run", str(cfg), "--yes"])
    assert result.exit_code == 2 and no_cluster == []
    assert pc.seed_tokens(result.output) == []


def test_spent_seed_refused_at_load_and_never_printed(tmp_path, monkeypatch, held):
    from lakebench.config import ConfigProtectedCorpusError, LoadPurpose, load_config

    cfg = pc.financial_config(tmp_path / "c.yaml", seed=pc.SPENT)
    with pytest.raises(ConfigProtectedCorpusError) as info:
        load_config(cfg, purpose=LoadPurpose.RUN)
    assert str(pc.SPENT) not in str(info.value)


def test_teardown_and_read_load_a_spent_registered_config(tmp_path, monkeypatch):
    """After its look the registered seed is spent: destroy, status and
    config show must still load the config (they generate and score nothing)."""
    from lakebench.config import ConfigProtectedCorpusError, LoadPurpose, load_config

    pc.use_heldout(monkeypatch, looks=[{"role": "evaluation", "seed": pc.EV}])
    cfg = pc.financial_config(tmp_path / "c.yaml", seed=pc.EV, role="evaluation")
    for purpose in (LoadPurpose.TEARDOWN, LoadPurpose.READ, LoadPurpose.INSPECT):
        assert load_config(cfg, purpose=purpose).architecture.workload.datagen.seed == pc.EV
    for purpose in (LoadPurpose.RUN, LoadPurpose.MUTATE):
        with pytest.raises(ConfigProtectedCorpusError):
            load_config(cfg, purpose=purpose)


def test_unreadable_record_is_a_config_error_not_a_crash(tmp_path, monkeypatch):
    from lakebench.config import ConfigValidationError, LoadPurpose, load_config

    def gone():
        raise FileNotFoundError(2, "No such file", "heldout_hashes.json")

    monkeypatch.setattr(ds, "_heldout", gone)
    cfg = pc.financial_config(tmp_path / "c.yaml", seed=1234567)
    with pytest.raises(ConfigValidationError, match="cannot read its records"):
        load_config(cfg, purpose=LoadPurpose.RUN)


# -- the reason functions ----------------------------------------------------


def _cfg(seed=None, role=None, schema="financial"):
    dg = SimpleNamespace(corpus_role=role, seed=seed)
    wl = SimpleNamespace(datagen=dg, schema_type=SimpleNamespace(value=schema))
    return SimpleNamespace(architecture=SimpleNamespace(workload=wl))


def test_protected_corpus_reason(held):
    assert lg.protected_corpus_reason(_cfg(role="evaluation")) == "corpus_role evaluation"
    assert "robustness" in lg.protected_corpus_reason(_cfg(seed=pc.RB))
    assert "evaluation" in lg.protected_corpus_reason(_cfg(seed=pc.EV, role="calibration"))
    assert lg.protected_corpus_reason(_cfg(seed=pc.CALIBRATION)) is None
    assert lg.protected_corpus_reason(_cfg()) is None
    assert lg.protected_corpus_reason(_cfg(seed=42, schema="customer360")) is None
    assert lg.protected_corpus_reason(_cfg(seed=pc.EV, schema="customer360"))
    for v in (lg.protected_corpus_reason(_cfg(seed=s)) for s in (pc.EV, pc.RB)):
        assert pc.seed_tokens(v) == []


def test_protected_corpus_reason_fails_closed_for_aml(monkeypatch):
    def gone():
        raise ValueError("the compiled held-out floor is not initialised")

    monkeypatch.setattr(ds, "_heldout", gone)
    assert "cannot be read" in lg.protected_corpus_reason(_cfg(seed=pc.CALIBRATION))
    # Customer 360 corpora are not AML data: no refusal when the record is gone.
    assert lg.protected_corpus_reason(_cfg(seed=42, schema="customer360")) is None


def _rec(**corpus):
    return {"run_id": "r", "experiment": {"corpus": {"schema": "financial", **corpus}}}


def test_protected_record_reason(held):
    ref = ds.seed_hash(held.salt, pc.EV)
    assert lg.protected_record_reason(_rec(corpus_role="robustness", seed=1))
    assert "evaluation" in lg.protected_record_reason(_rec(seed=pc.EV))
    assert "evaluation" in lg.protected_record_reason(_rec(seed=str(pc.EV)))
    assert "evaluation" in lg.protected_record_reason(_rec(seed=ref))
    assert "evaluation" in lg.protected_record_reason(_rec(seed={"seed_ref": ref, "role": None}))
    assert lg.protected_record_reason(_rec(seed={"seed_ref": "x", "role": "evaluation"}))
    assert "withheld" in lg.protected_record_reason(_rec(seed={"seed_ref": None}))
    for form in ({}, {"role": "calibration"}, {"role": None}):
        assert "withheld" in lg.protected_record_reason(_rec(seed=form))
    assert lg.recorded_seed_role("9e999999999") is None  # no enormous int is built
    assert lg.protected_record_reason(_rec(seed=pc.CALIBRATION)) is None
    # A spent seed has been looked at or voided: nothing left to protect.
    assert lg.protected_record_reason(_rec(seed=pc.SPENT)) is None
    assert lg.protected_record_reason({"experiment": {"corpus": {"seed": 42}}}) is None
    # Unidentified financial records: refused for the release gate and the
    # audit, read (as not established) by compare.
    for rec in ({"financial_scoring": {}}, _rec()):
        assert "unidentified" in lg.protected_record_reason(rec)
        assert lg.protected_record_reason(rec, require_identity=False) is None
    assert lg.protected_record_reason("not a record") == "the record cannot be read"
    for rec in (_rec(seed=pc.EV), _rec(seed=ref), _rec(seed={"seed_ref": ref})):
        assert pc.seed_tokens(lg.protected_record_reason(rec)) == []


@pytest.mark.parametrize(
    "form",
    [
        lambda h: f"+{pc.EV}",
        lambda h: f" {pc.EV} ",
        lambda h: f"{pc.EV}.0",
        lambda h: [pc.CALIBRATION, pc.EV],
        lambda h: ds.seed_hash(h.salt, pc.EV).upper(),
        lambda h: {"seed_ref": f"+{pc.EV}"},
        lambda h: {"seed_ref": [ds.seed_hash(h.salt, pc.EV)]},
    ],
    ids=["plus", "spaces", "dot-zero", "list", "upper-hex", "ref-plus", "ref-list"],
)
def test_every_recorded_seed_form_is_read(held, form):
    assert lg.protected_record_reason(_rec(seed=form(held)))


def test_ledger_bucket_of_a_registered_corpus_is_refused(tmp_path, monkeypatch, held, no_cluster):
    """A development config pointed at the bronze prefix a registered corpus
    was generated into (this host's corpus ledger) is refused before any
    cluster call: it would read the registered corpus."""
    ledger = tmp_path / "corpora.jsonl"
    monkeypatch.setenv("LB_AML_CORPORA_LEDGER", str(ledger))
    monkeypatch.chdir(tmp_path)
    dev = pc.financial_config(tmp_path / "dev.yaml", seed=pc.CALIBRATION)
    from lakebench.config import LoadPurpose, load_config
    from lakebench.deploy.datagen import bronze_datagen_prefix

    cfg = load_config(dev, purpose=LoadPurpose.RUN)
    uri = f"s3://{cfg.platform.storage.s3.buckets.bronze}/{bronze_datagen_prefix(cfg)}/"
    assert lg.protected_corpus_reason(cfg) is None
    ds.append_corpus_ledger(
        {
            "kind": "registered_corpus",
            "state": "generated",
            "role": "evaluation",
            "bronze_uri": uri,
            "attempt": "a1",
        }
    )
    assert "holds a registered evaluation corpus" in lg.protected_corpus_reason(cfg)
    _assert_refused(_invoke(["run", str(dev), "--yes"]), no_cluster, "run")
    # A config that sets no seed is checked against the ledger too.
    unset = pc.financial_config(tmp_path / "unset.yaml")
    unset_cfg = load_config(unset, purpose=LoadPurpose.RUN)
    assert unset_cfg.architecture.workload.datagen.seed is None
    assert "holds a registered evaluation corpus" in lg.protected_corpus_reason(unset_cfg)
    ledger.write_text("not json\n" + ledger.read_text())
    assert "line 1 of the corpus ledger" in lg.protected_corpus_reason(cfg)
    c360 = tmp_path / "c360.yaml"
    c360.write_text("name: lbtest-c360\n")
    assert lg.protected_corpus_reason(load_config(c360, purpose=LoadPurpose.RUN)) is None


def test_protected_record_reason_fails_closed_for_aml_only(monkeypatch):
    def gone():
        raise ValueError("unreadable")

    monkeypatch.setattr(ds, "_heldout", gone)
    assert "cannot be read" in lg.protected_record_reason(_rec(seed=pc.CALIBRATION))
    c360 = {"experiment": {"corpus": {"schema": "customer360", "seed": 42}}}
    assert lg.protected_record_reason(c360) is None
    # compare's setting: refuse only what is shown to be protected.
    assert lg.protected_record_reason(_rec(seed=pc.CALIBRATION), fail_closed=False) is None


# -- the manifest check: every row (S2, L3) ----------------------------------


def _mixed_manifest():
    """1,000 development rows, then 5 rows from the (test) evaluation seed
    after row 200."""
    return ts.manifest_rows(pc.CALIBRATION, 1000) + ts.manifest_rows(pc.EV, 5, start=1000)


def test_manifest_check_reads_every_row(held):
    rows = _mixed_manifest()
    reason = ds.manifest_protected_reason(iter(rows), heldout=held, spent=[42])
    assert reason == "the corpus manifest comes from the registered evaluation seed"
    assert pc.seed_tokens(reason) == []
    assert lg.manifest_protected_reason is ds.manifest_protected_reason


def test_a_200_row_sample_would_pass_the_mixed_manifest(held):
    """L3: the reference scorer's old 200-row sample misses the held-out rows."""
    rows = _mixed_manifest()
    assert ds.manifest_protected_reason(iter(rows[:200]), heldout=held, spent=[42]) is None


def test_instance_seed_hash_lookup_would_pass_the_mixed_manifest(held):
    """S2: hashing instance seeds (the d1 design) matches no corpus hash, so it
    would pass a held-out corpus; recovery of the corpus seed does not."""
    rows = _mixed_manifest()
    assert not any(ds.heldout_role(s, held) for _, s in rows)
    assert ds.manifest_protected_reason(iter(rows), heldout=held, spent=[42])


def test_manifest_check_passes_calibration_and_refuses_spent_and_unrecoverable(held):
    assert (
        ds.manifest_protected_reason(
            iter(ts.manifest_rows(pc.CALIBRATION, 50)), heldout=held, spent=[42]
        )
        is None
    )
    spent = ds.manifest_protected_reason(
        iter(ts.manifest_rows(pc.SPENT, 5)), heldout=held, spent=[42]
    )
    assert spent == "the corpus manifest comes from a spent seed"
    assert "cannot be recovered" in ds.manifest_protected_reason(
        iter([("bogus", 1)]), heldout=held, spent=[]
    )


def test_manifest_check_fails_closed_without_the_record(monkeypatch):
    def gone():
        raise FileNotFoundError("heldout_hashes.json")

    monkeypatch.setattr(ds, "_heldout", gone)
    reason = ds.manifest_protected_reason(iter(ts.manifest_rows(pc.CALIBRATION, 5)), spent=[])
    assert reason is not None and "cannot be read" in reason


# -- redaction -------------------------------------------------------------


def test_seed_is_protected_hides_on_an_unreadable_record(monkeypatch, held):
    assert ds.seed_is_protected(pc.EV) and ds.seed_is_protected(42)
    assert not ds.seed_is_protected(pc.CALIBRATION)
    assert not ds.seed_is_protected("not a seed") and not ds.seed_is_protected(True)

    def gone():
        raise ValueError("unreadable")

    monkeypatch.setattr(ds, "_heldout", gone)
    assert ds.seed_is_protected(pc.CALIBRATION)


def test_compare_redaction_hides_every_integer_form_when_unreadable(monkeypatch):
    from lakebench.metrics import compare as cm

    def gone():
        raise ValueError("unreadable")

    monkeypatch.setattr(ds, "_heldout", gone)
    hidden = cm._hidden_seeds()
    assert hidden is None
    for v in (pc.EV, f"+{pc.EV}", f"{pc.EV}.0", f"{pc.EV}.", "1.23456e5", float(43), " 7 "):
        assert cm._seed_out(v, hidden) == "<protected seed>", v
    assert cm._seed_out("1_234", hidden) == "<protected seed>"
    assert cm._seed_out({"seed_ref": str(pc.EV)}, hidden) == {"seed_ref": "<protected seed>"}
    assert cm._seed_out({"seed": pc.EV}, hidden) == {"seed": "<protected seed>"}
    doc = cm.redact({"note": f"corpus seed differs ({pc.EV}.0 vs +{pc.RB}, 1_234_567)"}, hidden)
    assert "1_234_567" not in doc["note"]
    assert pc.seed_tokens(json.dumps(doc)) == []


def test_compare_redaction_hides_held_out_seeds(held):
    from lakebench.metrics import compare as cm

    hidden = cm._hidden_seeds()
    doc = cm.redact({"seed": pc.EV, "note": f"seed {pc.RB} and seed 43"}, hidden)
    assert doc["seed"] == "<protected seed>"
    assert pc.seed_tokens(json.dumps(doc)) == [] and "seed 43" in doc["note"]
    assert pc.CALIBRATION not in hidden and pc.EV in hidden
    assert pc.EV not in (hidden - {pc.EV})


# -- every command that loads to change data is guarded or allowlisted -------

#: (module, function) pairs that load a config for MUTATE or RUN and may take
#: a protected corpus: they generate and score nothing (deploy creates the
#: deployment the registered corpus is generated in; clean removes data;
#: the validators only load), or guard it themselves (generate).
ALLOWED = {
    ("_deploy.py", "_deploy_impl"),
    ("_clean.py", "clean"),
    ("_config.py", "_validate_local"),
    ("__init__.py", "validate"),
    ("_generate.py", "generate"),
}

_GUARDS = {"refuse_if_protected", "protected_corpus_reason", "_load_config"}


def _loads_to_change(fn: ast.AST) -> bool:
    for node in ast.walk(fn):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "load_config":
            for kw in node.keywords:
                if kw.arg == "purpose" and getattr(kw.value, "attr", None) in ("MUTATE", "RUN"):
                    return True
    return False


def _calls(fn: ast.AST) -> set[str]:
    out = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            f = node.func
            out.add(getattr(f, "id", None) or getattr(f, "attr", None) or "")
    return out


def test_every_data_command_is_guarded():
    unguarded = []
    for path in sorted((ROOT / "src/lakebench/cli").glob("*.py")):
        tree = ast.parse(path.read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if not _loads_to_change(fn) or (path.name, fn.name) in ALLOWED:
                continue
            if fn.name == "_load_config" and path.name == "_financial.py":
                guarded = "refuse_if_protected" in _calls(fn)
            else:
                guarded = bool(_calls(fn) & _GUARDS)
            if not guarded:
                unguarded.append(f"{path.name}:{fn.lineno} {fn.name}")
    assert unguarded == [], unguarded


def test_allowlist_names_real_functions():
    names = set()
    for path in (ROOT / "src/lakebench/cli").glob("*.py"):
        for fn in ast.walk(ast.parse(path.read_text())):
            if isinstance(fn, ast.FunctionDef):
                names.add((path.name, fn.name))
    assert ALLOWED <= names, ALLOWED - names


def test_flat_driver_copy_finds_its_records(tmp_path):
    """On the Spark driver datagen_seed.py and the AML JSON files sit flat in
    one directory, with no lakebench package: the paths still resolve."""
    import subprocess
    import sys

    flat = tmp_path / "scripts"
    flat.mkdir()
    shutil.copy(ROOT / "src/lakebench/config/datagen_seed.py", flat)
    data = ROOT / "src/lakebench/spark/data/aml"
    for name in ("aml_preregistration.json", "aml_registered_looks.json", "heldout_hashes.json"):
        shutil.copy(data / name, flat)
    probe = (
        "import sys\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "import datagen_seed as d\n"
        "assert d.heldout_path().parent == d.Path(sys.argv[1]).resolve()\n"
        "assert d.looks_path().parent == d.Path(sys.argv[1]).resolve()\n"
        "assert isinstance(d.prereg_spent_seeds(), frozenset)\n"
        "print('ok')\n"
    )
    out = subprocess.run(
        [sys.executable, "-I", "-c", probe, str(flat)],
        cwd=flat,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert out.returncode == 0 and out.stdout.strip() == "ok", out.stderr[-400:]


def test_fixture_values_are_not_production(held):
    """The guard tests use the fixture record only."""
    assert (
        held.salt
        != json.loads((ROOT / "src/lakebench/spark/data/aml/heldout_hashes.json").read_text())[
            "salt"
        ]
    )


def test_scorer_checks_the_manifest_before_it_scores():
    """score_financial.main refuses a protected corpus before compute_scores."""
    tree = ast.parse((ROOT / "src/lakebench/spark/scripts/score_financial.py").read_text())
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    order = [
        node.func.id
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    assert "refuse_protected_corpus" in order and "compute_scores" in order
    lines = {
        node.func.id: node.lineno
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert lines["refuse_protected_corpus"] < lines["compute_scores"]


# -- owner, 10-03: bronze-verify refuses a protected corpus --------------------


def test_bronze_verify_marker_is_the_cli_marker():
    src = (ROOT / "src/lakebench/spark/scripts/bronze_verify_financial.py").read_text()
    assert f'PROTECTED_REFUSAL = "{lg.REFUSAL_MARKER}"' in src


@pytest.mark.parametrize(
    ("mode", "required_env", "required"),
    [("1", None, True), ("0", None, True), ("schema", None, False), ("schema", "1", True)],
)
def test_manifest_is_required_except_while_continuous_datagen_writes(
    load_script, monkeypatch, mode, required_env, required
):
    monkeypatch.setenv("LB_REGISTER_TABLE", mode)
    if required_env is None:
        monkeypatch.delenv("LB_MANIFEST_REQUIRED", raising=False)
    else:
        monkeypatch.setenv("LB_MANIFEST_REQUIRED", required_env)
    bvf = load_script("bronze_verify_financial")
    assert bvf.MANIFEST_REQUIRED is required


def test_refusal_in_log_finds_the_line():
    log = f"INFO x\nERROR: {lg.REFUSAL_MARKER}: the corpus manifest comes from a spent seed\nbye"
    assert lg.refusal_in_log(log).startswith(lg.REFUSAL_MARKER)
    assert lg.refusal_in_log("ordinary failure") is None and lg.refusal_in_log(None) is None


def test_cli_maps_a_bronze_verify_refusal_to_exit_2():
    """run's stage-failure branch and the continuous preflight read the
    marker before the generic failure; --skip-generate requires a manifest."""
    run_src = (ROOT / "src/lakebench/cli/_run.py").read_text()
    sus_src = (ROOT / "src/lakebench/cli/_sustained.py").read_text()
    for src in (run_src, sus_src):
        i = src.index("refusal_in_log(")
        assert "ExitCode.USAGE" in src[i : i + 600]
    assert '"LB_MANIFEST_REQUIRED": "1" if skip_generate else "0"' in sus_src


class _Job:
    def __init__(self):
        self.env = None

    def submit_job(self, job_type, cycle_env=None, **kw):
        from lakebench.spark.job import JobState

        self.env = cycle_env
        return SimpleNamespace(state=JobState.SUBMITTED, message="")


@pytest.mark.parametrize(
    ("success", "logs", "code"),
    [
        (True, "", None),
        (False, f"x\nERROR: {lg.REFUSAL_MARKER}: the corpus manifest comes from a spent seed", 2),
        (False, "LAKEBENCH-PROTECTED-CORPUS-UNCHECKED: the manifest could not be checked", 1),
    ],
    ids=["passes", "refused", "unchecked"],
)
def test_held_out_check_before_a_stage_subset(success, logs, code):
    import typer

    from lakebench.cli._run import _held_out_check_only

    job = _Job()
    monitor = SimpleNamespace(
        wait_for_completion=lambda *a, **k: SimpleNamespace(
            success=success, message="driver failed", driver_logs=logs
        )
    )
    if code is None:
        _held_out_check_only(job, monitor, "r1", None, 600)
    else:
        with pytest.raises(typer.Exit) as info:
            _held_out_check_only(job, monitor, "r1", None, 600)
        assert info.value.exit_code == code
    assert job.env == {"LB_REGISTER_TABLE": "check", "LB_RUN_ID": "r1"}


def test_a_financial_stage_subset_runs_the_check_before_its_stages():
    src = (ROOT / "src/lakebench/cli/_run.py").read_text()
    body = src[src.index("def _run_once(") :]
    check = body.index("_held_out_check_only(job_manager")
    assert body.index("stages = all_stages") < check < body.index("for cycle_idx in range")
    guard = body[check - 400 : check]
    assert "stages[0][0] != JobType.BRONZE_VERIFY" in guard
    # A multi-cycle run clears the prefix and generates each cycle's corpus.
    assert "total_cycles == 1" in guard
