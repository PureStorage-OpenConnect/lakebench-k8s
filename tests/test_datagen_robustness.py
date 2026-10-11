"""Robustness corpus option (AML-GOALS R3(b), Level 2 condition 5; lane T2).

The multipliers live in the Rust generator as named constants and must equal
the pre-registration's corpora.robustness_perturbation; the option is wired
config -> deployer -> job template -> entrypoint -> generator like the seed,
and is off by default with the argv unchanged. The distribution checks are in
datagen_rs/tests/robustness.rs.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import datagen_seed as ds
from tests.conftest import make_config
from tests.fixtures import heldout_test_seeds as ts

REPO = Path(__file__).resolve().parents[1]
PREREG = json.loads((REPO / "src/lakebench/spark/data/aml/aml_preregistration.json").read_text())
CORPORA = PREREG["corpora"]
# The plaintext held-out keys are dropped so no assertion can print them.
for _k in ("evaluation_seed", "robustness_seed"):
    CORPORA.pop(_k, None)
RUST = (REPO / "datagen_rs/src/robustness.rs").read_text()
# Test-only held-out seeds, registered in tests/fixtures/heldout_test.json.
ROBUST, EVAL = ts.TEST_ROBUSTNESS_SEED, ts.TEST_EVALUATION_SEED


@pytest.fixture(autouse=True)
def _fixture_heldout(monkeypatch):
    ts.use_fixture(monkeypatch)


def _no_plain(corpora: dict) -> dict:
    """``corpora`` without the plaintext held-out keys, so no assertion prints them."""
    return {k: v for k, v in corpora.items() if k not in ("evaluation_seed", "robustness_seed")}


def _rust_const(name: str) -> float:
    m = re.search(rf"pub const {name}: (?:f64|i64) = ([0-9_.]+);", RUST)
    assert m, f"{name} not found in robustness.rs"
    return float(m.group(1).replace("_", ""))


@pytest.mark.parametrize(
    ("const", "key"),
    [
        ("ROBUSTNESS_MEDIAN_AMOUNT_MULTIPLIER", "median_amount_multiplier"),
        ("ROBUSTNESS_PERSONA_SD_MULTIPLIER", "persona_sd_multiplier"),
        ("ROBUSTNESS_DORMANCY_RANGE_MULTIPLIER", "dormancy_range_multiplier"),
    ],
)
def test_rust_multipliers_are_the_preregistered_ones(const, key):
    assert _rust_const(const) == CORPORA["robustness_perturbation"][key]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _cfg(schema="financial", **dg):
    return make_config(architecture={"workload": {"schema": schema, "datagen": dg}})


@pytest.fixture
def looks_open(monkeypatch):
    opened = {**_no_plain(ds._corpora()), "registered_looks_open": True}
    monkeypatch.setattr(ds, "_corpora", lambda: opened)


def test_default_is_off():
    assert _cfg(seed=7777).architecture.workload.datagen.robustness_perturbation is False
    assert ds.config_perturbation(_cfg(seed=7777)) is False


def test_dev_seed_may_be_perturbed():
    assert ds.config_perturbation(_cfg(seed=7777, robustness_perturbation=True)) is True


def test_non_financial_refused():
    with pytest.raises(ValidationError, match="financial schema only"):
        _cfg("customer360", robustness_perturbation=True)


def test_robustness_role_requires_the_perturbation(looks_open):
    with pytest.raises(ValidationError, match="needs datagen.robustness_perturbation"):
        _cfg(seed=ROBUST, corpus_role="robustness")
    cfg = _cfg(seed=ROBUST, corpus_role="robustness", robustness_perturbation=True)
    assert ds.config_seed(cfg) == ROBUST
    assert ds.config_perturbation(cfg) is True


@pytest.mark.parametrize("role", ["calibration", "evaluation"])
def test_other_roles_refuse_the_perturbation(role, looks_open):
    seed = EVAL if role == "evaluation" else CORPORA["calibration_seed"]
    with pytest.raises(ValidationError, match="never perturbed"):
        _cfg(seed=seed, corpus_role=role, robustness_perturbation=True)


def test_refused_even_if_validation_is_bypassed(looks_open):
    cfg = _cfg(seed=ROBUST, corpus_role="robustness", robustness_perturbation=True)
    cfg.architecture.workload.datagen.robustness_perturbation = False
    with pytest.raises(ValueError, match="needs datagen.robustness_perturbation"):
        ds.config_perturbation(cfg)


# ---------------------------------------------------------------------------
# Deployer, template, entrypoint, reference job
# ---------------------------------------------------------------------------


def _job_args(cfg):
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    engine = DeploymentEngine(cfg, dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    job = yaml.safe_load(engine.renderer.render("datagen/job.yaml.j2", ctx))
    return ctx, job["spec"]["template"]["spec"]["containers"][0]["args"]


def test_deployer_renders_the_flag_only_when_on():
    ctx_off, off = _job_args(_cfg(seed=7777))
    ctx_on, on = _job_args(_cfg(seed=7777, robustness_perturbation=True))
    assert ctx_off["datagen_robustness_perturbation"] is False
    assert "--robustness-perturbation" not in off
    assert on.count("--robustness-perturbation") == 1
    # The only difference in argv is the flag itself.
    assert [a for a in on if a != "--robustness-perturbation"] == off


def _entrypoint():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "datagen_entrypoint_t2", REPO / "datagen_rs" / "entrypoint.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _entry_cmd(argv, monkeypatch):
    ep = _entrypoint()
    monkeypatch.setenv("CPU_LIMIT", "8")
    captured: dict = {}

    def _fake_exec(_path, cmd):
        captured["cmd"] = cmd
        raise SystemExit(0)

    with patch.object(sys, "argv", ["entrypoint.py", *argv]):
        with patch.object(ep.os, "execvp", _fake_exec):
            try:
                rc = ep.main()
            except SystemExit as exc:
                rc = None if "cmd" in captured else exc.code
    return rc, captured.get("cmd")


def test_entrypoint_forwards_the_flag_for_financial(monkeypatch):
    base = ["--schema", "financial", "--bucket", "b", "--seed", "7777"]
    _, off = _entry_cmd(base, monkeypatch)
    _, on = _entry_cmd([*base, "--robustness-perturbation"], monkeypatch)
    assert "--robustness-perturbation" not in off
    assert [a for a in on if a != "--robustness-perturbation"] == off
    assert on.count("--robustness-perturbation") == 1


def test_entrypoint_refuses_the_flag_for_c360(monkeypatch):
    rc, cmd = _entry_cmd(
        ["--schema", "customer360", "--bucket", "b", "--robustness-perturbation"], monkeypatch
    )
    assert rc == 2 and cmd is None


def test_reference_job_records_the_perturbation(monkeypatch):
    from lakebench.modules.pipeline_engines.spark import job as jobmod
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    monkeypatch.setattr(jobmod, "_lakebench_git_sha", lambda: "abc")

    def env(cfg):
        return {
            e["name"]: e.get("value")
            for e in SparkJobManager(cfg, MagicMock())._build_env_vars(
                JobType.SCORE_FINANCIAL_REFERENCE
            )
        }

    assert "LB_DATAGEN_ROBUSTNESS_PERTURBATION" not in env(_cfg(seed=7777))
    on = env(_cfg(seed=7777, robustness_perturbation=True))
    assert on["LB_DATAGEN_ROBUSTNESS_PERTURBATION"] == "true"


# ---------------------------------------------------------------------------
# Manifest stamp: the robustness look reads the perturbation from the corpus
# ---------------------------------------------------------------------------

MULT = {
    mkey: [str(CORPORA["robustness_perturbation"][pkey])]
    for pkey, mkey in ds.MANIFEST_MULTIPLIER_KEYS.items()
}


def test_manifest_keys_match_the_generator():
    for name, key in [
        ("MANIFEST_STAMP_KEY", ds.MANIFEST_STAMP_KEY),
        ("MANIFEST_MEDIAN_AMOUNT_KEY", ds.MANIFEST_MULTIPLIER_KEYS["median_amount_multiplier"]),
        ("MANIFEST_PERSONA_SD_KEY", ds.MANIFEST_MULTIPLIER_KEYS["persona_sd_multiplier"]),
        ("MANIFEST_DORMANCY_KEY", ds.MANIFEST_MULTIPLIER_KEYS["dormancy_range_multiplier"]),
    ]:
        m = re.search(rf'pub const {name}: &str = "([^"]+)";', RUST)
        assert m and m.group(1) == key, name
    assert set(ds.MANIFEST_MULTIPLIER_KEYS) == set(CORPORA["robustness_perturbation"]) - {"note"}


def _stamped(n=10, **override):
    vals = {ds.MANIFEST_STAMP_KEY: "true", **{k: v[0] for k, v in MULT.items()}, **override}
    return ds.summarise_stamp([(vals, n)])


def _plain(n=10):
    return ds.summarise_stamp([(dict.fromkeys(ds.MANIFEST_KEYS), n)])


def test_summarise_stamp():
    plain = _plain()
    assert plain["n_stamped"] == 0 and plain["n_instances"] == 10
    s = _stamped()
    assert s["n_stamped"] == 10 and s["multipliers"] == MULT
    # A stamp that is not exactly "true" does not count.
    assert _stamped(**{ds.MANIFEST_STAMP_KEY: "false"})["n_stamped"] == 0


def test_robustness_role_needs_the_stamp():
    assert ds.perturbation_stamp_error(CORPORA, "robustness", _plain())
    assert ds.perturbation_stamp_error(CORPORA, "robustness", _stamped()) is None
    # Empty manifest: not a stamped corpus.
    assert ds.perturbation_stamp_error(CORPORA, "robustness", _plain(0))


def test_robustness_role_needs_the_registered_multipliers():
    bad = _stamped(**{ds.MANIFEST_MULTIPLIER_KEYS["dormancy_range_multiplier"]: "1.1"})
    assert "dormancy_range_multiplier" in ds.perturbation_stamp_error(CORPORA, "robustness", bad)
    junk = _stamped(**{ds.MANIFEST_MULTIPLIER_KEYS["persona_sd_multiplier"]: "x"})
    assert ds.perturbation_stamp_error(CORPORA, "robustness", junk)


@pytest.mark.parametrize("role", ["calibration", "evaluation"])
def test_other_roles_refuse_the_stamp(role):
    assert ds.perturbation_stamp_error(CORPORA, role, _stamped())
    assert ds.perturbation_stamp_error(CORPORA, role, _plain()) is None


def test_mixed_manifest_refused():
    mixed = ds.summarise_stamp(
        [
            ({ds.MANIFEST_STAMP_KEY: "true", **{k: v[0] for k, v in MULT.items()}}, 5),
            (dict.fromkeys(ds.MANIFEST_KEYS), 5),
        ]
    )
    for role in (None, "robustness", "evaluation"):
        assert ds.perturbation_stamp_error(CORPORA, role, mixed)


def test_declared_must_match_the_corpus():
    assert ds.perturbation_stamp_error(CORPORA, None, _plain(), declared=True)
    assert ds.perturbation_stamp_error(CORPORA, None, _stamped(), declared=False)
    assert ds.perturbation_stamp_error(CORPORA, None, _stamped(), declared=True) is None
    assert ds.perturbation_stamp_error(CORPORA, None, _plain(), declared=False) is None
    # Locally nothing is declared: a perturbed dev corpus scores freely.
    assert ds.perturbation_stamp_error(CORPORA, None, _stamped()) is None


@pytest.fixture
def scorer(monkeypatch, load_script):
    """score_financial_reference with pyspark stubbed (not installed here)."""
    for mod in ("pyspark", "pyspark.sql", "pyspark.sql.functions"):
        monkeypatch.setitem(sys.modules, mod, MagicMock())
    ref, fidelity_gate = load_script("score_financial_reference", extra=("fidelity_gate",))

    opened = json.loads(json.dumps(PREREG))
    opened["corpora"]["registered_looks_open"] = True
    monkeypatch.setattr(fidelity_gate, "load_preregistration", lambda *a, **k: (opened, "x"))
    return ref


class _FakeAf:
    def __init__(self, groups):
        self.groups = groups

    def manifest_stamp_groups(self, _manifest, keys):
        assert tuple(keys) == ds.MANIFEST_KEYS
        return self.groups


class _Manifest:
    """Manifest rows derived from ``seed`` the way the generator does."""

    def __init__(self, seed, n=7):
        self.rows = ts.manifest_rows(seed, n)

    def select(self, *cols):
        assert cols == ("typology_id", "seed")
        return self

    def toLocalIterator(self):  # noqa: N802 -- the pyspark name
        return iter({"typology_id": t, "seed": s} for t, s in self.rows)


def _groups(stamped):
    if stamped:
        return [({ds.MANIFEST_STAMP_KEY: "true", **{k: v[0] for k, v in MULT.items()}}, 7)]
    return [(dict.fromkeys(ds.MANIFEST_KEYS), 7)]


def test_scorer_refuses_a_robustness_look_on_an_unstamped_corpus(scorer, monkeypatch):
    # What an old image (lb-datagen:14c4eee) produces for the robustness seed: the
    # right instance seeds, no stamp. The config still declares the
    # perturbation, so only the corpus can tell.
    monkeypatch.setenv("LB_DATAGEN_SEED", str(ROBUST))
    monkeypatch.setenv("LB_DATAGEN_CORPUS_ROLE", "robustness")
    monkeypatch.setenv("LB_DATAGEN_ROBUSTNESS_PERTURBATION", "true")
    with pytest.raises(SystemExit, match="no robustness stamp"):
        scorer._refuse_guarded_corpus(_FakeAf(_groups(False)), _Manifest(ROBUST), counts_only=False)
    stamp, verdict = scorer._refuse_guarded_corpus(
        _FakeAf(_groups(True)), _Manifest(ROBUST), counts_only=False
    )
    assert verdict.matches_claim is True
    assert stamp["n_stamped"] == stamp["n_instances"] == 7


def test_scorer_refuses_a_stamp_the_config_did_not_declare(scorer, monkeypatch):
    monkeypatch.setenv("LB_DATAGEN_SEED", "7777")
    monkeypatch.delenv("LB_DATAGEN_CORPUS_ROLE", raising=False)
    monkeypatch.delenv("LB_DATAGEN_ROBUSTNESS_PERTURBATION", raising=False)
    with pytest.raises(SystemExit, match="declares"):
        scorer._refuse_guarded_corpus(_FakeAf(_groups(True)), _Manifest(7777), counts_only=False)
    assert scorer._refuse_guarded_corpus(
        _FakeAf(_groups(False)), _Manifest(7777), counts_only=False
    )


@pytest.mark.parametrize("abbrev", ["--rob", "--robust", "--robustness"])
def test_entrypoint_does_not_accept_abbreviations(monkeypatch, abbrev):
    # An abbreviation is an unknown flag: refused (exit 2), never read as
    # --robustness-perturbation.
    rc, cmd = _entry_cmd(
        ["--schema", "financial", "--bucket", "b", "--seed", "7777", abbrev], monkeypatch
    )
    assert rc == 2 and cmd is None


def test_local_gate_refuses_an_unstamped_robustness_corpus_before_scoring(
    monkeypatch, tmp_path, capsys
):
    from tests.conftest import exec_repo_script

    af = MagicMock()
    af.read_manifest.return_value = _Manifest(ROBUST)
    af.manifest_stamp_groups.return_value = _groups(False)
    for mod, stub in (
        ("aml_features", af),
        ("pyspark", MagicMock()),
        ("pyspark.sql", MagicMock()),
        ("pyspark.sql.functions", MagicMock()),
    ):
        monkeypatch.setitem(sys.modules, mod, stub)
    gate = exec_repo_script(REPO / "scripts/aml_gate.py", "aml_gate_robustness_stamp")
    opened = json.loads(json.dumps(PREREG))
    opened["corpora"]["registered_looks_open"] = True
    from lakebench.aml import fidelity_gate

    monkeypatch.setattr(fidelity_gate, "load_preregistration", lambda *a, **k: (opened, "x"))
    monkeypatch.setattr(fidelity_gate, "library_versions", lambda: {})
    monkeypatch.setattr(gate, "version_mismatches", lambda _v: {})
    monkeypatch.setattr(gate, "clean_checkout_error", lambda: None)
    monkeypatch.setattr(gate, "seed_ever_recorded", lambda _s: None)
    monkeypatch.setattr(gate, "registered_corpus_problem", lambda *a: None)
    monkeypatch.setattr(gate, "predictions_error", lambda _i: None)
    monkeypatch.setattr("lakebench.config.datagen_seed.load_predictions", lambda: ({}, "x"))
    seed_file = tmp_path / "seed"
    seed_file.write_text(f"{ROBUST}\n")
    seed_file.chmod(0o600)
    corpus = tmp_path / "c"
    corpus.mkdir()
    rc = gate.main(
        [
            str(corpus),
            "--registered",
            "robustness",
            "--seed-file",
            str(seed_file),
            "--out",
            str(tmp_path / "o.json"),
            "--generator-image",
            "repo@sha256:" + "0" * 64,
        ]
    )
    err = capsys.readouterr().err
    assert rc == 1 and "no robustness stamp" in err, err
    af.build_gate_inputs.assert_not_called()
    af.corpus_scale.assert_not_called()
