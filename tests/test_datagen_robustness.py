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

REPO = Path(__file__).resolve().parents[1]
PREREG = json.loads((REPO / "src/lakebench/spark/data/aml/aml_preregistration.json").read_text())
CORPORA = PREREG["corpora"]
RUST = (REPO / "datagen_rs/src/robustness.rs").read_text()


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


def test_rust_registered_seeds_match_the_preregistration():
    assert int(_rust_const("ROBUSTNESS_SEED")) == CORPORA["robustness_seed"]
    assert int(_rust_const("EVALUATION_SEED")) == CORPORA["evaluation_seed"]


def test_prereg_block_has_exactly_the_three_multipliers():
    # A new multiplier in the prereg needs a generator change, not silence.
    keys = {k for k in CORPORA["robustness_perturbation"] if not k.startswith("note")}
    assert keys == {
        "median_amount_multiplier",
        "persona_sd_multiplier",
        "dormancy_range_multiplier",
    }


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _cfg(schema="financial", **dg):
    return make_config(architecture={"workload": {"schema": schema, "datagen": dg}})


@pytest.fixture
def looks_open(monkeypatch):
    opened = {**ds._corpora(), "registered_looks_open": True}
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
        _cfg(corpus_role="robustness")
    cfg = _cfg(corpus_role="robustness", robustness_perturbation=True)
    assert ds.config_seed(cfg) == CORPORA["robustness_seed"]
    assert ds.config_perturbation(cfg) is True


@pytest.mark.parametrize("role", ["calibration", "evaluation"])
def test_other_roles_refuse_the_perturbation(role, looks_open):
    with pytest.raises(ValidationError, match="never perturbed"):
        _cfg(corpus_role=role, robustness_perturbation=True)


def test_refused_even_if_validation_is_bypassed(looks_open):
    cfg = _cfg(corpus_role="robustness", robustness_perturbation=True)
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


def test_template_without_the_key_renders_unperturbed():
    # A renderer that forgets the key gets the unperturbed corpus, and the
    # generator refuses the robustness seed without the flag.
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    engine = DeploymentEngine(_cfg(seed=7777), dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    del ctx["datagen_robustness_perturbation"]
    job = yaml.safe_load(engine.renderer.render("datagen/job.yaml.j2", ctx))
    assert (
        "--robustness-perturbation" not in job["spec"]["template"]["spec"]["containers"][0]["args"]
    )


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
            except SystemExit:
                rc = None
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
