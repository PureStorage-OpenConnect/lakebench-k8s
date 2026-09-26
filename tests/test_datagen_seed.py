"""The cluster datagen seed comes from config and a spent AML seed is refused
(AML-GOALS section 9 #38/#39: the seed used to be hard-coded to the spent 42)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import datagen_seed as ds
from tests.conftest import make_config

PREREG = (
    Path(__file__).resolve().parents[1] / "src/lakebench/spark/data/aml/aml_preregistration.json"
)


def _cfg(schema: str, seed: int | None = None):
    dg = {} if seed is None else {"seed": seed}
    return make_config(architecture={"workload": {"schema": schema, "datagen": dg}})


def test_spent_seeds_come_from_the_preregistration():
    corpora = json.loads(PREREG.read_text())["corpora"]
    assert ds.spent_seeds() == frozenset(corpora["spent_seeds"])
    assert 42 in ds.spent_seeds()
    # The live roles are never spent.
    for role in ("calibration_seed", "evaluation_seed", "robustness_seed"):
        assert corpora[role] not in ds.spent_seeds(), role


def test_unset_seed_resolves_per_schema():
    assert (
        ds.config_seed(_cfg("financial"))
        == json.loads(PREREG.read_text())["corpora"]["calibration_seed"]
    )
    # Other schemas keep the seed their corpora always had.
    assert ds.config_seed(_cfg("customer360")) == 42


def test_explicit_seed_is_used():
    assert ds.config_seed(_cfg("financial", 7777)) == 7777
    assert ds.config_seed(_cfg("customer360", 7)) == 7


@pytest.mark.parametrize("seed", [42, 50000042])
def test_spent_financial_seed_refused_at_load(seed):
    with pytest.raises(ValidationError, match="spent"):
        _cfg("financial", seed)


def test_spent_seed_refused_even_if_validation_is_bypassed():
    cfg = _cfg("financial")
    cfg.architecture.workload.datagen.seed = 42  # no validate_assignment
    with pytest.raises(ValueError, match="spent"):
        ds.config_seed(cfg)


def test_non_aml_schema_may_use_42():
    assert ds.config_seed(_cfg("customer360", 42)) == 42


def test_deployer_renders_the_configured_seed():
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    cfg = _cfg("financial", 7777)
    engine = DeploymentEngine(cfg, dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    assert ctx["datagen_seed"] == 7777
    job = yaml.safe_load(engine.renderer.render("datagen/job.yaml.j2", ctx))
    args = job["spec"]["template"]["spec"]["containers"][0]["args"]
    assert args[args.index("--seed") + 1] == "7777"


def test_template_has_no_seed_default():
    # A default in the template would bring the spent 42 back for any
    # renderer that forgets the context key; StrictUndefined fails instead.
    src = (
        Path(__file__).resolve().parents[1] / "src/lakebench/templates/datagen/job.yaml.j2"
    ).read_text()
    assert "datagen_seed | default" not in src


def test_reference_job_reports_the_configured_seed(monkeypatch):
    from lakebench.modules.pipeline_engines.spark import job as jobmod
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    monkeypatch.setattr(jobmod, "_lakebench_git_sha", lambda: "abc")
    mgr = SparkJobManager(_cfg("financial", 7777), MagicMock())
    env = {
        e["name"]: e.get("value") for e in mgr._build_env_vars(JobType.SCORE_FINANCIAL_REFERENCE)
    }
    assert env["LB_DATAGEN_SEED"] == "7777"


# ---------------------------------------------------------------------------
# Evaluation and robustness seeds: only the registered run (prereg 3.5.2)
# ---------------------------------------------------------------------------

_CORPORA = json.loads(PREREG.read_text())["corpora"]
EVAL, ROBUST = _CORPORA["evaluation_seed"], _CORPORA["robustness_seed"]


def _cfg_role(seed, role):
    dg = {"corpus_role": role}
    if seed is not None:
        dg["seed"] = seed
    return make_config(architecture={"workload": {"schema": "financial", "datagen": dg}})


@pytest.mark.parametrize("seed", [EVAL, ROBUST])
def test_protected_seed_refused_without_its_role(seed):
    with pytest.raises(ValidationError, match="registered"):
        _cfg("financial", seed)


@pytest.mark.parametrize(("seed", "role"), [(EVAL, "evaluation"), (ROBUST, "robustness")])
def test_protected_seed_allowed_with_its_role(seed, role):
    assert ds.config_seed(_cfg_role(seed, role)) == seed
    # The role alone selects its registered seed.
    assert ds.config_seed(_cfg_role(None, role)) == seed


@pytest.mark.parametrize(
    ("seed", "role"),
    [(EVAL, "robustness"), (ROBUST, "evaluation"), (7777, "evaluation"), (EVAL, "calibration")],
)
def test_role_must_match_its_registered_seed(seed, role):
    with pytest.raises(ValidationError, match="registered for seed"):
        _cfg_role(seed, role)


def test_spent_seed_refused_even_with_a_role():
    with pytest.raises(ValidationError):
        _cfg_role(42, "evaluation")


def test_role_is_financial_only():
    with pytest.raises(ValidationError, match="financial"):
        make_config(
            architecture={
                "workload": {"schema": "customer360", "datagen": {"corpus_role": "evaluation"}}
            }
        )


def test_reference_job_records_the_declared_role(monkeypatch):
    from lakebench.modules.pipeline_engines.spark import job as jobmod
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    monkeypatch.setattr(jobmod, "_lakebench_git_sha", lambda: "abc")

    def env(cfg):
        mgr = SparkJobManager(cfg, MagicMock())
        return {
            e["name"]: e.get("value")
            for e in mgr._build_env_vars(JobType.SCORE_FINANCIAL_REFERENCE)
        }

    e = env(_cfg_role(None, "evaluation"))
    assert (e["LB_DATAGEN_SEED"], e["LB_DATAGEN_CORPUS_ROLE"]) == (str(EVAL), "evaluation")
    assert "LB_DATAGEN_CORPUS_ROLE" not in env(_cfg("financial", 7777))


def _gate():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "aml_gate_runner", Path(__file__).resolve().parents[1] / "scripts/aml_gate.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_gate_guard_refuses_unregistered_looks():
    g = _gate()
    # Calibration and unregistered seeds score freely.
    assert g.seed_guard_error(43, None, []) is None
    assert g.seed_guard_error(7777, None, []) is None
    assert g.seed_guard_error(None, None, []) is None
    # Spent: always refused, registered or not.
    assert "spent" in g.seed_guard_error(42, None, [])
    assert "spent" in g.seed_guard_error(None, "evaluation", [42])
    # Evaluation / robustness: only as the registered run for that role.
    assert "registered" in g.seed_guard_error(EVAL, None, [])
    assert g.seed_guard_error(EVAL, "evaluation", [EVAL]) is None
    assert g.seed_guard_error(ROBUST, "robustness", [ROBUST]) is None
    assert g.seed_guard_error(EVAL, "robustness", [EVAL]) is not None
    # --registered cannot be attached to another seed.
    assert g.seed_guard_error(7777, "evaluation", []) is not None


def test_gate_guard_uses_the_manifest_seed_not_the_claim():
    g = _gate()
    # An evaluation corpus scored with --seed omitted or misstated is refused.
    assert g.seed_guard_error(None, None, [EVAL]) is not None
    assert "not the claimed" in g.seed_guard_error(7777, None, [EVAL])
    assert "not the claimed" in g.seed_guard_error(7777, "evaluation", [EVAL])


def test_gate_refuses_before_spark_starts():
    # main() returns 1 on the pre-Spark check: no Spark import is reached.
    g = _gate()
    assert g.main(["/nonexistent", "--seed", str(EVAL)]) == 1
    assert g.main(["/nonexistent", "--seed", "42", "--registered", "evaluation"]) == 1
