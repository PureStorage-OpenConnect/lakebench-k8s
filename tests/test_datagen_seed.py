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
