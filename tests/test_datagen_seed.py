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


@pytest.fixture
def looks_open(monkeypatch):
    """The pre-registration after the freeze: registered looks open."""
    opened = {**ds._corpora(), "registered_looks_open": True}
    monkeypatch.setattr(ds, "_corpora", lambda: opened)


def test_registered_looks_are_closed_until_the_freeze():
    assert _CORPORA.get("registered_looks_open") is False
    with pytest.raises(ValidationError, match="closed"):
        _cfg_role(EVAL, "evaluation")


@pytest.fixture
def perturbation_on(monkeypatch):
    """Datagen applies corpora.robustness_perturbation (lane T2)."""
    monkeypatch.setattr(ds, "ROBUSTNESS_PERTURBATION_IMPLEMENTED", True)


@pytest.mark.parametrize(("seed", "role"), [(EVAL, "evaluation"), (ROBUST, "robustness")])
def test_protected_seed_allowed_with_its_role(seed, role, looks_open, perturbation_on):
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


def test_reference_job_records_the_declared_role(monkeypatch, looks_open):
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


def test_gate_guard_refuses_unregistered_looks(looks_open, perturbation_on):
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


def test_gate_guard_counts_only_is_not_a_look():
    g = _gate()
    # A counts-only smoke run may touch a protected corpus (no AP), never a
    # spent one, and never under a registered role.
    assert g.seed_guard_error(EVAL, None, [EVAL], counts_only=True) is None
    assert g.seed_guard_error(42, None, [], counts_only=True) is not None
    assert g.seed_guard_error(EVAL, "evaluation", [EVAL], counts_only=True) is not None


def test_mixed_corpus_counts_as_the_guarded_seed():
    # A manifest mixing a guarded seed's instances with others is refused:
    # any matching instance seed puts the guarded seed in `matched`.
    assert ds.aml_seed_error(_CORPORA, 7777, None, [EVAL]) is not None
    assert ds.aml_seed_error(_CORPORA, None, None, [42]) is not None


def test_guard_ships_flat_to_the_spark_driver():
    from lakebench.modules.pipeline_engines.spark import job as jobmod

    src = Path(jobmod.__file__).read_text()
    assert '_package_dir() / "config" / "datagen_seed.py"' in src
    ref = (
        Path(__file__).resolve().parents[1]
        / "src/lakebench/spark/scripts/score_financial_reference.py"
    ).read_text()
    assert "from datagen_seed import" in ref and "refusing to score this corpus" in ref


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


def test_flat_copy_imports_without_lakebench(tmp_path):
    # On the driver the module sits flat next to the scripts with no
    # lakebench package: it must import and work from the corpora dict alone.
    import subprocess
    import sys

    src = Path(ds.__file__).read_text()
    (tmp_path / "datagen_seed.py").write_text(src)
    code = (
        "import json,sys; sys.path.insert(0, '.');"
        "from datagen_seed import aml_seed_error;"
        f"c=json.loads({json.dumps(json.dumps(_CORPORA))});"
        f"assert aml_seed_error(c, {EVAL}) and aml_seed_error(c, 43) is None"
    )
    r = subprocess.run(
        [sys.executable, "-I", "-c", code], cwd=tmp_path, capture_output=True, text=True
    )
    assert r.returncode == 0, r.stderr


def test_registered_run_needs_a_verified_corpus(looks_open):
    g = _gate()
    # A seed-43 corpus scored as the registered evaluation run: the manifest
    # does not come from the evaluation seed, so the look is refused.
    assert "not verified" in g.seed_guard_error(EVAL, "evaluation", [], claim_verified=False)
    assert g.seed_guard_error(EVAL, "evaluation", [EVAL], claim_verified=True) is None
    # Generation time (no corpus yet) is not a verification failure.
    assert ds.aml_seed_error(ds._corpora(), EVAL, "evaluation") is None


def test_cluster_refusal_runs_before_anything_is_written():
    src = (
        Path(__file__).resolve().parents[1]
        / "src/lakebench/spark/scripts/score_financial_reference.py"
    ).read_text()
    main = src[src.index("def main()") :]
    assert main.index("_refuse_guarded_corpus(") < main.index("compute_leakage_gate(")


def test_robustness_look_refused_until_the_perturbation_exists(looks_open):
    assert ds.ROBUSTNESS_PERTURBATION_IMPLEMENTED is False
    err = ds.aml_seed_error(ds._corpora(), ROBUST, "robustness", [ROBUST], claim_verified=True)
    assert err is not None and "perturbation" in err
    # Evaluation is unaffected.
    assert ds.aml_seed_error(ds._corpora(), EVAL, "evaluation", [EVAL], claim_verified=True) is None


@pytest.mark.parametrize(
    "bad",
    [{k: v for k, v in _CORPORA.items() if k != "spent_seeds"}, {**_CORPORA, "spent_seeds": "42"}],
)
def test_damaged_spent_seeds_fail_closed(bad):
    with pytest.raises((KeyError, ValueError)):
        ds.aml_seed_error(bad, 42)
    with pytest.raises((KeyError, ValueError)):
        ds.aml_seed_error(bad, 43)


@pytest.mark.parametrize("flag", ["false", "true", 1, None])
def test_looks_open_only_when_literally_true(flag):
    corpora = {**_CORPORA, "registered_looks_open": flag}
    err = ds.aml_seed_error(corpora, EVAL, "evaluation", [EVAL], claim_verified=True)
    assert err is not None and "closed" in err


@pytest.mark.parametrize(
    "extra",
    [
        ["--diagnostic"],
        ["--label-role", "participant"],
        ["--allow-version-mismatch"],
        ["--prereg", "/x.json"],
        [],  # no --out
    ],
)
def test_registered_look_runs_only_as_registered(extra, looks_open):
    g = _gate()
    out = [] if extra == [] else ["--out", "/nonexistent/r.json"]
    argv = ["/nonexistent", "--seed", str(EVAL), "--registered", "evaluation", *out, *extra]
    assert g.main(argv) == 1


def test_registered_look_gets_no_operator_retry(looks_open):
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    def policy(cfg):
        mgr = SparkJobManager(cfg, MagicMock())
        m = mgr._build_manifest(JobType.SCORE_FINANCIAL_REFERENCE)
        return m["spec"]["restartPolicy"]

    reg = policy(_cfg_role(None, "evaluation"))
    assert reg["onFailureRetries"] == 0 and reg["onSubmissionFailureRetries"] == 5
    assert policy(_cfg("financial", 7777))["type"] == "OnFailure"


def test_binary_spent_list_is_a_subset_of_the_preregistration():
    import re

    src = (Path(__file__).resolve().parents[1] / "datagen_rs/src/bin/generate.rs").read_text()
    m = re.search(r"const SPENT_SEEDS: &\[i64\] = &\[([^\]]*)\];", src)
    assert m, "SPENT_SEEDS not found in generate.rs"
    rust = {int(x.replace("_", "")) for x in m.group(1).split(",") if x.strip()}
    assert rust and rust <= set(_CORPORA["spent_seeds"])


def test_entrypoint_requires_a_financial_seed():
    import subprocess
    import sys

    ep = Path(__file__).resolve().parents[1] / "datagen_rs/entrypoint.py"
    r = subprocess.run(
        [sys.executable, str(ep), "--schema", "financial", "--bucket", "b"],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 2 and "--seed is required" in r.stderr


def test_registered_look_claims_out_before_spark(tmp_path, looks_open):
    g = _gate()
    out = tmp_path / "look.json"
    out.write_text("{}")  # an earlier look's record
    argv = ["/nonexistent", "--seed", str(EVAL), "--registered", "evaluation", "--out", str(out)]
    assert g.main(argv) == 1
    assert out.read_text() == "{}"
    missing = tmp_path / "no-such-dir" / "look.json"
    argv[-1] = str(missing)
    assert g.main(argv) == 1
