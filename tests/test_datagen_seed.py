"""The cluster datagen seed comes from config and a spent AML seed is refused
(AML-GOALS section 9 #38/#39: the seed used to be hard-coded to the spent 42)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import datagen_seed as ds
from tests.conftest import make_config
from tests.fixtures import heldout_test_seeds as ts

PREREG = (
    Path(__file__).resolve().parents[1] / "src/lakebench/spark/data/aml/aml_preregistration.json"
)


def _cfg(schema: str, seed: int | None = None):
    dg = {} if seed is None else {"seed": seed}
    return make_config(architecture={"workload": {"schema": schema, "datagen": dg}})


def test_prereg_spent_list_holds_no_unlooked_heldout_seed():
    # The calibration seed is never spent; the held-out seeds are known only by
    # hash, and a spent seed hashes to one only when its look or burn is recorded.
    assert _CORPORA["calibration_seed"] not in ds.spent_seeds()
    assert not any(ds.heldout_role(s) for s in ds.spent_seeds() - ds.recorded_seeds())


def test_unset_financial_seed_resolves_to_the_calibration_seed():
    assert ds.config_seed(_cfg("financial")) == _CORPORA["calibration_seed"]


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


def test_template_without_the_seed_fails_instead_of_defaulting():
    # A default in the template would bring the spent 42 back for any
    # renderer that forgets the context key; StrictUndefined fails instead.
    from jinja2 import UndefinedError

    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    engine = DeploymentEngine(_cfg("financial", 7777), dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    ctx.pop("datagen_seed")
    assert not ctx.get("datagen_seed_secret")
    with pytest.raises(UndefinedError):
        engine.renderer.render("datagen/job.yaml.j2", ctx)


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
# The plaintext held-out keys are dropped so no assertion can print them.
for _k in ("evaluation_seed", "robustness_seed"):
    _CORPORA.pop(_k, None)
# Test-only held-out seeds, registered in tests/fixtures/heldout_test.json.
EVAL, ROBUST = ts.TEST_EVALUATION_SEED, ts.TEST_ROBUSTNESS_SEED


@pytest.fixture(autouse=True)
def _fixture_heldout(monkeypatch):
    ts.use_fixture(monkeypatch)


def _no_plain(corpora: dict) -> dict:
    """``corpora`` without the plaintext held-out keys, so no assertion prints them."""
    return {k: v for k, v in corpora.items() if k not in ("evaluation_seed", "robustness_seed")}


def _cfg_role(seed, role, perturb=None):
    dg = {"corpus_role": role}
    if seed is not None:
        dg["seed"] = seed
    # The registered robustness corpus is the perturbed one.
    dg["robustness_perturbation"] = role == "robustness" if perturb is None else perturb
    return make_config(architecture={"workload": {"schema": "financial", "datagen": dg}})


@pytest.mark.parametrize("seed", [EVAL, ROBUST])
def test_protected_seed_refused_without_its_role(seed):
    with pytest.raises(ValidationError, match="registered"):
        _cfg("financial", seed)


@pytest.fixture
def looks_open(monkeypatch):
    """The pre-registration after the freeze: registered looks open."""
    opened = {**_no_plain(ds._corpora()), "registered_looks_open": True}
    monkeypatch.setattr(ds, "_corpora", lambda: opened)


@pytest.mark.parametrize(("seed", "role"), [(EVAL, "evaluation"), (ROBUST, "robustness")])
def test_protected_seed_allowed_with_its_role(seed, role, looks_open):
    assert ds.config_seed(_cfg_role(seed, role)) == seed
    # The role alone cannot fill the seed: there is no plaintext to fill it from.
    with pytest.raises(ValidationError, match="names its seed in datagen.seed"):
        _cfg_role(None, role)


@pytest.mark.parametrize(
    ("seed", "role"),
    [(EVAL, "robustness"), (ROBUST, "evaluation"), (7777, "evaluation"), (EVAL, "calibration")],
)
def test_role_must_match_its_registered_seed(seed, role):
    with pytest.raises(ValidationError, match="registered for|does not match"):
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
            e["name"]: e.get("value", e.get("valueFrom"))
            for e in mgr._build_env_vars(JobType.SCORE_FINANCIAL_REFERENCE)
        }

    from lakebench.config.seed_secret import seed_secret_env

    e = env(_cfg_role(EVAL, "evaluation"))
    # A registered corpus's seed comes from the seed Secret, never as a value.
    assert e["LB_DATAGEN_SEED"] == seed_secret_env(_cfg_role(EVAL, "evaluation"))["valueFrom"]
    assert e["LB_DATAGEN_CORPUS_ROLE"] == "evaluation"
    assert env(_cfg("financial", 7777))["LB_DATAGEN_SEED"] == "7777"
    assert "LB_DATAGEN_CORPUS_ROLE" not in env(_cfg("financial", 7777))


def _gate():
    from tests.conftest import exec_repo_script

    return exec_repo_script(
        Path(__file__).resolve().parents[1] / "scripts/aml_gate.py", "aml_gate_runner"
    )


@pytest.mark.parametrize(
    ("seed", "role", "matched", "counts_only", "looks_open_", "refused"),
    [
        # Calibration and unregistered seeds score freely.
        (43, None, [], False, True, False),
        (7777, None, [], False, True, False),
        (None, None, [], False, True, False),
        # Spent: always refused, registered or not.
        (42, None, [], False, True, True),
        (None, "evaluation", [42], False, True, True),
        # Evaluation / robustness: only as the registered run for that role.
        (EVAL, None, [], False, True, True),
        (EVAL, "evaluation", [EVAL], False, True, False),
        (ROBUST, "robustness", [ROBUST], False, True, False),
        (EVAL, "robustness", [EVAL], False, True, True),
        # --registered cannot be attached to another seed.
        (7777, "evaluation", [], False, True, True),
        # A counts-only smoke run may touch a protected corpus (no AP), never a
        # spent one, and never under a registered role.
        (EVAL, None, [EVAL], True, False, False),
        (42, None, [], True, False, True),
        (EVAL, "evaluation", [EVAL], True, False, True),
        # A manifest mixing a guarded seed's instances with others is refused:
        # any matching instance seed puts the guarded seed in `matched`.
        (7777, None, [EVAL], False, False, True),
        (None, None, [42], False, False, True),
        # An evaluation corpus scored with --seed omitted or misstated is refused.
        (None, None, [EVAL], False, False, True),
        (7777, "evaluation", [EVAL], False, False, True),
    ],
)
def test_gate_guard_table(seed, role, matched, counts_only, looks_open_, refused, monkeypatch):
    if looks_open_:
        opened = {**_no_plain(ds._corpora()), "registered_looks_open": True}
        monkeypatch.setattr(ds, "_corpora", lambda: opened)
    err = _gate().seed_guard_error(seed, role, matched, counts_only)
    assert (err is not None) is refused


def test_guard_ships_flat_to_the_spark_driver():
    from lakebench.config import LakebenchConfig
    from lakebench.modules.pipeline_engines.spark.scripts_maps import build_script_configmaps

    seed_src = (
        Path(__file__).resolve().parents[1] / "src/lakebench/config/datagen_seed.py"
    ).read_text()
    shipped = {
        k: v
        for cm in build_script_configmaps(LakebenchConfig(name="t"), "ns")
        for k, v in cm["data"].items()
    }
    assert shipped["datagen_seed.py"] == seed_src, "the guard ships flat, byte for byte"


def test_gate_refuses_before_spark_starts():
    # main() returns 1 on the pre-Spark check: no Spark import is reached.
    g = _gate()
    assert g.main(["/nonexistent", "--seed", str(EVAL)]) == 1
    assert g.main(["/nonexistent", "--seed", "42", "--registered", "evaluation"]) == 1


def test_flat_copy_imports_without_lakebench(tmp_path):
    # On the driver the module sits flat next to the scripts with no
    # lakebench package: it must import and work from the corpora dict and
    # the hash file mounted next to it.
    import subprocess

    src = Path(ds.__file__).read_text()
    (tmp_path / "datagen_seed.py").write_text(src)
    (tmp_path / ds.HELDOUT_FILENAME).write_text(ts.FIXTURE.read_text())
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
    assert ds.aml_seed_error(_no_plain(ds._corpora()), EVAL, "evaluation") is None


def test_cluster_refusal_runs_before_anything_is_written():
    src = (
        Path(__file__).resolve().parents[1]
        / "src/lakebench/spark/scripts/score_financial_reference.py"
    ).read_text()
    main = src[src.index("def main()") :]
    assert main.index("_refuse_guarded_corpus(") < main.index("compute_leakage_gate(")


@pytest.mark.parametrize(
    "bad",
    [{k: v for k, v in _CORPORA.items() if k != "spent_seeds"}, {**_CORPORA, "spent_seeds": "42"}],
)
def test_damaged_spent_seeds_fail_closed(bad):
    with pytest.raises((KeyError, ValueError)):
        ds.aml_seed_error(bad, 42)
    with pytest.raises((KeyError, ValueError)):
        ds.aml_seed_error(bad, 43)


@pytest.mark.parametrize("flag", [False, "false", "true", 1, None])
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
def test_registered_look_runs_only_as_registered(extra, looks_open, tmp_path, monkeypatch):
    g = _gate()
    # Refusal comes before Spark: importing the scorer's Spark side would fail.
    monkeypatch.setitem(sys.modules, "aml_features", None)
    out = [] if extra == [] else ["--out", "/nonexistent/r.json"]
    argv = [
        "/nonexistent",
        "--seed-file",
        str(_seed_file(tmp_path, EVAL)),
        "--generator-image",
        "repo@sha256:" + "0" * 64,
        "--registered",
        "evaluation",
        *out,
        *extra,
    ]
    assert g.main(argv) == 1


def _seed_file(tmp_path, seed):
    """The owner-only seed file a registered look reads its seed from."""
    import os

    f = tmp_path / "seed"
    f.write_text(f"{seed}\n")
    os.chmod(f, 0o600)
    return f


def test_registered_look_gets_no_operator_retry(looks_open):
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    def policy(cfg):
        mgr = SparkJobManager(cfg, MagicMock())
        m = mgr._build_manifest(JobType.SCORE_FINANCIAL_REFERENCE)
        return m["spec"]["restartPolicy"]

    assert policy(_cfg_role(EVAL, "evaluation"))["onFailureRetries"] == 0
    dev = policy(_cfg("financial", 7777))
    assert dev["type"] == "OnFailure" and dev["onFailureRetries"] > 0


def test_binary_spent_list_is_a_subset_of_the_preregistration():
    import re

    src = (Path(__file__).resolve().parents[1] / "datagen_rs/src/bin/generate.rs").read_text()
    m = re.search(r"const SPENT_SEEDS: &\[i64\] = &\[([^\]]*)\];", src)
    assert m, "SPENT_SEEDS not found in generate.rs"
    rust = {int(x.replace("_", "")) for x in m.group(1).split(",") if x.strip()}
    assert rust and rust <= set(_CORPORA["spent_seeds"])


def test_entrypoint_requires_a_financial_seed(monkeypatch):
    import importlib.util
    from unittest.mock import patch

    path = Path(__file__).resolve().parents[1] / "datagen_rs/entrypoint.py"
    spec = importlib.util.spec_from_file_location("datagen_entrypoint_seed", path)
    ep = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ep)
    monkeypatch.delenv("LB_DATAGEN_SEED", raising=False)
    monkeypatch.setenv("CPU_LIMIT", "8")

    def _no_exec(*_a):
        raise AssertionError("generator started without a seed")

    argv = ["entrypoint.py", "--schema", "financial", "--bucket", "b"]
    with patch.object(sys, "argv", argv), patch.object(ep.os, "execvp", _no_exec):
        assert ep.main() == 2


def test_registered_look_claims_out_before_spark(tmp_path, looks_open, monkeypatch):
    g = _gate()
    monkeypatch.setitem(sys.modules, "aml_features", None)
    # The look preconditions before the claim are met (a clean checkout, no
    # earlier look of this seed, committed predictions), so the claim decides.
    monkeypatch.setattr(g, "clean_checkout_error", lambda: None)
    monkeypatch.setattr(g, "seed_ever_recorded", lambda seed: None)
    # The corpus is the one generate --registered-corpus wrote (its own tests
    # are in test_registered_corpus.py).
    monkeypatch.setattr(g, "registered_corpus_problem", lambda *a: None)
    monkeypatch.setattr(g, "predictions_error", lambda image: None)
    monkeypatch.setattr(ds, "load_predictions", lambda *a, **k: ({}, "0" * 64))
    out = tmp_path / "look.json"
    out.write_text("{}")  # an earlier look's record
    argv = [
        "/nonexistent",
        "--seed-file",
        str(_seed_file(tmp_path, EVAL)),
        "--generator-image",
        "repo@sha256:" + "0" * 64,
        "--registered",
        "evaluation",
        "--out",
        str(out),
    ]
    assert g.main(argv) == 1
    assert out.read_text() == "{}"
    missing = tmp_path / "no-such-dir" / "look.json"
    argv[-1] = str(missing)
    assert g.main(argv) == 1
    assert not missing.exists()
