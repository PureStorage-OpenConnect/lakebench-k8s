"""``architecture.benchmark.investigator_sessions`` (AML continuous only).

The key is refused at load unless the workload is financial, the run is
continuous, TM operations are on and the query engine is trino or
spark-thrift, and ``run`` refuses it again with the mode it resolves (one
predicate for both). The identity keys on the sessions that ran
(``experiment.investigators.run``), not the configured N, and only when the
key is set: a default config's inputs and identity do not move.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lakebench.config.schema import investigator_sessions_problem
from lakebench.metrics import comparability as cmp
from lakebench.metrics import experiment as ex
from tests.conftest import make_config
from tests.test_comparability import _rec, _verdict
from tests.test_experiment import _cfg, _metrics


def _aml(sessions=8, *, schema="financial", mode="continuous", tm=True, recipe=None):
    kw = {}
    if recipe:
        kw["recipe"] = recipe
    return make_config(
        workload={"schema": schema, "tm_operations": {"enabled": tm}},
        architecture={
            "pipeline": {"mode": mode},
            "benchmark": {"investigator_sessions": sessions},
        },
        **kw,
    )


def test_aml_continuous_on_trino_or_thrift_accepts_the_key():
    assert _aml().architecture.benchmark.investigator_sessions == 8
    thrift = _aml(recipe="hive-iceberg-spark-thrift")
    assert thrift.architecture.query_engine.type.value == "spark-thrift"


@pytest.mark.parametrize(
    ("kw", "names"),
    [
        ({"schema": "customer360"}, "the workload is customer360, not financial"),
        ({"tm": False}, "workload.tm_operations.enabled is false"),
        ({"recipe": "hive-iceberg-spark-duckdb"}, "the query engine is duckdb"),
    ],
    ids=["c360", "tm-off", "duckdb"],
)
def test_the_key_is_refused_outside_aml_continuous(kw, names):
    """Each refusal names the condition that fails."""
    with pytest.raises(ValidationError) as info:
        _aml(**kw)
    text = str(info.value)
    assert "investigator_sessions (8)" in text and names in text, text


@pytest.mark.parametrize("bad", [0, 33])
def test_the_key_is_bounded(bad):
    with pytest.raises(ValidationError):
        _aml(bad)


def test_the_mode_is_runs_to_check():
    """A batch AML config with the key loads; run refuses it in batch (the
    RUN_RULES row, the same predicate with the run's mode) and accepts it
    with --continuous."""
    from lakebench.cli._run_args import RunArgs, run_args_problems

    batch = _aml(mode="batch")
    (rule,) = run_args_problems(RunArgs(), batch)
    assert "investigator_sessions runs only on an AML continuous run" in rule.text(RunArgs())
    assert run_args_problems(RunArgs(continuous=True), batch) == []
    assert run_args_problems(RunArgs(), _aml()) == []
    arch = batch.architecture
    assert "the run is batch" in investigator_sessions_problem(arch, "batch")
    assert investigator_sessions_problem(arch, "continuous") is None
    assert investigator_sessions_problem(arch, check_mode=False) is None
    # A default config never reaches the predicate's conditions.
    assert investigator_sessions_problem(make_config().architecture, "batch") is None


def test_a_default_config_has_no_investigators_input_or_identity_key():
    cfg = _cfg(schema="financial", mode="continuous")
    assert "investigator_sessions" not in ex.experiment_inputs(cfg)
    exp = ex.build_experiment(_metrics(cfg))
    assert "investigators" not in exp
    assert "investigator sessions" not in ex.identity(exp)
    assert "investigator sessions" not in cmp.optional_keys(exp)


def test_the_block_records_the_sessions_that_ran():
    cfg = _aml()
    assert ex.experiment_inputs(cfg)["investigator_sessions"] == 8
    m = _metrics(cfg)
    # No sessions round recorded: none ran.
    assert ex.build_experiment(m)["investigators"] == {"requested": 8, "run": 0}
    m.continuous = {"investigators": {"sessions_requested": 8, "sessions_run": 3}}
    exp = ex.build_experiment(m)
    assert exp["investigators"] == {"requested": 8, "run": 3}
    # The v2 identity's optional key reads what ran (this synthetic record
    # has no system identity, so its identity is v1, which has no such key).
    assert cmp.optional_keys(exp)["investigator sessions"] == 3


def _with_sessions(run_id, run):
    return _rec(new_id=run_id, experiment__investigators={"requested": 8, "run": run})


def test_a_lowered_round_is_not_like_for_like():
    """Both configured at 8; one ran 8 sessions, one 3 (fewer cases): not
    like-for-like. Keyed on the configured N they would read like-for-like."""
    v = _verdict(_with_sessions("a", 8), _with_sessions("b", 3))
    assert (v.verdict, v.keys(cmp.CONDITIONS)) == (
        cmp.NOT_LIKE_FOR_LIKE,
        ["investigator sessions"],
    )


def test_a_skipped_round_against_a_default_run_is_not_like_for_like():
    v = _verdict(_with_sessions("a", 0), _rec(new_id="b"))
    assert v.verdict == cmp.NOT_LIKE_FOR_LIKE
    assert v.keys(cmp.CONDITIONS) == ["investigator sessions"]


def _exp2(run=None):
    """A v1.7 (exp2) experiment block, with investigator sessions when *run*
    is not None."""
    from tests.test_comparability import _fresh

    e = _fresh().to_dict()["experiment"]
    assert e["schema"] == "exp2"
    if run is not None:
        e["investigators"] = {"requested": 8, "run": run}
    return e


def _gate(base, run):
    return ex.stored_identity_refusals(
        ex.identity(base), ex.result_fingerprints(base), run, "baseline"
    )


def test_the_perf_gate_does_not_refuse_on_the_sessions_that_ran():
    """An outcome condition: a regression that costs sessions (8 to 3) reads
    as a regression, not as a different experiment. On exp2 records, whose
    identity carries the key."""
    assert ex.identity(_exp2(8))["investigator sessions"] == 8
    assert _gate(_exp2(8), _exp2(3)) == []
    assert _gate(_exp2(3), _exp2(8)) == []


def test_the_perf_gate_refuses_load_against_no_load():
    """Load is never matched to no load: none configured, or configured but
    no session ran (a skipped round), against sessions that ran."""
    for base, run in (
        (_exp2(None), _exp2(8)),
        (_exp2(8), _exp2(None)),
        (_exp2(8), _exp2(0)),
        (_exp2(0), _exp2(8)),
    ):
        refusals = _gate(base, run)
        assert any("investigator load differs" in r for r in refusals), (base, run, refusals)
    assert _gate(_exp2(None), _exp2(None)) == []
    assert _gate(_exp2(None), _exp2(0)) == []
