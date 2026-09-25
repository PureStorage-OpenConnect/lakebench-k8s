"""AML batch runs must not report success when detection measured nothing
(2026-09-24 audit: zero alerts or every rule crashed still exited 0)."""

from __future__ import annotations

from types import SimpleNamespace

from lakebench.cli._run import _aml_batch_gate_problems

_PASS = {"1": {"reconciliation": {"status": "pass", "detail": ""}}}


def _job(alerts=None, errors=None, tm=_PASS):
    return SimpleNamespace(alerts_by_rule=alerts or {}, rule_errors=errors or {}, tm_invariants=tm)


def test_zero_alerts_fails():
    probs, _ = _aml_batch_gate_problems([_job({"W2": 0})])
    assert any("zero alerts" in p for p in probs)


def test_scoring_total_is_authoritative():
    probs, _ = _aml_batch_gate_problems([_job({})], {"total_alerts": 0})
    assert any("zero alerts" in p for p in probs)
    assert _aml_batch_gate_problems([_job({"W2": 0})], {"total_alerts": 12}) == ([], [])


def test_missing_logs_without_scoring_is_a_warning_not_a_failure():
    probs, warns = _aml_batch_gate_problems([_job({})], None)
    assert probs == [] and warns


def test_crashed_rule_fails_even_with_other_alerts():
    probs, _ = _aml_batch_gate_problems([_job({"W2": 5, "W7": 0}, {"W7": "AnalysisException"})])
    assert probs == ["Detection rule W7 failed: AnalysisException"]


def test_healthy_run_passes_and_last_cycle_wins():
    bad_then_good = [_job({"W2": 0}), _job({"W2": 3})]
    assert _aml_batch_gate_problems(bad_then_good) == ([], [])


def test_no_gold_job_is_not_judged_here():
    assert _aml_batch_gate_problems([]) == ([], [])


def test_skipped_behavioural_rule_warns():
    from types import SimpleNamespace

    from lakebench.cli._run import _aml_batch_gate_problems

    job = SimpleNamespace(
        rule_errors={},
        alerts_by_rule={"W2_structuring": 5},
        rules_skipped={"W1_connected_components": "giant-component"},
        tm_invariants=_PASS,
    )
    problems, warnings = _aml_batch_gate_problems([job], None)
    assert not problems
    assert any("gather_scatter" in w and "not run" in w for w in warnings)


# P10.2: the TM workflow invariants are a gate, every cycle.


def test_failed_workflow_invariant_fails_the_run():
    tm = {"1": {"sars_le_cases": {"status": "fail", "detail": "SARs 5 <= cases 4"}}}
    probs, _ = _aml_batch_gate_problems([_job({"W2": 3}, tm=tm)])
    assert probs == [
        "gold-finalize: cycle 1: workflow invariant sars_le_cases fail: SARs 5 <= cases 4"
    ]


def test_workflow_error_fails_the_run():
    tm = {"1": {"workflow": {"status": "error", "detail": "RuntimeError: no manifest"}}}
    probs, _ = _aml_batch_gate_problems([_job({"W2": 3}, tm=tm)])
    assert len(probs) == 1 and "workflow" in probs[0] and "error" in probs[0]


def test_parsed_log_without_invariants_fails_the_run():
    probs, _ = _aml_batch_gate_problems([_job({"W2": 3}, tm={})])
    assert any("reported no workflow invariants" in p for p in probs)


def test_every_cycle_is_gated_not_only_the_last():
    bad = {"1": {"funnel_monotone": {"status": "fail", "detail": "cases=3 < sars=4"}}}
    probs, _ = _aml_batch_gate_problems([_job({"W2": 3}, tm=bad), _job({"W2": 3})])
    assert probs and probs[0].startswith("gold-finalize 1: cycle 1:")
