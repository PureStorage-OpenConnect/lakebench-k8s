"""AML batch runs must not report success when detection measured nothing
(2026-09-24 audit: zero alerts or every rule crashed still exited 0)."""

from __future__ import annotations

from types import SimpleNamespace

from lakebench.cli._run import _aml_batch_gate_problems


def _job(alerts=None, errors=None):
    return SimpleNamespace(alerts_by_rule=alerts or {}, rule_errors=errors or {})


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
