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


# P10: the TM operations verdict is separate from detection. Only violated
# invariants fail the run; a layer that could not run is "not run".


def _tm(jobs, enabled=True):
    from lakebench.cli._run import _aml_tm_verdict

    return _aml_tm_verdict(jobs, enabled=enabled)


def _tjob(alerts=None, tm=_PASS, status=None, ops=None):
    return SimpleNamespace(
        alerts_by_rule=alerts or {},
        rule_errors={},
        rules_skipped={},
        tm_invariants=tm,
        tm_status=status or {},
        tm_ops=ops,
    )


def test_tm_is_not_part_of_the_detection_gate():
    tm = {"1": {"sars_le_cases": {"status": "fail", "detail": "SARs 5 <= cases 4"}}}
    probs, _ = _aml_batch_gate_problems([_job({"W2": 3}, tm=tm)])
    assert probs == []


def test_failed_workflow_invariant_fails_the_verdict():
    tm = {"1": {"sars_le_cases": {"status": "fail", "detail": "SARs 5 <= cases 4"}}}
    v = _tm([_tjob({"W2": 3}, tm=tm)])
    assert v["status"] == "fail"
    assert v["problems"] == [
        "gold-finalize: cycle 1: workflow invariant sars_le_cases fail: SARs 5 <= cases 4"
    ]


def test_missing_manifest_is_not_run_not_a_failure():
    st = {"1": {"status": "not_run", "reason": "no ground-truth manifest (bronze.manifest)"}}
    v = _tm([_tjob({"W2": 3}, tm={}, status=st)])
    assert v["status"] == "not_run" and not v["problems"]
    assert "manifest" in v["reason"]


def test_parsed_log_without_tm_lines_is_not_run():
    v = _tm([_tjob({"W2": 3}, tm={})])
    assert v["status"] == "not_run" and "no TM lines" in v["reason"]


def test_no_log_at_all_is_unknown():
    v = _tm([_tjob({}, tm={})])
    assert v["status"] == "unknown" and not v["problems"]


def test_disabled_skips_the_gate_even_with_failures():
    tm = {"1": {"sars_le_cases": {"status": "fail", "detail": "x"}}}
    v = _tm([_tjob({"W2": 3}, tm=tm)], enabled=False)
    assert v["status"] == "disabled" and not v["problems"]


def test_every_cycle_is_gated_not_only_the_last():
    bad = {"1": {"funnel_monotone": {"status": "fail", "detail": "cases=3 < sars=4"}}}
    good = {"2": {"funnel_monotone": {"status": "pass", "detail": ""}}}
    v = _tm([_tjob({"W2": 3}, tm=bad), _tjob({"W2": 3}, tm=good)])
    assert v["status"] == "fail" and v["problems"][0].startswith("gold-finalize: cycle 1:")


def test_one_cycle_not_run_is_reported_even_when_others_pass():
    st = {"2": {"status": "not_run", "reason": "error: boom"}}
    v = _tm([_tjob({"W2": 3}), _tjob({"W2": 3}, tm={}, status=st)])
    assert (
        v["status"] == "not_run" and "cycle 2" in v["reason"] and "ran on cycles [1]" in v["reason"]
    )


def test_all_pass():
    v = _tm([_tjob({"W2": 3}, ops={"funnel": {}})])
    assert v["status"] == "pass" and v["ops"] == {"funnel": {}} and v["mode"] == "batch"
