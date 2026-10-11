"""AML batch runs must not report success when detection measured nothing
(2026-09-24 audit: zero alerts or every rule crashed still exited 0)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

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


def test_missing_logs_without_an_alert_total_is_a_warning_not_a_failure():
    probs, warns = _aml_batch_gate_problems([_job({})], {"status": "scored"})
    assert probs == [] and warns


@pytest.mark.parametrize(
    "scoring", [None, {"mode": "batch", "status": "not_scored", "reason": "refused"}]
)
def test_scoring_without_a_result_fails(scoring):
    """A batch run whose score job failed or refused the corpus has
    unchecked answers: it fails, with the reason."""
    probs, _ = _aml_batch_gate_problems([_job({"W2": 3})], scoring)
    assert len(probs) == 1 and "AML scoring did not produce a result" in probs[0]


def test_crashed_rule_fails_even_with_other_alerts():
    probs, _ = _aml_batch_gate_problems(
        [_job({"W2": 5, "W7": 0}, {"W7": "AnalysisException"})], {"total_alerts": 5}
    )
    assert probs == ["Detection rule W7 failed: AnalysisException"]


def test_healthy_run_passes_and_last_cycle_wins():
    bad_then_good = [_job({"W2": 0}), _job({"W2": 3})]
    assert _aml_batch_gate_problems(bad_then_good, {"total_alerts": 3}) == ([], [])


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
    problems, warnings = _aml_batch_gate_problems([job], {"total_alerts": 5})
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
    probs, _ = _aml_batch_gate_problems([_job({"W2": 3}, tm=tm)], {"total_alerts": 3})
    assert probs == []


def _tm_case(name):
    ok = {"1": {"reconciliation": {"status": "pass", "detail": ""}}}
    ok3 = {"3": {"reconciliation": {"status": "pass", "detail": ""}}}
    fail1 = {"1": {"sars_le_cases": {"status": "fail", "detail": "SARs 5 <= cases 4"}}}
    return {
        "invariant-fails": ([_tjob({"W2": 3}, tm=fail1)], True),
        "manifest-missing": (
            [
                _tjob(
                    {"W2": 3},
                    tm={},
                    status={
                        "1": {
                            "status": "not_run",
                            "reason": "no ground-truth manifest (bronze.manifest)",
                        }
                    },
                )
            ],
            True,
        ),
        "log-without-tm-lines": ([_tjob({"W2": 3}, tm={})], True),
        "no-log-at-all": ([_tjob({}, tm={})], True),
        "disabled-with-failures": (
            [_tjob({"W2": 3}, tm={"1": {"sars_le_cases": {"status": "fail", "detail": "x"}}})],
            False,
        ),
        "first-cycle-fails": (
            [
                _tjob(
                    {"W2": 3},
                    tm={"1": {"funnel_monotone": {"status": "fail", "detail": "cases=3 < sars=4"}}},
                ),
                _tjob({"W2": 3}, tm={"2": {"funnel_monotone": {"status": "pass", "detail": ""}}}),
            ],
            True,
        ),
        "one-cycle-not-run": (
            [
                _tjob({"W2": 3}),
                _tjob(
                    {"W2": 3}, tm={}, status={"2": {"status": "not_run", "reason": "error: boom"}}
                ),
            ],
            True,
        ),
        "a-cycle-without-a-log": (
            [_tjob({"W2": 3}, tm=ok), _tjob({}, tm={}), _tjob({"W2": 3}, tm=ok3)],
            True,
        ),
        "all-pass": ([_tjob({"W2": 3}, ops={"funnel": {}})], True),
    }[name]


@pytest.mark.parametrize(
    ("case", "status", "problems"),
    [
        ("invariant-fails", "fail", 1),
        ("first-cycle-fails", "fail", 1),  # every cycle is gated, not only the last
        ("manifest-missing", "not_run", 0),  # not a failure
        ("log-without-tm-lines", "not_run", None),
        ("one-cycle-not-run", "not_run", None),  # reported even when others pass
        ("no-log-at-all", "unknown", 0),
        ("a-cycle-without-a-log", "unknown", None),  # unknown, never pass
        ("disabled-with-failures", "disabled", 0),
        ("all-pass", "pass", None),
    ],
)
def test_aml_tm_verdict(case, status, problems):
    jobs, enabled = _tm_case(case)
    v = _tm(jobs) if enabled else _tm(jobs, enabled=False)
    assert v["status"] == status
    if problems is not None:
        assert len(v["problems"]) == problems
    if case == "first-cycle-fails":
        assert v["problems"][0].startswith("gold-finalize: cycle 1:")
    if case == "all-pass":
        assert v["ops"] == {"funnel": {}} and v["mode"] == "batch"


def test_scoring_count_line_never_calls_every_typology_scored():
    """Live s1 run 2026-09-26 said '15 typologies scored' when 6 were."""
    from lakebench.cli._run import scoring_count_line

    typs = [{"typology_type": f"t{i}", "detection_status": "scored"} for i in range(6)]
    typs += [{"typology_type": f"n{i}", "detection_status": "no_rule"} for i in range(8)]
    typs += [{"typology_type": "gather_scatter", "detection_status": "rule_skipped"}]
    line = scoring_count_line({"typologies": typs})
    assert line == "6 of 15 typologies scored; 8 no rule, 1 rule skipped"
    counts = {"scored": 6, "no_rule": 8, "rule_skipped": 1}
    assert scoring_count_line({"typologies": typs, "typology_counts": counts}) == line
