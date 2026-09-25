"""The P10 workflow replay (tm_operations.simulate_customer) and invariants.

simulate_customer is plain Python that runs on Spark executors; these tests
drive it directly. The Spark and Iceberg half is in
tests/spark/test_tm_operations_spark.py.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"))

import tm_operations as tm  # noqa: E402

PARAMS = {
    "seed": 7,
    "analyst_accuracy": 1.0,
    "investigator_accuracy": 1.0,
    "qa_sample_rate": 0.0,
    "alert_sla_days": 60,
    "lookback_months": 12,
    "late_filing_rate": 0.0,
}
D0 = date(2024, 1, 1)


def _alert(key, day, truth, priority="high", rule="W2_structuring"):
    return {
        "alert_id": f"id-{key}",
        "alert_key": key,
        "rule_id": rule,
        "generated_date": D0 + timedelta(days=day),
        "truth": truth,
        "triage_priority": priority,
        "priority_score": 4.0,
    }


def _run(alerts, as_of_day=720, **over):
    p = dict(PARAMS, **over)
    return tm.simulate_customer(42, "high", alerts, p, D0 + timedelta(days=as_of_day))


def test_perfect_analyst_escalates_exactly_the_true_alerts():
    # 400 days apart: each SAR's case and its 90-day review are closed
    # before the next alert arrives, so every alert reaches L1.
    alerts = [_alert(f"a{i}", i * 400, truth=(i % 2 == 0)) for i in range(4)]
    disp, cases = _run(alerts, as_of_day=2000)
    by = {d["alert_key"]: d for d in disp}
    assert [by[f"a{i}"]["disposition"] for i in range(4)] == [
        "escalated",
        "closed_nfa",
        "escalated",
        "closed_nfa",
    ]
    alert_cases = [c for c in cases if c["case_type"] == "alert_escalation"]
    assert len(alert_cases) == 2
    assert all(c["sar_decision"] == "sar_filed" for c in alert_cases)


def test_alert_on_customer_with_open_case_attaches_to_it():
    # a0 escalates (L1 within 7 days of day 0); a1 arrives on day 20, while
    # the case is still under investigation (10-60 days), and is suppressed
    # into it rather than triaged again.
    disp, cases = _run([_alert("a0", 0, True), _alert("a1", 20, False)])
    by = {d["alert_key"]: d for d in disp}
    alert_cases = [c for c in cases if c["case_type"] == "alert_escalation"]
    assert len(alert_cases) == 1
    case = alert_cases[0]
    assert by["a0"]["disposition"] == "escalated"
    assert by["a1"]["disposition"] == "attached"
    assert by["a1"]["case_id"] == case["case_id"] == by["a0"]["case_id"]
    assert by["a1"]["l1_decision_date"] is None
    assert case["alert_count"] == 2 and case["escalated_alert_count"] == 1


def test_case_pulls_alerts_still_waiting_for_l1():
    # Low priority waits 3-21 days for L1; a critical alert the next day is
    # decided in 1-3 days and opens the case first, which pulls the waiting
    # low-priority alert in with it.
    alerts = [_alert("slow", 0, False, "low"), _alert("fast", 0, True, "critical")]
    seed = next(
        s
        for s in range(200)
        if tm.draw_int(s, "l1_turnaround", "slow", 3, 21)
        > tm.draw_int(s, "l1_turnaround", "fast", 1, 3)
    )
    disp, cases = _run(alerts, seed=seed)
    by = {d["alert_key"]: d for d in disp}
    assert by["fast"]["disposition"] == "escalated"
    assert by["slow"]["disposition"] == "attached"
    assert len(cases) >= 1 and by["slow"]["case_id"] == cases[0]["case_id"]


def test_at_most_one_open_case_per_customer_at_every_date():
    alerts = [_alert(f"a{i}", i * 3, True, "critical") for i in range(60)]
    _, cases = _run(alerts, as_of_day=400)
    spans = []
    for c in cases:
        end = c["filing_date"] or c["determination_date"]
        if c["sar_decision"] == "no_sar":
            end = c["determination_date"]
        spans.append((c["opened_date"], end or date.max))
    spans.sort()
    for (_, end), (start, _) in zip(spans, spans[1:], strict=False):
        assert start >= end, "a second case opened while one was still open"


def test_sar_clocks_and_filing_deadline():
    disp, cases = _run([_alert("a0", 0, True)], as_of_day=400)
    (case,) = [c for c in cases if c["case_type"] == "alert_escalation"]
    assert case["determination"] == "suspicious"
    det = case["determination_date"]
    assert case["filing_deadline_date"] == det + timedelta(days=30)
    assert 2 <= case["determination_to_filing_days"] <= 28
    assert case["filed_late"] is False
    # Alert-to-decision runs from the alert, not from the case opening.
    assert case["alert_to_decision_days"] == (det - (D0 + timedelta(days=0))).days
    assert disp[0]["decision_date"] == det


def test_no_suspect_gets_sixty_days():
    d = date(2024, 3, 1)
    assert tm.filing_deadline(d, True) == d + timedelta(days=30)
    assert tm.filing_deadline(d, False) == d + timedelta(days=60)


def test_late_filing_rate_produces_late_filings():
    alerts = [_alert("a0", 0, True)]
    _, cases = _run(alerts, as_of_day=400, late_filing_rate=1.0)
    (case,) = [c for c in cases if c["case_type"] == "alert_escalation"]
    assert case["determination_to_filing_days"] >= 31
    assert case["filed_late"] is True


def test_continuing_activity_review_fires_ninety_days_after_sar():
    disp, cases = _run([_alert("a0", 0, True), _alert("later", 300, True)], as_of_day=500)
    parent = [c for c in cases if c["case_type"] == "alert_escalation"][0]
    assert parent["continuing_review_due_date"] == parent["filing_date"] + timedelta(days=90)
    review = [c for c in cases if c["case_type"] == "continuing_activity"]
    assert review, "no continuing-activity review opened"
    assert parent["continuing_review_case_id"] == review[0]["case_id"]
    assert review[0]["opened_date"] == parent["continuing_review_due_date"]
    assert review[0]["parent_case_id"] == parent["case_id"]


def test_review_not_due_yet_is_not_fired():
    _, cases = _run([_alert("a0", 0, True)], as_of_day=100)
    parent = cases[0]
    if parent["filing_date"] is not None:
        assert parent["continuing_review_due_date"] > D0 + timedelta(days=100)
    assert parent["continuing_review_case_id"] is None


def test_decisions_after_as_of_have_not_happened():
    disp, cases = _run([_alert("a0", 0, True)], as_of_day=1)
    assert disp[0]["disposition"] is None
    assert disp[0]["queue_status"] == "open"
    assert disp[0]["aging_days"] == 1
    assert cases == []


def test_replay_is_deterministic_and_order_independent():
    alerts = [_alert(f"a{i}", i * 11, i % 3 == 0) for i in range(30)]
    p = dict(PARAMS, analyst_accuracy=0.7, investigator_accuracy=0.8, qa_sample_rate=0.5)
    as_of = D0 + timedelta(days=500)
    a = tm.simulate_customer(1, "low", alerts, p, as_of)
    b = tm.simulate_customer(1, "low", list(reversed(alerts)), p, as_of)
    key = lambda d: d["alert_key"]  # noqa: E731
    assert sorted(a[0], key=key) == sorted(b[0], key=key)
    assert a[1] == b[1]


def test_imperfect_analyst_errs_at_about_the_stated_rate():
    alerts = [_alert(f"a{i}", i * 400, i % 2 == 0) for i in range(400)]
    disp, _ = tm.simulate_customer(
        1, "low", alerts, dict(PARAMS, analyst_accuracy=0.8), D0 + timedelta(days=400 * 400)
    )
    decided = [d for d in disp if d["analyst_correct"] is not None]
    rate = sum(d["analyst_correct"] for d in decided) / len(decided)
    assert 0.72 < rate < 0.88


def test_qa_sample_and_disagreement():
    alerts = [_alert(f"a{i}", i * 400, i % 2 == 0) for i in range(300)]
    p = dict(PARAMS, analyst_accuracy=0.8, investigator_accuracy=1.0, qa_sample_rate=0.5)
    disp, _ = tm.simulate_customer(1, "low", alerts, p, D0 + timedelta(days=300 * 400))
    sampled = [d for d in disp if d["qa_sampled"]]
    assert 0.35 < len(sampled) / len(disp) < 0.65
    # A perfect QA reviewer disagrees exactly when L1 was wrong.
    for d in sampled:
        assert d["qa_disagrees"] == (not d["analyst_correct"])


def test_triage_priority_is_weight_times_crr():
    assert tm.triage_priority(3.0 * 3.0) == "critical"
    assert tm.triage_priority(2.0 * 3.0) == "critical"
    assert tm.triage_priority(2.0 * 2.0) == "high"
    assert tm.triage_priority(1.0 * 2.0) == "medium"
    assert tm.triage_priority(1.0 * 1.0) == "low"
    assert set(tm.PRIORITY_RANK) == {"low", "medium", "high", "critical"}


def test_split_run_id():
    assert tm.split_run_id("run-abc-c3") == ("run-abc", 3)
    assert tm.split_run_id("run-abc") == ("run-abc", 1)
    assert tm.split_run_id("run-abc", cycle=9) == ("run-abc", 9)


def _counts(**over):
    base = {
        "source": 100,
        "silver": 100,
        "monitored": 60,
        "excluded": 40,
        "alerts": 12,
        "dispositions": 12,
        "customer_alerts": 10,
        "escalated": 4,
        "cases": 4,
        "alert_cases": 3,
        "sars": 2,
        "alert_sars": 2,
        "max_open_per_customer": 1,
        "reviews_due": 1,
        "reviews_missing": 0,
    }
    base.update(over)
    return base


def test_invariants_pass_on_a_consistent_cycle():
    inv = tm.evaluate_invariants(_counts())
    assert {n for n, s, _ in inv if s != "pass"} == set()
    assert {n for n, _, _ in inv} >= {
        "reconciliation",
        "escalated_le_alerts",
        "cases_le_escalated",
        "sars_le_cases",
        "one_open_case_per_customer",
        "funnel_monotone",
        "continuing_review_fires",
        "every_alert_dispositioned",
    }


@pytest.mark.parametrize(
    "over,failing",
    [
        ({"excluded": 39}, "reconciliation"),
        ({"source": None}, "reconciliation"),
        ({"negative_items": ["exclusion.in_flight=-3"]}, "reconciliation"),
        ({"escalated": 11}, "escalated_le_alerts"),
        ({"alert_cases": 5}, "cases_le_escalated"),
        ({"sars": 5}, "sars_le_cases"),
        ({"max_open_per_customer": 2}, "one_open_case_per_customer"),
        ({"monitored": 5, "excluded": 95}, "funnel_monotone"),
        ({"reviews_missing": 1}, "continuing_review_fires"),
        ({"dispositions": 11}, "every_alert_dispositioned"),
    ],
)
def test_each_invariant_fails_when_violated(over, failing):
    inv = {n: s for n, s, _ in tm.evaluate_invariants(_counts(**over))}
    assert inv[failing] == "fail"


def test_add_months_clamps_day():
    assert tm.add_months(date(2024, 3, 31), -1) == date(2024, 2, 29)
    assert tm.add_months(date(2024, 1, 15), -12) == date(2023, 1, 15)
    assert tm.add_months(date(2024, 1, 31), -6) == date(2023, 7, 31)
