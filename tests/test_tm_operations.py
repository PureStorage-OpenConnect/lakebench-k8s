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
    "no_suspect_rate": 0.0,
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
    assert disp[0]["disposition"] == "pending_l1"  # never NULL
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
        "customers": 30,
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
        ({"customers": 0}, "monitored_population"),
        ({"monitored": 0, "excluded": 100}, "monitored_population"),
        ({"null_dispositions": 1}, "no_null_disposition"),
        ({"noncustomer_undeclared": 2, "noncustomer_alerts": 5}, "noncustomer_alerts_declared"),
        ({"reviews_folded_into_determined": 1}, "review_not_folded_into_determined"),
        ({"history_checked": True, "history_changed": 1}, "history_stable"),
    ],
)
def test_each_invariant_fails_when_violated(over, failing):
    inv = {n: s for n, s, _ in tm.evaluate_invariants(_counts(**over))}
    assert inv[failing] == "fail"


def test_add_months_clamps_day():
    assert tm.add_months(date(2024, 3, 31), -1) == date(2024, 2, 29)
    assert tm.add_months(date(2024, 1, 15), -12) == date(2023, 1, 15)
    assert tm.add_months(date(2024, 1, 31), -6) == date(2023, 7, 31)


def test_history_stable_is_only_checked_with_a_previous_cycle():
    names = {n for n, _, _ in tm.evaluate_invariants(_counts())}
    assert "history_stable" not in names
    inv = {n: s for n, s, _ in tm.evaluate_invariants(_counts(history_checked=True))}
    assert inv["history_stable"] == "pass"


# --- 31 CFR 1020.320 and the FinCEN continuing-activity clock ---------------


def test_no_suspect_case_gets_sixty_days_and_files_inside_them():
    _, cases = _run([_alert("a0", 0, True)], as_of_day=400, no_suspect_rate=1.0)
    (case,) = [c for c in cases if c["case_type"] == "alert_escalation"]
    assert case["suspect_identified"] is False
    assert case["regulatory_limit"] == "60_day_no_suspect"
    assert case["filing_deadline_date"] == case["determination_date"] + timedelta(days=60)
    assert 2 <= case["determination_to_filing_days"] <= 58 and case["filed_late"] is False


def test_no_suspect_rate_is_drawn_per_case():
    flags = []
    for cust in range(400):
        _, cases = tm.simulate_customer(
            cust,
            "high",
            [_alert("a0", 0, True)],
            dict(PARAMS, no_suspect_rate=0.2),
            D0 + timedelta(days=400),
        )
        flags += [c["suspect_identified"] for c in cases if c["case_type"] == "alert_escalation"]
    share = flags.count(False) / len(flags)
    assert 0.14 < share < 0.26


def _continuing(cust, late=0.0, weekly=True):
    # True alerts every 7 days for 400 days: continuing activity after each SAR.
    alerts = [_alert(f"k{cust}-{d}", d, True) for d in range(0, 400, 7 if weekly else 60)]
    return tm.simulate_customer(
        cust, "high", alerts, dict(PARAMS, late_filing_rate=late), D0 + timedelta(days=600)
    )


def test_continuing_activity_sar_meets_the_120_day_limit():
    n = 0
    for cust in range(300):
        _, cases = _continuing(cust)
        by = {c["case_id"]: c for c in cases}
        for c in cases:
            if c["case_type"] == "continuing_activity" and c["sar_decision"] == "sar_filed":
                n += 1
                prior = by[c["parent_case_id"]]
                gap = (c["filing_date"] - prior["filing_date"]).days
                assert c["days_since_prior_sar"] == gap
                assert c["filing_deadline_date"] <= prior["filing_date"] + timedelta(days=120)
                # Late only when flagged late: never a silent miss.
                assert gap <= 120 or c["filed_late"] is True, (cust, gap)
    assert n > 200


def test_continuing_activity_filed_past_120_days_is_flagged_late():
    late = 0
    for cust in range(200):
        _, cases = _continuing(cust, late=1.0)
        for c in cases:
            if c["case_type"] == "continuing_activity" and c["sar_decision"] == "sar_filed":
                assert c["filed_late"] is True
                late += c["days_since_prior_sar"] > 120
    assert late > 0


def test_review_is_never_folded_into_an_already_determined_case():
    # A true alert every week keeps a case open or pending filing when most
    # 90-day reviews fall due (the reviewer's probe A2/p2 scenario).
    folded = deferred = 0
    for cust in range(1500):
        alerts = [_alert(f"a{cust}", 0, True, "critical", "W3_round_tripping")]
        alerts += [
            _alert(f"b{cust}-{j}", 60 + 7 * j, True, "critical", "W3_round_tripping")
            for j in range(6)
        ]
        _, cases = tm.simulate_customer(cust, "high", alerts, PARAMS, D0 + timedelta(days=400))
        by = {c["case_id"]: c for c in cases}
        for c in cases:
            st = c["continuing_review_status"]
            if st == "folded":
                folded += 1
                target = by[c["continuing_review_case_id"]]
                det = target["determination_date"]
                assert det is None or det >= c["continuing_review_due_date"]
            elif st == "deferred":
                deferred += 1
            elif st == "opened":
                review = by[c["continuing_review_case_id"]]
                assert review["case_type"] == "continuing_activity"
                assert review["opened_date"] >= c["continuing_review_due_date"]
    assert folded > 0


def test_deferred_review_opens_when_the_blocking_case_files():
    # Find a seed where the review of SAR 1 falls due while case 2 is
    # determined and waiting to file.
    for seed in range(400):
        alerts = [_alert("a", 0, True, "critical")] + [
            _alert(f"b{j}", 60 + 3 * j, True, "critical") for j in range(20)
        ]
        _, cases = tm.simulate_customer(
            1, "high", alerts, dict(PARAMS, seed=seed), D0 + timedelta(days=900)
        )
        by = {c["case_id"]: c for c in cases}
        hits = [
            c
            for c in cases
            if c["continuing_review_status"] == "opened"
            and by[c["continuing_review_case_id"]]["opened_date"] > c["continuing_review_due_date"]
        ]
        if hits:
            c = hits[0]
            review = by[c["continuing_review_case_id"]]
            blocker = [
                x
                for x in cases
                if x["case_id"] != c["case_id"] and x["filing_date"] == review["opened_date"]
            ]
            assert blocker, "a delayed review opens the day the blocking case files"
            return
    pytest.fail("no seed produced a deferred review")


# --- Alert identity across cycles -----------------------------------------

_DAY_US = 86_400 * 1_000_000
_T0 = 1_704_067_200 * 1_000_000  # 2024-01-01T00:00Z in epoch microseconds


def _cur(aid, content, day, rule="W2_structuring", truth=False, prio="high"):
    return {
        "alert_id": aid,
        "alert_key": f"{content}-0",
        "content_hash": content,
        "rule_id": rule,
        "alert_ts": _T0 + day * _DAY_US,
        "generated_date": D0 + timedelta(days=day + 1),
        "truth": truth,
        "triage_priority": prio,
        "priority_score": 4.0,
        "scenario_weight": 2.0,
        "crr_tier": "high",
    }


def _prior(a, cycle=1, as_of=D0 + timedelta(days=200)):
    p = dict(a)
    p["first_seen_cycle"] = cycle
    p["first_seen_as_of"] = as_of
    p["in_current_detection"] = True
    return p


def test_match_keeps_identity_for_same_content_and_for_a_grown_window():
    prev = D0 + timedelta(days=200)
    p1 = _prior(_cur("x1", "c1", 10, truth=True, prio="critical"))
    p2 = _prior(_cur("x2", "c2", 50))
    cur = [
        _cur("y1", "c1", 10),  # identical
        _cur("y2", "c2-grown", 60, truth=True, prio="low"),  # window grew 10 days
    ]
    out = {a["alert_id"]: a for a in tm.match_alerts(cur, [p1, p2], prev, prev, 2)}
    assert out["y1"]["alert_key"] == "c1-0" and out["y1"]["first_seen_cycle"] == 1
    assert out["y1"]["truth"] is True and out["y1"]["triage_priority"] == "critical"
    g = out["y2"]
    assert g["alert_key"] == "c2-0"  # identity as first seen
    assert g["generated_date"] == p2["generated_date"] and g["truth"] is False
    assert g["triage_priority"] == "high" and g["content_hash"] == "c2-grown"


def test_match_carries_withdrawn_alerts_and_dates_backdated_new_ones():
    prev = D0 + timedelta(days=200)
    as_of = D0 + timedelta(days=201)
    p1 = _prior(_cur("x1", "c1", 10))
    cur = [
        _cur("n1", "new-old", 30, rule="W7_cross_border_high_risk"),  # predates the last cycle
        _cur(
            "n2", "new-recent", 200, rule="W8_dormant_reactivation"
        ),  # last payment on the previous as-of date's eve
        _cur("n3", "c1-far", 10 + 40),  # same rule, 40 days later: new activity
    ]
    out = {a["alert_id"]: a for a in tm.match_alerts(cur, [p1], prev, as_of, 2)}
    assert out["x1"]["in_current_detection"] is False and out["x1"]["alert_key"] == "c1-0"
    assert out["n1"]["generated_date"] == as_of and out["n1"]["first_seen_cycle"] == 2
    assert out["n2"]["generated_date"] == as_of
    assert out["n3"]["alert_key"] == "c1-far-0" and out["n3"]["in_current_detection"]


def test_match_never_reuses_a_carried_key():
    prev = D0 + timedelta(days=200)
    p = _prior(_cur("x1", "c1", 10))
    dup = _cur("y2", "c1", 10)
    out = tm.match_alerts([_cur("y1", "c1", 10), dup], [p], prev, prev, 2)
    keys = [a["alert_key"] for a in out]
    assert len(keys) == len(set(keys)) == 2


def test_decided_history_is_reproduced_the_next_cycle():
    """Cycle N decides; cycle N+1 sees more payments (grown windows, a
    withdrawn alert, new and backdated alerts). Every decision dated on or
    before cycle N's as-of date comes out the same."""
    import random

    p = dict(PARAMS, analyst_accuracy=0.8, investigator_accuracy=0.85, qa_sample_rate=0.3)
    changed = compared = 0
    for cust in range(150):
        rnd = random.Random(cust)
        c1 = [
            _cur(
                f"a{i}",
                f"h{cust}-{i}",
                d,
                truth=rnd.random() < 0.4,
                prio=rnd.choice(["low", "high", "critical"]),
            )
            for i, d in enumerate(sorted(rnd.sample(range(0, 300), 12)))
        ]
        as1 = D0 + timedelta(days=302)
        m1 = tm.match_alerts(c1, [], None, as1, 1)
        d1, k1 = tm.simulate_customer(cust, "high", m1, p, as1)
        prior = [dict(a) for a in m1]
        # Next cycle: grow half the windows, drop one, add new ones.
        c2 = []
        for i, a in enumerate(c1[1:]):
            if i % 2:
                c2.append(a)
            else:
                g = dict(
                    a, content_hash=a["content_hash"] + "g", alert_ts=a["alert_ts"] + 3 * _DAY_US
                )
                c2.append(g)
        c2 += [_cur(f"n{j}", f"new{cust}-{j}", 290 + 20 * j, truth=True) for j in range(3)]
        as2 = D0 + timedelta(days=362)
        m2 = tm.match_alerts(c2, prior, as1, as2, 2)
        d2, k2 = tm.simulate_customer(cust, "high", m2, p, as2)
        new = {d["alert_key"]: d for d in d2}
        for d in d1:
            if d["disposition"] in tm.WORKED_DISPOSITIONS:
                compared += 1
                n = new[d["alert_key"]]
                same = n["disposition"] == d["disposition"] and n["case_id"] == d["case_id"]
                if d["decision_date"] is not None:
                    same = same and n["decision_date"] == d["decision_date"]
                changed += not same
        newc = {c["case_id"]: c for c in k2}
        for c in k1:
            n = newc[c["case_id"]]
            if c["determination_date"] is not None:
                compared += 1
                changed += (n["determination"], n["determination_date"]) != (
                    c["determination"],
                    c["determination_date"],
                )
            if c["filing_date"] is not None:
                changed += n["filing_date"] != c["filing_date"]
    assert compared > 500
    assert changed == 0


def test_every_config_field_reaches_the_script(monkeypatch):
    from lakebench.config.schema import TmOperationsConfig

    cfg = TmOperationsConfig(
        enabled=False,
        seed=3,
        no_suspect_rate=0.4,
        max_alerts_per_customer=777,
        continuous_interval_seconds=90,
        counterparty_scenarios=["W4_risk_propagation"],
    )
    for k, v in cfg.env().items():
        monkeypatch.setenv(k, v)
    p = tm.params_from_env()
    assert p["enabled"] is False and p["seed"] == 3 and p["no_suspect_rate"] == 0.4
    assert p["max_alerts_per_customer"] == 777 and p["continuous_interval_seconds"] == 90
    assert p["counterparty_scenarios"] == ("W4_risk_propagation",)
    assert len(cfg.env()) == len(TmOperationsConfig.model_fields)
