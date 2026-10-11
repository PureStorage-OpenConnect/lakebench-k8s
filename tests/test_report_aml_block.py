"""RPT-3 AML results block, and the cap label
on the AML totals (QUEUE carry from ER-3, invariant 6).

Expected values are read from the record by the test. The per-reason-code
and covered-recall tables are tested on fixture records edited to carry the
AM-9 and AM-11 fields (``recall_by_code`` /
``fp_by_code``: ``{rule: {code: fraction}}``; ``mode: "covered"`` with
``covered.typologies[]``), since no stored record holds them yet.
"""

from __future__ import annotations

import re
from html import unescape

from tests.fixtures.report_consistency_helpers import _render_dict, mismatches
from tests.fixtures.report_goldens import page_text, render
from tests.fixtures.stored_records import load_record


def _plain(html: str) -> str:
    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", html)))


def _funnel(text: str) -> str:
    i = text.index("AML results funnel")
    return text[i : text.index("Transaction Monitoring Operations", i)]


def test_aml_block_values():
    """1320bd: funnel counts, their sources and the per-rule recall and
    chance equal the record."""
    record = load_record("1320bd")
    ops = record["tm_operations"]["ops"]
    scoring = record["financial_scoring"]
    text = _plain(render("1320bd"))
    funnel = _funnel(text)
    assert (
        f"Rule alerts (scoring) {scoring['total_alerts']:,} financial_scoring.total_alerts"
        in funnel
    )
    assert (
        f"Rule alerts in gold.alerts (TM) {ops['alerts_total']:,} tm_operations.ops.alerts_total"
        in funnel
    )
    assert f"dispositioned on customers (monitored) {ops['funnel']['alerts']:,}" in funnel
    for k, v in ops["alerts_by_disposition"].items():
        assert f"disposition: {k} {v:,} tm_operations.ops.alerts_by_disposition.{k}" in funnel
    assert f"SARs filed {ops['sars_filed']:,} tm_operations.ops.sars_filed" in funnel
    per = ops["funnel"]["alerts"] / ops["reconciliation"]["completeness.customers"]
    assert f"customer alerts per customer {per:.2f}" in funnel
    # Reconciliation labels.
    assert "scoring and TM rule alerts agree" in funnel
    assert (
        "TM rule alerts = customer + non-customer dispositions - withdrawn carried alerts" in funnel
    )
    assert "dispositions sum to customer + non-customer dispositions" in funnel
    cont = ops["reconciliation"]["funnel.continuing_sars"]
    assert f"SARs filed = {ops['funnel']['sars']:,} on alert cases + {cont:,}" in funnel
    # Per-rule recall with chance beside it.
    recall = next(t for t in scoring["typologies"] if t["typology_type"] == "rapid_layering")
    chance = scoring["chance_by_rule"]["W4_risk_propagation"]
    assert (
        f"W4_risk_propagation rapid_layering 98,479 {recall['recall'] * 100:.1f}% "
        f"{chance * 100:.1f}%" in text
    )


def test_funnel_names_a_difference_it_cannot_explain():
    record = load_record("1320bd")
    record["financial_scoring"]["total_alerts"] += 10
    text = _plain(_render_dict(record))
    assert "scoring rule alerts differ from TM's by +10; the record does not say why" in text


def test_reason_code_table():
    record = load_record("1320bd")
    record["financial_scoring"]["recall_by_code"] = {
        "W2_structuring": {"W2.base": 0.099, "W2.split": 0.05}
    }
    record["financial_scoring"]["fp_by_code"] = {"W2_structuring": {"W2.base": 0.993}}
    html = _render_dict(record)
    text = _plain(html)
    assert "By reason code" in text
    assert "W2_structuring W2.base 9.9% 99.3%" in text
    assert "W2_structuring W2.split 5.0% -" in text
    assert mismatches(record, html) == []


def _covered(status: str = "scored") -> dict:
    record = load_record("ebb26f")
    record["financial_scoring"] = {
        "mode": "covered",
        "status": status,
        "reason": "snapshot silver.transactions expired before scoring"
        if status == "not_scored"
        else None,
        "covered": {
            "typologies": [
                {
                    "typology_type": "rapid_layering",
                    "recall_covered": 0.612,
                    "covered_instances": 300,
                    "corpus_instances": 889,
                    "coverage": 0.3375,
                    "no_participant_txns": 0,
                }
            ],
            "excluded_typologies": {},
        },
        "chance_by_rule": {"W4_risk_propagation": 0.05},
    }
    return record


def test_continuous_recall_covered_with_coverage():
    record = _covered()
    html = _render_dict(record)
    text = _plain(html)
    assert "Recall over covered instances (uncalibrated, in-sample)" in text
    assert "rapid_layering 201,503 61.2% covered, coverage 33.8% 5.0%" in text
    assert "Chance (covered)" in text and "Off-target (covered)" in text
    assert "Recall (uncalibrated, in-sample)" not in text
    assert mismatches(record, html) == []


def test_totals_labelled_when_a_rule_skipped_on_a_cap():
    """W1 skipped on vertex-cap (a Lakebench cap the verdict lists in
    rule_caps): the funnel, Total alerts and the off-target rate carry the
    cap; the skip reason names it."""
    record = load_record("1320bd")
    gold = next(j for j in record["jobs"] if j["job_type"] == "gold-finalize")
    gold["rules_skipped"]["W1_connected_components"] = "vertex-cap"
    html = _render_dict(record)
    text = _plain(html)
    total = record["financial_scoring"]["total_alerts"]
    assert (
        f"Total alerts over the rules that ran (W1_connected_components skipped: vertex-cap): "
        f"{total:,} BOUNDED BY: rule W1_connected_components cap" in text
    )
    assert re.search(r"Overall off-target rate .*?: \d+\.\d% BOUNDED BY: rule W1", text)
    funnel = _funnel(text)
    assert f"Rule alerts (scoring) {total:,} BOUNDED BY: rule W1_connected_components cap" in funnel
    assert "Every count here comes from the rules that ran" in funnel


def test_totals_not_capped_on_a_data_skip():
    """giant-component is not a Lakebench cap: the totals say they cover
    the rules that ran, with no BOUNDED BY."""
    text = _plain(render("1320bd"))
    i = text.index("Total alerts")
    line = text[i : text.index("Subject customer check", i)]
    assert "over the rules that ran (W1_connected_components skipped: giant-component)" in line
    assert "BOUNDED BY" not in line


def test_nested_counts_and_withdrawn_alerts_reconcile():
    """tm_operations: undeclared non-customers are inside the non-customer
    count, over-capacity alerts inside the customer count, and withdrawn
    carried alerts are dispositioned but not in gold.alerts. A record with
    all three non-zero still reconciles."""
    record = load_record("1320bd")
    ops = record["tm_operations"]["ops"]
    ops["alerts_noncustomer_undeclared"] = 5
    ops["alerts_over_capacity"] = 50
    ops["alerts_withdrawn_carried"] = 100
    ops["funnel"]["alerts"] += 100
    ops["alerts_by_disposition"]["closed_nfa"] += 100
    text = _plain(_render_dict(record))
    funnel = _funnel(text)
    assert "of which not declared as counterparties 5" in funnel
    assert "of which over the per-customer cap (held back, not worked) 50 " in funnel
    assert "of which withdrawn, carried from an earlier cycle 100" in funnel
    # The cap bounds what was worked after it.
    sars = ops["sars_filed"]
    assert f"SARs filed {sars:,} BOUNDED BY: tm_max_alerts_per_customer" in funnel
    assert "TM rule alerts = customer + non-customer dispositions - withdrawn" in funnel
    assert "dispositions sum to customer + non-customer dispositions" in funnel
    assert "differ" not in funnel


def test_any_cap_skip_is_labelled_even_outside_the_allowed_set():
    """A skip reason naming a cap labels the totals whether or not the
    verdict allows it (metrics/bounds.py's rule)."""
    record = load_record("1320bd")
    gold = next(j for j in record["jobs"] if j["job_type"] == "gold-finalize")
    gold["rules_skipped"]["W2_structuring"] = "edge-cap"
    gold["alerts_by_rule"].pop("W2_structuring", None)
    text = _plain(_render_dict(record))
    assert "BOUNDED BY: rule W2_structuring cap" in text


def test_continuous_totals_name_their_scope():
    record = _covered()
    record["financial_scoring"].update(total_alerts=39075, fp_rate=0.9)
    text = _plain(_render_dict(record))
    assert "Total alerts over the rules continuous mode runs: 39,075" in text
    assert "Overall off-target rate (covered)" in text
