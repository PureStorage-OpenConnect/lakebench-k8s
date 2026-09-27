"""AML scorecard rows state what happened to each rule.

A crashed rule reads "error", not "ran, 0 alerts"; a typology the scorer
marks rule_error or partial reads that status. These are the rows a reader
uses to tell a rule that found nothing from a rule that did not work.
"""

from __future__ import annotations

from types import SimpleNamespace

from lakebench.benchmark.aml_queries import RULE_TARGETS
from lakebench.reports.scorecard import get_scorecard_block


def _metrics(*, rule_errors=None, alerts=None, typologies=None):
    job = SimpleNamespace(
        job_type="gold-finalize",
        alerts_by_rule=alerts or {},
        rules_skipped={},
        rule_errors=rule_errors or {},
    )
    scoring = {"typologies": typologies or [], "total_alerts": 10, "fp_rate": 0.5}
    return SimpleNamespace(jobs=[job], streaming=[], financial_scoring=scoring)


def _row(html: str, rule: str) -> str:
    start = html.index(rule)
    return html[start : html.index("</tr>", start)]


def test_crashed_rule_reads_error_not_zero_alerts():
    rule = next(iter(RULE_TARGETS))
    html = get_scorecard_block("financial").render_detail_html(
        _metrics(rule_errors={rule: "boom"}, alerts={})
    )
    row = _row(html, rule)
    assert "error" in row
    assert ">0<" not in row


def test_scorer_rule_error_and_partial_statuses_are_shown():
    rules = list(RULE_TARGETS)
    err_rule, part_rule = rules[0], rules[1]
    typologies = [
        {"typology_type": RULE_TARGETS[err_rule], "detection_status": "rule_error"},
        {
            "typology_type": RULE_TARGETS[part_rule],
            "detection_status": "partial",
            "recall": 0.25,
        },
    ]
    html = get_scorecard_block("financial").render_detail_html(
        _metrics(alerts={err_rule: 3, part_rule: 4}, typologies=typologies)
    )
    assert "error" in _row(html, err_rule)
    part = _row(html, part_rule)
    assert "partial" in part and "25.0%" in part
