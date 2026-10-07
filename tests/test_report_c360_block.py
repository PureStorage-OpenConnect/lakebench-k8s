"""RPT-3 C360 results block (DESIGN-v1.7 ch03 section 16).

The block shows ``record.c360_correctness``: a chip with passed/total and
whether the checks gate, failures first with observed, expected and
tolerance, then the passes grouped by family in collapsed lists. Expected
values are read from the record by the test.
"""

from __future__ import annotations

import re

from tests.fixtures.report_consistency_helpers import _render_dict
from tests.fixtures.report_goldens import page_text
from tests.fixtures.stored_records import load_record


def _plain(html: str) -> str:
    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def _block(html: str) -> str:
    i = html.index("Expected results (Customer 360)")
    return html[i : html.index("</section>", i)]


def test_c360_chip_agrees_with_the_verdict_on_a_failing_gated_check():
    """A pre-approval record (gating False) whose gated check failed: the
    verdict's c360 gate fails, and the chip says so, listing it first."""
    from lakebench.metrics.c360_correctness import gating_outcome

    record = load_record("5105a0")
    c = record["c360_correctness"]
    stat = next(x for x in c["checks"] if x["id"] == "avg_page_views_per_visit")
    stat["status"] = "fail"
    gated = next(x for x in c["checks"] if x["id"] == "bronze_to_silver_rows")
    gated["status"] = "fail"
    outcome, why = gating_outcome(c)
    assert outcome == "FAIL" and "bronze_to_silver_rows" in why
    text = _plain(_block(_render_dict(record)))
    assert f"gate: fails the run: {why}" in text
    rows = text[text.index("Not passed") :]
    assert rows.index("bronze_to_silver_rows") < rows.index("avg_page_views_per_visit")


def test_c360_absent_gated_check_is_listed_not_evaluated():
    from lakebench.metrics.c360_correctness import gating_outcome

    record = load_record("5105a0")
    c = record["c360_correctness"]
    c["checks"] = [x for x in c["checks"] if x["id"] != "avg_transaction_value_overall"]
    assert gating_outcome(c)[0] == "FAIL"
    text = _plain(_block(_render_dict(record)))
    assert "33/33 checks passed; gate: fails the run" in text
    assert "avg_transaction_value_overall (gating) a gating check absent from the record" in text
    assert "not evaluated" in text


def test_c360_block_absent_without_a_record():
    record = load_record("5105a0")
    del record["c360_correctness"]
    assert "Expected results (Customer 360)" not in _render_dict(record)


def test_c360_malformed_record_never_breaks_the_page():
    record = load_record("5105a0")
    record["c360_correctness"]["checks"] = [{"id": object.__name__, "kind": None}, "junk"]
    html = _render_dict(record)
    assert "Bottleneck Identification" in html
    assert "Expected results (Customer 360)" in html or "C360 results could not be rendered" in html


def test_c360_block_judges_the_ids_c360_correctness_judges(monkeypatch):
    """The block takes its gated set from c360_correctness.judged_gating_ids
    (one rule with the verdict's gate), not a copy: an id that function adds
    is tagged and, absent from the record, listed as not evaluated."""
    from lakebench.metrics import c360_correctness

    real = c360_correctness.judged_gating_ids
    monkeypatch.setattr(
        c360_correctness, "judged_gating_ids", lambda rec: real(rec) | {"probe_gate_id"}
    )
    block = _block(_render_dict(load_record("5105a0")))
    # The row, not only the chip's reason (gating_outcome names the id too).
    assert "<code class='mono'>probe_gate_id</code> <small>(gating)</small>" in block
    assert "<td>not evaluated</td>" in block
