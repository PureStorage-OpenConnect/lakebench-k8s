"""RPT-3 C360 results block (DESIGN-v1.7 ch03 section 16).

The block shows ``record.c360_correctness``: a chip with passed/total and
whether the checks gate, failures first with observed, expected and
tolerance, then the passes grouped by family in collapsed lists. Expected
values are read from the record by the test.
"""

from __future__ import annotations

import json
import re
from html import escape

from tests.fixtures.report_goldens import page_text, render
from tests.fixtures.stored_records import load_record
from tests.test_report_consistency import _render_dict


def _plain(html: str) -> str:
    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def _block(html: str) -> str:
    i = html.index("Expected results (Customer 360)")
    return html[i : html.index("</section>", i)]


def test_c360_block_values():
    """5105a0 renders all 34 checks; the chip's numbers are the record's."""
    record = load_record("5105a0")
    c = record["c360_correctness"]
    checks = c["checks"]
    assert len(checks) == 34
    n_pass = sum(1 for x in checks if x["status"] == "pass")
    assert n_pass == c["passed"] == 34
    html = render("5105a0")
    block = _block(html)
    text = _plain(block)
    from lakebench.metrics.c360_correctness import GATING_CHECKS

    # The gate as the verdict applies it now, not the record's pre-approval
    # gating flag (False on this record).
    assert c["gating"] is False and GATING_CHECKS
    assert (
        f"{n_pass}/{len(checks)} checks passed; gate: {len(GATING_CHECKS)} gating checks passed"
        in text
    )
    assert f"Recorded with the run: {c['note']}" in text
    for x in checks:
        assert f"<code class='mono'>{escape(x['id'])}</code>" in block, x["id"]
    for family in ("pipeline", "benchmark shapes", "statistical"):
        assert f"{family} checks that passed" in text
    assert "Not passed" not in text
    # A passed check's observed and expected values, as the record holds them.
    avg = next(x for x in checks if x["id"] == "avg_transaction_value_overall")
    assert re.search(
        rf"avg_transaction_value_overall (?:[^|]*? )?statistical {re.escape(str(avg['observed']))} "
        rf"{re.escape(str(avg['expected']))} ",
        text,
    )


def _with_failures() -> dict:
    record = load_record("5105a0")
    checks = record["c360_correctness"]["checks"]
    stat = next(x for x in checks if x["id"] == "avg_page_views_per_visit")
    stat.update(status="fail", observed=11.2, detail="outside 6 standard errors")
    shape = next(x for x in checks if x["id"] == "benchmark_rows_Q2")
    shape.update(status="unchecked", observed=None, detail="query did not run")
    record["c360_correctness"].update(
        status="fail", failed=["avg_page_views_per_visit"], passed=32, unchecked=1
    )
    return record


def test_c360_failure_listed_first_with_observed_and_expected():
    record = _with_failures()
    checks = record["c360_correctness"]["checks"]
    stat = next(x for x in checks if x["id"] == "avg_page_views_per_visit")
    html = _render_dict(record)
    block = _block(html)
    text = _plain(block)
    assert "32/34 checks passed; gate: " in text
    not_passed = text.index("Not passed")
    assert not_passed < text.index("pipeline checks that passed")
    fail_row = (
        f"avg_page_views_per_visit outside 6 standard errors statistical fail "
        f"{stat['observed']} {stat['expected']} {json.dumps(stat['tolerance'])}"
    )
    assert fail_row in text
    # The failure comes before the unchecked shape, and neither is in the
    # collapsed passes.
    assert text.index("avg_page_views_per_visit") < text.index("benchmark_rows_Q2")
    passes = text[text.index("pipeline checks that passed") :]
    assert "avg_page_views_per_visit" not in passes and "benchmark_rows_Q2 " not in passes


def test_c360_gating_tags_follow_the_verdict_gate():
    from lakebench.metrics.c360_correctness import GATING_CHECKS

    block = _block(render("5105a0"))
    for gid in GATING_CHECKS:
        assert f"<code class='mono'>{gid}</code> <small>(gating)</small>" in block
    assert "<code class='mono'>benchmark_rows_Q2</code> <small>(gating)</small>" not in block


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


def test_c360_no_checks_says_why():
    record = load_record("5105a0")
    record["c360_correctness"].update(checks=[], reason="no [c360-check] line in the driver log")
    text = _plain(_render_dict(record))
    assert "no [c360-check] line in the driver log" in text
    assert "0/0 checks passed; gate: fails the run" in text
    assert "No check ran." in text


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


def test_block_gating_ids_match_gating_outcome_on_every_record():
    """Drift guard for the copied gating rule: on every stored C360 record,
    dropping a GATING_CHECKS id fails gating_outcome exactly when the block
    counts that id as judged."""
    import copy

    from lakebench.metrics.c360_correctness import GATING_CHECKS, gating_outcome
    from lakebench.reports.scorecard import Customer360ScorecardBlock
    from tests.fixtures.stored_records import record_ids

    seen = 0
    for run_id in record_ids():
        rec = load_record(run_id).get("c360_correctness")
        if not isinstance(rec, dict) or gating_outcome(rec)[0] == "FAIL":
            continue
        seen += 1
        judged = Customer360ScorecardBlock.judged_gating_ids(rec)
        for gid in GATING_CHECKS:
            edited = copy.deepcopy(rec)
            edited["checks"] = [c for c in edited["checks"] if c.get("id") != gid]
            assert (gating_outcome(edited)[0] == "FAIL") == (gid in judged), (run_id, gid)
    assert seen >= 5


def test_c360_render_error_shows_a_notice(monkeypatch):
    from lakebench.reports.scorecard import Customer360ScorecardBlock

    def boom(self, record):
        raise ValueError("bad checks\nsecond line")

    monkeypatch.setattr(Customer360ScorecardBlock, "_render", boom)
    text = _plain(_render_dict(load_record("5105a0")))
    assert "C360 results could not be rendered: ValueError: bad checks" in text
    assert "second line" not in text


def test_c360_without_gating_checks_reads_reporting_only(monkeypatch):
    from lakebench.metrics import c360_correctness

    monkeypatch.setattr(c360_correctness, "GATING_CHECKS", frozenset())
    block = _block(_render_dict(load_record("5105a0")))
    assert "34/34 checks passed; gate: reporting only" in _plain(block)
    assert "(gating)" not in block


def test_c360_continuous_reporting_only_judges_nothing():
    from lakebench.reports.scorecard import Customer360ScorecardBlock

    rec = {"reporting_only": True, "mode": "continuous", "checks": []}
    assert Customer360ScorecardBlock.judged_gating_ids(rec) == set()
