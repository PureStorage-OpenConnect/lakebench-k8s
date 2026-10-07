"""RPT-2: every derived number on a report page agrees with the record.

DESIGN-v1.7 ch03 section 15. The renderer wraps each percentage, total and
count it computes in a ``data-lb-derived`` span naming its inputs by record
path (``lakebench.reports.derived``). This test recomputes each one from the
record's metrics.json, independently of the renderer's own arithmetic on the
loaded ``PipelineMetrics``, and compares it with the rendered text at the
rendered precision.

It runs on the six R0 goldens (``tests/fixtures/reports/``) and on a fresh
render of every stored fixture record. The goldens themselves must equal
today's render; there is no update flag (see
``tests/fixtures/report_goldens.py``).
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pytest

from tests.fixtures.report_consistency_helpers import _COUNT as _COUNT
from tests.fixtures.report_consistency_helpers import _FRACTION_UNITS as _FRACTION_UNITS
from tests.fixtures.report_consistency_helpers import _GIB as _GIB
from tests.fixtures.report_consistency_helpers import _HOUR as _HOUR
from tests.fixtures.report_consistency_helpers import _PERCENT_UNITS as _PERCENT_UNITS
from tests.fixtures.report_consistency_helpers import _TOKEN as _TOKEN
from tests.fixtures.report_consistency_helpers import DerivedSpan as DerivedSpan
from tests.fixtures.report_consistency_helpers import PathError as PathError
from tests.fixtures.report_consistency_helpers import _is_number as _is_number
from tests.fixtures.report_consistency_helpers import _num as _num
from tests.fixtures.report_consistency_helpers import _plain_text as _plain_text
from tests.fixtures.report_consistency_helpers import _render_dict as _render_dict
from tests.fixtures.report_consistency_helpers import _SpanParser as _SpanParser
from tests.fixtures.report_consistency_helpers import _split as _split
from tests.fixtures.report_consistency_helpers import _tokens as _tokens
from tests.fixtures.report_consistency_helpers import _unit_key as _unit_key
from tests.fixtures.report_consistency_helpers import derived_spans as derived_spans
from tests.fixtures.report_consistency_helpers import expr_value as expr_value
from tests.fixtures.report_consistency_helpers import formatted as formatted
from tests.fixtures.report_consistency_helpers import mismatches as mismatches
from tests.fixtures.report_consistency_helpers import recompute as recompute
from tests.fixtures.report_consistency_helpers import resolve as resolve
from tests.fixtures.report_consistency_helpers import term_value as term_value
from tests.fixtures.report_consistency_helpers import unit_problems as unit_problems
from tests.fixtures.report_goldens import GOLDEN_RUNS, golden_path, page_text, render
from tests.fixtures.stored_records import load_record, record_ids

# ---------------------------------------------------------------------------
# Page parsing
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Recomputation from the record, by path. Deliberately written apart from
# lakebench.reports.derived: the renderer only names paths; this resolves
# them.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("run_id", record_ids())
def test_render_derived_numbers_agree(run_id):
    """Every stored fixture record, freshly rendered."""
    assert mismatches(load_record(run_id), render(run_id)) == []


def _edit_first(html: str, kind: str, change) -> str:
    pat = re.compile(rf'(<span data-lb-derived="{kind}"[^>]*>)([^<]*)(</span>)')
    m = pat.search(html)
    assert m, f"no {kind} span"
    return html[: m.start(2)] + change(m.group(2)) + html[m.end(2) :]


def _bump(text: str) -> str:
    m = re.search(r"\d+", text)
    assert m
    return text[: m.start()] + str(int(m.group()) + 1) + text[m.end() :]


def test_mismatched_total_fails():
    """A golden whose rendered total is edited by one unit fails."""
    html = golden_path("ebb26f").read_text()
    record = load_record("ebb26f")
    assert mismatches(record, html) == []
    edited = _edit_first(html, "total", _bump)
    bad = mismatches(record, edited)
    assert len(bad) == 1 and "total" in bad[0], bad


def test_mismatched_count_and_pct_fail():
    record = load_record("5105a0")
    html = golden_path("5105a0").read_text()
    assert len(mismatches(record, _edit_first(html, "count", _bump))) == 1
    assert len(mismatches(record, _edit_first(html, "pct", _bump))) == 1


def test_wrong_path_fails():
    """A span naming a path the record does not hold is reported."""
    html = golden_path("5105a0").read_text()
    edited = html.replace(
        'data-lb-inputs="pipeline_benchmark.scores.scale_ratio"',
        'data-lb-inputs="pipeline_benchmark.scores.no_such_ratio"',
        1,
    )
    assert edited != html
    bad = mismatches(load_record("5105a0"), edited)
    assert bad and all("no_such_ratio" in b for b in bad)


def test_wrong_scale_fails():
    """A stored percentage scaled as a fraction fails even though the value
    and the claimed path agree (the test's unit table, not the page's)."""
    html = golden_path("1320bd").read_text()
    edited = html.replace(">+12.1%<", ">+1210.0%<", 1).replace(
        'data-lb-inputs="pipeline_benchmark.scores.maintenance_value_pct" '
        'data-lb-fmt="+.1f" data-lb-suffix="%" data-lb-scale="1"',
        'data-lb-inputs="pipeline_benchmark.scores.maintenance_value_pct" '
        'data-lb-fmt="+.1f" data-lb-suffix="%" data-lb-scale="100"',
        1,
    )
    assert edited != html
    bad = mismatches(load_record("1320bd"), edited)
    assert len(bad) == 1 and "needs 1" in bad[0], bad


def test_record_change_fails():
    """The same page against a record whose input moved is reported."""
    record = load_record("5105a0")
    record["pipeline_benchmark"]["scores"]["scale_ratio"] = 0.5
    bad = mismatches(record, golden_path("5105a0").read_text())
    assert bad and all("scale_ratio" in b for b in bad)


_ALLOWED: dict[str, str] = {
    # The bottleneck shares are kept as floats to pick the dominant stage;
    # the page text renders them through derived.pct.
    'd["weight"] / total_weight * 100': "ordering",
    'd["cpu_sec"] / total_cpu * 100 if total_cpu and d["cpu_sec"] is not None else 0.0': "ordering",
    # A direct unit call of the TM section has no record to name.
    'return f"{float(v) * 100:.1f}%"': "no record path",
}


def test_quoted_rule_key_still_renders_the_scorecard():
    """A continuous record whose ttd_by_rule key holds a quote keeps its
    Detection Scorecard (the old renderer showed it)."""
    r = load_record("ebb26f")
    for s in r["streaming"]:
        if s.get("ttd_by_rule"):
            first = next(iter(s["ttd_by_rule"]))
            s["ttd_by_rule"]['W"9_quoted'] = s["ttd_by_rule"].pop(first)
    html = _render_dict(r)
    assert "Detection Scorecard" in html
    assert mismatches(r, html) == []


class _BareText(HTMLParser):
    """Visible text outside derived spans, style and script."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.inside = 0
        self.skip = 0
        self.text: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in ("style", "script"):
            self.skip += 1
        if self.inside or (tag == "span" and any(k == "data-lb-derived" for k, _ in attrs)):
            self.inside += 1

    def handle_endtag(self, tag):
        if tag in ("style", "script"):
            self.skip -= 1
        if self.inside:
            self.inside -= 1

    def handle_data(self, data):
        if not self.inside and not self.skip and data.strip():
            self.text.append(data.strip())


def _stored_strings(o: Any) -> list[str]:
    if isinstance(o, str):
        return [o]
    if isinstance(o, dict):
        return [s for v in o.values() for s in _stored_strings(v)]
    if isinstance(o, list):
        return [s for v in o for s in _stored_strings(v)]
    return []


_PERCENT = re.compile(r"\d%")


@pytest.mark.parametrize("run_id", record_ids())
def test_every_page_percentage_is_derived(run_id):
    """A percentage in the page text is a derived span, or sits inside a
    string the record stores verbatim (a recorded reason) or a verdict
    reason or warning the front matter repeats from metrics/verdict.py."""
    from lakebench.metrics.verdict import compute_badge_status, compute_verdict
    from tests.fixtures.stored_records import load_metrics

    p = _BareText()
    p.feed(render(run_id))
    stored = _stored_strings(load_record(run_id))
    # The verdict's own reasons and warnings (metrics/verdict.py), which the
    # front matter repeats verbatim.
    m = load_metrics(run_id)
    _ok, reasons, warnings = compute_badge_status(m)
    stored += [*reasons, *warnings, *compute_verdict(m).reasons]
    bare = [t for t in p.text if _PERCENT.search(t)]
    unexplained = [t for t in bare if not any(_PERCENT.search(s) and s in t for s in stored)]
    assert unexplained == []


# ---------------------------------------------------------------------------
# Hand-written expected values (independent of the renderer and of the
# goldens, which the renderer produced)
# ---------------------------------------------------------------------------

_EXPECTED = Path(__file__).parent / "expected" / "report_goldens.json"


def _visible(html: str) -> str:
    text = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text)


@pytest.mark.parametrize("run_id", GOLDEN_RUNS)
def test_golden_shows_expected_values(run_id):
    """The phrases in tests/expected/report_goldens.json, each computed by
    hand from the record, are on today's page."""
    import json

    phrases = json.loads(_EXPECTED.read_text())[run_id]
    assert phrases
    text = _visible(render(run_id))
    missing = [p["text"] for p in phrases if p["text"] not in text]
    assert missing == []


@pytest.mark.parametrize("run_id", GOLDEN_RUNS)
def test_golden_is_clean(run_id):
    """A golden carries no endpoint, credential or protected AML seed: it is
    a tracked file built from a scrubbed record."""
    from lakebench.config import datagen_seed
    from tests.fixtures.scrub import check_clean

    html = golden_path(run_id).read_text()
    assert check_clean({"page": html}) == []
    # Every integer shape (grouped, embedded, hex), hashed: no held-out seed.
    assert datagen_seed.absence_problems({"page": html}) == [], "golden holds a protected seed"


# ---------------------------------------------------------------------------
# Records no fixture reaches, built from fixtures by named edits
# ---------------------------------------------------------------------------


def _q9_contention() -> dict:
    """ebb26f with Q9 contention observed in two of its four rounds."""
    r = load_record("ebb26f")
    for key in ("benchmark_rounds",):
        for rounds in (r["pipeline_benchmark"].get(key) or [], r.get(key) or []):
            for i, rnd in enumerate(rounds):
                meta = rnd.setdefault("round_meta", {})
                meta["q9_contention_observed"] = i in (1, 3)
    return r


def _legacy_no_pipeline_benchmark() -> dict:
    """5105a0 without its pipeline_benchmark block (the pre-scorecard layout)."""
    r = load_record("5105a0")
    del r["pipeline_benchmark"]
    return r


def _with_queries() -> dict:
    """5105a0 with a standalone query list (the Query Performance table)."""
    r = load_record("5105a0")
    r["queries"] = [
        {"query_name": "q1", "elapsed_seconds": 1.25, "rows_returned": 10, "success": True},
        {"query_name": "q2", "elapsed_seconds": 2.5, "rows_returned": 0, "success": False},
        {"query_name": "q3", "elapsed_seconds": 0.75, "rows_returned": 3, "success": True},
    ]
    return r


def _tm_from_jobs() -> dict:
    """1320bd without the run-level tm_operations block, so the TM section
    reads the last gold-finalize job's tm_ops."""
    r = load_record("1320bd")
    r.pop("tm_operations", None)
    assert any(j.get("tm_ops") for j in r["jobs"]), "1320bd jobs carry tm_ops"
    return r


_VARIANTS = {
    "q9_contention": (_q9_contention, "Q9 Contention"),
    "legacy_no_pipeline_benchmark": (_legacy_no_pipeline_benchmark, "Jobs"),
    "with_queries": (_with_queries, "Query Performance"),
    "tm_from_jobs": (_tm_from_jobs, "L1 escalation rate"),
}


@pytest.mark.parametrize("name", sorted(_VARIANTS))
def test_variant_derived_numbers_agree(name):
    build, marker = _VARIANTS[name]
    record = build()
    html = _render_dict(record)
    assert marker in html
    assert derived_spans(html)
    assert mismatches(record, html) == []
