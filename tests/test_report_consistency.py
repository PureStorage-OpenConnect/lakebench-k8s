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
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pytest

from tests.fixtures.report_goldens import GOLDEN_RUNS, golden_path, page_text, render
from tests.fixtures.stored_records import load_record, record_ids

# ---------------------------------------------------------------------------
# Page parsing
# ---------------------------------------------------------------------------


@dataclass
class DerivedSpan:
    kind: str
    inputs: str
    fmt: str
    suffix: str
    scale: float | None
    text: str


class _SpanParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.spans: list[DerivedSpan] = []
        self._open: dict[str, Any] | None = None
        self._depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = {k: v or "" for k, v in attrs}
        if self._open is not None:
            self._depth += 1
            if "data-lb-derived" in a:
                raise AssertionError("a derived span inside a derived span")
            return
        if tag == "span" and "data-lb-derived" in a:
            self._open = {**a, "_text": ""}
            self._depth = 0

    def handle_endtag(self, tag: str) -> None:
        if self._open is None:
            return
        if self._depth:
            self._depth -= 1
            return
        a = self._open
        self._open = None
        self.spans.append(
            DerivedSpan(
                kind=a["data-lb-derived"],
                inputs=a.get("data-lb-inputs", ""),
                fmt=a.get("data-lb-fmt", ""),
                suffix=a.get("data-lb-suffix", ""),
                scale=float(a["data-lb-scale"]) if a.get("data-lb-scale") else None,
                text=a["_text"],
            )
        )

    def handle_data(self, data: str) -> None:
        if self._open is not None:
            self._open["_text"] += data


def derived_spans(html: str) -> list[DerivedSpan]:
    p = _SpanParser()
    p.feed(html)
    p.close()
    return p.spans


# ---------------------------------------------------------------------------
# Recomputation from the record, by path. Deliberately written apart from
# lakebench.reports.derived: the renderer only names paths; this resolves
# them.
# ---------------------------------------------------------------------------


class PathError(KeyError):
    pass


def _split(s: str, sep: str) -> list[str]:
    """Split on *sep* outside brackets, parentheses and quotes."""
    out, cur, depth, quote = [], "", 0, False
    for ch in s:
        if ch == '"':
            quote = not quote
        elif not quote and ch in "[(":
            depth += 1
        elif not quote and ch in "])":
            depth -= 1
        if ch == sep and depth == 0 and not quote:
            out.append(cur)
            cur = ""
        else:
            cur += ch
    out.append(cur)
    return out


_TOKEN = re.compile(r'\.?([A-Za-z_][A-Za-z0-9_\-]*)|\[(\d+|\*|"[^"]*")\]')


def _tokens(path: str) -> list[tuple[str, Any]]:
    toks: list[tuple[str, Any]] = []
    pos = 0
    while pos < len(path):
        m = _TOKEN.match(path, pos)
        if not m or m.end() == pos:
            raise PathError(f"bad path {path!r} at {pos}")
        if m.group(1) is not None:
            toks.append(("key", m.group(1)))
        elif m.group(2) == "*":
            toks.append(("all", None))
        elif m.group(2).startswith('"'):
            toks.append(("key", m.group(2)[1:-1]))
        else:
            toks.append(("idx", int(m.group(2))))
        pos = m.end()
    return toks


def resolve(record: Any, path: str, *, lenient: bool = False) -> list[Any]:
    """Every value *path* names. A missing key raises, except that with
    *lenient* an element of a ``[*]`` expansion without it is skipped."""
    values = [(record, False)]
    for kind, arg in _tokens(path):
        nxt = []
        for v, expanded in values:
            if kind == "all":
                if not isinstance(v, list):
                    raise PathError(f"{path}: [*] over {type(v).__name__}")
                nxt.extend((x, True) for x in v)
            elif kind == "idx":
                if not isinstance(v, list) or arg >= len(v):
                    raise PathError(f"{path}: no index {arg}")
                nxt.append((v[arg], expanded))
            else:
                if isinstance(v, dict) and arg in v:
                    nxt.append((v[arg], expanded))
                elif lenient and expanded:
                    continue
                else:
                    raise PathError(f"{path}: no key {arg!r}")
        values = nxt
    return [v for v, _ in values]


def _num(v: Any, path: str) -> float:
    if isinstance(v, bool) or v is None:
        raise PathError(f"{path}: {v!r} is not a number")
    return float(v)


_COUNT = re.compile(r"^(count|truthy|falsy|positive)\((.*)\)$")


def term_value(record: Any, term: str) -> float:
    m = _COUNT.match(term)
    if m:
        pred, p = m.groups()
        vals = resolve(record, p, lenient=True)
        if pred == "count":
            if "[*]" not in p and len(vals) == 1 and isinstance(vals[0], (list, dict)):
                return float(len(vals[0]))
            return float(len(vals))
        if pred == "truthy":
            return float(sum(1 for v in vals if v))
        if pred == "falsy":
            return float(sum(1 for v in vals if not v))
        return float(
            sum(
                1 for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0
            )
        )
    value = 1.0
    for factor in _split(term, "*"):
        if factor.startswith("/"):
            value /= float(factor[1:])
            continue
        try:
            value *= float(factor)
            continue
        except ValueError:
            pass
        vals = resolve(record, factor)
        if not vals:
            raise PathError(f"{factor}: no values")
        value *= sum(_num(v, factor) for v in vals)
    return value


def expr_value(record: Any, expression: str) -> float:
    return sum(term_value(record, t) for t in _split(expression, "|"))


def recompute(record: Any, span: DerivedSpan) -> float:
    if span.kind in ("pct", "ratio") and span.scale is None:
        parts = _split(span.inputs, ";")
        if len(parts) != 2:
            raise AssertionError(f"{span.kind} needs num;den, got {span.inputs!r}")
        num, den = (expr_value(record, p) for p in parts)
        if den == 0:
            raise AssertionError(f"{span.inputs}: denominator is 0 in the record")
        return num / den * (100.0 if span.kind == "pct" else 1.0)
    if span.kind == "pct":
        return expr_value(record, span.inputs) * (span.scale or 1.0)
    if span.kind in ("total", "count"):
        return expr_value(record, span.inputs)
    raise AssertionError(f"unknown derived kind {span.kind!r}")


def formatted(value: float, fmt: str, suffix: str) -> str:
    if fmt.endswith("d"):
        return format(int(round(value)), fmt) + suffix
    return format(value, fmt) + suffix


# Units the test knows on its own, so a value and a path that are wrong the
# same way (a percent field scaled as a fraction, bytes shown as GiB without
# the conversion) still fail. A stored percentage is read with scale 1, a
# stored fraction with scale 100; a field in neither set fails until it is
# added here with its unit.
_PERCENT_UNITS = frozenset(
    {"maintenance_value_pct", "qph_degradation_pct", "maintenance_pct_of_pipeline"}
)
_FRACTION_UNITS = frozenset(
    {
        "scale_ratio",
        "ingest_ratio",
        "corpus_ingest_ratio",
        "fp_rate",
        "fp_rate_by_rule",
        "chance_by_rule",
        "txn_precision_by_rule",
        "recall",
        "incidental_recall",
        "l1_escalation_rate",
        "qa_disagreement_rate",
        "productive_rate",
        "sar_conversion",
        # A fraction despite its name (tm_operations writes count / total).
        "filed_over_30_days_pct",
        # Continuous covered-mode scoring and per-reason-code figures.
        "recall_covered",
        "coverage",
        "recall_by_code",
        "fp_by_code",
    }
)
_GIB = "/1073741824"
_HOUR = "/3600"


def _unit_key(path: str) -> str | None:
    for kind, arg in reversed(_tokens(path)):
        if kind == "key" and (arg in _PERCENT_UNITS or arg in _FRACTION_UNITS):
            return arg
    return None


def unit_problems(span: DerivedSpan) -> list[str]:
    out = []
    if span.kind == "pct" and span.scale is not None:
        key = _unit_key(span.inputs)
        if key is None:
            out.append(f"pct {span.inputs!r}: field unit unknown to the test")
        else:
            want = 1.0 if key in _PERCENT_UNITS else 100.0
            if span.scale != want:
                out.append(f"pct {span.inputs!r}: scale {span.scale:g}, {key} needs {want:g}")
    terms = [t for part in _split(span.inputs, ";") for t in _split(part, "|")]
    for t in terms:
        factors = _split(t, "*")
        paths = [f for f in factors if not f.startswith("/") and not _is_number(f)]
        if _GIB in factors and not any(f.endswith("_bytes") for f in paths):
            out.append(f"{span.inputs!r}: GiB conversion on a field that is not bytes")
        if _HOUR in factors and not any(f.endswith("_seconds") for f in paths):
            out.append(f"{span.inputs!r}: hour conversion on a field that is not seconds")
        if span.suffix.strip() == "GiB" and _GIB not in factors:
            out.append(f"{span.inputs!r}: shown in GiB without the byte conversion")
    return out


def _is_number(s: str) -> bool:
    try:
        float(s)
    except ValueError:
        return False
    return True


def mismatches(record: dict, html: str) -> list[str]:
    """Each derived span whose text differs from the value recomputed from
    *record*, whose inputs do not resolve, or whose units the test's own
    table contradicts."""
    bad = []
    for span in derived_spans(html):
        bad.extend(unit_problems(span))
        try:
            expected = formatted(recompute(record, span), span.fmt, span.suffix)
        except (PathError, ZeroDivisionError, AssertionError, ValueError) as exc:
            bad.append(f"{span.kind} {span.inputs!r}: {exc}")
            continue
        if expected != span.text:
            bad.append(f"{span.kind} {span.inputs!r}: page {span.text!r}, record {expected!r}")
    return bad


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("run_id", GOLDEN_RUNS)
def test_golden_matches_render(run_id):
    """The checked-in golden is today's page for that record."""
    golden = golden_path(run_id).read_text()
    assert render(run_id) == golden, (
        f"{golden_path(run_id)} differs from today's render; a change that moves "
        "the page rewrites the golden by hand and has its diff reviewed"
    )


@pytest.mark.parametrize("run_id", GOLDEN_RUNS)
def test_golden_derived_numbers_agree(run_id):
    html = golden_path(run_id).read_text()
    spans = derived_spans(html)
    # Non-degenerate: every golden page carries derived numbers to check.
    assert len(spans) >= 5, f"{run_id}: only {len(spans)} derived spans"
    assert mismatches(load_record(run_id), html) == []


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


# Inline percentage, percent-of and count arithmetic in the renderer: each
# must go through reports/derived.py. A listed line is not page text, or is
# a value the record does not hold.
_REPORTS = Path(__file__).parents[1] / "src" / "lakebench" / "reports"
_INLINE = re.compile(
    r"(:[+ ]?,?\.?\d*%\})"  # a percent format spec
    r"|(\d?f\}%)"  # a fixed spec followed by a literal %
    r"|(\*\s*100\b)|(\b100\s*\*)"  # percent arithmetic
    r"|(sum\(\s*1\s+for)"  # a filtered count, also across lines
    r"|(\{len\()"  # a count printed straight into page text
)
_ALLOWED: dict[str, str] = {
    # The bottleneck shares are kept as floats to pick the dominant stage;
    # the page text renders them through derived.pct.
    'd["weight"] / total_weight * 100': "ordering",
    'd["cpu_sec"] / total_cpu * 100 if total_cpu and d["cpu_sec"] is not None else 0.0': "ordering",
    # A direct unit call of the TM section has no record to name.
    'return f"{float(v) * 100:.1f}%"': "no record path",
}


def test_no_inline_percent_or_count_format():
    hits = []
    for name in ("generator.py", "scorecard.py"):
        text = (_REPORTS / name).read_text()
        lines = text.splitlines()
        for m in _INLINE.finditer(text):
            n = text.count("\n", 0, m.start())
            line = lines[n].strip()
            if line not in _ALLOWED:
                hits.append(f"{name}:{n + 1}: {line}")
    assert hits == [], "format these through reports/derived.py:\n" + "\n".join(hits)


def test_derived_never_raises():
    """A key the grammar cannot quote, or a value that is not finite, renders
    as plain text with no span; derived.py never raises into a render."""
    from lakebench.reports import derived as dv

    bad_key = dv.path("streaming", 0, "ttd_by_rule", 'W"1', "alerts")
    assert bad_key == dv.UNADDRESSABLE
    assert dv.total(5, paths=bad_key, fmt=",d") == "5"
    assert dv.total(float("nan"), paths="streaming[*].total_rows_processed", fmt=",d") == "nan"
    assert dv.count(3, path=dv.path("a", "b\\c")) == "3"
    assert dv.pct(1.0, 0.0, num_path="a", den_path="b") == "n/a"
    assert dv.pct(1.0, float("nan"), num_path="a", den_path="b") == "n/a"
    assert dv.ratio(1.0, None, a_path="a", b_path="b") == "n/a"


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


def test_expected_file_covers_every_golden():
    import json

    data = json.loads(_EXPECTED.read_text())
    assert set(data) - {"_about"} == set(GOLDEN_RUNS)


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


def _render_dict(record: dict) -> str:
    import tempfile

    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.generator import ReportGenerator
    from tests.fixtures.report_goldens import scrub_timestamp

    with tempfile.TemporaryDirectory() as tmp:
        metrics = MetricsStorage(tmp)._dict_to_metrics(record)
        html = ReportGenerator(metrics_dir=tmp)._generate_html(
            metrics, platform_metrics=metrics.platform_metrics
        )
    return scrub_timestamp(html)


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


def _plain_text(html: str) -> str:
    """Visible page text, tags removed and whitespace collapsed."""
    from html import unescape

    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", html)))
