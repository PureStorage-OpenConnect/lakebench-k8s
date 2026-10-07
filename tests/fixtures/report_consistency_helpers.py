"""Shared test helpers moved from tests/test_report_consistency.py (imported by several test files)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any

from tests.fixtures.report_goldens import page_text


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


def _plain_text(html: str) -> str:
    """Visible page text, tags removed and whitespace collapsed."""
    from html import unescape

    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", html)))
