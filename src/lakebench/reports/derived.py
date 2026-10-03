"""Derived numbers on the report page: each agrees with the record.

Every percentage, total and count the report computes from a run's record
goes through one of :func:`pct`, :func:`total`, :func:`count` or
:func:`ratio`. Each returns the formatted text wrapped in a span that names
where the number came from::

    <span data-lb-derived="pct"
          data-lb-inputs="pipeline_benchmark.scores.scale_ratio"
          data-lb-fmt=".1f" data-lb-suffix="%" data-lb-scale="100">99.1%</span>

``tests/test_report_consistency.py`` parses a rendered page and, for every
such span, recomputes the value from the record's metrics.json by those
paths and compares it with the rendered text at the rendered precision. The
renderer computes its value from the loaded ``PipelineMetrics``; the test
computes it from the JSON, so a span whose value and claimed inputs disagree
fails. A value and inputs that are wrong the same way (both counting every
job instead of the passed ones) agree with each other; the test catches
those through its own unit table (fraction or percent, bytes, seconds) and
the hand-written expected phrases in ``tests/expected/report_goldens.json``.

A number with no checkable source (a non-finite value, or a key the grammar
cannot quote) renders as plain text with no span. Nothing here raises into a
render.

Input grammar (``data-lb-inputs``)
----------------------------------

- A **path** addresses the metrics.json dict: ``a.b`` for keys, ``[3]`` for
  a list index, ``[*]`` for every element, ``["k.x"]`` for a key that holds
  a path character (build them with :func:`path`).
- A **term** is a product of factors joined by ``*``. A factor is a path, a
  number, or ``/<number>`` (divide by a constant). A path that yields many
  values (through ``[*]``) is summed.
- A term may instead be a count: ``count(<path>)`` (the elements of the one
  list or mapping the path names, or every value a ``[*]`` path yields),
  ``truthy(<path>)``, ``falsy(<path>)`` or ``positive(<path>)``.
- An **expression** is a sum of terms joined by ``|``.
- :func:`pct` and :func:`ratio` take two expressions separated by ``;``
  (numerator; denominator).
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from html import escape as _escape

# Characters a plain key segment may hold; anything else is quoted.
_PLAIN_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")

COUNT_PREDICATES = ("count", "truthy", "falsy", "positive")

# What path() returns for a key the grammar cannot quote (one holding ``"``
# or a backslash). A number whose inputs hold it renders as plain text with
# no span: the page still shows it, and nothing claims a source for it.
UNADDRESSABLE = "!unaddressable!"


def path(*parts: str | int) -> str:
    """A record path from its parts: ``path("jobs", 2, "elapsed_seconds")``
    is ``jobs[2].elapsed_seconds``; ``"*"`` is every element; a key with a
    path character is quoted (``["completeness.bronze"]``). Never raises."""
    out = ""
    for p in parts:
        if isinstance(p, int):
            out += f"[{p}]"
        elif p == "*":
            out += "[*]"
        elif _PLAIN_KEY.match(str(p)):
            out += f".{p}" if out else str(p)
        elif '"' in str(p) or "\\" in str(p):
            return UNADDRESSABLE
        else:
            out += f'["{p}"]'
    return out


def product(*factors: str | float) -> str:
    """A term multiplying paths and constants (``a*b*/3600``)."""
    return "*".join(str(f) for f in factors)


def counted(predicate: str, p: str) -> str:
    """A count term: ``counted("truthy", "jobs[*].success")``."""
    if predicate not in COUNT_PREDICATES:
        raise ValueError(f"unknown count predicate {predicate!r}")
    return f"{predicate}({p})"


def expr(terms: str | Sequence[str]) -> str:
    """A sum of terms."""
    if isinstance(terms, str):
        return terms
    return "|".join(terms)


def _fmt(value: float, fmt: str, suffix: str) -> str:
    """*value* in *fmt*; an integer spec (``",d"``) takes the value as an int."""
    if fmt.endswith("d"):
        return f"{format(int(round(value)), fmt)}{suffix}"
    return f"{format(value, fmt)}{suffix}"


def _render(
    kind: str, value: float, inputs: str, fmt: str, suffix: str, scale: float | None
) -> str:
    """The span, or plain text when there is no checkable number: a value
    that is not finite (shown as the record holds it) or an input the
    grammar cannot name. Rendering a report never raises here."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return _escape(f"{value}{suffix}")
    if not math.isfinite(value):
        return _escape(f"{value}{suffix}")
    text = _fmt(value, fmt, suffix)
    if UNADDRESSABLE in inputs:
        return _escape(text)
    attrs = (
        f'data-lb-derived="{kind}" data-lb-inputs="{_escape(inputs, quote=True)}" '
        f'data-lb-fmt="{_escape(fmt, quote=True)}"'
    )
    if suffix:
        attrs += f' data-lb-suffix="{_escape(suffix, quote=True)}"'
    if scale is not None:
        attrs += f' data-lb-scale="{scale:g}"'
    return f"<span {attrs}>{_escape(text)}</span>"


def _is_zero(den: float | None) -> bool:
    try:
        return den is None or float(den) == 0.0 or not math.isfinite(float(den))
    except (TypeError, ValueError):
        return True


def pct(
    num: float,
    den: float | None = None,
    *,
    num_path: str | Sequence[str],
    den_path: str | Sequence[str] | None = None,
    digits: int = 1,
    signed: bool = False,
    scale: float = 100.0,
    missing: str = "n/a",
) -> str:
    """A percentage. With a denominator it is ``100 * num / den``; without
    one, ``num * scale`` (``scale=100`` for a stored fraction, ``1`` for a
    value the record already holds in percent). A zero, missing or
    non-finite denominator renders *missing* with no span."""
    fmt = f"{'+' if signed else ''}.{int(digits)}f"
    if den_path is not None:
        if _is_zero(den):
            return _escape(missing)
        try:
            value = float(num) / float(den) * 100.0  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return _escape(missing)
        return _render("pct", value, f"{expr(num_path)};{expr(den_path)}", fmt, "%", None)
    try:
        value = float(num) * scale
    except (TypeError, ValueError):
        return _escape(missing)
    return _render("pct", value, expr(num_path), fmt, "%", scale)


def total(
    value: float,
    *,
    paths: str | Sequence[str],
    fmt: str = ",.0f",
    suffix: str = "",
) -> str:
    """A sum the page computed over record values."""
    return _render("total", value, expr(paths), fmt, suffix, None)


def count(
    n: int,
    *,
    path: str,
    where: str = "count",
    suffix: str = "",
) -> str:
    """How many values *path* yields (``where="count"``), or how many are
    truthy, falsy or positive."""
    return _render("count", n, counted(where, path), ",d", suffix, None)


def ratio(
    a: float,
    b: float | None,
    *,
    a_path: str | Sequence[str],
    b_path: str | Sequence[str],
    fmt: str = ".1f",
    suffix: str = "x",
    missing: str = "n/a",
) -> str:
    """``a / b``; *missing* with no span when *b* is zero."""
    if _is_zero(b):
        return _escape(missing)
    try:
        value = float(a) / float(b)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return _escape(missing)
    return _render("ratio", value, f"{expr(a_path)};{expr(b_path)}", fmt, suffix, None)
