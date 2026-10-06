"""When two Customer 360 gold tables hold the same gold (LB-267).

Not collected by pytest. Spark adds DOUBLEs in the order partial
aggregates reach the final aggregate, which the plan does not fix (a scan
of a multi-append Iceberg table assigns files to tasks differently from
scan to scan). Most gold KPIs do not depend on that order: sums of
cent-valued amounts stay within about 1e-12 of the cent grid, far from a
ROUND midpoint; a max has no order; an average of an INT column sums
integers exactly. An average of a DOUBLE amount does: its quotient lands
anywhere, so a last-bit difference in the sum can cross a ROUND midpoint
and move the KPI one quantum. Run twice over one silver table,
``avg_transaction_value`` and ``avg_estimated_ltv`` landed one cent apart
on one to five days in 200 and nothing else moved, so an exact hash of gold
is not stable (LB-267: stream and batch gold over one silver snapshot
hashed differently in CI).

Two gold tables match here when they have the same columns and types, the
same days, and every cell equal, except that the two averages in
``ORDER_SENSITIVE`` may differ by one quantum, with NULL only against NULL.
Spark rounds a DOUBLE HALF_UP through BigDecimal, which is monotone, so a
pre-ROUND difference below one quantum moves the result by at most one: at
these test sizes the pre-ROUND difference is a few ulps. Every other KPI,
including every count and every sum, is compared exactly, so a one-cent
change to one purchase still shows in that day's revenue. The rows must also
match under the product's result fingerprint (``benchmark/fingerprint.py``,
the two averages approximate at their quanta), the rule ``compare`` and the
perf gate apply to query results; beyond the cell check it catches a bias
of one quantum in the same direction on many days.

The rows come from ``table_fingerprint.table_rows`` in the Spark child.
Imported by the parity test modules in the parent and by
``test_c360_gold_compare.py``; stdlib and ``lakebench`` only.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

#: The day key of gold: one row per day.
GOLD_KEY = "interaction_date"

#: The gold KPIs whose value depends on summation order, and the quantum of
#: their ROUND: the averages of a DOUBLE silver column
#: (``common.get_daily_kpi_aggregations``; transaction_amount and
#: lifetime_value_estimate are the DOUBLE silver columns it averages).
ORDER_SENSITIVE = {
    "avg_transaction_value": 0.01,
    "avg_estimated_ltv": 0.01,
}


def kpi_source() -> str:
    """The source text of common.get_daily_kpi_aggregations."""
    src = (
        Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts/common.py"
    ).read_text()
    body = src[src.index("def get_daily_kpi_aggregations") :]
    return body[: body.index("\n\n\n")]


#: The DOUBLE silver columns the KPIs aggregate (every other one they read
#: is an INT, a string or a boolean; the parity test checks it on silver).
#: Both hold whole cents, which keeps their sums order-free; a KPI over a
#: DOUBLE column off the cent grid would make its sums order-sensitive too.
DOUBLE_SILVER = {"transaction_amount", "lifetime_value_estimate"}

_APPROX_TYPES = frozenset({"double", "float"})

#: Room for the binary representation of two rounded values a quantum apart
#: (0.01 apart reads as 0.009999999999990905 or 0.010000000000005116).
_REL = 1e-9


def gold_differences(
    a: dict[str, Any], b: dict[str, Any], quanta: dict[str, float] = ORDER_SENSITIVE
) -> list[str]:
    """Why *a* and *b* (``table_rows`` dicts) are not the same gold, or []."""
    if a["columns"] != b["columns"]:
        return [f"columns differ: {a['columns']} vs {b['columns']}"]
    if a["types"] != b["types"]:
        return [f"column types differ: {a['types']} vs {b['types']}"]
    cols, types = a["columns"], a["types"]
    problems = [
        f"order-sensitive column {c} is {types[c]}, not DOUBLE"
        for c in cols
        if c in quanta and types[c] not in _APPROX_TYPES
    ]
    if problems:
        return problems
    if GOLD_KEY not in cols:
        return [f"no {GOLD_KEY} column"]
    k = cols.index(GOLD_KEY)
    rows_a, rows_b = _by_key(a["rows"], k), _by_key(b["rows"], k)
    if isinstance(rows_a, str) or isinstance(rows_b, str):
        return [p for p in (rows_a, rows_b) if isinstance(p, str)]
    if rows_a.keys() != rows_b.keys():
        only_a = sorted(map(str, rows_a.keys() - rows_b.keys()))
        only_b = sorted(map(str, rows_b.keys() - rows_a.keys()))
        return [f"days differ: only in a {only_a[:5]}, only in b {only_b[:5]}"]
    for day in sorted(rows_a, key=str):
        for i, c in enumerate(cols):
            x, y = rows_a[day][i], rows_b[day][i]
            if c in quanta:
                if not _within(x, y, quanta[c]):
                    problems.append(f"{day} {c}: {x!r} vs {y!r} (quantum {quanta[c]})")
            elif not _same(x, y):
                problems.append(f"{day} {c}: {x!r} vs {y!r}")
    return problems


def product_mismatch(
    a: dict[str, Any], b: dict[str, Any], quanta: dict[str, float] = ORDER_SENSITIVE
) -> str | None:
    """``benchmark.fingerprint.mismatch`` of the two tables' rows, with the
    ``quanta`` columns approximate: None when the product's result
    fingerprint takes them for the same result."""
    from lakebench.benchmark.fingerprint import fingerprint_rows, mismatch

    def fp(t: dict[str, Any]) -> dict:
        approx = {i: quanta[c] for i, c in enumerate(t["columns"]) if c in quanta}
        return fingerprint_rows(t["rows"], approx_columns=approx)

    return mismatch(fp(a), fp(b))


def _by_key(rows: list[list[Any]], k: int) -> dict[Any, list[Any]] | str:
    out: dict[Any, list[Any]] = {}
    for r in rows:
        if r[k] in out:
            return f"{GOLD_KEY} {r[k]!r} appears twice"
        out[r[k]] = r
    return out


def _same(x: Any, y: Any) -> bool:
    """Exact, but a NaN equals a NaN (JSON gives a fresh float for each)."""
    if isinstance(x, float) and isinstance(y, float) and math.isnan(x) and math.isnan(y):
        return True
    return x == y


def _within(x: Any, y: Any, quantum: float) -> bool:
    if x is None or y is None:
        return x is None and y is None
    if not (math.isfinite(x) and math.isfinite(y)):
        return repr(x) == repr(y)  # NaN only with NaN, an infinity only with itself
    return abs(x - y) <= quantum + _REL * max(abs(x), abs(y), 1.0)
