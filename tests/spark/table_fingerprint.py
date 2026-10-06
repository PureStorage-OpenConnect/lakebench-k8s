"""An order-independent fingerprint of a table's rows, for parity tests.

Not collected by pytest. Shared by the multi-cycle equivalence scenario
(C36-2) and the gold batch/stream parity scenario (V16-7). It wraps the
product's ``common.frame_fingerprint`` (row count, the exact sum of one
xxhash64 per row, and the column types) over every column but the ones
named, so two tables with the same rows in any order and file layout match,
and one changed value does not. ``table_rows`` hands a small table's rows
to the parent instead, for gold, whose rounded DOUBLE KPIs an exact hash
cannot compare (``c360_gold_compare``, LB-267). Imported by Spark children,
which have the Spark scripts on their path.
"""

from __future__ import annotations

from typing import Any

#: When the row was processed: differs between two correct builds of the
#: same rows. Everything else, ``_batch_id`` included, must match.
NOT_BUSINESS = frozenset({"silver_processing_timestamp"})


#: ``frame_fingerprint`` takes at most this many columns (its null mask).
MAX_COLUMNS = 63


def table_fingerprint(
    df: Any, excluded: frozenset[str] = NOT_BUSINESS, key: str = "id"
) -> dict[str, Any]:
    """``{rows, columns, sha256}`` of a DataFrame over its other columns.

    A table wider than ``MAX_COLUMNS`` is fingerprinted in column groups that
    each carry *key* (when the table has it): with a unique key every
    group's multiset of rows pins its columns to their row, so the groups
    together match only when the rows do."""
    from common import frame_fingerprint

    cols = sorted(c for c in df.columns if c not in excluded)
    shared: list[str] = []
    if len(cols) > MAX_COLUMNS:
        # Groups are tied to their rows only through a unique key.
        if key not in cols:
            raise ValueError(f"{len(cols)} columns and no {key!r} column to group them by")
        if df.select(key).distinct().count() != df.count():
            raise ValueError(f"{key!r} is not unique, so column groups would not pin rows")
        shared = [key]
    rest = [c for c in cols if c not in shared]
    width = MAX_COLUMNS - len(shared)
    parts, rows = [], None
    for i in range(0, len(rest), width):
        n, fp, cols_sha = frame_fingerprint(df, shared + rest[i : i + width])
        rows = int(n)
        parts.append(f"{fp}:{cols_sha}")
    return {"rows": rows or 0, "columns": cols, "sha256": "|".join(parts)}


def table_rows(df: Any, excluded: frozenset[str] = NOT_BUSINESS) -> dict[str, Any]:
    """``{columns, types, rows}`` of a small table, JSON-ready, for a parent
    process to compare cell by cell (``c360_gold_compare``): the columns
    sorted, as in ``table_fingerprint``; dates and timestamps as ISO text.
    Collects every row, so only for tables of a few hundred rows."""
    from datetime import date, datetime

    cols = sorted(c for c in df.columns if c not in excluded)
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields if f.name in cols}

    def cell(v: Any) -> Any:
        return v.isoformat() if isinstance(v, (date, datetime)) else v

    rows = [[cell(r[c]) for c in cols] for r in df.select(*cols).collect()]
    return {"columns": cols, "types": types, "rows": rows}
