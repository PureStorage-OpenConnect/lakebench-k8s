"""Result fingerprints: an engine-independent identity for a query's result.

Two compositions may only be compared on performance when they return the
same results (mission invariant 2). After a benchmark query's timed samples,
the runner executes it once more, untimed, and records a fingerprint of the
rows. The perf gate and ``lakebench reproduce`` refuse when fingerprints do
not match; ``lakebench compare`` labels the comparison not comparable.

Spec ``rf2`` (rf1 rounded every number to 15 significant digits, summed
approximate columns without row association, and let NaN match anything). Bump SPEC whenever any rule here changes, so a fingerprint is
only ever matched against one computed the same way.

Cells. Each cell becomes a string:

- NULL: ``\\N``. Every engine reports a typed NULL on the fingerprint path
  (Trino JSON ``null``, beeline ``NULL`` with ``--nullemptystring=false``,
  DuckDB ``None``); the empty string stays the empty string.
- booleans: ``true`` / ``false``.
- numbers (a Python number, or text that parses as one: ``2263``,
  ``2263.0``, ``123.40``, ``1.2345E7``): parsed as Decimal, normalised and
  written fixed-point with trailing zeros stripped and ``-0`` as ``0``, so
  decimal scale differences between engines (Trino AVG at scale 2, Spark at
  scale 6) do not matter, and exponent notation does not either. No digit
  is ever rounded: integers and decimals keep every digit, and a double is
  its shortest round-trip text (Python ``repr``; the JDKs Trino and Spark 4
  run print the same, except Java 17's rare non-shortest ``Double.toString``,
  which then reads as a difference, never as a false match). ``NaN`` and
  infinities have fixed names.
- dates (``YYYY-MM-DD``): as is.
- timestamps (Trino ``... UTC``, Spark ``yyyy-MM-dd HH:mm:ss[.f]``, DuckDB
  ``...+00``, Python datetimes): converted to UTC and written
  ``YYYY-MM-DDTHH:MM:SS.ffffffZ``. A timestamp without a zone is taken as
  UTC: every engine session is pinned to UTC (Trino ``--timezone``, the
  Thrift server's ``spark.sql.session.timeZone``, DuckDB ``TimeZone``), and
  Spark prints a zoned timestamp without its zone, so the zone cannot be
  used to tell the two types apart.
- strings: surrounding whitespace trimmed.
- arrays, maps and structs: unsupported (the fingerprint says so rather
  than guessing a cross-engine rendering). No benchmark query returns one.

Rows. A row's cells are joined with U+001F and hashed with 8-byte blake2b.
``exact`` is the sum of the row hashes modulo 2**64: order independent, and
a duplicated row changes it (XOR would cancel it).

Approximate columns. A column computed from a DOUBLE aggregate cannot match
exactly across engines at scale (summation order moves the last digits the
query rounds to). A query declares those columns with a quantum
(``BenchmarkQuery.approx_columns``, e.g. ``{3: 0.01}`` for a ``ROUND(.., 2)``
revenue). Their values are left out of ``exact`` (only whether they are
NULL, NaN or infinite goes in) and summed per column with ``math.fsum``,
once plainly and once weighted by a per-row factor in [1, 2) from the
row's exact cells, so a value that moved between groups is seen. Two
results match when both sums are within ``approx_tolerance`` (twice it for
the weighted sum) and the NaN/infinity counts are equal.

The fingerprint is ``{spec, rows, cols, exact, approx, approx_w, quanta,
approx_special, engine,
adapted_sql_sha}``. ``engine`` and ``adapted_sql_sha`` (the SQL actually
sent, after the engine adapter) are evidence, not part of the match. A
result that could not be fingerprinted is ``{spec, unsupported}`` or
``{spec, error}`` and matches nothing.

This module imports only the standard library: the DuckDB executor ships its
source into the query pod and fingerprints there, so the rows never cross
kubectl.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

SPEC = "rf2"
NULL = "\\N"
_MOD = 1 << 64
_SEP = "\x1f"

_NUM = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")
_SPECIAL = {
    "nan": "nan",
    "+nan": "nan",
    "-nan": "nan",
    "infinity": "inf",
    "+infinity": "inf",
    "inf": "inf",
    "+inf": "inf",
    "-infinity": "-inf",
    "-inf": "-inf",
}
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TS = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?"
    r"\s*(Z|UTC|[+-]\d{2}(?::?\d{2})?)?$"
)


class Unsupported(ValueError):
    """A result the rf2 rules cannot fingerprint."""


def _canon_decimal(d: Decimal) -> str:
    """Exact fixed-point text of *d*: no digit is ever rounded away (a
    63-bit account id, a DECIMAL(38,2) sum). A Python float arrives as its
    shortest round-trip repr, which is also what current JDKs print for a
    double, so the same double renders the same on every engine."""
    if d.is_nan():
        return "nan"
    if d.is_infinite():
        return "inf" if d > 0 else "-inf"
    if d.is_zero():
        return "0"
    out = format(d.normalize(), "f")
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    return "0" if out in ("-0", "") else out


def _canon_datetime(ts: datetime) -> str:
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _parse_timestamp(m: re.Match[str]) -> datetime | None:
    year, month, day, hour, minute, sec, frac, zone = m.groups()
    micro = int((frac or "0").ljust(6, "0")[:6])
    try:
        ts = datetime(int(year), int(month), int(day), int(hour), int(minute), int(sec or 0), micro)
    except ValueError:
        return None
    if zone and zone not in ("Z", "UTC"):
        digits = zone[1:].replace(":", "")
        minutes = int(digits[:2]) * 60 + int(digits[2:4] or 0)
        from datetime import timedelta

        ts = ts - timedelta(minutes=minutes if zone[0] == "+" else -minutes)
    return ts


def canonical_cell(value: object) -> str:
    """The canonical text of one result cell (see the module docstring)."""
    if value is None:
        return NULL
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return _canon_decimal(Decimal(repr(value)))
    if isinstance(value, Decimal):
        return _canon_decimal(value)
    if isinstance(value, datetime):
        return _canon_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (list, tuple, dict, set)):
        raise Unsupported("a column holds an array, map or struct")
    text = str(value).strip()
    low = text.lower()
    if low in ("true", "false"):
        return low
    if low in _SPECIAL:
        return _SPECIAL[low]
    if _NUM.match(text):
        try:
            return _canon_decimal(Decimal(text))
        except InvalidOperation:
            return text
    if _DATE.match(text):
        return text
    m = _TS.match(text)
    if m:
        ts = _parse_timestamp(m)
        if ts is not None:
            return _canon_datetime(ts)
    return text


def _approx_value(value: object) -> float | str | None:
    """A finite float, a special name ("nan", "inf", "-inf"), or None."""
    if value is None:
        return None
    text = canonical_cell(value)
    if text == NULL:
        return None
    if text in ("nan", "inf", "-inf"):
        return text
    try:
        return float(text)
    except ValueError:
        raise Unsupported(f"approximate column holds a non-number ({text!r})") from None


#: Quantum that marks a column volatile rather than approximate: its value
#: is generated per pipeline run (FQ8's alert_id is a uuid()), so only
#: whether it is NULL enters the fingerprint. BenchmarkQuery.volatile_columns
#: is passed down in approx_columns with this value.
VOLATILE = -1.0

#: An approximate column matches when its sums differ by at most
#: quantum x (APPROX_FLIP_ALLOWANCE + sqrt(rows)): summation-order noise
#: moves a rounded value by about one quantum in a few rows, not in every
#: row, so the slack does not grow linearly into dollars at hundreds of rows.
APPROX_FLIP_ALLOWANCE = 10.0


def approx_tolerance(quantum: float, rows: int) -> float:
    return float(quantum) * (APPROX_FLIP_ALLOWANCE + math.sqrt(max(rows, 0)))


def fingerprint_rows(rows, approx_columns=None, engine=None, adapted_sql=None) -> dict:
    """The rf2 fingerprint of *rows* (sequences of cells).

    *approx_columns* maps a 0-based column index to its quantum. Raises
    Unsupported for a result the rules cannot handle; ``fingerprint_or_reason``
    turns that into a fingerprint that matches nothing.

    Each approximate column gets two sums: the plain sum, and a sum weighted
    by a per-row factor in [1, 2) taken from the row's exact cells. The
    plain sum cannot see a value moved from one group to another; the
    weighted sum can. NaN and infinities are counted per column, not summed,
    and must match exactly.
    """
    declared = {int(k): float(v) for k, v in (approx_columns or {}).items()}
    volatile = {k for k, v in declared.items() if v == VOLATILE}
    approx = {k: v for k, v in declared.items() if v != VOLATILE}
    sums: dict[int, list[float]] = {k: [] for k in approx}
    wsums: dict[int, list[float]] = {k: [] for k in approx}
    specials: dict[int, dict[str, int]] = {k: {} for k in approx}
    total = 0
    n = 0
    cols: int | None = None
    for row in rows:
        row = list(row)
        if cols is None:
            cols = len(row)
        elif len(row) != cols:
            raise Unsupported(f"rows have different column counts ({cols} and {len(row)})")
        cells = []
        values: dict[int, float] = {}
        for i, value in enumerate(row):
            if i in volatile:
                # A run-local value (a generated uuid): only its presence.
                cells.append(NULL if value is None else "?")
            elif i in approx:
                v = _approx_value(value)
                if v is None:
                    cells.append(NULL)
                elif isinstance(v, str):
                    cells.append(v)  # a special value is part of the exact hash
                    specials[i][v] = specials[i].get(v, 0) + 1
                else:
                    cells.append("~")
                    values[i] = v
            else:
                cells.append(canonical_cell(value))
        digest = int.from_bytes(
            hashlib.blake2b(_SEP.join(cells).encode(), digest_size=8).digest(), "big"
        )
        weight = 1.0 + digest / _MOD
        for i, v in values.items():
            sums[i].append(v)
            wsums[i].append(v * weight)
        total = (total + digest) % _MOD
        n += 1
    if cols is not None and any(k >= cols for k in declared):
        raise Unsupported(f"approximate column index out of range for {cols} columns")
    out: dict = {
        "spec": SPEC,
        "rows": n,
        "cols": cols or 0,
        "exact": f"{total:016x}",
        "approx": {str(k): math.fsum(v) for k, v in sorted(sums.items())},
        "approx_w": {str(k): math.fsum(v) for k, v in sorted(wsums.items())},
        "quanta": {str(k): q for k, q in sorted(approx.items())},
    }
    if volatile:
        out["volatile"] = sorted(volatile)
    special = {str(k): dict(sorted(v.items())) for k, v in sorted(specials.items()) if v}
    if special:
        out["approx_special"] = special
    if engine:
        out["engine"] = engine
    if adapted_sql is not None:
        out["adapted_sql_sha"] = hashlib.sha256(adapted_sql.encode()).hexdigest()[:16]
    return out


def fingerprint_or_reason(rows, approx_columns=None, engine=None, adapted_sql=None) -> dict:
    try:
        return fingerprint_rows(rows, approx_columns, engine, adapted_sql)
    except Unsupported as e:
        return unusable("unsupported", str(e), engine)


def unusable(kind: str, reason: str, engine: str | None = None) -> dict:
    """A fingerprint that matches nothing: kind is "unsupported" or "error"."""
    out = {"spec": SPEC, kind: reason}
    if engine:
        out["engine"] = engine
    return out


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def describe(fp: dict | None) -> str:
    """Short text form of a fingerprint for messages."""
    if not fp:
        return "none"
    if not isinstance(fp, dict):
        return str(fp)
    for kind in ("unsupported", "error"):
        if fp.get(kind):
            return f"{kind}: {fp[kind]}"
    approx = fp.get("approx") or {}
    tail = " approx{" + ", ".join(f"{k}:{v:.6g}" for k, v in approx.items()) + "}" if approx else ""
    return f"{fp.get('spec')} rows={fp.get('rows')} cols={fp.get('cols')} exact={fp.get('exact')}{tail}"


def usable(fp: object) -> bool:
    return (
        isinstance(fp, dict)
        and fp.get("spec") == SPEC
        and "exact" in fp
        and not fp.get("unsupported")
        and not fp.get("error")
    )


def mismatch(a: dict | None, b: dict | None) -> str | None:
    """Why fingerprints *a* and *b* do not show equal results, or None."""
    if not usable(a) or not usable(b):
        return "no usable result fingerprint on both sides"
    assert isinstance(a, dict) and isinstance(b, dict)
    for key in ("rows", "cols", "exact"):
        if a.get(key) != b.get(key):
            return f"{key} differs"
    if (a.get("quanta") or {}) != (b.get("quanta") or {}) or a.get("volatile") != b.get("volatile"):
        return "approximate or volatile columns declared differently"
    if (a.get("approx_special") or {}) != (b.get("approx_special") or {}):
        return "NaN or infinite values differ in an approximate column"
    rows = int(a.get("rows") or 0)
    for col, quantum in (a.get("quanta") or {}).items():
        for key, factor in (("approx", 1.0), ("approx_w", 2.0)):
            if key == "approx_w" and (key not in a or key not in b):
                return "a fingerprint lacks the row-weighted sums"
            va = float((a.get(key) or {}).get(col, 0.0))
            vb = float((b.get(key) or {}).get(col, 0.0))
            tolerance = factor * approx_tolerance(float(quantum), rows)
            if not abs(va - vb) <= tolerance + 1e-12 * max(abs(va), abs(vb), 1.0):
                what = "sums" if key == "approx" else "row-weighted sums"
                return f"column {col} {what} differ by more than {tolerance:g}"
    return None


# ---------------------------------------------------------------------------
# Engine output parsers (fingerprint path only)
# ---------------------------------------------------------------------------


def rows_from_trino_json(output: str) -> list[list]:
    """Rows of ``trino --output-format JSON``: one JSON object per line, in
    column order. Numbers are kept as their text, so canonical_cell treats
    them like any other engine's text (exact unless in exponent notation);
    duplicate column names are kept. Empty output is unsupported: the CLI
    prints nothing for an empty result, which cannot be told from no output
    at all (the runner checks the row count against the timed run)."""
    if not (output or "").strip():
        raise Unsupported("Trino printed no rows (an empty result cannot be told from no output)")
    rows = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(
                json.loads(
                    line,
                    parse_float=str,
                    parse_int=str,
                    object_pairs_hook=lambda pairs: [v for _, v in pairs],
                )
            )
        except json.JSONDecodeError as e:
            raise Unsupported(f"Trino output line is not JSON: {line[:80]!r}") from e
    return rows


def rows_from_beeline_tsv2(output: str) -> list[list]:
    """Rows of beeline ``--silent=true --outputformat=tsv2
    --nullemptystring=false`` output: a header line, then one tab-separated
    line per row, NULL as ``NULL``. tsv2 does not quote, so a string holding
    a tab or newline shows up as a row of the wrong width and the result is
    unsupported."""
    text = output or ""
    # Only the terminal newline goes: a row of empty cells (a one-column
    # empty string) is a line with nothing on it and still a row.
    if text.endswith("\n"):
        text = text[:-1]
    if text.endswith("\r"):
        text = text[:-1]
    if not text:
        raise Unsupported("beeline printed no tsv2 header (the statement produced no result set)")
    lines = text.split("\n")
    width = len(lines[0].split("\t"))
    rows = []
    for line in lines[1:]:
        cells = line.split("\t")
        if len(cells) != width:
            raise Unsupported("a Thrift string holds a tab or newline (tsv2 does not quote)")
        rows.append([None if c == "NULL" else c for c in cells])
    return rows


def last_json_line(output: str) -> dict | None:
    """The last line of *output* that parses as a JSON object, or None.

    DuckDB prints a progress bar on stdout (even to a pipe) for a query past
    2 s, so the payload is not necessarily the only line.
    """
    for line in reversed((output or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None
