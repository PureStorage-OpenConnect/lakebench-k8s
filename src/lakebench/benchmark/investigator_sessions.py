"""AML investigator sessions under load (continuous runs).

With ``architecture.benchmark.investigator_sessions`` set to N, an AML
continuous run runs one extra round right after its first in-stream round
that included the investigator queries (the baseline): N concurrent
sessions, each working one open case, picked in IQ1's queue order. Session k
runs IQ1, IQ2 and IQ3 bound to its case (``queries.bind_case``) and IQ4
unchanged, once each, through ``BenchmarkRunner.run_throughput``. The round
is recorded as ``continuous.investigators``, never as a benchmark round, so
in-stream QpH and the round count do not move.

The record says how many sessions ran (``sessions_run``, fewer than N when
fewer cases are open), each session's row counts, the nearest-rank p50 and
p95 latency per query over the sessions, the baseline round's time per
query, the session window on the CLI clock, the failed queries and a status:
``pass``; ``fail`` (any session's IQ1 or IQ3 returned 0 rows, or a session
query failed; it fails the investigators check, never the run);
``no_cases``; ``no_time`` (less than twice the baseline round's time left in
the window); ``case_query_failed``. The overlap of the detection ticks with
the session window is added after the window closes
(``tick_overlap``), and labels time to detect and continuous throughput.

Pure where it can be: the cluster is reached only through the runner's
executor.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any

from .queries import (
    CASE_ID_RE,
    INVESTIGATOR_QUERIES,
    IQ1_CASE_ORDER,
    SESSION_SUFFIX,
    BenchmarkQuery,
    bind_case,
)

#: The investigator queries whose 0 rows fail a session (IQ2 and IQ4 may be
#: legitimately empty: no escalation case, no open case past 60 days).
MUST_RETURN_ROWS = ("IQ1_customer_360", "IQ3_counterparty_two_hop")

#: The run window must have this many baseline rounds left for the sessions.
TIME_FACTOR = 2.0

#: Labels every investigators record carries.
LABELS = ("n=1 per arm", "shared S3 contention")

#: The memory limit Lakebench sets on Trino (templates/trino/configmap.yaml.j2).
TRINO_MEMORY_BOUND = "BOUNDED BY Trino query.max-memory (Lakebench-set)"


def nearest_rank(values: Iterable[float], pct: float) -> float | None:
    """The nearest-rank percentile *pct* (0 < pct <= 100) of *values*, or
    None for none: the smallest value with at least pct% of the values at or
    below it."""
    xs = sorted(float(v) for v in values)
    if not xs:
        return None
    rank = max(1, math.ceil(pct / 100.0 * len(xs)))
    return xs[rank - 1]


def case_query(runner: Any, n: int) -> str:
    """The untimed case selection: up to *n* case ids of this run, in IQ1's
    queue order, rendered for the runner's engine."""
    cases = runner._extra_tables["gold_cases"]
    run = (runner.tm_run_id or "").replace("'", "''")
    sql = (
        f"SELECT case_id FROM {runner.catalog}.{cases} WHERE base_run_id = '{run}' "
        f"ORDER BY {' '.join(IQ1_CASE_ORDER.split())} LIMIT {int(n)}"
    )
    return runner.executor.adapt_query(sql)


def select_cases(runner: Any, n: int) -> list[str]:
    """Up to *n* distinct case ids, in order. Raises RuntimeError when the
    query fails. The ids are read by their fixed shape from the engine's
    output, so no engine-specific parser is needed."""
    result = runner.executor.execute_query(case_query(runner, n), timeout=120)
    if not result.success:
        raise RuntimeError(result.error or "case query failed")
    ids = list(dict.fromkeys(CASE_ID_RE.findall(result.raw_output or "")))
    return ids[:n]


def session_queries(case_id: str) -> list[BenchmarkQuery]:
    """One session's queries, in order: IQ1, IQ2 and IQ3 bound to the case,
    IQ4 as it is."""
    return [bind_case(q, case_id) for q in INVESTIGATOR_QUERIES]


def _base_name(name: str) -> str:
    return name[: -len(SESSION_SUFFIX)] if name.endswith(SESSION_SUFFIX) else name


def baseline_seconds(round_queries: Iterable[Mapping[str, Any]]) -> dict[str, float]:
    """``{IQ: seconds}`` of the baseline round's investigator queries (the
    recorded round's query dicts)."""
    names = {q.name for q in INVESTIGATOR_QUERIES}
    out = {}
    for q in round_queries or []:
        name = q.get("name") or q.get("query_name")
        if name in names and q.get("success", True):
            out[name] = round(float(q.get("elapsed_seconds") or 0.0), 3)
    return out


def skipped(requested: int, status: str, reason: str) -> dict[str, Any]:
    """The record of a sessions round that did not run."""
    return {
        "sessions_requested": requested,
        "sessions_run": 0,
        "lowered_reason": None,
        "case_ids": [],
        "status": status,
        "reason": reason,
        "labels": list(LABELS),
    }


def run_sessions(
    runner: Any,
    requested: int,
    baseline: Mapping[str, float],
    *,
    now: Callable[[], datetime],
    query_timeout: int = 300,
) -> dict[str, Any]:
    """Pick the cases, run the sessions concurrently and return the record
    (``continuous.investigators``). Never raises for a query or case
    failure: those are the record's ``status``."""
    try:
        cases = select_cases(runner, requested)
    except Exception as e:  # noqa: BLE001 -- recorded, never fails the run
        return skipped(requested, "case_query_failed", f"case query failed: {e}")
    if not cases:
        return skipped(requested, "no_cases", "no case of this run was open")
    start = now()
    result = runner.run_throughput(
        cache="hot",
        iterations=1,
        fingerprint=False,
        query_timeout=query_timeout,
        stream_queries=[session_queries(c) for c in cases],
        shuffle=False,
    )
    end = now()
    return session_record(requested, cases, result, baseline, start, end)


def session_record(
    requested: int,
    cases: list[str],
    result: Any,
    baseline: Mapping[str, float],
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    """The record of a sessions round that ran (one stream per case)."""
    rows: dict[str, dict[str, int]] = {}
    times: dict[str, list[float]] = {q.name: [] for q in INVESTIGATOR_QUERIES}
    failed: list[dict[str, Any]] = []
    memory = False
    for stream in getattr(result, "stream_results", None) or []:
        case = cases[stream.stream_id]
        per = rows.setdefault(case, {})
        for qr in stream.queries:
            name = _base_name(qr.query.name)
            per[name] = int(qr.rows_returned or 0)
            if qr.success:
                times.setdefault(name, []).append(float(qr.elapsed_seconds))
            else:
                err = str(qr.error_message or "")
                memory = memory or _memory_error(err)
                failed.append({"case_id": case, "query": name, "error": err[:300]})
    empty = [
        {"case_id": c, "query": q}
        for c, per in rows.items()
        for q in MUST_RETURN_ROWS
        if q in per
        and per[q] == 0
        and not any(f["case_id"] == c and f["query"] == q for f in failed)
    ]
    latency = {
        name: {
            "p50_s": _round(nearest_rank(ts, 50)),
            "p95_s": _round(nearest_rank(ts, 95)),
            "n": len(ts),
        }
        for name, ts in times.items()
    }
    labels = list(LABELS)
    if memory:
        labels.append(TRINO_MEMORY_BOUND)
    return {
        "sessions_requested": requested,
        "sessions_run": len(cases),
        "lowered_reason": "fewer cases than sessions" if len(cases) < requested else None,
        "case_ids": list(cases),
        "rows_per_session": rows,
        "latency": latency,
        "baseline": dict(baseline),
        "window": {"start": _iso(start), "end": _iso(end)},
        "failed": failed,
        "empty": empty,
        "status": "fail" if failed or empty else "pass",
        "labels": labels,
    }


def _memory_error(text: str) -> bool:
    t = text.lower()
    return "exceeded" in t and "memory" in t


def _round(v: float | None) -> float | None:
    return round(v, 3) if v is not None else None


def _iso(t: datetime) -> str:
    return t.replace(tzinfo=None).isoformat() + "Z"


def _parse_iso(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).rstrip("Z"))
    except ValueError:
        return None


def tick_overlap(
    record: Mapping[str, Any],
    ticks: Iterable[Mapping[str, Any]],
    clock_offset_s: float | None,
) -> dict[str, Any] | None:
    """How the detection ticks met the session window: ``{tick_delta,
    load_label}``, or None when the sessions did not run or no tick carries
    a time. A tick (its ``ended_at`` on the cluster clock, minus its
    ``total`` phase) is shifted to the CLI clock by *clock_offset_s*; it
    overlaps when at least half of it lies inside the window, and is clean
    when none of it does (a tick partly inside but under half is neither)."""
    window = record.get("window") or {}
    w0, w1 = _parse_iso(window.get("start")), _parse_iso(window.get("end"))
    if w0 is None or w1 is None:
        return None
    shift = timedelta(seconds=clock_offset_s or 0.0)
    over: list[float] = []
    clean: list[float] = []
    timed = 0
    for t in ticks:
        end = _parse_iso(t.get("ended_at"))
        total = (t.get("phases") or {}).get("total")
        if end is None or total is None:
            continue
        timed += 1
        end = end - shift
        begin = end - timedelta(seconds=float(total))
        span = (end - begin).total_seconds()
        inside = (min(end, w1) - max(begin, w0)).total_seconds()
        inside = max(0.0, inside)
        if span <= 0:
            share = 1.0 if w0 <= end <= w1 else 0.0
        else:
            share = inside / span
        if share >= 0.5:
            over.append(float(total))
        elif inside == 0:
            clean.append(float(total))
    if timed == 0:
        return None
    return {
        "tick_delta": {
            "overlapping": {"n": len(over), "median_total_s": _median(over)},
            "clean": {"n": len(clean), "median_total_s": _median(clean)},
        },
        "load_label": (
            f"investigator load {window.get('start')}-{window.get('end')}: "
            f"{len(over)} of {timed} ticks overlap"
        ),
    }


def _median(xs: list[float]) -> float | None:
    return round(statistics.median(xs), 3) if xs else None
