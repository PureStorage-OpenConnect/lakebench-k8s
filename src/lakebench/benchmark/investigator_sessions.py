"""AML investigator sessions under load (continuous runs).

With ``architecture.benchmark.investigator_sessions`` set to N, an AML
continuous run runs one extra round right after its first in-stream round
that included the investigator queries (the baseline): N concurrent
sessions, each working one case of this run, picked in IQ1's queue order
(open cases first, by priority, oldest first). Session k
runs IQ1, IQ2 and IQ3 bound to its case (``queries.bind_case``) and IQ4
unchanged, once each, through ``BenchmarkRunner.run_throughput``. The round
is recorded as ``continuous.investigators``, never as a benchmark round, so
in-stream QpH and the round count do not move.

The record says how many sessions ran (``sessions_run``: the sessions
started, fewer than N when the run has fewer cases; a session whose queries
failed still counts, and its failures are in ``failed`` and ``status``),
each session's row counts and seconds, the nearest-rank p50 and p95 latency
per query over the sessions whose query succeeded (with the failed count),
the baseline round's time per query, the session window on this host's
clock, the failed queries and a status:
``pass``; ``fail`` (any session's IQ1 or IQ3 returned 0 rows, or a session
query failed; it fails the investigators check, never the run);
``no_cases``; ``no_rounds`` (no in-stream round ran); ``no_time`` (less
than twice the baseline round's time left in the window, or too little for
the per-query timeout); ``case_query_failed``. A query that failed on the
per-query timeout or on memory carries the matching BOUNDED BY label
(Lakebench sets both). The overlap of the detection ticks with
the session window is added after the window closes
(``metrics.tick_records.investigator_tick_overlap``) and labels time to
detect and continuous throughput through the verdict's ``investigators``
qualifier.

Pure where it can be: the cluster is reached only through the runner's
executor.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
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

#: The longest a session query may run (Lakebench-imposed), in seconds; less
#: when the window has less left (``query_timeout``).
MAX_QUERY_TIMEOUT_S = 300

#: Below this per-query timeout the sessions are not started (``no_time``).
MIN_QUERY_TIMEOUT_S = 30

#: The memory limits Lakebench sets on each engine (Trino:
#: templates/trino/configmap.yaml.j2; Spark Thrift: its server sizing).
MEMORY_BOUNDS = {
    "trino": "BOUNDED BY Trino query.max-memory (Lakebench-set)",
    "spark-thrift": "BOUNDED BY Spark Thrift server memory (Lakebench-set)",
}


#: Seconds an engine client may take past a query's timeout to clean it up
#: (the Trino executor kills a timed-out query by its source, up to 10 s).
CLEANUP_MARGIN_S = 10

#: The case pick's timeout is at most this, and a fifth of the time left.
MAX_CASE_QUERY_TIMEOUT_S = 120


def query_timeout(remaining_s: float) -> int:
    """The per-query timeout for the sessions: at most
    ``MAX_QUERY_TIMEOUT_S``, and small enough that a stream's four queries,
    each with the client's cleanup margin, end inside the window's
    *remaining_s* even if each runs to the limit, so the round does not
    stretch the window."""
    per_query = max(0.0, remaining_s) / len(INVESTIGATOR_QUERIES) - CLEANUP_MARGIN_S
    return int(min(MAX_QUERY_TIMEOUT_S, max(0.0, per_query)))


def case_query_timeout(remaining_s: float) -> int:
    return int(min(MAX_CASE_QUERY_TIMEOUT_S, max(0.0, remaining_s) / 5))


def timeout_bound(timeout_s: int) -> str:
    return f"BOUNDED BY Lakebench per-query timeout ({timeout_s}s)"


def session_sql_ids() -> dict[str, str]:
    """``{IQ: sha256[:12]}`` of each session query's SQL template (bound to a
    placeholder case id), so a record says which SQL its sessions ran."""
    import hashlib

    placeholder = "case-" + "0" * 24
    return {
        q.name: hashlib.sha256(bind_case(q, placeholder).sql.encode()).hexdigest()[:12]
        for q in INVESTIGATOR_QUERIES
    }


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


def select_cases(runner: Any, n: int, timeout: int = MAX_CASE_QUERY_TIMEOUT_S) -> list[str]:
    """Up to *n* distinct case ids, in order. Raises RuntimeError when the
    query fails. The ids are read by their fixed shape from the engine's
    output, so no engine-specific parser is needed."""
    result = runner.executor.execute_query(case_query(runner, n), timeout=max(1, timeout))
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
    remaining_s: float,
) -> dict[str, Any]:
    """Pick the cases, run the sessions concurrently and return the record
    (``continuous.investigators``). The per-query timeout keeps the round
    inside the *remaining_s* of the window. Never raises for a query or case
    failure: those are the record's ``status``."""
    import time

    def too_little(left: float) -> dict[str, Any]:
        return skipped(
            requested,
            "no_time",
            f"{left:.0f}s left in the window: under {MIN_QUERY_TIMEOUT_S}s per query",
        )

    if query_timeout(remaining_s) < MIN_QUERY_TIMEOUT_S:
        return too_little(remaining_s)
    picked = time.monotonic()
    try:
        cases = select_cases(runner, requested, timeout=case_query_timeout(remaining_s))
    except Exception as e:  # noqa: BLE001 -- recorded, never fails the run
        return skipped(requested, "case_query_failed", f"case query failed: {e}")
    if not cases:
        return skipped(requested, "no_cases", "the run has no case")
    # The case pick spent part of the time left.
    left = remaining_s - (time.monotonic() - picked)
    timeout = query_timeout(left)
    if timeout < MIN_QUERY_TIMEOUT_S:
        return too_little(left)
    start = now()
    result = runner.run_throughput(
        cache="hot",
        iterations=1,
        query_timeout=timeout,
        stream_queries=[session_queries(c) for c in cases],
        shuffle=False,
    )
    end = now()
    engine = runner._engine_name() if hasattr(runner, "_engine_name") else None
    return session_record(
        requested, cases, result, baseline, start, end, timeout_s=timeout, engine=engine
    )


def session_record(
    requested: int,
    cases: list[str],
    result: Any,
    baseline: Mapping[str, float],
    start: datetime,
    end: datetime,
    *,
    timeout_s: int = MAX_QUERY_TIMEOUT_S,
    engine: str | None = None,
) -> dict[str, Any]:
    """The record of a sessions round that ran (one stream per case)."""
    rows: dict[str, dict[str, int]] = {}
    seconds: dict[str, dict[str, float]] = {}
    times: dict[str, list[float]] = {q.name: [] for q in INVESTIGATOR_QUERIES}
    failed_n: dict[str, int] = {q.name: 0 for q in INVESTIGATOR_QUERIES}
    failed: list[dict[str, Any]] = []
    memory = timed_out = False
    for stream in getattr(result, "stream_results", None) or []:
        case = cases[stream.stream_id]
        per = rows.setdefault(case, {})
        secs = seconds.setdefault(case, {})
        for qr in stream.queries:
            name = _base_name(qr.query.name)
            per[name] = int(qr.rows_returned or 0)
            secs[name] = round(float(qr.elapsed_seconds), 3)
            if qr.success:
                times.setdefault(name, []).append(float(qr.elapsed_seconds))
            else:
                err = str(qr.error_message or "")
                memory = memory or _memory_error(err)
                # The client's own timeout, or Trino's query_max_run_time,
                # which its executor sets just under the timeout.
                low = err.lower()
                timed_out = timed_out or "timed out" in low or "exceeded maximum time" in low
                failed_n[name] = failed_n.get(name, 0) + 1
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
            "failed": failed_n.get(name, 0),
        }
        for name, ts in times.items()
    }
    labels = list(LABELS)
    if memory:
        labels.append(MEMORY_BOUNDS.get(engine or "", "BOUNDED BY engine memory (Lakebench-set)"))
    if timed_out:
        labels.append(timeout_bound(timeout_s))
    return {
        "sessions_requested": requested,
        "sessions_run": len(cases),
        "lowered_reason": "fewer cases than sessions" if len(cases) < requested else None,
        "case_ids": list(cases),
        "rows_per_session": rows,
        "seconds_per_session": seconds,
        "latency": latency,
        "baseline": dict(baseline),
        "window": {"start": _iso(start), "end": _iso(end), "clock": "lakebench host (UTC)"},
        "query_timeout_s": timeout_s,
        "session_sql": session_sql_ids(),
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
