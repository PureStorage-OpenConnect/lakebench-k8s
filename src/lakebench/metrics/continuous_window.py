"""The continuous-mode measurement window (DESIGN 5).

Continuous mode runs the stages concurrently over a corpus that keeps
arriving. Lakebench offers the corpus to the pipeline at a fixed trickle
(``max_files_per_trigger`` files per bronze trigger), so "arriving" means
bronze is still taking files. The CLI opens the window once every stream's
driver is running and closes it ``run_duration`` seconds later. Everything a
continuous score claims is measured inside that window:

- bronze rows ingested inside the window, and the seconds of the window data
  was still arriving (a corpus that runs out mid-window stops the clock at
  its last micro-batch plus one trigger, so rows/s is never averaged over
  idle time);
- silver commits and gold refreshes inside the window, and gold freshness
  from the cycles inside it.

Rows a stage took in before the window opened (a stream that started while
another was still waiting to submit) are counted separately and never enter
a window score.

Every event comes from the timestamped ``[lb] <utc> - <message>`` lines the
stage scripts print (``spark/scripts/common.py`` ``log``). The CLI stamps the
window with its own UTC clock; pod and CLI clocks are assumed NTP-synced.

Pure functions only: no Kubernetes, no Spark.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

#: Commits a stage needs inside the window for the run to count as
#: continuous processing: one commit is a single pass, not a stream.
MIN_WINDOW_COMMITS = 2

_LINE = re.compile(r"\[lb\] (\d{4}-\d\d-\d\dT[\d:.]+)Z? - (.*)$")
_WRITE = re.compile(r"Batch (\d+): writing ([\d,]+) rows")
_BRONZE_COMMIT = re.compile(r"Batch (\d+): committed in [\d.]+s")
_TRANSFORM = re.compile(r"Batch (\d+): transforming ([\d,]+) rows")
_OUTPUT = re.compile(r"Batch (\d+): ([\d,]+) rows after transforms")
_SILVER_COMMIT = re.compile(r"Batch (\d+): committed to \S+ in [\d.]+s")
_AGGREGATE = re.compile(r"Cycle (\d+): aggregating ([\d,]+) Silver records")
_REFRESHED = re.compile(r"Cycle (\d+): refreshed \S+ in [\d.]+s(?: \(([\d,]+) KPI records\))?")
_FRESHNESS = re.compile(r"Cycle (\d+): data freshness ([\d.]+)s( \(silver idle\))?")


@dataclass(frozen=True)
class StreamEvent:
    """One timestamped stage event (UTC, naive)."""

    at: datetime
    kind: str  # write, commit, transform, output, aggregate, refreshed, freshness
    ident: int  # batch id (bronze, silver) or cycle number (gold)
    rows: int | None = None
    value: float | None = None
    idle: bool = False


def _num(text: str) -> int:
    return int(text.replace(",", ""))


def _ts(text: str) -> datetime | None:
    try:
        at = datetime.fromisoformat(text)
    except ValueError:
        return None
    if at.tzinfo is not None:
        at = at.astimezone(timezone.utc).replace(tzinfo=None)
    return at


def utc_naive(at: datetime) -> datetime:
    """*at* as a naive UTC datetime, the form the log lines use."""
    if at.tzinfo is None:
        return at
    return at.astimezone(timezone.utc).replace(tzinfo=None)


def parse_events(logs: str | None, job_type: str) -> list[StreamEvent]:
    """The timestamped events in one stream's driver log, in log order.

    Lines without a parseable timestamp are skipped: an event that cannot be
    placed relative to the window is not evidence for it.
    """
    events: list[StreamEvent] = []
    for line in (logs or "").split("\n"):
        m = _LINE.search(line)
        if not m:
            continue
        at = _ts(m.group(1))
        if at is None:
            continue
        msg = m.group(2)
        if job_type == "bronze-ingest":
            w = _WRITE.search(msg)
            if w:
                events.append(StreamEvent(at, "write", int(w.group(1)), rows=_num(w.group(2))))
                continue
            c = _BRONZE_COMMIT.search(msg)
            if c:
                events.append(StreamEvent(at, "commit", int(c.group(1))))
        elif job_type == "silver-stream":
            t = _TRANSFORM.search(msg)
            if t:
                events.append(StreamEvent(at, "transform", int(t.group(1)), rows=_num(t.group(2))))
                continue
            o = _OUTPUT.search(msg)
            if o:
                events.append(StreamEvent(at, "output", int(o.group(1)), rows=_num(o.group(2))))
                continue
            c = _SILVER_COMMIT.search(msg)
            if c:
                events.append(StreamEvent(at, "commit", int(c.group(1))))
        elif job_type == "gold-refresh":
            a = _AGGREGATE.search(msg)
            if a:
                events.append(StreamEvent(at, "aggregate", int(a.group(1)), rows=_num(a.group(2))))
                continue
            r = _REFRESHED.search(msg)
            if r:
                kpi = _num(r.group(2)) if r.group(2) else None
                events.append(StreamEvent(at, "refreshed", int(r.group(1)), rows=kpi))
                continue
            f = _FRESHNESS.search(msg)
            if f:
                events.append(
                    StreamEvent(
                        at,
                        "freshness",
                        int(f.group(1)),
                        value=float(f.group(2)),
                        idle=bool(f.group(3)),
                    )
                )
    return events


def _inside(at: datetime, start: datetime, end: datetime) -> bool:
    return start <= at <= end


def window_stats(
    events: list[StreamEvent], job_type: str, start: datetime, end: datetime
) -> dict[str, Any]:
    """What one stream did inside the window [*start*, *end*] (naive UTC).

    Keys (None means the stream does not have the quantity):

    - ``window_input_rows``: rows taken in inside the window. Bronze: rows
      written by batches that started writing inside it. Silver: input rows
      of batches whose commit landed inside it. Gold: None (every cycle
      re-reads silver).
    - ``pre_window_input_rows``: the same count before the window opened.
    - ``window_commits``: bronze and silver commits, gold refreshes, inside.
    - ``window_new_data_cycles``: gold only. Refreshes inside the window whose
      cycle saw new silver data: not tagged "(silver idle)" and not reading
      fewer or the same silver rows as the cycle before it.
    - ``window_output_rows``: silver rows written after transforms by the
      batches committed inside the window (None when the log has no count).
    - ``output_rows``: rows the stage has written by the window's end. Bronze:
      rows written; silver: rows after transforms of every committed batch;
      gold: KPI rows of the last refresh (the full table: gold is rewritten
      each cycle). None when unmeasured.
    - ``freshness_seconds`` / ``freshness_active_seconds`` /
      ``trailing_idle_cycles``: gold only, over the freshness lines inside the
      window, with the trailing idle run split off as in LB-145.
    - ``last_write_offset_seconds``: bronze only, seconds from the window's
      start to its last write inside it (None when none).
    """
    out: dict[str, Any] = {
        "window_input_rows": None,
        "pre_window_input_rows": None,
        "window_commits": 0,
        "window_new_data_cycles": None,
        "window_output_rows": None,
        "output_rows": None,
        "freshness_seconds": None,
        "freshness_active_seconds": None,
        "trailing_idle_cycles": 0,
        "last_write_offset_seconds": None,
    }
    if job_type == "bronze-ingest":
        writes = [e for e in events if e.kind == "write" and e.at <= end]
        inside = [e for e in writes if e.at >= start]
        out["window_input_rows"] = sum(e.rows or 0 for e in inside)
        out["pre_window_input_rows"] = sum(e.rows or 0 for e in writes if e.at < start)
        out["window_commits"] = sum(
            1 for e in events if e.kind == "commit" and _inside(e.at, start, end)
        )
        out["output_rows"] = sum(e.rows or 0 for e in writes) if writes else 0
        if inside:
            out["last_write_offset_seconds"] = (max(e.at for e in inside) - start).total_seconds()
    elif job_type == "silver-stream":
        rows_in: dict[int, int] = {}
        rows_out: dict[int, int] = {}
        committed_in, committed_before = set(), set()
        for e in events:
            if e.at > end:
                continue
            if e.kind == "transform":
                rows_in[e.ident] = e.rows or 0
            elif e.kind == "output":
                rows_out[e.ident] = e.rows or 0
            elif e.kind == "commit":
                (committed_in if e.at >= start else committed_before).add(e.ident)
        committed_before -= committed_in
        out["window_input_rows"] = sum(rows_in.get(b, 0) for b in committed_in)
        out["pre_window_input_rows"] = sum(rows_in.get(b, 0) for b in committed_before)
        out["window_commits"] = sum(1 for b in committed_in if rows_in.get(b, 0) > 0)
        every = committed_in | committed_before
        if every and all(b in rows_out for b in every):
            out["output_rows"] = sum(rows_out[b] for b in every)
            out["window_output_rows"] = sum(rows_out[b] for b in committed_in)
    elif job_type == "gold-refresh":
        agg: dict[int, int] = {}
        idle: dict[int, bool] = {}
        refreshed: list[StreamEvent] = []
        fresh: list[StreamEvent] = []
        for e in events:
            if e.at > end:
                continue
            if e.kind == "aggregate":
                agg[e.ident] = e.rows or 0
            elif e.kind == "freshness":
                idle[e.ident] = e.idle
                if e.at >= start:
                    fresh.append(e)
            elif e.kind == "refreshed":
                refreshed.append(e)
        if refreshed and refreshed[-1].rows is not None:
            out["output_rows"] = refreshed[-1].rows
        inside = [e for e in refreshed if e.at >= start]
        out["window_commits"] = len(inside)
        cycles = sorted(agg)
        new = 0
        for e in inside:
            if idle.get(e.ident):
                continue
            prev = [agg[c] for c in cycles if c < e.ident]
            if e.ident in agg and prev and agg[e.ident] <= prev[-1]:
                continue
            new += 1
        out["window_new_data_cycles"] = new
        if fresh:
            out["freshness_seconds"] = max(e.value or 0.0 for e in fresh)
            cut = len(fresh)
            while cut > 0 and fresh[cut - 1].idle:
                cut -= 1
            out["trailing_idle_cycles"] = len(fresh) - cut
            if cut > 0:
                out["freshness_active_seconds"] = max(e.value or 0.0 for e in fresh[:cut])
    return out


def window_gate_problems(
    stats_by_job: dict[str, dict[str, Any] | None], min_commits: int = MIN_WINDOW_COMMITS
) -> list[str]:
    """Reasons a continuous run is not continuous processing (invariant 3).

    *stats_by_job* maps each submitted stream to its ``window_stats``, or
    None when its driver log could not be read. A run passes only when data
    arrived during the window (bronze ingested rows inside it), silver
    committed and gold refreshed on new data at least *min_commits* times
    inside it, and gold freshness was measured inside it.
    """
    problems: list[str] = []
    for job, stats in stats_by_job.items():
        if stats is None:
            problems.append(
                f"continuous gate: no {job} driver log with timestamped lines; cannot show "
                "it processed data during the window"
            )
    bronze = stats_by_job.get("bronze-ingest")
    if bronze is not None:
        inside, before = bronze["window_input_rows"] or 0, bronze["pre_window_input_rows"] or 0
        if inside == 0 and before > 0:
            problems.append(
                f"continuous gate: bronze ingested {before:,} rows before the window opened and "
                "none inside it: the corpus drained before the measurement window, so nothing "
                "was arriving while it was measured. Lower max_files_per_trigger so the trickle "
                "lasts the window, or shorten run_duration"
            )
        elif inside == 0:
            problems.append("continuous gate: bronze ingested 0 rows inside the window")
    silver = stats_by_job.get("silver-stream")
    if silver is not None and silver["window_commits"] < min_commits:
        problems.append(
            f"continuous gate: silver committed {silver['window_commits']} micro-batch(es) "
            f"with rows inside the window; continuous processing needs at least {min_commits}"
        )
    gold = stats_by_job.get("gold-refresh")
    if gold is not None:
        new = gold["window_new_data_cycles"] or 0
        if new < min_commits:
            problems.append(
                f"continuous gate: gold refreshed on new silver data {new} time(s) inside the "
                f"window ({gold['window_commits']} refresh(es) in all); continuous processing "
                f"needs at least {min_commits}"
            )
        if gold["freshness_seconds"] is None:
            problems.append("continuous gate: gold freshness was not measured inside the window")
    return problems


def arrival_seconds(
    window_s: float,
    last_write_offset_s: float | None,
    corpus_in_bronze: bool,
    bronze_trigger_s: float | None,
) -> float:
    """Seconds of the window data was arriving at bronze.

    The whole window while corpus was left to offer. When every corpus row
    had reached bronze by the window's end, the clock stops one trigger after
    bronze's last write inside the window (the trigger that found nothing
    new). No write inside the window is no arrival.
    """
    if last_write_offset_s is None:
        return 0.0
    if not corpus_in_bronze:
        return float(window_s)
    return float(min(window_s, last_write_offset_s + (bronze_trigger_s or 0.0)))


def expected_arrival_seconds(
    approx_bronze_gb: float,
    file_size_mb: float,
    max_files_per_trigger: int,
    bronze_trigger_s: float | None,
) -> float | None:
    """Rough seconds the trickle needs to offer the whole corpus, for the
    pre-run advisory. None when an input is unknown."""
    if not (approx_bronze_gb > 0 and file_size_mb > 0 and max_files_per_trigger > 0):
        return None
    if not bronze_trigger_s:
        return None
    files = approx_bronze_gb * 1024.0 / file_size_mb
    triggers = -(-files // max_files_per_trigger)  # ceil
    return float(triggers * bronze_trigger_s)


_DOWNLOAD = re.compile(
    r"\[FAILED\s*\]\s*(\S+?)!\S*:\s*Downloaded file size \((\d+)\) doesn't match "
    r"expected Content Length \((\d+)\)"
)


def classify_submission_failure(message: str | None) -> str:
    """A one-line reason for a SUBMISSION_FAILED status message.

    The operator runs spark-submit, which resolves ``spark.jars.packages``
    with Ivy; a truncated Maven download (0 bytes of a 63 MB jar in the
    2026-09-27 discovery run) is the common case and is named with the
    artifact and byte counts. Anything else is the message's first line.
    """
    text = message or ""
    m = _DOWNLOAD.search(text)
    if m:
        return (
            f"Maven dependency download failed: {m.group(1)} arrived with {int(m.group(2)):,} "
            f"of {int(m.group(3)):,} bytes"
        )
    if "FAILED DOWNLOADS" in text or "unresolved dependency" in text.lower():
        return "Maven dependency resolution failed: " + text.strip().splitlines()[0][:300]
    first = text.strip().splitlines()[0] if text.strip() else "no message"
    return first[:300]


def settle_state(
    events_by_job: dict[str, list[StreamEvent]], datagen_rows: int
) -> tuple[bool, str]:
    """Whether the pipeline has taken in and published the whole corpus.

    Settled when every datagen row reached bronze, silver committed every
    one of them, and a gold cycle read silver after silver's last commit and
    finished its refresh. Pod log timestamps only, so no CLI clock enters.
    """
    if datagen_rows <= 0:
        return False, "datagen row count not measured"
    bronze = events_by_job.get("bronze-ingest") or []
    silver = events_by_job.get("silver-stream") or []
    gold = events_by_job.get("gold-refresh") or []
    b_rows = sum(e.rows or 0 for e in bronze if e.kind == "write")
    if b_rows < datagen_rows:
        return False, f"bronze has {b_rows:,} of {datagen_rows:,} rows"
    rows_in: dict[int, int] = {}
    committed: dict[int, datetime] = {}
    for e in silver:
        if e.kind == "transform":
            rows_in[e.ident] = e.rows or 0
        elif e.kind == "commit":
            committed[e.ident] = e.at
    s_rows = sum(rows_in.get(b, 0) for b in committed)
    if s_rows < b_rows:
        return False, f"silver has committed {s_rows:,} of {b_rows:,} bronze rows"
    last_commit = max(committed.values()) if committed else None
    read_after = {
        e.ident for e in gold if e.kind == "aggregate" and last_commit and e.at > last_commit
    }
    if not any(e.kind == "refreshed" and e.ident in read_after for e in gold):
        return False, "no gold refresh has read silver since its last commit"
    return True, "settled"
