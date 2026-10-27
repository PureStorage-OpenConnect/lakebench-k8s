"""The continuous-mode measurement window.

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

import bisect
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

#: Commits a stage needs inside the window for the run to count as
#: continuous processing: one commit is a single pass, not a stream.
MIN_WINDOW_COMMITS = 2

_LINE = re.compile(r"\[lb\] (\d{4}-\d\d-\d\dT[\d:.]+)Z? - (.*)$")
_WRITE = re.compile(r"Batch (\d+): writing ([\d,]+) rows")
_BRONZE_COMMIT = re.compile(r"Batch (\d+): committed in ([\d.]+)s")
_TRANSFORM = re.compile(r"Batch (\d+): transforming ([\d,]+) rows")
_OUTPUT = re.compile(r"Batch (\d+): ([\d,]+) rows after transforms")
_SILVER_COMMIT = re.compile(r"Batch (\d+): committed to \S+ in ([\d.]+)s")
_AGGREGATE = re.compile(r"Cycle (\d+): aggregating ([\d,]+) Silver records")
_REFRESHED = re.compile(r"Cycle (\d+): refreshed \S+ in ([\d.]+)s(?: \(([\d,]+) KPI records\))?")
# AML gold: the whole tick (detection, baseline, time to detect, TM), logged
# at its end.
_TICK_TOTAL = re.compile(r"Cycle (\d+): tick timing .*?(?: tm=([\d.]+)s)? total=([\d.]+)s")
_FRESHNESS = re.compile(r"Cycle (\d+): data freshness ([\d.]+)s( \(silver idle\))?")
_LANDING = re.compile(r"\[landing\] batch=(\d+) oldest=([\d.]+) newest=([\d.]+)")
# AML gold: a cycle with no silver row new since the one before it.
_NOTHING_NEW = re.compile(r"Cycle (\d+): earliest new event time \(us\) None\s*$")


@dataclass(frozen=True)
class StreamEvent:
    """One timestamped stage event (UTC, naive)."""

    at: datetime
    kind: str  # write, commit, landing, transform, output, aggregate, refreshed, tick, freshness, nothing_new
    ident: int  # batch id (bronze, silver) or cycle number (gold)
    rows: int | None = None
    value: float | None = None
    idle: bool = False
    # AML gold tick: seconds of the tick the TM operations pass took.
    tm: float | None = None


def _num(text: str) -> int:
    return int(text.replace(",", ""))


def _ts(text: str) -> datetime | None:
    try:
        return utc_naive(datetime.fromisoformat(text))
    except ValueError:
        return None


def utc_naive(at: datetime) -> datetime:
    """*at* as a naive UTC datetime, the form the log lines use."""
    if at.tzinfo is None:
        return at
    return at.astimezone(timezone.utc).replace(tzinfo=None)


def committed_writes(events: list[StreamEvent]) -> list[StreamEvent]:
    """Bronze ``write`` events whose batch logged its commit, one per batch id.
    A batch whose stream was stopped mid-write logs ``writing`` but never
    commits, so its rows never reached the table and must not be counted as
    ingested. At most one write per batch id is returned: a driver-log capture
    resumed after the kubelet severed its tail re-fetches a ~1 s overlap, so
    the same ``Batch N: writing`` line can appear twice, and the callers sum
    ``rows`` per event. Keep the last write seen for each id (silver dedups the
    same way via a per-ident dict); a duplicate from any cause is counted once."""
    done = {e.ident for e in events if e.kind == "commit"}
    by_ident: dict[int, StreamEvent] = {}
    for e in events:
        if e.kind == "write" and e.ident in done:
            by_ident[e.ident] = e
    # Log order, by the write's own position: the callers take min/max of the
    # timestamps, so order is not relied on, but keep it stable and intuitive.
    return sorted(by_ident.values(), key=lambda e: e.at)


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
                # value: the batch's own time, start to commit.
                events.append(StreamEvent(at, "commit", int(c.group(1)), value=float(c.group(2))))
                continue
            land = _LANDING.search(msg)
            if land:
                # value: the newest landing time (epoch s) the batch took in.
                events.append(
                    StreamEvent(at, "landing", int(land.group(1)), value=float(land.group(3)))
                )
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
                events.append(StreamEvent(at, "commit", int(c.group(1)), value=float(c.group(2))))
        elif job_type == "gold-refresh":
            n = _NOTHING_NEW.search(msg)
            if n:
                events.append(StreamEvent(at, "nothing_new", int(n.group(1))))
                continue
            a = _AGGREGATE.search(msg)
            if a:
                events.append(StreamEvent(at, "aggregate", int(a.group(1)), rows=_num(a.group(2))))
                continue
            r = _REFRESHED.search(msg)
            if r:
                kpi = _num(r.group(3)) if r.group(3) else None
                events.append(
                    StreamEvent(at, "refreshed", int(r.group(1)), rows=kpi, value=float(r.group(2)))
                )
                continue
            k = _TICK_TOTAL.search(msg)
            if k:
                events.append(
                    StreamEvent(
                        at,
                        "tick",
                        int(k.group(1)),
                        value=float(k.group(3)),
                        tm=float(k.group(2)) if k.group(2) else None,
                    )
                )
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
      of batches whose commit landed inside it. Gold: silver rows read by the
      cycles that started inside it (every cycle re-reads silver).
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
      window, with the trailing idle run split off.
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
        "first_write_offset_seconds": None,
        # Bronze: its first write of the run, window-relative (negative when
        # before the window): when the trickle started releasing files.
        "trickle_start_offset_seconds": None,
        "write_batches": 0,
        # Totals as of the window's end (logs are read after it closes).
        "rows_to_end": 0,
        "committed_rows_to_end": None,
        # Offsets (s from the window start) of silver commits with rows and
        # of gold refreshes on new data, inside the window.
        "commit_offsets": [],
    }
    if job_type == "bronze-ingest":
        writes = [e for e in committed_writes(events) if e.at <= end]
        inside = [e for e in writes if e.at >= start]
        out["window_input_rows"] = sum(e.rows or 0 for e in inside)
        out["pre_window_input_rows"] = sum(e.rows or 0 for e in writes if e.at < start)
        out["window_commits"] = sum(
            1 for e in events if e.kind == "commit" and _inside(e.at, start, end)
        )
        out["output_rows"] = sum(e.rows or 0 for e in writes) if writes else 0
        out["rows_to_end"] = out["output_rows"]
        out["write_batches"] = len(inside)
        if writes:
            out["trickle_start_offset_seconds"] = (
                min(e.at for e in writes) - start
            ).total_seconds()
        if inside:
            out["last_write_offset_seconds"] = (max(e.at for e in inside) - start).total_seconds()
            out["first_write_offset_seconds"] = (min(e.at for e in inside) - start).total_seconds()
    elif job_type == "silver-stream":
        rows_in: dict[int, int] = {}
        rows_out: dict[int, int] = {}
        committed_in: set[int] = set()
        committed_before: set[int] = set()
        commit_at: dict[int, datetime] = {}
        transformed = 0
        for e in events:
            if e.at > end:
                continue
            if e.kind == "transform":
                rows_in[e.ident] = e.rows or 0
                transformed += e.rows or 0
            elif e.kind == "output":
                rows_out[e.ident] = e.rows or 0
            elif e.kind == "commit":
                (committed_in if e.at >= start else committed_before).add(e.ident)
                if e.at >= start:
                    commit_at[e.ident] = e.at
        committed_before -= committed_in
        out["rows_to_end"] = transformed
        if committed_in or committed_before:
            out["committed_rows_to_end"] = sum(
                rows_in.get(b, 0) for b in committed_in | committed_before
            )
        out["commit_offsets"] = sorted(
            (commit_at[b] - start).total_seconds() for b in committed_in if rows_in.get(b, 0) > 0
        )
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
        agg_at = {e.ident: e.at for e in events if e.kind == "aggregate" and e.at <= end}
        out["rows_to_end"] = sum(agg.values())
        out["window_input_rows"] = sum(n for c, n in agg.items() if agg_at[c] >= start)
        out["pre_window_input_rows"] = sum(n for c, n in agg.items() if agg_at[c] < start)
        cycles = sorted(agg)
        # The rows the previous cycle read, per cycle, in one pass.
        prev_rows: dict[int, int] = {}
        running: int | None = None
        for c in cycles:
            if running is not None:
                prev_rows[c] = running
            running = agg[c]

        def saw_new_data(cycle: int) -> bool:
            # A cycle that read no silver (empty or not ready), read no more
            # rows than the cycle before, or was tagged idle saw no new data.
            if idle.get(cycle) or not agg.get(cycle):
                return False
            return cycle not in prev_rows or agg[cycle] > prev_rows[cycle]

        new_at = [e for e in inside if saw_new_data(e.ident)]
        out["window_new_data_cycles"] = len(new_at)
        out["commit_offsets"] = [(e.at - start).total_seconds() for e in new_at]
        if fresh:
            out["freshness_seconds"] = max(e.value or 0.0 for e in fresh)
            # Trailing idle: the cycles after gold last saw new data, by the
            # idle tag (c360) or by silver rows not growing (AML has no tag).
            cut = len(fresh)
            while cut > 0 and not saw_new_data(fresh[cut - 1].ident):
                cut -= 1
            out["trailing_idle_cycles"] = len(fresh) - cut
            if cut > 0:
                out["freshness_active_seconds"] = max(e.value or 0.0 for e in fresh[:cut])
    return out


#: Share of the window data must keep arriving for: a corpus that runs out
#: earlier leaves a window that mostly measures an idle pipeline.
MIN_ARRIVAL_FRACTION = 0.5


def window_gate_problems(
    stats_by_job: dict[str, dict[str, Any] | None],
    window_seconds: float | None = None,
    min_commits: int = MIN_WINDOW_COMMITS,
    *,
    continuous_datagen: bool = False,
) -> list[str]:
    """Reasons a continuous run is not continuous processing (invariant 3).

    *stats_by_job* maps each submitted stream to its ``window_stats``, or
    None when its driver log could not be read. A run passes only when data
    arrived during the window (bronze ingested rows inside it), silver
    committed and gold refreshed on new data at least *min_commits* times
    inside it, and gold freshness was measured inside it. Silver commits and
    gold refreshes count only from bronze's first write inside the window,
    so a pipeline chewing through a pre-window backlog is not continuous.
    With *window_seconds*, bronze must also write at least *min_commits*
    batches inside the window, the last at or after
    MIN_ARRIVAL_FRACTION of it.
    """
    problems: list[str] = []
    for job, stats in stats_by_job.items():
        if stats is None:
            problems.append(
                f"continuous gate: no {job} batch lines in its driver log (it processed "
                "nothing, or the log could not be read); cannot show it processed data "
                "during the window"
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
        elif window_seconds:
            batches = bronze.get("write_batches") or 0
            last = bronze.get("last_write_offset_seconds") or 0.0
            if batches < min_commits or last < MIN_ARRIVAL_FRACTION * window_seconds:
                problems.append(
                    f"continuous gate: data stopped arriving {last:.0f}s into the "
                    f"{window_seconds:.0f}s window ({batches} bronze batch(es) inside it, "
                    f"{before:,} rows before it): at least {min_commits} batches and arrival "
                    f"through {MIN_ARRIVAL_FRACTION:.0%} of the window are needed. "
                    + (
                        "Either arrival exceeded what bronze takes in (its batches grow until "
                        "one fills the window): offer less with workload.datagen.cpu and "
                        "parallelism; or datagen "
                        "stopped writing: check the datagen pod logs"
                        if continuous_datagen
                        else "Lower max_files_per_trigger so the trickle lasts the window"
                    )
                )
    first = (bronze or {}).get("first_write_offset_seconds")

    def after_arrival(stats: dict[str, Any]) -> int:
        offsets = stats.get("commit_offsets")
        if offsets is None or first is None:
            new = stats.get("window_new_data_cycles")
            return int(new if new is not None else stats.get("window_commits") or 0)
        return sum(1 for o in offsets if o >= first)

    silver = stats_by_job.get("silver-stream")
    if silver is not None:
        n = after_arrival(silver) if bronze is not None else silver["window_commits"]
        if n < min_commits:
            problems.append(
                f"continuous gate: silver committed {n} micro-batch(es) with rows inside the "
                f"window after data arrived in it; continuous processing needs at least "
                f"{min_commits}"
            )
    gold = stats_by_job.get("gold-refresh")
    if gold is not None:
        new = after_arrival(gold) if bronze is not None else (gold["window_new_data_cycles"] or 0)
        if new < min_commits:
            problems.append(
                f"continuous gate: gold refreshed on new silver data {new} time(s) inside the "
                f"window after data arrived ({gold['window_commits']} refresh(es) in all); "
                "continuous processing "
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


def released_rows(
    window_s: float,
    trickle_start_offset_s: float | None,
    trigger_s: float | None,
    files_per_trigger: int | None,
    corpus_rows: int,
    corpus_files: int,
) -> int | None:
    """Rows the trickle had released to bronze by the window's end: one
    batch of *files_per_trigger* files per trigger from bronze's first write
    to the window's end, at the corpus's mean rows per file, never more than
    the corpus. None when any input is unknown."""
    if (
        trickle_start_offset_s is None
        or not trigger_s
        or not files_per_trigger
        or corpus_rows <= 0
        or corpus_files <= 0
    ):
        return None
    triggers = int(max(0.0, window_s - trickle_start_offset_s) // trigger_s) + 1
    files = min(corpus_files, triggers * files_per_trigger)
    return min(corpus_rows, round(files * corpus_rows / corpus_files))


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

    An application that sets ``spark.jars.packages`` resolves it with Ivy; a
    truncated Maven download is named with the artifact and byte counts.
    Lakebench's own jobs name their jars as lb-deps URLs instead. Anything
    else is the message's first line.
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


#: BOUNDED BY trickle needs ingested / offered rows at or
#: above this, and lag at window end within one trigger interval.
TRICKLE_KEPT_PACE_RATIO = 0.99
#: Window seconds are recorded to 0.1 s; the last write is not.
_LAG_ROUNDING_S = 0.05


def trickle_kept_pace(
    *,
    ingested_rows: float | None,
    released_rows: float | None,
    corpus_taken: bool,
    window_s: float | None,
    last_write_offset_s: float | None,
    trigger_s: float | None,
) -> dict[str, Any]:
    """Whether bronze kept pace with what the trickle offered:
    ingested / offered rows >= TRICKLE_KEPT_PACE_RATIO, offered rows
    being the rows the trickle had released (``released_rows``), and lag at
    window end (window seconds less bronze's last write) <= one trigger
    interval.

    When bronze had taken the whole corpus there was nothing left to offer:
    a last write long before the window end is not falling behind, so the
    lag is not tested. ``kept_pace`` is True, False, or None with
    ``not_measured`` naming the input that was missing.
    """
    out: dict[str, Any] = {
        "kept_pace": None,
        "ingested_rows": ingested_rows,
        "offered_rows": released_rows,
        "ratio": None,
        "lag_s": None,
        "trigger_s": trigger_s,
    }
    if ingested_rows is None or not released_rows:
        out["not_measured"] = "the rows the trickle offered are not known"
        return out
    ratio = float(ingested_rows) / float(released_rows)
    out["ratio"] = round(ratio, 4)
    lag_known = window_s is not None and last_write_offset_s is not None and bool(trigger_s)
    if lag_known and not corpus_taken:
        out["lag_s"] = round(float(window_s) - float(last_write_offset_s), 1)  # type: ignore[arg-type]
    if ratio < TRICKLE_KEPT_PACE_RATIO:
        out["kept_pace"] = False
        return out
    if corpus_taken:
        out["kept_pace"] = True
        out["lag_note"] = "bronze took the whole corpus; nothing was left to offer"
        return out
    if not lag_known:
        out["not_measured"] = "the lag at window end is not known"
        return out
    lag = float(window_s) - float(last_write_offset_s)  # type: ignore[arg-type]
    out["kept_pace"] = lag <= float(trigger_s) + _LAG_ROUNDING_S  # type: ignore[arg-type]
    return out


# ---------------------------------------------------------------------------
# Lag per handoff and balance
# ---------------------------------------------------------------------------

#: The handoffs, upstream first: (name, upstream, the stage that consumes).
HANDOFFS = (
    ("datagen->bronze", "datagen", "bronze-ingest"),
    ("bronze->silver", "bronze-ingest", "silver-stream"),
    ("silver->gold", "silver-stream", "gold-refresh"),
)
#: Seconds between the lag samples a run records.
LAG_SAMPLE_SECONDS = 30.0
#: A stage whose intake is row-capped keeps up only when it consumed at least
#: this share of the rows its upstream committed inside the window, less one
#: capped batch for the window-edge clock skew (see ``balance``). A row cap
#: bounds a micro-batch's size, not its rate, so the handoff lag stays near one
#: batch while the unread backlog grows: rows consumed / rows committed is the
#: signal the cap does not distort.
CAPPED_CONSUMED_RATIO = 0.99


def _epoch(at: datetime) -> float:
    return at.replace(tzinfo=timezone.utc).timestamp()


def _last_by_id(events: list[StreamEvent], kind: str) -> dict[int, StreamEvent]:
    """The last *kind* event per batch id: a replayed batch counts once."""
    out: dict[int, StreamEvent] = {}
    for e in events:
        if e.kind == kind:
            out[e.ident] = e
    return out


def handoff_lags(events_by_job: dict[str, list[StreamEvent]], at: datetime) -> dict[str, Any]:
    """Seconds the oldest upstream commit not yet taken by the next stage has
    waited, per handoff, at *at* (naive UTC); None when the handoff has no
    upstream commit yet.

    - datagen->bronze: datagen lands files continuously, so the oldest file
      bronze has not taken landed just after the newest one its last
      committed batch took (``[landing]`` lines; the landing time is the
      object store's clock).
    - bronze->silver and silver->gold: a micro-batch (a gold cycle) takes
      every upstream commit before it starts, so the oldest upstream commit
      after the stage's last start is waiting. Times only, so a driver log
      that lost its early lines (a restart) moves nothing.
    """
    return handoff_lags_at(_HandoffIndex(events_by_job), at)


class _HandoffIndex:
    """The per-stage times ``handoff_lags`` reads, sorted once so a sample at
    any *at* is a bisect, not a scan: landing times (datagen->bronze), each
    upstream stage's commit times, and each consumer's batch-start times."""

    def __init__(self, events_by_job: dict[str, list[StreamEvent]]):
        bronze = events_by_job.get("bronze-ingest") or []
        silver = events_by_job.get("silver-stream") or []
        gold = events_by_job.get("gold-refresh") or []
        self.landings = sorted(
            (e.at, e.value) for e in bronze if e.kind == "landing" and e.value is not None
        )
        self.commits = {
            "bronze->silver": sorted(e.at for e in bronze if e.kind == "commit"),
            "silver->gold": sorted(e.at for e in silver if e.kind == "commit"),
        }
        self.starts = {
            "bronze->silver": sorted(e.at for e in silver if e.kind == "transform"),
            "silver->gold": sorted(e.at for e in gold if e.kind == "aggregate"),
        }


def handoff_lags_at(index: _HandoffIndex, at: datetime) -> dict[str, Any]:
    """``handoff_lags`` from a prebuilt ``_HandoffIndex``, for a caller that
    samples the same events at many times (``balance``, ``_phase_samples``)."""
    out: dict[str, Any] = {name: None for name, _, _ in HANDOFFS}

    landed = [v for t, v in index.landings if t <= at and v is not None]
    if landed:
        out["datagen->bronze"] = max(0.0, _epoch(at) - max(landed))

    def waiting(name: str) -> Any:
        commits, starts = index.commits[name], index.starts[name]
        n_commit = bisect.bisect_right(commits, at)
        if n_commit == 0:
            return None
        n_start = bisect.bisect_right(starts, at)
        last = starts[n_start - 1] if n_start else None
        # The oldest commit at or before *at* that the stage's last start did
        # not take: the first commit after ``last`` (all of them when the
        # stage has not started).
        first_pending = bisect.bisect_right(commits, last) if last is not None else 0
        if first_pending >= n_commit:
            return 0.0
        return max(0.0, (at - commits[first_pending]).total_seconds())

    out["bronze->silver"] = waiting("bronze->silver")
    out["silver->gold"] = waiting("silver->gold")
    return out


def _finished(events: list[StreamEvent], job_type: str) -> dict[int, StreamEvent]:
    """Per batch (gold: cycle) id, the event that ends it, carrying its
    seconds. A gold cycle with a ``tick`` line ends there: ``refreshed``
    covers detection only, not the time to detect and TM pass after it."""
    if job_type != "gold-refresh":
        return _last_by_id(events, "commit")
    return {**_last_by_id(events, "refreshed"), **_last_by_id(events, "tick")}


def _batch_spans(events: list[StreamEvent], job_type: str) -> list[tuple[datetime, datetime]]:
    """(start, end) of each micro-batch (gold: cycle), from the time it logs
    with its end; a replayed batch counts once."""
    return [
        (e.at - timedelta(seconds=e.value or 0.0), e.at)
        for e in _finished(events, job_type).values()
        if e.value is not None
    ]


def _tm_share(events: list[StreamEvent], start: datetime, end: datetime) -> float:
    """Share of gold's tick time inside [start, end] its TM passes took."""
    ticks = [e for e in _last_by_id(events, "tick").values() if start <= e.at <= end]
    total = sum(e.value or 0.0 for e in ticks)
    return sum(e.tm or 0.0 for e in ticks) / total if total > 0 else 0.0


def _busy_share(events: list[StreamEvent], job_type: str, start: datetime, end: datetime) -> float:
    """Share of [start, end] the stage spent inside micro-batches (or cycles)."""
    busy = 0.0
    for began, done in _batch_spans(events, job_type):
        lo, hi = max(began, start), min(done, end)
        if hi > lo:
            busy += (hi - lo).total_seconds()
    span = (end - start).total_seconds()
    return min(1.0, busy / span) if span > 0 else 0.0


def _median_batch_seconds(
    events: list[StreamEvent], job_type: str, since: datetime, until: datetime
) -> float:
    """Median batch time over the batches that ended inside [since, until],
    or over all of them when none did. A gold cycle that found no new silver
    row is left out: it is a quick pass over nothing, not the stage's
    cadence, and counting it reads a back-to-back AML gold as a cadence far
    below its real tick time."""
    idle = {e.ident for e in events if e.kind == "nothing_new"}
    spans = [
        (e.at - timedelta(seconds=e.value or 0.0), e.at)
        for ident, e in _finished(events, job_type).items()
        if e.value is not None and ident not in idle
    ]
    inside = [(b, d) for b, d in spans if since <= d <= until]
    times = sorted((d - b).total_seconds() for b, d in (inside or spans))
    return times[len(times) // 2] if times else 0.0


#: The executor-count key a stage is resized with.
_EXECUTOR_KNOB = {
    "bronze-ingest": "bronze_executors",
    "silver-stream": "silver_executors",
    "gold-refresh": "gold_executors",
}


def _phase_samples(
    events_by_job: dict[str, list[StreamEvent]], name: str
) -> list[tuple[datetime, float]]:
    """One lag sample per batch of the handoff's consuming stage, each at the
    same phase of the batch (see ``balance``)."""
    if name == "datagen->bronze":
        return [
            (e.at, max(0.0, _epoch(e.at) - e.value))
            for e in events_by_job.get("bronze-ingest") or []
            if e.kind == "landing" and e.value is not None
        ]
    stage, kind = (
        ("silver-stream", "transform")
        if name == "bronze->silver"
        else ("gold-refresh", "aggregate")
    )
    index = _HandoffIndex(events_by_job)
    out = []
    for e in _last_by_id(events_by_job.get(stage) or [], kind).values():
        lag = handoff_lags_at(index, e.at - timedelta(milliseconds=1))[name]
        if lag is not None:
            out.append((e.at, lag))
    return sorted(out)


def _growth(samples: list[tuple[datetime, float]], seconds: float) -> float:
    """How far the least-squares trend of *samples* rises over *seconds*."""
    t0 = samples[0][0]
    xs = [(t - t0).total_seconds() for t, _ in samples]
    ys = [lag for _, lag in samples]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    var = sum((x - mx) ** 2 for x in xs)
    if var == 0:
        return 0.0
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / var
    return slope * seconds


def balance(
    events_by_job: dict[str, list[StreamEvent]],
    start: datetime,
    end: datetime,
    *,
    shape: dict[str, dict[str, Any]] | None = None,
    ran: dict[str, int] | None = None,
    intervals: dict[str, float | None] | None = None,
    unjudged: dict[str, str] | None = None,
    capped_stages: dict[str, int] | None = None,
    window_rows: dict[str, int | None] | None = None,
    step_s: float = LAG_SAMPLE_SECONDS,
) -> dict[str, Any]:
    """Whether every stage kept up inside the window [*start*, *end*].

    A stage keeps up when its lag did not climb through the window's second
    half, so the start-up surge (and the empty pipeline's ramp) is ignored
    and a growing backlog is caught. The lag is sampled once per batch of
    the stage at the same phase (bronze at each commit, from the newest file
    it took; silver and gold as each batch starts, from the oldest upstream
    commit waiting), so the sawtooth of a lag that swings by a batch time
    does not read as a trend. The stage keeps up when the least-squares
    trend of those samples rose less than one cadence across the second
    half: its trigger interval from *intervals* (a timer's lag saws up to
    it), or, back to back, its median batch time in the first half. With fewer than
    three samples there, its lag at the end must be no larger than the most
    it reached in the first half, or within two cadences. *shape* is the
    run record's ``streaming_shape`` and names the executors to raise;
    *ran* the executor count each stage was submitted with (after the
    concurrent budget), which the advice reports when it differs.
    *unjudged* maps a handoff to why its lag says nothing about the stage
    (a corpus written before the run, an intake cap): it is recorded, not
    judged.

    *capped_stages* maps a consuming stage to its per-trigger row cap when one
    is set (silver's ``silver_max_rows_per_trigger``). A row cap bounds a
    micro-batch's size, not its rate, so the handoff lag sits near one batch
    while the unread backlog grows and reads as keeping up when it is not. For
    such a handoff the pass rule is instead rows consumed by the stage inside
    the window over rows its upstream committed inside it
    (*window_rows*, each stage's ``window_input_rows``), which the cap does not
    distort: it keeps up when that share is at least ``CAPPED_CONSUMED_RATIO``,
    allowing a shortfall of one capped batch. The two counts are clocked
    differently at the window's edges (bronze counts a batch by its write,
    the consumer by its commit), so a consumer keeping pace can trail by the
    batch in flight at each boundary; the one-batch allowance forgives exactly
    that. The lag fields are still recorded for the handoff, as evidence, not
    as the verdict.
    """
    span = (end - start).total_seconds()
    offsets: list[float] = []
    t = 0.0
    while t < span:
        offsets.append(t)
        t += step_s
    offsets.append(span)
    index = _HandoffIndex(events_by_job)
    samples = [(o, handoff_lags_at(index, start + timedelta(seconds=o))) for o in offsets]
    handoffs: dict[str, Any] = {}
    for name, upstream, stage in HANDOFFS:
        if stage not in events_by_job:
            continue
        series = [(o, lags[name]) for o, lags in samples if lags[name] is not None]
        if not series:
            continue
        first = [lag for o, lag in series if o <= span / 2]
        first_max = max(first) if first else 0.0
        at_end = series[-1][1] if series[-1][0] == span else 0.0
        # Back to back, a batch takes what landed while the last one ran, so
        # the lag swings up to about two batch times. The batch time is the
        # first half's: a stage falling behind runs longer batches, and an
        # allowance taken from them would grow with the backlog it judges.
        mid = start + timedelta(seconds=span / 2)
        cadence = max(
            _median_batch_seconds(events_by_job.get(stage) or [], stage, start, mid),
            float((intervals or {}).get(stage) or 0.0),
        )
        allow = 2 * cadence
        late = [(t, lag) for t, lag in _phase_samples(events_by_job, name) if mid <= t <= end]
        growth = _growth(late, (end - mid).total_seconds()) if len(late) >= 3 else None
        kept = growth <= cadence if growth is not None else at_end <= max(first_max, allow)
        # A row-capped consuming stage is judged by rows, not lag: the cap
        # holds the lag near one batch while a backlog grows unseen. Falls back
        # to the lag verdict when either row count is missing (unmeasured).
        cap = (capped_stages or {}).get(stage)
        upstream_rows = (window_rows or {}).get(upstream)
        stage_rows = (window_rows or {}).get(stage)
        consumed_ratio = None
        if cap and upstream_rows is not None and stage_rows is not None and upstream_rows > 0:
            consumed_ratio = stage_rows / upstream_rows
            # The two counts are clocked differently at the window's edges:
            # bronze counts a batch by its write, silver by its commit, so a
            # silver keeping pace can still trail by up to the batch in flight
            # at each boundary. Forgive one capped batch's worth of rows before
            # calling it behind, so the default cap does not read a keeping-pace
            # silver as a backlog.
            kept = stage_rows >= CAPPED_CONSUMED_RATIO * upstream_rows - cap
        handoffs[name] = {
            "stage": stage,
            "upstream": upstream,
            "samples": [[round(o, 1), round(lag, 1)] for o, lag in series],
            "first_half_max_s": round(first_max, 1),
            "end_s": round(at_end, 1),
            "allowance_s": round(allow, 1),
            "cadence_s": round(cadence, 1),
            # Rise of the per-batch lag trend across the second half, and
            # the per-batch samples (seconds into the window, lag) it is
            # fitted to.
            "second_half_growth_s": round(growth, 1) if growth is not None else None,
            "trend_samples": [
                [round((t - start).total_seconds(), 1), round(lag, 1)] for t, lag in late
            ],
            # Set only for a row-capped handoff: rows the stage consumed inside
            # the window over rows its upstream committed inside it, and the cap
            # that made lag the wrong signal.
            **(
                {"consumed_ratio": round(consumed_ratio, 4), "row_cap": cap}
                if consumed_ratio is not None
                else {}
            ),
            "keeps_up": kept or name in (unjudged or {}),
            **({"not_judged": (unjudged or {})[name]} if name in (unjudged or {}) else {}),
            "busy_share": round(_busy_share(events_by_job.get(stage) or [], stage, start, end), 3),
        }
    behind = [h for h in handoffs.values() if not h["keeps_up"]]
    lever: str | None = None
    if behind:

        def _severity(h: dict[str, Any]) -> tuple[int, float]:
            # A row-capped stage fell behind by rows, not lag, so its lag
            # fields understate it: rank it first, by how little it consumed.
            if "consumed_ratio" in h:
                return (1, 1.0 - h["consumed_ratio"])
            lag = (
                h["second_half_growth_s"]
                if h["second_half_growth_s"] is not None
                else h["end_s"] - h["first_half_max_s"]
            )
            return (0, lag)

        worst = max(behind, key=_severity)
        stage = worst["stage"]
        knob = _EXECUTOR_KNOB[stage]
        need = ((shape or {}).get(stage) or {}).get("balance_need")
        planned = ((shape or {}).get(stage) or {}).get("executors")
        have = (ran or {}).get(stage) or planned
        from lakebench.config.schema import MAX_EXECUTOR_OVERRIDE

        lever = f"platform.compute.spark.{knob}"
        advice = f"raise {lever}"
        if planned and have and have < planned:
            lever = "cluster capacity"
            advice = (
                f"it ran {have} executors, cut from the {planned} planned by the cluster's "
                "concurrent budget: free cluster capacity"
            )
            sized = ""
        elif need and need > MAX_EXECUTOR_OVERRIDE:
            lever = "platform.compute.spark." + knob.replace("_executors", "_executor_cores")
            advice = f"raise {lever}"
            sized = (
                f" (the offered load needs ~{need} executors, above the "
                f"{MAX_EXECUTOR_OVERRIDE} a stage takes)"
            )
        elif (
            stage == "gold-refresh"
            and (share := _tm_share(events_by_job.get(stage) or [], start, end)) >= 0.25
        ):
            lever = "workload.tm_operations.continuous_interval_seconds"
            advice = (
                f"the TM operations pass took {share:.0%} of gold's time: raise "
                "workload.tm_operations.continuous_interval_seconds, or raise "
                f"platform.compute.spark.{knob}"
            )
            sized = ""
        elif need and have and have >= need:
            sized = (
                f" (has {have}, which the sizing default rate expected to carry the load: "
                "its measured rate is below the default)"
            )
        elif need and have:
            sized = f" (has {have}, the offered load needs ~{need})"
        else:
            sized = ""
        if "consumed_ratio" in worst:
            rose = (
                f"it read {worst['consumed_ratio']:.0%} of the rows its upstream committed "
                f"inside the window (its {worst['row_cap']:,}-row per-trigger cap bounds batch "
                "size, not rate, so its lag stayed flat while the backlog grew)"
            )
        elif worst["second_half_growth_s"] is not None:
            rose = (
                f"its lag grew {worst['second_half_growth_s']:.0f}s across the window's second "
                f"half (one cadence is {worst['cadence_s']:.0f}s); {worst['end_s']:.0f}s at the "
                "window's end"
            )
        else:
            rose = (
                f"its lag rose to {worst['end_s']:.0f}s at the window's end from at most "
                f"{worst['first_half_max_s']:.0f}s in the first half"
            )
        line = (
            f"not balanced: {stage} fell behind {worst['upstream']}: {rose}, busy "
            f"{worst['busy_share']:.0%}; {advice}{sized} or lower the scale"
        )
    elif handoffs:
        busiest = max(handoffs.values(), key=lambda h: h["busy_share"])
        line = (
            f"balanced: busiest stage {busiest['stage']}, busy "
            f"{busiest['busy_share']:.0%} of the window"
        )
    else:
        line = "balance not measured: no handoff had a commit inside the window"
    return {
        "balanced": bool(handoffs) and not behind,
        "measured": bool(handoffs),
        "handoffs": handoffs,
        "bottleneck": line,
        # The setting the bottleneck line says to change (None: balanced or
        # not measured; "cluster capacity" when the cluster budget cut the
        # stage's executors).
        "lever": lever,
        "sample_seconds": step_s,
    }


def lag_line(lags: dict[str, Any]) -> str:
    """``datagen->bronze 12s, bronze->silver 40s, silver->gold 95s``."""
    parts = [f"{name} {lags[name]:.0f}s" for name, _, _ in HANDOFFS if lags.get(name) is not None]
    return ", ".join(parts) if parts else "no handoff yet"


def freshness_summary(gold: list[StreamEvent], start: datetime, end: datetime) -> dict[str, Any]:
    """p50, p95 and max of gold's freshness over the cycles inside the window
    that saw new data; None when no cycle measured it."""
    values = sorted(
        e.value
        for e in gold
        if e.kind == "freshness"
        and not e.idle
        and e.value is not None
        and _inside(e.at, start, end)
    )
    if not values:
        return {"p50_s": None, "p95_s": None, "max_s": None, "cycles": 0}

    def pct(q: float) -> float:
        return values[min(len(values) - 1, int(round(q * (len(values) - 1))))]

    return {
        "p50_s": round(pct(0.5), 1),
        "p95_s": round(pct(0.95), 1),
        "max_s": round(values[-1], 1),
        "cycles": len(values),
    }


_CLOCK = re.compile(r"\[landing\] object store clock is ([-+][\d.]+)s")


def store_clock_offset(bronze_log: str | None) -> float | None:
    """How far the object store's clock ran from the bronze pod's, from
    bronze's ``[landing]`` clock line; None when the line is absent."""
    found = _CLOCK.findall(bronze_log or "")
    return float(found[-1]) if found else None
