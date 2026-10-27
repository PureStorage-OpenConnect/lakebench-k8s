"""Time-travel reads of a continuous AML run (``continuous.time_travel``).

After the window, ``aml/time_travel_queries.retention_record`` checks the
transactions snapshots the gold-refresh ticks recorded
(``metrics/tick_records.time_travel_ticks``) through the query engine. This
module holds the shared pieces of that record: the maintenance round that
expired an expired snapshot (``expired_by``, over ``continuous.retention.
rounds``), the retention policy it is read under (``policy``), the per-tick
verdict (``verdict_of``), the not-run record and the one-line summary. The
check never fails the run; it is shown beside the verdict.

Pure: no cluster or Spark access.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

#: Record states of a snapshot read back intact (``pass`` also needs at
#: least one ``verified``).
TT_VERIFIED_STATES = ("verified", "verified_hash_only")
#: Record states that fail it.
TT_FAILING_STATES = ("mismatch", "missing_unexplained", "error", "not_supported")


def _utc(text: Any) -> datetime | None:
    """A recorded UTC time (ISO 8601, ``Z`` or naive) as an aware datetime."""
    from lakebench.metrics.tick_records import _parse_utc

    t = _parse_utc(text)
    return t.replace(tzinfo=timezone.utc) if t is not None else None


def _seconds(duration: Any) -> float | None:
    """An applied retention (``1h``, ``30m``, ``7d``) in seconds, or None."""
    from lakebench.modules.table_formats.iceberg.maintenance import _parse_threshold_seconds

    try:
        return float(_parse_threshold_seconds(str(duration)))
    except (TypeError, ValueError):
        return None


def _bare_table(name: Any) -> str:
    """``catalog.namespace.table`` or ``namespace.table`` as ``namespace.table``."""
    parts = str(name).split(".")
    return ".".join(parts[-2:])


def expired_by(
    tick: Mapping[str, Any],
    rounds: Sequence[Mapping[str, Any]],
    configured: Any,
    clock_offset_s: float | None,
) -> dict[str, Any] | None:
    """The first maintenance round that explains an expired tick snapshot,
    or None.

    A round explains it when it ran ``expire_snapshots`` on the tick's table
    (the statement finished or timed out, so it may have run) and the
    snapshot was committed (writer clock, in the cluster) before the round's
    latest possible cutoff minus the retention it applied. Trino, which runs
    AML maintenance, takes the cutoff on its own clock during the round, so
    the latest is the round's end (this host's clock) moved to the cluster
    clock by *clock_offset_s* (cluster minus host), when known. The entry
    names the earliest round that could have expired the snapshot
    (``basis``); ``ran_at`` is that round's end on this host's clock."""
    committed = _utc(tick.get("committed_at"))
    if committed is None:
        return None
    table = _bare_table(tick.get("table"))
    shift = timedelta(seconds=clock_offset_s or 0.0)
    for r in rounds:
        if table not in {_bare_table(t) for t in r.get("expired_tables") or ()}:
            continue
        ended = _utc(r.get("ended_at"))
        applied_s = _seconds(r.get("applied_expire"))
        if ended is None or applied_s is None:
            continue
        if committed < ended + shift - timedelta(seconds=applied_s):
            applied = r.get("applied_expire")
            configured_s = _seconds(configured)
            return {
                "round": r.get("round"),
                "basis": "the earliest maintenance round that could have expired it",
                "ran_at": r.get("ended_at"),
                "ran_at_clock": "host",
                "configured": configured,
                "applied": applied,
                "reason": (
                    "configured retention unknown"
                    if configured_s is None
                    else "live-stream floor"
                    if applied_s > configured_s
                    else "configured retention"
                ),
            }
    return None


def verdict_of(ticks: Sequence[Mapping[str, Any]], incomplete: bool) -> tuple[str, str]:
    """``(verdict, reason)`` of the time-travel check from the per-tick
    states (after expiry attribution): ``fail`` on any failing state, zero
    records, or no ``verified`` record (one whose scan was compared with
    the count its tick recorded: ``verified_hash_only`` alone compares
    nothing the tick recorded); ``incomplete`` when the Lakebench-imposed
    time-travel budget ran out before every snapshot was read; else
    ``pass``. The check never fails the run."""
    if not ticks:
        return "fail", "zero recorded snapshots"
    states = [str(t.get("state")) for t in ticks]
    bad = sorted({s for s in states if s in TT_FAILING_STATES})
    if bad:
        return "fail", "snapshot(s) " + ", ".join(f"{s} x{states.count(s)}" for s in bad)
    if incomplete or "not_read" in states:
        done = sum(1 for s in states if s != "not_read")
        return "incomplete", (
            f"the Lakebench-imposed time-travel budget ran out after {done} of "
            f"{len(states)} records"
        )
    if "verified" not in states:
        if "verified_hash_only" in states:
            return "fail", "no snapshot was compared with the count its tick recorded"
        return "fail", "no recorded snapshot was verified"
    unknown = sorted({s for s in states if s not in (*TT_VERIFIED_STATES, "expired")})
    if unknown:
        return "fail", "unrecognised state(s) " + ", ".join(unknown)
    return "pass", ""


def policy(retention: Mapping[str, Any] | None, configured_default: Any) -> dict[str, Any]:
    """The retention policy the time-travel result is read under:
    ``{configured, applied_expire}``, always stated (with ``skipped`` when
    the run skipped maintenance). *configured_default* is the config's
    ``retention_threshold``, for a run whose record has none."""
    retention = retention or {}
    configured = retention.get("configured")
    if configured is None:
        configured = configured_default
    out: dict[str, Any] = {
        "configured": configured,
        "applied_expire": retention.get("applied_expire"),
    }
    if retention.get("skipped"):
        out["skipped"] = retention["skipped"]
    return out


def not_run(continuous: dict, reason: str, pol: dict[str, Any], verdict: str = "not_run") -> dict:
    """Record a time-travel step that read nothing: *verdict* (``not_run``,
    or ``fail`` for a run with zero recorded snapshots) and why. Fails the
    check."""
    tt = continuous.setdefault("time_travel", {})
    tt.setdefault("ticks", [])
    tt.update({"verdict": verdict, "reason": reason, "policy": pol})
    return tt


def line(tt: Mapping[str, Any]) -> str:
    """One line for the console and the verdict qualifier: the check's
    verdict, the state counts and the retention policy, never a run FAIL."""
    states: dict[str, int] = {}
    for t in tt.get("ticks") or []:
        s = str(t.get("state"))
        states[s] = states.get(s, 0) + 1
    counts = ", ".join(f"{n} {s}" for s, n in sorted(states.items())) or "no records"
    if states:
        # The ticks come from the current gold-refresh driver pod's log.
        counts += f" of {sum(states.values())} ticks in the current driver log"
    policy = tt.get("policy") or {}
    pol = f"retention {policy.get('configured')}, applied {policy.get('applied_expire')}"
    if policy.get("skipped"):
        pol = f"maintenance skipped ({policy['skipped']})"
    verdict = str(tt.get("verdict") or "unknown")
    text = (
        f"time-travel check: {verdict if verdict == 'pass' else verdict.upper()} ({counts}; {pol})"
    )
    if tt.get("reason"):
        text += f": {tt['reason']}"
    budget = tt.get("budget") or {}
    if verdict == "incomplete" and budget.get("label"):
        text += f"; {budget['label']}"
    if verdict != "pass":
        text += "; not a run FAIL"
    return text
