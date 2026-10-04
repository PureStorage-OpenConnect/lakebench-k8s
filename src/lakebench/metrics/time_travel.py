"""Time-travel reads of a continuous AML run (``continuous.time_travel``).

After the window, ``cli/_aml_post.run_time_travel`` submits
``time_travel_financial.py`` over the transactions snapshots the gold-refresh
ticks recorded (``metrics/tick_records.time_travel_ticks``). This module
turns its result into the record: each tick's state, the maintenance round
that expired an expired snapshot (``continuous.retention.rounds``), the
retention policy the result is read under, and the time-travel check's
verdict. The check never fails the run; it is shown beside the verdict.

Pure: no cluster or Spark access.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

#: Record states that pass the time-travel check on their own.
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
    snapshot was committed before the round's end minus the retention the
    round applied. The round's end is on this host's clock; it is moved to
    the cluster clock (where the commit time and the expiry cutoff were
    taken) by *clock_offset_s* (cluster minus host), when known. The end is
    the latest moment the statement could have taken its cutoff, so a
    snapshot no round could have expired is never explained."""
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
            return {
                "round": r.get("round"),
                "ran_at": r.get("ended_at"),
                "configured": configured,
                "applied": applied,
                "reason": (
                    "live-stream floor"
                    if str(applied) != str(configured)
                    else "configured retention"
                ),
            }
    return None


def verdict_of(ticks: Sequence[Mapping[str, Any]], incomplete: bool) -> tuple[str, str]:
    """``(verdict, reason)`` of the time-travel check from the per-tick
    states (after expiry attribution): ``fail`` on any failing state, zero
    records or zero verified; ``incomplete`` when the budget ran out before
    every snapshot was read; else ``pass``. The check never fails the run."""
    if not ticks:
        return "fail", "zero recorded snapshots"
    states = [str(t.get("state")) for t in ticks]
    bad = sorted({s for s in states if s in TT_FAILING_STATES})
    if bad:
        return "fail", "snapshot(s) " + ", ".join(f"{s} x{states.count(s)}" for s in bad)
    if incomplete or "not_read" in states:
        done = sum(1 for s in states if s != "not_read")
        return "incomplete", f"the job budget ran out after {done} of {len(states)} records"
    if not any(s in TT_VERIFIED_STATES for s in states):
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


def merge(
    continuous: dict,
    result: Mapping[str, Any],
    pol: dict[str, Any],
) -> dict[str, Any]:
    """``continuous.time_travel`` from the job's ``time_travel.json``: each
    recorded tick gains its state, read time, rows and matches; an expired
    snapshot is attributed to the maintenance round that expired it
    (``expired_by``) or reads ``missing_unexplained``; then the verdict."""
    tt = continuous.setdefault("time_travel", {})
    recorded = list(tt.get("ticks") or [])
    by_key = {(r.get("start"), r.get("cycle")): r for r in result.get("ticks") or []}
    rounds = (continuous.get("retention") or {}).get("rounds") or []
    offset = (continuous.get("window") or {}).get("cluster_clock_offset_seconds")
    merged = []
    for t in recorded:
        got = by_key.get((t.get("start"), t.get("cycle")))
        if got is None:
            entry = {**t, "state": "error", "reason": "no result for this tick"}
        else:
            entry = {
                **t,
                **{
                    k: got.get(k)
                    for k in ("state", "read_s", "rows", "fp_match", "count_match", "reason")
                    if k in got
                },
            }
        if entry.get("state") == "expired":
            by = expired_by(t, rounds, pol.get("configured"), offset)
            if by is None:
                entry["state"] = "missing_unexplained"
                entry["reason"] = (
                    "expired, but no Lakebench maintenance round that ran expire_snapshots "
                    "on this table could have expired it"
                )
            else:
                entry["expired_by"] = by
        merged.append(entry)
    verdict, reason = verdict_of(merged, bool(result.get("incomplete")))
    if result.get("status") == "not_supported" and verdict != "fail":
        verdict, reason = "fail", str(result.get("reason") or "not supported")
    tt["ticks"] = merged
    tt["current_read_s"] = (result.get("current") or {}).get("read_s")
    tt["current"] = result.get("current")
    tt["policy"] = pol
    tt["verdict"] = verdict
    if reason:
        tt["reason"] = reason
    else:
        tt.pop("reason", None)
    return tt


def line(tt: Mapping[str, Any]) -> str:
    """One line for the console and the verdict qualifier: the check's
    verdict, the state counts and the retention policy, never a run FAIL."""
    states: dict[str, int] = {}
    for t in tt.get("ticks") or []:
        s = str(t.get("state"))
        states[s] = states.get(s, 0) + 1
    counts = ", ".join(f"{n} {s}" for s, n in sorted(states.items())) or "no records"
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
    if verdict != "pass":
        text += "; not a run FAIL"
    return text
