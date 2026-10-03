"""Tick records of the continuous AML gold-refresh driver.

``gold_refresh_financial.run_tick`` logs three lines per tick, each carrying
the run id::

    Cycle N: pinned txns=<id> entities=<id> accounts=<id> versions=<id> at=<epoch_s> run=<run>
    Cycle N: committed alerts=<id> status=<id> run=<run>
    Cycle N: completed run=<run>

and ``main`` logs ``Drain complete: ... last completed cycle N run=<run>``
once the drain marker stopped the loop. An id is an Iceberg snapshot id, or
``none`` (the table had no snapshot) or ``unknown`` (the lookup failed, or
for ``versions`` the pinned read was not used). Cycle numbers restart with
each driver start, so records are kept per driver start (the banner line
opens a new one) and only the drain's own driver start is scored.

Pure parsing: no cluster or Spark access.
"""

from __future__ import annotations

import re
from typing import Any

_LOG_TS = re.compile(r"\[lb\] (\d{4}-\d\d-\d\dT[\d:.]+) - ")
#: The first line ``main`` logs after its separator: a driver start.
_BANNER = "Gold Refresh (Financial) -- baseline + periodic detection"
_PINNED = re.compile(
    r"Cycle (\d+): pinned txns=(\S+) entities=(\S+) accounts=(\S+) versions=(\S+) "
    r"at=([\d.]+) run=(\S+)\s*$"
)
_COMMITTED = re.compile(r"Cycle (\d+): committed alerts=(\S+) status=(\S+) run=(\S+)\s*$")
_COMPLETED = re.compile(r"Cycle (\d+): completed run=(\S+)\s*$")
_DRAIN = re.compile(
    r"Drain complete: (?:stop marker present at start; )?last completed cycle (\d+) run=(\S+)\s*$"
)

#: The pins a covered score needs, as (tick key, table named in a reason).
SCORED_PINS = (
    ("pinned_txns", "silver.transactions"),
    ("pinned_entities", "silver.entities"),
    ("pinned_accounts", "silver.accounts"),
    ("pinned_versions", "silver.silver_batch_versions"),
    ("committed_alerts", "gold.alerts"),
    ("committed_status", "gold.detection_status"),
)


def _token(raw: str) -> int | str:
    """A logged snapshot token: the int id, or the string as logged."""
    try:
        return int(raw)
    except ValueError:
        return raw


def is_pin(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def drain_cycle(logs: str | None, run_id: str) -> int | None:
    """The cycle number of this run's ``Drain complete`` line, the last one in
    the log; None when the log has none for *run_id*."""
    found = None
    for line in (logs or "").splitlines():
        m = _DRAIN.search(line)
        if m and m.group(2) == run_id:
            found = int(m.group(1))
    return found


def parse_tick_records(logs: str | None, run_id: str) -> dict[str, Any]:
    """Every tick of *run_id* in the log, and the drain.

    Returns ``{"ticks": [...], "drain_cycle": int | None, "drain_start":
    int | None}``. Each tick is ``{start, cycle, pinned_txns,
    pinned_entities, pinned_accounts, pinned_versions, pinned_at,
    committed_alerts, committed_status, completed, completed_at}``: ``start`` numbers
    the driver start (0 for the first in the log), a field never logged is
    None. ``drain_start`` is the driver start the drain line belongs to.
    Lines of another run id are ignored.
    """
    start = -1
    ticks: dict[tuple[int, int], dict[str, Any]] = {}
    order: list[tuple[int, int]] = []
    drain_cycle_n = None
    drain_start = None

    def tick(cycle: int) -> dict[str, Any]:
        key = (max(start, 0), cycle)
        if key not in ticks:
            ticks[key] = {
                "start": key[0],
                "cycle": cycle,
                "pinned_txns": None,
                "pinned_entities": None,
                "pinned_accounts": None,
                "pinned_versions": None,
                "pinned_at": None,
                "committed_alerts": None,
                "committed_status": None,
                "completed": False,
                "completed_at": None,
            }
            order.append(key)
        return ticks[key]

    for line in (logs or "").splitlines():
        if _BANNER in line:
            start += 1
            continue
        m = _PINNED.search(line)
        if m and m.group(7) == run_id:
            t = tick(int(m.group(1)))
            t["pinned_txns"] = _token(m.group(2))
            t["pinned_entities"] = _token(m.group(3))
            t["pinned_accounts"] = _token(m.group(4))
            t["pinned_versions"] = _token(m.group(5))
            t["pinned_at"] = float(m.group(6))
            continue
        m = _COMMITTED.search(line)
        if m and m.group(4) == run_id:
            t = tick(int(m.group(1)))
            t["committed_alerts"] = _token(m.group(2))
            t["committed_status"] = _token(m.group(3))
            continue
        m = _COMPLETED.search(line)
        if m and m.group(2) == run_id:
            ts = _LOG_TS.search(line)
            t = tick(int(m.group(1)))
            t["completed"] = True
            t["completed_at"] = ts.group(1) + "Z" if ts else None
            continue
        m = _DRAIN.search(line)
        if m and m.group(2) == run_id:
            drain_cycle_n = int(m.group(1))
            drain_start = max(start, 0)
    return {
        "ticks": [ticks[k] for k in order],
        "drain_cycle": drain_cycle_n,
        "drain_start": drain_start,
    }


def ticks_unpinned(ticks: list[dict[str, Any]]) -> int:
    """Ticks whose logged ``txns`` or ``versions`` token is not an int (d3):
    detection ran on them without the pinned sealed read."""
    return sum(
        1 for t in ticks if not (is_pin(t.get("pinned_txns")) and is_pin(t.get("pinned_versions")))
    )


def scored_tick(parsed: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """The tick a covered score reads, or ``(None, reason)``.

    Only the drain's last completed cycle, in the drain's driver start, is
    scored, and only when that same cycle is the last one there to have
    committed and completed, and every pin it logged is an int. An earlier
    tick is never scored instead: gold.alerts holds the last tick's rewrite.
    """
    n = parsed.get("drain_cycle")
    if n is None:
        return None, "no drain line in the gold-refresh log"
    if n == 0:
        return None, "the drain stopped the driver before its first tick completed"
    start = parsed.get("drain_start")
    mine = [t for t in parsed.get("ticks") or [] if t["start"] == start]
    done = [t["cycle"] for t in mine if t.get("completed")]
    committed = [t["cycle"] for t in mine if t.get("committed_alerts") is not None]
    if not done or max(done) != n:
        return None, f"no completed tick record for the drain's last cycle {n}"
    if not committed or max(committed) != n:
        return None, f"no committed tick record for the drain's last cycle {n}"
    t = next(t for t in mine if t["cycle"] == n)
    for key, table in SCORED_PINS:
        if not is_pin(t.get(key)):
            return None, f"{table} snapshot unknown at the last completed tick"
    return t, ""


def tick_list(ticks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """``continuous.ticks[]`` as recorded (DESIGN interfaces): the pins and
    times of each tick, with the commit snapshots and the driver start."""
    keys = (
        "cycle",
        "pinned_txns",
        "pinned_entities",
        "pinned_accounts",
        "pinned_versions",
        "pinned_at",
        "completed_at",
        "committed_alerts",
        "committed_status",
        "start",
    )
    return [{k: t.get(k) for k in keys} for t in ticks]
