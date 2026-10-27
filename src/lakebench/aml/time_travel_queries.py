"""Time-travel retention check as queries, assembled on the CLI side.

This replaced the ``time_travel_financial`` Spark job (deleted in 1.7.14).
The job fingerprinted every recorded snapshot twice with full scans; an AML
programme instead asks, of a case weeks later, "is the snapshot this alert
read still there, and does it still hold the evidence". So after the window
this does two cheap things per recorded tick:

* a metadata read of the table's ``$snapshots`` list, which says whether the
  tick's snapshot is still present, expired by maintenance (attributed to the
  round that expired it, as the job's record did) or missing without an
  explanation;
* for a present snapshot, one timed point read: the newest alert that tick
  committed, its payments re-read from ``silver.transactions`` as of the
  tick's pinned snapshot, confirming the evidence rows are there, with the
  same count at the current snapshot for a timing comparison.

That is one short query per tick, seconds each, instead of two full scans of
every snapshot. The record keeps the keys ``metrics.time_travel`` and the
verdict read before: ``ticks[]`` with ``state``, ``read_s``, ``rows``,
``reason`` and ``expired_by``; ``policy``; ``verdict``; ``reason``.

Pure: the SQL runs through an injected ``run_tsv(sql, timeout) ->
list[list[str | None]]`` (one list per data row, an empty cell ``None``), so
the module imports no pyspark and no kubectl. The CLI supplies an adapter
over the query executor; the tests supply a fake.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from lakebench.metrics import time_travel as tt_mod

RunTsv = Callable[[str, int], list[list[str | None]]]

_TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "benchmark/queries/aml/trino/time_travel"


def _template(name: str) -> str:
    return (_TEMPLATE_DIR / name).read_text()


def _bare(table: Any) -> str:
    """The table name under silver (``silver.transactions`` -> ``transactions``,
    a bare name unchanged)."""
    return str(table).split(".")[-1]


def _present_snapshots(run_tsv: RunTsv, catalog: str, table: str, timeout: int) -> set[str]:
    """The live snapshot ids of one silver table, as strings."""
    sql = _template("snapshots_present.sql.tmpl").format(catalog=catalog, table=table)
    return {row[0] for row in run_tsv(sql, timeout) if row and row[0] is not None}


def _point_read(
    run_tsv: RunTsv,
    catalog: str,
    *,
    txns_snapshot: int,
    alerts_snapshot: int,
    timeout: int,
) -> dict[str, Any]:
    """One case's evidence as of the tick snapshot: ``{found, evidence,
    read_s}``. The alert is chosen inside the statement, so no alert id is
    interpolated. Times only the query."""
    sql = _template("case_as_of.sql.tmpl").format(
        catalog=catalog,
        txns_pin=f"FOR VERSION AS OF {int(txns_snapshot)}",
        alerts_pin=f"FOR VERSION AS OF {int(alerts_snapshot)}",
    )
    t0 = time.monotonic()
    rows = run_tsv(sql, timeout)
    read_s = round(time.monotonic() - t0, 3)
    if not rows or rows[0][0] is None:
        return {"read_s": read_s, "found": None, "evidence": None}
    r = rows[0]
    return {
        "read_s": read_s,
        "found": int(r[0]) if r[0] is not None else None,
        "evidence": int(r[1]) if len(r) > 1 and r[1] is not None else None,
    }


def _state_of_present(point: dict[str, Any]) -> tuple[str, str | None]:
    """The verified state of a present snapshot from its point read. A read
    whose as-of count equals the alert's evidence size (and both are non-zero)
    is ``verified``; a snapshot present but with no case to read (no alert, or
    an alert with no payments) is ``verified_hash_only`` (present and readable,
    nothing compared); a count that falls short is a ``mismatch``."""
    found, evidence = point.get("found"), point.get("evidence")
    if evidence is None or evidence == 0 or found is None:
        return "verified_hash_only", "snapshot present; no case evidence to compare"
    if found < evidence:
        return (
            "mismatch",
            f"{found} of {evidence} evidence rows present at the tick snapshot",
        )
    return "verified", None


def retention_record(
    continuous: dict,
    run_tsv: RunTsv,
    catalog: str,
    pol: dict[str, Any],
    *,
    timeout: int = 300,
) -> dict[str, Any]:
    """Build ``continuous.time_travel`` from the recorded ticks and queries.

    *continuous* carries the recorded ticks on ``time_travel.ticks`` (each
    ``{start, cycle, table, snapshot, committed_at, ...}``) and the pinned
    snapshot ids and newest committed alert the point read binds. Every tick
    gains a state and, where present, a timed read; an absent snapshot is
    attributed to the maintenance round that expired it, or reads
    ``missing_unexplained``. Never raises for a query failure: a tick whose
    query errored reads ``error`` with the message.
    """
    tt = continuous.setdefault("time_travel", {})
    recorded = list(tt.get("ticks") or [])
    rounds = (continuous.get("retention") or {}).get("rounds") or []
    offset = (continuous.get("window") or {}).get("cluster_clock_offset_seconds")
    # The snapshot list of each table, looked up once; a failed lookup is
    # cached too, so a table that cannot be listed costs one query, not one
    # per tick of it.
    present_by_table: dict[str, set[str]] = {}
    list_error: dict[str, str] = {}
    merged: list[dict[str, Any]] = []
    for t in recorded:
        entry = dict(t)
        table = _bare(t.get("table"))
        snapshot = t.get("snapshot")
        if table not in present_by_table and table not in list_error:
            try:
                present_by_table[table] = _present_snapshots(run_tsv, catalog, table, timeout)
            except Exception as e:  # noqa: BLE001 -- a measurement; never fails the run
                list_error[table] = f"listing {table} snapshots failed: {e}"
        if table in list_error:
            entry["state"] = "error"
            entry["reason"] = list_error[table]
            merged.append(entry)
            continue
        present = present_by_table[table]

        if snapshot is None or str(snapshot) not in present:
            # Not present: expired by maintenance, or lost. expired_by reads
            # the maintenance rounds; None turns into missing_unexplained,
            # exactly as the Spark-job record did.
            by = tt_mod.expired_by(t, rounds, pol.get("configured"), offset)
            if by is None:
                entry["state"] = "missing_unexplained"
                entry["reason"] = (
                    "the recorded snapshot is gone and no Lakebench maintenance round "
                    "that ran expire_snapshots on this table could have expired it"
                )
            else:
                entry["state"] = "expired"
                entry["expired_by"] = by
            merged.append(entry)
            continue

        if not _pins_ok(t):
            entry["state"] = "verified_hash_only"
            entry["reason"] = "snapshot present; no pinned alerts snapshot to read a case from"
            merged.append(entry)
            continue
        try:
            point = _point_read(
                run_tsv,
                catalog,
                txns_snapshot=int(t["snapshot"]),
                alerts_snapshot=int(t["committed_alerts"]),
                timeout=timeout,
            )
        except Exception as e:  # noqa: BLE001 -- a measurement; never fails the run
            entry["state"] = "error"
            entry["reason"] = f"reading the case as of the snapshot failed: {e}"
            merged.append(entry)
            continue
        state, reason = _state_of_present(point)
        entry["state"] = state
        entry["read_s"] = point["read_s"]
        entry["rows"] = point["found"]
        if reason:
            entry["reason"] = reason
        else:
            entry.pop("reason", None)
        merged.append(entry)

    verdict, reason = tt_mod.verdict_of(merged, incomplete=False)
    tt["ticks"] = merged
    tt["policy"] = pol
    tt["verdict"] = verdict
    if reason:
        tt["reason"] = reason
    else:
        tt.pop("reason", None)
    return tt


def _is_pin(value: Any) -> bool:
    """Whether *value* is a usable snapshot id (an int, not a logged
    ``none``/``unknown`` token)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _pins_ok(tick: Mapping[str, Any]) -> bool:
    """Both snapshots the point read pins are ids."""
    return _is_pin(tick.get("snapshot")) and _is_pin(tick.get("committed_alerts"))
