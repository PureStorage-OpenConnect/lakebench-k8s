"""The snapshots AML batch gold-finalize read, and their fingerprints.

gold-finalize logs one line per silver table it reads, before its first
read (``gold_finalize_financial.read_snapshot_line``)::

    [read-snapshot] table=<t> snapshot=<id|none|unknown> total_records=<n|null>

``lakebench run`` passes them to the batch scorer (``--read-snapshot
<t>=<id>:<n>``), which fingerprints every column of each snapshot
(``common.frame_fingerprint``) and returns them in ``recall.json`` as
``read_snapshots``, so the run record holds ``financial_scoring.read_snapshots
= [{table, snapshot, total_records, rows, fp, cols_sha}]``. ``financial
reproduce`` reads the snapshots back from the record.

Pure parsing: no cluster or Spark access.
"""

from __future__ import annotations

import re
from typing import Any

_LINE = re.compile(
    r"\[read-snapshot\] table=(?P<table>\S+) snapshot=(?P<snapshot>\S+) "
    r"total_records=(?P<total>\S+)\s*$"
)
_ARG = re.compile(
    r"^(?P<table>[A-Za-z0-9_.]+)=(?P<snapshot>-?\d+|none|unknown):(?P<total>\d+|null)$"
)


def _token(raw: str) -> int | str:
    try:
        return int(raw)
    except ValueError:
        return raw


def _count(raw: str) -> int | None:
    try:
        return int(raw)
    except ValueError:
        return None


def parse_read_snapshots(logs: str | None) -> list[dict[str, Any]]:
    """``[{table, snapshot, total_records}]`` from gold-finalize's log, in
    the order logged; the last line wins for a table logged twice (a
    restarted driver)."""
    found: dict[str, dict[str, Any]] = {}
    for line in (logs or "").splitlines():
        m = _LINE.search(line)
        if m:
            found.pop(m["table"], None)
            found[m["table"]] = {
                "table": m["table"],
                "snapshot": _token(m["snapshot"]),
                "total_records": _count(m["total"]),
            }
    return list(found.values())


def score_arguments(snapshots: list[dict[str, Any]]) -> list[str]:
    """``--read-snapshot <t>=<id>:<n>`` arguments for the batch scorer."""
    out: list[str] = []
    for s in snapshots:
        total = s.get("total_records")
        out += [
            "--read-snapshot",
            f"{s['table']}={s['snapshot']}:{'null' if total is None else int(total)}",
        ]
    return out


def parse_score_argument(value: str) -> dict[str, Any]:
    """One ``--read-snapshot`` value back to ``{table, snapshot,
    total_records}``; ValueError on anything else."""
    m = _ARG.match(value or "")
    if not m:
        raise ValueError(f"--read-snapshot {value!r} is not <table>=<snapshot>:<records>")
    return {
        "table": m["table"],
        "snapshot": _token(m["snapshot"]),
        "total_records": _count(m["total"]),
    }


#: The tables a reproduction needs, in the order gold-finalize logs them.
REPRODUCE_TABLE_ROLES = ("transactions", "entities", "versions")


def usable(snapshots: Any) -> str | None:
    """Why a record's ``read_snapshots`` cannot drive a reproduction, or
    None: three entries, each with an int snapshot. A missing fingerprint
    only rules out the content-equivalent read (the reproduce job then
    needs the recorded snapshot itself)."""
    if not isinstance(snapshots, list) or not snapshots:
        return (
            "the run recorded no read snapshots (it predates 1.7, or gold-finalize's "
            "log was not read)"
        )
    if len(snapshots) != len(REPRODUCE_TABLE_ROLES):
        return f"the run recorded {len(snapshots)} read snapshots, not {len(REPRODUCE_TABLE_ROLES)}"
    for s in snapshots:
        if not isinstance(s, dict):
            return "a read snapshot entry is not a mapping"
        snap = s.get("snapshot")
        if isinstance(snap, bool) or not isinstance(snap, int):
            return f"{s.get('table')}: gold read no known snapshot ({snap})"
    return None
