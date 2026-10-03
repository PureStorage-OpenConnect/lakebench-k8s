#!/usr/bin/env python3
"""Per-rule stage profile of an AML gold-finalize run from its Spark event log.

The fallback for ``experiment.attribution`` when the driver could not read
its status store (``stage_profile_unavailable`` in the record): rerun
gold-finalize with an uncompressed event log (``spark.eventLog.enabled=true``,
``spark.eventLog.compress=false``, ``spark.eventLog.dir`` pointing at a
diagnostics prefix), download the log, and run::

    python scripts/aml_stage_attribution.py EVENTLOG [--top 3] [--record metrics.json]

EVENTLOG is the log file or the rolling log's directory.

Each detection rule runs in its own Spark job group ``lb-rule-<rule>-<id>``
(gold_finalize_financial.run_detection_rules). The log's job starts give the
group of each job and its stages; task ends give each stage's executor run
time and its longest task; stage completions give the stage name, task
count and wall time. The output has the shape of ``jobs[].stage_profile``
(the heaviest ``--top`` stages per rule by executor run time, the last
attempt of each stage), with ``complete`` true, ``truncated`` false,
``lossy`` None (the event-log listener's own drops cannot be seen in the
log; check the driver log for "Dropped ... events from eventLog") and
``source`` eventlog. A plain text or ``.gz`` log file is read, or a rolling
log directory (Spark 4's default), whose ``events_<n>_*`` files are read in
order.

With ``--record``, the run whose job groups the log names must be that
record's run (run gold-finalize again with ``run --stage gold-finalize``
and attach to that run's record), and its gold-finalize job gets this
profile (its ``stage_profile_unavailable`` entries for the profiled rules are
removed) and ``experiment.attribution`` is recomputed, marked
``"profile_source": "eventlog"``. The file is rewritten in place.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

_GROUP_RE = re.compile(r"^lb-rule-(?P<rule>[A-Za-z0-9_]+)-[0-9a-f]+$")


def _event_files(path: Path) -> list[Path]:
    """The log's event files in order: a single file, or the ``events_<n>_*``
    files of a rolling log directory (Spark 4 rolls event logs by default)."""
    if not path.is_dir():
        return [path]

    def index(p: Path) -> int:
        m = re.match(r"events_(\d+)_", p.name)
        return int(m.group(1)) if m else 0

    return sorted((p for p in path.iterdir() if p.name.startswith("events_")), key=index)


def _lines(path: Path):
    for f in _event_files(path):
        opener = gzip.open if f.suffix == ".gz" else open
        with opener(f, "rt", encoding="utf-8") as fh:  # type: ignore[operator]
            yield from fh


def profile_from_events(lines, top: int = 3) -> dict[str, list[dict[str, Any]]]:
    """``{rule: [stage, ...]}`` from Spark event-log JSON lines."""
    return read_events(lines, top=top)[0]


def read_events(lines, top: int = 3) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
    """``(profile, run_ids)``: the per-rule profile, and the Lakebench run ids
    the rule groups name (each group's description is ``<rule> run <run_id>``)."""
    stage_group: dict[int, str] = {}
    run_ids: set[str] = set()
    exec_ms: dict[tuple[int, int], int] = defaultdict(int)
    max_ms: dict[tuple[int, int], int] = defaultdict(int)
    shuffle_read: dict[tuple[int, int], int] = defaultdict(int)
    info: dict[tuple[int, int], dict[str, Any]] = {}
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        ev = json.loads(raw)
        kind = ev.get("Event")
        if kind == "SparkListenerJobStart":
            props = ev.get("Properties") or {}
            group = props.get("spark.jobGroup.id")
            desc = str(props.get("spark.job.description") or "")
            if group and _GROUP_RE.match(group) and " run " in desc:
                run_ids.add(desc.split(" run ", 1)[1].strip())
            if group:
                for sid in ev.get("Stage IDs") or []:
                    stage_group[int(sid)] = group
        elif kind == "SparkListenerTaskEnd":
            key = (int(ev["Stage ID"]), int(ev.get("Stage Attempt ID", 0)))
            metrics = ev.get("Task Metrics") or {}
            run_ms = int(metrics.get("Executor Run Time") or 0)
            exec_ms[key] += run_ms
            max_ms[key] = max(max_ms[key], run_ms)
            sr = metrics.get("Shuffle Read Metrics") or {}
            shuffle_read[key] += int(sr.get("Remote Bytes Read") or 0) + int(
                sr.get("Local Bytes Read") or 0
            )
        elif kind == "SparkListenerStageCompleted":
            si = ev.get("Stage Info") or {}
            key = (int(si["Stage ID"]), int(si.get("Stage Attempt ID", 0)))
            sub, comp = si.get("Submission Time"), si.get("Completion Time")
            info[key] = {
                "status": "FAILED" if si.get("Failure Reason") else "COMPLETE",
                "name": " ".join(str(si.get("Stage Name") or "").split())[:120],
                "tasks": int(si.get("Number of Tasks") or 0),
                "wall_s": round((comp - sub) / 1000.0, 1) if sub and comp else None,
            }
    # The last attempt of each stage, as the live profile reads it.
    last_attempt: dict[int, int] = {}
    for sid, attempt in info:
        last_attempt[sid] = max(attempt, last_attempt.get(sid, attempt))
    by_rule: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (sid, attempt), meta in info.items():
        if attempt != last_attempt[sid]:
            continue
        m = _GROUP_RE.match(stage_group.get(sid, ""))
        if not m:
            continue
        key = (sid, attempt)
        by_rule[m.group("rule")].append(
            {
                "stage": sid,
                "attempt": attempt,
                "status": meta["status"],
                "tasks": meta["tasks"],
                "wall_s": meta["wall_s"],
                "exec_s": round(exec_ms[key] / 1000.0, 1),
                "shuffle_read_mb": round(shuffle_read[key] / 1048576.0, 1),
                "max_task_s": round(max_ms[key] / 1000.0, 1),
                "name": meta["name"],
            }
        )
    out: dict[str, list[dict[str, Any]]] = {}
    for rule, stages in sorted(by_rule.items()):
        stages.sort(key=lambda r: (-r["exec_s"], r["stage"]))
        # The log holds every event its listener received, but that
        # listener's queue can drop events too, which the log cannot show:
        # lossy is unknown (None), not false.
        out[rule] = [
            {
                **s,
                "stages": len(stages),
                "truncated": False,
                "complete": True,
                "lossy": None,
                "source": "eventlog",
            }
            for s in stages[:top]
        ]
    return out, run_ids


def attach(
    record_path: Path, profile: dict[str, list[dict[str, Any]]], run_ids: set[str]
) -> dict[str, Any]:
    """Put *profile* on the record's gold-finalize job and recompute
    ``experiment.attribution``. Returns the new attribution block. Refuses a
    log whose rule groups do not name the record's run id: stages of one run
    are never paired with another run's rule times."""
    from lakebench.metrics.attribution import attribution
    from lakebench.metrics.collector import JobMetrics
    from lakebench.metrics.storage import _dataclass_from_dict

    data = json.loads(record_path.read_text())
    record_id = str(data.get("run_id") or "")
    # Batch cycles run gold-finalize as <run_id>-c<n>.
    if not any(r == record_id or r.startswith(record_id + "-c") for r in run_ids if record_id):
        raise SystemExit(
            f"{record_path}: the event log's rule groups name run(s) {sorted(run_ids)}, "
            f"not this record's {data.get('run_id')!r}; attach it to that run's record"
        )
    gold = [j for j in data.get("jobs") or [] if j.get("job_type") == "gold-finalize"]
    if not gold:
        raise SystemExit(f"{record_path}: no gold-finalize job")
    job = gold[-1]
    job["stage_profile"] = {**(job.get("stage_profile") or {}), **profile}
    unavailable = dict(job.get("stage_profile_unavailable") or {})
    for rule in profile:
        unavailable.pop(rule, None)
    job["stage_profile_unavailable"] = unavailable
    jobs = [_dataclass_from_dict(JobMetrics, j) for j in data.get("jobs") or []]
    block = attribution(type("M", (), {"jobs": jobs})())
    if block is not None:
        block["profile_source"] = "eventlog"
        data.setdefault("experiment", {})["attribution"] = block
    record_path.write_text(json.dumps(data, indent=2, default=str) + "\n")
    return block or {}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("eventlog", type=Path)
    ap.add_argument("--top", type=int, default=3)
    ap.add_argument("--record", type=Path, help="metrics.json to attach the profile to")
    args = ap.parse_args(argv)
    profile, run_ids = read_events(_lines(args.eventlog), top=args.top)
    if not profile:
        print("no lb-rule-* job groups in the event log", file=sys.stderr)
        return 1
    if args.record:
        print(json.dumps(attach(args.record, profile, run_ids), indent=2))
    else:
        print(json.dumps(profile, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
