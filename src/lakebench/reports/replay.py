"""The Workload replay tab of the HTML report.

``build_replay`` turns a serialized run record into the replay's data block:
the five workload stages with their recorded windows and counts, the Spark
jobs, and the storage rate over time. ``render_replay`` wraps the block in
the markup, styles and script that play it back.

Every value in the block comes from a recorded field and carries its record
path. A replayed count that grows over a stage's window is derived (the
record holds the window and the final count, not the count in between) and
is labelled so. A lane with nothing recorded is shown as not measured; no
curve is ever generated.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from html import escape
from typing import Any

from lakebench.metrics.io import bucket_series
from lakebench.reports.tiles import UNIT_CSS, screened_by_cycle, unit_switch

REPLAY_VERSION = 1

# The five stages of every workload: the record's stage name, the phase
# caption, and for each workload and mode the card title, implementation and
# the caption under the count.
_STAGE_KEYS = ("datagen", "bronze", "silver", "gold", "query")
_PHASES = ("Generate", "Ingest", "Prepare", "Assemble", "Query")
_IMPL = (
    "Datagen → S3",
    "Spark · Bronze",
    "Spark · Silver",
    "Spark · Gold",
    "Gold → Trino",
)

_PROFILES: dict[tuple[str, str], dict[str, Any]] = {
    ("customer360", "batch"): {
        "kind": "batch",
        "title": "C360 / Batch",
        "subtitle": "Customer 360",
        "phases": _PHASES,
        "names": ("Customer data", "Ingest", "Conform", "Customer 360", "Customer view"),
        "labels": (
            "Rows generated",
            "Rows ingested",
            "Rows conformed",
            "Gold records",
            "Queries completed",
        ),
    },
    ("financial", "batch"): {
        "kind": "batch",
        "title": "AML / Batch",
        "subtitle": "Financial · Transaction monitoring",
        "phases": ("Generate", "Ingest", "Prepare", "Monitor", "Output"),
        "names": ("Payments", "Ingest", "Process", "AML rules", "Alert queries"),
        "labels": (
            "Rows generated",
            "Rows ingested",
            "Rows prepared",
            "Transactions screened",
            "Queries completed",
        ),
        # The count a stage shows when it is not its output_rows: the monitor
        # stage's output is the alerts it raised, its work is the transactions
        # it screened.
        "fields": {"gold": "input_rows"},
    },
    ("financial", "sustained"): {
        "kind": "sustained",
        "title": "AML / Continuous",
        "subtitle": "Financial · Transaction monitoring",
        "phases": ("Generate", "Ingest", "Prepare", "Monitor", "Detect"),
        "names": ("Payments", "Ingest", "Process", "AML rules", "Alerts"),
        "impl": (
            "Datagen → S3",
            "Spark · Bronze stream",
            "Spark · Silver stream",
            "Spark · Gold refresh",
            "Alerts raised",
        ),
        "labels": (
            "Payments released",
            "Rows ingested",
            "Rows prepared",
            "Payments screened",
            "Alerts detected",
        ),
    },
}

# The sustained streams, upstream first: the record's stream name, and the
# handoff (as the balance block names it) that feeds the stage after it.
_STREAMS = ("bronze-ingest", "silver-stream", "gold-refresh")
_HANDOFFS = ("datagen->bronze", "bronze->silver", "silver->gold")

_MAX_BUCKETS = 240


def _epoch(stamp: Any) -> float | None:
    if not stamp:
        return None
    try:
        return datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _offset(stamp: Any, t0: float) -> float | None:
    """Seconds from the run start to *stamp*, or None when it is not recorded."""
    at = _epoch(stamp)
    return None if at is None else at - t0


def _profile_for(record: Mapping[str, Any]) -> dict[str, Any] | None:
    pb = record.get("pipeline_benchmark") or {}
    mode = str(pb.get("pipeline_mode") or "")
    schema = str((record.get("config_snapshot") or {}).get("workload_schema") or "customer360")
    return _PROFILES.get((schema, mode))


_LANE_LABELS = (("compute", "Compute"), ("network", "Network"), ("k8s", "Kubernetes"))
_GIB = 1024**3
_MB = 1e6


def _total(series: Mapping[str, Any], family: str, scale: float) -> list[float]:
    """The sum over pod groups of *family*, per step, divided by *scale*."""
    groups = [g for g in (series.get(family) or {}).values() if isinstance(g, list)]
    if not groups:
        return []
    n = max(len(g) for g in groups)
    return [round(sum(g[k][1] for g in groups if k < len(g)) / scale, 3) for k in range(n)]


def _lanes(record: Mapping[str, Any], t0: float) -> dict[str, Any]:
    """The platform lanes (compute, network, pods) from the record's
    ``platform_metrics.series``: totals over the pod groups per step. Absent
    when the run recorded no series (observability off, or an older record)."""
    series = (record.get("platform_metrics") or {}).get("series")
    if not isinstance(series, Mapping):
        return {}
    start = _epoch(series.get("start"))
    step = series.get("step_seconds")
    if start is None or not isinstance(step, (int, float)) or step <= 0:
        return {}
    base = {"step": float(step), "shift": round(start - t0, 3)}
    lanes: dict[str, Any] = {}
    cpu, mem = _total(series, "cpu_cores", 1.0), _total(series, "memory_bytes", _GIB)
    if cpu:
        lanes["compute"] = {**base, "cpu": cpu, "mem": mem}
    rx, tx = (
        _total(series, "network_rx_bytes_per_s", _MB),
        _total(series, "network_tx_bytes_per_s", _MB),
    )
    if rx or tx:
        lanes["network"] = {**base, "rx": rx, "tx": tx}
    pods = _total(series, "pods", 1.0)
    if pods:
        lanes["k8s"] = {**base, "pods": pods}
    return lanes


def _sustained(
    record: Mapping[str, Any], profile: Mapping[str, Any], t0: float
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any] | None]:
    """Stages, streams (as jobs), handoff lags and the lag chart of a
    sustained record. The windows are the recorded ones: each stream from the
    moment it was running to the end of the measured window; the evaluation
    cycles are the recorded completion times of the gold ticks."""
    cont = record.get("continuous") or {}
    pb = record.get("pipeline_benchmark") or {}
    scores = pb.get("scores") or {}
    by_name = {s.get("stage_name"): s for s in pb.get("stages") or [] if isinstance(s, Mapping)}
    win = cont.get("window") or {}
    w0, w1 = _offset(win.get("start"), t0), _offset(win.get("end"), t0)
    streams = cont.get("streams") or {}
    running = {n: _offset((streams.get(n) or {}).get("running_at"), t0) for n in _STREAMS}
    screened = screened_by_cycle(record)
    steps = [
        [o, screened[c]]
        for c, o in (
            (tk.get("cycle"), _offset(tk.get("completed_at"), t0))
            for tk in cont.get("ticks") or []
            if isinstance(tk, Mapping)
        )
        if o is not None and c in screened
    ]
    steps.sort()
    spec = [  # start, value, path, counts rows
        (w0, scores.get("released_rows"), "pipeline_benchmark.scores.released_rows", True),
        (
            running["bronze-ingest"],
            (by_name.get("bronze") or {}).get("output_rows"),
            "pipeline_benchmark.stages[bronze].output_rows",
            True,
        ),
        (
            running["silver-stream"],
            (by_name.get("silver") or {}).get("input_rows"),
            "pipeline_benchmark.stages[silver].input_rows",
            True,
        ),
        (
            running["gold-refresh"],
            steps[-1][1] if steps else None,
            "continuous.time_travel.ticks[*].total_records",
            True,
        ),
        (
            w0,
            scores.get("time_to_detect_alerts"),
            "pipeline_benchmark.scores.time_to_detect_alerts",
            False,
        ),
    ]
    stages = []
    for idx, (start, value, path, rows) in enumerate(spec):
        placed = start is not None and w1 is not None
        stages.append(
            {
                "key": _STAGE_KEYS[idx],
                "phase": profile["phases"][idx],
                "name": profile["names"][idx],
                "impl": profile["impl"][idx],
                "label": profile["labels"][idx],
                "start": start if placed else None,
                "end": w1 if placed else None,
                "value": value,
                "path": path if value is not None else None,
                "window": "recorded" if placed else None,
                "ends": None,
                "steps": steps if idx == 3 and steps else None,
                "rows": rows,
            }
        )
    jobs = [
        {
            "name": f"lakebench-{n}",
            "type": n,
            "start": running[n],
            "end": w1,
            "elapsed": None,
            "success": None,
        }
        for n in _STREAMS
    ]
    handoffs = []
    lag_series = []
    balance_h = (cont.get("balance") or {}).get("handoffs") or {}
    for i, name in enumerate(_HANDOFFS):
        h = balance_h.get(name)
        samples = [
            [float(a), float(b)]
            for a, b in (h or {}).get("samples") or []
            if isinstance(a, (int, float)) and isinstance(b, (int, float))
        ]
        if not samples or w0 is None:
            continue
        handoffs.append(
            {
                "index": i,
                "label": f"{profile['phases'][i]} → {profile['phases'][i + 1]}",
                "samples": samples,
                "keeps_up": (h or {}).get("keeps_up"),
                "path": f"continuous.balance.handoffs.{name}.samples",
            }
        )
        lag_series.append({"name": name, "samples": samples})
    lag = {"shift": w0, "series": lag_series} if lag_series else None
    return stages, jobs, handoffs, lag


def build_replay(record: Mapping[str, Any]) -> dict[str, Any] | None:
    """The replay data block for *record*, or None when this record has no
    replay (a mode or workload without a profile, or no recorded timeline)."""
    profile = _profile_for(record)
    if profile is None:
        return None
    t0, t1 = _epoch(record.get("start_time")), _epoch(record.get("end_time"))
    if t0 is None or t1 is None or t1 <= t0:
        return None
    by_name = {
        s.get("stage_name"): s
        for s in (record.get("pipeline_benchmark") or {}).get("stages") or []
        if isinstance(s, Mapping)
    }
    io = record.get("io") or {}
    intervals = [i for i in io.get("intervals") or [] if isinstance(i, Mapping)]
    bench_ends = sorted(
        e - t0
        for i in intervals
        if i.get("source") == "trino" and i.get("label") == "benchmark"
        for e in [_epoch(i.get("end"))]
        if e is not None
    )
    trino_windows = [
        (s, e)
        for i in intervals
        if i.get("source") == "trino"
        for s, e in [(_epoch(i.get("start")), _epoch(i.get("end")))]
        if s is not None and e is not None
    ]

    stages: list[dict[str, Any]] = []
    handoffs: list[dict[str, Any]] = []
    lag = None
    sustained = profile.get("kind") == "sustained"
    for idx, key in enumerate(() if sustained else _STAGE_KEYS):
        rec = by_name.get(key)
        start = end = value = None
        path = None
        if key == "query":
            if trino_windows:
                start = min(s for s, _ in trino_windows) - t0
                end = max(e for _, e in trino_windows) - t0
            bench = record.get("benchmark") or {}
            queries = [q for q in bench.get("queries") or [] if isinstance(q, Mapping)]
            if queries:
                value = sum(1 for q in queries if q.get("success"))
                path = "benchmark.queries[*].success"
        elif rec is not None:
            s, e = _epoch(rec.get("start_time")), _epoch(rec.get("end_time"))
            start = None if s is None else s - t0
            end = None if e is None else e - t0
            field = (profile.get("fields") or {}).get(key, "output_rows")
            value = rec.get(field)
            path = f"pipeline_benchmark.stages[{key}].{field}"
        stages.append(
            {
                "key": key,
                "phase": profile["phases"][idx],
                "name": profile["names"][idx],
                "impl": (profile.get("impl") or _IMPL)[idx],
                "label": profile["labels"][idx],
                "start": start,
                "end": end,
                "value": value,
                "path": path,
                # Recorded: the window and the final count. The running count
                # between them is derived by the player.
                "window": "recorded" if start is not None and end is not None else None,
                "ends": bench_ends if key == "query" else None,
            }
        )

    if sustained:
        stages, jobs, handoffs, lag = _sustained(record, profile, t0)
    else:
        jobs = [
            {
                "name": j.get("job_name"),
                "type": j.get("job_type"),
                "start": _offset(j.get("start_time"), t0),
                "end": _offset(j.get("end_time"), t0),
                "elapsed": j.get("elapsed_seconds"),
                "success": j.get("success"),
            }
            for j in record.get("jobs") or []
            if isinstance(j, Mapping)
        ]

    duration = t1 - t0
    step = float(max(1, -(-int(duration) // _MAX_BUCKETS)))
    placed = [i for i in intervals if i.get("start") and i.get("end")]
    storage = None
    if placed:
        start_iso = datetime.fromtimestamp(t0).astimezone().isoformat()
        end_iso = datetime.fromtimestamp(t1).astimezone().isoformat()
        series = bucket_series(placed, step_seconds=step, start=start_iso, end=end_iso)
        storage = {
            "step": step,
            "read": [round(v / step / 1e6, 3) for v in series["read"]],
            "write": [round(v / step / 1e6, 3) for v in series["write"]],
        }
    not_placed = [i for i in intervals if not (i.get("start") and i.get("end"))]
    # Stages whose storage bytes are not collected: the lane is blank for
    # them because nothing was measured, not because nothing moved.
    gaps = sorted(
        {
            f"{n.get('source')} {n.get('stage')}"
            for n in io.get("not_measured") or []
            if isinstance(n, Mapping) and n.get("stage") != "query"
        }
    )

    lanes = _lanes(record, t0)

    return {
        "version": REPLAY_VERSION,
        "title": profile["title"],
        "subtitle": profile["subtitle"],
        "mode": (record.get("pipeline_benchmark") or {}).get("pipeline_mode"),
        "kind": profile["kind"],
        "window": (
            {"start": stages[0]["start"], "end": stages[0]["end"]}
            if sustained and stages[0]["start"] is not None
            else None
        ),
        "handoffs": handoffs,
        "lag": lag,
        "run_id": record.get("run_id"),
        "duration": duration,
        "stages": stages,
        "jobs": jobs,
        "storage": storage,
        "storage_unplaced": [i.get("source") for i in not_placed],
        "storage_gaps": gaps,
        "storage_problems": [
            str(x) for x in io.get("problems") or [] if str(x).startswith("spark ")
        ],
        "lanes": lanes,
        "not_measured": [label for key, label in _LANE_LABELS if key not in lanes],
    }


_CSS = """
#lb-replay .headline{display:flex;align-items:end;justify-content:space-between;gap:12px;min-height:40px}
#lb-replay .eyebrow{font-size:11px;text-transform:uppercase;letter-spacing:1px;color:#5f8b73;font-weight:800}
#lb-replay .headline h1{font-size:25px;margin:1px 0 0;letter-spacing:-1.1px}
#lb-replay .headline-right{display:flex;gap:26px}
#lb-replay .headline-right .mini{display:grid;gap:2px}
#lb-replay .headline-right strong{font-size:22px;letter-spacing:-.5px}
#lb-replay .headline-right span{font-size:11px;color:#7e8b80}
#lb-replay .canvas{background:#fff;border:1px solid #dce6dd;border-radius:12px;display:flex;flex-direction:column;flex:1;min-height:0;overflow:hidden}
#lb-replay .layer-head{flex-shrink:0;min-height:34px;padding:7px 13px;display:flex;align-items:center;justify-content:space-between;gap:8px;border-bottom:1px solid #e4ebe5;font-size:14px;font-weight:750}
#lb-replay .layer-head small{font-size:12px;color:#75897a;font-weight:450}
#lb-replay .runsteps{display:flex;gap:7px;padding:9px 12px 6px;flex:1;min-height:188px;align-items:stretch}
#lb-replay .step{flex:1;min-width:0;padding:12px;background:#fbfcfa;border:1px solid #dce5de;border-radius:11px;display:flex;flex-direction:column;gap:6px;opacity:.82;transition:flex .3s,border .3s,background .3s;cursor:pointer}
#lb-replay .step.current{flex:1.75;background:#fff;border:2px solid #6ba886;opacity:1;box-shadow:0 4px 15px #31644610}
#lb-replay .step.done{opacity:.9}
#lb-replay .step[data-id="4"].current{border-color:#9071b2}
#lb-replay .step small{font-size:12px;text-transform:uppercase;letter-spacing:.7px;font-weight:850;color:#567b67}
#lb-replay .step h2{font-size:23px;letter-spacing:-.8px;margin:0}
#lb-replay .step .implementation{font-size:13px;color:#778a7e}
#lb-replay .art{height:80px;flex:1;min-height:60px;display:flex;align-items:center;justify-content:center;background:#f3f8f4;border-radius:8px;overflow:hidden}
#lb-replay .step.current .art{background:#edf6ef}
#lb-replay .art svg{width:100%;height:100%;max-height:115px}
#lb-replay .value{border-top:1px solid #e7eee7;padding-top:7px;margin-top:auto}
#lb-replay .value strong{font-size:26px;letter-spacing:-.7px;display:block;font-variant-numeric:tabular-nums}
#lb-replay .value label{font-size:12px;color:#778b7d}
#lb-replay .phasepanel{margin:1px 12px 6px;border:1px solid #d9e7dd;background:#f7faf7;border-radius:9px;padding:8px 12px;min-height:78px;display:grid;grid-template-columns:1.2fr 2.3fr;gap:14px;align-items:center}
#lb-replay .phasename small{font-size:12px;color:#518166;font-weight:750}
#lb-replay .phasename strong{display:block;font-size:18px;margin-top:3px;letter-spacing:-.3px}
#lb-replay .phasename span{font-size:12px;color:#778b7e}
#lb-replay .jobs{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:6px}
#lb-replay .job{padding:8px;background:white;border:1px solid #dfe9e0;border-radius:6px;min-width:0}
#lb-replay .job b{font-size:12px;color:#617f69;display:block}
#lb-replay .job strong{font-size:19px;display:block;margin-top:4px}
#lb-replay .job span{font-size:11px;color:#819084}
#lb-replay .job.on{border-color:#81b79a;background:#edf8f0}
#lb-replay .pipe-head,#lb-replay .platform-head{display:flex;align-items:center;justify-content:space-between;padding:4px 14px 2px;font-weight:750;font-size:14px}
#lb-replay .pipe-head small,#lb-replay .platform-head small{font-size:11px;font-weight:450;color:#859187}
#lb-replay .pipe{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;padding:3px 12px 7px}
#lb-replay .handoff{border:1px solid #dae7dd;border-radius:8px;padding:10px 12px;background:#f7fbf8;min-width:0}
#lb-replay .handoff-head{display:flex;align-items:center;justify-content:space-between;gap:5px}
#lb-replay .handoff b{font-size:13px}
#lb-replay .handoff strong{font-size:14px;color:#277352}
#lb-replay .track{height:6px;border-radius:10px;background:#e7f0e8;overflow:hidden;margin-top:8px}
#lb-replay .fill{height:100%;width:0;background:#73ad8b;transition:width .2s}
#lb-replay .handoff .sub{font-size:11px;color:#7f9184;margin-top:3px}
#lb-replay .platform{margin:0 12px 6px;border:1px solid #dce6dd;background:#fafdfa;border-radius:8px;padding:8px 12px;flex:0 0 192px;min-height:0}
#lb-replay .platformnav{display:flex;gap:5px;flex-wrap:wrap;align-items:center}
#lb-replay .platformnav button{border:1px solid #d7e5dc;border-radius:6px;background:#fff;color:#57715e;padding:5px 11px;font-size:12px}
#lb-replay .platformnav button.on{background:#e3f3e9;border-color:#82b89b;color:#1d6548;font-weight:800}
#lb-replay .platformnav button:last-of-type{margin-right:auto}
#lb-replay .traces{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-top:7px;height:125px}
#lb-replay .graph{border:1px solid #e0ebe2;border-radius:7px;padding:8px;display:flex;flex-direction:column;min-width:0}
#lb-replay .graph .charttitle{display:flex;align-items:center;justify-content:space-between}
#lb-replay .graph .charttitle b{font-size:13px}
#lb-replay .graph .charttitle strong{font-size:17px;color:#246d50}
#lb-replay .graph svg{width:100%;flex:1;min-height:0;background:#f4f8f4;border-radius:4px}
#lb-replay .controls{height:44px;padding:5px 13px;display:flex;align-items:center;gap:8px;background:#203f33;color:#fff;flex-shrink:0}
#lb-replay .controls button{border:0;border-radius:6px;background:#d4f4dd;color:#1d5439;padding:7px 11px;font-size:12px;font-weight:750}
#lb-replay .controls input{flex:1;min-width:0;accent-color:#6fbb89}
#lb-replay .controls .time{font:12px ui-monospace,monospace;color:#e9f4ec;min-width:104px;text-align:right}
#lb-replay .controls .mark{font-size:11px;color:#d6e5d8;white-space:nowrap}
@media(min-width:1100px) and (min-height:760px){#lb-replay .runsteps{min-height:150px}#lb-replay .step{padding:10px}#lb-replay .art{min-height:45px}#lb-replay .phasepanel{min-height:70px}#lb-replay .platform{flex-basis:170px}#lb-replay .traces{height:105px}#lb-replay .headline h1{font-size:23px}#lb-replay .value strong{font-size:23px}#lb-replay .step h2{font-size:21px}}
@media(max-width:900px){#lb-replay .runsteps{overflow-x:auto;min-height:310px}#lb-replay .step{flex:0 0 175px}#lb-replay .step.current{flex:0 0 245px}#lb-replay .pipe{grid-template-columns:repeat(2,1fr)}#lb-replay .phasepanel{grid-template-columns:1fr}#lb-replay .platform{flex:auto}#lb-replay .traces{height:170px}#lb-replay .controls{position:sticky;bottom:0}}

#lb-replay{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#1c332c;margin-bottom:1.5rem}
#lb-replay *{box-sizing:border-box}
#lb-replay h1,#lb-replay h2{border:0;padding:0;line-height:1.15}
#lb-replay button,#lb-replay input{font:inherit}
#lb-replay button{cursor:pointer}
#lb-replay button:disabled{cursor:default;opacity:.55}
#lb-replay .headline{margin-bottom:8px}
#lb-replay .headline-right strong span{font-size:inherit;color:inherit}
#lb-replay .canvas{display:block;background:#fff;border:1px solid #dce6dd;border-radius:12px;overflow:hidden}
#lb-replay .runsteps{min-height:300px}
#lb-replay .platform{flex:none;height:auto}
#lb-replay .traces{height:140px}
#lb-replay .platformnav button:disabled{text-decoration:line-through}
#lb-replay .job span.fail{color:#b3261e}
#lb-replay .controls .lb-unit button{background:transparent;color:#fff;border:1px solid #6fbb89;padding:6px 12px;font-size:14px}
#lb-replay .controls .lb-unit button.on{background:#d4f4dd;border-color:#d4f4dd;color:#1d5439}
#lb-replay .legend{display:flex;gap:12px;font-size:12px;color:#57715e;min-height:0}
#lb-replay .legend:empty{display:none}
#lb-replay .legend i{display:inline-block;width:10px;height:3px;margin-right:5px;vertical-align:middle;border-radius:2px}
#lb-replay .note{padding:2px 14px 8px;font-size:11px;color:#859187}

"""

_JS = r"""
(function(){
var el=document.getElementById('lb-replay-data');if(!el)return;
var D=JSON.parse(el.textContent),root=document.getElementById('lb-replay');
var S=D.stages,J=D.jobs,T=D.duration,t=0,playing=false,speed=1,last=0,sel=null,lane=D.lag?'lag':D.storage?'storage':(D.lanes.compute?'compute':(D.lanes.network?'network':(D.lanes.k8s?'k8s':'storage')));
function $(s){return root.querySelector(s)}
function svg(s){return '<svg viewBox="0 0 200 95" aria-hidden="true">'+s+'</svg>'}
var ART=[
 svg('<g fill="#97bbbf">'+[0,1,2,3,4].map(function(i){return '<rect x="'+(20+i*32)+'" y="'+(30+i%2*12)+'" width="23" height="34" rx="3" opacity="'+(1-i*.13)+'"/><path d="M'+(25+i*32)+' '+(37+i%2*12)+'h13m-13 5h13" stroke="white" stroke-width="1.6"/>'}).join('')+'</g>'),
 svg('<g fill="#accddd">'+[0,1,2,3].map(function(i){return '<rect x="'+(16+i*25)+'" y="'+(22+i%2*9)+'" width="18" height="40" rx="3"/>'}).join('')+'</g><path d="M115 48h23" stroke="#5994b5" stroke-width="3"/><rect x="140" y="22" width="40" height="48" rx="6" fill="#d8edf4" stroke="#5994b5" stroke-width="2"/><path d="m148 46 10 9 15-19" fill="none" stroke="#5994b5" stroke-width="3"/>'),
 svg('<g fill="#e2c895">'+[0,1,2,3,4,5,6,7,8].map(function(i){return '<rect x="'+(10+(i%3)*26)+'" y="'+(19+Math.floor(i/3)*19)+'" width="19" height="13" rx="2"/>'}).join('')+'</g><path d="M100 48h28m-7-7 8 7-8 7" stroke="#c1a574" stroke-width="2" fill="none"/><rect x="138" y="23" width="47" height="47" rx="5" fill="#f5ebda" stroke="#d2b485"/><g fill="#d0b68b"><rect x="147" y="32" width="7" height="28"/><rect x="160" y="39" width="7" height="21"/><rect x="173" y="29" width="7" height="31"/></g>'),
 svg('<g fill="#83b598">'+[0,1,2,3].map(function(i){return '<rect x="'+(29+i*34)+'" y="'+(27+i%2*9)+'" width="28" height="12" rx="4" opacity="'+(.55+i*.1)+'"/>'}).join('')+'</g><g stroke="#739e82" stroke-width="2" fill="none"><path d="M42 42v25h95V50"/><path d="m129 57 8-7 8 7"/></g>'),
 svg('<g fill="#ded0e9" stroke="#a986c4">'+[0,1,2,3].map(function(i){return '<rect x="'+(19+i*43)+'" y="'+(28+i%2*5)+'" width="32" height="45" rx="6"/>'}).join('')+'</g><g stroke="#a986c4" stroke-width="2">'+[0,1,2,3].map(function(i){return '<path d="M'+(25+i*43)+' '+(46+i%2*5)+'h18m-18 7h14"/>'}).join('')+'</g>')];
root.querySelectorAll('.art').forEach(function(a,i){a.innerHTML=ART[i]});
function fmtT(x){x=Math.max(0,Math.round(x));var m=Math.floor(x/60),s=x%60;return (m<10?'0':'')+m+':'+(s<10?'0':'')+s}
function num(n){return Math.round(n).toLocaleString('en-US')}
function frac(s){if(s.start==null||s.end==null)return null;if(t<=s.start)return 0;if(t>=s.end)return 1;return (t-s.start)/Math.max(1e-9,s.end-s.start)}
function stageValue(s){
 if(s.value==null)return null;
 if(s.steps){var v=0;for(var i=0;i<s.steps.length;i++)if(s.steps[i][0]<=t)v=s.steps[i][1];return v}
 if(s.ends){var n=0;for(var i=0;i<s.ends.length;i++)if(s.ends[i]<=t)n++;return n}
 var f=frac(s);return f===null?s.value:s.value*f}
function state(s){var f=frac(s);return f===null?null:f===0?'waiting':f===1?'done':'running'}
function rate(x){var r=0;S.forEach(function(s){if(s.key!=='query'&&s.rows!==false&&!s.steps&&s.value!=null&&s.start!=null&&s.end>s.start&&x>=s.start&&x<s.end)r+=s.value/(s.end-s.start)});return r}
var RATE=[],N=240;for(var i=0;i<N;i++)RATE.push(rate((i+.5)/N*T));
var RMAX=Math.max.apply(null,RATE.concat([1]));
function shade(){if(!D.window)return '';var a=5+590*D.window.start/T,b=5+590*D.window.end/T;return '<rect x="'+a.toFixed(1)+'" y="0" width="'+(b-a).toFixed(1)+'" height="80" fill="#e4f1e8"/>'}
function plot(id,series,mx){
 var o='<rect width="600" height="80" fill="#f4f8f4"/>'+shade();
 S.forEach(function(s){if(s.start!=null&&s.key!=='datagen'){var x=5+590*s.start/T;o+='<line x1="'+x+'" x2="'+x+'" y1="4" y2="77" stroke="#a4b9a9" stroke-dasharray="3 4" opacity=".65"/>'}});
 series.forEach(function(sr){if(!sr.v)return;var n=sr.v.length,d='',dt=sr.dt||T/n,sh=sr.shift||0;
  for(var i=0;i<n;i++){var x=5+590*(sh+i*dt)/T,x2=5+590*(sh+(i+1)*dt)/T,y=73-(sr.v[i]/mx)*61;d+=(i?'L':'M')+x.toFixed(1)+','+y.toFixed(1)+'L'+x2.toFixed(1)+','+y.toFixed(1)}
  o+='<path d="'+d+'" fill="none" stroke="'+sr.c+'" stroke-width="2.5" stroke-linejoin="round"/>'});
 var cx=5+590*t/T;o+='<line x1="'+cx+'" x2="'+cx+'" y1="2" y2="78" stroke="#277353" stroke-width="2" stroke-dasharray="3 3"/>';
 $(id).innerHTML=o}
function lagAt(h){var v=null;for(var i=0;i<h.samples.length;i++){if(D.lag.shift+h.samples[i][0]<=t)v=h.samples[i][1]}return v}
var LMAX=1;if(D.lag)D.lag.series.forEach(function(sr){sr.samples.forEach(function(p){if(p[1]>LMAX)LMAX=p[1]})});
function fmtLag(x){return x<120?Math.round(x)+' s':Math.round(x/60)+' min'}
function lagBars(){D.handoffs.forEach(function(h,i){var v=lagAt(h);
 $('[data-f="'+i+'"]').style.width=(v===null?0:Math.min(100,v/LMAX*100))+'%';
 $('[data-h="'+i+'"]').textContent=v===null?'Waiting':fmtLag(v)+' behind';
 $('[data-s="'+i+'"]').textContent=h.keeps_up===false?'falls behind the arrival rate':h.keeps_up?'keeps up':''})}
var LCOL=['#4b9d78','#be904a','#538da9'];
function plotLag(){var o='<rect width="600" height="80" fill="#f4f8f4"/>'+shade(),cols=LCOL;
 D.lag.series.forEach(function(sr,k){var d='';sr.samples.forEach(function(p,i){var x=5+590*(D.lag.shift+p[0])/T,y=73-p[1]/LMAX*61;d+=(i?'L':'M')+x.toFixed(1)+','+y.toFixed(1)});
  o+='<path d="'+d+'" fill="none" stroke="'+cols[k%3]+'" stroke-width="2.5" stroke-linejoin="round"/>'});
 var cx=5+590*t/T;o+='<line x1="'+cx+'" x2="'+cx+'" y1="2" y2="78" stroke="#277353" stroke-width="2" stroke-dasharray="3 3"/>';$('#rp-lplot').innerHTML=o}
var st=D.storage,SMAX=st?Math.max(1,Math.max.apply(null,st.read.concat(st.write))):1;
function draw(){
 var cur=null,doneAll=t>=T;
 S.forEach(function(s,i){
  var c=root.querySelector('[data-step="'+i+'"]'),v=stageValue(s),stt=state(s);
  if(stt==='running')cur=i;
  c.querySelector('.value strong').textContent=v==null?'NR':num(v);
  c.classList.toggle('done',stt==='done')});
 var focus=sel!==null?sel:(cur!==null?cur:(doneAll?S.length-1:0));
 S.forEach(function(s,i){root.querySelector('[data-step="'+i+'"]').classList.toggle('current',i===focus)});
 var f=S[focus],ff=frac(f);
 $('#rp-phase').textContent=f.phase+' · '+f.name;
 $('#rp-phasenote').textContent=ff===null?'no window recorded':ff===0?'not started':ff===1?'complete':Math.round(ff*100)+'% of the stage window';
 J.forEach(function(j,i){var c=root.querySelector('[data-job="'+i+'"]');
  var js=(j.start==null||j.end==null)?null:(t<j.start?'waiting':t>=j.end?'done':'running');
  c.classList.toggle('on',js==='running');
  c.querySelector('strong').textContent=js==='done'&&j.elapsed!=null?j.elapsed.toFixed(1)+' s':js==='running'?'running':'-';
  c.querySelector('span').textContent=js==='done'&&j.success===false?'failed':js||''});
 if(D.kind==='sustained')lagBars();else for(var i=0;i<4;i++){var b=S[i+1],fb=frac(b);
  $('[data-f="'+i+'"]').style.width=(fb===null?0:fb*100)+'%';
  $('[data-h="'+i+'"]').textContent=fb===null?'NR':fb===0?'Waiting':fb===1?'Complete':'Processing';
  $('[data-s="'+i+'"]').textContent=fb===null?'no window recorded':fb===0?'Awaiting upstream':fb===1?'Output available':Math.round(fb*100)+'% of '+b.name}
 var k=st?Math.min(st.read.length-1,Math.floor(t/st.step)):0;
 if(lane==='lag'){
  $('#rp-lname').textContent='Lag behind upstream';
  $('#rp-lval').textContent=D.handoffs.map(function(h){var v=lagAt(h);return v===null?'-':fmtLag(v)}).join(' · ');
  plotLag();$('#rp-legend').innerHTML=D.handoffs.map(function(h,k){return '<span><i style="background:'+LCOL[k%3]+'"></i>'+h.label+'</span>'}).join('')}
 else if(lane==='storage'){
  $('#rp-legend').innerHTML='';
  $('#rp-lname').textContent='Storage / S3';
  var spark=S.some(function(s){return s.key!=='datagen'&&s.key!=='query'&&s.start!=null&&t>=s.start&&t<s.end});
  $('#rp-lval').textContent=!st||spark?'NR':st.read[k].toFixed(1)+' MB/s read · '+st.write[k].toFixed(1)+' MB/s written';
  plot('#rp-lplot',st?[{v:st.read,c:'#37876d'},{v:st.write,c:'#be904a'}]:[],SMAX)}
 else{
  var L=D.lanes[lane],ix=function(a){return Math.max(0,Math.min(a.length-1,Math.floor((t-L.shift)/L.step)))};
  if(lane==='compute'){
   $('#rp-legend').innerHTML='';
   $('#rp-lname').textContent='Compute / CPU';
   $('#rp-lval').textContent=L.cpu[ix(L.cpu)].toFixed(1)+' cores · '+(L.mem.length?L.mem[ix(L.mem)].toFixed(0):'NR')+' GiB';
   plot('#rp-lplot',[{v:L.cpu,c:'#538da9',dt:L.step,shift:L.shift}],Math.max(1,Math.max.apply(null,L.cpu)))}
  else if(lane==='network'){
   $('#rp-legend').innerHTML='';
   $('#rp-lname').textContent='Pod network (includes shuffle)';
   $('#rp-lval').textContent=(L.rx.length?L.rx[ix(L.rx)].toFixed(1):'NR')+' MB/s in · '+(L.tx.length?L.tx[ix(L.tx)].toFixed(1):'NR')+' MB/s out';
   plot('#rp-lplot',[{v:L.rx,c:'#37876d',dt:L.step,shift:L.shift},{v:L.tx,c:'#be904a',dt:L.step,shift:L.shift}],Math.max(1,Math.max.apply(null,L.rx.concat(L.tx))))}
  else{
   $('#rp-legend').innerHTML='';
   $('#rp-lname').textContent='Pods running';
   $('#rp-lval').textContent=L.pods[ix(L.pods)]+' pods';
   plot('#rp-lplot',[{v:L.pods,c:'#899cad',dt:L.step,shift:L.shift}],Math.max(1,Math.max.apply(null,L.pods)))}}
 var kr=Math.min(N-1,Math.floor(t/T*N));
 $('#rp-evalue').textContent=num(RATE[kr])+' rows/s';
 plot('#rp-eplot',[{v:RATE,c:'#4b9d78'}],RMAX);
 $('[data-act=clock]').textContent=fmtT(t)+' / '+fmtT(T);
 $('[data-act=seek]').value=t}
function tick(now){if(playing){t=Math.min(T,t+(now-last)/1000*speed*Math.max(1,T/60));if(t>=T){playing=false;$('[data-act=play]').textContent='▶ Play'}draw()}last=now;requestAnimationFrame(tick)}
root.querySelectorAll('.step').forEach(function(c){c.onclick=function(){sel=+c.dataset.step;draw()};c.onkeydown=function(e){if(e.key==='Enter'||e.key===' '){e.preventDefault();sel=+c.dataset.step;draw()}}});
root.querySelectorAll('[data-lane]').forEach(function(b){b.onclick=function(){lane=b.dataset.lane;root.querySelectorAll('[data-lane]').forEach(function(x){x.classList.toggle('on',x===b)});draw()}});
$('[data-act=play]').onclick=function(e){if(t>=T)t=0;playing=!playing;last=performance.now();e.currentTarget.textContent=playing?'Ⅱ Pause':'▶ Play'};
$('[data-act=speed]').onclick=function(e){speed=speed===1?2:speed===2?4:1;e.currentTarget.textContent=speed+'×'};
$('[data-act=restart]').onclick=function(){t=0;sel=null;playing=false;$('[data-act=play]').textContent='▶ Play';draw()};
var sk=$('[data-act=seek]');sk.max=T;sk.oninput=function(e){t=+e.target.value;draw()};
draw();requestAnimationFrame(tick);
})();
"""


def _json_for_script(data: Mapping[str, Any]) -> str:
    return json.dumps(data, separators=(",", ":")).replace("</", "<\\/")


def render_replay(data: Mapping[str, Any], pace: Any = None) -> str:
    """The replay tab's panel: v28's markup and styles, the data block and
    the player. *pace* is the scorecard's Pace tile; its figure leads the
    headline and follows the same Count / Bytes switch."""
    e = escape
    stages = data["stages"]
    steps = "".join(
        f'<article class="step" data-step="{i}" tabindex="0" role="button">'
        f"<small>{e(s['phase'])}</small><h2>{e(s['name'])}</h2>"
        f'<div class="implementation">{e(s["impl"])}</div><div class="art"></div>'
        f'<div class="value"><strong>-</strong><label>{e(s["label"])}</label></div></article>'
        for i, s in enumerate(stages)
    )
    jobs = "".join(
        f'<div class="job" data-job="{i}"><b>{e(str(j["name"]))}</b><strong>-</strong><span></span></div>'
        for i, j in enumerate(data["jobs"])
    )
    pipe = "".join(
        f'<div class="handoff"><div class="handoff-head"><b>{e(stages[i]["phase"])} → '
        f'{e(stages[i + 1]["phase"])}</b><strong data-h="{i}">Waiting</strong></div>'
        f'<div class="track"><div class="fill" data-f="{i}"></div></div>'
        f'<div class="sub" data-s="{i}"></div></div>'
        for i in range(4)
    )
    if data.get("kind") == "sustained":
        pipe = "".join(
            f'<div class="handoff"><div class="handoff-head"><b>{e(h["label"])}</b>'
            f'<strong data-h="{i}">Waiting</strong></div>'
            f'<div class="track"><div class="fill" data-f="{i}"></div></div>'
            f'<div class="sub" data-s="{i}"></div></div>'
            for i, h in enumerate(data["handoffs"])
        )
    lanes = list(data["not_measured"])
    if not data["storage"]:
        lanes.insert(0, "Storage")
    have = data.get("lanes") or {}
    first = (
        "lag"
        if data.get("lag")
        else "storage"
        if data["storage"]
        else next((k for k, _ in _LANE_LABELS if k in have), "")
    )
    on = lambda k: ' class="on"' if k == first else ""  # noqa: E731
    nav = (
        f'<button type="button" data-lane="lag"{on("lag")}>Lag</button>' if data.get("lag") else ""
    )
    nav += (
        f'<button type="button" data-lane="storage"{on("storage")}>Storage</button>'
        if data["storage"]
        else ""
    )
    nav += "".join(
        f'<button type="button" data-lane="{k}"{on(k)}>{e(label)}</button>'
        for k, label in _LANE_LABELS
        if k in have
    )
    nav += "".join(
        f'<button type="button" disabled title="not in the run record">{e(n)}</button>'
        for n in lanes
    )
    gaps = sorted(set(data["storage_gaps"]) | set(data["storage_unplaced"]))
    unmeasured = [*lanes, *(f"{g} storage" for g in gaps)]
    note = "Not measured: " + ", ".join(unmeasured) + "."
    win = data.get("window")
    if win:
        mm = lambda x: f"{int(x) // 60}:{int(x) % 60:02d}"  # noqa: E731
        note += (
            f" Shaded: the measured window, {mm(win['start'])} to {mm(win['end'])};"
            " the rest is drain and maintenance."
        )
    problems = data.get("storage_problems") or []
    if problems:
        note += " Partial: " + "; ".join(problems) + "."
    switch = unit_switch(pace.unit_label) if pace is not None and pace.alt_value else ""
    pace_html = ""
    if pace is not None and pace.evidence != "nr":
        alt = (
            f'<strong data-unit="bytes" hidden>{pace.alt_value}</strong>' if pace.alt_value else ""
        )
        count_hidden = ' data-unit="count"' if pace.alt_value else ""
        pace_html = (
            f'<div class="mini"><strong{count_hidden}>{pace.value}</strong>{alt}'
            f"<span>{e(pace.sub.split(' in ')[0].split(' ', 1)[-1])} per second</span></div>"
        )
    return (
        f"<style>{_CSS}{UNIT_CSS}</style>"
        '<div id="lb-replay"><div class="headline"><div>'
        f'<div class="eyebrow">{e(data["subtitle"])}</div><h1>{e(data["title"])}</h1></div>'
        '<div class="headline-right">'
        f"{pace_html}"
        '<div class="mini">'
        f"<strong>{int(data['duration']) // 60}:{int(data['duration']) % 60:02d}</strong>"
        "<span>run length</span></div></div></div>"
        '<div class="canvas"><div class="layer-head">Workload</div>'
        f'<div class="runsteps">{steps}</div>'
        f'<div class="phasepanel"><div class="phasename"><small>{"Continuous" if data.get("kind") == "sustained" else "Batch"} execution</small>'
        '<strong id="rp-phase">-</strong><span id="rp-phasenote"></span></div>'
        f'<div class="jobs">{jobs}</div></div>'
        '<div class="pipe-head">Pipeline</div>'
        f'<div class="pipe">{pipe}</div>'
        '<div class="platform-head">Platform</div>'
        f'<div class="platform"><div class="platformnav">{nav}</div>'
        '<div class="traces"><div class="graph"><div class="charttitle">'
        '<b id="rp-lname">Storage / S3</b><strong id="rp-lval">-</strong></div>'
        '<svg id="rp-lplot" viewBox="0 0 600 80" preserveAspectRatio="none"></svg>'
        '<div class="legend" id="rp-legend"></div></div>'
        '<div class="graph"><div class="charttitle"><b>Rows per second</b>'
        '<strong id="rp-evalue">-</strong></div>'
        '<svg id="rp-eplot" viewBox="0 0 600 80" preserveAspectRatio="none"></svg></div>'
        "</div></div>"
        f'<div class="note">{e(note)}</div>'
        '<div class="controls"><button type="button" data-act="play">▶ Play</button>'
        '<button type="button" data-act="speed">1×</button>'
        '<button type="button" data-act="restart">↺ Restart</button>'
        f"{switch}"
        '<input type="range" min="0" max="0" value="0" step="1" data-act="seek" '
        'aria-label="Run timeline"><span class="time" data-act="clock"></span></div></div></div>'
        f'<script type="application/json" id="lb-replay-data">{_json_for_script(data)}</script>'
        f"<script>{_JS}</script>"
    )
