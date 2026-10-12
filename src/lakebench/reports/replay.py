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
        "title": "AML / Batch",
        "subtitle": "Financial · Transaction monitoring",
        "phases": ("Generate", "Ingest", "Prepare", "Monitor", "Output"),
        "names": ("Payments", "Ingest", "Process", "AML rules", "Detections"),
        "labels": (
            "Rows generated",
            "Rows ingested",
            "Rows prepared",
            "Rows evaluated",
            "Queries completed",
        ),
    },
}

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
    for idx, key in enumerate(_STAGE_KEYS):
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
            value = rec.get("output_rows")
            path = f"pipeline_benchmark.stages[{key}].output_rows"
        stages.append(
            {
                "key": key,
                "phase": profile["phases"][idx],
                "name": profile["names"][idx],
                "impl": _IMPL[idx],
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

    return {
        "version": REPLAY_VERSION,
        "title": profile["title"],
        "subtitle": profile["subtitle"],
        "mode": (record.get("pipeline_benchmark") or {}).get("pipeline_mode"),
        "run_id": record.get("run_id"),
        "duration": duration,
        "stages": stages,
        "jobs": jobs,
        "storage": storage,
        "storage_unplaced": [i.get("source") for i in not_placed],
        "storage_gaps": gaps,
        "not_measured": ["Compute", "Network", "Kubernetes"],
    }


_CSS = """
.rp{--rp-ink:#1e293b;--rp-mut:#64748b;--rp-line:#e2e8f0;--rp-acc:#2563eb;--rp-w:#d97706}
.rp .rp-head{display:flex;justify-content:space-between;align-items:flex-end;gap:1rem;margin-bottom:1rem}
.rp .rp-eyebrow{font-size:.7rem;text-transform:uppercase;letter-spacing:.08em;color:var(--rp-mut)}
.rp h2.rp-title{font-size:1.5rem;border:0;margin:0;padding:0}
.rp .rp-legend{font-size:.75rem;color:var(--rp-mut)}
.rp .rp-legend b{color:var(--rp-ink)}
.rp .rp-layer{font-size:.75rem;text-transform:uppercase;letter-spacing:.08em;color:var(--rp-mut);margin:1.25rem 0 .5rem}
.rp .rp-steps{display:grid;grid-template-columns:repeat(5,1fr);gap:.75rem}
.rp .rp-step{border:1px solid var(--rp-line);border-radius:.5rem;padding:.75rem;background:#fff;cursor:pointer;text-align:left;font:inherit;color:inherit}
.rp .rp-step.sel{border-color:var(--rp-acc);box-shadow:0 0 0 2px rgba(37,99,235,.15)}
.rp .rp-step.run{border-color:var(--rp-acc)}
.rp .rp-step.done{background:#f0fdf4}
.rp .rp-step small{display:block;font-size:.7rem;text-transform:uppercase;color:var(--rp-mut)}
.rp .rp-step h3{font-size:1rem;margin:.1rem 0}
.rp .rp-step .rp-impl{font-size:.75rem;color:var(--rp-mut)}
.rp .rp-step strong{display:block;font-size:1.4rem;margin-top:.5rem;font-variant-numeric:tabular-nums}
.rp .rp-step label{font-size:.7rem;color:var(--rp-mut)}
.rp .rp-step .rp-state{font-size:.7rem;margin-top:.25rem;color:var(--rp-mut)}
.rp .rp-jobs{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:.5rem;margin-top:.75rem}
.rp .rp-job{border:1px solid var(--rp-line);border-radius:.4rem;padding:.5rem .75rem;font-size:.8rem;background:#fff}
.rp .rp-job b{display:block;font-weight:600}
.rp .rp-job.run{border-color:var(--rp-acc)}
.rp .rp-pipe{display:grid;grid-template-columns:repeat(4,1fr);gap:.75rem}
.rp .rp-hand{border:1px solid var(--rp-line);border-radius:.5rem;padding:.6rem .75rem;background:#fff}
.rp .rp-hand b{font-size:.85rem}
.rp .rp-hand strong{float:right;font-size:.8rem}
.rp .rp-track{height:8px;background:#e2e8f0;border-radius:4px;margin:.5rem 0;overflow:hidden}
.rp .rp-fill{height:100%;width:0;background:var(--rp-acc)}
.rp .rp-hand .rp-sub{font-size:.7rem;color:var(--rp-mut)}
.rp .rp-plat{display:grid;grid-template-columns:1fr;gap:.75rem}
.rp .rp-nav{display:flex;gap:.5rem;flex-wrap:wrap}
.rp .rp-nav span{font-size:.8rem;padding:.2rem .7rem;border-radius:9999px;border:1px solid var(--rp-line)}
.rp .rp-nav span.on{background:var(--rp-acc);color:#fff;border-color:var(--rp-acc)}
.rp .rp-nav span.off{color:var(--rp-mut);text-decoration:line-through}
.rp .rp-graph{border:1px solid var(--rp-line);border-radius:.5rem;padding:.6rem .75rem;background:#fff}
.rp .rp-graph .rp-ct{display:flex;justify-content:space-between;font-size:.85rem}
.rp .rp-graph svg{width:100%;height:110px;display:block}
.rp .rp-ctl{display:flex;align-items:center;gap:.75rem;margin-top:1rem;flex-wrap:wrap}
.rp .rp-ctl button{font:inherit;border:1px solid var(--rp-line);background:#fff;border-radius:.4rem;padding:.3rem .8rem;cursor:pointer}
.rp .rp-ctl input[type=range]{flex:1;min-width:160px}
.rp .rp-clock{font-variant-numeric:tabular-nums;font-size:.85rem;color:var(--rp-mut)}
.rp .rp-note{font-size:.75rem;color:var(--rp-mut);margin-top:.75rem}
@media(max-width:800px){.rp .rp-steps,.rp .rp-pipe{grid-template-columns:1fr}}
"""

_JS = r"""
(function(){
var el=document.getElementById('lb-replay-data');if(!el)return;
var D=JSON.parse(el.textContent),root=document.getElementById('lb-replay');
var S=D.stages,J=D.jobs,T=D.duration,t=0,playing=false,speed=1,last=0,sel=null;
function $(s){return root.querySelector(s)}
function fmtT(x){x=Math.max(0,Math.round(x));var m=Math.floor(x/60),s=x%60;return (m<10?'0':'')+m+':'+(s<10?'0':'')+s}
function num(n){return Math.round(n).toLocaleString('en-US')}
function frac(s){if(s.start==null||s.end==null)return null;if(t<=s.start)return 0;if(t>=s.end)return 1;return (t-s.start)/Math.max(1e-9,s.end-s.start)}
function stageValue(s){
 if(s.value==null)return null;
 if(s.ends){var n=0;for(var i=0;i<s.ends.length;i++)if(s.ends[i]<=t)n++;return n}
 var f=frac(s);if(f===null)return s.value;return s.value*f}
function state(s){var f=frac(s);if(f===null)return 'no window recorded';return f===0?'waiting':f===1?'done':'running'}
function plot(id,arr,color,mx,step){
 var svg=$(id),w=600,h=100,p='';
 if(!arr){svg.innerHTML='';return}
 var n=arr.length;
 var d='M0,'+h;for(var i=0;i<n;i++){var x=i/n*w,y=h-(arr[i]/mx)*(h-6);d+='L'+x.toFixed(1)+','+y.toFixed(1)+'L'+((i+1)/n*w).toFixed(1)+','+y.toFixed(1)}
 d+='L'+w+','+h+'Z';
 p='<path d="'+d+'" fill="'+color+'" fill-opacity=".25" stroke="'+color+'" stroke-width="1.2"/>';
 svg.insertAdjacentHTML('beforeend',p)}
function drawStorage(){
 var st=D.storage;if(!st)return;
 var svg=$('#rp-st'),w=600,h=100,mx=Math.max(1,Math.max.apply(null,st.read.concat(st.write)));
 svg.innerHTML='';plot('#rp-st',st.read,'#2563eb',mx);plot('#rp-st',st.write,'#d97706',mx);
 svg.insertAdjacentHTML('beforeend','<line id="rp-ph" x1="0" x2="0" y1="0" y2="'+h+'" stroke="#1e293b" stroke-width="1"/>');
 $('#rp-stmax').textContent='peak '+mx.toFixed(1)+' MB/s'}
function draw(){
 var running=null;
 S.forEach(function(s,i){
  var c=root.querySelector('[data-step="'+i+'"]'),v=stageValue(s),st=state(s);
  c.className='rp-step '+(st==='running'?'run ':'')+(st==='done'?'done ':'')+(sel===i?'sel':'');
  c.querySelector('strong').textContent=v==null?'not measured':num(v);
  c.querySelector('.rp-state').textContent=st+(s.window?'':'');
  if(st==='running')running=s});
 J.forEach(function(j,i){var c=root.querySelector('[data-job="'+i+'"]');
  var st=(j.start==null||j.end==null)?'':(t<j.start?'waiting':t>=j.end?'done':'running');
  c.className='rp-job '+(st==='running'?'run':'');
  c.querySelector('span').textContent=st==='done'?(j.elapsed==null?'done':j.elapsed.toFixed(1)+' s'+(j.success===false?' · failed':'')):st||'-'});
 for(var i=0;i<4;i++){var a=S[i],b=S[i+1],f=frac(b),fl=root.querySelector('[data-fill="'+i+'"]'),h=root.querySelector('[data-hs="'+i+'"]');
  var pct=f===null?0:f*100;fl.style.width=pct+'%';
  h.textContent=f===null?'not recorded':f===0?'Waiting':f===1?'Done':'Moving'}
 var ph=running?running.phase+' · '+running.name:(t>=T?'Run complete':'Waiting');
 $('#rp-phase').textContent=ph;
 $('#rp-clock').textContent=fmtT(t)+' / '+fmtT(T);
 root.querySelector('[data-act=seek]').value=t;
 var line=$('#rp-ph');if(line){var x=t/T*600;line.setAttribute('x1',x);line.setAttribute('x2',x)}
 if(D.storage){var k=Math.min(D.storage.read.length-1,Math.floor(t/D.storage.step));
  $('#rp-stval').textContent=D.storage.read[k].toFixed(1)+' MB/s read · '+D.storage.write[k].toFixed(1)+' MB/s written'}
}
function tick(now){if(playing){t=Math.min(T,t+(now-last)/1000*speed*Math.max(1,T/60));if(t>=T){playing=false;$('[data-act=play]').textContent='▶ Play'}}last=now;draw();requestAnimationFrame(tick)}
root.querySelectorAll('.rp-step').forEach(function(c){c.onclick=function(){sel=+c.dataset.step;draw()}});
$('[data-act=play]').onclick=function(e){if(t>=T)t=0;playing=!playing;last=performance.now();e.currentTarget.textContent=playing?'Ⅱ Pause':'▶ Play'};
$('[data-act=speed]').onclick=function(e){speed=speed===1?2:speed===2?4:1;e.currentTarget.textContent=speed+'×'};
$('[data-act=restart]').onclick=function(){t=0;playing=false;$('[data-act=play]').textContent='▶ Play';draw()};
root.querySelector('[data-act=seek]').oninput=function(e){t=+e.target.value;draw()};
root.querySelector('[data-act=seek]').max=T;
drawStorage();t=T;draw();requestAnimationFrame(tick);
})();
"""


def _json_for_script(data: Mapping[str, Any]) -> str:
    return json.dumps(data, separators=(",", ":")).replace("</", "<\\/")


def render_replay(data: Mapping[str, Any]) -> str:
    """The replay tab's panel: markup, styles, data block and player."""
    e = escape
    steps = "".join(
        f'<button class="rp-step" data-step="{i}" type="button">'
        f"<small>{e(s['phase'])}</small><h3>{e(s['name'])}</h3>"
        f'<div class="rp-impl">{e(s["impl"])}</div>'
        f"<strong>-</strong><label>{e(s['label'])}</label>"
        f'<div class="rp-state"></div></button>'
        for i, s in enumerate(data["stages"])
    )
    jobs = "".join(
        f'<div class="rp-job" data-job="{i}"><b>{e(str(j["name"]))}</b><span>-</span></div>'
        for i, j in enumerate(data["jobs"])
    )
    pipe = "".join(
        f'<div class="rp-hand"><b>{e(data["stages"][i]["name"])} → '
        f'{e(data["stages"][i + 1]["name"])}</b><strong data-hs="{i}">Waiting</strong>'
        f'<div class="rp-track"><div class="rp-fill" data-fill="{i}"></div></div>'
        f'<div class="rp-sub">share of the downstream stage window elapsed</div></div>'
        for i in range(4)
    )
    if data["storage"]:
        storage = (
            '<div class="rp-graph"><div class="rp-ct"><b>Storage (S3)</b>'
            '<span id="rp-stval"></span></div>'
            '<svg id="rp-st" viewBox="0 0 600 100" preserveAspectRatio="none"></svg>'
            '<div class="rp-ct"><span class="rp-clock"><span style="color:#2563eb">read</span>'
            ' · <span style="color:#d97706">written</span> · MB/s</span>'
            '<span class="rp-clock" id="rp-stmax"></span></div></div>'
        )
    else:
        storage = '<div class="rp-graph"><b>Storage (S3)</b>: not measured</div>'
    nav = '<span class="on">Storage</span>' + "".join(
        f'<span class="off">{e(n)}</span>' for n in data["not_measured"]
    )
    notes = []
    if data["storage_unplaced"]:
        notes.append(
            "No time window recorded, so not on the storage lane: "
            + ", ".join(sorted(set(data["storage_unplaced"])))
            + "."
        )
    if data["storage_gaps"]:
        notes.append(
            "Storage not measured for: "
            + ", ".join(data["storage_gaps"])
            + " (a blank lane there means no figure, not no traffic)."
        )
    notes.append(
        "Not measured: " + ", ".join(data["not_measured"]) + " (no figures in the run record)."
    )
    return (
        f"<style>{_CSS}</style>"
        '<div class="rp" id="lb-replay">'
        '<div class="rp-head"><div>'
        f'<div class="rp-eyebrow">{e(data["subtitle"])}</div>'
        f'<h2 class="rp-title">{e(data["title"])}</h2></div>'
        '<div class="rp-legend"><b>Recorded:</b> stage and job windows, row counts, storage bytes. '
        "<b>Derived:</b> the running count and the handoff bars, from each window. "
        "Nothing is simulated.</div></div>"
        '<div class="rp-layer">01 / Workload</div>'
        f'<div class="rp-steps">{steps}</div>'
        f'<div class="rp-jobs">{jobs}</div>'
        '<div class="rp-layer">02 / Pipeline <span id="rp-phase"></span></div>'
        f'<div class="rp-pipe">{pipe}</div>'
        '<div class="rp-layer">03 / Platform</div>'
        f'<div class="rp-nav">{nav}</div>'
        f'<div class="rp-plat">{storage}</div>'
        '<div class="rp-ctl"><button type="button" data-act="play">▶ Play</button>'
        '<button type="button" data-act="speed">1×</button>'
        '<button type="button" data-act="restart">↺ Restart</button>'
        '<input type="range" min="0" max="0" value="0" step="1" data-act="seek" '
        'aria-label="Run timeline">'
        '<span class="rp-clock" id="rp-clock"></span></div>'
        f'<div class="rp-note">{e(" ".join(notes))}</div>'
        "</div>"
        f'<script type="application/json" id="lb-replay-data">{_json_for_script(data)}</script>'
        f"<script>{_JS}</script>"
    )
