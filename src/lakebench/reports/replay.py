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
#lb-replay .canvas{display:block;background:#fff;border:1px solid #dce6dd;border-radius:12px;overflow:hidden}
#lb-replay .runsteps{min-height:300px}
#lb-replay .platform{flex:none;height:auto}
#lb-replay .traces{height:140px}
#lb-replay .platformnav button:disabled{text-decoration:line-through}
#lb-replay .job span.fail{color:#b3261e}
#lb-replay .note{padding:2px 14px 8px;font-size:11px;color:#859187}

"""

_JS = r"""
(function(){
var el=document.getElementById('lb-replay-data');if(!el)return;
var D=JSON.parse(el.textContent),root=document.getElementById('lb-replay');
var S=D.stages,J=D.jobs,T=D.duration,t=0,playing=false,speed=1,last=0,sel=null,lane='storage';
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
 if(s.ends){var n=0;for(var i=0;i<s.ends.length;i++)if(s.ends[i]<=t)n++;return n}
 var f=frac(s);return f===null?s.value:s.value*f}
function state(s){var f=frac(s);return f===null?null:f===0?'waiting':f===1?'done':'running'}
function rate(x){var r=0;S.forEach(function(s){if(s.key!=='query'&&s.value!=null&&s.start!=null&&s.end>s.start&&x>=s.start&&x<s.end)r+=s.value/(s.end-s.start)});return r}
var RATE=[],N=240;for(var i=0;i<N;i++)RATE.push(rate((i+.5)/N*T));
var RMAX=Math.max.apply(null,RATE.concat([1]));
function plot(id,series,mx){
 var o='<rect width="600" height="80" fill="#f4f8f4"/>';
 S.forEach(function(s){if(s.start!=null&&s.key!=='datagen'){var x=5+590*s.start/T;o+='<line x1="'+x+'" x2="'+x+'" y1="4" y2="77" stroke="#a4b9a9" stroke-dasharray="3 4" opacity=".65"/>'}});
 series.forEach(function(sr){if(!sr.v)return;var n=sr.v.length,d='';
  for(var i=0;i<n;i++){var x=5+590*i/n,x2=5+590*(i+1)/n,y=73-(sr.v[i]/mx)*61;d+=(i?'L':'M')+x.toFixed(1)+','+y.toFixed(1)+'L'+x2.toFixed(1)+','+y.toFixed(1)}
  o+='<path d="'+d+'" fill="none" stroke="'+sr.c+'" stroke-width="2.5" stroke-linejoin="round"/>'});
 var cx=5+590*t/T;o+='<line x1="'+cx+'" x2="'+cx+'" y1="2" y2="78" stroke="#277353" stroke-width="2" stroke-dasharray="3 3"/>';
 $(id).innerHTML=o}
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
 for(var i=0;i<4;i++){var b=S[i+1],fb=frac(b);
  $('[data-f="'+i+'"]').style.width=(fb===null?0:fb*100)+'%';
  $('[data-h="'+i+'"]').textContent=fb===null?'NR':fb===0?'Waiting':fb===1?'Complete':'Processing';
  $('[data-s="'+i+'"]').textContent=fb===null?'no window recorded':fb===0?'Awaiting upstream':fb===1?'Output available':Math.round(fb*100)+'% of '+b.name}
 var k=st?Math.min(st.read.length-1,Math.floor(t/st.step)):0;
 $('#rp-lname').textContent='Storage / S3';
 var spark=S.some(function(s){return s.key!=='datagen'&&s.key!=='query'&&s.start!=null&&t>=s.start&&t<s.end});
 $('#rp-lval').textContent=!st||spark?'NR':st.read[k].toFixed(1)+' MB/s read · '+st.write[k].toFixed(1)+' MB/s written';
 plot('#rp-lplot',st?[{v:st.read,c:'#37876d'},{v:st.write,c:'#be904a'}]:[],SMAX);
 var kr=Math.min(N-1,Math.floor(t/T*N));
 $('#rp-evalue').textContent=num(RATE[kr])+' rows/s';
 plot('#rp-eplot',[{v:RATE,c:'#4b9d78'}],RMAX);
 $('[data-act=clock]').textContent=fmtT(t)+' / '+fmtT(T);
 $('[data-act=seek]').value=t}
function tick(now){if(playing){t=Math.min(T,t+(now-last)/1000*speed*Math.max(1,T/60));if(t>=T){playing=false;$('[data-act=play]').textContent='▶ Play'}draw()}last=now;requestAnimationFrame(tick)}
root.querySelectorAll('.step').forEach(function(c){c.onclick=function(){sel=+c.dataset.step;draw()};c.onkeydown=function(e){if(e.key==='Enter'||e.key===' '){e.preventDefault();sel=+c.dataset.step;draw()}}});
$('[data-act=play]').onclick=function(e){if(t>=T)t=0;playing=!playing;last=performance.now();e.currentTarget.textContent=playing?'Ⅱ Pause':'▶ Play'};
$('[data-act=speed]').onclick=function(e){speed=speed===1?2:speed===2?4:1;e.currentTarget.textContent=speed+'×'};
$('[data-act=restart]').onclick=function(){t=0;sel=null;playing=false;$('[data-act=play]').textContent='▶ Play';draw()};
var sk=$('[data-act=seek]');sk.max=T;sk.oninput=function(e){t=+e.target.value;draw()};
draw();requestAnimationFrame(tick);
})();
"""


def _json_for_script(data: Mapping[str, Any]) -> str:
    return json.dumps(data, separators=(",", ":")).replace("</", "<\\/")


def render_replay(data: Mapping[str, Any]) -> str:
    """The replay tab's panel: v28's markup and styles, the data block and
    the player."""
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
        f'<div class="handoff"><div class="handoff-head"><b>{e(stages[i]["name"])} → '
        f'{e(stages[i + 1]["name"])}</b><strong data-h="{i}">Waiting</strong></div>'
        f'<div class="track"><div class="fill" data-f="{i}"></div></div>'
        f'<div class="sub" data-s="{i}"></div></div>'
        for i in range(4)
    )
    lanes = list(data["not_measured"])
    if not data["storage"]:
        lanes.insert(0, "Storage")
    nav = (
        '<button type="button" class="on">Storage</button>' if data["storage"] else ""
    ) + "".join(
        f'<button type="button" disabled title="not in the run record">{e(n)}</button>'
        for n in lanes
    )
    gaps = sorted(set(data["storage_gaps"]) | set(data["storage_unplaced"]))
    note = "Not measured: " + ", ".join([*lanes, *(f"storage {g}" for g in gaps)])
    return (
        f"<style>{_CSS}</style>"
        '<div id="lb-replay"><div class="headline"><div>'
        f'<div class="eyebrow">{e(data["subtitle"])}</div><h1>{e(data["title"])}</h1></div>'
        '<div class="headline-right"><div class="mini">'
        f"<strong>{int(data['duration']) // 60}:{int(data['duration']) % 60:02d}</strong>"
        "<span>run length</span></div></div></div>"
        '<div class="canvas"><div class="layer-head">01 / Workload</div>'
        f'<div class="runsteps">{steps}</div>'
        '<div class="phasepanel"><div class="phasename"><small>Batch execution</small>'
        '<strong id="rp-phase">-</strong><span id="rp-phasenote"></span></div>'
        f'<div class="jobs">{jobs}</div></div>'
        '<div class="pipe-head">02 / Pipeline</div>'
        f'<div class="pipe">{pipe}</div>'
        '<div class="platform-head">03 / Platform</div>'
        f'<div class="platform"><div class="platformnav">{nav}</div>'
        '<div class="traces"><div class="graph"><div class="charttitle">'
        '<b id="rp-lname">Storage / S3</b><strong id="rp-lval">-</strong></div>'
        '<svg id="rp-lplot" viewBox="0 0 600 80" preserveAspectRatio="none"></svg></div>'
        '<div class="graph"><div class="charttitle"><b>Rows per second</b>'
        '<strong id="rp-evalue">-</strong></div>'
        '<svg id="rp-eplot" viewBox="0 0 600 80" preserveAspectRatio="none"></svg></div>'
        "</div></div>"
        f'<div class="note">{e(note)}</div>'
        '<div class="controls"><button type="button" data-act="play">▶ Play</button>'
        '<button type="button" data-act="speed">1×</button>'
        '<button type="button" data-act="restart">↺ Restart</button>'
        '<input type="range" min="0" max="0" value="0" step="1" data-act="seek" '
        'aria-label="Run timeline"><span class="time" data-act="clock"></span></div></div></div>'
        f'<script type="application/json" id="lb-replay-data">{_json_for_script(data)}</script>'
        f"<script>{_JS}</script>"
    )
