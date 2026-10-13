"""The scorecard tile grid: eight headline tiles, each naming its record field.

A tile shows one recorded figure, the field it comes from, and an evidence
class: **recorded** (the stored value as is) or **derived** (computed on the
page from recorded fields; the span names them, see ``reports/derived.py``).
A field the record does not hold shows ``NR`` and says why. Nothing is
estimated, and a tile is never dropped to hide a gap.

The tiles are the same eight for every run of a mode; the batch and
continuous sets differ in the first two and in the query-rate field.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from html import escape
from typing import Any

from lakebench.reports import derived as dv

_GIB = 1024**3

EVIDENCE_RECORDED = "recorded"
EVIDENCE_DERIVED = "derived"
EVIDENCE_NR = "nr"


@dataclass(frozen=True)
class Tile:
    label: str
    value: str  # HTML (a derived span, or plain text)
    sub: str  # plain text under the value
    evidence: str
    source: str  # the record field(s) the tile reads
    alt_value: str = ""  # the same figure in the other unit (bytes), when it has one
    alt_sub: str = ""
    unit_label: str = ""  # what the count view counts ("Interactions"), for the switch


def _num(record: Mapping[str, Any], *keys: str) -> float | None:
    cur: Any = record
    for k in keys:
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(k)
    if isinstance(cur, bool) or not isinstance(cur, (int, float)):
        return None
    return float(cur)


def _nr(label: str, why: str, source: str) -> Tile:
    return Tile(label, "NR", f"not measured: {why}", EVIDENCE_NR, source)


def _scalar(
    record: Mapping[str, Any],
    label: str,
    keys: tuple[str, ...],
    *,
    fmt: str,
    suffix: str,
    sub: str,
) -> Tile:
    path = ".".join(keys)
    value = _num(record, *keys)
    if value is None:
        return _nr(label, "not in the run record", path)
    return Tile(
        label,
        dv.total(value, paths=path, fmt=fmt, suffix=suffix),
        sub,
        EVIDENCE_RECORDED,
        path,
    )


def _storage_tile(record: Mapping[str, Any]) -> Tile:
    io = record.get("io")
    totals = io.get("totals") if isinstance(io, Mapping) else None
    trino = totals.get("trino") if isinstance(totals, Mapping) else None
    read = trino.get("read_bytes") if isinstance(trino, Mapping) else None
    written = trino.get("write_bytes") if isinstance(trino, Mapping) else None
    if not isinstance(io, Mapping) or not isinstance(read, (int, float)):
        return _nr("Storage", "no Trino storage I/O in the run record", "io.totals")
    gaps = sorted(
        {
            str(n.get("source"))
            for n in (io.get("not_measured") if isinstance(io, Mapping) else None) or []
            if isinstance(n, Mapping) and n.get("stage") != "query"
        }
    )
    where = "Trino queries only" + (f"; {', '.join(gaps)} not measured" if gaps else "")
    # The record holds the bytes on each benchmark round's queries; the io
    # block is built from them, so the tile cites those fields.
    paths = sorted(
        {
            str(i["paths"]["read"])
            for i in io.get("intervals") or []
            if isinstance(i, Mapping)
            and i.get("source") == "trino"
            and isinstance(i.get("paths"), Mapping)
            and i["paths"].get("read")
        }
    )
    if paths:
        span = dv.total(
            read / _GIB,
            paths=[dv.product(p, "/1073741824") for p in paths],
            fmt=".2f",
            suffix=" GiB",
        )
    else:
        span = f"{read / _GIB:.2f} GiB"
    out = (
        f", {written / _GIB:.2f} GiB written"
        if isinstance(written, (int, float)) and written
        else ""
    )
    return Tile("Storage", span, f"read{out} - {where}", EVIDENCE_DERIVED, "io.totals.trino")


def _gates_tile(record: Mapping[str, Any], gates: Mapping[str, str] | None) -> Tile:
    if not gates:
        return _nr("Gates", "no gate outcomes", "verdict.gates")
    passed = sum(1 for v in gates.values() if v == "PASS")
    failed = sorted(k for k, v in gates.items() if v != "PASS")
    sub = "all passed" if not failed else "not passed: " + ", ".join(failed)
    return Tile("Gates", f"{passed} / {len(gates)}", sub, EVIDENCE_DERIVED, "verdict.gates")


def _support_tile(record: Mapping[str, Any]) -> Tile:
    sup = (record.get("experiment") or {}).get("support")
    state = sup.get("state") if isinstance(sup, Mapping) else None
    if not state:
        return _nr("Support", "no support state in the run record", "experiment.support.state")
    note = {
        "supported": "a release-matrix run validated this combination",
        "unverified": "passed does not establish support",
        "unsupported": "known not to work; numbers are not evidence",
    }.get(str(state), "")
    return Tile(
        "Support",
        escape(str(state).capitalize()),
        note,
        EVIDENCE_RECORDED,
        "experiment.support.state",
    )


# The workload's unit of operation, the stage that does the work on it and the
# record field that counts it: Customer 360 cleans interaction events (silver's
# output rows), AML screens payment transactions (the rows gold's detection
# took in). The bytes view reads that same stage's recorded input size.
_PACE = {
    "customer360": ("silver", "output_rows", "interactions cleaned", "interactions"),
    "financial": ("gold", "input_rows", "transactions screened", "transactions"),
}


def _pace_batch(record: Mapping[str, Any]) -> Tile:
    """Units of operation per second from the first stage's start to gold
    ready (``time_to_value_seconds``), and the bytes per second behind them."""
    schema = str((record.get("config_snapshot") or {}).get("workload_schema") or "customer360")
    stage_name, field, did, noun = _PACE.get(schema, _PACE["customer360"])
    stages = [
        s
        for s in (record.get("pipeline_benchmark") or {}).get("stages") or []
        if isinstance(s, Mapping)
    ]
    idx = next((i for i, s in enumerate(stages) if s.get("stage_name") == stage_name), None)
    count = _num(stages[idx], field) if idx is not None else None
    gib = _num(stages[idx], "input_size_gb") if idx is not None else None
    ttv = _num(record, "pipeline_benchmark", "scores", "time_to_value_seconds")
    count_path = dv.path("pipeline_benchmark", "stages", idx or 0, field)
    gib_path = dv.path("pipeline_benchmark", "stages", idx or 0, "input_size_gb")
    ttv_path = "pipeline_benchmark.scores.time_to_value_seconds"
    paths = f"{count_path} / {ttv_path}"
    if count is None or not ttv:
        return _nr("Pace", f"no {noun} count or time to value in the run record", paths)
    value = dv.ratio(
        count, ttv, a_path=count_path, b_path=ttv_path, fmt=",.0f", suffix=f" {noun}/s"
    )
    alt = ""
    alt_sub = ""
    if gib is not None:
        alt = dv.ratio(gib, ttv, a_path=gib_path, b_path=ttv_path, fmt=dv.AUTO_GIB, suffix="/s")
        alt_sub = f"{stage_name} input bytes in {ttv:,.0f} s"
    return Tile(
        "Pace",
        value,
        f"{count:,.0f} {did} in {ttv:,.0f} s",
        EVIDENCE_DERIVED,
        paths,
        alt,
        alt_sub,
        noun.capitalize(),
    )


def _pace_continuous(record: Mapping[str, Any]) -> Tile:
    """Rows entering bronze per second over the window, and whether the
    pipeline kept pace with them."""
    path = "pipeline_benchmark.scores.sustained_throughput_rps"
    rps = _num(record, "pipeline_benchmark", "scores", "sustained_throughput_rps")
    if rps is None:
        return _nr("Pace", "not in the run record", path)
    schema = str((record.get("config_snapshot") or {}).get("workload_schema") or "customer360")
    noun = _PACE.get(schema, _PACE["customer360"])[3]
    saturated = ((record.get("pipeline_benchmark") or {}).get("scores") or {}).get(
        "pipeline_saturated"
    )
    if saturated is True:
        kept = "offered load; the pipeline fell behind it"
    elif saturated is False:
        kept = "offered load; the pipeline kept pace"
    else:
        kept = "offered load"
    return Tile(
        "Pace",
        dv.total(rps, paths=path, fmt=",.0f", suffix=f" {noun}/s"),
        kept,
        EVIDENCE_RECORDED,
        path,
    )


def build_tiles(
    record: Mapping[str, Any],
    *,
    mode: str,
    gates: Mapping[str, str] | None,
    metrics: Any = None,
) -> list[Tile]:
    """The eight tiles of a run record. *gates* is the verdict's gate
    outcomes (recomputed, as the page's verdict is). *metrics* is the loaded
    run, for the scores it stored. Pace leads: rows through the pipeline per
    second, the one rate both modes share."""
    stored = getattr(metrics, "stored_scores", None) if metrics is not None else None
    if isinstance(stored, Mapping):
        # The stored scores, not the recomputed ones: a tile cites the record.
        record = {
            **record,
            "pipeline_benchmark": {**(record.get("pipeline_benchmark") or {}), "scores": stored},
        }
    sc = ("pipeline_benchmark", "scores")
    samples = _num(record, *sc, "benchmark_samples_per_query")
    qph_sub = "QpH" + (f" - {int(samples)} samples per query" if samples else "")
    if mode == "batch":
        lead = [
            _pace_batch(record),
            _scalar(
                record,
                "Time to value",
                (*sc, "time_to_value_seconds"),
                fmt=",.0f",
                suffix=" s",
                sub="first stage start to last stage end",
            ),
        ]
        qph_key = "composite_qph"
    else:
        lead = [
            _pace_continuous(record),
            _scalar(
                record,
                "Freshness",
                (*sc, "data_freshness_seconds"),
                fmt=",.0f",
                suffix=" s",
                sub="gold behind input",
            ),
        ]
        qph_key = "in_stream_composite_qph"
    return [
        *lead,
        _scalar(record, "Query rate", (*sc, qph_key), fmt=",.1f", suffix="", sub=qph_sub),
        _scalar(
            record,
            "Compute",
            (*sc, "total_core_hours"),
            fmt=".2f",
            suffix="",
            sub="core-hours requested",
        ),
        _scalar(
            record,
            "Efficiency",
            (*sc, "compute_efficiency_gb_per_core_hour"),
            fmt=".2f",
            suffix="",
            sub="stage-input GiB per core-hour",
        ),
        _storage_tile(record),
        _gates_tile(record, gates),
        _support_tile(record),
    ]


_CSS = """
.lb-tiles{margin-bottom:1.5rem}
.lb-tiles .lb-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.lb-tiles .lb-tile{background:var(--card-bg);border:1px solid var(--border);border-radius:10px;padding:18px;min-height:110px}
.lb-tiles .lb-tile b{display:block;font-size:13px;font-weight:600;color:#5f7767}
.lb-tiles .lb-tile strong{display:block;font-size:32px;letter-spacing:-1px;margin-top:7px;font-variant-numeric:tabular-nums}
.lb-tiles .lb-tile small{display:block;font-size:12px;color:#7c8e81;margin-top:2px}
.lb-tiles .lb-tile.nr strong{color:#9aa89d}
@media(max-width:900px){.lb-tiles .lb-grid{grid-template-columns:repeat(2,1fr)}}
"""


UNIT_CSS = """
.lb-unit{display:inline-flex;gap:4px;align-items:center}
.lb-unit button{border:1px solid #6fbb89;background:transparent;color:#fff;border-radius:6px;padding:6px 12px;font:inherit;font-size:14px;font-weight:650;cursor:pointer}
.lb-unit button.on{background:#d4f4dd;border-color:#d4f4dd;color:#1d5439}
"""


def unit_switch(label: str) -> str:
    """The Count / Bytes switch, named for what the count view counts. Every
    switch on the page flips every figure that has a second unit."""
    return (
        '<span class="lb-unit" role="group" aria-label="Pace unit">'
        f'<button type="button" data-set="count" class="on">{escape(label)}</button>'
        '<button type="button" data-set="bytes">Bytes</button></span>'
    )


_SCRIPT = (
    "<script>(function(){var u='count';function set(n){u=n;document.querySelectorAll("
    "'[data-unit]').forEach(function(e){e.hidden=e.dataset.unit!==u});document."
    "querySelectorAll('.lb-unit button').forEach(function(b){b.className="
    "b.dataset.set===u?'on':''})}document.querySelectorAll('.lb-unit button').forEach("
    "function(b){b.onclick=function(){set(b.dataset.set)}})})();</script>"
)


def render_tiles(tiles: list[Tile]) -> str:
    """The tile grid. A tile's record field is its hover text. A tile with a
    second unit carries a Count / Bytes switch that flips every such figure
    on the page together."""

    def cell(t: Tile) -> str:
        if t.alt_value:
            value = (
                f'<span data-unit="count">{t.value}</span>'
                f'<span data-unit="bytes" hidden>{t.alt_value}</span>'
            )
            sub = (
                f'<span data-unit="count">{escape(t.sub)}</span>'
                f'<span data-unit="bytes" hidden>{escape(t.alt_sub)}</span>'
            )
        else:
            value, sub = t.value, escape(t.sub)
        return (
            f'<div class="lb-tile {t.evidence}" title="{escape(t.source, quote=True)}">'
            f"<b>{escape(t.label)}</b><strong>{value}</strong><small>{sub}</small></div>"
        )

    cells = "".join(cell(t) for t in tiles)
    with_alt = any(t.alt_value for t in tiles)
    script = _SCRIPT if with_alt else ""
    return (
        f"<style>{_CSS}{UNIT_CSS}</style>"
        f'<div class="lb-tiles"><div class="lb-grid">{cells}</div></div>{script}'
    )
