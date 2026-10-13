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


def build_tiles(
    record: Mapping[str, Any],
    *,
    mode: str,
    gates: Mapping[str, str] | None,
    metrics: Any = None,
) -> list[Tile]:
    """The eight tiles of a run record. *gates* is the verdict's gate
    outcomes (recomputed, as the page's verdict is). *metrics* is the loaded
    run, for the scores it stored."""
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
            _scalar(
                record,
                "Time to value",
                (*sc, "time_to_value_seconds"),
                fmt=",.0f",
                suffix=" s",
                sub="wall-clock to queryable gold",
            ),
            _scalar(
                record,
                "Throughput",
                (*sc, "pipeline_throughput_gb_per_second"),
                fmt=".3f",
                suffix=" GiB/s",
                sub="stage inputs processed per second",
            ),
        ]
        qph_key = "composite_qph"
    else:
        lead = [
            _scalar(
                record,
                "Throughput",
                (*sc, "sustained_throughput_rps"),
                fmt=",.0f",
                suffix=" rows/s",
                sub="sustained offered load",
            ),
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
.lb-tiles .lb-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:.75rem}
.lb-tiles .lb-tile{background:var(--card-bg);border-radius:.5rem;padding:1rem;box-shadow:0 1px 3px rgba(0,0,0,.1);border-top:3px solid var(--border)}
.lb-tiles .lb-tile.recorded{border-top-color:var(--primary)}
.lb-tiles .lb-tile.derived{border-top-color:var(--warning)}
.lb-tiles .lb-tile.nr{border-top-color:var(--border);color:var(--text-muted)}
.lb-tiles .lb-tile b{display:block;font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted)}
.lb-tiles .lb-tile strong{display:block;font-size:1.6rem;margin:.25rem 0;font-variant-numeric:tabular-nums}
.lb-tiles .lb-tile small{display:block;font-size:.72rem;color:var(--text-muted)}
.lb-tiles .lb-legend{font-size:.75rem;color:var(--text-muted);margin-top:.6rem}
.lb-tiles .lb-legend i{display:inline-block;width:.7rem;height:.7rem;border-radius:2px;margin:0 .25rem 0 .75rem;vertical-align:-1px}
@media(max-width:900px){.lb-tiles .lb-grid{grid-template-columns:repeat(2,1fr)}}
"""


def render_tiles(tiles: list[Tile]) -> str:
    """The tile grid and the evidence legend."""
    cells = "".join(
        f'<div class="lb-tile {t.evidence}" title="{escape(t.source, quote=True)}">'
        f"<b>{escape(t.label)}</b><strong>{t.value}</strong><small>{escape(t.sub)}</small></div>"
        for t in tiles
    )
    return (
        f"<style>{_CSS}</style>"
        '<div class="lb-tiles"><div class="lb-grid">'
        f"{cells}</div>"
        '<div class="lb-legend"><b>Evidence:</b>'
        '<i style="background:var(--primary)"></i>Recorded: a stored value, shown as is.'
        '<i style="background:var(--warning)"></i>Derived: computed on this page from recorded '
        "fields."
        '<i style="background:var(--border)"></i>NR: not measured. Hover a tile for its record '
        "field.</div></div>"
    )
