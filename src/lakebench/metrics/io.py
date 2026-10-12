"""Storage I/O of a run, as the engines themselves reported it.

``build_io`` derives the record's ``io`` block from the rest of the record, so
it is never stored separately and an older record simply has less in it. The
block holds intervals (a source, a stage, a time window, bytes read and bytes
written) and the places where nothing was measured. ``bucket_series`` spreads
the intervals over fixed time buckets for a rate-over-time view.

What a figure means:

- ``None`` is not measured. ``0`` is measured and zero.
- Every figure names its basis. Bytes from different bases are never added:
  Spark task output bytes and S3A write bytes count the same data twice for
  Delta tables, so only one of them is ever used for writes.
- Storage is what the storage clients report. Pod network bytes are a
  separate measurement and are not derived from or added to these.
- Trino bytes are ``physicalInputDataSize`` and ``physicalWrittenDataSize``
  (not the decoded, output or exchange sizes).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

IO_VERSION = 1

BASIS_TRINO_READ = "trino_physical_input"
BASIS_TRINO_WRITE = "trino_physical_written"
BASIS_DATAGEN = "datagen_fleet"
BASIS_NOT_MEASURED = "not_measured"

#: Why a stage has no figure, as recorded in ``io.not_measured``.
REASON_EXECUTOR_IO = "executor_io_not_collected"
REASON_ENGINE = "engine_not_instrumented"
REASON_NOT_RECORDED = "io_not_recorded"

_SPARK_STAGES = ("bronze", "silver", "gold")


def _iso(epoch: Any) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat(timespec="milliseconds")


def _parse(stamp: str) -> datetime:
    """An ISO timestamp as written by this module or by the record (Z or offset)."""
    return datetime.fromisoformat(stamp.replace("Z", "+00:00"))


def _benchmarks(record: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    """The benchmark rounds of the record with a label each.

    ``pipeline_benchmark.query_benchmark`` mirrors ``benchmark`` and is not
    read, so a round is never counted twice.
    """
    out: list[tuple[str, Mapping[str, Any]]] = []
    pre = (record.get("pipeline_benchmark") or {}).get("pre_compaction_benchmark")
    if isinstance(pre, Mapping) and pre.get("queries"):
        out.append(("pre_compaction", pre))
    bench = record.get("benchmark")
    if isinstance(bench, Mapping) and bench.get("queries"):
        out.append(("benchmark", bench))
    for i, rnd in enumerate(record.get("benchmark_rounds") or []):
        if not isinstance(rnd, Mapping) or not rnd.get("queries"):
            continue
        meta = rnd.get("round_meta") or {}
        index = rnd.get("index", meta.get("round_index", i + 1))
        out.append((f"round_{index}", rnd))
    return out


def _sum(intervals: list[dict[str, Any]], source: str, key: str) -> int | None:
    vals = [i[key] for i in intervals if i["source"] == source and i.get(key) is not None]
    return int(sum(vals)) if vals else None


def build_io(record: Mapping[str, Any]) -> dict[str, Any] | None:
    """The ``io`` block for *record* (a serialized run record), or None when the
    record has nothing to say about storage I/O."""
    intervals: list[dict[str, Any]] = []
    not_measured: list[dict[str, str]] = []
    problems: list[str] = []

    short = 0
    for label, bench in _benchmarks(record):
        engine = bench.get("engine") or "trino"
        queries = bench.get("queries") or []
        if engine != "trino":
            not_measured.append(
                {"source": engine, "stage": "query", "direction": "both", "reason": REASON_ENGINE}
            )
            continue
        for q in queries:
            io = q.get("io") if isinstance(q, Mapping) else None
            if not isinstance(io, Mapping):
                continue
            samples = int(io.get("samples") or 0)
            captured = int(io.get("captured") or 0)
            if captured < samples:
                short += samples - captured
            intervals.append(
                {
                    "source": "trino",
                    "stage": "query",
                    "label": label,
                    "query": q.get("name") or q.get("query_name"),
                    "start": _iso(io.get("start_epoch")),
                    "end": _iso(io.get("end_epoch")),
                    "clock": "trino_coordinator",
                    "read_bytes": io.get("read_bytes"),
                    "write_bytes": io.get("write_bytes"),
                    "read_basis": BASIS_TRINO_READ,
                    "write_basis": BASIS_TRINO_WRITE,
                    "complete": captured == samples and samples > 0,
                }
            )
        if not any(isinstance(q, Mapping) and isinstance(q.get("io"), Mapping) for q in queries):
            not_measured.append(
                {
                    "source": "trino",
                    "stage": "query",
                    "direction": "both",
                    "reason": REASON_NOT_RECORDED,
                }
            )
    if short:
        problems.append(f"trino: {short} query sample(s) had no I/O report")
    # One entry per round would repeat itself.
    not_measured = list(
        {(n["source"], n["stage"], n["direction"], n["reason"]): n for n in not_measured}.values()
    )

    fleet = record.get("datagen_fleet")
    if isinstance(fleet, Mapping) and fleet.get("total_bytes_written") is not None:
        stage: Mapping[str, Any] = next(
            (
                s
                for s in (record.get("pipeline_benchmark") or {}).get("stages") or []
                if s.get("stage_name") == "datagen"
            ),
            {},
        )
        intervals.append(
            {
                "source": "datagen",
                "stage": "datagen",
                "label": "datagen",
                "query": None,
                "start": stage.get("start_time") or None,
                "end": stage.get("end_time") or None,
                "clock": "host",
                "read_bytes": None,
                "write_bytes": int(fleet["total_bytes_written"]),
                "read_basis": None,
                "write_basis": BASIS_DATAGEN,
                "complete": int(fleet.get("pods_missing") or 0) == 0,
            }
        )

    stage_names = {
        s.get("stage_name") for s in (record.get("pipeline_benchmark") or {}).get("stages") or []
    }
    for name in _SPARK_STAGES:
        if name in stage_names:
            for direction in ("read", "write"):
                not_measured.append(
                    {
                        "source": "spark",
                        "stage": name,
                        "direction": direction,
                        "reason": REASON_EXECUTOR_IO,
                    }
                )

    if not intervals and not not_measured:
        return None
    sources = sorted({i["source"] for i in intervals})
    return {
        "version": IO_VERSION,
        "intervals": intervals,
        "not_measured": not_measured,
        "totals": {
            src: {
                "read_bytes": _sum(intervals, src, "read_bytes"),
                "write_bytes": _sum(intervals, src, "write_bytes"),
            }
            for src in sources
        },
        "problems": problems,
    }


def bucket_series(
    intervals: list[Mapping[str, Any]],
    *,
    step_seconds: float,
    start: str,
    end: str,
) -> dict[str, Any]:
    """Bytes read and written per ``step_seconds`` bucket from *start* to *end*.

    Each interval's bytes are spread evenly over its own window, so the
    resolution of a bucket is no finer than the intervals that feed it. An
    interval with no window, or entirely outside ``[start, end]``, adds to
    ``unplaced`` instead of a bucket; one that overlaps the edge keeps only
    the overlapping share in the buckets and the rest in ``unplaced``. An
    interval of zero length lands in the bucket holding its start.
    """
    t0, t1 = _parse(start).timestamp(), _parse(end).timestamp()
    n = max(1, int((t1 - t0) // step_seconds) + (1 if (t1 - t0) % step_seconds else 0))
    series = {"read": [0.0] * n, "write": [0.0] * n}
    unplaced = {"read": 0.0, "write": 0.0}
    for iv in intervals:
        s, e = iv.get("start"), iv.get("end")
        for direction in ("read", "write"):
            nbytes = iv.get(f"{direction}_bytes")
            if nbytes is None:
                continue
            if not s or not e:
                unplaced[direction] += nbytes
                continue
            a, b = _parse(s).timestamp(), _parse(e).timestamp()
            b = max(a, b)
            if b < t0 or a > t1:
                unplaced[direction] += nbytes
                continue
            if b == a:
                series[direction][min(n - 1, int((a - t0) // step_seconds))] += nbytes
                continue
            span = b - a
            inside = 0.0
            for k in range(n):
                lo, hi = t0 + k * step_seconds, t0 + (k + 1) * step_seconds
                overlap = min(b, hi) - max(a, lo)
                if overlap > 0:
                    share = nbytes * overlap / span
                    series[direction][k] += share
                    inside += share
            unplaced[direction] += nbytes - inside
    return {
        "start": start,
        "step_seconds": step_seconds,
        "read": series["read"],
        "write": series["write"],
        "unplaced": {k: round(v) for k, v in unplaced.items()},
    }
