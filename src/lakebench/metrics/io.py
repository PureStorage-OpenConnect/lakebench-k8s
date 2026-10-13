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
- Spark writes are the executors' task output bytes; Spark reads are the S3A
  client's read bytes, which cover raw Parquet and Delta and not Iceberg table
  reads. Streaming checkpoint and state bytes are in neither. Both come from
  the executors' exporter counters (``observability/spark_io.py``) and exist
  only when observability was on.
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
BASIS_SPARK_WRITE = "executor_task_output"
BASIS_SPARK_READ = "executor_s3a_read"
#: The driver's total of the stage's task output bytes: every task, so the
#: whole stage. Placed over the stage window when the executors' counters
#: are absent; the part those counters did not see is placed at the stage end.
BASIS_SPARK_DRIVER = "driver_task_output"
BASIS_SPARK_UNSEEN = "driver_task_output_unseen_by_executors"
BASIS_NOT_MEASURED = "not_measured"

#: Why a stage has no figure, as recorded in ``io.not_measured``.
REASON_EXECUTOR_IO = "executor_io_not_collected"
REASON_ENGINE = "engine_not_instrumented"
REASON_NOT_RECORDED = "io_not_recorded"
#: Iceberg table reads go through S3FileIO, which has no client-side byte counter.
REASON_ICEBERG_READS = "iceberg_reads_have_no_client_counter"

_SPARK_STAGES = ("bronze", "silver", "gold")
#: How far back from a stage's end the part of its writes the executors did not
#: see is placed (the scrape step).
_UNSEEN_SECONDS = 15.0
_JOB_STAGE = {"bronze-verify": "bronze", "silver-build": "silver", "gold-finalize": "gold"}
_GIB = 1024**3
#: A Spark stage's write counters under this share of the bytes that landed
#: are flagged: the executors exit before their last increments are scraped.
COVERAGE_FLOOR = 0.95


def _reads_a_table(stage: str, mode: str) -> bool:
    """Whether *stage* reads a lakehouse table (not raw files). Bronze reads
    raw files; batch silver reads bronze's raw Parquet and batch gold reads
    the silver table; continuous silver and gold read tables."""
    return stage == "gold" or (stage == "silver" and mode != "batch")


def _iso(epoch: Any) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat(timespec="milliseconds")


def _parse(stamp: str) -> datetime:
    """An ISO timestamp as written by this module or by the record (Z or offset)."""
    return datetime.fromisoformat(stamp.replace("Z", "+00:00"))


def _benchmarks(record: Mapping[str, Any]) -> list[tuple[str, str, Mapping[str, Any]]]:
    """The benchmark rounds of the record: a label, the record path of the
    round, and the round itself.

    ``pipeline_benchmark.query_benchmark`` mirrors ``benchmark`` and is not
    read, so a round is never counted twice.
    """
    out: list[tuple[str, str, Mapping[str, Any]]] = []
    pre = (record.get("pipeline_benchmark") or {}).get("pre_compaction_benchmark")
    if isinstance(pre, Mapping) and pre.get("queries"):
        out.append(("pre_compaction", "pipeline_benchmark.pre_compaction_benchmark", pre))
    bench = record.get("benchmark")
    if isinstance(bench, Mapping) and bench.get("queries"):
        out.append(("benchmark", "benchmark", bench))
    for i, rnd in enumerate(record.get("benchmark_rounds") or []):
        if not isinstance(rnd, Mapping) or not rnd.get("queries"):
            continue
        meta = rnd.get("round_meta") or {}
        index = rnd.get("index", meta.get("round_index", i + 1))
        out.append((f"round_{index}", f"benchmark_rounds[{i}]", rnd))
    return out


def _sum(intervals: list[dict[str, Any]], source: str, key: str) -> int | None:
    vals = [i[key] for i in intervals if i["source"] == source and i.get(key) is not None]
    return int(sum(vals)) if vals else None


def _spark_intervals(
    stage: str, series: Mapping[str, Any], block: Mapping[str, Any], reads: bool
) -> list[dict[str, Any]]:
    """One interval per scrape step of a Spark stage, with the bytes the
    executors' counters gained over that step. A step with no gain adds none."""
    start = _parse(str(block["start"])).timestamp()
    write, read = series.get("write") or [], series.get("read") or []
    out: list[dict[str, Any]] = []
    for k in range(1, len(write)):
        w = max(0.0, write[k][1] - write[k - 1][1])
        r = max(0.0, read[k][1] - read[k - 1][1]) if reads and k < len(read) else None
        if not w and not r:
            continue
        out.append(
            {
                "source": "spark",
                "stage": stage,
                "label": stage,
                "query": None,
                "start": _iso(start + write[k - 1][0]),
                "end": _iso(start + write[k][0]),
                "clock": "host",
                "read_bytes": int(r) if r is not None else None,
                "write_bytes": int(w),
                "read_basis": BASIS_SPARK_READ if reads else None,
                "write_basis": BASIS_SPARK_WRITE,
                "complete": True,
                "paths": {
                    "read": f"platform_metrics.spark_io.stages.{stage}.read" if reads else None,
                    "write": f"platform_metrics.spark_io.stages.{stage}.write",
                },
            }
        )
    return out


def _driver_writes(record: Mapping[str, Any]) -> dict[str, int]:
    """Stage -> the driver's complete total of task output bytes, from the
    job's ``extra_metrics`` (``output_bytes_written`` with
    ``output_bytes_complete: true``). A total the driver marked possibly low
    is not used."""
    out: dict[str, int] = {}
    for j in record.get("jobs") or []:
        if not isinstance(j, Mapping):
            continue
        stage = _JOB_STAGE.get(str(j.get("job_type")))
        extra = j.get("extra_metrics") or {}
        if not stage or str(extra.get("output_bytes_complete")) != "true":
            continue
        try:
            out[stage] = out.get(stage, 0) + int(extra["output_bytes_written"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _interval(
    stage: str,
    start: str | None,
    end: str | None,
    *,
    read: int | None,
    write: int,
    write_basis: str,
    read_basis: str | None,
    paths: Mapping[str, str | None],
) -> dict[str, Any]:
    return {
        "source": "spark",
        "stage": stage,
        "label": stage,
        "query": None,
        "start": start,
        "end": end,
        "clock": "host",
        "read_bytes": read,
        "write_bytes": write,
        "read_basis": read_basis,
        "write_basis": write_basis,
        "complete": True,
        "paths": dict(paths),
    }


def build_io(record: Mapping[str, Any]) -> dict[str, Any] | None:
    """The ``io`` block for *record* (a serialized run record), or None when the
    record has nothing to say about storage I/O."""
    intervals: list[dict[str, Any]] = []
    not_measured: list[dict[str, str]] = []
    problems: list[str] = []

    short = 0
    # Engines that report their own per-query read bytes (``queries[*].io``):
    # Trino via system.runtime.tasks, DuckDB via the httpfs filesystem log,
    # Spark Thrift via the Spark REST SQL metrics. An engine with no io block
    # on any query is marked not measured.
    read_basis_for = {
        "trino": BASIS_TRINO_READ,
        "duckdb": "duckdb_filesystem_read",
        "spark-thrift": "spark_thrift_input",
    }
    for label, group, bench in _benchmarks(record):
        engine = bench.get("engine") or "trino"
        queries = bench.get("queries") or []
        read_basis = read_basis_for.get(engine)
        if read_basis is None:
            not_measured.append(
                {
                    "source": engine,
                    "stage": "query",
                    "label": label,
                    "direction": "both",
                    "reason": REASON_ENGINE,
                }
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
                    "source": engine,
                    "stage": "query",
                    "label": label,
                    "query": q.get("name") or q.get("query_name"),
                    "start": _iso(io.get("start_epoch")),
                    "end": _iso(io.get("end_epoch")),
                    "clock": f"{engine}_client",
                    "read_bytes": io.get("read_bytes"),
                    "write_bytes": io.get("write_bytes"),
                    "read_basis": read_basis,
                    "write_basis": BASIS_TRINO_WRITE if engine == "trino" else None,
                    "complete": captured == samples and samples > 0,
                    # Where the figures sit in the record, as a path covering
                    # every query of the round: a rendered total can name it.
                    "paths": {
                        "read": f"{group}.queries[*].io.read_bytes",
                        "write": f"{group}.queries[*].io.write_bytes",
                    },
                }
            )
        if not any(isinstance(q, Mapping) and isinstance(q.get("io"), Mapping) for q in queries):
            not_measured.append(
                {
                    "source": engine,
                    "stage": "query",
                    "label": label,
                    "direction": "both",
                    "reason": REASON_NOT_RECORDED,
                }
            )
    if short:
        problems.append(f"{short} query sample(s) had no I/O report")
    # One entry per round would repeat itself.
    not_measured = list(
        {
            (n["source"], n["stage"], n.get("label"), n["direction"], n["reason"]): n
            for n in not_measured
        }.values()
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
                "paths": {"read": None, "write": "datagen_fleet.total_bytes_written"},
            }
        )

    stage_names = {
        s.get("stage_name") for s in (record.get("pipeline_benchmark") or {}).get("stages") or []
    }
    spark_io = (record.get("platform_metrics") or {}).get("spark_io")
    spark_stages = (spark_io or {}).get("stages") or {}
    iceberg = str((record.get("config_snapshot") or {}).get("table_format") or "") == "iceberg"
    mode = str((record.get("pipeline_benchmark") or {}).get("pipeline_mode") or "batch")
    stage_rows = {
        s.get("stage_name"): s
        for s in (record.get("pipeline_benchmark") or {}).get("stages") or []
        if isinstance(s, Mapping)
    }
    driver = _driver_writes(record)
    for name in _SPARK_STAGES:
        if name not in stage_names:
            continue
        measured = spark_stages.get(name)
        # A stage that reads an Iceberg table (silver/gold continuous, gold
        # batch) only has read bytes when the s3 FileSystem counter was
        # populated -- Iceberg exposes none before 1.12.0 (PR #17236), so the
        # series is all zero there. Trust the data: reads are measured when the
        # read series carries bytes, not from a version string.
        iceberg_read = iceberg and _reads_a_table(name, mode)
        has_read_bytes = bool(measured) and any(
            v > 0 for _, v in (measured or {}).get("read") or []
        )
        reads = bool(measured) and (not iceberg_read or has_read_bytes)
        row = stage_rows.get(name) or {}
        total = driver.get(name)
        counted: list[dict[str, Any]] = []
        if measured:
            counted = _spark_intervals(name, measured, spark_io or {}, reads)
        wrote = sum(i["write_bytes"] for i in counted)
        if total is not None:
            # The driver's total is the whole stage's. The executors' counters
            # time what they saw; the rest is placed at the stage end.
            if wrote > total * 1.01:
                problems.append(
                    f"spark {name}: the executors' counters ({wrote:,} B) exceed the "
                    f"driver's total ({total:,} B)"
                )
            unseen = max(0, total - wrote)
            if unseen or not counted:
                end, start = row.get("end_time"), row.get("start_time")
                if counted and end and start:
                    lo = max(_parse(start).timestamp(), _parse(end).timestamp() - _UNSEEN_SECONDS)
                    start = _iso(lo)
                counted.append(
                    _interval(
                        name,
                        start,
                        end,
                        read=None,
                        write=unseen if counted else total,
                        write_basis=BASIS_SPARK_UNSEEN if counted else BASIS_SPARK_DRIVER,
                        read_basis=None,
                        paths={
                            "read": None,
                            "write": "jobs[*].extra_metrics.output_bytes_written",
                        },
                    )
                )
        elif measured:
            landed = float(row.get("output_size_gb") or 0) * _GIB
            if landed > 0 and wrote < COVERAGE_FLOOR * landed:
                for i in counted:
                    i["complete"] = False
                problems.append(
                    f"spark {name}: write counters cover {wrote / landed:.0%} of the "
                    f"{landed / _GIB:.2f} GiB that landed"
                )
        intervals.extend(counted)
        for direction in ("read", "write"):
            if direction == "write" and (measured or total is not None):
                continue
            if direction == "read" and reads:
                continue
            not_measured.append(
                {
                    "source": "spark",
                    "stage": name,
                    "direction": direction,
                    "reason": REASON_ICEBERG_READS if measured else REASON_EXECUTOR_IO,
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
