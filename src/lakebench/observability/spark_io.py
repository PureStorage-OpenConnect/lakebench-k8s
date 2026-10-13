"""Spark storage bytes over time, from the executors' exporter counters.

The executors export two cumulative counters per pod (see
``modules/pipeline_engines/spark/jmx_exporter.py``): the bytes their tasks
wrote (``executor.bytesWritten``) and the bytes the client read. Reads come
under two Hadoop filesystem schemes: ``s3a`` for the stages that read raw
files (bronze) and ``s3`` for the stages that read Iceberg tables through
S3FileIO (silver, gold) -- S3FileIO counts under scheme ``s3``, not ``s3a``,
and its counter is only correct from Iceberg 1.12.0 (PR #17236). A pod reads
through one scheme, so summing both per pod does not double count. A pod's
counter starts at zero with the pod, so the run's total is the sum over pods
of each pod's last value, and the series is the same sum at each step with a
finished pod keeping its last value.

``build_spark_io`` turns the per-pod Prometheus results into the record's
``platform_metrics.spark_io`` block. It is a pure function of those results.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

SPARK_IO_VERSION = 1
STEP_SECONDS = 15

WRITE_QUERY = (
    'sum by (pod) ({{__name__=~"metrics_lakebench_.+_executor_byteswritten_count",'
    'namespace="{ns}"}})'
)
READ_QUERY = (
    # s3a (raw files, bronze) and s3 (Iceberg tables via S3FileIO, silver/gold).
    # A pod reads through one scheme, so the per-pod sum does not double count.
    'sum by (pod) ({{__name__=~"metrics_lakebench_.+_executor_filesystem_s3a?_read_bytes_value",'
    'namespace="{ns}"}})'
)


def stage_of(pod: str) -> str:
    """The pipeline stage a Spark pod belongs to, from its name."""
    lower = pod.lower()
    for stage in ("bronze", "silver", "gold"):
        if stage in lower:
            return stage
    return "other"


def _points(result: Mapping[str, Any]) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for ts, val in result.get("values") or []:
        try:
            out.append((float(ts), float(val)))
        except (TypeError, ValueError):
            continue
    return sorted(out)


def _cumulative(
    pods: Sequence[list[tuple[float, float]]], t0: float, t1: float, step: float
) -> list[list[float]]:
    """``[[offset_s, bytes], ...]``: the sum over *pods* of each pod's last
    value at or before each grid time; 0 for a pod not yet seen."""
    n = int((t1 - t0) // step) + 1
    out: list[list[float]] = []
    for k in range(n):
        t = t0 + k * step
        total = 0.0
        for pts in pods:
            last = 0.0
            for ts, val in pts:
                if ts > t:
                    break
                last = val
            total += last
        out.append([round(k * step, 3), total])
    return out


def build_spark_io(
    write: Sequence[Mapping[str, Any]],
    read: Sequence[Mapping[str, Any]],
    start: datetime,
    end: datetime,
    *,
    step: float = STEP_SECONDS,
) -> dict[str, Any] | None:
    """The ``spark_io`` block, or None when no executor reported either counter
    (the exporter was not running or nothing was scraped)."""
    t0, t1 = start.timestamp(), end.timestamp()
    if t1 <= t0:
        return None
    by_stage: dict[str, dict[str, dict[str, list[tuple[float, float]]]]] = {}
    for direction, results in (("write", write), ("read", read)):
        for r in results:
            pod = (r.get("metric") or {}).get("pod")
            pts = _points(r)
            if not pod or not pts:
                continue
            by_stage.setdefault(stage_of(pod), {"write": {}, "read": {}})[direction][pod] = pts
    if not by_stage:
        return None
    stages: dict[str, Any] = {}
    for stage, dirs in sorted(by_stage.items()):
        pods = set(dirs["write"]) | set(dirs["read"])
        stages[stage] = {
            "pods": len(pods),
            "write": _cumulative(list(dirs["write"].values()), t0, t1, step),
            "read": _cumulative(list(dirs["read"].values()), t0, t1, step),
        }
    return {
        "version": SPARK_IO_VERSION,
        "step_seconds": step,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "stages": stages,
    }
