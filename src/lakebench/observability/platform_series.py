"""Platform series for the report's replay: CPU, memory, network and pod count
over the run, per pod group.

The deployment's Prometheus already scrapes kubelet cAdvisor every 15 s; the
collector reads each pod's series after the run. ``build_series`` sums them
per group of pods (a Spark stage, datagen, Trino, the catalog, other) at each
step and keeps the sums, so the report can draw a lane over time. Pod network
includes executor-to-executor shuffle traffic, so it is its own lane and is
never read as storage traffic.

A sum is over the pods that had a sample at a step: a pod that has ended adds
nothing afterward. The pod count at a step is the number of pods with a
memory sample then.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

SERIES_VERSION = 1
STEP_SECONDS = 15

NETWORK_RX_QUERY = (
    "sum by (pod) (max by (pod, interface) "
    '(rate(container_network_receive_bytes_total{{namespace="{ns}", container="POD", '
    'interface!~"lo|veth.*"}}[1m])))'
)
NETWORK_TX_QUERY = (
    "sum by (pod) (max by (pod, interface) "
    '(rate(container_network_transmit_bytes_total{{namespace="{ns}", container="POD", '
    'interface!~"lo|veth.*"}}[1m])))'
)

#: Pods that are the deployment's tooling, not the workload under test.
_TOOLING = ("prometheus", "grafana", "pushgateway", "alertmanager", "kube-state-metrics", "lb-deps")


def group_of(pod: str) -> str | None:
    """The pod's group, or None for the deployment's own tooling."""
    lower = pod.lower()
    if any(t in lower for t in _TOOLING):
        return None
    if "datagen" in lower:
        return "datagen"
    if "trino" in lower or "duckdb" in lower:
        return "engine"
    if any(c in lower for c in ("hive", "polaris", "postgres")):
        return "catalog"
    for stage in ("bronze", "silver", "gold"):
        if stage in lower:
            return stage
    return "other"


def _points(result: Mapping[str, Any]) -> dict[float, float]:
    out: dict[float, float] = {}
    for ts, val in result.get("values") or []:
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        if v == v:  # not NaN
            out[float(ts)] = v
    return out


def _by_group(
    results: Sequence[Mapping[str, Any]],
    t0: float,
    t1: float,
    step: float,
    *,
    count: bool,
) -> dict[str, list[list[float]]]:
    """Per group, at each step from *t0*: the sum of the pods' values, or the
    number of pods with a sample when *count*. A sample is placed on the grid
    step nearest its timestamp (Prometheus rounds timestamps); a step a pod
    has no sample for adds nothing."""
    n = int((t1 - t0) // step) + 1
    rows: dict[str, list[float]] = {}
    for r in results:
        pod = (r.get("metric") or {}).get("pod")
        group = group_of(pod) if pod else None
        pts = _points(r)
        if group is None or not pts:
            continue
        row = rows.setdefault(group, [0.0] * n)
        for ts, v in pts.items():
            k = round((ts - t0) / step)
            if 0 <= k < n:
                row[k] += 1.0 if count else v
    return {g: [[round(k * step, 3), v] for k, v in enumerate(row)] for g, row in rows.items()}


def build_series(
    cpu: Sequence[Mapping[str, Any]],
    memory: Sequence[Mapping[str, Any]],
    rx: Sequence[Mapping[str, Any]],
    tx: Sequence[Mapping[str, Any]],
    start: datetime,
    end: datetime,
    *,
    step: float = STEP_SECONDS,
) -> dict[str, Any] | None:
    """The ``series`` block, or None when no pod reported anything."""
    t0, t1 = start.timestamp(), end.timestamp()
    if t1 <= t0:
        return None
    families = {
        "cpu_cores": _by_group(cpu, t0, t1, step, count=False),
        "memory_bytes": _by_group(memory, t0, t1, step, count=False),
        "network_rx_bytes_per_s": _by_group(rx, t0, t1, step, count=False),
        "network_tx_bytes_per_s": _by_group(tx, t0, t1, step, count=False),
        "pods": _by_group(memory, t0, t1, step, count=True),
    }
    if not any(families.values()):
        return None
    return {
        "version": SERIES_VERSION,
        "step_seconds": step,
        "start": start.isoformat(),
        "end": end.isoformat(),
        **families,
    }
