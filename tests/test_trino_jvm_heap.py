"""Trino JVM heap sits below the container memory limit and fits the memory properties."""

from __future__ import annotations

import pytest

from lakebench.deploy.engine import JVM_HEAP_FRACTION, k8s_memory_bytes
from tests.fixtures.trino_configmap import (
    configmap,
    engine,
    render,
    size_bytes,
    xmx_bytes,
)
from tests.fixtures.trino_configmap import props as parse_props

# Trino defaults when the properties are not set: each is 30% of the heap.
_TRINO_DEFAULT_FRACTION = {
    "query.max-memory-per-node": 0.3,
    "memory.heap-headroom-per-node": 0.3,
}


def _container_limit(doc: dict) -> int:
    (container,) = [c for c in doc["spec"]["template"]["spec"]["containers"] if "resources" in c]
    res = container["resources"]
    assert res["limits"]["memory"] == res["requests"]["memory"]
    return k8s_memory_bytes(res["limits"]["memory"])


def _trino_overrides(engine_type: str, coord_mem: str, worker_mem: str) -> dict:
    return {
        "architecture": {
            "query_engine": {
                "type": engine_type,
                "trino": {
                    "coordinator": {"memory": coord_mem},
                    "worker": {"memory": worker_mem},
                },
            }
        }
    }


@pytest.mark.parametrize(
    ("coord_mem", "worker_mem"),
    [("8Gi", "16Gi"), ("4Gi", "8Gi"), ("16Gi", "64Gi"), ("4096Mi", "12G")],
)
def test_trino_xmx_below_pod_limit_and_memory_props_fit(coord_mem, worker_mem):
    ctx = engine(**_trino_overrides("trino", coord_mem, worker_mem)).context

    cm = configmap(ctx)
    (coord,) = [d for d in render("trino/coordinator.yaml.j2", ctx) if d["kind"] == "Deployment"]
    workers = [d for d in render("trino/worker.yaml.j2", ctx) if d["kind"] == "StatefulSet"]
    assert len(workers) == 1

    for role, pod, configured in (
        ("coordinator", coord, coord_mem),
        ("worker", workers[0], worker_mem),
    ):
        heap = xmx_bytes(cm[f"jvm.config.{role}"])
        limit = _container_limit(pod)
        assert limit == k8s_memory_bytes(configured)  # pod limit unchanged
        assert heap <= JVM_HEAP_FRACTION * limit
        assert heap >= 0.75 * limit  # not needlessly small either

        # Trino refuses to start if max-memory-per-node + headroom > heap.
        props = parse_props(cm[f"config.properties.{role}"])
        used = sum(
            size_bytes(props[k]) if k in props else frac * heap
            for k, frac in _TRINO_DEFAULT_FRACTION.items()
        )
        assert used <= heap, (role, props)


@pytest.mark.parametrize("engine_type", ["spark-thrift", "duckdb", "none"])
def test_bad_trino_memory_ignored_for_other_engines(engine_type):
    """An unused Trino field must not block that deployment's destroy, which
    builds the same context."""
    ctx = engine(**_trino_overrides(engine_type, "8g", "lots")).context
    assert ctx["trino_coordinator_heap"] == "" and ctx["trino_worker_heap"] == ""
    assert ctx["trino_memory"] == {}
    assert "query.max-memory" not in parse_props(configmap(ctx)["config.properties.worker"])
