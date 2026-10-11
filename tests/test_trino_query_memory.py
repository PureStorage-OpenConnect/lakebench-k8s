"""Trino query memory is sized from the deployed workers."""

from __future__ import annotations

import pytest

from lakebench.config.scale import full_compute_guidance
from lakebench.deploy.engine import trino_memory_properties
from tests.fixtures.trino_configmap import configmap, engine, size_bytes, xmx_bytes
from tests.fixtures.trino_configmap import props as parse_props

_TRINO_DEFAULT_MAX_MEMORY = 20 * 2**30


def _scale_overrides(scale: int, engine_type: str = "trino") -> dict:
    return {
        "architecture": {
            "workload": {"datagen": {"scale": scale}},
            "query_engine": {"type": engine_type},
        }
    }


@pytest.mark.parametrize("scale", [1, 10, 100])
def test_query_memory_sized_from_workers(scale):
    guidance = full_compute_guidance(scale).trino
    ctx = engine(**_scale_overrides(scale)).context
    # The autosizer's worker shape is what the configmap is sized from.
    assert ctx["trino_worker_replicas"] == guidance.worker_replicas
    assert ctx["trino_worker_memory"] == guidance.worker_memory
    workers = guidance.worker_replicas

    cm = configmap(ctx)
    per_role = {}
    for role in ("coordinator", "worker"):
        props = parse_props(cm[f"config.properties.{role}"])
        heap = xmx_bytes(cm[f"jvm.config.{role}"])
        for key in (
            "query.max-memory",
            "query.max-memory-per-node",
            "memory.heap-headroom-per-node",
        ):
            assert key in props, (role, key)
        per_node = size_bytes(props["query.max-memory-per-node"])
        headroom = size_bytes(props["memory.heap-headroom-per-node"])
        # Trino refuses to start if per-node + headroom exceeds the heap.
        assert per_node + headroom <= heap, role
        # Two cap-sized queries fit the node's pool (heap - headroom) at once,
        # so concurrent streams do not block on the low-memory killer.
        assert 2 * per_node <= heap - headroom, role
        # Headroom no smaller than Trino's own default, and the per-node cap
        # no smaller than Trino's default either (never a regression).
        assert headroom >= 0.3 * heap - 2**20, role
        assert per_node >= 0.3 * heap, role
        per_role[role] = (props, heap, per_node, headroom)

    w_props, w_heap, w_per_node, w_headroom = per_role["worker"]
    c_props = per_role["coordinator"][0]
    max_memory = size_bytes(c_props["query.max-memory"])
    # Coordinator enforces the cluster caps; workers carry the same values.
    assert w_props["query.max-memory"] == c_props["query.max-memory"]
    # query.max-total-memory stays at Trino's default (2 x max-memory).
    assert "query.max-total-memory" not in c_props
    assert "query.max-total-memory" not in w_props
    # Cluster user cap = workers x worker per-node and never above the pool.
    # Trino's effective query.max-total-memory is then its default,
    # 2 x max-memory (the property is unset, asserted above).
    assert max_memory == workers * w_per_node
    assert max_memory <= workers * (w_heap - w_headroom)
    # Never below the cap Trino's defaults gave: min(20GB, workers x 30% heap).
    assert max_memory > min(_TRINO_DEFAULT_MAX_MEMORY, workers * 0.3 * w_heap)


def test_scale_100_lifts_the_20gb_default():
    """The shape that failed live: 4 x 48Gi workers."""
    cm = configmap(engine(**_scale_overrides(100)).context)
    props = parse_props(cm["config.properties.coordinator"])
    assert size_bytes(props["query.max-memory"]) > 2.5 * _TRINO_DEFAULT_MAX_MEMORY


def test_explicit_worker_count_drives_cluster_cap():
    ctx = engine(
        architecture={
            "query_engine": {
                "type": "trino",
                "trino": {"worker": {"replicas": 7, "memory": "32Gi"}},
            }
        }
    ).context
    props = parse_props(configmap(ctx)["config.properties.coordinator"])
    per_node = size_bytes(
        parse_props(configmap(ctx)["config.properties.worker"])["query.max-memory-per-node"]
    )
    assert size_bytes(props["query.max-memory"]) == 7 * per_node


def test_zero_workers_still_renders_a_valid_cap():
    props = trino_memory_properties("3276m", "6553m", 0)
    assert props["max_memory"] == "2293MB"  # one worker's worth, not 0MB
