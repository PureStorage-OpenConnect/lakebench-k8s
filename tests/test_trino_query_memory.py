"""Trino query memory is sized from the deployed workers.

Live: AML batch scale 100, polaris-iceberg-spark-trino, 4 x 48Gi workers. FQ3
failed every sample with "Query exceeded distributed user memory limit of
20GB": lakebench set no query.max-memory, so Trino's flat 20GB default capped
the query on a cluster with about 107 GB of memory pool. These tests render
the real configmap from the engine context at the scale 1, 10 and 100 sizes
the autosizer picks.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.config.scale import full_compute_guidance
from lakebench.deploy.engine import (
    TRINO_HEAP_HEADROOM_FRACTION,
    TRINO_QUERY_MEMORY_PER_NODE_FRACTION,
    DeploymentEngine,
    TemplateRenderer,
    trino_memory_properties,
)
from tests.conftest import make_config

_TRINO_DEFAULT_MAX_MEMORY = 20 * 2**30


def _engine(**overrides) -> DeploymentEngine:
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    with patch.object(DeploymentEngine, "_detect_openshift", return_value=False):
        return DeploymentEngine(make_config(**overrides), k8s_client=k8s)


def _configmap(ctx: dict) -> dict[str, str]:
    docs = [
        d
        for d in yaml.safe_load_all(TemplateRenderer().render("trino/configmap.yaml.j2", ctx))
        if d
    ]
    return docs[0]["data"]


def _props(text: str) -> dict[str, str]:
    return dict(
        line.strip().split("=", 1)
        for line in text.splitlines()
        if "=" in line and not line.strip().startswith("#")
    )


def _size_bytes(value: str) -> int:
    """Parse a Trino DataSize (airlift: MB and GB are binary)."""
    m = re.fullmatch(r"(\d+)(B|kB|MB|GB|TB)", value.strip())
    assert m, value
    return (
        int(m.group(1)) * {"B": 1, "kB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40}[m.group(2)]
    )


def _xmx_bytes(jvm_config: str) -> int:
    m = re.search(r"^\s*-Xmx(\d+)m\s*$", jvm_config, re.MULTILINE)
    assert m, jvm_config
    return int(m.group(1)) * 2**20


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
    ctx = _engine(**_scale_overrides(scale)).context
    # The autosizer's worker shape is what the configmap is sized from.
    assert ctx["trino_worker_replicas"] == guidance.worker_replicas
    assert ctx["trino_worker_memory"] == guidance.worker_memory
    workers = guidance.worker_replicas

    cm = _configmap(ctx)
    per_role = {}
    for role in ("coordinator", "worker"):
        props = _props(cm[f"config.properties.{role}"])
        heap = _xmx_bytes(cm[f"jvm.config.{role}"])
        for key in (
            "query.max-memory",
            "query.max-memory-per-node",
            "memory.heap-headroom-per-node",
        ):
            assert key in props, (role, key)
        per_node = _size_bytes(props["query.max-memory-per-node"])
        headroom = _size_bytes(props["memory.heap-headroom-per-node"])
        # Trino refuses to start if per-node + headroom exceeds the heap.
        assert per_node + headroom <= heap, role
        assert per_node == pytest.approx(TRINO_QUERY_MEMORY_PER_NODE_FRACTION * heap, abs=2**20)
        assert headroom == pytest.approx(TRINO_HEAP_HEADROOM_FRACTION * heap, abs=2**20)
        per_role[role] = (props, heap, per_node, headroom)

    w_props, w_heap, w_per_node, w_headroom = per_role["worker"]
    c_props = per_role["coordinator"][0]
    max_memory = _size_bytes(c_props["query.max-memory"])
    # Coordinator enforces the cluster caps; workers carry the same values.
    assert w_props["query.max-memory"] == c_props["query.max-memory"]
    # query.max-total-memory stays at Trino's default (2 x max-memory).
    assert "query.max-total-memory" not in c_props
    assert "query.max-total-memory" not in w_props
    # Cluster user cap = workers x worker per-node, never above the pool.
    assert max_memory == workers * w_per_node
    assert max_memory <= workers * (w_heap - w_headroom)
    # Never below the cap Trino's defaults gave: min(20GB, workers x 30% heap).
    assert max_memory > min(_TRINO_DEFAULT_MAX_MEMORY, workers * 0.3 * w_heap)


def test_scale_100_lifts_the_20gb_default():
    """The shape that failed live: 4 x 48Gi workers."""
    cm = _configmap(_engine(**_scale_overrides(100)).context)
    props = _props(cm["config.properties.coordinator"])
    assert _size_bytes(props["query.max-memory"]) > 3 * _TRINO_DEFAULT_MAX_MEMORY
    assert props["query.max-memory"] == "62912MB"


def test_explicit_worker_count_drives_cluster_cap():
    ctx = _engine(
        architecture={
            "query_engine": {
                "type": "trino",
                "trino": {"worker": {"replicas": 7, "memory": "32Gi"}},
            }
        }
    ).context
    props = _props(_configmap(ctx)["config.properties.coordinator"])
    per_node = _size_bytes(
        _props(_configmap(ctx)["config.properties.worker"])["query.max-memory-per-node"]
    )
    assert _size_bytes(props["query.max-memory"]) == 7 * per_node


def test_zero_workers_still_renders_a_valid_cap():
    props = trino_memory_properties("3276m", "6553m", 0)
    assert props["max_memory"] == "2621MB"  # one worker's worth, not 0MB


@pytest.mark.parametrize("bad", ["", "8g", "0m", "12Gi"])
def test_bad_heap_raises(bad):
    with pytest.raises(ValueError):
        trino_memory_properties(bad, "6553m", 2)


@pytest.mark.parametrize("engine_type", ["spark-thrift", "duckdb", "none"])
def test_unparseable_trino_memory_leaves_defaults_for_other_engines(engine_type):
    ctx = _engine(
        architecture={
            "query_engine": {
                "type": engine_type,
                "trino": {"coordinator": {"memory": "8g"}, "worker": {"memory": "lots"}},
            }
        }
    ).context
    assert ctx["trino_memory"] == {}
    props = _props(_configmap(ctx)["config.properties.worker"])
    assert "query.max-memory" not in props
