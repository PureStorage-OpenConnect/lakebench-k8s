"""LB-146: JVM heap must sit below the container memory limit.

Live 2026-09-24: hive-delta-spark-trino c360 scale 1 under parallel load, Q2
got the coordinator OOM-killed (exit 137) with limit 4Gi and -Xmx4G. Heap plus
metaspace, thread stacks, direct buffers and code cache exceeded the cgroup
limit. These tests render the real templates from the engine context.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.deploy.engine import (
    JVM_HEAP_FRACTION,
    DeploymentEngine,
    TemplateRenderer,
    jvm_heap_for_limit,
    k8s_memory_bytes,
)
from tests.conftest import make_config

# Trino defaults when the properties are not set: each is 30% of the heap.
_TRINO_DEFAULT_FRACTION = {
    "query.max-memory-per-node": 0.3,
    "memory.heap-headroom-per-node": 0.3,
}


def _engine(**overrides) -> DeploymentEngine:
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    with patch.object(DeploymentEngine, "_detect_openshift", return_value=False):
        return DeploymentEngine(make_config(**overrides), k8s_client=k8s)


def _render(name: str, ctx: dict) -> list[dict]:
    return [d for d in yaml.safe_load_all(TemplateRenderer().render(name, ctx)) if d]


def _xmx_bytes(jvm_config: str) -> int:
    m = re.search(r"^\s*-Xmx(\d+)([mMgG])\s*$", jvm_config, re.MULTILINE)
    assert m, jvm_config
    return int(m.group(1)) * (2**20 if m.group(2) in "mM" else 2**30)


def _container_limit(doc: dict) -> int:
    (container,) = [c for c in doc["spec"]["template"]["spec"]["containers"] if "resources" in c]
    res = container["resources"]
    assert res["limits"]["memory"] == res["requests"]["memory"]
    return k8s_memory_bytes(res["limits"]["memory"])


def _props(text: str) -> dict[str, str]:
    return dict(
        line.strip().split("=", 1)
        for line in text.splitlines()
        if "=" in line and not line.strip().startswith("#")
    )


def _size_bytes(value: str) -> int:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(B|kB|MB|GB|TB)", value.strip())
    assert m, value
    mult = {"B": 1, "kB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40}[m.group(2)]
    return int(float(m.group(1)) * mult)


@pytest.mark.parametrize(
    ("limit", "expected"),
    [
        ("4Gi", "3276m"),
        ("8Gi", "6553m"),
        ("16Gi", "13107m"),
        ("4096Mi", "3276m"),
        ("8G", "6103m"),
        ("2000M", "1525m"),
    ],
)
def test_heap_is_80_percent_of_limit(limit, expected):
    assert jvm_heap_for_limit(limit) == expected


@pytest.mark.parametrize("bad", ["8g", "4096m", "lots", "", "0Gi", "-1Gi"])
def test_unparseable_limits_raise(bad):
    with pytest.raises(ValueError):
        jvm_heap_for_limit(bad)


@pytest.mark.parametrize(
    ("coord_mem", "worker_mem"),
    [("8Gi", "16Gi"), ("4Gi", "8Gi"), ("16Gi", "64Gi"), ("4096Mi", "12G")],
)
def test_trino_xmx_below_pod_limit_and_memory_props_fit(coord_mem, worker_mem):
    engine = _engine()
    trino = engine.config.architecture.query_engine.trino
    object.__setattr__(trino.coordinator, "memory", coord_mem)
    object.__setattr__(trino.worker, "memory", worker_mem)
    ctx = engine._build_context()

    cm = _render("trino/configmap.yaml.j2", ctx)[0]["data"]
    (coord,) = _render("trino/coordinator.yaml.j2", ctx)[-1:]
    workers = [d for d in _render("trino/worker.yaml.j2", ctx) if d["kind"] == "StatefulSet"]
    assert len(workers) == 1

    for role, pod in (("coordinator", coord), ("worker", workers[0])):
        heap = _xmx_bytes(cm[f"jvm.config.{role}"])
        limit = _container_limit(pod)
        assert heap <= JVM_HEAP_FRACTION * limit
        assert heap >= 0.75 * limit  # not needlessly small either

        # Trino refuses to start if max-memory-per-node + headroom > heap.
        props = _props(cm[f"config.properties.{role}"])
        used = sum(
            _size_bytes(props[k]) if k in props else frac * heap
            for k, frac in _TRINO_DEFAULT_FRACTION.items()
        )
        assert used <= heap, (role, props)


def test_spark_thrift_driver_heap_below_pod_limit():
    ctx = _engine()._build_context()
    docs = _render("spark-thrift/sparkapplication.yaml.j2", ctx)
    (dep,) = [d for d in docs if d["kind"] == "Deployment"]
    container = dep["spec"]["template"]["spec"]["containers"][0]
    script = container["command"][-1]
    heaps = re.findall(r"spark\.driver\.memory=(\d+)m", script)
    assert heaps, "spark.driver.memory must be rendered in MiB"
    limit = k8s_memory_bytes(container["resources"]["limits"]["memory"])
    for h in heaps:
        assert int(h) * 2**20 <= JVM_HEAP_FRACTION * limit
