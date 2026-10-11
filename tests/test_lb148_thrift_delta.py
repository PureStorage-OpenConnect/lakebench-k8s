"""Spark Thrift on Delta.

(a) Delta's OptimizeMetadataOnlyDeltaQuery rewrite crashes MIN/MAX on date
    partition columns (ClassCastException LocalDate -> java.sql.Date), so the
    rewrite is disabled for Delta + Hive in the Thrift server and Spark jobs.
(b) The Thrift pod limit is heap + overhead, never heap == limit.
(c) Delta + Hive + Thrift gets a larger auto-sized default.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.config.autosizer import (
    resolve_auto_sizing,
)
from lakebench.deploy.engine import (
    DeploymentEngine,
    TemplateRenderer,
    k8s_memory_bytes,
    spark_memory_bytes,
    thrift_pod_memory_limit,
)
from tests.conftest import make_config

FLAG = "spark.databricks.delta.optimizeMetadataQuery.enabled"
GIB = 2**30
MIB = 2**20


def _engine(cfg) -> DeploymentEngine:
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    with patch(
        "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
        return_value=False,
    ):
        return DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)


def _render_thrift(cfg) -> dict:
    engine = _engine(cfg)
    from lakebench.deps.manifest import consumer_context, placeholder_handle

    ctx = {**engine.context, **consumer_context(placeholder_handle(engine.config))}
    rendered = TemplateRenderer().render("spark-thrift/sparkapplication.yaml.j2", ctx)
    return yaml.safe_load(rendered)


def _container(doc: dict) -> dict:
    (container,) = doc["spec"]["template"]["spec"]["containers"]
    return container


def _conf(container: dict) -> dict[str, str]:
    script = container["command"][-1]
    out = {}
    for line in script.splitlines():
        line = line.strip().rstrip("\\").strip()
        if line.startswith("--conf "):
            key, _, value = line[len("--conf ") :].strip('"').partition("=")
            out[key] = value
    return out


def _thrift_cfg(recipe: str, **thrift):
    arch = {"query_engine": {"type": "spark-thrift"}}
    if thrift:
        arch["query_engine"]["spark_thrift"] = thrift
    return make_config(recipe=recipe, architecture=arch)


# -- (a) metadata-only query rewrite ----------------------------------------


def test_thrift_delta_hive_disables_metadata_query_rewrite():
    conf = _conf(_container(_render_thrift(_thrift_cfg("hive-delta-spark-thrift"))))
    assert conf[FLAG] == "false"
    assert conf["spark.sql.catalog.spark_catalog"].endswith("DeltaCatalog")


# -- (b) pod limit = heap + overhead -----------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("4G", 4 * GIB),
        ("4g", 4 * GIB),
        ("4gb", 4 * GIB),
        ("4096m", 4 * GIB),
        ("4096", 4 * GIB),
        ("1536M", 1536 * MIB),
        ("4194304k", 4 * GIB),
        ("4294967296b", 4 * GIB),
        ("1t", 1024 * GIB),
        ("", None),
        ("abc", None),
        ("4Gi", None),
        ("0g", None),
        ("-1g", None),
        ("1.5g", None),
        ("1e1g", None),
        ("4 g", None),
        ("infg", None),
        ("4x", None),
    ],
)
def test_spark_memory_bytes(value, expected):
    if expected is None:
        with pytest.raises(ValueError):
            spark_memory_bytes(value)
    else:
        assert spark_memory_bytes(value) == expected


@pytest.mark.parametrize(
    ("heap", "limit"),
    [("512m", "1536Mi"), ("4g", "5120Mi"), ("16g", "18023Mi")],
)
def test_thrift_pod_limit_exceeds_heap(heap, limit):
    assert thrift_pod_memory_limit(heap) == limit
    assert k8s_memory_bytes(limit) > spark_memory_bytes(heap)


@pytest.mark.parametrize(
    "recipe,heap",
    [("hive-iceberg-spark-thrift", "4g"), ("hive-delta-spark-thrift", "4g")],
)
def test_rendered_thrift_pod_limit_above_heap(recipe, heap):
    container = _container(_render_thrift(_thrift_cfg(recipe, cores=2, memory=heap)))
    conf = _conf(container)
    assert conf["spark.driver.memory"] == heap
    res = container["resources"]
    assert res["requests"]["memory"] == res["limits"]["memory"] == "5120Mi"
    assert k8s_memory_bytes(res["limits"]["memory"]) > spark_memory_bytes(heap)


def test_non_thrift_context_tolerates_bad_thrift_memory():
    cfg = make_config(architecture={"query_engine": {"spark_thrift": {"memory": "lots"}}})
    assert _engine(cfg).context["spark_thrift_memory_k8s"] == ""


# -- (c) Delta + Thrift default size ------------------------------------------


def test_delta_thrift_explicit_values_win():
    cfg = _thrift_cfg("hive-delta-spark-thrift", cores=2, memory="4g")
    resolve_auto_sizing(cfg, None)
    thrift = cfg.architecture.query_engine.spark_thrift
    assert (thrift.cores, thrift.memory) == (2, "4g")


def test_delta_thrift_fitted_to_small_node():
    cap = SimpleNamespace(
        total_cpu_millicores=64000,
        total_memory_bytes=256 * GIB,
        node_count=4,
        largest_node_cpu_millicores=6000,
        largest_node_memory_bytes=16 * GIB,
    )
    cfg = _thrift_cfg("hive-delta-spark-thrift")
    resolve_auto_sizing(cfg, cap)
    thrift = cfg.architecture.query_engine.spark_thrift
    assert thrift.cores * 1000 <= cap.largest_node_cpu_millicores
    pod_limit = k8s_memory_bytes(thrift_pod_memory_limit(thrift.memory))
    assert pod_limit < cap.largest_node_memory_bytes
