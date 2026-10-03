"""Spark Thrift and the Spark jobs load one jar set, in one order (ch01 s2.6, UX D2).

Both come from the deployment's dependency set: the jobs name the set's jars
in ``spark.jars``, Thrift fetches the same files and puts them on its
classpath. For every supported catalog and format on Spark 4.0 and 4.1, the
rendered Thrift classpath names the jobs' jars in the jobs' order (the
manifest's jar_order), with the image's own jars first.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock
from urllib.parse import unquote

import pytest

from lakebench.deploy.engine import TemplateRenderer
from lakebench.deps import manifest as m
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.conftest import make_config

RECIPES = [
    "hive-iceberg-spark-thrift",
    "polaris-iceberg-spark-thrift",
    "hive-delta-spark-thrift",
    "hive-iceberg-spark-trino",
    "hive-delta-spark-trino",
]


def thrift_classpath(cfg, handle) -> list[str]:
    from lakebench.deploy.engine import DeploymentEngine

    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    engine = DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)
    ctx = {**engine.context, **m.consumer_context(handle)}
    text = TemplateRenderer().render("spark-thrift/sparkapplication.yaml.j2", ctx)
    (cp,) = re.findall(r'--conf "spark\.driver\.extraClassPath=([^"]+)"', text)
    return cp.split(":")


def job_jars(cfg, handle) -> list[str]:
    mgr = SparkJobManager(cfg, MagicMock())
    mgr.deps = handle
    conf = mgr._build_manifest(JobType.BRONZE_VERIFY)["spec"]["sparkConf"]
    return [unquote(u.rsplit("/", 1)[1]) for u in conf["spark.jars"].split(",")]


@pytest.mark.parametrize("image", ["apache/spark:4.0.2-python3", "apache/spark:4.1.1-python3"])
@pytest.mark.parametrize("recipe", RECIPES)
def test_thrift_and_jobs_share_the_set_in_its_order(recipe, image):
    cfg = make_config(recipe=recipe, images={"spark": image})
    handle = m.placeholder_handle(cfg)
    jars = job_jars(cfg, handle)
    assert jars == list(handle.manifest["jar_order"])
    cp = thrift_classpath(cfg, handle)
    # The conf dir and the image's jars first, then the set in its order.
    assert cp[:2] == ["/opt/spark/conf", "/opt/spark/jars/*"]
    assert [p.rsplit("/", 1)[1] for p in cp[2:]] == jars
    runtimes = [j for j in jars if "iceberg-spark-runtime-" in j]
    assert len(runtimes) == (0 if "delta" in recipe else 1)
    if runtimes and "4.1.1" in image:
        assert "iceberg-spark-runtime-4.1_2.13-" in runtimes[0]


def test_a_reordered_set_reorders_both():
    """The classpath follows the manifest's jar order, not a sorted or glob
    order (the 10-01 amendment: Thrift keeps jar_order)."""
    cfg = make_config(recipe="hive-iceberg-spark-thrift")
    h = m.placeholder_handle(cfg)
    order = list(reversed(h.manifest["jar_order"]))
    flipped = m.DepsHandle(
        pinset_sha256=h.pinset_sha256,
        request_sha256=h.request_sha256,
        base_url=h.base_url,
        server_pod_uid="",
        manifest={**h.manifest, "jar_order": order},
    )
    assert job_jars(cfg, flipped) == order
    assert [p.rsplit("/", 1)[1] for p in thrift_classpath(cfg, flipped)[2:]] == order
