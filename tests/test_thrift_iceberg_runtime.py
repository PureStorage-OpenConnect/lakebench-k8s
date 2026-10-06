"""UX D2 (SD-20): Spark Thrift loads the same Iceberg runtime as the jobs.

The Thrift package list (now the dependency set) read `_ICEBERG_RUNTIME_SUFFIX`
directly, which maps Spark 4.1 to the 4.0 runtime whatever the Iceberg
version. The jobs call `iceberg_runtime_suffix_for`, which picks the native
4.1 runtime from Iceberg 1.11.0. On Spark 4.1 with Iceberg 1.11, Thrift
loaded `iceberg-spark-runtime-4.0` and the jobs `iceberg-spark-runtime-4.1`.
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


def _cfg(image: str, iceberg: str):
    return make_config(
        recipe="hive-iceberg-spark-thrift",
        images={"spark": image},
        architecture={"table_format": {"type": "iceberg", "iceberg": {"version": iceberg}}},
    )


def _runtime(jars: list[str]) -> str:
    (runtime,) = [j for j in jars if "iceberg-spark-runtime-" in j]
    return runtime


@pytest.mark.parametrize(
    "image,iceberg,expected",
    [
        # Native 4.1 runtime from Iceberg 1.11.0: the case today's line got wrong.
        (
            "apache/spark:4.1.1-python3",
            "1.11.0",
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        ),
        # No 4.1 runtime before 1.11: both borrow 4.0.
        (
            "apache/spark:4.1.1-python3",
            "1.10.1",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
        ),
        (
            "apache/spark:4.0.2-python3",
            "1.11.0",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
        ),
    ],
)
def test_thrift_and_jobs_load_the_same_iceberg_runtime(image, iceberg, expected):
    cfg = _cfg(image, iceberg)
    handle = m.placeholder_handle(cfg)
    thrift = _runtime([p.rsplit("/", 1)[1] for p in thrift_classpath(cfg, handle)[1:]])
    job = _runtime(job_jars(cfg, handle))
    assert thrift == job == m.ivy_jar_name(expected)
