"""UX D2 (SD-20): Spark Thrift loads the same Iceberg runtime as the jobs.

`DeploymentEngine._build_spark_thrift_packages` read `_ICEBERG_RUNTIME_SUFFIX`
directly, which maps Spark 4.1 to the 4.0 runtime whatever the Iceberg
version. The jobs call `iceberg_runtime_suffix_for`, which picks the native
4.1 runtime from Iceberg 1.11.0. On Spark 4.1 with Iceberg 1.11, Thrift
loaded `iceberg-spark-runtime-4.0` and the jobs `iceberg-spark-runtime-4.1`.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakebench.deploy.engine import DeploymentEngine
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.conftest import make_config


def _cfg(image: str, iceberg: str):
    return make_config(
        recipe="hive-iceberg-spark-thrift",
        images={"spark": image},
        architecture={"table_format": {"type": "iceberg", "iceberg": {"version": iceberg}}},
    )


def _runtime(packages: str) -> str:
    (runtime,) = [p for p in packages.split(",") if ":iceberg-spark-runtime-" in p]
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
    thrift = _runtime(DeploymentEngine._build_spark_thrift_packages(cfg))
    job_conf = SparkJobManager(cfg, MagicMock())._build_manifest(JobType.BRONZE_VERIFY)["spec"][
        "sparkConf"
    ]
    job = _runtime(job_conf["spark.jars.packages"])
    assert thrift == expected
    assert job == expected
