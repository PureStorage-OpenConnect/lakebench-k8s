"""Spark Thrift and the Spark jobs load one package list (ch01 s2.6, UX D2).

Guards the regression SD-20 fixed: for every supported catalog and format on
Spark 4.0 and 4.1, Thrift's package list equals the jobs'. On 46cc3f4 the
Spark 4.1 Iceberg rows fail (Thrift borrowed the 4.0 runtime).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakebench.deploy.engine import DeploymentEngine
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.conftest import make_config

RECIPES = [
    "hive-iceberg-spark-thrift",
    "polaris-iceberg-spark-thrift",
    "hive-delta-spark-thrift",
    "hive-iceberg-spark-trino",
    "hive-delta-spark-trino",
]


@pytest.mark.parametrize("image", ["apache/spark:4.0.2-python3", "apache/spark:4.1.1-python3"])
@pytest.mark.parametrize("recipe", RECIPES)
def test_thrift_and_jobs_share_package_list(recipe, image):
    cfg = make_config(recipe=recipe, images={"spark": image})
    thrift = DeploymentEngine._build_spark_thrift_packages(cfg).split(",")
    conf = SparkJobManager(cfg, MagicMock())._build_manifest(JobType.BRONZE_VERIFY)["spec"][
        "sparkConf"
    ]
    assert thrift == conf["spark.jars.packages"].split(",")
    runtimes = [p for p in thrift if ":iceberg-spark-runtime-" in p]
    assert len(runtimes) == (0 if "delta" in recipe else 1)
    if runtimes and "4.1.1" in image:
        assert ":iceberg-spark-runtime-4.1_2.13:" in runtimes[0]
