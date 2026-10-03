"""The Spark-tier harness keeps each module's session shape to that module
when several modules share one JVM (QA-2). pyspark hands the first
builder's conf to spark-submit as JVM system properties, so without the
bare gateway a Delta module's ``spark_catalog`` stays in every later
module's SparkConf. Runs a pytest session with a Delta module and an
Iceberg-only module, in both orders."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytest_plugins = ["pytester"]
pytestmark = pytest.mark.requires_jars("iceberg", "delta")

HARNESS = Path(__file__).resolve().parent / "conftest.py"
ROOT = HARNESS.parents[2]

_DELTA = """
import pytest

pytestmark = pytest.mark.requires_jars("delta")

def test_delta_session(spark_session):
    conf = spark_session.sparkContext.getConf()
    assert conf.get("spark.sql.catalog.spark_catalog").endswith("DeltaCatalog")
"""

_ICEBERG = """
import pytest

pytestmark = pytest.mark.requires_jars("iceberg")

def test_iceberg_session(spark_session):
    conf = spark_session.sparkContext.getConf()
    assert conf.get("spark.sql.catalog.spark_catalog", None) is None
    assert conf.get("spark.sql.extensions").endswith("IcebergSparkSessionExtensions")
"""


@pytest.mark.parametrize("reverse", [False, True])
def test_one_modules_static_conf_does_not_reach_the_next(pytester, monkeypatch, reverse):
    pytester.makeconftest(HARNESS.read_text())
    pytester.makepyfile(test_a_delta=_DELTA, test_b_iceberg=_ICEBERG)
    # The inner session sets PYSPARK_SUBMIT_ARGS from LB_SPARK_TEST_JARS itself.
    monkeypatch.delenv("PYSPARK_SUBMIT_ARGS", raising=False)
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(ROOT), str(ROOT / "src")]))
    args = ["-p", "no:cacheprovider", "-q"] + (["--lb-reverse"] if reverse else [])
    res = pytester.runpytest_subprocess(*args, timeout=300)
    res.assert_outcomes(passed=2)
