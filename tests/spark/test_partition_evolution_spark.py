"""Executed: a reused Iceberg table moves from days() to months() partitioning,
and a full overwrite afterwards leaves exactly one copy of the data.

Needs the Iceberg Spark runtime jar; set LB_SPARK_TEST_JARS to a directory or
comma-separated list of jars (see tests/spark/test_c360_stream_replay_spark.py).
"""

from __future__ import annotations

import glob
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


def _jars() -> str:
    spec = os.environ.get("LB_SPARK_TEST_JARS", "")
    paths = []
    for part in filter(None, spec.split(",")):
        paths += (
            glob.glob(os.path.join(part, "iceberg-spark-runtime*.jar"))
            if os.path.isdir(part)
            else [part]
        )
    return ",".join(p for p in paths if "iceberg" in os.path.basename(p))


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    jars = _jars()
    if not jars:
        pytest.skip("LB_SPARK_TEST_JARS has no Iceberg runtime jar")
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    wh = tmp_path_factory.mktemp("wh")
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.t", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.t.type", "hadoop")
        .config("spark.sql.catalog.t.warehouse", str(wh))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_days_to_months_then_overwrite(spark):
    from common import _partition_transforms, ensure_partition_transform
    from pyspark.sql.functions import lit

    spark.sql(
        "CREATE TABLE t.db.x (id bigint, ts timestamp) USING iceberg PARTITIONED BY (days(ts))"
    )
    spark.sql(
        "INSERT INTO t.db.x VALUES (1, TIMESTAMP '2024-03-01 10:00:00'), (2, TIMESTAMP '2024-03-02 10:00:00')"
    )
    assert _partition_transforms(spark, "t.db.x") == ["days(ts)"]

    assert ensure_partition_transform(spark, "t.db.x", "days(ts)", "months(ts)") is True
    assert _partition_transforms(spark, "t.db.x") == ["months(ts)"]
    # Idempotent: a second call is a no-op.
    assert ensure_partition_transform(spark, "t.db.x", "days(ts)", "months(ts)") is False

    # The silver write path: a full overwrite replaces old-spec files too.
    df = spark.table("t.db.x")
    df.writeTo("t.db.x").overwrite(lit(True))
    assert spark.table("t.db.x").count() == 2
    specs = {r[0] for r in spark.sql("SELECT spec_id FROM t.db.x.files").collect()}
    assert len(specs) == 1


def test_missing_table_is_logged_not_raised(spark):
    from common import ensure_partition_transform

    assert ensure_partition_transform(spark, "t.db.nope", "days(ts)", "months(ts)") is False
