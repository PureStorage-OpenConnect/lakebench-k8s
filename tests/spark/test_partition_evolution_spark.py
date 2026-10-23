"""Executed: a reused Iceberg table moves from days() to months() partitioning
through ``common.ensure_partition_transform``.

Runs in a fresh Python process through ``spark_subprocess``: the Iceberg jar
from ``LB_SPARK_TEST_JARS`` must be on the JVM's classpath at startup, and
another module's session may already own this process's JVM.
"""

from __future__ import annotations

import textwrap

import pytest

pytest.importorskip("pyspark")


# argv: <jars> <warehouse>. spark_subprocess puts the scripts on PYTHONPATH.
SCENARIO = textwrap.dedent(
    """
    import sys
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.jars", sys.argv[1])
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.t", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.t.type", "hadoop")
         .config("spark.sql.catalog.t.warehouse", sys.argv[2])
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    from common import _partition_transforms, ensure_partition_transform
    s.sql("CREATE TABLE t.db.x (id bigint, ts timestamp) USING iceberg PARTITIONED BY (days(ts))")
    s.sql("INSERT INTO t.db.x VALUES (1, TIMESTAMP '2024-03-01 10:00:00'), (2, TIMESTAMP '2024-03-02 10:00:00')")
    assert _partition_transforms(s, "t.db.x") == ["days(ts)"]
    assert ensure_partition_transform(s, "t.db.x", "days(ts)", "months(ts)") is True
    assert _partition_transforms(s, "t.db.x") == ["months(ts)"]
    assert ensure_partition_transform(s, "t.db.x", "days(ts)", "months(ts)") is False
    # A missing table is logged, not raised.
    assert ensure_partition_transform(s, "t.db.nope", "days(ts)", "months(ts)") is False
    s.stop()
    print("SCENARIO-OK")
    """
)


@pytest.mark.requires_jars("iceberg")
def test_days_to_months(tmp_path, spark_subprocess, spark_jars):
    r = spark_subprocess("-c", SCENARIO, spark_jars.classpath, tmp_path / "wh", timeout=600)
    assert "SCENARIO-OK" in r.stdout, r.stdout[-2000:] + r.stderr[-4000:]
