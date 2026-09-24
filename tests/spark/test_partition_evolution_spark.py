"""Executed: a reused Iceberg table moves from days() to months() partitioning,
and a full overwrite afterwards leaves exactly one copy of the data.

Runs in a fresh Python process: the Iceberg jar must be on the JVM's classpath
at startup, and another module's session may already own this process's JVM.
Needs LB_SPARK_TEST_JARS (a directory or comma-separated jars, as for
tests/spark/test_c360_stream_replay_spark.py).
"""

from __future__ import annotations

import glob
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
SCRIPTS = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"


def _jars() -> str:
    paths = []
    for part in filter(None, os.environ.get("LB_SPARK_TEST_JARS", "").split(",")):
        paths += (
            glob.glob(os.path.join(part, "iceberg-spark-runtime*.jar"))
            if os.path.isdir(part)
            else [part]
        )
    return ",".join(p for p in paths if "iceberg" in os.path.basename(p))


SCENARIO = textwrap.dedent(
    """
    import os, sys
    sys.path.insert(0, sys.argv[1])
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    s = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.jars", sys.argv[2])
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.t", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.t.type", "hadoop")
         .config("spark.sql.catalog.t.warehouse", sys.argv[3])
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    from common import _partition_transforms, ensure_partition_transform
    s.sql("CREATE TABLE t.db.x (id bigint, ts timestamp) USING iceberg PARTITIONED BY (days(ts))")
    s.sql("INSERT INTO t.db.x VALUES (1, TIMESTAMP '2024-03-01 10:00:00'), (2, TIMESTAMP '2024-03-02 10:00:00')")
    assert _partition_transforms(s, "t.db.x") == ["days(ts)"]
    assert ensure_partition_transform(s, "t.db.x", "days(ts)", "months(ts)") is True
    assert _partition_transforms(s, "t.db.x") == ["months(ts)"]
    assert ensure_partition_transform(s, "t.db.x", "days(ts)", "months(ts)") is False
    # The silver write path: a full overwrite replaces the old-spec files too.
    s.table("t.db.x").writeTo("t.db.x").overwrite(lit(True))
    assert s.table("t.db.x").count() == 2
    assert len({r[0] for r in s.sql("SELECT spec_id FROM t.db.x.files").collect()}) == 1
    # A missing table is logged, not raised.
    assert ensure_partition_transform(s, "t.db.nope", "days(ts)", "months(ts)") is False
    s.stop()
    print("SCENARIO-OK")
    """
)


def test_days_to_months_then_overwrite(tmp_path):
    jars = _jars()
    if not jars:
        pytest.skip("LB_SPARK_TEST_JARS has no Iceberg runtime jar")
    r = subprocess.run(
        [sys.executable, "-c", SCENARIO, str(SCRIPTS), jars, str(tmp_path / "wh")],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert "SCENARIO-OK" in r.stdout, r.stdout[-2000:] + r.stderr[-4000:]
