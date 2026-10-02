"""Executed: financial replay reads silver at its resolved snapshot on the
default Iceberg of Spark 4.x.

Replay read with ``spark.read.option("snapshot-id", ...)``, which Iceberg 1.11
removed, so on Spark 4.1.1 + Iceberg 1.11.0 it failed before any rule ran.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg")


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, "lakehouse", tmp_path_factory.mktemp("replay-snap-wh"))
    spark_session.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    return spark_session


def test_replay_reads_the_table_as_of_its_snapshot(spark, load_script):
    replay = load_script("replay_financial")
    fq = "lakehouse.silver.transactions_replay"
    spark.sql(f"DROP TABLE IF EXISTS {fq}")
    spark.createDataFrame([(1,), (2,)], "n bigint").writeTo(fq).create()
    first = spark.sql(f"SELECT snapshot_id FROM {fq}.snapshots").collect()[0][0]
    spark.createDataFrame([(3,)], "n bigint").writeTo(fq).append()
    got = sorted(r.n for r in replay.read_at_snapshot_id(spark, fq, first).collect())
    assert got == [1, 2]
