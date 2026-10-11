"""A continuous gold reader that looks every few seconds sees a commit made
by another process (common.pinned_read). Iceberg's caching catalog keeps a
table until it goes unread for 30 s, so a back-to-back reader without a
refresh kept the snapshot it first loaded: AML gold read an empty silver for
a whole live run, and Customer360 gold could recompute a date from an older
silver than the batch it was taking."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]


def test_pinned_read_sees_a_commit_from_another_writer(spark_session, iceberg_catalog, tmp_path):
    from common import pinned_read

    spark = spark_session
    # Two catalog names over one warehouse: "wr" plays the silver writer's
    # pod, "rd" the gold reader with the cache on, as deployed.
    rd = iceberg_catalog(spark, "rd", tmp_path / "wh", cache_enabled=True)
    wr = iceberg_catalog(spark, "wr", tmp_path / "wh", cache_enabled=False)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {wr}.silver")
    spark.sql(f"CREATE TABLE {wr}.silver.t (id BIGINT) USING iceberg")
    spark.sql(f"INSERT INTO {wr}.silver.t VALUES (1)")
    first, sid1 = pinned_read(spark, f"{rd}.silver.t", "iceberg")
    assert first.count() == 1
    spark.sql(f"INSERT INTO {wr}.silver.t VALUES (2)")
    second, sid2 = pinned_read(spark, f"{rd}.silver.t", "iceberg")
    assert sid2 != sid1 and second.count() == 2
