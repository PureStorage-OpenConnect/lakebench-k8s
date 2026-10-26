"""``tm_operations.current_snapshot_id`` names the table's current snapshot,
not the one with the newest commit time.

Commit times come from each writer's clock. A snapshot whose writer's clock
runs ahead, or one a rollback left behind, sorts as the newest while the
table reads another, and the TM silver pin, the ledger's recorded snapshots
and the first-detection lookup then read an older or abandoned state.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]

CATALOG = "cur"


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, CATALOG, tmp_path_factory.mktemp("current-snapshot-wh"))
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.db")
    return spark_session


def _ids(spark, fq):
    rows = spark.sql(f"SELECT snapshot_id FROM {fq}.snapshots ORDER BY committed_at").collect()
    return [int(r[0]) for r in rows]


def test_current_snapshot_after_a_rollback(spark):
    import gold_refresh_financial as gr
    from tm_operations import current_snapshot_id

    fq = f"{CATALOG}.db.rolled_back"
    spark.sql(f"CREATE TABLE {fq} (n BIGINT) USING iceberg")
    assert current_snapshot_id(spark, fq) is None
    spark.sql(f"INSERT INTO {fq} VALUES (1)")
    spark.sql(f"INSERT INTO {fq} VALUES (2)")
    first, second = _ids(spark, fq)
    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('db.rolled_back', {first})")
    # The abandoned snapshot still has the newest commit time.
    assert current_snapshot_id(spark, fq) == first != second
    assert gr._current_snapshot(spark, fq) == first


def test_a_failed_lookup_is_the_callers_unknown(spark):
    import gold_refresh_financial as gr
    from common import TTD_SNAPSHOT_UNKNOWN
    from tm_operations import current_snapshot_id

    fq = f"{CATALOG}.db.missing"
    assert current_snapshot_id(spark, fq) is None
    assert gr._current_snapshot(spark, fq) == TTD_SNAPSHOT_UNKNOWN
