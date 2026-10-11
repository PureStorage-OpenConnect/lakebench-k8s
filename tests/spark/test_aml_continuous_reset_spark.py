"""Executed: the AML continuous reset leaves no silver table the continuous
run writes, and a fresh stream checkpoint refuses any of them populated.

The reset (bronze_verify_financial._continuous_reset) dropped transactions,
edges, entities and accounts but not account_statements, entity_profiles or
the silver_batch_versions sidecar, and the stream's fresh-checkpoint refusal
looked only at transactions and edges. A reused catalog then carried an
earlier run's statements and profiles into the new run's silver: the stream
appends statements and folds profiles into what is there.

Runs on a local Hadoop Iceberg catalog named like the scripts' default.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg")

CAT = "lakehouse"
# Every silver table silver_stream_financial writes, by its default name.
STREAM_SILVER = (
    "silver.transactions",
    "silver.counterparty_edges",
    "silver.entities",
    "silver.accounts",
    "silver.account_statements",
    "silver.entity_profiles",
    "silver.silver_batch_versions",
)
# The gold tables gold-refresh writes besides the TM ones the reset already
# dropped; a reader (score_financial) would take their rows as this run's.
STREAM_GOLD = (
    "gold.alerts",
    "gold.risk_scores",
    "gold.entity_clusters",
    "gold.daily_dashboards",
    "gold.detection_status",
)


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, CAT, tmp_path_factory.mktemp("aml-reset-wh"))
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.silver")
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.default")
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.bronze")
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.gold")
    return spark_session


def _populate(spark, tables):
    for t in tables:
        spark.sql(f"DROP TABLE IF EXISTS {CAT}.{t}")
        spark.createDataFrame([(1,), (2,)], "n bigint").writeTo(f"{CAT}.{t}").create()


def _exists(spark, t):
    return spark.catalog.tableExists(f"{CAT}.{t}")


@pytest.mark.parametrize("kind", ["silver", "gold"])
def test_reset_drops_every_table_the_continuous_run_writes(spark, load_script, monkeypatch, kind):
    monkeypatch.setenv("LB_CATALOG_TYPE", "polaris")  # no explicit bronze location
    bvf = load_script("bronze_verify_financial")
    if kind == "silver":
        # The script's own set, so a table added there is populated and
        # checked too; it must still name the seven the stream writes today.
        tables = tuple(bvf.CONTINUOUS_SILVER_TABLES)
        assert set(STREAM_SILVER) <= set(tables)
    else:
        # Until gold-refresh's first tick, the previous run's alerts and
        # detection status would read as this run's.
        tables = STREAM_GOLD
    _populate(spark, tables)
    bvf._continuous_reset(spark, spark.createDataFrame([("x",)], "msg_id string"))
    left = [t for t in tables if _exists(spark, t)]
    assert left == [], f"the continuous reset left {left}"


@pytest.mark.parametrize("table", STREAM_SILVER)
def test_fresh_checkpoint_refuses_any_populated_stream_table(spark, load_script, tmp_path, table):
    ssf = load_script("silver_stream_financial")
    common = load_script("common")
    for t in STREAM_SILVER:
        spark.sql(f"DROP TABLE IF EXISTS {CAT}.{t}")
    _populate(spark, [table])
    with pytest.raises(common.SilverAbort):
        ssf.refuse_reused_silver(spark, str(tmp_path / "fresh-ckpt"))
