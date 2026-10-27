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
# dropped; a reader (the scorer) would take their rows as this run's.
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
        # The script's own set, so a gold table added there is populated and
        # checked too; until gold-refresh's first tick, the previous run's
        # alerts and detection status would read as this run's.
        tables = tuple(bvf.CONTINUOUS_GOLD_TABLES)
        assert set(STREAM_GOLD) <= set(tables)
    _populate(spark, tables)
    bvf._continuous_reset(spark, spark.createDataFrame([("x",)], "msg_id string"))
    left = [t for t in tables if _exists(spark, t)]
    assert left == [], f"the continuous reset left {left}"


# A table the refusal once missed; one refusal path covers the list, whose
# membership tests/test_aml_continuous_reset_tables.py pins.
def test_fresh_checkpoint_refuses_any_populated_stream_table(spark, load_script, tmp_path):
    ssf = load_script("silver_stream_financial")
    common = load_script("common")
    for t in STREAM_SILVER:
        spark.sql(f"DROP TABLE IF EXISTS {CAT}.{t}")
    _populate(spark, ["silver.account_statements"])
    with pytest.raises(common.SilverAbort):
        ssf.refuse_reused_silver(spark, str(tmp_path / "fresh-ckpt"))


def test_register_accounts_keeps_the_holder_map_only(spark, load_script, monkeypatch, tmp_path):
    """bronze.accounts holds (holder_entity_id, iban) from account.parquet; a
    missing file registers nothing and reports False."""
    bvf = load_script("bronze_verify_financial")
    path = tmp_path / "account.parquet"
    spark.createDataFrame(
        [(1, "DE01", "BIC1", "EUR"), (2, "DE02", "BIC2", "EUR")],
        "holder_entity_id bigint, iban string, bank_bic string, currency string",
    ).write.parquet(str(path))
    monkeypatch.setattr(bvf, "ACCOUNT_URI", str(path))
    spark.sql(f"DROP TABLE IF EXISTS {CAT}.bronze.accounts")
    assert bvf.register_accounts(spark) is True
    rows = spark.table(f"{CAT}.bronze.accounts")
    assert rows.columns == ["holder_entity_id", "iban"]
    assert rows.count() == 2

    monkeypatch.setattr(bvf, "ACCOUNT_URI", str(tmp_path / "missing.parquet"))
    assert bvf.register_accounts(spark) is False


def test_reset_drops_the_previous_runs_account_map(spark, load_script, monkeypatch):
    monkeypatch.setenv("LB_CATALOG_TYPE", "polaris")
    bvf = load_script("bronze_verify_financial")
    _populate(spark, ["bronze.accounts", "bronze.manifest"])
    bvf._continuous_reset(spark, spark.createDataFrame([("x",)], "msg_id string"))
    # Both re-register from files that do not exist here, so both are gone.
    assert not _exists(spark, "bronze.accounts")
    assert not _exists(spark, "bronze.manifest")


def test_refuse_no_op_when_all_tables_empty(spark, load_script, tmp_path):
    """Empty tables + fresh checkpoint: no refusal."""
    common = load_script("common")
    tbl_a, tbl_b = f"{CAT}.silver.empty_a", f"{CAT}.silver.empty_b"
    for t in (tbl_a, tbl_b):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        spark.sql(f"CREATE TABLE {t} (n BIGINT) USING iceberg")
    # Should not raise:
    common.refuse_fresh_checkpoint_over_data(spark, str(tmp_path / "empty-ckpt"), [tbl_a, tbl_b])
