"""B3: refuse_fresh_checkpoint_over_data raises SilverAbort on populated silver.

A fresh Iceberg streaming checkpoint would replay every bronze row into a
populated silver table, silently duplicating each. The AML stream now
refuses this at startup with a SilverAbort (uniform silver exit contract).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, "ice", tmp_path_factory.mktemp("aml-refuse-wh"))
    return spark_session


def test_refuse_fresh_checkpoint_over_populated_aml_silver(spark, tmp_path):
    """A populated table named as one str, plus a fresh checkpoint ->
    SilverAbort. The list form runs through refuse_reused_silver in
    test_aml_continuous_reset_spark.py."""
    from common import SilverAbort, refuse_fresh_checkpoint_over_data

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    tbl_t = "ice.silver.transactions_aml_str"
    spark.createDataFrame([(1,), (2,)], "n bigint").writeTo(tbl_t).create()

    ckpt = str(tmp_path / "aml-fresh-ckpt")  # never used
    with pytest.raises(SilverAbort):
        refuse_fresh_checkpoint_over_data(spark, ckpt, tbl_t)


def test_refuse_no_op_when_all_tables_empty(spark, tmp_path):
    """Empty tables + fresh checkpoint: no refusal."""
    from common import refuse_fresh_checkpoint_over_data

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    tbl_a = "ice.silver.empty_a"
    tbl_b = "ice.silver.empty_b"
    spark.sql(f"CREATE TABLE {tbl_a} (n BIGINT) USING iceberg")
    spark.sql(f"CREATE TABLE {tbl_b} (n BIGINT) USING iceberg")
    # Should not raise:
    refuse_fresh_checkpoint_over_data(spark, str(tmp_path / "empty-ckpt"), [tbl_a, tbl_b])
