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


@pytest.mark.parametrize("as_list", [True, False], ids=["table-list", "single-str"])
def test_refuse_fresh_checkpoint_over_populated_aml_silver(spark, tmp_path, as_list):
    """A populated table, named in a list or as one str, plus a fresh
    checkpoint -> SilverAbort."""
    from common import SilverAbort, refuse_fresh_checkpoint_over_data

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    suffix = "list" if as_list else "str"
    tbl_t = f"ice.silver.transactions_aml_{suffix}"
    tbl_e = f"ice.silver.edges_aml_{suffix}"
    # Populate one of the tables (edges empty is fine; any populated table
    # on the list must trigger refusal).
    spark.createDataFrame([(1,), (2,)], "n bigint").writeTo(tbl_t).create()
    spark.sql(f"CREATE TABLE {tbl_e} (n BIGINT) USING iceberg")

    ckpt = str(tmp_path / "aml-fresh-ckpt")  # never used
    with pytest.raises(SilverAbort):
        refuse_fresh_checkpoint_over_data(spark, ckpt, [tbl_t, tbl_e] if as_list else tbl_t)


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
