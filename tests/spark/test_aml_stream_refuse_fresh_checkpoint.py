"""B3: refuse_fresh_checkpoint_over_data raises SilverAbort on populated silver.

A fresh Iceberg streaming checkpoint would replay every bronze row into a
populated silver table, silently duplicating each. The AML stream now
refuses this at startup with a SilverAbort (uniform silver exit contract).
"""

from __future__ import annotations

import glob
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

_JARS = os.environ.get("LB_SPARK_TEST_JARS", "")
_NEEDED = ("iceberg-spark-runtime",)


def _have_jars() -> bool:
    if not _JARS or not Path(_JARS).is_dir():
        return False
    names = [p.name for p in Path(_JARS).glob("*.jar")]
    return all(any(n.startswith(k) for n in names) for k in _NEEDED)


pytestmark = pytest.mark.skipif(
    not _have_jars(), reason="LB_SPARK_TEST_JARS with Iceberg jars not set"
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    wh = tmp_path_factory.mktemp("aml-refuse-wh")
    jars = ",".join(sorted(glob.glob(os.path.join(_JARS, "*.jar"))))
    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", f"file://{wh}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_refuse_fresh_checkpoint_over_populated_aml_silver(spark, tmp_path):
    """Populated silver.transactions + fresh checkpoint -> SilverAbort."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))
    from common import SilverAbort, refuse_fresh_checkpoint_over_data

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    tbl_t = "ice.silver.transactions_aml"
    tbl_e = "ice.silver.edges_aml"
    # Populate one of the two tables (edges empty is fine; any populated
    # table on the list must trigger refusal).
    spark.createDataFrame([(1,), (2,)], "n bigint").writeTo(tbl_t).create()
    spark.sql(f"CREATE TABLE {tbl_e} (n BIGINT) USING iceberg")

    ckpt = str(tmp_path / "aml-fresh-ckpt")  # never used
    with pytest.raises(SilverAbort):
        refuse_fresh_checkpoint_over_data(spark, ckpt, [tbl_t, tbl_e])


def test_refuse_accepts_single_str_arg(spark, tmp_path):
    """Backwards compatibility: str signature still works."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))
    from common import SilverAbort, refuse_fresh_checkpoint_over_data

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    tbl = "ice.silver.single_arg"
    spark.createDataFrame([(1,)], "n bigint").writeTo(tbl).create()
    with pytest.raises(SilverAbort):
        refuse_fresh_checkpoint_over_data(spark, str(tmp_path / "ckpt-never-used"), tbl)


def test_refuse_no_op_when_all_tables_empty(spark, tmp_path):
    """Empty tables + fresh checkpoint: no refusal."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))
    from common import refuse_fresh_checkpoint_over_data

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    tbl_a = "ice.silver.empty_a"
    tbl_b = "ice.silver.empty_b"
    spark.sql(f"CREATE TABLE {tbl_a} (n BIGINT) USING iceberg")
    spark.sql(f"CREATE TABLE {tbl_b} (n BIGINT) USING iceberg")
    # Should not raise:
    refuse_fresh_checkpoint_over_data(spark, str(tmp_path / "empty-ckpt"), [tbl_a, tbl_b])
