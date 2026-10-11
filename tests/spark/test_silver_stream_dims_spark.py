"""Executed test: continuous silver appends the entities and accounts each
micro-batch introduces, with KYC, and a replayed batch writes nothing twice
(continuous mode never runs the batch silver_build that fills them).

The append path needs a V2 (Iceberg) table, so the checks run in a Spark
child (``spark_subprocess``) whose fresh JVM has the Iceberg runtime jar from
``LB_SPARK_TEST_JARS`` on its classpath.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest


@pytest.mark.requires_jars("iceberg")
def test_continuous_dimensions_in_a_fresh_jvm(spark_subprocess, spark_jars):
    pytest.importorskip("pyspark")
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    assert "OK dims" in res.stdout and "OK kyc" in res.stdout


def _check_dims(spark) -> None:
    import silver_build_financial as sb
    import silver_stream_financial as ss

    from tests.fixtures.silver_kyc_helpers import _bronze, _refs, _txns

    ss.CATALOG = "lh"
    ss.SILVER_ENTITIES = "silver.ents"
    ss.SILVER_ACCOUNTS = "silver.accts"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    for name, ddl in (("silver.ents", sb.DDL_ENTITIES), ("silver.accts", sb.DDL_ACCOUNTS)):
        # The job's own DDL, with this catalog and table swapped in.
        spark.sql(f"CREATE TABLE lh.{name} ({ddl.split('(', 1)[1]}")
    bronze = _bronze(spark)
    kyc = sb.build_kyc(*_refs(spark))
    txns = _txns(bronze)
    assert ss.append_new_dimensions(spark, bronze, txns, kyc) == (2, 2)
    # A replay (or a later batch with the same parties) adds nothing.
    assert ss.append_new_dimensions(spark, bronze, txns, kyc) == (0, 0)
    ents = {r["name"]: r for r in spark.table("lh.silver.ents").collect()}
    assert ents["ALICE"]["is_customer"] is True and ents["ALICE"]["crr_tier"] == "medium"
    assert ents["BOB"]["country"] == "GB" and ents["BOB"]["is_customer"] is False
    accts = {r["iban"]: r for r in spark.table("lh.silver.accts").collect()}
    assert accts["US01"]["home_fi"] == "MERIUS2L" and accts["US01"]["is_customer"] is True
    print("OK dims")


def _check_kyc_absent(spark, tmp: Path) -> None:
    import silver_build_financial as sb
    import silver_stream_financial as ss

    sb.PARTY_PATH = str(tmp / "nope/party.parquet")
    sb.ACCOUNT_PATH = str(tmp / "nope/account.parquet")
    sb.MANIFEST_GLOB = str(tmp / "man/manifest*.parquet")
    ss.KYC_WAIT_S = 0
    # Nothing there and no manifest: after the wait the stream fails loudly.
    ss._KYC, ss._KYC_LOADED = None, False
    try:
        ss._kyc(spark)
        raise AssertionError("missing masters with no manifest did not raise")
    except RuntimeError as e:
        assert "pre-KYC" in str(e)
    # A pre-KYC manifest: NULL KYC, loaded once, no hang.
    spark.createDataFrame([("datagen-v2-rs-0.1",)], "model_version string").write.parquet(
        str(tmp / "man/manifest.parquet")
    )
    ss._KYC, ss._KYC_LOADED = None, False
    assert ss._kyc(spark) is None and ss._KYC_LOADED is True
    print("OK kyc")


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts and tests/spark on
    # PYTHONPATH; argv[1] is the comma-separated jar classpath.
    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as d:
        spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.jars", sys.argv[1])
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lh.type", "hadoop")
            .config("spark.sql.catalog.lh.warehouse", str(Path(d) / "wh"))
            .getOrCreate()
        )
        _check_dims(spark)
        _check_kyc_absent(spark, Path(d))
        spark.stop()
