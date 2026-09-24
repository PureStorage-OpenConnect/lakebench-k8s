"""Executed test: continuous silver appends the entities and accounts each
micro-batch introduces, with KYC, and a replayed batch writes nothing twice
(continuous mode never runs the batch silver_build that fills them).

The append path needs a V2 (Iceberg) table, so the checks run in a child
process with a fresh JVM that has the iceberg runtime jar on its classpath:
an earlier test module in the same pytest run has already started a JVM
without it, and spark.jars cannot be added to a running JVM.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parents[1] / "src/lakebench/spark/scripts"


def _iceberg_jar() -> str | None:
    """LB_TEST_ICEBERG_JAR, or an iceberg-spark-runtime-4.0 jar in a local ivy cache."""
    import glob
    import os

    env = os.environ.get("LB_TEST_ICEBERG_JAR")
    if env and Path(env).exists():
        return env
    hits = sorted(
        glob.glob(str(Path.home() / ".lakebench/local/*/ivy/cache/org.apache.iceberg/*/jars/*.jar"))
        + glob.glob(str(Path.home() / ".ivy2*/cache/org.apache.iceberg/*/jars/*.jar"))
    )
    return next((h for h in hits if "spark-runtime-4.0" in h), None)


def test_continuous_dimensions_in_a_fresh_jvm():
    pytest.importorskip("pyspark")
    jar = _iceberg_jar()
    if jar is None:
        pytest.skip("no iceberg-spark-runtime-4.0 jar available (set LB_TEST_ICEBERG_JAR)")
    res = subprocess.run(
        [sys.executable, __file__, jar], capture_output=True, text=True, timeout=600
    )
    assert res.returncode == 0, res.stdout[-3000:] + res.stderr[-3000:]
    assert "OK dims" in res.stdout and "OK kyc" in res.stdout


def _check_dims(spark) -> None:
    import silver_build_financial as sb
    import silver_stream_financial as ss
    from test_silver_kyc_spark import _bronze, _refs, _txns

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
    import os

    sys.path[:0] = [str(SCRIPTS), str(HERE)]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as d:
        spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.jars", sys.argv[1])
            .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lh.type", "hadoop")
            .config("spark.sql.catalog.lh.warehouse", str(Path(d) / "wh"))
            .getOrCreate()
        )
        _check_dims(spark)
        _check_kyc_absent(spark, Path(d))
        spark.stop()
