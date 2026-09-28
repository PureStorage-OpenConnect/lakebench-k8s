"""I10: silver_batch_versions semi-join hides a mid-batch crash from
consumers.

Simulates the crash window between the transactions/edges commits and the
sealed-marker insert. We drive _merge_batch directly, but replace the
sealed INSERT with a raise so silver_batch_versions never receives the
row for that batch. Then a reader that semi-joins silver.transactions
against silver_batch_versions on (_stream_id, _batch_id) sees zero rows
for the crashed batch.

Runs in a child process because Iceberg jars must be on the driver
classpath at JVM launch, matching the pattern in
``tests/spark/test_aml_stream_one_delete_per_restart.py``.
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parents[1] / "src/lakebench/spark/scripts"


def _iceberg_jar() -> str | None:
    env = os.environ.get("LB_TEST_ICEBERG_JAR")
    if env and Path(env).exists():
        return env
    hits = sorted(
        glob.glob(str(Path.home() / ".lakebench/local/*/ivy/cache/org.apache.iceberg/*/jars/*.jar"))
        + glob.glob(str(Path.home() / ".ivy2*/cache/org.apache.iceberg/*/jars/*.jar"))
    )
    return next((h for h in hits if "spark-runtime-4.0" in h), None)


def test_crash_window_is_invisible_to_semi_join():
    jar = _iceberg_jar()
    if jar is None:
        pytest.skip("no iceberg-spark-runtime-4.0 jar available (set LB_TEST_ICEBERG_JAR)")
    res = subprocess.run(
        [sys.executable, __file__, jar], capture_output=True, text=True, timeout=600
    )
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-4000:]
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Batch 0 sealed cleanly -- txns rows visible under the filter.
    assert out["sealed_visible_rows"] == 1, out
    # Batch 1 crashed (INSERT into versions raised) -- txns rows exist in
    # the raw table but the semi-join hides them.
    assert out["unfiltered_rows"] == 2, out
    assert out["filtered_rows"] == 1, out
    # Ghosts row count: sanity check that the (sid, batch=1) rows exist raw.
    assert out["ghost_rows_batch_1"] == 1, out
    assert out["versions_row_count"] == 1, out


# ---------------------------------------------------------------------------
# Subprocess payload
# ---------------------------------------------------------------------------

_PACS_SCHEMA = (
    "txn_id string, uetr string, "
    "dbtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "cdtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "dbtr_agt struct<bicfi:string>, cdtr_agt struct<bicfi:string>, "
    "dbtr_acct struct<iban:string>, cdtr_acct struct<iban:string>, "
    "intrmy_agt_1 struct<bicfi:string>, "
    "intrmy_agt_2 struct<bicfi:string>, "
    "intrmy_agt_3 struct<bicfi:string>, "
    "intr_bk_sttlm_amt decimal(18,2), intr_bk_sttlm_ccy string, "
    "cre_dt_tm timestamp, purp_cd string, "
    "rgltry_rptg array<string>, msg_id string"
)


def _bronze_row(spark, txn_id, ts):
    def party(nm):
        return (nm, "US", ("NYC", "MAIN ST"), (f"LEI-{nm}",))

    row = (
        txn_id,
        f"UETR-{txn_id}",
        party(f"O-{txn_id}"),
        party(f"B-{txn_id}"),
        ("MERIUS2L",),
        ("NRTHGB3X",),
        ("US01",),
        ("GB02",),
        (None,),
        (None,),
        (None,),
        Decimal("100.00"),
        "USD",
        ts,
        "SALA",
        [],
        f"MSG-{txn_id}",
    )
    return spark.createDataFrame([row], _PACS_SCHEMA)


_TXNS_DDL = """
CREATE TABLE lh.silver.transactions (
    txn_id                  STRING NOT NULL,
    uetr                    STRING NOT NULL,
    originator_id           BIGINT NOT NULL,
    beneficiary_id          BIGINT NOT NULL,
    originator_bank_bic     STRING,
    beneficiary_bank_bic    STRING,
    txn_amount              DECIMAL(18, 2) NOT NULL,
    txn_currency            STRING NOT NULL,
    txn_amount_usd          DECIMAL(18, 2),
    txn_timestamp           TIMESTAMP NOT NULL,
    txn_type                STRING NOT NULL,
    purpose_code            STRING,
    correspondent_chain     ARRAY<STRING>,
    cross_border            BOOLEAN,
    regulatory_reported     BOOLEAN NOT NULL,
    rptd_originator_name    STRING,
    rptd_originator_address STRING,
    rptd_beneficiary_name   STRING,
    rptd_beneficiary_address STRING,
    source_message_ref      STRING,
    _batch_id               BIGINT,
    _stream_id              STRING,
    ingest_ts               TIMESTAMP
) USING iceberg PARTITIONED BY (months(txn_timestamp))
"""

_EDGES_DDL = """
CREATE TABLE lh.silver.counterparty_edges (
    source_entity_id       BIGINT NOT NULL,
    target_entity_id       BIGINT NOT NULL,
    first_seen_ts          TIMESTAMP NOT NULL,
    last_seen_ts           TIMESTAMP NOT NULL,
    cumulative_amount_usd  DECIMAL(38, 2) NOT NULL,
    txn_count              BIGINT NOT NULL,
    _batch_id              BIGINT,
    _stream_id             STRING
) USING iceberg PARTITIONED BY (bucket(64, source_entity_id))
"""

_VERSIONS_DDL = """
CREATE TABLE lh.silver.silver_batch_versions (
    stream_id      STRING NOT NULL,
    batch_id       BIGINT NOT NULL,
    committed_at   TIMESTAMP NOT NULL
) USING iceberg
"""


def _run(jar):
    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as work:
        spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lh.type", "hadoop")
            .config("spark.sql.catalog.lh.cache-enabled", "false")
            .config("spark.sql.catalog.lh.warehouse", f"file://{work}/wh")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        import silver_stream_financial as ss

        ss.CATALOG = "lh"
        ss.SILVER_TXNS = "silver.transactions"
        ss.SILVER_EDGES = "silver.counterparty_edges"
        ss.SILVER_BATCH_VERSIONS = "silver.silver_batch_versions"

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
        spark.sql(_TXNS_DDL)
        spark.sql(_EDGES_DDL)
        spark.sql(_VERSIONS_DDL)

        # Skip KYC + dimension writes (tables not created here).
        ss._KYC = None
        ss._KYC_LOADED = True
        ss._kyc = lambda _s: None
        ss.append_new_dimensions = lambda *_a, **_kw: (0, 0)

        def bronze(bid):
            return _bronze_row(
                spark,
                f"T{bid}-o-b",
                datetime(2024, 6, 1) + timedelta(days=bid),
            )

        # ---- Batch 0: sealed cleanly.
        ss._merge_batch(bronze(0), 0)

        # ---- Batch 1: crash between phase-2 (edges commit) and phase-4
        # (sealed-marker insert). Intercept spark.sql: allow every phase-1/2
        # DELETE/INSERT to run, but raise on the sealed-marker INSERT to
        # silver_batch_versions.
        real_sql = spark.sql

        def _sql(stmt, *a, **kw):
            if "silver_batch_versions" in stmt and stmt.strip().upper().startswith("INSERT"):
                raise RuntimeError("simulated driver crash before sealed marker")
            return real_sql(stmt, *a, **kw)

        spark.sql = _sql  # type: ignore[assignment]
        try:
            try:
                ss._merge_batch(bronze(1), 1)
            except RuntimeError as e:
                assert "simulated driver crash" in str(e), e
        finally:
            spark.sql = real_sql  # type: ignore[assignment]

        # ---- Consumer: semi-join filter identical to gold_finalize's
        # _sealed_txns helper.
        from pyspark.sql.functions import col

        txns = spark.table("lh.silver.transactions")
        versions = spark.table("lh.silver.silver_batch_versions").select(
            col("stream_id").alias("_sv_stream_id"),
            col("batch_id").alias("_sv_batch_id"),
        )
        filtered = txns.join(
            versions,
            (txns["_stream_id"] == versions["_sv_stream_id"])
            & (txns["_batch_id"] == versions["_sv_batch_id"]),
            "left_semi",
        )

        unfiltered_rows = txns.count()
        filtered_rows = filtered.count()
        sealed_visible_rows = filtered.where(col("_batch_id") == 0).count()
        ghost_rows_batch_1 = txns.where(col("_batch_id") == 1).count()
        versions_row_count = spark.table("lh.silver.silver_batch_versions").count()

        out = {
            "unfiltered_rows": int(unfiltered_rows),
            "filtered_rows": int(filtered_rows),
            "sealed_visible_rows": int(sealed_visible_rows),
            "ghost_rows_batch_1": int(ghost_rows_batch_1),
            "versions_row_count": int(versions_row_count),
        }
        print(json.dumps(out))
        spark.stop()


if __name__ == "__main__":
    sys.path[:0] = [str(SCRIPTS)]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    _run(sys.argv[1])
