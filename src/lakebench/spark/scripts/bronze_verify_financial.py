"""Bronze Verify (Financial) -- validates pacs.008 datagen output.

Companion to ``bronze_verify.py``. Instead of the Customer 360 schema
this script asserts the flat pacs.008 fields the Financial silver_build
depends on: msg_id, uetr, txn_id, party chain, settlement date/amount.
It reads the datagen Parquet under ``LB_BRONZE_URI`` and can optionally
register (create-if-not-exists) the Iceberg table under
``LB_FINANCIAL_BRONZE_TABLE`` when ``LB_REGISTER_TABLE=1``.
"""

from __future__ import annotations

import time

from common import env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
PACS_PREFIX = env("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/")
REGISTER = env("LB_REGISTER_TABLE", "0") == "1"
CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")


REQUIRED_FLAT_COLS = (
    "msg_id",
    "cre_dt_tm",
    "intr_bk_sttlm_dt",
    "txn_id",
    "end_to_end_id",
    "uetr",
    "intr_bk_sttlm_amt",
    "intr_bk_sttlm_ccy",
    "dbtr",
    "cdtr",
    "dbtr_agt",
    "cdtr_agt",
    "purp_cd",
)


def main() -> None:
    spark = SparkSession.builder.appName("lb-bronze-verify-financial").getOrCreate()
    start_time = time.time()

    log("=" * 60)
    log("Bronze Data Verification (Financial / pacs.008)")
    log("=" * 60)
    log(f"Reading from: {BRONZE_URI}{PACS_PREFIX}")

    try:
        df = spark.read.parquet(BRONZE_URI + PACS_PREFIX)
    except Exception as e:
        log(f"ERROR: Cannot read Bronze pacs.008 data: {e}")
        spark.stop()
        raise SystemExit(1) from e

    row_count = df.count()
    col_count = len(df.columns)

    log("=" * 60)
    log("DATA SUMMARY")
    log("=" * 60)
    log(f"Rows: {row_count:,}")
    log(f"Columns: {col_count}")

    missing = [c for c in REQUIRED_FLAT_COLS if c not in df.columns]
    if missing:
        log(f"ERROR: pacs.008 dataset missing required columns: {missing}")
        spark.stop()
        raise SystemExit(2)

    # Basic non-null invariants -- catches datagen bugs at bronze-verify time.
    null_counts: dict[str, int] = {}
    for c in ("uetr", "txn_id", "msg_id"):
        n = df.filter(col(c).isNull()).count()
        null_counts[c] = n
        if n:
            log(f"WARNING: {n} rows have NULL {c}")

    partition_days = df.select("intr_bk_sttlm_dt").distinct().count()
    log(f"Partition days present: {partition_days}")

    if REGISTER:
        # Bronze is an external Iceberg table over the datagen Parquet files.
        # The full column schema lives in src/lakebench/deploy/financial_ddl.py;
        # here we register a lightweight external table that reads the flat
        # Parquet directly. silver_build_financial.py owns the CREATE-TABLE
        # for silver + gold. Idempotent via IF NOT EXISTS.
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_TABLE}
            USING iceberg
            PARTITIONED BY (days(intr_bk_sttlm_dt))
            TBLPROPERTIES (
                'format-version' = '2',
                'write.parquet.compression-codec' = 'snappy'
            )
            AS SELECT * FROM parquet.`{BRONZE_URI}{PACS_PREFIX}` LIMIT 0
        """)
        # Populate by inserting the actual data.
        spark.sql(f"""
            INSERT INTO {CATALOG}.{BRONZE_TABLE}
            SELECT * FROM parquet.`{BRONZE_URI}{PACS_PREFIX}`
        """)
        log(f"Registered Iceberg table: {CATALOG}.{BRONZE_TABLE}")

    elapsed = time.time() - start_time
    log("=" * 60)
    log(
        f"Bronze verification complete in {elapsed:.1f}s: rows={row_count:,} "
        f"cols={col_count} days={partition_days}"
    )
    log("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
