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
from pyspark.sql.functions import col, expr

BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
# datagen_rs writes pacs.008 files under `<uploader_prefix>/bronze/pacs008/*`
# (see datagen_rs/src/bin/generate.rs: `bronze/pacs008/...`). The datagen_py
# Financial generator writes under `pacs008/`. Datagen_py is the shipping
# batch-mode path (`lakebench generate` uses the Docker image), so default
# to that layout; operators using the Rust datagen at scale >100 override
# to `bronze/pacs008/`.
PACS_PREFIX = env("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/")
# Default REGISTER=1: silver_build reads a real Iceberg table
# (spark.table(CATALOG.pacs008_raw)), not the raw parquet, so without
# registration the entire pipeline stalls at silver-build with
# TableNotFoundException. The lakebench run path invokes bronze_verify
# once per pipeline and expects registration as a side effect. Setting
# LB_REGISTER_TABLE=0 turns off the register when the operator wants
# a verify-only pass without CTAS/add_files side effects.
REGISTER = env("LB_REGISTER_TABLE", "1") == "1"
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

    # Non-null invariants. `intr_bk_sttlm_dt` is required (partition key of
    # the target Iceberg table); one NULL row rejects the whole CTAS with
    # a cryptic partition-transform error. Fail loud here rather than there.
    null_counts: dict[str, int] = {}
    for c in ("uetr", "txn_id", "msg_id"):
        n = df.filter(col(c).isNull()).count()
        null_counts[c] = n
        if n:
            log(f"WARNING: {n} rows have NULL {c}")
    n_null_sttlm_dt = df.filter(col("intr_bk_sttlm_dt").isNull()).count()
    if n_null_sttlm_dt:
        log(
            f"ERROR: {n_null_sttlm_dt} rows have NULL intr_bk_sttlm_dt; "
            "bronze registration will fail on the days() partition transform."
        )
        spark.stop()
        raise SystemExit(3)

    partition_days = df.select("intr_bk_sttlm_dt").distinct().count()
    log(f"Partition days present: {partition_days}")

    if REGISTER:
        # Register the Iceberg table over the datagen Parquet files. Two
        # implementations are attempted in order:
        #
        # 1. add_files (preferred, no data copy): create the Iceberg table
        #    with the FULL parquet schema first (via spark.read.parquet
        #    inferred schema), then attach existing files. The previous
        #    revision created the target table with only 5 hard-coded
        #    columns before add_files; add_files resolves by column NAME
        #    against the target schema, so the other 30+ pacs.008
        #    fields (dbtr.*, cdtr.*, xchg_rate, rgltry_rptg, etc)
        #    silently dropped and silver_build immediately blew up on
        #    the missing struct columns.
        #
        # 2. CTAS fallback: if add_files isn't supported by the catalog
        #    (e.g. Nessie REST prior to a certain version), rewrite the
        #    data into the Iceberg table. Doubles S3 usage.
        try:
            # Full inferred schema for the CREATE, so add_files finds
            # every column of the parquet source under its real name.
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_TABLE}")
            src = spark.read.parquet(BRONZE_URI + PACS_PREFIX)
            # Iceberg CREATE ... USING iceberg PARTITIONED BY (days(...))
            # requires the partition column to be declared in the schema,
            # which it already is (intr_bk_sttlm_dt DATE). Use writeTo
            # with .create() to get schema-from-DataFrame + partition
            # spec + tblproperties in a single atomic call.
            (
                src.limit(0)
                .writeTo(f"{CATALOG}.{BRONZE_TABLE}")
                .using("iceberg")
                .partitionedBy(expr("days(intr_bk_sttlm_dt)"))
                .tableProperty("format-version", "2")
                .tableProperty("write.parquet.compression-codec", "snappy")
                .create()
            )
            spark.sql(
                f"CALL {CATALOG}.system.add_files("
                f"  table => '{BRONZE_TABLE}', "
                f"  source_table => 'parquet.`{BRONZE_URI}{PACS_PREFIX}`'"
                f")"
            )
            log(f"Registered Iceberg table by reference: {CATALOG}.{BRONZE_TABLE}")
        except Exception as e:  # noqa: BLE001
            log(
                f"add_files failed ({e}); falling back to CTAS. "
                "This doubles S3 usage; investigate catalog support for add_files."
            )
            spark.sql(f"""
                CREATE OR REPLACE TABLE {CATALOG}.{BRONZE_TABLE}
                USING iceberg
                PARTITIONED BY (days(intr_bk_sttlm_dt))
                TBLPROPERTIES ('format-version' = '2')
                AS SELECT * FROM parquet.`{BRONZE_URI}{PACS_PREFIX}`
            """)
            log(f"Registered via CTAS fallback: {CATALOG}.{BRONZE_TABLE}")

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
