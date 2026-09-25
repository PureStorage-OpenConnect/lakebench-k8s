"""Bronze Verify (Financial) -- validates pacs.008 datagen output.

Companion to ``bronze_verify.py``. Instead of the Customer 360 schema
this script asserts the flat pacs.008 fields the Financial silver_build
depends on: msg_id, uetr, txn_id, party chain, settlement date/amount.
It reads the datagen Parquet under ``LB_BRONZE_URI`` and can optionally
register (create-if-not-exists) the Iceberg table under
``LB_FINANCIAL_BRONZE_TABLE`` when ``LB_REGISTER_TABLE=1``.
"""

from __future__ import annotations

import os
import time

from common import ensure_namespaces, env, log, log_job_metrics, one_line, path_size_gb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
# datagen_rs is the only shipping datagen after the Python generator was
# removed. It writes into a nested layout under the ROOT prefix it was
# invoked with (--prefix, mirrored to LB_FINANCIAL_BRONZE_PREFIX by
# lakebench/modules/pipeline_engines/spark/job.py from path_template):
#     {root}/bronze/pacs008/part-*.parquet   (pacs.008 transactions)
#     {root}/bronze/party.parquet            (reference table)
#     {root}/bronze/account.parquet          (reference table)
#     {root}/manifest/manifest*.parquet      (typology ground truth; one file per cycle)
# LB-089: prior to PR-F this script assumed the flat datagen_py layout
# where the ROOT prefix directly held the pacs.008 files, and Spark
# listing the ROOT hit the three subdirs and failed with
# UNABLE_TO_INFER_SCHEMA. PACS_PATH is derived from the root plus the
# datagen_rs sub-path; LB_FINANCIAL_PACS_PATH lets an operator override
# for a bespoke layout without leaking that concern into every reader.
BRONZE_ROOT_PREFIX = env("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/")
PACS_PREFIX = env(
    "LB_FINANCIAL_PACS_PATH",
    BRONZE_ROOT_PREFIX.rstrip("/") + "/bronze/pacs008/",
)
# Manifest sidecar (typology ground truth). The AML benchmark queries
# rule_precision, rule_recall, rule_pattern_span, and aggregate_typology_coverage
# all read `{catalog}.bronze.manifest`; without a registration here the
# whole scoring stack fails at Trino with 'Table does not exist'.
# LB-089 round 1 fixed only the pacs.008 read; round 2 (this) adds the
# manifest registration so an AML benchmark actually produces recall.
MANIFEST_PATH = env(
    "LB_FINANCIAL_MANIFEST_PATH",
    # A glob: multi-cycle datagen writes manifest-c001.parquet etc. next to
    # cycle 0's manifest.parquet, and a cycle left out is unlabelled truth.
    BRONZE_ROOT_PREFIX.rstrip("/") + "/manifest/manifest*.parquet",
)
MANIFEST_TABLE = env("LB_FINANCIAL_MANIFEST_TABLE", "bronze.manifest")
# Default REGISTER=1: silver_build reads a real Iceberg table
# (spark.table(CATALOG.pacs008_raw)), not the raw parquet, so without
# registration the entire pipeline stalls at silver-build with
# TableNotFoundException. The lakebench run path invokes bronze_verify
# once per pipeline and expects registration as a side effect. Setting
# LB_REGISTER_TABLE=0 turns off the register when the operator wants
# a verify-only pass without CTAS/add_files side effects.
#
# LB_REGISTER_TABLE=schema is the continuous preflight, and it resets the
# stream's tables for a fresh run. Every continuous run starts clean: the CLI
# deletes the stream checkpoints first, and this drops and recreates
#   - bronze, empty, with the inferred schema plus ingest_ts. Registering the
#     files present at preflight time would ingest each of them twice, since
#     bronze-ingest streams every file under the prefix itself.
#   - silver.transactions and silver.counterparty_edges, which silver-stream
#     recreates. Rows left by an earlier batch or continuous run would
#     otherwise be counted again next to the re-ingested corpus.
# A pod restart inside a run does not re-run the preflight, so it keeps its
# checkpoints and tables.
_REGISTER_MODE = env("LB_REGISTER_TABLE", "1")
REGISTER = _REGISTER_MODE == "1"
CONTINUOUS_RESET = _REGISTER_MODE == "schema"
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_EDGES = env("LB_FINANCIAL_SILVER_EDGES", "silver.counterparty_edges")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_ACCOUNTS = env("LB_FINANCIAL_SILVER_ACCOUNTS", "silver.accounts")
CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")


def _bronze_location():
    """Explicit S3 location for the bronze table on Hive, None on Polaris.

    Hive's ``default`` database already exists with the Stackable metastore's
    local warehouse (file:/stackable/warehouse), so a table created there
    without a location lands on the metastore pod's disk and every write fails
    with "Invalid S3 URI, cannot determine scheme: file:". Polaris places the
    table under its S3 warehouse. Same layout as the c360 bronze_ingest.
    """
    if os.getenv("LB_CATALOG_TYPE", "hive") == "polaris":
        return None
    ns, _, table = BRONZE_TABLE.rpartition(".")
    return f"{BRONZE_URI.rstrip('/')}/warehouse/{ns or 'default'}.db/{table}"


def _location_clause():
    loc = _bronze_location()
    return f"LOCATION '{loc}'" if loc else ""


def _with_location(writer):
    loc = _bronze_location()
    return writer.tableProperty("location", loc) if loc else writer


BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")

# add_files preflight thresholds (LB-110). At scale 100+ the pacs008/
# tree can be 700 GB and 700k+ files; add_files manifest generation
# scans every file and holds per-file state on the driver, which OOMs
# the default 4Gi bronze-verify executor. When either threshold is
# exceeded we deliberately skip add_files and go straight to CTAS with
# a loud warning (doubles S3 usage; caller can raise the thresholds or
# bump the bronze-verify executor sizing to keep zero-copy).
# Default 1.5 TiB / 800k files. Scale 100 pacs008/ is ~730 GB, well
# under the cap so scale-100 keeps the zero-copy register path -- the
# previous 500 GB default would have force-CTAS'd scale 100 and doubled
# S3 usage silently at every UAT run. Heuristic, not a validated safe
# cap: 4Gi bronze-verify has been proven OK at scale 100 (~700 GB /
# ~200k files), and untested above that. Callers going past scale 200
# should either raise `bronze-verify` executor memory OR set these
# env vars lower to force CTAS and know they're doubling S3 by choice.
ADD_FILES_MAX_BYTES = int(env("LB_BRONZE_ADD_FILES_MAX_BYTES", str(1536 * 1024**3)))
ADD_FILES_MAX_FILES = int(env("LB_BRONZE_ADD_FILES_MAX_FILES", "800000"))


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


def _table_location(spark, fq_table):
    """The table's storage location, or None if it does not exist."""
    try:
        for row in spark.sql(f"DESCRIBE TABLE EXTENDED {fq_table}").collect():
            if (row["col_name"] or "").strip() == "Location":
                return (row["data_type"] or "").strip() or None
    except Exception as e:  # noqa: BLE001
        log(f"Continuous reset: no location for {fq_table} ({e})")
    return None


def _norm(uri):
    return uri.replace("s3a://", "s3://").rstrip("/") + "/"


def _delete_dir_if_disjoint(spark, location, raw_uri):
    """Recursively delete ``location`` unless it overlaps ``raw_uri``."""
    loc, raw = _norm(location), _norm(raw_uri)
    if raw.startswith(loc) or loc.startswith(raw):
        log(f"Continuous reset: kept {location} (overlaps the raw datagen path)")
        return
    jvm = spark._jvm  # type: ignore[attr-defined]
    hconf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    target = location.replace("s3://", "s3a://", 1)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(target), hconf)
    path = jvm.org.apache.hadoop.fs.Path(target)
    if fs.exists(path):
        fs.delete(path, True)
        log(f"Continuous reset: deleted {location}")


def register_manifest(spark) -> bool:
    """(Re)register the typology manifest as an Iceberg table from its file.

    Returns False, with a warning, when the file is not there or the CTAS
    fails. Not fatal: score_financial reads the manifest file directly; the
    Trino recall/precision queries then fail loudly on the missing table.
    """
    manifest_ns = MANIFEST_TABLE.split(".", 1)[0] if "." in MANIFEST_TABLE else "bronze"
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{manifest_ns}")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {CATALOG}.{MANIFEST_TABLE}
            USING iceberg
            TBLPROPERTIES ('format-version' = '2')
            AS SELECT * FROM parquet.`{BRONZE_URI}{MANIFEST_PATH}`
        """)
        log(f"Registered manifest Iceberg table: {CATALOG}.{MANIFEST_TABLE}")
        return True
    except Exception as e:  # noqa: BLE001
        log(
            f"WARNING: manifest registration failed ({one_line(e)}). "
            "AML precision/recall/pattern_span/coverage queries will fail "
            "at benchmark time; alert-volume queries and score_financial "
            "(reads manifest via --manifest arg) are unaffected."
        )
        return False


def _continuous_reset(spark, df):
    """Drop the stream-written tables and create an empty bronze table."""
    # Bronze: plain DROP, never PURGE. After a batch run it may hold
    # add_files-registered datagen files, and PURGE would delete the corpus
    # the stream is about to read. The table's own directory (data files a
    # previous stream wrote) is removed separately, and only when it cannot
    # contain the raw datagen files; left behind, it inflated the measured
    # bronze size that continuous throughput is computed from.
    location = _table_location(spark, f"{CATALOG}.{BRONZE_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_TABLE}")
    if location:
        _delete_dir_if_disjoint(spark, location, BRONZE_URI + PACS_PREFIX)
    # Silver stream tables own their files, so PURGE them rather than leave
    # orphaned data in the silver bucket on every rerun. Entities and accounts
    # too: the continuous stream only appends dimension rows it has not seen,
    # so rows from an earlier run (another seed, scale or a pre-KYC corpus)
    # would otherwise survive the reset.
    # _drop_owned_table falls back to a plain DROP where Polaris refuses PURGE.
    for t in (SILVER_TXNS, SILVER_EDGES, SILVER_ENTITIES, SILVER_ACCOUNTS):
        _drop_owned_table(spark, t)
    _with_location(
        df.limit(0)
        .withColumn("ingest_ts", current_timestamp())
        .writeTo(f"{CATALOG}.{BRONZE_TABLE}")
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "snappy")
    ).create()
    log(
        f"Continuous reset: empty {CATALOG}.{BRONZE_TABLE} created; "
        f"dropped {SILVER_TXNS}, {SILVER_EDGES}, {SILVER_ENTITIES}, {SILVER_ACCOUNTS}"
    )
    # The previous run's manifest table must not outlive the reset: this
    # run's datagen writes a new schedule, and scoring against the old one
    # is silently wrong. Register now if the new file is already there;
    # gold-refresh registers it once it appears otherwise.
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{MANIFEST_TABLE}")
    register_manifest(spark)
    # The P10 operations tables are projections of the previous run's alerts
    # (cases, dispositions, the reconciliation ledger). gold-refresh rebuilds
    # them from its first tick, but until then a reader would see the old
    # run's queue as this run's.
    from tm_operations import TM_TABLES

    for t in TM_TABLES:
        _drop_owned_table(spark, t)
    log("Continuous reset: dropped the TM operations tables")


def _drop_owned_table(spark, table):
    """DROP ... PURGE a table whose files only it owns.

    Polaris refuses PURGE (403) unless DROP_WITH_PURGE_ENABLED is set, which
    the bootstrap does not do; fall back to a plain DROP and delete the
    table's own directory, as common.reset_stream_tables does for c360. Only
    a directory named after the table and disjoint from the raw datagen path:
    never a namespace or warehouse root that other tables share.
    """
    fq = f"{CATALOG}.{table}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fq} PURGE")
        return
    except Exception as e:  # noqa: BLE001
        log(f"Continuous reset: PURGE of {fq} refused ({one_line(e)}); plain DROP")
    loc = _table_location(spark, fq)
    spark.sql(f"DROP TABLE IF EXISTS {fq}")
    if loc and loc.rstrip("/").rsplit("/", 1)[-1] == table.rsplit(".", 1)[-1]:
        _delete_dir_if_disjoint(spark, loc, BRONZE_URI + PACS_PREFIX)
    elif loc:
        log(f"Continuous reset: kept {loc}; it is not named after {table}")


def _log_bronze_metrics(spark, source_bytes, row_count, elapsed):
    """JOB METRICS for the collector. Reuses the preflight byte count when
    the register path already listed the source tree."""
    if source_bytes is not None:
        size_gb = source_bytes / (1024**3)
    else:
        size_gb = path_size_gb(spark, BRONZE_URI + PACS_PREFIX)
    log_job_metrics(
        "bronze-verify",
        input_size_gb=size_gb,
        input_rows=row_count,
        output_rows=row_count,
        elapsed_seconds=elapsed,
    )


def main() -> None:
    spark = SparkSession.builder.appName("lb-bronze-verify-financial").getOrCreate()
    # Hive does not pre-create namespaces the way the Polaris bootstrap does.
    ensure_namespaces(spark, CATALOG, (BRONZE_TABLE,))
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
            "every date-range read and the per-day rollups would silently drop them."
        )
        spark.stop()
        raise SystemExit(3)

    partition_days = df.select("intr_bk_sttlm_dt").distinct().count()
    log(f"Partition days present: {partition_days}")

    source_bytes = None
    if CONTINUOUS_RESET:
        _continuous_reset(spark, df)
    elif REGISTER:
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
        #    (e.g. Nessie REST prior to a certain version) OR the source
        #    parquet exceeds ADD_FILES_MAX_BYTES / ADD_FILES_MAX_FILES
        #    (LB-110: add_files manifest generation OOMs a 4Gi executor
        #    at scale 100+), rewrite the data into the Iceberg table.
        #    Doubles S3 usage; operators can bump the thresholds or the
        #    executor sizing to keep zero-copy.
        force_ctas = False
        try:
            # Enumerate source files once. Uses Hadoop FS listing via the
            # Spark session; cheap at 100k files, and gives us honest bytes
            # + file count for the preflight decision.
            jvm = spark._jvm  # type: ignore[attr-defined]
            hconf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
            uri = jvm.java.net.URI(BRONZE_URI + PACS_PREFIX)
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)
            path = jvm.org.apache.hadoop.fs.Path(BRONZE_URI + PACS_PREFIX)
            it = fs.listFiles(path, True)  # recursive
            total_bytes = 0
            total_files = 0
            while it.hasNext():
                st = it.next()
                if st.isFile() and st.getPath().getName().endswith(".parquet"):
                    total_bytes += int(st.getLen())
                    total_files += 1
            source_bytes = total_bytes
            log(
                f"Source parquet: {total_files:,} files, "
                f"{total_bytes / 1024**3:.1f} GB "
                f"(add_files thresholds: {ADD_FILES_MAX_FILES:,} files, "
                f"{ADD_FILES_MAX_BYTES / 1024**3:.1f} GB)"
            )
            if total_bytes > ADD_FILES_MAX_BYTES or total_files > ADD_FILES_MAX_FILES:
                log(
                    "WARNING: source exceeds add_files preflight threshold; "
                    "skipping zero-copy register and using CTAS (doubles S3). "
                    "Raise LB_BRONZE_ADD_FILES_MAX_BYTES / "
                    "LB_BRONZE_ADD_FILES_MAX_FILES or the bronze-verify "
                    "executor sizing to keep zero-copy."
                )
                force_ctas = True
        except Exception as e:  # noqa: BLE001
            # Enumeration is a safety check, not a correctness gate. If it
            # fails (rare FS oddity), fall through and let add_files run;
            # its own exception handler still routes to CTAS.
            log(f"Preflight enumeration failed ({e}); proceeding to add_files.")

        if force_ctas:
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_TABLE}")
            spark.sql(f"""
                CREATE TABLE {CATALOG}.{BRONZE_TABLE}
                USING iceberg
                {_location_clause()}
                TBLPROPERTIES ('format-version' = '2')
                AS SELECT * FROM parquet.`{BRONZE_URI}{PACS_PREFIX}`
            """)
            log(f"Registered via CTAS (preflight): {CATALOG}.{BRONZE_TABLE}")
            elapsed = time.time() - start_time
            log("=" * 60)
            log(
                f"Bronze verification complete in {elapsed:.1f}s: rows={row_count:,} "
                f"cols={col_count} days={partition_days}"
            )
            log("=" * 60)
            _log_bronze_metrics(spark, source_bytes, row_count, elapsed)
            spark.stop()
            return

        try:
            # Full inferred schema for the CREATE, so add_files finds
            # every column of the parquet source under its real name.
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_TABLE}")
            src = spark.read.parquet(BRONZE_URI + PACS_PREFIX)
            # Every register path (add_files, and both CTAS fallbacks below)
            # produces the same unpartitioned layout, so benchmark numbers do
            # not change with the path taken.
            #
            # OWNERSHIP HAZARD: add_files makes Iceberg treat the datagen files
            # as its own. A DELETE / rewrite_data_files / compaction on this
            # table followed by expire_snapshots, or DROP TABLE ... PURGE,
            # physically deletes the raw datagen corpus. Keep bronze out of
            # compaction; destroy is the only place allowed to do this.
            #
            # The target is UNPARTITIONED on purpose. add_files can only map
            # source files onto identity partitions read from Hive-style
            # directories; against a days(intr_bk_sttlm_dt) spec it fails
            # with "Invalid partition transformation", and the old code then
            # fell back to CTAS on every run, doubling bronze storage and
            # spending most of bronze-verify rewriting it (measured scale 1:
            # 8.4 GB source became 13.9 GB, 245 of 273 s). Datagen writes a
            # flat, date-clustered file layout, so Iceberg's per-file min/max
            # on intr_bk_sttlm_dt still prunes date filters at file level,
            # and silver-build scans the whole table regardless.
            _with_location(
                src.limit(0)
                .writeTo(f"{CATALOG}.{BRONZE_TABLE}")
                .using("iceberg")
                .tableProperty("format-version", "2")
                .tableProperty("write.parquet.compression-codec", "snappy")
            ).create()
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
                {_location_clause()}
                TBLPROPERTIES ('format-version' = '2')
                AS SELECT * FROM parquet.`{BRONZE_URI}{PACS_PREFIX}`
            """)
            log(f"Registered via CTAS fallback: {CATALOG}.{BRONZE_TABLE}")

        # Manifest registration (LB-089 round 2). One file, small; CTAS
        # unconditionally. Failure of the pacs.008 registration above
        # would have already raised, so if we're here the catalog and
        # the SparkSession are known good. Manifest failure is however
        # not fatal to bronze itself -- we log and continue so batch
        # can still emit alerts; the benchmark scoring queries will
        # then fail loudly at their own `bronze.manifest` reads.
        register_manifest(spark)

    elapsed = time.time() - start_time
    log("=" * 60)
    log(
        f"Bronze verification complete in {elapsed:.1f}s: rows={row_count:,} "
        f"cols={col_count} days={partition_days}"
    )
    log("=" * 60)
    _log_bronze_metrics(spark, source_bytes, row_count, elapsed)

    spark.stop()


if __name__ == "__main__":
    main()
