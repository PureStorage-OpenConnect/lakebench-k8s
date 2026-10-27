"""
Adaptive Silver Build (Delta Lake) - Scale-aware transformation pipeline

Delta Lake variant of silver_build.py. Uses Delta write APIs instead of
Iceberg's DataFrameWriterV2.

Selects the processing strategy from the bronze size:
- SIMPLE: < 100GB, standard processing
- STREAMING: >= 100GB, single pass, rows clustered by day before the write
"""

from __future__ import annotations

import os
import re
import time

from common import (
    SilverAbort,
    _is_unity_catalog,
    _s3_table_path,
    apply_silver_dynamic_config,
    apply_silver_transformations_anchored,
    c360_bronze_path,
    c360_bronze_run_path,
    cluster_by_partition,
    delta_batch_txn_options,
    determine_silver_strategy,
    env,
    files_added_by_last_commit,
    finish_silver_build,
    log,
    refuse_orphan_delta_log,
    refuse_unforced_rebuild,
    resolve_data_clock,
    set_utc_session,
    silver_input,
    table_exists,
    write_delta_table,
)
from pyspark.sql import SparkSession

# Rows counted during the build.
_COUNTED: dict[str, int] = {}

_LOGLESS = ("DELTA_TABLE_NOT_FOUND", "DELTA_PATH_DOES_NOT_EXIST")


def drop_logless_entry(spark, silver_tbl):
    """Drop a catalog entry whose Delta table has no files left; True if dropped.

    ``lakebench clean silver`` empties the silver bucket and keeps the
    catalog, so the next build met an entry at a location with no Delta log
    and every read of it failed (DELTA_TABLE_NOT_FOUND), forced or not. Such
    an entry holds no rows, so it is dropped and the build starts the table
    afresh. An entry whose location still holds any file is refused instead:
    those files are not a table Lakebench can read, and they are not deleted.
    Any other error from the check is raised. The table is managed, so the
    metastore deletes its (empty) location on DROP; a writer that added files
    between the listing and the DROP would lose them, which only another job
    of this deployment could do.
    """
    try:
        spark.table(silver_tbl).schema  # noqa: B018 -- forces resolution
        return False
    except Exception as e:  # noqa: BLE001
        if not any(code in str(e) for code in _LOGLESS):
            return False  # not found, or another error table_exists reports
    db, name = silver_tbl.split(".")[-2:]
    jvm = spark._jvm
    ident = jvm.org.apache.spark.sql.catalyst.TableIdentifier(name, jvm.scala.Some(db))
    location = str(spark._jsparkSession.sessionState().catalog().getTableMetadata(ident).location())
    path = jvm.org.apache.hadoop.fs.Path(location)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(path) and fs.listFiles(path, True).hasNext():
        raise SilverAbort(
            f"silver-build: {silver_tbl} has no Delta log but {location} still holds files; "
            "refusing to rebuild over them. Remove those files if their data may go (or "
            "empty the silver bucket with `lakebench clean silver`)."
        )
    log(
        f"{silver_tbl}: no files left at {location} (the bucket was emptied, for example "
        "by `lakebench clean silver`); dropping the catalog entry and building it afresh"
    )
    spark.sql(f"DROP TABLE IF EXISTS {silver_tbl}")
    return True


def _delta_write_props() -> dict[str, str]:
    """Common Delta table properties for silver writes."""
    return {
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    }


def rows_added_by_last_commit(spark, silver_tbl):
    """Rows the table's latest Delta commit wrote (this cycle's write).

    Commit metadata only. Returns None when the metric is missing, never
    the table's row count: that is cumulative and would hide a cycle that
    wrote nothing. Callers refuse to publish None as ``output_rows``.
    """
    try:
        r = spark.sql(f"DESCRIBE HISTORY {silver_tbl} LIMIT 1").collect()
        n = (r[0]["operationMetrics"] or {}).get("numOutputRows") if r else None
        if n is not None:
            return int(n)
    except Exception as e:  # noqa: BLE001
        log(f"Warning: commit metrics unavailable ({e}); output_rows will be unknown")
    return None


_TXN_APP = "lb-silver-build"
_TXN_APP_ID = re.compile(r"^lb-silver-build-rebuild-(\d+)$")


def committed_epochs(spark, silver_tbl):
    """{rebuild epoch: last committed cycle} from the table's Delta log.

    Reads the SetTransaction entries Delta keeps for every txnAppId that has
    written to the table (they survive overwrites, and nothing sets
    delta.setTransactionRetentionDuration, so a table holds the keys of every
    epoch that ever wrote to it; a key that did expire would not be skipped
    by Delta either). Only this build's app ids
    (``delta_batch_txn_options``) are returned. Any failure to read them
    raises: without them a write cannot be shown not to be skipped.
    """
    try:
        location = spark.sql(f"DESCRIBE DETAIL {silver_tbl}").collect()[0]["location"]
        jvm = spark._jvm
        snapshot = jvm.org.apache.spark.sql.delta.DeltaLog.forTableWithSnapshot(
            spark._jsparkSession, location
        )._2()
        it = snapshot.transactions().iterator()
        txns = {}
        while it.hasNext():
            kv = it.next()
            txns[str(kv._1())] = int(kv._2())
    except Exception as e:  # noqa: BLE001
        raise SilverAbort(
            f"silver-build: cannot read the Delta transaction ids of {silver_tbl} ({e}); "
            "refusing to write, because a write whose id the log already holds is "
            "skipped without error"
        ) from e
    out = {}
    for app_id, version in txns.items():
        m = _TXN_APP_ID.match(app_id)
        if m:
            out[int(m.group(1))] = version
    return out


def resolve_txn_epoch(committed, configured, appending, cycle):
    """The rebuild epoch this cycle's Delta txnAppId uses.

    The configured epoch (``LB_REBUILD_EPOCH``, from the
    ``lakebench-silver-state`` ConfigMap) does not live as long as the table:
    when it reads lower than an epoch the table's log already holds, Delta
    skips this run's cycles as already committed. So the table decides:

    - a full build (cycle 0, a --force-rebuild, or a later cycle that found
      no table) takes an epoch above every epoch in the log, so its key is
      new by construction and it starts the newest epoch;
    - an append continues the newest epoch in the log, which is the one this
      run's full build started (a full build over a log the catalog does not
      know is refused, so it never resumes an old epoch), whatever the
      configured epoch reads. Its own
      cycle already committed there is an operator retry, which Delta skips
      as intended; a later cycle already committed there cannot be this
      run's (a manual re-run of an earlier cycle) and is refused.

    ``committed`` maps epoch to last committed cycle (``committed_epochs``).
    The configured epoch is used as is only by a table with no such keys.
    """
    newest = max(committed) if committed else None
    if not appending:
        return configured if newest is None else max(configured, newest + 1)
    epoch = configured if newest is None else newest
    last = committed.get(epoch)
    if last is not None and last > cycle:
        raise SilverAbort(
            f"silver-build: epoch {epoch} of this table already committed cycle {last}, "
            f"after this cycle ({cycle}); Delta would skip this write. Rebuild silver "
            "with --force-rebuild"
        )
    return epoch


def cluster_silver(spark, silver_df):
    """Cluster silver rows by interaction_date before the Delta write.

    Without it every write task put a file into almost every day's partition
    (tasks x days files). spark.lb.silver.distribution_mode=none skips it,
    matching the Iceberg silver override.
    """
    dist_mode = spark.conf.get("spark.lb.silver.distribution_mode", "hash")
    log(f"Silver write distribution: {dist_mode} (clustered by interaction_date unless none)")
    return cluster_by_partition(spark, silver_df, ["interaction_date"], dist_mode)


def log_silver_files(spark, silver_tbl):
    """Log how many data files this build's commit wrote."""
    n = files_added_by_last_commit(spark, silver_tbl)
    if n is not None:
        log(f"Silver data files written by this commit: {n:,}")


def write_silver(spark, silver_df, silver_tbl, appending, exists, cycle, rebuild_epoch):
    """Append *silver_df* as cycle *cycle*, or replace the table with it.

    Every write carries a (txnAppId, txnVersion) from
    ``delta_batch_txn_options``, so Delta skips a resubmission of the same
    (rebuild_epoch, cycle) at the transaction log. ``rebuild_epoch`` comes
    from ``resolve_txn_epoch``: a full build's key is one the log has never
    held, so only an append can be skipped, and only as a retry. The full
    build overwrites the table when it *exists* and creates it otherwise.
    """
    silver_bucket = env("LB_SILVER_URI", "s3a://lb-silver/")
    if appending:
        log(f"Appending to existing table (incremental mode, cycle={cycle})")
        txn_opts = delta_batch_txn_options(_TXN_APP, rebuild_epoch, cycle)
        write_delta_table(
            spark, silver_df, silver_tbl, silver_bucket, mode="append", options=txn_opts
        )
        return
    opts = {"overwriteSchema": "true", "compression": "snappy"}
    opts.update(_delta_write_props())
    opts.update(delta_batch_txn_options(_TXN_APP, rebuild_epoch, cycle))
    write_delta_table(
        spark,
        silver_df,
        silver_tbl,
        silver_bucket,
        mode="overwrite" if exists else "append",
        partition_cols=["interaction_date"],
        options=opts,
    )


def silver_simple(spark, source, silver_tbl, exists, appending=False, cycle=0, rebuild_epoch=0):
    """SIMPLE strategy: one pass over bronze. For < 100GB."""
    log("Executing SIMPLE strategy...")

    df_bronze = spark.read.parquet(source)
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")
    _COUNTED["bronze_rows"] = bronze_count
    # strict: the job always exports LB_DATA_CLOCK, so a missing one is a
    # configuration error.
    anchor = resolve_data_clock(df_bronze, strict=True)

    silver_df = apply_silver_transformations_anchored(df_bronze, anchor)
    silver_count = silver_df.count()
    silver_df = cluster_silver(spark, silver_df)

    log(f"Writing {silver_count:,} records to {silver_tbl}")
    write_silver(spark, silver_df, silver_tbl, appending, exists, cycle, rebuild_epoch)
    log_silver_files(spark, silver_tbl)
    return silver_count


def silver_streaming(
    spark, source, silver_tbl, exists, input_size_gb, appending=False, cycle=0, rebuild_epoch=0
):
    """STREAMING strategy: single pass, no intermediate counts. For >= 100GB.

    - No repartition(N) and no intermediate count(): one pass over bronze
    - One shuffle, REBALANCE by interaction_date (cluster_silver), the
      Delta counterpart of Iceberg silver's distribution-mode=hash: without
      it each task writes a file into every day it holds (tasks x days
      files). spark.lb.silver.distribution_mode=none restores the direct
      write.
    """
    log("Executing STREAMING strategy (single pass)...")
    log(f"Input size: {input_size_gb:.1f} GB")

    df_bronze = spark.read.parquet(source)

    # Parquet count() reads row-group footers only, so bronze_rows costs no
    # data scan.
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")
    _COUNTED["bronze_rows"] = bronze_count

    anchor = resolve_data_clock(df_bronze, strict=True)
    silver_df = apply_silver_transformations_anchored(df_bronze, anchor)
    silver_df = cluster_silver(spark, silver_df)

    log(f"Writing to {silver_tbl} (single pass, no intermediate counts)...")
    write_silver(spark, silver_df, silver_tbl, appending, exists, cycle, rebuild_epoch)

    silver_count = rows_added_by_last_commit(spark, silver_tbl)
    log(
        f"Wrote {silver_count:,} records"
        if silver_count is not None
        else "Wrote unknown records (commit metrics unavailable)"
    )
    log_silver_files(spark, silver_tbl)

    return silver_count


# ============================================================
# MAIN EXECUTION
# ============================================================

catalog = env("LB_ICEBERG_CATALOG", "ice")
bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")
silver_uri = env("LB_SILVER_URI", "s3a://lb-silver/")

log("=" * 60)
log("Customer 360 Silver Build (Delta) - Adaptive Transformation Pipeline")
log("=" * 60)

spark = SparkSession.builder.appName("lb-silver-build-delta").getOrCreate()
set_utc_session(spark)

log("Creating Delta schema...")
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    log(f"Created schema {catalog}.silver")
except Exception as e:
    log(f"Schema creation note: {str(e)}")

start_time = time.time()

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
log(f"Target table: {silver_tbl}")

# Cycles 2+ of a multi-cycle run append to the table (LB_SILVER_INCREMENTAL).
incremental_mode = os.environ.get("LB_SILVER_INCREMENTAL", "false").lower() == "true"
if incremental_mode:
    log("INCREMENTAL MODE: will append to existing table")

# The cycle (0-based) and the configured rebuild epoch (bumped by
# --force-rebuild through the lakebench-silver-state ConfigMap) key each
# write's Delta transaction id. The epoch written is resolve_txn_epoch's,
# checked against the table's log below.
_cycle = int(os.environ.get("LB_BRONZE_CYCLE", "0"))
_rebuild_epoch = int(os.environ.get("LB_REBUILD_EPOCH", "0"))

if not _is_unity_catalog():
    drop_logless_entry(spark, silver_tbl)

_exists = table_exists(spark, silver_tbl)
appending = incremental_mode and _exists
# Later cycles of a multi-cycle run append only their own bronze files. A
# full build reads this run's files: every file in a single-cycle run, else
# cycle 0's and cycles 1..k's (a later cycle that found no table), as
# bronze-verify counts them, not files of later cycles an earlier run with
# more cycles left under the prefix. Profile, size and read the same path.
bronze_source = (
    c360_bronze_path(bronze_uri, True) if appending else c360_bronze_run_path(bronze_uri)
)
log(f"Bronze source: {bronze_source}")

if not appending and _exists:
    refuse_unforced_rebuild(spark, silver_tbl)

# The ConfigMap epoch can read lower than one this table's log already holds
# (the ConfigMap was lost or recreated while the table survived, or job.py's
# read fell back to 0), and Delta would then skip this run's cycles as
# committed: silver short, exit 0. The table's own log decides the epoch.
if _is_unity_catalog() and not _exists:
    # write_delta_table checks for a surviving log only on the Hive path;
    # on Unity a build would append under a key the old log may hold.
    refuse_orphan_delta_log(
        spark, silver_tbl, _s3_table_path(silver_uri, silver_tbl.split(".", 1)[-1])
    )
_committed = committed_epochs(spark, silver_tbl) if _exists else {}
_txn_epoch = resolve_txn_epoch(_committed, _rebuild_epoch, appending, _cycle)
log(
    f"Rebuild epoch: {_txn_epoch} (configured {_rebuild_epoch}; "
    f"epochs in the table log, with last cycle: {_committed or 'none'})"
)
if appending and _committed.get(_txn_epoch) == _cycle:
    log(
        f"Cycle {_cycle} is already committed under epoch {_txn_epoch}: "
        "a retry of this cycle, which Delta skips"
    )

input_size_gb, skew_factor, input_size_gb_source = silver_input(spark, bronze_source)
strategy = determine_silver_strategy(spark, input_size_gb)
apply_silver_dynamic_config(spark, input_size_gb, skew_factor)

if strategy == "simple":
    silver_count = silver_simple(
        spark,
        bronze_source,
        silver_tbl,
        _exists,
        appending=appending,
        cycle=_cycle,
        rebuild_epoch=_txn_epoch,
    )
else:
    silver_count = silver_streaming(
        spark,
        bronze_source,
        silver_tbl,
        _exists,
        input_size_gb,
        appending=appending,
        cycle=_cycle,
        rebuild_epoch=_txn_epoch,
    )

total_time = time.time() - start_time

finish_silver_build(
    spark,
    title="Customer 360 Silver Build (Delta)",
    strategy=strategy,
    silver_tbl=silver_tbl,
    silver_count=silver_count,
    bronze_rows=_COUNTED.get("bronze_rows", "unknown"),
    input_size_gb=input_size_gb,
    input_size_gb_source=input_size_gb_source,
    total_time=total_time,
    unknown_reason="commit metrics unavailable",
)
