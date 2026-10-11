"""Bronze Ingest (Financial, sustained) -- Structured Streaming pacs.008 -> bronze.

Reads new Parquet files from LB_BRONZE_URI/<prefix> as they land and appends
to the bronze Iceberg table. maxFilesPerTrigger throttles per micro-batch to
smooth downstream silver_stream load.

Prerequisites:
- bronze_verify_financial must have run first with LB_REGISTER_TABLE=schema
  (the continuous preflight) to create the empty target Iceberg table
  (schema inferred from the first parquet).
  Structured Streaming's parquet source requires an explicit schema or a
  pre-existing table to sink into; we take the second route.
- SIGTERM triggers a graceful stopQuery so an in-flight micro-batch commits
  (or aborts atomically) before the pod exits, avoiding an inconsistent
  checkpoint state.
"""

from __future__ import annotations

import signal
import sys
import time
from datetime import datetime

from common import arrival_time, env, log, log_landing, stream_batch_lines
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
# Honor LB_FINANCIAL_BRONZE_PREFIX with the same semantics as
# bronze_verify_financial: the env var is the ROOT prefix datagen_rs was
# invoked with (mirrored from path_template by job.py). Transactions
# land under {root}/bronze/pacs008/. Pre-PR-F this file used the env
# var as the *inner* path with default "bronze/pacs008/", which broke
# whenever job.py set the env var to the outer prefix -- the same class
# of drift that bit the batch path.
BRONZE_ROOT_PREFIX = env("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/")
PACS_PREFIX = env(
    "LB_FINANCIAL_PACS_PATH",
    BRONZE_ROOT_PREFIX.rstrip("/") + "/bronze/pacs008/",
)
CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
CHECKPOINT_URI = env(
    "LB_FINANCIAL_BRONZE_CHECKPOINT", "s3a://lb-bronze/_checkpoints/bronze_ingest_financial/"
)
MAX_FILES = int(env("LB_FINANCIAL_BRONZE_MAX_FILES", "8"))
#: How long to wait for datagen's first file before the schema check.
FIRST_FILE_WAIT_S = 1800
TRIGGER_S = int(env("LB_FINANCIAL_BRONZE_TRIGGER_S", "0"))


def _fields(schema) -> dict:
    """Field name to type (as JSON), nullability left out."""
    return {f.name: f.dataType.json() for f in schema.fields}


def check_first_file(spark) -> None:
    """Wait for datagen's first file and exit 2 when its schema is not the
    shipped pacs008_schema.json the continuous preflight created bronze
    from: a column the generator renamed or retyped would otherwise read as
    null in every row, silently."""
    from bronze_verify_financial import pacs008_schema

    jvm = spark._jvm  # type: ignore[attr-defined]
    hconf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    pattern = jvm.org.apache.hadoop.fs.Path(f"{BRONZE_URI}{PACS_PREFIX}part-*.parquet")
    fs = pattern.getFileSystem(hconf)
    deadline = time.time() + FIRST_FILE_WAIT_S
    while True:
        found = fs.globStatus(pattern) or []
        if found:
            break
        if time.time() >= deadline:
            log(
                f"ERROR: no datagen file under {BRONZE_URI}{PACS_PREFIX} after {FIRST_FILE_WAIT_S}s"
            )
            sys.exit(2)
        time.sleep(5)
    first = min(found, key=lambda s: s.getPath().toString()).getPath().toString()
    want, got = _fields(pacs008_schema()), _fields(spark.read.parquet(first).schema)
    if want != got:
        diff = sorted(k for k in set(want) | set(got) if want.get(k) != got.get(k))
        log(f"ERROR: {first} does not match pacs008_schema.json; columns differ: {diff}")
        sys.exit(2)
    log(f"Schema check: {first} matches pacs008_schema.json ({len(want)} columns)")


def _log_new_progress(query, logged_batch: int) -> int:
    """Log each micro-batch once, in the line format the metrics collector
    parses (without these the continuous scorecard read zero rows
    ingested for a healthy AML stream). Returns the last batch id logged."""
    for p in query.recentProgress:
        rows = int(p.get("numInputRows") or 0)
        # Idle triggers report the NEXT batch id with zero rows. Advancing
        # past them would skip the real batch that later reuses that id.
        if rows <= 0:
            continue
        batch_id = int(p.get("batchId", -1))
        if batch_id <= logged_batch:
            continue
        logged_batch = batch_id
        seconds = (p.get("durationMs") or {}).get("triggerExecution", 0) / 1000.0
        for line in stream_batch_lines(batch_id, rows, seconds, f"{CATALOG}.{BRONZE_TABLE}"):
            log(line)
        # The observed row: a Row (PySpark 4), or a dict or list from JSON.
        landed = (p.get("observedMetrics") or {}).get("landing")
        if isinstance(landed, dict):
            landed = (landed.get("lo"), landed.get("hi"))
        lo, hi = tuple(landed) if landed is not None and len(landed) == 2 else (None, None)
        if lo is not None and hi is not None:
            log_landing(batch_id, lo / 1e6, hi / 1e6, _started(p) + seconds)
    return logged_batch


def _started(progress) -> float:
    """A progress entry's trigger start, in epoch seconds (now if absent)."""
    stamp = progress.get("timestamp")
    try:
        return datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return time.time()


def main() -> None:
    spark = SparkSession.builder.appName("lb-bronze-ingest-financial").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    log("=" * 60)
    log("Bronze Ingest (Financial, streaming)")
    log(f"Source: {BRONZE_URI}{PACS_PREFIX}")
    log(f"Target: {CATALOG}.{BRONZE_TABLE}")
    log(f"Checkpoint: {CHECKPOINT_URI}")
    log(f"maxFilesPerTrigger={MAX_FILES} triggerSeconds={TRIGGER_S}")
    log("=" * 60)

    # Require the target table to exist so we have a schema to attach to the
    # readStream. Reading against `.format("parquet")` alone requires either
    # spark.sql.streaming.schemaInference=true or an explicit .schema(...),
    # which the previous implementation had neither of -- streaming source
    # failed immediately with "Schema must be specified when creating a
    # streaming source" and the pod restarted on backoffLimit until the pod
    # was declared failed.
    try:
        target_schema = spark.table(f"{CATALOG}.{BRONZE_TABLE}").schema
    except Exception as e:  # noqa: BLE001
        log(
            f"ERROR: {CATALOG}.{BRONZE_TABLE} does not exist; run "
            f"bronze_verify_financial with LB_REGISTER_TABLE=1 or =schema first. ({e})"
        )
        sys.exit(2)

    # The datagen files carry no ingest_ts; the stream stamps each row with
    # its arrival (its file's landing time, see arrival_time), the start of
    # the end-to-end freshness clock that gold_refresh reports.
    source_schema = StructType([f for f in target_schema.fields if f.name != "ingest_ts"])
    arrived = arrival_time(spark, BRONZE_URI, CHECKPOINT_URI)
    check_first_file(spark)
    reader = spark.readStream.format("parquet").schema(source_schema)
    # 0: no per-trigger limit, so each micro-batch takes every landed file.
    if MAX_FILES > 0:
        reader = reader.option("maxFilesPerTrigger", MAX_FILES)
    df = reader.load(BRONZE_URI + PACS_PREFIX).withColumn("ingest_ts", arrived)
    # Each micro-batch's landing span, read back from its progress
    # (observedMetrics) for the landing-to-bronze lag.
    df = df.observe(
        "landing",
        F.unix_micros(F.min("ingest_ts")).alias("lo"),
        F.unix_micros(F.max("ingest_ts")).alias("hi"),
    )

    query = (
        df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_URI)
        .trigger(processingTime=f"{TRIGGER_S} seconds")
        .toTable(f"{CATALOG}.{BRONZE_TABLE}")
    )

    # SIGTERM/SIGINT: stop the streaming query cleanly, which lets the
    # current micro-batch commit or abort atomically. Structured Streaming's
    # StreamingQuery.stop() blocks until the in-flight batch finishes, so
    # the checkpoint isn't left in a half-committed state.
    def _shutdown_handler(signum, frame):  # noqa: ARG001
        log(f"Signal {signum} received; stopping stream cleanly")
        try:
            query.stop()
        except Exception as e:  # noqa: BLE001
            log(f"query.stop failed (already stopped?): {e}")

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _shutdown_handler)
        except ValueError:
            pass

    # Poll instead of awaitTermination(): awaitTermination raises on any
    # streaming exception and doesn't wake for signal.SIG* under some
    # Python versions. Polling every second checks for graceful stop
    # from the signal handler AND surfaces streaming exceptions promptly.
    logged_batch = -1
    while query.isActive:
        time.sleep(1)
        logged_batch = _log_new_progress(query, logged_batch)
    _log_new_progress(query, logged_batch)

    # Re-raise any streaming exception so a jar-download stall / schema
    # mismatch / S3 auth failure surfaces as pod exit != 0. Without this,
    # the query dies but the pod exits 0 and sustained mode reports PASS
    # with zero rows -- the exit-0-with-no-data shape this file is meant to
    # avoid.
    exc = query.exception()
    if exc is not None:
        log(f"Streaming query failed: {exc}")
        spark.stop()
        raise exc

    log("Stream stopped; last batch progress:")
    if query.lastProgress:
        log(f"  batchId: {query.lastProgress.get('batchId')}")
        log(f"  inputRowsPerSecond: {query.lastProgress.get('inputRowsPerSecond')}")
        log(f"  processedRowsPerSecond: {query.lastProgress.get('processedRowsPerSecond')}")
    spark.stop()


if __name__ == "__main__":
    main()
