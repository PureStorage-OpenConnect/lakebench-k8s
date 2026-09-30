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

from common import env, log, stream_batch_lines
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType

BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
# LB-089: honor LB_FINANCIAL_BRONZE_PREFIX with the same semantics as
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
TRIGGER_S = int(env("LB_FINANCIAL_BRONZE_TRIGGER_S", "10"))


def _log_new_progress(query, logged_batch: int) -> int:
    """Log each micro-batch once, in the line format the metrics collector
    parses (LB-136: without these the continuous scorecard read zero rows
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
        for line in stream_batch_lines(
            batch_id,
            rows,
            (p.get("durationMs") or {}).get("triggerExecution", 0) / 1000.0,
            f"{CATALOG}.{BRONZE_TABLE}",
        ):
            log(line)
    return logged_batch


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

    # The datagen files carry no ingest_ts; the stream stamps it per
    # micro-batch (current_timestamp is fixed per batch). It is the start of
    # the continuous freshness clock that gold_refresh reports.
    source_schema = StructType([f for f in target_schema.fields if f.name != "ingest_ts"])
    df = (
        spark.readStream.format("parquet")
        .schema(source_schema)
        .option("maxFilesPerTrigger", MAX_FILES)
        .load(BRONZE_URI + PACS_PREFIX)
        .withColumn("ingest_ts", current_timestamp())
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
    # with zero rows -- the exact LB-044 shape this file is meant to
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
