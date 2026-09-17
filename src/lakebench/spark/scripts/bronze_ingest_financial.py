"""Bronze Ingest (Financial, sustained) -- Structured Streaming pacs.008 -> bronze.

Reads new Parquet files from LB_BRONZE_URI/pacs008/ as they land and appends
to the bronze Iceberg table. maxFilesPerTrigger throttles per micro-batch to
smooth downstream silver_stream load.
"""

from __future__ import annotations

from common import env, log
from pyspark.sql import SparkSession

BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
PACS_PREFIX = env("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/")
CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
CHECKPOINT_URI = env(
    "LB_FINANCIAL_BRONZE_CHECKPOINT", "s3a://lb-bronze/_checkpoints/bronze_ingest_financial/"
)
MAX_FILES = int(env("LB_FINANCIAL_BRONZE_MAX_FILES", "8"))
TRIGGER_S = int(env("LB_FINANCIAL_BRONZE_TRIGGER_S", "10"))


def main() -> None:
    spark = SparkSession.builder.appName("lb-bronze-ingest-financial").getOrCreate()

    log("=" * 60)
    log("Bronze Ingest (Financial, streaming)")
    log(f"Source: {BRONZE_URI}{PACS_PREFIX}")
    log(f"Target: {CATALOG}.{BRONZE_TABLE}")
    log(f"maxFilesPerTrigger={MAX_FILES} triggerSeconds={TRIGGER_S}")
    log("=" * 60)

    df = (
        spark.readStream.format("parquet")
        .option("maxFilesPerTrigger", MAX_FILES)
        .load(BRONZE_URI + PACS_PREFIX)
    )

    query = (
        df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_URI)
        .trigger(processingTime=f"{TRIGGER_S} seconds")
        .toTable(f"{CATALOG}.{BRONZE_TABLE}")
    )

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
