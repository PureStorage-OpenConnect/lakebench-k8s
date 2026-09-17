"""Score (Financial) -- compute recall from manifest + gold.alerts.

Reads the datagen manifest sidecar (typology ground truth) and the
detection alerts written by workloads W2/W3/W4/etc. Joins on
related_txn_ids and computes recall per typology_type. Writes
recall.parquet under the run's output prefix.
"""

from __future__ import annotations

import argparse

from common import env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array_intersect,
    col,
    explode,
    lit,
    size,
    when,
)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute recall from manifest + alerts")
    parser.add_argument("--manifest", required=True, help="S3 URI to manifest.parquet")
    parser.add_argument("--output", required=True, help="S3 URI for recall.parquet")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("lb-score-financial").getOrCreate()
    log("=" * 60)
    log("Financial recall scoring")
    log(f"Manifest: {args.manifest}")
    log(f"Output:   {args.output}")
    log("=" * 60)

    manifest = spark.read.parquet(args.manifest)
    alerts = spark.table(f"{CATALOG}.{GOLD_ALERTS}")

    # For each typology instance, count how many participant_uetrs appear in
    # any alert's related_txn_ids. detected=1 if any intersection is non-empty.
    per_instance = (
        manifest.crossJoin(alerts.select("related_txn_ids"))
        .withColumn(
            "hit",
            when(
                size(array_intersect(col("participant_uetrs"), col("related_txn_ids"))) > 0, 1
            ).otherwise(0),
        )
        .groupBy("typology_id", "typology_type", "expected_workload")
        .agg({"hit": "max"})
        .withColumnRenamed("max(hit)", "detected")
    )

    per_typology = (
        per_instance.groupBy("typology_type", "expected_workload")
        .agg({"detected": "avg", "typology_id": "count"})
        .withColumnRenamed("avg(detected)", "recall")
        .withColumnRenamed("count(typology_id)", "instance_count")
    )

    per_typology = per_typology.withColumn("computed_by", lit("lb-score-financial"))
    per_typology.write.mode("overwrite").parquet(args.output)
    log(f"Wrote recall.parquet: {per_typology.count()} typology rows")
    spark.stop()


if __name__ == "__main__":
    main()


# Guard against ruff unused-import warnings for symbols imported for clarity.
_ = (explode,)
