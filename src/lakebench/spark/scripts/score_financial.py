"""Score (Financial) -- compute recall + false-positive rate from
manifest + gold.alerts.

Reads the datagen manifest sidecar (typology ground truth) and the
detection alerts written by workloads W2/W3/W4/etc. Joins on
related_txn_ids and computes recall + FP rate per typology_type. Writes
recall.parquet under the run's output prefix.

Recall definition: fraction of typology instances for which at least
one participant_uetr appears in some alert's related_txn_ids.

Rewritten 2026-09-19 to avoid a crossJoin between manifest and alerts.
The original design paired every typology instance with every alert row
(N x M) then filtered by array intersection -- at scale 10, that's
~1k * ~10k = 10M shuffle pairs before the filter fires, quadratic in
alert volume. New design explodes each side once by uetr and joins on
the shared column, which is linear in the total UETR footprint and
executes at scale-100 without a shuffle spill.
"""

from __future__ import annotations

import argparse

from common import env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    explode,
    explode_outer,
    lit,
    when,
)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute recall + FP rate from manifest + alerts")
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
    manifest_count = manifest.count()
    if manifest_count == 0:
        raise SystemExit(
            f"Manifest at {args.manifest} is empty -- either the datagen run did not "
            "write it, or the S3 URI is wrong. Cannot compute recall without ground truth."
        )
    log(f"Manifest typology instances: {manifest_count:,}")

    try:
        alerts = spark.table(f"{CATALOG}.{GOLD_ALERTS}")
        total_alerts = alerts.count()
    except Exception as e:  # noqa: BLE001
        raise SystemExit(
            f"Cannot read {CATALOG}.{GOLD_ALERTS}: {e}. gold_finalize_financial "
            "must run before scoring."
        ) from e
    log(f"gold.alerts rows: {total_alerts:,}")

    # Explode manifest to (typology_id, typology_type, expected_workload, uetr) pairs.
    # explode_outer, not explode: a manifest row with a NULL/empty participant_uetrs
    # array must still contribute to the denominator with detected=0. explode
    # would silently drop the whole instance, inflating recall by removing
    # the "we never fired for this instance" data points.
    manifest_uetrs = (
        manifest.select(
            "typology_id",
            "typology_type",
            "expected_workload",
            explode_outer(col("participant_uetrs")).alias("uetr"),
        )
        .distinct()
    )

    # Explode alerts to (alert_id, uetr) pairs. On empty alerts, this frame is
    # empty and the left-join below leaves detected=0 everywhere -- the correct
    # answer, which matters because the pipeline runs early UAT with empty
    # alerts before rules are wired.
    alert_uetrs = alerts.select(
        col("alert_id"),
        explode(col("related_txn_ids")).alias("uetr"),
    ).distinct() if total_alerts > 0 else spark.createDataFrame(
        [], "alert_id STRING, uetr STRING"
    )

    # A typology instance is "detected" if any of its uetrs appears in any
    # alert -- so for each (typology_id, uetr), left-join and mark hit=1 if
    # the alert row is present. Then per typology_id: detected = max(hit).
    # coalesce(hit, 0) covers instances whose participant_uetrs was NULL
    # (explode_outer emits NULL uetr; the join leaves alert_id NULL too).
    per_instance = (
        manifest_uetrs.join(alert_uetrs, on="uetr", how="left")
        .withColumn(
            "hit",
            coalesce(when(col("alert_id").isNotNull(), 1).otherwise(0), lit(0)),
        )
        .groupBy("typology_id", "typology_type", "expected_workload")
        .agg({"hit": "max"})
        .withColumnRenamed("max(hit)", "detected")
    )

    per_typology = (
        per_instance.groupBy("typology_type", "expected_workload")
        .agg(
            {"detected": "avg", "typology_id": "count"},
        )
        .withColumnRenamed("avg(detected)", "recall")
        .withColumnRenamed("count(typology_id)", "instance_count")
    )

    # False-positive rate is a global stat, not per-typology: fraction of
    # alerts that touch no manifest UETR. Precision would need per-alert
    # per-typology labels, which we don't have here.
    if total_alerts > 0:
        all_manifest_uetrs = manifest.select(
            explode(col("participant_uetrs")).alias("uetr")
        ).distinct()
        alert_ids_with_hits = alert_uetrs.join(
            all_manifest_uetrs, on="uetr", how="inner"
        ).select("alert_id").distinct()
        tp_alerts = alert_ids_with_hits.count()
        fp_alerts = total_alerts - tp_alerts
        fp_rate = fp_alerts / total_alerts if total_alerts else 0.0
    else:
        tp_alerts = 0
        fp_alerts = 0
        fp_rate = 0.0

    log(f"Alerts: total={total_alerts:,} TP={tp_alerts:,} FP={fp_alerts:,} FP_rate={fp_rate:.4f}")

    per_typology = (
        per_typology
        .withColumn("total_alerts", lit(total_alerts))
        .withColumn("fp_alerts", lit(fp_alerts))
        .withColumn("fp_rate", lit(fp_rate))
        .withColumn("computed_by", lit("lb-score-financial"))
    )
    per_typology.write.mode("overwrite").parquet(args.output)
    n_rows = per_typology.count()
    log(f"Wrote recall.parquet: {n_rows} typology rows")

    # Guard against ruff unused-import warning for countDistinct kept for future use.
    _ = countDistinct
    spark.stop()


if __name__ == "__main__":
    main()
