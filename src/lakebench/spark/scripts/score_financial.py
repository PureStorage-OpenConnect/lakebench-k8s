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
GOLD_STATUS = env("LB_FINANCIAL_GOLD_DETECTION_STATUS", "gold.detection_status")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute recall + FP rate from manifest + alerts")
    parser.add_argument("--manifest", required=True, help="S3 URI to manifest.parquet")
    parser.add_argument("--output", required=True, help="S3 URI for recall.parquet")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("lb-score-financial").getOrCreate()
    # LB-126: force sort-merge joins. The recall/FP joins explode
    # gold.alerts.related_txn_ids to (alert_id, uetr) -- at realistic alert
    # volumes (100k+ alerts, each with a related-txn array) that side is far
    # larger than Spark's size estimate, so auto-broadcast tries to build a
    # broadcast table on the driver and dies with
    # notEnoughMemoryToBuildAndBroadcastTableError (observed live: the score
    # job's driver exited 1 on a scale-1 run with ~173k alerts). Disabling the
    # broadcast threshold makes every join sort-merge, which is the correct
    # strategy for these uetr-keyed joins and scales to 100+ without a driver
    # OOM. The manifest side is small enough that losing its broadcast is
    # negligible.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
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
        alerts_all = spark.table(f"{CATALOG}.{GOLD_ALERTS}")
    except Exception as e:  # noqa: BLE001
        raise SystemExit(
            f"Cannot read {CATALOG}.{GOLD_ALERTS}: {e}. gold_finalize_financial "
            "must run before scoring."
        ) from e

    # LB-119: read gold.detection_status ONCE up front for two things --
    # (1) the run_id that produced the current gold.alerts, and (2) which
    # rules skipped. Scoping the alert read to that run_id is essential:
    # on a reused catalog a rule that SKIPPED (W1 above the vertex cap)
    # never runs its DELETE, so a PRIOR run's alerts survive with a foreign
    # run_id. Without this filter those stale rows inflate total_alerts and
    # fp_rate here even though recall is protected -- the same stale-row
    # hazard, one consumer further on. It also excludes any foreign rows a
    # `financial replay` wrote into gold.alerts with its own run_id.
    # detection_status is overwritten every gold_finalize run, so it carries
    # exactly one run_id; if it is absent (older run) we fall back to the
    # unscoped read, preserving prior behaviour.
    current_run_id: str | None = None
    skipped_typologies: list[str] = []
    try:
        status = spark.table(f"{CATALOG}.{GOLD_STATUS}")
        run_ids = [r["run_id"] for r in status.select("run_id").distinct().collect()]
        if len(run_ids) == 1:
            current_run_id = run_ids[0]
        elif len(run_ids) > 1:
            log(
                f"gold.detection_status has {len(run_ids)} run_ids "
                f"{run_ids}; expected one. Not scoping alerts by run_id."
            )
        skipped_typologies = [
            r["target_typology"]
            for r in status.filter(
                (col("status") == lit("skipped")) & col("target_typology").isNotNull()
            )
            .select("target_typology")
            .distinct()
            .collect()
        ]
    except Exception as e:  # noqa: BLE001
        log(f"gold.detection_status not readable ({e}); alerts unscoped, all typologies scored.")

    alerts = (
        alerts_all.filter(col("run_id") == lit(current_run_id))
        if current_run_id is not None
        else alerts_all
    )
    total_alerts = alerts.count()
    log(f"gold.alerts rows (run_id={current_run_id or 'unscoped'}): {total_alerts:,}")

    # Explode manifest to (typology_id, typology_type, expected_workload, uetr) pairs.
    # explode_outer, not explode: a manifest row with a NULL/empty participant_uetrs
    # array must still contribute to the denominator with detected=0. explode
    # would silently drop the whole instance, inflating recall by removing
    # the "we never fired for this instance" data points.
    manifest_uetrs = manifest.select(
        "typology_id",
        "typology_type",
        "expected_workload",
        explode_outer(col("participant_uetrs")).alias("uetr"),
    ).distinct()

    # Explode alerts to (alert_id, uetr) pairs. On empty alerts, this frame is
    # empty and the left-join below leaves detected=0 everywhere -- the correct
    # answer, which matters because the pipeline runs early UAT with empty
    # alerts before rules are wired.
    alert_uetrs = (
        alerts.select(
            col("alert_id"),
            explode(col("related_txn_ids")).alias("uetr"),
        ).distinct()
        if total_alerts > 0
        else spark.createDataFrame([], "alert_id STRING, uetr STRING")
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

    # LB-119: mark typologies whose sole detector rule was SKIPPED this run
    # as "not run" rather than reporting recall 0 (skipped_typologies was
    # read from gold.detection_status up front). A typology whose targeting
    # rule skipped has no chance of a recall signal, so a 0 there measures
    # the cap, not the stack; we override recall to NULL +
    # detection_status='rule_skipped'. This is a deliberate "score against
    # the DESIGNATED rule only" choice (RULE_TARGET_TYPOLOGY is 1:1): if a
    # future second detector for a shared typology exists, revisit this so
    # its incidental detections are not masked. Today only W1 skips, so only
    # gather_scatter is ever marked rule_skipped.
    if skipped_typologies:
        log(f"Rules skipped this run; typologies marked not-run: {skipped_typologies}")
        per_typology = per_typology.withColumn(
            "detection_status",
            when(col("typology_type").isin(skipped_typologies), lit("rule_skipped")).otherwise(
                lit("scored")
            ),
        ).withColumn(
            "recall",
            when(col("typology_type").isin(skipped_typologies), lit(None).cast("double")).otherwise(
                col("recall")
            ),
        )
    else:
        per_typology = per_typology.withColumn("detection_status", lit("scored"))

    # False-positive rate is a global stat, not per-typology: fraction of
    # alerts that touch no manifest UETR. Precision would need per-alert
    # per-typology labels, which we don't have here.
    if total_alerts > 0:
        all_manifest_uetrs = manifest.select(
            explode(col("participant_uetrs")).alias("uetr")
        ).distinct()
        alert_ids_with_hits = (
            alert_uetrs.join(all_manifest_uetrs, on="uetr", how="inner")
            .select("alert_id")
            .distinct()
        )
        tp_alerts = alert_ids_with_hits.count()
        fp_alerts = total_alerts - tp_alerts
        fp_rate = fp_alerts / total_alerts if total_alerts else 0.0
    else:
        tp_alerts = 0
        fp_alerts = 0
        fp_rate = 0.0

    log(f"Alerts: total={total_alerts:,} TP={tp_alerts:,} FP={fp_alerts:,} FP_rate={fp_rate:.4f}")

    per_typology = (
        per_typology.withColumn("total_alerts", lit(total_alerts))
        .withColumn("fp_alerts", lit(fp_alerts))
        .withColumn("fp_rate", lit(fp_rate))
        .withColumn("computed_by", lit("lb-score-financial"))
    )
    per_typology.write.mode("overwrite").parquet(args.output)
    n_rows = per_typology.count()
    log(f"Wrote recall.parquet: {n_rows} typology rows")

    # LB-123: write a small recall.json sidecar next to recall.parquet so
    # `lakebench run` can fold recall into the batch scorecard using boto3
    # alone -- the CLI has no pandas/pyarrow to read the parquet. Written as
    # a single object through the already-configured S3A FileSystem, so no
    # extra dependency is added to the Spark image. Best-effort: a failure
    # here does not fail scoring (recall.parquet is already durable).
    import json as _json

    rows = [r.asDict() for r in per_typology.collect()]
    summary = {
        "typologies": [
            {
                "typology_type": r.get("typology_type"),
                "expected_workload": r.get("expected_workload"),
                "recall": r.get("recall"),
                "instance_count": r.get("instance_count"),
                "detection_status": r.get("detection_status"),
            }
            for r in rows
        ],
        "total_alerts": int(total_alerts),
        "fp_alerts": int(fp_alerts),
        "fp_rate": float(fp_rate),
        "run_id": current_run_id,
        "computed_by": "lb-score-financial",
    }
    # Keep the parquet's directory; swap the basename for recall.json.
    out = args.output.rstrip("/")
    json_uri = (out.rsplit("/", 1)[0] + "/recall.json") if "/" in out else "recall.json"
    try:
        jvm = spark.sparkContext._jvm
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(json_uri), hconf)
        stream = fs.create(jvm.org.apache.hadoop.fs.Path(json_uri), True)
        stream.write(bytearray(_json.dumps(summary), "utf-8"))
        stream.close()
        log(f"Wrote recall.json sidecar: {json_uri}")
    except Exception as e:  # noqa: BLE001
        log(f"Could not write recall.json sidecar ({e}); recall.parquet still written.")

    # Guard against ruff unused-import warning for countDistinct kept for future use.
    _ = countDistinct
    spark.stop()


if __name__ == "__main__":
    main()
