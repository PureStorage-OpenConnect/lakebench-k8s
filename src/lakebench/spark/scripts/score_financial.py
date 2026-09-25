"""Score (Financial) -- compute recall + false-positive rate from
manifest + gold.alerts.

Reads the datagen manifest sidecar (typology ground truth) and the
detection alerts written by workloads W2/W3/W4/etc. Joins on
related_txn_ids and computes recall + FP rate per typology_type. Writes
recall.parquet under the run's output prefix.

Recall definition (per typology): fraction of typology instances for which
at least one participant_uetr appears in an alert raised by one of the
typology's DESIGNATED rules (the rules whose target_typology it is, taken
from gold.detection_status for this run). Alerts from other rules count only
toward ``incidental_recall``, published separately; the ``random`` control
typology's incidental recall is the chance floor. Before 2026-09-24 any alert
from any rule counted, so one broad rule (W1's giant component) made every
typology score 1.0.

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
    broadcast,
    coalesce,
    col,
    explode,
    explode_outer,
    lit,
    when,
)
from pyspark.sql.functions import max as smax

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
GOLD_STATUS = env("LB_FINANCIAL_GOLD_DETECTION_STATUS", "gold.detection_status")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_ACCOUNTS = env("LB_FINANCIAL_SILVER_ACCOUNTS", "silver.accounts")
_BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
_BRONZE_ROOT = env("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/").rstrip("/")
ACCOUNT_PATH = env(
    "LB_FINANCIAL_ACCOUNT_PATH", f"{_BRONZE_URI}{_BRONZE_ROOT}/bronze/account.parquet"
)


def subject_customer_check(spark, manifest, entities, id_map, scoped_typologies) -> dict:
    """Whether every planted subject is a customer in silver (GOALS P10 stage 0).

    Customer-scoped rules drop alerts on non-customers, so a subject that
    silver does not mark is_customer (an IBAN -> holder -> party join that
    missed, or an entity id collision) loses its designated alert and its
    typology's recall falls with no other sign. The subject is the role
    typology::subject_index names (aml_features.subject_index mirrors it),
    resolved from the manifest's participant_entity_ids and instance seed;
    ``id_map`` maps the datagen id to the silver entity_id. ``unmapped`` counts
    subjects with no silver entity at all, ``not_customer`` those whose entity
    is not a customer (an entity listed twice is a customer if any row says
    so, as the rules' semi join treats it). Only ``scoped_typologies`` count
    toward the status (targets of a customer-scoped rule that ran):
    ``fail`` when a subject is not a customer; else ``incomplete`` when a
    subject has no silver entity (a continuous run whose silver has not caught
    up, or a join miss); else ``unchecked`` when some instance names no
    participants or seed; else ``ok``.
    """
    from aml_features import subject_index

    need = {"typology_type", "participant_entity_ids", "seed"}
    missing = sorted(need - set(manifest.columns))
    if missing:
        return {"status": "unchecked", "reason": f"manifest has no {', '.join(missing)}"}
    rows = []
    unresolved = set()
    for r in manifest.select("typology_type", "participant_entity_ids", "seed").collect():
        ids = r["participant_entity_ids"] or []
        if not ids or r["seed"] is None:
            unresolved.add(r["typology_type"])
            continue
        idx = subject_index(r["typology_type"], len(ids), int(r["seed"]))
        rows.append((r["typology_type"], int(ids[idx])))
    if not rows:
        return {"status": "unchecked", "reason": "no manifest row names its participants"}
    subj = spark.createDataFrame(rows, "typology_type string, dg_id long")
    ents = (
        entities.select(
            col("entity_id").alias("key"),
            coalesce(col("is_customer"), lit(False)).cast("int").alias("_c"),
        )
        .groupBy("key")
        .agg(smax("_c").alias("_c"))
        .select("key", (col("_c") == lit(1)).alias("is_customer"))
    )
    joined = subj.join(id_map, "dg_id", "left").join(ents, "key", "left")
    by_typology = {}
    for r in (
        joined.groupBy("typology_type")
        .agg(
            {"dg_id": "count"},
        )
        .withColumnRenamed("count(dg_id)", "subjects")
        .join(
            joined.where(col("key").isNull()).groupBy("typology_type").count(),
            "typology_type",
            "left",
        )
        .withColumnRenamed("count", "unmapped")
        .join(
            joined.where(col("key").isNotNull() & ~coalesce(col("is_customer"), lit(False)))
            .groupBy("typology_type")
            .count()
            .withColumnRenamed("count", "not_customer"),
            "typology_type",
            "left",
        )
        .collect()
    ):
        by_typology[r["typology_type"]] = {
            "subjects": int(r["subjects"]),
            "unmapped": int(r["unmapped"] or 0),
            "not_customer": int(r["not_customer"] or 0),
        }
    scoped = {t: v for t, v in by_typology.items() if t in scoped_typologies}
    bad = {t for t, v in scoped.items() if v["not_customer"]}
    gaps = {t for t, v in scoped.items() if v["unmapped"]}
    if bad:
        status = "fail"
    elif gaps:
        status = "incomplete"
    elif unresolved & set(scoped_typologies):
        status = "unchecked"
    else:
        status = "ok"
    return {
        "status": status,
        "subjects": sum(v["subjects"] for v in by_typology.values()),
        "unmapped": sum(v["unmapped"] for v in by_typology.values()),
        "not_customer": sum(v["not_customer"] for v in by_typology.values()),
        "failing_typologies": sorted(bad | gaps),
        "unresolved_typologies": sorted(unresolved),
        "by_typology": by_typology,
    }


def _run_subject_check(spark, manifest, status_rows) -> dict:
    """subject_customer_check against the lakehouse silver tables; any read
    failure is reported as ``unchecked`` with the reason, never as ok."""
    try:
        from aml_features import _account_id_map
        from detection_rules import CUSTOMER_SCOPED_RULES, RULE_TARGET_TYPOLOGY

        entities = spark.table(f"{CATALOG}.{SILVER_ENTITIES}")
        iban_to_key = (
            spark.table(f"{CATALOG}.{SILVER_ACCOUNTS}")
            .select(col("iban"), col("holder_entity_id").alias("key"))
            .filter(col("iban").isNotNull())
        )
        id_map = _account_id_map(spark.read.parquet(ACCOUNT_PATH), iban_to_key)
        # Only rules that ran this run: a mode-excluded rule (W7/W8 in
        # continuous) drops nothing.
        ran = {r["rule_id"] for r in status_rows if r.get("status") == "ran"}
        scoped = {RULE_TARGET_TYPOLOGY[r] for r in CUSTOMER_SCOPED_RULES & ran} - {None}
        return subject_customer_check(spark, manifest, entities, id_map, scoped)
    except Exception as e:  # noqa: BLE001 -- reported, not raised
        return {"status": "unchecked", "reason": f"{type(e).__name__}: {e}"[:300]}


def compute_scores(spark, manifest, alerts, status_rows: list[dict]):
    """Per-typology recall and per-rule false positives for one run.

    ``manifest``: typology_id, typology_type, expected_workload,
    participant_uetrs. ``alerts``: alert_id, rule_id, related_txn_ids, already
    scoped to the run. ``status_rows``: gold.detection_status rows (rule_id,
    status, target_typology, ...).

    Returns ``(per_typology_df, summary_dict)``.
    """
    designated: dict[str, list[str]] = {}
    rule_status: dict[str, str] = {}
    for r in status_rows:
        rule_status[r["rule_id"]] = r["status"]
        if r.get("target_typology"):
            designated.setdefault(r["target_typology"], []).append(r["rule_id"])

    def _typology_state(typ: str) -> str:
        rules = designated.get(typ, [])
        if not rules:
            return "no_rule"
        states = {rule_status.get(rid) for rid in rules}
        if "ran" in states and states != {"ran"}:
            # Some designated rules ran, others skipped or errored: the recall
            # below covers only the rules that ran.
            return "partial"
        if "ran" in states:
            return "scored"
        if "error" in states:
            return "rule_error"
        return "rule_skipped"

    manifest_uetrs = (
        manifest.select(
            "typology_id",
            "typology_type",
            "expected_workload",
            explode_outer(col("participant_uetrs")).alias("uetr"),
        )
        .distinct()
        .cache()
    )
    # Cached: reused by every join and action below (about 7).
    alert_uetrs = (
        alerts.select("alert_id", "rule_id", explode(col("related_txn_ids")).alias("uetr"))
        .distinct()
        .cache()
    )

    # (typology_type, rule_id) for designated rules that actually ran.
    pairs = [
        (typ, rid)
        for typ, rids in designated.items()
        for rid in rids
        if rule_status.get(rid) == "ran"
    ]
    pair_df = spark.createDataFrame(pairs, "typology_type STRING, rule_id STRING")

    designated_hits = (
        manifest_uetrs.join(broadcast(pair_df), "typology_type")
        .join(alert_uetrs.select("rule_id", "uetr").distinct(), ["rule_id", "uetr"])
        .select("typology_id")
        .distinct()
        .withColumn("designated_hit", lit(1))
    )
    incidental_hits = (
        manifest_uetrs.join(alert_uetrs.select("uetr").distinct(), "uetr")
        .select("typology_id")
        .distinct()
        .withColumn("any_hit", lit(1))
    )
    per_instance = (
        manifest_uetrs.select("typology_id", "typology_type", "expected_workload")
        .distinct()
        .join(designated_hits, "typology_id", "left")
        .join(incidental_hits, "typology_id", "left")
        .select(
            "typology_id",
            "typology_type",
            "expected_workload",
            coalesce(col("designated_hit"), lit(0)).alias("designated_hit"),
            coalesce(col("any_hit"), lit(0)).alias("any_hit"),
        )
    )
    agg = (
        per_instance.groupBy("typology_type", "expected_workload")
        .agg(
            {"designated_hit": "avg", "any_hit": "avg", "typology_id": "count"},
        )
        .withColumnRenamed("avg(designated_hit)", "recall_raw")
        .withColumnRenamed("avg(any_hit)", "incidental_recall")
        .withColumnRenamed("count(typology_id)", "instance_count")
    )
    state_rows = [
        (typ, _typology_state(typ), ",".join(sorted(designated.get(typ, []))) or None)
        for typ in sorted({r["typology_type"] for r in agg.select("typology_type").collect()})
    ]
    state_df = spark.createDataFrame(
        state_rows, "typology_type STRING, detection_status STRING, designated_rules STRING"
    )
    per_typology = (
        agg.join(state_df, "typology_type", "left")
        .withColumn(
            "recall",
            when(col("detection_status").isin("scored", "partial"), col("recall_raw")).otherwise(
                lit(None).cast("double")
            ),
        )
        .drop("recall_raw")
    )

    # False positives. Global: alerts touching no planted txn at all. Per
    # rule: alerts of a rule touching no txn of that rule's target typology.
    total_alerts = alerts.count()
    fp_by_rule: dict[str, float | None] = {}
    txn_precision_by_rule: dict[str, float] = {}
    chance_by_rule: dict[str, float] = {}
    if total_alerts > 0:
        manifest_all = manifest_uetrs.select("uetr").where(col("uetr").isNotNull()).distinct()
        tp_global = alert_uetrs.join(manifest_all, "uetr").select("alert_id").distinct().count()
        fp_alerts = total_alerts - tp_global
        fp_rate: float | None = fp_alerts / total_alerts
        targeted = {rid: typ for typ, rids in designated.items() for rid in rids}
        target_df = spark.createDataFrame(
            list(targeted.items()) or [("", "")], "rule_id STRING, target STRING"
        )
        target_uetrs = manifest_uetrs.select("uetr", "typology_type").where(col("uetr").isNotNull())
        # One row per (alert, uetr) with whether that txn belongs to the
        # alert rule's target typology. Rules with no target (W5/W6 list
        # matches) are left out: "false positive" has no meaning for them.
        refs = (
            alert_uetrs.join(broadcast(target_df), "rule_id")
            .join(target_uetrs, "uetr", "left")
            .withColumn(
                "on_target",
                when(col("typology_type") == col("target"), lit(1)).otherwise(lit(0)),
            )
            .groupBy("alert_id", "rule_id", "uetr")
            .agg(smax("on_target").alias("on_target"))
            .cache()
        )
        # Alert-level: an alert is a false positive if it touches none of its
        # target's txns. Txn-level precision: the share of an alert's txns
        # that are planted target txns, so one giant alert over the whole
        # corpus cannot score itself perfect.
        per_alert = refs.groupBy("alert_id", "rule_id").agg(smax("on_target").alias("hit"))
        for row in per_alert.groupBy("rule_id").agg({"hit": "avg"}).collect():
            fp_by_rule[row["rule_id"]] = 1.0 - float(row["avg(hit)"])
        for row in refs.groupBy("rule_id").agg({"on_target": "avg"}).collect():
            txn_precision_by_rule[row["rule_id"]] = float(row["avg(on_target)"])
        # Per-rule chance: the share of random-control instances a rule's
        # alerts touch. Recall at or below this is indistinguishable from
        # chance for that rule.
        random_ids = manifest_uetrs.where(col("typology_type") == lit("random"))
        n_random = random_ids.select("typology_id").distinct().count()
        if n_random:
            hits = (
                random_ids.join(alert_uetrs.select("rule_id", "uetr").distinct(), "uetr")
                .select("rule_id", "typology_id")
                .distinct()
                .groupBy("rule_id")
                .count()
                .collect()
            )
            for row in hits:
                chance_by_rule[row["rule_id"]] = row["count"] / n_random
    else:
        fp_alerts = 0
        fp_rate = None  # no alerts: there is no false-positive rate to report

    per_typology = (
        per_typology.withColumn("total_alerts", lit(total_alerts))
        .withColumn("fp_alerts", lit(fp_alerts))
        .withColumn("fp_rate", lit(fp_rate).cast("double"))
        .withColumn("computed_by", lit("lb-score-financial"))
    )
    random_row = per_typology.filter(col("typology_type") == lit("random")).collect()
    summary = {
        "total_alerts": int(total_alerts),
        "fp_alerts": int(fp_alerts),
        "fp_rate": fp_rate,
        "fp_rate_by_rule": fp_by_rule,
        "txn_precision_by_rule": txn_precision_by_rule,
        "chance_by_rule": chance_by_rule,
        "random_control_floor": (float(random_row[0]["incidental_recall"]) if random_row else None),
    }
    return per_typology, summary


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

    # Run scoping. gold.detection_status is overwritten by every
    # gold_finalize run and names the run that produced the current alerts.
    # Scoring against anything else would mix runs, so a missing or
    # ambiguous status is a hard failure (it used to fall back to scoring
    # every alert in the table).
    try:
        status = spark.table(f"{CATALOG}.{GOLD_STATUS}")
        status_rows = [r.asDict() for r in status.collect()]
    except Exception as e:  # noqa: BLE001
        raise SystemExit(
            f"Cannot read {CATALOG}.{GOLD_STATUS} ({e}); cannot tell which run "
            "produced gold.alerts, so recall would mix runs. Re-run gold-finalize."
        ) from e
    run_ids = sorted({r["run_id"] for r in status_rows})
    if len(run_ids) != 1:
        raise SystemExit(
            f"{CATALOG}.{GOLD_STATUS} holds {len(run_ids)} run_ids {run_ids}; "
            "expected exactly one. Re-run gold-finalize."
        )
    current_run_id = run_ids[0]
    pending = sorted(r["rule_id"] for r in status_rows if r.get("status") == "pending")
    if pending:
        raise SystemExit(
            f"Run {current_run_id} is incomplete: rules {pending} are still 'pending' in "
            f"{CATALOG}.{GOLD_STATUS}, so gold-finalize did not finish and gold.alerts "
            "may be half rewritten. Re-run gold-finalize."
        )
    alerts = alerts_all.filter(col("run_id") == lit(current_run_id))

    per_typology, summary = compute_scores(spark, manifest, alerts, status_rows)
    summary["run_id"] = current_run_id
    # Loud on purpose: a subject silver does not call a customer has its
    # designated alert dropped by the customer-scoped rules.
    check = _run_subject_check(spark, manifest, status_rows)
    summary["subject_customer_check"] = check
    log(
        f"[score] subject-customer-check status={check['status']} "
        f"subjects={check.get('subjects')} unmapped={check.get('unmapped')} "
        f"not_customer={check.get('not_customer')} "
        f"failing={','.join(check.get('failing_typologies') or []) or '-'}"
        + (f" reason={check['reason']}" if check.get("reason") else "")
    )
    if check["status"] in ("fail", "incomplete"):
        log(
            "WARNING: planted subjects that silver does not hold as customers; the "
            "customer-scoped rules cannot alert on them, so recall for "
            f"{check['failing_typologies']} is understated."
        )
    total_alerts = summary["total_alerts"]
    fp_alerts = summary["fp_alerts"]
    fp_rate = summary["fp_rate"]
    log(f"gold.alerts rows (run_id={current_run_id}): {total_alerts:,}")
    log(
        f"Alerts: total={total_alerts:,} FP={fp_alerts:,} "
        f"FP_rate={'n/a' if fp_rate is None else f'{fp_rate:.4f}'}"
    )
    log(f"Random-control floor (incidental recall of 'random'): {summary['random_control_floor']}")

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
    summary["typologies"] = [
        {
            "typology_type": r.get("typology_type"),
            "expected_workload": r.get("expected_workload"),
            "designated_rules": r.get("designated_rules"),
            "recall": r.get("recall"),
            "incidental_recall": r.get("incidental_recall"),
            "instance_count": r.get("instance_count"),
            "detection_status": r.get("detection_status"),
            "subjects_not_customer": (
                (check.get("by_typology") or {}).get(r.get("typology_type"), {})
            ).get("not_customer"),
        }
        for r in rows
    ]
    summary["computed_by"] = "lb-score-financial"
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

    spark.stop()


if __name__ == "__main__":
    main()
