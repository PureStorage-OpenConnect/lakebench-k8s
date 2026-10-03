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
import os

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
from pyspark.sql.functions import count as scount
from pyspark.sql.functions import max as smax
from pyspark.sql.functions import sum as ssum

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
GOLD_STATUS = env("LB_FINANCIAL_GOLD_DETECTION_STATUS", "gold.detection_status")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_ACCOUNTS = env("LB_FINANCIAL_SILVER_ACCOUNTS", "silver.accounts")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_BATCH_VERSIONS = env("LB_FINANCIAL_SILVER_BATCH_VERSIONS", "silver.silver_batch_versions")
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


def _iban_to_key(accounts):
    """silver.accounts as the (iban, key) frame ``_account_id_map`` takes."""
    return accounts.select(col("iban"), col("holder_entity_id").alias("key")).filter(
        col("iban").isNotNull()
    )


def _run_subject_check(spark, manifest, status_rows, entities=None, accounts=None) -> dict:
    """subject_customer_check against the lakehouse silver tables (or the
    given pinned frames); any read failure is reported as ``unchecked`` with
    the reason, never as ok."""
    try:
        from aml_features import _account_id_map
        from detection_rules import CUSTOMER_SCOPED_RULES, RULE_TARGET_TYPOLOGY

        if entities is None:
            entities = spark.table(f"{CATALOG}.{SILVER_ENTITIES}")
        if accounts is None:
            accounts = spark.table(f"{CATALOG}.{SILVER_ACCOUNTS}")
        id_map = _account_id_map(spark.read.parquet(ACCOUNT_PATH), _iban_to_key(accounts))
        # Only rules that ran this run: a mode-excluded rule (W7/W8 in
        # continuous) drops nothing.
        ran = {r["rule_id"] for r in status_rows if r.get("status") == "ran"}
        scoped = {RULE_TARGET_TYPOLOGY[r] for r in CUSTOMER_SCOPED_RULES & ran} - {None}
        return subject_customer_check(spark, manifest, entities, id_map, scoped)
    except Exception as e:  # noqa: BLE001 -- reported, not raised
        return {"status": "unchecked", "reason": f"{type(e).__name__}: {e}"[:300]}


def check_status_run(status_run_id: str, own_run_id: str) -> None:
    """Refuse to score a detection status another run wrote.

    ``own_run_id`` is this job's LB_RUN_ID. Batch gold jobs run per cycle as
    ``<run>-c<n>``, so that form of this run counts as this run. Without a run
    id (``lakebench financial score`` outside a run) the status decides.
    """
    if not own_run_id:
        return
    if status_run_id == own_run_id or status_run_id.startswith(own_run_id + "-c"):
        return
    raise SystemExit(
        f"{CATALOG}.{GOLD_STATUS} names run {status_run_id}, not this run ({own_run_id}): "
        "gold still holds another run's alerts, so recall would be that run's. Wait for "
        "this run's gold-finalize or gold-refresh, or reset the deployment's gold."
    )


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
        # The manifest column is the generator's AML category, not the
        # detecting rule (designated_rules is): name it for what it is.
        .withColumnRenamed("expected_workload", "workload_category")
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
        # alert rule's target typology. A rule with no target is left out:
        # "false positive" has no meaning for it.
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
    # Per-typology outcome counts, so a caller cannot report every manifest
    # typology as "scored" (the 2026-09-26 live run said 15 of 15 when 6 were).
    states = {typ: st for typ, st, _ in state_rows}
    counts = {
        k: sum(1 for v in states.values() if v == k)
        for k in ("scored", "partial", "no_rule", "rule_skipped", "rule_error")
    }
    # Every rule's own outcome, skip and error reasons included.
    rules = [
        {
            "rule_id": r["rule_id"],
            "status": r.get("status"),
            "reason": r.get("reason"),
            "target_typology": r.get("target_typology"),
            "alert_count": r.get("alert_count"),
        }
        for r in sorted(status_rows, key=lambda x: x["rule_id"])
    ]
    # Alerts whose related_txn_ids a per-alert evidence cap cut (W1, the W2
    # beneficiary kind, W4 and the W5 rescreen write txns_truncated into
    # their evidence). Scoring matches planted
    # payments against related_txn_ids, so a cut alert can miss planted
    # payments past the cut: recall for a typology such a rule detects is
    # bounded by a Lakebench-imposed cap, and says so (invariant 6).
    capped_by_rule: dict[str, int] = {}
    if "evidence" in alerts.columns:
        for row in (
            alerts.where(col("evidence").getItem("txns_truncated") == lit("true"))
            .groupBy("rule_id")
            .count()
            .collect()
        ):
            capped_by_rule[row["rule_id"]] = int(row["count"])
    capped_typologies = {
        typ: sorted(r for r in rids if capped_by_rule.get(r))
        for typ, rids in sorted(designated.items())
        if any(capped_by_rule.get(r) for r in rids)
    }
    summary = {
        "evidence_capped_alerts_by_rule": dict(sorted(capped_by_rule.items())),
        # typology -> its designated rules with a cut alert: that typology's
        # recall is bounded by an evidence cap.
        "recall_bounded_by_evidence_cap": capped_typologies,
        "typology_counts": counts,
        "rules": rules,
        "total_alerts": int(total_alerts),
        "fp_alerts": int(fp_alerts),
        "fp_rate": fp_rate,
        "fp_rate_by_rule": fp_by_rule,
        "txn_precision_by_rule": txn_precision_by_rule,
        "chance_by_rule": chance_by_rule,
        "random_control_floor": (float(random_row[0]["incidental_recall"]) if random_row else None),
    }
    return per_typology, summary


# --- covered mode: recall over what the last completed tick saw ---------------

#: The tick-record snapshots covered mode reads, by option suffix, and the
#: table each names. All six come from the last completed tick of a drained
#: continuous run (gold_refresh_financial.py tick record).
COVERED_SNAPSHOTS = (
    ("txns", SILVER_TXNS),
    ("entities", SILVER_ENTITIES),
    ("accounts", SILVER_ACCOUNTS),
    ("versions", SILVER_BATCH_VERSIONS),
    ("alerts", GOLD_ALERTS),
    ("status", GOLD_STATUS),
)

#: Columns of the alert-set fingerprint (the batch alert set's identity of an alert: the
#: alert id is a uuid and the run id differs between runs).
ALERT_SET_COLUMNS = ["rule_id", "entity_id", "alert_ts"]


class NotScored(Exception):
    """Covered mode cannot score this run; the message is the reason."""


def covered_snapshot_ids(args) -> dict | None:
    """{name: int snapshot id} from the --covered-* options; None when none is
    given (normal mode). Raises NotScored when only some are given or one is
    not an integer: a covered score never runs on a guessed snapshot."""
    given = {name: getattr(args, f"covered_{name}_snapshot") for name, _ in COVERED_SNAPSHOTS}
    if all(v is None for v in given.values()):
        return None
    missing = sorted(n for n, v in given.items() if v is None)
    if missing:
        raise NotScored(f"covered mode needs every tick snapshot; missing {', '.join(missing)}")
    ids = {}
    for name, table in COVERED_SNAPSHOTS:
        try:
            ids[name] = int(str(given[name]).strip())
        except ValueError:
            raise NotScored(f"{table} snapshot unknown at the last completed tick") from None
    return ids


def _check_snapshots_exist(spark, ids: dict) -> None:
    """NotScored when a recorded snapshot is gone (expired) or unreadable.
    Never falls back to the table's current state."""
    for name, table in COVERED_SNAPSHOTS:
        fq = f"{CATALOG}.{table}"
        try:
            n = spark.sql(
                f"SELECT count(*) AS n FROM {fq}.snapshots WHERE snapshot_id = {ids[name]}"
            ).collect()[0]["n"]
        except Exception as e:  # noqa: BLE001 -- reported as the reason
            raise NotScored(f"snapshots of {table} not readable: {one_line_text(e)}") from e
        if not n:
            raise NotScored(f"snapshot {table} expired before scoring")


def one_line_text(e) -> str:
    return " ".join(str(e).split())[:300]


def _at(spark, table: str, snapshot_id: int):
    return spark.sql(f"SELECT * FROM {CATALOG}.{table} VERSION AS OF {int(snapshot_id)}")


def covered_instances(manifest, sealed_uetrs, id_map, entity_keys):
    """One row per manifest instance: typology_id, typology_type, covered,
    no_participant_txns.

    Covered: the instance names at least one participant uetr, every one of
    them is in ``sealed_uetrs`` (silver.transactions at the tick's snapshot,
    through the tick's sealed filter), and every participant entity maps
    through ``id_map`` (datagen id -> silver key, through silver.accounts at
    the tick's snapshot) to a key in ``entity_keys`` (silver.entities at the
    tick's snapshot). An unmapped participant is not covered. An instance
    with no participant uetr is never covered and is counted in
    ``no_participant_txns``.
    """
    inst = manifest.select(
        "typology_id", "typology_type", "participant_uetrs", "participant_entity_ids"
    )
    present = sealed_uetrs.select("uetr").distinct().withColumn("_present", lit(1))
    by_uetr = (
        inst.select("typology_id", explode_outer(col("participant_uetrs")).alias("uetr"))
        .join(present, "uetr", "left")
        .groupBy("typology_id")
        .agg(
            scount(col("uetr")).alias("_n_uetr"),
            ssum(
                when(col("uetr").isNotNull() & col("_present").isNull(), lit(1)).otherwise(lit(0))
            ).alias("_missing_uetr"),
        )
    )
    keys = entity_keys.select("key").distinct().withColumn("_entity", lit(1))
    # explode_outer: an instance that names no entity is not covered (its
    # entities cannot be checked), so its null id counts as missing.
    by_entity = (
        inst.select("typology_id", explode_outer(col("participant_entity_ids")).alias("_pid"))
        .select("typology_id", col("_pid").cast("long").alias("dg_id"))
        .join(id_map, "dg_id", "left")
        .join(keys, "key", "left")
        .groupBy("typology_id")
        .agg(ssum(when(col("_entity").isNull(), lit(1)).otherwise(lit(0))).alias("_missing_entity"))
    )
    return (
        inst.select("typology_id", "typology_type")
        .distinct()
        .join(by_uetr, "typology_id", "left")
        .join(by_entity, "typology_id", "left")
        .select(
            "typology_id",
            "typology_type",
            (
                (coalesce(col("_n_uetr"), lit(0)) > 0)
                & (coalesce(col("_missing_uetr"), lit(0)) == 0)
                & (coalesce(col("_missing_entity"), lit(0)) == 0)
            ).alias("covered"),
            (coalesce(col("_n_uetr"), lit(0)) == 0).alias("no_participant_txns"),
        )
    )


def _excluded_typologies(status_rows: list[dict]) -> dict[str, str]:
    """typology -> reason, for a typology whose designated rules were all
    skipped as excluded from this mode (W1 and W5 to W8 in continuous)."""
    by_typ: dict[str, list[dict]] = {}
    for r in status_rows:
        if r.get("target_typology"):
            by_typ.setdefault(r["target_typology"], []).append(r)
    return {
        typ: "mode-excluded"
        for typ, rows in by_typ.items()
        if all(r.get("status") == "skipped" and r.get("reason") == "mode-excluded" for r in rows)
    }


def score_covered(spark, manifest, ids: dict, own_run_id: str):
    """Covered-mode scores for a drained continuous run.

    Returns ``(rows, summary)``: one row per manifest typology for
    recall.parquet, and the recall.json body. Raises NotScored with the
    reason when the run cannot be scored. Recall is the designated-hit rate
    over covered instances only (``recall_covered``); false positives and
    precision count every planted transaction of the full manifest, so an
    alert on a planted transaction the tick had not yet covered is not a
    false positive. The chance floor uses covered random instances, as recall
    does. No key named ``recall`` is written, so nothing can render this as
    the batch recall.
    """
    from aml_features import _account_id_map, check_manifest
    from common import SealedFilterError, frame_fingerprint, sealed_txns_filter_at

    need = {"typology_id", "typology_type", "participant_uetrs", "participant_entity_ids"}
    missing = sorted(need - set(manifest.columns))
    if missing:
        raise NotScored(f"manifest has no {', '.join(missing)}")
    try:
        # Instances are keyed by typology_id; a repeated one would merge two.
        check_manifest(manifest)
    except ValueError as e:
        raise NotScored(str(e)) from None
    _check_snapshots_exist(spark, ids)

    status_rows = [r.asDict() for r in _at(spark, GOLD_STATUS, ids["status"]).collect()]
    run_ids = sorted({r["run_id"] for r in status_rows})
    if len(run_ids) != 1:
        raise NotScored(
            f"{GOLD_STATUS} at snapshot {ids['status']} holds {len(run_ids)} run ids, not one"
        )
    run_id = run_ids[0]
    try:
        check_status_run(run_id, own_run_id)
    except SystemExit as e:
        raise NotScored(str(e)) from None
    pending = sorted(r["rule_id"] for r in status_rows if r.get("status") == "pending")
    if pending:
        raise NotScored(f"rules {pending} still pending at the last completed tick")
    alerts = _at(spark, GOLD_ALERTS, ids["alerts"]).filter(col("run_id") == lit(run_id))

    try:
        sealed = sealed_txns_filter_at(
            spark,
            _at(spark, SILVER_TXNS, ids["txns"]),
            CATALOG,
            SILVER_BATCH_VERSIONS,
            ids["versions"],
        )
    except (TypeError, SealedFilterError) as e:
        raise NotScored(one_line_text(e)) from e
    entities = _at(spark, SILVER_ENTITIES, ids["entities"])
    accounts = _at(spark, SILVER_ACCOUNTS, ids["accounts"])
    id_map = _account_id_map(spark.read.parquet(ACCOUNT_PATH), _iban_to_key(accounts))
    per_inst = covered_instances(
        manifest, sealed.select("uetr"), id_map, entities.select(col("entity_id").alias("key"))
    ).cache()
    counts = {
        r["typology_type"]: r
        for r in per_inst.groupBy("typology_type")
        .agg(
            scount(lit(1)).alias("corpus"),
            ssum(col("covered").cast("int")).alias("covered"),
            ssum(col("no_participant_txns").cast("int")).alias("no_participant"),
        )
        .collect()
    }
    covered_manifest = manifest.join(
        per_inst.where(col("covered")).select("typology_id").distinct(), "typology_id", "left_semi"
    )

    # Recall and the chance floor over covered instances; FP and precision
    # over the full manifest.
    cov_typ, cov_summary = compute_scores(spark, covered_manifest, alerts, status_rows)
    full_typ, full_summary = compute_scores(spark, manifest, alerts, status_rows)
    cov_rows = {r["typology_type"]: r.asDict() for r in cov_typ.collect()}
    full_rows = {r["typology_type"]: r.asDict() for r in full_typ.collect()}
    excluded = _excluded_typologies(status_rows)

    typologies = []
    for typ in sorted(counts):
        c = counts[typ]
        corpus_n = int(c["corpus"])
        covered_n = int(c["covered"] or 0)
        cov = cov_rows.get(typ) or {}
        full = full_rows.get(typ) or {}
        # compute_scores leaves recall null unless the typology's rules ran
        # (scored or partial); zero covered instances is null too.
        recall_covered = cov.get("recall") if covered_n else None
        typologies.append(
            {
                "typology_type": typ,
                "recall_covered": recall_covered,
                "incidental_recall_covered": cov.get("incidental_recall") if covered_n else None,
                "covered_instances": covered_n,
                "corpus_instances": corpus_n,
                "coverage": (covered_n / corpus_n) if corpus_n else None,
                "no_participant_txns": int(c["no_participant"] or 0),
                "detection_status": full.get("detection_status"),
                "designated_rules": full.get("designated_rules"),
                "workload_category": full.get("workload_category"),
            }
        )

    rows_all, fp_all, cols_sha = frame_fingerprint(alerts, ALERT_SET_COLUMNS)
    by_rule = {}
    for rid in sorted({r["rule_id"] for r in alerts.select("rule_id").distinct().collect()}):
        n, h, _ = frame_fingerprint(alerts.where(col("rule_id") == lit(rid)), ALERT_SET_COLUMNS)
        by_rule[rid] = {"rows": int(n), "h": str(h)}
    alert_set = {
        "spec": "as1",
        "columns": list(ALERT_SET_COLUMNS),
        "cols_sha": cols_sha,
        "rows": int(rows_all),
        "h": str(fp_all),
        "by_rule": by_rule,
    }

    # Subjects of covered instances only: an uncovered instance's subject is
    # usually not in silver yet, which would hide a real is_customer miss.
    check = _run_subject_check(
        spark, covered_manifest, status_rows, entities=entities, accounts=accounts
    )
    covered_block = {
        **{f"{name}_snapshot": ids[name] for name, _ in COVERED_SNAPSHOTS},
        "typologies": typologies,
        "excluded_typologies": [
            {"typology_type": t, "reason": why} for t, why in sorted(excluded.items())
        ],
        "covered_instances": sum(t["covered_instances"] for t in typologies),
        "corpus_instances": sum(t["corpus_instances"] for t in typologies),
        # Full manifest: an alert on a planted txn not yet covered is no FP.
        "total_alerts": full_summary["total_alerts"],
        "fp_alerts": full_summary["fp_alerts"],
        "fp_rate": full_summary["fp_rate"],
        "fp_rate_by_rule": full_summary["fp_rate_by_rule"],
        "txn_precision_by_rule": full_summary["txn_precision_by_rule"],
        # Covered random instances, as recall_covered.
        "chance_by_rule": cov_summary["chance_by_rule"],
        "random_control_floor": cov_summary["random_control_floor"],
        "typology_counts": full_summary["typology_counts"],
        "rules": full_summary["rules"],
        "evidence_capped_alerts_by_rule": full_summary["evidence_capped_alerts_by_rule"],
        "recall_bounded_by_evidence_cap": full_summary["recall_bounded_by_evidence_cap"],
        "subject_customer_check": check,
    }
    summary = {
        "mode": "covered",
        "status": "scored",
        "run_id": run_id,
        "covered": covered_block,
        "alert_set": alert_set,
    }
    per_inst.unpersist()
    return typologies, summary


def _write_text(spark, uri: str, text: str) -> None:
    """One object through the already-configured S3A FileSystem."""
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(uri), hconf)
    stream = fs.create(jvm.org.apache.hadoop.fs.Path(uri), True)
    try:
        stream.write(bytearray(text, "utf-8"))
    finally:
        stream.close()


def _json_uri(output: str) -> str:
    """recall.json beside recall.parquet."""
    out = output.rstrip("/")
    return (out.rsplit("/", 1)[0] + "/recall.json") if "/" in out else "recall.json"


def run_covered(spark, args, manifest, ids: dict) -> None:
    """Covered mode: write recall.parquet (no ``recall`` column) and
    recall.json; a run that cannot be scored writes only recall.json with
    ``status: not_scored`` and its reason, and exits 0."""
    import json as _json

    try:
        typologies, summary = score_covered(
            spark, manifest, ids, os.environ.get("LB_RUN_ID", "").strip()
        )
    except NotScored as e:
        _write_not_scored(spark, args.output, str(e), ids)
        return
    summary["computed_by"] = "lb-score-financial"
    cols = (
        "typology_type STRING, recall_covered DOUBLE, covered_instances BIGINT, "
        "corpus_instances BIGINT, coverage DOUBLE, no_participant_txns BIGINT, "
        "detection_status STRING, designated_rules STRING"
    )
    spark.createDataFrame(
        [
            (
                t["typology_type"],
                t["recall_covered"],
                t["covered_instances"],
                t["corpus_instances"],
                t["coverage"],
                t["no_participant_txns"],
                t["detection_status"],
                t["designated_rules"],
            )
            for t in typologies
        ],
        cols,
    ).write.mode("overwrite").parquet(args.output)
    cov = summary["covered"]
    log(
        f"[score] covered mode: {cov['covered_instances']:,} of {cov['corpus_instances']:,} "
        f"instances covered at txns snapshot {cov['txns_snapshot']}; "
        f"alerts={cov['total_alerts']:,} alert_set rows={summary['alert_set']['rows']:,}"
    )
    _write_text(spark, _json_uri(args.output), _json.dumps(summary))
    log(f"Wrote recall.json sidecar: {_json_uri(args.output)}")


def _write_not_scored(spark, output: str, reason: str, ids: dict | None) -> None:
    import json as _json

    log(f"[score] covered mode not scored: {reason}")
    body = {
        "mode": "covered",
        "status": "not_scored",
        "reason": reason,
        "computed_by": "lb-score-financial",
    }
    if ids:
        body["covered"] = {f"{name}_snapshot": ids.get(name) for name, _ in COVERED_SNAPSHOTS}
    _write_text(spark, _json_uri(output), _json.dumps(body))
    log(f"Wrote recall.json sidecar: {_json_uri(output)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute recall + FP rate from manifest + alerts")
    parser.add_argument("--manifest", required=True, help="S3 URI to manifest.parquet")
    parser.add_argument("--output", required=True, help="S3 URI for recall.parquet")
    for name, table in COVERED_SNAPSHOTS:
        parser.add_argument(
            f"--covered-{name}-snapshot",
            default=None,
            help=f"Covered mode: {table} snapshot of the last completed tick",
        )
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
        ids = covered_snapshot_ids(args)
    except NotScored as e:
        _write_not_scored(spark, args.output, str(e), None)
        spark.stop()
        return
    if ids is not None:
        run_covered(spark, args, manifest, ids)
        spark.stop()
        return

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
    check_status_run(current_run_id, os.environ.get("LB_RUN_ID", "").strip())
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
            # The manifest's expected_workload is the generator's AML
            # category (datagen_rs typology.rs Spec.workload: "W2_structuring"
            # for stack, dormant_reactivation, random, ...), not the rule that
            # detects the typology; designated_rules (RULE_TARGET_TYPOLOGY,
            # AML-GOALS #27) is. It is reported under a name that says so.
            "workload_category": r.get("workload_category"),
            "designated_rules": r.get("designated_rules"),
            "recall": r.get("recall"),
            "incidental_recall": r.get("incidental_recall"),
            "instance_count": r.get("instance_count"),
            "detection_status": r.get("detection_status"),
            "subjects_not_customer": (
                (check.get("by_typology") or {}).get(r.get("typology_type"), {})
            ).get("not_customer"),
            # Designated rules with an alert cut by an evidence cap: this
            # recall is bounded by that Lakebench-imposed cap.
            "bounded_by_evidence_cap": summary["recall_bounded_by_evidence_cap"].get(
                r.get("typology_type"), []
            ),
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
