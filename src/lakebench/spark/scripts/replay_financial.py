"""Replay (Financial, W8) -- rerun a detection rule against a past snapshot.

Reads the Iceberg snapshot closest to but not after (now - depth_months)
via time-travel, runs the requested detection rule against that historical
silver state, writes alerts to the specified output table.

Rule modules ship alongside the workload scripts (LB-108, deferred). Until
those land, the replay path writes an empty alerts frame WITH THE FULL
gold.alerts SCHEMA so downstream scoring can join on `related_txn_ids`
without an AnalysisException. Callers using this against `gold.alerts` (as
opposed to `gold.alerts_replay`) get a controlled empty state, not a
schema truncation of the real table.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timedelta, timezone

from common import env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")


def resolve_snapshot_id(spark, catalog: str, table: str, depth_months: int) -> int:
    # Use ISO-formatted date with an explicit strftime rather than
    # datetime.isoformat(), which produces "2026-09-19T15:30:00+00:00"
    # -- Spark SQL rejects both the 'T' and the '+00:00' inside a
    # TIMESTAMP literal. Format as "YYYY-MM-DD HH:MM:SS" instead.
    # 30.436875 days/month is astronomically closer to the calendar
    # mean than the naive 30.5, keeping deep-history replays inside the
    # right retention window (the Iceberg retention_workload keeper
    # trims by calendar months, not day-count months).
    target = datetime.now(timezone.utc) - timedelta(
        days=depth_months * 30.436875
    )
    target_sql = target.strftime("%Y-%m-%d %H:%M:%S")
    result = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {catalog}.{table}.snapshots
        WHERE committed_at <= TIMESTAMP '{target_sql}'
        ORDER BY committed_at DESC
        LIMIT 1
        """
    ).collect()
    if not result:
        raise SystemExit(
            f"No snapshot at depth {depth_months} months; "
            "retention_workload may be off or maintenance expired too aggressively."
        )
    return result[0].snapshot_id


def _empty_alerts_df(spark):
    """Empty gold.alerts-shaped DataFrame. Schema mirrors
    gold_finalize_financial.py DDL_ALERTS exactly so a
    writeTo(...).createOrReplace() on the alerts table preserves the
    downstream contract with score_financial (related_txn_ids field
    must exist even when empty)."""
    schema = (
        "alert_id STRING, "
        "rule_id STRING, "
        "rule_version STRING, "
        "model_id STRING, "
        "model_version STRING, "
        "entity_id BIGINT, "
        "related_txn_ids ARRAY<STRING>, "
        "related_entity_ids ARRAY<BIGINT>, "
        "alert_ts TIMESTAMP, "
        "alert_score DOUBLE, "
        "priority STRING, "
        "status STRING, "
        "disposition STRING, "
        "alert_type STRING, "
        "run_id STRING, "
        "narrative STRING, "
        "evidence MAP<STRING, STRING>"
    )
    return spark.createDataFrame([], schema)


def main() -> None:
    parser = argparse.ArgumentParser(description="Financial rule replay against past snapshot")
    parser.add_argument("--rule", required=True, help="Rule id, e.g. W2_structuring")
    parser.add_argument("--depth-months", type=int, required=True, help="Snapshot depth in months")
    parser.add_argument(
        "--threshold", type=float, default=None, help="Rule-specific threshold override"
    )
    parser.add_argument(
        "--output-alerts", required=True,
        help="Fully-qualified output alerts table (must be catalog.namespace.table)",
    )
    args = parser.parse_args()

    # Refuse an unqualified table like 'gold.alerts_replay'. Under Delta+Hive
    # the session default catalog is DeltaCatalog, so an unqualified write
    # fails at commit with "not an Iceberg table" -- the error is opaque and
    # arrives after the replay has already run. Require operators to spell
    # the catalog explicitly.
    if args.output_alerts.count(".") < 2:
        raise SystemExit(
            f"--output-alerts must be catalog.namespace.table; got {args.output_alerts!r}. "
            "For Iceberg REST, pass e.g. 'lakehouse.gold.alerts_replay'."
        )

    spark = SparkSession.builder.appName(f"lb-replay-financial-{args.rule}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    log("=" * 60)
    log(f"Financial replay: rule={args.rule} depth={args.depth_months}mo")
    log(f"Threshold: {args.threshold}   Output: {args.output_alerts}")
    log("=" * 60)

    snap = resolve_snapshot_id(spark, CATALOG, SILVER_TXNS, args.depth_months)
    log(f"Resolved snapshot: {snap}")

    historical = spark.read.option("snapshot-id", snap).table(f"{CATALOG}.{SILVER_TXNS}")
    historical_count = historical.count()
    log(f"Historical silver rows: {historical_count:,}")

    # Rule dispatch. Detection modules land under LB-108; until then the
    # replay path exercises end-to-end retention/time-travel plumbing by
    # writing an empty alerts frame with the correct FULL schema so caller
    # assertions and score_financial's join don't blow up.
    if args.rule not in ("W2_structuring", "W3_round_tripping", "W4_risk_propagation"):
        log(f"Unknown rule: {args.rule}")
        sys.exit(2)

    log("Rule module not yet packaged in this build; writing empty alert frame")
    empty = _empty_alerts_df(spark)
    # Fill run_id so downstream can at least distinguish which replay ran.
    replay_run_id = str(uuid.uuid4())
    empty = empty.withColumn("run_id", lit(replay_run_id))
    empty = empty.withColumn("alert_ts", current_timestamp())
    empty.writeTo(args.output_alerts).createOrReplace()

    log(f"Replay output: {args.output_alerts} (rows: {empty.count()})")
    spark.stop()


if __name__ == "__main__":
    main()
