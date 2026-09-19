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

    # Rule dispatch. Rule functions in detection_rules.py accept a silver
    # DataFrame + params and return a gold.alerts-shaped DataFrame.
    from detection_rules import get_rule, known_rules

    rule_fn = get_rule(args.rule)
    if rule_fn is None:
        log(f"Unknown rule {args.rule!r}; known rules: {known_rules()}")
        sys.exit(2)

    replay_run_id = str(uuid.uuid4())
    log(f"Running {args.rule} with run_id={replay_run_id}")

    # Rule functions accept keyword args -- pass threshold when provided
    # (rule signature varies but all accept run_id).
    kwargs = {"run_id": replay_run_id}
    if args.threshold is not None:
        # w2_structuring interprets threshold as count. Others may ignore.
        kwargs["threshold_count"] = int(args.threshold)
    try:
        alerts = rule_fn(historical, **{
            k: v for k, v in kwargs.items()
            if k in rule_fn.__code__.co_varnames
        })
    except Exception as e:  # noqa: BLE001
        log(f"Rule execution failed: {e}")
        sys.exit(4)

    alert_count = alerts.count()
    log(f"Rule produced {alert_count} alert rows")
    # Bootstrap the replay target with the full alerts schema + partition
    # spec (days(alert_ts)) via CREATE IF NOT EXISTS so the subsequent
    # overwrite preserves partitioning. createOrReplace() on the writer
    # would reset partitioning on every run (silent regression -- same
    # defect class silver_build/gold_finalize fixed). Idempotent.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {args.output_alerts} (
            alert_id           STRING NOT NULL,
            rule_id            STRING NOT NULL,
            rule_version       STRING NOT NULL,
            model_id           STRING NOT NULL,
            model_version      STRING NOT NULL,
            entity_id          BIGINT NOT NULL,
            related_txn_ids    ARRAY<STRING>,
            related_entity_ids ARRAY<BIGINT>,
            alert_ts           TIMESTAMP NOT NULL,
            alert_score        DOUBLE,
            priority           STRING,
            status             STRING,
            disposition        STRING,
            alert_type         STRING,
            run_id             STRING NOT NULL,
            narrative          STRING,
            evidence           MAP<STRING, STRING>
        ) USING iceberg PARTITIONED BY (days(alert_ts))
        TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
    """)
    alerts.writeTo(args.output_alerts).overwrite(lit(True))
    log(f"Wrote {args.output_alerts}")
    spark.stop()


if __name__ == "__main__":
    main()
