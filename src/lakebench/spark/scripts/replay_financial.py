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

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
# Same W1 vertex cap the batch gold_finalize path honours (LB-119). Threaded
# into rules that accept it so replaying W1 uses the configured cap, not the
# rule's hard-coded default -- otherwise replay and gold_finalize disagree on
# whether W1 runs for a 5M-8M-vertex snapshot.
try:
    _W1_MAX_VERTICES = int(env("LB_FINANCIAL_W1_MAX_VERTICES", "8000000"))
except ValueError:
    _W1_MAX_VERTICES = 8_000_000


def resolve_snapshot_id(spark, catalog: str, table: str, depth_months: int) -> int:
    # Use ISO-formatted date with an explicit strftime rather than
    # datetime.isoformat(), which produces "2026-09-19T15:30:00+00:00"
    # -- Spark SQL rejects both the 'T' and the '+00:00' inside a
    # TIMESTAMP literal. Format as "YYYY-MM-DD HH:MM:SS" instead.
    # 30.436875 days/month is astronomically closer to the calendar
    # mean than the naive 30.5, keeping deep-history replays inside the
    # right retention window (the Iceberg retention_workload keeper
    # trims by calendar months, not day-count months).
    target = datetime.now(timezone.utc) - timedelta(days=depth_months * 30.436875)
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
        "evidence MAP<STRING, STRING>, "
        "detected_ts TIMESTAMP"
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
        "--output-alerts",
        required=True,
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
    from detection_rules import RuleSkipped, cleanup_w1_checkpoints, get_rule, known_rules

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
    # Thread the configured W1 vertex cap; the signature filter below drops
    # it for rules that don't accept it, so this is safe for every rule.
    if _W1_MAX_VERTICES > 0:
        kwargs["max_vertices"] = _W1_MAX_VERTICES
    # Filter by the rule's real SIGNATURE parameters, not the code object's
    # local-variable names (which also include function-body locals).
    # Matches the primitive gold_finalize deliberately uses, so a future
    # rule whose internal local collides with a kwarg name can't get the
    # value mis-injected here.
    import inspect

    _params = inspect.signature(rule_fn).parameters
    try:
        alerts = rule_fn(historical, **{k: v for k, v in kwargs.items() if k in _params})
    except RuleSkipped as skip:
        # A structural skip (e.g. W1 above its vertex cap on a large
        # historical snapshot) is not a replay failure. Log it
        # distinguishably and exit 0 so the K8s Job is not marked failed;
        # the target table keeps whatever rows it already had for this rule.
        log(f"Rule skipped: reason={skip.reason} detail={skip.detail}")
        cleanup_w1_checkpoints(spark)
        spark.stop()
        sys.exit(0)
    except Exception as e:  # noqa: BLE001
        log(f"Rule execution failed: {e}")
        sys.exit(4)

    alert_count = alerts.count()
    log(f"Rule produced {alert_count} alert rows")
    # Bootstrap the target with the full alerts schema + partition spec
    # (months(alert_ts)) via CREATE IF NOT EXISTS. Preserves partitioning
    # on repeat runs.
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
            evidence           MAP<STRING, STRING>,
            detected_ts        TIMESTAMP
        ) USING iceberg PARTITIONED BY (months(alert_ts))
        TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
    """)
    # LB-125 upgrade guard: rules now emit detected_ts, so a pre-existing
    # target table (reused catalog, or gold.alerts created before detected_ts)
    # must gain the column or the append below fails on schema mismatch. Check
    # the live schema and add only when missing -- Spark/Iceberg has no
    # `ADD COLUMN IF NOT EXISTS` for columns.
    try:
        _cols = [f.name for f in spark.table(args.output_alerts).schema.fields]
        if "detected_ts" not in _cols:
            spark.sql(f"ALTER TABLE {args.output_alerts} ADD COLUMNS (detected_ts TIMESTAMP)")
            log(f"[startup] added detected_ts to {args.output_alerts} (reused-catalog upgrade)")
    except Exception as e:  # noqa: BLE001
        log(f"[startup] detected_ts upgrade check on {args.output_alerts} skipped: {e}")
    # Delete-then-append scoped to THIS rule's rows. Multiple rules coexist
    # in the same alerts table keyed by rule_id (W2, W3, W4 append side by
    # side). Re-running the same rule replaces only its own rows, not
    # other rules'. score_financial reads all rows and joins by uetr.
    spark.sql(f"DELETE FROM {args.output_alerts} WHERE rule_id = '{args.rule}'")
    if alert_count > 0:
        alerts.writeTo(args.output_alerts).append()
    log(f"Wrote {args.output_alerts} (rule={args.rule} rows={alert_count})")
    cleanup_w1_checkpoints(spark)
    spark.stop()


if __name__ == "__main__":
    main()
