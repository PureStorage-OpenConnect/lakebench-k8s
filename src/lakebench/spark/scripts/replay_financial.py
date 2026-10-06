"""Replay (Financial, W8) -- rerun a detection rule against a past snapshot.

Reads the Iceberg snapshot closest to but not after (now - depth_months)
via time-travel, runs the requested detection rule against that historical
silver state, writes alerts to the specified output table.

Rule modules ship alongside the workload scripts (deferred). Until
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

from common import (
    ICEBERG_V2_SNAPPY_PROPS_SQL,
    ensure_alert_columns,
    env,
    log,
    sealed_txns_filter,
)
from pyspark.sql import SparkSession

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_BATCH_VERSIONS = env("LB_FINANCIAL_SILVER_BATCH_VERSIONS", "silver.silver_batch_versions")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")


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


def read_at_snapshot_id(spark, fq_table: str, snapshot_id: int):
    """``fq_table`` as of Iceberg snapshot ``snapshot_id``.

    SQL ``VERSION AS OF``: Iceberg 1.11 removed the ``snapshot-id`` read
    option, so ``spark.read.option("snapshot-id", ...)`` failed before any
    rule ran on Spark 4.x with the default Iceberg.
    """
    return spark.sql(f"SELECT * FROM {fq_table} VERSION AS OF {int(snapshot_id)}")


def _empty_alerts_df(spark):
    """Empty gold.alerts-shaped DataFrame (detection_rules.ALERT_COLUMNS)."""
    from detection_rules import ALERT_COLUMNS

    schema = ", ".join(f"{name} {ddl_type}" for name, ddl_type, _ in ALERT_COLUMNS)
    return spark.createDataFrame([], schema)


def _target_ddl(fq_table: str) -> str:
    """CREATE TABLE IF NOT EXISTS for the replay target: gold.alerts'
    columns (detection_rules.ALERT_COLUMNS) and partitioning."""
    from detection_rules import ALERT_COLUMNS

    cols = ",\n".join(
        f"    {name:<18} {ddl_type}{'' if nullable else ' NOT NULL'}"
        for name, ddl_type, nullable in ALERT_COLUMNS
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {fq_table} (\n{cols}\n) USING iceberg "
        f"PARTITIONED BY (months(alert_ts))\nTBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})"
    )


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

    historical_raw = read_at_snapshot_id(spark, f"{CATALOG}.{SILVER_TXNS}", snap)
    # I10: hide mid-batch crash rows from the historical replay too. A batch
    # whose transactions committed but whose versions row never landed must
    # not surface as a "detectable" window during a later replay. The
    # versions table is joined at CURRENT state -- by the time replay runs
    # (months back), the crashed batch is either sealed later or never; if
    # never, we correctly hide those rows from the rule.
    historical = sealed_txns_filter(spark, historical_raw, CATALOG, SILVER_BATCH_VERSIONS)
    historical_count = historical.count()
    log(f"Historical silver rows: {historical_count:,}")

    # Rule dispatch. Rule functions in detection_rules.py accept a silver
    # DataFrame + params and return a gold.alerts-shaped DataFrame.
    from detection_rules import (
        RuleSkipped,
        cleanup_w1_checkpoints,
        get_rule,
        known_rules,
        rule_params,
    )

    rule_fn = get_rule(args.rule)
    if rule_fn is None:
        log(f"Unknown rule {args.rule!r}; known rules: {known_rules()}")
        sys.exit(2)

    replay_run_id = str(uuid.uuid4())
    log(f"Running {args.rule} with run_id={replay_run_id}")

    # The parameters gold-finalize uses (detection_rules.rule_params: run_id,
    # silver.entities for the customer-scoped rules, the configured W1
    # vertex cap), plus the threshold override when given. Filtered by the
    # rule's real signature, never its local names.
    try:
        silver_entities = spark.table(f"{CATALOG}.{SILVER_ENTITIES}")
    except Exception as e:  # noqa: BLE001 -- customer-scoped rules then skip
        log(f"silver.entities not readable ({e}); customer-scoped rules will skip")
        silver_entities = None
    kwargs = rule_params(rule_fn, replay_run_id, silver_entities)
    if args.threshold is not None:
        # w2_structuring interprets threshold as count. Others may ignore.
        kwargs["threshold_count"] = int(args.threshold)
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

    # Persisted so the count and the append below share one computation of
    # the rule (without it the rule ran twice).
    from pyspark import StorageLevel

    alerts = alerts.persist(StorageLevel.MEMORY_AND_DISK)
    alert_count = alerts.count()
    log(f"Rule produced {alert_count} alert rows")
    # Bootstrap the target with the full alerts schema + partition spec
    # (months(alert_ts)) via CREATE IF NOT EXISTS. Preserves partitioning
    # on repeat runs.
    spark.sql(_target_ddl(args.output_alerts))
    # Reused target (an older replay table): missing trailing columns are
    # appended; a table whose columns differ otherwise fails here.
    from detection_rules import ALERT_COLUMNS

    ensure_alert_columns(spark, args.output_alerts, ALERT_COLUMNS)
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
