"""Reproduce (Financial, W10) -- reproduce a specific past alert via time-travel.

Given a historical alert (identified by alert_id or txn UETR), reads the
silver snapshot at the alert's original commit timestamp, re-runs the rule
that generated it, asserts the reproduced alert matches the original.
"""

from __future__ import annotations

import argparse
import sys

from common import env, log
from pyspark.sql import SparkSession

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")


def main() -> None:
    parser = argparse.ArgumentParser(description="Financial alert reproduction via time-travel")
    parser.add_argument("--alert-id", required=True, help="Original alert_id to reproduce")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"lb-reproduce-financial-{args.alert_id}").getOrCreate()
    log("=" * 60)
    log(f"Reproducing alert {args.alert_id}")
    log("=" * 60)

    alert = spark.sql(
        f"SELECT * FROM {CATALOG}.{GOLD_ALERTS} WHERE alert_id = '{args.alert_id}' LIMIT 1"
    ).collect()
    if not alert:
        log(f"Alert not found: {args.alert_id}")
        sys.exit(2)
    row = alert[0]
    log(f"Alert rule={row.rule_id} entity={row.entity_id} ts={row.alert_ts}")

    # Iceberg time-travel: read silver as of the alert timestamp.
    ts = row.alert_ts.isoformat()
    log(f"Reading silver at TIMESTAMP AS OF '{ts}'")
    historical = spark.sql(
        f"SELECT * FROM {CATALOG}.{SILVER_TXNS} FOR TIMESTAMP AS OF TIMESTAMP '{ts}'"
    )
    log(f"Historical silver rows: {historical.count():,}")

    # v1: emit a smoke record proving snapshot resolution + time-travel work.
    # Full rule replay + bit-identity check lands with the detection modules
    # under ENG-2C.4.*.
    log("Reproduction plumbing verified; full rule replay pending ENG-2C.4.* landing")

    spark.stop()


if __name__ == "__main__":
    main()
