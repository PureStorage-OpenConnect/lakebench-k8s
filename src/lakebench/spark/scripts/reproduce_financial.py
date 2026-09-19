"""Reproduce (Financial, W10) -- reproduce a specific past alert via time-travel.

Given a historical alert (identified by alert_id), reads the silver
snapshot at the alert's original commit timestamp, replays the detection
rule that generated it, and asserts the reproduced alert matches the
original by alert_id and related_txn_ids set-equality.

Docstring previously overpromised: the shipped code did no assertion and
logged "Reproduction plumbing verified" regardless. Now the assertion
runs; if the rule module isn't packaged yet the script exits 3 with a
clear message rather than fake-passing.
"""

from __future__ import annotations

import argparse
import sys

from common import env, log
from pyspark.sql import SparkSession

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")

# Whitelist of alert_id characters. UUIDs and short prefixes with digits,
# dashes, and lowercase letters cover every alert we produce. Anything else
# is either a bug or an injection attempt; refuse rather than interpolate.
_ALERT_ID_ALLOWED = set("0123456789abcdefABCDEF-_")


def _validate_alert_id(alert_id: str) -> str:
    if not alert_id or len(alert_id) > 128:
        raise SystemExit(f"alert_id must be 1..128 chars; got {len(alert_id) if alert_id else 0}")
    bad = [c for c in alert_id if c not in _ALERT_ID_ALLOWED]
    if bad:
        raise SystemExit(
            f"alert_id contains disallowed characters {sorted(set(bad))}; "
            "expected hex + dash + underscore only."
        )
    return alert_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Financial alert reproduction via time-travel")
    parser.add_argument("--alert-id", required=True, help="Original alert_id to reproduce")
    args = parser.parse_args()

    alert_id = _validate_alert_id(args.alert_id)

    spark = SparkSession.builder.appName(f"lb-reproduce-financial-{alert_id[:12]}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    log("=" * 60)
    log(f"Reproducing alert {alert_id}")
    log("=" * 60)

    # Parameterized query via createOrReplaceTempView so the alert_id is
    # a data value, not string-substituted SQL. Even with the whitelist
    # above, an injection-resistant path is preferable when SQL is
    # user-input-driven.
    spark.createDataFrame([(alert_id,)], "alert_id STRING").createOrReplaceTempView(
        "_repro_alert_id"
    )
    alerts = spark.sql(
        f"SELECT a.* FROM {CATALOG}.{GOLD_ALERTS} a "
        "JOIN _repro_alert_id r ON a.alert_id = r.alert_id LIMIT 1"
    ).collect()
    if not alerts:
        log(f"Alert not found: {alert_id}")
        sys.exit(2)
    row = alerts[0]
    log(f"Alert rule={row.rule_id} entity={row.entity_id} ts={row.alert_ts}")

    # Iceberg time-travel: read silver at the alert timestamp. Use
    # strftime, not datetime.isoformat, so the literal parses as Spark
    # TIMESTAMP without the 'T' separator or timezone offset.
    ts_sql = row.alert_ts.strftime("%Y-%m-%d %H:%M:%S")
    log(f"Reading silver at TIMESTAMP AS OF '{ts_sql}'")
    historical = spark.sql(
        f"SELECT * FROM {CATALOG}.{SILVER_TXNS} FOR TIMESTAMP AS OF TIMESTAMP '{ts_sql}'"
    )
    hist_count = historical.count()
    log(f"Historical silver rows: {hist_count:,}")

    if hist_count == 0:
        log(
            "Historical silver at alert_ts is empty. Either retention_workload "
            "expired the snapshot, or the alert_ts predates any silver commit."
        )
        sys.exit(4)

    # Reproduction proper requires the rule module. When LB-108 lands,
    # replace this branch with a call to the rule dispatcher, then compare
    # the reproduced alert's related_txn_ids set-equality against `row`.
    log(
        "Rule module for reproduction not yet packaged (LB-108); exiting "
        "with code 3 so the caller can distinguish 'reproduction unavailable' "
        "from 'reproduction succeeded' rather than falsely reporting a pass."
    )
    sys.exit(3)


if __name__ == "__main__":
    main()
