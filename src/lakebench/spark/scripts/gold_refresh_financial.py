"""Gold Refresh (Financial, sustained) -- periodic dashboard rollup.

Recomputes the daily_dashboards baseline against the moving silver
transactions on a periodic timer (LB_FINANCIAL_GOLD_REFRESH_S seconds).
Detection workloads own alerts / risk_scores / entity_clusters -- this
script only refreshes the aggregate dashboard rows.
"""

from __future__ import annotations

import time
import uuid

from common import env, log
from gold_finalize_financial import build_baseline_dashboards
from pyspark.sql import SparkSession

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
GOLD_DASH = env("LB_FINANCIAL_GOLD_DASHBOARDS", "gold.daily_dashboards")
REFRESH_S = int(env("LB_FINANCIAL_GOLD_REFRESH_S", "60"))
RUN_ID = env("LB_RUN_ID", str(uuid.uuid4()))


def main() -> None:
    spark = SparkSession.builder.appName("lb-gold-refresh-financial").getOrCreate()

    log("=" * 60)
    log("Gold Refresh (Financial)")
    log(f"Refresh interval: {REFRESH_S}s   Run ID: {RUN_ID}")
    log("=" * 60)

    while True:
        start = time.time()
        try:
            txns = spark.table(f"{CATALOG}.{SILVER_TXNS}")
            baseline = build_baseline_dashboards(txns)
            baseline.writeTo(f"{CATALOG}.{GOLD_DASH}").createOrReplace()
            log(f"Refreshed dashboards in {time.time() - start:.1f}s")
        except Exception as e:  # noqa: BLE001
            log(f"Refresh failed: {e}")
        time.sleep(REFRESH_S)


if __name__ == "__main__":
    main()
