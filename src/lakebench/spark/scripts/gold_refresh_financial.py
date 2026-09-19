"""Gold Refresh (Financial, sustained) -- periodic dashboard rollup.

Recomputes the daily_dashboards baseline against the moving silver
transactions on a periodic timer (LB_FINANCIAL_GOLD_REFRESH_S seconds).
Detection workloads own alerts / risk_scores / entity_clusters -- this
script only refreshes the aggregate dashboard rows keyed
`rule_id = 'baseline'`.

Refresh discipline:
- Overwrites ONLY the baseline rows (rule_id='baseline'). Detection
  workloads writing rows under other rule_ids in the same 60s window
  used to be wiped by the prior createOrReplace() every tick -- silent
  data loss on the exact rows the scorer joins against.
- SIGTERM/SIGINT triggers a clean loop exit rather than kubelet SIGKILL,
  matching the datagen shutdown discipline.
- A failed refresh does NOT silently swallow-and-sleep: failures are
  counted, and after 5 consecutive failures the script exits non-zero
  so K8s Job status reflects reality (LB-044 pattern).
"""

from __future__ import annotations

import signal
import sys
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
MAX_CONSECUTIVE_FAILURES = int(env("LB_FINANCIAL_GOLD_MAX_FAILS", "5"))

_SHUTDOWN = False


def _install_signal_handlers() -> None:
    def _handler(signum, frame):  # noqa: ARG001
        global _SHUTDOWN
        _SHUTDOWN = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handler)
        except ValueError:
            pass


def main() -> None:
    _install_signal_handlers()
    spark = SparkSession.builder.appName("lb-gold-refresh-financial").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    log("=" * 60)
    log("Gold Refresh (Financial)")
    log(f"Refresh interval: {REFRESH_S}s   Run ID: {RUN_ID}")
    log(f"Max consecutive failures: {MAX_CONSECUTIVE_FAILURES}")
    log("=" * 60)

    consecutive_failures = 0

    while not _SHUTDOWN:
        tick = time.time()
        try:
            txns = spark.table(f"{CATALOG}.{SILVER_TXNS}")
            baseline = build_baseline_dashboards(txns, RUN_ID)
            # Delete then append: overwrite ONLY the baseline rows so
            # detection workloads writing other rule_ids in the same 60s
            # window are preserved. Prior createOrReplace wiped everything
            # every tick (silent data loss of the exact rows the scorer
            # joins against). DELETE + append are two separate Iceberg
            # commits; a crash between them would leave the table without
            # baseline rows for one tick, which then refreshes on the
            # next cycle -- acceptable staleness bound vs the prior loss.
            spark.sql(
                f"DELETE FROM {CATALOG}.{GOLD_DASH} WHERE rule_id = 'baseline'"
            )
            baseline.writeTo(f"{CATALOG}.{GOLD_DASH}").append()
            elapsed = time.time() - tick
            log(f"Refreshed dashboards in {elapsed:.1f}s")
            consecutive_failures = 0
        except Exception as e:  # noqa: BLE001
            consecutive_failures += 1
            log(f"Refresh failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}): {e}")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log(
                    f"Aborting: {MAX_CONSECUTIVE_FAILURES} consecutive "
                    "refresh failures; the sustained-mode monitor should "
                    "restart or a real defect needs investigation."
                )
                spark.stop()
                sys.exit(1)

        # Sleep in short intervals so shutdown is detected within ~1s of
        # a signal rather than waiting the full REFRESH_S.
        remaining = max(0.0, REFRESH_S - (time.time() - tick))
        while remaining > 0 and not _SHUTDOWN:
            step = min(1.0, remaining)
            time.sleep(step)
            remaining -= step

    log("SIGTERM/SIGINT received; refresh loop exiting cleanly")
    spark.stop()


if __name__ == "__main__":
    main()
