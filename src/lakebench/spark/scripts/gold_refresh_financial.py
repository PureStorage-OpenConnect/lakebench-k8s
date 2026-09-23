"""Gold Refresh (Financial, continuous) -- baseline rollup + periodic detection.

Runs the gold stage of the continuous FAML pipeline (bronze_ingest ->
silver_stream -> this) on a periodic timer (LB_FINANCIAL_GOLD_REFRESH_S
seconds). Each tick does two things against the moving silver corpus:

1. Refresh the daily_dashboards baseline rows (rule_id='baseline').
2. Re-run the detection rules over the FULL silver corpus and rewrite their
   alerts into gold.alerts.

Before LB-127 this script refreshed only the baseline dashboard and ran NO
detection: a continuous FAML run produced zero alerts, nothing to score, and
(with the LB-044 honest-runner gap) could still report PASS. This is the
continuous half of the detection spine.

Why full re-detection each tick, not a sliding window (LB-127, post-review):
An earlier design scanned a data-clock window [max(txn_timestamp) - w, max]
each tick. Adversarial review found two fatal flaws: (a) the corpus is a
static historical dataset trickled onto bronze OUT OF event-time order, so
max(txn_timestamp) pins to the corpus end almost immediately and every window
anchors there, leaving older typologies permanently unscanned -> silent recall
loss vs batch; and (b) the content-hash alert_id keyed on the in-window txn
SET drifts as the window slides, so the same episode produced many alert rows.
Re-running the WHOLE corpus each tick and rewriting per rule (the batch
DELETE-per-rule + INSERT path, reused verbatim via run_detection_rules)
removes both: full recall, and DELETE-then-INSERT makes each tick's alerts the
single correct set for that rule regardless of how the txn set grew during
ingestion. It is more expensive per tick (bounded by the batch detection
cost), which is the honest cost of keeping gold fresh under a growing corpus.

Rule set in continuous mode:
- RUN: W2/W3/W4 -- they need only silver.transactions, which silver_stream
  maintains.
- SKIPPED (recorded as status='skipped' so score renders "not run", not a
  false 0%): W1 (per-tick connected-components recompute is too costly; the
  windowed-recompute variant is Phase 5b), W7 (needs silver.entities, which
  silver_stream does not maintain -- Phase 5a builds continuous dimensions),
  W8 (needs a >=90-day dormancy gap a narrow continuous corpus cannot hold).

detected_ts semantics in continuous (LB-127): because each tick rewrites a
rule's alerts, detected_ts carries the LAST re-detection time, not the first.
That is sufficient for Phase 3 (alerts exist, recall is scorable); the
first-detection-preserving time-to-detect / freshness measurement is Phase 4.

Refresh discipline:
- Baseline overwrites ONLY rule_id='baseline' rows (DELETE + append); the
  detection rows (other rule_ids) written the same tick are never wiped.
- SIGTERM/SIGINT triggers a clean loop exit rather than a kubelet SIGKILL.
- After MAX_CONSECUTIVE_FAILURES consecutive whole-tick failures the script
  exits non-zero so K8s Job status reflects reality (LB-044). A single
  misbehaving rule is isolated inside run_detection_rules and does NOT count
  as a tick failure.
"""

from __future__ import annotations

import signal
import sys
import time
import uuid

from common import env, log
from gold_finalize_financial import (
    DDL_ALERTS,
    DDL_CLUSTERS,
    DDL_DASH,
    DDL_RISK,
    DDL_STATUS,
    build_baseline_dashboards,
    run_detection_rules,
)
from pyspark.sql import SparkSession

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
GOLD_DASH = env("LB_FINANCIAL_GOLD_DASHBOARDS", "gold.daily_dashboards")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
REFRESH_S = int(env("LB_FINANCIAL_GOLD_REFRESH_S", "60"))
RUN_ID = env("LB_RUN_ID", str(uuid.uuid4()))
MAX_CONSECUTIVE_FAILURES = int(env("LB_FINANCIAL_GOLD_MAX_FAILS", "5"))

# Rules run every tick over the full corpus (need only silver.transactions).
CONTINUOUS_RULES = (
    "W2_structuring",
    "W3_round_tripping",
    "W4_risk_propagation",
)
# Rules deliberately not run in continuous mode, recorded as 'skipped' so
# their typologies render "not run" rather than a false 0% recall.
CONTINUOUS_SKIPPED_RULES = (
    "W1_connected_components",
    "W7_cross_border_high_risk",
    "W8_dormant_reactivation",
)

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


def _bootstrap_gold_tables(spark) -> None:
    """Create ALL gold tables the tick touches if absent, and add detected_ts
    to a pre-LB-125 reused gold.alerts.

    In continuous mode gold_finalize NEVER runs, so this script owns the full
    gold-table bootstrap -- not just alerts. The tick issues DELETE FROM
    gold.daily_dashboards (baseline refresh) and run_detection_rules issues
    DELETE FROM gold.alerts + writeTo overwrite on gold.risk_scores /
    gold.entity_clusters (derived projections), all of which require the
    tables to exist. Creating only alerts+status (the earlier bug) left a
    fresh continuous catalog failing every tick on a missing daily_dashboards
    and silently failing the derived projections. Mirrors the exact DDL set
    gold_finalize_financial.main() bootstraps so both modes converge on one
    schema. All CREATE TABLE IF NOT EXISTS -- idempotent on restart.
    """
    for ddl in (DDL_ALERTS, DDL_RISK, DDL_CLUSTERS, DDL_DASH, DDL_STATUS):
        spark.sql(ddl)
    try:
        cols = [f.name for f in spark.table(f"{CATALOG}.{GOLD_ALERTS}").schema.fields]
        if "detected_ts" not in cols:
            spark.sql(f"ALTER TABLE {CATALOG}.{GOLD_ALERTS} ADD COLUMNS (detected_ts TIMESTAMP)")
            log(f"[startup] added detected_ts to {GOLD_ALERTS} (reused-catalog upgrade)")
    except Exception as e:  # noqa: BLE001
        log(f"[startup] detected_ts upgrade check on {GOLD_ALERTS} skipped: {e}")


def main() -> None:
    _install_signal_handlers()
    spark = SparkSession.builder.appName("lb-gold-refresh-financial").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    log("=" * 60)
    log("Gold Refresh (Financial) -- baseline + periodic detection")
    log(f"Refresh interval: {REFRESH_S}s   Run ID: {RUN_ID}")
    log(f"Detection rules: run={list(CONTINUOUS_RULES)} skipped={list(CONTINUOUS_SKIPPED_RULES)}")
    log(f"Max consecutive failures: {MAX_CONSECUTIVE_FAILURES}")
    log("=" * 60)

    _bootstrap_gold_tables(spark)

    consecutive_failures = 0

    while not _SHUTDOWN:
        tick = time.time()
        try:
            txns = spark.table(f"{CATALOG}.{SILVER_TXNS}")

            # Baseline: overwrite ONLY rule_id='baseline' rows so the detection
            # rows written below (other rule_ids) are preserved. DELETE +
            # append are two Iceberg commits; a crash between them leaves one
            # tick without baseline rows, refreshed next cycle -- acceptable.
            baseline = build_baseline_dashboards(txns, RUN_ID)
            spark.sql(f"DELETE FROM {CATALOG}.{GOLD_DASH} WHERE rule_id = 'baseline'")
            baseline.writeTo(f"{CATALOG}.{GOLD_DASH}").append()

            # Detection: reuse the batch driver over the full corpus. Per-rule
            # DELETE + INSERT makes this tick's alerts the single correct set
            # for each rule; per-rule errors are isolated inside the driver, so
            # only a failure to read silver at all raises up to here.
            run_detection_rules(
                spark,
                txns,
                RUN_ID,
                rules=CONTINUOUS_RULES,
                skipped_rules=CONTINUOUS_SKIPPED_RULES,
            )

            total_alerts = int(spark.table(f"{CATALOG}.{GOLD_ALERTS}").count())
            elapsed = time.time() - tick
            log(f"[detection] cumulative gold.alerts rows: {total_alerts}")
            log(f"Tick complete in {elapsed:.1f}s (gold.alerts rows: {total_alerts})")
            consecutive_failures = 0
        except Exception as e:  # noqa: BLE001
            consecutive_failures += 1
            log(f"Tick failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}): {e}")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log(
                    f"Aborting: {MAX_CONSECUTIVE_FAILURES} consecutive tick "
                    "failures; the continuous-mode monitor should restart or a "
                    "real defect needs investigation."
                )
                spark.stop()
                sys.exit(1)

        # Sleep in short steps so a signal is honoured within ~1s.
        remaining = max(0.0, REFRESH_S - (time.time() - tick))
        while remaining > 0 and not _SHUTDOWN:
            step = min(1.0, remaining)
            time.sleep(step)
            remaining -= step

    log("SIGTERM/SIGINT received; refresh loop exiting cleanly")
    spark.stop()


if __name__ == "__main__":
    main()
