"""Gold Refresh (Financial, continuous) -- baseline rollup + periodic detection.

Runs the gold stage of the continuous AML pipeline (bronze_ingest ->
silver_stream -> this) on a periodic timer (LB_FINANCIAL_GOLD_REFRESH_S
seconds). Each tick does two things against the moving silver corpus:

1. Re-run the detection rules over the FULL silver corpus and rewrite their
   alerts into gold.alerts.
2. Refresh the daily_dashboards baseline rows (rule_id='baseline').

Before LB-127 this script refreshed only the baseline dashboard and ran NO
detection: a continuous AML run produced zero alerts, nothing to score, and
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
- RUN: W2/W3/W4/W17. All read silver.transactions; W2 is customer-scoped and
  also reads silver.entities, which silver_stream appends per micro-batch
  (after the batch's transactions, so a tick can miss a new customer's alert
  until the next tick).
- SKIPPED (recorded as status='skipped' so score renders "not run", not a
  false 0%): W1 (per-tick connected-components recompute is too costly; the
  windowed-recompute variant is Phase 5b), W7 (skipped since before
  silver_stream appended silver.entities; that original reason no longer
  holds, and enabling it here has not been tested), W8 (needs a >=90-day dormancy gap a narrow continuous corpus cannot hold).

A rule that errors or skips on a tick loses the rows it wrote on earlier
ticks of the run, and its status for the tick is 'error' or 'skipped'. So a
transient failure on the last tick scores that rule's typologies as not run,
never as the previous tick's alerts under a status that says otherwise.

detected_ts semantics in continuous (LB-127): because each tick rewrites a
rule's alerts, detected_ts carries the LAST re-detection time, not the first.

Time to detect (Phase 4a): alert_id is a fresh uuid() on every tick, so first
detection is found by content instead. An alert is newly raised on a tick
when its (rule_id, entity_id, sorted related_txn_ids) was not in gold.alerts
at the snapshot before the tick. Its time to detect is the moment its rule's
INSERT into gold.alerts committed minus the newest bronze ingest_ts among its
related transactions, i.e. from the moment the last piece of its evidence
entered bronze to the moment the alert was visible in gold. The rule's
detected_ts is not used: it is stamped when the rule starts computing, so it
would leave the detection time itself out. Each tick logs a histogram
(common.ttd_line); the collector merges the ticks.

Tick order (lane U, run-20260925-135005-4b7a97: 1,280 s time to detect at
scale 10). Every tick is a full recompute, so what can be cut is what sits
between new evidence and its alert. The tick pins one silver snapshot of
transactions, then runs the rules cheapest first (W4 and W2 are single joins
and windows; W17 and W3 are multi-level path searches, about four fifths of
the detection pass on a local measurement), each committing its alerts as it
finishes, and refreshes the baseline dashboard after detection instead of
before it. Every rule still sees the whole pinned corpus, so its alerts are
the batch result for that snapshot (W2 also reads silver.entities live).
The time-to-detect measurement changed with it: an alert is measured at its
own rule's commit, not at the end of the pass. The ticks also log the old
pass-end histogram and one histogram per rule, so a run stays comparable with
earlier ones and a rule that now runs later (W3) shows as such. Each tick logs a phase breakdown ("Cycle N: tick timing
...") that the collector keeps per cycle.
An episode that grows gets a new content key, so each re-raise is measured
against the transaction that changed it. An alert re-raised after a rule
error, or raised late because its evidence lies outside related_txn_ids (W2
waits for the customer's entity row), is measured against its related
transactions and so reads as a long time to detect, never a short one.

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

from bronze_verify_financial import MANIFEST_TABLE, register_manifest
from common import (
    TTD_SNAPSHOT_UNKNOWN,
    TtdBaseline,
    ensure_namespaces_for_ddl,
    ensure_partition_transform,
    env,
    iceberg_table_stats,
    log,
    one_line,
    table_exists,
    ttd_line,
)
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
from pyspark.sql.functions import (
    array_join,
    array_sort,
    broadcast,
    coalesce,
    col,
    concat_ws,
    count,
    explode_outer,
    floor,
    greatest,
    lit,
    sha2,
    unix_micros,
    when,
)
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import sum as spark_sum
from pyspark.storagelevel import StorageLevel
from tm_operations import (
    bootstrap_tm_tables,
    params_from_env,
    read_at_snapshot,
    run_tm_operations,
    tm_pass_due,
    window_start_marker,
)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
GOLD_DASH = env("LB_FINANCIAL_GOLD_DASHBOARDS", "gold.daily_dashboards")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
REFRESH_S = int(env("LB_FINANCIAL_GOLD_REFRESH_S", "60"))
TM_PARAMS = params_from_env()
# The CLI's continuous window length (0: unknown), so the TM layer times a
# final pass before the window closes. The window's start is the first
# driver's start, persisted in this job's checkpoint (tm_operations
# .window_start_marker), so a restarted driver keeps it.
WINDOW_S = int(env("LB_CONTINUOUS_WINDOW_S", "0") or 0)
GOLD_CHECKPOINT = env("CHECKPOINT_LOCATION", "")
RUN_ID = env("LB_RUN_ID", str(uuid.uuid4()))
MAX_CONSECUTIVE_FAILURES = int(env("LB_FINANCIAL_GOLD_MAX_FAILS", "5"))
# Time-to-detect histogram resolution. Percentiles are reported at the bin's
# upper edge, so they overstate by less than one bin.
TTD_BIN_S = 10
# New-alert transaction rows up to which the silver lookup broadcasts them
# instead of shuffling silver (a tick normally raises a few thousand).
TTD_BROADCAST_ROWS = 2_000_000

# Rules run every tick over the full corpus. W2 is customer-scoped and also
# reads silver.entities; silver_stream commits a batch's entities after its
# transactions, so a tick can miss a new customer's alert, and the next tick's
# full re-detection raises it.
# Order: each rule's alerts are visible when its own write commits, so the
# single-join rules go before the path searches, and among those W17 (many
# alerts) before W3 (few), which makes W3's alerts wait on W17. Measured on a
# local 3.4M-row silver (lane U, warm tick): W4 8.8 s for 33,902 alerts, W2
# 5.5 s for 259, W17 32.8 s for 2,542, W3 29.1 s for 155. Rules are
# independent, so the order changes when alerts appear, never which.
CONTINUOUS_RULES = (
    "W4_risk_propagation",
    "W2_structuring",
    "W17_layering_chain",
    "W3_round_tripping",
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
    ensure_namespaces_for_ddl(
        spark, CATALOG, (DDL_ALERTS, DDL_RISK, DDL_CLUSTERS, DDL_DASH, DDL_STATUS)
    )
    for ddl in (DDL_ALERTS, DDL_RISK, DDL_CLUSTERS, DDL_DASH, DDL_STATUS):
        spark.sql(ddl)
    bootstrap_tm_tables(spark)
    ensure_partition_transform(
        spark, f"{CATALOG}.{GOLD_ALERTS}", "days(alert_ts)", "months(alert_ts)"
    )
    try:
        cols = [f.name for f in spark.table(f"{CATALOG}.{GOLD_ALERTS}").schema.fields]
        if "detected_ts" not in cols:
            spark.sql(f"ALTER TABLE {CATALOG}.{GOLD_ALERTS} ADD COLUMNS (detected_ts TIMESTAMP)")
            log(f"[startup] added detected_ts to {GOLD_ALERTS} (reused-catalog upgrade)")
    except Exception as e:  # noqa: BLE001
        log(f"[startup] detected_ts upgrade check on {GOLD_ALERTS} skipped: {e}")


def _newest_ingest_epoch_s(spark, fq_table):
    """Newest ingest_ts in the table, in epoch seconds (None if unknown).

    Captured before the tick reads silver, so the tick saw at least this row.
    Iceberg answers MAX from file metadata, so this does not scan the data.
    """
    try:
        row = spark.sql(f"SELECT unix_micros(MAX(ingest_ts)) AS t FROM {fq_table}").collect()[0]
        return row["t"] / 1e6 if row["t"] is not None else None
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] newest ingest_ts of {fq_table} unavailable: {one_line(e)}")
        return None


def _alert_content_keys(alerts):
    """One row per distinct alert content: (_key, rule_id, related_txn_ids).

    alert_id is a uuid() redrawn every tick, so it cannot say whether an
    alert is new. The content key is stable while the alert's evidence is
    unchanged and changes when a transaction joins or leaves it.
    """
    key = sha2(
        concat_ws(
            "|",
            col("rule_id"),
            coalesce(col("entity_id").cast("string"), lit("")),
            coalesce(array_join(array_sort(col("related_txn_ids")), ","), lit("")),
        ),
        256,
    )
    return alerts.select(key.alias("_key"), col("rule_id"), col("related_txn_ids")).dropDuplicates(
        ["_key"]
    )


def new_alert_txns(current, prior):
    """(_key, rule_id, uetr) for each related transaction of the alerts in
    ``current`` whose content was not in ``prior`` (None: the table had no
    snapshot, so every alert is new). An alert without related transactions
    keeps one row with a NULL uetr."""
    cur = _alert_content_keys(current)
    if prior is not None:
        cur = cur.join(_alert_content_keys(prior).select("_key"), "_key", "left_anti")
    return cur.select("_key", "rule_id", explode_outer("related_txn_ids").alias("uetr"))


def new_alert_arrivals(new_txns, txns, small=False):
    """Per new alert (``_key``, ``rule_id``), the newest ingest_ts among its
    related transactions (``arrival_ts``; NULL when none of them is in
    ``txns``). ``small``
    broadcasts ``new_txns`` so silver is filtered in place, not shuffled:
    an inner join with the broadcast on the build side (Spark will not
    broadcast the preserved side of an outer join), then the unmatched
    alerts are added back from the distinct keys."""
    pairs = new_txns.select("_key", "uetr")
    side = broadcast(pairs) if small else pairs
    matched = (
        txns.select("uetr", "ingest_ts")
        .join(side, "uetr", "inner")
        .groupBy("_key")
        .agg(max_("ingest_ts").alias("arrival_ts"))
    )
    keys = new_txns.select("_key", "rule_id").distinct()
    return keys.join(matched, "_key", "left")


def _detected_expr(detected_s, detected_by_rule=None):
    """Detection time of each alert row: its rule's commit time from
    ``detected_by_rule`` ({rule_id: epoch seconds}; a rule whose value is None
    is left out), else ``detected_s``."""
    expr_ = lit(float(detected_s))
    for rule_id, t in sorted((detected_by_rule or {}).items()):
        if t is not None:
            expr_ = when(col("rule_id") == lit(rule_id), lit(float(t))).otherwise(expr_)
    return expr_


def ttd_stats(arrivals, detected_s, late_before_s=None, bin_s=TTD_BIN_S, detected_by_rule=None):
    """Histogram of detection time minus ``arrival_ts`` over ``arrivals``.

    The detection time is ``detected_s``, or for an alert whose rule is in
    ``detected_by_rule`` the time that rule's alerts were committed.

    Returns {"alerts", "late", "unmatched", "max_s", "bin_s", "bins"}: bins
    maps floor(ttd / bin_s) to a count; ``late`` counts measured alerts with
    arrival_ts <= ``late_before_s`` (their evidence was in silver before the
    previous detection pass read it: a re-raise after a rule error, or
    evidence outside related_txn_ids). They stay in the histogram, so it
    never reads shorter for them. Clock skew between the bronze and gold
    drivers can make a ttd slightly negative; it is clamped to 0.

    With ``detected_by_rule`` it also returns "by_rule" ({rule_id: {"alerts",
    "max_s", "bins"}}, per-commit times) and "pass_end" ({"alerts", "max_s",
    "bins"}: every alert measured at ``detected_s``, the definition used
    before per-rule commit times), so a rule that got slower is not hidden in
    the merged histogram. pass_end keeps the old timestamp but not the old
    tick: the baseline no longer precedes detection and silver is pinned at
    tick start, so against a run before lane U, old-TTD minus pass_end is the
    baseline move plus the pin, and pass_end minus the merged figure is the
    rule order.
    """
    arrival_s = unix_micros(col("arrival_ts")) / 1e6
    ttd = greatest(lit(0.0), _detected_expr(detected_s, detected_by_rule) - arrival_s)
    ttd_end = greatest(lit(0.0), lit(float(detected_s)) - arrival_s)
    matched = col("arrival_ts").isNotNull()
    late = (
        when(matched & (arrival_s <= lit(float(late_before_s))), lit(1)).otherwise(lit(0))
        if late_before_s is not None
        else lit(0)
    )
    detail = detected_by_rule is not None
    rule = col("rule_id") if detail else lit(None).cast("string")
    rows = (
        arrivals.select(
            rule.alias("r"),
            when(matched, floor(ttd / bin_s)).otherwise(lit(-1)).cast("long").alias("b"),
            when(matched, floor(ttd_end / bin_s)).otherwise(lit(-1)).cast("long").alias("be"),
            when(matched, ttd).alias("t"),
            when(matched, ttd_end).alias("te"),
            late.alias("late"),
        )
        .groupBy("r", "b", "be")
        .agg(
            count(lit(1)).alias("n"),
            max_("t").alias("mx"),
            max_("te").alias("mxe"),
            spark_sum("late").alias("late"),
        )
        .collect()
    )
    measured = [r for r in rows if r["b"] >= 0]

    def _hist(rs, b_key, mx_key):
        bins = {}
        for r in rs:
            bins[int(r[b_key])] = bins.get(int(r[b_key]), 0) + int(r["n"])
        peaks = [float(r[mx_key]) for r in rs if r[mx_key] is not None]
        return bins, (max(peaks) if peaks else None)

    bins, mx = _hist(measured, "b", "mx")
    out = {
        "alerts": sum(bins.values()),
        "late": sum(int(r["late"] or 0) for r in measured),
        "unmatched": sum(int(r["n"]) for r in rows if r["b"] < 0),
        "max_s": mx,
        "bin_s": int(bin_s),
        "bins": bins,
    }
    if detail:
        by_rule = {}
        for rid in sorted({r["r"] for r in measured}):
            rb, rmx = _hist([r for r in measured if r["r"] == rid], "b", "mx")
            by_rule[rid] = {"alerts": sum(rb.values()), "max_s": rmx, "bins": rb}
        eb, emx = _hist(measured, "be", "mxe")
        out["by_rule"] = by_rule
        out["pass_end"] = {"alerts": sum(eb.values()), "max_s": emx, "bins": eb}
    return out


def ttd_detail_lines(cycle, stats):
    """The per-rule and pass-end histograms of ttd_stats as log lines
    (collector _TTD_DETAIL_LINE). None of them matches the main ttd_line."""

    def _fmt(h):
        bins = ",".join(f"{b}:{n}" for b, n in sorted(h["bins"].items()))
        mx = h["max_s"]
        return (
            f"alerts={h['alerts']} max={'-' if mx is None else f'{mx:.1f}'}s "
            f"bin={stats['bin_s']}s bins={bins}"
        )

    lines = []
    if "pass_end" in stats:
        lines.append(f"Cycle {cycle}: time to detect at pass end {_fmt(stats['pass_end'])}")
    for rid, h in sorted(stats.get("by_rule", {}).items()):
        lines.append(f"Cycle {cycle}: time to detect rule={rid} {_fmt(h)}")
    return lines


def _empty_ttd_stats():
    return {"alerts": 0, "late": 0, "unmatched": 0, "max_s": None, "bin_s": TTD_BIN_S, "bins": {}}


def _current_snapshot(spark, fq_table):
    """``fq_table``'s current snapshot id; None when it has none yet;
    TTD_SNAPSHOT_UNKNOWN when the lookup failed."""
    try:
        rows = spark.sql(
            f"SELECT snapshot_id FROM {fq_table}.history "
            "WHERE is_current_ancestor ORDER BY made_current_at DESC LIMIT 1"
        ).collect()
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] snapshot of {fq_table} unavailable: {one_line(e)}")
        return TTD_SNAPSHOT_UNKNOWN
    return int(rows[0][0]) if rows else None


def _prior_alerts_snapshot(spark):
    """gold.alerts' current snapshot id; None when it has none yet (every
    alert is new); TTD_SNAPSHOT_UNKNOWN when the lookup failed."""
    return _current_snapshot(spark, f"{CATALOG}.{GOLD_ALERTS}")


def _pin_silver(spark):
    """(txns, snapshot id, row count, newest ingest_ts in epoch seconds) of
    silver.transactions at its current snapshot.

    The rules, the baseline and the time-to-detect lookup read this one
    frame, so they all see the same transactions, and the newest ingest_ts
    is exactly the newest row detection saw. Two readers do not: W2 reads
    silver.entities live (a customer row landing mid-tick can only add an
    alert), and the TM pass pins silver again itself when it runs. Without a
    snapshot (empty table, or the lookup failed) the table is read as it is
    and the probes fall back to the live table, as before the pin.
    """
    fq = f"{CATALOG}.{SILVER_TXNS}"
    sid = _current_snapshot(spark, fq)
    if sid is None or sid == TTD_SNAPSHOT_UNKNOWN:
        rows, _ = iceberg_table_stats(spark, fq)
        return spark.table(fq), None, rows, _newest_ingest_epoch_s(spark, fq)
    txns = read_at_snapshot(spark, fq, sid)
    rows = None
    try:
        r = spark.sql(
            f"SELECT summary['total-records'] AS n FROM {fq}.snapshots WHERE snapshot_id = {sid}"
        ).collect()
        rows = int(r[0]["n"]) if r and r[0]["n"] is not None else None
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] row count of {fq} at {sid} unavailable: {one_line(e)}")
    if rows is None:
        rows, _ = iceberg_table_stats(spark, fq)
    try:
        t = txns.agg(unix_micros(max_("ingest_ts")).alias("t")).collect()[0]["t"]
        newest = t / 1e6 if t is not None else None
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] newest ingest_ts of {fq} unavailable: {one_line(e)}")
        newest = None
    return txns, sid, rows, newest


def _log_time_to_detect(spark, cycle, base, detected_s, silver=None, detected_by_rule=None) -> bool:
    """Log this tick's time-to-detect line against ``base`` (TtdBaseline).
    ``silver`` is the tick's pinned silver frame (the live table when None);
    ``detected_by_rule`` maps a rule to the time its alerts were committed,
    ``detected_s`` covers any other rule. Returns True when the line was
    logged. Best effort: a failure costs the measurement, never the tick; the
    collector counts cycles without a line as unmeasured."""
    prior_sid, late_before_s = base
    if prior_sid == TTD_SNAPSHOT_UNKNOWN:
        log(f"[metrics] time to detect unavailable on cycle {cycle}: no prior snapshot")
        return False
    try:
        fq_alerts = f"{CATALOG}.{GOLD_ALERTS}"
        current = spark.table(fq_alerts).where(col("run_id") == RUN_ID)
        prior = (
            read_at_snapshot(spark, fq_alerts, prior_sid).where(col("run_id") == RUN_ID)
            if prior_sid is not None
            else None
        )
        new_txns = new_alert_txns(current, prior).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            n = new_txns.count()
            if n == 0:
                stats = _empty_ttd_stats()
            else:
                txns = silver if silver is not None else spark.table(f"{CATALOG}.{SILVER_TXNS}")
                arrivals = new_alert_arrivals(new_txns, txns, small=n <= TTD_BROADCAST_ROWS)
                stats = ttd_stats(
                    arrivals, detected_s, late_before_s, detected_by_rule=detected_by_rule
                )
        finally:
            new_txns.unpersist(blocking=False)
        log(ttd_line(cycle, stats))
        for line in ttd_detail_lines(cycle, stats):
            log(line)
        return True
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] time to detect unavailable on cycle {cycle}: {one_line(e)}")
        return False


def tick_timing_line(cycle, silver_rows, phases):
    """The per-tick phase breakdown the collector parses
    (metrics/collector.py _TICK_TIMING_LINE). ``phases`` is an ordered
    {name: seconds}; names are identifiers (phase names and rule ids)."""
    # Clamped at 0: a remainder phase (detect_setup) can round below zero.
    parts = " ".join(f"{k}={max(0.0, float(v)):.1f}s" for k, v in phases.items())
    return f"Cycle {cycle}: tick timing silver_rows={int(silver_rows or 0)} {parts}"


class TickState:
    """What a tick carries to the next: manifest registration, the last
    freshness sample, the time-to-detect baseline and the TM clock."""

    def __init__(self, run_start, window_end_s=0.0, manifest_ready=False):
        self.manifest_ready = manifest_ready
        self.last_ingest_s = 0.0
        self.ttd_baseline = TtdBaseline()
        self.prev_tick_ingest_s = None
        self.tm_clock = {"run_start": run_start, "start": None, "end": None, "elapsed": 0.0}
        self.window_end_s = window_end_s
        # Consecutive ticks whose baseline refresh failed; run_tick raises at
        # MAX_CONSECUTIVE_FAILURES so a dashboard that never refreshes still
        # fails the job (LB-044), as it did when the baseline ran first.
        self.baseline_failures = 0

    @classmethod
    def start(cls, spark):
        run_start = time.time()
        # The window's start is the first driver's start, persisted in this
        # job's checkpoint (tm_operations.window_start_marker), so a
        # restarted driver keeps it.
        window_end_s = (
            window_start_marker(spark, GOLD_CHECKPOINT, run_start) + WINDOW_S
            if WINDOW_S > 0
            else 0.0
        )
        return cls(
            run_start,
            window_end_s,
            manifest_ready=table_exists(spark, f"{CATALOG}.{MANIFEST_TABLE}"),
        )


def run_tick(spark, state, cycle) -> dict:
    """One detection tick. Returns its phase timings ({name: seconds}, in
    order, ``total`` last), which are also logged as the tick timing line.
    Raises when the tick fails as a whole (silver unreadable, a gold write
    failed); a single rule's failure is isolated inside run_detection_rules."""
    tick = time.time()
    phases = {}
    # The continuous reset drops the previous run's manifest table; register
    # this run's once datagen has written it.
    if not state.manifest_ready:
        state.manifest_ready = register_manifest(spark)
    # Pinned before anything reads silver, so every reader sees this corpus.
    txns, _, silver_rows, newest_ingest_s = _pin_silver(spark)
    newest_bronze_s = _newest_ingest_epoch_s(spark, f"{CATALOG}.{BRONZE_TABLE}")
    if silver_rows == 0:
        log(f"Cycle {cycle}: Silver table is empty, skipping")
    else:
        log(f"Cycle {cycle}: aggregating {silver_rows:,} Silver records")
    phases["probe"] = time.time() - tick

    # Detection: reuse the batch driver over the full pinned corpus. Per-rule
    # DELETE + INSERT makes this tick's alerts the single correct set for each
    # rule; per-rule errors are isolated inside the driver, so only a failure
    # to read silver at all raises up to here. The snapshot before the rewrite
    # is what this tick's alerts are compared against to find the newly
    # raised ones.
    t = time.time()
    ttd_base = state.ttd_baseline.begin(_prior_alerts_snapshot(spark), state.prev_tick_ingest_s)
    state.prev_tick_ingest_s = newest_ingest_s
    detection = run_detection_rules(
        spark,
        txns,
        RUN_ID,
        rules=CONTINUOUS_RULES,
        skipped_rules=CONTINUOUS_SKIPPED_RULES,
    )
    detection_end_s = time.time()
    detection = detection or {}
    rule_times = detection.get("rules", {})
    # Everything in the pass but the rules and the status/projection writes:
    # the snapshot lookup, the pending status, clearing other runs' rows.
    phases["detect_setup"] = (
        detection_end_s
        - t
        - detection.get("finish_s", 0.0)
        - detection.get("cleanup_s", 0.0)
        - sum(v.get("elapsed_s", 0.0) for v in rule_times.values())
    )
    for rule_id in CONTINUOUS_RULES:
        if rule_id in rule_times:
            phases[rule_id] = rule_times[rule_id]["elapsed_s"]
    # W3/W17's path-spill deletes and the cache clears after each rule.
    phases["detect_cleanup"] = detection.get("cleanup_s", 0.0)
    phases["detect_finish"] = detection.get("finish_s", 0.0)

    # Baseline: overwrite ONLY rule_id='baseline' rows so the detection rows
    # (other rule_ids) are preserved. DELETE + append are two Iceberg commits;
    # a crash between them leaves one tick without baseline rows, refreshed
    # next cycle -- acceptable. After detection, so alerts do not wait on it,
    # and best effort: this tick's alerts are already committed, and failing
    # the tick here would cost their freshness and time-to-detect samples.
    t = time.time()
    baseline_error = None
    try:
        baseline = build_baseline_dashboards(txns, RUN_ID)
        spark.sql(f"DELETE FROM {CATALOG}.{GOLD_DASH} WHERE rule_id = 'baseline'")
        baseline.writeTo(f"{CATALOG}.{GOLD_DASH}").append()
        state.baseline_failures = 0
    except Exception as e:  # noqa: BLE001
        state.baseline_failures += 1
        baseline_error = e
        log(
            f"[baseline] cycle {cycle}: {GOLD_DASH} refresh failed "
            f"({state.baseline_failures}/{MAX_CONSECUTIVE_FAILURES}): {one_line(e)}"
        )
    phases["baseline"] = time.time() - t

    # This run's alerts only. Rules skipped in continuous mode keep alerts
    # from earlier runs, and counting those let the continuous gate pass with
    # no continuous detection at all.
    t = time.time()
    total_alerts = int(
        spark.table(f"{CATALOG}.{GOLD_ALERTS}").where(col("run_id") == RUN_ID).count()
    )
    phases["count"] = time.time() - t
    elapsed = time.time() - tick
    log(f"[detection] cumulative gold.alerts rows: {total_alerts}")
    log(f"Tick complete in {elapsed:.1f}s (gold.alerts rows: {total_alerts})")
    # Collector line formats (LB-136). Freshness: how long ago the newest row
    # this tick's detection saw entered bronze, i.e. how stale the alerts are
    # against the input. Reported when silver moved on, and also whenever
    # bronze holds rows silver has not seen: a stalled silver-stream then
    # shows staleness growing tick by tick instead of going quiet and leaving
    # the last good value as the score. Ticks with nothing new anywhere (the
    # finite corpus has drained) are not reported. A stall before bronze is
    # caught by the ingest-ratio and zero-row gates, not here.
    log(f"Cycle {cycle}: refreshed {GOLD_ALERTS} in {elapsed:.1f}s")
    backlog = (
        newest_bronze_s is not None
        and newest_ingest_s is not None
        and newest_bronze_s > newest_ingest_s
    )
    fresh_sampled = False
    if newest_ingest_s is not None and (newest_ingest_s > state.last_ingest_s or backlog):
        log(f"Cycle {cycle}: data freshness {max(0.0, time.time() - newest_ingest_s):.0f}s")
        state.last_ingest_s = newest_ingest_s
        fresh_sampled = True

    t = time.time()
    committed = {r: v.get("committed_s") for r, v in rule_times.items()}
    if _log_time_to_detect(
        spark, cycle, ttd_base, detection_end_s, silver=txns, detected_by_rule=committed
    ):
        state.ttd_baseline.measured(_prior_alerts_snapshot(spark))
    phases["ttd"] = time.time() - t

    # P10 operations layer. It runs every continuous_interval_seconds,
    # measured from the end of the last pass, so detection ticks always run
    # between passes however long a pass takes; plus one final pass timed to
    # finish before the window closes. One pass over the full corpus costs
    # about a minute at scale 10 (55.3 s on run-20260925-135005-4b7a97).
    # Waits for data and the manifest (dispositions are simulated from it); a
    # window that ends first reports the layer as not run. Never raises.
    t = time.time()
    now = t
    tm_clock, window_end_s = state.tm_clock, state.window_end_s
    if TM_PARAMS["enabled"] and tm_pass_due(now, tm_clock, TM_PARAMS, window_end_s):
        if silver_rows > 0 and state.manifest_ready:
            tm_clock["start"] = now
            run_tm_operations(spark, txns, RUN_ID, cycle=cycle, continuous=True, params=TM_PARAMS)
            tm_clock["end"] = time.time()
            tm_clock["elapsed"] = tm_clock["end"] - now
            # Gold was not refreshed while the pass ran: its staleness now
            # includes the pass, so it is sampled again while data is moving
            # -- the tick sampled, or bronze took rows during the pass that
            # gold has not seen. A drained corpus's idle time never counts.
            after_bronze_s = _newest_ingest_epoch_s(spark, f"{CATALOG}.{BRONZE_TABLE}")
            arrived = (
                after_bronze_s is not None
                and newest_ingest_s is not None
                and after_bronze_s > newest_ingest_s
            )
            if newest_ingest_s is not None and (fresh_sampled or arrived):
                log(f"Cycle {cycle}: data freshness {max(0.0, time.time() - newest_ingest_s):.0f}s")
        else:
            log(
                # cycle=0: no operations pass has a number yet (they are
                # numbered from the ledger, not by tick).
                f"[tm-status] status=waiting cycle=0 reason=tick {cycle}, "
                + ("silver is empty" if silver_rows == 0 else "no manifest yet")
            )
    elif not TM_PARAMS["enabled"] and cycle == 1:
        run_tm_operations(spark, txns, RUN_ID, cycle=cycle, params=TM_PARAMS)
    phases["tm"] = time.time() - t
    phases["total"] = time.time() - tick
    log(tick_timing_line(cycle, silver_rows, phases))
    if baseline_error is not None and state.baseline_failures >= MAX_CONSECUTIVE_FAILURES:
        # Raised after the tick's measurements are logged; the main loop
        # counts it as a failed tick.
        raise RuntimeError(
            f"baseline refresh failed on {state.baseline_failures} consecutive ticks"
        ) from baseline_error
    return phases


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

    # Earlier runs' alerts are cleared by run_detection_rules on each tick,
    # after the tick's 'pending' status is written.
    consecutive_failures = 0
    state = TickState.start(spark)
    cycle = 0

    while not _SHUTDOWN:
        tick = time.time()
        cycle += 1
        try:
            run_tick(spark, state, cycle)
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
