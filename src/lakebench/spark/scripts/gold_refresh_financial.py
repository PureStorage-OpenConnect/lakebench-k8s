"""Gold Refresh (Financial, continuous) -- baseline rollup + periodic detection.

Runs the gold stage of the continuous AML pipeline (bronze_ingest ->
silver_stream -> this) on a periodic timer (LB_FINANCIAL_GOLD_REFRESH_S
seconds). Each tick does two things against the moving silver corpus:

1. Bring each detection rule's alerts up to date with the pinned silver
   (re-detecting what the new rows can change) and rewrite them into
   gold.alerts.
2. Refresh the daily_dashboards baseline rows (rule_id='baseline').

Previously this script refreshed only the baseline dashboard and ran NO
detection: a continuous AML run produced zero alerts, nothing to score, and
(before the honest-runner gate landed) could still report PASS. This is the
continuous half of the detection spine.

Why re-detection from the new rows, not a sliding window:
An earlier design scanned a data-clock window [max(txn_timestamp) - w, max]
each tick. Adversarial review found two fatal flaws: (a) a static corpus
trickled onto bronze arrives out of event-time order, so max(txn_timestamp)
pinned to the corpus end almost immediately and every window anchored there,
leaving older typologies permanently unscanned -> silent recall loss vs
batch; and (b) the content-hash alert_id keyed on the in-window txn SET
drifted as the window slid, so the same episode produced many alert rows.
Gold then re-ran every rule over the whole corpus each tick, whose cost grew
with the run. Now each rule re-detects from the earliest event time among
the rows new since its last pass (incremental_detection): every rule is local
in event time, so only alerts within the rule's own windows of those rows can
change, and the rest are kept. That holds for any arrival order (old rows
only widen the pass), and each tick's alerts equal a full recompute over the
pinned silver. A continuous run's own datagen delivers in event-time order,
so a pass reads about the new rows plus the rule's windows.

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

detected_ts semantics in continuous: each tick rewrites a rule's alerts, and
an alert whose content (entity, sorted related transactions) an earlier tick
wrote keeps that tick's detected_ts, so it is the first detection.

Every continuous rule runs every tick.

Time to detect (Phase 4a): alert_id is a fresh uuid() on every tick, so first
detection is found by content instead. An alert is newly raised on a tick
when its (rule_id, entity_id, sorted related_txn_ids) was not in gold.alerts
at the snapshot before the tick. Its time to detect is the moment its rule's
INSERT into gold.alerts committed minus the newest bronze ingest_ts among its
related transactions, i.e. from the moment the last piece of its evidence
arrived (its file landed in the raw zone, see common.arrival_time) to the
moment the alert was visible in gold. The rule's
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
  exits non-zero so K8s Job status reflects reality. A single
  misbehaving rule is isolated inside run_detection_rules and does NOT count
  as a tick failure.
"""

from __future__ import annotations

import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from bronze_verify_financial import BRONZE_URI, MANIFEST_PATH, MANIFEST_TABLE, register_manifest
from common import (
    TTD_SNAPSHOT_UNKNOWN,
    SealedFilterError,
    TtdBaseline,
    ensure_alert_columns,
    ensure_namespaces_for_ddl,
    ensure_partition_transform,
    env,
    iceberg_table_stats,
    log,
    one_line,
    refresh_table,
    sealed_txns_filter,
    sealed_txns_filter_at,
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
from pyspark.sql.functions import min as min_
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
SILVER_BATCH_VERSIONS = env("LB_FINANCIAL_SILVER_BATCH_VERSIONS", "silver.silver_batch_versions")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
GOLD_DASH = env("LB_FINANCIAL_GOLD_DASHBOARDS", "gold.daily_dashboards")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
GOLD_STATUS = env("LB_FINANCIAL_GOLD_DETECTION_STATUS", "gold.detection_status")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_ACCOUNTS = env("LB_FINANCIAL_SILVER_ACCOUNTS", "silver.accounts")
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

# The continuous rules. W2 is customer-scoped and also reads silver.entities;
# silver_stream commits a batch's entities after its transactions, so a tick
# can miss a new customer's alert, and the next tick raises it (W2 keeps its
# windows before the customer filter, which it applies every pass).
# Order: each rule's alerts are visible when its own write commits, so the
# single-join rules go before the path searches, and among those W17 (many
# alerts) before W3 (few), which makes W3's alerts wait on W17. Measured on a
# local 3.4M-row silver (lane U, warm tick): W4 8.8 s for 33,902 alerts, W2
# 5.5 s for 259, W17 32.8 s for 2,542, W3 29.1 s for 155. Rules are
# independent, so the order changes when alerts appear, never which.
CONTINUOUS_RULES = (
    "W4_risk_propagation",
    "W2_structuring",
    "W5_sanctions_match",
    "W6_pep_counterparty",
    "W17_layering_chain",
    "W3_round_tripping",
)
# Continuous W4 raises one alert per entity per week of payments, not one per
# entity over its whole history, so a tick recomputes only the weeks of its
# new rows (owner, 2026-10-08). Batch W4 keeps one alert per entity.
# Continuous W5 is the transaction screen only: a rescreen alert reads a
# counterparty's whole payment history (owner, 2026-10-08: batch only,
# labelled).
CONTINUOUS_RULE_OVERRIDES = {
    "W4_risk_propagation": {"alert_window_hours": 7 * 24},
    "W5_sanctions_match": {"rescreen": False},
}
# Rules deliberately not run in continuous mode, recorded as 'skipped' so
# their typologies render "not run" rather than a false 0% recall.
CONTINUOUS_SKIPPED_RULES = (
    "W1_connected_components",
    "W7_cross_border_high_risk",
    "W8_dormant_reactivation",
)

_SHUTDOWN = False
# When stop_requested last logged a read error: it polls once a second, so a
# lasting S3 error is logged once a minute, not every second.
_MARKER_ERROR_LOGGED_AT = 0.0
# Name of the drain marker the CLI writes under CHECKPOINT_LOCATION; its body
# is the run id it is meant for (cli/_aml_post.py).
STOP_MARKER = "_lb_stop"


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
    to an older reused gold.alerts missing the column.

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
    # Reused-catalog upgrade, as gold_finalize_financial does: missing
    # trailing ALERT_COLUMNS are appended; a table whose columns differ in
    # any other way fails bootstrap, before the first tick writes positionally.
    from detection_rules import ALERT_COLUMNS

    ensure_alert_columns(spark, f"{CATALOG}.{GOLD_ALERTS}", ALERT_COLUMNS)


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


def _inside(seen):
    """Whether a silver row's batch is inside the sealed position *seen*
    ({stream_id: newest batch_id}; None: nothing was)."""
    inside = lit(False)
    for stream_id, batch_id in sorted((seen or {}).items()):
        inside = when(
            col("_stream_id") == lit(stream_id), col("_batch_id") <= lit(int(batch_id))
        ).otherwise(inside)
    return inside


def new_alert_arrivals(new_txns, txns, small=False, seen=None):
    """Per new alert (``_key``, ``rule_id``), the newest ingest_ts among its
    related transactions (``arrival_ts``; NULL when none of them is in
    ``txns``), and ``seen_before``: every matched one was in the sealed
    silver position ``seen`` ({stream_id: newest batch_id}) the previous
    detection pass read. Arrival times are file landing times, which a later
    batch can carry older than an earlier one's, so only the batch says what
    the previous pass could have seen. ``small``
    broadcasts ``new_txns`` so silver is filtered in place, not shuffled:
    an inner join with the broadcast on the build side (Spark will not
    broadcast the preserved side of an outer join), then the unmatched
    alerts are added back from the distinct keys."""
    pairs = new_txns.select("_key", "rule_id", "uetr")
    side = broadcast(pairs) if small else pairs
    batched = {"_stream_id", "_batch_id"} <= set(txns.columns)
    inside = _inside(seen) if batched else lit(False)
    matched = (
        txns.select("uetr", "ingest_ts", *(["_stream_id", "_batch_id"] if batched else []))
        .join(side, "uetr", "inner")
        .withColumn("_inside", inside)
        .groupBy("_key")
        .agg(
            max_("ingest_ts").alias("arrival_ts"),
            (min_(col("_inside").cast("int")) == 1).alias("seen_before"),
        )
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


def ttd_stats(arrivals, detected_s, bin_s=TTD_BIN_S, detected_by_rule=None):
    """Histogram of detection time minus ``arrival_ts`` over ``arrivals``.

    The detection time is ``detected_s``, or for an alert whose rule is in
    ``detected_by_rule`` the time that rule's alerts were committed.

    Returns {"alerts", "late", "unmatched", "max_s", "bin_s", "bins"}: bins
    maps floor(ttd / bin_s) to a count; ``late`` counts measured alerts that
    are ``seen_before`` (their evidence was in silver before the previous
    detection pass read it: a re-raise after a rule error, or evidence
    outside related_txn_ids). They stay in the histogram, so it
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
    late = when(matched & coalesce(col("seen_before"), lit(False)), lit(1)).otherwise(lit(0))
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


def _token(snapshot):
    """How a tick record names a snapshot: the id, or ``none`` / ``unknown``."""
    if snapshot is None:
        return "none"
    if snapshot == TTD_SNAPSHOT_UNKNOWN:
        return "unknown"
    return str(int(snapshot))


def _pin_silver(spark):
    """(txns, txns snapshot, versions_used, row count, newest ingest_ts in
    epoch seconds) of silver.transactions at its current snapshot, filtered
    to sealed micro-batches by the versions table at ONE snapshot.

    The rules, the baseline and the time-to-detect lookup read this one
    frame, so they all see the same transactions, and the newest ingest_ts
    is exactly the newest row detection saw. Two readers do not: W2 reads
    silver.entities live (a customer row landing mid-tick can only add an
    alert), and the TM pass pins silver again itself when it runs.

    The sealed filter reads the versions table ``VERSION AS OF`` the
    snapshot captured right after the transactions snapshot
    (``sealed_txns_filter_at``), so the covered scorer, given the same two
    ids, sees exactly the sealed set detection saw. ``versions_used`` is that
    id only when the pinned filter was built; otherwise (no versions
    snapshot, a failed lookup, or a pinned read that raised) the tick falls
    back to today's current-state filter and ``versions_used`` is ``none`` or
    ``unknown``, so a tick record never carries an id detection did not use.
    Without a transactions snapshot (empty table, or the lookup failed) the
    table is read as it is and the probes fall back to the live table.

    The sixth element is the time-travel record of the transactions snapshot
    (``snapshot_record``), from the snapshot metadata only; None without a
    snapshot. The row count falls back to ``iceberg_table_stats`` for the
    tick's ``silver_rows`` log when the summary has no count, but that value
    is the current table's, not the snapshot's, so the record never takes it.
    """
    fq = f"{CATALOG}.{SILVER_TXNS}"
    sid = _current_snapshot(spark, fq)
    if sid is None or sid == TTD_SNAPSHOT_UNKNOWN:
        rows, _ = iceberg_table_stats(spark, fq)
        # I10: hide mid-batch crash rows from every rule that reads this
        # frame. Fallback path (no pinned snapshot) still filters.
        return (
            sealed_txns_filter(spark, spark.table(fq), CATALOG, SILVER_BATCH_VERSIONS),
            sid,
            _token(sid),
            rows,
            _newest_ingest_epoch_s(spark, fq),
            None,
        )
    vsid = _current_snapshot(spark, f"{CATALOG}.{SILVER_BATCH_VERSIONS}")
    pinned = read_at_snapshot(spark, fq, sid)
    txns = None
    versions_used = _token(vsid)
    if isinstance(vsid, int) and not isinstance(vsid, bool):
        try:
            txns = sealed_txns_filter_at(spark, pinned, CATALOG, SILVER_BATCH_VERSIONS, vsid)
        except (TypeError, SealedFilterError) as e:
            log(
                f"[metrics] pinned versions read at {vsid} failed, filtering current: {one_line(e)}"
            )
            versions_used = "unknown"
    if txns is None:
        # I10: today's current-state filter; a batch sealed after this pin
        # but before the filter runs becomes visible on the next tick.
        txns = sealed_txns_filter(spark, pinned, CATALOG, SILVER_BATCH_VERSIONS)
    tt = snapshot_record(spark, fq, sid)
    rows = tt["total_records"]
    if rows is None:
        rows, _ = iceberg_table_stats(spark, fq)
    try:
        t = txns.agg(unix_micros(max_("ingest_ts")).alias("t")).collect()[0]["t"]
        newest = t / 1e6 if t is not None else None
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] newest ingest_ts of {fq} unavailable: {one_line(e)}")
        newest = None
    return txns, sid, versions_used, rows, newest, tt


def _summary_int(value):
    """A snapshot summary value (a string in Iceberg's summary map) as an int,
    or None when it is absent or not an integer."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def snapshot_record(spark, fq, sid):
    """The time-travel record of ``fq`` at snapshot ``sid``, read from the
    snapshot's metadata only (one query on ``{fq}.snapshots``, no data scan):
    ``{snapshot, committed_at, total_records, pos_deletes, eq_deletes,
    count_source}``. ``committed_at`` is ISO 8601 UTC. ``total_records`` is
    the summary's ``total-records``: the sum of the record counts of the data
    files live at ``sid``, deletes not subtracted, so it equals the live row
    count only when ``pos_deletes`` and ``eq_deletes`` (the summary's
    deleted-row totals) are 0, as under the copy-on-write tables Lakebench
    creates. ``count_source`` is ``summary`` when ``total_records`` is
    present, else ``unavailable``; the fields the summary does carry are kept
    either way. A failed or empty lookup leaves every field None, and nothing
    is ever filled in from the current table."""
    rec = {
        "snapshot": sid,
        "committed_at": None,
        "total_records": None,
        "pos_deletes": None,
        "eq_deletes": None,
        "count_source": "unavailable",
    }
    try:
        r = spark.sql(
            "SELECT unix_micros(committed_at) AS committed_us, "
            "summary['total-records'] AS n, "
            "summary['total-position-deletes'] AS pos, "
            "summary['total-equality-deletes'] AS eq "
            f"FROM {fq}.snapshots WHERE snapshot_id = {sid}"
        ).collect()
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] snapshot summary of {fq} at {sid} unavailable: {one_line(e)}")
        return rec
    if not r:
        log(f"[metrics] snapshot {sid} of {fq} not in its snapshots table")
        return rec
    row = r[0]
    us = _summary_int(row["committed_us"])
    if us is not None:
        stamp = datetime.fromtimestamp(us // 1_000_000, timezone.utc)
        rec["committed_at"] = stamp.replace(microsecond=us % 1_000_000).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
    rec["total_records"] = _summary_int(row["n"])
    rec["pos_deletes"] = _summary_int(row["pos"])
    rec["eq_deletes"] = _summary_int(row["eq"])
    if rec["total_records"] is not None:
        rec["count_source"] = "summary"
    return rec


def tt_record_line(cycle, table, rec):
    """The tick's time-travel record line (metrics/tick_records.py
    parse_tick_records): the snapshot detection read and its metadata
    counts, for the post-run time-travel read. ``null`` for an unknown
    value."""

    def v(x):
        return "null" if x is None else x

    return (
        f"Cycle {cycle}: tt-record table={table} snapshot={_token(rec['snapshot'])} "
        f"committed_at={v(rec['committed_at'])} total_records={v(rec['total_records'])} "
        f"pos_deletes={v(rec['pos_deletes'])} eq_deletes={v(rec['eq_deletes'])} "
        f"count_source={rec['count_source']} run={RUN_ID}"
    )


def rule_status_line(cycle, status_rows) -> str:
    """``Cycle N: rule-status W2_structuring=ran W3_round_tripping=skipped:path-cap
    ... run=<run>`` (metrics/tick_records.py parses it): each rule's status at
    the tick, a skip with its reason, any other status as itself."""

    def one(row):
        rule_id, status, reason = row[0], row[1], row[2]
        if status == "skipped" and reason:
            return f"{rule_id}=skipped:{str(reason).replace(' ', '-')}"
        return f"{rule_id}={status}"

    body = " ".join(one(r) for r in status_rows)
    return f"Cycle {cycle}: rule-status {body} run={RUN_ID}"


def tick_pinned_line(cycle, sid, entities_sid, accounts_sid, versions_used, at_s):
    """The tick record's first line (metrics/collector.py parse_tick_records):
    the snapshots this tick's detection read."""
    return (
        f"Cycle {cycle}: pinned txns={_token(sid)} entities={_token(entities_sid)} "
        f"accounts={_token(accounts_sid)} versions={versions_used} at={at_s:.3f} run={RUN_ID}"
    )


def _ttd_scope(scope, committed, incremental):
    """``scope`` ({rule_id: (write spans, read spans)}) with this tick's rules
    added: since the last measured tick, a rule's new alerts lie in its write
    spans and their transactions in its read spans; None for either means
    anywhere. A rule that wrote nothing this tick adds nothing."""
    from incremental_detection import merge_spans

    def union(a, b):
        return None if a is None or b is None else merge_spans(a + b)

    out = dict(scope or {})
    for rule_id, at in (committed or {}).items():
        if at is None:
            continue
        last = incremental.last.get(rule_id, {}) if incremental is not None else {}
        here = (
            incremental.write_spans(rule_id) if incremental is not None else None,
            last.get("read"),
        )
        old = out.get(rule_id)
        out[rule_id] = here if old is None else (union(old[0], here[0]), union(old[1], here[1]))
    return out


def _scoped(alerts, scope):
    """``alerts`` cut to the rows ``scope`` (_ttd_scope) says can be new."""
    from gold_finalize_financial import _in_spans

    keep = lit(False)
    for rule_id, (write, _) in sorted(scope.items()):
        here = col("rule_id") == lit(rule_id)
        keep = keep | (here if write is None else here & _in_spans(col("alert_ts"), write))
    return alerts.where(keep)


def _log_time_to_detect(
    spark, cycle, base, detected_s, silver=None, detected_by_rule=None, scope=None
) -> bool:
    """Log this tick's time-to-detect line against ``base`` (TtdBaseline).
    ``silver`` is the tick's pinned silver frame (the live table when None);
    ``detected_by_rule`` maps a rule to the time its alerts were committed,
    ``detected_s`` covers any other rule. ``scope`` (_ttd_scope, since the
    baseline's tick) limits the alerts compared to those that can be new and
    the silver read to the spans their transactions lie in, so the cost
    follows the new rows, not the accumulated alerts and silver; None
    compares everything. Returns True when the line was logged. Best effort:
    a failure costs the measurement, never the tick; the collector counts
    cycles without a line as unmeasured."""
    prior_sid, seen = base
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
        if scope is not None:
            current = _scoped(current, scope)
            prior = _scoped(prior, scope) if prior is not None else None
        new_txns = new_alert_txns(current, prior).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            n = new_txns.count()
            if n == 0:
                stats = _empty_ttd_stats()
            else:
                # I10: the pinned frame (silver) is already filtered by
                # _pin_silver; the fallback live read needs the same filter.
                txns = (
                    silver
                    if silver is not None
                    else sealed_txns_filter(
                        spark,
                        spark.table(f"{CATALOG}.{SILVER_TXNS}"),
                        CATALOG,
                        SILVER_BATCH_VERSIONS,
                    )
                )
                arrivals = _scoped_arrivals(new_txns, txns, n, seen, scope)
                stats = ttd_stats(arrivals, detected_s, detected_by_rule=detected_by_rule)
        finally:
            new_txns.unpersist(blocking=False)
        log(ttd_line(cycle, stats))
        for line in ttd_detail_lines(cycle, stats):
            log(line)
        return True
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] time to detect unavailable on cycle {cycle}: {one_line(e)}")
        return False


def _scoped_arrivals(new_txns, txns, n, seen, scope):
    """new_alert_arrivals over the silver spans ``scope`` names (all of
    silver without one). An alert none of whose transactions lies there (a
    W2 burst rebuilt whole, a customer that arrived late) is looked up in
    all of silver; such alerts are few. Matching only inside the spans gives
    the same arrival: the newest transaction of a new alert is a new row,
    inside its rule's read spans, and the old ones left out were all seen."""
    from incremental_detection import _within, merge_spans

    small = n <= TTD_BROADCAST_ROWS
    if not scope or any(read is None for _, read in scope.values()):
        return new_alert_arrivals(new_txns, txns, small=small, seen=seen)
    read = merge_spans(sum((read for _, read in scope.values()), ()))
    arrivals = new_alert_arrivals(new_txns, _within(txns, read), small=small, seen=seen).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    missing = arrivals.where(col("arrival_ts").isNull()).select("_key")
    if missing.limit(1).count() == 0:
        return arrivals
    again = new_alert_arrivals(
        new_txns.join(missing, "_key", "left_semi"), txns, small=True, seen=seen
    )
    return arrivals.join(missing, "_key", "left_anti").unionByName(again)


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

    def __init__(self, run_start, window_end_s=0.0, manifest_ready=False, incremental=None):
        self.manifest_ready = manifest_ready
        # Per-rule detection state between ticks (incremental_detection);
        # None runs every rule as a full recompute.
        self.incremental = incremental
        # The last completed tick's record lines (pinned, tt-record,
        # committed), repeated with the drain line.
        self.tick_records = []
        # The earliest event time (epoch us) whose baseline days are not yet
        # rewritten (None: all are; FULL: rebuild every day).
        self.baseline_cut = None
        # The manifest files the table was last registered from (None: not
        # known, so the first tick registers again).
        self.manifest_files = None
        self.last_ingest_s = 0.0
        self.ttd_baseline = TtdBaseline()
        # What the alerts since the last measured tick can be (_ttd_scope).
        self.ttd_scope = {}
        self.prev_tick_seen = None
        self.tm_clock = {"run_start": run_start, "start": None, "end": None, "elapsed": 0.0}
        self.window_end_s = window_end_s
        # Consecutive ticks whose baseline refresh failed; run_tick raises at
        # MAX_CONSECUTIVE_FAILURES so a dashboard that never refreshes still
        # fails the job, as it did when the baseline ran first.
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
            incremental=_incremental_detection(spark),
        )


def _incremental_detection(spark):
    """The driver's IncrementalDetection, its state under the gold bucket and
    this application's id; None without LB_GOLD_URI. State an earlier driver
    left is deleted first: a new driver starts every rule from a full
    recompute."""
    import os

    from detection_rules import _delete_uri
    from incremental_detection import IncrementalDetection

    base = os.getenv("LB_GOLD_URI")
    if not base:
        log("[incremental] LB_GOLD_URI unset: every tick is a full recompute")
        return None
    parent = f"{base.rstrip('/')}/_checkpoints/incremental"
    _delete_uri(spark, parent, "incremental")
    return IncrementalDetection(spark, f"{parent}/{spark.sparkContext.applicationId}")


def refresh_baseline(spark, txns, cut) -> None:
    """Rewrite the baseline rows (rule_id 'baseline') of the days in ``cut``
    (event-time spans, see tick_position) from ``txns``; every day when
    ``cut`` is incremental_detection.FULL; nothing when it is None."""
    from incremental_detection import DAY_US, FULL, _within, widen

    if cut is None:
        return
    where = "rule_id = 'baseline'"
    rows = txns
    if cut != FULL:
        days = widen(cut, 0, 0, DAY_US)
        rows = _within(txns, days)
        where += (
            " AND ("
            + " OR ".join(
                f"(dashboard_date >= to_date(timestamp_micros({lo}))"
                f" AND dashboard_date < to_date(timestamp_micros({hi})))"
                for lo, hi in days
            )
            + ")"
        )
    baseline = build_baseline_dashboards(rows, RUN_ID)
    spark.sql(f"DELETE FROM {CATALOG}.{GOLD_DASH} WHERE {where}")
    baseline.writeTo(f"{CATALOG}.{GOLD_DASH}").append()


def _union_cut(a, b):
    """Two cuts (tick_position) as one: FULL wins, None is nothing."""
    from incremental_detection import FULL, merge_spans

    if a is None or b is None:
        return b if a is None else a
    if a == FULL or b == FULL:
        return FULL
    return merge_spans(a + b)


def tick_position(txns, seen):
    """``(cut, position)`` of the rows this tick reads (``txns``) against the
    previous tick's ``seen`` ({stream_id: newest batch_id}): ``cut`` is the
    event-time spans (epoch microseconds, whole UTC days, merged) that hold
    the rows outside ``seen``, None when there are none and
    incremental_detection.FULL when ``seen`` is None (unknown); ``position`` is ``seen`` advanced by the newest batch of
    those rows per stream. A silver without the stream's batch columns gives
    (FULL, None), so every tick recomputes in full.

    Both come from one read of the same frame the rules read, so a batch
    counts as seen only once its rows were read: a sealed batch whose rows
    are briefly absent (silver replays it after a restart) is new when they
    come back."""
    from incremental_detection import FULL

    if not {"_stream_id", "_batch_id"} <= set(txns.columns):
        # No batch columns to place a row: every tick is a full recompute.
        return FULL, None
    prior = dict(seen or {})
    # Plain comparisons, so Iceberg skips the files of batches already seen
    # by their column bounds.
    new = ~col("_stream_id").isin(sorted(prior)) if prior else lit(True)
    for stream_id, batch_id in sorted(prior.items()):
        new = new | (
            (col("_stream_id") == lit(stream_id)) & (col("_batch_id") > lit(int(batch_id)))
        )
    from incremental_detection import DAY_US, merge_spans

    # One row per (stream, day of new rows): datagen pods drift apart in
    # event time, so a tick's new rows lie on separate runs of days.
    day = (unix_micros(col("txn_timestamp")) / lit(DAY_US)).cast("long")
    rows = (
        txns.filter(new)
        .groupBy("_stream_id", day.alias("d"))
        .agg(max_("_batch_id").alias("b"))
        .collect()
    )
    position = dict(prior)
    for r in rows:
        if r["_stream_id"] is not None and r["b"] is not None:
            position[r["_stream_id"]] = max(int(r["b"]), position.get(r["_stream_id"], -1))
    if seen is None:
        return FULL, position
    days = sorted({int(r["d"]) for r in rows if r["d"] is not None})
    return (merge_spans((d * DAY_US, (d + 1) * DAY_US) for d in days) or None), position


def manifest_files(spark):
    """The manifest files under MANIFEST_PATH now, with their sizes; None
    when they cannot be listed."""
    try:
        jvm = spark._jvm  # type: ignore[attr-defined]
        hconf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
        path = jvm.org.apache.hadoop.fs.Path(f"{BRONZE_URI}{MANIFEST_PATH}")
        found = path.getFileSystem(hconf).globStatus(path) or []
        return frozenset(f"{s.getPath().toString()}:{s.getLen()}" for s in found)
    except Exception as e:  # noqa: BLE001
        log(f"[manifest] could not list {MANIFEST_PATH}: {one_line(e)}")
        return None


def run_tick(spark, state, cycle) -> dict:
    """One detection tick. Returns its phase timings ({name: seconds}, in
    order, ``total`` last), which are also logged as the tick timing line.
    Raises when the tick fails as a whole (silver unreadable, a gold write
    failed); a single rule's failure is isolated inside run_detection_rules."""
    tick = time.time()
    phases = {}
    # Back to back, ticks read these tables more often than the catalog
    # cache expires, so without a refresh a tick keeps the snapshots the
    # first one loaded (an empty silver, for the whole run).
    for table in (
        SILVER_TXNS,
        SILVER_BATCH_VERSIONS,
        SILVER_ENTITIES,
        SILVER_ACCOUNTS,
        BRONZE_TABLE,
    ):
        refresh_table(spark, f"{CATALOG}.{table}")
    # The continuous reset drops the previous run's manifest table; register
    # this run's once datagen has written it, and again whenever the files
    # change: continuous datagen adds one manifest per live period.
    files = manifest_files(spark)
    if not state.manifest_ready or (files is not None and files != state.manifest_files):
        if register_manifest(spark):
            state.manifest_ready = True
            state.manifest_files = files
    # Pinned before anything reads silver, so every reader sees this corpus.
    pinned_at = time.time()
    txns, sid, versions_used, silver_rows, newest_ingest_s, tt = _pin_silver(spark)
    # The tick record: the snapshots this tick read (entities and accounts by
    # metadata only; W2 still reads entities live), for the covered scorer.
    records = [
        tick_pinned_line(
            cycle,
            sid,
            _current_snapshot(spark, f"{CATALOG}.{SILVER_ENTITIES}"),
            _current_snapshot(spark, f"{CATALOG}.{SILVER_ACCOUNTS}"),
            versions_used,
            pinned_at,
        )
    ]
    if tt is not None:
        # Metadata counts of the pinned snapshot, for the time-travel read
        # after the window; nothing on the tick path scans it.
        records.append(tt_record_line(cycle, SILVER_TXNS, tt))
    for line in records:
        log(line)
    newest_bronze_s = _newest_ingest_epoch_s(spark, f"{CATALOG}.{BRONZE_TABLE}")
    if silver_rows == 0:
        log(f"Cycle {cycle}: Silver table is empty, skipping")
    else:
        log(f"Cycle {cycle}: aggregating {silver_rows:,} Silver records")
    phases["probe"] = time.time() - tick

    # Detection: the batch driver over the pinned corpus, each rule brought
    # up to date from the new rows (incremental_detection). Per-rule DELETE +
    # INSERT makes this tick's alerts the single correct set for each rule; per-rule errors are isolated inside the driver, so only a failure
    # to read silver at all raises up to here. The snapshot before the rewrite
    # is what this tick's alerts are compared against to find the newly
    # raised ones.
    t = time.time()
    ttd_base = state.ttd_baseline.begin(_prior_alerts_snapshot(spark), state.prev_tick_seen)
    # The silver position this tick read, from its own rows: the next tick's
    # new rows, late alerts and incremental cut are all judged against it.
    cut, seen_now = tick_position(txns, state.prev_tick_seen)
    # The baseline days from this cut are pending until a refresh writes
    # them, even if this tick fails before its baseline step.
    if cut is not None:
        state.baseline_cut = _union_cut(state.baseline_cut, cut)
    # Parsed by metrics/continuous_window.py: a cycle with None found no new
    # silver row, and is left out of gold's cadence.
    from incremental_detection import FULL

    earliest = cut if cut is None or cut == FULL else cut[0][0]
    log(f"Cycle {cycle}: earliest new event time (us) {earliest}")
    if cut is not None and cut != FULL:
        days = sum(hi - lo for lo, hi in cut) / 86_400_000_000
        log(f"Cycle {cycle}: new rows in {len(cut)} span(s), {days:.0f} day(s)")
    if state.incremental is not None:
        state.incremental.begin_tick(cut, CONTINUOUS_RULES)
    state.prev_tick_seen = seen_now
    detection = run_detection_rules(
        spark,
        txns,
        RUN_ID,
        rules=CONTINUOUS_RULES,
        skipped_rules=CONTINUOUS_SKIPPED_RULES,
        first_detection=True,
        incremental=state.incremental,
        rule_overrides=CONTINUOUS_RULE_OVERRIDES,
    )
    detection_end_s = time.time()
    # The alerts and statuses this tick committed: the covered scorer reads
    # gold.alerts and the detection status at exactly these snapshots.
    # Each rule's status at this tick, so a verdict can judge the scored
    # tick's rules even when scoring could not run.
    records.append(rule_status_line(cycle, (detection or {}).get("status", [])))
    log(records[-1])
    records.append(
        f"Cycle {cycle}: committed alerts={_token(_current_snapshot(spark, f'{CATALOG}.{GOLD_ALERTS}'))}"
        f" status={_token(_current_snapshot(spark, f'{CATALOG}.{GOLD_STATUS}'))} run={RUN_ID}"
    )
    log(records[-1])
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
        # Only the days from the earliest new event time on can change: a
        # day's row aggregates that day's transactions alone.
        refresh_baseline(spark, txns, state.baseline_cut)
        state.baseline_cut = None
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
    records.append(f"Cycle {cycle}: completed run={RUN_ID}")
    log(records[-1])
    state.tick_records = records
    # Collector line formats. Freshness: how long ago the newest row
    # this tick's detection saw arrived (bronze ingest_ts), i.e. how stale the alerts are
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
    state.ttd_scope = _ttd_scope(state.ttd_scope, committed, state.incremental)
    if _log_time_to_detect(
        spark,
        cycle,
        ttd_base,
        detection_end_s,
        silver=txns,
        detected_by_rule=committed,
        scope=state.ttd_scope,
    ):
        state.ttd_baseline.measured(_prior_alerts_snapshot(spark))
        state.ttd_scope = {}
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


def _stop_marker_path() -> str:
    return GOLD_CHECKPOINT.rstrip("/") + "/" + STOP_MARKER


def stop_requested(spark) -> bool:
    """Whether the CLI asked this run to drain: the marker exists under the
    checkpoint and its body is this run's id (a marker meant for another run
    of the deployment is ignored). A read error counts as absent, logged:
    stopping on a transient S3 error would end detection mid-window."""
    if not GOLD_CHECKPOINT:
        return False
    try:
        jvm = spark._jvm
        path = jvm.org.apache.hadoop.fs.Path(_stop_marker_path())
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(path):
            return False
        stream = fs.open(path)
        try:
            body = jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
        finally:
            stream.close()
        return str(body).strip() == RUN_ID
    except Exception as e:  # noqa: BLE001 -- absent, and say so
        global _MARKER_ERROR_LOGGED_AT
        if time.time() - _MARKER_ERROR_LOGGED_AT >= 60:
            _MARKER_ERROR_LOGGED_AT = time.time()
            log(f"[drain] stop marker unreadable, treated as absent: {one_line(e)}")
        return False


def _idle_after_drain(spark, last_cycle, at_start=False, records=()) -> None:
    """Report the drain, free the executors, and keep the pod (and its log)
    until the CLI deletes the application. If this script exited the operator
    would move the SparkApplication to COMPLETED and delete the pod before
    the CLI reads the drain line (and would NOT auto-rerun it either:
    streaming jobs use ``restartPolicy OnFailure`` with
    ``onFailureRetries=0``). The drain line is logged again every minute so
    it stays at the tail of the log the CLI polls whatever Spark logs while
    it stops. ``records`` (the last completed tick's pinned, tt-record and
    committed lines) go before each drain line: a driver log rotated during
    that tick would otherwise lose the snapshots the scorer reads."""
    where = "stop marker present at start; " if at_start else ""
    line = f"Drain complete: {where}last completed cycle {last_cycle} run={RUN_ID}"

    def report():
        for rec in records:
            log(rec)
        log(line)

    report()
    try:
        spark.stop()
    except Exception as e:  # noqa: BLE001
        log(f"[drain] spark.stop failed: {one_line(e)}")
    report()
    waited = 0
    while not _SHUTDOWN:
        time.sleep(1.0)
        waited += 1
        if waited % 60 == 0:
            report()


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

    # If this process started on top of an already-drained run (stop marker
    # present), do no further work. The streaming policy
    # (OnFailure, onFailureRetries=0) will not restart this script after a
    # drain; the check is kept for defence in depth.
    if stop_requested(spark):
        _idle_after_drain(spark, 0, at_start=True)
        return

    _bootstrap_gold_tables(spark)

    # Earlier runs' alerts are cleared by run_detection_rules on each tick,
    # after the tick's 'pending' status is written.
    consecutive_failures = 0
    state = TickState.start(spark)
    cycle = 0
    last_completed = 0
    draining = False

    while not _SHUTDOWN and not draining:
        if stop_requested(spark):
            break
        tick = time.time()
        cycle += 1
        try:
            run_tick(spark, state, cycle)
            last_completed = cycle
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

        # Sleep in short steps to a deadline, so a signal or the drain
        # marker is honoured within about a second and the marker read's own
        # time does not stretch the refresh interval.
        deadline = tick + REFRESH_S
        while not _SHUTDOWN:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            if stop_requested(spark):
                draining = True
                break
            time.sleep(min(1.0, remaining))

    if not _SHUTDOWN:
        # The drain marker: the current tick finished, and every rule ran on
        # its one silver snapshot, so its alerts are the ones scored; report
        # and wait.
        if state.incremental is not None:
            # No tick follows a drain; the alerts are in gold.alerts.
            state.incremental.discard_all()
        _idle_after_drain(spark, last_completed, records=state.tick_records)
        return
    log("SIGTERM/SIGINT received; refresh loop exiting cleanly")
    spark.stop()


if __name__ == "__main__":
    main()
