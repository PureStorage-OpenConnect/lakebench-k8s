"""Gold Finalize (Financial) -- bootstrap gold tables, baseline rollups, and run detection rules.

Runs at the end of the batch pipeline (bronze_verify -> silver_build ->
this). Steps:

1. Bootstraps the four Financial gold tables (create-if-not-exists),
   matching src/lakebench/deploy/financial_ddl.py.
2. Emits an initial daily_dashboards baseline row per (day, "baseline")
   derived from silver.transactions -- row count and total volume --
   so downstream consumers have a starting shape to plot against.
3. Invokes every W-rule in detection_rules.py against silver and writes
   the union of their alerts to gold.alerts. Idempotent: DELETE per
   rule_id before append, so re-runs against the same silver corpus
   produce reproducible alert counts. Rule failures are isolated per
   rule -- a broken rule does not abort the pipeline; a stderr line
   surfaces the error for postmortem.

4. Runs the transaction-monitoring operations layer on the alerts
   (tm_operations.py, GOALS P10 stages 1, 6, 7, 8): reconciliation,
   scenario coverage, L1 dispositions, cases and SAR decisions, then the
   workflow invariants the CLI gates on.

5. Prints the alert-set fingerprint of the run's alerts (``LB_ALERT_SET``),
   last, so the CLI can take its seconds off the stage's time.

The detection step means `lakebench run` on a batch AML config produces
alerts as part of the pipeline itself, so the baseline row is populated
from `metrics.json` without a separate `lakebench financial replay`
invocation.
"""

from __future__ import annotations

import json
import time
import uuid

from common import (
    ALERT_SET_SPEC,
    ICEBERG_V2_SNAPPY_PROPS_SQL,
    alert_set_fingerprint,
    ensure_alert_columns,
    ensure_namespaces_for_ddl,
    ensure_partition_transform,
    env,
    iceberg_table_stats,
    log,
    log_job_metrics,
    one_line,
    rule_profile_mark,
    rule_stage_profile,
    sealed_txns_filter,
)
from detection_rules import ALERT_COLUMNS
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    col,
    countDistinct,
    current_timestamp,
    explode,
    lit,
    to_date,
)
from pyspark.sql.functions import (
    count as count_,
)
from pyspark.sql.functions import (
    sum as sum_,
)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_BATCH_VERSIONS = env("LB_FINANCIAL_SILVER_BATCH_VERSIONS", "silver.silver_batch_versions")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
GOLD_RISK = env("LB_FINANCIAL_GOLD_RISK_SCORES", "gold.risk_scores")
GOLD_CLUSTERS = env("LB_FINANCIAL_GOLD_CLUSTERS", "gold.entity_clusters")
GOLD_DASH = env("LB_FINANCIAL_GOLD_DASHBOARDS", "gold.daily_dashboards")
GOLD_STATUS = env("LB_FINANCIAL_GOLD_DETECTION_STATUS", "gold.detection_status")
RUN_ID = env("LB_RUN_ID", str(uuid.uuid4()))

# W1 connected-components vertex cap: detection_rules.rule_params reads
# LB_FINANCIAL_W1_MAX_VERTICES (default 8,000,000, above the scale-10 vertex
# count of 1.1M entities; raise it via the `financial.w1_max_vertices` config
# field for larger scales). A value <= 0 keeps the rule's own default.

# Which W-rules to invoke as part of gold_finalize. Ordering is
# intentional (cheapest first) so a failure in an expensive rule does
# not prevent the cheap ones from writing their alerts. W5 and W6 screen
# payment beneficiaries against the corpus watchlist
# (bronze/watchlist.parquet); a corpus without one records them as skipped.
#: Rules that take the shared screening base (detection_rules.screen_base_frame).
SCREEN_BASE_RULES = ("W5_sanctions_match", "W6_pep_counterparty")

DEFAULT_DETECTION_RULES = (
    "W5_sanctions_match",
    "W6_pep_counterparty",
    "W2_structuring",
    "W3_round_tripping",
    "W17_layering_chain",
    "W4_risk_propagation",
    "W7_cross_border_high_risk",
    "W8_dormant_reactivation",
    "W1_connected_components",  # last -- most expensive (graph propagation)
)


def _alerts_ddl_columns() -> str:
    """The gold.alerts column list, from detection_rules.ALERT_COLUMNS."""
    return ",\n".join(
        f"    {name:<18} {ddl_type}{'' if nullable else ' NOT NULL'}"
        for name, ddl_type, nullable in ALERT_COLUMNS
    )


DDL_ALERTS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_ALERTS} (
{_alerts_ddl_columns()}
) USING iceberg PARTITIONED BY (months(alert_ts))
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""

DDL_RISK = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_RISK} (
    entity_id              BIGINT NOT NULL,
    model_id               STRING NOT NULL,
    model_version          STRING NOT NULL,
    risk_score             DOUBLE NOT NULL,
    risk_tier              STRING NOT NULL,
    contributing_rule_ids  ARRAY<STRING>,
    computed_ts            TIMESTAMP NOT NULL,
    run_id                 STRING NOT NULL
) USING iceberg
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""

DDL_CLUSTERS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_CLUSTERS} (
    cluster_id            STRING NOT NULL,
    detection_run_id      STRING NOT NULL,
    detection_algorithm   STRING NOT NULL,
    member_entity_ids     ARRAY<BIGINT> NOT NULL,
    cluster_size          INT NOT NULL,
    first_seen_ts         TIMESTAMP NOT NULL,
    detected_ts           TIMESTAMP NOT NULL,
    suspicion_score       DOUBLE,
    cluster_type          STRING
) USING iceberg
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""

DDL_STATUS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_STATUS} (
    rule_id          STRING NOT NULL,
    status           STRING NOT NULL,
    reason           STRING,
    target_typology  STRING,
    alert_count      BIGINT,
    run_id           STRING NOT NULL,
    computed_ts      TIMESTAMP NOT NULL
) USING iceberg
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""
# Durable, data-plane record of what each detection rule did this
# run -- 'ran' (with alert_count), 'skipped' (with reason, e.g. vertex-cap),
# or 'error' (with the exception class). This is the bridge that lets
# score_financial mark a skipped rule's target typology "not run" in
# recall.parquet instead of 0%, which the log-only rules_skipped in
# metrics.json could not reach (the recall scorer runs in Spark and reads
# the data plane, not the driver log). Overwritten every gold_finalize run
# so it reflects the run that produced the current gold.alerts.

DDL_DASH = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_DASH} (
    dashboard_date        DATE NOT NULL,
    rule_id               STRING NOT NULL,
    disposition           STRING,
    priority              STRING,
    alert_count           BIGINT NOT NULL,
    entity_count          BIGINT NOT NULL,
    total_alerted_amount_usd DECIMAL(20, 2),
    tp_count              BIGINT,
    fp_count              BIGINT,
    recall                DOUBLE,
    precision_val         DOUBLE,
    computed_ts           TIMESTAMP NOT NULL,
    run_id                STRING NOT NULL
) USING iceberg
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""


def _sealed_txns(spark, txns_fq: str):
    """Return ``silver.transactions`` semi-joined against silver_batch_versions
    (I10). Thin wrapper over ``common.sealed_txns_filter`` that also does
    the initial ``spark.table`` read. Callers that pin at an Iceberg
    snapshot call ``sealed_txns_filter`` directly with their pinned frame.
    """
    return sealed_txns_filter(
        spark,
        spark.table(f"{CATALOG}.{txns_fq}"),
        CATALOG,
        SILVER_BATCH_VERSIONS,
    )


def build_baseline_dashboards(txns, run_id: str):
    """Baseline: one row per settlement day, keyed rule_id='baseline'.

    `alert_count` uses the raw txn count as a baseline volumetric aggregate
    -- explicitly NOT an alert count. Consumers must filter `rule_id !=
    'baseline'` when summing real alerts, or every raw txn becomes a
    phantom alert (~10^9 at scale 100).

    `entity_count` counts distinct entities across BOTH originator and
    beneficiary sides. The prior implementation only counted originators,
    which systematically undercounted unique entities by ~2x on the
    executive dashboard.

    `run_id` is passed in explicitly, not read from a module-level closure.
    The prior design closed over `RUN_ID` at import time, so a refresh
    loop's per-tick UUID from a caller module never reached the written
    rows -- every refresh silently reused the finalize module's UUID.
    """
    # Stack originator + beneficiary into a single column so we can count
    # distinct entities across both sides. pyspark can't call explode()
    # inside an .agg() (explode is table-valued), so we explode BEFORE
    # aggregating and count distinct on the resulting long-form column.
    txn_dates = txns.select(
        to_date(col("txn_timestamp")).alias("dashboard_date"),
        col("txn_amount_usd"),
        explode(array(col("originator_id"), col("beneficiary_id"))).alias("entity_id"),
    )
    return (
        txn_dates.groupBy("dashboard_date")
        .agg(
            (count_(lit(1)) / lit(2)).cast("bigint").alias("alert_count"),
            countDistinct("entity_id").alias("entity_count"),
            (sum_("txn_amount_usd") / lit(2))
            .cast("decimal(20,2)")
            .alias("total_alerted_amount_usd"),
        )
        .select(
            col("dashboard_date"),
            lit("baseline").alias("rule_id"),
            lit(None).cast("string").alias("disposition"),
            lit(None).cast("string").alias("priority"),
            col("alert_count"),
            col("entity_count"),
            col("total_alerted_amount_usd"),
            lit(None).cast("bigint").alias("tp_count"),
            lit(None).cast("bigint").alias("fp_count"),
            lit(None).cast("double").alias("recall"),
            lit(None).cast("double").alias("precision_val"),
            current_timestamp().alias("computed_ts"),
            lit(run_id).alias("run_id"),
        )
    )


def read_snapshot_line(table: str, snapshot, total_records) -> str:
    """One ``[read-snapshot]`` line (metrics/read_snapshots.py parses it):
    the snapshot of *table* gold read, ``none`` when the table has no
    snapshot and ``unknown`` when the lookup failed; ``null`` for an unknown
    record count."""
    snap = "none" if snapshot is None else str(snapshot)
    count = "null" if total_records is None else str(int(total_records))
    return f"[read-snapshot] table={table} snapshot={snap} total_records={count}"


def log_read_snapshots(spark) -> None:
    """Log the current snapshot of silver.transactions, silver.entities and
    the versions table (it decides which batches are sealed) with its
    summary record count, from ``.history`` and ``.snapshots`` only."""
    for table in (SILVER_TXNS, SILVER_ENTITIES, SILVER_BATCH_VERSIONS):
        fq = f"{CATALOG}.{table}"
        snapshot = total = None
        try:
            rows = spark.sql(
                f"SELECT snapshot_id FROM {fq}.history "
                "WHERE is_current_ancestor ORDER BY made_current_at DESC LIMIT 1"
            ).collect()
            snapshot = int(rows[0][0]) if rows else None
            if snapshot is not None:
                r = spark.sql(
                    f"SELECT summary['total-records'] AS n FROM {fq}.snapshots "
                    f"WHERE snapshot_id = {snapshot}"
                ).collect()
                total = int(r[0]["n"]) if r and r[0]["n"] is not None else None
        except Exception as e:  # noqa: BLE001 -- recorded as unknown
            log(f"[read-snapshot] {table} lookup failed: {one_line(e)}")
            snapshot = "unknown" if snapshot is None else snapshot
        log(read_snapshot_line(table, snapshot, total))


def main() -> None:
    spark = SparkSession.builder.appName("lb-gold-finalize-financial").getOrCreate()
    # UTC pin (same rationale as silver_build): to_date(txn_timestamp) uses
    # session tz for the day boundary, so a non-UTC executor tz shifts
    # dashboard_date by up to one day for late-night UTC txns and breaks the
    # score_financial join to the manifest's UTC-based UETRs.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    start = time.time()

    log("=" * 60)
    log("Gold Finalize (Financial)")
    log(f"Run ID: {RUN_ID}")
    log(f"Session TZ: {spark.conf.get('spark.sql.session.timeZone')}")
    log("=" * 60)

    ensure_namespaces_for_ddl(
        spark, CATALOG, (DDL_ALERTS, DDL_RISK, DDL_CLUSTERS, DDL_DASH, DDL_STATUS)
    )
    for name, ddl in (
        ("alerts", DDL_ALERTS),
        ("risk_scores", DDL_RISK),
        ("entity_clusters", DDL_CLUSTERS),
        ("daily_dashboards", DDL_DASH),
        ("detection_status", DDL_STATUS),
    ):
        spark.sql(ddl)
        log(f"Bootstrapped gold.{name}")

    # Reused-catalog upgrade: `CREATE TABLE IF NOT EXISTS` is a no-op on an
    # older gold.alerts (without detected_ts or reason_codes), and the
    # detection loop's positional `INSERT INTO gold.alerts SELECT *` needs
    # the table's columns to be ALERT_COLUMNS in order. Missing trailing
    # columns are appended; any other difference fails the job here rather
    # than writing columns into each other.
    ensure_alert_columns(spark, f"{CATALOG}.{GOLD_ALERTS}", ALERT_COLUMNS)
    ensure_partition_transform(
        spark, f"{CATALOG}.{GOLD_ALERTS}", "days(alert_ts)", "months(alert_ts)"
    )

    # Earlier runs' alerts are cleared inside run_detection_rules, after this
    # run's 'pending' status is written (see there for why the order matters).

    # The snapshots gold reads, before its first silver read, so financial
    # reproduce can read exactly them later (batch has no concurrent writer:
    # silver-build refuses to run beside a stream). Metadata only.
    log_read_snapshots(spark)

    # I10: read silver.transactions through the sealed-batch filter so a
    # driver crash between the stream's transactions/edges commits and the
    # sidecar sealed marker is invisible to every downstream rule and the
    # baseline dashboards.
    txns = _sealed_txns(spark, SILVER_TXNS)
    baseline = build_baseline_dashboards(txns, RUN_ID)
    # Delete-only-baseline-then-append: overwrite ONLY the baseline rows,
    # not the whole table. Detection workloads write rows keyed by
    # non-'baseline' rule_ids into the same table; the previous
    # createOrReplace / overwrite(lit(True)) would wipe every one of them
    # on a finalize re-run (silent data loss of the exact rows scoring
    # depends on). Uses SQL DELETE + append because DataFrameWriterV2's
    # overwrite(condition) requires a Column that references source-side
    # columns, which is awkward when the intent is a target-side filter.
    spark.sql(f"DELETE FROM {CATALOG}.{GOLD_DASH} WHERE rule_id = 'baseline'")
    baseline.writeTo(f"{CATALOG}.{GOLD_DASH}").append()
    log(f"Wrote {GOLD_DASH} baseline rows")

    try:
        run_detection_rules(spark, txns, RUN_ID, profile_stages=True)
    finally:
        # W1's reliable checkpoints; see detection_rules.cleanup_w1_checkpoints.
        from detection_rules import cleanup_w1_checkpoints

        cleanup_w1_checkpoints(spark)

    # P10: the operations layer on this cycle's alerts. Never raises; a
    # failure is logged as the 'workflow' invariant, which fails the run.
    from tm_operations import run_tm_operations

    run_tm_operations(spark, txns, RUN_ID)

    # The alert-set fingerprint, after the last write to gold.alerts
    # (run_tm_operations only reads it) and last in the stage. It is
    # Lakebench's work, not the pipeline's: the line carries its seconds and
    # the CLI takes them off the stage's time (cli/_run.py
    # _exclude_alert_set_time), as it does for the c360 check.
    log(alert_set_line(spark, RUN_ID))

    elapsed = time.time() - start
    log("=" * 60)
    log(f"Gold finalize complete in {elapsed:.1f}s")
    log("=" * 60)
    silver_rows, silver_gb = iceberg_table_stats(spark, f"{CATALOG}.{SILVER_TXNS}")
    try:
        run_alerts = spark.table(f"{CATALOG}.{GOLD_ALERTS}").where(col("run_id") == RUN_ID).count()
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] alert count unavailable: {one_line(e)}")
        run_alerts = 0
    log_job_metrics(
        "gold-finalize",
        input_size_gb=silver_gb,
        input_rows=silver_rows,
        output_rows=run_alerts,
        elapsed_seconds=elapsed,
    )
    spark.stop()


ALERT_SET_TAG = "LB_ALERT_SET"


def alert_set_line(spark, run_id: str, table: str | None = None) -> str:
    """The ``LB_ALERT_SET {json}`` line for this run's rows of gold.alerts:
    ``common.alert_set_fingerprint`` plus ``seconds``, the time
    the fingerprint took. Never raises: when the fingerprint cannot be
    computed the line carries ``unavailable`` with the reason instead, and
    the record then has no alert set, which ``compare`` reads as results
    not established on an exp2 record. Computed in Spark, never through the
    query engine (engines hash differently), and never per continuous tick.
    """
    t0 = time.time()
    try:
        alerts = spark.table(table or f"{CATALOG}.{GOLD_ALERTS}").where(
            col("run_id") == lit(run_id)
        )
        body = alert_set_fingerprint(alerts)
    except Exception as e:  # noqa: BLE001
        body = {"spec": ALERT_SET_SPEC, "unavailable": one_line(e)}
    body["seconds"] = round(time.time() - t0, 3)
    return f"{ALERT_SET_TAG} {json.dumps(body, sort_keys=True)}"


def run_detection_rules(
    spark,
    txns,
    run_id: str,
    rules=None,
    skipped_rules=None,
    profile_stages=False,
    first_detection=False,
    incremental=None,
    rule_overrides=None,
) -> dict:
    """Invoke each configured detection rule and append alerts to gold.alerts.

    Per-rule isolation: a rule that raises is logged and skipped, and
    the pipeline continues with the remaining rules. That means one
    bad rule cannot cost the whole run its baseline row -- which is
    exactly the failure mode that showed up in the first live S1 run
    before the W1 alias fix landed.

    Idempotency + partial-write safety: for each rule that produces
    alerts we (1) materialise the alerts frame to a temp view so the
    write step does not re-compute the rule against silver, then (2)
    inside a Spark SQL BEGIN/COMMIT-free sequence, ``DELETE`` prior
    rows for this rule_id and ``INSERT INTO ... SELECT * FROM tmp``
    in the same driver call. Iceberg's per-statement snapshot commit
    means a failure of the INSERT half rolls the writer forward with
    the DELETE snapshot already committed, so we log the incident and
    exit non-zero for that one rule. The next successful invocation
    against the same silver corpus re-materialises the alerts from
    the deterministic rule function. Downstream consumers reading
    gold.alerts between commits see either the prior committed state
    or the new one -- never a mix.

    Two concurrent ``lakebench run`` invocations against the
    SAME catalog will race on ``DELETE WHERE rule_id = ...``; the
    intended usage model is one live pipeline per catalog / namespace
    at a time. Cross-namespace concurrent
    runs are safe because each has its own Iceberg gold.alerts table.

    Metrics: the per-rule ``alerts=N`` line is picked up by the
    driver-log parser in metrics/collector.py. On a rule failure we
    still emit ``alerts=0 error=...`` (instead of a bare FAILED line)
    so the parser sees a row for every rule that was attempted, not
    just the ones that succeeded.

    Returns the pass's timings, which the continuous tick logs and measures
    time to detect against: {"setup_s", "cleanup_s", "finish_s", "rules": {rule_id:
    {"elapsed_s", "committed_s"}}}. ``committed_s`` is the epoch time at
    which the rule's alerts were committed to gold.alerts, None for a rule
    that did not run (skip or error). ``cleanup_s`` is the per-rule cache
    and path-spill cleanup, summed. Batch ignores it.

    ``profile_stages`` (batch gold-finalize): run each rule in its own Spark
    job group and log its heaviest stages after it (``[stage-profile]``,
    common.rule_stage_profile), restoring the caller's job group after each
    rule. The profile runs after the rule's commit, so the rule's elapsed
    time and commit time do not include it; it counts in ``cleanup_s``. Off
    by default, so the continuous tick's timings are unchanged.

    ``first_detection`` (continuous): an alert whose content an earlier
    pass already wrote keeps that pass's ``detected_ts``. The returned
    timings carry this pass's ``status`` rows. ``incremental`` (continuous):
    an ``incremental_detection.IncrementalDetection`` that runs its rules in
    place of their full recompute, with the same alerts; its state for a rule
    becomes current once the rule's alerts are written, and only the rows in
    its ``write_spans`` are rewritten. ``rule_overrides`` ({rule_id:
    {param: value}}) sets rule parameters on top of ``rule_params``.
    """
    import inspect

    from detection_rules import (
        RULE_TARGET_TYPOLOGY,
        RuleSkipped,
        cleanup_path_search_spill,
        get_rule,
        rule_params,
        sweep_stale_path_spill,
    )
    from incremental_detection import INCREMENTAL_RULES, incremental_line

    # Which rules to run this invocation. Batch passes None (the full default
    # set); the continuous gold loop passes a bounded set (W2/W3/W4/W17) plus a
    # skipped_rules list for the rules it deliberately does NOT run there --
    # W1 (per-tick graph recompute too costly), W7 (skipped from before
    # silver_stream appended silver.entities; see gold_refresh_financial), and
    # W8 (needs a 90-day dormancy gap a narrow continuous corpus cannot hold).
    # Marking them 'skipped' (not simply omitting them) is what lets
    # score_financial render their typologies "not run" instead of a false 0%.
    rules = tuple(rules) if rules is not None else DEFAULT_DETECTION_RULES
    skipped_rules = tuple(skipped_rules or ())
    pass_start = time.time()
    timings: dict = {"setup_s": 0.0, "cleanup_s": 0.0, "finish_s": 0.0, "rules": {}}

    # Load silver.entities once. The customer-scoped rules (W2, W5-W8) and
    # W7's country lookup need it; loading up-front is cheap
    # (Iceberg metadata read) and avoids each rule invocation paying its
    # own catalog resolution cost.
    try:
        silver_entities = spark.table(f"{CATALOG}.{SILVER_ENTITIES}")
    except Exception as e:  # noqa: BLE001 -- broad catch on catalog errors
        log(
            f"[detection] silver.entities not readable ({e}); "
            "customer-scoped rules (W2, W5-W8) will skip."
        )
        silver_entities = None

    # W3/W17 path levels a killed earlier driver left under the gold bucket.
    sweep_stale_path_spill(spark)

    log("=" * 60)
    log(f"Running detection rules: {list(rules)}")
    log("=" * 60)

    total_alerts = 0
    # Per-rule status accumulated for the durable gold.detection_status table
    # Each entry: (rule_id, status, reason, target_typology,
    # alert_count). status in {'pending','ran','skipped','error'}.
    status_rows: list[tuple] = []
    # Mark this run as in progress BEFORE touching gold.alerts. The per-rule
    # DELETE+INSERT below rewrites alerts rule by rule; if the driver died
    # mid-loop, detection_status would still name the PREVIOUS run and a
    # later score would read half-rewritten alerts as a complete run.
    # Scoring refuses any 'pending' status.
    _write_detection_status(
        spark,
        [(rid, "pending", None, RULE_TARGET_TYPOLOGY.get(rid), None) for rid in rules],
        run_id,
    )
    # gold.alerts holds THIS run's alerts only. Detection replaces each
    # rule's rows as it runs, so a rule that is skipped or fails left an
    # earlier run's rows behind: benchmark queries read them (one leftover
    # giant-component row made every read of gold.alerts fail on a 1 GB
    # Parquet page), and a "not run" rule still showed alerts. This runs
    # AFTER the pending write: done first, a crash in between left
    # detection_status naming the previous run as complete with its alerts
    # gone, and scoring then reported 0% recall as a valid result.
    spark.sql(f"DELETE FROM {CATALOG}.{GOLD_ALERTS} WHERE run_id <> '{run_id}'")
    log(f"[detection] cleared {GOLD_ALERTS} rows from runs other than {run_id}")
    timings["setup_s"] = time.time() - pass_start
    # With profile_stages, each rule runs in its own Spark job group, so its
    # jobs and stages can be attributed to it ([stage-profile] lines, read
    # from the status store after the rule). The caller's group is restored
    # after every rule.
    sc = spark.sparkContext
    caller_group = _job_group_props(sc) if profile_stages else None
    # W5 and W6 screen the same input (silver txns joined to the
    # beneficiary's country). When both run, it is built once, persisted, and
    # passed to each as screen_base; results are the same frame either way.
    screen_rules = [r for r in rules if r in SCREEN_BASE_RULES]
    screen_base = None
    # Not in continuous: the shared base joins all of silver, and an
    # incremental pass screens only the payments of its cut.
    if len(screen_rules) > 1 and silver_entities is not None and incremental is None:
        from detection_rules import screen_base_frame

        try:
            screen_base = screen_base_frame(txns, silver_entities).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            log(f"[detection] shared screening base for {', '.join(screen_rules)}")
        except Exception as e:  # noqa: BLE001 -- each rule then builds its own
            log(f"[detection] shared screening base not built: {one_line(e)}")
            screen_base = None
    for rule_id in rules:
        target_typology = RULE_TARGET_TYPOLOGY.get(rule_id)
        if (
            incremental is not None
            and rule_id in INCREMENTAL_RULES
            and incremental.unchanged(rule_id, silver_entities)
        ):
            # Nothing new since this rule's last pass: its alerts in
            # gold.alerts are this pass's, so they are not rewritten.
            rule_start = time.time()
            standing = int(
                spark.table(f"{CATALOG}.{GOLD_ALERTS}")
                .where((col("rule_id") == rule_id) & (col("run_id") == run_id))
                .count()
            )
            elapsed = time.time() - rule_start
            timings["rules"][rule_id] = {"elapsed_s": elapsed, "committed_s": None}
            log(f"[detection] {rule_id}: alerts={standing} elapsed={elapsed:.1f}s")
            log(f"[incremental] {rule_id}: mode=unchanged s={elapsed:.1f}")
            status_rows.append((rule_id, "ran", None, target_typology, standing))
            total_alerts += standing
            continue
        fn = get_rule(rule_id)
        if fn is None:
            log(f"[detection] {rule_id}: alerts=0 error=unknown-rule elapsed=0.0s")
            status_rows.append((rule_id, "error", "unknown-rule", target_typology, None))
            timings["rules"][rule_id] = {"elapsed_s": 0.0, "committed_s": None}
            continue
        group = mark = None
        if profile_stages:
            # Unique per invocation: a rerun of the rule in the same driver
            # must not read the earlier run's jobs from the status store.
            group = f"lb-rule-{rule_id}-{uuid.uuid4().hex[:8]}"
            sc.setJobGroup(group, f"{rule_id} run {run_id}", interruptOnCancel=False)
            mark = rule_profile_mark(spark)
        rule_start = time.time()
        rule_frame = None
        try:
            # Signature-based param filter, NOT ``__code__.co_varnames``
            # -- co_varnames includes every local in the function body,
            # so a future rule that uses ``silver_entities`` as an
            # internal local would silently receive the DataFrame as a
            # kwarg and TypeError, and the broad except below would
            # swallow it into a zero-alert result. inspect.signature is
            # the correct primitive for "does this function accept X
            # as a parameter".
            sig = inspect.signature(fn)
            # The parameters replay and reproduce use too (run_id, entities,
            # the configured W1 vertex cap), so a reproduction runs the rule
            # as gold did.
            params = rule_params(fn, run_id, silver_entities)
            params.update((rule_overrides or {}).get(rule_id, {}))
            if "silver_entities" in sig.parameters and "silver_entities" not in params:
                params["silver_entities"] = None  # the rule decides (it skips)
            if "screen_base" in sig.parameters and screen_base is not None:
                params["screen_base"] = screen_base
            run_rule = fn
            if incremental is not None and rule_id in INCREMENTAL_RULES:
                run_rule = incremental.rule(rule_id, fn)
            alerts = run_rule(txns, **params)
            # Persist before counting, so the rule is computed exactly once.
            # The loop used to count the frame and then INSERT from a temp
            # view, which is not materialised: every rule ran twice, each run
            # re-reading silver and redoing its joins. A rule that fails
            # while computing has its rows removed by the error handler
            # below, so gold.alerts never shows alerts for a rule whose
            # status is not 'ran'.
            spans = (
                incremental.write_spans(rule_id)
                if incremental is not None and rule_id in INCREMENTAL_RULES
                else None
            )
            if spans is not None:
                # Every row outside the spans is as the last pass wrote it,
                # so only the spans' rows are materialised and rewritten; the
                # full set is the rule's written state, counted in place.
                alert_count = alerts.count()
                alerts = alerts.filter(_in_spans(col("alert_ts"), spans)).persist(
                    StorageLevel.MEMORY_AND_DISK
                )
                rule_frame = alerts
                write_count = alerts.count()
            else:
                alerts = alerts.persist(StorageLevel.MEMORY_AND_DISK)
                rule_frame = alerts
                alert_count = write_count = alerts.count()
            if first_detection:
                alerts = _keep_first_detection(spark, alerts, rule_id, spans)
                rule_frame = alerts
            _write_rule_alerts(spark, alerts, rule_id, write_count, spans)
            committed_s = time.time()
            if incremental is not None and rule_id in INCREMENTAL_RULES:
                incremental.committed(rule_id)
                log(f"[incremental] {rule_id}: {incremental_line(incremental.last.get(rule_id))}")
            elapsed = committed_s - rule_start
            timings["rules"][rule_id] = {"elapsed_s": elapsed, "committed_s": committed_s}
            log(f"[detection] {rule_id}: alerts={alert_count} elapsed={elapsed:.1f}s")
            status_rows.append((rule_id, "ran", None, target_typology, int(alert_count)))
            total_alerts += alert_count
        except RuleSkipped as skip:
            # A structural skip is NOT a zero and NOT an error. Emit a
            # third log shape the collector records distinctly so the
            # scorecard renders "not run" rather than 0% recall. Rows of
            # earlier runs are already gone; rows this rule wrote on an
            # earlier tick of this run (continuous mode) are removed too, so
            # gold.alerts never shows alerts for a rule the status calls
            # not run.
            _drop_rule_alerts(spark, rule_id)
            if incremental is not None:
                incremental.failed(rule_id)
            elapsed = time.time() - rule_start
            timings["rules"][rule_id] = {"elapsed_s": elapsed, "committed_s": None}
            log(
                f"[detection] {rule_id}: skipped={skip.reason} "
                f"detail={one_line(skip.detail)} elapsed={elapsed:.1f}s"
            )
            status_rows.append((rule_id, "skipped", skip.reason, target_typology, None))
        except Exception as e:  # noqa: BLE001 -- one rule cannot fail the pipeline
            elapsed = time.time() - rule_start
            # Emit the alerts=0 shape so a downstream metrics parser
            # that greps for ``alerts=`` sees a row for every rule
            # that was attempted, not a silent hole for the failed
            # ones. ``error=`` carries the class/message; the rule's
            # rows are dropped below, so a failed write leaves none.
            err = one_line(f"{type(e).__name__}: {e}")
            log(f"[detection] {rule_id}: alerts=0 error={err} elapsed={elapsed:.1f}s")
            _drop_rule_alerts(spark, rule_id)
            if incremental is not None:
                incremental.failed(rule_id, drop_state=True)
            timings["rules"][rule_id] = {"elapsed_s": elapsed, "committed_s": None}
            status_rows.append((rule_id, "error", err, target_typology, None))
        finally:
            cleanup_start = time.time()
            if profile_stages:
                rule_stage_profile(spark, group, rule_id, mark=mark)
                _restore_job_group(sc, caller_group)
            # The alerts frame and W1/W3/W17's intermediate frames (edges,
            # step, path levels) are persisted. Nothing outlives the rule's
            # write, and left cached they hold executor memory and scratch
            # through every later rule. The one exception is the shared
            # screening base while a later screening rule still needs it:
            # then only this rule's alerts frame is dropped (W5 and W6 persist
            # nothing else).
            if screen_base is not None and rule_id in screen_rules[:-1]:
                if rule_frame is not None:
                    rule_frame.unpersist()
            else:
                spark.catalog.clearCache()
                if rule_id in screen_rules:
                    screen_base = None
            # W3/W17 write their path levels and results under the gold
            # bucket; the alerts are written (or dropped) by now.
            cleanup_path_search_spill(spark)
            timings["cleanup_s"] += time.time() - cleanup_start
    log(f"[detection] total alerts written: {total_alerts}")

    finish_start = time.time()
    for rule_id in skipped_rules:
        target_typology = RULE_TARGET_TYPOLOGY.get(rule_id)
        log(f"[detection] {rule_id}: skipped=mode-excluded elapsed=0.0s")
        status_rows.append((rule_id, "skipped", "mode-excluded", target_typology, None))

    timings["status"] = list(status_rows)
    _write_detection_status(spark, status_rows, run_id)

    # P0.5: project the two derived gold tables from the alerts the rules
    # just wrote. No new detection compute -- entity_clusters is a
    # reshape of W1 alerts, risk_scores a reshape of W4 alerts. Best-effort
    # and isolated: a failure here does not fail the pipeline, and an empty
    # source (e.g. W1 skipped at high scale) writes an empty derived table,
    # which is the correct "nothing to project" state, not a bug.
    _project_derived_gold(spark, run_id)
    timings["finish_s"] = time.time() - finish_start
    return timings


_JOB_GROUP_KEYS = ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")


def _job_group_props(sc) -> dict:
    """The thread's job-group local properties (None when unset)."""
    return {k: sc.getLocalProperty(k) for k in _JOB_GROUP_KEYS}


def _restore_job_group(sc, props: dict) -> None:
    """Put back the job-group properties ``_job_group_props`` read. A None
    value removes the property, as it was before the rule's group was set."""
    for k in _JOB_GROUP_KEYS:
        sc.setLocalProperty(k, props.get(k))


def _drop_rule_alerts(spark, rule_id: str) -> None:
    """Remove ``rule_id``'s rows after it skipped or failed. Best effort: a
    failure here is logged, and the status row still says the rule did not
    run."""
    try:
        spark.sql(f"DELETE FROM {CATALOG}.{GOLD_ALERTS} WHERE rule_id = '{rule_id}'")
    except Exception as e:  # noqa: BLE001
        log(f"[detection] {rule_id}: could not clear its rows: {one_line(e)}")


def _spans_sql(spans) -> str:
    """SQL predicate on alert_ts for ``spans`` (epoch-microsecond pairs)."""
    return (
        "("
        + " OR ".join(
            f"(alert_ts >= timestamp_micros({int(lo)}) AND alert_ts < timestamp_micros({int(hi)}))"
            for lo, hi in spans
        )
        + ")"
    )


def _in_spans(ts, spans):
    """Whether timestamp column ``ts`` lies in ``spans``."""
    from pyspark.sql.functions import timestamp_micros

    out = lit(False)
    for lo, hi in spans:
        out = out | ((ts >= timestamp_micros(lit(int(lo)))) & (ts < timestamp_micros(lit(int(hi)))))
    return out


def _keep_first_detection(spark, alerts, rule_id: str, spans=None):
    """``alerts`` (persisted) with each alert's ``detected_ts`` taken from
    the row of the same content (entity and sorted related transactions) an
    earlier pass wrote, when there is one: in continuous a rule's alerts are
    rewritten every pass, and detected_ts is when an alert was first raised.
    Persisted and counted here, before the write deletes those rows. With
    ``spans``, ``alerts`` holds only the rows there, and only those are read.
    """
    from pyspark.sql.functions import array_join, array_sort, coalesce, concat_ws
    from pyspark.sql.functions import min as min_

    def key(df):
        return concat_ws(
            "|",
            coalesce(df["entity_id"].cast("string"), lit("")),
            coalesce(array_join(array_sort(df["related_txn_ids"]), ","), lit("")),
        )

    from tm_operations import current_snapshot_id, read_at_snapshot

    # Pinned: the write's DELETE drops cached frames that read gold.alerts,
    # and a recompute of a live read would see the rows already deleted.
    fq = f"{CATALOG}.{GOLD_ALERTS}"
    prior = read_at_snapshot(spark, fq, current_snapshot_id(spark, fq)).where(
        col("rule_id") == lit(rule_id)
    )
    if spans is not None:
        prior = prior.where(_in_spans(col("alert_ts"), spans))
    first = (
        prior.select(key(prior).alias("_key"), col("detected_ts").alias("_first"))
        .groupBy("_key")
        .agg(min_("_first").alias("_first"))
    )
    merged = (
        alerts.withColumn("_key", key(alerts))
        .join(first, "_key", "left")
        .withColumn("detected_ts", coalesce(col("_first"), col("detected_ts")))
        .select(*alerts.columns)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    merged.count()
    return merged


def _write_rule_alerts(spark, alerts, rule_id: str, alert_count: int, spans=None) -> None:
    """Replace ``rule_id``'s rows in gold.alerts with ``alerts`` (persisted);
    with ``spans``, only its rows with alert_ts there, and ``alerts`` holds
    just those.

    DELETE then INSERT: two Iceberg commits, see run_detection_rules for the
    partial-write semantics.
    """
    tmp_view = f"_lb_alerts_{rule_id}"
    alerts.createOrReplaceTempView(tmp_view)
    where = f"rule_id = '{rule_id}'"
    if spans is not None:
        where += f" AND {_spans_sql(spans)}"
    try:
        spark.sql(f"DELETE FROM {CATALOG}.{GOLD_ALERTS} WHERE {where}")
        if alert_count > 0:
            spark.sql(f"INSERT INTO {CATALOG}.{GOLD_ALERTS} SELECT * FROM {tmp_view}")
    finally:
        spark.catalog.dropTempView(tmp_view)


def _write_detection_status(spark, status_rows: list, run_id: str) -> None:
    """Persist per-rule detection status to gold.detection_status.

    Overwrites the table with this run's status so it reflects the run that
    produced the current gold.alerts. Not best-effort: scoring scopes alerts
    to the run_id recorded here, so if this write failed silently the table
    would still name the PREVIOUS run and scoring would read that run's
    alerts as this run's results. A failure fails gold-finalize.
    """
    from pyspark.sql.functions import current_timestamp, lit

    try:
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("rule_id", StringType(), False),
                StructField("status", StringType(), False),
                StructField("reason", StringType(), True),
                StructField("target_typology", StringType(), True),
                StructField("alert_count", LongType(), True),
            ]
        )
        df = (
            spark.createDataFrame(status_rows, schema=schema)
            .withColumn("run_id", lit(run_id))
            .withColumn("computed_ts", current_timestamp())
        )
        df.writeTo(f"{CATALOG}.{GOLD_STATUS}").overwrite(lit(True))
        log(f"[detection] wrote {GOLD_STATUS} ({len(status_rows)} rule rows)")
    except Exception as e:  # noqa: BLE001
        log(f"[detection] detection_status write failed: {type(e).__name__}: {e}")
        raise


def _project_derived_gold(spark, run_id: str) -> None:
    """Reshape W1 alerts -> gold.entity_clusters and W4 alerts ->
    gold.risk_scores. Both are full overwrites keyed off gold.alerts, so
    re-running against the same alerts is deterministic and idempotent.

    Kept separate from the rule loop so a projection error cannot cost a
    rule its alerts, and so the derived-table schemas live next to their
    DDL. alert_ts is the last contributing transaction's event time (not a
    generation timestamp), so it seeds first_seen_ts; computed_ts /
    detected_ts use current_timestamp() to record when the projection ran.

    Scoped to THIS run's alerts (``run_id`` filter). Without it, a rule
    that SKIPPED this run (W1 above its vertex cap) would leave a prior
    run's W1 rows in gold.alerts, and this projection would re-emit them as
    "freshly detected" (detected_ts = now) while the alert scorecard says
    W1 did not run -- a direct contradiction on a reused catalog.
    Filtering by run_id makes a skipped rule contribute zero
    rows this run, so the derived table correctly shows "not run".
    """
    from pyspark.sql.functions import array, current_timestamp, size, when

    try:
        alerts = spark.table(f"{CATALOG}.{GOLD_ALERTS}").filter(col("run_id") == lit(run_id))
    except Exception as e:  # noqa: BLE001
        log(f"[derived] gold.alerts not readable ({e}); skipping projection.")
        return

    # gold.entity_clusters from W1 connected-components alerts. The
    # ``related_entity_ids IS NOT NULL`` filter guarantees the NOT NULL
    # member_entity_ids / cluster_size columns receive non-null input
    # regardless of the Spark storeAssignmentPolicy:
    # gold.alerts declares related_entity_ids nullable, so a by-name write
    # into the NOT NULL target would otherwise rely on ANSI runtime
    # assertion, and under STRICT would throw and (being caught below)
    # silently leave the table empty.
    try:
        clusters = (
            alerts.filter(col("rule_id") == lit("W1_connected_components"))
            .filter(col("related_entity_ids").isNotNull())
            .select(
                col("alert_id").alias("cluster_id"),
                col("run_id").alias("detection_run_id"),
                lit("connected_components").alias("detection_algorithm"),
                col("related_entity_ids").alias("member_entity_ids"),
                size(col("related_entity_ids")).cast("int").alias("cluster_size"),
                col("alert_ts").alias("first_seen_ts"),
                current_timestamp().alias("detected_ts"),
                col("alert_score").alias("suspicion_score"),
                lit("connected_component").alias("cluster_type"),
            )
        )
        clusters.writeTo(f"{CATALOG}.{GOLD_CLUSTERS}").overwrite(lit(True))
        n = spark.table(f"{CATALOG}.{GOLD_CLUSTERS}").count()
        log(f"[derived] wrote {GOLD_CLUSTERS} from W1 alerts ({n} rows)")
    except Exception as e:  # noqa: BLE001
        log(f"[derived] entity_clusters projection failed: {type(e).__name__}: {e}")

    # gold.risk_scores from W4 risk-propagation alerts. ``alert_score IS
    # NOT NULL`` filter guarantees the NOT NULL risk_score target receives
    # non-null input (same storeAssignmentPolicy rationale as clusters).
    try:
        from pyspark.sql.functions import expr

        # One row per entity: continuous W4 raises an alert per entity per
        # week, and the entity's score is its highest.
        risk = (
            alerts.filter(col("rule_id") == lit("W4_risk_propagation"))
            .filter(col("alert_score").isNotNull())
            .groupBy("entity_id", "model_id", "model_version", "run_id")
            .agg(
                expr("max(alert_score)").alias("risk_score"),
                # The entity's highest priority over its alerts, so the tier
                # does not depend on which of two equal scores comes first.
                expr(
                    "element_at(array('LOW', 'MED', 'HIGH'), max(CASE priority "
                    "WHEN 'HIGH' THEN 3 WHEN 'MED' THEN 2 ELSE 1 END))"
                ).alias("priority"),
            )
            .select(
                col("entity_id"),
                col("model_id"),
                col("model_version"),
                col("risk_score"),
                when(col("priority") == lit("HIGH"), lit("high"))
                .when(col("priority") == lit("MED"), lit("medium"))
                .otherwise(lit("low"))
                .alias("risk_tier"),
                array(lit("W4_risk_propagation")).alias("contributing_rule_ids"),
                current_timestamp().alias("computed_ts"),
                col("run_id"),
            )
        )
        risk.writeTo(f"{CATALOG}.{GOLD_RISK}").overwrite(lit(True))
        n = spark.table(f"{CATALOG}.{GOLD_RISK}").count()
        log(f"[derived] wrote {GOLD_RISK} from W4 alerts ({n} rows)")
    except Exception as e:  # noqa: BLE001
        log(f"[derived] risk_scores projection failed: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
