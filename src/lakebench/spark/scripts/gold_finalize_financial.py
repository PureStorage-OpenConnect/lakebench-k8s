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

The detection step means `lakebench run` on a batch FAML config produces
alerts as part of the pipeline itself, so the baseline row is populated
from `metrics.json` without a separate `lakebench financial replay`
invocation (LB-092).
"""

from __future__ import annotations

import time
import uuid

from common import env, iceberg_table_stats, log, log_job_metrics, one_line
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
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
GOLD_RISK = env("LB_FINANCIAL_GOLD_RISK_SCORES", "gold.risk_scores")
GOLD_CLUSTERS = env("LB_FINANCIAL_GOLD_CLUSTERS", "gold.entity_clusters")
GOLD_DASH = env("LB_FINANCIAL_GOLD_DASHBOARDS", "gold.daily_dashboards")
GOLD_STATUS = env("LB_FINANCIAL_GOLD_DETECTION_STATUS", "gold.detection_status")
RUN_ID = env("LB_RUN_ID", str(uuid.uuid4()))

# W1 connected-components vertex cap. Defaults above the scale-10 vertex
# count (1.1M entities) so W1 runs cleanly at scale 10 out of the box;
# raise it via the `financial.w1_max_vertices` config field for larger
# scales that have the executor budget. Whether W1 completes in
# acceptable wall-clock above the cap is a measured question (LB-120),
# not a config guarantee. A value <= 0 means "use the rule's own
# default" so a mis-set env var cannot silently disable W1.
try:
    _W1_MAX_VERTICES = int(env("LB_FINANCIAL_W1_MAX_VERTICES", "8000000"))
except ValueError:
    _W1_MAX_VERTICES = 8_000_000

# Which W-rules to invoke as part of gold_finalize. Ordering is
# intentional (cheapest first) so a failure in an expensive rule does
# not prevent the cheap ones from writing their alerts. W5 and W6 are
# excluded from the batch path: they depend on rptd_beneficiary_name
# fuzzy joins against packaged reference lists, which the silver schema
# and the driver-pod ConfigMap only partially wire up today; a future
# PR that adds the reference-list mount and completes the silver
# rptd_beneficiary_name column will move them into this list.
DEFAULT_DETECTION_RULES = (
    "W2_structuring",
    "W3_round_tripping",
    "W4_risk_propagation",
    "W7_cross_border_high_risk",
    "W8_dormant_reactivation",
    "W1_connected_components",  # last -- most expensive (graph propagation)
)


DDL_ALERTS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_ALERTS} (
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
) USING iceberg PARTITIONED BY (days(alert_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
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
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
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
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
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
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""
# LB-119: durable, data-plane record of what each detection rule did this
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
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""


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

    for name, ddl in (
        ("alerts", DDL_ALERTS),
        ("risk_scores", DDL_RISK),
        ("entity_clusters", DDL_CLUSTERS),
        ("daily_dashboards", DDL_DASH),
        ("detection_status", DDL_STATUS),
    ):
        spark.sql(ddl)
        log(f"Bootstrapped gold.{name}")

    # LB-125 upgrade guard: on a REUSED catalog whose gold.alerts predates
    # detected_ts, `CREATE TABLE IF NOT EXISTS` is a no-op, and the detection
    # loop's positional `INSERT INTO gold.alerts SELECT *` (now 18 columns)
    # would fail against a 17-column table. Check the live schema and add the
    # column only when it is genuinely missing -- Spark/Iceberg has no
    # `ADD COLUMN IF NOT EXISTS` for columns (that clause is for PARTITION), so
    # a blind ALTER would ParseException, and the plain `ADD COLUMNS` would
    # error if the column already exists. On a fresh table (created 18-col by
    # the DDL above) this reads the column present and does nothing.
    try:
        _alert_cols = [f.name for f in spark.table(f"{CATALOG}.{GOLD_ALERTS}").schema.fields]
        if "detected_ts" not in _alert_cols:
            spark.sql(f"ALTER TABLE {CATALOG}.{GOLD_ALERTS} ADD COLUMNS (detected_ts TIMESTAMP)")
            log(f"[startup] added detected_ts to {GOLD_ALERTS} (reused-catalog upgrade)")
    except Exception as e:  # noqa: BLE001
        log(f"[startup] detected_ts upgrade check on {GOLD_ALERTS} skipped: {e}")

    txns = spark.table(f"{CATALOG}.{SILVER_TXNS}")
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

    run_detection_rules(spark, txns, RUN_ID)

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


def run_detection_rules(spark, txns, run_id: str, rules=None, skipped_rules=None) -> None:
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

    Note that two concurrent ``lakebench run`` invocations against the
    SAME catalog will race on ``DELETE WHERE rule_id = ...``; the
    intended usage model is one live pipeline per catalog / namespace
    at a time (per shared-cluster ownership discipline in
    docs/architecture-shared-cluster.md). Cross-namespace concurrent
    runs are safe because each has its own Iceberg gold.alerts table.

    Metrics: the per-rule ``alerts=N`` line is picked up by the
    driver-log parser in metrics/collector.py. On a rule failure we
    still emit ``alerts=0 error=...`` (instead of a bare FAILED line)
    so the parser sees a row for every rule that was attempted, not
    just the ones that succeeded.
    """
    import inspect

    from detection_rules import RULE_TARGET_TYPOLOGY, RuleSkipped, get_rule

    # Which rules to run this invocation. Batch passes None (the full default
    # set); the continuous gold loop passes a bounded set (W2/W3/W4) plus a
    # skipped_rules list for the rules it deliberately does NOT run there --
    # W1 (per-tick graph recompute too costly), W7 (silver.entities is not
    # maintained by silver_stream, so it would false-report 0% recall), and
    # W8 (needs a 90-day dormancy gap a narrow continuous corpus cannot hold).
    # Marking them 'skipped' (not simply omitting them) is what lets
    # score_financial render their typologies "not run" instead of a false 0%.
    rules = tuple(rules) if rules is not None else DEFAULT_DETECTION_RULES
    skipped_rules = tuple(skipped_rules or ())

    # Load silver.entities once. W7 needs it; loading up-front is cheap
    # (Iceberg metadata read) and avoids each rule invocation paying its
    # own catalog resolution cost.
    try:
        silver_entities = spark.table(f"{CATALOG}.{SILVER_ENTITIES}")
    except Exception as e:  # noqa: BLE001 -- broad catch on catalog errors
        log(f"[detection] silver.entities not readable ({e}); W7 will skip.")
        silver_entities = None

    log("=" * 60)
    log(f"Running detection rules: {list(rules)}")
    log("=" * 60)

    total_alerts = 0
    # Per-rule status accumulated for the durable gold.detection_status table
    # (LB-119). Each entry: (rule_id, status, reason, target_typology,
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
    for rule_id in rules:
        target_typology = RULE_TARGET_TYPOLOGY.get(rule_id)
        fn = get_rule(rule_id)
        if fn is None:
            log(f"[detection] {rule_id}: alerts=0 error=unknown-rule elapsed=0.0s")
            status_rows.append((rule_id, "error", "unknown-rule", target_typology, None))
            continue
        rule_start = time.time()
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
            params = {"run_id": run_id}
            if "silver_entities" in sig.parameters:
                params["silver_entities"] = silver_entities
            # Thread the configured W1 vertex cap through to any rule that
            # accepts it (W1 today). A non-positive override means "leave
            # the rule default in place" so a mis-set env var cannot
            # silently disable the rule by capping it at zero.
            if "max_vertices" in sig.parameters and _W1_MAX_VERTICES > 0:
                params["max_vertices"] = _W1_MAX_VERTICES
            alerts = fn(txns, **params)
            alert_count = alerts.count()
            # Snapshot prior alert count for this rule_id so that a
            # partial-write incident (DELETE commits, INSERT throws)
            # is visible in the driver log. Round-2 finding: without
            # this, an executor OOM or S3 5xx during INSERT silently
            # drops the prior committed alerts for this rule; the
            # next full run rewrites them, but this cycle's metrics
            # show a false zero for the affected rule with no signal.
            try:
                prior_row = spark.sql(
                    f"SELECT COUNT(*) AS c FROM {CATALOG}.{GOLD_ALERTS} WHERE rule_id = '{rule_id}'"
                ).collect()
                prior_count = int(prior_row[0]["c"]) if prior_row else 0
            except Exception:  # noqa: BLE001 -- diagnostic only
                prior_count = -1
            # Materialise the alerts frame so the write half does not
            # re-execute the rule on retry. Also lets us fail with a
            # clear signal if the count/write disagrees.
            tmp_view = f"_lb_alerts_{rule_id}"
            alerts.createOrReplaceTempView(tmp_view)
            spark.sql(f"DELETE FROM {CATALOG}.{GOLD_ALERTS} WHERE rule_id = '{rule_id}'")
            if alert_count > 0:
                spark.sql(f"INSERT INTO {CATALOG}.{GOLD_ALERTS} SELECT * FROM {tmp_view}")
            spark.catalog.dropTempView(tmp_view)
            elapsed = time.time() - rule_start
            log(
                f"[detection] {rule_id}: alerts={alert_count} "
                f"prior={prior_count} elapsed={elapsed:.1f}s"
            )
            status_rows.append((rule_id, "ran", None, target_typology, int(alert_count)))
            total_alerts += alert_count
        except RuleSkipped as skip:
            # A structural skip is NOT a zero and NOT an error. Emit a
            # third log shape the collector records distinctly so the
            # scorecard renders "not run" rather than 0% recall. Prior
            # rows for this rule_id are left untouched (we never reached
            # the DELETE), so a later run at a higher cap can still write
            # them without a stale-delete gap.
            elapsed = time.time() - rule_start
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
            # ones. ``error=`` carries the class/message. If the
            # exception fired between DELETE and INSERT, gold.alerts
            # is now missing this rule's prior rows -- the operator
            # sees this as a delta from prior_count on the next
            # successful run.
            err = one_line(f"{type(e).__name__}: {e}")
            log(f"[detection] {rule_id}: alerts=0 error={err} elapsed={elapsed:.1f}s")
            status_rows.append((rule_id, "error", err, target_typology, None))
    log(f"[detection] total alerts written: {total_alerts}")

    for rule_id in skipped_rules:
        target_typology = RULE_TARGET_TYPOLOGY.get(rule_id)
        log(f"[detection] {rule_id}: skipped=mode-excluded elapsed=0.0s")
        status_rows.append((rule_id, "skipped", "mode-excluded", target_typology, None))

    _write_detection_status(spark, status_rows, run_id)

    # P0.5: project the two derived gold tables from the alerts the rules
    # just wrote. No new detection compute -- entity_clusters is a
    # reshape of W1 alerts, risk_scores a reshape of W4 alerts. Best-effort
    # and isolated: a failure here does not fail the pipeline, and an empty
    # source (e.g. W1 skipped at high scale) writes an empty derived table,
    # which is the correct "nothing to project" state, not a bug.
    _project_derived_gold(spark, run_id)


def _write_detection_status(spark, status_rows: list, run_id: str) -> None:
    """Persist per-rule detection status to gold.detection_status (LB-119).

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
    W1 did not run -- a direct contradiction on a reused catalog (LB-119
    review F1). Filtering by run_id makes a skipped rule contribute zero
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
    # regardless of the Spark storeAssignmentPolicy (LB-119 review F2):
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
        log(f"[derived] wrote {GOLD_CLUSTERS} from W1 alerts ({clusters.count()} rows)")
    except Exception as e:  # noqa: BLE001
        log(f"[derived] entity_clusters projection failed: {type(e).__name__}: {e}")

    # gold.risk_scores from W4 risk-propagation alerts. ``alert_score IS
    # NOT NULL`` filter guarantees the NOT NULL risk_score target receives
    # non-null input (same storeAssignmentPolicy rationale as clusters).
    try:
        risk = (
            alerts.filter(col("rule_id") == lit("W4_risk_propagation"))
            .filter(col("alert_score").isNotNull())
            .select(
                col("entity_id"),
                col("model_id"),
                col("model_version"),
                col("alert_score").alias("risk_score"),
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
        log(f"[derived] wrote {GOLD_RISK} from W4 alerts ({risk.count()} rows)")
    except Exception as e:  # noqa: BLE001
        log(f"[derived] risk_scores projection failed: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
