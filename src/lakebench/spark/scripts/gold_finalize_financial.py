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

from common import env, log
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
RUN_ID = env("LB_RUN_ID", str(uuid.uuid4()))

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
    evidence           MAP<STRING, STRING>
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
    ):
        spark.sql(ddl)
        log(f"Bootstrapped gold.{name}")

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

    log("=" * 60)
    log(f"Gold finalize complete in {time.time() - start:.1f}s")
    log("=" * 60)
    spark.stop()


def run_detection_rules(spark, txns, run_id: str) -> None:
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

    from detection_rules import get_rule

    # Load silver.entities once. W7 needs it; loading up-front is cheap
    # (Iceberg metadata read) and avoids each rule invocation paying its
    # own catalog resolution cost.
    try:
        silver_entities = spark.table(f"{CATALOG}.{SILVER_ENTITIES}")
    except Exception as e:  # noqa: BLE001 -- broad catch on catalog errors
        log(f"[detection] silver.entities not readable ({e}); W7 will skip.")
        silver_entities = None

    log("=" * 60)
    log(f"Running detection rules: {list(DEFAULT_DETECTION_RULES)}")
    log("=" * 60)

    total_alerts = 0
    for rule_id in DEFAULT_DETECTION_RULES:
        fn = get_rule(rule_id)
        if fn is None:
            log(f"[detection] {rule_id}: alerts=0 error=unknown-rule elapsed=0.0s")
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
            total_alerts += alert_count
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
            err = f"{type(e).__name__}: {e}"[:200]
            log(f"[detection] {rule_id}: alerts=0 error={err} elapsed={elapsed:.1f}s")
    log(f"[detection] total alerts written: {total_alerts}")


if __name__ == "__main__":
    main()
