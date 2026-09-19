"""Gold Finalize (Financial) -- initial gold table bootstrap + baseline rollups.

At initial deployment there are no alerts, no risk scores, and no entity
clusters (detection workloads have not run yet). This script:

1. Bootstraps the four Financial gold tables (create-if-not-exists), matching
   src/lakebench/deploy/financial_ddl.py.
2. Emits an initial daily_dashboards baseline row per (day, "baseline") derived
   from silver.transactions -- row count and total volume -- so downstream
   consumers have a starting shape to plot against.
3. Leaves alerts / risk_scores / entity_clusters empty; workload W-N scripts
   append to these tables as they run.
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
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
GOLD_RISK = env("LB_FINANCIAL_GOLD_RISK_SCORES", "gold.risk_scores")
GOLD_CLUSTERS = env("LB_FINANCIAL_GOLD_CLUSTERS", "gold.entity_clusters")
GOLD_DASH = env("LB_FINANCIAL_GOLD_DASHBOARDS", "gold.daily_dashboards")
RUN_ID = env("LB_RUN_ID", str(uuid.uuid4()))


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
            (sum_("txn_amount_usd") / lit(2)).cast("decimal(20,2)").alias("total_alerted_amount_usd"),
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
    # `.overwrite(lit(True))` replaces data rows while preserving the table's
    # partition spec and schema, unlike `.createOrReplace()` which reverts
    # partitioning on every write (see silver_build_financial for the same
    # fix). More importantly for the refresh loop path: a partial-data
    # refresh via createOrReplace would wipe rows written by concurrent
    # detection workloads. `.overwrite(lit(True))` still wipes them, so
    # gold_refresh must ONLY overwrite its own baseline rows -- see
    # gold_refresh_financial.py for the partition-predicate refresh path.
    baseline.writeTo(f"{CATALOG}.{GOLD_DASH}").overwrite(lit(True))
    log(f"Wrote {GOLD_DASH} baseline rows")

    log("=" * 60)
    log(f"Gold finalize complete in {time.time() - start:.1f}s")
    log("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
