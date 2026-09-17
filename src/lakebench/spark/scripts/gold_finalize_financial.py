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
    col,
    countDistinct,
    current_timestamp,
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


def build_baseline_dashboards(txns):
    """Baseline: one row per settlement day, keyed rule_id='baseline'."""
    return (
        txns.groupBy(to_date(col("txn_timestamp")).alias("dashboard_date"))
        .agg(
            count_(lit(1)).alias("alert_count"),
            countDistinct(col("originator_id")).alias("entity_count"),
            sum_(col("txn_amount_usd")).cast("decimal(20,2)").alias("total_alerted_amount_usd"),
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
            lit(RUN_ID).alias("run_id"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.appName("lb-gold-finalize-financial").getOrCreate()
    start = time.time()

    log("=" * 60)
    log("Gold Finalize (Financial)")
    log(f"Run ID: {RUN_ID}")
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
    baseline = build_baseline_dashboards(txns)
    baseline.writeTo(f"{CATALOG}.{GOLD_DASH}").createOrReplace()
    log(f"Wrote {GOLD_DASH} baseline rows")

    log("=" * 60)
    log(f"Gold finalize complete in {time.time() - start:.1f}s")
    log("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
