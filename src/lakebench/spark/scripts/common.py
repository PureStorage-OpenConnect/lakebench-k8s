"""Common utilities for Spark pipeline scripts.

Bridge module: provides lakebench helper functions (log, env) and
shared transformation logic used by both batch and streaming scripts.

Environment variables set by lakebench job.py:
  BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET, CATALOG_NAME,
  LB_BRONZE_URI, LB_SILVER_URI, LB_ICEBERG_CATALOG
"""

import os
from datetime import datetime


def log(msg):
    """Timestamped log line."""
    print(f"[lb] {datetime.utcnow().isoformat()} - {msg}", flush=True)


def env(name, default=None):
    """Read required env var."""
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v


def parse_size_gb(s):
    """Parse size string to GB."""
    return float(s)


# ============================================================
# Shared transformation logic (used by both batch and streaming)
# ============================================================


def apply_silver_transformations(df_bronze):
    """Apply Silver layer transformations - cleaning, standardization, enrichment.

    Pure column-level transforms: no joins, no shuffles. Each row is
    processed independently. Shared between silver_build.py (batch) and
    silver_stream.py (streaming via foreachBatch).
    """
    from pyspark.sql.functions import (
        col,
        concat_ws,
        current_timestamp,
        expr,
        lower,
        regexp_replace,
        to_date,
        trim,
        upper,
        when,
    )

    return (
        df_bronze
        # Light filtering - only remove truly bad data (~2% removed)
        .filter(col("data_quality_flag") != "duplicate_suspected")
        # === STANDARDIZED/CLEANED VERSIONS (keep originals) ===
        .withColumn(
            "email_clean", regexp_replace(lower(trim(col("email_raw"))), "\\.duplicate", "")
        )
        .withColumn(
            "phone_clean",
            regexp_replace(regexp_replace(col("phone_raw"), "[^0-9]", ""), "^1?(\\d{10})$", "+1$1"),
        )
        # Geographic standardization
        .withColumn(
            "state_standardized",
            when(upper(col("state_raw")).isin("CA", "CALIFORNIA"), "CA")
            .when(upper(col("state_raw")).isin("TX", "TEXAS"), "TX")
            .when(upper(col("state_raw")).isin("NY", "NEW YORK"), "NY")
            .when(upper(col("state_raw")) == "FL", "FL")
            .otherwise(upper(col("state_raw"))),
        )
        .withColumn(
            "city_standardized",
            when(upper(col("city_raw")).isin("NEW YORK", "NYC"), "New York")
            .when(upper(col("city_raw")) == "LA", "Los Angeles")
            .otherwise(col("city_raw")),
        )
        # === DERIVED TIME DIMENSIONS ===
        .withColumn("interaction_date", to_date(col("event_timestamp")))
        .withColumn("interaction_hour", expr("hour(event_timestamp)"))
        .withColumn("interaction_day_of_week", expr("dayofweek(event_timestamp)"))
        .withColumn("interaction_week_of_year", expr("weekofyear(event_timestamp)"))
        .withColumn("interaction_month", expr("month(event_timestamp)"))
        .withColumn("interaction_year", expr("year(event_timestamp)"))
        .withColumn("is_weekend", expr("dayofweek(event_timestamp) in (1, 7)"))
        .withColumn("is_business_hours", expr("hour(event_timestamp) between 9 and 17"))
        .withColumn(
            "is_peak_hours",
            expr("""
                hour(event_timestamp) between 12 and 14 or
                hour(event_timestamp) between 18 and 20
            """),
        )
        # === CUSTOMER VALUE SEGMENTATION ===
        .withColumn(
            "customer_value_tier",
            when(col("transaction_amount") > 500, "high_value")
            .when(col("transaction_amount") > 100, "medium_value")
            .when(col("transaction_amount") > 0, "low_value")
            .otherwise("browser_only"),
        )
        .withColumn(
            "transaction_size_category",
            when(col("transaction_amount") > 1000, "large")
            .when(col("transaction_amount") > 250, "medium")
            .when(col("transaction_amount") > 0, "small")
            .otherwise("none"),
        )
        # === BEHAVIORAL ANALYTICS ===
        .withColumn(
            "engagement_score",
            expr("""
                case when page_views = 0 then 0
                     when page_views <= 2 then 1
                     when page_views <= 5 then 2
                     when page_views <= 10 then 3
                     else 4 end
            """),
        )
        .withColumn(
            "session_depth_category",
            when(col("page_views") > 10, "deep")
            .when(col("page_views") > 3, "medium")
            .when(col("page_views") > 0, "shallow")
            .otherwise("bounce"),
        )
        .withColumn(
            "time_spent_category",
            when(col("time_on_site_seconds") > 1800, "long")
            .when(col("time_on_site_seconds") > 300, "medium")
            .when(col("time_on_site_seconds") > 0, "short")
            .otherwise("none"),
        )
        .withColumn(
            "channel_preference",
            when(col("channel") == "mobile_app", "mobile_first")
            .when(col("channel") == "web", "web_first")
            .when(col("channel") == "store", "physical_first")
            .otherwise("omnichannel"),
        )
        # === ADVANCED ANALYTICS (ML Features) ===
        .withColumn(
            "lifetime_value_estimate",
            expr("round(transaction_amount * (1 + points_earned/1000.0), 2)"),
        )
        .withColumn(
            "customer_recency_score",
            expr("30 - datediff(current_date(), to_date(event_timestamp))"),
        )
        .withColumn(
            "engagement_velocity",
            expr("round(page_views / greatest(time_on_site_seconds/60.0, 1.0), 4)"),
        )
        .withColumn(
            "churn_risk_indicator",
            when(col("satisfaction_score") <= 2, "high_risk")
            .when(col("satisfaction_score") <= 3, "medium_risk")
            .when(col("satisfaction_score").isNull(), "unknown_risk")
            .otherwise("low_risk"),
        )
        # === MARKETING ATTRIBUTION ===
        .withColumn(
            "attribution_channel",
            when(col("utm_source").isNotNull(), col("utm_source")).otherwise("direct"),
        )
        .withColumn(
            "attribution_quality",
            when(col("utm_source").isNotNull() & col("utm_medium").isNotNull(), "high")
            .when(col("utm_source").isNotNull(), "medium")
            .otherwise("low"),
        )
        .withColumn(
            "customer_journey_stage",
            when(col("interaction_type") == "browse", "awareness")
            .when(col("interaction_type") == "abandoned_cart", "consideration")
            .when(col("interaction_type") == "purchase", "conversion")
            .when(col("interaction_type") == "support", "retention")
            .otherwise("other"),
        )
        # === DEVICE CONTEXT ===
        .withColumn(
            "device_category",
            when(col("device_type") == "mobile", "mobile")
            .when(col("device_type") == "tablet", "tablet")
            .otherwise("desktop"),
        )
        .withColumn(
            "browser_family",
            when(col("browser").isin("chrome", "edge"), "chromium")
            .when(col("browser") == "safari", "webkit")
            .when(col("browser") == "firefox", "gecko")
            .otherwise("other"),
        )
        # === COMPOSITE FEATURES ===
        .withColumn(
            "interaction_context",
            concat_ws("|", col("device_type"), col("browser"), col("channel")),
        )
        .withColumn(
            "customer_segment_key",
            concat_ws(
                ":", col("customer_value_tier"), col("channel_preference"), col("loyalty_tier")
            ),
        )
        # === DATA LINEAGE ===
        .withColumn("silver_processing_timestamp", current_timestamp())
        .withColumn(
            "data_quality_score",
            when(col("data_quality_flag") == "clean", 1.0)
            .when(col("data_quality_flag") == "format_inconsistent", 0.8)
            .when(col("data_quality_flag") == "incomplete_data", 0.6)
            .otherwise(0.5),
        )
    )


def get_daily_kpi_aggregations():
    """Return the list of aggregation expressions for daily KPIs.

    Shared between gold_finalize.py (batch) and gold_refresh.py
    (streaming via foreachBatch). Produces 46 KPI columns.
    """
    from pyspark.sql.functions import (
        avg,
        col,
        countDistinct,
        when,
    )
    from pyspark.sql.functions import (
        max as max_,
    )
    from pyspark.sql.functions import (
        round as round_,
    )
    from pyspark.sql.functions import (
        sum as sum_,
    )

    return [
        # Customer metrics
        countDistinct("customer_id").alias("daily_active_customers"),
        countDistinct("email_clean").alias("unique_emails"),
        countDistinct("session_id").alias("total_sessions"),
        # Revenue metrics
        round_(sum_("transaction_amount"), 2).alias("total_daily_revenue"),
        round_(avg("transaction_amount"), 2).alias("avg_transaction_value"),
        round_(max_("transaction_amount"), 2).alias("largest_transaction"),
        sum_(when(col("transaction_amount") > 0, 1).otherwise(0)).alias("total_transactions"),
        # Channel revenue breakdown
        round_(
            sum_(when(col("channel") == "web", col("transaction_amount")).otherwise(0)), 2
        ).alias("web_revenue"),
        round_(
            sum_(when(col("channel") == "mobile_app", col("transaction_amount")).otherwise(0)), 2
        ).alias("mobile_revenue"),
        round_(
            sum_(when(col("channel") == "store", col("transaction_amount")).otherwise(0)), 2
        ).alias("store_revenue"),
        round_(
            sum_(when(col("channel") == "call_center", col("transaction_amount")).otherwise(0)), 2
        ).alias("call_center_revenue"),
        # Engagement metrics
        round_(avg("engagement_score"), 2).alias("avg_engagement_score"),
        round_(avg("time_on_site_seconds"), 0).alias("avg_time_on_site_seconds"),
        round_(avg("page_views"), 1).alias("avg_page_views"),
        # Conversion funnel
        sum_(when(col("customer_journey_stage") == "awareness", 1).otherwise(0)).alias(
            "awareness_interactions"
        ),
        sum_(when(col("customer_journey_stage") == "consideration", 1).otherwise(0)).alias(
            "consideration_interactions"
        ),
        sum_(when(col("customer_journey_stage") == "conversion", 1).otherwise(0)).alias(
            "conversions"
        ),
        sum_(when(col("customer_journey_stage") == "retention", 1).otherwise(0)).alias(
            "retention_interactions"
        ),
        # Loyalty metrics
        sum_(when(col("loyalty_member") == True, 1).otherwise(0)).alias(  # noqa: E712
            "loyalty_member_interactions"
        ),
        sum_("points_earned").alias("total_points_earned"),
        sum_("points_redeemed").alias("total_points_redeemed"),
        # Customer satisfaction
        countDistinct("support_ticket_id").alias("support_tickets_created"),
        round_(avg("satisfaction_score"), 2).alias("avg_satisfaction_score"),
        # Risk indicators
        sum_(when(col("churn_risk_indicator") == "high_risk", 1).otherwise(0)).alias(
            "high_churn_risk_count"
        ),
        sum_(when(col("churn_risk_indicator") == "medium_risk", 1).otherwise(0)).alias(
            "medium_churn_risk_count"
        ),
        # Value metrics
        round_(sum_("lifetime_value_estimate"), 2).alias("total_estimated_ltv"),
        round_(avg("lifetime_value_estimate"), 2).alias("avg_estimated_ltv"),
        # Channel distribution
        sum_(when(col("channel") == "web", 1).otherwise(0)).alias("web_interactions"),
        sum_(when(col("channel") == "mobile_app", 1).otherwise(0)).alias("mobile_interactions"),
        sum_(when(col("channel") == "store", 1).otherwise(0)).alias("store_interactions"),
    ]
