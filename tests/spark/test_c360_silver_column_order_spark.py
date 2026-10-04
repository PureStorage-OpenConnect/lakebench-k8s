"""The batch C360 silver frame has the column order 1.6.0 wrote.

A Hive Metastore refuses a ``createOrReplace`` that changes column types by
position, so a deployment whose silver table 1.6 wrote could not be rebuilt
by 1.7 while ``_batch_id`` sat mid-row (live upgrade test, 2026-10-04).
"""

from __future__ import annotations

import sys
from datetime import date

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")

# Computed from lakebench 1.6.0's common.apply_silver_transformations_anchored
# followed by its withColumn("_batch_id"), on c360_stream_scenarios.bronze_df.
V160_ORDER = [
    "id", "row_id", "event_timestamp", "event_id", "session_id", "customer_id",
    "email_raw", "phone_raw", "interaction_type", "product_id", "product_category",
    "transaction_amount", "currency", "channel", "device_type", "browser",
    "ip_address", "city_raw", "state_raw", "zip_code", "page_views",
    "time_on_site_seconds", "support_ticket_id", "satisfaction_score", "utm_source",
    "utm_medium", "loyalty_member", "loyalty_tier", "points_earned",
    "points_redeemed", "data_quality_flag", "interaction_payload", "email_clean",
    "phone_clean", "state_standardized", "city_standardized", "interaction_date",
    "interaction_hour", "interaction_day_of_week", "interaction_week_of_year",
    "interaction_month", "interaction_year", "is_weekend", "is_business_hours",
    "is_peak_hours", "customer_value_tier", "transaction_size_category",
    "engagement_score", "session_depth_category", "time_spent_category",
    "channel_preference", "lifetime_value_estimate", "customer_recency_score",
    "engagement_velocity", "churn_risk_indicator", "attribution_channel",
    "attribution_quality", "customer_journey_stage", "device_category",
    "browser_family", "interaction_context", "customer_segment_key",
    "silver_processing_timestamp", "data_quality_score", "_batch_id",
]  # fmt: skip


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_batch_silver_frame_has_the_v160_column_order(spark):
    from c360_stream_scenarios import bronze_df
    from common import apply_silver_transformations_anchored, batch_id_last
    from pyspark.sql.functions import lit

    # As silver_build does: tag bronze, transform, then move _batch_id last.
    tagged = bronze_df(spark, 5).withColumn("_batch_id", lit(0).cast("bigint"))
    out = batch_id_last(apply_silver_transformations_anchored(tagged, date(2026, 9, 1)))
    assert out.columns == V160_ORDER
