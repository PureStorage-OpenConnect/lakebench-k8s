"""Benchmark query definitions for Lakebench.

8 performance queries across five categories that mirror real analytical
usage patterns against the Customer 360 medallion pipeline.

Queries use placeholders that are templated at runtime:

- ``{catalog}`` -- Trino catalog name (from config)
- ``{silver_table}`` -- Fully-qualified silver table (namespace.table)
- ``{gold_table}`` -- Fully-qualified gold table (namespace.table)

Categories:
  - scan (Q1): Full table scan with aggregation. I/O throughput bound.
  - filter_prune (Q2, Q4): Date-range and predicate filtering with GROUP BY.
  - aggregation (Q3, Q7): Hash aggregation and conditional SUM(CASE).
  - analytics (Q5, Q6): Window functions and CTE with multi-branch CASE.
  - operational (Q9): Gold layer executive dashboard read.
"""

from __future__ import annotations

from dataclasses import dataclass

from lakebench.config.schema import WorkloadSchema


@dataclass(frozen=True)
class BenchmarkQuery:
    """A single benchmark query definition."""

    name: str
    display_name: str
    query_class: str  # "scan", "analytics", "gold"
    sql: str


# ---------------------------------------------------------------------------
# Class A: Silver Table Scans (large table, I/O-bound)
# ---------------------------------------------------------------------------

_Q1 = BenchmarkQuery(
    name="Q1_full_aggregation_scan",
    display_name="Full aggregation scan",
    query_class="scan",
    sql="""\
SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT session_id) AS unique_sessions,
  ROUND(SUM(transaction_amount), 2) AS total_revenue,
  ROUND(AVG(transaction_amount), 2) AS avg_transaction
FROM {catalog}.{silver_table}""",
)

_Q2 = BenchmarkQuery(
    name="Q2_filtered_aggregation",
    display_name="Filtered aggregation",
    query_class="filter_prune",
    sql="""\
SELECT
  interaction_date,
  interaction_type,
  COUNT(*) AS interactions,
  COUNT(DISTINCT customer_id) AS customers,
  ROUND(SUM(transaction_amount), 2) AS revenue
FROM {catalog}.{silver_table}
WHERE interaction_date >= (SELECT MIN(interaction_date) FROM {catalog}.{silver_table})
  AND interaction_date < date_add('month', 3,
    (SELECT MIN(interaction_date) FROM {catalog}.{silver_table}))
GROUP BY interaction_date, interaction_type
ORDER BY interaction_date, revenue DESC""",
)

_Q3 = BenchmarkQuery(
    name="Q3_customer_segmentation",
    display_name="Customer segmentation",
    query_class="aggregation",
    sql="""\
SELECT
  customer_value_tier,
  channel_preference,
  COUNT(DISTINCT customer_id) AS customers,
  ROUND(SUM(transaction_amount), 2) AS total_spend,
  ROUND(AVG(engagement_score), 2) AS avg_engagement,
  ROUND(AVG(lifetime_value_estimate), 2) AS avg_ltv
FROM {catalog}.{silver_table}
WHERE transaction_amount > 0
GROUP BY customer_value_tier, channel_preference
ORDER BY total_spend DESC""",
)

_Q4 = BenchmarkQuery(
    name="Q4_churn_risk_analysis",
    display_name="Churn risk analysis",
    query_class="filter_prune",
    sql="""\
SELECT
  churn_risk_indicator,
  customer_journey_stage,
  device_category,
  COUNT(*) AS interactions,
  COUNT(DISTINCT customer_id) AS at_risk_customers,
  ROUND(AVG(engagement_score), 2) AS avg_engagement,
  ROUND(AVG(satisfaction_score), 2) AS avg_satisfaction
FROM {catalog}.{silver_table}
WHERE churn_risk_indicator IN ('high_risk', 'medium_risk')
GROUP BY churn_risk_indicator, customer_journey_stage, device_category
HAVING COUNT(DISTINCT customer_id) > 10
ORDER BY at_risk_customers DESC""",
)

# ---------------------------------------------------------------------------
# Class B: Complex Analytics (joins, CTEs, window functions)
# ---------------------------------------------------------------------------

_Q5 = BenchmarkQuery(
    name="Q5_revenue_trend_ma7",
    display_name="Revenue trend MA7",
    query_class="analytics",
    sql="""\
WITH daily AS (
  SELECT
    interaction_date,
    COUNT(DISTINCT customer_id) AS dau,
    ROUND(SUM(transaction_amount), 2) AS revenue,
    COUNT(*) AS total_interactions
  FROM {catalog}.{silver_table}
  GROUP BY interaction_date
)
SELECT
  interaction_date,
  dau,
  revenue,
  total_interactions,
  ROUND(AVG(revenue) OVER (
    ORDER BY interaction_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS revenue_ma7,
  ROUND(AVG(dau) OVER (
    ORDER BY interaction_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 0) AS dau_ma7
FROM daily
ORDER BY interaction_date DESC
LIMIT 90""",
)

_Q6 = BenchmarkQuery(
    name="Q6_customer_rfm",
    display_name="Customer RFM scoring",
    query_class="analytics",
    sql="""\
WITH customer_rfm AS (
  SELECT
    customer_id,
    DATE_DIFF('day', MAX(interaction_date), CURRENT_DATE) AS recency_days,
    COUNT(DISTINCT interaction_date) AS frequency,
    ROUND(SUM(transaction_amount), 2) AS monetary
  FROM {catalog}.{silver_table}
  GROUP BY customer_id
)
SELECT
  CASE
    WHEN recency_days <= 30 AND frequency >= 20 AND monetary > 1000 THEN 'Champions'
    WHEN recency_days <= 60 AND frequency >= 10 AND monetary > 500 THEN 'Loyal'
    WHEN recency_days <= 30 AND monetary > 200 THEN 'Potential Loyalists'
    WHEN recency_days <= 90 AND frequency >= 5 THEN 'At Risk'
    WHEN recency_days > 180 THEN 'Lost'
    ELSE 'Casual'
  END AS rfm_segment,
  COUNT(*) AS customer_count,
  ROUND(AVG(monetary), 2) AS avg_spend,
  ROUND(AVG(frequency), 1) AS avg_frequency,
  ROUND(AVG(recency_days), 0) AS avg_recency
FROM customer_rfm
GROUP BY 1
ORDER BY avg_spend DESC""",
)

_Q7 = BenchmarkQuery(
    name="Q7_channel_conversion_funnel",
    display_name="Channel conversion funnel",
    query_class="aggregation",
    sql="""\
SELECT
  channel,
  COUNT(*) AS total_interactions,
  SUM(CASE WHEN customer_journey_stage = 'awareness' THEN 1 ELSE 0 END) AS awareness,
  SUM(CASE WHEN customer_journey_stage = 'consideration' THEN 1 ELSE 0 END) AS consideration,
  SUM(CASE WHEN customer_journey_stage = 'conversion' THEN 1 ELSE 0 END) AS conversions,
  SUM(CASE WHEN customer_journey_stage = 'retention' THEN 1 ELSE 0 END) AS retention,
  ROUND(
    CAST(SUM(CASE WHEN customer_journey_stage = 'conversion' THEN 1 ELSE 0 END) AS DOUBLE)
    / NULLIF(SUM(CASE WHEN customer_journey_stage = 'awareness' THEN 1 ELSE 0 END), 0) * 100,
    2
  ) AS conversion_rate_pct,
  ROUND(SUM(transaction_amount), 2) AS channel_revenue,
  ROUND(AVG(engagement_score), 2) AS avg_engagement
FROM {catalog}.{silver_table}
GROUP BY channel
ORDER BY channel_revenue DESC""",
)

# ---------------------------------------------------------------------------
# Class E: Operational (Gold Table)
# ---------------------------------------------------------------------------

_Q9 = BenchmarkQuery(
    name="Q9_executive_dashboard",
    display_name="Executive dashboard",
    query_class="operational",
    sql="""\
SELECT
  interaction_date,
  daily_active_customers,
  total_daily_revenue,
  conversions,
  avg_engagement_score,
  high_churn_risk_count,
  ROUND(total_daily_revenue - LAG(total_daily_revenue) OVER (ORDER BY interaction_date), 2) AS revenue_change,
  ROUND(
    (CAST(daily_active_customers AS DOUBLE) - LAG(daily_active_customers) OVER (ORDER BY interaction_date))
    / NULLIF(LAG(daily_active_customers) OVER (ORDER BY interaction_date), 0) * 100,
    1
  ) AS dau_change_pct
FROM {catalog}.{gold_table}
ORDER BY interaction_date DESC
LIMIT 30""",
)

# ---------------------------------------------------------------------------
# Public query lists (per workload schema)
# ---------------------------------------------------------------------------

_CUSTOMER360_QUERIES: list[BenchmarkQuery] = [
    _Q1,  # scan
    _Q2,
    _Q4,  # filter_prune
    _Q3,
    _Q7,  # aggregation
    _Q5,
    _Q6,  # analytics
    _Q9,  # operational
]


# Financial (FinServ-Crime, AML) benchmark queries land with the ENG-2C.4.6-7
# investigator-workload authoring. Empty list until then; callers should
# treat an empty list as "no benchmark configured for this schema" and skip
# the benchmark rather than fail.
_FINANCIAL_QUERIES: list[BenchmarkQuery] = []


BENCHMARK_QUERIES_BY_DOMAIN: dict[WorkloadSchema, list[BenchmarkQuery]] = {
    WorkloadSchema.CUSTOMER360: _CUSTOMER360_QUERIES,
    WorkloadSchema.FINANCIAL: _FINANCIAL_QUERIES,
    WorkloadSchema.CUSTOM: _CUSTOMER360_QUERIES,
}


def get_benchmark_queries(schema: WorkloadSchema) -> list[BenchmarkQuery]:
    """Return the benchmark query set for a workload schema.

    Unknown schemas fall back to the Customer 360 set to preserve prior
    behavior for CUSTOM and any other value pending its own query set.
    """
    return BENCHMARK_QUERIES_BY_DOMAIN.get(schema, _CUSTOMER360_QUERIES)


# Backward-compatible alias. New code should call
# ``get_benchmark_queries(schema)`` or read ``BENCHMARK_QUERIES_BY_DOMAIN``
# directly so the benchmark set travels with the workload schema.
BENCHMARK_QUERIES: list[BenchmarkQuery] = _CUSTOMER360_QUERIES
