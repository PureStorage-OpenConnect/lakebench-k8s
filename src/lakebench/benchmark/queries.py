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
WITH data_clock AS (
  SELECT MAX(interaction_date) AS as_of FROM {catalog}.{silver_table}
),
customer_rfm AS (
  SELECT
    s.customer_id,
    DATE_DIFF('day', MAX(s.interaction_date), MAX(c.as_of)) AS recency_days,
    COUNT(DISTINCT s.interaction_date) AS frequency,
    ROUND(SUM(s.transaction_amount), 2) AS monetary
  FROM {catalog}.{silver_table} s
  CROSS JOIN data_clock c
  GROUP BY s.customer_id
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


# Financial (FinServ-Crime, AML) benchmark queries.
#
# Eight queries covering the analyst workloads a fraud/AML investigator
# actually runs against a completed medallion pipeline:
#
# - FQ1: silver.transactions full scan aggregation (I/O)
# - FQ2: top corridors by volume in a rolling window (filter + agg)
# - FQ3: entity risk propagation via edges (join + agg)
# - FQ4: rolling running-balance window over account_statements (analytic)
# - FQ5: gold.alerts triage by priority + rule (operational)
# - FQ6: structuring-band transaction detection (filter, mirrors W2 rule)
# - FQ7: cross-border corridor concentration (Trino/Iceberg hint test)
# - FQ8: alert-to-entity join for case investigation (small-N join)
#
# Uses table placeholders {catalog}.{silver_table}, {silver_entities},
# {silver_counterparty_edges}, {silver_account_statements}, {gold_alerts},
# {gold_daily_dashboards} filled by benchmark/runner.py.

_FQ1 = BenchmarkQuery(
    name="FQ1_txn_full_scan",
    display_name="Silver transactions full aggregation",
    query_class="scan",
    sql="""\
SELECT
  COUNT(*) AS total_txns,
  COUNT(DISTINCT originator_id) AS unique_originators,
  COUNT(DISTINCT beneficiary_id) AS unique_beneficiaries,
  ROUND(SUM(txn_amount_usd), 2) AS total_volume_usd,
  ROUND(AVG(txn_amount_usd), 2) AS avg_txn_usd
FROM {catalog}.{silver_table}""",
)

_FQ2 = BenchmarkQuery(
    name="FQ2_top_corridors_window",
    display_name="Top payment corridors by volume (last 30 days)",
    query_class="filter_prune",
    # `date_add('day', -30, ts)` is Trino syntax; Spark Thrift and DuckDB
    # reject the 3-arg form. Use the ANSI INTERVAL literal which both
    # engines parse consistently. Trino: `ts - INTERVAL '30' DAY`, Spark:
    # `ts - INTERVAL 30 DAYS`, DuckDB: `ts - INTERVAL 30 DAY`. All three
    # of those parse `ts - INTERVAL '30' DAY` correctly.
    sql="""\
SELECT
  originator_bank_bic,
  beneficiary_bank_bic,
  txn_currency,
  COUNT(*) AS txn_count,
  ROUND(SUM(txn_amount_usd), 2) AS volume_usd
FROM {catalog}.{silver_table}
WHERE txn_timestamp >= (SELECT MAX(txn_timestamp) FROM {catalog}.{silver_table}) - INTERVAL '30' DAY
GROUP BY originator_bank_bic, beneficiary_bank_bic, txn_currency
ORDER BY volume_usd DESC
LIMIT 100""",
)

_FQ3 = BenchmarkQuery(
    name="FQ3_entity_edge_risk",
    display_name="Entity out-degree + volume via edges",
    query_class="aggregation",
    sql="""\
SELECT
  e.name,
  e.entity_type,
  COUNT(DISTINCT ce.target_entity_id) AS distinct_beneficiaries,
  SUM(ce.txn_count) AS total_txns,
  ROUND(SUM(ce.cumulative_amount_usd), 2) AS total_out_usd
FROM {catalog}.{silver_counterparty_edges} ce
JOIN {catalog}.{silver_entities} e ON ce.source_entity_id = e.entity_id
GROUP BY e.entity_id, e.name, e.entity_type
ORDER BY total_out_usd DESC
LIMIT 200""",
)

_FQ4 = BenchmarkQuery(
    name="FQ4_running_balance_window",
    display_name="Running balance for high-activity accounts",
    query_class="analytics",
    sql="""\
WITH top_accts AS (
  SELECT account_id
  FROM {catalog}.{silver_account_statements}
  GROUP BY account_id
  ORDER BY COUNT(*) DESC
  LIMIT 50
)
SELECT
  s.account_id,
  s.book_ts,
  s.cdt_dbt_ind,
  s.amt,
  s.bal_after,
  ROW_NUMBER() OVER (PARTITION BY s.account_id ORDER BY s.book_ts) AS entry_ord
FROM {catalog}.{silver_account_statements} s
JOIN top_accts t ON t.account_id = s.account_id
ORDER BY s.account_id, entry_ord""",
)

_FQ5 = BenchmarkQuery(
    name="FQ5_alert_triage",
    display_name="Alert triage by priority + rule",
    query_class="operational",
    sql="""\
SELECT
  rule_id,
  priority,
  status,
  COUNT(*) AS alerts,
  COUNT(DISTINCT entity_id) AS entities,
  ROUND(AVG(alert_score), 3) AS avg_score
FROM {catalog}.{gold_alerts}
GROUP BY rule_id, priority, status
ORDER BY alerts DESC""",
)

_FQ6 = BenchmarkQuery(
    name="FQ6_structuring_scan",
    display_name="Structuring-band transaction detection (W2 shape)",
    query_class="filter_prune",
    # Currency bands must match detection_rules._STRUCTURING_THRESHOLDS
    # and datagen_rs::amounts::structuring_band. A missing currency
    # under-fires vs. the W2 rule this query benchmarks against.
    sql="""\
SELECT
  originator_id,
  txn_currency,
  COUNT(*) AS txn_count,
  MIN(txn_timestamp) AS first_ts,
  MAX(txn_timestamp) AS last_ts,
  ROUND(SUM(txn_amount), 2) AS total_amount
FROM {catalog}.{silver_table}
WHERE (
       (txn_currency IN ('USD', 'CAD', 'AUD') AND txn_amount BETWEEN 9000 AND 9999)
    OR (txn_currency IN ('GBP', 'EUR', 'CHF') AND txn_amount BETWEEN 14000 AND 14995)
    OR (txn_currency IN ('JPY', 'INR')       AND txn_amount BETWEEN 900000 AND 999999)
    OR (txn_currency = 'AED'                 AND txn_amount BETWEEN 49500 AND 54999)
    OR (txn_currency = 'SGD'                 AND txn_amount BETWEEN 18000 AND 19999)
    OR (txn_currency = 'MXN'                 AND txn_amount BETWEEN 90000 AND 99999)
    OR (txn_currency IN ('CNY', 'BRL')       AND txn_amount BETWEEN 45000 AND 49999)
    OR (txn_currency = 'HKD'                 AND txn_amount BETWEEN 67500 AND 74999)
    OR (txn_currency = 'KRW'                 AND txn_amount BETWEEN 9000000 AND 9999999)
  )
GROUP BY originator_id, txn_currency
HAVING COUNT(*) >= 3
ORDER BY txn_count DESC
LIMIT 500""",
)

_FQ7 = BenchmarkQuery(
    name="FQ7_cross_border_concentration",
    display_name="Cross-border corridor concentration",
    query_class="aggregation",
    sql="""\
SELECT
  originator_bank_bic,
  beneficiary_bank_bic,
  ROUND(SUM(CASE WHEN cross_border THEN txn_amount_usd ELSE 0 END), 2) AS xborder_usd,
  ROUND(SUM(txn_amount_usd), 2) AS total_usd,
  ROUND(
    SUM(CASE WHEN cross_border THEN txn_amount_usd ELSE 0 END) * 1.0 / NULLIF(SUM(txn_amount_usd), 0),
    3
  ) AS xborder_share
FROM {catalog}.{silver_table}
GROUP BY originator_bank_bic, beneficiary_bank_bic
HAVING SUM(txn_amount_usd) > 0
ORDER BY xborder_usd DESC
LIMIT 100""",
)

_FQ8 = BenchmarkQuery(
    name="FQ8_alert_to_entity_join",
    display_name="Case investigation: alert -> entity -> recent txns",
    query_class="operational",
    sql="""\
WITH recent_alerts AS (
  SELECT alert_id, entity_id, alert_ts, related_txn_ids
  FROM {catalog}.{gold_alerts}
  ORDER BY alert_ts DESC
  LIMIT 100
)
SELECT
  a.alert_id,
  a.alert_ts,
  e.name AS entity_name,
  e.entity_type,
  cardinality(a.related_txn_ids) AS txns_in_alert
FROM recent_alerts a
LEFT JOIN {catalog}.{silver_entities} e ON a.entity_id = e.entity_id
ORDER BY a.alert_ts DESC""",
)


_FINANCIAL_QUERIES: list[BenchmarkQuery] = [
    _FQ1,  # scan
    _FQ2,  # filter_prune
    _FQ6,  # filter_prune
    _FQ3,  # aggregation
    _FQ7,  # aggregation
    _FQ4,  # analytics
    _FQ5,  # operational
    _FQ8,  # operational
]


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
