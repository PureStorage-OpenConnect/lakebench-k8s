# Query Reference

Lakebench ships 8 benchmark queries that run against the Customer 360
medallion pipeline's silver and gold Iceberg tables. This page documents each
query, its purpose, and what it tests.

## Benchmark Queries

The queries execute in the order listed below during a power run. In
throughput mode, each stream shuffles the order independently.

### Q1: Full Aggregation Scan

| Field | Value |
|---|---|
| **ID** | `Q1_full_aggregation_scan` |
| **Category** | scan |
| **Table** | Silver (`customer_interactions_enriched`) |
| **Description** | Full table scan with global aggregation: total records, unique customers, unique sessions, total revenue, average transaction amount. |
| **What it tests** | Raw I/O throughput and scan performance. The query touches every row in the silver table with no predicates. Performance scales linearly with data volume. |
| **Expected rows** | 1 (single summary row) |

### Q2: Filtered Aggregation

| Field | Value |
|---|---|
| **ID** | `Q2_filtered_aggregation` |
| **Category** | filter_prune |
| **Table** | Silver (`customer_interactions_enriched`) |
| **Description** | Filters the silver table to the first 3 months of data using a subquery on `MIN(interaction_date)`, then groups by date and interaction type with revenue ordering. |
| **What it tests** | Predicate pushdown, date-range pruning, and grouped aggregation. The correlated subquery for the date range tests Trino's ability to optimize min-value lookups. |
| **Expected rows** | Proportional to (dates in 3-month window) x (interaction types). Typically tens to low hundreds of rows. |

### Q3: Customer Segmentation

| Field | Value |
|---|---|
| **ID** | `Q3_customer_segmentation` |
| **Category** | aggregation |
| **Table** | Silver (`customer_interactions_enriched`) |
| **Description** | Segments customers by `customer_value_tier` and `channel_preference`. Computes customer count, total spend, average engagement, and average lifetime value per segment. Filters to transactions with `transaction_amount > 0`. |
| **What it tests** | Hash aggregation on two group-by columns with multiple aggregate functions. The `transaction_amount > 0` filter tests predicate evaluation before grouping. |
| **Expected rows** | (number of value tiers) x (number of channel preferences). Typically under 50 rows. |

### Q4: Churn Risk Analysis

| Field | Value |
|---|---|
| **ID** | `Q4_churn_risk_analysis` |
| **Category** | filter_prune |
| **Table** | Silver (`customer_interactions_enriched`) |
| **Description** | Filters to high-risk and medium-risk churn indicators, then groups by churn risk level, journey stage, and device category. Applies `HAVING COUNT(DISTINCT customer_id) > 10` to eliminate sparse segments. |
| **What it tests** | IN-list predicate filtering, three-column GROUP BY, HAVING with COUNT DISTINCT. The HAVING clause tests post-aggregation filtering. |
| **Expected rows** | Variable. Depends on how many (risk, journey, device) combinations have more than 10 distinct customers. Typically tens of rows. |

### Q5: Revenue Trend MA7

| Field | Value |
|---|---|
| **ID** | `Q5_revenue_trend_ma7` |
| **Category** | analytics |
| **Table** | Silver (`customer_interactions_enriched`) |
| **Description** | CTE computes daily DAU, revenue, and interaction counts. Outer query applies 7-day moving average window frames for revenue and DAU. Results ordered by date descending, limited to 90 days. |
| **What it tests** | CTE materialization, window functions with ROWS BETWEEN frame, and ORDER BY with LIMIT. Tests Trino's ability to compute rolling aggregates efficiently. |
| **Expected rows** | Up to 90 (one per day, limited by LIMIT clause) |

### Q6: Customer RFM Scoring

| Field | Value |
|---|---|
| **ID** | `Q6_customer_rfm` |
| **Category** | analytics |
| **Table** | Silver (`customer_interactions_enriched`) |
| **Description** | CTE computes per-customer Recency (days since last interaction), Frequency (distinct interaction dates), and Monetary (total spend). Outer query classifies customers into segments (Champions, Loyal, Potential Loyalists, At Risk, Lost, Casual) using a multi-branch CASE expression, then aggregates per segment. |
| **What it tests** | CTE with per-customer aggregation, `DATE_DIFF` function, multi-branch CASE classification, and re-aggregation of CTE results. Exercises both hash aggregation and expression evaluation. |
| **Expected rows** | Up to 6 (one per RFM segment: Champions, Loyal, Potential Loyalists, At Risk, Lost, Casual) |

### Q7: Channel Conversion Funnel

| Field | Value |
|---|---|
| **ID** | `Q7_channel_conversion_funnel` |
| **Category** | aggregation |
| **Table** | Silver (`customer_interactions_enriched`) |
| **Description** | Builds a per-channel conversion funnel using conditional `SUM(CASE)` expressions to count awareness, consideration, conversion, and retention interactions. Computes conversion rate as a percentage and total channel revenue. |
| **What it tests** | Conditional aggregation (4 SUM(CASE) expressions), NULLIF for safe division, CAST for type promotion. Tests aggregation throughput when computing many derived columns per group. |
| **Expected rows** | One per channel. Typically 4-6 rows (web, mobile, store, call_center, etc.). |

### Q9: Executive Dashboard

| Field | Value |
|---|---|
| **ID** | `Q9_executive_dashboard` |
| **Category** | operational |
| **Table** | Gold (`customer_executive_dashboard`) |
| **Description** | Reads the pre-aggregated gold table and computes day-over-day revenue change and DAU growth percentage using LAG window functions. Returns the most recent 30 days. |
| **What it tests** | Gold layer read performance. The gold table is small and pre-aggregated, so this tests the "last mile" query path that executive dashboards use. LAG window functions verify that Trino can compute deltas on pre-materialized data. |
| **Expected rows** | Up to 30 (one per day, limited by LIMIT clause) |

## Query Categories Summary

| Category | Queries | Focus |
|---|---|---|
| `scan` | Q1 | Full table I/O throughput |
| `filter_prune` | Q2, Q4 | Predicate pushdown and date-range pruning |
| `aggregation` | Q3, Q7 | Hash aggregation and conditional expressions |
| `analytics` | Q5, Q6 | Window functions, CTEs, complex classification |
| `operational` | Q9 | Gold layer dashboard reads |

Category-level QpH is also computed in the benchmark results, allowing you
to identify whether scan, aggregation, or analytics queries are the bottleneck
for a given configuration.

## Running Ad-Hoc Queries

Lakebench provides a `query` command for running arbitrary SQL against the
deployed Trino cluster.

### Custom SQL

```bash
lakebench query config.yaml --sql "SELECT count(*) FROM lakehouse.silver.customer_interactions_enriched"
```

### Built-in Examples

Run a named example query:

```bash
lakebench query config.yaml --example count
```

Available examples: `count`, `revenue`, `channels`, `engagement`, `funnel`,
`clv`. Run `lakebench query config.yaml` with no arguments to see the full
list with descriptions.

### From File

```bash
lakebench query config.yaml --file my-query.sql
```

Or pipe from stdin:

```bash
echo "SELECT count(*) FROM lakehouse.gold.customer_executive_dashboard" | lakebench query config.yaml --file -
```

### Interactive Shell

Start a REPL for iterative query exploration:

```bash
lakebench query config.yaml --interactive
```

### Output Formats

All query modes support `--format` for output control:

```bash
lakebench query config.yaml --example revenue --format json
lakebench query config.yaml --example revenue --format csv
lakebench query config.yaml --example revenue --format table   # default
```
