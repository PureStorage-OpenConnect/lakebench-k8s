# Customer 360 benchmark: query set

## 6. Query set

Customer 360 runs 8 queries over the silver and gold tables. Queries are
defined in `src/lakebench/benchmark/queries.py`. Silver is
`customer_interactions_enriched`; gold is `customer_executive_dashboard`.

`query_set_id` is `qs8-32043638dbc4`: `qs<count>-` plus the first 12 hex
characters of a SHA-256 over each distinct query name and its registry SQL,
sorted by name (a name not in the registry hashes by name only).

- The hash covers the registry SQL, not engine-rewritten SQL. Changing any
  query's SQL changes the id, and QpH across ids is refused.
- A batch run's id covers every query name in the result, successful or not.
- A continuous round records the set of queries that succeeded in it; the
  run's id is that of its aggregated rounds ([8.2](metrics.md#82-continuous)).
- Power-run order: Q1, Q2, Q4, Q3, Q7, Q5, Q6, Q9 (by class: scan,
  filter/prune, aggregation, analytics, operational). There is no Q8.
- Throughput mode: each stream shuffles the order independently.
- The results report QpH per class, which shows whether scan, aggregation or
  analytics queries are the bottleneck.

| Id | Class | Reads | Business question | Rows on a correct corpus |
|---|---|---|---|---|
| `Q1_full_aggregation_scan` | scan | silver | How many interactions, customers and sessions, total revenue, mean transaction value? No predicates | 1 |
| `Q2_filtered_aggregation` | filter_prune | silver | Per day of the first three months of data (subquery on `MIN(interaction_date)`) and interaction type: interactions, customers, revenue; ordered by revenue | 5 x days in the first 3 months |
| `Q4_churn_risk_analysis` | filter_prune | silver | Where are high and medium churn-risk interactions, by journey stage and device category, `HAVING COUNT(DISTINCT customer_id) > 10`? | 6 (2 risks x retention x 3 device categories) |
| `Q3_customer_segmentation` | aggregation | silver | For `transaction_amount > 0`: customers, spend, engagement and estimated LTV per `customer_value_tier` x `channel_preference` | 12 (3 tiers x 4 preferences) |
| `Q7_channel_conversion_funnel` | aggregation | silver | Per channel: awareness, consideration, conversion and retention counts (4 `SUM(CASE)`), conversion rate %, revenue, engagement | 5 |
| `Q5_revenue_trend_ma7` | analytics | silver | CTE of daily active customers, revenue and interactions with 7-day moving averages; date descending, latest 90 days | min(90, days) |
| `Q6_customer_rfm` | analytics | silver | Per-customer recency, frequency (distinct dates) and monetary, CASE into Champions, Loyal, Potential Loyalists, At Risk, Lost, Casual; customers, average spend, frequency and recency per segment | 1 to 6 (one per segment present) |
| `Q9_executive_dashboard` | operational | gold | Latest 30 days of the dashboard with day-over-day revenue change and DAU growth % (`LAG`) | min(30, days) |

What each query stresses:

| Id | Stresses |
|---|---|
| `Q1_full_aggregation_scan` | raw I/O and scan; scales linearly with data volume |
| `Q2_filtered_aggregation` | predicate pushdown, date-range pruning, grouped aggregation, min-value lookup optimization |
| `Q4_churn_risk_analysis` | IN-list filter, three-column GROUP BY, post-aggregation HAVING |
| `Q3_customer_segmentation` | two-column hash aggregation, several aggregates, predicate before grouping |
| `Q7_channel_conversion_funnel` | conditional aggregation, `NULLIF` safe division, `CAST`, many derived columns per group |
| `Q5_revenue_trend_ma7` | CTE, `ROWS BETWEEN` window frames, ORDER BY with LIMIT |
| `Q6_customer_rfm` | `DATE_DIFF`, multi-branch CASE, re-aggregation of a CTE |
| `Q9_executive_dashboard` | a small pre-aggregated gold read: the dashboard path |

The row counts are those the `benchmark_rows_*` checks require
(`metrics/c360_correctness.py:benchmark_checks`), under the conditions in
[5.2](correctness.md#52-expected-result-checks-batch-only):

- Q2 is checked when every interaction type appears every day;
- Q3 is checked at 5,000 transactions or more;
- Q4 is checked at 5,000 support rows or more.

Rules for every query:

- **Tie-breaking.** Every `ORDER BY` feeding a `LIMIT` or window ends in a
  unique key: Q5 and Q9 order by `interaction_date`, unique per row of their
  input. Queries without a `LIMIT` (Q2, Q3, Q4, Q6, Q7) may return ties in
  engine-dependent order.
- **Time zone.** Stage and query sessions are pinned to UTC on every engine
  (Trino `--timezone`, Spark Thrift `spark.sql.session.timeZone=UTC`, DuckDB
  `SET TimeZone='UTC'`).
- **Dialect.** Queries are in Trino dialect. Engine adapters rewrite dialect
  and, on DuckDB, table references, nothing else. For example, on Spark
  Thrift `date_add('month', 3, x)` becomes `add_months(x, 3)` and
  `DATE_DIFF('day', a, b)` becomes `DATEDIFF(b, a)`.
- Q6 computes recency from `MAX(interaction_date)` in the data, not the run
  date, so its result is a function of the corpus.

## Running ad-hoc queries

`lakebench query` runs SQL against the deployment's configured query engine,
for either workload.

| Mode | Command |
|---|---|
| Custom SQL | `lakebench query config.yaml --sql "SELECT count(*) FROM lakehouse.silver.customer_interactions_enriched"` |
| Built-in example | `lakebench query config.yaml --example count` |
| From file | `lakebench query config.yaml --sql-file my-query.sql` |
| From stdin | `echo "SELECT count(*) FROM lakehouse.gold.customer_executive_dashboard" \| lakebench query config.yaml --sql-file -` |
| Interactive shell (REPL) | `lakebench query config.yaml --interactive` |

- Examples: `count`, `revenue`, `channels`, `engagement`, `funnel`, `clv`.
  `lakebench query config.yaml` with no arguments lists them with
  descriptions.
- `--format table` (default), `json` or `csv` works in every mode.
