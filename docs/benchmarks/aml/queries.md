# AML benchmark: query set

## 6. Query set

The AML set is `get_benchmark_queries("financial")` (`benchmark/queries.py`):
twelve queries. FQ1 to FQ8 read the AML silver and gold tables; IQ1 to IQ4
(investigator) read the transaction monitoring operations tables. Execution
order: `FQ1_txn_full_scan`, `FQ2_top_corridors_window`,
`FQ6_structuring_scan`, `FQ3_entity_edge_risk`,
`FQ7_cross_border_concentration`, `FQ4_running_balance_window`,
`FQ5_alert_triage`, `FQ8_alert_to_entity_join`, then `IQ1_customer_360`,
`IQ2_case_activity_12m`, `IQ3_counterparty_two_hop`,
`IQ4_open_cases_over_60_days`.

The investigator class runs only when TM operations is enabled and a TM run
id is supplied. Investigator queries read only that run's rows.

- Batch `run` supplies the id for its scored round when the TM verdict is
  `pass` or `fail`.
- Continuous rounds add it once the run has a case
  ([4.3](pipeline.md#43-continuous-mode)).
- The pre-compaction round and runs with TM disabled execute FQ1 to FQ8. So
  the pre- and post-maintenance rounds of an AML batch run time different
  query sets.

**Query set id.** `qs<N>-<first 12 hex of sha256>` over the sorted names and
SQL of the queries actually recorded, failed ones included.

| Set | Id |
|---|---|
| FQ1 to FQ8 plus IQ1 to IQ4 | `qs12-910d16a91962` |
| FQ1 to FQ8 | `qs8-ffe2bc1a012e` |
| IQ1 to IQ4 | `qs4-bc3b5e556bf7` |

- A `--class` subset gets its own id.
- Each continuous round records the set it executed. Rounds over different
  sets are not combined into one assessed QpH ([8.2](continuous-metrics.md#82-continuous)).
- The query set id is a workload identity key, so an 8-query AML run is
  never set against a 12-query one as like for like.
- A run recorded before the id existed can get a pinned historical id. That
  needs its query names to be the c360 set or the 8-query AML set, recorded
  after that set's last SQL change. It then stays comparable with runs over
  the same SQL.
- Older records and other legacy sets are `unknown`. A run of an older branch
  recorded after that date is the one case this cannot tell apart.

**Queries that read the mode's layout.**

- Continuous AML writes `silver.counterparty_edges` as one row per (source,
  target) pair per micro-batch, where batch writes one per pair.
- It stores `silver.account_statements` running balances (`bal_after`) in
  arrival order, labelled `arrival_order_running_balance` when a statement
  arrives late.
- The queries do not depend on either. FQ3 and both of IQ3's hops sum edge
  rows per pair, and FQ4 recomputes each running balance in ledger order.
- On batch silver they return what they did before, row for row. On settled
  continuous silver over the same corpus, they return the batch answer.
- The change moved the AML query-set ids (12 queries, and the 8 before the
  first TM pass), so no record from before it compares with one after. It is
  part of workload version `aml-2`.
- A batch record is still never compared with a continuous one: mode is a
  workload identity key.

| ID | Class | Business question | Reads | Ordering and checks |
|---|---|---|---|---|
| `FQ1_txn_full_scan` | scan | Payment count, distinct originators and beneficiaries, total and average USD | silver.transactions | one row; `avg_txn_usd` (fifth column) at a quantum of 0.01 |
| `FQ2_top_corridors_window` | filter_prune | Top 100 bank-to-bank corridors by USD in the last 30 days of data | silver.transactions | window anchored on MAX(txn_timestamp), not the wall clock; total order by volume, then BICs and currency |
| `FQ6_structuring_scan` | filter_prune | Originators with 3 or more payments just under a currency's reporting threshold (the W2 shape) | silver.transactions | top 500, total order by count, originator, currency |
| `FQ3_entity_edge_risk` | aggregation | Per-entity out-degree and outbound USD | counterparty_edges, entities | top 200 by outbound USD, entity id |
| `FQ7_cross_border_concentration` | aggregation | Cross-border share of USD per corridor | silver.transactions | top 100, total order; `xborder_share` (fifth column) at a quantum of 0.001 |
| `FQ4_running_balance_window` | analytics | Ordered statement entries for the 50 most active accounts, running balance recomputed in ledger order | account_statements | see below |
| `FQ5_alert_triage` | operational | Alerts by rule, priority and status with entity count and mean score | gold.alerts | ORDER BY alerts DESC only, no LIMIT; fingerprint order-independent; `avg_score` (sixth column) at a quantum of 0.001 |
| `FQ8_alert_to_entity_join` | operational | 100 most recent alerts with entity and payment count | gold.alerts, entities | total order by alert_ts, entity, rule, alert_id; `alert_id` volatile (NULL-ness only); `txns_in_alert` is Lakebench-capped, see below |
| `IQ1_customer_360` | investigator | 360 view of the top open case | cases, alert_dispositions, accounts, entities | case picked by status, priority, opened date, case id; TM reads scoped to the TM run id |
| `IQ2_case_activity_12m` | investigator | Monthly activity in the 365 days before the newest escalation case | cases, silver.transactions | `allow_empty` |
| `IQ3_counterparty_two_hop` | investigator | Counterparties and two-hop network of the oldest open case | cases, edges, entities, alert_dispositions | both hops sum edge rows per pair; top 50 hop-1, top 500 overall, total order |
| `IQ4_open_cases_over_60_days` | investigator | Open cases older than 60 days | cases, entities | ordered by age, case id; `allow_empty` |

- **FQ4.** Balance = the account's last stored `bal_after`, less the sum of
  its entries, plus the running sum over (book_ts, txn_id, debit first). So
  batch and settled continuous silver agree.
  - ROW_NUMBER over (book_ts, txn_id, debit first, entry_seq); outer order
    account, entry.
  - It returns every entry of those accounts, so it grows with scale.
    1,491,153 rows were recorded on the 1.6 batch scale-10 record (n=1, not
    comparable with 1.7).
- **FQ8.** `txns_in_alert` is `cardinality(related_txn_ids)`, bounded by the
  per-alert evidence caps ([7.4](execution-rules.md#74-lakebench-imposed-caps)):
  a Lakebench-capped count.

Rules for every query:

- Every ORDER BY feeding a LIMIT or ROW_NUMBER ends in keys that make the
  order total.
- Engine sessions are pinned to UTC (Trino `--timezone UTC`, DuckDB
  `SET TimeZone='UTC'`, Spark Thrift session time zone UTC). The fingerprint
  reads zone-less timestamps as UTC.
- Queries are in Trino dialect. Engine adapters change dialect (`date_add`,
  `DATE_DIFF`, and on DuckDB `cardinality` to `len`), with at most one
  `DATE_DIFF` per investigator query.
- The DuckDB adapter also replaces each catalog table name with a direct
  `iceberg_scan` of the table's storage path. So DuckDB reads table metadata
  from object storage, not the catalog; the run records
  `query_access_path: direct_storage`.
- Result check (batch): after the scored round's timed samples, one untimed
  execution per successful query records an `rf2` fingerprint.
  - It is an order-independent sum of per-row hashes over exact
    Decimal-normalised cells.
  - Approximate columns are summed per column, once plainly and once
    weighted by a per-row factor. They match when both sums are within
    tolerance.
  - Volatile columns count as NULL-ness.
  - A row-count mismatch between the timed and fingerprint executions marks
    the fingerprint unusable.
  - The pre-compaction round and continuous rounds are never fingerprinted.

Ad-hoc SQL against the deployment uses `lakebench query`
([Customer 360 queries](../c360/queries.md#running-ad-hoc-queries)).

**Scoring queries (not timed).** `benchmark/aml_queries.py` also defines 40
scoring queries: four per rule, named `<rule_id>_detect`, `_precision`,
`_recall` and `_pattern_span` (for example `W2_structuring_recall`), plus the
aggregates `aggregate_alert_volume`, `aggregate_top_entities`,
`aggregate_typology_coverage` and `aggregate_reference_vs_rule`.

- `lakebench run` does not execute or time them; scoring and the verdict use
  only the rule-to-typology map. They are not in the AML query set or its
  QpH.
- The templates use a 7-day window and are called only from the unit tests
  (`load_aml_queries`). No CLI command runs them, and they do not produce the
  published recall or precision.
- A recall figure cited with a 7-day window is wrong: the shipped numbers
  come from `score_financial.py` and have no window.
- `aggregate_typology_coverage` asks whether at least one rule fires on each
  planted typology. `aggregate_reference_vs_rule` sets rule recall beside the
  reference model's per-typology row in `reference_metrics.parquet`.

**Pattern span is not detection latency.**

- `pattern_span_s` (per rule, once labelled "time-to-detect") spans from a
  planted typology's injection start to the event time of the last payment a
  rule cites for it.
- `alert_ts` is the last contributing payment's event time, not when the
  alert was produced.
- The value is a property of the generator's typology windows
  (`datagen_rs/src/typology.rs`: 3 to 21 days for `micro_structuring`, one
  civil day for `rapid_layering`). It does not change with stack speed.
- The runtime scorer does not compute it. Use it only to check that a rule
  fires inside its typology window, never to compare speed.
- Detection latency is `time_to_detect_seconds` ([8.2](continuous-metrics.md#82-continuous)).
