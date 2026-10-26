# AML benchmark: query set

## 6. Query set

The AML set is twelve queries. FQ1 to FQ8 read the AML silver and gold tables; IQ1 to IQ4
(investigator) read the transaction monitoring operations tables. Execution
order: `FQ1_txn_full_scan`, `FQ2_top_corridors_window`,
`FQ6_structuring_scan`, `FQ3_entity_edge_risk`,
`FQ7_cross_border_concentration`, `FQ4_running_balance_window`,
`FQ5_alert_triage`, `FQ8_alert_to_entity_join`, then `IQ1_customer_360`,
`IQ2_case_activity_12m`, `IQ3_counterparty_two_hop`,
`IQ4_open_cases_over_60_days`.

The investigator class runs only when TM operations is enabled and a TM run
id is supplied. Investigator queries read only that run's rows.

- Batch `run` supplies the id to every benchmark round when the TM verdict
  is `pass` or `fail`.
- Continuous rounds add it once the run has a case
  ([4.3](pipeline.md#43-continuous-mode)).
- Runs with TM disabled execute FQ1 to FQ8. When a batch run has a
  pre-maintenance round (scale below 50), it executes the same set as the
  scored round.

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
- The query set id is a workload identity key: an 8-query AML run is never
  compared with a 12-query one.

**Queries that read the mode's layout.**

- Continuous AML writes `silver.counterparty_edges` as one row per (source,
  target) pair per micro-batch, where batch writes one per pair.
- It stores `silver.account_statements` running balances (`bal_after`) in
  arrival order, labelled `arrival_order_running_balance` when a statement
  arrives late.
- The queries depend on neither: FQ3 and both of IQ3's hops sum edge rows
  per pair, and FQ4 recomputes each running balance in ledger order. All
  three return the same answer on batch and settled continuous silver.
- A batch record is still never compared with a continuous one: mode is a
  workload identity key.

| ID | Class | Business question | Reads | Ordering and checks |
|---|---|---|---|---|
| `FQ1_txn_full_scan` | scan | Payment count, distinct originators and beneficiaries, total and average USD | silver.transactions | one row |
| `FQ2_top_corridors_window` | filter_prune | Top 100 bank-to-bank corridors by USD in the last 30 days of data | silver.transactions | window anchored on MAX(txn_timestamp), not the wall clock; total order by volume, then BICs and currency |
| `FQ6_structuring_scan` | filter_prune | Originators with 3 or more payments just under a currency's reporting threshold (the W2 shape) | silver.transactions | top 500, total order by count, originator, currency |
| `FQ3_entity_edge_risk` | aggregation | Per-entity out-degree and outbound USD | silver.counterparty_edges, silver.entities | top 200 by outbound USD, entity id |
| `FQ7_cross_border_concentration` | aggregation | Cross-border share of USD per corridor | silver.transactions | top 100, total order |
| `FQ4_running_balance_window` | analytics | Ordered statement entries for the 50 most active accounts, running balance recomputed in ledger order | silver.account_statements | see below |
| `FQ5_alert_triage` | operational | Alerts by rule, priority and status with entity count and mean score | gold.alerts | ORDER BY alerts DESC only, no LIMIT |
| `FQ8_alert_to_entity_join` | operational | 100 most recent alerts with entity and payment count | gold.alerts, silver.entities | total order by alert_ts, entity, rule, alert_id; `alert_id` is a per-run uuid; `txns_in_alert` is Lakebench-capped, see below |
| `IQ1_customer_360` | investigator | 360 view of the top open case | gold.cases, gold.alert_dispositions, silver.accounts, silver.entities | case picked by status, priority, opened date, case id; TM reads scoped to the TM run id |
| `IQ2_case_activity_12m` | investigator | Monthly activity in the 365 days before the newest escalation case | gold.cases, silver.transactions | `allow_empty` |
| `IQ3_counterparty_two_hop` | investigator | Counterparties and two-hop network of the oldest open case | gold.cases, silver.counterparty_edges, silver.entities, gold.alert_dispositions | both hops sum edge rows per pair; top 50 hop-1, top 500 overall, total order |
| `IQ4_open_cases_over_60_days` | investigator | Open cases older than 60 days | gold.cases, silver.entities | ordered by age, case id; `allow_empty` |

- **FQ4.** Balance = the account's last stored `bal_after`, less the sum of
  its entries, plus the running sum over (book_ts, txn_id, debit first).
  - ROW_NUMBER over (book_ts, txn_id, debit first, entry_seq); outer order
    account, entry.
  - It returns every entry of those accounts, so it grows with scale.
    One batch run at scale 10 returned 1,491,153 rows (n=1).
- **FQ8.** `txns_in_alert` is `cardinality(related_txn_ids)`, bounded by the
  per-alert evidence caps ([7.4](execution-rules.md#74-lakebench-imposed-caps)):
  a Lakebench-capped count.

Rules for every query:

- Every ORDER BY feeding a LIMIT or ROW_NUMBER ends in keys that make the
  order total.
- Trino sessions are pinned to UTC (`--timezone UTC`); the financial
  workload runs on no other query engine.
- Queries are in Trino dialect.
- Each query's answer is recorded as its row count from the timed run; no
  answer hash is recorded.

Ad-hoc SQL against the deployment uses `lakebench query`
([Customer 360 queries](../c360/queries.md#running-ad-hoc-queries)).

**`alert_ts` is event time.** An alert's `alert_ts` is the event time of the
last payment it cites, not when the alert was produced. Detection latency is
`time_to_detect_seconds` ([8.2](continuous-metrics.md#82-continuous)).
