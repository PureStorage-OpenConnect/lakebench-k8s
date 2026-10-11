# AML benchmark: data model

## 2. Data model

- All tables are Iceberg; the AML workload refuses Delta at config load.
- Every table is format-version 2, snappy, metadata delete-after-commit, 50
  previous metadata versions (`spark/scripts/common.py`).
- The executing DDL is inline in each stage script. `deploy/financial_ddl.py`
  is a reference copy no runtime code imports; unit tests keep it in step
  with the silver and TM column lists.
- No table declares a primary key; the keys below are logical.

Row counts marked recorded come from the 1.6 batch records (scale 1 and 10),
on the 1.6 generator image, not comparable with 1.7, n=1 each at that run's
seed. Bronze and everything downstream move with the seed through the
screening track ([3.2](generation.md#32-scale-factor)). Both records hold the
corpus as declared by the config, not observed from the datagen pods
(`experiment.corpus.observed: false`).

### 2.1 Raw corpus (S3, written by the generator)

| Object | Grain | Key | Layout | Scale 1 | Scale 10 |
|---|---|---|---|---|---|
| `bronze/pacs008/part-NNNNNN.parquet` | one payment (pacs.008 credit transfer) | `txn_id`, `uetr` | flat files, no partition directories; 137 files at scale 1, 1,371 at scale 10 | 26,671,867 rows (recorded) | 266,718,594 rows (recorded) |
| `bronze/party.parquet` | one party of the modelled world | `entity_id` | single object | 111,111 (formula) | 1,111,110 (formula) |
| `bronze/account.parquet` | one account | `account_id` / `iban` | single object | not recorded | not recorded |
| `bronze/watchlist.parquet` | one sanctions (v1, v2) or PEP list entry | `list_id` | single object | not recorded | not recorded |
| `manifest/manifest.parquet` | one planted instance (the answer key) | `typology_id` | separate `manifest/` prefix | 7,285 (recorded) | 73,083 (recorded) |

The pacs.008 file has 41 top-level columns (`datagen_rs/src/schema.rs`):

- group header (`msg_id`, `cre_dt_tm`, `nb_of_txs`, `ctrl_sum`, settlement
  date and method) and payment type information;
- the transaction: `txn_id`, `end_to_end_id`, `uetr`, settlement and
  instructed amounts as DECIMAL(18,5) with currencies, exchange rate, charge
  bearer;
- instructing, instructed and initiating parties; up to three intermediary
  and previous instructing agents;
- full debtor and creditor party, account and agent structs; ultimate
  parties; purpose, regulatory reporting and remittance information.

Bronze carries no label column. The answer key is only in the manifest: per
instance its typology, participant entity ids, participant UETRs, injection
window, parameters, expected workload, severity, seed and `model_version`.

The party file carries identity, address, contact, LEI/BIC and KYC columns
(`is_customer`, `home_fi`, `customer_since`, `customer_type`,
`expected_monthly_volume_usd`, `crr_score`, `crr_tier`, `crr_factors`) and
`model_version`. The account file's `current_balance` is always NULL.

The pacs.008 row count exceeds the base-payment formula (26,666,640 at scale
1) because sanctions and PEP screening rows are added on top; their number
depends on the seed.

### 2.2 Bronze tables

| Table | Grain | Partitioning | Scale 1 | Scale 10 |
|---|---|---|---|---|
| `architecture.tables.bronze` (default `default.bronze_raw`) | one payment, schema inferred from the Parquet | unpartitioned (zero-copy `add_files`, or a CTAS fallback) | 26,671,867 | 266,718,594 |
| `bronze.manifest` | one planted instance, CTAS of the manifest Parquet | unpartitioned | 7,285 | 73,083 |

In continuous mode the bronze table gains `ingest_ts` and the bronze-ingest
stream fills it.

### 2.3 Silver tables

| Table | Grain | Logical key | Partitioning | Scale 1 | Scale 10 |
|---|---|---|---|---|---|
| `silver.transactions` | one payment | `txn_id` | `months(txn_timestamp)` | 26,671,867 | 266,718,594 |
| `silver.entities` | one distinct party seen in payments, joined to KYC | `entity_id` | none | 113,721 | 1,137,292 |
| `silver.accounts` | one IBAN | `account_id` | none | 113,721 | 1,137,292 |
| `silver.account_statements` | one booked entry (camt.053 shape): one DBIT and one CRDT per payment | (`account_id`, `entry_seq`) | `months(book_ts)` | 53,343,734 | 533,437,188 |
| `silver.counterparty_edges` | one (originator, beneficiary) pair (batch); one pair per micro-batch (continuous) | (`source_entity_id`, `target_entity_id`) | `bucket(64, source_entity_id)` | 4,894,537 | 49,005,465 |
| `silver.entity_profiles` | one entity's behavioural baseline | `entity_id` | `bucket(64, entity_id)` | 113,721 | 1,137,292 |
| `silver.silver_batch_versions` | one sealed commit marker | (`stream_id`, `batch_id`) | none | one per batch cycle | one per batch cycle |
| `silver.counterparty_pairs` | one (originator, beneficiary) pair the first time a batch sees it; continuous only | (`originator_id`, `beneficiary_id`) | none | n/a | n/a |

- In both 1.6 batch records entities = accounts = profiles, statements = 2 x
  transactions, and edges < transactions. Lakebench records these counts
  (`jobs[].silver_tables`) but does not check the relations
  ([12](limitations.md#12-known-limitations)).
- `entity_id` is `xxhash64` of the LEI, else a hash of upper(name), country
  and upper(city). `silver.entities` is larger than the party file because
  it includes counterparties outside the modelled world.
- Silver rows carry the per-run sentinels `_batch_id`, `_stream_id` and
  `ingest_ts`, excluded from content parity.
- `silver.entity_profiles.profile_updated_ts` is stamped from the silver data
  clock ([3.5](generation.md#35-content-time-range-and-dirty-data)).

### 2.4 Gold tables

| Table | Grain | Logical key | Partitioning | Scale 1 | Scale 10 |
|---|---|---|---|---|---|
| `gold.alerts` | one alert from one rule in this run | `alert_id` (per-run UUID) | `months(alert_ts)` | 712,775 | 7,419,581 |
| `gold.detection_status` | one rule's outcome for the run | (`rule_id`, `run_id`) | none | 9 | 9 |
| `gold.daily_dashboards` | one (day, rule_id, disposition); one `baseline` row per settlement day | (`day`, `rule_id`, `disposition`) | none | not recorded | not recorded |
| `gold.risk_scores` | one entity scored by W4 | (`entity_id`, `model_id`) | none | not recorded | not recorded |
| `gold.entity_clusters` | one W1 cluster member | cluster id | none | not recorded (W1 skipped) | not recorded (W1 skipped) |
| `gold.tm_reconciliation` | one monitoring-completeness ledger row per cycle | (`run_id`, cycle) | none | not recorded | not recorded |
| `gold.scenario_coverage` | one scenario's coverage row | scenario | none | not recorded | not recorded |
| `gold.alert_dispositions` | one alert's L1 disposition | alert identity | none | 712,775 | 7,419,581 |
| `gold.cases` | one L2 case (at most one open per customer) | `case_id` | none | 66,177 | 687,407 |

`gold.alerts` columns: `alert_id`, `rule_id`, `rule_version`, `model_id`,
`model_version`, `entity_id`, `related_txn_ids`, `related_entity_ids`,
`alert_ts`, `alert_score`, `priority`, `status`, `disposition`,
`alert_type`, `run_id`, `narrative`, `evidence` (map), `detected_ts`,
`reason_codes` (array, added in 1.7; [4.2](rules.md#42-detection-rules)).
