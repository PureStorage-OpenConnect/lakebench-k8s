# Data Generation Schema

Lakebench generates synthetic **Customer360** data that simulates a retail and
e-commerce customer interaction dataset. The data is written as Snappy-compressed
Apache Parquet files directly to the S3 bronze bucket, where it serves as the
input for the medallion pipeline (bronze -> silver -> gold).

The generator (`datagen/generate.py`) produces deterministic, reproducible output
seeded per file. Each file contains approximately 122,000 rows at the default
512 MB file size, with ~4.1 KB per row after Snappy compression.

## Column Reference

The schema contains 38 columns spanning customer identity, interaction events,
product activity, marketing attribution, loyalty programs, and data quality
metadata.

| # | Column | Type | Description | Example Values |
|---|--------|------|-------------|----------------|
| 1 | `id` | int64 | Monotonically increasing row identifier | `0`, `122000`, `244000` |
| 2 | `row_id` | int64 | Global row identifier (same as `id`) | `0`, `122000`, `244000` |
| 3 | `event_timestamp` | timestamp(us) | Timestamp of the interaction event within a configurable date range | `2024-06-15 14:32:07.123456` |
| 4 | `event_id` | string | UUID v4 uniquely identifying the event | `a3f1b2c4-d5e6-4f78-9a0b-1c2d3e4f5678` |
| 5 | `session_id` | string | UUID v4 identifying the user session | `b7c8d9e0-f1a2-4b3c-8d4e-5f6a7b8c9d0e` |
| 6 | `customer_id` | int64 | Zipf-distributed customer identifier (see Realism Features) | `42`, `1337`, `499999` |
| 7 | `email_raw` | string | Raw email address, intentionally dirty (duplicates, corruption) | `user123456@gmail.com`, `user789.DUPLICATE@yahoo.com` |
| 8 | `phone_raw` | string | Raw phone number in mixed formats | `+12025551234`, `(415) 555-6789` |
| 9 | `interaction_type` | string | Type of customer interaction (weighted distribution) | `purchase`, `browse`, `support`, `login`, `abandoned_cart` |
| 10 | `product_id` | string | Product identifier (null for login/support events) | `PRD10234`, `PRD87654` |
| 11 | `product_category` | string | Product category (null for login/support events) | `electronics`, `clothing`, `home_garden`, `books`, `sports` |
| 12 | `transaction_amount` | float64 | Transaction value in local currency (non-zero only for purchases, log-normal distribution) | `73.42`, `549.99`, `0.0` |
| 13 | `currency` | string | Transaction currency code | `USD`, `EUR`, `GBP`, `CAD` |
| 14 | `channel` | string | Interaction channel | `web`, `mobile_app`, `store`, `call_center`, `social_media` |
| 15 | `device_type` | string | Device used (null for store/call_center channels) | `desktop`, `mobile`, `tablet` |
| 16 | `browser` | string | Browser name (null for store/call_center channels) | `chrome`, `safari`, `firefox`, `edge` |
| 17 | `ip_address` | string | IPv4 address of the client | `192.168.1.42`, `10.0.55.123` |
| 18 | `city_raw` | string | Raw city name with intentional inconsistencies and misspellings | `New York`, `NYC`, `Chicgao`, `Los Angelas` |
| 19 | `state_raw` | string | Raw state name with inconsistent abbreviation and casing | `NY`, `New York`, `california`, `Tex.` |
| 20 | `zip_code` | string | 5-digit US ZIP code | `10001`, `90210`, `60601` |
| 21 | `page_views` | int32 | Number of pages viewed (non-zero only for browse/purchase events) | `0`, `5`, `17` |
| 22 | `time_on_site_seconds` | int32 | Session duration in seconds (non-zero only when page_views > 0) | `0`, `120`, `3600` |
| 23 | `bounce_rate` | float64 | Bounce indicator: 1.0 if page_views == 1, otherwise 0.0 | `0.0`, `1.0` |
| 24 | `click_count` | int32 | Number of clicks during session (null for login/support events) | `1`, `42`, `100` |
| 25 | `cart_value` | float64 | Current cart value in dollars (null/NaN for login/support events) | `0.0`, `249.99`, `8500.00` |
| 26 | `items_in_cart` | int32 | Number of items in cart (null for login/support events) | `0`, `3`, `20` |
| 27 | `support_ticket_id` | string | Support ticket ID (non-null only for support events) | `TKT54321` |
| 28 | `issue_category` | string | Support issue category (non-null only for support events) | `billing`, `technical`, `general_inquiry` |
| 29 | `satisfaction_score` | int32 | CSAT score 1-5 (non-null only for support events) | `1`, `3`, `5` |
| 30 | `campaign_id` | string | Marketing campaign ID (present for ~40% of events) | `CMP456` |
| 31 | `utm_source` | string | UTM source for marketing attribution (null when no campaign) | `google`, `facebook`, `email`, `direct` |
| 32 | `utm_medium` | string | UTM medium for marketing attribution (null when no campaign) | `cpc`, `organic`, `referral` |
| 33 | `loyalty_member` | bool | Whether the customer is a loyalty program member | `true`, `false` |
| 34 | `loyalty_tier` | string | Loyalty tier (null for non-members) | `bronze`, `silver`, `gold` |
| 35 | `points_earned` | int32 | Loyalty points earned (non-zero only for purchase events by members) | `0`, `734`, `5499` |
| 36 | `points_redeemed` | int32 | Loyalty points redeemed (10% chance per member interaction) | `0`, `250`, `999` |
| 37 | `data_source` | string | Origin system for the record (weighted distribution) | `primary_system`, `legacy_import`, `manual_entry`, `third_party_api` |
| 38 | `data_quality_flag` | string | Quality classification assigned at generation time | `clean`, `duplicate_suspected`, `incomplete_data`, `format_inconsistent` |
| 39 | `raw_user_agent` | string | Synthetic browser user-agent string | `chrome/118.0 (desktop; Windows NT 10.0)` |
| 40 | `session_fingerprint` | string | SHA-256 style hex fingerprint (64 chars) | `a1b2c3d4e5f6...` (64 hex characters) |
| 41 | `interaction_payload` | string | Random hex payload used as a compression anchor | `4f2a8b...` (configurable size, default 2 KB) |

> **Note:** The column count in the table above is 41 rows because `id` and
> `row_id` are two distinct columns that carry the same value, and every column
> in the Arrow schema is listed individually. The PyArrow schema definition in
> `generate.py` defines exactly 41 fields.

## Realism Features

The generator implements seven realism features designed to produce data that
exercises real-world data engineering challenges in the silver cleaning layer.

### 1. Zipf-Distributed Customer IDs

Customer IDs follow a Zipf distribution (alpha=1.5) rather than uniform random.
This produces power-law activity where a small number of "whale" customers
generate a disproportionate share of events, matching real retail behavior.
The maximum customer ID is configurable (default: 500,000).

### 2. Weighted Interaction Types

Interaction types are drawn from a weighted distribution rather than uniform:
- browse: 35%
- login: 20%
- purchase: 18%
- abandoned_cart: 15%
- support: 12%

This produces a realistic conversion funnel where browsing dominates and
purchases are a minority of events.

### 3. Conditional Nulls

Columns are conditionally nulled based on the interaction type to reflect
real-world semantics:
- **Login and support events** have null `product_id`, `product_category`,
  `cart_value`, `items_in_cart`, and `click_count` because these fields are
  meaningless for non-commerce interactions.
- **Support events** are the only events that populate `support_ticket_id`,
  `issue_category`, and `satisfaction_score`.
- **Marketing attribution** (`campaign_id`, `utm_source`, `utm_medium`) is
  present for approximately 40% of events.

### 4. Dirty Data Corruption

Approximately 8% of rows (configurable via `dirty_data_ratio`) receive
corruption across multiple fields:

- **Emails:** Six corruption modes -- missing `@`, ALL CAPS, leading/trailing
  whitespace, double `@@`, missing TLD, `@` replaced with `.at.`.
- **Phone numbers:** Four corruption modes -- digits only (no formatting),
  extra dashes, truncated numbers, letter `O` substituted for digit `0`.
- **City names:** Intentional misspellings (`Chicago` -> `Chicgao`,
  `Phoenix` -> `Pheonix`, `Los Angeles` -> `Los Angelas`).
- **State names:** Inconsistent casing and abbreviation styles (`CA` vs.
  `california` vs. `CALIFORNIA` vs. `Calif.`).

### 5. Channel-Device Coherence

When the interaction channel is `store` or `call_center` (offline channels),
`device_type` and `browser` are set to null. A physical store visit does not
have a browser. This tests the silver layer's ability to handle structurally
absent fields without treating them as data quality issues.

### 6. Log-Normal Transaction Amounts

Purchase transaction amounts follow a log-normal distribution
(mu=4.3, sigma=1.2) clipped to the range $1.00--$9,999.99. This produces a
long-tailed distribution where most purchases are modest but occasional
high-value transactions appear, matching real retail patterns. Non-purchase
events have a transaction amount of 0.0.

### 7. Customer-Consistent Loyalty

Each customer ID is deterministically mapped to a loyalty membership status and
tier using a pre-built lookup table seeded once per generation run. The same
customer always has the same loyalty attributes across all their events.
Distribution: 60% of customers are members, split 70/20/10 across
bronze/silver/gold tiers.

## Data Quality Flags

Every row receives a `data_quality_flag` drawn from a weighted distribution:

| Flag | Weight | Meaning |
|------|--------|---------|
| `clean` | 92% | No known issues |
| `duplicate_suspected` | 2% | Row may be a duplicate |
| `incomplete_data` | 3% | Row has missing or partial fields |
| `format_inconsistent` | 3% | Row has formatting inconsistencies |

These flags are assigned independently of the dirty data corruption (feature 4).
The silver layer uses these flags for filtering and quality reporting.

In addition, approximately 10% of email addresses contain a `.DUPLICATE` marker
injected before the `@` symbol (e.g., `user123.DUPLICATE@gmail.com`), simulating
duplicate records from upstream systems.

## Data Source Distribution

Each row is tagged with a `data_source` indicating its origin system:

| Source | Weight | Description |
|--------|--------|-------------|
| `primary_system` | 70% | Main transactional system |
| `legacy_import` | 15% | Migrated from legacy platform |
| `manual_entry` | 10% | Manually entered records |
| `third_party_api` | 5% | External API integrations |

## Partitioning and Output Format

Data is written as Snappy-compressed Parquet files to the configured S3 bronze
bucket under the path prefix `customer/interactions/`. Files are named by
worker index: `part-000000.parquet`, `part-000001.parquet`, and so on.

When running as a Kubernetes Indexed Job, each pod receives a
`JOB_COMPLETION_INDEX` that determines which file IDs it generates. Files are
interleaved across nodes (file `N` goes to node `N % total_nodes`), ensuring
even distribution regardless of pod count.

The generator supports checkpoint-resume: completed file IDs are persisted to a
JSON checkpoint file, and interrupted runs can resume without regenerating
already-uploaded files.

## Scale Factor

Data volume is controlled by the `scale` parameter in the Lakebench
configuration. One scale unit produces approximately 10 GB of on-disk bronze
data. Common scale factors:

| Scale | Approximate Bronze Size | Total Files (at 512 MB) |
|-------|------------------------|------------------------|
| 1 | ~10 GB | ~20 |
| 10 | ~100 GB | ~200 |
| 100 | ~1 TB | ~2,048 |
| 1000 | ~10 TB | ~20,480 |
