# Customer 360 benchmark: data model

## 2. Data model

The stage scripts create tables through Spark DataFrameWriterV2 (Iceberg) or
the Delta write helpers; Customer 360 has no DDL file. Default names come from
`TableNamesConfig` (`default.bronze_raw`,
`silver.customer_interactions_enriched`, `gold.customer_executive_dashboard`),
prefixed at runtime with the pipeline catalog.

| Object | Layer | Mode | Grain | Key | Partitioning | Rows, scale 1 | Rows, scale 10 |
|---|---|---|---|---|---|---|---|
| Landing Parquet `s3://<bronze>/customer/interactions/part-NNNNNN.parquet` | bronze (raw) | both | one interaction event | `id` = `row_id` (unique per file block) | none; one object per file id | 2,478,560 (sizing code: 160 files x 15,491 rows) | 24,770,109 (1,599 files x 15,491; also recorded on the `c360-1` scale-10 record) |
| `default.bronze_raw` | bronze (table) | continuous only | one landed event | none enforced | unpartitioned | equals landing rows once drained (not separately recorded) | not recorded |
| `silver.customer_interactions_enriched` | silver | both | one bronze row whose `data_quality_flag` is not `duplicate_suspected` | none enforced (`event_id` is unique in the generator) | identity on `interaction_date` | about 98% of bronze (not in a published record) | 24,274,519 (recorded) |
| `gold.customer_executive_dashboard` | gold | both | one `interaction_date` present in silver (enforced in batch, [5.1](correctness.md#51-checks-that-gate-a-run-fail-it)) | `interaction_date` | unpartitioned, one file per write | 366 (one per day of the default 2024 window) | 366 (recorded) |

- The `c360-1` scale-10 record ran on generator image `lb-datagen:034f998`
  under identity version 1. Its row counts hold, but its corpus id names a
  different image, so it is not comparable with any run of this release and
  cannot be a reference run.
- The `c360-2` bump changed gold-finalize's strategy choice on repeat runs,
  not a first run's row counts.
- Dirty-data injection rewrites `email_raw`, `phone_raw`, `city_raw` and
  `state_raw` after the row is drawn. Row counts, `data_quality_flag` and
  every column the query set groups or filters on do not depend on it;
  silver `email_clean`, `phone_clean`, `state_standardized` and
  `city_standardized`, and gold `unique_emails`, do.
- Domain dimensions (`config/scale.py`): scale 1 = 100,000 customer ids,
  scale 10 = 1,000,000; nominal bronze 10 GB per scale unit. Its
  `approx_rows` (customers x 24) is guidance only; datagen's row count comes
  from file sizing ([3](generation.md#3-data-generation)).

### 2.1 Bronze columns (41, Arrow schema `customer360_schema()`)

Bronze is synthetic retail and e-commerce customer interactions. The
generator (`datagen_rs/src/customer360.rs`) writes Snappy-compressed Apache
Parquet directly to the S3 bronze bucket. Output is deterministic, seeded per
(seed, file_id). Generation rules are in [3](generation.md#3-data-generation).

The Arrow schema is in `datagen_rs/src/schema.rs`.

| # | Column | Type | Description | Example values |
|---|---|---|---|---|
| 1 | `id` | int64 | Monotonically increasing row identifier | `0`, `122000`, `244000` |
| 2 | `row_id` | int64 | Global row identifier (same value as `id`) | `0`, `122000`, `244000` |
| 3 | `event_timestamp` | timestamp(us, UTC) | Event time within a configurable date range | `2024-06-15 14:32:07.123456` |
| 4 | `event_id` | string | UUID v4 per event | `a3f1b2c4-d5e6-4f78-9a0b-1c2d3e4f5678` |
| 5 | `session_id` | string | UUID v4 per session | `b7c8d9e0-f1a2-4b3c-8d4e-5f6a7b8c9d0e` |
| 6 | `customer_id` | int64 | Zipf-distributed customer identifier; the top id gets about 10% of events, so no id dominates every aggregate | `42`, `1337`, `499999` |
| 7 | `email_raw` | string | Raw email, intentionally dirty (duplicates, corruption) | `user123456@gmail.com`, `user789.DUPLICATE@yahoo.com` |
| 8 | `phone_raw` | string | Raw phone number in mixed formats | `+12025551234`, `(415) 555-6789` |
| 9 | `interaction_type` | string | Interaction type (weighted) | `purchase`, `browse`, `support`, `login`, `abandoned_cart` |
| 10 | `product_id` | string | Product identifier (null for login/support) | `PRD10234`, `PRD87654` |
| 11 | `product_category` | string | Product category (null for login/support) | `electronics`, `clothing`, `home_garden`, `books`, `sports` |
| 12 | `transaction_amount` | float64 | Value in local currency; non-zero only for purchases (log-normal) | `73.42`, `549.99`, `0.0` |
| 13 | `currency` | string | Currency code | `USD`, `EUR`, `GBP`, `CAD` |
| 14 | `channel` | string | Interaction channel | `web`, `mobile_app`, `store`, `call_center`, `social_media` |
| 15 | `device_type` | string | Device (null for store/call_center: a store visit has no browser; silver treats it as absent, not as a quality issue) | `desktop`, `mobile`, `tablet` |
| 16 | `browser` | string | Browser (null for store/call_center) | `chrome`, `safari`, `firefox`, `edge` |
| 17 | `ip_address` | string | Random IPv4 address of the client | `10.0.1.50` |
| 18 | `city_raw` | string | Raw city name with inconsistencies and misspellings | `New York`, `NYC`, `Chicgao`, `Los Angelas` |
| 19 | `state_raw` | string | Raw state with inconsistent abbreviation and casing | `NY`, `New York`, `california`, `Tex.` |
| 20 | `zip_code` | string | 5-digit US ZIP code | `10001`, `90210`, `60601` |
| 21 | `page_views` | int32 | Pages viewed; non-zero only for browse/purchase | `0`, `5`, `17` |
| 22 | `time_on_site_seconds` | int32 | Session duration; non-zero only when `page_views` > 0 | `0`, `120`, `3600` |
| 23 | `bounce_rate` | float64 | 1.0 if `page_views` == 1, else 0.0 | `0.0`, `1.0` |
| 24 | `click_count` | int32, nullable | Clicks in session (null for login/support) | `1`, `42`, `100` |
| 25 | `cart_value` | float64 | Cart value in dollars (NaN, not null, for login/support) | `0.0`, `249.99`, `8500.00` |
| 26 | `items_in_cart` | int32, nullable | Items in cart (null for login/support) | `0`, `3`, `20` |
| 27 | `support_ticket_id` | string | Ticket ID (support only) | `TKT54321` |
| 28 | `issue_category` | string | Support issue category (support only) | `billing`, `technical`, `general_inquiry` |
| 29 | `satisfaction_score` | int32, nullable | CSAT 1-5 (support only) | `1`, `3`, `5` |
| 30 | `campaign_id` | string | Marketing campaign ID (about 40% of events) | `CMP456` |
| 31 | `utm_source` | string | UTM source (null when no campaign) | `google`, `facebook`, `email`, `direct` |
| 32 | `utm_medium` | string | UTM medium (null when no campaign) | `cpc`, `organic`, `referral` |
| 33 | `loyalty_member` | bool | Loyalty program member, the same on every event of a customer | `true`, `false` |
| 34 | `loyalty_tier` | string | Loyalty tier (null for non-members) | `bronze`, `silver`, `gold` |
| 35 | `points_earned` | int32 | Points earned; non-zero only for member purchases | `0`, `734`, `5499` |
| 36 | `points_redeemed` | int32 | Points redeemed (10% chance per member interaction) | `0`, `250`, `999` |
| 37 | `data_source` | string | Origin system (weighted, below) | `primary_system`, `legacy_import`, `manual_entry`, `third_party_api` |
| 38 | `data_quality_flag` | string | Quality class assigned at generation (below) | `clean`, `duplicate_suspected`, `incomplete_data`, `format_inconsistent` |
| 39 | `raw_user_agent` | string | Synthetic user-agent string | `chrome/118.0 (desktop; Windows NT 10.0)` |
| 40 | `session_fingerprint` | string | SHA-256-style hex fingerprint | `a1b2c3d4e5f6...` (64 hex characters) |
| 41 | `interaction_payload` | string | Random hex payload, a compression anchor: 2 KiB of random bytes as 4,096 hex characters | `4f2a8b...` |

`data_source` and `data_quality_flag` are drawn independently of dirty-data
corruption. Silver uses the flags for filtering and quality reporting.

| `data_source` | Weight | Meaning |
|---|---|---|
| `primary_system` | 70% | main transactional system |
| `legacy_import` | 15% | migrated from a legacy platform |
| `manual_entry` | 10% | manually entered records |
| `third_party_api` | 5% | external API integrations |

| `data_quality_flag` | Weight | Meaning |
|---|---|---|
| `clean` | 92% | no known issues |
| `duplicate_suspected` | 2% | row may be a duplicate |
| `incomplete_data` | 3% | row has missing or partial fields |
| `format_inconsistent` | 3% | row has formatting inconsistencies |

### 2.2 Silver columns

All 41 bronze columns plus 32 derived per row with no joins or shuffles:

- cleaned: `email_clean`, `phone_clean`, `state_standardized`,
  `city_standardized`;
- time: `interaction_date` (UTC date of `event_timestamp`),
  `interaction_hour`, `interaction_day_of_week`, `interaction_week_of_year`,
  `interaction_month`, `interaction_year`, `is_weekend`,
  `is_business_hours`, `is_peak_hours`;
- `customer_value_tier` (high > 500, medium > 100, low > 0, else
  `browser_only`), `transaction_size_category`;
- `engagement_score` (0 to 4 from `page_views`), `session_depth_category`,
  `time_spent_category`;
- `channel_preference` (mobile_first, web_first, physical_first, else
  omnichannel);
- `lifetime_value_estimate` (`round(amount * (1 + points_earned/1000), 2)`);
- `customer_recency_score` (30 minus days from the data clock,
  [4.1](pipeline.md#41-batch-mode-pipelinemode-batch)),
  `engagement_velocity`;
- `churn_risk_indicator` (satisfaction <= 2 high, <= 3 medium, NULL unknown,
  else low);
- `attribution_channel`, `attribution_quality`;
- `customer_journey_stage` (browse awareness, abandoned_cart consideration,
  purchase conversion, support retention, login other);
- `device_category` (NULL device reads `desktop`), `browser_family`,
  `interaction_context`, `customer_segment_key`;
- `silver_processing_timestamp` (wall clock at transform,
  non-deterministic), `data_quality_score`.

Batch silver adds `_batch_id` (the cycle index); continuous silver adds
`_stream_id` and `_batch_id` (stream query id and micro-batch id), used for
replay idempotency.

### 2.3 Gold columns

`interaction_date` plus 30 KPIs from one aggregation list shared by all four
gold writers (batch and continuous, Iceberg and Delta):

- `daily_active_customers`, `unique_emails`, `total_sessions`;
- `total_daily_revenue`, `avg_transaction_value` (rows with amount > 0),
  `largest_transaction`, `total_transactions`;
- four channel revenues (web, mobile_app, store, call_center);
- `avg_engagement_score`; `avg_time_on_site_seconds` and `avg_page_views`
  (rows with page_views > 0);
- four funnel counts (awareness, consideration, conversions, retention);
- `loyalty_member_interactions`, `total_points_earned`,
  `total_points_redeemed`;
- `support_tickets_created` (non-NULL ticket ids, not distinct),
  `avg_satisfaction_score`;
- `high_churn_risk_count`, `medium_churn_risk_count`;
- `total_estimated_ltv`, `avg_estimated_ltv` (per transaction);
- three channel interaction counts (web, mobile_app, store).
