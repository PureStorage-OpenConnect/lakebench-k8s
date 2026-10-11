# Customer 360 benchmark: correctness contract

## 5. Correctness contract

### 5.1 Checks that gate a run (fail it)

| Check | Mode | Where |
|---|---|---|
| Unsupported workload x architecture x mode, or a scale above the datagen ceiling (600), refused before anything runs (exit 2) | both | `cli/_run.py`, `config/support.py` |
| Prerequisites (cluster capacity included), infrastructure readiness, and a Spark Operator not ready or not watching the namespace: refused (exit 4); a missing namespace exits 5 unless `--yes` deploys it first | both | `cli/_run.py` |
| A dependency set missing or stale (exit 4) or not matching the deployment (exit 3), refused before anything is recorded | both | `cli/_helpers.py` |
| At run end, these fail the run and the verdict's `dependency_set` gate: a query engine pod that ran a different dependency set; jobs built on a set other than the recorded one; pods unreadable for the check. The gate is skipped after an interrupt or a prerequisite failure | both | `cli/_helpers.py`, `metrics/verdict.py` |
| `run --generate` or `generate` over a non-empty datagen prefix refused (exit 3) unless `--regenerate`, `--skip-generate` or `--allow-stale-bronze` applies ([3](generation.md#3-data-generation)); an unreadable bronze bucket exits 4 | batch | `cli/_helpers.py`, `deploy/datagen.py` |
| Datagen exceeds its wait: exit 1 after deleting the datagen Job and any stream apps; the verdict's reasons say datagen timed out. A failed datagen in any cycle fails the run | batch | `cli/_run.py` |
| bronze-verify: schema, a key column NULL in every row, rows > 0, rows surviving the filter > 0 | batch | `bronze_verify.py` |
| silver-build: rows written > 0, known row count, data clock set, no unintended full rebuild | batch | `silver_build*.py` |
| gold-finalize: silver exists and is non-empty; gold rows = distinct silver `interaction_date` count (Iceberg and Delta, every strategy) | batch | `gold_finalize*.py`, `common.py` |
| Any stage exit code other than success | both | `cli/_run.py`, `cli/_sustained.py` |
| Correctness gate: a `GATING_CHECKS` check that fails, does not run or is absent, or a record with no expected-result facts ([5.2](#52-expected-result-checks-batch-only)) | batch | `metrics/c360_correctness.py`, `cli/_run.py` |
| Benchmark gate: every query succeeds and returns a row (no Customer 360 query is `allow_empty`; the known-failure list is empty) | batch | `cli/_run.py` |
| In-stream rounds: a failed query other than Q9 fails the run in any round; an empty result fails only in the last round. A failed Q9 after its retries is reported and tolerated in every round, the last included; an empty Q9 is tolerated except in the last round | continuous | `cli/_sustained.py` |
| Result check, when it runs: every query succeeds and is non-empty (skipped or unable to run: recorded as not checked) | continuous | `cli/_sustained.py` |
| The benchmark runner for rounds and the result check cannot be created: exit 1 before any stream starts | continuous | `cli/_sustained.py` |
| With gold on an interval, `run_duration` under three gold refresh intervals refused (exit 2) | continuous | `cli/_sustained.py` |
| Continuous window gate ([5.3](#53-continuous-window-gate)) | continuous | `metrics/continuous_window.py` |
| Balance gate: a handoff's lag rose by more than one cadence over the second half | continuous | `metrics/continuous_window.py`, `cli/_sustained.py` |
| A stream not RUNNING at window close, or restarted or resubmitted inside it | continuous | `cli/_sustained.py` |
| bronze-ingest or silver-stream processed 0 rows, or has no parseable log | continuous | `cli/_sustained.py` |
| A failing gate exits non-zero; metrics are still written with the failed verdict | both | `cli/_sustained.py`, `cli/_run.py` |

### 5.2 Expected-result checks (batch only)

After gold-finalize the CLI evaluates 26 pipeline checks from the
`[c360-bronze]` and `[c360-check]` facts, and after the benchmark 8 query
row-count checks, into `metrics.json` `c360_correctness`. Its status is
`fail` when any check failed, else `pass` when the five core checks ran and
passed, else `unknown`. Continuous, `--local` and `--stage` runs do not
evaluate them, so their verdict carries no `c360` gate.

Sixteen checks gate the run (`GATING_CHECKS` in
`metrics/c360_correctness.py`): every invariant and reconcile check and
`avg_transaction_value_overall`.

- A gated check that fails, does not run or is absent fails the run. So does
  a record with no expected-result facts.
- A failed run has a non-zero exit, `metrics.success` false and the
  verdict's `c360` gate failed; the report shows the run as failed.
- A failure among the other 18 (remaining statistical checks and the
  benchmark row counts) is listed under the verdict's
  `c360_failed_not_gating` qualifier. It changes neither exit code nor
  verdict.

Kinds and tolerances:

- invariant: exact, gated;
- reconcile: exact, gated; "core" marks a core check;
- statistical: 6 standard errors (8 or 10 on the right tail of amounts);
  only `avg_transaction_value_overall` is gated;
- shape: the benchmark row counts.

| Check id | Kind | Asserts |
|---|---|---|
| `silver_duplicate_filter_applied` | invariant | no `duplicate_suspected` row in silver |
| `amount_only_on_purchases` | invariant | amount > 0 only on purchases, and transactions = purchases |
| `purchase_amount_range` | invariant | purchase amount in [1, 9999.99] |
| `silver_no_null_keys` | invariant | no NULL customer, date or amount |
| `customer_ids_in_id_space` | invariant | customer ids in [1, id space] |
| `dates_in_window` | invariant | dates in [window start, window end - 1 day] |
| `one_ticket_and_score_per_support` | invariant | ticket rows = support rows = satisfaction rows |
| `gold_counts_non_negative` | invariant | gold counts never negative or NULL |
| `gold_daily_identities` | invariant | per-day gold identities, listed below |
| `daily_active_within_customers` | invariant | max daily active customers <= id space |
| `bronze_rows_match_datagen` | reconcile (core) | bronze rows = datagen sizing rows for some codec |
| `bronze_to_silver_rows` | reconcile (core) | silver rows = bronze rows passing the filter |
| `silver_to_gold_days` | reconcile (core) | gold rows = gold distinct dates = silver distinct dates, no NULL date; the row-count part also gates in gold-finalize ([5.1](#51-checks-that-gate-a-run-fail-it)) |
| `silver_to_gold_counts` | reconcile (core) | summed gold counts equal silver counts for nine KPIs |
| `silver_to_gold_revenue` | reconcile | summed gold revenue equals silver revenue within 0.005 per gold row |
| `duplicate_filter_share` | statistical | duplicate filter share 0.02 |
| `interaction_mix` | statistical | interaction mix |
| `avg_transaction_value_overall` (gated) | statistical | overall average transaction value |
| `avg_transaction_value_daily` | statistical | per-day average transaction value (days with >= 200 transactions) |
| `avg_page_views_per_visit` | statistical | average page views per visit |
| `avg_time_on_site_per_visit` | statistical | average time on site per visit |
| `avg_satisfaction_score` | statistical | average satisfaction 3.0 |
| `high_churn_share` | statistical | high churn share 0.4 of support rows |
| `medium_churn_share` | statistical | medium churn share 0.2 of support rows |
| `distinct_customers` | statistical | distinct customers against the Zipf model (plus 3%) |
| `gold_days_cover_window` | statistical | every window day present when there are at least 30 sessions a day |
| `benchmark_rows_Q1` to `_Q7`, `benchmark_rows_Q9` | shape | Q1 1; Q7 5; Q3 12 (when >= 5,000 transactions); Q4 6 (when >= 5,000 support rows); Q5 min(90, gold days); Q9 min(30, gold days); Q2 5 x days in the first three months (when every type appears daily); Q6 1 to 6 |

`gold_daily_identities` asserts, per day:

- avg transaction value = revenue / transactions, within 0.011;
- avg estimated LTV = total LTV / transactions, within 0.011;
- both averages NULL exactly on days with no transactions;
- transactions = conversions; tickets = support;
- channel revenue <= total; churn <= support;
- largest transaction in range; LTV >= revenue.

### 5.3 Continuous window gate

A continuous run fails unless, inside the window:

- bronze ingested rows, wrote at least 2 batches, and wrote its last batch at
  or after 50% of the window (a corpus drained before the window opened fails
  explicitly);
- silver committed at least 2 micro-batches with rows after bronze's first
  write in the window;
- gold refreshed on new silver data at least 2 times after that write (a
  cycle tagged `(silver idle)`, reading no silver, or reading no more silver
  rows than the previous cycle does not count);
- gold freshness was measured;
- every stream's driver log has timestamped stage lines;
- the pipeline was balanced: no handoff's lag rose by more than one cadence
  across the window's second half (fails with "balance gate: ..." and a
  bottleneck line; see [Benchmarking](../../benchmarking.md)).

### 5.4 Result equivalence

- Batch: after the scored round's timed samples, each successful query runs
  once more, untimed, and its result is fingerprinted (spec `rf2`,
  `benchmark/fingerprint.py`):
  - order-independent row hashes;
  - approximate DOUBLE-derived columns compared as plain and row-weighted
    sums, within a tolerance scaled from each column's declared quantum and
    the row count;
  - timestamps normalised to UTC.
- Continuous: the fingerprints of the post-settle result check.
- In-stream rounds and the pre-maintenance round are never fingerprinted.

Fingerprints are compared only between runs of the same corpus. Two runs are
not comparable when (`metrics/comparability.py`):

- they were recorded under different identity versions;
- their corpus id differs (v2 on exp2 records; corpus id and generator image
  on exp1);
- seed, scale or corpus role differ;
- their generator image digests differ where both observed one.

A tester therefore makes a fresh reference run on the same generator image and seed
before comparing.
