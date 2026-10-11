# Customer 360 benchmark: pipeline

## 4. Pipeline

Unsuffixed stage scripts are the Iceberg adapters, `*_delta.py` the Delta
adapters; `bronze_verify.py` serves both. Every Spark session is pinned to
UTC before any date is derived.

### 4.1 Batch mode (`pipeline.mode: batch`)

`lakebench run` runs the stages in order, each as one SparkApplication; a
failed stage stops the run. Single-cycle: datagen runs first only with
`--generate`. With `cycles > 1`, datagen runs before every cycle unless
`--skip-generate` reuses a finished multi-cycle corpus, and `--generate` is
refused before any cluster call (exit 2, `cli/_run_args.py:RUN_RULES`).

1. **bronze-verify** (no bronze table is written in batch). Input: every
   landing Parquet file of this run's cycles. Checks:
   - 23 required columns with the right type families;
   - 9 pass-through columns (warn only);
   - row count > 0, and at least one row surviving the silver filter;
   - no key column (`event_timestamp`, `customer_id`, `interaction_type`,
     `channel`, `data_quality_flag`) NULL in every row (NULL in only some
     rows is a warning).

   Any failure exits 1. Outputs: the `[c360-bronze] rows=N
   silver_filter_rows=M` fact line, and `max(event_timestamp) + 1 day` in the
   `lakebench-silver-state` ConfigMap as the bronze data clock.
2. **silver-build**. Input: the same landing files (cycle-scoped globs in a
   multi-cycle run).
   - Drops rows flagged `duplicate_suspected` (and NULL flags), then derives
     the 32 columns of [2.2](data-model.md#22-silver-columns).
   - `customer_recency_score` is anchored to the data clock `LB_DATA_CLOCK`,
     the day before the clock value. Multi-cycle: every cycle takes the
     exclusive end of the range its cycles cover (`data_clock_source`
     `cycle_series_end`). Otherwise: `timestamp_end`, bronze-verify's
     ConfigMap value, `timestamp_start`, today at 00:00 UTC.
   - Strategy: `simple` below 100 GB of bronze, `streaming` at or above
     (single pass, hash distribution by partition); `salted` is refused.
   - Output: `createOrReplace` on cycle 0; later cycles append after deleting
     that cycle's `_batch_id` rows.
   - An existing table whose columns differ (such as one a continuous run
     wrote) is first dropped from the catalog. On Hive and Polaris its files
     stay in the bucket. A write that then fails leaves no table, which the
     next run builds again.
   - Refuses a full rebuild over a populated silver table unless
     `--force-rebuild`; refuses an unset data clock; fails when rows written
     is 0 or unreadable from the snapshot summary.
3. **gold-finalize**. Input: the whole silver table, grouped by
   `interaction_date` with the 30 KPIs.
   - Strategy: `simple_agg` below 500 GB of silver, `two_phase_agg` above;
     cycles 2+ force `incremental` (recompute from the latest gold date
     inclusive and rewrite gold in one commit).
   - Output: gold, `createOrReplace`, coalesced to one file. Exits 1 when
     silver is missing or empty.
   - Non-degeneracy gate: counts gold rows and distinct silver
     `interaction_date` values (two full reads) and exits 1 when they
     differ, on Iceberg and Delta, under every strategy.
   - Then it logs the `[c360-check]` fact line (one silver aggregation pass
     and a gold collect, reporting only). Only that check's seconds are
     subtracted from the stage's time, so the gate's reads count toward the
     stage and time to value ([8.1](metrics.md#81-batch)).
4. **Pre-benchmark maintenance and benchmark**
   ([7.2](execution-rules.md#72-maintenance-policy-handling),
   [6](queries.md#6-query-set)): an optional pre-maintenance round, table
   maintenance, a storage settle wait, and the scored round with result
   fingerprints.

### 4.2 Continuous mode (`pipeline.mode: continuous` or `run --continuous`)

1. **Preflight.** Resolves the maintenance schedule and, under
   `--skip-generate`, the [trickle](../../glossary.md#trickle) (`max_files_per_trigger`, files per
   micro-batch). Refuses the run when:
   - a trickle would stop arriving before the window ends;
   - an explicit maintenance or compaction interval cannot fire inside the
     window;
   - with gold on an interval, `run_duration < 3 x gold_refresh_interval`;
   - it would drop existing Customer 360 state (tables, checkpoints, or raw
     data larger than this run regenerates) without `--force-reset`.

   It stops leftover streams, deletes stream checkpoints, and clears raw data
   unless `--skip-generate`.
2. **Datagen** generates successive time slices for the whole window. It
   starts once the three streams run; every pod writes until the run writes
   the `_corpus/stop` marker at the window's end.
3. **Continuous reset.** bronze-verify with `LB_CONTINUOUS_RESET=1` drops
   `bronze_raw`, silver and gold and their owned directories; it verifies
   nothing and writes no data clock.
4. **Three concurrent streams**, each a SparkApplication, back to back by
   default (trigger intervals `0 seconds`):
   - **bronze-ingest** (`bronze_trigger_interval`): Structured Streaming file
     source over the landing prefix, no per-trigger limit unless
     `max_files_per_trigger` is set. Each row keeps its file's landing time as
     `ingest_ts` (moved onto the cluster clock). Appends to `bronze_raw`
     exactly once per micro-batch (Iceberg snapshot properties or Delta
     txnAppId/txnVersion). Waits up to 1,800 s for the first file. Refuses a
     fresh checkpoint over a non-empty table.
   - **silver-stream** (`silver_trigger_interval`): incremental read of
     `bronze_raw`, the batch silver transformation per micro-batch, carrying
     `ingest_ts`; exactly-once append keyed by (`_stream_id`, `_batch_id`).
     Waits up to `silver_bronze_wait_seconds` (default `run_duration / 4`, at
     least 600 s) for bronze. Refuses resume when bronze's snapshot lineage
     changed under its checkpoint. Fails at stop when it wrote 0 rows.
   - **gold-refresh** (`gold_refresh_interval`): streams the silver table.
     Each micro-batch is the silver commits since gold's checkpoint, read at
     one pinned silver snapshot. It recomputes the 30 KPIs for every date the
     new rows touch, from all silver rows on those dates, and replaces those
     dates in gold. It then logs data freshness ([8.2](metrics.md#82-continuous)).
     A silver read error gets 5 attempts (4 retries). The batch
     non-degeneracy gate does not run here.
5. **Measurement window.** Opens at datagen's first data file (a warning if
   none arrives within 300 s) and closes `run_duration` seconds later
   (default 1,800). Inside it: in-stream benchmark rounds
   ([6](queries.md#6-query-set)) and table maintenance rounds
   ([7.2](execution-rules.md#72-maintenance-policy-handling)).
6. **Gates** ([5.3](correctness.md#53-continuous-window-gate)), then **settle
   and result check**, none of it scored.
   - The CLI estimates how long the rest of the corpus needs to reach gold.
     The estimate is remaining rows at the rate bronze held, plus two silver
     cycles, two gold refreshes and 60 s. A silver cycle is its trigger, or
     back to back its median batch time, at least 120 s.
   - Above 1,800 s the result check is skipped with no wait, and the run
     records `results.not_checked`.
   - Otherwise the streams run until every datagen row is in bronze, silver
     has committed all of them and a gold refresh has read silver after its
     last commit. Then the streams stop, the query set runs once and every
     result is fingerprinted.
