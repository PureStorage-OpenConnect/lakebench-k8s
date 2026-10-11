# AML benchmark: pipeline

See also: rules in [4.2](rules.md#42-detection-rules).

## 4.1 Batch mode

`lakebench run` executes, in order (`cli/_run.py`):

| # | Stage | Input | Transformation | Output |
|---|---|---|---|---|
| 1 | datagen, only with `--generate` | seed, scale | [3](generation.md) | raw corpus |
| 2 | bronze-verify | `pacs008/` Parquet, manifest | schema and null checks; zero-copy Iceberg registration (`add_files`) of the files; CTAS of the manifest as `bronze.manifest` (missing manifest: a warning); writes the bronze data clock (max settlement date) | bronze table, `bronze.manifest` |
| 3 | silver-build | bronze, party, account | full rebuild each cycle, listed below; then a sealed marker | six silver tables plus `silver_batch_versions` |
| 4 | gold-finalize | sealed silver | baseline dashboard; nine detection rules; the W1 and W4 projections (`gold.entity_clusters`, `gold.risk_scores`, best effort); TM operations | gold tables ([2.4](data-model.md#24-gold-tables)) |
| 5 | score-financial | `gold.alerts`, `gold.detection_status`, manifest | recall and false-positive scoring ([8.4](scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails)) | `recall.parquet`, `recall.json` |
| 6 | AML batch gate, TM verdict | driver logs, `recall.json` | [5](correctness.md#5-correctness-contract) | pass or fail |
| 7 | pre-compaction benchmark (scale below 50, maintenance on) | silver, gold | warm-up pass, then FQ1 to FQ8, not fingerprinted | `pre_compaction_qph` |
| 8 | table maintenance | maintained tables | expire snapshots and orphan removal, compaction on silver and gold only (never bronze), under policy `m2-2026-09-26` and one shared 1,800 s budget; DuckDB recipes run none | `maintenance_outcomes` |
| 9 | settle wait (skipped when no maintenance statement ran), then the scored benchmark | silver, gold | 12 queries when TM operations gave a `pass` or `fail` verdict, else FQ1 to FQ8; power mode, hot, `benchmark.iterations` samples per query | QpH, fingerprints |

- Bronze-verify falls back from `add_files` to CTAS (doubles bronze storage)
  above 1.5 TiB or 800,000 files, or when `add_files` fails.
- Silver-build steps:
  - payments to USD at fixed FX rates;
  - deterministic entity ids (a hash of name, country, city and LEI; no
    fuzzy matching);
  - one account per IBAN;
  - double-entry statements with running balances;
  - counterparty pair aggregates;
  - per-entity behavioural profiles.
- Stages 5 and 6 run only on a full run (no `--stage`) whose three pipeline
  stages succeeded.
- The scorer refuses a `gold.detection_status` written by another run, so a
  run with stale gold carries no recall.

**Silver business rules** (they affect every downstream figure):

- profiles are computed over the whole corpus and store the Welford M2 term,
  so the continuous stream can merge later batches with the parallel Welford
  recurrence;
- `txn_type` is always `wire`;
- `cross_border` is NULL when either country is unknown;
- `regulatory_reported` is true when the message carries regulatory
  reporting;
- statement opening balances are seeded deterministically from the account
  id;
- `current_balance` is the last statement entry's balance, NULL for an
  account with no statements.

**Corpus source.** Without `--generate` the run measures whatever corpus is
in bronze.

- Its corpus parameters come from the datagen fleet record an earlier
  `lakebench generate` left for the namespace.
- With none, seed and image are recorded as declared by the config
  (`experiment.corpus.observed: false`, with `observed_note`).
- Every run reads the generator's per-node markers under the bronze prefix
  at run end. It records corpus id v2 and the generator lineage
  (`metrics/corpus_identity.py`).
- A publishable run generates its corpus in the same run.

**Generate refusals.**

- A non-empty datagen prefix with `--generate` and without `--regenerate`:
  exit 3 before datagen (exit 4 when the bucket cannot be listed).
- `--regenerate` clears only the datagen prefix of the bronze bucket,
  aborting incomplete multipart uploads, and only when this deployment
  created the bucket.
- On any other bucket the run refuses unless `--allow-stale-bronze` is given.
  Datagen then writes over the objects, and the record carries
  `datagen_stale_bronze` and a verdict warning.
- Before generating, the run deletes any earlier datagen Job: exit 3 if its
  pods still run five minutes later.
- Datagen past its wait budget (the per-job timeout) stops the datagen Job
  and fails the run with exit 1; `verdict.reasons` says "datagen timed out".

**Stage invariants.**

- Silver-build:
  - every pre-staged frame has a row before any write;
  - statements and accounts are non-empty after writing (else
    `silver.transactions` is truncated and the stage fails);
  - account rows equal distinct IBANs; silver output rows > 0;
  - it refuses to run while a continuous stream's `_STARTED` marker exists,
    unless `--force-rebuild` (batch only).
- Bronze-verify fails the stage on a missing required column (driver exit 2)
  or any NULL settlement date (driver exit 3). It only warns on NULL `uetr`,
  `txn_id` or `msg_id`. It has no zero-row or duplicate check of its own (the
  record gates in [5](correctness.md#5-correctness-contract) cover zero
  rows).

**Multi-cycle batch** (`cycles > 1`):

- The run generates every cycle itself and refuses `--generate` and
  `--repeat` with exit 2.
- Before cycle 0 it clears an owned datagen prefix as `--regenerate` would.
- Silver rebuilds from cumulative bronze.
- Gold re-detects over the whole corpus with run id `<run_id>-cN` (a
  single-cycle run uses `<run_id>-c1`), deleting other cycles' alerts.
- The last cycle's gold is the result; the TM verdict covers every cycle.

## 4.3 Continuous mode

`lakebench run` with `pipeline.mode: continuous` (`cli/_sustained.py`):

1. **Window checks.** Refuses (exit 2):
   - a window shorter than three gold refresh intervals when gold is on an
     interval;
   - an explicit `retention_interval` or `compaction_interval` that cannot
     fire inside the window, unless maintenance is disabled.

   With its own datagen, bronze has no per-trigger limit. Under
   `--skip-generate` it resolves `max_files_per_trigger` (files per
   micro-batch):
   - unset, it is int(files x trigger seconds / (1.2 x `run_duration`)),
     clamped to 1 to 50 (a Lakebench cap);
   - so arrival lasts about 1.2 x `run_duration`;
   - refused when the corpus would run out first;
   - recorded in `continuous.trickle` and
     `experiment.limits.max_files_per_trigger`.
2. **Reset.** Stops leftover streams, deletes stream checkpoints, and clears
   the raw datagen prefix unless `--skip-generate`.
3. **Schema preflight** (bronze-verify in `schema` mode, before datagen):
   - drops bronze without PURGE (registered datagen files survive) and
     recreates it empty from `pacs008_schema.json` plus `ingest_ts`;
   - drops all eight silver tables (the seven batch ones and
     `silver.counterparty_pairs`);
   - drops and re-registers `bronze.manifest`;
   - drops the TM operations tables and the gold tables `alerts`,
     `risk_scores`, `entity_clusters`, `daily_dashboards` and
     `detection_status`.

   Bronze-ingest exits 2 if datagen's first file does not match that schema.
4. **Streams**, then datagen once they run. The window (`run_duration`,
   default 1,800 s) opens at datagen's first data file, with a warning if
   none arrives within 300 s.
   - **bronze-ingest**: Structured Streaming over the Parquet prefix; no
     per-trigger limit unless `max_files_per_trigger` is set; a micro-batch
     as soon as the last finishes (`bronze_trigger_interval`, default 0 s);
     appends with `ingest_ts`.
   - **silver-stream**: Iceberg streaming read of bronze, `foreachBatch`
     back to back (`silver_trigger_interval`, default 0 s). Per micro-batch,
     in order: transactions (idempotent delete-then-append on replay), edges
     (per-batch aggregates; readers must SUM), dimension MERGE with KYC,
     statements and balances, profile MERGE with the Welford recurrence,
     then the sealed marker.
   - Every MERGE reads a source materialised from a local checkpoint
     (`spark/scripts/common.py`, `materialised_source`). On Spark 4.1 with
     Iceberg, a MERGE whose source is a temp view over an Iceberg table fails
     inside Spark.
   - **gold-refresh**: a loop of [ticks](../../glossary.md#tick) (detection passes), back to back by
     default (`gold_refresh_interval` `0 seconds`). Each tick:
     - refreshes the catalog's view of silver and pins one sealed silver
       snapshot;
     - brings W4, W2, W5, W6, W17 and W3 up to date with it
       (`config/support.py`, `AML_CONTINUOUS_RULES`), re-detecting only what
       rows new since each rule's last pass can change;
     - refreshes the baseline dashboard rows and logs freshness and time to
       detect;
     - runs a TM pass when due (every `continuous_interval_seconds`, default
       1,800 s), with a final pass before the window closes.
   - Every rule runs every tick. An alert keeps the `detected_ts` of the
     tick that first wrote its content.
   - W5 is the transaction screen only: a payment is screened on arrival. A
     later list version or party country change does not rescreen it.
   - W1, W7 and W8 are written to `gold.detection_status` as `skipped` with
     reason `mode-excluded`. They are not in `experiment.rules.skipped`;
     `experiment.support.mode_note` names them.
   - After five consecutive failed ticks (or baseline refreshes) the driver
     exits 1. The operator restarts it under the streams' restart policy.
5. **In-stream benchmark rounds**: power mode, hot, one sample per query, not
   fingerprinted.
   - With TM operations on, each round first probes `gold.cases` for a case
     of this run's `base_run_id` (untimed, 60 s timeout).
   - Once a case exists the round runs all twelve queries. Before that it
     runs FQ1 to FQ8, labelled `investigator_queries`: `included`,
     `absent_no_cases` or `probe_failed`.
   - With `architecture.benchmark.investigator_sessions` set, one extra round
     of concurrent investigator sessions runs after the first round that ran
     the investigator queries. It is not a benchmark round
     ([8.5](tm-operations.md#85-tm-operations)).
6. **In-window maintenance**: expire snapshots and orphan removal every
   `retention_interval` (default `run_duration / 3`, within 300 to 7,200 s);
   compaction every `compaction_interval` (default twice that). While streams
   run:
   - AML compacts silver only;
   - Iceberg expiry is floored at 1 h;
   - orphan removal never uses less than 24 h 10 min.

   The applied retention is in `continuous.retention`.
7. **[Drain](../../glossary.md#drain) and score** ([4.5](#45-continuous-drain-and-tick-records)).
   - If the run passed every gate, `score-financial` runs in covered mode over
     exactly the snapshots the last completed tick read
     ([8.4](scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails)).
   - On the same condition `time-travel-financial` then re-reads every
     `silver.transactions` snapshot the ticks recorded
     ([8.2](continuous-metrics.md#82-continuous)). It runs with the streams
     stopped, outside every measured interval, and never fails the run.
8. **Gates**: the continuous window gate, the drain check, the AML continuous
   gate, the TM verdict and the record gates ([5](correctness.md#5-correctness-contract)).

The end-of-run result check does not run for AML continuous; the record says
why in `experiment.results.not_checked`. No published record carries a
covered score yet; the published continuous record predates the drain.

## 4.4 Out-of-run AML commands

None of these is part of `lakebench run` or its verdict (`cli/_financial.py`).

**`lakebench financial score --manifest <uri> --output <uri>`** runs the
recall scorer outside a run. With no run id it scores the run
`gold.detection_status` names.

**`lakebench financial replay --rule <id>`** re-runs one rule against the
`silver.transactions` snapshot committed at least `--depth-months` (default
60) before now.

- Depth is measured on snapshot commit time, not settlement dates. A
  deployment younger than the depth has no snapshot, and the job exits
  non-zero.
- It writes to the gold alerts table name with an `_replay` suffix
  (`--output-alerts` to change it), replacing only that rule's rows. The
  batch run's `gold.alerts` is never touched.
- The historical-replay scenario asserts against a 60-month replay's
  wall-clock budget.
- `--rule` takes a detection rule id such as `W2_structuring`. The replay
  scenario is not a rule; `W8` there would mean rule
  `W8_dormant_reactivation`.

**`lakebench financial reproduce CONFIG --alert-id <id>`** (`--run RUN_ID`)
reruns one batch alert's rule on exactly what that run's gold-finalize read
(`spark/scripts/reproduce_financial.py`).

- Gold-finalize logs the snapshot of `silver.transactions`,
  `silver.entities` and `silver.silver_batch_versions` it reads.
- Before maintenance the batch scorer fingerprints every column of each
  (`financial_scoring.read_snapshots`: table, snapshot, `total_records`,
  `rows`, `fp`, `cols_sha`), reading all three tables once per run.
- It reads the run record (`--run`, or the deployment's latest AML batch
  record on this host; exit 2 when there is none).
- Before any cluster call it refuses a record from a protected corpus
  (exit 2) or one with no read snapshots (exit 4: it predates 1.7, or was not
  scored, like a `run --stage` subset).
- It reads each table at its recorded snapshot. Once that expired, it reads
  the current table when its fingerprint is the same (content and batch
  stamping equal: `basis: equivalent`).
- It filters transactions to the batches the versions table had sealed when
  gold read it. It runs the rule with gold's parameters over the whole silver
  snapshot. It matches the alert on (rule, entity, `alert_ts`) and the set of
  related transactions.

| Exit | Meaning |
|---|---|
| 0 | reproduced |
| 1 | not reproduced (no match, several, a different set, or the rule declined to run) |
| 1 | the alert is not in `gold.alerts` for that run (a rule version other than the running code's included) |
| 4 | a snapshot is gone and the content changed |

The result is written to `scoring/reproduce/<alert_id>/result.json`. What is
not pinned is in [12](limitations.md#12-known-limitations). Continuous alerts
are not reproduced.

**`lakebench financial reference-score CONFIG --manifest ... --output-prefix ...`**
runs the reference detector, the band-leakage report and the fidelity gate
([8.8](scoring.md#88-band-leakage-report-and-reference-detector)).

- It installs hash-checked scikit-learn wheels from the deployment's
  dependency set in an init container.
- `--leakage-threshold` defaults to 0.10.
- On a deployment that declares `corpus_role: evaluation` or `robustness`
  its driver refuses after submission, so the command exits 1.
- Its features come from silver read without the sealed-batch filter
  ([12](limitations.md#12-known-limitations)).

## 4.5 Continuous drain and tick records

**The drain.** A continuous run ends its window by draining gold-refresh, not
by deleting it mid-tick (`cli/_aml_post.py`).

- The CLI writes `<checkpoint_base>/gold-refresh/_lb_stop` in the gold
  bucket, body the run id.
- The driver finishes its tick, logs `Drain complete: last completed cycle
  N`, frees its executors and waits until the streams stop. The CLI waits up
  to 1,800 s for that line (a Lakebench cap).
- The run fails when the drain times out (`gold drain timed out; last tick
  interrupted`), when gold-refresh is deleted before it drains, or when the
  driver restarted and found the marker before its first tick. In each case
  `gold.alerts` may be half rewritten.
- A marker that cannot be written does not fail the run; its recall reads
  `not_scored`.
- `lakebench stop` drains the same way with a 300 s budget. It stops the jobs
  whether or not the drain is confirmed, Ctrl-C included.

**Tick records.** Every tick logs the snapshots it read and wrote:

- `silver.transactions`, `silver.entities`, `silver.accounts` and
  `silver_batch_versions` when it pinned silver;
- `gold.alerts` and `gold.detection_status` once detection committed.

Detection filters the pinned transactions through the versions table at the
logged snapshot, so the scorer sees exactly the sealed batches detection saw.

- The record keeps them as `continuous.ticks[]`, with `continuous.drain` and
  `continuous.ticks_unpinned` (ticks whose transactions or versions snapshot
  was not pinned; detection then read the current versions table).
- `continuous.ticks[].rule_status` holds each rule's status (`ran`,
  `skipped:<reason>` or `error`). When scoring does not run, the rules gate
  judges the rule status of the last tick completed at the drain.
- Records come from the current gold-refresh driver pod's log, so a restarted
  driver leaves its earlier pod's ticks out (`continuous.drain.ticks_scope`).
- Lakebench keeps each stream driver's log from the moment its pod runs
  (`<run_dir>/drivers/`). It reads that copy, continued by the pod log, so
  kubelet log rotation does not trim a long window.
- `continuous.drain.log_from_driver_start` is false when the log still does
  not reach the driver's first tick. The run then fails with "driver log
  incomplete".
- The drained driver repeats its last tick's records with each drain line,
  so a log rotated during that tick still yields the snapshots the scorer
  reads.
