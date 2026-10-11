# Customer 360 benchmark: execution rules

## 7. Execution rules

### 7.1 Required

- **Batch**: all three stages in order, then the benchmark unless the recipe
  has no query engine. `--stage` runs one stage and skips the correctness
  evaluation, so it is not a benchmark run.
- **Continuous**: all three streams for the full window, the gates and the
  result check. A run without the result check (`--skip-benchmark`, no query
  engine, a corpus that did not settle or was too large to settle within
  1,800 s) cannot be shown comparable.
- **Iterations**: batch timed samples per query = `benchmark.iterations`
  (default 3, range 1 to 100), scored by the per-query median. In-stream
  rounds take 1 sample per query whatever the config says; the result check
  takes 1.
- **Timeouts**: per stage `--timeout`, else `max(3600, scale x 120)` s.
  Benchmark query 300 s. The phase-3 datagen wait uses the stage timeout
  (per cycle in a multi-cycle run); exceeding it deletes the datagen Job and
  the stream apps and exits 1, and a cycle that exceeds it fails the run.
  Stream start deadline 1,800 s.
- **Repeat batch run** on one deployment: needs `--force-rebuild` (silver
  refuses to overwrite a populated table). With `--generate` it also needs
  `--regenerate`, which works only on a bronze bucket this deployment can
  prove it owns. On any other bucket `--regenerate` is refused, and `--allow-stale-bronze` generates over the
  objects and records that ([3](generation.md#3-data-generation)). Use
  `--skip-generate` to reuse the corpus.

### 7.2 Maintenance policy handling

Maintenance is an execution condition. Policy id `m2-2026-09-26`;
`--skip-maintenance` stamps `m2-2026-09-26+skipped`. What ran per operation is
recorded in `experiment.effective_maintenance` with classes `ran`,
`ran_no_effect`, `not_supported`, `skipped_by_user`, `failed`, `not_run`.

| Composition | Batch pre-benchmark | Continuous in-window |
|---|---|---|
| Iceberg + Trino | expire_snapshots at 0 s and remove_orphan_files at 24 h 10 min on silver and gold; compaction on silver and gold as `ALTER TABLE ... EXECUTE optimize(file_size_threshold => '128MB')`, split into statements of at most 90 `interaction_date` partitions when silver holds more | expire at max(retention_threshold, 1 h) and orphan removal at 24 h 10 min on bronze_raw, silver, gold every `retention_interval` (auto `run_duration / 3`, 300 to 7,200 s); the same compaction on silver and gold every `compaction_interval` (auto 2 x retention) |
| Iceberg + Spark Thrift | expire and orphan removal as for Trino; compaction as `CALL <catalog>.system.rewrite_data_files(table => ...)` with Iceberg's default file selection | as for Trino, with the Thrift compaction |
| Delta + Trino | VACUUM at the resolved retention; OPTIMIZE never runs (`not_supported`) | VACUUM at Delta's 7-day default while streams are live (`ran_no_effect`); no OPTIMIZE. No effective maintenance; the record says so in `known_limitations` |
| Delta + Spark Thrift | VACUUM and OPTIMIZE skipped (`not_supported`) | same |
| DuckDB (Iceberg) | none (`not_supported`); the settle wait is skipped because no statement ran | none (`not_supported`) |
| No query engine | no benchmark, no maintenance | no rounds, result check or maintenance |

Both Iceberg compaction statements aim at the table's 128 MB
`write.target-file-size-bytes` but select files differently. The record names
the operation (`compaction=ran(trino_optimize:128MB)` against
`compaction=ran(iceberg_rewrite_data_files)`), and the compaction operation is
a Conditions key, so a Trino-vs-Thrift pair that both compacted is not
like-for-like (`metrics/maintenance_policy.py`,
[10](comparability.md#10-comparability)).

**Batch sequence** (with a query engine, and none of `--skip-benchmark`,
`--skip-maintenance` or `pre_benchmark_maintenance: false`):

1. file count probe;
2. a pre-maintenance round (warm-up pass, then timed, not fingerprinted), at
   scale below 50 only;
3. maintenance and compaction under one 1,800 s budget; the first statement
   timeout or the deadline stops the rest and records `maintenance_stopped`;
4. query-engine readiness wait;
5. **storage settle wait** (`benchmark/settle.py`): one scan query every 60 s
   until two consecutive probes agree within 10% and neither is slower than
   the pre-maintenance median by more than that bound.
   - The bound widens to the probe query's own spread (twice its median
     absolute deviation, at most 20%) when the pre round timed it three or
     more times.
   - With no pre-maintenance time (scale 50 and above) the wait needs three
     agreeing probes and is recorded as unverified.
   - Three failed probes in a row end it.
   - At most 2,700 s (configurable in `benchmark.maintenance_settle`);
6. the scored round, preceded by a warm-up pass only when a pre-maintenance
   round was measured. At scale 50 and above, with `--skip-maintenance`, or
   with `pre_benchmark_maintenance: false` it starts cold.

- The settle wait is outside every stage and time to value. A wait that does
  not settle leaves `maintenance_value_pct` null.
- When stream apps are present or unreadable in the namespace, batch
  pre-benchmark maintenance switches to live-stream retention and records it.
- Continuous statements get `min(600 s, interval / 2)` each; a round is
  skipped when too little window is left. When the configured engine's pod is
  missing, maintenance falls back to the other engine; when tables or rounds
  then compacted with different statements, the record names both as
  `mixed(...)`.
- Batch Customer 360 has no bronze table, so batch maintenance never targets
  `bronze_raw`.

### 7.3 Permitted tuning (still publishable)

None of these changes the Workload or Corpus identity keys, so runs differing
only here stay comparable. State them beside any published comparison.

| Difference | Identity effect |
|---|---|
| Component or version | Architecture (with a System difference too, no difference can be put down to either) |
| Cluster | System |
| Explicit Spark executor overrides, driver overrides, user Spark conf | Architecture keys when set; an executor override below the profile's ask also enters as bound kind `*: executor override`, a condition |
| Trino, Thrift or DuckDB sizing, storage classes, datagen execution, continuous trigger intervals | not in the identity: equal identities, unless a larger `gold_refresh_interval` raises the floor of the benchmark warmup and interval and changes the in-stream round count (a condition) |
| Conditions of [10](comparability.md#10-comparability), the dependency pinset | not like-for-like |

What may be tuned:

- Architecture among the supported compositions
  ([11](../C360.md#11-supported-compositions)); component versions
  within `_SUPPORTED_SPARK_VERSIONS` (3.5, 4.0, 4.1) and
  `_FORMAT_VERSION_COMPAT`. Spark 3.5 is accepted (Iceberg only; Delta needs
  Spark 4.x) but in no release-matrix row, so its runs are unverified.
- System sizing: Spark per-job executor counts
  (`platform.compute.spark.*_executors`), driver cores and memory, Spark conf
  overrides, query-engine sizing (Trino workers and memory, Thrift, DuckDB),
  storage classes. Per-executor profiles are fixed by `_JOB_PROFILES`.
- Datagen execution: `parallelism`, `cpu`, `memory`, `generators`,
  `datagen.mode` ([3](generation.md#3-data-generation)).
- Continuous:
  - trigger intervals;
  - `max_files_per_trigger` (within the arrival rule; an explicit value has
    no upper bound; in no identity key);
  - maintenance intervals and retention threshold (Conditions key
    "maintenance settings": not like-for-like);
  - benchmark warmup and interval (both floored at `gold_refresh_interval`)
    and `run_duration`. They are not like-for-like when they change the
    in-stream round count or, for `run_duration`, the auto maintenance
    intervals derived from it.
- Batch: `benchmark.iterations`, `pre_benchmark_maintenance`,
  `maintenance_settle`, `--skip-maintenance`. Iterations and the effective
  maintenance are Conditions keys (not like-for-like); `maintenance_settle`
  is not in the identity.

### 7.4 Prohibited changes (invalidate a result or are refused)

**Refused.**

- At deploy, generate and run: a scale above the datagen ceiling
  ([11](../C360.md#11-supported-compositions)).
- At config load or run start:
  - an unsupported composition (`_SUPPORTED_COMBINATIONS`,
    `WORKLOAD_TABLE_FORMATS`, `WORKLOAD_MODES`);
  - `schema: custom`; Spark 4.2;
  - `corpus_role` or `robustness_perturbation` on Customer 360;
  - `cycles > 1` with continuous mode;
  - an explicit `max_files_per_trigger` that offers the corpus before the
    window ends;
  - an explicit maintenance or compaction interval that cannot fire in the
    window;
  - `run_duration < 3 x gold_refresh_interval` with gold on an interval.
- At job start (the stage fails): `spark.lb.silver.strategy=salted` in
  silver-build; `spark.lb.gold.strategy=incremental`, or a value naming no
  strategy, in gold-finalize.

**Not comparable**:

- records of different identity versions (exp1 against exp2);
- a record missing a required identity key or with its seed withheld;
- a run that did not pass (including a failed gating `c360_correctness`
  check);
- a different workload, workload version, parameters id, mode, query set,
  corpus id, seed, scale, corpus role or cycle count above 1;
- a different generator image (exp1) or generator digest (when both runs
  recorded one);
- a mixed or disagreeing datagen fleet;
- any differing result fingerprint.

**Corpus id** (`metrics/experiment.py`, `metrics/corpus_identity.py`):

- On exp2 it is v2: the hash of the generator's resolved arguments, model
  version, image lineage and cycle count.
- On exp1 it is the config hash of schema, generator image, seed, corpus
  role, perturbation, scale, timestamp window, `dirty_data_ratio` and
  `unique_customers`.
- A record is exp2 only when all of these hold: the generator wrote its
  per-node corpus markers (`_corpus/` in the bronze scope); the run started
  under identity version 2; (cluster runs) part of the system identity was
  observed.
- Otherwise it is exp1 and lists what was missing in
  `experiment.v2_unavailable`.

- `datagen.file_size` is fixed at `64mb`, so it cannot move the corpus.
- The codec (`DG_COMPRESSION`) changes every file's rows but is reachable only
  by editing the datagen Job by hand; an exp1 corpus id does not see it, so do
  not publish a comparison across such an edit.
- Editing stage scripts or the KPI list without bumping `WORKLOAD_VERSIONS`
  would compare under the same version; bump it. Nothing detects such a
  change: the tests pin only the current value.

### 7.5 Lakebench-imposed caps and how a bound cap is reported

| Cap | Value | Reported |
|---|---|---|
| Executor ceiling | 28 (`_MAX_EXECUTORS_SAFE`); per job: bronze-verify 20, silver-build 28, gold-finalize 28, bronze-ingest 10, silver-stream 20, gold-refresh 10. Batch counts are constant up to scale 10 (4 / 8 / 4) | `experiment.limits.executors[].cap_hit` and `limits.bound` when the scale asks for more |
| Continuous concurrent executor budget | 90% of the cluster CPU (and memory) left after the always-on pods, datagen and the stream drivers; each executor to the stream with the smallest share of its need | `limits.executors[].budget_cap`, `limits.bound` |
| Streaming trigger intervals | back to back by default; any interval set in the config | `limits.trigger_bound`, beside freshness (not a bound kind: the interval is in the config) |
| [Trickle](../../glossary.md#trickle) (`max_files_per_trigger`, files per micro-batch) | auto, under `--skip-generate` only: `int(files x trigger_s / (1.2 x run_duration))`, clamped to 1 to 50 files per trigger. Files is the nominal corpus at 64 MB (10 x scale x 1024 / 64); 50 files per trigger when the corpus size cannot be read | `config_limits.max_files_per_trigger`, `continuous.trickle`, `intake_limit: trickle_rate` |
| Trickle, examples | at the trickle's 30 s trigger and a 1,800 s window: scale 1 gives 2 (about 2,400 s of arrival), scale 10 gives 22 (about 2,190 s) | as above |
| Trickle, explicit | an explicit `max_files_per_trigger` is used as given, bounded only below (>= 1) and by the arrival rule | as above |
| Pre-benchmark maintenance budget | 1,800 s | `maintenance_stopped`, `limits.bound` |
| Settle waits | batch 2,700 s default; continuous 1,800 s, the result check skipped without waiting when the estimate exceeds it | `maintenance_settle_capped`; `results.not_checked` |
| Sizing cuts to fit the cluster | as printed | `limits.autosize_cuts`, `limits.bound` |

- A bound cap enters `limits.bound_kinds`, part of the like-for-like
  identity; so does an explicit per-job executor override below the
  profile's ask at that scale (`metrics/bounds.py`).
- Scratch per executor is fixed by `_JOB_PROFILES`.
- The Spark peak at each scale, which silver-build sets in batch, is the
  "Spark peak" column of the generated sizing table in
  [Sizing](../../sizing.md), from `compute_peak_requirements()`.
