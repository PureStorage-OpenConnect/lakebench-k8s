# Changelog

All notable changes to Lakebench are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Breaking changes
- **A config needs a `name:` to change data.** `deploy`, `generate`,
  `run`, `benchmark`, `query`, `clean`, `compare`, `reproduce`,
  `financial` and `validate` refuse a nameless config and offer a name to
  add; `config upgrade` refuses one too. A nameless config no longer gets a
  name written to `.lakebench/state.json`; that file is only read. Because
  v1.6 gave every nameless config in a directory the name in that file,
  nothing ties a v1.6 deployment to any one of them, so `destroy`, `stop`,
  `admin`, `status`, `logs`, `report` and `results` refuse a nameless
  config in such a directory, naming the v1.6 name and any other nameless
  configs there: add `name:` with that name to the config that deployed
  it. `info` and `config show`, `storage`, `recommend` look at no
  deployment and still load it under that name. Without that file,
  `destroy`, `stop` and `admin` refuse a nameless config.
- **Removed config keys are refused by the commands that change data**
  (the list above, except `config upgrade`), with what to do instead.
  `destroy`, `stop`, `status`, `logs`, `report`, `info`, `config show`,
  `config upgrade` and `admin` still load such a config and list the
  dropped keys in one "Upgrade notes" block on stderr. An old
  `datagen.file_size` is treated the same way.
- **Counts are bounded at load.** `trino.worker.replicas` 1 to 256,
  `datagen.generators` 0 to 1024 (0 is auto), the Polaris and Unity ports
  1 to 65535, and at least 1 for every core count (Spark driver and
  executor, `driver_cores`, Spark Thrift, DuckDB), the Hive thrift thread
  counts, `executor.instances`, the per-job executor overrides and
  `customer360.unique_customers` and `date_range_days`. The two upper
  bounds sit far above anything lakebench sizes (200 workers at the top
  scale; generator threads follow the pod CPU). A config with a zero,
  negative or out-of-range count, which v1.6 accepted, is refused.
- **Flat top-level config keys are deprecated.** `endpoint:`, `scale:` and
  the other flat spellings still load, each with a note naming the nested
  key to write. Both spellings set: the flat value still wins, with a note.

### Changed
- Config errors name the nearest key: an unknown key gets "did you mean"
  from its own section, then from the whole schema (for a key written in
  the wrong section), and an unknown recipe names the nearest recipe.
- A setting of the other workload (`customer360.*` or `dirty_data_ratio`
  under `schema: financial`; `tm_operations` or
  `w1_max_vertices` under `schema: customer360`) loads with a note that the
  workload does not read it.
- Read-only commands create no files: `validate` no longer opens a journal,
  and `report` and `results` no longer create `lakebench-output/runs/`.
- `platform.storage.s3.secret_ref` without `access_key` and `secret_key` is
  refused at load: nothing reads an existing Secret, so such a config
  deployed empty S3 credentials (LB-190). `destroy`, `status` and `clean`
  still load it, so an old deployment stays destroyable. Set alongside
  inline keys, it loads with a warning that it has no effect. `config
  validate` and the deploy preflight now ask for the inline keys only.
- Run provenance and the Hive deploy result record Hive 3.1.3, the version
  the Stackable HiveCluster template renders, instead of the tag of
  `images.hive`. An `images.hive` naming another version warns at load that
  it has no effect (LB-189).
- Setting `architecture.catalog.hive.thrift.*`,
  `architecture.catalog.polaris.version` or `observability.storage_class`
  to a non-default value warns that it has no effect; nothing reads them.
  The generated config template no longer carries them or `secret_ref`.
- Deploy step labels say "Verifying scratch StorageClass" and "Checking
  Spark Operator and watch list", and the deploy summary lists the operator
  step whether or not `operator.install` is set (LB-191). The HTML report's
  continuous section is headed "Continuous Pipeline".
- `docs/reproductions/c360-scale-0-1.yaml` is marked as a legacy package
  that `reproduce` refuses.
- **`deploy --timeout` now bounds every wait (DEP-6).** It used to be
  checked only between steps, so a step waiting on Spark Thrift (300 s),
  DuckDB (900 s), Polaris (600 s plus 600 s) or an operator rollout could
  run past it. Every wait is now clamped to the time left, including the
  wait for the cluster lease, and a wait the deadline cuts short fails the
  step with "deploy timeout (N s) reached after M s while waiting for
  <component>: <resource> (<last state>)". No shared change (a helm
  upgrade of the Spark Operator watch list, a Stackable or observability
  install) starts after the deadline; one that has started is completed,
  with its rollout and verify, before the step fails, so the shared
  operator is never left mid-restart. Helm calls already running finish
  first, and a polling wait can overrun by one poll interval (10 s at
  most).
- The deploy failure panel no longer claims that successful steps are
  skipped on retry: re-running deploy re-applies every step and keeps the
  existing resources.
### Fixed

- Trino compaction of the Customer 360 silver table no longer fails with
  "Exceeded limit of 100 open writers for partitions" when it rewrites files
  in more than 100 `interaction_date` partitions, as the silver of the one
  recorded continuous Customer 360 run did (LB-210, n=1). A silver table with more than 90 partitions is now
  compacted in chunks of at most 90, after a read of its partition values.
  This also applies to batch runs, whose single statement happened to
  succeed (batch silver holds a few large files per partition): the
  pre-benchmark maintenance of a batch Customer 360 run on Trino now runs
  one partition read and several `optimize` statements where it ran one,
  which can change the recorded maintenance time. Compaction outcomes count
  tables, not statements, and `experiment.effective_maintenance` names each
  table whose compaction failed in `reasons` and
  `detail.compaction_failures`. The maintenance policy id and the effective
  maintenance `id` are unchanged.
### Changed
- **`destroy` removes what it used to leave in a surviving namespace.** With
  `create_namespace: false`, destroy left the PostgreSQL ServiceAccount, the
  `lakebench-ca-certificate` Secret (with `s3.ca_cert`) and, with
  observability on, the Pushgateway Deployment, Service and PVC, the
  Prometheus ConfigMap and five PodMonitors. A new step, after the component
  steps and before the namespace step, deletes them by name from the
  Category-1 registry (`deploy/category1.py`). The registry lists every
  object deploy and run create in the namespace and the step that deletes
  it; a unit test runs deploy and the run-time creators against it, and
  checks every template. The `lakebench-silver-state` ConfigMap is kept on
  purpose (its rebuild counters must not reset while table data can outlive
  destroy), and so are the deployment's identity annotations.
- **`destroy` keeps a namespace an operator pod still watches.** After it
  removes the namespace from the Spark Operator watch list and the operator
  restarts, destroy waits inside the cluster lease (up to 120 s) until no
  operator pod that is still running, and no operator Deployment template,
  lists the namespace in `--namespaces=`. A stale pod nobody is replacing
  gets one more restart of the shared operator's controller and webhook (as
  the watch-list change itself does). If something still lists it, destroy keeps
  the namespace and exits 1 with "operator pods [...] still watch it",
  because the operator crash-loops on a watched namespace that no longer
  exists.
- **The legacy SecretClass cleanup in `destroy` runs under the cluster
  lease.** The cluster-wide count of other lakebench namespaces and the
  deletes of `lakebench-s3-credentials-class` and
  `lakebench-s3-ca-cert-class` used to run without it. The lease is taken
  only when one of them exists; if it stays held for 600 s the cleanup is
  skipped and they are kept.
- **The cluster lease holder names the process (LB-178).** The `holder`
  field is now `<host>@<user>@<sha>#<pid>-<8 hex>`, unique to each acquire,
  and release matches the lease's write nonce, so a process never deletes a
  lease another run from the same host wrote in the same second. An acquire
  that fails or is interrupted after its write landed (a lost reply, a 504,
  a Ctrl-C, a SIGTERM) releases that lease instead of leaving it to the
  3600 s TTL, and an acquire whose own write comes back as a conflict adopts
  it instead of waiting on itself.
- **`destroy` deletes the PostgreSQL data PVC when the namespace survives
  (LB-187).** With `create_namespace: false`, `data-lakebench-postgres-<n>`
  and the catalog metadata on it used to survive destroy, because the cleanup
  selected on a label the claim never carried; the next deploy then started
  on the old metastore. Destroy now deletes the claims by name. It no longer
  selects on `app.kubernetes.io/component=postgres`, which could only ever
  match another application's claim in a shared namespace.
- **Spark scripts ship in one ConfigMap per role (DEP-1, LB-207).** The single
  `lakebench-spark-scripts` ConfigMap, about 45 KB from the 1 MiB limit with
  every AML addition, is replaced by six maps (`lakebench-scripts-common`,
  `-c360`, `-aml-rules`, `-aml-jobs`, `-aml-gate`, `-aml-data`), projected
  together at `/opt/spark/scripts`, so script paths and imports are unchanged.
  Each map is refused above 80% of 1 MiB, measured on the bytes applied. A
  script listed for shipping but missing from the installed package now stops
  `run` before any job is submitted, where 1.6 skipped it and the driver
  failed later with an ImportError. `run` will not change a scripts map that a
  running SparkApplication mounts, and a job is not submitted if its maps
  changed since its run applied them. The first 1.7 `run` deletes the 1.6 map
  unless a running SparkApplication still mounts it; `destroy` deletes all
  scripts maps, also when `create_namespace: false`.
- **Interrupts wait for the cluster lease to be released.** A Ctrl-C,
  SIGTERM or SIGHUP while a command holds the `lakebench-cluster-lock`
  lease no longer stops it between a `helm upgrade` of the shared Spark
  Operator and the operator restart. The command prints the hold budget
  left (750 s, 1800 s for `admin` commands), finishes the shared change,
  releases the lease, then stops; a third interrupt aborts at once and
  still releases the lease. `kubectl`, `helm` and `oc` run under the lease
  in their own session with a timeout from that budget, and are stopped
  with SIGTERM rather than killed, so a terminal Ctrl-C or a timeout no
  longer leaves the release `pending-upgrade`. A leased command that runs
  out of time fails deploy or destroy closed (destroy keeps the namespace);
  for helm the error names `helm rollback` and
  `lakebench admin repair-operator`.
### Fixed
- **Spark Thrift on Spark 4.1 with Iceberg 1.11 loaded the 4.0 runtime.**
  Thrift picked the Iceberg runtime from the Spark version alone, so it loaded
  `iceberg-spark-runtime-4.0` while the pipeline jobs loaded the native
  `iceberg-spark-runtime-4.1`. Thrift now makes the same choice as the jobs.
  Thrift deployments on Spark 4.1 with Iceberg 1.11 change runtime jar on
  their next deploy; other combinations are unchanged.

### Added
- **Corpus id v2 (EVD-6).** A run whose corpus observation was recorded
  (the generator's per-node markers and `series.json` under the datagen
  prefix, read once before the record is saved) gains in
  `experiment.corpus`: `id_v2` (null when it cannot be computed, with the
  reason in `id_v2_unavailable`), `id_version`, `args_sha256`, `declared`
  (the config's corpus settings, for display), and, when markers exist,
  `lineage`, `lineage_observed` and `lineage_notes`; `warnings` when the
  config disagrees with the corpus it read. The id hashes the arguments the
  generator resolved, the model version and the image lineage (the digest
  in `series.json`, mapped through `src/lakebench/config/datagen_lineage.yaml`
  and resolved when the run is observed), so a config edited after
  generation does not change it. Markers that disagree, miss a node or
  cycle, or come from different builds are corpus problems, which
  `compare` reads as not comparable. `corpus.id` (v1) is unchanged, and no
  stored id or identity digest moves.

## [1.6.0] - 2026-09-30

Lakebench 1.6 makes the workload a first-class part of an experiment and
runs two workloads, Customer 360 and AML, through the same composable
architectures. Every run now records what produced it, `compare` refuses to
compare runs whose workload results differ, and several published metrics
changed meaning. Most numbers recorded by 1.5 and earlier are not comparable
with 1.6; the first section lists why.

### Known limitations
- **AML recall is uncalibrated.** v1.6 publishes no held-out Level-2
  result. Recall and precision are in-sample on the calibration corpus, and
  the report labels the column "Recall (uncalibrated)". The registered
  held-out looks are deferred to v1.7.
- **No v1.6 performance baselines.** The performance re-baseline and the
  AML frozen-generator performance and size measurements are deferred to
  v1.7. No pinned config is required by the release gate's
  `perf-baselines` check.
- **Trino OPTIMIZE hits the open-writer limit (LB-210).** On the Customer
  360 continuous silver table `silver.customer_interactions_enriched`,
  Trino OPTIMIZE can fail with "Exceeded limit of 100 open writers for
  partitions".
- **AML continuous recall is not scored (LB-168).** Stopping the streams
  can interrupt a gold-refresh tick; scoring refuses the partial pass.
  Batch recall is unaffected.
- **AML gold-finalize is slow (LB-201).** At scale 10 it took 4,100 s of its
  5,400 s auto timeout on 4 executors (run 20260929-214442-825153, n=1); at
  scale 100 a skewed detection stage takes minutes per task. Under heavy
  parallel load, raise `--timeout` for AML runs at scale 10 and above.
- **AML expected-size estimate is off (LB-105).** The financial bronze size
  estimate in `config/scale.py` does not match the generator, so the AML
  `scale_ratio` score is not exact.

### Read this first: comparability with earlier releases
- **Datagen file size is fixed at 64mb and datagen scale is banded (LB-204).**
  `workload.datagen.file_size` accepts only `64mb` (any case); another size
  is refused by `deploy`, `generate` and `run` (destroy and clean still load
  such a config, with a warning). Batch runs published at another size are
  not like-for-like at the bronze stage. Datagen scale has per-workload
  limits: AML supported to 300, unverified to 800, refused above; Customer
  360 supported to 300, unverified to 600, refused above.
- **Datagen batch delivery now actually runs batch (LB-196).** Since the Rust
  default flipped to continuous, `datagen.mode: batch` silently ran
  continuous. Datagen timings recorded as batch before this fix were
  continuous.
- **Before 1.6.0 no Iceberg `expire_snapshots` or `remove_orphan_files` and
  no Delta `VACUUM` ever ran (LB-172, LB-173, LB-174), so no earlier
  continuous number is comparable with 1.6.** Trino refused every Iceberg
  expiry and orphan removal below its 7-day system minimum, the Spark Thrift
  form failed a parameter-binding error, Delta VACUUM sent its retention
  override in a separate session so it never applied, and `exec_sql`
  discarded the exit code, so every failure was reported as a success. Batch
  pre-benchmark maintenance was therefore compaction only. Continuous
  freshness, rows/s, in-stream QpH, object counts and maintenance-value
  numbers from 1.5 or earlier are not comparable with 1.6.
- **`maintenance_policy_id` is stamped into every metrics.json**
  (`m2-2026-09-26` for 1.6; a record without it reads as `m1-legacy`, and a
  `--skip-maintenance` or `--local` run as `<id>+skipped`). The perf gate and
  `reproduce` refuse to compare, record or verify across policies; `compare`
  warns. report.html shows the policy.
- **Delta continuous ships with no effective table maintenance in 1.6.**
  Delta VACUUM keeps Delta's 7-day retention while streams are live, so a
  continuous run shorter than 7 days removes nothing (Trino runs VACUUM and
  it is recorded as `ran_no_effect`; Spark Thrift does not run it and records
  `not_supported`), and no bounded OPTIMIZE runs in continuous mode. The run
  header, the evidence (`effective_maintenance.known_limitations`) and the
  report state this, and the report shows the in-window QpH trend
  (`qph_trend`: first and last round, silver file count at each end) so a
  median over a falling series is not read as steady state.
- **Iceberg metadata cleanup after commit.** Every Iceberg table Lakebench
  creates sets `write.metadata.delete-after-commit.enabled=true` and
  `write.metadata.previous-versions-max=50`; before, old `metadata.json`
  files grew by one per commit for the whole of a continuous run. Tables in a
  reused catalog keep their old properties until they are recreated.
- **Query sets changed, so QpH from 1.5 does not compare.** Total-order
  tiebreakers (FQ2, FQ3, FQ4, FQ6, FQ7, FQ8, IQ3, Q4), Q1 averaging
  purchases only, the AML set growing to 12 benchmark queries, and QpH as
  the median of 3 samples all move the `query_set_id` or the estimator;
  `compare`, the perf gate and `reproduce` refuse QpH across them.
- **Perf-gate baselines and reproduce packages recorded before 1.6 refuse
  every run** until re-recorded: they carry no experiment identity, no
  maintenance policy and poll-quantized stage times.
- **Continuous metrics changed meaning** (details under Changed): scores are
  taken only inside the measurement window, `ingest_ratio` is measured
  against the rows the trickle released, the headline freshness is the
  scored `data_freshness_seconds`, and the default trickle and maintenance
  interval are derived from the window.
- **Batch stage times come from the Spark driver's end** (`finishedAt`), not
  the 15 s monitor poll, so every stage reads up to 15 s shorter than in 1.5.
  The perf gate refuses to compare runs with different timing sources.
- **Run, stage and round timestamps are UTC** with the zone in metrics.json
  (they were naive host-local time). Run ids keep their host-local form.
- **AML results are not comparable with 1.5.** The generator is frozen at
  `datagen-v2-rs-0.3`, answer keys are out of bronze, W5/W6 are now scored
  screens, W7 uses the June 2026 FATF lists plus synthetic corridors, and
  several rules changed their targets (see Breaking changes).

### Behaviour changes you must know
- **Every result is an identified experiment.** Every metrics.json carries
  an `experiment` block: workload and version, generator model version,
  corpus id, seed and scale as read from the datagen pods, the datagen image
  and its pod digest, recipe and component versions, query access path
  (`catalog` or `direct_storage`), mode, requested and effective
  maintenance, stages and detection rules executed or skipped with the
  reason, Lakebench-imposed limits and which of them bound, the support
  state, repetitions, and a result fingerprint per benchmark query. It also
  records `provenance` (Lakebench version, git commit, dirty flag).
  report.html shows the block.
- **Result fingerprints.** After the timed samples each successful query is
  run once more, untimed, and its rows hashed after canonicalising cells
  across engines (fingerprint spec `rf2`: exact digits, approximate sums for
  declared DOUBLE columns with a row-keyed weighted sum, volatile columns
  fingerprinted by NULL-ness only). Two runs with equal fingerprints
  returned the same rows.
- **`lakebench compare` verdicts.** Three verdicts: **comparable** (the
  workload results are equivalent), **NOT COMPARABLE** (different
  experiments, different results, a failed run, or a record without the
  experiment block; deltas and winner withheld, exit 1) and
  **comparability not established** (a side has no checked results:
  `--skip-benchmark`, a recipe without a query engine, or a continuous run
  without a settled result check; raw numbers shown, no deltas, exit 0). A
  comparable pair whose execution conditions differ (effective maintenance,
  access path, system, benchmark iterations, in-stream round count, bound
  limits) is labelled not like-for-like. `comparison.json` gains `verdict`,
  `comparable`, `like_for_like`, `condition_differences`, `support` and
  `refusals`; CSV output gains `comparable` and `like_for_like`.
- **Support states: supported, unverified, unsupported.** The state is
  computed per workload x recipe x mode (`lakebench.config.support`). A
  combination the architecture, workload or mode checks refuse is
  **unsupported** and refused at config load, or by `run` for the mode
  `--continuous` selects, before anything is deployed. A valid combination
  is **unverified** unless the release validation record
  (`config/validated_combinations.yaml`) lists it with the live runs that
  validated it, in which case it is **supported**. The record ships empty in
  this tree; it is filled from release validation runs. The state is frozen
  at run start and stamped in the evidence; a checkout with local changes,
  or one whose git status is unknown, is never stamped supported, and local
  runs are never supported. `config recipes` lists the state per workload x
  mode, `config show` prints the config's state, and `compare` shows both
  sides'. The recipe and support tables in the docs are generated from this
  code.
- **Lakebench-imposed caps are labelled.** The experiment block lists the
  limits that applied (executor ceiling and per-job `max_executors`, the
  continuous concurrent executor budget as requested vs granted, the
  pre-benchmark maintenance budget, the continuous trickle, in-stream
  benchmark rounds and iterations, TM alert capacity, `w1_max_vertices`) and
  which of them bound, so a bound figure is not read as infrastructure
  performance.
- **AML generator image.** `images.datagen` defaults to
  `docker.io/sillidata/lb-datagen:1.6.0` (digest
  `sha256:5fda9025fb9b455b390e1138d82e9f6ef16d214dfa9419815be0111d2f6fce0a`),
  generator `MODEL_VERSION` `datagen-v2-rs-0.3`, the release build of the
  same datagen_rs source as the validated `034f998` image. Its output is
  byte-identical to the frozen generator built from 9382420 source (seed 43,
  141/141 objects, across thread and pod counts). It adds the per-pod memory
  model with a 16Gi cap, fixed 64 MB files and delivery-mode forwarding
  (LB-204, LB-196). It is the functional default, not the registered-look
  image: registered-look, D8, A6 and calibration corpora pass an explicit
  frozen digest via `--generator-image` (see `docs/internal/aml-protocol.md`).
  The pinned image is the reproducibility unit; bit-exact output holds within
  one build environment. A corpus from an image before the freeze is
  pre-freeze. Prior tags `e14d0fd`, `30603b1`, `9382420` (digest
  `sha256:2faad1cc0252a165a56361a06f159a62ba7c4387c83adfb7c46fe260af23b8f2`,
  live-metrics Pushgateway push), `b6f2905`, `25f1aa8`, `7c24641` and
  `0a83acd` are recorded in
  `src/lakebench/config/schema.py::ImagesConfig.datagen` (`7c24641` digest
  `sha256:c5a6bc80d89341b0753dccd39abb5cbe835ed31a9d14b33e0989863cca774f3b`);
  all but `e14d0fd`
  were deleted from docker.io and must be rebuilt from source.
- **Config contract (v1.6).** `workload` is a top-level key; the old
  `architecture.workload` block still loads with a deprecation warning, and
  setting both with different values is an error. `continuous` is the
  canonical pipeline mode (`mode: continuous`, `pipeline.continuous`,
  `run --continuous`); `sustained` is accepted as a deprecated alias, and
  metrics files keep recording `pipeline_mode="sustained"`. Refused at load:
  the financial (AML) workload on Delta (its scripts write Iceberg only),
  workload schema `custom` (it silently ran the Customer 360 queries), and
  Iceberg 1.11+ on a Java 11 Spark image. `images.prometheus`,
  `images.grafana`, `observability.reports`, `table_format.iceberg.file_format`
  and the Iceberg and Delta `properties` never did anything; a non-default
  value now warns, and they are removed in v1.7. `pipeline.pattern` is
  deprecated.
- **`--local` refuses AML and continuous.** Local mode ran the Customer 360
  batch job map whatever the config named; it now refuses an AML config and
  continuous mode instead of running Customer 360 under the AML label.
- **Default bucket names are `<name>-bronze`, `<name>-silver`, `<name>-gold`.**
  They were the fixed `lakebench-bronze/-silver/-gold`, which collide on
  stores where bucket names are global (FlashBlade across accounts, AWS) and
  were shared by every deployment on one store. A deployment that relied on
  the old defaults must set `platform.storage.s3.buckets` to the old names
  to keep using (and to destroy) its existing buckets.
- **A benchmark that raises fails the run.** It used to print a warning and
  leave the run successful. Now `run` exits non-zero, no QpH is recorded,
  the journal records the benchmark as failed, and metrics.json and the
  report carry `benchmark_error`.
- **A benchmark query that returns no rows fails the run** unless the query
  is declared allowed-empty (IQ2 and IQ4). In continuous mode only the last
  in-stream round is held to this, and an empty Q9 there fails the gate.
- **Recipes without a query engine skip the benchmark** and `run` exits 0;
  no QpH is recorded. `benchmark_type` names the engine that actually ran
  (it read `trino_query` on DuckDB and Spark Thrift).
- **The observability stack is shared.** kube-prometheus-stack installs
  cluster-wide objects, so `deploy` installs one release into the
  `lakebench-observability` namespace only when none exists, never upgrades
  an existing one, and `destroy` never uninstalls it (a release an older
  lakebench put in the deployment's own namespace is still removed).
- **Cross-engine row counts were wrong on Spark Thrift and DuckDB.** Thrift
  reported `n + 3 x ceil(n/100)` rows (beeline options after `-e` were
  dropped, so it printed its table format), and DuckDB reported 2 rows for
  any query slower than 2 s (its progress bar broke the JSON payload and the
  executor counted lines). Both are fixed; a DuckDB payload that cannot be
  read is now an error. Every engine session is pinned to UTC.
- **AML sanctions and PEP screening is scored (generator 0.3).** Every AML
  corpus now carries a synthetic, dated watchlist
  (`bronze/watchlist.parquet`: a sanctions list in two versions and a PEP
  list) and planted payments to listed parties under name variants
  (aliases, token-order swaps, one-letter typos, other romanisations,
  dropped legal suffixes), with namesake decoys. W5 is a fuzzy screen
  against that list at transaction time plus a rescreen on each list
  version; W6 uses the same screen for PEP payments (MED priority at
  $10,000 or more, LOW below). Both are scored for recall and precision
  against planted `sanctions_match` / `pep_match` instances. The packaged
  `sanctions_list.json` and `pep_list.json` are gone; a corpus without a
  watchlist reports W5/W6 as not run, and continuous mode skips them.
  This is a benchmark screening workload, not a production sanctions list.
- **No answer keys in the AML party zone.** `party.parquet` no longer has
  `sanctions_status`, `pep_status` or `initial_risk_score`, the customer
  risk rating no longer uses PEP status, and `silver.entities` leaves those
  three columns NULL.
- **FATF list refreshed to June 2026.** `high_risk_jurisdictions.json` now
  holds the FATF lists published on 19 June 2026, dated and sourced. No
  generator home country is on them, so W7 also alerts on the generator's
  synthetic high-risk corridor countries (`synthetic_corridors.json`,
  labelled synthetic; the same pool the generator plants
  `corridor_high_risk` from). W7 recall and alert volume are not comparable
  with 1.5.
- **AML scoring counts are truthful.** `recall.json` gains
  `typology_counts` (scored, partial, no rule, rule skipped, rule error) and
  a `rules` list with each rule's status, reason, target and alert count;
  the CLI prints "N of M typologies scored" from them (it claimed 15 when 6
  were scored). The generator's AML category is reported as
  `workload_category`, beside `designated_rules`.
- **Maintenance statements report their real outcome.** Trino sends
  `SET SESSION <catalog>.<procedure>_min_retention` in the same submission
  as the procedure, Spark uses a `TIMESTAMP` literal, Delta VACUUM and its
  retention setting run in one submission, and a failed or timed-out
  statement is reported as one. The effective maintenance in the evidence
  comes from what each call did, per operation: `ran`, `ran_no_effect`,
  `not_supported`, `skipped_by_user`, `failed` or `not_run`, with the
  applied retention of each operation. Batch Customer 360 maintenance no
  longer targets the continuous-only `bronze_raw` table.
- **Retention floors.** Orphan-file removal never runs below 24 h 10 min on
  any engine or path. Iceberg snapshot expiry is floored at 1 h while
  streams are live. Delta VACUUM keeps Delta's 7-day default while streams
  are live.
- **`retention_threshold` is strict.** It must be a whole number and one
  unit (`s`, `m`, `h` or `d`, for example `30m` or `7d`); anything else,
  such as `1.5h` or `30min`, is rejected when the config loads. A
  continuous Iceberg config that sets it below the 1 h live-stream floor
  warns once; the applied value is recorded in `continuous.retention`.
- **Pre-benchmark maintenance has a 30-minute budget.** Expire, orphan
  removal and compaction share it; the first statement timeout or the
  deadline stops the rest and the benchmark runs anyway. When maintenance
  stopped early or ran beside live stream apps, the perf gate excludes
  post-maintenance QpH, and if no QpH metric is left to gate the verdict is
  `NOT_COMPARABLE` (exit 2, never a pass, never a baseline). Continuous
  maintenance and compaction statements get `min(600 s, interval / 2)` each,
  a round is capped at half the interval and at the time left in the run,
  and the next round resumes at the table after the one that timed out.
- **Destroy never deletes files outside proven ownership (LB-186).** Trino
  `DROP TABLE` deleted every file an Iceberg (Hive) table referenced,
  including `add_files`-registered datagen files, and a managed Delta
  table's directory, before the bucket step's ownership checks ran; a
  bucket destroy refuses (another deployment's shared bronze, an adopted
  bucket holding data, an untagged pre-provisioned bucket) still lost the
  files its tables pointed at. Destroy now runs Trino
  `system.unregister_table` (catalog entry only). Spark Thrift keeps
  `DROP TABLE` for Iceberg (no PURGE) and drops a Delta table only when
  `DESCRIBE DETAIL` puts its location in a bucket destroy will empty. Tables
  left registered, and the refused buckets whose files remain, are printed
  and journaled. On Polaris, when destroy deletes the namespace the drops are
  skipped (the catalog's only state is in the deployment's PostgreSQL PVC),
  so a polaris+trino destroy no longer exits 1 on refused purge-drops.
  `write_delta_table` refuses to create a managed table over an existing
  `_delta_log` that is not in the catalog.
- **Destroy on stores without bucket tagging (FlashBlade) empties only
  buckets it can prove it owns.** It used to empty any config-named bucket
  whose name matched the deployment. It now also needs the namespace's
  created-buckets record, or the record of a bucket deploy adopted while
  empty; an unrecorded match is left in place and reported FAILED, and
  `--force-legacy` empties it but never deletes it. `clean` and the
  continuous reset follow the same rule.
- **Destroy semantics.** Destroy runs no table maintenance (no Iceberg
  expire or orphan removal, no Delta VACUUM). It empties the buckets it owns
  and then deletes only the ones this deployment created (the
  created-buckets record on the namespace plus the ownership checks);
  pre-provisioned, adopted and `--keep-buckets` buckets are emptied and
  kept. If a recorded bucket cannot be deleted, the namespace is kept as
  the ownership record. Destroy waits until the namespace is NotFound
  before reporting it deleted (`--namespace-timeout`, default 600 s; exit 3
  when it is still terminating). The scratch StorageClass is never deleted.
- **Spark Operator watch-list edits keep the installed chart and hold the
  lease.** A watch-list add or remove could move the shared operator to the
  repository's latest chart or to a tenant's pin; it now pins the chart of
  the installed release. `validate` treats an unwatched namespace as
  advisory before the first deploy, no longer suggests a raw
  `helm upgrade --reuse-values` (which bypassed the lease), and fails when
  the credentials cannot upgrade the operator release.

### Breaking changes (read before upgrading)
- **Unknown config keys are rejected.** Every config model forbids extra
  keys, and the error names the full path (for example
  `architecture.workload.datagen.scael: Extra inputs are not permitted`).
  Keys that were silently ignored now stop every command, including
  `destroy`. Keys removed in earlier releases warn and are ignored:
  `images.pull_secrets`, `table_format.hudi`, `medallion.silver.strategy`,
  `customer360.channels` / `event_types` / `quality_distribution`,
  `scratch.create_storage_class`. If an old deployment's config has a
  mistyped key, delete it before running `destroy`: correcting it can
  retarget the namespace or buckets.
- **Double spellings are errors.** Setting both `processing` and
  `pipeline`, both `continuous` and `sustained`, or both `schema` and
  `schema_type` used to drop one silently.
- **`-f` means `--file`.** `destroy`/`clean`: use `--force`, `--yes` or
  `-y`. `init`: `--force`. `results`: `--format` / `-o`. `logs`:
  `--follow` / `-F`. Admin commands gain `-f/--file`.
- **`-f` on `destroy` and `clean` exits 2 in every context** and names
  `--force` / `-y`; `LAKEBENCH_LEGACY_SHORT_F=1` restores the old meaning
  with a warning. `results` rejects a `--format` that conflicts with `-f`.
- **`results -o json|csv`** prints plain stdout.
- **A continuous run whose explicit settings cannot produce continuous
  evidence is refused at start:** a `max_files_per_trigger` that would offer
  the whole corpus before the window ends, a `retention_interval` or
  `compaction_interval` whose first round cannot run inside the window
  (unless maintenance or compaction is turned off), and a window shorter
  than three gold refreshes (`run_duration` below `3 x
  gold_refresh_interval`, 900 s at the defaults). Each refusal names the
  setting and the value to use.
- **A c360 continuous run over existing state refuses without
  `--force-reset`.** It lists the non-empty tables, checkpoints and raw
  prefixes it would delete (LB-142).
- **AML rule targets changed.** W3 now searches 2-5 hop cycles and is
  scored against `cycle`; W4 is scored against `rapid_layering`; a new
  chain rule `W17_layering_chain` is scored against `stack`; W2 adds a
  per-beneficiary alert kind; W5 and W6 are scored (see above). Per-rule
  recall and FP are not comparable with earlier runs.
- **AML customer-scoped rules alert on customers only.** W2, W5, W6, W7 and
  W8 drop alerts whose entity is not a customer in `silver.entities`; the
  graph rules (W1, W3, W4, W17) stay unscoped. Alert counts and false
  positives drop.
- **QpH is the median of 3 samples per query (LB-150).**
  `architecture.benchmark.iterations` defaults to 3 and `lakebench run`
  passes it to both benchmark rounds (it was ignored before, and the
  `benchmark --iterations` default of 1 overrode the config). Throughput
  QpH counts executions. The perf gate and `reproduce` refuse a QpH taken
  with a different sample count.
- **The AML benchmark set grew from 8 to 12 queries** with the investigator
  class (IQ1-IQ4). `metrics.json` records a `query_set_id`; `compare` and
  `reproduce` refuse QpH across different or unrecorded sets.
- **Metric meanings changed:** `maintenance_value_pct` is null when not
  measured (was 0.0); c360 `customer_recency_score` and Q6 are anchored to
  the data clock, not the run date (Q6 and c360 QpH not comparable);
  silver-build `output_rows` in incremental mode is per cycle; a scale ratio
  of 0 means not measured and no longer shows as Complete.
- **Customer 360 gold KPIs were wrong and are corrected.**
  `avg_transaction_value` averaged every interaction (82% have amount 0.0)
  and read about 5.5x low; it is now per transaction, as is
  `avg_estimated_ltv`. `avg_page_views` and `avg_time_on_site_seconds` were
  about 1.9x low and are now per visit. `support_tickets_created` merged
  colliding ticket ids and is now one per support interaction. Multi-cycle
  batch counted every earlier cycle again; each appending cycle now reads
  only its own bronze files. Gold KPIs from 1.5 are not comparable.
- **Release process (maintainers):** one `release.yml`; PyPI uploads after
  the GitHub Release; tags must be the normalised version and on `main`;
  `uat/results-<version>.md` with a `# UAT results <version>` heading and a
  results table citing at least one run id, each resolving to a
  metrics.json, is required; pre-release and dev versions are refused. See
  `docs/releasing.md` for the repository settings the gates depend on.

### Changed: continuous mode
- **Scores come from inside the measurement window** (from every stream
  running to `run_duration` later), from the timestamped stream log lines.
  Rows taken in before the window are recorded as `pre_window_rows` and
  kept out of every score. Totals are cut at the window's end.
- **The continuous gate needs genuinely continuous output:** bronze took
  rows in at least two batches inside the window (the last past its
  halfway point), silver committed at least two micro-batches and gold
  refreshed on new silver data at least twice after bronze's first write,
  gold freshness was measured, and every stream was still RUNNING when the
  window closed. A corpus drained before the window opened, or a stream that
  restarted inside it, fails the run.
- **Stream submission failures are visible.** Each stream's
  SUBMISSION_FAILED retries (for example a truncated Maven download, named
  by artifact and byte count) are printed, journaled and recorded per
  stream with the seconds they cost (`submission_retry_seconds`). An error
  or Ctrl-C after submission fails the run and stops the streams.
- **Result check after settle.** After a run that passed, the streams keep
  running (up to 1,800 s, not scored) until the whole corpus has reached
  gold, then stop, and the query set is fingerprinted over the settled
  tables. Those fingerprints are the continuous run's results; without them
  (corpus did not settle, `--skip-benchmark`, gates failed, AML continuous)
  results are not established and the perf gate and `reproduce` refuse the
  run.
- **`ingest_ratio` is bronze rows over the rows the trickle had released**
  by the window's end, so `pipeline_saturated` means bronze fell behind what
  arrived. The whole-corpus share is kept as `corpus_ingest_ratio`; records
  without a window or file count fall back to it. `corpus_drained` means
  every datagen row reached bronze and was committed by silver.
- **The trickle is derived from the window.** `max_files_per_trigger`
  is unset (auto) by default: the most files per trigger, up to 50, whose arrival
  lasts 1.2 x `run_duration`, from the nominal corpus size (at the defaults,
  2 files for c360 scale 1, 22 for scale 10, the 50 cap from scale 100). The
  run prints the trickle and records it in the evidence as a
  Lakebench-imposed limit.
- **The default maintenance interval fires inside the window.** An unset
  `retention_interval` resolves at run start to `run_duration / 3` within
  300-7,200 s (600 s at the default window), and automatic compaction to
  twice that. It used to default to 1,800 s, equal to the default window, so
  a defaults-only continuous run never ran maintenance. The resolved values
  are recorded.
- **Headline freshness is the scored `data_freshness_seconds`.** The
  in-stream probe (query time minus the newest gold event date) measures
  the corpus's event-time position, not pipeline freshness; it is recorded
  as `gold_event_age_seconds` per round and
  `query_time_event_age_seconds` in the scores, and no longer headlines the
  score line. Old files with `gold_freshness_seconds` load into the new
  field. On Spark Thrift the probe uses Spark SQL.
- **QpH round counts are recorded.** Scores carry `composite_qph_rounds`
  and the experiment block `limits.benchmark_rounds`; a pair with different
  in-stream round counts is comparable but not like-for-like, and a run
  whose every in-stream round failed is refused against an in-stream
  baseline. A round that cannot fit the time left is skipped once and
  journaled.
- **Stage fields are measured or absent.** Unmeasured freshness, gold
  unique rows and failed table-health probes are absent instead of 0.0 or
  -1; `output_rows` comes from the logs.
- **Delta continuous works.** hive-delta recipes failed at reset
  (`REQUIRES_SINGLE_PART_NAMESPACE`); every table is now named in the
  pipeline catalog and Delta bronze is created at
  `s3a://<bronze>/warehouse/default.db/bronze_raw`.

### Changed: batch mode and measurement
- **Stage times come from the Spark application.** A stage ends at the
  driver container's `terminated.finishedAt` (else the SparkApplication's
  `terminationTime`), mapped to this host's clock by the API server's clock
  offset; elapsed runs from the SparkApplication's creation. An implausible
  end falls back to the poll. Each job and stage records `timing_source`
  and `timing_resolution_seconds`; the stage poll is 5 s. Time to value runs
  to the gold application's end and includes Lakebench's work between
  stages.
- **Batch SUBMISSION_FAILED retries are printed, journaled and recorded**
  (`submission_failures`, `submission_retry_seconds`); the stage line says
  how long it waited on failed operator submissions.
- **Storage settle wait before the post-maintenance round (LB-150).** In
  batch mode a probe query runs every 60 s after maintenance until two
  consecutive probes agree within 10% and neither is slower than the
  pre-maintenance median by more than the tolerance (widened by twice the
  median absolute deviation of the pre-maintenance samples, capped at 20%),
  capped at 45 minutes. It is skipped when no maintenance statement ran.
  `maintenance_value_pct` is null when the wait is skipped, capped or
  fails. The wait is not a stage and is not counted in time to value.
  Configured under `architecture.benchmark.maintenance_settle`.
- **Customer 360 expected-result checks.** Bronze-verify and gold-finalize
  log facts that `metrics/c360_correctness.py` checks against the
  generator's semantics (bronze rows equal to the rows datagen was sized to
  write, bronze to silver rows, KPI identities, benchmark row counts); the
  verdict is recorded in metrics.json as `c360_correctness`. A missing or
  failed fact collection is unknown, never a pass.
- **Delta silver is clustered by `interaction_date` before the write.** It
  wrote about one file per task per day (tasks x 366); Q3/Q6 on
  hive-delta-spark-thrift exceeded the 300 s timeout. Set
  `spark.lb.silver.distribution_mode=none` for the old layout.
- **Delta table-health file counts** come from `DESCRIBE DETAIL numFiles`
  on Spark Thrift; on Trino they are reported as unavailable. A Delta file
  count never measures a compaction.
- `config show` and `info` report the peak requested resources from
  `compute_peak_requirements()` plus co-resident services (at scale 1 the
  pipeline alone peaks at 36 cores / 512 GB); `info` names the workload by
  pipeline mode and says what the datagen mode means for a continuous run.
  `recommend` never sizes Spark below the peak request.

### Added
- **`lakebench admin` subcommand tree.** Cluster admins run one-time setup
  (`install-spark-operator`, `install-scratch-storage-class`) before
  developers can `deploy`. Also `status`, `doctor`, `release-lock`,
  `migrate-deployment` (for legacy pre-ownership namespaces),
  `repair-operator` (reconciles the Spark Operator watch list and the
  controller `/tmp` size), and `reclaim-bucket`. Every mutating admin
  command acquires the cluster-wide `lakebench-cluster-lock` lease.
- **Spark Operator controller `/tmp` sizing.** spark-submit runs in the
  operator controller and resolves `spark.jars.packages` into `/tmp/.ivy2`;
  the chart's 1Gi `/tmp` emptyDir is smaller than one Spark line's jars, so
  the kubelet evicted the controller repeatedly under load and every
  tenant's submissions failed and retried. `admin install-spark-operator`
  and `admin repair-operator` set the controller `/tmp` to 8Gi
  (`--controller-tmp-size`, floor 4Gi) under the cluster lease and verify
  it; `--dry-run` shows the plan. `admin doctor` and `admin status` report
  the size and current storage evictions. Installing over an existing
  release keeps the tenants' watch lists and the installed chart version.
- **Shared-cluster ownership discipline.** Every deployment carries an
  identity: a `lakebench.deployment/name` annotation on its namespace, a
  per-deploy nonce, and a matching `lakebench.deployment` tag on each of its
  S3 buckets where the store supports tagging. Destroy and clean verify
  identity before mutating; a foreign stamp is a hard refusal. Cluster-scoped
  resources (Stackable `SecretClass`) are named per deployment. See
  `docs/design/namespace-isolation.md`.
- **`--force-legacy` on `deploy`, `clean` and `destroy`;
  `--allow-unverified-cluster` on `destroy`.** Explicit escape hatches for
  pre-ownership deployments. `destroy` refuses annotation-less namespaces
  and untagged buckets by default; run `lakebench admin migrate-deployment
  <namespace>` first, or pass `--force-legacy` once you have confirmed the
  resource is yours. Foreign stamps are always refused.
- **`lakebench reproduce`.** Records a reproduction package from a run and
  verifies later runs against it with per-metric direction tables and
  tolerance bands. Exit codes 0/1/2 distinguish pass / performance drift /
  correctness drift. Packages carry the experiment identity and maintenance
  policy.
- **Performance regression gate.** `benchmarks/perf/` holds pinned configs
  (c360 batch s10, c360 continuous s10, AML batch s1) and
  `baselines.yaml`; `scripts/perf_gate.py` compares a run with its baseline
  and refuses runs that are not like-for-like (experiment identity,
  maintenance policy, stage timing basis, result fingerprints). The release
  gate gains a local `perf-baselines` check. The checked-in baselines are
  legacy and refuse until re-recorded.
- **AML workload (financial crime / transaction monitoring).** Rust
  generator schema `financial` with a monitored population of one reporting
  bank, minimal KYC and customer risk rating, planted typologies with a
  ground-truth manifest, and the sanctions/PEP screening track; detection
  rules scored for recall and precision against planted instances; a
  tracked fidelity gate (one feature definition in
  `spark/scripts/aml_features.py` feeding a pre-registered reference model,
  scored per customer and UTC month, with a leakage check); per-rule alert
  counts and errors in metrics.json (`alerts_by_rule`, `rule_errors`); and
  `lakebench financial replay` / `reproduce`. See `docs/aml-scoring.md`.
- **TM operations layer for AML.** After detection, `tm_operations.py`
  writes `tm_reconciliation`, `scenario_coverage`, `alert_dispositions` and
  `cases` to gold, with dispositions simulated from the datagen ground truth
  at a configured analyst and investigator accuracy (truth is rule-aware:
  a sanctions or PEP hit is true only for W5/W6 alerts). A violated workflow
  invariant fails the run; a layer that could not run is reported as
  `not_run`. Configured under `workload.tm_operations`.
- **AML continuous: time to detect.** `time_to_detect_seconds` (median),
  `_p95_seconds`, `_max_seconds`, `_alerts`, `_late_alerts` and
  `_unmeasured_cycles` on AML continuous scorecards, from the newest bronze
  ingest of an alert's transactions to the end of the detection pass that
  first raised it. `intake_limit` and `bronze_busy_fraction` say whether
  bronze's own processing bounded intake.
- **DuckDB runs all AML analytical and investigator queries.**
- **Per-file coverage floors** on the scoring, metrics and detection code
  (`scripts/check_coverage.py`), run in CI, plus a gitleaks tree scan.
- **Root `--version` / `-V` flag.**
- **`--regenerate` on `run --generate` and `generate` (A4).** A non-empty
  bronze prefix is now refused by the CLI unless `--regenerate` is passed
  (exit 2 with the prefix, object count and size named); with the flag, the
  whole bronze bucket is emptied via `S3Client.empty_bucket()` (which also
  aborts dangling multipart uploads on FlashBlade) before datagen submits.
  Before, `lakebench generate` and `run --generate` deployed datagen
  straight onto whatever was in bronze and the deployer's LB-185 clear
  covered only buckets this deployment recorded creating.

### Changed
- Datagen image `lb-datagen:1.6.0` (LB-204): AML datagen pod memory at scale
  100 falls from 18.18 GiB to 5.70 GiB (mimalloc allocator, typology rows kept
  only for each pod's own files, world columns recomputed on demand). Output is
  byte-identical to the v1.6 AML generator freeze (seed-43 byte-compare on the
  pushed image; pinned in `datagen_rs/tests/cycles.rs`). The autosizer memory
  model is re-fit to cluster measurements and a datagen pod never requests more
  than 16Gi; AML above scale 100 runs at least 8 datagen pods.
- **Datagen: the Python image is retired; the Rust image serves both
  schemas** (`datagen_rs/`, `--schema customer360` and `--schema
  financial`). Per-pod throughput on customer360 measured at 590 MB/s
  (snappy, 8 CPU / 8 Gi, n=1), against 6-8 MB/s for the Python path. The
  default codec is `snappy` (was `zstd1`); override with
  `DG_COMPRESSION=zstd`, `lz4` or `none`. An unset `datagen.cpu` is 8 in
  both modes, and an unset `datagen.memory` is derived from a measured
  peak-RSS model for the schema, scale, thread count and file size, with a
  4 Gi floor (continuous pods were fixed at 24 Gi).
- **Higher cluster minimum for AML continuous.** Under `schema: financial`
  bronze-ingest runs 5 executors x 4 cores, silver-stream 10 x 4 and
  gold-refresh 12 x 4, sized from scale-10 runs where the smaller streams
  fell behind. The full request is 118 cores / 980 GB / 2,300 Gi scratch at
  scale 1-10 and 222 cores / 1,948 GB / 4,660 Gi at scale 100. In continuous
  mode a smaller cluster runs degraded with a WARNING naming the capped
  stages when the capped request fits (AML scale 1-10: 57 cores), and fails
  preflight only when it does not. Stream stages report the executor
  count actually granted, so core-hours are right on capped clusters.
- **AML bronze-verify sizing (LB-118).** Under `schema: financial`
  bronze-verify gets 500Gi scratch per executor, `executors_per_100_scale: 8`
  and `max_executors: 28`; the capacity preflight uses the AML profile.
- **Continuous runs held to the trickle are not saturation (LB-156).** A run
  whose bronze ran a micro-batch on at least 90% of the window's triggers,
  inside each trigger, with silver keeping up, reports
  `intake_limit: trickle_rate`, `pipeline_saturated: false` and
  `corpus_drain_seconds` (the window that would drain the corpus at the rate
  held). Stream stages carry `batch_span_seconds`.
- **`ScratchStorageConfig.create_storage_class` removed.** The StorageClass
  is shared infrastructure; `deploy` verifies it exists and points at
  `lakebench admin install-scratch-storage-class`. YAML that still carries
  the key loads with a warning.
- **Spark Operator watch-list mutation is lease-gated in strict mode.** If
  removing the namespace from the watch list fails, destroy raises
  `WatchListMutationError` and does not delete the namespace (a deleted
  watched namespace crash-loops the operator for every tenant); run
  `lakebench admin repair-operator`.
- **`LB_FINANCIAL_BRONZE_PREFIX` is the datagen root on every AML reader
  (LB-165).** `bronze_ingest_financial` read it as the inner path; a caller
  that set `bronze/pacs008/` there now reads zero rows. Set it to the
  datagen root, or set `LB_FINANCIAL_PACS_PATH`.
- **DuckDB probes** get realistic timeouts (startup 15 s, readiness and
  liveness 10 s); the 1 s default restarted the container at random.
- **SparkApplication status reads** time out after 30 s and retry on
  timeouts, resets, 429 and 5xx; the monitor logs slow reads and stalls.
- Dependency floors for click and jinja2 raised past known CVEs.

### Removed
- `platform.storage.scratch.create_storage_class` (see Changed).
- Checkpoint resume for data generation. The Rust generator does not
  implement it. The `--resume` CLI flag and the `workload.datagen.checkpoint.*`
  config block are removed. Old configs that carry `datagen.checkpoint:`
  load with a `DeprecationWarning` and the block is dropped from the
  loaded config. An interrupted `lakebench generate` re-runs from the
  start and needs `--regenerate` to empty the partial bronze data first.
- `workload.datagen.uploaders`. Never forwarded to the Rust generator;
  uploader concurrency is fixed inside the S3 sink. Old configs that
  carry the field load with a `DeprecationWarning` and the field is
  dropped from the loaded config.
- The Python datagen (`datagen/`) and its image.
- Packaged `sanctions_list.json` and `pep_list.json` (replaced by the
  per-corpus watchlist).

### Fixed
- **Datagen timeout on `run --generate` no longer prints "Datagen
  completed" (A4).** The wait loop's timeout fell through to a success line
  even when the datagen Job was still running; the follow-up pipeline
  stages then built on a partial bronze (invariant 3). The run now exits
  with a distinct code (`4`), stops the datagen Job so it stops writing,
  and deletes any leftover streaming SparkApplication
  (`bronze-ingest`, `silver-stream`, `gold-refresh`) that was consuming
  the trickle so the timed-out generate does not leave orphan compute
  behind.
- **Spark Thrift `s3://` table locations** map to S3A, so Polaris orphan
  removal can open them, and the Thrift server sets `fs.s3a.endpoint.region`
  like the Spark jobs (LB-052). A failed orphan removal is no longer stamped
  as maintenance that ran.
- **`validate` failed on every example config before deploy** ("Spark
  Operator does not watch namespace"); the check is advisory before the
  first deploy.
- **The continuous monitoring loop busy-spun** (0 s sleeps) when an
  in-stream benchmark round could not fit the time left (LB-180).
- **The destroy table step says what it did** when no engine can drop
  tables, and a re-run after the buckets were emptied no longer fails on a
  Delta table whose directory is gone.
- **The continuous reset** deletes the owned Delta bronze path when the table
  is not in the catalog, so an interrupted reset no longer wedges the
  deployment.
- **`list_runs` ordered runs by timestamp text**, so with mixed local and UTC
  timestamps a UTC+X host picked an older run as the latest; it now orders
  by instant.
- **LB-146: Trino coordinator OOM-killed under load.** `-Xmx` equalled the
  container memory limit, so heap plus native memory exceeded the cgroup
  limit (exit 137). The coordinator and worker heaps are now 80% of the pod
  memory limit; pod limits are unchanged.
- **LB-148: hive-delta-spark-thrift failed 5 of 8 c360 queries.** The
  delta-spark `ClassCastException` on MIN/MAX of the date column (LB-034,
  Q2 and Q6) is worked around with
  `spark.databricks.delta.optimizeMetadataQuery.enabled=false` for Delta +
  Hive, in the Thrift server and the Spark jobs; Q2 on Delta + Thrift is no
  longer tolerated as a known failure. The Thrift pod limit is now heap +
  max(10% of heap, 1 GiB). Delta + Hive + Thrift defaults to 8 cores / 16g
  heap when unset (fitted down on small nodes); Iceberg keeps 2 / 4g.
- **LB-147: W3/W17 path search failed at AML scale 10** after executor
  loss (`CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND`). Path levels are written as
  Parquet under the gold bucket instead of local checkpoints, the step
  frames persist to disk only, and the join is partitioned by edge count.
  Alerts are identical to the previous code on the test graphs.
- **LB-145: continuous freshness grew with wall clock** once a finite
  corpus had drained. The trailing idle gold cycles of a drained run are
  left out of freshness; any other idle cycle still counts as a stall.
- **LB-144: silver overestimated c360 customers** (14.7M at scale 10 where
  there are 1M). It now uses a Chao1 estimate over the sample.
- **LB-141: maintenance value reported when compaction changed nothing.**
  It is reported only when compaction changed the file count, and no
  compaction ratio is recorded when the post file count is unknown.
- **LB-149: destroy kept the namespace** when FlashBlade listed a finished
  multipart upload and the abort raised `NoSuchUpload`; that is now
  treated as done.
- **c360 continuous writers are exactly-once** across a driver restart,
  bronze-verify fails on columns silver or gold compute on, and Q6 recency
  uses the data clock.
- **LB-117: AML analytical query QpH was unstable** (an iteration read 0.0
  when Spark Thrift ran out of memory). The AML Spark Thrift memory target is
  24g, and the benchmark `query_timeout` for `schema: financial` is 900 s
  both before and after compaction (300 s before it for other schemas). On clusters with under 36 GiB
  allocatable the target is `max(4, min(20, allocatable - 8))g`.
- **LB-118: AML bronze-verify ran out of scratch disk at scale 5 and above**
  on its CTAS fallback. See the AML bronze-verify sizing under Changed.
- **LB-112: batch AML never ran the detection rules** and wrote an empty
  `gold.alerts`. gold-finalize now runs every scheduled rule after the
  baseline dashboards, cheapest first, with per-rule error isolation and a
  delete-then-insert per rule so a re-run gives reproducible counts. The
  per-job timeout gains 900 s under `schema: financial`.
- **LB-113: Spark Thrift's 4g default ran out of memory on every AML query
  at scale 1.** The autosizer raises it for `schema: financial` when the
  field is at its default, capped to what the largest node can hold. An
  already-deployed Thrift pod needs destroy and deploy to pick it up.
- **LB-114: W1 connected components failed with an ambiguous-column error**
  on the second label-propagation iteration. The evidence now carries
  `converged=true|false`.
- **LB-115: W7 crashed in the driver on every run** (no `silver.entities`
  passed, and the AML reference data could not be found inside the Spark
  image). The reference data ships in the scripts ConfigMap, W7 loads
  `silver.entities` itself, and its country choice per entity is
  deterministic.
- **LB-165: the AML pipeline could not read datagen_rs output.** The bronze
  readers read a flat `pacs008/` layout; datagen_rs writes
  `bronze/pacs008/`, `bronze/party.parquet`, `bronze/account.parquet` and
  `manifest/manifest.parquet`. Both readers now derive the pacs.008 path
  from the datagen root (`LB_FINANCIAL_PACS_PATH` overrides it), and
  bronze-verify registers `bronze.manifest`, which the ad-hoc AML scoring
  query templates read (no QpH query reads bronze).
- **LB-164: FlashBlade does not implement bucket tagging.** Deploy and
  destroy fall back to name ownership with longest-prefix-wins against the
  other lakebench deployments on the cluster, refuse when those cannot be
  listed, and now also require the namespace's bucket record (see Behaviour
  changes). `lakebench config storage` reports bucket tagging as an advisory
  check.

## [1.5.0] - 2026-09-16

Hardening release built on mandatory adversarial review: 22 bugs fixed
(LB-072 to LB-093). Reconstructed from the published release notes.

### Changed
- Polaris `client_secret` is required; no hardcoded default (LB-090).
- Wait diagnostics fail fast on terminal Waiting reasons, with a
  3-restart debounce for CrashLoopBackOff (LB-091).
- Local-mode `ContainerRuntime.apply()` fingerprints the whole container
  spec, so env, mount or port changes recreate the container (LB-092).
- Storage conformance passes `ca_cert` / `verify_ssl` through (LB-075).

### Fixed
- Destroy paths that reported success on failure, an S3 client that
  returned silently on timeout, metrics computed as 0 when the
  denominator was unknown, and sustained runs that passed with 0 rows.
- Shared-cluster races on Stackable SecretClass, the scratch
  StorageClass and OpenShift SCC bindings (refcounted, read-modify-write).
- Dead config fields with plausible defaults that nothing read.

## [1.4.0] - 2026-07-29

Local mode, component version refresh, storage conformance.
Reconstructed from the tag message and repository history.

### Added
- `lakebench config storage`: graded S3 backend conformance checks
  (FlashBlade and Garage validated; SeaweedFS refused).
- Local mode (single-host container runtime).

### Changed
- Polaris 1.6.0, Spark Operator 2.5.1, kube-prometheus-stack chart pinned
  (87.19.2).
- S3A sets `fs.s3a.endpoint.region` on every job (LB-052).
- Documented cluster minimums come from `compute_peak_requirements()`
  (LB-050).
- Iceberg runtime chosen by Spark and Iceberg version; Iceberg 1.11 with a
  Java 11 Spark image is refused at config time.

## [1.3.1] - 2026-04-05

### Fixed
- **LB-049: silver-build at scale 100 never completed.** Two compounding root
  causes. First, the STREAMING strategy wrote with
  `write.distribution-mode=none`, producing `tasks x partitions` files (2286 x
  365 = 836K at scale 100). The Hive Metastore commit of 836K files timed out
  before returning. Second, Spark 4.0.2's `AppStatusListener` does an
  `O(liveTasks)` flush on every executor heartbeat, which stalled the driver
  at 5000+ tasks and starved the task scheduler. Fix: changed STREAMING
  default from `distribution-mode=none` to `hash` (Iceberg now clusters rows
  by `interaction_date` before writing, producing ~9K files and committing in
  340ms). Added `spark.lb.silver.distribution_mode` conf override for 5TB+
  scale testing. Also reduced `target_tasks` from 5000 to 2000, added
  `spark.ui.liveUpdate.minFlushPeriod=30s` and retained state caps, and
  disabled the Spark UI (prevents Jetty overhead -- listener still runs but
  the flush is throttled). Tested A/B/C/D on the live cluster at scale 100:
  hash distribution passed in 466s; `none` with fanout still produced 800K+
  files and OOMed; narrowed date range (30 days) passed but is a workaround,
  not a fix.
- **Prerequisite tick rendering.** `_run.py` replaced unicode tick/cross
  (`\u2713`/`\u2717`) with ASCII `+`/`x` to match the `check` command style.

## [1.3.0] - 2026-03-27

### Added
- **Config Schema v2.** Flat top-level fields (`endpoint`, `access_key`,
  `secret_key`, `scale`, `namespace`, `mode`, `cycles`, `spark_image`) for
  minimal 4-line configs. Environment variable substitution with `${VAR}` and
  `${VAR:-default}` syntax. Auto-generated deployment names persisted to
  `.lakebench/state.json`.
- **`compare` command.** Run two configs sequentially and display side-by-side
  scorecard comparison. Supports `--format` (table/json/csv) and `--output`.
- **`config` subcommands.** `config show` (resolved config with source
  annotations), `config validate`, `config recommend`, `config upgrade`
  (v1.2 nested -> v1.3 flat format).
- **Maintenance cost metrics.** 11 new scoring fields: pre/post compaction
  file counts, compaction ratio, maintenance elapsed time, pre/post compaction
  QpH, and maintenance value percentage. Run flow changed to measure QpH
  before and after maintenance.
- **Prerequisite detection.** 8-check engine (kubectl, helm, K8s cluster, S3
  config, S3 connectivity, Spark Operator, Stackable operators, namespace)
  with actionable error messages. Runs as Phase 1 of the 7-phase run flow.
- **7-phase run output.** Progress headers (Phase 1/7 through 7/7) for
  Prerequisites, Infrastructure, Generate, Pipeline, Maintenance, Benchmark,
  Results.
- **Run command flags.** `--skip-preflight` (skip prerequisite checks),
  `--skip-generate`, `--skip-maintenance`, `--deploy-only`, `--generate-only`,
  `--yes/-y`. With `--yes`, `run` auto-deploys if infrastructure is missing.
- **Init quick mode.** Default 2-step wizard (endpoint/keys + review).
  `--advanced` for full 5-step wizard with recipe and mode selection.

### Changed
- `cli.py` (6,181 lines) converted to `cli/` package with sustained helpers
  extracted to `cli/_sustained.py` (1,279 lines).
- `deploy/engine.py` reduced from 1,682 to 859 lines by extracting
  `destroy_all()` to `deploy/destroy.py`.
- All component implementations extracted to `modules/` package:
  query engines, catalogs, table formats, pipeline engine.
- Module Protocol interfaces (`CatalogModule`, `QueryEngineModule`,
  `PipelineEngineModule`, `TableFormatModule`) and `ModuleRegistry`.
- Old commands (`validate`, `info`, `recommend`) marked deprecated in favor
  of `config` subcommands.
- Polaris bootstrap template: pinned `curlimages/curl:latest` to `8.11.1`.

### Fixed
- `config/scale.py`: `compute_guidance()` mode field changed from
  `"continuous"` to `"sustained"` (advisory only, not Docker image mode).
- **Sustained mode (LB-044).** `bronze_ingest.py` created Iceberg tables
  using Hive Metastore default warehouse (`file:/stackable/warehouse/`)
  instead of S3, causing crash-loops. Fixed with explicit S3 table location.
  Sustained streaming now fully functional across all Iceberg recipes.
- **QpH calculation (LB-042).** Power and throughput QpH counted failed
  queries as successful. 8 failed queries in 14s reported QpH 1,950.
  Now only counts `result.success == True`.
- **Pre-compaction benchmark at scale (LB-041).** Added per-query progress
  (`[1/8] Query name... 12.3s OK`), file count check (skips pre-compaction
  at >200K files), and 60s timeout for pre-compaction queries.
- **Delta+Thrift maintenance (LB-043).** VACUUM OOMs Spark Thrift at 4Gi.
  Maintenance now skipped for Delta+Spark Thrift combinations.
- **DuckDB deploy timeout (LB-033).** Increased from 600s to 900s for
  parallel deployments.
- **`lakebench results` (LB-039).** Now accepts optional config file argument.
- **`query --file` duplicate (LB-027).** SQL file input renamed to `--sql-file`.
- **`config recommend` crash (LB-022).** Passed config Path as int parameter.
- **Iceberg compat matrix (LB-026).** Removed Iceberg 1.7.1-1.9.1 for Spark
  4.0/4.1 (Maven artifacts only exist for 1.10.0+).
- **Deploy timeouts (LB-023).** Hive/Polaris increased from 300s to 600s.
- **SecretClass race (LB-024).** Parallel deploys race on cluster-scoped
  resource creation; now catches 409 AlreadyExists.
- **`--generate-only` (LB-025).** Missing `yes=yes` passthrough.
- `run --generate` now shows Rich progress bar with pod count (ENH-001).
- `run` auto-deploys when namespace missing and `--yes` set (ENH-002).
- `--skip-deploy` renamed to `--skip-preflight` (alias kept) (ENH-003).
- Deploy tip updated from deprecated `lakebench validate` to
  `lakebench config validate` (ENH-004).
- DuckDB no longer shows misleading maintenance value (ENH-005).
- Empty Phase 5/7 and 6/7 headers now say "Skipped" for `--skip-benchmark`
  and `--skip-maintenance` (ENH-006).
- Trino worker memory: performance tier (scale 51-500) bumped from 32Gi to
  48Gi per worker.
- Pipeline stage heartbeat: elapsed time printed every 60s during long jobs.

## [1.2.0] - 2026-03-26

### Added
- **Delta Lake support.** Three new recipes: `hive-delta-spark-trino`,
  `hive-delta-spark-thrift`, `hive-delta-spark-none`. Delta pipeline scripts
  (`bronze_ingest_delta.py`, `silver_build_delta.py`, `silver_stream_delta.py`,
  `gold_refresh_delta.py`, `gold_finalize_delta.py`) mirror the Iceberg variants.
  Delta maintenance helpers in `deploy/delta_maintenance.py` (VACUUM, OPTIMIZE,
  DESCRIBE DETAIL, DROP TABLE). Pre-benchmark OPTIMIZE is skipped for Delta+Trino
  and Delta+Spark Thrift to avoid OOM.
- **Spark 4.1.x support.** `_SUPPORTED_SPARK_VERSIONS` now includes `(4, 1)`.
  `_ICEBERG_RUNTIME_SUFFIX` dict maps `(4,1) -> "4.0"` because Iceberg has no
  `4.1_2.13` runtime artifact on Maven Central. `_delta_spark_artifact()` handles
  Delta 4.1.0's new Maven coordinate naming (`delta-spark_4.1_2.13`).
- **Spark 3.5.x support.** `(3, 5)` added to `_SUPPORTED_SPARK_VERSIONS`.
  Only Iceberg is supported with Spark 3.5 (Delta 4.x requires Spark 4.x).
- **Config-time format version auto-resolution.** `LakebenchConfig` model_validator
  calls `resolve_format_version()` at load time. `DeltaConfig.version` defaults to
  `"auto"` (previously `"4.0.0"`), resolving to the correct version for the Spark
  image in use. Incompatible combinations (e.g. Delta 4.0.0 + Spark 4.1) are
  rejected at config load with a clear error message.
- **`PipelineEngine` protocol.** `src/lakebench/engine/protocol.py` defines a
  `@runtime_checkable` Protocol with `engine_name`, `submit_job`,
  `wait_for_completion`, `get_logs`, `cancel_job`. `get_engine(cfg, k8s)` factory
  returns the appropriate engine (currently only `SparkJobManager`). Wired into
  `cli.py` so `run` goes through the factory rather than instantiating
  `SparkJobManager` directly.
- **Unity Catalog infrastructure.** `deploy/unity.py` and `templates/unity/`
  deploy OSS Unity Catalog as a K8s Deployment with PostgreSQL backend.
  Unity + Delta excluded from v1.2 (STS issue on non-AWS S3, planned for v1.3).
  Unity + Iceberg excluded (v0.4.0 REST API is read-only).
- **`docs/compatibility-matrix.md`.** Spark + format version matrix for user reference.
- **`examples/` directory.** Hardened example config YAMLs for all 11 recipes.

### Changed
- `DeltaConfig.version` default changed from `"4.0.0"` to `"auto"` to prevent
  cross-version failures when switching Spark images.
- `_FORMAT_VERSION_COMPAT[(4,1)]["delta"]` changed from `["4.0.0", "4.1.0"]` to
  `["4.1.0"]` -- Delta 4.0.0 is not compatible with Spark 4.1.
- PostgreSQL authentication changed from MD5 to SCRAM-SHA-256 (`pg_hba.conf` and
  `password_encryption = scram-sha-256`).
- `cli.py` batch and sustained paths use `get_engine()` factory instead of
  instantiating `SparkJobManager` directly.

### Fixed
- **Iceberg runtime artifact for Spark 4.1.** `iceberg-spark-runtime-4.1_2.13` does
  not exist on Maven Central. Spark 4.1.x must use the `4.0` runtime. Fixed with
  `_ICEBERG_RUNTIME_SUFFIX` dict in `spark/job.py` and `deploy/engine.py`.
- **Delta 4.0.0 incorrectly allowed with Spark 4.1.** The format version compat
  matrix previously listed `4.0.0` as compatible with Spark `(4,1)`. Live cluster
  UAT showed silver-build fails at runtime. Fixed by removing `4.0.0` from the
  Spark 4.1 delta compat list.

### Known Issues
- **Delta + Spark Thrift Q2 ClassCastException.** `OptimizeMetadataOnlyDeltaQuery`
  throws `ClassCastException: java.time.LocalDate cannot be cast to java.sql.Date`
  on MIN/MAX date partition queries. Q2 is skipped for Delta+Spark Thrift.
- **Unity + Delta** excluded. `UCSingleCatalog` 0.4.0 calls
  `generateTemporaryTableCredentials` (STS) for managed tables. Non-AWS S3 has
  no STS endpoint. Planned for v1.3.

## [1.1.0] - 2026-03-05

### Added
- **Iterative batch cycles.** New `pipeline.cycles` config field (1-50) runs N
  batch iterations where cycle 1 is full overwrite and cycles 2-N are incremental
  append/merge, simulating multi-day lakehouse behavior. Per-cycle datagen splits
  the timestamp range evenly across cycles.
- **Iceberg compaction.** New `build_compaction_sql()` in `deploy/iceberg.py`
  runs `optimize` (Trino) or `rewrite_data_files` (Spark Thrift) to merge small
  files. Pre-benchmark maintenance (`pipeline.pre_benchmark_maintenance`, default
  true) runs compaction + expire before QpH measurement. Sustained mode runs
  compaction on a separate timer (`sustained.compaction_interval`, default 2x
  retention_interval).
- **Table health tracking.** `BenchmarkRoundMeta` now captures
  `silver_data_file_count`, `silver_snapshot_count`, `gold_data_file_count`,
  `gold_snapshot_count` from Iceberg system tables at each benchmark round.
- **QpH degradation metric.** `qph_degradation_pct` in sustained scores
  compares first-half vs second-half median QpH across in-stream benchmark
  rounds (requires 4+ rounds). Positive = degradation.
- **`CycleMetrics` dataclass.** Per-cycle metrics including datagen timing,
  job metrics, benchmark results, and table health snapshot.
- **`cycle_progression` batch score.** When `cycles > 1`, the scores dict
  includes per-cycle elapsed time, QpH, and table health.
- **`query_sql()` in `deploy/iceberg.py`.** Variant of `exec_sql` that returns
  stdout for table health queries.
- **`build_table_health_sql()` in `deploy/iceberg.py`.** Generates queries for
  Iceberg `$files` and `$snapshots` system tables.
- **Interactive init wizard.** `lakebench init` now launches a 5-step guided
  wizard by default (identity, recipe, storage, workload, review). Includes
  Rich-formatted panels, inline S3 connectivity validation, config preview
  with syntax highlighting, and back-navigation between steps. Use
  `--no-interactive` for scripted/CI usage. Passing `--endpoint`/`--access-key`/
  `--secret-key` flags auto-skips the wizard.

### Changed
- Spark scripts (`silver_build.py`, `gold_finalize.py`) support incremental
  mode via `LB_SILVER_INCREMENTAL` and `LB_GOLD_INCREMENTAL` env vars.
- `submit_job()` in `spark/job.py` accepts optional `cycle_env` dict for
  per-cycle environment variable injection.
- `DatagenDeployer` gains `deploy_cycle()` method for per-cycle timestamp
  window and scaled-down data volume.

### Fixed
- **Spark Thrift benchmark Q2 and Q6 returning 0 rows.** Benchmark queries use
  Trino SQL as the canonical dialect.  Q2's `date_add('month', 3, expr)` and
  Q6's `DATE_DIFF('day', start, end)` are Trino-specific 3-arg forms that
  Spark SQL silently misinterprets.  `SparkThriftExecutor.adapt_query()` now
  rewrites these to `add_months()` and `DATEDIFF()` respectively.
- **DuckDB benchmark returning 0 rows for all queries.** Three issues:
  (1) `DuckDBExecutor.adapt_query()` never provided the S3 warehouse location
  to `iceberg_scan()` -- now rewrites `catalog.namespace.table` references to
  `iceberg_scan('s3://bucket/warehouse/namespace.db/table')` with Hive's `.db`
  directory convention, `allow_moved_paths`, and `unsafe_enable_version_guessing`.
  (2) Multi-line SQL from benchmark queries broke the `python -c` one-liner
  with an unterminated string literal -- now collapsed to a single line via
  `" ".join(sql.split())` before embedding in the script.
  (3) Q2's Trino `date_add('month', 3, expr)` unsupported by DuckDB -- added
  `_rewrite_date_add()` that translates to `(expr + INTERVAL 3 MONTH)`.
  (4) DuckDB pod startup installed the Python package but not the `iceberg`
  and `httpfs` extensions -- updated `deployment.yaml.j2` startup command
  to `INSTALL` both extensions and startup probe to verify they load.
- **Streaming log parser missing empty batches.** Silver and gold streaming
  jobs log `"Batch N: empty, skipping"` when a micro-batch has no data, but
  `parse_streaming_logs()` only matched data-carrying patterns.  Empty silver
  and gold batches are now counted (bronze empty batches remain excluded).
- **Table health probe always returning -1.** Two bugs: (1) `build_table_health_sql`
  wrapped the full qualified name in double quotes (`"catalog.schema.table$files"`),
  which Trino interprets as a single identifier with literal dots -- fixed by
  quoting only the table segment (`catalog.schema."table$files"`).
  (2) `_probe_table_health()` used `isdigit()` to extract counts from output,
  which fails on padded lines -- switched to regex-based extraction.
  (3) Trino CLI wraps scalar results in double quotes (`"91"`); the parsing
  now strips quotes before numeric matching.
- **CycleMetrics not populated in multi-cycle batch runs.** The CLI cycle
  loop referenced `collector.cycles` (non-existent) instead of
  `collector.current_run.cycles`.  Also, `build_pipeline_benchmark()` was
  not passing cycles from the run to the benchmark object.

## [1.0.12] - 2026-03-04

### Added
- **Iceberg retention for sustained pipelines.** Periodic `expire_snapshots` +
  `remove_orphan_files` during the sustained monitoring loop. New config fields
  `sustained.retention_interval` (default 1800s) and
  `sustained.retention_threshold` (default `30m`).
- **Engine-aware Iceberg maintenance.** Maintenance operations (sustained loop
  and destroy path) now work with Trino or Spark Thrift Server. DuckDB is
  read-only and skipped. Shared helpers in `deploy/iceberg.py`.
- **Timestamp range documentation.** Config YAML and user docs now document
  the impact of `timestamp_start`/`timestamp_end` on Iceberg partition count
  and sustained-mode small-file proliferation.
- **`total_s3_objects` sustained score.** Counts total S3 objects across
  bronze/silver/gold buckets at end of run. Signals whether retention is
  keeping pace with snapshot growth.

### Fixed
- **S3 bucket creation during deploy.** `deploy_all()` now creates S3 buckets
  (bronze, silver, gold) when `create_buckets: true` (the default). Previously
  the config field existed but was never wired into the deploy engine, causing
  datagen to fail with a 404 on HeadBucket on fresh deployments.
- **Datagen Phase 2 race condition in duration mode.** Generator processes
  exited on empty queue before the main loop could feed Phase 2 file IDs,
  causing sustained pipelines to receive no new data after the initial burst.
  Generators now wait for the explicit poison pill in duration mode.
- **Trino coordinator label selector in Iceberg maintenance.** Both the
  sustained monitoring loop and the destroy path used a wrong label selector
  (`app=lakebench-trino-coordinator` instead of
  `app=lakebench-trino,component=coordinator`), causing maintenance to
  silently skip every cycle.

### Changed
- **Datagen image switched to `:latest` tag.** Default image is now
  `docker.io/sillidata/lb-datagen:latest` (was `:v3`). Default `pull_policy`
  changed from `IfNotPresent` to `Always` so pods always pull the current image.

## [1.0.11] - 2026-03-02

### Added
- **HTTPS S3 endpoint support with self-signed CA certificates.** New config
  fields `platform.storage.s3.ca_cert` (path to PEM certificate) and
  `platform.storage.s3.verify_ssl` enable HTTPS for all components. At deploy
  time, the PEM content is embedded into a Kubernetes Secret. JVM components
  (Spark, Trino, Polaris, Hive) get an init container that imports the CA
  into a JKS truststore. Python components (datagen, S3 client) pass the PEM
  path to boto3. Supports both self-signed CAs (FlashBlade) and public CAs
  (AWS S3). See [Configuration Reference](docs/configuration.md#example-https-endpoint-with-self-signed-ca).
- 21 new tests for HTTPS support: config field validation (3), Spark
  truststore manifest (2), DuckDB SSL conditional (2), and template rendering
  across all component categories (17 -- spark-thrift, trino, polaris, hive,
  datagen, secrets).
- Datagen `--duration` mode for sustained pipelines. Generators now run for
  the configured `run_duration` instead of exiting after a finite file count.
  Fixes the 5TB scale-test blocker where datagen pods exited in 21 minutes,
  leaving the streaming pipeline starved. Requires datagen image v3.
- `lakebench recommend --mode sustained` computes concurrent resource budget
  (datagen + streaming + trino running simultaneously).
- `lakebench recommend --slow-datagen` replaces deprecated `--extended`.
- 8 new tests: Polaris Spark manifest coverage (4) and sustained scoring
  edge cases (4).

### Changed
- Default datagen image bumped from `lb-datagen:v2` to `lb-datagen:v3`.
- `lakebench info` shows streaming executor counts, trigger intervals, and
  run duration when config uses sustained mode.
- `lakebench recommend` output includes pipeline mode label and mode-specific
  resource breakdown.
- String literal pipeline mode checks replaced with `PipelineMode` enum values.
- Jinja2 template renderer uses `StrictUndefined` -- undefined variables now
  raise errors instead of silently producing empty strings.
- `_MAX_EXECUTORS_SAFE = 28` module constant replaces raw integer in job profiles.
- Polaris templates use `{{ polaris_image }}` from `ImagesConfig` instead of
  hardcoded image tags.

### Fixed
- **Datagen `verify=False` unconditionally disabled SSL.** `datagen/generate.py`
  hardcoded `verify=False` for all custom endpoints, silently disabling SSL
  certificate verification even on HTTPS endpoints. Now uses `S3_CA_CERT` and
  `S3_VERIFY_SSL` environment variables for conditional verification.
- **Spark Thrift `ssl.enabled=false` hardcoded.** The Spark Thrift Server
  template hardcoded `spark.hadoop.fs.s3a.connection.ssl.enabled=false`. Now
  conditional on the endpoint scheme (`{{ s3_use_ssl | lower }}`).
- **DuckDB `s3_use_ssl=false` hardcoded.** The DuckDB executor always disabled
  SSL regardless of the endpoint scheme. Now conditional on whether the
  endpoint starts with `https://`.
- **Spark Operator v2.4.0 volume injection.** ConfigMap volumes from
  `.spec.volumes` / `.spec.driver.volumeMounts` are not injected into driver
  pods by the v2 webhook. Fixed by using `driver.template` /
  `executor.template` pod templates for ConfigMap volumes and
  `spark.kubernetes.*.volumes.emptyDir.*` conf properties for emptyDir volumes.
- `destroy_all()` no longer swallows exceptions as SUCCESS. Missing resources
  report SKIPPED; real errors report FAILED with full traceback logging.
- Deployer exception handlers now include `logger.exception()` for traceback
  visibility.
- Deprecation warnings for `mode: continuous` now also log via
  `logger.warning()` (previously only `warnings.warn()`).

### Removed
- 8 orphaned Jinja2 templates (3 Prometheus, 5 Grafana) that were never
  referenced by any deployer.
- `--extended` flag hidden from `lakebench recommend` help (still works
  with deprecation warning; replaced by `--slow-datagen`).

## [1.0.10] - 2026-03-02

### Changed
- **Spark 4.0.x support.** Lakebench now supports Spark 4.0.x images alongside
  Spark 3.5.x. `_spark_compat()` returns Scala 2.13 suffix, Hadoop AWS 3.4.1,
  and AWS SDK 1.12.367 for Spark 4. Spark Thrift Server template uses
  `{{ scala_suffix }}` instead of hardcoded `_2.12`.
- **Spark Operator installed during `deploy`.** `lakebench deploy` now installs
  the Spark Operator (when `operator.install: true`) instead of deferring to
  `run`. `run` only verifies the operator is present.
- **Deploy output shows component versions.** `lakebench deploy` now prints
  component name, version, and elapsed time in columnar format. Each deployer
  returns `label` and `detail` on `DeploymentResult`.
- **Bronze ingest no longer sets explicit table location.** `bronze_ingest.py`
  relied on the catalog to assign table locations instead of hardcoding
  `s3a://<bucket>/warehouse/<table>/`. Both Hive and Polaris catalogs assign
  correct locations from namespace defaults.
- **Default Spark image bumped to 4.0.2.** Recipes, schema default, and init
  template now use `apache/spark:4.0.2-python3`. Spark 3.5.x images remain
  fully supported -- set `images.spark` to `apache/spark:3.5.8-python3` to
  use Spark 3.

### Fixed
- **Spark 4 streaming job crash from jar bloat.** Iceberg 1.10.1's
  `iceberg-aws-bundle` is self-contained (bundles Hadoop AWS + AWS SDK v1 + v2).
  The packages list also included explicit `hadoop-aws` and
  `aws-java-sdk-bundle`, doubling the jar payload to ~1.2GB per executor.
  The driver's netty file server couldn't stream this to 12+ executors --
  connections timed out with `StacklessClosedChannelException`. Fix: Spark 4
  uses only 2 packages (iceberg-spark-runtime + iceberg-aws-bundle); Spark 3
  keeps all 4 packages unchanged.
- **Polaris allowed-locations scheme mismatch.** Polaris does literal prefix
  matching on S3 URIs -- `s3a://` (Spark) did not match `s3://` in
  `allowedLocations`. Bootstrap template now lists both `s3://` and `s3a://`
  for each bucket. Per-namespace locations and `default-base-location` also
  corrected to match the bucket-per-layer topology.
- **Spark Operator RBAC lost after namespace recreate.** After
  `lakebench destroy` + `deploy`, operator RBAC (Roles/RoleBindings) was lost
  but `ensure_installed()` reported success because the namespace was still in
  `spark.jobNamespaces`. Added `recreate_namespace_rbac()` that does a
  remove-then-re-add Helm cycle to force fresh RBAC creation.
- **Datagen batch OOM from hardcoded worker count.** The datagen template
  hardcoded `--workers 4` regardless of mode. Batch mode only allocates 4Gi
  memory, which is insufficient for 4 concurrent workers with v2 realism
  features. Worker count now comes from the autosizer (`generators` config),
  which sets 1 worker for batch and 8 for continuous.
- **Benchmark executor kubectl stderr noise.** `kubectl exec` without `-c`
  printed "Defaulted container" to stderr for multi-container pods, filling the
  200-char error buffer before actual Trino errors. Added explicit `-c trino`
  and `-c spark-thrift` container flags.
- **Prometheus status check name mismatch.** `lakebench status` looked for
  StatefulSet `lakebench-observability-prometheus` but the actual name is
  `prometheus-lakebench-observability-ku-prometheus`.
- **NoneType crash in sustained pipeline scorecard.** `data_freshness_seconds`
  is `float | None` but was used in comparisons and format strings without
  None guards.
- **Polaris bootstrap job timeout race.** 180s timeout with 10s polling caused
  a race condition. Bumped to 300s timeout with 5s polling.
- **Ivy cache cold start in streaming jobs.** Three concurrent streaming jobs
  each independently downloading ~200MB of Maven dependencies caused resolution
  to exceed the run duration. Added `resolve-deps` init container to pre-warm
  Ivy cache via shared emptyDir volume.

## [1.0.8] - 2026-02-21

### Changed
- Renamed pipeline mode `continuous` to `sustained`. The old `--continuous` CLI flag
  and `mode: continuous` YAML value still work with a deprecation warning.
- Bottleneck chart bar uses flexbox layout (no more line-wrap from subpixel rounding).
- Bottleneck chart bar always shows CPU share -- the dimension common to all stages
  including Trino query. Latency stays in the table column for sustained mode.
- Bottleneck identification picks dominant stage by latency in sustained mode
  (concurrent stages) and by compute in batch mode (sequential stages).
- Query stage CPU now derived from Trino config snapshot (coordinator + workers)
  instead of showing 0% (Trino has no Spark executors).
- Report title/header: "LakeBench" renamed to "Lakebench".

### Fixed
- **scale_ratio formula was triple-counting data.** The batch scorecard's
  `scale_ratio` used `total_data_processed_gb` (sum of bronze + silver + gold
  inputs) divided by `approx_bronze_gb`. At scale 50 with three stages each
  reading ~500 GB, this produced a ratio of ~3.0 instead of ~1.0. Now uses
  only bronze stage input GB in the numerator.

### Added
- Deploy success panel now shows `lakebench run --generate` as a "Next" option.
- HTML report layout section in benchmarking docs -- describes every section
  of the scorecard (verdict cards, bottleneck chart, data validity, stability,
  freshness, contention, job tables, query performance, config, platform
  metrics) and which sections appear in batch vs sustained mode.

## [1.0.7] - 2026-02-20

### Added
- **Stackable operator auto-install.** Set
  `architecture.catalog.hive.operator.install: true` to auto-install all four
  Stackable operators (commons, listener, secret, hive) via Helm during
  `lakebench deploy`. Mirrors the existing Spark Operator auto-install pattern.
  Requires cluster-admin. Operator namespace and version are configurable.
- Preflight and validate commands are now install-aware -- they warn instead of
  failing when Stackable CRDs are missing and auto-install is enabled.
- Documentation updated across getting-started, component-hive, configuration
  reference, and operators-and-catalogs guides.

### Fixed
- README Quick Start now shows `lakebench run --generate` as the single-command
  option (6 steps instead of 7).

## [1.0.6] - 2026-02-20

### Fixed
- **Datagen templates missing from PyPI wheel.** The `datagen/` sdist exclude
  pattern in `pyproject.toml` was unanchored, causing hatchling to also exclude
  `templates/datagen/*.yaml.j2` from the wheel. `lakebench generate` and
  `lakebench run --generate` failed with "'datagen/job.yaml.j2' not found in
  search path" when installed from PyPI. Fixed by anchoring the exclude to the
  repo root (`/datagen/`).
- README rewritten for clarity. Value proposition, quick start, and example
  scorecard output now front and center.

## [1.0.5] - 2026-02-20

### Added
- In-stream periodic benchmarking for continuous pipelines. The full 8-query
  Trino benchmark now runs at regular intervals *during* the streaming window,
  producing per-round QpH and freshness measurements. The final continuous QpH
  is the median across all in-stream rounds.
- Query-time freshness: gold-table staleness is probed via SQL at the moment
  Trino queries run, reported as `query_time_freshness_seconds` in the scorecard.
- Q9 contention handling: gold `createOrReplace()` contention is detected,
  retried up to twice with 30s/60s backoff, and reported per round
  (`q9_contention_observed`, `q9_retry_used`). Benchmark rounds are offset
  by half the gold refresh interval to land between rewrite cycles.
- `benchmark_interval` and `benchmark_warmup` config fields on
  `architecture.pipeline.continuous` control in-stream benchmark scheduling.
- HTML report "In-Stream Benchmark Rounds" section with per-round QpH,
  per-query times, freshness, and Q9 contention status.
- Terminal rounds summary table after streaming completes, with median QpH
  and freshness.
- New continuous scores: `query_time_freshness_seconds`, `in_stream_composite_qph`,
  `benchmark_rounds_count`.
- Removed `--include-datagen` flag (redundant with `--generate`). Continuous mode
  always runs datagen automatically.
- `run` command preflight check: verifies namespace, Postgres, catalog, and query
  engine are deployed and ready before starting the pipeline. Blocks with
  actionable guidance if infrastructure is missing or misconfigured.
- **Scorecard v2.0 report overhaul.** HTML report restructured into three
  layers: Verdict (5 summary cards), Diagnosis (bottleneck, data validity,
  query behavior), and Evidence (detail tables).
- Bottleneck Identification section: stacked CPU bar chart and per-stage
  breakdown table identifying the dominant pipeline stage.
- Data Validity panel: green/red indicators for scale ratio (batch) or
  ingest ratio (continuous), job success rate, and failed query count.
- Query Behavior section: per-class QpH breakdown table showing relative
  performance of scan, join, analytics, and aggregate query categories.
- Stability Over Time section (continuous only): dual-axis inline SVG chart
  plotting QpH and freshness per benchmark round, with trend analysis.
- Contention Map section (continuous only): summary and detail table of Q9
  gold-table contention events across benchmark rounds.
- Query-Time Freshness diagnostic section (continuous only): shows median
  query-time freshness vs worst-case data freshness with gap analysis and
  variability interpretation.
- `total_core_hours` field added to pipeline benchmark JSON output (both
  batch and continuous modes).
- `advanced_metrics` stub key in JSON output (null when Prometheus Tier 2
  is not deployed).
- **Tier 2 engine-level metrics.** Spark PrometheusServlet sink and Trino
  JMX exporter are now enabled when `observability.enabled` is true.
  - Spark: PrometheusServlet sink exposes GC, shuffle, and task metrics at
    `:4040/metrics/prometheus`.
  - Trino: JMX exporter JAR injected via init container from configurable
    `images.jmx_exporter` image, exposes metrics at port 9090.
  - PlatformCollector queries engine-level metrics (Spark GC, shuffle bytes,
    Trino query counts) from Prometheus when available.
  - Engine metrics rendered in the Platform section of the HTML report.
- `images.jmx_exporter` config field for specifying the JMX exporter
  container image (default: `bitnami/jmx-exporter:latest`).
- Platform metrics are now collected from Prometheus at the end of each
  pipeline run (both batch and continuous) when `observability.enabled` is
  true. Pod CPU/memory, S3 metrics, and engine-level Tier 2 metrics (Spark GC,
  shuffle, Trino query counts) are saved in `metrics.json` and rendered in the
  HTML report's Platform Metrics section.
- **Default StorageClass prerequisite documented.** Getting Started guide now
  lists a default StorageClass as a cluster prerequisite (needed for PostgreSQL
  metadata PVC).
- **PostgreSQL PVC troubleshooting entry.** New section in troubleshooting guide
  for diagnosing "PVC stuck in Pending" when no default StorageClass exists.

### Removed
- **Delta Lake recipes and support.** Removed `hive-delta-trino` and
  `hive-delta-none` recipes, the `DELTA` enum value from `TableFormatType`,
  `DeltaConfig` class, and both Delta entries from `_SUPPORTED_COMBINATIONS`.
  Delta Lake was never tested or supported in the pipeline -- keeping it in the
  codebase misled users. Recipe count drops from 10 to 8.

### Changed
- **4-slot recipe naming convention.** Recipe names now encode all four
  architecture axes: `<catalog>-<format>-<engine>-<query_engine>`. Old 3-slot
  names (`hive-iceberg-trino`) are replaced by 4-slot names
  (`hive-iceberg-spark-trino`). The Spark Thrift query engine slot uses
  `thrift` (not `spark`) to avoid `spark-spark` ambiguity. No backward
  compatibility aliases -- clean break.
- **`_SUPPORTED_COMBINATIONS` expanded to 4-tuples.** Validation now checks
  `(catalog, table_format, engine, query_engine)` instead of 3-tuples. Added
  `PipelineEngineType` enum with single value `SPARK`.
- **Config snapshot includes `pipeline_engine`.** `build_config_snapshot()` and
  the HTML report banner now render the full 4-slot recipe name.
- **Trino workers default to ephemeral storage.** When `storage_class` is empty
  (the default), Trino workers now use `emptyDir` instead of requiring a PVC.
  This removes the need for a provisioned StorageClass on new clusters. Set
  `storage_class` to a class name to opt back into PVC-backed storage.
- Documentation restructured with **quick-recipe** terminology. The `recipe:`
  field is now called a "quick-recipe" (one-line shorthand), with a new
  "Advanced Configuration" section in the Recipes guide showing how to
  override individual settings while keeping the recipe base.
- Updated all documentation to remove Delta Lake references (README, recipes,
  configuration, supported-components, getting-started, architecture,
  component-hive, component-trino, operators-and-catalogs, docs index).
- Streaming-log freshness metric changed from average to worst-case (max).
  The worst staleness spike is what matters for streaming SLAs, not the average
  that hides it.
- Continuous pipeline no longer runs a post-stream benchmark. The in-stream
  rounds are the benchmark -- measuring QpH against a different (larger) table
  state after streaming stops is not a meaningful comparison.
- Primary continuous `composite_qph` is the in-stream median QpH.
- **HTML report overhaul for continuous mode.** Summary cards now show
  streaming KPIs (duration, data processed, data throughput GB/s, sustained
  throughput rows/s, in-stream QpH, data freshness) instead of batch-derived
  zeros. Duplicate QpH card eliminated. Empty "Job Performance" section
  hidden. Section renamed to "Pipeline Stages" with column tooltips.
  Detail cards (ingest ratio, compute efficiency, total CPU-hours) placed
  below stage table. Run context banner added (mode, dataset, stack,
  duration). All cards have hint text explaining what each number means.
- Batch summary: 5 primary cards (Time to Value, Pipeline Throughput,
  Compute Efficiency, QpH, Scale Ratio) plus metadata row (Total Time,
  Jobs). Replaces the previous variable-count card layout.
- Continuous summary: 5 primary cards (Data Freshness, Sustained Throughput,
  Compute Efficiency, In-Stream QpH, Total CPU-hours) plus metadata row
  (Duration, Data Processed).
- Data Freshness card always shows worst-case `data_freshness_seconds` from
  streaming logs. Previously swapped to query-time freshness when available.
- Trend analysis requires minimum 5 benchmark rounds (was 3). Rounds 2-4
  show "Insufficient data" message instead of potentially misleading trends.
- Pipeline Stages table: CPU-hours column added per stage. Latency column
  auto-converts to seconds when values exceed 1000ms. Cores and memory
  merged into "Cores x Mem" column.
- Streaming table enriched with compute columns (Executors, Cores x Mem,
  CPU-sec) and a total compute summary line.
- Batch pipeline score cards updated with hint text (Time to Value, Pipeline
  Throughput, Compute Efficiency, Scale Ratio).
- `total_data_processed_gb` and `pipeline_throughput_gb_per_second` now
  computed for continuous mode (previously batch-only). Both appear in
  continuous JSON scorecard output.
- `stage_latency_profile` JSON format changed from array to named object
  (`{"bronze_ms": ..., "silver_ms": ..., "gold_ms": ...}`). Old array
  format still loads via backward-compatible deserialization.
- Run context banner includes `storage_backend` when available in config.
- Renamed metric field `ingestion_completeness_ratio` to `ingest_ratio` and
  `scale_verified_ratio` to `scale_ratio` in pipeline benchmark data model
  and JSON output. Old JSON keys still load via backward-compatible
  deserialization. Report labels updated: "Completeness" -> "Ingest Ratio",
  "Scale Verified" -> "Scale Ratio".
- Added `score_descriptions` dict to pipeline benchmark JSON output. Each
  score key gets a human-readable explanation for downstream tools and
  manual inspection.
- `score_descriptions` updated with `total_core_hours` and reorganized into
  batch, continuous, and shared sections. `data_freshness_seconds` marked
  as "Primary freshness score", `query_time_freshness_seconds` marked as
  "Diagnostic" with gap explanation.

### Fixed
- **DuckDB deploy fails on OpenShift.** The `python:3.11-slim` container runs
  as non-root on OpenShift, causing `pip install duckdb` to fail with
  `Permission denied: /.local`. Fixed by setting `HOME=/tmp` and using
  `--no-cache-dir` in the DuckDB deployment template.
- **DuckDB readiness probe too aggressive.** Added a `startupProbe` with
  `failureThreshold: 30` (300s window) to allow time for `pip install`.
  Reduced readiness/liveness `initialDelaySeconds` to 5 since the startup
  probe handles the init window.
- **DuckDB deployer timeout too short.** Increased from 180s to 300s.
- **Prometheus service discovery for platform metrics.** The
  kube-prometheus-stack Helm chart truncates service names based on release
  name length, making the hardcoded URL
  `lakebench-observability-prometheus` incorrect (actual name:
  `lakebench-observability-ku-prometheus`). Platform metric collection now
  discovers the Prometheus service dynamically via Helm release labels.
- `benchmark_rounds` was serialized to JSON but never deserialized back
  when loading saved runs. The Stability chart, Contention Map, and
  Benchmark Rounds table were empty on `lakebench report` from saved data.
  Now fully round-tripped including `BenchmarkRoundMeta` (Q9 contention
  flags, freshness, timestamps).
- Trino PodMonitor label selectors matched `app.kubernetes.io/name: trino`
  and `app.kubernetes.io/component: coordinator`, but actual pod labels use
  `app.kubernetes.io/name: lakebench` and
  `app.kubernetes.io/component: trino-coordinator`. Fixed both coordinator
  and worker PodMonitors.
- Prometheus ConfigMap scrape configs for Trino had the same label mismatch.
  Fixed to match actual pod labels.
- PodMonitor templates were never rendered or applied. Wired
  `_apply_podmonitor_templates()` into the observability deployer to apply
  them after the kube-prometheus-stack Helm install.
- PodMonitor `release` label was `prometheus` but the Helm release is
  `lakebench-observability`. Fixed to match the actual Prometheus Operator
  selector.
- JMX exporter config with empty `rules: []` only exported JVM metrics.
  Added catch-all rule to emit whitelisted Trino MBeans.
- Default `images.jmx_exporter` was `bitnami/jmx-exporter:1.0.1` (does not
  exist). Changed to `bitnami/jmx-exporter:latest`.

## [1.0.3] - 2026-02-17

### Added
- `architecture.pipeline.mode` config field (`batch` | `continuous`) sets the
  pipeline execution mode in YAML. The `--continuous` CLI flag still works as
  an override for one-off runs.
- `report --summary` / `-s` flag prints key pipeline scores (per-stage table,
  time-to-value, throughput, efficiency, QpH) to the terminal without opening
  the HTML report.
- `lakebench init` template includes `pipeline.mode: batch` in generated configs.
- `info` command shows the active pipeline mode.

### Changed
- Scorecard stage label changed from "datagen" to "data-generation" for clarity.
- `lakebench.yaml` example: flattened double-commented sections (`#   #` patterns)
  to single-level comments for readability.

### Fixed
- Documentation: observability annotated example in `configuration.md` used the
  old nested YAML structure (`metrics.prometheus.enabled`, `dashboards.grafana`).
  Updated to match the flat schema (`observability.enabled`, etc.).
- Documentation: `query_engine.type` reference table and supported combinations
  table were missing `duckdb` as a valid option.
- Documentation: `running-pipelines.md` and `configuration.md` now document
  `pipeline.mode` config field alongside the `--continuous` CLI flag.
- Documentation: `benchmarking.md` now documents `report --summary` in the
  "Viewing Results on the Command Line" section.

## [1.0.2] - 2026-02-14

### Changed
- Spark Operator `install` default changed from `true` to `false`. The operator
  requires cluster-admin and platform-specific patches -- explicit opt-in is safer.
- `run --timeout` now auto-scales from the data scale factor (`max(3600, scale * 60)`)
  when not explicitly set.
- `status` command is config-aware: only shows components matching the configured
  catalog, query engine, and observability settings. Includes Trino workers and
  datagen job progress.
- Spark job monitor fires progress callback on every poll while RUNNING (not just
  on state transitions). CLI only prints executor count when it changes.
- `validate` summary distinguishes warnings from passes/failures and shows a
  warning count.

### Added
- Spark Operator namespace watching detection: `validate` and `run` now check
  whether `spark.jobNamespaces` includes the target namespace. When `install: true`,
  the namespace is added automatically via `helm upgrade --reuse-values` and the
  operator controller is restarted to pick up the change. When `install: false`,
  the exact fix command is shown.
- Prerequisite detection in `validate`: checks kubectl and helm on PATH, checks
  Stackable CRDs for Hive recipes (with install commands), checks Spark Operator
  with full manual `helm install` command when `install: false`.
- `deploy` now blocks with exit 1 when Stackable operators are missing for Hive
  recipes (was a non-blocking warning). Suggests Polaris as an alternative.
- `generate --yes/-y` flag to skip confirmation prompt.
- Confirmation prompt before `generate` submits the datagen job.
- OOM and crash-loop detection in datagen progress: `generate --wait` exits
  immediately with actionable guidance when pods are OOMKilled.
- Pending pod count shown during `generate --wait`.
- Bucket name overlap warning in `validate` (bronze/silver/gold sharing names).
- Active datagen guard in `clean`: warns and prompts before deleting S3 data
  while generation is running.
- S3 `empty_bucket()` progress callback; `clean` command shows deletion progress.
- `S3Client._check_client()` raises `S3AuthError` early when boto init failed.
- ANSI escape code stripping for `logs` command output.
- Helm failure recovery commands in observability deployer error messages.
- Workflow hint in CLI help epilog: `init -> validate -> deploy -> generate -> run -> report -> destroy`.
- Minimum viable config block in generated YAML (3 required fields).
- "Next step" hints in `deploy`, `generate`, and `destroy` success panels.
- `deploy` preflight prints a validate reminder tip.
- Scale 100 (~1 TB) configuration example in docs.

### Fixed
- `generate` showed "Target: 0.00 GB" (wrong dict key `target_size_bytes`
  instead of `target_tb`).
- `validate` Stackable error listed all 4 operators even when only 1 CRD was
  missing. Now lists only missing operators plus prerequisites.
- JSON query output (`--format json`) now produces proper `{"key": "value"}`
  dicts instead of raw tab-delimited arrays.
- CSV query output strips surrounding quotes from fields.
- ConfigMap deploy failure in `spark/job.py` now returns `False` (hard failure)
  instead of raising an unhandled exception.
- 11 bare `except: pass` patterns replaced with `logger.debug()` or
  `logger.warning()` across cli.py, engine.py, k8s/client.py, s3/client.py,
  hive.py, and metrics/collector.py.
- 6 e2e test bugs: continuous pipeline used `--timeout` instead of `--duration`,
  destroy assertion failed on Completed pods, `--mode` flag doesn't exist (use
  `--continuous`), gold row count regex matched run ID hex digits, scale matrix
  namespace not registered with Spark Operator, integration test rejected
  Succeeded postgres phase.

## [1.0.1] - 2026-02-13

### Fixed
- Rename "Recipe" label to "Workload" in `lakebench info` output to avoid
  collision with the architecture recipe concept (`<catalog>-<format>-<engine>`).

## [1.0.0] - 2026-02-12

Initial public release.

### Features
- Deploy and benchmark lakehouse stacks on Kubernetes from a single YAML config.
- Recipe system -- 10 validated (catalog, table format, query engine) combinations.
  Single `recipe:` field sets the full stack.
- Catalogs: Hive Metastore (Stackable operator) and Apache Polaris (REST, OAuth2).
- Table formats: Apache Iceberg, Delta Lake.
- Query engines: Trino, Spark Thrift Server (HiveServer2 JDBC), DuckDB (single-pod).
- Synthetic Customer360 data generation at configurable scale (1 GB to 10+ TB).
- Batch medallion pipeline: bronze-verify, silver-build, gold-finalize (Spark).
- Streaming/continuous pipeline: Structured Streaming bronze-ingest, silver-stream,
  gold-refresh.
- 8-query benchmark suite across 5 query categories.
- `QueryExecutor` protocol with pluggable engine backends and `adapt_query()` for
  engine-specific SQL rewriting.
- HTML report generation with job performance, query latencies, and platform metrics.
- Platform observability via `kube-prometheus-stack` Helm chart. `ObservabilityDeployer`
  handles deploy/destroy lifecycle.
- S3 metrics wrapper (`S3MetricsWrapper`) instruments CLI-side boto3 operations with
  Prometheus counters and histograms.
- Platform metrics collection (`PlatformCollector`) snapshots CPU, memory, and S3 I/O
  per pod after benchmark runs.
- Auto-sizing engine for Spark executor counts based on scale factor.
- `lakebench recommend` command with binary search for max feasible scale.
- Interactive init mode, deploy confirmation, progress bars for `generate --wait`.
- S3 client with FlashBlade multipart upload handling.
- OpenShift SCC integration for Spark pods.
- Pre-built binaries for Linux (amd64) and macOS (amd64, arm64).
