# Changelog

All notable changes to Lakebench are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
  `-y` (old `-f` refuses at a terminal, still forces in scripts with a
  warning). `init`: `--force`. `results`: `--format` / `-o`. `logs`:
  `--follow` / `-F`. Admin commands gain `-f/--file`.
- **`-f` on `destroy` and `clean` exits 2 in every context** and names
  `--force` / `-y`; `LAKEBENCH_LEGACY_SHORT_F=1` restores the old meaning
  with a warning. `results` rejects a `--format` that conflicts with `-f`.
- **`results -o json|csv`** prints plain stdout.
- **AML rule targets changed.** W3 now searches 2-5 hop cycles and is
  scored against `cycle`; W4 is scored against `rapid_layering`; a new
  chain rule is scored against `stack`; W2 adds a per-beneficiary alert
  kind. Per-rule recall and FP are not comparable with earlier runs, and
  the AML query set grew from 30 to 34, so AML QpH is not comparable.
- **Metric meanings changed:** `maintenance_value_pct` is null when not
  measured (was 0.0); c360 `customer_recency_score` and Q6 are anchored to
  the data clock, not the run date (Q6 and c360 QpH not comparable);
  silver-build `output_rows` in incremental mode is per cycle.
- **Release process (maintainers):** one `release.yml`; PyPI uploads after
  the GitHub Release; tags must be the normalised version and on `main`;
  `uat/results-<version>.md` with a `# UAT results <version>` heading and a
  results table is required; pre-release and dev versions are refused. See `docs/releasing.md` for the
  repository settings the gates depend on.
- **QpH is the median of 3 samples per query (LB-150).**
  `architecture.benchmark.iterations` now defaults to 3 and `lakebench run`
  passes it to both benchmark rounds (it was ignored before, and the
  `benchmark --iterations` default of 1 overrode the config). Throughput
  QpH counts executions. Earlier QpH was a single sample and is not
  comparable; the perf gate and `reproduce` refuse a QpH taken with a
  different sample count.
- **The AML benchmark set grew from 8 to 12 queries** with the investigator
  class (IQ1-IQ4). `metrics.json` records a `query_set_id`; `compare` and
  `reproduce` refuse QpH across different or unrecorded sets.
- **AML customer-scoped rules alert on customers only.** W2, W5, W6, W7 and
  W8 drop alerts whose entity is not a customer in `silver.entities`; the
  graph rules (W1, W3, W4, W17) stay unscoped. Alert counts and false
  positives drop; per-rule numbers are not comparable with earlier runs.
- **A c360 continuous run over existing state refuses without
  `--force-reset`.** It lists the non-empty tables, checkpoints and raw
  prefixes it would delete (LB-142).

### Added
- **TM operations layer for AML (GOALS P10 stages 1, 6-9).** After
  detection, `tm_operations.py` writes `tm_reconciliation`,
  `scenario_coverage`, `alert_dispositions` and `cases` to gold, with
  dispositions simulated from the datagen ground truth at a configured
  analyst and investigator accuracy. Workflow invariants are logged per
  cycle; a violated invariant fails the run, and a layer that could not run
  is reported as `not_run` without touching detection scoring. Configured
  under `architecture.workload.tm_operations`. The AML scorecard gains a
  Transaction Monitoring Operations section.
- **Storage settle wait before the post-maintenance round (LB-150).** In
  batch mode a probe query runs every 60 s after maintenance until two
  consecutive probes agree within 10% and neither is slower than the
  pre-maintenance median by more than 10%, capped at 45 minutes.
  `maintenance_value_pct` is null when the wait is capped or fails. The
  wait is not a stage and is not counted in time to value. Configured
  under `architecture.benchmark.maintenance_settle`.
- **Performance regression gate.** `benchmarks/perf/` holds pinned configs
  (c360 batch s10, c360 continuous s10, AML batch s1) and
  `baselines.yaml`; `scripts/perf_gate.py` compares a run with its
  baseline and refuses runs that are not like for like. The release gate
  gains a `perf-baselines` check.
- **Tracked AML fidelity gate.** One feature definition
  (`spark/scripts/aml_features.py`) feeds a pre-registered reference model
  on silver (cluster) and bronze (local), scored per (customer, UTC month);
  the model's outputs are persisted with the report.
- **AML datagen: monitored population and minimal KYC/CRR.** Half the
  parties are customers of one reporting bank, accounts carry `home_fi`,
  and customers carry tenure and a risk rating. Silver refuses NULL KYC on
  a KYC-era corpus. `--cycle n` gives each multi-cycle run its own streams
  and keys.
- **AML rules that can detect their typologies.** New
  `W17_layering_chain` (scored against `stack`) and a W2 per-beneficiary
  alert kind; each rule is computed once and replayed instead of twice.
- **DuckDB runs all eight AML analytical queries and the investigator
  queries.** Auxiliary financial tables resolve to their layer's bucket,
  timestamptz columns are cast to text before the fetch, and
  `cardinality()` is rewritten to `len()`. Every executor now reports the
  final exception of an engine error, not the first 200 characters of
  stderr.
- **Per-file coverage floors** on the scoring, metrics and detection code
  (`scripts/check_coverage.py`), run in CI.
- **LB-116: per-rule AML alert counts + errors surfaced into
  `metrics.json`.** ``JobMetrics`` gains ``alerts_by_rule: dict[str,
  int]`` and ``rule_errors: dict[str, str]`` fields. Populated from the
  ``[detection] {rule_id}: alerts=N ...`` lines the driver already
  emits per rule; a crashed rule shows up as ``alerts=0`` with the
  exception preserved in ``rule_errors``. Anchors on trailing
  ``elapsed=Ns`` at end-of-line so an inline ``elapsed=`` or ``prior=``
  substring inside an exception message survives intact (adversarial
  review caught the earlier non-greedy slurp truncating error text).

### Fixed
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
- **LB-117 (P2): AML analytical query QpH unstable.** Three S1 iters
  saw QpH 6.6 / 0.0 / 8.2 -- the 0.0 was a spark-thrift OOM mid-benchmark
  on ``aggregate_typology_coverage.sql`` at the AML 16g target, and
  the successful iters clipped queries that overran the 300s default.
  Fix: (a) AML ``spark_thrift.memory`` target 16g -> 24g in
  ``_apply_schema_overrides``; (b) benchmark ``query_timeout`` 300s ->
  900s (post-compaction) and 60s -> 180s (pre-compaction) when
  ``workload.schema=financial``; (c) small-cluster cap threshold
  reworked to leave ~8 GiB headroom for Spark overhead + kubelet: below
  36 GiB allocatable target = ``max(4, min(20, allocatable - 8))g``.
- **LB-118 (P1): AML bronze-verify ran out of scratch disk at scale
  >= 5.** `bronze_verify_financial.py` trips its CTAS fallback path
  above `ADD_FILES_MAX_BYTES` / `ADD_FILES_MAX_FILES` and rewrites the
  full pacs.008 source through an Iceberg CTAS, spilling ~2x the
  per-executor input to local disk. The c360-shaped base profile
  gave bronze-verify only 50Gi/executor -- fine for the thin
  add_files register c360 does, blown out at 78 min for AML scale
  10 with `No space left on device`. Fix: added
  `_SCHEMA_PROFILE_OVERRIDES` and `_resolve_job_profile()` in
  `modules/pipeline_engines/spark/job.py`. AML bronze-verify now
  gets 500Gi scratch, `executors_per_100_scale: 8` (vs c360's 4),
  and `max_executors: 28` (vs 20) so per-executor input load
  halves at scale 100 and stays under the fabric8 ceiling. Base
  `_JOB_PROFILES` stays c360-shaped. `_job_requirement`,
  `compute_peak_requirements`, and `_build_manifest` all take an
  optional `schema_type` arg and route through the resolver; the
  capacity-preflight in `cli/_prerequisites.py` reads
  `cfg.architecture.workload.schema_type` and plumbs it down so
  documented cluster minimums reflect AML sizing when the workload
  is financial.
- **LB-112 (P0): batch AML pipeline never invoked the detection
  rules.** `lakebench run` on a batch financial config completed
  bronze-verify + silver-build + gold-finalize and emitted an empty
  `gold.alerts` table. Baseline population required a separate
  `lakebench financial replay --depth-months 0` per rule, which does
  not include the detection wall-clock in `metrics.json` -- so the
  baseline row's TTV understated the pipeline's real cost. Fix:
  `gold_finalize_financial.main()` now calls
  `run_detection_rules(spark, txns, RUN_ID)` after baseline
  dashboards. Iterates `DEFAULT_DETECTION_RULES` = (W2, W3, W4, W7,
  W8, W1 -- cheapest first, W1 last as most expensive), materialises
  each rule's alerts frame into a temp view, DELETE-per-rule_id then
  INSERT so re-runs against the same silver corpus produce
  reproducible alert counts and a transient write failure does not
  silently split. Per-rule try/except so one bad rule cannot abort
  the pipeline; failure log line uses `alerts=0 error=...` shape so
  metrics parsers see a row for every rule attempted. Signature
  filter uses `inspect.signature(fn).parameters` (not
  `fn.__code__.co_varnames`, which leaks locals into the kwarg
  filter). Per-job timeout in `cli/_run.py` also bumped by 900 s
  when `workload.schema=financial` so gold-finalize + detection
  fits at scale ~5+.

- **LB-113 (P0): Spark Thrift default 4Gi OOMs every AML benchmark
  query at scale 1.** First live S1 run 2026-09-21 showed exit 137
  on query 1 (silver full aggregation) cascading to "container not
  found" on 7/7 remaining queries as the thrift pod terminated,
  producing 0/8 QpH. Fix: autosizer `_apply_schema_overrides` bumps
  `query_engine.spark_thrift.memory` to `16g` when
  `workload.schema=financial` and the field is at its default.
  Guarded on cluster capacity: when the largest allocatable node
  has < 20 GiB, cap to `max(4g, 0.8 * largest_node_gi)` so a small
  cluster does not silently get a Pending pod. Autosizer test
  actually invokes the autosizer against a real config (not a
  source grep) so a future refactor that keeps the strings but
  breaks the mutation trips a real assertion. Does not
  self-propagate on upgrade -- an already-deployed 4g thrift pod
  needs a destroy-then-deploy cycle to pick up the new default.

- **LB-114 (P0): W1_connected_components crashed with
  `AnalysisException: Column dst#63L are ambiguous` on iteration 2
  of label propagation.** `labels` after the first iteration
  inherits Catalyst attribute IDs from `edges_u` via the prior
  `unionByName(neighbour_labels)`, so `labels.join(edges_u, ...)`
  on subsequent iterations cannot disambiguate `edges_u["dst"]`
  from `labels`' shared IDs. Fix: `.alias("lbl")` and `.alias("eg")`
  on both sides inside each loop iteration + qualified
  `col("lbl.id") == col("eg.src")` join predicate. Also:
  `labels.unpersist(blocking=False)` at function return so the
  cached vertex-label frame is not pinned in executor storage
  through the `collect_set` shuffle downstream. Evidence map now
  carries `converged=true|false` so a non-convergence emission of
  partial-labeling alerts is visible to downstream metrics.
  Surfaced by first live S1 run 2026-09-21.

- **LB-115 (P0): W7_cross_border_high_risk crashed inside the driver
  on every S1 run.** Two independent defects combined into one hard
  failure: (a) the caller (both `replay_financial` and the new
  gold-finalize detection loop) did not pass `silver_entities`, and
  (b) `_aml_data_path()` tried `import lakebench.spark.data` which
  raises ImportError inside the apache/spark image (lakebench pkg
  not installed there), and that ImportError was uncaught -- the
  whole rule dispatcher stack-traced. Fix has four parts: (i)
  `_aml_data_candidates()` walks env-override -> pkg-import ->
  script-dir-subfolder -> script-dir with import-error tolerance,
  and returns `[override]` early when `LB_AML_DATA_DIR` is set so
  a partial override does not silently mix with pkg data (split-
  brain reference state); (ii) `w7_cross_border_high_risk`
  auto-loads `silver.entities` from `LB_ICEBERG_CATALOG` +
  `LB_FINANCIAL_SILVER_ENTITIES` when the caller passes None,
  catching `AnalysisException` so a missing table degrades to
  empty alerts rather than crashing the rule dispatcher; (iii)
  `deploy_scripts_configmap` ships the three AML JSON sidecars
  (`sanctions_list.json`, `pep_list.json`,
  `high_risk_jurisdictions.json`, ~8 KB total) alongside the .py
  scripts using flat keys since ConfigMap keys cannot contain
  slashes -- they mount under `/opt/spark/scripts/` where the
  script-dir candidate finds them; (iv) new
  `get_aml_data_dir()` helper in `lakebench._resources`. Also
  fixed in W7: `dropDuplicates(['entity_id'])` on
  `silver.entities` was shuffle-order-dependent, so an entity
  with multiple country values produced different W7 alert
  counts across runs; replaced with deterministic
  `groupBy + min(country)`.

- **LB-089 (P0): AML pipeline broken end-to-end since the datagen
  Rust rewrite.** `bronze_verify_financial.py` and
  `bronze_ingest_financial.py` read pacs.008 transactions from
  `LB_BRONZE_URI + LB_FINANCIAL_BRONZE_PREFIX` (default `pacs008/`)
  -- the flat layout the retired Python datagen used. datagen_rs
  writes into a nested layout to hold four related bronze tables
  (`{root}/bronze/pacs008/part-*.parquet` for the pacs.008
  transactions, plus `{root}/bronze/party.parquet`,
  `{root}/bronze/account.parquet`, and
  `{root}/manifest/manifest.parquet`). Spark listing the root
  hits three subdirs with different schemas and refuses with
  `UNABLE_TO_INFER_SCHEMA`. Found live on 2026-09-21 during the
  first end-to-end AML deploy attempt. Same systemic pattern as
  LB-088: shipped through PR-A/B/C/D/E because no unit test read
  real datagen v2 output and no live pipeline run gated the
  branch. C360 was unaffected -- its datagen writes flat and its
  reader reads flat. Round 1 fix: both bronze readers now honour
  `LB_FINANCIAL_BRONZE_PREFIX` as the ROOT prefix (matches what
  `job.py` mirrors from `path_template`) and derive
  `PACS_PREFIX = root + "bronze/pacs008/"`. New env
  `LB_FINANCIAL_PACS_PATH` is the escape hatch for a bespoke
  layout without leaking that concern into every reader.
  Adversarial pass on round 1 found round 2: `bronze.manifest`
  Iceberg table was never registered so 4 of 7 AML benchmark
  queries (`rule_precision`, `rule_recall`, `rule_ttd`,
  `aggregate_typology_coverage`) failed at Trino with "Table does
  not exist" -- fixed by extending `bronze_verify_financial` to
  register `{catalog}.bronze.manifest` from the manifest sidecar
  via CTAS (small table, unconditional; failure is logged and
  non-fatal so batch alerts still ship). Added
  `tests/test_aml_datagen_reader_layout.py` (5 tests): AST-based
  cross-language lock-step gate between the Rust writer and the
  Python reader; docstrings stripped so a comment mentioning the
  string cannot satisfy the check while the code path reverts.
  Not live-verified post-fix -- unit-tested only; live re-run
  pending on `aml-baseline-s1`. LB-090 and LB-091 opened for
  sustained-mode-only follow-ups (env-var contract drift on
  streaming, and `bronze_verify` never scheduled by the sustained
  CLI).

### Changed
- **`LB_FINANCIAL_BRONZE_PREFIX` semantic on `bronze_ingest_financial`
  (LB-089).** The env var previously meant the INNER path
  (default `bronze/pacs008/`) on `bronze_ingest_financial.py`
  while it meant the OUTER root on `bronze_verify_financial.py`
  and in `job.py`. Both scripts now agree: OUTER root, datagen
  v2 sub-path derived internally. Any external caller that had
  set `LB_FINANCIAL_BRONZE_PREFIX=bronze/pacs008/` on
  `bronze_ingest_financial` for its old-contract semantics
  will now double-prefix to `bronze/pacs008/bronze/pacs008/`
  and read zero rows without erroring; set
  `LB_FINANCIAL_BRONZE_PREFIX` to the datagen root
  (`pacs008/` at the default) or use `LB_FINANCIAL_PACS_PATH`
  to override the derived sub-path directly.
- **LB-088 (P0): FlashBlade returns `NotImplemented` on
  `GetBucketTagging` and `PutBucketTagging`.** The primary tested S3
  target does not implement the tagging APIs that PR-1's ownership
  discipline uses as the identity mechanism. Found live on 2026-09-21
  when the first end-to-end AML deploy failed at the `s3-buckets`
  step. Shipped through unit tests, PR-1 code review, PR-2 adversarial
  review, and PR-C regression tests because the tests used moto (which
  implements tagging cleanly); no path exercised a backend that
  returns `NotImplemented`. Fix: new `IdentityVerdict.UNSUPPORTED`
  verdict and `BucketTaggingUnsupported` exception surface the gap
  distinctly from `NoSuchTagSet` (no tags) or `NoSuchBucket` (does not
  exist). `write_bucket_ownership_tag` and `read_bucket_ownership_tag`
  catch the `NotImplemented` `ClientError` (narrowed from an earlier
  draft that also caught `MethodNotAllowed` -- that's a permissions
  problem, not a missing feature). Deploy s3-buckets warns once per
  run and skips the tag write; UNSUPPORTED-verdict buckets require a
  name-prefix match against the deployment name, and any freshly
  created bucket we cannot claim is deleted so a rerun doesn't leave
  orphans. Destroy s3-buckets falls back to the same name-prefix
  check on UNSUPPORTED, refusing unless `--force-legacy`. New
  `bucket_name_matches_deployment` helper enforces **longest-prefix
  wins** against a cluster-scan of other lakebench deployment names
  (`list_lakebench_deployment_names(core_v1)`) so that deployment
  `prod` cannot silently adopt bucket `prod-eu-bronze` owned by
  deployment `prod-eu`. Round-3 adversarial review of the round-2
  fix found TWO more silent-corruption holes: `admin reclaim-bucket`
  still called the helper without the sibling list (naive prefix
  re-opened on the admin path), and `list_lakebench_deployment_names`
  returned an empty list on every exception -- a namespace-scoped
  kubeconfig hitting RBAC 403 downgraded to naive prefix silently,
  the round-1 bug re-opened for every multi-tenant OpenShift token.
  Round-3 fix: `list_lakebench_deployment_names` returns
  `list[str] | None`, where `None` means "cannot tell"; every
  UNSUPPORTED-verdict caller (deploy, destroy, admin) refuses on
  `None` unless `--force-legacy`. Narrowed the catch to
  `ApiException` + `ConfigException` only (unexpected exceptions
  bubble so a future refactor bug does not disarm the safety check).
  Destroy uses `get_k8s_client(context=cfg.context)` instead of
  ambient kubeconfig so a stale `KUBECONFIG` cannot enumerate the
  wrong cluster. Orphan-cleanup on refuse checks the bucket is empty
  before deleting (racing writer's data is left with a WARN naming
  the orphan). `report()` fires per-bucket on the `--force-legacy`
  branch of destroy. `admin reclaim-bucket` enumerates siblings
  symmetrically. `--force-legacy` help text on both `deploy` and
  `destroy` names the second case. `lakebench config storage` gains
  a `bucket-tagging` ADVISORY check that reports SKIP for
  unsupporting backends. Destroy summary distinguishes tag-verified,
  name-prefix, and --force-legacy buckets. `docs/storage-backends.md`
  documents the fallback; `docs/design/namespace-isolation.md`
  documents the Category 1 identity-carrier trade-off. New tests:
  `TestBucketTaggingUnsupported` (5), `TestBucketNamePrefixFallback`
  (10 including nested-prefix collision + empty-name fail-safe),
  `TestListLakebenchDeploymentNames` (7 including None-on-403 and
  bubble-on-unexpected-exception). Known debt: TOCTOU race on
  parallel first-time deploys of sibling-prefix names on
  unsupporting backends documented but not resolved (requires
  s3-buckets-under-cluster-lease refactor). Follow-up: integration
  fake-tagging-unsupported boto client.

### Added
- **Shared-cluster ownership discipline.** Every deployment now carries an
  identity: a `lakebench.deployment/name` annotation on its namespace
  and a matching `lakebench.deployment` tag on each of its S3 buckets.
  Destroy and clean paths verify identity before mutating; a foreign
  stamp is a hard refusal, not a warning. Cluster-scoped resources
  (Stackable `SecretClass`) are renamed per-deployment to prevent name
  collisions between parallel deploys. See
  `docs/design/namespace-isolation.md` for the full taxonomy.
- **`lakebench admin` subcommand tree.** Cluster admins run one-time
  setup (`install-spark-operator`, `install-scratch-storage-class`)
  before developers can `deploy`. Also `status`, `doctor`,
  `release-lock`, `migrate-deployment` (for legacy pre-ownership
  namespaces), `repair-operator` (reconciles the Spark Operator watch
  list), and `reclaim-bucket`. Every mutating admin command acquires
  the cluster-wide `lakebench-cluster-lock` lease.
- **`lakebench reproduce` command.** Records a reproduction package
  from a run and verifies later runs against it with per-metric
  direction tables and tolerance banding. Exit codes 0/1/2
  distinguish pass / performance drift / correctness drift.
- **`--force-legacy` on `deploy`, `clean`, and `destroy`;
  `--allow-unverified-cluster` on `destroy`.** Explicit escape hatches
  for the pre-ownership world. `destroy` now REFUSES on legacy
  annotation-less namespaces and untagged buckets by default (the
  design invariant is "no destroy without proof of ownership"); use
  `lakebench admin migrate-deployment <namespace>` first to stamp
  identity, or pass `--force-legacy` if you have confirmed the
  resource is yours. Foreign tags/annotations are refused always,
  regardless of the flag.
- **Root `--version` / `-V` flag.** Both `lakebench --version` and
  the existing `lakebench version` subcommand now work.
- **AML product-surface docs.** Marcus's roleplay-persona pass in
  `dev-artifacts/roleplay/COLLATED.md` flagged that AML was invisible
  from the front door: no mention in `README.md`, no getting-started
  section, and `docs/financial-benchmark-baselines.md` reads as
  "not run yet." New `docs/aml-scoring.md` explains what benchmark
  precision measures vs what an AML ops team's FP rate measures, the
  W1/W2/W3/W4/W7/W8 rule to typology mapping, the leakage gate + the
  scikit-learn reference detector shipped in PR-A, `UNMAPPED_TYPOLOGIES`
  and why untargeted-typology recall is untestable, and the three
  metric-trust caveats Marcus surfaced (`compute_efficiency_gb_per_core_hour`
  reports requested cores, `ingest_ratio` denominator is scale-derived,
  `qph_degradation_pct` wants at least four rounds). README now names
  the two workloads (Customer 360 and AML) as a first-class distinction
  above Quick Start, adds `financial` to the commands table, and links
  the new doc. Getting-started grows a "Choosing a Workload" section
  after "Choosing a Recipe" with a worked deploy -> generate -> run ->
  score example and pointers to `financial replay` /
  `financial reproduce`. The one populated baseline row in
  `docs/financial-benchmark-baselines.md` remains deferred until a live
  scale-10 and scale-100 UAT run.
- **AML leakage gate + reference detector (`score_financial_reference.py`,
  `lakebench.aml.reference_score`).** Closes the "distribution checks
  do not prove semantics -- must run reference detector + leakage
  check" standing rule that came out of a prior review pass and
  reappeared in the AML audit. The gate compares baseline
  (log-normal) transaction density against typology density inside
  each currency-specific structuring band; a ratio below 10 % means
  the band effectively IS the label, and an all-empty report is not
  a pass. Writes `leakage_report.parquet` with per-band verdict +
  hint. The reference detector is a scikit-learn Gradient Boosted
  Classifier trained on a deliberately narrow feature set
  (``log_amount_mean``, ``log_amount_std``, ``amount_pct_of_ceiling``,
  ``mean_hour``, ``std_hour``) that excludes both the API-level
  leaks (``amount_in_structuring_band`` and four other columns the
  library refuses to accept) and three known-leaky structural
  proxies against the current datagen (``txn_count``,
  ``unique_counterparties``, any country-set membership feature).
  The trade-off is documented in
  ``score_financial_reference.py``'s ``_build_reference_feature_frame``:
  the corridor and cardinality-planted typologies will read as
  ``recall = 0`` until the datagen ships probabilistic overlays
  that mix baseline and typology distributions in those features.
  Writes `reference_metrics.parquet`. New benchmark aggregate
  `aggregate_reference_vs_rule` joins rule recall with reference
  recall per typology, distinguishing "model didn't run"
  (verdict `not_run`, recall NULL) from "model got 0" (verdict
  `ok`, recall 0.0).

### Changed
- **Datagen: Python image retired; Rust image handles both schemas.** The
  `datagen/` directory (Python generator: `generate.py`, `financial.py`,
  `realism.py`, `typologies.py`, `verify_run.py`, `manifest.py`, Dockerfile,
  tests) has been removed. The Rust image at `datagen_rs/` now serves both
  `--schema customer360` and `--schema financial` and is what
  `docker.io/sillidata/lb-datagen:latest` points to. Per-pod throughput on
  customer360 measured at 590 MB/s (snappy, 8 CPU / 8 Gi), ~76x the Python
  path's 6-8 MB/s. Efficiency 6 CPU-hours per TB written, down from ~344.
- **Datagen default codec is now `snappy`.** Was `zstd1`. Snappy is 40-55%
  faster on both schemas at the cost of ~1.5x on-disk file size. Override
  with `DG_COMPRESSION=zstd` (or `lz4`, `none`) in the pod env if disk size
  matters more than throughput.
- **Continuous-mode datagen pod default memory: 24 Gi -> 8 Gi.** The Rust
  generator uses under 2 GiB per pod in measured runs (vs Python's ~8 GiB
  with per-worker process overhead), so the old 24 Gi lock was 3-8x
  over-provisioned. Continuous mode still hard-locks CPU at 8. Batch mode
  unchanged (4 CPU / 4 Gi).
- **`ScratchStorageConfig.create_storage_class` removed.** The
  StorageClass is Category 2 shared infrastructure: a create-race
  between parallel deploys could strip it out from under an in-flight
  run. `deploy` now preflight-verifies the SC exists and refuses with
  a pointer to `lakebench admin install-scratch-storage-class`.
  Existing YAML that still carries `create_storage_class: true|false`
  loads without error but the field is silently ignored.
- **Spark Operator watch-list mutation now lease-gated in strict mode.**
  `destroy` calls the strict path so parallel destroys cannot race the
  same helm upgrade. On failure, destroy raises
  `WatchListMutationError` and BLOCKS the namespace delete -- deleting
  a namespace the operator still watches crash-loops the operator
  globally, and refusing to delete is the only safe response. The
  user is directed to `lakebench admin repair-operator` to reconcile.

### Removed
- **`platform.storage.scratch.create_storage_class`.** See above.


### AML continuous: time to detect and bronze sizing
- **Higher cluster minimum for AML continuous.** bronze-ingest runs 5
  executors x 4 cores (was 2 x 2) under `schema: financial`, sized from
  run-20260925-104452-21bf3a, where it drained 58% of the scale-10 corpus in
  a 30-minute window. The preflight minimum is now 54 cores / 340 GB at
  scale 1-10 (was 38 / 272) and 106 cores / 788 GB at scale 100 (was 84 /
  690). The concurrent budget keeps silver-stream and gold-refresh at their
  earlier share and warns, naming the job, when it caps a stream.
- **`time_to_detect_seconds`** (median), `_p95_seconds`, `_max_seconds`,
  `_alerts`, `_late_alerts` and `_unmeasured_cycles` on AML continuous
  scorecards: from the newest bronze ingest of an alert's transactions to the
  end of the detection pass that first raised it.
- **`intake_limit`** and **`bronze_busy_fraction`** say whether bronze's own
  processing bounded intake when `ingest_ratio` is short.
- Streaming stages report the executor count actually requested (after the
  concurrent budget), so core-hours and GB per core-hour are right on capped
  clusters.

### AML continuous: silver-stream and gold-refresh sizing
- **Higher cluster minimum for AML continuous.** Under `schema: financial`,
  silver-stream runs 10 executors x 4 cores (was 4) with 80 shuffle
  partitions, adding 8 per 100 scale, and gold-refresh 12 x 4 (was 2) with 96,
  growing 12 per 10 scale to the 28 cap; per-executor sizing unchanged. Sized
  from run-20260925-135005-4b7a97 (scale 10): silver micro-batches took 299 s
  against a 60 s trigger and fell behind bronze, and gold ticks took 349.5 s
  against a 300 s refresh interval while reading a lagging silver, together
  most of the 1,280 s median time to detect. The full request is now 118
  cores / 980 GB / 2,300 Gi scratch at scale 1-10 (was 54 / 340 / 700) and
  222 cores / 1,948 GB / 4,660 Gi at scale 100 (was 106 / 788 / 1,760).
- **Smaller clusters run degraded rather than failing preflight.** In
  continuous mode the capacity check passes with a WARNING naming the capped
  stages when the request after the concurrent budget, plus Trino,
  Hive/Postgres and datagen, fits (AML scale 1-10: 57 cores), and fails
  only when that does not fit or a single pod fits no node. With schema
  overrides the budget now nets out the three drivers before sharing, so a
  capped run does not ask for more cores than the cluster has. The budget gives each
  overridden stage its base-split floor first, then spare cores upstream
  first: bronze, silver up to its keep-up count (7 for AML), gold, then the
  rest of silver.

### Continuous: a corpus larger than the trickle is not saturation (LB-156)
- **Continuous runs held to the trickle rate are no longer reported as
  saturated or failed.** The offered load in continuous mode is the configured
  trickle, `max_files_per_trigger` files per `bronze_trigger_interval` (50 per
  30 s, about 107 MB/s, at every scale and for both workloads); the scale
  factor sets the corpus size, not the rate. The c360 scale-100 run
  run-20260925-191402-bc5b57 took 50 files on each of 60 triggers with bronze
  idle 32% of the window and silver keeping up, yet read `ingest_ratio` 0.19,
  `pipeline_saturated: true` and a failed report. Such a run now reports
  `intake_limit: trickle_rate`, `pipeline_saturated: false`, a report warning
  instead of a failure, and **`corpus_drain_seconds`** (9,600 s for that run),
  the window that would drain the corpus at the rate held.
- `trickle_rate` needs bronze to have run a micro-batch on at least 90% of its
  triggers, each inside the trigger; `pipeline_saturated` is then false only
  if silver also committed all but one silver trigger, one silver batch and
  one bronze trigger of bronze's rows. A late start, a stall, a bronze that
  overruns its trigger, a silver that falls behind, or unknown trigger config
  keeps the saturated verdict.
  `ingest_ratio` is unchanged: the share of the corpus the window consumed.

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
