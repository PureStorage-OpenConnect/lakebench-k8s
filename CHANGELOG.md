# Changelog

All notable changes to Lakebench are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
