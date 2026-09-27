# Lakebench Design

This document states what Lakebench is, the objects it is built from, the
rules those objects must obey, and how the product is extended. It is the
durable reference: release plans change, this model should not. Where the
current code departs from the model, the departure is listed under "Open
contradictions" at the end rather than hidden in the model.

Detail lives elsewhere: `docs/architecture.md` (modules, deploy order),
`docs/design/namespace-isolation.md` (ownership), `docs/benchmarking.md`
(scores), `docs/aml-scoring.md` (the AML workload), `docs/recipes.md`.

## 1. Product model

Lakebench runs a real data workload through a composed data architecture on
a known system and records what happened. The model is:

```
workload x architecture x system x execution conditions -> evidence
```

The **system** (Kubernetes, compute, network, storage) is fixed for an
experiment. The **architecture** is composed from a catalog, table format,
pipeline engine and query engine. The **workload** is real business work: a
generated corpus, a pipeline over it, a definition of the correct answer and
measurements; Customer 360 and AML today. The **execution conditions** are
scale, mode, seed, maintenance policy and any limit Lakebench imposes.

A benchmark, a demonstration and a proof are three uses of one experiment,
not three subsystems. A benchmark compares experiments that differ in one
axis; a demonstration shows one experiment's stages and outcome; a proof
shows an architecture ran a workload to the correct answer. All three read
the same evidence.

Composability is the load-bearing property. A workload must mean the same
thing on every architecture it is allowed to run on. If a composition cannot
preserve that meaning, it is rejected, not run and reported.

## 2. Object model

### 2.1 System

The Kubernetes cluster and object store an experiment runs on. Lakebench
observes the system rather than configuring it. Today the evidence records
only the S3 endpoint, buckets and scratch storage class
(`metrics/collector.py` `build_config_snapshot`). Backend behaviour is
characterised by `lakebench config storage` (`s3/conformance.py`), which
reports and never gates.

### 2.2 Architecture

Four component slots, each an enum in `config/schema.py`:

| Slot | Enum | Values today |
|---|---|---|
| Catalog | `CatalogType` | hive, polaris, unity (no supported combination) |
| Table format | `TableFormatType` | iceberg, delta |
| Pipeline engine | `PipelineEngineType` | spark |
| Query engine | `QueryEngineType` | trino, spark-thrift, duckdb, none |

The supported set is `_SUPPORTED_COMBINATIONS` (`config/schema.py`),
enforced at load by `ArchitectureConfig.validate_component_combination`, which
explains rejections via `_COMBINATION_NOTES` and `nearest_supported()`.
Component versions are governed by `_SUPPORTED_SPARK_VERSIONS`,
`_FORMAT_VERSION_COMPAT` and `_ICEBERG_RUNTIME_SUFFIX` in
`modules/pipeline_engines/spark/job.py`.

A **recipe** is a named preset for the four slots plus default images, named
`<catalog>-<format>-<engine>-<query_engine>` (`config/recipes.py`, `RECIPES`,
with `RECIPE_NOTES` for per-recipe caveats). A recipe is architecture only; it
never selects or alters a workload. Every recipe must map to one entry of
`_SUPPORTED_COMBINATIONS`.

Components live under `src/lakebench/modules/` against the protocols in
`modules/base.py` (`CatalogModule`, `TableFormatModule`,
`PipelineEngineModule`, `QueryEngineModule`, `Deployer`), plus
`QueryExecutor` (`benchmark/executor.py`) and `PipelineEngine`
(`engine/protocol.py`, factory `get_engine`). `modules/registry.py` exists but
the deploy engine does not use it yet.

### 2.3 Workload

A workload is defined by five things. Each must exist for a workload to be
credible, and none of them may depend on which architecture runs it.

1. **Corpus and generator.** The Rust generator in `datagen_rs/` produces the
   corpus; `datagen_rs/entrypoint.py` selects the schema (`financial` or
   `customer360`). Scale maps to domain dimensions per workload in
   `config/scale.py`. The corpus is named by generator version plus seed; the
   seed resolves in `config/datagen_seed.py`.
2. **Pipeline stages.** Batch: bronze-verify, silver-build, gold-finalize.
   Continuous: bronze-ingest, silver-stream, gold-refresh. Stage logic is
   Spark scripts in `spark/scripts/` (`*_financial.py` for AML; unsuffixed and
   `*_delta.py` for Customer 360). AML adds scoring and TM operations stages.
3. **Expected correctness.** What a correct run produces. Customer 360 has
   non-empty guards in its stage scripts. AML has the batch honesty gate
   (`cli/_run.py` `_aml_batch_gate_problems`), recall and precision against
   planted ground truth, a reference detector and a leakage gate (`aml/`).
4. **Measurements.** Stage timings and throughput, the query set and its QpH
   (`benchmark/queries.py`, `get_benchmark_queries` keyed by workload, with a
   `query_set_id` per set), and workload-specific scores such as AML recall.
5. **Modes.** The execution modes the workload supports (section 5).

The workload is selected by `architecture.workload.schema`
(`WorkloadSchema` in `config/schema.py`: customer360, financial, custom).
There is no Workload object today; dispatch is by string comparison (see
contradictions).

### 2.4 Experiment

An experiment is one concrete tuple: a workload and its version, a corpus
(generator identity and seed), an architecture (recipe plus component
versions), a system, and execution conditions: scale, mode, maintenance
policy, benchmark iterations, and any Lakebench-imposed cap. The config file
declares the experiment; `lakebench deploy`, `generate` and `run` execute it;
`destroy` removes what it owned.

Caps are part of the experiment, not the system: for example
`_MAX_EXECUTORS_SAFE` and per-job `max_executors` (`job.py`), the 30-minute
pre-benchmark maintenance budget, `workload.w1_max_vertices`.

### 2.5 Evidence

Every run writes under `lakebench-output/` (`_constants.py`
`DEFAULT_OUTPUT_DIR`):

- `runs/run-<id>/metrics.json` (`metrics/collector.py`, `metrics/storage.py`):
  config snapshot, per-stage metrics, mode-conditional scores and code
  provenance (`metrics/provenance.py`).
- `runs/run-<id>/report.html` (`reports/generator.py`, `reports/scorecard.py`).
- `journal/session-<name>.jsonl` (`journal/`): what was done, in order.

A result must identify, where applicable: workload and workload version;
generator identity and seed; recipe and component versions; system identity;
scale; mode; maintenance policy; stages and rules executed or skipped; caps
that applied and whether they bound; and the number of repetitions behind any
figure that claims repeatability. A result that cannot say what produced it
is not evidence.

## 3. Boundaries

Dependencies run one way: the config declares workload, architecture and
conditions; orchestration (`deploy/`, `cli/`) executes them; evidence
(`metrics/`, `reports/`, `journal/`) records the outcome.

- **A workload does not know which architecture runs it.** It may state what
  it requires of an architecture (for example: a table format with snapshot
  retention for replay, or a query engine that can express its query set),
  and the combination check uses those requirements. It does not branch on a
  catalog, format or engine name.
- **A component module does not know which workload runs through it.** It
  deploys its component, supplies its Spark and client configuration,
  translates SQL dialect where needed, and exposes its maintenance
  capability. It must not hold table names, schemas, query text or resource
  sizing for a specific workload, and must not change query semantics when
  adapting dialect.
- **Where both are genuinely needed** (a stage script written for one table
  format, a resource profile that depends on a workload's spill behaviour),
  the pairing is declared data owned by the workload, looked up by
  (workload, component), not an if/elif in engine code.
- **Orchestration does not decide correctness.** It runs stages and hands
  their outputs to the workload's correctness checks; a stage exit code alone
  never makes a run pass.
- **Evidence does not recompute meaning.** Scoring reads what the workload
  declared as its measurements; it does not assume one workload's shape for
  another.

## 4. Invariants

These hold across releases. Changing one is a product decision for the
owner.

1. **Correctness before comparison.** A performance number is valid only for
   a run that completed its workload correctly. Two experiments may be
   compared only if they executed equivalent work and produced equivalent
   workload results; otherwise the comparison is refused, not caveated.
2. **Non-degenerate pass.** A successful exit is not evidence if the output
   is empty, degenerate, skipped or otherwise invalid. Every stage and every
   gate checks what it produced, not that it returned.
3. **Provenance.** Published evidence identifies the workload and the
   conditions that produced it (section 2.5). Missing identity is a defect in
   the evidence, not a gap the reader fills in.
4. **Caps are not infrastructure performance.** When a Lakebench limit binds
   (an executor cap, a time budget, a vertex or alert cap), the evidence says
   so, and the bound figure is never presented as what the system can do.
5. **Repeatability is labelled.** A published measurement that claims
   repeatability is either repeated or labelled `n=1`, with the sample count
   carried in the evidence.
6. **Destroy isolation.** Destroying deployment A never affects deployment B.
   The four ownership categories (per-deployment, shared read-only
   infrastructure, shared operators, shared mutable state) are defined in
   `docs/design/namespace-isolation.md` and enforced by `deploy/ownership.py`
   (identity stamps, bucket tags, deploy nonce), `deploy/cluster_lock.py` and
   `deploy/destroy.py` (UID and nonce re-check). Destroy deletes only
   category 1 resources it can prove it owns.
7. **Held-out evaluation data and pre-registered measurement semantics are
   immutable without an owner decision.** AML evaluation and robustness
   corpora are generated and scored once, as the registered run for their
   role; spent seeds are refused. The protocol is the pre-registration at
   `src/lakebench/spark/data/aml/aml_preregistration.json`, its look record
   `aml_registered_looks.json` beside it, and the guard in
   `config/datagen_seed.py`. Gate constants and metric definitions registered
   there change only by owner decision.
8. **Workload meaning is architecture-independent.** Swapping a component
   must not silently change what the workload computes. A combination that
   cannot preserve the meaning is rejected at config load.

## 5. Terminology

- **Batch mode**: the workload's stages run once, sequentially, over a
  generated corpus. Primary score: time to value.
- **Continuous mode**: the workload's stages run concurrently over a corpus
  that keeps arriving, with periodic gold refresh and maintenance. Primary
  score: data freshness.
- There is no "streaming" mode; do not call continuous mode streaming, even
  though it uses Spark Structured Streaming internally.
- The code still says `sustained` for continuous: `PipelineMode.SUSTAINED`,
  `pipeline.sustained`, `--sustained` and `pipeline_mode="sustained"` in
  metrics. Read `sustained` as continuous.
- **Datagen mode** (`DatagenMode`: batch, continuous, auto) is a different
  concept: the generator's process model and resource profile, not the
  pipeline mode.
- **Workload schema** is the config name (`customer360`, `financial`);
  "financial" and "AML" name the same workload.

## 6. Extension rules

### 6.1 Adding a workload

A new workload brings all five parts of section 2.3 and touches no
component module:

1. A generator schema in `datagen_rs/` and its entry in
   `datagen_rs/entrypoint.py`, with a seed policy and dimensions in
   `config/scale.py`.
2. Stage scripts for each mode it supports, per table format it supports.
3. Correctness checks that fail the run on wrong or degenerate output, and a
   statement of what correct means that a reviewer can check.
4. A query set with its own `query_set_id`, and any workload scores, declared
   so the collector and report render them without guessing.
5. A declaration of which formats, engines and modes it supports. Anything
   not declared is rejected at config load.

### 6.2 Adding a catalog, table format, pipeline engine or query engine

1. Implement the protocol in `modules/base.py` and a `Deployer` whose
   resources carry the stamps of `deploy/ownership.py`.
2. Add the enum value, the 4-tuples to `_SUPPORTED_COMBINATIONS`, a reason in
   `_COMBINATION_NOTES` per known-bad pairing, version entries in the
   compatibility tables (verify artifacts by direct fetch), and a recipe per
   tuple.
3. A query engine's `adapt_query` changes dialect only, never semantics.
4. Per workload, show equivalent results to a supported composition.

### 6.3 Adding a query

Queries belong to a workload's set in `benchmark/queries.py`. Changing one
changes the set's `query_set_id`, and QpH across ids is refused
(`qph_comparable`). A query must return a non-empty, checkable result.

### 6.4 Adding a metric

A metric is defined once, with unit, direction, meaning and the workloads
and modes it applies to, in the collector's descriptions
(`metrics/collector.py`). One that depends on a cap or a simulated parameter
says so. Changing a published metric's meaning is a product decision.

### 6.5 When a combination is "supported"

A combination (workload, catalog, format, pipeline engine, query engine,
mode) is supported only when:

- config load accepts it and rejects its neighbours that are known broken,
  with a reason;
- a live run on the release tree completed it end to end with correctness
  checks passing and non-degenerate output;
- its workload results match an existing supported composition, or it is the
  reference for that workload;
- any upstream limitation that changes behaviour (such as a skipped
  maintenance step) is in `RECIPE_NOTES` and in the evidence.

Incompatible combinations are rejected at config load, never discovered as a
failed job.

## 7. Open contradictions

Ordered by impact on the mission. **[owner]** needs a product decision;
**[impl]** is implementation work inside the model above.

1. **Comparisons never check that results match.** [impl] `_build_comparison`
   (`cli/_compare.py:299-375`) refuses QpH across query sets and only warns on
   sample count and maintenance policy; the perf gate guards config and volume
   only (`metrics/perf_gate.py:22-23`). `rows_returned` is captured
   (`benchmark/runner.py:37`) but never compared, and no result digest exists.
   Fix: per-query digests and per-stage row counts; refuse on mismatch.

2. **Degenerate runs can pass.** [impl] Job success is Spark state alone
   (`modules/pipeline_engines/spark/monitor.py:127-134`); a benchmark
   exception leaves the run successful (`cli/_run.py:2320-2324`); no validity
   flag gates `composite_qph` or throughput; the report shows a scale ratio of
   0 as "Complete" (`reports/generator.py:700-704`). Fix: one run-level
   validity verdict honoured by scores, report and exit code.

3. **AML on Delta is accepted and runs Iceberg code.** [impl]
   `_SUPPORTED_COMBINATIONS` (`config/schema.py:1232-1253`) has no workload
   axis; `job.py:1774-1797` picks financial scripts before checking format, and
   they hard-code `USING iceberg` (`silver_build_financial.py:189`,
   `deploy/financial_ddl.py:114`). Fix: workloads declare supported formats
   and modes; reject at load.

4. **Customer 360 has no expected-correctness definition.** [impl] Its
   scripts only refuse empty input (`bronze_verify.py:139`,
   `silver_build.py:449`, `gold_finalize.py:313`). Nothing checks gold KPIs
   against what the generator produced, so a wrong but non-empty dashboard
   passes. Fix: derive expected aggregates from the generator and check them.

5. **Evidence does not identify corpus, system or effective conditions.**
   [impl] `build_config_snapshot` (`metrics/collector.py:1767-1860`) omits the
   datagen seed and corpus role (recorded only by the AML reference score,
   `score_financial_reference.py:628`), format and catalog versions, recipe
   name, image digests, Kubernetes version and storage backend. Skipped stages
   other than `--skip-maintenance` are not recorded, and engine-driven
   maintenance skips (`cli/_sustained.py:964-978`) keep the full policy id.
   `config_sha256` is read by the perf gate (`perf_gate.py:656`) and written
   nowhere. Fix: record effective conditions and fingerprint on them.

6. **Recorded benchmark settings are not what ran.** [impl] The snapshot
   records `benchmark.mode`, `streams` and `cache` (`collector.py:1846-1848`),
   but `lakebench run` always calls `run_power(cache="hot", ...)`
   (`cli/_run.py:2237-2239`). Fix: record what executed; reject ignored
   settings.

7. **Caps are not recorded as caps.** [impl] Whether `_MAX_EXECUTORS_SAFE` or
   a job's `max_executors` (`job.py:49`, `:62`) bound, the maintenance budget
   and query timeouts (`cli/_run.py:2052-2054`) are absent from metrics.json;
   only settle and maintenance stops are flagged (`collector.py:786`). Fix: a
   `caps` section with value and "bound" per cap, shown in the report.

8. **Nothing labels `n=1`.** [impl] Baselines are one run
   (`perf_gate.py:1120`), compare's noise floor is a fixed figure
   (`cli/_compare.py:156-159`), and pipeline scores carry no repetition count.
   Fix: sample count on every published figure; label single runs.

9. **Workload is nested in architecture and has no object.** [impl; moving
   the YAML key is owner] `ArchitectureConfig.workload` and `.tables`
   (`config/schema.py:1596-1598`) hold workload identity and table names;
   `financial_table_defaults` (`:1623-1648`, dead code `:1649-1654`) rewrites
   names from an architecture validator; dispatch is `== "financial"` across
   `cli/_run.py`, `deploy/datagen.py:63`, `job.py` and `config/scale.py`;
   local mode hard-codes c360 tables (`local_job.py:196`, `cli/_local.py`).
   Fix: one Workload definition looked up by name.

10. **Custom workloads fall back to Customer 360.** [owner]
    `WorkloadSchema.CUSTOM` maps to the c360 query set
    (`benchmark/queries.py:616`) and unknown schemas fall back to it, so a
    custom workload would publish c360 QpH. Recommendation: reject `custom`
    until workloads can be declared.

11. **DuckDB bypasses the catalog.** [owner] `duckdb/executor.py:210-237`
    rewrites tables to `iceberg_scan`/`delta_scan` on a guessed path, so the
    catalog is not in the query path. Recommendation: keep the recipes, label
    them catalog-bypassing, exclude them from catalog comparisons.

12. **Observability deploys shared cluster objects from `deploy`.** [impl]
    When enabled (default off, `config/schema.py:1733`),
    `deploy/observability.py:143-158` installs kube-prometheus-stack, a
    category 3 chart with CRDs and cluster roles, without stamp or lock, and
    destroy uninstalls it (`deploy/destroy.py:2028-2037`). Fix: install via
    `lakebench admin` like the other operators.

13. **Workload sizing lives in the pipeline-engine module.** [impl]
    `_JOB_PROFILES` (`job.py:51`) is c360-shaped, patched by
    `_SCHEMA_PROFILE_OVERRIDES["financial"]` (`job.py:211`); AML-only job types
    register for every workload (`job.py:1798-1806`). Fix: resource demands
    belong to the workload definition.

14. **The registry and part of the protocol surface are unused.** [impl]
    `modules/registry.py:1-10` states deploy and destroy do not use it;
    `TableFormatModule.get_pipeline_scripts` (`modules/base.py:188`) has no
    implementation or caller. Fix: route through the registry, or delete.

15. **The code deprecates the product's mode name.** [owner]
    `PipelineMode.SUSTAINED` (`config/schema.py:164-175`); `--continuous` is a
    hidden alias of `--sustained` (`cli/_run.py:1109-1120`);
    `pipeline.continuous` is deprecated (`config/schema.py:910-926`);
    `ProcessingPattern` (`:126-133`) adds "streaming", read only by the
    autosizer (`config/autosizer.py:558`). Recommendation: make `continuous`
    canonical, keep `sustained` as an alias, remove `ProcessingPattern`.

16. **Config fields nothing reads.** [impl] `ImagesConfig.prometheus` and
    `.grafana` (`config/schema.py:210-211`), `ReportsConfig` (`:1711-1722`),
    `IcebergConfig.file_format` and `.properties` (`:549-550`) look like
    controls and change nothing. Fix: remove with a clear error on use.

17. **Published docs disagree with the supported set.** [impl]
    `docs/architecture.md` and `docs/supported-components.md:122` omit the
    three Hive + Delta recipes; `docs/compatibility-matrix.md:175` presents
    Unity + Delta as working though no Unity combination is supported; the
    config template lists a nonexistent `iot` schema (`config/loader.py:664`).
    Fix: generate these tables from `_SUPPORTED_COMBINATIONS` and `RECIPES`.
