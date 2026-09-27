# Lakebench Design

This document states what Lakebench is, the objects it is built from, the
rules those objects must obey, and how the product is extended. It is the
durable reference: release plans change, this model should not. Where the
current code departs from the model, the departure is recorded in
`docs/internal/design-contradictions.md` (maintainer material, not shipped
with the package) rather than hidden in the model.

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

Benchmarking, demonstration and proof are different uses of the same
experiment, not three subsystems; all three read the same evidence. How each
use is defined in detail (for example, what a proof must show) is a product
decision and is not fixed here.

Composability is the load-bearing property. A workload must mean the same
thing on every architecture it is allowed to run on. If a composition cannot
correctly support a workload, it is rejected at config load, or clearly
excluded and labelled in the evidence; it is never presented as comparable.

## 2. Object model

### 2.1 System

The Kubernetes cluster and object store an experiment runs on. Lakebench
mostly observes the system rather than configuring it: shared infrastructure
such as StorageClasses and operators is installed once by `lakebench admin`,
and `deploy` only verifies it. The exceptions are opt-in: `deploy` installs
the Spark or Stackable operator when its `install` option is set
(`SparkOperatorConfig`, `StackableOperatorConfig`) and the observability stack
when `observability.enabled` is set. What the evidence records about
the system comes from `build_config_snapshot` (`metrics/collector.py`).
Backend behaviour is characterised by `lakebench config storage`
(`s3/conformance.py`), which reports and never gates.

### 2.2 Architecture

Four component slots, each an enum in `config/schema.py`: `CatalogType`,
`TableFormatType`, `PipelineEngineType` and `QueryEngineType`. An enum value
is a name the config accepts, not a promise of support; `CatalogType` and
`QueryEngineType` both carry a `none` value, and a value that appears in no
supported combination can never pass validation. `docs/recipes.md` and
`docs/compatibility-matrix.md` list what is supported in a given release.

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
orchestration does not route through it yet.

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
3. **Expected correctness.** What a correct run produces, and the gates that
   fail a run that did not produce it. Existing gates: non-empty guards in
   the Customer 360 stage scripts; `_benchmark_gate_problems` (failed
   queries) and, for AML, `_aml_batch_gate_problems` (crashed rules, zero
   alerts) and `_aml_tm_verdict` (TM operations invariants) in `cli/_run.py`;
   in continuous mode `_c360_continuous_gate_problems` (zero rows) and the
   AML continuous gate (no gold-refresh logs or zero alerts) in
   `cli/_sustained.py`. AML also scores recall and precision against planted
   ground truth and has a reference detector and leakage gate (`aml/`).
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

Caps are part of the experiment, not the system: for example the executor
ceiling `_MAX_EXECUTORS_SAFE` and per-job `max_executors` (`job.py`), the
pre-benchmark maintenance budget (`PRE_BENCHMARK_MAINTENANCE_CAP`,
`cli/_run.py`) and `workload.w1_max_vertices`.

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

These are the product's invariants as the mission states them. The owner
alone changes them.

1. **Correctness before comparison.** Performance results are valid only when
   the workload completed correctly, and a performance comparison is invalid
   when the compared systems produced different workload results. How the
   tooling acts on an invalid comparison is an implementation and product
   question: today `lakebench compare` warns, while the perf gate and
   `lakebench reproduce` refuse.
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
7. **Held-out evaluation data.** Held-out AML evaluation data must not be
   used during development, and pre-registered measurement semantics do not
   change without an owner decision. The registered AML protocol defines the
   mechanism; see `docs/aml-scoring.md`.
8. **Workload meaning is architecture-independent.** Changing a component
   must not silently change the meaning of the workload. A combination that
   cannot preserve it is rejected at config load, or clearly excluded and
   labelled in the evidence.

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
(`qph_comparable`). A new query should return a non-empty result whose
correctness can be checked on a correct corpus; the harness does not yet
enforce this (see the contradictions file).

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

Incompatible combinations are rejected at config load, or clearly excluded
and labelled in the evidence; they are never discovered as a failed job and
never presented as comparable. A combination the config accepts is not
thereby supported.

## 7. Open contradictions

Where the implementation or published docs disagree with this model is
tracked, with file:line evidence and a recommended resolution, in
`docs/internal/design-contradictions.md`.


