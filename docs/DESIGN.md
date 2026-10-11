# Lakebench Design

This document states what Lakebench is, the objects it is built from, the
rules those objects must obey, and how the product is extended. It is the
durable reference: release plans change, this model should not. Where the
current code departs from the model, the departure is recorded in
`docs/internal/design-contradictions.md` (maintainer material, not shipped
with the package) rather than hidden in the model.

Detail lives elsewhere: `docs/architecture.md` (modules, deploy order),
`docs/internal/namespace-isolation.md` (ownership), `docs/benchmarking.md`
(scores), `docs/aml-scoring.md` (the AML workload), `docs/recipes.md`.

## 1. Product model

Lakebench runs a real data workload through a composed data architecture on
a known system and records what happened. The model is:

```
workload x architecture x system x execution conditions -> evidence
```

- The **system** (Kubernetes, compute, network, storage) is known for every
  experiment. It is held constant across a controlled comparison unless the
  system itself is the variable under test.
- The **architecture** is a composition of a catalog, table format, pipeline
  engine and query engine, together with the access paths between them.
- The **workload** is real business work: a generated corpus, a pipeline over
  it, a definition of the correct answer and measurements. Customer 360 and
  AML today.
- The **execution conditions** are scale, mode, seed, maintenance policy and
  any limit Lakebench imposes.

A controlled comparison holds every factor but one constant and attributes
the difference to that one:

```
same workload, same system, same conditions, architecture A vs B -> architecture differential
same workload, same architecture, same conditions, system X vs Y -> system differential
```

Benchmarking, demonstration and proof are different uses of the same
experiment, not three subsystems; all three read the same evidence. How each
use is defined in detail (for example, what a proof must show) is a product
decision and is not fixed here.

Composability is the load-bearing property. Workload semantics are
architecture-independent: a workload must mean the same thing on every
architecture it is allowed to run on. If a composition cannot correctly
support a workload, it is rejected at config load, or clearly excluded and
labelled in the evidence; it is never presented as comparable.

## 2. Object model

### 2.1 System

The Kubernetes cluster, its compute and network, and the object store an
experiment runs on. Shared cluster infrastructure and operators are
administered separately from an experiment, through `lakebench admin`.
`deploy` verifies them and creates only experiment-owned resources. The
evidence identifies the system (`build_config_snapshot`,
`metrics/collector.py`). Object-store behaviour is characterised by
`lakebench config storage` (`s3/conformance.py`), which reports and never
gates.

### 2.2 Architecture

An architecture is the composition of four components plus the access paths
between them. Each component slot is an enum in `config/schema.py`:
`CatalogType`, `TableFormatType`, `PipelineEngineType` and `QueryEngineType`.
An enum value is a name the config accepts, not a promise of support;
`CatalogType` and `QueryEngineType` both carry a `none` value.

**Access paths are part of the architecture.** Trino reads through the
catalog: query engine -> catalog -> table metadata -> storage. DuckDB reads
table metadata and storage directly, without the catalog. The access path is
part of the architecture actually instantiated and is recorded in the
evidence (for example `query_access_path=direct_storage`).

Two whole compositions are comparable when their workload results match. Any
difference in effective execution conditions, such as maintenance policy, is
shown alongside the comparison, and it is not labelled like-for-like
(section 6.5). Attributing a difference to one component in isolation is not
valid when the access paths differ.

`_SUPPORTED_COMBINATIONS` (`config/schema.py`) lists the structurally valid
architecture compositions. It is enforced at load by
`ArchitectureConfig.validate_component_combination`, which explains
rejections via `_COMBINATION_NOTES` and `nearest_supported()`. Structural
validity is necessary for release support, not sufficient (section 6.5).
Component versions are governed by `_SUPPORTED_SPARK_VERSIONS`,
`_FORMAT_VERSION_COMPAT` and `_ICEBERG_RUNTIME_SUFFIX` in
`modules/pipeline_engines/spark/job.py`.

A **recipe** is a named preset for the four slots plus default images, named
`<catalog>-<format>-<engine>-<query_engine>` (`config/recipes.py`, `RECIPES`,
with `RECIPE_NOTES` for per-recipe caveats). A recipe is architecture only; it
never selects or alters a workload. Every recipe maps to one entry of
`_SUPPORTED_COMBINATIONS`.

Components live under `src/lakebench/modules/` against the protocols in
`modules/base.py` (`CatalogModule`, `TableFormatModule`,
`PipelineEngineModule`, `QueryEngineModule`, `Deployer`), plus
`QueryExecutor` (`benchmark/executor.py`) and `PipelineEngine`
(`engine/protocol.py`, factory `get_engine`). `modules/registry.py` exists but
orchestration does not route through it yet.

### 2.3 Workload

A workload is defined by five things. Each must exist for a workload to be
credible. The workload's semantics and correctness contract are
architecture-independent. Its implementations may be component-specific (for
example a stage script per table format), but every implementation must
preserve the workload's meaning and pass the same correctness contract.

1. **Corpus and generator.** The Rust generator in `datagen_rs/` produces the
   corpus; `datagen_rs/entrypoint.py` selects the schema (`financial` or
   `customer360`). Scale maps to domain dimensions per workload in
   `config/scale.py`. The corpus is named by generator version plus seed; the
   seed resolves in `config/datagen_seed.py`.
2. **Pipeline stages.** Batch: bronze-verify, silver-build, gold-finalize.
   Continuous: bronze-ingest, silver-stream, gold-refresh. Stage logic is
   Spark scripts in `spark/scripts/` (`*_financial.py` for AML; unsuffixed and
   `*_delta.py`, a per-format adapter, for Customer 360). AML adds scoring and
   TM operations stages.
3. **Correctness contract.** What a correct run produces, and the gates that
   fail a run that did not produce it. Existing gates:
   - non-empty guards in the Customer 360 stage scripts;
   - in `cli/_run.py`: `_benchmark_gate_problems` (failed queries) and, for
     AML, `_aml_batch_gate_problems` (crashed rules, zero alerts) and
     `_aml_tm_verdict` (TM operations invariants);
   - in continuous mode, in `cli/_sustained.py`:
     `_c360_continuous_gate_problems` (zero rows) and the AML continuous gate
     (no gold-refresh logs or zero alerts).

   AML also scores recall and precision against planted ground truth and has
   a reference detector and leakage gate (`aml/`). Customer 360 expected
   results are derived by the implementation; the owner approves their
   meaning before they gate a run.
4. **Measurements.** Stage timings and throughput, the query set and its QpH
   (`benchmark/queries.py`, `get_benchmark_queries` keyed by workload, with a
   `query_set_id` per set), and workload-specific scores such as AML recall.
5. **Modes.** The execution modes the workload supports (section 5).

The workload is the top-level `workload.schema` config key (`WorkloadSchema`
in `config/schema.py`). The old `architecture.workload` location loads with a
deprecation warning. `custom` is refused at load. There is no Workload object
yet; dispatch is by string comparison.

### 2.4 Experiment

An experiment is one concrete tuple: a workload and its version, a corpus
(generator identity and seed), an architecture (recipe, component versions
and access paths), a system, and execution conditions: scale, mode,
maintenance policy, benchmark iterations, and any Lakebench-imposed cap. The
config file declares the experiment; `lakebench deploy`, `generate` and
`run` execute it; `destroy` removes what it owned.

**Maintenance policy is an execution condition**, not workload semantics.
The effective policy (what actually ran) is always stamped in the
evidence, and runs with different effective policies are not like-for-like.
Where a composition cannot execute the requested policy (DuckDB cannot run
maintenance, for example), the difference from the request is visible.

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

A result must identify, where applicable:

- workload and workload version;
- generator identity and seed;
- recipe, component versions and access paths;
- system identity, scale, mode and effective maintenance policy;
- stages and rules executed or skipped;
- caps that applied and whether they bound;
- the support state of the combination (section 6.5);
- the number of repetitions behind any figure that claims repeatability.

A result that cannot say what produced it is not evidence.

## 3. Boundaries

Dependencies run one way: the config declares workload, architecture and
conditions; orchestration (`deploy/`, `cli/`) executes them; evidence
(`metrics/`, `reports/`, `journal/`) records the outcome.

- **Workload semantics do not depend on the architecture.** A workload states
  what it requires of an architecture, and the compatibility check uses those
  requirements. Examples: a table format with snapshot retention for replay,
  or a query engine that can express its query set.
- **Adapters may be component-specific; meaning may not.** A workload may
  need a per-component implementation (a stage script per table format, a
  resource profile for a workload's spill behaviour). That adapter is
  declared data owned by the workload, looked up by (workload, component). It
  must pass the workload's correctness contract. It is not an if/elif in
  engine code.
- **A component module does not know which workload runs through it.** It
  deploys its component, supplies its Spark and client configuration,
  translates SQL dialect where needed, and exposes its maintenance
  capability. It must not hold table names, schemas, query text or resource
  sizing for a specific workload, and must not change query semantics when
  adapting dialect.
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
   when the compared runs produced different workload results.
   Every run's report shows the evidence: a result fingerprint per query,
   and for an AML batch run its alert set. A reader can see whether two
   runs produced the same results. `reproduce` refuses a
   run whose query results differ from their reference.
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
   `docs/internal/namespace-isolation.md` and enforced by `deploy/ownership.py`
   (identity stamps, bucket tags, deploy nonce), `deploy/cluster_lock.py` and
   `deploy/destroy.py` (UID and nonce re-check). Destroy deletes only
   category 1 resources it can prove it owns.
7. **Held-out evaluation data.** Held-out AML evaluation data must not be
   used during development, and pre-registered measurement semantics do not
   change without an owner decision. The registered AML protocol defines the
   mechanism; see `docs/aml-scoring.md`.
8. **Workload semantics are architecture-independent.** Changing a component
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
- `continuous` is the canonical name. `sustained` is a transitional alias
  that the code still uses (`PipelineMode.SUSTAINED`, `pipeline.sustained`,
  `--sustained`, `pipeline_mode="sustained"` in metrics); read it as
  continuous.
- **Datagen mode** (`DatagenMode`: batch, continuous, auto) is the S3
  delivery pattern for the corpus: `batch` = one PUT per Parquet file,
  `continuous` = S3 multipart upload as row-groups close, `auto` =
  `continuous` at every scale (owner, 2026-09-28).
  - Row content is byte-identical across modes at a fixed seed.
  - Pod CPU and memory are sized by scale via the autosizer, independently of
    delivery mode. This enum is not a resource-profile tier.
- **Workload schema** is the config name (`customer360`, `financial`);
  "financial" and "AML" name the same workload.
- **Support states**: supported, unverified, unsupported (section 6.5).

## 6. Extension rules

### 6.1 Adding a workload

A new workload brings all five parts of section 2.3. It adds no
workload-specific logic to component modules; any per-component adapter it
needs is owned by the workload and passes its correctness contract.

1. A generator schema in `datagen_rs/` and its entry in
   `datagen_rs/entrypoint.py`, with a seed policy and dimensions in
   `config/scale.py`.
2. Stage scripts for each mode it supports, with per-format adapters where a
   format needs one.
3. A correctness contract: checks that fail the run on wrong or degenerate
   output, and a statement of what correct means that a reviewer can check
(for Customer 360 the owner approves its meaning before it gates, D6).
4. A query set with its own `query_set_id`, and any workload scores, declared
   so the collector and report render them without guessing.
5. A declaration of which formats, engines and modes it is compatible with.
   Anything not declared is rejected at config load.

### 6.2 Adding a catalog, table format, pipeline engine or query engine

1. Implement the protocol in `modules/base.py` and a `Deployer` whose
   resources carry the stamps of `deploy/ownership.py`.
2. Add the enum value, the structurally valid 4-tuples to
   `_SUPPORTED_COMBINATIONS`, a reason in `_COMBINATION_NOTES` per known-bad
   pairing, version entries in the compatibility tables (verify artifacts by
   direct fetch), and a recipe per tuple.
3. Declare the access path the component uses (through the catalog, or
   direct to metadata and storage) so the evidence records it.
4. A query engine's `adapt_query` changes dialect only, never semantics.
5. Per workload, show equivalent results to a supported composition.

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

### 6.5 Support states

Support is judged over workload x mode x architecture, in layers:

1. **Architecture-valid**: the composition is in `_SUPPORTED_COMBINATIONS`.
2. **Workload-compatible**: the workload declares it can run on that
   composition.
3. **Mode-compatible**: the workload declares the mode on that composition.
4. **Validated**: a live run on the release tree completed it end to end with
   the correctness contract passing and non-degenerate output, and its
   results match an existing supported composition or it is the reference
   for that workload.

A combination passing all four is **supported**. One passing 1 to 3 but not
validated on the release tree is **unverified**. One failing 1, 2 or 3 is
**unsupported** and is refused: rejected at config load, or clearly excluded
and labelled in the evidence; it is never discovered as a failed job and never
presented as comparable. Upstream limitations that change behaviour are
recorded in `RECIPE_NOTES` and in the evidence.

- **Supported**: correct and release-validated for this workload x
  architecture x mode.
- **Unverified**: structurally valid and ran correctly, but not
  release-validated as supported.
- **Unsupported**: known unable to preserve workload semantics, or known
  broken. Refused.

Unverified experiments may be reported and compared when their correctness
checks pass. Their support status must be visible in the evidence and in
comparison output, and they must not be presented as proof that the
combination is supported. This keeps Lakebench usable for exploring a
combination before it is supported.

Support is independent of comparability, which has two levels:

- **Comparable**: the workload results are equivalent.
- **Like-for-like**: comparable, and the relevant execution conditions
  (for example the effective maintenance policy) also match.

An unverified run can therefore be unverified, comparable and not
like-for-like at once. That is legitimate evidence when Lakebench states
exactly which it is.

## 7. Open contradictions

Where the implementation or published docs disagree with this model is
tracked, with file:line evidence, a recommended resolution and the owner
decisions taken, in `docs/internal/design-contradictions.md`.
