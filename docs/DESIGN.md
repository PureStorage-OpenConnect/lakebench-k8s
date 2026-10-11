# Lakebench Design

This document states what Lakebench is, the objects it is built from, the
rules those objects must obey, and how the product is extended. It is the
durable reference: release plans change, this model should not. Where the
current code departs from the model, the departure is tracked as a
maintainer concern rather than hidden in the model.

Detail lives elsewhere: `docs/architecture.md` (modules, deploy order),
`docs/benchmarking.md` (scores), `docs/aml-scoring.md` (the AML workload),
`docs/recipes.md`.

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

Components live under `src/lakebench/modules/`. Query engines implement
`QueryExecutor` (`benchmark/executor.py`). The pipeline engine is created by
`get_engine` (`engine/protocol.py`); the `PipelineEngine` interface there
does not yet match what Spark implements.

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
   infrastructure, shared operators, shared mutable state) are enforced by
   `deploy/ownership.py` (identity stamps, bucket tags, deploy nonce),
   `deploy/cluster_lock.py` and `deploy/destroy.py` (UID and nonce
   re-check). Destroy deletes only category 1 resources it can prove it
   owns.
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

See [Development](development.md#extension-rules) for extension rules.

## 7. Open contradictions

Where the implementation or published docs disagree with this model is
tracked with file:line evidence, a recommended resolution and the owner
decisions taken. That record is maintainer-only and not shipped with the
package.
