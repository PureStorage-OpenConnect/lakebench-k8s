# LakeBench Next Specification

Working draft. Two headline changes: Spark 4.2 as the new default runtime across every recipe, and a new workload domain (FinServ-Crime, AML v1) that adds a bank-realistic ISO 20022 data model and workload set. Structured as requirements first, then solution architecture, then engineering specifications, so each design choice traces back to something that has to be true and forward to something that has to be built. Existing Customer360 recipes continue to work under Spark 4.2 with no schema change; new recipes ship alongside.

Requirement IDs use the pattern REQ-`{category}`-`{n}`. Solution architecture elements use SOL-`{category}`-`{n}` and cite the requirements they satisfy. Engineering specifications use ENG-`{category}`-`{n}` and cite the solution elements they implement. A traceability matrix (Appendix J, to be populated) lets any reader walk from any layer to the others.

This spec is written to be implementable by human engineers, LLM code agents, or combinations of both. Part 0 below states the requirements the spec itself must satisfy for that to work, and the discovery/verify/assess phase every implementer runs before writing code. Part 5 defines the Workload/Component Addition standard (WCA) for future OSS contributions to lakebench-k8s, using this spec's structural conventions.

---

# Part 0. Implementation Preamble

## 0.1 Agent-executability requirements

**REQ-A-01** (unambiguous references). Every engineering specification shall reference existing artifacts by full path, function or class name, config key, or DDL.
- Acceptance: no engineering spec block contains phrases like "the relevant script" or "in the appropriate module"; every reference resolves to a specific location in the lakebench-k8s repo.
- Rationale: an LLM agent (and often a human) cannot correctly implement a spec that assumes context they do not have.

**REQ-A-02** (atomic tasks). Engineering specifications shall be decomposable into implementation tasks that are PR-sized and have declared dependencies.
- Acceptance: each ENG-* block corresponds to at most one merge unit; dependencies between blocks are declared in the ENG header; the DAG has no cycles.
- Rationale: LLM agents work best on bounded tasks; the same decomposition makes human review tractable.

**REQ-A-03** (verification criteria). Each engineering specification shall include a verification criterion expressed as a specific test invocation or observable output.
- Acceptance: every ENG-* block ends with a "Verification" line naming a pytest command, a CLI invocation with expected output, or a specific artifact to inspect.
- Rationale: "did I implement this correctly" must be answerable without appeal to interpretation.

**REQ-A-04** (scope boundaries). Each Part and each ENG block shall declare what is out of scope for that unit.
- Acceptance: every Part has an explicit "not this Part" statement; every ENG block that could reasonably be interpreted broadly has a "not this task" line.
- Rationale: LLM agents (and eager humans) tend to over-implement; explicit scope keeps units mergeable.

**REQ-A-05** (no unspecified choices). Engineering specifications shall not require the implementer to make architectural choices not already made in the spec.
- Acceptance: no ENG block contains phrases like "figure out the right approach", "decide during implementation", or "choose between X and Y"; where a choice exists, the spec makes it and cites a rationale.
- Rationale: agent-authored code is only as good as the spec; unspecified choices produce inconsistent implementations across runs and reviewers.

## 0.2 Discovery: read these

Before writing code for this spec, the implementer reads the following files in the lakebench-k8s repository. Purpose is to ground every subsequent claim in the actual current state.

- Repo shape: `README.md`, `docs/architecture.md`, `docs/configuration.md`, `docs/recipes.md`, `docs/getting-started.md`, `docs/data-generation.md`, `docs/datagen-schema.md`, `docs/running-pipelines.md`, `docs/benchmarking.md`, `docs/compatibility-matrix.md`, `docs/supported-components.md`
- Top-level module layout: `src/lakebench/__init__.py`, `src/lakebench/__main__.py`, `src/lakebench/_constants.py`
- Config layer: `src/lakebench/config/schema.py`, `src/lakebench/config/scale.py`, `src/lakebench/config/loader.py`, `src/lakebench/config/recipes.py`, `src/lakebench/config/autosizer.py`
- Deploy layer: `src/lakebench/deploy/engine.py`, `src/lakebench/deploy/datagen.py`, `src/lakebench/deploy/observability.py`, `src/lakebench/deploy/iceberg.py`
- Datagen: `datagen/generate.py`, `src/lakebench/templates/datagen/configmap.yaml.j2`
- Batch pipeline: `src/lakebench/spark/scripts/bronze_verify.py`, `src/lakebench/spark/scripts/silver_build.py`, `src/lakebench/spark/scripts/gold_finalize.py`, and their `_delta` variants
- Sustained mode: `src/lakebench/cli/_sustained.py`, `src/lakebench/spark/scripts/bronze_ingest.py`, `src/lakebench/spark/scripts/silver_stream.py`, `src/lakebench/spark/scripts/gold_refresh.py`
- Benchmarking: `src/lakebench/benchmark/queries.py`, `src/lakebench/benchmark/runner.py`, `src/lakebench/benchmark/executor.py`
- Metrics and reporting: `src/lakebench/metrics/collector.py`, `src/lakebench/metrics/storage.py`, `src/lakebench/reports/generator.py`
- Tests: `tests/test_functional_pipeline.py`, `tests/test_e2e.py`, `tests/test_config.py`, `tests/test_scale.py`

## 0.3 Verify: check these assumptions

The spec was written against a snapshot of lakebench-k8s v1.3.1 (2026-04-05) and updated after a live-repo review. Before writing code, verify each of the following holds. If any check fails: pause implementation, open an issue documenting the drift, do not proceed with spec sections that depend on the failed assumption.

| # | Claim | How to verify | Confirmed at review |
|---|---|---|---|
| V-01 | `WorkloadSchema` enum in `config/schema.py` contains only `CUSTOMER360` and `CUSTOM` | `grep -A5 "class WorkloadSchema" src/lakebench/config/schema.py` | yes; add `FINANCIAL` for Financial (see V-11) |
| V-02 | `SCHEMA_DIMENSION_MAP` in `config/scale.py` contains `customer360`, `iot`, `financial` | `grep -A5 "SCHEMA_DIMENSION_MAP" src/lakebench/config/scale.py` | yes; `financial_dimensions()` is an unimplemented stub (500K accounts × 4 txn/mo × 12 months). Financial replaces this stub, does not add a new key |
| V-03 | Datagen ConfigMap has `customer360.py` and `generate.py` sections | inspect `src/lakebench/templates/datagen/configmap.yaml.j2` | yes |
| V-04 | `datagen/generate.py` is a monolithic Customer360 script with no generator class | inspect `datagen/generate.py` for class definitions | confirmed: no `Customer360Generator` class exists. Dispatch refactor requires class extraction first |
| V-05 | Silver build has strategy framework (SIMPLE / STREAMING / SALTED) | `grep -A5 "class SilverStrategy" src/lakebench/spark/scripts/silver_build.py` | yes; reusable for Financial silver_build |
| V-06 | Benchmark queries in `benchmark/queries.py` as `BenchmarkQuery` records | inspect file for the dataclass and Q1..Q9 list | yes; but `BENCHMARK_QUERIES` is a hardcoded module-level list with Customer360-specific column references, imported directly by `runner.py`. Not domain-parametric (see V-12) |
| V-07 | Executor profiles table documented in `docs/architecture.md` | grep for "Spark Executor Profiles" or "silver-build" cores/memory | confirmed at line 176: bronze-verify 2c/4g/2g/50Gi, silver-build 4c/48g/12g/150Gi, gold-finalize 4c/32g/8g/100Gi; streaming jobs lighter; per-executor sizing fixed, executor count scales via `SCALE_TIER` logic |
| V-08 | Sustained mode subcommand as `src/lakebench/cli/_sustained.py` | file exists | yes; sustained mode fully functional since v1.3 LB-044 fix |
| V-09 | Recipe naming pattern `{catalog}-{format}-{processor}-{engine}` | `ls examples/*.yaml` and check filenames | yes; 11 example recipes shipped |
| V-10 | Default Spark version is 4.0.2 with 4.1.x and 3.5.x also supported | inspect `docs/supported-components.md` and `src/lakebench/_constants.py` | yes; 4.2 bump is genuinely new |
| V-11 | Docs (`docs/data-generation.md`) promise IoT and Financial schemas as future options | inspect file for "Other schemas" | yes; Financial is the fulfilment of the Financial promise |
| V-12 | Report generator (`src/lakebench/reports/generator.py`) has no domain-awareness | inspect file for hardcoded Customer360 rendering | confirmed: 92K of Customer360-specific rendering; domain hooks do not exist |
| V-13 | JobType enum and script map are hardcoded in `modules/pipeline_engines/spark/job.py` around line 1172 | inspect file | confirmed; also a Delta variant map, and a hardcoded `spark_scripts_to_ship` list around line 1883 |
| V-14 | Pre-benchmark maintenance step defaults to `retention_threshold="0s"` (expire all snapshots) | grep for `_run_iceberg_maintenance` in `src/lakebench/cli/_run.py` | confirmed; this directly kills W8 and W10 unless overridden. See REQ-R-05 |
| V-15 | `StageMetrics` in `metrics/collector.py` has no `load_condition` field | inspect the dataclass around line 291 | confirmed; only stage_name, stage_type, engine as context; concurrent-load measurement needs a new field. See REQ-M-05 |
| V-16 | Module protocols are defined for Catalog / QueryEngine / PipelineEngine / TableFormat only | inspect `src/lakebench/modules/base.py` | confirmed; no DomainModule or WorkloadModule protocol exists. See SOL-2B.8 |
| V-17 | `ModuleRegistry` exists but is not yet consumed by `deploy/engine.py` | inspect `src/lakebench/modules/registry.py` | confirmed; the docstring says "Not yet used by: deploy/engine.py, deploy/destroy.py" (v1.4 target) |
| V-18 | Local mode exists (`cli/_local.py`, `deploy/local.py`, dedicated tests) | file exists | confirmed; `LOCAL_SCALE_ADVISORY_MAX = 1.0` in `modules/pipeline_engines/spark/job.py:162`; the "scale ≤ 1" ceiling in REQ-P-02.1 aligns with existing convention |
| V-19 | Autosizer (`config/autosizer.py`) sizes resources per schema type | inspect the module | confirmed schema-agnostic: `full_compute_guidance(scale)` in `config/scale.py:313` takes only scale, no schema parameter. Financial silver_build will inherit customer360 sizing (4c/48g). Graph workloads need larger scratch; autosizer needs a schema branch to size differently. ENG-2C.10 scope grows |
| V-20 | Init wizard (`init_wizard.py`) is schema-aware | inspect the module for schema selection prompts | confirmed NOT schema-aware: `WizardState` dataclass has no schema field. Wizard defaults to customer360 for every recipe. ENG-2C.11 must first add schema to the wizard, then add Financial as an option |
| V-21 | Journal (`journal/`) records per-stage events | inspect `journal/events.py` | confirmed: journal events are generic. `EventType.PIPELINE_STAGE` is a single event parameterized by `stage_name`. New Financial stages need no new event types, only new stage_name string values. ENG-2C.12 is smaller than initially specified |
| V-22 | Multi-cycle batch mode exists (`pipeline.cycles`) | grep for `pipeline.cycles` and "Multi-Cycle Batch" in CHANGELOG | confirmed: `pipeline.cycles` accepts 1-50, cycle 1 is full overwrite, cycles 2-N are incremental appends. Datagen splits timestamp range evenly across cycles. Cycle-progression scoring measures degradation across cycles. **Not in original spec; adds a third execution mode alongside batch and sustained.** See new SOL-2B.9 and ENG-2C.15 |
| V-23 | Iceberg maintenance `retention_threshold` parser accepts s/m/h/d only | inspect `modules/table_formats/iceberg/maintenance.py:97` | confirmed; `_parse_threshold_seconds` maps `m` to **minutes**, not months. A retention of 66 months must be expressed as `2013d` (60 × 30.5 + 6 × 30.5 headroom) or the parser must be extended with a new `mo` suffix. ENG-R-05 updated accordingly |
| V-24 | `full_compute_guidance` and `compute_guidance` produce sizing tiers via `SCALE_TIER_*` thresholds | inspect `config/scale.py:313` and `250` | to verify tier boundaries against Financial dimensions |
| V-25 | Example configs use nested `platform:` / `images:` / `architecture:` blocks; the v1.3 flat top-level style is additive not exclusive | inspect `examples/hive-iceberg-spark-trino.yaml` | confirmed nested style still shipped as default in examples; ENG-2C.6 config sample updated to show both |
| V-26 | Compare command (`cli/_compare.py`) already runs two configs sequentially and produces side-by-side scorecard, supports `--local`, `--scale` override, and multiple output formats | inspect file | confirmed; ML champion/challenger (W15) can build on this rather than reimplementing the pattern. ENG-2C.9 updated |

## 0.4 Assess: answer these

In the implementing issue or PR description, record answers to:

1. What is the current default Spark version? Does the spec's assumption of Spark 4.2 as default require a version bump PR that lands before Financial work begins?
2. Confirm V-04: `datagen/generate.py` is a monolithic Customer360 script. The dispatch refactor (ENG-2C.2) requires three sub-PRs: (a) extract Customer360Generator class, (b) introduce Generator protocol, (c) add dispatch. Confirm order and lead time.
3. What is the current CI matrix? What tests are affected by adding Spark 4.2 to the matrix?
4. What existing tests reference `WorkloadSchema.CUSTOMER360` or `WorkloadSchema.CUSTOM`? Repo grep returns ~34 references. These are the regression surface when adding `FINANCIAL`.
5. Live install test: is GraphFrames 0.9.3+ installable and importable against the target Spark version? Run the actual install; do not assume from documentation.
6. Live install test: is Splink 4.0.x installable and importable against the target Spark version?
7. Current Iceberg version bundled with the Spark image (v1.11.0 confirmed at review). Do the time-travel workloads (W10) or Iceberg v3 features (REQ-U-04) require a bump?
8. Current snapshot expiry defaults: `_run_iceberg_maintenance()` in `cli/_run.py` calls with `retention_threshold="0s"` (expire everything). REQ-R-05 requires schema-aware retention. What is the smallest patch to `_run_iceberg_maintenance()` that honours a workload declaration?
9. `BENCHMARK_QUERIES` refactor path (V-12): (a) split into `BENCHMARK_QUERIES_BY_DOMAIN` dict; (b) create query provider protocol via `ModuleRegistry`. Confirm choice; if (b), how does it depend on the v1.4 registry-driven deployment target?
10. `reports/generator.py` domain-hook strategy: (a) in-place extension with domain conditionals; (b) extract Customer360 rendering into a `Customer360ScorecardBlock` class first, then add `FinancialScorecardBlock`. (b) is the cleaner target but a larger refactor. Confirm which lands in v1 of Financial.
11. `financial` slot repurpose (V-02): confirm Option A (replace `financial_dimensions()` and add `WorkloadSchema.FINANCIAL`) rather than adding a parallel `financial` key. Rationale: the existing stub has no generator; docs already promise Financial as a future schema; users get one Financial schema, not two.
12. Local mode scope for Financial (V-18): confirm the scale ceiling. Proposal: local mode supports scale ≤ 1 for Financial (developer testing only); larger scales require Kubernetes. Recipes that ship a `local-` prefix skip the graph workloads.
13. Autosizer integration (V-19): what fields does `config/autosizer.py` key on today? Financial stages (silver_build, graph workloads) probably need different executor profiles than Customer360; confirm the addition surface.
14. `distribution_mode` (LB-049 lesson): the current knob is `spark.lb.silver.distribution_mode`. Confirm this is inherited by Financial silver_build and the graph workloads without additional plumbing.

## 0.5 Impact classify

Each ENG block carries an impact classification declaring what merge risk it creates:

- **Additive**: adds new files, config, or tables; does not modify existing behaviour. Merge risk low.
- **Extension**: modifies existing shared code with existing behaviour preserved behind config flags. Merge risk medium. Backward-compat CI proof required: existing customer360 recipes produce identical scorecards to a pre-change reference run.
- **Refactor**: modifies existing shared code with cross-cutting effects. Merge risk high. PR series with intermediate green CI required at each step.
- **Breaking**: changes default behaviour. Merge risk highest. Requires major version bump and migration notes.

Impact classification for the ENG blocks in this spec:

| Block | Impact | Reason |
|---|---|---|
| ENG-U-01 (Spark 4.2 default) | Extension | Adds to supported matrix and changes default; existing users can pin |
| ENG-U-02 (GraphFrames image) | Additive | New optional image layer |
| ENG-U-03 (Splink image) | Additive | New optional image layer |
| ENG-U-04 (Iceberg v3 support) | Additive | New optional format-version config |
| ENG-U-05 (Financial dimensions) | Extension | Replaces existing `financial_dimensions()` stub; adds `WorkloadSchema.FINANCIAL` enum value; existing users of the stub are none (no generator existed) |
| ENG-U-06 (distribution mode config) | Additive | New Spark conf knob for graph-workload silver_build |
| ENG-2C.1 (data model) | Additive | New tables; no touch to existing |
| ENG-2C.2 (datagen) | Refactor | Three PR sub-series: extract `Customer360Generator` class from monolithic `generate.py`, introduce `Generator` protocol, then add dispatch. Customer360 output must be byte-identical before and after |
| ENG-2C.3 (pipeline scripts) | Additive | New `_financial` scripts and `JobType` enum values at `modules/pipeline_engines/spark/job.py:1172`; existing scripts and `JobType` values unchanged |
| ENG-2C.4.1-5 (detection workloads) | Additive | New Spark scripts |
| ENG-2C.4.6-7 (investigator queries) | Refactor | `BENCHMARK_QUERIES` in `benchmark/queries.py` is a hardcoded module-level list, imported directly by `runner.py`. Making it domain-parametric requires a query-provider mechanism. Customer360 query results must be byte-identical before and after |
| ENG-2C.4.8-11 (replay, writeback, time-travel, ingest) | Additive | New scripts; new CLI subcommands |
| ENG-2C.5 (scoring and metrics) | Refactor | `reports/generator.py` is 92K of Customer360-hardcoded rendering with no domain hooks. Introducing a `ScorecardBlock` interface and extracting `Customer360ScorecardBlock` is the clean path; risk contained by requiring byte-identical Customer360 reports before and after |
| ENG-2C.6 (recipes) | Extension | `_SUPPORTED_COMBINATIONS` at `config/schema.py:930` extended; existing recipes unchanged |
| ENG-2C.7 (component matrix) | Extension | Matrix extended |
| ENG-2C.8 (batch vs sustained) | Extension | Sustained mode gains `workloads:` config |
| ENG-2C.9 (ML extension) | Additive | Optional workloads under time-permitting extension |
| ENG-2C.10 (autosizer integration) | Extension | New schema branch in `config/autosizer.py`; existing customer360 sizing unchanged |
| ENG-2C.11 (init wizard) | Extension | New schema choice; existing wizard flow preserved |
| ENG-2C.12 (journal events) | Additive | New event types for Financial stages |
| ENG-2C.13 (prerequisite checks) | Additive | New checks for GraphFrames image, Splink availability |
| ENG-2C.14 (DomainModule protocol) | Refactor | New protocol in `modules/base.py` alongside the existing four; `ModuleRegistry` extended. Aligns with the v1.4 registry-driven deployment target. Optional in v1 of Financial (implementable as plain code) but strongly recommended before KYC arrives |
| ENG-R-05 (maintenance retention override) | Extension | `_run_iceberg_maintenance()` in `cli/_run.py` becomes schema-aware; default (customer360) unchanged; retention_workload declarations skip or reduce expiry |
| ENG-M-05 (StageMetrics load_condition) | Extension | New optional field on `StageMetrics`; existing serialisation and reporting unchanged when field is null |

**Not this Part.** Part 0 defines conventions and pre-implementation phases. It does not itself modify any lakebench-k8s code; the checks and assessments produce documentation output only. If Part 0's verifications fail, the corrective action is to open an issue and update the spec, not to fix the codebase.

---

# Part 1. Baseline upgrades (all recipes)

## 1.1 Requirements

**REQ-U-01** (Spark version). The benchmark shall run on the latest Apache Spark release with LTS eligibility within six months of GA, to remain useful to practitioners on current-generation stacks.
- Acceptance: default `spark_image` in the shipped configs points to a Spark 4.x GA release; matrix supports at least three Spark 4.x versions to cover in-flight upgrades.
- Rationale: LakeBench is a benchmark for the platform practitioners are actually deploying; running on n-2 Spark makes the numbers moot.

**REQ-U-02** (Graph analytics availability). The benchmark shall support graph workloads as first-class operations without requiring a proprietary graph engine.
- Acceptance: GraphFrames 0.9+ installable into the Spark image via a documented recipe; graph workloads can run without fallback.
- Rationale: The new Financial domain needs graph algorithms; forcing DataFrame reimplementations of Pregel and connected components inflates scope and lowers correctness.

**REQ-U-03** (Entity resolution availability). The benchmark shall support fuzzy entity resolution workloads without requiring a proprietary matching engine.
- Acceptance: Splink 4+ installable into the Spark image via a documented recipe.
- Rationale: Entity resolution is a first-order workload in AML per both Databricks and Cloudera reference architectures.

**REQ-U-04** (Iceberg format version). The benchmark shall support Iceberg v3 as an option, keeping v2 as the default until v3 is universal across engines.
- Acceptance: config knob `iceberg_format_version` accepts {2, 3}; recipes that require v3 (time-travel workloads at scale, row-level updates) document their format-version dependency.
- Rationale: Iceberg v3 features (VARIANT, row lineage, encryption) enable workloads that v2 cannot; not all engines have caught up.

**REQ-U-05** (Scale factor semantics). Scale factor semantics shall be domain-specific and explicitly documented.
- Acceptance: `docs/data-generation.md` states that scale=N produces different physical row counts across domains; per-domain dimensions functions in `config/scale.py` published as the authoritative mapping.
- Rationale: Financial rows are structurally denser than Customer360; a shared scale factor is more useful than a shared row count.

**REQ-U-06** (Write distribution mode as first-class config). Silver-build stages that produce partitioned Iceberg tables shall support a first-class `distribution_mode` config knob per stage or per script, with defaults that avoid file-count explosion at scale.
- Acceptance: `spark.lb.silver.distribution_mode` (existing knob from LB-049) generalises to accept per-schema and per-stage overrides; graph-workload stages that produce many small partitions inherit the same knob; default is `hash` for any stage producing more than 100 partitions.
- Rationale: LB-049 (v1.3.1 fix) documented a scale=100 failure where `distribution-mode=none` produced 836K files and crashed the Hive Metastore commit. Financial silver_build at bank scale, and the graph workloads (W1-W4) which produce many small motif-match outputs, are the same failure mode waiting to happen. Making distribution_mode first-class is a cheap preventive measure.

## 1.2 Solution architecture

**SOL-U-01** (satisfies REQ-U-01). Default Spark version moves to 4.2.0. Supported matrix retains 4.0.2 and 4.1.2 for in-flight users. 3.5.x drops out of the default set, remains available via explicit override.

**SOL-U-02** (satisfies REQ-U-02). GraphFrames 0.9+ shipped as an optional layer over the Spark base image. Recipes that need it declare `graphframes: true` in the config; the image builder pulls the correct GraphFrames artifact for the Spark version.

**SOL-U-03** (satisfies REQ-U-03). Splink 4+ shipped as an optional Python dependency layer. Recipes that need it declare `splink: true`.

**SOL-U-04** (satisfies REQ-U-04). Iceberg v3 supported alongside v2. Recipe metadata declares the format version required for its workloads. Default remains v2 in v1 of this release; v3 becomes default in a follow-on release once engine support is uniform.

**SOL-U-05** (satisfies REQ-U-05). `config/scale.py` grows `financial_dimensions()` **replacing the existing `financial_dimensions()` stub** (500K accounts × 4 txn/mo × 12 months, which was never implemented with a real generator). The `financial` slot in `SCHEMA_DIMENSION_MAP` becomes the Financial dimensions. `WorkloadSchema` enum adds `FINANCIAL = "financial"` (not a separate `FINFRAUD` value). `docs/data-generation.md` gains per-domain dimensions tables and the fulfilment of the Financial-schema promise already present in the docs.

**SOL-U-06** (satisfies REQ-U-06). Existing `spark.lb.silver.distribution_mode` conf generalised into a per-stage lookup table with sensible defaults per schema. Graph workload scripts (W1-W4) declare their expected partition fanout and select `hash` or `range` distribution accordingly. `docs/component-spark.md` gains a section on distribution mode selection at scale.

## 1.3 Engineering specifications

**ENG-U-01** (implements SOL-U-01).
- File: `src/lakebench/_constants.py` `DEFAULT_SPARK_IMAGE` = `apache/spark:4.2.0-python3`.
- File: `src/lakebench/config/schema.py` `SUPPORTED_SPARK_VERSIONS` = {`3.5.4`, `4.0.2`, `4.1.2`, `4.2.0`}.
- CI: matrix in `.github/workflows/` expanded to run functional tests against each supported Spark version.
- Docs: `docs/supported-components.md` updated.

**ENG-U-02** (implements SOL-U-02).
- File: new `datagen/Dockerfile.graphframes` builds an image layered over the Spark base with GraphFrames 0.9.3 and its Scala compatibility jar for the target Spark version.
- File: `src/lakebench/config/schema.py` adds `SparkExtensions.graphframes: bool = False`.
- File: `src/lakebench/deploy/engine.py` selects the graphframes image when the config declares it; otherwise uses the base image.

**ENG-U-03** (implements SOL-U-03). Analogous to ENG-U-02 with Splink 4.0.x. Optional dependency layer.

**ENG-U-04** (implements SOL-U-04).
- File: `src/lakebench/config/schema.py` adds `IcebergConfig.format_version: Literal[2, 3] = 2`.
- File: `src/lakebench/modules/table_formats/iceberg.py` gains `format-version` in the Iceberg table properties emitted during table creation.
- Docs: `docs/component-iceberg.md` (new) documents v2 vs v3 trade-offs.

**ENG-U-05** (implements SOL-U-05). Impact: Extension (replaces stub).
- File: `src/lakebench/config/scale.py` **replaces** the existing `financial_dimensions(scale)` stub function body with the Financial dimensions (accounts × 5,000 per scale unit, 45 txn/mo, 60 months). The function name stays `financial_dimensions` for backward compatibility with any existing string references.
- File: `src/lakebench/config/scale.py` `SCHEMA_DIMENSION_MAP` retains `"financial": financial_dimensions` (unchanged key, replaced function body).
- File: `src/lakebench/config/schema.py` `WorkloadSchema` enum adds `FINANCIAL = "financial"`.
- File: `src/lakebench/config/schema.py` `WorkloadConfig` grows `financial: FinancialConfig = Field(default_factory=FinancialConfig)` following the `customer360: Customer360Config` pattern; `FinancialConfig` new class with Financial-specific tunables (typology density, correspondent chain ratio, cross-border ratio, currency mix, retention months).
- File: `datagen/generate.py` (post-dispatch refactor from ENG-2C.2) registers `FinancialGenerator` under key `"financial"`.
- Verification: `python -c "from lakebench.config.scale import get_dimensions; print(get_dimensions('financial', 1))"` returns Financial dimensions.
- Docs: `docs/data-generation.md` updates the "Other schemas (IoT, Financial)" line to say Financial is now implemented, and adds the Financial dimensions table.

**ENG-U-06** (implements SOL-U-06). Impact: Additive.
- File: `src/lakebench/spark/scripts/common.py` gains a `select_distribution_mode(stage_name, schema_type, partition_count_estimate)` helper.
- File: `src/lakebench/spark/scripts/silver_build.py` and `silver_build_delta.py` unchanged in Customer360 path (already fixed in LB-049 with `hash`); the helper is called from `silver_build_financial.py` (see ENG-2C.3).
- File: `src/lakebench/spark/scripts/silver_build_financial.py` selects `hash` distribution by default; overridable via `spark.lb.silver_financial.distribution_mode`.
- File: Financial graph-workload scripts (W1-W4 in ENG-2C.4.*) select distribution mode using the helper based on their expected fanout.
- Verification: at scale=100, silver-build-financial produces fewer than 10K files in the transactions partition; at scale=1000, fewer than 50K.
- Docs: `docs/component-spark.md` gains a "Distribution mode selection at scale" section citing LB-049.

---

# Part 2. FinServ-Crime domain (AML v1)

**Naming convention.** This spec uses two names for one thing. `financial` (lowercase) is the code identifier: schema key in `SCHEMA_DIMENSION_MAP`, `WorkloadSchema.FINANCIAL` enum value, per-domain config field, class prefix (`FinancialGenerator`, `FinancialConfig`, `FinancialModule`, `FinancialScorecardBlock`), file suffix (`silver_build_financial.py`). "FinServ-Crime" is the user-facing positioning name for the domain in documentation, marketing, and WCA references. Both stay stable; the mix is intentional, matching the existing lakebench convention where the code identifier `customer360` maps to the user-facing name "Customer 360".

## 2A. Requirements

### 2A.1 Fidelity requirements

**REQ-F-01** (schema shape). The bronze data model shall reflect the ISO 20022 message schema (pacs.008.001.14 for bank-to-bank credit transfers, camt.053.001.13 for statements, pain.001.001.13 for customer initiations) rather than a research-abstraction schema.
- Acceptance: bronze pacs008 field list derivable from the ISO 20022 XSD by a mechanical mapping; every field name in the bronze table matches the ISO 20022 XPath element name (or a documented flattening).
- Rationale: Practitioner recognition and defensibility; the benchmark exists to measure workloads against data shaped like what banks actually process on the wire post-November-2025 SWIFT migration.

**REQ-F-02** (correspondent chain). The transaction schema shall include the full correspondent banking chain (up to 3 intermediary agents per transaction, plus previous instructing agents).
- Acceptance: bronze pacs008 exposes `IntrmyAgt1`, `IntrmyAgt2`, `IntrmyAgt3` and `PrvsInstgAgt1..3` as first-class fields; datagen produces multi-hop chains for a configurable fraction of wire transactions (default 15%).
- Rationale: Layering typologies (AMLworld "stack", "cycle", "scatter-gather" patterns) live in correspondent chains; without chain modelling these detections are trivially impossible.

**REQ-F-03** (regulatory reporting fields). The transaction schema shall include structured regulatory reporting fields.
- Acceptance: bronze pacs008 supports `RgltryRptg` as an array of up to 10 entries per ISO 20022 structure (debit/credit indicator, authority, jurisdiction details).
- Rationale: Sanction-relevant flows carry structured signals here; workloads that would otherwise scan free text can prune by field.

**REQ-F-04** (UETR). Every transaction shall carry a UETR (Unique End-to-end Transaction Reference).
- Acceptance: every generated pacs008 row has a UETR populated as UUIDv4; UETR is preserved through silver and gold; time-travel reproduction (REQ-R-02) uses UETR as the alert key.
- Rationale: UETR is the identifier banks and regulators use to trace payments across chains and time; reproducibility depends on it.

**REQ-F-05** (business realism). Transaction timing shall follow realistic banking patterns rather than uniform distribution.
- Acceptance: hourly transaction volume follows a documented weekly and monthly profile: 3x business-hours skew in local timezone, 5x on salary days (1st, 15th, 25th), reduced weekend volume for wires, national-holiday troughs.
- Rationale: Workloads that measure concurrent behaviour depend on realistic burst patterns; uniform distribution hides the operational stresses the benchmark is designed to expose.

### 2A.2 Ground truth requirements

**REQ-G-01** (manifest). Datagen shall produce a ground-truth manifest of every injected typology instance.
- Acceptance: `manifest.parquet` exists at `bronze/manifest/`, with one row per typology instance and fields `(typology_id, typology_type, participating_entity_ids array, transaction_ids array, injection_start_ts, injection_end_ts, injection_parameters map)`.
- Rationale: Detection recall is not measurable without ground truth.

**REQ-G-02** (manifest isolation). The manifest shall not be accessible to detection workloads during execution.
- Acceptance: `bronze/manifest/` sits outside the paths the detection workloads read from; workload code has no reference to the manifest path; scoring runs after workload completion as a separate step.
- Rationale: A workload that reads its own ground truth is not measuring detection; it is measuring retrieval.

**REQ-G-03** (injection density). Typology injection density shall match published SAR base rates.
- Acceptance: total injected typology transactions divided by total transactions equals 0.1% ± 0.02% across a scale=100 run.
- Rationale: Detection recall is meaningful only when the signal-to-noise ratio is realistic; injecting 10% laundering trivialises the benchmark.

**REQ-G-04** (typology coverage). The datagen shall inject at least the eight AMLworld graph patterns and be extensible to additional typologies.
- Acceptance: manifest includes instances of fan-in, fan-out, bipartite, stack, random, cycle, scatter-gather, gather-scatter; new typologies are addable via a plugin interface without modifying core datagen code.
- Rationale: These are the reference patterns in both AMLworld and the Databricks accelerator; extensibility future-proofs for regional variants (SEPA-specific, US SAR-specific).

### 2A.3 Detection requirements

**REQ-D-01** (workload coverage). Detection workloads shall implement the reference AML graph patterns documented by Databricks and Cloudera.
- Acceptance: W1 (synthetic identity via connected components), W2 (structuring via 4-to-1 motif), W3 (round-tripping via 4-cycle motif), W4 (risk propagation via Pregel) are present and functional.
- Rationale: Practitioner recognition and comparability with published vendor accelerators.

**REQ-D-02** (engine flexibility). Detection workloads shall be implementable via GraphFrames when available and via DataFrame joins when not.
- Acceptance: each detection workload has two implementations. Recipes without GraphFrames use the DataFrame path.
- Rationale: GraphFrames is optional; a benchmark that requires it locks out any Iceberg-based lakehouse without a Spark image that includes it.

**REQ-D-03** (determinism). Detection workloads shall produce bit-identical output given fixed input.
- Acceptance: running the same workload twice against the same silver snapshot with the same rule version produces bit-identical `alerts.parquet` output (sorted by `(rule_id, alert_ts, entity_id)` for comparability).
- Rationale: Reproducibility across benchmark runs; a workload with nondeterministic output is not comparable across runs.

**REQ-D-04** (recall metric). Detection workloads shall produce a per-typology recall metric.
- Acceptance: after a detection workload runs, a scoring step reads the manifest and the workload output, produces `recall.parquet` with `(rule_id, typology_type, injected_count, detected_count, recall, precision)` per typology.
- Rationale: Numbers that survive practitioner scrutiny.

**REQ-D-05** (scale execution). Detection workloads shall execute to completion at each published scale point.
- Acceptance: W1-W4 execute at scale factors {1, 10, 100, 1000, 10000} within documented timeout limits per workload per scale point.
- Rationale: The benchmark exists to characterise how these workloads behave under scale, not just at trivial sizes.

### 2A.4 Investigation requirements

**REQ-I-01** (operational patterns). Investigator workloads shall reflect the operational access patterns used in AML case management.
- Acceptance: W6 (entity timeline: 24 months of transactions, counterparties, alerts, related accounts for a target entity), W7 (two-hop counterparty traversal filtered by value threshold and watchlist match).
- Rationale: These are what investigators actually run in real case management systems; the benchmark measures what matters operationally.

**REQ-I-02** (engine portability). Investigator workloads shall run under both Spark SQL and Trino when both engines are deployed.
- Acceptance: each investigator workload is expressed as ANSI-compliant SQL producing identical results under Spark SQL and Trino; both paths measured when both engines are deployed.
- Rationale: Engine portability is a core lakebench value proposition.

**REQ-I-03** (percentile latency). Investigator workloads shall be measured at P50 and P95 latency.
- Acceptance: each investigator workload runs at least 100 executions in a measurement window; P50 and P95 latencies published per workload per engine per load condition.
- Rationale: Single-shot latency is noise; percentiles are the operational metric.

### 2A.5 Concurrency requirements

**REQ-C-01** (load conditions). The benchmark shall measure investigator query latency under three load conditions.
- Acceptance: the Financial orchestrator executes three passes: idle (investigator queries alone), pairwise (investigator queries with one batch detection workload active), full (all workloads concurrent). P50 and P95 captured per pass per query type.
- Rationale: Concurrent workload degradation is the primary storage-differentiation measurement; without three modes there is no ratio.

**REQ-C-02** (degradation ratio). The scorecard shall publish a documented concurrent-degradation ratio.
- Acceptance: scorecard rows include `concurrent_degradation_ratio_c1` and `concurrent_degradation_ratio_c2` = P95_full / P95_idle, per scale point.
- Rationale: The ratio is what a customer compares; raw numbers are uninterpretable without the reference.

**REQ-C-03** (workload isolation). Concurrent workloads shall not corrupt each other's output.
- Acceptance: running W1-W11 concurrently produces per-workload outputs that pass the same acceptance tests as running each workload alone.
- Rationale: A benchmark that produces wrong results under concurrency measures nothing useful.

### 2A.6 Scale requirements

**REQ-S-01** (tier-1 target). The benchmark shall be executable at data volumes representative of a tier-1 universal bank's AML retention estate under EU AMLR (60 months of wire and institutional volumes).
- Acceptance: `scale=10000` recipe produces approximately 100 TB curated Parquet; scale points {1, 10, 100, 1000, 10000} all execute end-to-end at reasonable lab infrastructure; scaling curves published per workload.
- Rationale: 100 TB is the credibility floor for the tier-1 narrative from the parent solution doc.

**REQ-S-02** (linear predictability). The scaling relationship shall be linear and predictable in the domain dimensions.
- Acceptance: doubling scale doubles account count, doubles transaction count, doubles storage footprint (within ±10% compression variance).
- Rationale: Users need to reason about resource requirements when picking a scale point.

**REQ-S-03** (calibration). The scale-to-storage mapping shall be verified by measurement, not assumption.
- Acceptance: `scale=1` run publishes actual bytes per row, actual total corpus size, actual ratio to predicted; dimensions function recalibrated if the actual ratio falls outside 0.8-1.2 of predicted.
- Rationale: Row-size estimates from XSD field counts are approximations; publishing calibrated numbers matters.

### 2A.7 Reproducibility requirements

**REQ-R-01** (historical replay). Historical replay shall re-execute a detection workload against a specified past corpus depth.
- Acceptance: W8 accepts a `--depth-months` flag with values {3, 12, 36, 60}; runs the detection logic against silver as it existed N months ago; publishes wall-clock time per depth.
- Rationale: This is the specific capability regulators require in post-enforcement lookbacks (TD Bank pattern).

**REQ-R-02** (time-travel reproduction). Time-travel alert reproduction shall deterministically reproduce a specific historical alert.
- Acceptance: W10 accepts an alert UETR from a past run; uses Iceberg `FOR TIMESTAMP AS OF` at the alert's snapshot to reproduce the exact silver state; reproduced alert fields match the historical alert bit-for-bit.
- Rationale: The reproducibility mechanism supervisory workflows require.

**REQ-R-03** (snapshot retention). Iceberg snapshot retention shall span the full corpus depth for time-travel workloads.
- Acceptance: when `retention_workload: true`, snapshot expiry is set to at least 66 months (60 + headroom); maintenance step between multi-cycle batches preserves snapshots.
- Rationale: Time-travel is not possible against expired snapshots.

**REQ-R-04** (datagen determinism). Datagen shall produce bit-identical output for a given seed and scale.
- Acceptance: two independent runs of datagen with the same seed and scale produce bit-identical Parquet files (sorted comparably).
- Rationale: Cross-run comparability across labs, and across time in the same lab, requires bit-identical inputs.

**REQ-R-05** (maintenance preserves retention workload snapshots). The pre-benchmark maintenance step shall preserve snapshots required by declared retention workloads.
- Acceptance: when a recipe declares `retention_workload: true` and the schema is `financial`, `_run_iceberg_maintenance()` in `cli/_run.py` overrides the default `retention_threshold="0s"` to at least `retention_months + 6` months of retention; W8 (historical replay) and W10 (time-travel reproduction) execute successfully after the maintenance phase.
- Rationale: The current default expires every snapshot older than now. This kills time-travel workloads silently. Without an explicit schema-aware override, Financial recipes will appear to work but W8 and W10 will return "snapshot not found" or wrong results.

### 2A.8 Portability requirements

**REQ-P-01** (storage portability). The benchmark shall be portable across any S3-compatible object storage.
- Acceptance: same recipe executes end-to-end against FlashBlade, MinIO, AWS S3, GCP Cloud Storage, Azure Blob (S3 API), producing scorecard results comparable within storage-driven variance.
- Rationale: Non-negotiable for a public benchmark; vendor-specific benchmark is not credible.

**REQ-P-02** (engine portability). The benchmark shall be portable across Iceberg-supporting query engines to the extent each engine's feature set allows.
- Acceptance: Financial recipes ship for Trino, Spark Thrift, Spark direct, and DuckDB (with documented workload subsets where engine features preclude some workloads, e.g. GraphFrames workloads are Spark-only).
- Rationale: Engine portability is a core lakebench value; extending it to Financial preserves the promise.

**REQ-P-02.1** (local mode scope). Financial recipes shall have an explicit local-mode support declaration.
- Acceptance: local-mode recipes support Financial at scale ≤ 1 for developer iteration only; scale > 1 requires Kubernetes; graph workloads (W1-W4) are skipped in local mode with a documented reason.
- Rationale: Local mode exists as first-class functionality in lakebench (`cli/_local.py`, dedicated test suites). Silently failing to run in local mode at scale=1 damages the developer experience. Silently attempting to run at scale=1000 in local mode wastes hours.

**REQ-P-03** (open source stack). The benchmark shall not require Databricks, Cloudera, or any commercial platform.
- Acceptance: all runtime components ship as Apache-2.0 or compatible OSS; no dependency on any commercial platform SDK.
- Rationale: A public benchmark of vendor-neutral workloads cannot itself depend on a vendor.

### 2A.9 Measurability requirements

**REQ-M-01** (backward compatibility). The existing lakebench scorecard (Time to Value, Throughput, Efficiency, Scale, QpH) shall render unchanged for existing recipes.
- Acceptance: customer360 recipes produce the same scorecard as before this release; scorecard rendering is domain-aware.
- Rationale: Backward compatibility for existing users.

**REQ-M-02** (domain-specific scorecard). Financial recipes shall produce a domain-specific scorecard in addition to the base scorecard.
- Acceptance: Financial scorecard includes `detection_recall` (per typology), `investigator_p50_idle`, `investigator_p95_idle`, `investigator_p50_loaded`, `investigator_p95_loaded`, `concurrent_degradation_ratio`, `replay_wallclock` (per depth), `timetravel_reproduction_success`, `timetravel_reproduction_latency`, `sustained_ingest_tps`.
- Rationale: Domain metrics are what the domain audience reads; storage metrics are what the storage audience reads; both belong.

**REQ-M-03** (provenance). All published metrics shall be traceable to captured measurements.
- Acceptance: every scorecard value has a corresponding row in `metrics.parquet` with `(metric_name, scale_factor, workload_id, engine, load_condition, value, unit, captured_at, run_id)`.
- Rationale: Numbers without provenance are not credible.

**REQ-M-04** (no dedupe ratios). Storage efficiency scorecard shall exclude deduplication ratios.
- Acceptance: storage efficiency row lists `logical_size`, `physical_size`, `compression_ratio`, `replication_overhead`; does not list `dedupe_ratio`.
- Rationale: Dedupe on synthetic data is gameable by generator choices (parent solution doc rationale).

**REQ-M-05** (load-condition-aware metrics). Per-stage measurements shall carry the load condition under which they were captured.
- Acceptance: `StageMetrics` in `metrics/collector.py` grows an optional `load_condition: str | None` field accepting values in `{idle, pairwise, full, None}`. `None` preserves existing behaviour for Customer360 and any recipe that does not declare concurrent workloads. Investigator query rows (W6, W7) in Financial runs populate the field. The scorecard renderer groups query metrics by load condition when the field is populated.
- Rationale: REQ-C-01 declares three load conditions but the existing `StageMetrics` dataclass has no place to record which condition produced a measurement. Without this field, the concurrent-degradation ratio (REQ-C-02) is not derivable from the raw metrics table, and reruns cannot be correlated to their run mode.

### 2A.10 ML/training extension requirements (time-permitting)

This subsection scopes ML/training workloads as a time-permitting extension of the FinServ-Crime v1 release. Shipping this incomplete is preferable to delaying the core Financial workload set (W1-W11). If deferred, it moves to the next release. Requirements below are lower priority than 2A.1 through 2A.9.

**REQ-ML-01** (feature coverage). ML feature engineering shall cover the feature families used in Databricks and Cloudera AML implementations.
- Acceptance: W12 produces features across velocity (transactions per account per rolling window, amount velocity), diversity (unique counterparties per window, geographic dispersion, currency diversity), temporal (hour-of-day patterns, day-of-week patterns, inter-transaction gap statistics), graph (in-degree, out-degree, PageRank, community membership from W1 output), and entity (customer age, KYC risk score, sanctions match count, PEP status).
- Rationale: matches how banks actually featurise for AML ML; anything narrower is a research toy.

**REQ-ML-02** (portability). ML training and inference shall run within Spark without requiring a dedicated ML platform.
- Acceptance: W13 uses Spark MLlib (`pyspark.ml.classification.GBTClassifier` or equivalent) for training; W14 uses Spark UDF or MLlib for batch inference; MLflow tracking optional; no runtime dependency on Databricks ML, Vertex AI, SageMaker, or similar.
- Rationale: lakebench is a Spark benchmark; ML workloads should measure Spark's ML capability, not routing to external systems.

**REQ-ML-03** (versioning). ML models shall be versioned with their training corpus snapshot.
- Acceptance: W13 output includes `model_id`, `training_snapshot_ts`, `feature_hash`; W14 inference outputs reference the `model_id` used; both are queryable to reproduce a specific model's training conditions.
- Rationale: model reproducibility depends on knowing the corpus the model was trained against.

**REQ-ML-04** (champion/challenger). ML workloads shall support running a challenger model alongside a champion.
- Acceptance: W15 runs two models against the same test set; produces comparison metrics (accuracy, precision, recall, latency) suitable for scorecard display.
- Rationale: this is the operational pattern Databricks documents for ML in AML; benchmarking it makes the recipe useful for teams evaluating ML replacement of rule-based detection.

**REQ-ML-05** (drift detection). Feature drift between training corpus and inference corpus shall be detectable.
- Acceptance: W16 compares current feature distributions against training-time distributions (Population Stability Index or equivalent); flags features exceeding a configurable threshold.
- Rationale: drift detection is a first-order operational concern; without it the ML workload set is incomplete relative to what banks run.

## 2B. Solution architecture

### 2B.1 Positioning

**SOL-2B.1** (satisfies REQ-F-*, REQ-P-*). Position the FinServ-Crime domain as the first open, at-scale lakehouse benchmark grounded in ISO 20022, running the reference AML workload set that both Databricks and Cloudera publicly describe, portable across any Iceberg-based lakehouse. Explicitly not tied to any vendor platform. Deferred to a later release: KYC as a separate domain, Kafka ingest as a separate recipe, cross-institution sharing. The domain implements the `financial` schema slot already reserved in `SCHEMA_DIMENSION_MAP` and already promised in `docs/data-generation.md`; from a user-facing perspective this is the concrete realisation of the "Financial (future)" line in current docs, not the addition of a new schema.

### 2B.2 Data zones

**SOL-2B.2** (satisfies REQ-F-01 through REQ-F-04, REQ-G-*, REQ-R-*). Three medallion zones on Iceberg, plus a separate manifest sidecar for ground truth.

- Bronze zone reflects the ISO 20022 message wire format. `bronze/pacs008/`, `bronze/camt053/`, `bronze/pain001/`, `bronze/party/`, `bronze/reference/`.
- Silver zone is analytical, aligned with the Databricks accelerator's table shape so reference workloads apply without translation. `silver/transactions/`, `silver/entities/`, `silver/accounts/`, `silver/counterparty_edges/`.
- Gold zone holds derived outputs versioned by rule or model. `gold/alerts/`, `gold/risk_scores/`, `gold/synthetic_id_clusters/`, `gold/investigations/`.
- Manifest sidecar at `bronze/manifest/` isolated from the medallion path; not accessible to workloads.

### 2B.3 Workload set

**SOL-2B.3** (satisfies REQ-D-*, REQ-I-*, REQ-R-*). Eleven workloads mapping to the Databricks accelerator patterns, Cloudera architecture requirements, regulatory patterns, and Iceberg-native capabilities.

| # | Name | Source | Engine | Spec section |
|---|---|---|---|---|
| W1 | Synthetic identity (connected components) | Databricks NB1 | Spark + GraphFrames | 2C.4.1 |
| W2 | Structuring (motif 4→1) | Databricks NB1 | Spark + GraphFrames | 2C.4.2 |
| W3 | Round-tripping (4-node cycle motif) | Databricks NB1 | Spark + GraphFrames | 2C.4.3 |
| W4 | Risk propagation (Pregel, 3 iterations) | Databricks NB1 | Spark + GraphFrames | 2C.4.4 |
| W5 | Entity resolution (fuzzy dedupe) | Databricks NB3 (Splink) | Spark + Splink | 2C.4.5 |
| W6 | Investigator entity timeline (24 months) | Both refs + regulatory | Spark or Trino | 2C.4.6 |
| W7 | Investigator two-hop traversal | Both refs | Spark or Trino | 2C.4.7 |
| W8 | Historical rule replay (3, 12, 36, 60 months) | Regulatory (TD Bank lookback) | Spark | 2C.4.8 |
| W9 | Derived writeback (versioned alerts, scores) | Both refs | Spark | 2C.4.9 |
| W10 | Time-travel alert reproduction | Iceberg-native | Spark or Trino | 2C.4.10 |
| W11 | Sustained ingest (file-drop, Auto Loader equivalent) | Databricks reference | Spark Structured Streaming | 2C.4.11 |

### 2B.4 Concurrency measurement

**SOL-2B.4** (satisfies REQ-C-*, REQ-M-05). New CLI subcommand `lakebench financial-run` with `--mode` flag accepting {idle, pairwise, full}. Sustained mode extended to accept a `workloads:` list in config so any recipe can execute concurrent workload patterns. `StageMetrics` gains an optional `load_condition` field populated by the orchestrator per submitted query. The concurrent-degradation ratio is derived by the scoring step from `metrics.parquet` rows grouped by `(workload_id, engine, load_condition)`, not stored as its own metric.

### 2B.5 Scoring and metrics

**SOL-2B.5** (satisfies REQ-M-*, REQ-D-04). Scorecard renderer is domain-aware. Existing scorecard preserved for customer360. Financial recipes produce an additional domain-specific scorecard block. Recall calculation runs as a post-workload scoring step reading the isolated manifest. All metric values written to `metrics.parquet` with full provenance.

### 2B.6 Scale calibration

**SOL-2B.6** (satisfies REQ-S-*). `financial_dimensions(scale)` produces domain dimensions calibrated so scale=100 ≈ 1 TB curated and scale=10000 ≈ 100 TB. Actual row size verified at scale=1 during CI; dimensions function auto-recalibrated in a follow-on release if the calibrated ratio drifts outside the acceptable band.

### 2B.7 ML/training extension solution (time-permitting)

**SOL-2B.7** (satisfies REQ-ML-*). Five ML workloads added to the FinServ-Crime domain, all Spark-native:

- W12 Feature engineering: produces `silver.features` from `silver.transactions`, `silver.entities`, and W1 output. Feature families per REQ-ML-01.
- W13 Model training: Spark MLlib GBTClassifier trained on `silver.features` with the manifest as label source. Writes model artifact to `gold.models` versioned per REQ-ML-03.
- W14 Batch inference: scores all transactions in a window using a specified model. Writes scored records to `gold.ml_scores`.
- W15 Champion/challenger comparison: runs two models against the same test set, produces `gold.model_comparison` with delta metrics.
- W16 Feature drift detection: compares current feature distributions against training distribution via PSI; produces `gold.feature_drift`.

MLflow used for experiment tracking when the runtime detects it; degrades to filesystem-based tracking otherwise. Model artifacts stored as Iceberg tables so time-travel applies to model history. Feature store as `silver.features` versioned by `(feature_hash, training_window)`.

Scorecard additions:
- `ml_training_wallclock` per model type per scale
- `ml_inference_throughput` transactions per second per core
- `ml_champion_challenger_delta` precision/recall/latency deltas
- `ml_feature_drift_detected_pct` fraction of features drifting beyond threshold

### 2B.8 DomainModule protocol (design proposal)

**SOL-2B.8** (satisfies REQ-A-*, aligns with the v1.4 registry-driven deployment target). Introduce a `DomainModule` protocol in `src/lakebench/modules/base.py` alongside the existing four (`CatalogModule`, `QueryEngineModule`, `PipelineEngineModule`, `TableFormatModule`). A domain module owns:

- its `WorkloadSchema` enum value and schema-string identifier
- its `Dimensions` function
- its per-domain config model (analogous to `Customer360Config`)
- its datagen generator class
- its pipeline script names (batch and sustained)
- its benchmark query set (subject to the query registry refactor from ENG-2C.4.6-7)
- its scoring/reporting hook (subject to the reports refactor from ENG-2C.5)
- optional declarations for retention workloads, ML extensions, etc.

Rationale: the current codebase implements domain-specific behaviour through scattered hardcoded references (`WorkloadSchema` enum, `SCHEMA_DIMENSION_MAP`, `WorkloadConfig` per-domain fields, hardcoded `Customer360Generator` logic in `generate.py`, hardcoded `BENCHMARK_QUERIES`, hardcoded reports rendering). Each new domain adds another layer of scattered references. The `ModuleRegistry` already exists for the four other module types; extending it to domains is the natural next step and makes the WCA Type 2 contribution mechanically clean.

Scope in v1 of Financial: **optional**. Financial can be implemented via the existing scattered pattern in v1 and then refactored under a DomainModule protocol before KYC arrives, or the protocol can be introduced during the Financial work. Making the introduction concurrent with Financial costs one additional refactor PR but produces a much smaller KYC delivery later. Recommended: introduce the protocol, apply it to Customer360 (extract `Customer360Module`) and Financial (`FinancialModule`) as reference implementations, ship both in the same release. See ENG-2C.14 for the engineering surface.

### 2B.9 Design decisions

Decisions the earlier draft deferred, now resolved. Each decision names the option chosen and the option rejected, with rationale.

**D-01 (7-phase run flow integration).** FinServ-Crime workloads plug into the existing 7-phase flow as follows. Phase 1 (Prerequisites) gains three new checks (see ENG-2C.13). Phase 2 (Infrastructure) unchanged. Phase 3 (Generate) invokes `FinancialGenerator` via the datagen dispatch (ENG-2C.2). Phase 4 (Pipeline) invokes `bronze_verify_financial → silver_build_financial → gold_finalize_financial` when `schema: financial`. Phase 5 (Maintenance) applies the schema-aware retention override (ENG-R-05). Phase 6 (Benchmark) invokes both the domain query set (via BENCHMARK_QUERIES_BY_DOMAIN dispatch, ENG-2C.4.pre) AND the domain-specific detection workloads (W1-W5) as an inserted sub-phase 6a; investigator queries run as sub-phase 6b under the three load conditions. Phase 7 (Results) invokes the domain-aware scorecard renderer. Rejected: replacing the existing pipeline stages entirely (breaks Customer360 in a shared runner), inserting a new phase 4.5 (breaks phase numbering compatibility for downstream tooling that parses phase headers).

**D-02 (detection workload invocation).** W1-W5 are submitted as SparkApplication jobs during Phase 6a via the existing Spark job dispatch (JobType extended in ENG-2C.3). They are not benchmark queries in the QpH sense; they produce `gold.alerts` and `gold.risk_scores` as side effects, and their wall-clock is captured as a separate scorecard row (`detection_wallclock_per_workload`). Rejected: implementing W1-W5 as Trino queries (GraphFrames and Splink both require Spark), as a new `lakebench detect` command (fragments the run flow), or as part of a new benchmark class (conflates detection with QpH-shaped queries).

**D-03 (multi-cycle × detection).** Detection workloads (W1-W5) run at the end of every cycle when `cycles > 1`. Rationale: multi-cycle simulates operational reality where detection reruns after each batch load; running detection once at end misses the cycle-progression measurement that is the primary value of multi-cycle for FinServ-Crime; the compute cost is bounded because each cycle's silver grows linearly and detection scales sub-linearly. Rejected: running detection once at the final cycle (loses progression data), user-configurable per-cycle detection toggle (adds a decision the user shouldn't have to make in v1).

**D-04 (detection recall calibration).** Recall is measured **per typology instance**, not per transaction. A typology instance (e.g. a structuring event with ~20 constituent transactions) counts as detected when the workload's alerts table contains at least one alert citing any of the instance's `transaction_ids` or any of the instance's `participating_entity_ids`. Precision is measured on the alerts side: an alert is a true positive if it cites any injected transaction or entity. Rationale: banks operationalise typology detection at the case level, not the transaction level; a workload that catches 1 of the 20 structuring transactions surfaces the case for investigation; a per-transaction recall measurement penalises the workload for realistic aggregation behaviour. Rejected: per-transaction recall (unrealistic and punitive), per-entity recall (loses granularity across single-entity typologies).

**D-05 (Delta variants for FinServ-Crime).** Ship four Delta variants: `hive-delta-spark-trino-financial`, `hive-delta-spark-thrift-financial`, `hive-delta-spark-none-financial` (and the equivalent without the `-financial` suffix in the file name; the domain suffix is a config field not a filename change). These recipes ship with W1-W5 (graph detection workloads) skipped and marked "not supported on Delta + GraphFrames in v1" in the scorecard. W6, W7, W8, W9, W10, W11 all run on Delta and produce complete scorecard entries. Rationale: 60% of the FinServ-Crime workload set works on Delta; refusing to ship Delta variants because 40% don't is disproportionate; users evaluating Delta for AML get useful data from the six workloads that do work. Rejected: shipping no Delta variants (loses value), shipping Delta variants with graph workloads reimplemented in DataFrame joins (adds significant code without evidence of demand; DataFrame reimplementation of Pregel is genuinely hard).

**D-06 (Naming across FinServ-Crime artifacts).** As stated at the top of Part 2: `financial` in code, "FinServ-Crime" in positioning/docs. Historical "FinFraud" and "AML v1" references replaced with "FinServ-Crime" or "Financial" as appropriate; "AML" retained where the acronym clarifies specific compliance context. Rejected: unifying to a single name (loses the code-vs-marketing distinction), unifying to "FinFraud" (parent solution doc calls it "Fraud and AML", and "FinFraud" misses the AML/sanctions half).

**D-07 (Where per-domain config lives in the config schema).** A new `FinancialConfig` class in `src/lakebench/config/schema.py` following the `Customer360Config` pattern exactly. Nested at `architecture.workload.financial`. Fields: `retention_months: int = 60`, `typology_density: float = 0.001`, `correspondent_chain_ratio: float = 0.15`, `cross_border_ratio: float = 0.20`, `currency_mix: dict[str, float] = ..., `holiday_calendar: str = "iso"`, `sanctions_list: str = "generated"`, `ml: FinancialMLConfig | None = None`. All fields optional with documented defaults. Rejected: adding fields directly to `WorkloadConfig` (breaks the per-domain grouping convention), splitting across multiple config classes (`FinancialDatagenConfig`, `FinancialPipelineConfig`, etc.; premature and inconsistent with Customer360).

## 2C. Engineering specifications

### 2C.1 Data model

**ENG-2C.1** (implements SOL-2B.2, satisfies REQ-F-*, REQ-G-01).

Bronze table DDL (representative, expressed in Iceberg SQL):

```sql
CREATE TABLE bronze.pacs008 (
  msg_id STRING NOT NULL,
  cre_dt_tm TIMESTAMP NOT NULL,
  nb_of_txs INT NOT NULL,
  ctrl_sum DECIMAL(18,5),
  ttl_intr_bk_sttlm_amt DECIMAL(18,5),
  intr_bk_sttlm_dt DATE,
  sttlm_inf STRUCT<sttlm_mtd: STRING, ...>,
  pmt_tp_inf STRUCT<instr_prty: STRING, clr_chanl: STRING, svc_lvl: STRING, lcl_instrm: STRING, ctgy_purp: STRING>,
  instg_agt STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  instd_agt STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  -- per-transaction fields flattened for analytical use
  txn_id STRING NOT NULL,
  instr_id STRING,
  end_to_end_id STRING NOT NULL,
  uetr STRING NOT NULL,
  clr_sys_ref STRING,
  intr_bk_sttlm_amt DECIMAL(18,5) NOT NULL,
  intr_bk_sttlm_ccy STRING NOT NULL,
  instd_amt DECIMAL(18,5),
  instd_ccy STRING,
  xchg_rate DECIMAL(11,10),
  chrg_br STRING,
  -- correspondent chain
  intrmy_agt_1 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  intrmy_agt_2 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  intrmy_agt_3 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  prvs_instg_agt_1 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  prvs_instg_agt_2 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  prvs_instg_agt_3 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  -- party chain
  ultmt_dbtr STRUCT<nm: STRING, lei: STRING, ctry: STRING>,
  initg_pty STRUCT<nm: STRING, lei: STRING>,
  dbtr STRUCT<nm: STRING, pstl_adr: STRUCT<strt_nm: STRING, twn_nm: STRING, ctry: STRING>, id: STRUCT<any_bic: STRING, lei: STRING>, ctry_of_res: STRING>,
  dbtr_acct STRUCT<iban: STRING, othr: STRING, ccy: STRING>,
  dbtr_agt STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  cdtr_agt STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
  cdtr STRUCT<nm: STRING, pstl_adr: STRUCT<strt_nm: STRING, twn_nm: STRING, ctry: STRING>, id: STRUCT<any_bic: STRING, lei: STRING>, ctry_of_res: STRING>,
  cdtr_acct STRUCT<iban: STRING, othr: STRING, ccy: STRING>,
  ultmt_cdtr STRUCT<nm: STRING, lei: STRING, ctry: STRING>,
  -- purpose and reporting
  purp_cd STRING,
  purp_prtry STRING,
  rgltry_rptg ARRAY<STRUCT<dbt_cdt_rptg_ind: STRING, authrty_nm: STRING, authrty_ctry: STRING, details: ARRAY<STRING>>>,
  -- remittance
  rmt_inf_ustrd ARRAY<STRING>,
  rmt_inf_strd ARRAY<STRUCT<ref_doc: STRING, amt: DECIMAL(18,5)>>
)
USING iceberg
PARTITIONED BY (days(intr_bk_sttlm_dt))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);
```

Silver DDL (representative):

```sql
CREATE TABLE silver.transactions (
  txn_id STRING NOT NULL,
  uetr STRING NOT NULL,
  originator_id BIGINT NOT NULL,
  beneficiary_id BIGINT NOT NULL,
  originator_bank_bic STRING,
  beneficiary_bank_bic STRING,
  txn_amount DECIMAL(18,2) NOT NULL,
  txn_currency STRING NOT NULL,
  txn_amount_usd DECIMAL(18,2),
  txn_timestamp TIMESTAMP NOT NULL,
  txn_type STRING NOT NULL,  -- {wire, ach, rtp, internal, card}
  purpose_code STRING,
  correspondent_chain ARRAY<STRING>,
  cross_border BOOLEAN NOT NULL,
  regulatory_reported BOOLEAN NOT NULL,
  rptd_originator_name STRING,
  rptd_originator_address STRING,
  rptd_beneficiary_name STRING,
  rptd_beneficiary_address STRING,
  source_message_ref STRING
)
USING iceberg
PARTITIONED BY (days(txn_timestamp));

CREATE TABLE silver.entities (
  entity_id BIGINT NOT NULL,
  entity_type STRING NOT NULL,  -- {Person, Company, FI}
  name STRING NOT NULL,
  legal_name STRING,
  address STRUCT<street: STRING, town: STRING, region: STRING, postcode: STRING, country: STRING>,
  email_addr STRING,
  phone_number STRING,
  country STRING,
  lei STRING,
  bic STRING,
  sanctions_status STRING,
  pep_status BOOLEAN,
  initial_risk_score DOUBLE
)
USING iceberg;

CREATE TABLE silver.accounts (
  account_id BIGINT NOT NULL,
  iban STRING,
  holder_entity_id BIGINT NOT NULL,
  bank_bic STRING NOT NULL,
  currency STRING NOT NULL,
  opened_date DATE NOT NULL,
  closed_date DATE,
  current_balance DECIMAL(18,2)
)
USING iceberg;

CREATE TABLE silver.counterparty_edges (
  source_entity_id BIGINT NOT NULL,
  target_entity_id BIGINT NOT NULL,
  first_seen_ts TIMESTAMP NOT NULL,
  last_seen_ts TIMESTAMP NOT NULL,
  cumulative_amount_usd DECIMAL(18,2) NOT NULL,
  txn_count BIGINT NOT NULL
)
USING iceberg
PARTITIONED BY (bucket(64, source_entity_id));
```

Gold DDL and manifest DDL: to be populated in this section.

### 2C.2 Datagen

**ENG-2C.2** (implements REQ-G-*, REQ-R-04, REQ-F-05). Impact: Refactor. Three-PR sub-series.

**PR-A (Refactor, Customer360 byte-identical required):** Extract Customer360 logic from monolithic `datagen/generate.py` into a `Customer360Generator` class.
- Current state (verified): `datagen/generate.py` has no class definition. Top-level functions and a monolithic main.
- Target: `class Customer360Generator` with methods `__init__(config)`, `generate_file_data(file_id) -> pa.Table`, `total_files() -> int`.
- Verification: `pytest tests/test_functional_pipeline.py` passes; Customer360 datagen at scale=1 produces byte-identical Parquet output before and after (compare via `md5sum` of sorted Parquet).

**PR-B (Refactor, no behaviour change):** Introduce `Generator` protocol in `datagen/generate.py` and adapt `Customer360Generator` to implement it. Add a dispatch table `GENERATORS: dict[str, type[Generator]] = {"customer360": Customer360Generator}`. Main function selects generator via `WORKLOAD_SCHEMA` env var, defaulting to `"customer360"`.
- File: `src/lakebench/deploy/datagen.py` passes `WORKLOAD_SCHEMA` env var (currently unset; new addition).
- Verification: existing e2e tests pass unchanged; unit test for dispatch behaviour added.

**PR-C (Additive):** Add `FinancialGenerator` class implementing the Financial generation.
- File: `src/lakebench/templates/datagen/configmap.yaml.j2` gains three new sections: `financial.py` (schema module), `typologies.py` (injection functions), `manifest.py` (manifest emitter).
- `FinancialGenerator.__init__(config)`: deterministic construction of account and entity tables from `hash(seed, id)`. Time-partitioned file assignment: `file_id → (start_ts, end_ts)` deterministic mapping.
- `FinancialGenerator.generate_file_data(file_id) -> pa.Table`: emits pacs008-shaped rows for the time window; applies scheduled typology injections that overlap the window; writes to manifest sidecar via `manifest.py`.
- Registers under key `"financial"` in `GENERATORS`.
- Typology plugin interface: `class TypologyInjector: def schedule(seed, scale) -> list[TypologyInstance]; def inject(rows, instance) -> rows`. Eight AMLworld primitives ship as reference implementations.
- Verification: `WORKLOAD_SCHEMA=financial python datagen/generate.py` at scale=1 produces valid pacs008 Parquet and manifest.

### 2C.3 Pipeline

**ENG-2C.3** (implements SOL-2B.2, satisfies REQ-D-03, REQ-R-01, REQ-R-02, REQ-R-05). Impact: Additive.

Batch mode pipeline (default):
- File: `src/lakebench/spark/scripts/bronze_verify_financial.py` validates pacs008 schema, checks partition integrity, registers Iceberg table over the datagen output.
- File: `src/lakebench/spark/scripts/silver_build_financial.py` reads bronze pacs008, camt053, pain001; joins with party and reference tables; produces silver transactions, entities, accounts, counterparty_edges. Reuses the strategy framework (`SilverStrategy` enum) from `silver_build.py`.
- File: `src/lakebench/spark/scripts/gold_finalize_financial.py` computes daily and monthly aggregates for the dashboard workloads; populates gold from initial (empty) silver state.

Sustained mode pipeline:
- File: `src/lakebench/spark/scripts/bronze_ingest_financial.py` reads new Parquet files as they appear (Spark Structured Streaming, `maxFilesPerTrigger`), writes to bronze.
- File: `src/lakebench/spark/scripts/silver_stream_financial.py` incrementally transforms bronze → silver as new files land; updates counterparty_edges via merge.
- File: `src/lakebench/spark/scripts/gold_refresh_financial.py` periodically recomputes gold aggregates against the moving silver state.

Special-purpose scripts:
- File: `src/lakebench/spark/scripts/replay_financial.py` implements W8 (rule replay against a specific past corpus depth).
- File: `src/lakebench/spark/scripts/reproduce_financial.py` implements W10 (time-travel alert reproduction).
- File: `src/lakebench/spark/scripts/score_financial.py` runs after detection workloads; reads manifest and workload output; writes `recall.parquet`.

JobType and script registration:
- File: `src/lakebench/modules/pipeline_engines/spark/job.py` extends the `JobType` enum with `BRONZE_VERIFY_FINFRAUD`, `SILVER_BUILD_FINFRAUD`, `GOLD_FINALIZE_FINFRAUD`, `BRONZE_INGEST_FINFRAUD`, `SILVER_STREAM_FINFRAUD`, `GOLD_REFRESH_FINFRAUD`, `REPLAY_FINFRAUD`, `REPRODUCE_FINFRAUD`, `SCORE_FINFRAUD`.
- File: Same file around line 1172 extends the script map with the new script filenames.
- File: Same file around line 1883 extends `spark_scripts_to_ship` (the list of scripts baked into the Spark image) with the new scripts.
- Delta variants: not shipped in Financial v1 (see Part 4 deferred; Delta + graph compatibility uncertain).

**ENG-R-05** (implements REQ-R-05, satisfies retention workload survival). Impact: Extension.
- File: `src/lakebench/cli/_run.py` around the `_run_iceberg_maintenance` call site: consult the workload declaration and pass a schema-aware `retention_threshold`. **Note (V-23)**: the existing parser at `modules/table_formats/iceberg/maintenance.py:97` maps suffix `m` to **minutes**, not months. Retention windows expressed in days:
  ```python
  if cfg.architecture.workload.schema_type == WorkloadSchema.FINANCIAL and cfg.architecture.pipeline.retention_workload:
      # 60 months × ~30.5 days + 6 months headroom = ~2013 days
      retention_days = int((cfg.architecture.workload.financial.retention_months + 6) * 30.5)
      retention_threshold = f"{retention_days}d"
  else:
      retention_threshold = "0s"  # existing default
  ```
- File: `src/lakebench/config/schema.py` `PipelineConfig` gains `retention_workload: bool = False`.
- File (optional): `src/lakebench/modules/table_formats/iceberg/maintenance.py` may add a new suffix `mo` for months if a natural months representation is preferred; strictly optional since days-notation works today.
- Verification: recipe with `retention_workload: true` executes the maintenance step without expiring snapshots newer than the retention window; W10 (time-travel reproduction) successfully queries a snapshot from 24 months prior.
- Not this task: automatic snapshot retention tuning based on actual workload usage (deferred).

Executor profiles for Financial stages: to be characterised in Appendix D once representative silver_build runs are measured. Initial estimates: silver_build likely needs the same 4 cores / 48 GB / 12 GB overhead / 150 GB scratch as customer360 silver_build; graph workloads (W1-W4) likely need larger scratch for GraphFrames shuffles.

### 2C.4 Workloads

**ENG-2C.4.1** (W1 Synthetic Identity via Connected Components; implements REQ-D-01, REQ-D-02).

Path A (Spark + GraphFrames):
```python
identity_edges = spark.sql("""
  SELECT entity_id AS src, CAST(concat('addr:', address.street, '|', address.town, '|', address.country) AS STRING) AS dst FROM silver.entities WHERE address IS NOT NULL
  UNION
  SELECT entity_id AS src, CAST(concat('email:', email_addr) AS STRING) AS dst FROM silver.entities WHERE email_addr IS NOT NULL
  UNION
  SELECT entity_id AS src, CAST(concat('phone:', phone_number) AS STRING) AS dst FROM silver.entities WHERE phone_number IS NOT NULL
""")
identity_nodes = # union of entity IDs and attribute IDs typed as Person/Address/Email/Phone
g = GraphFrame(identity_nodes, identity_edges)
degrees = g.degrees
pruned = identity_nodes.join(degrees, "id").filter("type = 'Person' OR degree > 1")
g2 = GraphFrame(pruned, identity_edges)
components = g2.connectedComponents()
suspicious = components.filter(person_count_per_component > 1)
# write to gold.synthetic_id_clusters
```

Path B (Spark DataFrame native, iterative CC on labelled edges): same output, uses iterative merge on smallest neighbour labels.

Measured: rows scanned, components found, iterations to convergence, wall-clock, storage read throughput.

**ENG-2C.4.2** (W2 Structuring 4-to-1 Motif; implements REQ-D-01).

Path A (GraphFrames motif finding):
```python
txn_edges = spark.sql("SELECT originator_id AS src, beneficiary_id AS dst, txn_amount, txn_id AS id FROM silver.transactions WHERE txn_timestamp >= '<window_start>' AND txn_timestamp < '<window_end>'")
txn_nodes = # distinct entity IDs
g = GraphFrame(txn_nodes, txn_edges)
motif = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (d)-[e3]->(f); (f)-[e5]->(c); (c)-[e6]->(g)")
joined = motif.alias("g1").join(motif.alias("g2"), "g1.g.id = g2.g.id").filter("g1.e6.txn_amount + g2.e6.txn_amount > 10000")
# write to gold.alerts with rule_id='W2_structuring'
```

Path B: sequence of DataFrame joins implementing the same motif. Uglier but engine-portable.

Measured: rows scanned, motif matches, wall-clock.

**ENG-2C.4.3** (W3 Round-tripping 4-Cycle Motif; implements REQ-D-01). Impact: Additive.

Detects cycles where funds return to their origin after passing through 2-3 intermediaries within a short time window. Round-tripping is a canonical layering typology surfacing in the AMLworld dataset and the Databricks accelerator NB1.

Path A (GraphFrames motif finding):
```python
from graphframes import GraphFrame
from pyspark.sql.functions import col, unix_timestamp, sum as sum_

# Time window: 30 days by default; configurable per run
window_start = "'2026-06-01'"
window_end = "'2026-06-30'"

txn_edges = spark.sql(f"""
    SELECT
      originator_id AS src,
      beneficiary_id AS dst,
      txn_amount_usd AS amount,
      txn_timestamp AS ts,
      txn_id AS id
    FROM {{catalog}}.{{silver_transactions_table}}
    WHERE txn_timestamp >= TIMESTAMP {window_start}
      AND txn_timestamp <  TIMESTAMP {window_end}
      AND txn_amount_usd >= 1000
""")
txn_nodes = spark.sql(f"""
    SELECT DISTINCT entity_id AS id FROM {{catalog}}.{{silver_entities_table}}
""")

g = GraphFrame(txn_nodes, txn_edges)
cycles = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)")

# Constrain to temporal ordering (each hop later than the previous) and amount similarity (round-trip returns similar value)
tight_cycles = cycles.filter(
    "e1.ts < e2.ts AND e2.ts < e3.ts AND e3.ts < e4.ts "
    "AND unix_timestamp(e4.ts) - unix_timestamp(e1.ts) < 30 * 86400 "
    "AND abs(e4.amount - e1.amount) / e1.amount < 0.10"
)

alerts = tight_cycles.select(
    col("a.id").alias("entity_id"),
    col("e1.id").alias("outbound_txn"),
    col("e4.id").alias("inbound_txn"),
    col("e1.amount").alias("cycle_amount_usd"),
    (unix_timestamp("e4.ts") - unix_timestamp("e1.ts")).alias("cycle_duration_seconds")
).withColumn("rule_id", lit("W3_round_tripping")).withColumn("rule_version", lit("v1"))

alerts.writeTo(f"{catalog}.gold.alerts").append()
```

Path B (DataFrame joins, no GraphFrames):
```python
t = spark.table(f"{catalog}.{silver_transactions_table}").filter(
    "txn_timestamp >= '2026-06-01' AND txn_timestamp < '2026-06-30' AND txn_amount_usd >= 1000"
).select("originator_id", "beneficiary_id", "txn_amount_usd", "txn_timestamp", "txn_id")

# Self-join four times, tracking temporal order
hop1 = t.alias("h1")
hop2 = t.alias("h2").filter("txn_timestamp > (SELECT MIN(txn_timestamp) FROM t)")
# ... four-way join a → b → c → d → a with temporal-order and amount-similarity filters
```
Path B is significantly slower (four-way join with self-join skew) and used only when GraphFrames is unavailable in the recipe.

Measured: rows scanned, cycles found, wall-clock, storage read throughput. Alerts written to `gold.alerts` with `rule_id = 'W3_round_tripping'`.

Not this task: n-way cycles beyond 4 (deferred; parameterisable in a follow-on release).

**ENG-2C.4.4** (W4 Risk Propagation via Pregel; implements REQ-D-01).

Path A (GraphFrames Pregel):
```python
g = GraphFrame(entity_nodes_with_risk, txn_edges)
ranks = g.pregel \
  .setMaxIter(3) \
  .withVertexColumn("risk_score", col("initial_risk"), coalesce(Pregel.msg() + col("risk_score"), col("risk_score"))) \
  .sendMsgToDst(Pregel.src("risk_score") / lit(2)) \
  .aggMsgs(sum(Pregel.msg())) \
  .run()
# write to gold.risk_scores versioned by model_id
```

Path B: iterative DataFrame join implementing the same message-passing semantics.

Measured: iterations to convergence, vertex updates per second, wall-clock.

**ENG-2C.4.5** (W5 Entity Resolution via Splink; implements REQ-D-01).

```python
from splink import Linker
settings = {
  "link_type": "dedupe_only",
  "blocking_rules": ["l.txn_amount = r.txn_amount"],
  "comparison_columns": [
    {"col_name": "rptd_originator_name", "term_frequency_adjustments": True},
    {"col_name": "rptd_originator_address", "term_frequency_adjustments": True},
    {"col_name": "originator_bank_bic"}
  ]
}
linker = Linker(settings, silver.transactions, spark)
matches = linker.get_scored_comparisons()
# write matches to gold.entity_resolution_scores
```

Measured: pairwise comparison count, match count, throughput (rows per second per core).

**ENG-2C.4.pre** (query registry refactor; prerequisite for ENG-2C.4.6-7; implements SOL-2B.5). Impact: Refactor. Two-PR sub-series. Customer360 byte-identical required.

**PR-Q-A (Refactor, no behaviour change):** Restructure `src/lakebench/benchmark/queries.py` from a hardcoded module-level `BENCHMARK_QUERIES` list into a per-domain registry. Existing Customer360 queries move to `BENCHMARK_QUERIES_BY_DOMAIN["customer360"]` and continue to be exposed as `BENCHMARK_QUERIES` (via a module-level alias reading the customer360 entry) for backward compatibility. `benchmark/runner.py` grows a lookup `queries = BENCHMARK_QUERIES_BY_DOMAIN[config.architecture.workload.schema_type.value]`, falling back to Customer360 for the legacy alias path.
- File: `src/lakebench/benchmark/queries.py` restructured; `BENCHMARK_QUERIES` retained as `BENCHMARK_QUERIES_BY_DOMAIN["customer360"]` for backward compat with any external import.
- File: `src/lakebench/benchmark/runner.py` at lines 218 and 274 updated to select by domain.
- Verification: `pytest tests/test_benchmark.py` passes unchanged; Customer360 recipe produces identical benchmark output before and after (compare `metrics.parquet`).

**PR-Q-B (Additive):** Add Financial query set as `BENCHMARK_QUERIES_BY_DOMAIN["financial"]`.
- File: `src/lakebench/benchmark/queries.py` gains a `_FF_Q1` through `_FF_Q7` block of `BenchmarkQuery` records covering the investigator query set (W6, W7 below) plus scan, filter-prune, aggregation, and operational (dashboard) query classes to keep parity with the Customer360 five-category coverage.
- Verification: `WORKLOAD_SCHEMA=financial pytest tests/test_financial_workloads.py` (new test module) passes.

**ENG-2C.4.6** (W6 Investigator Entity Timeline; implements REQ-I-01, REQ-I-02). Impact: Refactor (depends on ENG-2C.4.pre).

Registered as `_FF_Q_TIMELINE` in `BENCHMARK_QUERIES_BY_DOMAIN["financial"]`. ANSI-compliant SQL, runs on both Spark SQL and Trino. Query template uses the same `{catalog}`, `{silver_table}`, `{gold_table}` placeholders as Customer360 queries; also introduces `{silver_entities_table}` and `{gold_alerts_table}` placeholders that the runner templates from `config.architecture.tables` (which gains `silver_entities`, `silver_accounts`, `gold_alerts` fields alongside the existing `silver` and `gold`).
```sql
SELECT
  t.txn_timestamp,
  t.txn_id,
  t.uetr,
  t.txn_amount_usd,
  t.txn_type,
  t.cross_border,
  cp.name AS counterparty_name,
  cp.country AS counterparty_country,
  cp.sanctions_status AS counterparty_sanctions,
  a.rule_id AS alert_rule
FROM {catalog}.{silver_table} t
LEFT JOIN {catalog}.{silver_entities_table} cp ON cp.entity_id =
  CASE WHEN t.originator_id = ? THEN t.beneficiary_id ELSE t.originator_id END
LEFT JOIN {catalog}.{gold_alerts_table} a ON a.related_txn_id = t.txn_id
WHERE (t.originator_id = ? OR t.beneficiary_id = ?)
  AND t.txn_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' MONTH
ORDER BY t.txn_timestamp DESC;
```

Measured: P50, P95 latency (via existing `queries_per_hour` and per-query timings in `StageMetrics`), rows returned, data read, load condition (via new `StageMetrics.load_condition` field from ENG-M-05).

**ENG-2C.4.7** (W7 Two-Hop Traversal; implements REQ-I-01, REQ-I-02). Impact: Refactor (depends on ENG-2C.4.pre).

Registered as `_FF_Q_TWOHOP`.
```sql
WITH direct AS (
  SELECT DISTINCT beneficiary_id AS hop1_entity
  FROM {catalog}.{silver_table}
  WHERE originator_id = ? AND txn_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' MONTH
),
two_hop AS (
  SELECT DISTINCT t.beneficiary_id AS hop2_entity
  FROM {catalog}.{silver_table} t
  JOIN direct d ON t.originator_id = d.hop1_entity
  WHERE t.txn_amount_usd > 10000
    AND t.txn_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' MONTH
)
SELECT th.hop2_entity, e.name, e.country, e.sanctions_status
FROM two_hop th
JOIN {catalog}.{silver_entities_table} e ON e.entity_id = th.hop2_entity
WHERE e.sanctions_status IS NOT NULL OR e.pep_status = TRUE;
```

Measured: P50, P95 latency, fan-out count, load condition.

**ENG-2C.4.8** (W8 Historical Replay; implements REQ-R-01). Impact: Additive.

Runs a specified detection rule against silver as it existed at a chosen past corpus depth. This is the specific capability supervisors require in post-enforcement lookbacks (TD Bank pattern). The workload validates two things: that the historical snapshot is queryable at all (retention_workload plumbing works), and that the detection cost at N months' depth is reasonable (regulators impose deadlines).

CLI:
```bash
lakebench financial-replay <config.yaml> \
    --rule W2_structuring \
    --depth-months 60 \
    --new-threshold 8000 \
    --output-alerts gold.alerts_replay_2020_08
```

Script (`src/lakebench/spark/scripts/replay_financial.py`, invoked as a SparkApplication):
```python
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import sys

def resolve_snapshot_id(spark, catalog, table, depth_months):
    """Find the Iceberg snapshot id closest to but not after the target timestamp."""
    target_ts = datetime.utcnow() - timedelta(days=int(depth_months * 30.5))
    snapshots = spark.sql(f"""
        SELECT snapshot_id, committed_at
        FROM {catalog}.{table}.snapshots
        WHERE committed_at <= TIMESTAMP '{target_ts.isoformat()}'
        ORDER BY committed_at DESC
        LIMIT 1
    """).collect()
    if not snapshots:
        sys.exit(f"No snapshot at depth {depth_months} months; retention_workload may be off")
    return snapshots[0].snapshot_id

def run(rule_id, depth_months, threshold, output_table):
    spark = SparkSession.builder.appName(f"replay_{rule_id}").getOrCreate()
    snap = resolve_snapshot_id(spark, catalog, "silver.transactions", depth_months)
    # Rebuild silver at the historical snapshot; the rule module is imported from the detection workload script
    if rule_id == "W2_structuring":
        from silver_build_financial import build_view_at_snapshot
        from workload_w2_structuring import detect
        historical_silver = build_view_at_snapshot(spark, snap)
        alerts = detect(historical_silver, structuring_threshold=threshold)
    elif rule_id == "W3_round_tripping":
        # same pattern
        ...
    else:
        sys.exit(f"Unsupported rule: {rule_id}")
    alerts.writeTo(output_table).createOrReplace()
```

Verification: at scale=100 with `retention_workload: true`, running `--rule W2_structuring --depth-months 60` returns a non-empty alerts table within the timeout budget documented in `docs/financial-benchmark-baselines.md`; the number of alerts differs from a current-corpus W2 run (proving snapshot isolation).

Not this task: rule versioning across replay windows (deferred; assumes the rule module code is stable across the replay window).

**ENG-2C.4.9** (W9 Derived Writeback; implements REQ-D-01, REQ-M-03). Impact: Additive.

Measures the write pattern that operational AML systems produce: each rule execution appends alerts to `gold.alerts` with version metadata, and updates entity risk scores in `gold.risk_scores` via row-level merge. The workload is not "run new detection"; it's "quantify the write cost of running detection when scores need to be updated in place and alerts append at the current cadence".

Script (`src/lakebench/spark/scripts/writeback_financial.py`):
```python
def write_alerts(alerts_df, table, rule_id, rule_version, model_id="rule", model_version="v1"):
    from pyspark.sql.functions import current_timestamp, lit
    enriched = alerts_df \
        .withColumn("rule_id", lit(rule_id)) \
        .withColumn("rule_version", lit(rule_version)) \
        .withColumn("model_id", lit(model_id)) \
        .withColumn("model_version", lit(model_version)) \
        .withColumn("alert_ts", current_timestamp()) \
        .withColumn("run_id", lit(spark.conf.get("spark.lb.run_id")))
    enriched.writeTo(table).append()

def merge_risk_scores(new_scores_df, table, model_id, model_version):
    """Iceberg row-level MERGE into gold.risk_scores keyed by (entity_id, model_id)."""
    new_scores_df.createOrReplaceTempView("_updates")
    spark.sql(f"""
        MERGE INTO {table} t
        USING _updates u
        ON t.entity_id = u.entity_id AND t.model_id = '{model_id}'
        WHEN MATCHED THEN UPDATE SET
            t.risk_score = u.risk_score,
            t.model_version = '{model_version}',
            t.updated_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (entity_id, model_id, model_version, risk_score, updated_ts)
            VALUES (u.entity_id, '{model_id}', '{model_version}', u.risk_score, CURRENT_TIMESTAMP())
    """)
```

Measured: write throughput (rows per second), merge cost (versioned append vs row-level merge time delta), version count per entity over time, file count growth per cycle (relevant for maintenance planning). Emits `StageMetrics` rows with `stage_name = "financial_writeback_alerts"` and `stage_name = "financial_writeback_risk_scores"`.

Not this task: streaming writeback via Iceberg equality-delete files (deferred; batch merge sufficient for v1).

**ENG-2C.4.10** (W10 Time-Travel Reproduction; implements REQ-R-02). Impact: Additive.

Given a historical alert (identified by its UETR and alert_ts), reproduce the exact silver state at the alert's snapshot and re-execute the alerting rule. The reproduced alert must match the historical alert bit-for-bit on the fields the rule computes. This is the supervisory-workflow reproducibility mechanism.

Script (`src/lakebench/spark/scripts/reproduce_financial.py`):
```python
def reproduce(alert_uetr):
    # 1. Find the historical alert record
    hist = spark.sql(f"""
        SELECT rule_id, rule_version, alert_ts, entity_id, related_txn_id, snapshot_id
        FROM {catalog}.gold.alerts
        WHERE uetr = '{alert_uetr}'
    """).collect()[0]

    # 2. Snapshot the silver state as of the alert generation
    silver_at_alert = spark.sql(f"""
        SELECT * FROM {catalog}.silver.transactions
        FOR VERSION AS OF {hist.snapshot_id}
    """)
    silver_at_alert.createOrReplaceTempView("silver_transactions_hist")

    # 3. Re-run the alerting rule against the historical silver
    module = importlib.import_module(f"workload_{hist.rule_id.lower()}")
    reproduced = module.detect_for_entity(hist.entity_id, silver_table="silver_transactions_hist")

    # 4. Compare bit-for-bit
    reproduced_alert = reproduced.filter(f"related_txn_id = '{hist.related_txn_id}'").collect()
    if len(reproduced_alert) != 1:
        return {"status": "FAIL", "reason": f"Expected 1 alert, got {len(reproduced_alert)}"}
    for field in ["entity_id", "related_txn_id", "alert_amount", "alert_score"]:
        if getattr(reproduced_alert[0], field) != getattr(hist, field, None):
            return {"status": "FAIL", "reason": f"Field {field} differs"}
    return {"status": "PASS", "latency_ms": ...}
```

Measured: reproduction success rate (successful reproductions / attempted reproductions), reproduction latency P50 and P95 per rule, snapshot resolution time (time from UETR to snapshot_id). Emits scorecard row `timetravel_reproduction_success_rate_pct` and `timetravel_reproduction_latency_p95_ms`.

Verification: at scale=100 with 60 months retention and 12 monthly cycles, sample 100 historical alerts from cycle 1's alerts table and reproduce each against silver as of cycle 1's snapshot. Success rate should be 100%. If below, investigate: was the rule module non-deterministic, did datagen inputs change between cycles for the same file_id, did compaction rewrite the underlying data.

Not this task: reproduction across rule version changes (deferred; assumes rule module code stable).

**ENG-2C.4.11** (W11 Sustained Ingest; implements SOL-U-01, satisfies REQ-P-*). Impact: Additive.

Measures sustained ingest throughput and freshness. Datagen runs continuously (existing `DatagenMode.CONTINUOUS`), producing Parquet files at a target TPS to the drop path. `bronze_ingest_financial.py` (Spark Structured Streaming) reads via `maxFilesPerTrigger` and appends to `bronze.pacs008`. `silver_stream_financial.py` processes incremental micro-batches into silver. `gold_refresh_financial.py` recomputes gold aggregates on schedule.

Script (`src/lakebench/spark/scripts/bronze_ingest_financial.py`):
```python
def run():
    spark = SparkSession.builder.appName("bronze_ingest_financial").getOrCreate()
    schema = load_pacs008_schema()  # see Appendix B

    stream = spark.readStream \
        .format("parquet") \
        .schema(schema) \
        .option("maxFilesPerTrigger", 200) \
        .option("recursiveFileLookup", "true") \
        .load(bronze_drop_path)

    query = stream.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("path", f"{catalog}.bronze.pacs008") \
        .option("checkpointLocation", f"{checkpoint_path}/bronze_ingest_financial") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()
```

Config (`architecture.workload.financial.sustained_ingest`):
```yaml
sustained_ingest:
  target_tps: 25000           # sub-scope baseline for a tier-1 bank (wires + institutional)
  peak_tps: 150000            # burst target
  bronze_ingest:
    max_files_per_trigger: 200
    processing_time_s: 30
  silver_stream:
    max_files_per_trigger: 100
    processing_time_s: 60
  gold_refresh:
    processing_time_s: 300
  duration_minutes: 30        # measurement window
  warmup_minutes: 5
```

Measured: sustained achieved TPS (rows/second averaged over the measurement window, excluding warmup), transaction-to-bronze latency P50 and P95 (time from file drop to Iceberg commit), transaction-to-silver latency P50 and P95 (time from file drop to silver append), ingest lag (rows in bronze but not yet in silver), file count growth in bronze (relevant for maintenance planning). Scorecard rows: `sustained_ingest_achieved_tps`, `ingest_latency_bronze_p95_ms`, `ingest_latency_silver_p95_ms`.

Verification: at scale=100, sustained_ingest with target 25000 TPS achieves at least 20000 TPS averaged over 30 minutes on the reference lab cluster; transaction-to-silver P95 stays under 5 minutes.

Not this task: exactly-once semantics beyond Iceberg's default (deferred), Kafka source (deferred to `-kafka` recipe, Part 4).

### 2C.5 Scoring and metrics

**ENG-2C.5** (implements SOL-2B.5, satisfies REQ-M-*, REQ-D-04). Impact: Refactor. Two-PR sub-series. Customer360 byte-identical required.

**PR-R-A (Refactor, no behaviour change):** Introduce a `ScorecardBlock` interface in `src/lakebench/reports/generator.py` and extract the existing Customer360 rendering into a `Customer360ScorecardBlock` class. Existing scorecard output must be byte-identical after this PR.
- File: `src/lakebench/reports/generator.py` gains a `class ScorecardBlock(Protocol)` with methods `render_html(metrics: MetricsSet) -> str`, `render_console(metrics: MetricsSet) -> str`, `metric_names() -> list[str]`.
- File: same file gains `class Customer360ScorecardBlock` implementing the protocol, containing the currently-hardcoded QpH cards, Time to Value cards, throughput cards, maintenance cards, etc.
- File: The top-level `generate_report` function selects the correct block based on schema type; defaults to Customer360 for backward compat.
- Verification: run a customer360 e2e test before and after, compare rendered HTML with `diff` (must be zero delta).

**PR-R-B (Additive):** Add `FinancialScorecardBlock`.
- File: `src/lakebench/reports/generator.py` gains `class FinancialScorecardBlock` implementing the protocol. Renders detection recall (per typology, from `recall.parquet`), investigator P50/P95 (idle vs loaded), concurrent degradation ratio, replay wall-clock (per depth), time-travel reproduction success rate, sustained ingest TPS.
- File: `src/lakebench/spark/scripts/score_financial.py` runs after detection workloads. Reads `bronze/manifest/manifest.parquet` (isolated, per REQ-G-02) and workload output (`gold/alerts/`). Joins on typology_id and transaction_ids. Writes `recall.parquet` with `(rule_id, typology_type, injected_count, detected_count, recall, precision)`.
- File: `src/lakebench/metrics/collector.py` extended with Financial metric definitions (new metric_name enum values); all values write to `metrics.parquet` with full provenance.
- Verification: Financial e2e test produces scorecard HTML with all Financial metrics populated; concurrent degradation ratio computed from `metrics.parquet` matches independent calculation.

Metrics schema (`metrics.parquet`) additions (post ENG-M-05):
```
metric_name       STRING NOT NULL  -- e.g., 'investigator_p95_loaded'
scale_factor      DOUBLE NOT NULL
workload_id       STRING           -- e.g., 'W7'
engine            STRING           -- e.g., 'trino', 'spark_thrift'
load_condition    STRING           -- {idle, pairwise, full, null}
value             DOUBLE NOT NULL
unit              STRING NOT NULL  -- {ms, gb_s, ratio, count, pct}
captured_at       TIMESTAMP NOT NULL
run_id            STRING NOT NULL
```

**ENG-M-05** (implements REQ-M-05, prerequisite for concurrent measurement). Impact: Extension.
- File: `src/lakebench/metrics/collector.py` `StageMetrics` dataclass (around line 291) gains `load_condition: str | None = None` field.
- File: `src/lakebench/metrics/collector.py` `to_dict()` includes the field only when non-null (preserves existing serialisation shape for Customer360).
- File: `src/lakebench/metrics/storage.py` (metric persistence to parquet) includes the new field in the Iceberg/Parquet schema; existing records without the field default to null.
- File: `src/lakebench/benchmark/runner.py` `run_power` and `run_throughput` accept a `load_condition: str | None = None` parameter, tag each `StageMetrics` row emitted.
- File: Financial orchestrator (`cli/financial_run.py`, new; see ENG-2C.4.pre + concurrent modes) passes `load_condition` per pass.
- Verification: existing Customer360 runs produce `metrics.parquet` with `load_condition` null (verified byte-equivalence with pre-change reference); Financial runs at scale=10 produce populated `load_condition` values across idle/pairwise/full modes.

### 2C.6 Recipes

**ENG-2C.6** (implements SOL-U-05, satisfies REQ-P-02).

Recipe naming: existing convention `{catalog}-{format}-{processor}-{engine}` preserved. Domain becomes a separate config field `architecture.workload.domain` accepting {customer360, financial}. Users pick a recipe (architecture) and a domain (workload) independently.

Ships with Financial domain support for:

| Recipe | GraphFrames | Splink | W1-W4 | W5 | W6-W11 |
|---|---|---|---|---|---|
| `hive-iceberg-spark-trino` | optional | optional | Spark | Spark | both engines |
| `hive-iceberg-spark-thrift` | optional | optional | Spark | Spark | Spark only |
| `hive-iceberg-spark-none` | optional | optional | Spark | Spark | Spark only |
| `hive-iceberg-spark-duckdb` | no | no | not supported | not supported | W6, W7, W10 on DuckDB; others skipped |
| `polaris-iceberg-spark-trino` | optional | optional | Spark | Spark | both engines |
| `polaris-iceberg-spark-thrift` | optional | optional | Spark | Spark | Spark only |
| `polaris-iceberg-spark-none` | optional | optional | Spark | Spark | Spark only |
| `polaris-iceberg-spark-duckdb` | no | no | not supported | not supported | W6, W7, W10 on DuckDB; others skipped |
| `hive-delta-*` variants | no (Delta+GraphFrames compatibility incomplete) | optional | Spark DataFrame path only | Spark | both engines where applicable |

Where a workload is not supported by an engine, the recipe's scorecard row for that workload is `null` with a documented reason. Not a failure.

Recipe registration: `_SUPPORTED_COMBINATIONS` in `src/lakebench/config/schema.py:930` gains no new entries (existing 11 combinations already cover the Financial recipe surface). The schema-domain axis is orthogonal to the recipe axis and lives in `WorkloadSchema` / `SCHEMA_DIMENSION_MAP`. Users select recipe and domain independently.

New config fields (v1.3-style, matching the current flat top-level convention):
```yaml
# Top-level fields (v1.3 flat convention)
endpoint: ${S3_ENDPOINT}
access_key: ${S3_ACCESS_KEY}
secret_key: ${S3_SECRET_KEY}
namespace: lb-financial-100
scale: 100
mode: batch                    # {batch, sustained}
spark_image: apache/spark:4.2.0-python3

# Nested architecture block (existing pattern)
architecture:
  workload:
    schema: financial          # {customer360, financial, custom}
    datagen:
      scale: 100
      mode: continuous         # existing DatagenMode; auto at scale > 10
    financial:                 # new per-domain block, parallel to customer360
      typology_density: 0.001
      correspondent_chain_ratio: 0.15
      cross_border_ratio: 0.20
      retention_months: 60
  pipeline:
    retention_workload: true   # ENG-R-05: preserves 66 months of snapshots

# Spark extensions (new)
spark_extensions:
  graphframes: true
  splink: true
```

### 2C.7 Component matrix

**ENG-2C.7** (implements SOL-U-01 through SOL-U-04, satisfies REQ-U-*).

| Component | Version | Status for Financial |
|---|---|---|
| Apache Spark | 3.5.4, 4.0.2, 4.1.2, 4.2.0 (default) | 4.2.0 recommended; graph workloads validated on 4.1.2 and 4.2.0 |
| Spark Operator | 2.5.1 (Kubeflow) | unchanged |
| Apache Iceberg | 1.11.0 (default), format-version 2 or 3 | v2 default; W10 (time-travel) validated on both |
| Delta Lake | 4.0.0 or 4.1.0 (auto by Spark version) | graph workloads not supported on Delta in v1 (compatibility TBD) |
| Hive Metastore | 3.1.3 (Stackable 25.7.0) | unchanged |
| Apache Polaris | 1.6.0 | unchanged |
| Trino | 483 | W6, W7, W10 supported |
| DuckDB | bundled | W6, W7 partial support; graph workloads not supported |
| PostgreSQL | 16, 17, 18 | unchanged |
| GraphFrames | 0.9.3+ | optional; required for graph workload native path |
| Splink | 4.0.x | optional; required for W5 |
| Kubernetes | 1.26+ | unchanged |

### 2C.8 Batch, multi-cycle batch, and sustained modes

**ENG-2C.8** (implements SOL-2B.3, satisfies REQ-C-*, REQ-M-02, REQ-R-01). Impact: Extension.

**Confirmed at review (V-22):** three execution modes exist in lakebench-k8s today, not two. The spec's original batch/sustained framing missed multi-cycle batch, which is a v1.1.0 feature (`pipeline.cycles` 1-50). All three apply to Financial.

**Batch mode (`mode: batch, cycles: 1`).** Datagen runs to completion producing the full historical corpus at the target scale. Pipeline runs `bronze_verify` → `silver_build` → `gold_finalize` once. Detection workloads (W1-W5) execute against the finished silver. Investigator workloads (W6, W7) execute against a static silver. Replay (W8) and reproduction (W10) execute against Iceberg snapshots created during silver_build. Sustained ingest (W11) not applicable. This is the default mode and the fastest way to produce a full scorecard.

**Multi-cycle batch mode (`mode: batch, cycles: N`).** Datagen splits the timestamp range evenly across N cycles. Cycle 1 is full overwrite, cycles 2 through N are incremental appends. The pipeline runs once per cycle. Cycle-progression scoring measures how QpH, silver throughput, and maintenance cost degrade or maintain across cycles. **This is the natural execution mode for Financial** because it simulates ongoing operation: monthly SAR filings, weekly detection reruns, incremental typology injection. Historical replay (W8) at cycle N naturally uses snapshots from earlier cycles as its target corpus.

**Sustained mode (`mode: sustained`).** Datagen runs continuously at a target TPS. `bronze_ingest_financial` (streaming) writes new files to bronze as they land. `silver_stream_financial` incrementally transforms. `gold_refresh_financial` recomputes aggregates on a schedule. Detection workloads (W1-W5) can run in sustained mode against a moving silver, or against a specific snapshot. Investigator workloads (W6, W7) execute concurrently with ingest. W11 is the ingest itself. Concurrent workload orchestration (REQ-C-01) applies primarily to sustained mode.

Workload compatibility:

| Workload | Batch (cycles=1) | Multi-cycle batch (cycles>1) | Sustained | Notes |
|---|---|---|---|---|
| W1-W4 (detection) | yes | yes (per cycle) | yes (against snapshot) | Multi-cycle: detection runs at end of each cycle; QpH per cycle |
| W5 (entity resolution) | yes | yes (per cycle) | yes | Multi-cycle: incremental resolution across cycles |
| W6 (timeline) | yes | yes (post-cycle) | yes | Sustained mode is where concurrent-load lives |
| W7 (two-hop) | yes | yes (post-cycle) | yes | Same as W6 |
| W8 (replay) | yes | **native fit** | yes | Multi-cycle: replay at cycle N against corpus as of cycle N-K |
| W9 (writeback) | yes | yes (append per cycle) | yes | Multi-cycle: writes get natural versioning per cycle |
| W10 (time-travel) | yes | **native fit** | yes | Multi-cycle: each cycle creates its own snapshot naturally |
| W11 (sustained ingest) | no | no | yes | Sustained mode only |

Measurement differences:

| Metric | Batch (cycles=1) | Multi-cycle batch | Sustained mode |
|---|---|---|---|
| Time to Value | Total pipeline wall-clock, datagen to gold | Cycle 1 TTV; cycle N wall-clock for cycles 2..N | Not published; use freshness metrics |
| Throughput | Effective processing rate across pipeline | Per-cycle silver throughput; cycle progression | Sustained ingest TPS |
| Investigator P95 | Static silver | Static silver per cycle; **degradation across cycles is the primary Financial measurement** | Moving silver under load |
| Concurrent degradation ratio | Measured in scripted pairwise runs | Measured across cycles as maintenance/compaction cost grows | Measured naturally alongside ingest and stream |
| Freshness | N/A | N/A (per-cycle) | Transaction-to-bronze, transaction-to-silver latency |
| Cycle progression (v1.1.0) | N/A | **First-class**: QpH cycle-over-cycle, silver-throughput cycle-over-cycle, maintenance-cost cycle-over-cycle | N/A |
| Historical replay (W8) | Against static snapshot | **Native**: replay at cycle N against snapshots from cycles 1..N-1 | Against snapshot |

### 2C.15 Multi-cycle batch integration for Financial

**ENG-2C.15** (satisfies REQ-R-01, integrates with existing multi-cycle batch). Impact: Additive.

**Confirmed at review (V-22):** `pipeline.cycles` config field accepts 1-50 today; the datagen already splits timestamp range evenly across cycles; scoring already produces `cycle_progression` when `cycles > 1`. Financial recipes need to plug into this cleanly rather than reimplementing.

- File: `datagen/generate.py` (post ENG-2C.2 refactor) `FinancialGenerator` respects `CYCLE_INDEX` and `TOTAL_CYCLES` env vars (existing customer360 datagen convention) and produces the correct timestamp range slice for its cycle.
- File: Financial silver_build honours the incremental-append semantics: cycle 1 is full overwrite, cycles 2..N append to the existing silver.transactions and merge into silver.counterparty_edges.
- File: `src/lakebench/spark/scripts/replay_financial.py` (W8) accepts a `--from-cycle N` flag as an alternative to `--depth-months`; runs the detection logic against the snapshot as-of the end of cycle N.
- File: `src/lakebench/metrics/collector.py` `CycleMetrics` (existing) is populated per Financial cycle without new fields; scorecard includes cycle-progression trends for the Financial-specific metrics.
- Config example:
  ```yaml
  architecture:
    workload:
      schema: financial
      financial:
        retention_months: 60
        typology_density: 0.001
    pipeline:
      cycles: 12                # simulate 12 monthly cycles
      retention_workload: true
  ```
- Verification: `lakebench run <config>` with `cycles: 3` at scale=10 completes all three cycles, produces a scorecard with per-cycle metrics and cycle-progression trends; W8 replay against cycle-2 snapshot succeeds.
- Not this task: cycle-aware ML training (deferred to ML extension refinement).

### 2C.10 Autosizer integration

**ENG-2C.10** (satisfies REQ-S-*, integrates with existing autosizer). Impact: Extension.

**Confirmed at review (V-19):** the autosizer is schema-agnostic today. `full_compute_guidance(scale)` in `src/lakebench/config/scale.py:313` takes only `scale`. Executor profiles (`docs/architecture.md:176`) are fixed per-stage: silver-build 4c/48g/12g/150Gi. Financial silver_build would inherit this profile. Graph workloads (W1-W4) likely need more scratch for GraphFrames shuffle spill and possibly more memory for large motif matches.

- File: `src/lakebench/config/scale.py` `full_compute_guidance` signature gains a `schema_type: str = "customer360"` parameter. Existing callers unchanged (default preserves behaviour). Financial path returns adjusted `SparkGuidance` with larger scratch (300Gi) for the graph workload stages.
- File: `src/lakebench/config/autosizer.py` invokes `full_compute_guidance(scale, config.architecture.workload.schema_type)`.
- File: `docs/architecture.md` gains a "Financial stage profiles" subsection under "Spark Executor Profiles" with the adjusted values.
- File: `tests/test_autosizer.py` extended with Financial sizing test cases at scale 1, 10, 100.
- Verification: `pytest tests/test_autosizer.py -k financial` passes; customer360 sizing at every scale point is byte-identical before and after; scale=100 Financial pipeline completes without OOM on the lab cluster.
- Not this task: sizing tuning based on measured resource utilisation (deferred until observability data available).

### 2C.11 Init wizard support

**ENG-2C.11** (satisfies REQ-A-05, integrates with existing wizard). Impact: Extension.

**Confirmed at review (V-20):** the init wizard `WizardState` dataclass in `src/lakebench/init_wizard.py` has no `schema` field. Every wizard-generated config defaults to customer360; schema selection today requires manual config editing. Adding Financial as a wizard choice requires first adding schema to the wizard.

- File: `src/lakebench/init_wizard.py` `WizardState` gains `schema: str = "customer360"` field.
- File: same file: new wizard step "Select workload schema" between recipe selection and scale selection. Options: `customer360` (Customer 360 analytics, default), `financial` (AML/fraud on ISO 20022). Description text for each.
- File: same file: when `schema == "financial"` selected, the review step additionally prompts for typology density (default 0.001), retention months (default 60), and whether to enable retention workload mode (default true).
- File: same file: the config YAML the wizard emits includes the `architecture.workload.schema` field and the per-domain block.
- File: `examples/hive-iceberg-spark-trino-financial.yaml` (new) is the reference config for the wizard's Financial output.
- Verification: `lakebench init --advanced` walks through the schema selection step; `lakebench init --schema financial` accepts a non-interactive flag and produces a valid config that runs end-to-end at scale=1.

### 2C.12 Journal events

**ENG-2C.12** (integrates with existing journal). Impact: Additive.

**Confirmed at review (V-21):** journal events are generic. `EventType.PIPELINE_STAGE` in `src/lakebench/journal/events.py` is a single event type parameterized by `stage_name`. New Financial stages need no new event types.

- File: `src/lakebench/spark/scripts/silver_build_financial.py` (and other Financial scripts) emit `EventType.PIPELINE_STAGE` events with `stage_name` values `financial_bronze_verify`, `financial_silver_build`, `financial_gold_finalize`, `financial_detection`, `financial_investigator`, `financial_replay`, `financial_reproduction`, `financial_scoring`.
- File: `src/lakebench/journal/journal.py` (event router) requires no changes; the generic PIPELINE_STAGE path handles new stage_name values.
- Verification: Financial run at scale=1 produces expected sequence of PIPELINE_STAGE events in the journal output, one per stage completion.

### 2C.13 Prerequisite checks

**ENG-2C.13** (integrates with the 7-phase run flow, satisfies REQ-A-04). Impact: Additive.

Phase 1 of the run flow is prerequisite detection (8-check engine per v1.3 CHANGELOG). Financial recipes need additional prerequisites: GraphFrames image variant available if declared, Splink installed if declared, sufficient snapshot retention configured if the recipe declares retention workloads.

- File: `src/lakebench/cli/_prerequisites.py` gains: `check_graphframes_image` (verifies the layered image tag exists when `spark_extensions.graphframes: true`), `check_splink_installed` (verifies Splink 4.0.x is importable in the Spark image when declared), `check_retention_compatible` (verifies `retention_workload: true` is compatible with the maintenance step configuration).
- Verification: prerequisites report all checks + / x for a Financial recipe at both a healthy and a deliberately misconfigured setup.

### 2C.14 DomainModule protocol implementation

**ENG-2C.14** (implements SOL-2B.8, aligns with v1.4 registry-driven deployment). Impact: Refactor. Optional in Financial v1; strongly recommended before KYC.

- File: `src/lakebench/modules/base.py` gains a `DomainModule` Protocol with methods:
  - `name` (schema string, e.g. `"customer360"`, `"financial"`)
  - `workload_schema_enum_value` (the `WorkloadSchema` enum member)
  - `dimensions(scale) -> ScaleDimensions`
  - `config_model` (per-domain config class)
  - `generator_class` (datagen `Generator` subclass)
  - `pipeline_scripts` (batch and sustained script names)
  - `benchmark_queries` (list of `BenchmarkQuery`)
  - `scorecard_block` (the domain's `ScorecardBlock` subclass)
  - `retention_workloads_declared: bool`
  - `ml_extension: bool`
- File: `src/lakebench/modules/registry.py` gains `_domains` registration and lookup.
- File: `src/lakebench/modules/domains/customer360/` new module implementing `Customer360Module` as a reference (extracts and consolidates the currently-scattered Customer360 references).
- File: `src/lakebench/modules/domains/financial/` new module implementing `FinancialModule` for Financial.
- Config resolution in `deploy/engine.py`, `benchmark/runner.py`, `reports/generator.py`, and `cli/_run.py` switches from hardcoded schema branching to `ModuleRegistry.get_domain(schema_type)` lookups.
- Verification: `pytest tests/test_characterization.py -k module_registry` passes; Customer360 and Financial recipes both execute end-to-end via the registry path.
- Not this task: registry-driven deployment for the four existing module types (that's the v1.4 target, orthogonal to this work).



**ENG-2C.9** (implements SOL-2B.7). Impact: Additive.

Depends on: ENG-2C.1 (silver tables must exist), ENG-2C.3 (silver_build must produce silver.transactions and silver.entities), ENG-2C.4.1 (W1 output feeds graph features into W12).

Files to create:
- `src/lakebench/spark/scripts/features_financial.py` -- feature engineering pipeline producing `silver.features` with columns per REQ-ML-01. Reads `silver.transactions`, `silver.entities`, `gold.synthetic_id_clusters` (W1 output). Uses windowed aggregations for velocity and diversity features; joins entity attributes; joins graph features. Output partitioned by `feature_window_end_date`.
- `src/lakebench/spark/scripts/train_financial.py` -- GBT training via `pyspark.ml.classification.GBTClassifier`. Reads `silver.features` and joins `bronze/manifest/manifest.parquet` for labels (typology-injected transactions are positives). Trains, evaluates, writes model artifact + metrics to `gold.models` and `gold.model_metrics` versioned by `(model_id, training_snapshot_ts)`.
- `src/lakebench/spark/scripts/infer_financial.py` -- batch inference. Reads model artifact by `model_id`, applies via Spark UDF or MLlib transformer, writes scored records to `gold.ml_scores` with reference to `model_id` and `inference_run_id`.
- `src/lakebench/spark/scripts/champion_challenger_financial.py` -- takes two model_ids and a test window; produces `gold.model_comparison` with delta metrics. **V-26 note:** the existing `lakebench compare` command (`cli/_compare.py`) already runs two configs sequentially with side-by-side scorecard output and supports `--format` and `--local`. Champion/challenger W15 is invoked via `lakebench compare config-champion.yaml config-challenger.yaml --skip-benchmark=false` when the two configs differ only in `financial.ml.model_id`; the `champion_challenger_financial.py` script is only needed when comparing two models within a single run's gold tables (post-hoc) rather than across two runs.
- `src/lakebench/spark/scripts/drift_financial.py` -- computes PSI (Population Stability Index) per feature between a reference distribution (training snapshot) and current distribution; produces `gold.feature_drift` with per-feature PSI values; flags features exceeding configurable threshold (default 0.2).

Files to modify:
- `src/lakebench/config/schema.py` -- add `WorkloadExtension.ml: bool = False` field to the workload config; when true, ML workloads run as part of the recipe.
- `src/lakebench/spark/scripts/score_financial.py` (from ENG-2C.5) -- extended to include ML metrics when `WorkloadExtension.ml` is true.
- `src/lakebench/reports/generator.py` -- extended to render ML scorecard block when present.

Config additions (`architecture.workload.extensions.ml`):
```yaml
architecture:
  workload:
    domain: financial
    extensions:
      ml: true
      ml_config:
        training_window_months: 12
        challenger_model_types: ["gbt", "rf"]
        drift_psi_threshold: 0.2
```

Verification:
- End-to-end ML pipeline (features → train → infer → drift) runs at scale=10 (~100 GB) within a documented time budget (TBD during first run; recorded in `docs/ml-benchmark-baselines.md`).
- Trained model achieves recall ≥ 0.6 against injected typologies at scale=10 (baseline sanity check; not a benchmark KPI).
- `gold.models` contains at least one row with valid `model_id`, `training_snapshot_ts`, `feature_hash`.
- `gold.ml_scores` contains at least one row per input transaction with valid `model_id` reference.
- Champion/challenger comparison produces two rows in `gold.model_comparison` with matching test window.
- Drift detection produces PSI values for all features; at least one feature under an intentionally drifted test corpus flags above threshold.

Not this task:
- Real-time / streaming inference (deferred to sustained mode ML integration, next release)
- Graph neural network models (deferred to GraphFrames + ML integration, requires separate compatibility work)
- Autoencoder-based anomaly detection (deferred)
- LLM-based transaction narrative analysis (out of scope for lakebench)
- Feature store as a separate service (silver.features table is the feature store for v1)

### 2C.16 Data realism specification

**ENG-2C.16** (satisfies REQ-F-*, extends ENG-2C.2). Impact: Additive (specification detail, no new code beyond ENG-2C.2).

The generated data has to look like real bank data to a practitioner. Wrong-shape data destroys credibility on inspection. These are the concrete generation rules.

**Identifiers (all deterministically generated from seed):**

| Identifier | Generation rule | Validity |
|---|---|---|
| LEI (Legal Entity Identifier) | 20-char, prefix `LB` + 16 hex chars + ISO 17442 mod-97 check digits | Structurally valid; **not** on the GLEIF registry; documented as synthetic |
| BIC (Bank Identifier Code) | 8 or 11 char, prefix `LB` + 4-char country + 2-char location + optional 3-char branch; drawn from a fixed 500-BIC synthetic bank set | Structurally valid; documented as synthetic |
| IBAN | Country-specific format with **valid mod-97 check digits**; account portion generated from `hash(seed, account_id)`; country weighted by config's `currency_mix` | Structurally valid; passes standard IBAN validators |
| Account number (non-IBAN countries) | 10-12 digit numeric; deterministic from `hash(seed, account_id)` | Structurally valid |
| UETR | UUIDv4 generated from `hash(seed, txn_id)` (deterministic v4 shape) | Structurally valid; unique per transaction; stable across runs with same seed |
| End-to-End ID | `E2E-{txn_id_hex}` | Structurally valid |
| Country codes | ISO 3166-1 alpha-2 codes only | Real codes; realistic weighting |
| Currency codes | ISO 4217 alpha-3 codes only | Real codes; per-country weighting |
| Purpose codes | ISO 20022 ExternalPurpose1Code enum (SALA, COMC, GDDS, etc.) | Real codes; realistic distribution |

**Addresses:** structured per `PostalAddress24`. Country from ISO 3166. Town names from a synthetic 10K city list per country (deterministic hash of `(country, hash(seed, address_id))`). Street names generated from templates (`{Number} {AdjectiveList} {StreetType}`). No PII lookup against any real address database. Documented as synthetic.

**Currency mix (default):** USD 40%, EUR 30%, GBP 10%, JPY 5%, CHF 5%, other 10%. Configurable per run. Cross-border ratio (default 20%) determines what fraction of transactions have different debtor and creditor country codes.

**Exchange rates:** fixed table shipped with datagen. Rates approximate real 2026 mid-market rates within ±5%. Not time-varying by default; a `--realistic-fx` flag enables monthly rate walks (deferred to a follow-on release).

**Holiday calendars:** ISO country-based. Ships with US federal, UK bank, ECB TARGET2, JP, CH, SG calendars. Weekend suppression for wire transactions; retail (card) transactions have no weekend suppression. Config field `holiday_calendar: str = "iso"` selects the calendar set; `"none"` disables holiday behaviour.

**Time zone:** all timestamps stored as UTC. Business-hours skew (3x local business-day peak) applied per debtor country using its IANA timezone. Salary spikes (5x on 1st, 15th, 25th) applied per debtor country's local calendar.

**Sanctions and PEP lists:** synthetic, not real. Sanctions list: 0.05% of generated entities flagged with `sanctions_status = 'SDN'`, drawn deterministically from `hash(seed, entity_id)`. PEP list: 0.02% flagged with `pep_status = true`. Config field `sanctions_list: str = "generated"` in v1; a `"real"` option is deferred (real lists like OFAC SDN require license clearance for redistribution).

**Amounts:** log-normal distribution per transaction type, calibrated to plausible bands. Wires: median USD 5,000, p95 USD 500,000, p99 USD 5M. Cards: median USD 45, p95 USD 500. ACH: median USD 200, p95 USD 3,000. Structuring typology injections deliberately generate amounts near the CTR threshold (USD 9,500-9,999) as expected.

**Entity types:** 55% Person, 40% Company, 5% Financial Institution. Configurable.

**Realism disclaimer** printed at datagen start: "The dataset generated is entirely synthetic. LEIs are not registered with GLEIF. BICs are not registered with SWIFT. IBANs are structurally valid but do not correspond to real accounts. Entity names, addresses, sanctions flags, and PEP flags are generated deterministically from a seed and do not correspond to real people, companies, or sanctioned entities."

### 2C.17 Failure recovery

**ENG-2C.17** (satisfies REQ-A-*). Impact: Additive.

The spec's happy path is well-defined; the unhappy path (silver_build fails at hour 8, cycle 3 of 12 fails, sustained ingest crashes) needs explicit handling. Financial recipes at scale=100+ will have long runtimes; failure recovery is a real requirement.

**Datagen failure.** Existing `--resume` flag (v1.1.0) works for Financial without change; deterministic file assignment means resumed pods produce the same output as originally-planned pods. New addition: `--verify-resumed` flag runs a checksum pass over already-written files before continuing.

**Bronze verify failure.** Additive to existing behaviour. If bronze_verify_financial fails, the SparkApplication is retried once by the operator; on second failure, run aborts with a diagnostic showing which pacs008 field triggered the schema validation error.

**Silver build failure at scale.** New: `silver_build_financial` writes a per-partition checkpoint (`bronze/checkpoints/silver_build/{run_id}/{partition_date}`) after each partition completes. On restart, the driver reads checkpoints and skips completed partitions. Resume behaviour is idempotent because Iceberg supports position-file rewrite. Config field `pipeline.silver_checkpoint: bool = true` (default on for Financial, off for Customer360 to preserve existing behaviour).

**Multi-cycle failure at cycle N.** New: cycle progression is journaled per cycle to `bronze/cycle_state/{run_id}`. On resume with `--resume-cycle N`, the pipeline reads cycle_state to determine what has completed and picks up from the next stage of cycle N. Cycles 1..N-1 are treated as immutable. If cycle N was mid-silver_build, the silver checkpoint mechanism above handles resume.

**Detection workload failure.** Detection workloads (W1-W5) are idempotent by design (deterministic output for given input, see REQ-D-03). Failure mode: kill and re-run. Alerts written to gold.alerts are keyed by `(rule_id, rule_version, related_txn_id, run_id)`; re-run writes to a new run_id and does not corrupt prior state.

**Sustained ingest failure.** Spark Structured Streaming checkpointing (already in bronze_ingest_financial, from ENG-2C.4.11) handles crash recovery. On restart, streaming resumes from the last successful commit. New addition: silver_stream_financial's counterparty_edges merge is idempotent (upsert semantics), so re-processed micro-batches produce correct final state.

**Time-travel unavailable at reproduction time.** W10 (reproduce_financial.py) may find that a snapshot has been expired by an unrelated maintenance run. New behaviour: reproduction script checks snapshot availability first, produces a clear diagnostic ("snapshot X was expired at time Y by maintenance operation Z; enable retention_workload to preserve"), records a `timetravel_reproduction_success = false` metric row rather than crashing.

**Cleanup after failure.** `lakebench clean --run-id <id>` drops any temporary namespaces, orphan snapshots, and checkpoint files from a specific failed run without affecting other in-flight runs.

Not this task: automatic retry policies beyond Spark operator defaults (deferred), cross-region disaster recovery (out of scope for benchmark), snapshot backup before maintenance (out of scope).

### 2C.18 Existing command compatibility

**ENG-2C.18** (satisfies REQ-M-01 backward compat, satisfies REQ-A-04). Impact: Extension.

Every existing CLI command needs to work correctly with Financial recipes, or explicitly refuse with a clear reason.

| Command | Behaviour for Financial | Change required |
|---|---|---|
| `lakebench init` | Prompts for schema; Financial is a valid choice (ENG-2C.11) | Extension per ENG-2C.11 |
| `lakebench deploy` | Deploys same infrastructure as customer360; layered images if GraphFrames or Splink declared | Extension: layered image selection in `deploy/engine.py` |
| `lakebench generate` | Invokes dispatched generator (ENG-2C.2 PR-C); progress bar shows pods and files as before | Additive: pass `WORKLOAD_SCHEMA` env; existing progress bar works unchanged |
| `lakebench run` | Full 7-phase flow with Financial-specific pipeline stages (D-01) | Extension: schema-aware phase 4 dispatch |
| `lakebench clean` | Drops Financial tables in addition to Customer360; respects retention_workload flag (does not expire snapshots newer than retention window) | Extension: table list is schema-aware |
| `lakebench destroy` | Cleans everything the deploy created; **destroy is unaware of retention_workload** by default because destroy is total-teardown; a new `--preserve-snapshots` flag opts out for forensic scenarios | Extension: new flag |
| `lakebench compare` | Handles two configs with same or different schemas; when schemas differ, the scorecard shows only scores present in both (intersection) with a warning | Extension: scorecard renderer already keyed by domain per ENG-2C.5; compare logic already schema-neutral (V-26) |
| `lakebench query --sql-file` | Works unchanged; user's SQL runs against whatever tables exist | No change |
| `lakebench results` | Displays Financial scorecard when the last run was Financial; existing customer360 display unchanged | Extension: renderer domain-aware |
| `lakebench config show` | Prints the FinancialConfig block with source annotations | Extension: config schema knows new fields |
| `lakebench config validate` | Validates FinancialConfig fields; catches known-bad combinations (e.g. `mode: sustained` with `cycles: 12` is invalid) | Additive: new validation rules |
| `lakebench config recommend` | Suggests scale and mode for Financial; recommends `retention_workload: true` when W8 or W10 workloads are declared | Extension: recommendation logic domain-aware |
| `lakebench config upgrade` | Handles v1.2 → v1.3 flat format for Financial configs (config schema v3?) | Deferred until config schema versioning event |
| `lakebench financial-replay` | New subcommand for W8 (ENG-2C.4.8) | Additive |

Any command that fails against a Financial config with an unhelpful error before this work is a bug. Compatibility test suite: `tests/test_financial_command_compat.py` runs each command against a scale=1 Financial config in local mode and checks for successful exit or expected-refusal exit.

### 2C.19 Observability integration

**ENG-2C.19** (satisfies REQ-M-*, extends existing observability). Impact: Additive.

lakebench-k8s ships three Grafana dashboards today (Pipeline Overview, Spark Detail, Storage I/O) via `kube-prometheus-stack`. Financial recipes benefit from a domain-specific dashboard for long-running scale=100+ runs.

**New Grafana dashboard: FinServ-Crime.** Panels:
- Detection workload progress: per-workload rows scanned, motif matches found, alerts written, wall-clock elapsed
- Injected typology count vs detected typology count (from `manifest.parquet` and `gold.alerts`)
- Ingest lag (sustained mode): time from file drop to bronze commit, bronze to silver commit
- Snapshot count over time per table (for retention workload monitoring; sudden drops indicate unintended expiry)
- Alert cadence per rule per cycle (multi-cycle mode)
- Compaction cost per cycle (maintenance metrics)
- Concurrent load conditions: which workloads are running now, current load_condition tag

Dashboard JSON in `src/lakebench/templates/grafana/dashboards/financial.json`. Auto-installed when `observability.enabled: true` AND `architecture.workload.schema: financial`.

**New Prometheus metrics** (added to the platform_collector):
- `lb_financial_detection_alerts_total{rule_id}` counter
- `lb_financial_ingest_lag_seconds{stage}` gauge (bronze, silver)
- `lb_financial_snapshot_count{table}` gauge
- `lb_financial_typology_recall{typology_type}` gauge (updated by score_financial.py)
- `lb_financial_concurrent_workloads` gauge

**Log correlation.** Financial script log lines include `run_id`, `cycle_index` (when multi-cycle), and `load_condition` (when concurrent) as structured fields. Enables grouping in Grafana Loki when the observability stack includes it.

Not this task: alerting rules on Prometheus metrics (deferred; benchmark runs don't need PagerDuty), tracing via OpenTelemetry (deferred).

### 2C.20 Test strategy

**ENG-2C.20** (satisfies REQ-A-03, extends existing test coverage). Impact: Additive.

Concrete test surface for Financial. The existing test suite has ~50 files; Financial adds the following.

**Unit tests (fast, run in every CI job):**
- `tests/test_financial_dimensions.py` -- dimensions function edge cases, boundary scale factors
- `tests/test_financial_config.py` -- config validation, FinancialConfig defaults, invalid combinations
- `tests/test_financial_generator.py` -- deterministic output (same seed → same bytes), scale boundary correctness, typology injection density
- `tests/test_financial_pacs008_schema.py` -- bronze schema matches ISO 20022 XSD (mechanical check)
- `tests/test_financial_typologies.py` -- each of the 8 AMLworld primitives produces valid graph structure
- `tests/test_financial_scoring.py` -- recall calculation correctness on synthetic input

**Integration tests (slower, run on merge to main):**
- `tests/test_financial_pipeline.py` -- end-to-end batch pipeline at scale=1 on one recipe (`hive-iceberg-spark-none-financial`)
- `tests/test_financial_workloads.py` -- each of W1-W11 produces expected output shape at scale=1
- `tests/test_financial_command_compat.py` -- every CLI command works or refuses cleanly (see ENG-2C.18)
- `tests/test_financial_multi_cycle.py` -- 3-cycle run at scale=1 produces cycle-progression scorecard

**E2E tests (slow, run against lab cluster, tagged `extended`):**
- Extension of `tests/test_e2e.py::TestExtendedScales` to include Financial at scale 1, 10, 100
- `tests/test_e2e.py::TestFinancialSustained` -- sustained mode at scale=10 for 30 minutes

**Stress tests (very slow, tagged `stress`, on-demand):**
- Scale 1000 and 10000 Financial runs; verify successful completion and scorecard shape

**Regression tests (mandatory for every refactor PR):**
- `tests/test_customer360_regression.py` -- byte-identical scorecard, byte-identical Parquet output at scale=1 for the `hive-iceberg-spark-none` recipe

Total new tests: approximately 30 test files, 200-300 test cases. Total added CI time at each PR: fast tests <2 minutes, integration tests <15 minutes on the standard CI runner.

### 2C.21 Executor profiles for Financial stages

**ENG-2C.21** (extends `docs/architecture.md` Spark Executor Profiles table). Impact: Additive.

Existing Customer360 profiles (from `docs/architecture.md:176`):

| Stage | Cores | Memory | Overhead | Scratch PVC |
|---|---|---|---|---|
| `bronze-verify` | 2 | 4g | 2g | 50Gi |
| `silver-build` | 4 | 48g | 12g | 150Gi |
| `gold-finalize` | 4 | 32g | 8g | 100Gi |

Financial stage profiles (initial estimates; to be validated by ENG-2C.10 autosizer calibration):

| Stage | Cores | Memory | Overhead | Scratch PVC | Rationale |
|---|---|---|---|---|---|
| `bronze-verify-financial` | 2 | 4g | 2g | 50Gi | Same as customer360; identical schema-validation workload |
| `silver-build-financial` | 4 | 48g | 12g | 200Gi | Pacs.008 rows are wider; more spill during joins with entities |
| `gold-finalize-financial` | 4 | 32g | 8g | 100Gi | Same as customer360; aggregation workload |
| `workload-w1-cc` (connected components) | 4 | 64g | 16g | 300Gi | GraphFrames CC has heavy shuffle; larger memory and scratch |
| `workload-w2-motif` (structuring) | 4 | 48g | 12g | 250Gi | Motif finding on transaction edges; per-cycle bounded |
| `workload-w3-motif` (round-trip) | 4 | 48g | 12g | 250Gi | Same as W2 |
| `workload-w4-pregel` (risk propagation) | 4 | 64g | 16g | 300Gi | Pregel message passing has memory pressure at scale |
| `workload-w5-splink` (entity resolution) | 4 | 32g | 8g | 200Gi | Splink pairwise comparison; blocking helps |
| `workload-w6-w7` (investigator queries) | 2 | 8g | 2g | 20Gi | Per-query small; run in the query engine (Trino/Spark Thrift) not standalone SparkApp |
| `bronze-ingest-financial` | 2 | 16g | 4g | 50Gi | Structured Streaming; lower resource than batch |
| `silver-stream-financial` | 2 | 24g | 6g | 100Gi | Streaming merge into counterparty_edges |
| `gold-refresh-financial` | 2 | 16g | 4g | 50Gi | Periodic aggregate recompute |

These are starting values; the autosizer (ENG-2C.10) uses them as defaults and adjusts based on scale factor and observed spill/OOM behaviour in future releases.

### 2C.22 Wall-clock and cluster-size estimates

**ENG-2C.22** (extends `docs/data-generation.md` scale table). Impact: Additive.

Estimates for a reference lab cluster (10 nodes, 32 cores/node, 256 GB/node, 100 Gbps network, S3-compatible storage with 10 GB/s aggregate throughput). Numbers are order-of-magnitude, to be refined after first end-to-end runs.

| Scale | Bronze size | Customer360 TTV | Financial TTV (batch) | Financial detection (W1-W5) | Financial full run |
|---|---|---|---|---|---|
| 1 | ~11 GB | ~5 min | ~10 min | ~5 min | ~20 min |
| 10 | ~110 GB | ~30 min | ~1 hour | ~30 min | ~2 hours |
| 100 | ~1.1 TB | ~2-3 hours | ~4-6 hours | ~2-3 hours | ~8-12 hours |
| 1000 | ~11 TB | ~1 day | ~2 days | ~1 day | ~3-4 days |
| 10000 | ~110 TB | requires larger cluster | requires larger cluster (est. 20+ nodes) | est. 1 week | est. 2 weeks |

Multi-cycle multiplier: N cycles adds roughly 60-80% per additional cycle for Financial (cycle 1 is full overwrite; cycles 2-N are incremental). A 12-cycle scale=100 run is approximately 4x the single-cycle wall-clock, not 12x, because incremental cycles are much smaller.

Sustained mode is bounded by the measurement window duration (default 30 minutes plus 5 minutes warmup), not by scale. Sustained-mode TTV concept does not apply.

Storage requirement per scale (curated Parquet, silver + gold + snapshots for 60-month retention):

| Scale | Silver | Gold | Snapshots (60 months) | Total |
|---|---|---|---|---|
| 1 | 8 GB | 1 GB | 3 GB | ~12 GB |
| 100 | 800 GB | 100 GB | 300 GB | ~1.2 TB |
| 10000 | 80 TB | 10 TB | 30 TB | ~120 TB |

The cluster needs enough scratch PVC capacity for the largest concurrent Spark job. For scale=100 Financial full run with concurrent workloads, that is: silver-build (200 GiB per executor × 8 executors = 1.6 TiB) + concurrent W4 (300 GiB × 8 = 2.4 TiB) = ~4 TiB scratch capacity required during the peak. Cluster storage class must provision at least this much fast local scratch.

Estimates published in `docs/financial-benchmark-baselines.md` and revised after first-run measurements.

### 2C.23 Baseline discovery workstream engineering

**ENG-2C.23** (satisfies parent solution doc Workstream 8, feeds REQ-S-01 credibility). Impact: Additive (workstream, not code).

Real bank practitioners have baselines the benchmark should be anchored against. Publishing FinServ-Crime numbers without checking against reality produces credible-looking but disconnected results. This ENG block is the workstream that produces those anchors.

**Deliverables:**
- Interview guide document (`docs/financial-baseline-discovery.md`) with 30-40 structured questions covering: current AML detection latency and cost, current investigator query patterns and typical response-time SLAs, historical replay frequency and depth requirements, current time-travel/reproducibility mechanisms, sustained ingest volumes and peak rates, retention policy specifics (EU AMLR compliance depth), compaction and maintenance windows, alert volumes and typology distributions, false-positive rates.
- 8-12 practitioner interviews (60 minutes each), recorded and transcribed with consent
- Anonymised summary published as an appendix to `docs/financial-benchmark-baselines.md` correlating benchmark scale points to real bank tiers
- Recalibration of REQ-S-01 tier-1 target if the interviews reveal that 100 TB is materially wrong
- Recalibration of default config values (typology density, correspondent chain ratio, sustained ingest TPS) if interviews reveal defaults are materially unrealistic

**Timeline:** first six interviews and initial calibration before FinServ-Crime v1 release; remaining interviews and refinement over the following quarter. Interview transcripts are not shipped with the OSS repo (privacy); the anonymised summary is.

Not this task: automated ingestion of practitioner-provided real data as benchmark input (deferred; scope creep and data-sharing complications).



## 3.1 Sustained-mode concurrent workload orchestration

Extend sustained mode to accept a `workloads:` list in config that submits additional Spark or Trino jobs on schedules alongside streaming ingest. Benefits every domain, not just Financial.

## 3.2 Retention policy override for time-travel

Iceberg snapshot expiry override at recipe level so 60 months of snapshots survive maintenance. Default expiry increased from 7 days to 66 months when `retention_workload: true`.

## 3.3 Manifest-based recall metric infrastructure

Scorecard infrastructure that reads a ground-truth manifest and computes recall for detection workloads. Generalisable beyond Financial.

## 3.4 Domain-aware scorecard renderer

Rendering pipeline in `reports/generator.py` becomes domain-aware, with pluggable scorecard blocks per domain. Enables future domains (KYC, cybersecurity, trading surveillance) without touching the core renderer.

---

# Part 4. Not in this release

- KYC schema and workloads (deferred to follow-on release with equivalent scope)
- Kafka ingest path (separate `-kafka` recipe planned, not v1)
- Address validation computer-vision workload (dropped; out of scope for lakehouse benchmarking)
- OpenSharing extension for cross-institution shares (separate scope, tracks parent solution doc Part 4)
- RAPIDS / GPU workloads (parent solution doc Phase 2)
- Streaming beyond file-drop pattern (parent solution doc Phase 3)
- FIBO semantic layer (deferred; not required for v1 workloads)
- Delta Lake + graph workload compatibility (deferred; GraphFrames + Delta compatibility path uncertain)

---

# Part 5. Workload/Component Addition standard (WCA)

Version 1. This Part defines the process, deliverables, and quality bar for adding a workload, component, or domain to lakebench-k8s via OSS contribution after this release. Written to make future additions reviewable, comparable, and mergeable without re-litigating architecture with every new contribution. Modelled on Kubernetes Enhancement Proposals (KEP) and Python Enhancement Proposals (PEP): a lightweight structured proposal, an accept/reject decision, then delivery via PR series.

**Not this Part.** Part 5 defines the standard itself. It does not modify lakebench-k8s runtime code. Applying the standard to a specific new addition happens through a WCA proposal issue and its resulting PR series, not through this Part.

## 5.1 Scope

Applies to additions of a new:
- **Type 1**: workload within an existing domain (e.g. adding W12 to FinServ-Crime)
- **Type 2**: workload domain, including datagen schema and full workload set (e.g. adding KYC)
- **Type 3**: component: catalog, table format, query engine, processor, storage backend (e.g. adding StarRocks as a query engine)
- **Type 4**: scale profile within an existing domain (e.g. adding a "regional bank" profile)
- **Type 5**: scoring metric or scorecard block (e.g. adding cost-per-query)

Does not apply to bug fixes, documentation improvements, dependency version bumps within the supported range, or CI improvements. Those follow standard PR review.

## 5.2 Definitions

- **Domain**: a workload domain like Customer360 or FinServ-Crime. Comprises a datagen schema, a dimensions function, a set of workloads, and a scorecard block.
- **Workload**: a specific analytical operation. Named `W-N` within a domain.
- **Component**: a swappable piece of the lakehouse stack. Categories: catalog (Hive, Polaris), table format (Iceberg, Delta), query engine (Trino, Spark Thrift, DuckDB), processor engine (Spark), storage backend (S3, MinIO, FlashBlade, Garage).
- **Recipe**: a validated combination of components. Named `{catalog}-{format}-{processor}-{engine}`.
- **Scale profile**: the mapping from abstract scale factor to domain dimensions.
- **Scorecard block**: a named set of related metrics rendering as a group in the report output.

## 5.3 Proposal process

1. Contributor opens a GitHub issue using the WCA proposal template (section 5.4).
2. Proposal states: title, contribution type, motivation, requirements (REQ format per Part 0 conventions), non-goals, deliverables checklist, reviewer bandwidth.
3. Maintainers triage within two weeks: accept, request changes, or reject with documented reason.
4. On acceptance: implementation via PR series, one PR per DAG-independent unit.
5. Each PR references the accepted proposal issue, includes its portion of the deliverables checklist, and passes CI.
6. Proposal closes when all deliverables are complete and merged.

Rejected proposals include a rationale. Common reasons: unclear motivation, missing requirements, deliverables incompatible with existing architecture, no committed reviewer or contributor bandwidth.

## 5.4 Proposal template

```markdown
# WCA-XXX: <Title>

## Contribution type
[ ] Type 1: Workload within existing domain
[ ] Type 2: New domain
[ ] Type 3: New component
[ ] Type 4: New scale profile
[ ] Type 5: New scoring metric

## Motivation
<Why this exists. What breaks or is missing today. Who benefits.>

## Requirements
<REQ-*-* format per Part 0. Each REQ has statement, acceptance criterion, rationale.>

## Non-goals
<Explicit list of what this proposal does not cover.>

## Impact classification
<Additive / Extension / Refactor / Breaking, with reasoning.>

## Deliverables checklist
<Copied from the relevant contribution type section (5.5), adjusted for specifics.>

## Reviewer bandwidth
<Named maintainers who have committed to review.>

## Timeline
<Rough estimate: PR count, expected wall-clock to merge.>

## Discovery findings
<For Type 2 and Type 3: results of the Part 0 discovery/verify/assess phase against the current codebase.>
```

## 5.5 Contribution type deliverables

Each contribution type has a minimum deliverables set. PRs that do not include the full set are held in review.

### 5.5.1 Type 1: Workload within an existing domain

- Workload implementation (SQL, Spark script, or query file) under the domain's script directory
- Test cases in `tests/test_<domain>_workloads.py`:
  - Determinism (run twice, output matches after canonical sort)
  - Expected output shape (columns, row count reasonable for scale=1)
  - Performance sanity (executes within budget at scale=1)
- Scorecard integration if the workload produces a new metric
- Documentation: extend the domain's workload table in `docs/` and in the LakeBench Next spec

### 5.5.2 Type 2: New domain

- Dimensions function in `src/lakebench/config/scale.py` with unit tests
- Schema enum value in `src/lakebench/config/schema.py:WorkloadSchema`
- Datagen schema module and generator class added to `src/lakebench/templates/datagen/configmap.yaml.j2`
- `datagen/generate.py` updated to dispatch to the new generator (if dispatch pattern not already in place, dispatch refactor is a separate prerequisite PR)
- Pipeline scripts:
  - Batch mode: `bronze_verify_<domain>.py`, `silver_build_<domain>.py`, `gold_finalize_<domain>.py`
  - Sustained mode: `bronze_ingest_<domain>.py`, `silver_stream_<domain>.py`, `gold_refresh_<domain>.py`
- Full workload set: minimum three workloads with Type 1 rigour
- Scoring module in `src/lakebench/spark/scripts/score_<domain>.py`
- Recipe registration in `src/lakebench/config/recipes.py`
- Configuration additions in `src/lakebench/config/schema.py`
- End-to-end integration test at scale=1 for at least one recipe
- Documentation:
  - Domain spec addition (in the LakeBench Next spec or a domain spec doc if large enough to warrant one) following REQ/SOL/ENG structure
  - `docs/data-generation.md` extended with domain dimensions
  - `docs/recipes.md` extended with recipe combinations
  - Example config in `examples/<domain>-<recipe>.yaml`

### 5.5.3 Type 3: New component

- Deploy module in `src/lakebench/deploy/<component>.py`
- Kubernetes templates in `src/lakebench/templates/<component>/`
- Integration in `src/lakebench/deploy/engine.py` orchestration
- Config schema addition in `src/lakebench/config/schema.py`
- Recipe combinations in `examples/` covering existing domains that support the new component
- Component documentation in `docs/component-<name>.md`
- Compatibility matrix update in `docs/compatibility-matrix.md`
- Integration test proving the component deploys and interoperates with existing components

### 5.5.4 Type 4: New scale profile within an existing domain

- Named profile function in `config/scale.py`
- Config schema addition to select the profile
- Documentation of profile intent and dimensions in the domain's `docs/`
- Test verifying dimensions match the published table

Typically a single PR.

### 5.5.5 Type 5: New scoring metric

- Scoring computation in the relevant score script or a new one
- Metrics schema extension in `src/lakebench/metrics/collector.py`
- Report renderer extension in `src/lakebench/reports/generator.py`
- Provenance: metric writes to `metrics.parquet` with full context
- Documentation: metric definition, computation method, interpretation

## 5.6 Testing requirements

Every WCA contribution includes:

- **Unit tests** for pure functions (config, dimensions, schema validation)
- **Integration tests** using `tests/test_e2e.py` patterns for at least one recipe at scale=1
- **Determinism tests** for new workloads: run twice, compare output byte-for-byte after canonical sort
- **Backward compatibility tests** for Extension, Refactor, or Breaking impacts: existing customer360 recipes produce byte-identical scorecards to a reference run
- **CI green** across the full supported Spark version matrix

## 5.7 Style conventions

- File naming: `<phase>_<domain>_<variant>.py` for Spark scripts (e.g. `silver_build_financial.py`, `silver_build_financial_delta.py`)
- Config schema: extend existing schemas rather than fork; enums for choice fields; document defaults inline
- SQL: ANSI-compliant unless explicitly Spark-only or Trino-only (both documented)
- Python: existing lakebench style (Black, isort, mypy strict for new code)
- Docs: follow existing `docs/` structure; add a new file for major additions, extend existing files for minor
- REQ/SOL/ENG structure per Part 0 conventions for any new specification

## 5.8 Backward compatibility rules

- Existing recipes must continue to work with no config changes across a minor version bump
- New config fields must have documented defaults
- Existing scorecard rows preserved; new domains add rows, do not modify existing ones
- Existing table formats and catalogs remain functional
- Spark version increments follow the versioning policy in Part 1

Breaking changes require a major version bump and a migration note in `CHANGELOG.md`.

## 5.9 Impact classification

Every ENG block in a WCA proposal declares one of the four classifications from Part 0.5 (Additive / Extension / Refactor / Breaking) with the same merge-risk semantics.

## 5.10 Review criteria

Maintainers assess:

- Does the contribution match its type's deliverables?
- Are requirements testable (pass/fail acceptance criteria)?
- Does the engineering follow existing patterns, or is deviation justified?
- Do tests cover the acceptance criteria?
- Is documentation sufficient for a new user to run the addition?
- Backward compatibility preserved, or documented as breaking?
- Impact classification correct?
- Discovery findings (Type 2 and Type 3) accurate against the current codebase?

## 5.11 LLM-agent contributions

Contributions authored in part or whole by LLM code agents (Claude Code, Cursor, Copilot, etc.) are welcome. Additional requirements for agent-authored PRs:

- PR description states the agent used and the human review process applied
- Each PR is bounded to one ENG block or one deliverable from the type's checklist
- Human contributor takes responsibility for the correctness of the merged code
- Agent output is reviewed against the same style and testing bar as human-authored code

Part 0 (Implementation Preamble) is written to be agent-executable and provides the pattern for agent-friendly specifications. New WCA proposals with Type 2 or Type 3 scope are strongly encouraged to include a Part-0-style implementation preamble in their spec addition.

## 5.12 Reference implementations

- **Type 2 reference**: FinServ-Crime domain in Part 2. Follow its REQ/SOL/ENG structure for new domain proposals.
- **Type 3 reference (version bump)**: Spark 4.2 baseline upgrade in Part 1.
- **Type 3 reference (new engine)**: Trino as query engine in the current lakebench-k8s codebase (pre-existing; use for pattern reference).
- **Mature Type 2 reference**: Customer360 domain in current lakebench-k8s. What a landed Type 2 contribution looks like after full delivery.

## 5.13 Governance

- This standard is versioned. Changes to the standard itself follow the same PR process, with a required maintainer supermajority for approval (2 of 3 named maintainers).
- The standard is reviewed annually.
- WCA proposal numbers are assigned sequentially by maintainers on acceptance; rejected proposals do not consume numbers.
- Superseded WCA proposals are marked as such but retained in the repository for provenance.

---

# Part 6. PR sequencing and delivery DAG

Concrete ordering of the implementation work. Every ENG block corresponds to one or more PRs. This Part describes the dependency graph, the recommended landing order, and the parallelisation opportunities.

## 6.1 Sequencing constraints

**Hard dependencies (must land before):**

| PR | Depends on | Reason |
|---|---|---|
| ENG-2C.2 PR-B (dispatch) | ENG-2C.2 PR-A (extract class) | Cannot dispatch without a class to dispatch to |
| ENG-2C.2 PR-C (Financial generator) | ENG-2C.2 PR-B (dispatch) | Generator registers into dispatch map |
| ENG-2C.4.pre PR-Q-B (Financial queries) | ENG-2C.4.pre PR-Q-A (registry) | Queries register into map |
| ENG-2C.5 PR-R-B (Financial scorecard) | ENG-2C.5 PR-R-A (ScorecardBlock protocol) | Scorecard class implements protocol |
| ENG-2C.4.1-5 (detection workloads) | ENG-2C.1 (data model), ENG-2C.3 (pipeline scripts), ENG-U-02 (GraphFrames image), ENG-U-03 (Splink image) | Workloads read silver.transactions and silver.entities; need graph libs |
| ENG-2C.4.6-7 (investigator queries) | ENG-2C.4.pre PR-Q-A (registry) | Queries registered per domain |
| ENG-2C.4.8 (W8 replay) | ENG-2C.4.1-4 (detection modules to import), ENG-R-05 (retention override) | Replay imports rule modules; needs snapshots |
| ENG-2C.4.10 (W10 reproduction) | ENG-2C.4.1-4, ENG-R-05, ENG-2C.4.9 (writeback with snapshot_id) | Reproduces alerts written by writeback |
| ENG-2C.4.11 (W11 sustained ingest) | ENG-2C.3 (silver_stream_financial), ENG-U-01 (Spark 4.2 for Structured Streaming behaviour) | Streaming scripts depend on sustained pipeline |
| ENG-2C.9 (ML extension) | ENG-2C.4.1 (W1 output feeds features), ENG-2C.3, ENG-2C.5 | ML depends on graph features and scoring plumbing |
| ENG-2C.10 (autosizer) | ENG-U-05 (schema enum), ENG-2C.21 (profiles table) | Autosizer needs new schema value and profile values |
| ENG-2C.11 (wizard) | ENG-U-05 (schema enum) | Wizard offers new schema choice |
| ENG-2C.14 (DomainModule) | ENG-U-05, ENG-2C.2 PR-B, ENG-2C.4.pre PR-Q-A, ENG-2C.5 PR-R-A | Domain module aggregates the refactored surfaces |
| ENG-M-05 (StageMetrics field) | none | Standalone data model addition |
| ENG-R-05 (maintenance override) | ENG-U-05 (schema field), pipeline config field | Consults schema type to apply override |

**Soft dependencies (recommended before):** ENG-U-01 (Spark 4.2 default) before any FinFraud CI runs, so the target Spark version is stable across the work.

## 6.2 Recommended landing order

Grouped into phases; PRs within a phase can land in parallel.

**Phase A: Baseline (foundation, no FinServ-Crime content yet)**
- A1. ENG-U-01 Spark 4.2 default
- A2. ENG-U-02 GraphFrames optional image
- A3. ENG-U-03 Splink optional image
- A4. ENG-U-04 Iceberg v3 config
- A5. ENG-U-06 distribution mode helper
- A6. ENG-M-05 StageMetrics.load_condition field

Phase A can land as five parallel PRs (A2 through A6 are independent of A1) with the Spark bump landing last after CI validates the matrix.

**Phase B: Refactors (backwards-compat required)**
- B1. ENG-2C.2 PR-A: extract Customer360Generator (byte-identical output required)
- B2. ENG-2C.2 PR-B: introduce Generator protocol and dispatch (behaviour unchanged)
- B3. ENG-2C.4.pre PR-Q-A: BENCHMARK_QUERIES_BY_DOMAIN restructure (byte-identical scorecard required)
- B4. ENG-2C.5 PR-R-A: introduce ScorecardBlock protocol (byte-identical rendering required)

Phase B is sequential within each refactor family (B1→B2), parallel across families (B1, B3, B4 all independent). Recommended: land B1-B4 in a single sprint with strict CI enforcement of byte-equivalence.

**Phase C: Financial data foundation**
- C1. ENG-U-05 replace financial_dimensions stub, add FinancialConfig
- C2. ENG-2C.1 bronze/silver/gold DDL (new tables, no existing touch)
- C3. ENG-2C.2 PR-C: FinancialGenerator class (depends on B1, B2)
- C4. ENG-2C.16 data realism specification (documentation delivery, sits alongside C3)
- C5. ENG-2C.21 executor profiles for Financial stages (adds to docs)

**Phase D: Financial pipeline and maintenance**
- D1. ENG-2C.3 batch pipeline scripts (bronze_verify, silver_build, gold_finalize for Financial)
- D2. ENG-2C.3 sustained pipeline scripts (bronze_ingest, silver_stream, gold_refresh for Financial)
- D3. ENG-R-05 maintenance retention override (depends on C1)
- D4. ENG-2C.10 autosizer schema branch (depends on C1, C5)
- D5. ENG-2C.15 multi-cycle batch integration for Financial (depends on D1)

Phase C and Phase D can overlap: C1, C2, C4, C5 can land while D1, D3, D4 are being written.

**Phase E: Financial workloads**
- E1. ENG-2C.4.1 W1 connected components (depends on D1)
- E2. ENG-2C.4.2 W2 structuring motif (depends on D1)
- E3. ENG-2C.4.3 W3 round-tripping motif (depends on D1)
- E4. ENG-2C.4.4 W4 Pregel risk propagation (depends on D1)
- E5. ENG-2C.4.5 W5 Splink entity resolution (depends on D1)
- E6. ENG-2C.4.pre PR-Q-B: FinancialConfig query registration (depends on B3)
- E7. ENG-2C.4.6 W6 investigator timeline (depends on E6)
- E8. ENG-2C.4.7 W7 two-hop traversal (depends on E6)
- E9. ENG-2C.4.9 W9 derived writeback (depends on any of E1-E5)
- E10. ENG-2C.4.11 W11 sustained ingest (depends on D2)

Phase E workloads are highly parallelisable; each is an independent Spark script or query definition. Estimated seven PRs in flight simultaneously by different contributors.

**Phase F: Time-travel workloads and scoring**
- F1. ENG-2C.4.8 W8 historical replay (depends on E1-E4, D3)
- F2. ENG-2C.4.10 W10 reproduction (depends on E9, D3)
- F3. ENG-2C.5 PR-R-B: FinancialScorecardBlock and score_financial.py (depends on B4, and any workload that produces a metric it renders)

**Phase G: Integration**
- G1. ENG-2C.6 recipe combinations shipped (config schema, examples)
- G2. ENG-2C.7 component matrix documentation
- G3. ENG-2C.8 batch/multi-cycle/sustained mode documentation
- G4. ENG-2C.11 init wizard schema support
- G5. ENG-2C.12 journal event stage_names
- G6. ENG-2C.13 prerequisite checks
- G7. ENG-2C.17 failure recovery mechanisms
- G8. ENG-2C.18 existing command compatibility
- G9. ENG-2C.19 observability integration
- G10. ENG-2C.20 test strategy implementation

**Phase H: Optional and follow-on**
- H1. ENG-2C.9 ML extension (time-permitting)
- H2. ENG-2C.14 DomainModule protocol (recommended before KYC, optional now)
- H3. ENG-2C.23 baseline discovery workstream (parallel with all technical work)

## 6.3 Landing timeline (estimate)

Assumptions: 3 engineers full-time, 1 LLM code agent operator, existing v1.3.1 codebase, standard CI budget.

| Phase | Duration | Notes |
|---|---|---|
| Phase A | 2 weeks | Version bump + optional layers |
| Phase B | 3 weeks | Refactors with strict byte-equivalence gates |
| Phase C | 2 weeks | Data foundation, parallel with Phase B end |
| Phase D | 3 weeks | Pipeline and maintenance |
| Phase E | 4 weeks | Workload set (parallelisable) |
| Phase F | 2 weeks | Time-travel and scoring |
| Phase G | 3 weeks | Integration (parallelisable) |
| Phase H | 2-6 weeks | Optional; H1 (ML) is time-permitting; H2-H3 continue after v1 |

Total to Financial v1 (Phases A-G): approximately 15-19 weeks with 3 FTE + agent operator. With more contributors, the refactor phases (B) do not parallelise well (sequential dependencies), but Phases E and G do.

## 6.4 Delivery milestones

- **M1 (end of Phase A):** Spark 4.2 the default; all existing recipes green on 4.2. No FinServ-Crime user-visible content.
- **M2 (end of Phase B):** all refactors landed; existing customer360 recipes produce byte-identical output; new domain plug-in surfaces exist but no Financial content yet.
- **M3 (end of Phase D):** Financial data foundation and pipeline exist; scale=1 batch run produces valid bronze, silver, gold. No workloads yet.
- **M4 (end of Phase E):** all core workloads (W1-W7, W9, W11) execute and produce output; no time-travel yet.
- **M5 (end of Phase F, "FinServ-Crime v1 code-complete"):** all 11 core workloads execute; scorecard produces all metrics; scale=100 batch run demonstrated.
- **M6 (end of Phase G, "FinServ-Crime v1 ship-ready"):** integration surfaces complete; CI green; docs published; scale=1000 batch run demonstrated; scale=100 sustained run demonstrated.

---

# Appendices

## Appendix A. Spark 4.2 changes affecting existing recipes

Compiled from the Spark 4.0, 4.1, 4.2 release notes as they affect customer360 and Financial recipes.

**ANSI SQL by default.** From Spark 4.0, ANSI mode is on by default. Behaviour differences vs Spark 3.5:
- Division by zero throws instead of returning null. Silver_build scripts with implicit `/` on aggregates should be reviewed for explicit `NULLIF` guards.
- Integer overflow throws instead of wrapping. Check any `sum(count_col)` where the count could exceed INT_MAX at large scale; use `sum(count_col.cast('long'))`.
- Explicit CAST required in more places. `CAST(x AS DOUBLE)` where `x` is a string, previously implicit, now required. Customer360 SQL already ANSI-compliant per v1.3 fixes.

**Iceberg v3 available.** Format version 3 supports VARIANT, row lineage, deletion vectors, encryption. Not the default in Financial v1 (see ENG-U-04). Recipes that opt in via `iceberg_format_version: 3` gain access.

**VARIANT semi-structured type.** Alternative to nested STRUCT for evolving schemas. Not used by Financial v1 (pacs.008 schema is stable per ISO 20022); potentially useful for `SupplementaryData` field expansion in future.

**Fine-grained access control.** Row-level and column-level ACL via Iceberg v3. Not exercised by Financial v1 workloads but available for future multi-tenant scenarios.

**Spark Connect fully supported.** All benchmark scripts can connect via Spark Connect protocol without behaviour change. Reduces client-side resource consumption for interactive query runs.

**Deprecations:** `spark.sql.legacy.*` conf flags for pre-3.0 behaviour continue to work but log deprecation warnings; suppressed in CI.

**Performance improvements not requiring code change:**
- Adaptive Query Execution (AQE) refinements reduce shuffle skew in W2, W3 motif finding
- Parquet zstd compression default (was snappy); recipes can override
- Broadcast hash join thresholds relaxed; potential regression for silver_build on small entities join; mitigated by explicit `broadcast()` hints

## Appendix B. ISO 20022 schema reference for Financial bronze

Full field mapping from ISO 20022 XSDs to bronze Iceberg schemas.

### B.1 pacs.008.001.14 (bank-to-bank credit transfer)

Verified against the pacs.008.001.09 XSD (structurally identical at the analytical level; 001.14 adds three fields not exercised in Financial v1).

**GroupHeader (per message, message-level fields duplicated onto each transaction row for analytical convenience):**
- `msg_id STRING NOT NULL` -- MessageIdentification (Max35Text)
- `cre_dt_tm TIMESTAMP NOT NULL` -- CreationDateTime (ISODateTime)
- `nb_of_txs INT NOT NULL` -- NumberOfTransactions
- `ctrl_sum DECIMAL(18,5)` -- ControlSum (sum of amounts, informational)
- `ttl_intr_bk_sttlm_amt DECIMAL(18,5)` -- TotalInterbankSettlementAmount
- `intr_bk_sttlm_dt DATE` -- InterbankSettlementDate
- `sttlm_inf STRUCT<sttlm_mtd: STRING, ...>` -- SettlementInstruction
- `pmt_tp_inf STRUCT<instr_prty, clr_chanl, svc_lvl, lcl_instrm, ctgy_purp>` -- PaymentTypeInformation
- `instg_agt STRUCT<bicfi: STRING, lei: STRING, nm: STRING>` -- InstructingAgent
- `instd_agt STRUCT<bicfi: STRING, lei: STRING, nm: STRING>` -- InstructedAgent

**CreditTransferTransaction (per transaction, 1..N per message):**
Payment identification: `txn_id`, `instr_id`, `end_to_end_id STRING NOT NULL`, `uetr STRING NOT NULL`, `clr_sys_ref`.
Payment type info: `pmt_tp_inf STRUCT`.
Amounts: `intr_bk_sttlm_amt DECIMAL(18,5) NOT NULL`, `intr_bk_sttlm_ccy STRING NOT NULL`, `instd_amt DECIMAL(18,5)`, `instd_ccy STRING`, `xchg_rate DECIMAL(11,10)`, `chrg_br STRING`.
Correspondent chain: `intrmy_agt_1..3`, `prvs_instg_agt_1..3` each as `STRUCT<bicfi, lei, nm>`.
Party chain: `ultmt_dbtr`, `initg_pty`, `dbtr STRUCT<nm, pstl_adr, id, ctry_of_res>`, `dbtr_acct STRUCT<iban, othr, ccy>`, `dbtr_agt STRUCT<bicfi, lei, nm>`, `cdtr_agt STRUCT<bicfi, lei, nm>`, `cdtr STRUCT<...>`, `cdtr_acct STRUCT<...>`, `ultmt_cdtr STRUCT<nm, lei, ctry>`.
Purpose and reporting: `purp_cd STRING`, `purp_prtry STRING`, `rgltry_rptg ARRAY<STRUCT<dbt_cdt_rptg_ind, authrty_nm, authrty_ctry, details ARRAY<STRING>>>`.
Remittance: `rmt_inf_ustrd ARRAY<STRING>`, `rmt_inf_strd ARRAY<STRUCT<ref_doc, amt>>`.

**Field count:** approximately 65 flat + nested columns per row. Estimated row width: 1200-1800 bytes uncompressed, 400-700 bytes Parquet+snappy.

### B.2 camt.053.001.13 (bank-to-customer account statement)

**Statement (per statement, per account, per day):**
- `stmt_id STRING NOT NULL` -- Identification (Max35Text)
- `elctrnc_seq_nb INT` -- ElectronicSequenceNumber
- `cre_dt_tm TIMESTAMP NOT NULL` -- CreationDateTime
- `fr_to_dt STRUCT<fr_dt_tm, to_dt_tm>` -- FromToDate range
- `acct STRUCT<id: STRUCT<iban, othr>, ccy, ownr STRUCT<nm, lei>, svcr STRUCT<bicfi, lei>>` -- Account (holder and servicing bank)
- `rltd_acct STRUCT<...>` -- RelatedAccount (parent/child accounts)
- `bal ARRAY<STRUCT<tp: STRING, amt DECIMAL(18,5), amt_ccy STRING, cdt_dbt_ind: STRING, dt DATE>>` -- Balance (opening, closing, forward, available)
- `txs_summry STRUCT<ttl_ntries STRUCT<nb_of_ntries: INT, sum: DECIMAL(18,5), ttl_net_ntry: STRUCT<amt, cdt_dbt_ind>>>` -- TransactionsSummary

**Entry (per transaction within statement, 0..N per statement):**
- `ntry_ref STRING` -- EntryReference
- `amt DECIMAL(18,5) NOT NULL`, `amt_ccy STRING NOT NULL`
- `cdt_dbt_ind STRING NOT NULL` -- CRDT or DBIT
- `sts STRING NOT NULL` -- Status (BOOK, PDNG, INFO)
- `bookg_dt STRUCT<dt, dt_tm>`, `val_dt STRUCT<dt, dt_tm>`
- `bk_tx_cd STRUCT<domn STRUCT<cd, fmly STRUCT<cd, sub_fmly_cd>>>` -- BankTransactionCode
- `ntry_dtls ARRAY<STRUCT<tx_dtls: ARRAY<STRUCT<refs, amt, amt_dtls, chrgs, rltd_pties, rltd_agts, purp, rltd_rmt_inf, rmt_inf, rtr_inf, tax, addtl_tx_inf>>>>` -- EntryDetails (nested transaction details)

Bronze camt053 table partitioned by `days(cre_dt_tm)`; entries denormalised into a separate silver.account_movements table for analytical use.

### B.3 pain.001.001.13 (customer credit transfer initiation)

**GroupHeader:** MessageIdentification, CreationDateTime, NumberOfTransactions, ControlSum, InitiatingParty.
**PaymentInformation (0..N per message, batches by execution date):** PmtInfId, PmtMtd (TRF), BtchBookg, NbOfTxs, CtrlSum, PmtTpInf, ReqdExctnDt, Dbtr, DbtrAcct, DbtrAgt, ChrgBr, CdtTrfTxInf ARRAY.
**CreditTransferTransactionInformation (1..N per PaymentInformation):** PmtId (InstrId, EndToEndId, UETR), Amt, ChrgBr, Cdtr, CdtrAcct, CdtrAgt, Purp, RmtInf.

Bronze pain001 table partitioned by `days(reqd_exctn_dt)`; used for the retail-initiated payment portion of the workload set (less than 20% of transaction volume in the default currency mix).

### B.4 BIAN alignment for silver.entities

BIAN v14.0 Customer service domain defines the canonical banking Customer entity. Silver.entities fields map as follows:

| silver.entities column | BIAN Customer attribute | Notes |
|---|---|---|
| entity_id | Customer Reference | Internal ID, not the bank's operational customer number |
| entity_type | Customer Type | {Person, Company, FI} |
| name | Legal Full Name | For Person: FirstName + LastName; for Company: RegisteredName |
| legal_name | Registered Business Name | Company only; matches Company Registration record |
| address.* | Postal Address Details | ISO 3166 country, structured to PostalAddress24 |
| email_addr | Contact Point Email | Not always populated for FI |
| phone_number | Contact Point Phone | E.164 format |
| country | Country of Residence (Person) / Country of Registration (Company) | ISO 3166-1 alpha-2 |
| lei | Legal Entity Identifier | For legal entities only; null for natural persons |
| bic | Business Identifier Code | For FI type only |
| sanctions_status | Sanction Screening Result | Values: null (no match), SDN (Specially Designated National), block-list-name |
| pep_status | Politically Exposed Person Indicator | boolean |
| initial_risk_score | Customer Risk Rating (initial) | 0.0-1.0 scale |

BIAN service definitions not modelled in silver.entities v1 but candidate for follow-on: Customer Access Entitlements, Customer Behavior Insights, Customer Case, Customer Product Portfolio.

## Appendix C. Typology → graph pattern mapping

Eight AMLworld primitives mapped to practitioner-facing typology names. This is the mapping used by `TypologyInjector.schedule()` in the datagen and by the score_financial.py recall calculation.

| AMLworld primitive | Practitioner name | Graph structure | Detection workload | Notes |
|---|---|---|---|---|
| fan-in | Concentration / mule collection | N sources → 1 sink | W1 (via clustering), W2 (via motif) | High in-degree at a suspicious sink |
| fan-out | Distribution / mule dispersal | 1 source → N sinks | W2 (motif), W7 (traversal) | High out-degree at a suspicious source |
| bipartite | Layered account movement | Two disjoint entity sets with transactions only between them | W1 (component isolation), W2 | Common in shell-company structures |
| stack | Vertical layering / correspondent chain abuse | Long chain of pass-through transactions | W2, W3 (via intermediary chain length) | Uses the pacs.008 IntrmyAgt fields |
| random | Noise / mixed-purpose activity | No consistent pattern | none (control) | Injected to test false-positive rates |
| cycle | Round-tripping / kite-flying | Funds return to origin | W3 (motif finding) | Time-window and amount-similarity constraints matter |
| scatter-gather | Smurf-then-consolidate | 1 → many → 1 | W2 + W1 combined | Two-stage; detection needs both workloads |
| gather-scatter | Consolidate-then-distribute | many → 1 → many | W2 + W7 combined | Reverse of scatter-gather |

**Injection rules:**
- Each typology instance has a `typology_id`, participating `entity_ids`, participating `transaction_ids`, `injection_start_ts`, `injection_end_ts`, and instance-specific parameters (chain length, cycle amount, fan-out width).
- Instances are scheduled across the datagen time range with uniform distribution; density per typology configurable via `financial.typology_distribution`.
- Default distribution: fan-in 15%, fan-out 15%, bipartite 10%, stack 20%, cycle 20%, scatter-gather 10%, gather-scatter 10%. Random noise transactions are all non-injected transactions (99.9% of total).

## Appendix D. Datagen design

Full design of the Financial datagen. Complements ENG-2C.2.

**Deterministic per-pod generation.** Kubernetes Indexed Job assigns `JOB_COMPLETION_INDEX` to each pod (0 to N-1). The FinancialGenerator computes its file range as follows:

```python
class FinancialGenerator:
    def __init__(self, config):
        self.seed = config.seed
        self.scale = config.scale
        self.pod_index = int(os.environ["JOB_COMPLETION_INDEX"])
        self.total_pods = int(os.environ["JOB_COMPLETIONS"])

        # Compute dimensions once
        dims = financial_dimensions(self.scale)
        self.total_accounts = dims.customers
        self.txns_per_account_per_month = 45
        self.retention_months = config.retention_months  # 60 default

        # Total files across all pods
        self.total_files = self._compute_total_files()
        # This pod owns files [start, end)
        self.file_start = (self.pod_index * self.total_files) // self.total_pods
        self.file_end = ((self.pod_index + 1) * self.total_files) // self.total_pods

    def _compute_total_files(self):
        target_rows_per_file = 500_000
        total_txns = self.total_accounts * self.txns_per_account_per_month * self.retention_months
        return max(1, total_txns // target_rows_per_file)

    def file_time_range(self, file_id):
        """Deterministic mapping: file_id → (start_ts, end_ts)."""
        total_range = self.retention_months * 30.5 * 86400  # seconds
        seconds_per_file = total_range / self.total_files
        start_s = file_id * seconds_per_file
        end_s = (file_id + 1) * seconds_per_file
        base_ts = self._epoch_now() - int(total_range)
        return base_ts + int(start_s), base_ts + int(end_s)

    def generate_all(self):
        for file_id in range(self.file_start, self.file_end):
            self.generate_file_data(file_id)
```

**Time-partitioned file assignment** ensures both endpoints of every transaction land in the same file naturally. Transactions are generated within the file's time window; the debtor and creditor are looked up from the deterministic account and entity tables. Cross-file coordination is not needed.

**Account and entity table construction.** Every pod deterministically constructs the same account and entity tables from the seed:

```python
def build_accounts(seed, total_accounts):
    """Returns (accounts, entities) as pyarrow Tables."""
    rng = np.random.default_rng(seed)
    account_ids = np.arange(total_accounts)
    entity_ids = account_ids  # 1:1 for simplicity in v1
    countries = rng.choice(COUNTRY_LIST, size=total_accounts, p=COUNTRY_WEIGHTS)
    currencies = np.array([COUNTRY_TO_CURRENCY[c] for c in countries])
    lei_hashes = [f"LB{sha256(f'{seed}:{aid}'.encode()).hexdigest()[:16].upper()}" for aid in account_ids]
    # + check digits, address generation, entity type sampling, sanctions/PEP flagging
    ...
```

Since every pod runs this with the same seed, every pod holds identical account and entity tables in memory. No cross-pod coordination.

**Typology injection.** After the base transaction stream is emitted for a file, TypologyInjectors overlay their planted patterns:

```python
def generate_file_data(self, file_id):
    start_ts, end_ts = self.file_time_range(file_id)
    base_txns = self._generate_base_transactions(start_ts, end_ts)

    # Apply scheduled typology instances that fall within this window
    for injector in self.typology_injectors:
        instances = injector.instances_in_window(start_ts, end_ts, self.seed)
        for instance in instances:
            base_txns = injector.inject(base_txns, instance)
            self.manifest.record(instance)

    return base_txns
```

**Ground truth manifest format.**

```python
@dataclass
class TypologyInstance:
    typology_id: str            # UUIDv4
    typology_type: str          # 'fan-in', 'stack', 'cycle', etc.
    participating_entity_ids: list[int]
    transaction_ids: list[str]  # UUIDv4 UETRs
    injection_start_ts: datetime
    injection_end_ts: datetime
    injection_parameters: dict  # typology-specific: chain_length, cycle_amount, fanout_width, etc.
```

Written to `bronze/manifest/manifest.parquet` as one row per instance, partitioned by `days(injection_start_ts)`. Each pod appends its instances via Iceberg append (concurrent writers safe). The `bronze/manifest/` path is deliberately outside the medallion path so detection workloads cannot accidentally read it.

**Continuous mode.** In `DatagenMode.CONTINUOUS`, the pod runs an infinite loop generating files with time windows advancing at the configured target TPS rate. The multi-process pattern (8 generator processes + 2 uploader threads per pod) is unchanged from customer360; only the per-file generation logic differs.

**Determinism verification.** `pytest tests/test_financial_generator.py::test_deterministic` runs two independent datagen sessions with the same seed and scale, then compares the produced Parquet files byte-for-byte after canonical sort. Must pass at scale=1 in CI, at scale=10 nightly.

## Appendix E. Workload orchestration and concurrency measurement

Full orchestration detail for the Financial workload set.

**Three run modes** for concurrent measurement:

1. **Idle mode.** Each workload runs alone, no other Spark jobs, no ingest, no queries. Baseline.
2. **Pairwise mode.** One batch detection workload (typically W2 or W4, the two heavier ones) runs concurrently with the investigator query stream. Measures the pairwise degradation.
3. **Full mode.** All workloads submitted concurrently: W1-W5 as Spark jobs, W6-W7 as query streams, W11 as ingest, W9 writing to gold throughout. Maximum load.

**Orchestrator implementation** in `src/lakebench/cli/_financial_run.py`:

```python
def financial_run(config, mode: Literal["idle", "pairwise", "full"]):
    if mode == "idle":
        for wid in ["W1", "W2", "W3", "W4", "W5"]:
            submit_workload(wid, load_condition="idle")
            wait_for_completion(wid)
        for qid in ["W6", "W7"]:
            run_query_stream(qid, iterations=100, load_condition="idle")

    elif mode == "pairwise":
        batch = submit_workload("W2", load_condition="pairwise")
        run_query_stream("W6", iterations=100, load_condition="pairwise")
        run_query_stream("W7", iterations=100, load_condition="pairwise")
        wait_for_completion(batch)

    elif mode == "full":
        batch_jobs = [submit_workload(w, load_condition="full") for w in ["W1","W2","W3","W4","W5"]]
        ingest_stream = start_sustained_ingest(load_condition="full")
        run_query_stream("W6", iterations=100, load_condition="full")
        run_query_stream("W7", iterations=100, load_condition="full")
        wait_all(batch_jobs)
        stop_sustained_ingest(ingest_stream)
```

**Timing methodology.** For batch workloads: wall-clock from SparkApplication submission to completion, captured in `StageMetrics.elapsed_seconds`. For query streams: per-query timing captured in `BenchmarkResult.queries`; percentiles computed over the 100-iteration sample; `queries_per_hour` derived. All metrics tagged with `load_condition` from ENG-M-05.

**Warmup handling.** Query streams discard the first 5 iterations before computing percentiles (JVM warmup, cache warming). Batch workloads have no warmup discard; the wall-clock is the reported number.

**Test corpus determination.** All three modes run against the same silver snapshot to make load-condition comparisons meaningful. In multi-cycle mode, orchestration runs three times per cycle (once per mode) against the cycle's silver snapshot; six per-cycle scorecard rows (P50 and P95 for W6 and W7, three modes each).

## Appendix F. Scale factor calibration methodology

How to verify that the Financial dimensions produce the promised storage footprint at each scale point, and how to recalibrate if they don't.

**Row-size measurement procedure:**

1. Run datagen at scale=1 with default config. Capture bytes written and rows written.
2. Verify silver_build_financial at scale=1. Capture silver.transactions bytes and rows.
3. Compute observed compressed row size: `silver_bytes / silver_rows`.
4. Compare to spec assumption of 800 bytes/row. Acceptable band: 600-1000 bytes/row.
5. If outside the band:
   - Determine the cause: overly-populated struct fields, unexpected null density, compression codec mismatch.
   - Choose the corrective action: adjust the dimensions function (customers per scale unit), adjust the row shape, or adjust the storage codec.

**Dimensions function recalibration:**

If observed row size is Y bytes and the desired storage-per-scale-unit is Z GB (default: 10 GB), then customers-per-scale-unit should be:

```
target_rows_per_scale_unit = Z * 1e9 / Y
customers_per_scale_unit = target_rows_per_scale_unit / (45 * 60)
                        = Z * 1e9 / (Y * 45 * 60)
```

For Z=10GB, Y=800 bytes: customers_per_scale_unit = 10e9 / (800 * 45 * 60) = 4,630. Rounded to 5,000 as in ENG-U-05.

**Verification cadence:** re-measure at every major refactor of silver_build_financial or FinancialGenerator, and at every Spark or Iceberg version bump. Publish the current calibration in `docs/financial-benchmark-baselines.md` with git-sha reference.

**Multi-scale spot checks:** at scale=10, 100, 1000, verify that observed storage is within ±15% of the linear extrapolation from scale=1. Non-linearity above ±15% indicates a design bug (e.g. per-scale-unit overhead not scaling correctly, or per-file coordination overhead dominating at large pod counts).

## Appendix G. Baseline discovery workstream

Interview guide and delivery detail for ENG-2C.23.

**Target profile:** AML/FinCrime engineering leads, senior data platform architects, or FIU (Financial Intelligence Unit) technical leads at tier-1 or tier-2 universal banks, regional banks, or dedicated compliance-analytics vendors.

**Interview structure (60 minutes):**

1. Context (5 min): institution size, retention requirements, regulator jurisdiction, existing AML platform.
2. Current detection latency (10 min): how often does detection run, wall-clock for a full-corpus rerun, what's the SLA for new-transaction inclusion.
3. Investigator workflow (10 min): typical query patterns, what fields are the primary lookup keys, what's the acceptable P95 latency for case-management queries, what's the concurrent-user count during peak.
4. Historical replay (5 min): how often is a lookback required, typical depth (months), typical wall-clock, business impact if a replay takes longer than the regulator's deadline.
5. Time-travel and reproducibility (5 min): what mechanism today enables reproducing a historical alert, what's the audit requirement, what's the retention policy.
6. Sustained ingest (5 min): typical daily volume, peak volume, source shape (files, streams, message bus), current freshness SLA.
7. Retention policy (5 min): EU AMLR compliance depth, national variations, snapshot vs immutable log strategy.
8. Compaction and maintenance (5 min): how often, what window, cost, blast radius.
9. Alert cadence (5 min): daily alert volume, false-positive rate, disposition workflow.
10. Reactions to benchmark scale points (5 min): does 100 TB at scale 10K match, is 60 months retention realistic, is 45 txn/account/month realistic.

**Deliverable:** anonymised summary with per-question aggregated ranges, appended to `docs/financial-benchmark-baselines.md`. Recalibration recommendations for any spec assumption where the interview majority disagrees.

**Timeline and count:** minimum 6 interviews before FinServ-Crime v1 release, target 8-12. Aim for geographic spread (US, EU, UK, APAC) and institution size spread (top-5 tier 1, mid-sized regional, dedicated FIU vendor).

**Recruitment:** existing customer relationships, industry conferences (SIBOS, ACAMS), practitioner communities (r/AML, LinkedIn AML groups). Non-attributed interviews only.

## Appendix H. Adversarial review

The parent solution document's Section 18 identified four adversarial arguments the benchmark must survive. This appendix maps each to the FinServ-Crime v1 spec's response.

**Adversarial claim 1: "This is compute-bound, not storage-bound. Storage vendors gaming the story."**

Spec response: the benchmark measures both. QpH is compute-heavy (Trino queries). Sustained ingest is I/O-heavy (write throughput). Concurrent degradation ratio isolates the storage contribution (P95_full_load / P95_idle is a storage-and-scheduler measurement, not a compute measurement). Historical replay and time-travel reproduction are cold-storage-heavy (reads snapshots not in the current working set). The scorecard's honest customer reads all of these together, not just the compute-flattering ones.

**Adversarial claim 2: "The historical corpus is cold; nobody actually queries snapshots from 5 years ago."**

Spec response: W8 (historical replay) is exactly this workload, and the parent solution doc's citation of the TD Bank enforcement (mandated lookback) is the counter-evidence. Regulators require it. The workload's wall-clock at 60-month depth is a first-class scorecard row. If the benchmark on a specific architecture fails to complete W8 at 60 months within reasonable time, that's the point of the measurement.

**Adversarial claim 3: "Per-consumer copies destroy the shared-storage narrative."**

Spec response: the benchmark is designed for shared storage. It does not exercise per-consumer copies. If a specific storage architecture requires copies to achieve the concurrent-degradation numbers, that architecture pays the storage-cost overhead visible in the storage efficiency scorecard rows. The comparison is transparent.

**Adversarial claim 4: "Incumbent DBMS-plus-warehouse economics with existing bundled licenses may still beat lakehouse TCO."**

Spec response: the benchmark deliberately does not publish TCO comparisons. It publishes workload throughput and latency per storage architecture; the user brings their own cost model. TCO is a business-context calculation, not a benchmark output. The benchmark's honesty is in refusing to game that comparison in either direction.

## Appendix I. Positioning relative to Databricks accelerator and Cloudera / riskCanvas

FinServ-Crime v1 is complementary, not competitive, with the vendor accelerators it draws its workload set from. Positioning statement:

The Databricks industry-solutions AML accelerator is a set of runnable notebooks tied to the Databricks platform, using Databricks-specific features (Unity Catalog, Photon, Lakehouse Federation, Managed MLflow). It's the reference implementation for AML on Databricks. FinServ-Crime v1 in lakebench uses the same workload set (motif finding for structuring, connected components for synthetic identity, Pregel for risk propagation, Splink for entity resolution) reimplemented as portable Spark scripts that run on any Iceberg-based lakehouse. The purpose is to enable comparison across storage architectures and lakehouse platforms using workloads a Databricks-shop practitioner recognises.

Cloudera CDP (with its Data Warehouse and Data Engineering services) and Cloudera's riskCanvas partnership similarly define an AML reference architecture with Iceberg, Impala, Spark, and typology detection. Their reference does not ship as OSS runnable code. FinServ-Crime v1 provides that runnable OSS equivalent using the same architectural pattern.

Neither vendor's accelerator is trying to be a benchmark. lakebench with FinServ-Crime is not trying to be a production AML platform. The three artifacts serve different purposes and reference each other without overlap.

## Appendix J. Traceability matrix

REQ → SOL → ENG → test mapping. Every requirement in Part 1 and Part 2A is traced to the solution architecture that implements it, the engineering block that delivers it, and the test that verifies it.

| REQ | SOL | ENG | Test |
|---|---|---|---|
| REQ-U-01 | SOL-U-01 | ENG-U-01 | tests/test_config.py::test_supported_spark_versions; CI matrix |
| REQ-U-02 | SOL-U-02 | ENG-U-02 | tests/test_deployers.py::test_graphframes_image_pull |
| REQ-U-03 | SOL-U-03 | ENG-U-03 | tests/test_deployers.py::test_splink_image_pull |
| REQ-U-04 | SOL-U-04 | ENG-U-04 | tests/test_iceberg_runtime.py::test_format_version_config |
| REQ-U-05 | SOL-U-05 | ENG-U-05 | tests/test_scale.py::test_financial_dimensions |
| REQ-U-06 | SOL-U-06 | ENG-U-06 | tests/test_financial_workloads.py::test_distribution_mode_at_scale |
| REQ-F-01 | SOL-2B.2 | ENG-2C.1 | tests/test_financial_pacs008_schema.py |
| REQ-F-02 | SOL-2B.2 | ENG-2C.1 | tests/test_financial_pacs008_schema.py::test_correspondent_chain |
| REQ-F-03 | SOL-2B.2 | ENG-2C.1 | tests/test_financial_pacs008_schema.py::test_regulatory_reporting |
| REQ-F-04 | SOL-2B.2 | ENG-2C.1 | tests/test_financial_generator.py::test_uetr_populated_and_unique |
| REQ-F-05 | SOL-2B.2, SOL-2B.6 | ENG-2C.16 | tests/test_financial_generator.py::test_business_hours_skew |
| REQ-G-01 | SOL-2B.2 | ENG-2C.1, ENG-2C.2 PR-C | tests/test_financial_generator.py::test_manifest_written |
| REQ-G-02 | SOL-2B.2 | ENG-2C.1 | tests/test_financial_workloads.py::test_manifest_isolation |
| REQ-G-03 | SOL-2B.6 | ENG-2C.16 | tests/test_financial_generator.py::test_typology_density |
| REQ-G-04 | SOL-2B.3 | ENG-2C.2 PR-C, Appendix C | tests/test_financial_typologies.py |
| REQ-D-01 | SOL-2B.3 | ENG-2C.4.1 to 2C.4.5 | tests/test_financial_workloads.py::test_w1 to test_w5 |
| REQ-D-02 | SOL-2B.3 | ENG-2C.4.1 to 2C.4.4 | tests/test_financial_workloads.py::test_w1_path_b |
| REQ-D-03 | SOL-2B.3 | ENG-2C.4.1 to 2C.4.5 | tests/test_financial_workloads.py::test_detection_deterministic |
| REQ-D-04 | SOL-2B.5 | ENG-2C.5 PR-R-B, Appendix C | tests/test_financial_scoring.py::test_recall_calculation |
| REQ-D-05 | SOL-2B.6 | ENG-2C.4.*, ENG-2C.22 | e2e stress: tests/test_e2e.py::TestFinancialAtScale |
| REQ-I-01 | SOL-2B.3 | ENG-2C.4.6, 2C.4.7 | tests/test_financial_workloads.py::test_w6, test_w7 |
| REQ-I-02 | SOL-2B.3 | ENG-2C.4.6, 2C.4.7 | tests/test_financial_workloads.py::test_investigator_queries_ansi |
| REQ-I-03 | SOL-2B.5 | ENG-2C.5 | tests/test_benchmark.py::test_percentile_reporting |
| REQ-C-01 | SOL-2B.4 | ENG-2C.4.pre, Appendix E | tests/test_financial_pipeline.py::test_three_mode_orchestration |
| REQ-C-02 | SOL-2B.5 | ENG-2C.5 PR-R-B | tests/test_financial_scoring.py::test_degradation_ratio |
| REQ-C-03 | SOL-2B.4 | ENG-2C.15, Appendix E | tests/test_financial_workloads.py::test_concurrent_isolation |
| REQ-S-01 | SOL-2B.6 | ENG-2C.22, ENG-U-05 | e2e: scale 10000 target run |
| REQ-S-02 | SOL-U-05 | ENG-U-05 | tests/test_scale.py::test_linear_scaling |
| REQ-S-03 | SOL-2B.6, Appendix F | ENG-U-05, ENG-2C.22 | tests/test_scale.py::test_row_size_calibration |
| REQ-R-01 | SOL-2B.3 | ENG-2C.4.8 | tests/test_financial_workloads.py::test_w8_replay |
| REQ-R-02 | SOL-2B.3 | ENG-2C.4.10 | tests/test_financial_workloads.py::test_w10_reproduction |
| REQ-R-03 | SOL-2B.3 | ENG-R-05 | tests/test_financial_pipeline.py::test_retention_workload |
| REQ-R-04 | SOL-2B.2 | ENG-2C.2 PR-C, Appendix D | tests/test_financial_generator.py::test_deterministic |
| REQ-R-05 | SOL-2B.3 | ENG-R-05 | tests/test_financial_pipeline.py::test_maintenance_preserves_snapshots |
| REQ-P-01 | SOL-2B.1 | ENG-2C.6 | tests/test_s3_conformance.py |
| REQ-P-02 | SOL-2B.1 | ENG-2C.6 | tests/test_e2e.py::TestFinancialOnEachEngine |
| REQ-P-02.1 | SOL-2B.1 | ENG-2C.18 | tests/test_local_mode.py::test_financial_at_scale_1 |
| REQ-P-03 | SOL-2B.1 | ENG-2C.6, ENG-U-02, ENG-U-03 | licence audit; CI |
| REQ-M-01 | SOL-2B.5 | ENG-2C.5 PR-R-A | tests/test_customer360_regression.py |
| REQ-M-02 | SOL-2B.5 | ENG-2C.5 PR-R-B | tests/test_financial_scoring.py::test_scorecard_block |
| REQ-M-03 | SOL-2B.5 | ENG-2C.5 | tests/test_metrics.py::test_provenance_columns |
| REQ-M-04 | SOL-2B.5 | ENG-2C.5 | tests/test_metrics.py::test_no_dedupe_ratio |
| REQ-M-05 | SOL-2B.4 | ENG-M-05 | tests/test_metrics.py::test_load_condition_field |
| REQ-ML-01 | SOL-2B.7 | ENG-2C.9 (time-permitting) | tests/test_financial_ml.py::test_feature_families |
| REQ-ML-02 | SOL-2B.7 | ENG-2C.9 (time-permitting) | tests/test_financial_ml.py::test_no_external_deps |
| REQ-ML-03 | SOL-2B.7 | ENG-2C.9 (time-permitting) | tests/test_financial_ml.py::test_model_versioned |
| REQ-ML-04 | SOL-2B.7 | ENG-2C.9 (time-permitting) | tests/test_financial_ml.py::test_champion_challenger |
| REQ-ML-05 | SOL-2B.7 | ENG-2C.9 (time-permitting) | tests/test_financial_ml.py::test_drift_detection |
| REQ-A-01 | Part 0.1 | Part 0 conventions | manual review at spec merge |
| REQ-A-02 | Part 0.1 | Part 6 (PR DAG) | manual review at PR merge |
| REQ-A-03 | Part 0.1 | ENG-* verification lines | CI runs the verification commands |
| REQ-A-04 | Part 0.1 | ENG-* "not this task" lines | manual review at PR merge |
| REQ-A-05 | Part 0.1 | Part 2B.9 (design decisions) | manual review at spec merge |

Rows highlighted at implementation time as "not delivered" or "test not written" block M5 (code-complete) and M6 (ship-ready) milestones.

## Appendix K. ML/training extension detail

Complements ENG-2C.9. Concrete feature list, training procedure, PSI computation.

**Feature families (per REQ-ML-01):**

Velocity (windowed per entity, 30-day and 7-day windows):
- txns_out_count, txns_in_count
- amount_out_sum, amount_in_sum, amount_out_avg, amount_in_avg
- distinct_days_active, distinct_counterparties

Diversity (windowed per entity, 30-day window):
- unique_counterparty_countries
- unique_currencies_touched
- geographic_dispersion (Herfindahl index over counterparty countries)
- currency_dispersion (Herfindahl over currencies)

Temporal (per entity, historical statistics):
- weekend_txn_ratio
- business_hours_txn_ratio
- inter_txn_gap_median_seconds
- inter_txn_gap_p95_seconds

Graph (per entity, from W1 output):
- in_degree, out_degree
- clustering_coefficient
- pagerank_score
- component_id, component_size

Entity (per entity, static):
- entity_type (encoded)
- country_risk_score
- account_age_days
- sanctions_status_encoded, pep_status
- kyc_completeness_score

**Training procedure (per REQ-ML-02):**

```python
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler

def train(features_df, manifest_df, model_id):
    labeled = features_df.join(
        manifest_df.select("transaction_ids", "typology_type").withColumn("label", lit(1.0)),
        expr("array_contains(manifest_df.transaction_ids, features_df.txn_id)"),
        "left"
    ).fillna({"label": 0.0})

    train_df, test_df = labeled.randomSplit([0.8, 0.2], seed=42)

    feature_cols = [c for c in labeled.columns if c not in ("txn_id", "label", "typology_type")]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=50)
    model = gbt.fit(assembler.transform(train_df))

    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    auc = evaluator.evaluate(model.transform(assembler.transform(test_df)))

    training_snapshot_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0].ts
    feature_hash = sha256(",".join(feature_cols).encode()).hexdigest()[:16]

    save_model_artifact(model, model_id, training_snapshot_ts, feature_hash, auc)
```

**PSI (Population Stability Index) drift computation (per REQ-ML-05):**

```python
def compute_psi(reference_dist, current_dist, num_buckets=10):
    """PSI = sum((current - reference) * ln(current / reference)) across buckets."""
    ref_pct = bucket_frequencies(reference_dist, num_buckets)
    cur_pct = bucket_frequencies(current_dist, num_buckets)
    # Guard against zero buckets
    ref_pct = np.where(ref_pct == 0, 1e-6, ref_pct)
    cur_pct = np.where(cur_pct == 0, 1e-6, cur_pct)
    return np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))

# Thresholds (industry convention):
#   PSI < 0.1     : no significant drift
#   0.1 <= PSI < 0.2 : moderate drift, investigate
#   PSI >= 0.2    : significant drift, retrain
```

**MLflow integration (optional).** When the Spark image includes MLflow, training runs log:
- Model artifact (model.write().save(...) plus MLflow model registration)
- Metrics: AUC, precision, recall at multiple thresholds
- Params: max_iter, max_depth, feature_hash, training_snapshot_ts
- Tags: run_id, cycle_index (if multi-cycle), model_id, model_version

Without MLflow, artifacts go to Iceberg gold.models table with the same fields as columns.

## Appendix L. LLM-agent implementation guide

Guidance for LLM code agents (Claude Code, Cursor, Copilot) implementing this spec.

**Task decomposition example.** Take ENG-2C.4.1 (W1 connected components). An agent should decompose it as:

1. Read `docs/architecture.md` (repo shape), the spec section (ENG-2C.4.1), and the Databricks accelerator NB1 code linked in the spec.
2. Verify GraphFrames 0.9.3+ is installable in the target Spark image (V-19 check).
3. Draft the Path A script at `src/lakebench/spark/scripts/workload_w1_cc_financial.py`. Include the input tables from ENG-2C.1, the algorithm from the spec, the output table shape (gold.synthetic_id_clusters).
4. Draft the Path B DataFrame-native alternative in the same file behind a `if GRAPHFRAMES_AVAILABLE:` check.
5. Add JobType.WORKLOAD_W1_FINANCIAL to `src/lakebench/modules/pipeline_engines/spark/job.py:1172` (verify current line number first).
6. Add script filename to `spark_scripts_to_ship` list at same file around line 1883.
7. Write test at `tests/test_financial_workloads.py::test_w1`. Determinism check, output shape check, synthetic-input correctness check.
8. Run `pytest tests/test_financial_workloads.py::test_w1` locally.
9. Open PR referencing ENG-2C.4.1. Description includes: which spec section this implements, dependencies checked, test output.

**Prompt scaffolds for the discovery/verify/assess phase (Part 0):**

```
System: You are implementing spec block ENG-<id>. Before writing any code:

1. Read the following files: <list from Part 0.2>
2. Run the following verification commands and record output: <commands from Part 0.3 table>
3. Answer the following assessment questions in the PR description: <questions from Part 0.4 relevant to this ENG>
4. Confirm the impact classification for this ENG matches Part 0.5.

If any verification fails, do not write code. Open an issue documenting the drift.

Only after these steps, proceed to implementation. Reference the spec block by ID in every commit message.
```

**Common failure modes and how the spec prevents them:**

- **"Agent invents file paths."** Prevention: every ENG block names files by full path; agent unable to guess plausibly wrong ones.
- **"Agent guesses at protocol/interface shape."** Prevention: SOL-2B.8 (DomainModule) defines the protocol; ENG-2C.14 lists the exact methods.
- **"Agent implements in scope beyond the task."** Prevention: every ENG block has a "Not this task" list.
- **"Agent's implementation depends on undocumented behaviour of another module."** Prevention: Part 0.3 verify table lists every assumption; agent halts on mismatch.
- **"Agent's PR is too big to review."** Prevention: Part 6 PR sequencing DAG bounds each PR to one atomic unit; Refactor blocks are pre-decomposed into sub-PRs.
- **"Agent chooses inconsistent naming."** Prevention: Part 2 naming convention note; global search-replace patterns documented.
- **"Agent misses backward-compatibility requirement."** Prevention: Impact classification in Part 0.5 flags Refactor and Breaking blocks; the byte-identical CI check is spelled out for each.

**Recommended agent workflow per ENG block:**

1. `cat <spec-block>` -- read the block
2. `read Part 0.2 files` -- discovery
3. `bash <verify-commands>` -- verification
4. Draft implementation
5. `pytest <verification-test>` -- local test
6. `git diff --stat` -- bounded scope check
7. Draft PR description including ENG ID, discovery findings, test output
8. Human review at PR