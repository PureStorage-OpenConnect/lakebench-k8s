# LakeBench Next Specification -- v1.4.0 Addendum

**Baseline drift.** The spec was written against v1.3.1 with a v1.4-candidate review. `main` is now at v1.4.0 (commit `cb6224b`, PR #17), which landed +9,300 LOC across local mode, container runtime, storage conformance, and the Garage deployer. A Verify pass against the current tree confirmed that no V-* claim in §0.3 is broken -- 22 of 26 hold as written, 4 drift on line numbers only. This addendum records the corrections and one design decision that must land before Phase 3 execution starts. Every point below cites the spec section it amends; assume the rest of the spec is unchanged.

---

## A.1 §0.3 V-* line-number corrections

Update the following rows in the Verify table. Underlying claims still hold; only the pointers changed.

| ID | Correction |
|---|---|
| V-10 | Default Spark image `apache/spark:4.0.2-python3` does **not** live in `src/lakebench/_constants.py`. It is defined at 11 sites in `src/lakebench/config/recipes.py` (per-recipe defaults). ENG-U-01's file list changes accordingly (see A.2). |
| V-12 | Report generator has already been de-branded. Only one hardcoded `Customer360` string remains (`src/lakebench/reports/generator.py:194`, section header). The "92K of Customer360-specific rendering" characterization is stale; the actual refactor surface for domain-aware rendering is roughly one string plus the addition of the `ScorecardBlock` hook. ENG-2C.5 rescopes accordingly (see A.4). |
| V-13 | Current line numbers in `src/lakebench/modules/pipeline_engines/spark/job.py`: `JobType` enum at `:751`, script map at `:1175`, Delta variant map at `:1185`, scripts-to-ship at `:1883` (local variable `script_files` inside `_write_scripts_configmap`, not a module-level `spark_scripts_to_ship`). |
| V-24 | `compute_guidance` is at `src/lakebench/config/scale.py:252`, not `:250`. Two-line offset only. |

---

## A.2 ENG-U-01 (Spark 4.2 baseline bump) -- file list correction

**Amends §1.3, ENG-U-01.**

The spec's engineering block for the Spark 4.2 default bump names `_constants.py` and `config/schema.py` as target files. There is no `DEFAULT_SPARK_IMAGE` or `SUPPORTED_SPARK_VERSIONS` constant in `_constants.py`; the default lives inline in `config/recipes.py`. Amend the file list to:

- `src/lakebench/config/recipes.py` -- 11 per-recipe default sites (change `apache/spark:4.0.2-python3` -> `apache/spark:4.2.0-python3`).
- `src/lakebench/config/schema.py` -- `_FORMAT_VERSION_COMPAT`, `_ICEBERG_RUNTIME_SUFFIX`, `_HADOOP_AWS_COMPAT` if the AWS SDK compat matrix key needs a `(4, 2)` entry.
- `src/lakebench/modules/pipeline_engines/spark/job.py` -- `_delta_spark_artifact()` if Delta 4.2.0 vs 4.1.0 artifact naming diverges (verify via a direct POM fetch per gotcha 33; do not trust the search API).
- `docs/supported-components.md:15` (matrix) and `docs/compatibility-matrix.md` (full matrix).

**Non-negotiable pre-check** (per the v1.4 lesson on Spark 4.2, LB-069): a live UAT write cycle on Spark 4.2 + Iceberg + Delta + Polaris + Hive must pass before the default bumps. Iceberg-side binary incompatibilities against internal Spark APIs (`SparkView`, `View`) are invisible to research; only a live driver load surfaces them. Do not merge the default bump on the strength of image/Java/Maven checks alone.

---

## A.3 ENG-2C.3 (Financial pipeline) -- runtime routing decision

**Amends §2C.3.**

Financial pipeline scripts (`financial/bronze_ingest.py`, `financial/silver_enrich.py`, `financial/gold_score.py`, plus any streaming variants) **shall be authored once and dispatched through the `runtime.protocol.Runtime` abstraction**. One script set runs on both Kubernetes and single-host local; local mode invokes them via `runtime.container.ContainerRuntime` (podman-first, docker fallback), and K8s mode invokes them via the existing Spark-operator path wrapped as `K8sRuntime`.

Consequences:

- **`cli/_financial_run.py`** builds a `ComponentSpec` (from `runtime/protocol.py`) per stage and hands it to the active `Runtime`, selected by whether `--local` was passed. It does not shell out to `podman` directly, and it does not import from `modules/pipeline_engines/spark/local_job.py`.
- **No `local_financial_job.py`.** The parallel-scripts pattern used for Customer360 local mode is not repeated for Financial. The local/K8s split is expressed at the runtime boundary, not the script boundary.
- **Financial scripts declare their own `ComponentSpec`** (image, command, args, env, mounts) in a single canonical location, so `ContainerRuntime`'s fingerprint-based container reuse (`SPEC_FINGERPRINT_LABEL`) works out of the box for local dev loops.
- **Migration note for future work, not this task:** Customer360 pipeline scripts remain on the current pattern for v1; unifying them onto `ContainerRuntime` is not in scope for this release.

Verification for ENG-2C.3 changes to a pair: (a) `pytest tests/test_financial_pipeline.py -v` passes with K8s runtime mocked; (b) `pytest tests/test_financial_local.py -v` passes with `ContainerRuntime` executing against a real podman on the test host.

---

## A.4 ENG-2C.5 (Scorecard / report generator) -- rescope

**Amends §2C.5.**

The report generator refactor is materially smaller than the spec assumed. The current `src/lakebench/reports/generator.py` is already domain-neutral in structure (no per-domain rendering branches); the surviving Customer360-specific surface is:

- One hardcoded string at line 194 (section header wording).
- The `BENCHMARK_QUERIES` list at `src/lakebench/benchmark/queries.py:233` -- Customer360-column-referenced, not a report-generator concern.

**Revised scope for ENG-2C.5:**

1. Introduce a `ScorecardBlock` interface (name + `render(metrics) -> html`) as designed. Adds ~1 protocol.
2. Extract the current hardcoded section header behind a `domain` accessor on `PipelineBenchmark`. One-line change.
3. Register a Customer360 `ScorecardBlock` and a Financial `ScorecardBlock`. Two new files, each roughly the size of the current inline rendering block for their metric set.
4. **Deleted from scope:** any generalized "92K refactor" language; any "rewrite the generator" implication.

Verification unchanged: `pytest tests/test_report_generator.py -v` plus a rendered-report visual diff.

---

## A.5 REQ-P-02.1 / SOL-2B.1 (local-mode storage substrate)

**Amends §2A.8 (portability requirements) and §2B.1 (positioning).**

The spec leaves the local-mode S3 substrate implicit. v1.4 shipped a first-class Garage deployer (`src/lakebench/deploy/garage.py`) and an S3 conformance suite (`src/lakebench/s3/conformance.py`, tests at `tests/test_s3_conformance*.py`, docs at `docs/storage-backends.md`). Amend the spec to name them:

- **Local-mode storage default is Garage.** All Financial example configs shipping in `examples/` for local mode use `deploy.garage: true` and reference the bundled Garage endpoint. FlashBlade and AWS S3 remain valid for K8s mode, unchanged.
- **Storage portability acceptance leans on the existing suite.** REQ-P-01 does not require new conformance checks; `lakebench config storage <config>` is the acceptance harness. Financial recipes must pass with `overall_success: True` against Garage, FlashBlade, and (once validated) AWS S3.
- **SeaweedFS remains disqualified** per gotcha 30 / CLAUDE.md storage backend table.

Traceability: the matrix in Appendix J should cite `tests/test_s3_conformance.py` (already listed near line 2392 of the spec) for REQ-P-01 acceptance; it does not need a new test artifact.

---

## A.6 Discovery language cleanup

§0.2 lists "and their `_delta` variants" for the batch pipeline scripts. `bronze_verify_delta.py` does not exist and is not intended to -- `bronze_verify` is format-agnostic per the comment at `src/lakebench/modules/pipeline_engines/spark/job.py:1171-1174`. Only `silver_build_delta.py` and `gold_finalize_delta.py` are real Delta variants. Tighten the discovery language on the next spec revision; no code impact.

---

## A.7 Items not in this addendum

- No changes to Part 2 requirements (§2A) or solution architecture (§2B), beyond the two clarifications in A.5.
- No changes to the ENG-* dependency DAG or PR-sizing conventions.
- No changes to the traceability-matrix convention; individual matrix cells will absorb the corrections above when Appendix J is populated.
- Sustained-mode concurrent-workload orchestration (§3.1) is unchanged and remains a Part 3 (Deferred) item.

---

## A.8 Implementation gate

Phase 3 execution may begin against **spec + this addendum** once the following are done:

1. §0.4 Assess questions answered -- **DONE**, recorded in A.9. Q5/Q6 (live install tests for GraphFrames and Splink) deferred to the Spark 4.2 UAT window.
2. §0.5 Impact classify pass completed against the amended ENG-* set. Existing table (spec §0.5) requires two updates: ENG-2C.4.6-7 downgrades from Refactor to Extension per Q9 answer; ENG-2C.5 stays Refactor but the risk paragraph is retired per V-12 rescope in A.4.
3. ENG-U-01 pre-check UAT (Spark 4.2 live write cycle + GraphFrames + Splink installability) is scheduled or complete; the default-bump PR does not merge until it passes.

---

## A.9 §0.4 Assess -- answers of record

Answers recorded 2026-09-16 during addendum drafting. Numbering matches spec §0.4.

| # | Question (short) | Answer | ENG impact |
|---|---|---|---|
| 1 | Default Spark version, does Spark 4.2 need a bump PR first? | Default is `apache/spark:4.0.2-python3` at 11 sites in `config/recipes.py`. Spark 4.2 bump (ENG-U-01) lands as its own PR gated on UAT (A.2), does not block Financial pipeline authoring on 4.0/4.1 in the meantime. | ENG-U-01 unchanged. |
| 2 | Datagen refactor sub-PR order (extract class -> Generator protocol -> dispatch) | Order confirmed: (a) extract `Customer360Generator` class from `datagen/generate.py`, (b) introduce `Generator` protocol, (c) add dispatch and register both generators. Sequential merges; each PR keeps Customer360 output byte-identical. Lead time not scheduled here; owner picks. | ENG-2C.2 unchanged. |
| 3 | Current CI matrix; Spark 4.2 impact | `.github/workflows/ci.yml` = Python 3.10/3.11/3.12/3.13 only. No Spark version axis in CI (Spark runs on cluster). Spark 4.2 adds two UAT rows (batch + sustained), zero CI rows. | No CI change; UAT matrix +2 rows. |
| 4 | Tests referencing `WorkloadSchema.CUSTOMER360`/`CUSTOM` -- regression surface | Actual grep: **1** ref (`config/schema.py:921`, field default). Spec's "~34" is stale. Regression surface for adding `FINANCIAL` is one enum + one default site. | ENG-U-05 shrinks; V-01 note in A.1 could add the count. |
| 5 | GraphFrames 0.9.3+ installable against target Spark | **VERIFIED 2026-09-16 against `apache/spark:4.2.0-python3`.** PyPI package name is `graphframes-py` (0.12.2), NOT `graphframes` (which is a stale 0.6 placeholder). Install command: `pip install --user graphframes-py typing_extensions`. Import needs `PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*-src.zip`. The spec's "0.9.3+" version target is stale; current release is 0.12.2 (2026-08-28). | ENG-U-02: update package name, pin `graphframes-py>=0.12.2`, add `typing_extensions` to image install list, document PYTHONPATH. |
| 6 | Splink 4.0.x installable against target Spark | **VERIFIED 2026-09-16 against `apache/spark:4.2.0-python3`.** `pip install --user splink>=4.0.0,<5` resolves to `splink==4.0.17`. Install requires `HOME=/tmp` (or any writable path) because Spark image sets HOME=/nonexistent. Import ok. | ENG-U-03: set `HOME=/tmp` in Dockerfile/pod env before pip install; pin `splink>=4.0.17,<5`. |
| 7 | Iceberg version; Iceberg v3 / W10 need a bump? | Current default `1.11.0` (`IcebergConfig.version` at `schema.py:443`). Covers Java 17 + native Spark 4.1 runtime + W10 time-travel + REQ-U-04. **No bump needed for v1 Financial.** Revisit if Iceberg v3 format features are wanted (1.12+ territory). | ENG-U-04 no version bump; W10 uses 1.11.0. |
| 8 | Smallest schema-aware patch to `_run_iceberg_maintenance()` | Function at `cli/_sustained.py:213`, called from `cli/_run.py:1336` with `"0s"`. Patch: add optional `retention_workload: WorkloadRetentionPolicy \| None = None` param; when present, per-table retention overrides the flat threshold. Additive: default (Customer360) unchanged. Requires new `WorkloadRetentionPolicy` field on the workload config. | ENG-R-05 file list: `cli/_sustained.py`, `cli/_run.py`, `config/schema.py`. |
| 9 | `BENCHMARK_QUERIES` refactor path | **Option (a)**: split into `BENCHMARK_QUERIES_BY_DOMAIN` dict keyed by `WorkloadSchema`. No `ModuleRegistry` dependency. Ships in v1 Financial without waiting on v1.4 registry-driven deployment work. | ENG-2C.4.6-7 downgrades Refactor -> Extension. |
| 10 | Report generator domain-hook strategy | Resolved in addendum A.4: `ScorecardBlock` interface + extract `Customer360ScorecardBlock` first + add `FinancialScorecardBlock`. | ENG-2C.5 rescoped in A.4. |
| 11 | `financial` slot repurpose | **Option A**: replace the `financial_dimensions()` stub, add `WorkloadSchema.FINANCIAL`. No parallel key. Existing stub has no generator; docs already promise Financial; one canonical schema. | ENG-U-05 unchanged. |
| 12 | Local mode scope for Financial | **Scale <= 1, graph workloads (W6/W7) skipped in `local-` recipes.** Aligns with existing `LOCAL_SCALE_ADVISORY_MAX = 1.0`. Non-graph workloads run locally at scale 1 for developer sanity. | `local-` prefixed Financial recipes ship in `examples/`; graph workloads gated on non-local. |
| 13 | Autosizer fields | `resolve_auto_sizing(config, cluster_capacity)` in `config/autosizer.py:97` reads only `config.architecture.workload.datagen.scale`. Financial branch: dispatch on `config.architecture.workload.schema_type` for executor profiles. Additive; Customer360 unchanged. | ENG-2C.10 as-drafted. |
| 14 | `distribution_mode` inheritance for Financial | `spark.lb.silver.distribution_mode` is a Spark conf, read at `spark/scripts/silver_build.py:348`. Set on the SparkApplication spec, not Python-plumbed. Financial `silver_build` inherits automatically via the same K8s runtime path (addendum A.3). No extra plumbing. | ENG-U-06 unchanged. |

**Sequencing summary for Phase 3.** The gate items above translate to a merge order:

1. Datagen sub-PRs (Q2 order) -- three sequential PRs, each Customer360 byte-identical.
2. `WorkloadSchema.FINANCIAL` enum + `financial_dimensions()` real implementation (Q4, Q11).
3. `BENCHMARK_QUERIES_BY_DOMAIN` split (Q9).
4. `_run_iceberg_maintenance` schema-aware patch (Q8).
5. `ScorecardBlock` protocol + Customer360 extraction (A.4), then Financial block.
6. Autosizer schema branch (Q13).
7. Financial pipeline scripts under `runtime.protocol.Runtime` (A.3).
8. Local-mode Financial recipes (Q12) once (1)-(7) land.
9. Spark 4.2 default bump (ENG-U-01), gated on UAT including Q5/Q6 install tests, independent of the Financial chain above.

---

## A.10 §0.5 Impact classify -- overrides

Two rows in the spec's §0.5 impact table are corrected by the answers in A.9. This section is the authoritative override; where it disagrees with §0.5, this section wins.

| Block | Original impact | Override | Reason |
|---|---|---|---|
| ENG-2C.4.6-7 (investigator queries) | Refactor | **Extension** | Q9 chose dict-split over query-provider protocol. `BENCHMARK_QUERIES_BY_DOMAIN` is a dict-keyed lookup, not a new mechanism; Customer360 list moves under `WorkloadSchema.CUSTOMER360` unchanged; `runner.py` gains one dict lookup. No cross-cutting effect. |
| ENG-2C.5 (scoring and metrics) | Refactor | **Refactor** (unchanged classification, revised risk) | Classification stays Refactor because a new protocol is introduced. Risk paragraph "`reports/generator.py` is 92K of Customer360-hardcoded rendering with no domain hooks" is retired per A.4 and V-12: the file is already ~1 hardcoded string. Byte-identical acceptance still required. |

All other rows in §0.5 stand as written.
