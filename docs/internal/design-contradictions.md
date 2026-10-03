# Design contradictions

Where the implementation or published docs disagree with `docs/DESIGN.md`.
Maintainer material: `docs/internal/` is excluded from the source
distribution. Citations are file:line against integrate/v1.5.0 at 70b4cf2 and
will drift; the symbol names are the stable reference.

Ordered by impact on the mission. **[owner]** needs a product decision;
**[impl]** is implementation work inside the model. **[decided]** marks an
owner decision already taken (see the decision log at the end); the item then
remains open only as implementation work.

1. **Nothing records whether compared runs produced the same results.**
   `_build_comparison` (`cli/_compare.py:299-375`) refuses QpH across query
   sets and, by design, only warns on sample count and maintenance policy
   (`:339-355`); the perf gate refuses on config and volume guards
   (`metrics/perf_gate.py:22-23`) and `lakebench reproduce` refuses on policy
   (`cli/_reproduce.py` `_policy_refusal`), but none compares workload
   results. The mission makes such a comparison invalid. The cross-engine
   row-count differences seen so far were harness counting bugs (Thrift
   beeline flag order, DuckDB progress-bar output), not data differences;
   they are being fixed on lane/compare-equiv. Resolution: record per-query
   result fingerprints and per-stage row counts and digests in metrics.json,
   and mark mismatched comparisons invalid [impl]. [decided D1] `compare`
   shows the evidence, gives a NOT COMPARABLE verdict, suppresses any winner
   or performance conclusion, and exits non-zero.

2. **Queries are never checked for non-empty or correct results.**
   `rows_returned` (`benchmark/runner.py:37`) is only displayed
   (`cli/_query.py:196`, `reports/generator.py:1896`);
   `_benchmark_gate_problems` (`cli/_run.py:667-694`) checks only `success`.
   A query returning 0 rows or a wrong answer counts toward QpH.
   Resolution: fail a query with an empty result where the set declares one
   impossible, and add expected-result checks per workload query set [impl].

3. **The supported set is far larger than the evidence behind it.**
   [decided D3]
   `_SUPPORTED_COMBINATIONS` (`config/schema.py:1232-1253`) has 11 tuples, and
   both workloads and both modes are accepted on each: about 44 experiment
   shapes. The tuple list expresses architecture validity only. DESIGN.md 6.5
   requires workload and mode compatibility plus a live, correct end-to-end
   run for "supported"; most combinations have no such run, for example AML
   on Polaris, Thrift, DuckDB or no query engine, and AML continuous.
   Remaining work [impl]: declare workload and mode compatibility, compute
   the support state (supported / unverified / unsupported) per workload x
   mode x architecture, and stamp it and the comparability level
   (comparable, like-for-like) in the evidence and compare output. Owner
   decision 2026-09-26: unverified runs may be reported and compared when
   their correctness checks pass, with status visible, never as proof of
   support.

4. **AML on Delta is accepted and runs Iceberg code.** [impl] The tuple list
   has no workload axis; `job.py:1774-1797` picks financial scripts before
   checking format, and they hard-code `USING iceberg`
   (`silver_build_financial.py:189`, `deploy/financial_ddl.py:114`).
   Resolution: workloads declare supported formats and modes; reject at load.

5. **Maintenance work differs by composition.** [decided D5] DuckDB and Delta with
   Thrift skip maintenance (`cli/_sustained.py:964-978`); Delta OPTIMIZE is
   skipped for Trino and Thrift (`cli/_sustained.py:1116-1125`); the run keeps
   the full maintenance policy id, which marks only `--skip-maintenance`.
   The owner rejected treating maintenance as workload semantics: it is an
   execution condition, and runs with different effective policies are not
   like-for-like. Remaining work [impl]: always stamp the effective policy
   (what actually ran, not what was requested) in the evidence, and make any
   difference from the request, and between compared runs, visible in the
   report and in comparisons.

6. **Customer 360 has no expected-result definition.** [decided D6, resolved]
   Expected results are derived from the generator in
   `metrics/c360_correctness.py`, and the owner approved 16 of them as
   gating on 2026-09-27 (`GATING_CHECKS`, read by the CLI and the verdict's
   `c360` gate). A batch run on the cluster is gated; continuous runs keep
   the zero-row gate (`cli/_sustained.py`) and no expected-result check.

7. **A benchmark exception leaves the run successful.** [impl] Existing gates
   cover failed queries and AML zero alerts, but an exception in the
   benchmark block is a warning and `pipeline_success` stays true
   (`cli/_run.py:2320-2324`); the report also shows a scale ratio of 0 as
   "Complete" (`reports/generator.py:700-704`). Resolution: record the
   benchmark as not run, withhold QpH, and render an unmeasured ratio as
   unmeasured.

8. **Evidence does not identify corpus, system or effective conditions.**
   [impl] `build_config_snapshot` (`metrics/collector.py:1750-1863`) omits the
   datagen seed and corpus role (recorded only by the AML reference score,
   `score_financial_reference.py:628`), format and catalog versions, recipe
   name, image digests, Kubernetes version and storage backend. Skipped
   stages other than maintenance are not recorded. `config_sha256` is read by
   the perf gate (`perf_gate.py:656`) and written nowhere. Resolution: add
   these to the snapshot and to the perf-gate fingerprint.

9. **Recorded benchmark settings are not what ran.** [impl] The snapshot
   records `benchmark.mode`, `streams` and `cache` (`collector.py:1846-1848`);
   `lakebench run` always calls `run_power(cache="hot", ...)`
   (`cli/_run.py:2237-2239`). Resolution: record what executed.

10. **Caps are not recorded as caps.** [impl] Whether `_MAX_EXECUTORS_SAFE`
    or a job's `max_executors` (`job.py:49`, `:62`) bound, and the value of
    `PRE_BENCHMARK_MAINTENANCE_CAP` (`cli/_run.py:43`), are absent from
    metrics.json; only settle and maintenance stops are flagged
    (`collector.py:786`). Resolution: a `caps` section with value and "bound"
    per cap, shown in the report.

11. **Nothing labels `n=1`.** [impl] Baselines are one run
    (`perf_gate.py:1120`), compare's noise floor is a fixed figure
    (`cli/_compare.py:156-159`), and pipeline scores carry no repetition
    count. Resolution: sample count on every published figure.

12. **Workload is nested in architecture and has no object.** [decided D12] `ArchitectureConfig.workload` and `.tables`
    (`config/schema.py:1596-1598`) hold workload identity and table names;
    `financial_table_defaults` (`:1623-1648`, unreachable code `:1649-1654`)
    rewrites names from an architecture validator; dispatch is
    `== "financial"` across `cli/_run.py`, `deploy/datagen.py:63`, `job.py`
    and `config/scale.py`; local mode hard-codes c360 tables
    (`local_job.py:196`, `cli/_local.py`). Remaining work [impl]: make
    `workload` a top-level config key, accept the old location with a
    deprecation warning, and add one Workload definition looked up by name.

13. **Custom workloads fall back to Customer 360.** [decided D13]
    `WorkloadSchema.CUSTOM` maps to the c360 query set
    (`benchmark/queries.py:616`) and unknown schemas fall back to it.
    Remaining work [impl]: reject `custom` at config load in v1.6.

14. **DuckDB bypasses the catalog, and the access path is not recorded.**
    [decided D14] `duckdb/executor.py:210-237` rewrites tables to
    `iceberg_scan`/`delta_scan` on a guessed path. Under DESIGN.md 2.2 the
    access path is part of the architecture. Remaining work [impl]: record
    `query_access_path` (for example `catalog` or `direct_storage`) in the
    evidence; allow whole-composition comparison when results match; refuse
    to attribute a difference to a single component when access paths
    differ.

15. **`deploy` can still install shared operators and observability.**
    [impl] DESIGN.md 2.1 says shared infrastructure is administered
    separately and `deploy` creates only experiment-owned resources. Two
    opt-in paths contradict it. The Spark and Stackable operators install from
    `deploy` when their `install` option is set (`SparkOperatorConfig.install`,
    `StackableOperatorConfig.install`, `config/schema.py:341`, `:442`, default
    False). With `observability.enabled` (default off), `deploy` still
    installs kube-prometheus-stack, a category 3 chart with CRDs and cluster
    roles, but only when no release exists anywhere on the cluster, into the
    shared `lakebench-observability` namespace and under the cluster lease
    (`ObservabilityDeployer.deploy` / `_deploy_locked`); an existing release
    is reused and never modified. `destroy` no longer uninstalls the shared
    release; it removes only a pre-v1.6 release installed into the
    deployment's own namespace. The remaining gap: `deploy`, not `admin`,
    performs the first install. Resolution: move these installs to
    `lakebench admin` and have `deploy` verify only.

16. **Workload sizing lives in the pipeline-engine module.** [impl]
    `_JOB_PROFILES` (`job.py:51`) is c360-shaped, patched by
    `_SCHEMA_PROFILE_OVERRIDES["financial"]` (`job.py:211`); AML-only job
    types register for every workload (`job.py:1798-1806`). Resolution:
    resource demands belong to the workload definition.

17. **The registry and part of the protocol surface are unused.** [impl]
    `modules/registry.py:1-10` states deploy and destroy do not use it;
    `TableFormatModule.get_pipeline_scripts` (`modules/base.py:188`) has no
    implementation or caller. Resolution: route through the registry, or
    delete.

18. **The code deprecates the product's mode name.** [decided D18]
    `PipelineMode.SUSTAINED` (`config/schema.py:164-175`); `--continuous` is
    a hidden alias of `--sustained` (`cli/_run.py:1109-1120`);
    `pipeline.continuous` is deprecated (`config/schema.py:910-926`);
    `ProcessingPattern` (`:126-133`) adds "streaming", read only by the
    autosizer (`config/autosizer.py:558`). Remaining work [impl]: make
    `continuous` canonical with `sustained` as a transitional alias, keep old
    metrics readable, and remove `ProcessingPattern`.

19. **Config fields nothing reads.** [decided D19]
    `ImagesConfig.prometheus` and `.grafana` (`config/schema.py:210-211`),
    `ReportsConfig` (`:1711-1722`), `IcebergConfig.file_format` and
    `.properties` (`:549-550`) look like controls and change nothing.
    Remaining work [impl]: warn on use in v1.6; remove in v1.7.

20. **Published docs disagree with the supported set.** [impl]
    `docs/architecture.md` and `docs/supported-components.md:122` omit the
    Hive + Delta recipes; `docs/compatibility-matrix.md:175` presents Unity +
    Delta as working though no Unity combination is supported; the config
    template lists a nonexistent `iot` schema (`config/loader.py:664`).
    Resolution: generate these tables from `_SUPPORTED_COMBINATIONS` and
    `RECIPES`.

21. **Support has a scale layer DESIGN 6.5 does not name.** [owner]
    DESIGN.md 6.5 judges support over workload x mode x architecture in four
    layers. The code adds datagen scale bands per workload
    (`config/support.py` `DATAGEN_SCALE_BANDS`, owner decision 2026-09-29,
    "per-workload bands"): Customer 360 is supported up to scale 300,
    unverified up to 600 and unsupported (refused) above; AML (financial) is
    supported up to 300, unverified up to 800 and unsupported above. The
    ceiling is where a datagen pod would exceed the 16 GiB per-pod memory cap.
    `support_state` caps the architecture-level state with the band, and
    `deploy` and `generate` refuse a config above the ceiling. Resolution
    pending owner text for DESIGN.md 6.5 describing the scale layer.

## Owner decisions, 2026-09-26

- **D1** (item 1), accepted. `compare` shows the evidence, gives a NOT
  COMPARABLE verdict, suppresses any winner or performance conclusion, and
  exits non-zero when the runs are not comparable.
- **D3** (item 3), amended. Support has three states, supported, unverified
  and unsupported, judged over workload x mode x architecture.
- **D5** (item 5), rejected as proposed. Maintenance policy is an execution
  condition, not workload semantics. Runs with different effective
  maintenance policies are not like-for-like. When a composition cannot
  execute the requested policy (DuckDB cannot run maintenance, for example),
  the effective policy is stamped and the difference is visible.
- **D6** (item 6), accepted. Customer 360 expected results are derived by the
  implementation; the owner approves their meaning before they gate.
  Approved 2026-09-27: checks 0 to 14 and 17 of the expected-results table
  gate a batch run (`metrics/c360_correctness.py` `GATING_CHECKS`); the other
  statistical checks and the benchmark row-count checks report only.
- **D12** (item 12), accepted. `workload` is a top-level config key; the old
  location is accepted with a deprecation warning.
- **D13** (item 13), accepted. Reject `custom` in v1.6.
- **D14** (item 14), amended. The architecture includes the access paths
  between components; the access path is recorded in evidence. Whole
  compositions are comparable when results match; single-component
  attribution is not valid when access paths differ.
- **D18** (item 18), accepted. `continuous` is canonical; `sustained` is a
  transitional alias.
- **D19** (item 19), accepted. Warn in v1.6, remove in v1.7.
