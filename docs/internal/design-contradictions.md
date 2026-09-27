# Design contradictions

Where the implementation or published docs disagree with `docs/DESIGN.md`.
Maintainer material: `docs/internal/` is excluded from the source
distribution. Citations are file:line against integrate/v1.5.0 at 70b4cf2 and
will drift; the symbol names are the stable reference.

Ordered by impact on the mission. **[owner]** needs a product decision;
**[impl]** is implementation work inside the model.

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
   and mark mismatched comparisons invalid [impl]. Whether `compare`
   hard-refuses instead of warning is a CLI contract change [owner];
   recommendation: refuse the performance rows, keep the rest visible.

2. **Queries are never checked for non-empty or correct results.**
   `rows_returned` (`benchmark/runner.py:37`) is only displayed
   (`cli/_query.py:196`, `reports/generator.py:1896`);
   `_benchmark_gate_problems` (`cli/_run.py:667-694`) checks only `success`.
   A query returning 0 rows or a wrong answer counts toward QpH.
   Resolution: fail a query with an empty result where the set declares one
   impossible, and add expected-result checks per workload query set [impl].

3. **The supported set is far larger than the evidence behind it.** [owner]
   `_SUPPORTED_COMBINATIONS` (`config/schema.py:1232-1253`) has 11 tuples, and
   both workloads and both modes are accepted on each: about 44 experiment
   shapes. DESIGN.md 6.5 requires a live, correct end-to-end run for a
   combination to be supported; most have none, for example AML on Polaris,
   Thrift, DuckDB or no query engine, and AML continuous. Recommendation: the
   release declares a small supported set per workload (for example
   hive-iceberg-spark-trino and polaris-iceberg-spark-trino for both, plus
   the Delta and Thrift recipes for Customer 360) backed by release-tree runs;
   everything else the config accepts is labelled "unverified" in the
   evidence and excluded from published comparisons.

4. **AML on Delta is accepted and runs Iceberg code.** [impl] The tuple list
   has no workload axis; `job.py:1774-1797` picks financial scripts before
   checking format, and they hard-code `USING iceberg`
   (`silver_build_financial.py:189`, `deploy/financial_ddl.py:114`).
   Resolution: workloads declare supported formats and modes; reject at load.

5. **Maintenance work differs by composition.** [owner] DuckDB and Delta with
   Thrift skip maintenance (`cli/_sustained.py:964-978`); Delta OPTIMIZE is
   skipped for Trino and Thrift (`cli/_sustained.py:1116-1125`); the run keeps
   the full maintenance policy id, which marks only `--skip-maintenance`.
   Compositions therefore do not execute equivalent work. Whether maintenance
   is part of the workload's work or of the architecture's cost is
   measurement meaning. Recommendation: treat it as part of the workload,
   stamp the effective policy in the evidence, and mark comparisons across
   different effective policies invalid.

6. **Customer 360 has no expected-result definition.** [impl, with owner
   sign-off on what "correct" means] Non-empty guards exist
   (`bronze_verify.py:139`, `silver_build.py:449`, `gold_finalize.py:313`) and
   the continuous zero-row gate (`cli/_sustained.py:48`), but a zero KPI count
   is only logged and nothing checks gold KPIs against what the generator
   produced. Resolution: derive expected aggregates from the generator and
   check them in gold-finalize.

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

12. **Workload is nested in architecture and has no object.** [impl; moving
    the YAML key is owner] `ArchitectureConfig.workload` and `.tables`
    (`config/schema.py:1596-1598`) hold workload identity and table names;
    `financial_table_defaults` (`:1623-1648`, unreachable code `:1649-1654`)
    rewrites names from an architecture validator; dispatch is
    `== "financial"` across `cli/_run.py`, `deploy/datagen.py:63`, `job.py`
    and `config/scale.py`; local mode hard-codes c360 tables
    (`local_job.py:196`, `cli/_local.py`). Resolution: one Workload
    definition looked up by name.

13. **Custom workloads fall back to Customer 360.** [owner]
    `WorkloadSchema.CUSTOM` maps to the c360 query set
    (`benchmark/queries.py:616`) and unknown schemas fall back to it.
    Recommendation: reject `custom` until workloads can be declared.

14. **DuckDB bypasses the catalog.** [owner] `duckdb/executor.py:210-237`
    rewrites tables to `iceberg_scan`/`delta_scan` on a guessed path.
    Recommendation: keep the recipes, label them catalog-bypassing in the
    evidence, exclude them from catalog comparisons.

15. **Observability deploys shared cluster objects from `deploy`.** [impl]
    When enabled (default off, `config/schema.py:1733`),
    `deploy/observability.py:143-158` installs kube-prometheus-stack, a
    category 3 chart with CRDs and cluster roles, without stamp or lock;
    destroy uninstalls it (`deploy/destroy.py:2028-2037`). Resolution: install
    via `lakebench admin` like the other operators.

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

18. **The code deprecates the product's mode name.** [owner]
    `PipelineMode.SUSTAINED` (`config/schema.py:164-175`); `--continuous` is
    a hidden alias of `--sustained` (`cli/_run.py:1109-1120`);
    `pipeline.continuous` is deprecated (`config/schema.py:910-926`);
    `ProcessingPattern` (`:126-133`) adds "streaming", read only by the
    autosizer (`config/autosizer.py:558`). Recommendation: make `continuous`
    canonical, keep `sustained` as an alias, remove `ProcessingPattern`.

19. **Config fields nothing reads.** [owner: config contract]
    `ImagesConfig.prometheus` and `.grafana` (`config/schema.py:210-211`),
    `ReportsConfig` (`:1711-1722`), `IcebergConfig.file_format` and
    `.properties` (`:549-550`) look like controls and change nothing.
    Recommendation: a deprecation warning on use for one release, then
    removal with a clear error.

20. **Published docs disagree with the supported set.** [impl]
    `docs/architecture.md` and `docs/supported-components.md:122` omit the
    Hive + Delta recipes; `docs/compatibility-matrix.md:175` presents Unity +
    Delta as working though no Unity combination is supported; the config
    template lists a nonexistent `iot` schema (`config/loader.py:664`).
    Resolution: generate these tables from `_SUPPORTED_COMBINATIONS` and
    `RECIPES`.
