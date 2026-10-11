# Comparing runs

Reference: how to judge whether two runs are a fair comparison.

Lakebench does not compare runs: read both reports' Experiment sections (the `experiment` block of `metrics.json`). Check in order:

1. **Both runs passed** ([verdict.md](verdict.md)). A run not PASSED has no performance to compare.
2. **Same workload and data.** Workload and version, corpus id (generator, seed and scale) and mode are equal. The identity digest hashes workload, corpus, mode, execution conditions and system, not the recipe or its components: check the recipe and component rows too.
3. **Same answers.** The query set and every query's result fingerprint are equal; for AML batch, every rule's alert count and hash in the Alert set table too. Different fingerprints: not comparable. A run without checked results (`--skip-benchmark`, a `*-none` recipe, a continuous run whose result check did not settle) cannot show this.
4. **Same execution conditions.** Effective maintenance and its compaction operation, maintenance settings, benchmark iterations, in-stream rounds and the binding [Lakebench caps](../glossary.md#caps) ([maintenance.md](maintenance.md)). A difference here is not attributable to the architecture. Maintenance skipped on every operation on both sides counts as the same, so an Iceberg and a Delta run both with `--skip-maintenance` compare on architecture.
5. **One thing varied:** the architecture (recipe, components and versions, query access path, dependency set, Spark executor or driver overrides, user Spark conf) or the system (cluster and object store, `experiment.system_identity`). When both differ, neither explains a difference. Pre-1.7 records have no system identity.

Before reading the numbers:

- A number bounded by a Lakebench cap or the [trickle](../glossary.md#trickle) is labelled and is not a measure of the infrastructure.
- With `run --repeat 3` on each side, compare medians and ranges.
- `experiment.observed`: allocatable CPU and memory and other namespaces' pod requests at run start and at save.
- Both runs must show the expected volume: `scale_ratio` (batch) or `ingest_ratio` (continuous).

**By hand.** Once the `experiment` blocks match and results are equivalent, diff the two `metrics.json` files:

| Field | Use |
|---|---|
| `scorecard.time_to_value_seconds` | primary batch score |
| `scorecard.pipeline_throughput_gb_per_second` | throughput |
| `scorecard.composite_qph` | query performance |
| `stage_matrix` | per-stage breakdown |
| `config_snapshot` | the exact configuration (scale, executors, memory, engine workers, every tuning parameter), to tie a difference to a change |
