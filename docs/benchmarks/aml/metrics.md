# AML benchmark: metrics

See also: continuous metrics in [8.2](continuous-metrics.md#82-continuous); scoring in [8.4](scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails); TM operations in [8.5](tm-operations.md#85-tm-operations).

## 8. Metrics

Units, directions and bands come from the metric registry
(`metrics/metric_registry.py`), which `reproduce` and the report read.
`pipeline_benchmark.score_descriptions` describes each score a run recorded
in one line. Direction `none`: a delta has no better side. In `metrics.json`:

| Block | Holds |
|---|---|
| `pipeline_benchmark.scores` | pipeline scores |
| `financial_scoring` (top level) | AML scoring |
| `tm_operations` (top level) | TM operations |
| `storage_multiple` (top level) | the storage multiple |

## 8.1 Batch

Primary: `time_to_value_seconds`, lower is better.

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `time_to_value_seconds` | s | lower | wall clock from the start of the first pipeline job (bronze-verify) to the end of the last (gold-finalize); a job with no recorded end counts at its start. Datagen, maintenance, settle and the benchmark are outside it |
| `time_to_value_seconds`, recorded | s | -- | 874.51 s at scale 1 and 5,086.03 s at scale 10, n=1 each (1.6, not comparable with 1.7) |
| `time_to_value_datagen_excluded_seconds` | s | none | diagnostic, multi-cycle Customer 360 batch only: the cycles' datagen seconds left out of `time_to_value_seconds`. Never on an AML record (`collector.py` sets it for customer360 only) |
| `total_elapsed_seconds` | s | lower | the run's wall clock: datagen, stages and the gaps between them, maintenance and the benchmark |
| `total_data_processed_gb` | GiB | none | sum of the GiB each job reported reading (bronze the raw files, silver the bronze input, gold silver's current snapshot); never a bucket listing; the query stage reports no input |
| `pipeline_throughput_gb_per_second` | GiB/s | higher | `total_data_processed_gb / time_to_value_seconds` |
| `total_core_hours` | core-h | lower | requested executors x cores x elapsed / 3600 over the bronze, silver and gold jobs (drivers, datagen, query engine and maintenance excluded) |
| `compute_efficiency_gb_per_core_hour` | GiB/core-h | higher | `total_data_processed_gb / total_core_hours` |
| `scale_ratio` | ratio | target 1.0 | bronze input GB of the last bronze-verify job / expected bronze GB for the scale; a batch run below 0.95 fails |
| `composite_qph` | QpH | higher | power QpH of the scored round after maintenance: successful queries / sum of their per-query median seconds x 3600 |
| `pre_compaction_qph`, `post_compaction_qph` | QpH | higher | the pre- and post-maintenance rounds, same query set; left out of the scorecard when maintenance changed nothing to compare |
| `maintenance_value_pct`, `maintenance_value_reason`, `maintenance_paired_queries` | pct, text, count | none | the before-and-after change from the pre to the post round over queries that succeeded in both; null within noise, with the reason |
| `benchmark_samples_per_query`, `qph_spread` | count, struct | none | samples behind the medians; QpH of the slowest and fastest sample combination |
| `maintenance_stopped`, `maintenance_stop_reason` | bool, text | none | pre-benchmark maintenance stopped on a statement timeout or its budget, and why |
| `maintenance_live_streams`, `maintenance_live_streams_reason` | bool, text | none | stream apps were present, or unreadable, during pre-benchmark maintenance |
| `maintenance_settle_seconds`, `maintenance_settled`, `maintenance_settle_capped`, `maintenance_settle_verified` | s, bool | none | the settle wait ([7.1](execution-rules.md#71-required)); not in time to value. `maintenance_settle_verified` is false at scale 50 and above, where no pre round ran |
| `cycle_progression` | struct | none | per-cycle elapsed, QpH and table health (multi-cycle) |

`compute_efficiency_gb_per_core_hour` divides by requested core-hours, not
used ones: a pod requesting 8 cores and using 2 reads 4 x less efficient than
one requesting and using 2, for identical work. Use it for regressions between
releases on one config, not to compare stacks sized differently.

## 8.3 Both modes

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `snapshots_expired`, `orphan_files_removed`, `storage_reclaimed_mb` | count, count, MB | none | reserved for what table maintenance removed; no code path fills them in this release, so they never appear in a record |
| `maintenance_elapsed_seconds` | s | lower | maintenance and compaction time (batch pre-benchmark, or continuous in-window) |
| `maintenance_pct_of_pipeline` | pct | none | maintenance time as a share of `total_elapsed_seconds` |
| `pre_compaction_file_count`, `post_compaction_file_count` | count | none | data files before and after compaction |
| `compaction_ratio` | ratio | higher | pre over post file count; diagnostic |
| `total_s3_objects` | count | none | objects across the three buckets at run end (continuous: window end); unbounded growth means maintenance is not keeping up |
| `datagen_aggregate_mbps`, `datagen_mbps_per_pod` | MB/s | higher | datagen fleet and per-pod write throughput |
| `datagen_cpu_hr_per_tb` | cpu-h/TB | lower | datagen CPU-hours per TB written |
| `storage_multiple_total` | ratio | lower, diagnostic (never a directional delta) | physical over logical table bytes at run end, measured once after maintenance and only when the run passed (`storage_multiple.total.multiple`); a condition of the maintenance policy, not a system score |

`reproduce` derives the three datagen figures from the datagen record and
`storage_multiple_total` from the `storage_multiple` block; none is under
`scores`. Per-stage and per-query figures (`<stage>_seconds`,
`query_qph_<query>`) are recorded beside these and follow the stage and query definitions above.
