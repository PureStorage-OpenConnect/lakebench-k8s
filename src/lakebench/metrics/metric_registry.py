"""One source of metric metadata: unit, direction, band, modes, workloads
and Lakebench caps a metric depends on.

Readers: ``lakebench compare`` (which deltas carry a better side),
``lakebench reproduce`` and the perf gate (``reproduce_class``), the HTML
report's "higher is better" hints (``direction_hint``), and the collector's
``score_descriptions``. Still outside it: compare's QpH query-set check and
its "capped" rendering, and the perf gate's own rule that continuous stage
seconds are not measurements (later work moves them here). A score emitted
with no entry fails ``tests/test_metric_registry.py``.

``band`` decides who uses a metric:

- ``performance``: a rate or a time; feeds deltas, reproduce tolerance and
  the perf gate.
- ``correctness``: exact in reproduce (``scale_ratio``, best at 1.0).
- ``guard``: a range the run must sit in (``ingest_ratio``).
- ``diagnostic``, ``config_bound``, ``label`` and ``result``: shown, never
  directional. ``config_bound`` follows a configured value (a continuous
  stream stage runs for the whole window); ``result`` is a workload result
  (AML recall, false positives), never a directional delta.

``direction`` is ``higher``, ``lower``, ``target`` (best at a value, never
a winner direction) or ``none``. A metric is directional, so a delta in it
can be better or worse, only when its band is ``performance`` and its
direction is ``higher`` or ``lower``.

``cap_dependence`` lists the bound kinds (``limits.bound_kinds``, as
``metrics/experiment._bound_kinds`` writes them; ``*`` matches any job
type, and ``{job}`` the job type of the stage a key names) that, when they
bound a run, bound this metric: it is then capped, not infrastructure
performance. ``capped_by`` takes the trickle, which is not a bound kind, as
``extra``. No reader calls ``capped_by`` yet: compare still labels a delta
capped when any limit bound either run.

Some keys mean different things by mode (a continuous stream stage's
seconds are the window length; continuous core-hours scale with it): they
have one entry per mode, and ``lookup`` takes the run's mode. ``lookup``
refuses ``mode=None`` for a key whose unit, direction or band differs by
mode (``ModeRequired``), and answers it with every mode's caps when only the
caps differ; reproduce and the perf gate, which read numbers without a
mode, use ``reproduce_class``.

History of band and direction changes to published metrics (to be named in
UPGRADING-1.7.md):

- 1.7: compare had derived direction from name tokens. ``qph_degradation_pct``
  is lower is better (was higher). Not directional (were higher or lower):
  ``qph_spread``, ``maintenance_value_pct``, ``window_seconds``,
  ``benchmark_rounds_count``, ``benchmark_samples_per_query``,
  ``total_rows_processed``, ``bronze_busy_fraction``, ``corpus_ingest_ratio``,
  ``query_time_event_age_seconds``, ``arrival_seconds``,
  ``window_arrival_fraction``, ``pre_window_rows``, ``released_rows``,
  ``corpus_drain_seconds``, the time-to-detect alert and cycle counts,
  ``maintenance_pct_of_pipeline``, ``maintenance_paired_queries``,
  ``maintenance_settle_seconds``, the maintenance file and snapshot counts,
  ``storage_reclaimed_mb``, and in a continuous run ``total_core_hours`` and
  ``total_elapsed_seconds``. ``compaction_ratio`` is higher (was lower) and
  diagnostic; ``ingest_ratio`` is a guard, best inside [0.95, 1.05] (was
  higher). The reproduce and perf-gate classification of every metric they
  extract is unchanged.
"""

from __future__ import annotations

import fnmatch
import re
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Literal

Direction = Literal["higher", "lower", "target", "none"]
Band = Literal[
    "performance", "correctness", "guard", "diagnostic", "config_bound", "result", "label"
]

#: The units a metric may carry.
UNITS = frozenset(
    {
        "s",
        "ms",
        "rows/s",
        "rows",
        "GB/s",
        "GB",
        "MB",
        "MB/s",
        "QpH",
        "ratio",
        "count",
        "pct",
        "bool",
        "text",
        "core-h",
        "GB/core-h",
        "cpu-h/TB",
        # A dict or list of figures (shown, never a delta).
        "struct",
    }
)

BATCH = "batch"
CONTINUOUS = "sustained"
_BATCH = frozenset({BATCH})
_CONT = frozenset({CONTINUOUS})
_BOTH = frozenset({BATCH, CONTINUOUS})
_ALL_WL = frozenset({"customer360", "financial"})
_AML = frozenset({"financial"})

# Bound kinds come from metrics/bounds.py (the one registry of kinds);
# BOUND_TRICKLE is not a kind, and a reader passes it to ``capped_by`` as
# ``extra`` when ``bounds.trickle_bound`` holds.
from lakebench.metrics.bounds import (  # noqa: E402
    BOUND_AUTOSIZE,
    BOUND_EXECUTOR_BUDGET,
    BOUND_EXECUTOR_CAP,
    BOUND_EXECUTOR_OVERRIDE,
    BOUND_MAINTENANCE,
    BOUND_RULE_CAP,
    BOUND_TM_ALERTS,
    BOUND_TRICKLE,
)

#: Caps on batch pipeline times, throughput and compute: executor counts
#: (cap, budget, a binding override), auto-sizing, and AML rule and TM caps (a skipped rule makes gold faster).
_PIPELINE_CAPS = (
    BOUND_EXECUTOR_CAP,
    BOUND_EXECUTOR_BUDGET,
    BOUND_EXECUTOR_OVERRIDE,
    BOUND_AUTOSIZE,
    BOUND_RULE_CAP,
    BOUND_TM_ALERTS,
)
#: Caps on continuous latencies (freshness, time to detect) and compute:
#: executor counts and auto-sizing. The trickle bounds intake, so it caps the
#: throughputs only (``_INTAKE_CAPS``).
_STREAM_CAPS = (
    BOUND_EXECUTOR_CAP,
    BOUND_EXECUTOR_BUDGET,
    BOUND_EXECUTOR_OVERRIDE,
    BOUND_AUTOSIZE,
)
_INTAKE_CAPS = (*_STREAM_CAPS, BOUND_TRICKLE)
_AML_CAPS = (BOUND_RULE_CAP, BOUND_TM_ALERTS)


@dataclass(frozen=True)
class MetricMeta:
    id: str
    unit: str
    direction: Direction
    band: Band
    modes: frozenset[str]
    workloads: frozenset[str]
    cap_dependence: tuple[str, ...] = ()
    guard_range: tuple[float, float] | None = None
    #: ``ml_loop`` metrics are assessed only between equal loop groups.
    group: Literal["pipeline", "ml_loop"] = "pipeline"
    #: ``scores``: emitted under ``pipeline_benchmark.scores`` (and listed in
    #: ``score_descriptions``); ``derived``: computed by reproduce or the perf
    #: gate from elsewhere in the record; ``ml_loop``: in the record's
    #: ``ml_loop`` block.
    source: Literal["scores", "derived", "ml_loop"] = "scores"
    #: A median over in-stream rounds: when the rounds executed different
    #: query sets (``scores.composite_qph_basis.blended``) the number blends
    #: them and is not assessed between runs.
    blended_by_rounds: bool = False
    description: str = ""

    @property
    def directional(self) -> bool:
        """Whether a delta in this metric has a better side."""
        return self.band == "performance" and self.direction in ("higher", "lower")


_ENTRIES: tuple[MetricMeta, ...] = (
    MetricMeta(
        "total_core_hours",
        "core-h",
        "lower",
        "performance",
        _BATCH,
        _ALL_WL,
        _PIPELINE_CAPS,
        description="Total CPU core-hours of requested compute (executor_count x cores x elapsed / 3600). Uses per-job profile cores from K8s manifest, not the global executor.cores config value",
    ),
    MetricMeta(
        "total_core_hours",
        "core-h",
        "none",
        "config_bound",
        _CONT,
        _ALL_WL,
        _STREAM_CAPS,
        description="Total CPU core-hours of requested compute (executor_count x cores x elapsed / 3600). Uses per-job profile cores from K8s manifest, not the global executor.cores config value",
    ),
    MetricMeta(
        "compute_efficiency_gb_per_core_hour",
        "GB/core-h",
        "higher",
        "performance",
        _BATCH,
        _ALL_WL,
        _PIPELINE_CAPS,
        description="GB processed per core-hour of requested compute (higher is better)",
    ),
    MetricMeta(
        "compute_efficiency_gb_per_core_hour",
        "GB/core-h",
        "higher",
        "performance",
        _CONT,
        _ALL_WL,
        _INTAKE_CAPS,
        description="GB processed per core-hour of requested compute (higher is better)",
    ),
    MetricMeta(
        "total_data_processed_gb",
        "GB",
        "none",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="Sum of input data across all pipeline stages in GB",
    ),
    MetricMeta(
        "pipeline_throughput_gb_per_second",
        "GB/s",
        "higher",
        "performance",
        _BATCH,
        _ALL_WL,
        _PIPELINE_CAPS,
        description="Total data / wall-clock time in GB/s (higher is better)",
    ),
    MetricMeta(
        "pipeline_throughput_gb_per_second",
        "GB/s",
        "higher",
        "performance",
        _CONT,
        _ALL_WL,
        _INTAKE_CAPS,
        description="Total data / wall-clock time in GB/s (higher is better)",
    ),
    MetricMeta(
        "total_elapsed_seconds",
        "s",
        "lower",
        "performance",
        _BATCH,
        _ALL_WL,
        _PIPELINE_CAPS,
        description="Wall-clock seconds from pipeline start to final stage completion",
    ),
    MetricMeta(
        "total_elapsed_seconds",
        "s",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Wall-clock seconds from pipeline start to final stage completion",
    ),
    MetricMeta(
        "composite_qph",
        "QpH",
        "higher",
        "performance",
        _BATCH,
        _ALL_WL,
        (BOUND_MAINTENANCE, *_AML_CAPS),
        description="Queries per Hour -- median of in-stream rounds or single benchmark (higher is better)",
    ),
    MetricMeta(
        "composite_qph",
        "QpH",
        "higher",
        "performance",
        _CONT,
        _ALL_WL,
        _AML_CAPS,
        blended_by_rounds=True,
        description="Queries per Hour -- median of in-stream rounds or single benchmark (higher is better)",
    ),
    MetricMeta(
        "composite_qph_rounds",
        "count",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Continuous. In-stream benchmark rounds whose median is composite_qph (rounds with a QpH); 0 means composite_qph is the single post-stream benchmark. Runs with different counts are not like-for-like",
    ),
    MetricMeta(
        "composite_qph_basis",
        "struct",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Continuous. Whether composite_qph blends in-stream rounds that executed different query sets (blended), and the rounds per executed query set id (sets); 'not_recorded' for rounds that did not record their executed set",
    ),
    MetricMeta(
        "composite_qph_by_set",
        "struct",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Continuous. Median in-stream QpH per executed query set id",
    ),
    MetricMeta(
        "data_freshness_seconds",
        "s",
        "lower",
        "performance",
        _CONT,
        _ALL_WL,
        (*_STREAM_CAPS, *_AML_CAPS),
        description="Primary freshness score. Worst-case gold table staleness during the streaming window in seconds (lower is better)",
    ),
    MetricMeta(
        "sustained_throughput_rps",
        "rows/s",
        "higher",
        "performance",
        _CONT,
        _ALL_WL,
        _INTAKE_CAPS,
        description="Rows bronze ingested inside the measurement window per second of the window that data was arriving (arrival_seconds), higher is better. Rows ingested before the window opened are not counted. When intake_limit is trickle_rate this is the configured offered load, not a capacity",
    ),
    MetricMeta(
        "window_seconds",
        "s",
        "none",
        "config_bound",
        _CONT,
        _ALL_WL,
        (),
        description="Length of the measurement window: from the moment every stream's driver was running, run_duration seconds",
    ),
    MetricMeta(
        "arrival_seconds",
        "s",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Seconds of the window data was still arriving at bronze: the whole window while corpus was left, else until one bronze trigger after its last write inside the window",
    ),
    MetricMeta(
        "window_arrival_fraction",
        "ratio",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="arrival_seconds / window_seconds. Below 1 the corpus ran out inside the window; the window after that measured an idle pipeline",
    ),
    MetricMeta(
        "pre_window_rows",
        "rows",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Rows bronze ingested before the window opened (a stream that started while another waited to submit). Not part of any window score",
    ),
    MetricMeta(
        "ingest_ratio",
        "ratio",
        "target",
        "guard",
        _CONT,
        _ALL_WL,
        (),
        guard_range=(0.95, 1.05),
        description="Bronze rows ingested by the window's end / rows the trickle had released to bronze by then (released_rows: max_files_per_trigger files per bronze trigger since bronze's first write, at the corpus's mean rows per file, capped at the corpus). 1.0 = bronze kept up with what arrived. Falls back to corpus_ingest_ratio when released_rows is unknown",
    ),
    MetricMeta(
        "corpus_ingest_ratio",
        "ratio",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Bronze rows ingested by the window's end / datagen rows produced: the share of the whole corpus taken. Below 1 on a default run, whose trickle is sized to outlast the window",
    ),
    MetricMeta(
        "released_rows",
        "rows",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Rows the trickle had made available to bronze by the window's end (ingest_ratio's denominator); null when the corpus file count or window is unknown",
    ),
    MetricMeta(
        "stage_latency_profile",
        "struct",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Average micro-batch processing time per stage {bronze_ms, silver_ms, gold_ms} in ms",
    ),
    MetricMeta(
        "pipeline_saturated",
        "bool",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="True when ingest_ratio < 0.95: bronze fell behind the rows the trickle released. False when intake_limit is trickle_rate: the configured trickle, not the pipeline, bounded intake",
    ),
    MetricMeta(
        "intake_limit",
        "text",
        "none",
        "label",
        _CONT,
        _ALL_WL,
        (),
        description="What bounded intake when ingest_ratio < 0.95: bronze_capacity (bronze busy for most of the window; sustained_throughput_rps is its capacity), trickle_rate (bronze ran a micro-batch on nearly every trigger, each inside the trigger, with corpus left: the configured max_files_per_trigger per trigger bounded intake and the pipeline kept pace), below_bronze_capacity (bronze had idle time without that pattern: a late start or a stall), none (kept up: ingest_ratio >= 0.95). Whether the trickle held intake (BOUNDED BY trickle: "
        "ingested / offered rows >= 0.99 and lag within one trigger) is experiment.limits.trickle_bound",
    ),
    MetricMeta(
        "corpus_drain_seconds",
        "s",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (BOUND_TRICKLE,),
        description="When intake_limit is trickle_rate: seconds the trickle needs to ingest the whole corpus at the rate it held (datagen rows / sustained_throughput_rps); a window this long drains it",
    ),
    MetricMeta(
        "bronze_busy_fraction",
        "ratio",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Share of the window bronze spent inside micro-batches (batches x mean batch time / window)",
    ),
    MetricMeta(
        "time_to_detect_seconds",
        "s",
        "lower",
        "performance",
        _CONT,
        _AML,
        (*_STREAM_CAPS, *_AML_CAPS),
        description="AML continuous. Median seconds from the newest bronze ingest of an alert's related transactions to the commit of the rule's alerts on the tick that first raised it (lower is better)",
    ),
    MetricMeta(
        "time_to_detect_p95_seconds",
        "s",
        "lower",
        "performance",
        _CONT,
        _AML,
        (*_STREAM_CAPS, *_AML_CAPS),
        description="AML continuous. 95th percentile of time_to_detect (histogram bin upper edge, 10 s bins)",
    ),
    MetricMeta(
        "time_to_detect_max_seconds",
        "s",
        "lower",
        "performance",
        _CONT,
        _AML,
        (*_STREAM_CAPS, *_AML_CAPS),
        description="AML continuous. Longest time to detect of any newly raised alert",
    ),
    MetricMeta(
        "time_to_detect_alerts",
        "count",
        "none",
        "diagnostic",
        _CONT,
        _AML,
        (),
        description="AML continuous. Newly raised alerts the time to detect is measured over",
    ),
    MetricMeta(
        "time_to_detect_late_alerts",
        "count",
        "none",
        "diagnostic",
        _CONT,
        _AML,
        (),
        description="AML continuous. Measured alerts whose related transactions were all in silver before the previous detection pass (re-raised after a rule error, or evidence outside related_txn_ids); included in the percentiles",
    ),
    MetricMeta(
        "time_to_detect_unmeasured_cycles",
        "count",
        "none",
        "diagnostic",
        _CONT,
        _AML,
        (),
        description="AML continuous. Gold cycles that logged no time-to-detect line; their alerts are measured on the next cycle, late by one cycle",
    ),
    MetricMeta(
        "corpus_drained",
        "bool",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="True when every datagen row reached bronze and silver committed all of them before the window ended: freshness covers only gold cycles that saw new data, and arrival_seconds stops at bronze's last write",
    ),
    MetricMeta(
        "total_rows_processed",
        "rows",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Rows taken in across all streaming stages inside the measurement window (gold re-reads of silver included)",
    ),
    MetricMeta(
        "total_s3_objects",
        "count",
        "none",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="Total S3 objects across bronze/silver/gold buckets at end of run. Growth rate vs retention capacity is the key signal -- if this grows unbounded, metadata ops degrade",
    ),
    MetricMeta(
        "query_time_event_age_seconds",
        "s",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Diagnostic, not freshness. Median age of the newest event date in gold at benchmark query time (query time minus MAX(interaction_date), day resolution). It tracks where the corpus's event timestamps sit, not how stale gold is; data_freshness_seconds is the freshness score. Replaces query_time_freshness_seconds, which carried this figure under a freshness name",
    ),
    MetricMeta(
        "in_stream_composite_qph",
        "QpH",
        "higher",
        "performance",
        _CONT,
        _ALL_WL,
        _AML_CAPS,
        blended_by_rounds=True,
        description="Median QpH from in-stream benchmark rounds",
    ),
    MetricMeta(
        "benchmark_rounds_count",
        "count",
        "none",
        "diagnostic",
        _CONT,
        _ALL_WL,
        (),
        description="Number of in-stream benchmark rounds executed",
    ),
    MetricMeta(
        "qph_degradation_pct",
        "pct",
        "lower",
        "performance",
        _CONT,
        _ALL_WL,
        (),
        blended_by_rounds=True,
        description="QpH degradation from first-half to second-half of sustained run (positive = slower, negative = faster)",
    ),
    MetricMeta(
        "qph_degradation_withheld",
        "text",
        "none",
        "label",
        _CONT,
        _ALL_WL,
        (),
        description="Continuous. Why qph_degradation_pct is not recorded though the run has four or more rounds: the rounds ran different query sets, so the two halves time different work",
    ),
    MetricMeta(
        "time_to_value_seconds",
        "s",
        "lower",
        "performance",
        _BATCH,
        _ALL_WL,
        _PIPELINE_CAPS,
        description="Wall-clock seconds from first input to queryable gold (lower is better)",
    ),
    MetricMeta(
        "scale_ratio",
        "ratio",
        "target",
        "correctness",
        _BATCH,
        _ALL_WL,
        (),
        description="Actual data volume / expected volume for the scale factor (1.0 = complete)",
    ),
    MetricMeta(
        "time_to_value_datagen_excluded_seconds",
        "s",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description=(
            "Multi-cycle batch: seconds of the cycles' datagen inside the time-to-value "
            "span, left out of time_to_value_seconds"
        ),
    ),
    MetricMeta(
        "cycle_progression",
        "struct",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="Per-cycle elapsed time, QpH, and table health for multi-cycle batch runs",
    ),
    MetricMeta(
        "maintenance_elapsed_seconds",
        "s",
        "lower",
        "performance",
        _BOTH,
        _ALL_WL,
        (BOUND_MAINTENANCE,),
        description="Total seconds spent on expire_snapshots + remove_orphan_files + compaction",
    ),
    MetricMeta(
        "maintenance_stopped",
        "bool",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="True when pre-benchmark maintenance stopped early (a statement timed out or the 30 min cap hit); a statement may still have been running, so post_compaction_qph is not a clean measurement and maintenance_value_pct is null",
    ),
    MetricMeta(
        "maintenance_stop_reason",
        "text",
        "none",
        "label",
        _BATCH,
        _ALL_WL,
        (),
        description="Why pre-benchmark maintenance stopped",
    ),
    MetricMeta(
        "maintenance_live_streams",
        "bool",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="True when stream apps were present (or could not be read) during pre-benchmark maintenance; the post-maintenance QpH was measured with writers live and is not gated",
    ),
    MetricMeta(
        "maintenance_live_streams_reason",
        "text",
        "none",
        "label",
        _BATCH,
        _ALL_WL,
        (),
        description="Which stream apps were present, and any read errors",
    ),
    MetricMeta(
        "maintenance_pct_of_pipeline",
        "pct",
        "none",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="Maintenance time as percentage of total pipeline time",
    ),
    MetricMeta(
        "pre_compaction_file_count",
        "count",
        "none",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="Iceberg data files before rewrite_data_files / OPTIMIZE",
    ),
    MetricMeta(
        "post_compaction_file_count",
        "count",
        "none",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="Iceberg data files after rewrite_data_files / OPTIMIZE",
    ),
    MetricMeta(
        "compaction_ratio",
        "ratio",
        "higher",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="pre/post file count ratio (higher = more compaction benefit)",
    ),
    MetricMeta(
        "snapshots_expired",
        "count",
        "none",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="Number of snapshots removed by expire_snapshots",
    ),
    MetricMeta(
        "orphan_files_removed",
        "count",
        "none",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="Number of orphan files cleaned up by remove_orphan_files",
    ),
    MetricMeta(
        "storage_reclaimed_mb",
        "MB",
        "none",
        "diagnostic",
        _BOTH,
        _ALL_WL,
        (),
        description="MB of storage freed by maintenance operations",
    ),
    MetricMeta(
        "pre_compaction_qph",
        "QpH",
        "higher",
        "performance",
        _BATCH,
        _ALL_WL,
        _AML_CAPS,
        description="QpH measured before maintenance (on uncompacted data)",
    ),
    MetricMeta(
        "post_compaction_qph",
        "QpH",
        "higher",
        "performance",
        _BATCH,
        _ALL_WL,
        (BOUND_MAINTENANCE, *_AML_CAPS),
        description="QpH measured after maintenance (on compacted data) -- the primary QpH score",
    ),
    MetricMeta(
        "maintenance_value_pct",
        "pct",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="QpH change from maintenance over the queries that succeeded in both runs: (post - pre) / pre * 100; null when not measurable or within the within-round spread",
    ),
    MetricMeta(
        "maintenance_value_reason",
        "text",
        "none",
        "label",
        _BATCH,
        _ALL_WL,
        (),
        description="Why maintenance_value_pct is null: not measurable, one sample per query, or within noise",
    ),
    MetricMeta(
        "benchmark_samples_per_query",
        "count",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="Timed samples per query in the scored benchmark round (QpH uses the per-query median; 1 means no measured spread)",
    ),
    MetricMeta(
        "qph_spread",
        "struct",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="QpH of the slowest and fastest round the per-query samples allow, and their relative range",
    ),
    MetricMeta(
        "maintenance_paired_queries",
        "count",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="Queries that succeeded before and after maintenance (the base of maintenance_value_pct)",
    ),
    MetricMeta(
        "maintenance_settle_seconds",
        "s",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="Seconds from maintenance end until a storage-bound probe query was stable, before the post-maintenance round; not counted in time_to_value",
    ),
    MetricMeta(
        "maintenance_settled",
        "bool",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="True when the probe settled within the cap; false means the post round ran on unsettled storage and maintenance_value_pct is null",
    ),
    MetricMeta(
        "maintenance_settle_capped",
        "bool",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="True when the settle wait reached benchmark.maintenance_settle.max_seconds",
    ),
    MetricMeta(
        "maintenance_settle_verified",
        "bool",
        "none",
        "diagnostic",
        _BATCH,
        _ALL_WL,
        (),
        description="False when there was no pre-maintenance probe time (scale >= 50): the probes agreed with each other, which a slow plateau also does",
    ),
    MetricMeta(
        "datagen_cpu_hr_per_tb",
        "cpu-h/TB",
        "lower",
        "performance",
        _BOTH,
        _ALL_WL,
        (),
        source="derived",
        description="Datagen CPU-hours per TB written (fleet aggregate)",
    ),
    MetricMeta(
        "datagen_aggregate_mbps",
        "MB/s",
        "higher",
        "performance",
        _BOTH,
        _ALL_WL,
        (),
        source="derived",
        description="Datagen fleet write throughput in MB/s",
    ),
    MetricMeta(
        "datagen_mbps_per_pod",
        "MB/s",
        "higher",
        "performance",
        _BOTH,
        _ALL_WL,
        (),
        source="derived",
        description="Datagen write throughput per pod in MB/s",
    ),
)

#: Renamed keys a stored record may carry, to the key they became.
ALIASES: dict[str, str] = {
    # Carried the event-age figure under a freshness name until 1.6.
    "query_time_freshness_seconds": "query_time_event_age_seconds",
}

#: Batch stage short names (``PipelineBenchmark`` stages) to the job type
#: whose caps bound them. Continuous stages use the same short names.
STAGE_JOB_TYPES: dict[str, str] = {
    "bronze": "bronze-verify",
    "silver": "silver-build",
    "gold": "gold-finalize",
    "datagen": "datagen",
    "query": "query",
}
_STAGE_CAPS = (
    "{job}: executor cap",
    "{job}: concurrent executor budget",
    BOUND_AUTOSIZE,
    BOUND_RULE_CAP,
    BOUND_TM_ALERTS,
)

#: Open-namespace keys: (pattern, entries). ``{job}`` in a cap is replaced
#: by the stage's job type.
PATTERNS: tuple[tuple[re.Pattern[str], tuple[MetricMeta, ...]], ...] = (
    (
        re.compile(r"^query_qph_.+$"),
        (
            # The batch per-query QpH comes from the post-maintenance round,
            # as composite_qph does.
            MetricMeta(
                "query_qph_<query>",
                "QpH",
                "higher",
                "performance",
                _BATCH,
                _ALL_WL,
                (BOUND_MAINTENANCE, *_AML_CAPS),
                description="One query's QpH (3600 / its median seconds)",
            ),
            MetricMeta(
                "query_qph_<query>",
                "QpH",
                "higher",
                "performance",
                _CONT,
                _ALL_WL,
                _AML_CAPS,
                description="One query's QpH (3600 / its median seconds)",
            ),
        ),
    ),
    (
        # Stages a continuous run also times for real: datagen and the
        # post-window query benchmark.
        re.compile(r"^(?P<stage>datagen|query)_seconds$"),
        (
            MetricMeta(
                "<stage>_seconds",
                "s",
                "lower",
                "performance",
                _BOTH,
                _ALL_WL,
                _STAGE_CAPS,
                description="One stage's elapsed seconds",
            ),
        ),
    ),
    (
        re.compile(r"^(?P<stage>bronze|silver|gold)_seconds$"),
        (
            MetricMeta(
                "<stage>_seconds",
                "s",
                "lower",
                "performance",
                _BATCH,
                _ALL_WL,
                _STAGE_CAPS,
                description="One batch stage's elapsed seconds",
            ),
            MetricMeta(
                "<stage>_seconds",
                "s",
                "none",
                "config_bound",
                _CONT,
                _ALL_WL,
                description=(
                    "One continuous stream stage's seconds: the stream ran for the whole "
                    "window, so this is the window length, not a measurement"
                ),
            ),
        ),
    ),
)


def _by_id(entries: Iterable[MetricMeta]) -> dict[str, tuple[MetricMeta, ...]]:
    out: dict[str, tuple[MetricMeta, ...]] = {}
    for m in entries:
        out[m.id] = (*out.get(m.id, ()), m)
    return out


_BY_ID = _by_id(_ENTRIES)


def _merged(entries: tuple[MetricMeta, ...]) -> MetricMeta:
    """The first (batch) entry of a mode-split key with every entry's modes,
    caps and ``blended_by_rounds``: ``lookup(key, None)`` when the entries agree on unit,
    direction and band, and ``reproduce_class`` always."""
    first = entries[0]
    if len(entries) == 1:
        return first
    caps: list[str] = []
    for m in entries:
        caps += [c for c in m.cap_dependence if c not in caps]
    modes = frozenset().union(*(m.modes for m in entries))
    return replace(
        first,
        modes=modes,
        cap_dependence=tuple(caps),
        blended_by_rounds=any(m.blended_by_rounds for m in entries),
    )


#: Every exact key and its entries, one per mode set (most keys have one).
METRICS: dict[str, tuple[MetricMeta, ...]] = dict(_BY_ID)


def canonical_mode(mode: str | None) -> str | None:
    """``batch``, ``sustained`` (also spelled ``continuous``) or None.
    Raises ValueError on anything else, so a typo cannot pick the batch
    entry silently."""
    if mode is None or mode == BATCH:
        return mode
    if mode in ("continuous", CONTINUOUS):
        return CONTINUOUS
    raise ValueError(f"unknown pipeline mode {mode!r}")


class ModeRequired(ValueError):
    """``lookup(key, None)`` on a key whose meaning depends on the mode."""


def _pick(entries: tuple[MetricMeta, ...], mode: str | None, key: str) -> MetricMeta:
    if mode is None:
        if len({(m.unit, m.direction, m.band) for m in entries}) > 1:
            raise ModeRequired(f"metric {key} differs by mode; pass the run's mode")
        # Same meaning in every mode (only the caps differ): every mode's caps.
        return _merged(entries)
    for m in entries:
        if mode in m.modes:
            return m
    # Registered for the other mode only: the key is not emitted in this
    # one. Its single entry, rather than None, keeps a stored record's stray
    # key readable.
    if len(entries) == 1:
        return entries[0]
    raise ModeRequired(f"metric {key} has no {mode} entry")


def lookup(key: str, mode: str | None) -> MetricMeta | None:
    """The metadata of score *key* in *mode* (``batch``, ``sustained`` or
    ``continuous``), or None for a key the registry does not know (a reader
    then treats it as not directional). *mode* has no default; None is
    accepted only for a key whose meaning does not depend on the mode, and
    raises ``ModeRequired`` otherwise, so no reader gets one mode's answer
    for the other's record."""
    mode = canonical_mode(mode)
    key = ALIASES.get(key, key)
    entries = _BY_ID.get(key)
    if entries:
        return _pick(entries, mode, key)
    for pattern, metas in PATTERNS:
        m = pattern.match(key)
        if not m:
            continue
        meta = replace(_pick(metas, mode, key), id=key)
        stage = m.groupdict().get("stage")
        if stage:
            job = STAGE_JOB_TYPES[stage]
            meta = replace(
                meta, cap_dependence=tuple(c.format(job=job) for c in meta.cap_dependence)
            )
        return meta
    return None


def _lookup_or_none(key: str, mode: str | None) -> MetricMeta | None:
    """``lookup``, with a mode-split key under an unknown mode read as
    unknown (no better side), for readers that only colour or label."""
    try:
        return lookup(key, mode)
    except ModeRequired:
        return None


def is_directional(key: str, mode: str | None) -> bool:
    """Whether a delta in *key* has a better side; False for an unknown key
    and for a mode-split key when the mode is unknown."""
    meta = _lookup_or_none(key, mode)
    return bool(meta and meta.directional)


def higher_is_better(key: str, mode: str | None) -> bool:
    meta = _lookup_or_none(key, mode)
    return bool(meta and meta.directional and meta.direction == "higher")


def direction_hint(key: str, mode: str | None) -> str:
    """``higher is better``, ``lower is better``, or "" for a metric with
    no better side."""
    meta = _lookup_or_none(key, mode)
    if meta is None or not meta.directional:
        return ""
    return f"{meta.direction} is better"


def capped_by(
    key: str, bound_kinds: Iterable[str], mode: str | None, *, extra: Iterable[str] = ()
) -> list[str]:
    """The kinds in *bound_kinds* (``limits.bound_kinds``) and *extra* that
    cap *key*'s value; with *mode* None, against every mode's caps. *extra*
    carries a bound the record states elsewhere: ``BOUND_TRICKLE`` when the
    trickle held intake."""
    meta = _mode_free(key) if mode is None else lookup(key, mode)
    if meta is None:
        return []
    return [
        b
        for b in (*bound_kinds, *extra)
        if any(fnmatch.fnmatchcase(b, c) for c in meta.cap_dependence)
    ]


def descriptions() -> dict[str, str]:
    """Score key -> description, as metrics.json ``score_descriptions``."""
    return {k: v[0].description for k, v in METRICS.items() if v[0].source == "scores"}


def _mode_free(key: str) -> MetricMeta | None:
    """The batch entry with every mode's caps (``_merged``); reproduce and
    the perf gate read stored numbers without a mode."""
    key = ALIASES.get(key, key)
    if key in _BY_ID:
        return _merged(_BY_ID[key])
    for pattern, metas in PATTERNS:
        if pattern.match(key):
            return replace(_merged(metas), id=key)
    return None


def reproduce_class(key: str) -> tuple[str, str]:
    """``(band, direction)`` in the vocabulary reproduce and the perf gate
    use: band ``correctness`` (the registry's correctness and guard bands)
    or ``performance`` (every other band); direction ``higher`` or
    ``lower`` for a directional metric, else ``exact`` (either way of
    drift counts). Mode-free, as those callers are. A key the registry does
    not know keeps the rule those callers had: a ``*_seconds`` key is lower
    is better, anything else exact."""
    meta = _mode_free(key)
    if meta is None:
        if key.endswith("_seconds"):
            return ("performance", "lower")
        return ("performance", "exact")
    if meta.band in ("correctness", "guard"):
        return ("correctness", "exact")
    return ("performance", meta.direction if meta.directional else "exact")


def _check(entries: Iterable[MetricMeta]) -> None:
    for m in entries:
        if m.unit not in UNITS:
            raise ValueError(f"metric {m.id}: unknown unit {m.unit!r}")
        if (m.band == "guard") != (m.guard_range is not None):
            raise ValueError(f"metric {m.id}: a guard band needs a guard_range, and only it")


_check(_ENTRIES)
_check(m for _p, metas in PATTERNS for m in metas)
