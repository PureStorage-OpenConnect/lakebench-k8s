"""Metrics collection for Lakebench."""

from __future__ import annotations

import logging
import re
import statistics
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, ClassVar

logger = logging.getLogger(__name__)


@dataclass
class JobMetrics:
    """Metrics for a single Spark job."""

    job_name: str
    job_type: str  # bronze-verify, silver-build, gold-finalize
    start_time: datetime | None = None
    end_time: datetime | None = None
    elapsed_seconds: float = 0.0
    success: bool = False
    error_message: str | None = None

    # Data metrics
    input_size_gb: float = 0.0
    output_size_gb: float = 0.0
    input_rows: int = 0
    output_rows: int = 0

    # Resource metrics (allocation from job profile, not runtime utilization)
    executor_count: int = 0
    executor_cores: int = 0
    executor_memory_gb: float = 0.0
    cpu_seconds_requested: float = 0.0  # exec_count * cores * elapsed
    memory_gb_requested: float = 0.0  # exec_count * (mem + overhead)

    # Throughput
    throughput_gb_per_second: float = 0.0
    throughput_rows_per_second: float = 0.0

    # Detection rules (AML gold-finalize only). Empty for c360 and for
    # non-gold jobs. Populated from ``[detection] {rule_id}: alerts=N ...``
    # lines the driver emits per rule; a rule that crashed shows up as
    # ``alerts=0`` with ``rule_errors[rule_id]`` carrying the exception.
    # A rule that declined to run for a structural reason (e.g. W1 above
    # its vertex cap) shows up in ``rules_skipped[rule_id]`` with the skip
    # reason, and is deliberately ABSENT from ``alerts_by_rule`` so a skip
    # is never read as a zero-recall result (LB-119).
    alerts_by_rule: dict[str, int] = field(default_factory=dict)
    rule_errors: dict[str, str] = field(default_factory=dict)
    rules_skipped: dict[str, str] = field(default_factory=dict)
    # TM operations layer (GOALS P10, AML gold only), from the driver's
    # ``[tm-invariant]`` and ``[tm-ops]`` lines. ``tm_invariants`` is keyed
    # by cycle (as a string, the JSON key) then invariant name, each value
    # {"status", "detail"}. ``tm_ops`` is the last operations summary.
    tm_invariants: dict[str, dict[str, dict[str, str]]] = field(default_factory=dict)
    tm_ops: dict[str, Any] | None = None
    # ``[tm-status]`` lines by cycle: {"status", "reason"}; says whether the
    # layer ran and why not.
    tm_status: dict[str, dict[str, str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        if self.start_time:
            d["start_time"] = self.start_time.isoformat()
        if self.end_time:
            d["end_time"] = self.end_time.isoformat()
        return d


@dataclass
class StreamingJobMetrics:
    """Metrics for a streaming Spark job.

    For gold-refresh, ``total_rows_processed`` counts cumulative re-reads of
    silver (e.g. 5 cycles × 1M rows = 5M).  ``unique_rows_processed`` tracks
    the actual distinct input volume (the silver table size).  Bronze and
    silver stages set both fields to the same value since they don't re-read.
    """

    job_name: str
    job_type: str  # bronze-ingest, silver-stream, gold-refresh
    throughput_rps: float = 0.0
    freshness_seconds: float = 0.0
    # LB-145. Gold cycles tagged "(silver idle)" saw no new silver data. Only
    # the TRAILING run of idle cycles (after the last cycle that saw data) can
    # be a drained corpus; an idle stretch followed by new data is a stall and
    # stays in freshness_active_seconds. None when no cycle is outside the
    # trailing run.
    freshness_active_seconds: float | None = None
    trailing_idle_cycles: int = 0
    # Silver only: rows of micro-batches that logged a commit. None when no
    # commit line was seen (unknown, so a run cannot count as drained).
    committed_rows: int | None = None
    micro_batch_duration_ms: float = 0.0
    batch_size: int = 0
    total_batches: int = 0
    total_rows_processed: int = 0
    unique_rows_processed: int = 0
    elapsed_seconds: float = 0.0
    success: bool = False
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class CycleMetrics:
    """Per-cycle metrics for multi-cycle batch runs.

    Each cycle captures its own datagen window, per-stage job metrics,
    optional benchmark result, and Iceberg table health snapshot.
    """

    cycle_index: int
    timestamp_start: str = ""
    timestamp_end: str = ""
    datagen_elapsed_seconds: float = 0.0
    datagen_output_gb: float = 0.0
    jobs: list[JobMetrics] = field(default_factory=list)
    benchmark: BenchmarkMetrics | None = None
    table_health: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "cycle_index": self.cycle_index,
            "timestamp_start": self.timestamp_start,
            "timestamp_end": self.timestamp_end,
            "datagen_elapsed_seconds": round(self.datagen_elapsed_seconds, 2),
            "datagen_output_gb": round(self.datagen_output_gb, 3),
            "jobs": [j.to_dict() for j in self.jobs],
            "benchmark": self.benchmark.to_dict() if self.benchmark else None,
            "table_health": self.table_health,
        }


@dataclass
class BenchmarkRoundMeta:
    """Per-round metadata for in-stream benchmark rounds.

    Tracks which benchmark round this is, when it ran, the gold table
    freshness measured at query time, and whether Q9 (the only gold-table
    query) hit a contention window from ``createOrReplace()``.
    """

    round_index: int
    timestamp: datetime | None = None
    gold_freshness_seconds: float = 0.0
    q9_contention_observed: bool = False
    q9_retry_used: bool = False
    # Table health metrics (v1.1.0) -- captured at benchmark time
    silver_data_file_count: int = 0
    silver_snapshot_count: int = 0
    gold_data_file_count: int = 0
    gold_snapshot_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d: dict[str, Any] = {
            "round_index": self.round_index,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "gold_freshness_seconds": round(self.gold_freshness_seconds, 2),
            "q9_contention_observed": self.q9_contention_observed,
            "q9_retry_used": self.q9_retry_used,
        }
        # Only include table health when populated (non-zero)
        if self.silver_data_file_count or self.gold_data_file_count:
            d["table_health"] = {
                "silver_data_file_count": self.silver_data_file_count,
                "silver_snapshot_count": self.silver_snapshot_count,
                "gold_data_file_count": self.gold_data_file_count,
                "gold_snapshot_count": self.gold_snapshot_count,
            }
        return d


@dataclass
class QueryMetrics:
    """Metrics for a single query execution."""

    query_name: str  # e.g. "rfm", "cohort", or custom SQL hash
    query_text: str
    elapsed_seconds: float = 0.0
    rows_returned: int = 0
    success: bool = False
    error_message: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "query_name": self.query_name,
            "query_text": self.query_text,
            "elapsed_seconds": self.elapsed_seconds,
            "rows_returned": self.rows_returned,
            "success": self.success,
            "error_message": self.error_message,
        }


@dataclass
class PipelineMetrics:
    """Metrics for a complete pipeline run."""

    run_id: str
    deployment_name: str
    start_time: datetime
    end_time: datetime | None = None
    total_elapsed_seconds: float = 0.0
    success: bool = False

    # Data size
    bronze_size_gb: float = 0.0
    silver_size_gb: float = 0.0
    gold_size_gb: float = 0.0

    # Job metrics
    jobs: list[JobMetrics] = field(default_factory=list)

    # Query metrics
    queries: list[QueryMetrics] = field(default_factory=list)

    # Streaming job metrics (optional -- populated for sustained pipeline runs)
    streaming: list[StreamingJobMetrics] = field(default_factory=list)

    # Configuration snapshot
    config_snapshot: dict[str, Any] = field(default_factory=dict)

    # Benchmark results (optional -- populated after query benchmark runs)
    benchmark: BenchmarkMetrics | None = None

    # In-stream benchmark rounds (sustained mode only)
    benchmark_rounds: list[BenchmarkMetrics] = field(default_factory=list)

    # Pipeline benchmark (optional -- populated after build_pipeline_benchmark)
    pipeline_benchmark: PipelineBenchmark | None = None

    # Platform metrics (optional -- populated when observability is enabled)
    platform_metrics: dict[str, Any] | None = None

    # Per-cycle metrics (multi-cycle batch runs only)
    cycles: list[CycleMetrics] = field(default_factory=list)

    # Datagen fleet metrics (optional -- populated when the datagen pods
    # emitted LB_METRICS_JSON lines and the aggregator collected them).
    # Shape: dict from FleetSummary.to_dict().
    datagen_fleet: dict[str, Any] | None = None

    # Financial (AML) recall scoring (optional -- populated for a batch
    # financial run when `financial score` is folded into `run` (LB-123)).
    # Shape: the recall.json sidecar written by score_financial.py --
    # {"typologies": [{typology_type, expected_workload, recall,
    # instance_count, detection_status}], "total_alerts", "fp_alerts",
    # "fp_rate", "run_id", "computed_by"}. The scorecard reads this to render
    # per-rule recall/precision; None means recall was not computed.
    financial_scoring: dict[str, Any] | None = None

    # P10 TM operations verdict for the run (metrics/tm_ops.tm_verdict), with
    # the invariants by cycle and the last operations summary. Batch also
    # keeps them per gold-finalize job; continuous has no job record, so this
    # is where its TM section comes from.
    tm_operations: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = {
            "run_id": self.run_id,
            "deployment_name": self.deployment_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "total_elapsed_seconds": self.total_elapsed_seconds,
            "success": self.success,
            "bronze_size_gb": self.bronze_size_gb,
            "silver_size_gb": self.silver_size_gb,
            "gold_size_gb": self.gold_size_gb,
            "jobs": [j.to_dict() for j in self.jobs],
            "queries": [q.to_dict() for q in self.queries],
            "streaming": [s.to_dict() for s in self.streaming],
            "config_snapshot": self.config_snapshot,
        }
        if self.benchmark is not None:
            d["benchmark"] = self.benchmark.to_dict()
        if self.benchmark_rounds:
            d["benchmark_rounds"] = [r.to_dict() for r in self.benchmark_rounds]
        if self.pipeline_benchmark is not None:
            d["pipeline_benchmark"] = self.pipeline_benchmark.to_dict()
        if self.platform_metrics is not None:
            d["platform_metrics"] = self.platform_metrics
        if self.cycles:
            d["cycles"] = [c.to_dict() for c in self.cycles]
        if self.datagen_fleet is not None:
            d["datagen_fleet"] = self.datagen_fleet
        if self.financial_scoring is not None:
            d["financial_scoring"] = self.financial_scoring
        if self.tm_operations is not None:
            d["tm_operations"] = self.tm_operations
        return d


@dataclass
class BenchmarkMetrics:
    """Metrics from a Trino query benchmark run.

    When used as an in-stream benchmark round, ``round_meta`` carries
    per-round metadata (round index, timestamp, freshness at query time,
    Q9 contention status).  For aggregated benchmarks,
    ``round_meta`` is ``None``.
    """

    mode: str  # "power", "throughput", or "composite"
    cache: str  # "hot" or "cold"
    scale: float
    qph: float
    total_seconds: float
    queries: list[dict[str, Any]] = field(default_factory=list)
    iterations: int = 1
    streams: int = 1
    stream_results: list[dict[str, Any]] = field(default_factory=list)
    round_meta: BenchmarkRoundMeta | None = None
    # Identity of the query set QpH was measured over (queries.query_set_id).
    # None: derive it from ``queries`` with today's SQL (a benchmark being
    # recorded now). Records loaded from older metrics.json get their id from
    # queries.legacy_query_set_id instead. "unknown": not comparable.
    query_set_id: str | None = None

    def __post_init__(self) -> None:
        if self.query_set_id is None:
            from lakebench.benchmark.queries import query_set_id

            names = [
                (q.get("name") or q.get("query_name"))
                if isinstance(q, dict)
                else getattr(getattr(q, "query", q), "name", None)
                for q in self.queries or []
            ]
            self.query_set_id = query_set_id(names) if any(names) else "unknown"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d: dict[str, Any] = {
            "benchmark_type": "trino_query",
            "query_set_id": self.query_set_id,
            "mode": self.mode,
            "cache": self.cache,
            "scale": self.scale,
            "qph": round(self.qph, 1),
            "total_seconds": round(self.total_seconds, 2),
            "iterations": self.iterations,
            "streams": self.streams,
            "queries": self.queries,
        }
        if self.queries:
            from lakebench.benchmark.spread import spread

            d["spread"] = spread(self.queries)
        if self.stream_results:
            d["stream_results"] = self.stream_results
        if self.round_meta is not None:
            d["round_meta"] = self.round_meta.to_dict()
        return d


@dataclass
class StageMetrics:
    """Universal per-stage measurement -- the pipeline benchmark's atomic unit.

    Every pipeline stage (datagen, bronze, silver, gold, query) gets one of
    these, regardless of engine (Spark, Flink, Trino, K8s Job).  Consistent
    fields across stages enable component-agnostic comparison.

    Field groups:

    - **Timing**: elapsed_seconds, start_time, end_time, success.
    - **Data volume**: input_size_gb, output_size_gb, input_rows, output_rows.
    - **Throughput** (derived): ``input_size_gb / elapsed_seconds`` and
      ``input_rows / elapsed_seconds``.  Call :meth:`compute_derived` after
      modifying input/elapsed values.
    - **Resources** (allocation): executor_count, executor_cores,
      executor_memory_gb.  Sourced from ``_JOB_PROFILES`` in ``spark/job.py``.
    - **Streaming** (zero for batch): latency_ms, freshness_seconds,
      total_batches, batch_size.
    - **Query** (zero for non-query): queries_executed, queries_per_hour.
    """

    stage_name: str  # "datagen", "bronze", "silver", "gold", "query"
    stage_type: str  # "batch", "streaming", "query", "datagen"
    engine: str  # "spark", "trino", "datagen" -- what ran the stage

    # Timing
    start_time: datetime | None = None
    end_time: datetime | None = None
    elapsed_seconds: float = 0.0
    success: bool = False
    error_message: str | None = None

    # Data volume
    input_size_gb: float = 0.0
    output_size_gb: float = 0.0
    input_rows: int = 0
    output_rows: int = 0

    # Throughput (derived)
    throughput_gb_per_second: float = 0.0
    throughput_rows_per_second: float = 0.0

    # Resources (allocation, not utilization)
    executor_count: int = 0
    executor_cores: int = 0
    executor_memory_gb: float = 0.0

    # Streaming-specific (None = unmeasurable, 0.0 = measured-and-zero)
    latency_ms: float | None = None
    freshness_seconds: float | None = None
    freshness_active_seconds: float | None = None  # all but the trailing idle run (LB-145)
    trailing_idle_cycles: int = 0  # gold cycles after silver last moved
    committed_rows: int | None = None  # silver: rows in committed micro-batches
    total_batches: int = 0
    batch_size: int = 0
    unique_rows_processed: int | None = 0  # distinct input rows; None = unknown (gold re-reads)

    # Query-specific (zero for non-query)
    queries_executed: int = 0
    queries_per_hour: float = 0.0

    def compute_derived(self) -> None:
        """Compute throughput fields from raw measurements."""
        if self.elapsed_seconds > 0:
            if self.input_size_gb > 0:
                self.throughput_gb_per_second = self.input_size_gb / self.elapsed_seconds
            if self.input_rows > 0:
                self.throughput_rows_per_second = self.input_rows / self.elapsed_seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        if self.start_time:
            d["start_time"] = self.start_time.isoformat()
        else:
            d.pop("start_time", None)
        if self.end_time:
            d["end_time"] = self.end_time.isoformat()
        else:
            d.pop("end_time", None)
        return d


@dataclass
class PipelineBenchmark:
    """Full pipeline benchmark -- the unifying scorecard.

    The pipeline benchmark is the single view that makes heterogeneous stages
    (Spark batch, Spark streaming, Trino queries, datagen) comparable across
    runs, scales, and architectures.  Every stage gets a uniform
    :class:`StageMetrics` row; pipeline-level scores aggregate them into the
    numbers that answer "how fast is this lakehouse?"

    Scoring is **mode-conditional**: :meth:`compute_aggregates` detects
    ``pipeline_mode`` and computes the appropriate score set.

    Batch scoring (pipeline_mode="batch")
    --------------------------------------
    Answers: "How fast do we get from raw data to queryable gold?"

    **time_to_value_seconds** (primary score)
        Wall-clock seconds from the first stage's ``start_time`` to the last
        stage's ``end_time``.  Lower is better.  Falls back to
        ``total_elapsed_seconds`` when timestamps are absent.

    **total_elapsed_seconds**
        Sum of every stage's ``elapsed_seconds``.

    **total_data_processed_gb**
        Sum of every stage's ``input_size_gb``.

    **pipeline_throughput_gb_per_second**
        ``total_data_processed_gb / total_elapsed_seconds``.  Higher is better.

    Sustained scoring (pipeline_mode="sustained")
    -----------------------------------------------
    Answers: "How fresh is gold, and how fast are we sustaining it?"

    **data_freshness_seconds** (primary score)
        Worst-case (max) ``freshness_seconds`` across streaming stages.
        This is the staleness of the gold layer -- how far behind real-time
        the queryable data is.  Lower is better.

    **sustained_throughput_rps**
        Sum of ``throughput_rows_per_second`` across streaming stages.
        Aggregate sustained rows/sec the pipeline can maintain.  Higher is
        better.

    **stage_latency_profile**
        List of ``[bronze_avg_ms, silver_avg_ms, gold_avg_ms]`` latencies.
        Per-stage micro-batch processing latency.  Lower is better.

    **total_rows_processed**
        Sum of ``input_rows`` across streaming stages.  Total volume
        processed during the monitoring window.

    **total_elapsed_seconds** (shared)
        Sum of all stage durations (same as batch).

    Stage construction
    ------------------
    :func:`build_pipeline_benchmark` converts flat job lists into stages:

    - **Batch**: ``bronze-verify`` → bronze, ``silver-build`` → silver,
      ``gold-finalize`` → gold.  Output sizes fall back to measured S3 bucket
      sizes.  Gold input falls back to measured ``silver_size_gb`` when the
      Iceberg metadata query returns 0.
    - **Streaming**: ``bronze-ingest`` → bronze, ``silver-stream`` → silver,
      ``gold-refresh`` → gold.  Carries latency, freshness, and batch metrics.
    - **Query**: Created from :class:`BenchmarkMetrics` (Trino QpH benchmark).
    - **Datagen**: Optional; included when ``datagen_elapsed > 0``.

    Resource metrics
    ----------------
    Each stage carries ``executor_count``, ``executor_cores``, and
    ``executor_memory_gb`` sourced from the job profiles in
    ``spark/job.py:_JOB_PROFILES``.  These are **allocated** resources (what
    was requested), not runtime utilization.  Per-job resource totals:

    - ``cpu_seconds_requested = executor_count × cores × elapsed``
    - ``memory_gb_requested = executor_count × (memory + overhead)``

    Per-stage throughput
    --------------------
    ``throughput_gb_per_second = input_size_gb / elapsed_seconds``
    ``throughput_rows_per_second = input_rows / elapsed_seconds``

    JSON output
    -----------
    ``to_dict()`` produces the structure stored under ``pipeline_benchmark``
    in the metrics JSON.  The ``scores`` sub-dict contains mode-appropriate
    keys (batch keys for batch, sustained keys for sustained).
    ``to_matrix()`` produces the ``stage_matrix`` sub-object -- a dict keyed
    by stage name for spreadsheet/comparison use.
    """

    # Human-readable descriptions for every JSON score key.
    # Emitted as "score_descriptions" in to_dict() so downstream tools
    # (and humans reading metrics.json) know what each field means.
    _SCORE_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        # Both modes
        "total_core_hours": "Total CPU core-hours of requested compute (executor_count x cores x elapsed / 3600). Uses per-job profile cores from K8s manifest, not the global executor.cores config value",
        "compute_efficiency_gb_per_core_hour": "GB processed per core-hour of requested compute (higher is better)",
        "total_data_processed_gb": "Sum of input data across all pipeline stages in GB",
        "pipeline_throughput_gb_per_second": "Total data / wall-clock time in GB/s (higher is better)",
        "total_elapsed_seconds": "Wall-clock seconds from pipeline start to final stage completion",
        "composite_qph": "Queries per Hour -- median of in-stream rounds or single benchmark (higher is better)",
        # Sustained
        "data_freshness_seconds": "Primary freshness score. Worst-case gold table staleness during the streaming window in seconds (lower is better)",
        "sustained_throughput_rps": "Rows entering bronze per second (higher is better)",
        "ingest_ratio": "Bronze rows ingested / datagen rows produced (1.0 = all data consumed)",
        "stage_latency_profile": "Average micro-batch processing time per stage {bronze_ms, silver_ms, gold_ms} in ms",
        "pipeline_saturated": "True when ingest_ratio < 0.95 -- pipeline cannot keep pace with input",
        "corpus_drained": "True when the finite corpus was fully ingested before the window ended: freshness covers only cycles that saw new data, and sustained_throughput_rps is a lower bound",
        "total_rows_processed": "Cumulative rows processed across all streaming stages",
        "total_s3_objects": "Total S3 objects across bronze/silver/gold buckets at end of run. Growth rate vs retention capacity is the key signal -- if this grows unbounded, metadata ops degrade",
        "query_time_freshness_seconds": "Diagnostic. Median gold staleness measured at benchmark query time (lower is better). Gap between this and data_freshness_seconds indicates freshness variability.",
        "in_stream_composite_qph": "Median QpH from in-stream benchmark rounds",
        "benchmark_rounds_count": "Number of in-stream benchmark rounds executed",
        "qph_degradation_pct": "QpH degradation from first-half to second-half of sustained run (positive = slower, negative = faster)",
        # Batch
        "time_to_value_seconds": "Wall-clock seconds from first input to queryable gold (lower is better)",
        "scale_ratio": "Actual data volume / expected volume for the scale factor (1.0 = complete)",
        "cycle_progression": "Per-cycle elapsed time, QpH, and table health for multi-cycle batch runs",
        # Maintenance (v1.3)
        "maintenance_elapsed_seconds": "Total seconds spent on expire_snapshots + remove_orphan_files + compaction",
        "maintenance_pct_of_pipeline": "Maintenance time as percentage of total pipeline time",
        "pre_compaction_file_count": "Iceberg data files before rewrite_data_files / OPTIMIZE",
        "post_compaction_file_count": "Iceberg data files after rewrite_data_files / OPTIMIZE",
        "compaction_ratio": "pre/post file count ratio (higher = more compaction benefit)",
        "snapshots_expired": "Number of snapshots removed by expire_snapshots",
        "orphan_files_removed": "Number of orphan files cleaned up by remove_orphan_files",
        "storage_reclaimed_mb": "MB of storage freed by maintenance operations",
        "pre_compaction_qph": "QpH measured before maintenance (on uncompacted data)",
        "post_compaction_qph": "QpH measured after maintenance (on compacted data) -- the primary QpH score",
        "maintenance_value_pct": "QpH change from maintenance over the queries that succeeded in both runs: (post - pre) / pre * 100; null when not measurable or within the within-round spread",
        "maintenance_value_reason": "Why maintenance_value_pct is null: not measurable, one sample per query, or within noise",
        "benchmark_samples_per_query": "Timed samples per query in the scored benchmark round (QpH uses the per-query median; 1 means no measured spread)",
        "qph_spread": "QpH of the slowest and fastest round the per-query samples allow, and their relative range",
        "maintenance_paired_queries": "Queries that succeeded before and after maintenance (the base of maintenance_value_pct)",
        "maintenance_settle_seconds": "Seconds from maintenance end until a storage-bound probe query was stable, before the post-maintenance round; not counted in time_to_value",
        "maintenance_settled": "True when the probe settled within the cap; false means the post round ran on unsettled storage and maintenance_value_pct is null",
        "maintenance_settle_capped": "True when the settle wait reached benchmark.maintenance_settle.max_seconds",
    }

    run_id: str
    deployment_name: str
    pipeline_mode: str  # "batch" or "sustained"
    start_time: datetime
    end_time: datetime | None = None

    stages: list[StageMetrics] = field(default_factory=list)

    # Pipeline-level scores (batch)
    total_elapsed_seconds: float = 0.0
    total_data_processed_gb: float = 0.0
    pipeline_throughput_gb_per_second: float = 0.0
    time_to_value_seconds: float = 0.0

    # Pipeline-level scores (both modes)
    compute_efficiency_gb_per_core_hour: float = 0.0

    # Pipeline-level scores (batch only)
    scale_ratio: float = 0.0

    # Pipeline-level scores (both modes)
    total_core_hours: float = 0.0

    # Pipeline-level scores (sustained -- None = unmeasurable)
    data_freshness_seconds: float | None = None
    sustained_throughput_rps: float = 0.0
    stage_latency_profile: list[float] = field(default_factory=list)
    total_rows_processed: int = 0
    # None = denominator (datagen_output_rows) was not measured -- cannot
    # compute ratio. 0.0 would be a lie (says "no data ingested"), so the
    # unmeasurable case must be explicit.
    ingest_ratio: float | None = None
    pipeline_saturated: bool | None = None
    # True when the finite corpus was fully ingested and silver had caught up
    # before the window ended, so later gold cycles had nothing new to read.
    # Freshness then covers only the cycles that saw data, and
    # sustained_throughput_rps is corpus rows / window, a lower bound on what
    # the pipeline could sustain (LB-145). None when ingest_ratio is unknown.
    corpus_drained: bool | None = None

    # Trino detail (preserved for drill-down)
    query_benchmark: BenchmarkMetrics | None = None

    # In-stream benchmark rounds (sustained mode only)
    benchmark_rounds: list[BenchmarkMetrics] = field(default_factory=list)
    query_time_freshness_seconds: float = 0.0  # median freshness at Trino query time

    # S3 object health (sustained mode -- set by cli.py after monitoring)
    total_s3_objects: int = 0

    # Multi-cycle batch metrics (v1.1.0)
    cycles: list[CycleMetrics] = field(default_factory=list)

    # QpH degradation (sustained mode, v1.1.0)
    # Percent drop from first-half median to second-half median QpH.
    # Positive = degradation, negative = improvement, None = insufficient data.
    qph_degradation_pct: float | None = None

    # Maintenance cost metrics (v1.3)
    maintenance_elapsed_seconds: float = 0.0
    maintenance_pct_of_pipeline: float = 0.0
    pre_compaction_file_count: int = 0
    post_compaction_file_count: int = 0
    compaction_ratio: float = 0.0
    snapshots_expired: int = 0
    orphan_files_removed: int = 0
    storage_reclaimed_mb: float = 0.0
    pre_compaction_qph: float = 0.0
    post_compaction_qph: float = 0.0
    # None when maintenance did not run or no query succeeded in both runs;
    # 0.0 would read as "maintenance had no effect".
    maintenance_value_pct: float | None = None
    maintenance_paired_queries: int = 0
    # Set when maintenance_value_pct is None after both rounds ran: the
    # difference was unmeasurable or inside the within-round spread (LB-150).
    maintenance_value_reason: str = ""
    # The pre-maintenance round as BenchmarkResult.to_dict(), every sample
    # included, so the noise judgement can be rechecked from metrics.json.
    pre_compaction_benchmark: dict[str, Any] | None = None
    # Storage settle wait between maintenance and the post round (LB-150).
    # None when the wait did not run. Not a stage: it adds to the run's wall
    # clock only, never to time_to_value or maintenance_elapsed_seconds.
    maintenance_settle_seconds: float | None = None
    maintenance_settled: bool | None = None
    maintenance_settle_capped: bool = False
    # SettleResult.to_dict(): the probe query, every probe's offset and time,
    # the reference time and why the wait ended.
    maintenance_settle: dict[str, Any] | None = None

    config_snapshot: dict[str, Any] = field(default_factory=dict)
    success: bool = False

    def compute_aggregates(self) -> None:
        """Compute pipeline-level scores from stage metrics.

        Mode-conditional: detects ``pipeline_mode`` and computes the
        appropriate score set.

        **Batch** (4 scores):
            1. ``total_elapsed_seconds`` -- sum of all stage durations.
            2. ``total_data_processed_gb`` -- sum of all stage input sizes.
            3. ``pipeline_throughput_gb_per_second`` -- (2) / (1).
            4. ``time_to_value_seconds`` -- wall clock: ``max(end_time) -
               min(start_time)``.  Falls back to (1) when timestamps absent.

        **Sustained** (4 scores + shared ``total_elapsed_seconds``):
            1. ``data_freshness_seconds`` -- worst-case gold staleness.
            2. ``sustained_throughput_rps`` -- aggregate rows/sec.
            3. ``stage_latency_profile`` -- per-stage processing latency [b/s/g].
            4. ``total_rows_processed`` -- total volume processed.

        Must be called after all stages are added (called automatically by
        :func:`build_pipeline_benchmark`).
        """
        # Universal: total elapsed across all stages
        self.total_elapsed_seconds = sum(s.elapsed_seconds for s in self.stages)

        if self.pipeline_mode == "sustained":
            self._compute_sustained_scores()
        else:
            self._compute_batch_scores()

    def _compute_batch_scores(self) -> None:
        """Compute batch pipeline scores from stage metrics."""
        self.total_data_processed_gb = sum(s.input_size_gb for s in self.stages)

        # time_to_value: wall clock from first stage start to last stage end.
        # If any stage started but did not finish (crashed before writing
        # end_time), that stage's start_time is used as its notional end --
        # otherwise dropping it would silently understate wall-clock and
        # inflate pipeline_throughput_gb_per_second. Whole-pipeline failure
        # is reflected separately via `success = False`.
        starts = [s.start_time for s in self.stages if s.start_time]
        latest_end = None
        for s in self.stages:
            candidate = s.end_time or s.start_time
            if candidate and (latest_end is None or candidate > latest_end):
                latest_end = candidate
        if starts and latest_end:
            self.time_to_value_seconds = (latest_end - min(starts)).total_seconds()
        elif self.total_elapsed_seconds > 0:
            self.time_to_value_seconds = self.total_elapsed_seconds

        # Throughput: total data / time-to-value (wall-clock span, not summed stage time)
        if self.time_to_value_seconds > 0:
            self.pipeline_throughput_gb_per_second = (
                self.total_data_processed_gb / self.time_to_value_seconds
            )

        # Compute efficiency: GB processed per core-hour requested
        batch_stages = [s for s in self.stages if s.stage_type == "batch"]
        total_core_hours = sum(
            s.executor_count * s.executor_cores * s.elapsed_seconds / 3600.0 for s in batch_stages
        )
        self.total_core_hours = total_core_hours
        if total_core_hours > 0:
            self.compute_efficiency_gb_per_core_hour = (
                self.total_data_processed_gb / total_core_hours
            )

        # Scale verified ratio: bronze input GB / expected bronze GB for this scale.
        # Uses only bronze input -- not total_data_processed_gb which sums all
        # stages and would triple-count (bronze + silver + gold).
        expected_gb = self.config_snapshot.get("approx_bronze_gb", 0)
        if expected_gb > 0:
            bronze_stages = [s for s in self.stages if s.stage_name == "bronze"]
            bronze_gb = (
                bronze_stages[0].input_size_gb if bronze_stages else self.total_data_processed_gb
            )
            self.scale_ratio = bronze_gb / expected_gb

    def _compute_sustained_scores(self) -> None:
        """Compute sustained pipeline scores from streaming stage metrics."""
        streaming = [s for s in self.stages if s.stage_type == "streaming"]

        # Total data processed (shared with batch -- needed for GB/s and report)
        self.total_data_processed_gb = sum(s.input_size_gb for s in self.stages)

        # Sustained throughput: unique rows entering bronze / run duration
        bronze_stages = [s for s in streaming if s.stage_name == "bronze"]
        total_bronze_rows = sum(s.input_rows for s in bronze_stages)
        run_duration = max((s.elapsed_seconds for s in streaming), default=0.0)
        if run_duration > 0:
            self.sustained_throughput_rps = total_bronze_rows / run_duration

        # Pipeline throughput in GB/s (total data / wall-clock duration)
        if run_duration > 0 and self.total_data_processed_gb > 0:
            self.pipeline_throughput_gb_per_second = self.total_data_processed_gb / run_duration

        # Stage latency profile: [bronze_avg_ms, silver_avg_ms, gold_avg_ms]
        profile: list[float] = []
        for name in ("bronze", "silver", "gold"):
            stage = next((s for s in streaming if s.stage_name == name), None)
            val = stage.latency_ms if stage and stage.latency_ms is not None else 0.0
            profile.append(val)
        self.stage_latency_profile = profile

        # Total rows processed across all streaming stages
        self.total_rows_processed = sum(s.input_rows for s in streaming)

        # Ingestion completeness: bronze rows / datagen rows.
        # Both scores stay None when the denominator is unknown -- reporting
        # "saturated" against a missing measurement is worse than reporting
        # "unmeasurable". See LB-044-shape regression.
        datagen_rows = self.config_snapshot.get("datagen_output_rows", 0)
        if datagen_rows > 0:
            self.ingest_ratio = total_bronze_rows / datagen_rows
            self.pipeline_saturated = self.ingest_ratio < 0.95
        else:
            self.ingest_ratio = None
            self.pipeline_saturated = None

        # Drained: every datagen row reached bronze and silver COMMITTED all
        # of it, so the trailing idle gold cycles measured an empty feed, not a
        # slow pipeline (LB-145). A stall (rows missing, silver behind or its
        # last commit unlogged) is not drained and keeps its full staleness.
        silver = [s for s in streaming if s.stage_name == "silver"]
        silver_committed = (
            sum(s.committed_rows for s in silver)
            if silver and all(s.committed_rows is not None for s in silver)
            else None
        )
        if self.ingest_ratio is None:
            self.corpus_drained = None
        else:
            # Exact counts, no slack: a bronze stall that leaves even a few
            # files unread is a stall. Two trailing idle cycles, so a healthy
            # run whose window ends between two silver batches is not drained.
            self.corpus_drained = (
                total_bronze_rows >= datagen_rows
                and silver_committed is not None
                and silver_committed >= total_bronze_rows
                and any(s.trailing_idle_cycles >= 2 for s in streaming)
            )

        # Worst-case freshness (max = most stale stage). None means
        # unmeasurable (< 2 gold cycles, parse failed, or a drained run whose
        # every cycle was idle).
        freshness_vals = []
        for st in streaming:
            val = st.freshness_active_seconds if self.corpus_drained else st.freshness_seconds
            if val is not None and val > 0:
                freshness_vals.append(val)
        if freshness_vals:
            self.data_freshness_seconds = max(freshness_vals)

        # Override total_elapsed_seconds for sustained mode.
        # Streaming stages run concurrently -- use wall-clock, not sum.
        if self.start_time and self.end_time:
            self.total_elapsed_seconds = (self.end_time - self.start_time).total_seconds()
        else:
            # Fallback: max of stage durations (concurrent, not sum)
            self.total_elapsed_seconds = max((s.elapsed_seconds for s in streaming), default=0.0)

        # Query-time freshness from in-stream benchmark rounds
        if self.benchmark_rounds:
            round_freshness = [
                r.round_meta.gold_freshness_seconds
                for r in self.benchmark_rounds
                if r.round_meta and r.round_meta.gold_freshness_seconds > 0
            ]
            if round_freshness:
                self.query_time_freshness_seconds = statistics.median(round_freshness)

        # QpH degradation: compare first-half vs second-half median QpH
        if self.benchmark_rounds and len(self.benchmark_rounds) >= 4:
            mid = len(self.benchmark_rounds) // 2
            first_half = [r.qph for r in self.benchmark_rounds[:mid] if r.qph > 0]
            second_half = [r.qph for r in self.benchmark_rounds[mid:] if r.qph > 0]
            if first_half and second_half:
                m1 = statistics.median(first_half)
                m2 = statistics.median(second_half)
                if m1 > 0:
                    self.qph_degradation_pct = round((1 - m2 / m1) * 100, 2)

        # Compute efficiency: GB processed per core-hour requested
        total_input_gb = sum(s.input_size_gb for s in streaming)
        total_core_hours = sum(
            s.executor_count * s.executor_cores * s.elapsed_seconds / 3600.0 for s in streaming
        )
        self.total_core_hours = total_core_hours
        if total_core_hours > 0:
            self.compute_efficiency_gb_per_core_hour = total_input_gb / total_core_hours

    def _scores_dict(self) -> dict[str, Any]:
        """Build the mode-appropriate scores sub-dict for JSON output."""
        qph = round(self.query_benchmark.qph, 1) if self.query_benchmark else 0.0

        # If in-stream rounds exist, use their median QpH as the primary score
        in_stream_qph = 0.0
        if self.benchmark_rounds:
            round_qphs = [r.qph for r in self.benchmark_rounds if r.qph > 0]
            if round_qphs:
                in_stream_qph = round(statistics.median(round_qphs), 1)

        if self.pipeline_mode == "sustained":
            # stage_latency_profile: object with named keys (v2.0 format)
            slp = (
                {
                    f"{name}_ms": round(v, 1)
                    for name, v in zip(
                        ["bronze", "silver", "gold"],
                        self.stage_latency_profile,
                        strict=False,
                    )
                }
                if self.stage_latency_profile
                else {}
            )
            # Composite QpH: None when no benchmark data supports a measurement
            raw_qph = in_stream_qph if in_stream_qph > 0 else qph
            composite_qph: float | None = raw_qph if raw_qph > 0 else None
            scores: dict[str, Any] = {
                "data_freshness_seconds": (
                    round(self.data_freshness_seconds, 2)
                    if self.data_freshness_seconds is not None
                    else None
                ),
                "sustained_throughput_rps": round(self.sustained_throughput_rps, 1),
                "total_data_processed_gb": round(self.total_data_processed_gb, 3),
                "pipeline_throughput_gb_per_second": round(
                    self.pipeline_throughput_gb_per_second, 4
                ),
                "total_core_hours": round(self.total_core_hours, 2),
                "ingest_ratio": (
                    round(self.ingest_ratio, 4) if self.ingest_ratio is not None else None
                ),
                "compute_efficiency_gb_per_core_hour": round(
                    self.compute_efficiency_gb_per_core_hour, 4
                ),
                "stage_latency_profile": slp,
                "composite_qph": composite_qph,
                "pipeline_saturated": self.pipeline_saturated,
                "corpus_drained": self.corpus_drained,
                "total_rows_processed": self.total_rows_processed,
                "total_elapsed_seconds": round(self.total_elapsed_seconds, 2),
                "total_s3_objects": self.total_s3_objects,
            }
            if self.query_time_freshness_seconds > 0:
                scores["query_time_freshness_seconds"] = round(self.query_time_freshness_seconds, 2)
            if in_stream_qph > 0:
                scores["in_stream_composite_qph"] = in_stream_qph
                scores["benchmark_rounds_count"] = len(self.benchmark_rounds)
            if self.qph_degradation_pct is not None:
                scores["qph_degradation_pct"] = self.qph_degradation_pct
            # Maintenance metrics (v1.3) -- same fields for sustained
            if self.maintenance_elapsed_seconds > 0:
                scores["maintenance_elapsed_seconds"] = round(self.maintenance_elapsed_seconds, 2)
                scores["maintenance_pct_of_pipeline"] = round(self.maintenance_pct_of_pipeline, 2)
            if self.pre_compaction_file_count > 0:
                scores["pre_compaction_file_count"] = self.pre_compaction_file_count
                scores["post_compaction_file_count"] = self.post_compaction_file_count
                scores["compaction_ratio"] = round(self.compaction_ratio, 2)
            if self.snapshots_expired > 0:
                scores["snapshots_expired"] = self.snapshots_expired
            if self.orphan_files_removed > 0:
                scores["orphan_files_removed"] = self.orphan_files_removed
            if self.storage_reclaimed_mb > 0:
                scores["storage_reclaimed_mb"] = round(self.storage_reclaimed_mb, 1)
            return scores

        # Batch scores
        batch_scores: dict[str, Any] = {
            "time_to_value_seconds": round(self.time_to_value_seconds, 2),
            "total_elapsed_seconds": round(self.total_elapsed_seconds, 2),
            "total_data_processed_gb": round(self.total_data_processed_gb, 3),
            "pipeline_throughput_gb_per_second": round(self.pipeline_throughput_gb_per_second, 4),
            "total_core_hours": round(self.total_core_hours, 2),
            "compute_efficiency_gb_per_core_hour": round(
                self.compute_efficiency_gb_per_core_hour, 4
            ),
            "composite_qph": qph,
            "scale_ratio": round(self.scale_ratio, 3),
        }
        if self.query_benchmark and self.query_benchmark.queries:
            from lakebench.benchmark.spread import spread

            _spread = spread(self.query_benchmark.queries)
            if _spread["samples_per_query"] > 0:
                batch_scores["benchmark_samples_per_query"] = _spread["samples_per_query"]
                batch_scores["qph_spread"] = {
                    "low": _spread["qph_low"],
                    "high": _spread["qph_high"],
                    "relative_range": _spread["relative_range"],
                }
        if self.cycles:
            batch_scores["cycle_progression"] = [
                {
                    "cycle": c.cycle_index,
                    "elapsed_seconds": round(sum(j.elapsed_seconds for j in c.jobs), 2),
                    "qph": round(c.benchmark.qph, 1) if c.benchmark else None,
                    "table_health": c.table_health,
                }
                for c in self.cycles
            ]
        # Maintenance metrics (v1.3) -- included in both batch and sustained scores
        if self.maintenance_elapsed_seconds > 0:
            batch_scores["maintenance_elapsed_seconds"] = round(self.maintenance_elapsed_seconds, 2)
            batch_scores["maintenance_pct_of_pipeline"] = round(self.maintenance_pct_of_pipeline, 2)
        if self.pre_compaction_file_count > 0:
            batch_scores["pre_compaction_file_count"] = self.pre_compaction_file_count
            batch_scores["post_compaction_file_count"] = self.post_compaction_file_count
            batch_scores["compaction_ratio"] = round(self.compaction_ratio, 2)
        if self.pre_compaction_qph > 0:
            batch_scores["pre_compaction_qph"] = round(self.pre_compaction_qph, 1)
            batch_scores["post_compaction_qph"] = round(self.post_compaction_qph, 1)
            batch_scores["maintenance_value_pct"] = (
                None if self.maintenance_value_pct is None else round(self.maintenance_value_pct, 1)
            )
            batch_scores["maintenance_paired_queries"] = self.maintenance_paired_queries
            if self.maintenance_value_pct is None and self.maintenance_value_reason:
                batch_scores["maintenance_value_reason"] = self.maintenance_value_reason
        if self.maintenance_settle_seconds is not None:
            batch_scores["maintenance_settle_seconds"] = round(self.maintenance_settle_seconds, 1)
            batch_scores["maintenance_settled"] = self.maintenance_settled
            batch_scores["maintenance_settle_capped"] = self.maintenance_settle_capped
        if self.snapshots_expired > 0:
            batch_scores["snapshots_expired"] = self.snapshots_expired
        if self.orphan_files_removed > 0:
            batch_scores["orphan_files_removed"] = self.orphan_files_removed
        if self.storage_reclaimed_mb > 0:
            batch_scores["storage_reclaimed_mb"] = round(self.storage_reclaimed_mb, 1)
        return batch_scores

    def to_matrix(self) -> dict[str, dict[str, Any]]:
        """Export as stage-columns, metric-rows matrix for spreadsheet use."""
        matrix: dict[str, dict[str, Any]] = {}
        for stage in self.stages:
            matrix[stage.stage_name] = {
                "engine": stage.engine,
                "elapsed_seconds": round(stage.elapsed_seconds, 2),
                "input_size_gb": round(stage.input_size_gb, 3),
                "output_size_gb": round(stage.output_size_gb, 3),
                "input_rows": stage.input_rows,
                "output_rows": stage.output_rows,
                "throughput_gb_per_second": round(stage.throughput_gb_per_second, 4),
                "throughput_rows_per_second": round(stage.throughput_rows_per_second, 1),
                "executor_count": stage.executor_count,
                "executor_cores": stage.executor_cores,
                "executor_memory_gb": round(stage.executor_memory_gb, 1),
                "latency_ms": round(stage.latency_ms, 1) if stage.latency_ms is not None else None,
                "freshness_seconds": round(stage.freshness_seconds, 1)
                if stage.freshness_seconds is not None
                else None,
                "queries_per_hour": round(stage.queries_per_hour, 1),
                "success": stage.success,
            }
        return matrix

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        scores = self._scores_dict()
        d: dict[str, Any] = {
            "run_id": self.run_id,
            "deployment_name": self.deployment_name,
            "pipeline_mode": self.pipeline_mode,
            "scale_factor": self.config_snapshot.get("scale", 0),
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "success": self.success,
            "scorecard": scores,
            "scores": scores,  # backward compat alias
            "stages": [s.to_dict() for s in self.stages],
            "stage_matrix": self.to_matrix(),
            "bucket_sizes": self._bucket_sizes(),
            "config_snapshot": self.config_snapshot,
        }
        # Mode-specific top-level flags (spec Section 7.1)
        if self.pipeline_mode == "batch":
            d["scale_ratio"] = round(self.scale_ratio, 3)
        else:
            d["ingest_ratio"] = (
                round(self.ingest_ratio, 4) if self.ingest_ratio is not None else None
            )
            d["pipeline_saturated"] = self.pipeline_saturated
            d["corpus_drained"] = self.corpus_drained
        if self.query_benchmark:
            d["query_benchmark"] = self.query_benchmark.to_dict()
        if self.pre_compaction_benchmark:
            d["pre_compaction_benchmark"] = self.pre_compaction_benchmark
        if self.maintenance_settle:
            d["maintenance_settle"] = self.maintenance_settle
        if self.benchmark_rounds:
            d["benchmark_rounds"] = [r.to_dict() for r in self.benchmark_rounds]
        if self.cycles:
            d["cycles"] = [c.to_dict() for c in self.cycles]
        # Include human-readable descriptions for every score key present
        d["score_descriptions"] = {k: v for k, v in self._SCORE_DESCRIPTIONS.items() if k in scores}
        # Tier 2 advanced metrics placeholder (populated when Prometheus deployed)
        d["advanced_metrics"] = None
        return d

    def _bucket_sizes(self) -> dict[str, float]:
        """Extract measured S3 bucket sizes from stages or config snapshot.

        Silver/gold sizes come from output_size_gb only. Falling back to
        input_size_gb would silently substitute the previous layer's read
        size for this layer's write size (silver ends up reporting bronze's
        GB, indistinguishably from a real silver measurement). If output
        is not measured, the config-snapshot S3 measurement is the next
        fallback, and 0.0 means "unmeasured", not "empty".
        """
        sizes: dict[str, float] = {"bronze_gb": 0.0, "silver_gb": 0.0, "gold_gb": 0.0}
        for stage in self.stages:
            if stage.stage_name == "bronze":
                sizes["bronze_gb"] = round(stage.input_size_gb, 3)
            elif stage.stage_name == "silver":
                sizes["silver_gb"] = round(stage.output_size_gb, 3)
            elif stage.stage_name == "gold":
                sizes["gold_gb"] = round(stage.output_size_gb, 3)
        # Fallback to config snapshot's directly-measured S3 sizes.
        if sizes["bronze_gb"] == 0.0:
            sizes["bronze_gb"] = round(self.config_snapshot.get("bronze_size_gb", 0.0), 3)
        if sizes["silver_gb"] == 0.0:
            sizes["silver_gb"] = round(self.config_snapshot.get("silver_size_gb", 0.0), 3)
        if sizes["gold_gb"] == 0.0:
            sizes["gold_gb"] = round(self.config_snapshot.get("gold_size_gb", 0.0), 3)
        return sizes


def build_pipeline_benchmark(
    run: PipelineMetrics,
    datagen_elapsed: float = 0.0,
    datagen_output_gb: float = 0.0,
    datagen_output_rows: int = 0,
    datagen_fleet: dict[str, Any] | None = None,
) -> PipelineBenchmark:
    """Build a PipelineBenchmark from a completed PipelineMetrics.

    This is the main conversion function that transforms the flat, per-job
    metrics into the unified stage-matrix view.  It handles three pipeline
    modes:

    **Batch** (jobs list populated):
        Creates stages for each job via ``_STAGE_MAP``:

        - ``bronze-verify`` → bronze stage
        - ``silver-build``  → silver stage
        - ``gold-finalize`` → gold stage

        Output fallbacks: when a job reports ``output_size_gb == 0`` (the
        driver log didn't emit it), the measured S3 bucket size for that
        layer is used instead (``run.bronze_size_gb``, etc.).

        Gold input fallback: when gold's ``input_size_gb == 0`` (Iceberg
        metadata query returned nothing), ``run.silver_size_gb`` is used.

    **Streaming** (streaming list populated):
        Creates stages for each streaming job via ``_STREAMING_MAP``:

        - ``bronze-ingest``  → bronze stage (type=streaming)
        - ``silver-stream``  → silver stage (type=streaming)
        - ``gold-refresh``   → gold stage (type=streaming)

        Executor resource profiles are looked up from ``spark/job.py``.

    **Query** (benchmark populated):
        Appends a query stage with QpH and Trino benchmark seconds.

    After all stages are built, ``compute_aggregates()`` derives the four
    pipeline-level scores (see :class:`PipelineBenchmark` docstring).

    Args:
        run: Completed PipelineMetrics instance.
        datagen_elapsed: Datagen elapsed seconds (0 to omit datagen stage).
        datagen_output_gb: Datagen output size in GB.
        datagen_output_rows: Datagen output row count.

    Returns:
        PipelineBenchmark with stages populated and aggregates computed.
    """
    stages: list[StageMetrics] = []

    # Datagen stage (if provided). Fleet metrics take precedence: they
    # carry actual per-pod cores and CPU-seconds, which the elapsed-only
    # form has no way to know. The elapsed_seconds passed in remains the
    # wall time of the K8s Job as observed from outside; fleet's
    # wall_elapsed_max_s is the slowest pod's own timer -- these agree to
    # within a poll interval.
    #
    # NOTE: `datagen_fleet` alone is not enough to trigger a stage; an
    # empty fleet dict (all pods failed to emit) is truthy and would
    # append a bogus zero-length stage that gets counted in CPU-hour
    # aggregations. Require `pods_reported > 0` on the fleet path.
    fleet: dict[str, Any] = datagen_fleet or {}
    fleet_has_data = int(fleet.get("pods_reported", 0)) > 0
    if datagen_elapsed > 0 or fleet_has_data:
        elapsed = datagen_elapsed
        output_gb = datagen_output_gb
        output_rows = datagen_output_rows
        exec_count = 0
        exec_cores = 0
        if fleet_has_data:
            elapsed = elapsed or float(fleet.get("wall_elapsed_max_s", 0.0))
            fleet_bytes = float(fleet.get("total_bytes_written", 0))
            if fleet_bytes > 0:
                output_gb = output_gb or fleet_bytes / 1e9
            fleet_rows = int(fleet.get("total_rows_written", 0))
            if fleet_rows > 0:
                output_rows = output_rows or fleet_rows
            exec_count = int(fleet.get("pods_reported", 0))
            # Per-pod cores: total cores / reported pods, rounded. All pods
            # are sized identically so this is exact modulo integer division.
            # Downstream CPU-hours derives from executor_count * executor_cores
            # * elapsed_seconds, so this fields lets datagen roll into the
            # existing pipeline core-hours computation.
            if exec_count > 0:
                exec_cores = int(fleet.get("cores_total", 0)) // exec_count
        dg = StageMetrics(
            stage_name="datagen",
            stage_type="datagen",
            engine="datagen",
            elapsed_seconds=elapsed,
            output_size_gb=output_gb,
            output_rows=output_rows,
            executor_count=exec_count,
            executor_cores=exec_cores,
            success=True,
        )
        dg.compute_derived()
        stages.append(dg)

    # Batch stages from jobs
    _STAGE_MAP: dict[str, tuple[str, str]] = {
        "bronze-verify": ("bronze", "bronze_size_gb"),
        "silver-build": ("silver", "silver_size_gb"),
        "gold-finalize": ("gold", "gold_size_gb"),
    }
    for job in run.jobs:
        stage_name, size_attr = _STAGE_MAP.get(job.job_type, (job.job_type, ""))
        fallback_size = getattr(run, size_attr, 0.0) if size_attr else 0.0

        # Gold reads from silver -- use measured silver size as input fallback
        input_gb = job.input_size_gb
        if stage_name == "gold" and input_gb == 0.0 and run.silver_size_gb > 0:
            input_gb = run.silver_size_gb

        stage = StageMetrics(
            stage_name=stage_name,
            stage_type="batch",
            engine="spark",
            start_time=job.start_time,
            end_time=job.end_time,
            elapsed_seconds=job.elapsed_seconds,
            success=job.success,
            error_message=job.error_message,
            input_size_gb=input_gb,
            input_rows=job.input_rows,
            output_size_gb=job.output_size_gb if job.output_size_gb > 0 else fallback_size,
            output_rows=job.output_rows,
            throughput_gb_per_second=job.throughput_gb_per_second,
            throughput_rows_per_second=job.throughput_rows_per_second,
            executor_count=job.executor_count,
            executor_cores=job.executor_cores,
            executor_memory_gb=job.executor_memory_gb,
        )
        # Backfill executor_cores from profile when progress callback missed it
        if stage.executor_cores == 0:
            try:
                from lakebench.spark.job import get_job_profile as _get_profile

                # Schema-aware so AML overrides (e.g. bronze-verify 20Gi) are
                # reported, not the c360 base (LB-135 review finding).
                _schema = run.config_snapshot.get("workload_schema")
                _b_profile = _get_profile(job.job_type, _schema)
                if _b_profile:
                    stage.executor_cores = _b_profile["executor_cores"]
                    if stage.executor_memory_gb == 0.0:
                        _mem_str = _b_profile.get("executor_memory", "0g")
                        stage.executor_memory_gb = (
                            float(_mem_str.rstrip("gG"))
                            if _mem_str.rstrip("gG").replace(".", "").isdigit()
                            else 0.0
                        )
            except Exception:
                pass
        # Recompute throughput if input was corrected by fallback
        if input_gb != job.input_size_gb:
            stage.compute_derived()
        stages.append(stage)

    # Streaming stages (sustained mode)
    _STREAMING_MAP: dict[str, str] = {
        "bronze-ingest": "bronze",
        "silver-stream": "silver",
        "gold-refresh": "gold",
    }
    for sj in run.streaming:
        # Look up resource profile for streaming job type
        _s_cores = 0
        _s_mem = 0.0
        _s_execs = 0
        try:
            from lakebench.spark.job import get_executor_count as _get_exec_count
            from lakebench.spark.job import get_job_profile as _get_profile

            _s_schema = run.config_snapshot.get("workload_schema")
            _s_profile = _get_profile(sj.job_type, _s_schema)
            if _s_profile:
                _s_cores = _s_profile["executor_cores"]
                _s_scale = run.config_snapshot.get("scale", 10)
                _s_execs = _get_exec_count(sj.job_type, _s_scale, _s_schema)
                # Check config overrides (streaming jobs may have explicit counts)
                _override_key = sj.job_type.replace("-", "_")
                _overrides = run.config_snapshot.get("spark", {}).get("executor_overrides", {})
                _override_val = _overrides.get(_override_key)
                if _override_val is not None:
                    _s_execs = _override_val
                _mem_str = _s_profile.get("executor_memory", "0g")
                # Simple parse: strip trailing 'g'
                _s_mem = (
                    float(_mem_str.rstrip("gG"))
                    if _mem_str.rstrip("gG").replace(".", "").isdigit()
                    else 0.0
                )
        except Exception as e:
            logger.warning("Could not parse resource metrics for stage: %s", e)

        stage = StageMetrics(
            stage_name=_STREAMING_MAP.get(sj.job_type, sj.job_type),
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=sj.elapsed_seconds,
            success=sj.success,
            error_message=sj.error_message,
            input_rows=sj.total_rows_processed,
            unique_rows_processed=(
                sj.unique_rows_processed
                if sj.unique_rows_processed > 0
                else (sj.total_rows_processed if sj.job_type != "gold-refresh" else None)
            ),
            throughput_rows_per_second=sj.throughput_rps,
            latency_ms=sj.micro_batch_duration_ms or None,
            freshness_seconds=sj.freshness_seconds or None,
            freshness_active_seconds=sj.freshness_active_seconds,
            trailing_idle_cycles=sj.trailing_idle_cycles,
            committed_rows=sj.committed_rows,
            total_batches=sj.total_batches,
            batch_size=sj.batch_size,
            executor_count=_s_execs,
            executor_cores=_s_cores,
            executor_memory_gb=_s_mem,
        )
        stages.append(stage)

    # Backfill input_size_gb for streaming stages from measured S3 bucket sizes.
    # Bronze-ingest reads from landing zone (bronze bucket), silver-stream reads
    # from bronze Iceberg table (bronze bucket), gold-refresh reads from silver.
    _STREAMING_SIZE_MAP = {
        "bronze": "bronze_size_gb",
        "silver": "silver_size_gb",
        "gold": "silver_size_gb",
    }
    for stage in stages:
        if stage.stage_type == "streaming" and stage.input_size_gb == 0.0:
            fallback_attr = _STREAMING_SIZE_MAP.get(stage.stage_name)
            if fallback_attr:
                stage.input_size_gb = getattr(run, fallback_attr, 0.0)
                stage.compute_derived()

    # Query stage from benchmark
    if run.benchmark:
        query_stage = StageMetrics(
            stage_name="query",
            stage_type="query",
            engine="trino",
            elapsed_seconds=run.benchmark.total_seconds,
            success=True,
            input_size_gb=run.gold_size_gb,
            queries_executed=len(run.benchmark.queries),
            queries_per_hour=run.benchmark.qph,
        )
        stages.append(query_stage)

    # Build config snapshot with datagen row count for completeness ratio
    snapshot = dict(run.config_snapshot)
    if datagen_output_rows > 0:
        snapshot["datagen_output_rows"] = datagen_output_rows

    benchmark = PipelineBenchmark(
        run_id=run.run_id,
        deployment_name=run.deployment_name,
        pipeline_mode="sustained" if run.streaming else "batch",
        start_time=run.start_time,
        end_time=run.end_time,
        stages=stages,
        query_benchmark=run.benchmark,
        benchmark_rounds=list(run.benchmark_rounds),
        cycles=list(run.cycles),
        config_snapshot=snapshot,
        success=run.success,
    )
    benchmark.compute_aggregates()
    return benchmark


def aggregate_benchmark_rounds(rounds: list[BenchmarkMetrics]) -> BenchmarkMetrics:
    """Aggregate multiple in-stream benchmark rounds into a single result.

    Uses the median for QpH, total_seconds, and per-query elapsed times.
    The aggregated result has no ``round_meta`` (it represents the combined
    view, not a specific round).

    Args:
        rounds: List of per-round BenchmarkMetrics (must be non-empty).

    Returns:
        Aggregated BenchmarkMetrics with median values.

    Raises:
        ValueError: If ``rounds`` is empty.
    """
    if not rounds:
        raise ValueError("Cannot aggregate zero benchmark rounds")

    # Median QpH and total_seconds
    median_qph = statistics.median([r.qph for r in rounds])
    median_total = statistics.median([r.total_seconds for r in rounds])

    # Per-query median elapsed times.  Build a dict of query_name -> list of elapsed values.
    # All rounds should have the same queries in the same order, but handle
    # missing queries defensively.
    query_times: dict[str, list[float]] = {}
    query_template: dict[str, dict[str, Any]] = {}
    for rnd in rounds:
        for q in rnd.queries:
            name = q.get("name", "")
            if name not in query_times:
                query_times[name] = []
                query_template[name] = dict(q)
            query_times[name].append(q.get("elapsed_seconds", 0.0))

    aggregated_queries: list[dict[str, Any]] = []
    for name, times in query_times.items():
        qd = dict(query_template[name])
        qd["elapsed_seconds"] = round(statistics.median(times), 3)
        aggregated_queries.append(qd)

    return BenchmarkMetrics(
        mode=rounds[0].mode,
        cache=rounds[0].cache,
        scale=rounds[0].scale,
        qph=median_qph,
        total_seconds=median_total,
        queries=aggregated_queries,
        iterations=rounds[0].iterations,
        streams=rounds[0].streams,
    )


def build_config_snapshot(cfg: Any) -> dict[str, Any]:
    """Build a config snapshot for metrics recording.

    Captures the key configuration fields that affect benchmark results,
    enabling cross-run comparisons.

    Args:
        cfg: A :class:`~lakebench.config.LakebenchConfig` instance.

    Returns:
        Dict suitable for JSON serialization.
    """
    spark = cfg.platform.compute.spark
    datagen = cfg.architecture.workload.datagen
    pipeline = cfg.architecture.pipeline
    s3 = cfg.platform.storage.s3
    scratch = cfg.platform.storage.scratch

    snapshot: dict[str, Any] = {
        "name": cfg.name,
        "scale": datagen.get_effective_scale(),
        "approx_bronze_gb": round(cfg.get_scale_dimensions().approx_bronze_gb, 1),
        "processing_pattern": pipeline.pattern.value,
        "s3": {
            "endpoint": s3.endpoint,
            "buckets": {
                "bronze": s3.buckets.bronze,
                "silver": s3.buckets.silver,
                "gold": s3.buckets.gold,
            },
        },
        "scratch": {
            "enabled": scratch.enabled,
            "storage_class": scratch.storage_class,
            "size": scratch.size,
        },
        "spark": {
            "driver": {
                "cores": spark.driver.cores,
                "memory": spark.driver.memory,
            },
            "executor": {
                "instances": spark.executor.instances,
                "cores": spark.executor.cores,
                "memory": spark.executor.memory,
                "memory_overhead": spark.executor.memory_overhead,
            },
            "executor_overrides": {
                "bronze": spark.bronze_executors,
                "silver": spark.silver_executors,
                "gold": spark.gold_executors,
                "bronze_ingest": spark.bronze_ingest_executors,
                "silver_stream": spark.silver_stream_executors,
                "gold_refresh": spark.gold_refresh_executors,
            },
        },
        "catalog": cfg.architecture.catalog.type.value,
        "table_format": cfg.architecture.table_format.type.value,
        "pipeline_engine": cfg.architecture.pipeline_engine.value,
        "query_engine": cfg.architecture.query_engine.type.value,
        "workload_schema": cfg.architecture.workload.schema_type.value,
        "sustained": {
            "bronze_trigger_interval": pipeline.sustained.bronze_trigger_interval,
            "silver_trigger_interval": pipeline.sustained.silver_trigger_interval,
            "gold_refresh_interval": pipeline.sustained.gold_refresh_interval,
            "run_duration": pipeline.sustained.run_duration,
            "max_files_per_trigger": pipeline.sustained.max_files_per_trigger,
            "bronze_target_file_size_mb": pipeline.sustained.bronze_target_file_size_mb,
            "silver_target_file_size_mb": pipeline.sustained.silver_target_file_size_mb,
            "gold_target_file_size_mb": pipeline.sustained.gold_target_file_size_mb,
            "benchmark_interval": pipeline.sustained.benchmark_interval,
            "benchmark_warmup": pipeline.sustained.benchmark_warmup,
        },
        "datagen": {
            "scale": datagen.scale,
            "mode": datagen.mode.value,
            "parallelism": datagen.parallelism,
            "file_size": datagen.file_size,
        },
        "images": {
            "datagen": cfg.images.datagen,
            "spark": cfg.images.spark,
            "trino": cfg.images.trino,
        },
        "trino": {
            "coordinator": {
                "cpu": cfg.architecture.query_engine.trino.coordinator.cpu,
                "memory": cfg.architecture.query_engine.trino.coordinator.memory,
            },
            "worker": {
                "replicas": cfg.architecture.query_engine.trino.worker.replicas,
                "cpu": cfg.architecture.query_engine.trino.worker.cpu,
                "memory": cfg.architecture.query_engine.trino.worker.memory,
            },
        },
        "benchmark": {
            "mode": cfg.architecture.benchmark.mode.value,
            "streams": cfg.architecture.benchmark.streams,
            "cache": cfg.architecture.benchmark.cache,
            "iterations": cfg.architecture.benchmark.iterations,
        },
    }

    return snapshot


class MetricsCollector:
    """Collects metrics from Spark jobs."""

    def __init__(self):
        """Initialize metrics collector."""
        self.current_run: PipelineMetrics | None = None

    def start_run(
        self, run_id: str, deployment_name: str, config: dict[str, Any]
    ) -> PipelineMetrics:
        """Start a new pipeline run.

        Args:
            run_id: Unique run identifier
            deployment_name: Name of the deployment
            config: Configuration snapshot

        Returns:
            New PipelineMetrics instance
        """
        self.current_run = PipelineMetrics(
            run_id=run_id,
            deployment_name=deployment_name,
            start_time=datetime.now(),
            config_snapshot=config,
        )
        return self.current_run

    def end_run(self, success: bool = True) -> PipelineMetrics | None:
        """End the current pipeline run.

        Args:
            success: Whether the run was successful

        Returns:
            Completed PipelineMetrics
        """
        if not self.current_run:
            return None

        self.current_run.end_time = datetime.now()
        self.current_run.total_elapsed_seconds = (
            self.current_run.end_time - self.current_run.start_time
        ).total_seconds()
        self.current_run.success = success

        return self.current_run

    def record_job(self, metrics: JobMetrics) -> None:
        """Record metrics for a job.

        Args:
            metrics: JobMetrics to record
        """
        if self.current_run:
            self.current_run.jobs.append(metrics)

    def record_query(self, metrics: QueryMetrics) -> None:
        """Record metrics for a query.

        Args:
            metrics: QueryMetrics to record
        """
        if self.current_run:
            self.current_run.queries.append(metrics)

    def record_benchmark(self, benchmark: BenchmarkMetrics) -> None:
        """Record benchmark results.

        Args:
            benchmark: BenchmarkMetrics from a benchmark run
        """
        if self.current_run:
            self.current_run.benchmark = benchmark

    def record_benchmark_round(self, benchmark: BenchmarkMetrics) -> None:
        """Record an in-stream benchmark round.

        Args:
            benchmark: BenchmarkMetrics from a single round (should have
                ``round_meta`` set)
        """
        if self.current_run:
            self.current_run.benchmark_rounds.append(benchmark)

    def record_actual_sizes(
        self,
        s3_client: Any,
        bronze_bucket: str,
        silver_bucket: str,
        gold_bucket: str,
    ) -> int:
        """Measure and record actual S3 bucket sizes.

        Uses paginated listing via ``s3_client.get_bucket_size()``
        for accurate measurement.

        Args:
            s3_client: :class:`~lakebench.s3.S3Client` instance
            bronze_bucket: Bronze bucket name
            silver_bucket: Silver bucket name
            gold_bucket: Gold bucket name

        Returns:
            Total object count across all three buckets (0 if unmeasurable).
        """
        total_objects = 0
        if not self.current_run:
            return total_objects

        for layer, bucket in [
            ("bronze", bronze_bucket),
            ("silver", silver_bucket),
            ("gold", gold_bucket),
        ]:
            try:
                info = s3_client.get_bucket_size(bucket)
                if info.size_bytes is not None:
                    size_gb = info.size_bytes / (1024**3)
                    setattr(self.current_run, f"{layer}_size_gb", size_gb)
                    if info.object_count is not None:
                        total_objects += info.object_count
                    logger.info(
                        f"Measured {layer} bucket: {info.object_count:,} objects, {size_gb:.2f} GB"
                    )
                elif not getattr(info, "exists", True):
                    logger.warning(
                        f"Bucket '{bucket}' does not exist -- "
                        f"check that config buckets match actual bucket names"
                    )
                else:
                    logger.warning(f"Bucket '{bucket}' returned no size data")
            except Exception as e:
                logger.warning(f"Could not measure {layer} bucket size: {e}")

        return total_objects

    def record_actual_sizes_local(
        self,
        s3_client: Any,
        buckets: dict[str, str],
    ) -> int:
        """Measure per-layer sizes for a local run.

        Local mode keeps one Iceberg warehouse root, so bucket names alone do
        not identify a layer: sizing the gold bucket returns zero and sizing
        silver returns silver plus gold. ``local_layer_prefix()`` supplies the
        (bucket, prefix) pair that separates them, which is what keeps a local
        scorecard comparable with a cluster one.

        Args:
            s3_client: :class:`~lakebench.s3.S3Client` instance.
            buckets: Layer name to bucket name, for bronze, silver, and gold.

        Returns:
            Total object count across all layers (0 if unmeasurable).
        """
        from lakebench.modules.pipeline_engines.spark.local_job import local_layer_prefix

        total_objects = 0
        if not self.current_run:
            return total_objects

        for layer in ("bronze", "silver", "gold"):
            bucket_layer, prefix = local_layer_prefix(layer)
            bucket = buckets.get(bucket_layer, "")
            if not bucket:
                continue
            try:
                info = s3_client.get_bucket_size(bucket, prefix=prefix)
                if info.size_bytes is None:
                    continue
                setattr(self.current_run, f"{layer}_size_gb", info.size_bytes / (1024**3))
                total_objects += info.object_count or 0
                logger.info(
                    "Measured %s: %s objects, %.3f GB (%s/%s)",
                    layer,
                    f"{info.object_count or 0:,}",
                    info.size_bytes / (1024**3),
                    bucket,
                    prefix or "",
                )
            except Exception as e:
                logger.warning("Could not measure %s: %s", layer, e)

        return total_objects

    def parse_driver_logs(self, logs: str, job_type: str) -> JobMetrics:
        """Parse Spark driver logs to extract metrics.

        Args:
            logs: Driver log content
            job_type: Type of job (bronze-verify, silver-build, gold-finalize)

        Returns:
            JobMetrics extracted from logs
        """
        metrics = JobMetrics(
            job_name=f"lakebench-{job_type}",
            job_type=job_type,
        )

        # Parse JOB METRICS section
        metrics_match = re.search(
            r"=== JOB METRICS: (\S+) ===(.*?)={40}",
            logs,
            re.DOTALL,
        )

        if metrics_match:
            metrics_section = metrics_match.group(2)

            # Parse key-value pairs (lines may have [lb] timestamp prefix)
            for line in metrics_section.strip().split("\n"):
                # Strip [lb] prefix if present: "[lb] 2026-... - key: value"
                stripped = re.sub(r"^\[lb\]\s+\S+\s+-\s+", "", line.strip())
                match = re.match(r"(\w+):\s*(.+)", stripped)
                if match:
                    key, value = match.groups()
                    self._apply_metric(metrics, key, value)

        # Fallback timing for scripts without a JOB METRICS block. The block's
        # elapsed_seconds wins: this pattern is loose enough to match
        # unrelated JVM log lines, so it must never override a measured value.
        if metrics.elapsed_seconds <= 0:
            time_match = re.search(
                r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*completed? in (\d+\.?\d*)s",
                logs,
            )
            if time_match:
                metrics.elapsed_seconds = float(time_match.group(2))

        # LB-116: per-rule alert counts and per-rule errors, from the
        # driver log line ``[detection] {rule_id}: alerts=N ...``.
        # Emitter format is fixed with ``elapsed=Ns`` as the trailing token:
        #   success: ``[detection] {rule}: alerts=N prior=P elapsed=Ts``
        #   crashed: ``[detection] {rule}: alerts=0 error=<Class>: <msg> elapsed=Ts``
        # Anchor on the LAST ``elapsed=Ns`` at end-of-line via a greedy
        # middle capture + MULTILINE ``$``; the engine backtracks from
        # the newline to the final ``elapsed=`` on the line, so an inline
        # ``elapsed=`` substring inside the error message survives intact.
        # Rule slug is ``[A-Za-z0-9_]+`` (matches DEFAULT_DETECTION_RULES
        # plus any future additions) so a stray ``:`` in a message cannot
        # be mis-picked as the rule/alerts separator.
        detection_re = re.compile(
            r"\[detection\]\s+"
            r"(?P<rule>[A-Za-z0-9_]+):\s+"
            r"alerts=(?P<n>\d+)"
            r"(?P<mid>.*?)"
            r"\s+elapsed=[\d.]+s\s*$",
            re.MULTILINE,
        )
        for m in detection_re.finditer(logs):
            rule = m.group("rule")
            try:
                metrics.alerts_by_rule[rule] = int(m.group("n"))
            except ValueError:
                continue
            mid = (m.group("mid") or "").strip()
            err_match = re.search(r"\berror=(.+)$", mid)
            if err_match:
                metrics.rule_errors[rule] = err_match.group(1).strip()

        # LB-119: structural skips are a THIRD shape, distinct from
        # ``alerts=N``. Emitter format:
        #   ``[detection] {rule}: skipped=<reason> detail=<...> elapsed=Ts``
        # A skipped rule is recorded in ``rules_skipped`` and left OUT of
        # ``alerts_by_rule`` so downstream scoring renders it as "not run"
        # rather than as a zero. Anchor on the trailing ``elapsed=Ns`` the
        # same way the alerts line does, with a non-greedy detail capture.
        skip_re = re.compile(
            r"\[detection\]\s+"
            r"(?P<rule>[A-Za-z0-9_]+):\s+"
            r"skipped=(?P<reason>[A-Za-z0-9_-]+)"
            r"(?P<mid>.*?)"
            r"\s+elapsed=[\d.]+s\s*$",
            re.MULTILINE,
        )
        for m in skip_re.finditer(logs):
            metrics.rules_skipped[m.group("rule")] = m.group("reason").strip()

        # P10 TM operations lines (tm_operations.py).
        from lakebench.metrics.tm_ops import parse_tm_invariants, parse_tm_ops, parse_tm_status

        metrics.tm_invariants = {str(c): inv for c, inv in parse_tm_invariants(logs).items()}
        metrics.tm_status = {str(c): st for c, st in parse_tm_status(logs).items()}
        metrics.tm_ops = parse_tm_ops(logs)

        # Calculate throughput
        if metrics.elapsed_seconds > 0:
            if metrics.input_size_gb > 0:
                metrics.throughput_gb_per_second = metrics.input_size_gb / metrics.elapsed_seconds
            if metrics.input_rows > 0:
                metrics.throughput_rows_per_second = metrics.input_rows / metrics.elapsed_seconds

        return metrics

    def _apply_metric(self, metrics: JobMetrics, key: str, value: str) -> None:
        """Apply a parsed metric value.

        Args:
            metrics: JobMetrics to update
            key: Metric key
            value: Metric value string
        """
        key_lower = key.lower()

        try:
            if key_lower in ("total_size_gb", "bronze_size_gb", "input_size_gb"):
                metrics.input_size_gb = float(value)
            elif key_lower in ("output_size_gb",):
                metrics.output_size_gb = float(value)
            elif key_lower in ("estimated_rows", "input_rows", "estimated_input_rows"):
                metrics.input_rows = int(float(value))
            elif key_lower in ("output_rows",):
                metrics.output_rows = int(float(value))
            elif key_lower in ("elapsed_seconds",):
                metrics.elapsed_seconds = float(value)
        except (ValueError, TypeError) as e:
            logger.debug("Could not parse metric value %s=%s: %s", key_lower, value, e)

    def record_streaming(self, metrics: StreamingJobMetrics) -> None:
        """Record metrics for a streaming job.

        Args:
            metrics: StreamingJobMetrics to record
        """
        if self.current_run:
            self.current_run.streaming.append(metrics)

    def parse_streaming_logs(self, logs: str, job_type: str) -> StreamingJobMetrics:
        """Parse streaming driver logs to extract metrics.

        Extracts batch counts and row totals from the structured ``[lb]``
        log lines emitted by bronze_ingest.py, silver_stream.py, and
        gold_refresh.py.

        Args:
            logs: Driver log content
            job_type: Type of streaming job (bronze-ingest, silver-stream,
                gold-refresh)

        Returns:
            StreamingJobMetrics extracted from logs
        """
        metrics = StreamingJobMetrics(
            job_name=f"lakebench-{job_type}",
            job_type=job_type,
        )

        total_rows = 0
        batch_ids: set[int] = set()
        batch_durations: list[float] = []
        freshness_values: list[float] = []
        # (value, idle) per gold cycle, in log order (LB-145).
        freshness_cycles: list[tuple[float, bool]] = []
        # Silver: rows per batch id, and the batch ids that logged a commit.
        batch_rows: dict[int, int] = {}
        committed_batches: set[int] = set()

        for line in logs.split("\n"):
            # Bronze: "Batch N: writing X rows to ..."
            m = re.search(r"Batch (\d+): writing ([\d,]+) rows", line)
            if m:
                batch_ids.add(int(m.group(1)))
                total_rows += int(m.group(2).replace(",", ""))
                continue

            # Silver: "Batch N: transforming X rows"
            m = re.search(r"Batch (\d+): transforming ([\d,]+) rows", line)
            if m:
                batch_ids.add(int(m.group(1)))
                total_rows += int(m.group(2).replace(",", ""))
                batch_rows[int(m.group(1))] = int(m.group(2).replace(",", ""))
                continue

            # Silver: "Batch N: empty, skipping" (no data in micro-batch).
            # Only count empty batches for silver -- bronze uses the same
            # log format but empty bronze batches are not meaningful.
            if job_type == "silver-stream":
                m = re.search(r"Batch (\d+): empty", line)
                if m:
                    batch_ids.add(int(m.group(1)))
                    continue

            # Gold: "Cycle N: aggregating X Silver records"
            m = re.search(r"Cycle (\d+): aggregating ([\d,]+) Silver records", line)
            if m:
                batch_ids.add(int(m.group(1)))
                total_rows += int(m.group(2).replace(",", ""))
                continue

            # Gold: "Cycle N: Silver table is empty, skipping"
            if job_type == "gold-refresh":
                m = re.search(r"Cycle (\d+): Silver table is empty", line)
                if m:
                    batch_ids.add(int(m.group(1)))
                    continue

            # Gold: "Cycle N: refreshed ... in X.Xs (Y KPI records)"
            m = re.search(r"Cycle \d+: refreshed .+ in ([\d.]+)s", line)
            if m:
                batch_durations.append(float(m.group(1)))
                continue

            # Bronze: "Batch N: committed in X.Xs"
            m = re.search(r"Batch (\d+): committed in (\d+\.?\d*)s", line)
            if m:
                batch_durations.append(float(m.group(2)))
                continue

            # Silver: "Batch N: committed to ... in X.Xs"
            m = re.search(r"Batch (\d+): committed to .+ in (\d+\.?\d*)s", line)
            if m:
                batch_durations.append(float(m.group(2)))
                committed_batches.add(int(m.group(1)))
                continue

            # Gold: "Cycle N: data freshness Xs", optionally tagged
            # " (silver idle)" when silver had not moved since the last cycle.
            m = re.search(r"Cycle \d+: data freshness ([\d.]+)s( \(silver idle\))?", line)
            if m:
                freshness_values.append(float(m.group(1)))
                freshness_cycles.append((float(m.group(1)), bool(m.group(2))))
                continue

        metrics.total_batches = len(batch_ids)
        metrics.total_rows_processed = total_rows

        if batch_durations:
            metrics.micro_batch_duration_ms = (sum(batch_durations) / len(batch_durations)) * 1000

        if freshness_values:
            metrics.freshness_seconds = max(freshness_values)
        # Split off the trailing idle run: cycles after silver last moved.
        cut = len(freshness_cycles)
        while cut > 0 and freshness_cycles[cut - 1][1]:
            cut -= 1
        metrics.trailing_idle_cycles = len(freshness_cycles) - cut
        if cut > 0:
            metrics.freshness_active_seconds = max(v for v, _ in freshness_cycles[:cut])
        if job_type == "silver-stream" and committed_batches:
            metrics.committed_rows = sum(batch_rows.get(b, 0) for b in committed_batches)

        if metrics.total_batches > 0:
            metrics.batch_size = total_rows // metrics.total_batches

        # For bronze/silver, unique rows = total (no re-reads).
        # For gold, leave as 0 -- gold re-reads silver each cycle so
        # total_rows_processed includes amplification.
        if job_type in ("bronze-ingest", "silver-stream"):
            metrics.unique_rows_processed = metrics.total_rows_processed

        return metrics

    def get_summary(self) -> dict[str, Any]:
        """Get summary of current run metrics.

        Returns:
            Summary dict
        """
        if not self.current_run:
            return {}

        jobs = self.current_run.jobs
        total_input_gb = sum(j.input_size_gb for j in jobs)
        total_output_rows = sum(j.output_rows for j in jobs)
        total_time = sum(j.elapsed_seconds for j in jobs)

        return {
            "run_id": self.current_run.run_id,
            "deployment": self.current_run.deployment_name,
            "total_jobs": len(jobs),
            "successful_jobs": sum(1 for j in jobs if j.success),
            "total_input_gb": total_input_gb,
            "total_output_rows": total_output_rows,
            "total_job_time_seconds": total_time,
            "avg_throughput_gb_per_second": total_input_gb / total_time if total_time > 0 else 0,
        }
