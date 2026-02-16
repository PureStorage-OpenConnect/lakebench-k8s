"""Metrics collection for Lakebench."""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

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

    # Streaming job metrics (optional -- populated for continuous pipeline runs)
    streaming: list[StreamingJobMetrics] = field(default_factory=list)

    # Configuration snapshot
    config_snapshot: dict[str, Any] = field(default_factory=dict)

    # Benchmark results (optional -- populated after query benchmark runs)
    benchmark: BenchmarkMetrics | None = None

    # Pipeline benchmark (optional -- populated after build_pipeline_benchmark)
    pipeline_benchmark: PipelineBenchmark | None = None

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
        if self.pipeline_benchmark is not None:
            d["pipeline_benchmark"] = self.pipeline_benchmark.to_dict()
        return d


@dataclass
class BenchmarkMetrics:
    """Metrics from a Trino query benchmark run."""

    mode: str  # "power", "throughput", or "composite"
    cache: str  # "hot" or "cold"
    scale: int
    qph: float
    total_seconds: float
    queries: list[dict[str, Any]] = field(default_factory=list)
    iterations: int = 1
    streams: int = 1
    stream_results: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d: dict[str, Any] = {
            "benchmark_type": "trino_query",
            "mode": self.mode,
            "cache": self.cache,
            "scale": self.scale,
            "qph": round(self.qph, 1),
            "total_seconds": round(self.total_seconds, 2),
            "iterations": self.iterations,
            "streams": self.streams,
            "queries": self.queries,
        }
        if self.stream_results:
            d["stream_results"] = self.stream_results
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

    # Streaming-specific (zero for batch)
    latency_ms: float = 0.0
    freshness_seconds: float = 0.0
    total_batches: int = 0
    batch_size: int = 0
    unique_rows_processed: int = 0  # distinct input rows (gold re-reads inflate input_rows)

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

    Continuous scoring (pipeline_mode="continuous")
    ------------------------------------------------
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
    keys (batch keys for batch, continuous keys for continuous).
    ``to_matrix()`` produces the ``stage_matrix`` sub-object -- a dict keyed
    by stage name for spreadsheet/comparison use.
    """

    run_id: str
    deployment_name: str
    pipeline_mode: str  # "batch" or "continuous"
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
    scale_verified_ratio: float = 0.0

    # Pipeline-level scores (continuous -- zero for batch)
    data_freshness_seconds: float = 0.0
    sustained_throughput_rps: float = 0.0
    stage_latency_profile: list[float] = field(default_factory=list)
    total_rows_processed: int = 0
    ingestion_completeness_ratio: float = 0.0
    pipeline_saturated: bool = False

    # Trino detail (preserved for drill-down)
    query_benchmark: BenchmarkMetrics | None = None

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

        **Continuous** (4 scores + shared ``total_elapsed_seconds``):
            1. ``data_freshness_seconds`` -- worst-case gold staleness.
            2. ``sustained_throughput_rps`` -- aggregate rows/sec.
            3. ``stage_latency_profile`` -- per-stage processing latency [b/s/g].
            4. ``total_rows_processed`` -- total volume processed.

        Must be called after all stages are added (called automatically by
        :func:`build_pipeline_benchmark`).
        """
        # Universal: total elapsed across all stages
        self.total_elapsed_seconds = sum(s.elapsed_seconds for s in self.stages)

        if self.pipeline_mode == "continuous":
            self._compute_continuous_scores()
        else:
            self._compute_batch_scores()

    def _compute_batch_scores(self) -> None:
        """Compute batch pipeline scores from stage metrics."""
        self.total_data_processed_gb = sum(s.input_size_gb for s in self.stages)

        # time_to_value: wall clock from first stage start to last stage end
        starts = [s.start_time for s in self.stages if s.start_time]
        ends = [s.end_time for s in self.stages if s.end_time]
        if starts and ends:
            self.time_to_value_seconds = (max(ends) - min(starts)).total_seconds()
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
        if total_core_hours > 0:
            self.compute_efficiency_gb_per_core_hour = (
                self.total_data_processed_gb / total_core_hours
            )

        # Scale verified ratio: actual GB / expected GB for this scale
        expected_gb = self.config_snapshot.get("approx_bronze_gb", 0)
        if expected_gb > 0:
            self.scale_verified_ratio = self.total_data_processed_gb / expected_gb

    def _compute_continuous_scores(self) -> None:
        """Compute continuous pipeline scores from streaming stage metrics."""
        streaming = [s for s in self.stages if s.stage_type == "streaming"]

        # Worst-case freshness (max = most stale stage)
        freshness_vals = [s.freshness_seconds for s in streaming if s.freshness_seconds > 0]
        if freshness_vals:
            self.data_freshness_seconds = max(freshness_vals)

        # Sustained throughput: unique rows entering bronze / run duration
        bronze_stages = [s for s in streaming if s.stage_name == "bronze"]
        total_bronze_rows = sum(s.input_rows for s in bronze_stages)
        run_duration = max((s.elapsed_seconds for s in streaming), default=0.0)
        if run_duration > 0:
            self.sustained_throughput_rps = total_bronze_rows / run_duration

        # Stage latency profile: [bronze_avg_ms, silver_avg_ms, gold_avg_ms]
        profile: list[float] = []
        for name in ("bronze", "silver", "gold"):
            stage = next((s for s in streaming if s.stage_name == name), None)
            profile.append(stage.latency_ms if stage else 0.0)
        self.stage_latency_profile = profile

        # Total rows processed across all streaming stages
        self.total_rows_processed = sum(s.input_rows for s in streaming)

        # Ingestion completeness: bronze rows / datagen rows
        datagen_rows = self.config_snapshot.get("datagen_output_rows", 0)
        if datagen_rows > 0:
            self.ingestion_completeness_ratio = total_bronze_rows / datagen_rows
        self.pipeline_saturated = self.ingestion_completeness_ratio < 0.95

        # Compute efficiency: GB processed per core-hour requested
        total_input_gb = sum(s.input_size_gb for s in streaming)
        total_core_hours = sum(
            s.executor_count * s.executor_cores * s.elapsed_seconds / 3600.0 for s in streaming
        )
        if total_core_hours > 0:
            self.compute_efficiency_gb_per_core_hour = total_input_gb / total_core_hours

    def _scores_dict(self) -> dict[str, Any]:
        """Build the mode-appropriate scores sub-dict for JSON output."""
        qph = round(self.query_benchmark.qph, 1) if self.query_benchmark else 0.0
        if self.pipeline_mode == "continuous":
            return {
                "data_freshness_seconds": round(self.data_freshness_seconds, 2),
                "sustained_throughput_rps": round(self.sustained_throughput_rps, 1),
                "ingestion_completeness_ratio": round(self.ingestion_completeness_ratio, 4),
                "compute_efficiency_gb_per_core_hour": round(
                    self.compute_efficiency_gb_per_core_hour, 4
                ),
                "stage_latency_profile": [round(v, 1) for v in self.stage_latency_profile],
                "composite_qph": qph,
                "pipeline_saturated": self.pipeline_saturated,
                "total_rows_processed": self.total_rows_processed,
                "total_elapsed_seconds": round(self.total_elapsed_seconds, 2),
            }
        return {
            "time_to_value_seconds": round(self.time_to_value_seconds, 2),
            "total_elapsed_seconds": round(self.total_elapsed_seconds, 2),
            "total_data_processed_gb": round(self.total_data_processed_gb, 3),
            "pipeline_throughput_gb_per_second": round(self.pipeline_throughput_gb_per_second, 4),
            "compute_efficiency_gb_per_core_hour": round(
                self.compute_efficiency_gb_per_core_hour, 4
            ),
            "composite_qph": qph,
            "scale_verified_ratio": round(self.scale_verified_ratio, 3),
        }

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
                "latency_ms": round(stage.latency_ms, 1),
                "freshness_seconds": round(stage.freshness_seconds, 1),
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
            d["scale_verified_ratio"] = round(self.scale_verified_ratio, 3)
        else:
            d["ingestion_completeness_ratio"] = round(self.ingestion_completeness_ratio, 4)
            d["pipeline_saturated"] = self.pipeline_saturated
        if self.query_benchmark:
            d["query_benchmark"] = self.query_benchmark.to_dict()
        return d

    def _bucket_sizes(self) -> dict[str, float]:
        """Extract measured S3 bucket sizes from stages or config snapshot."""
        sizes: dict[str, float] = {"bronze_gb": 0.0, "silver_gb": 0.0, "gold_gb": 0.0}
        for stage in self.stages:
            if stage.stage_name == "bronze" and stage.stage_type == "batch":
                sizes["bronze_gb"] = round(stage.input_size_gb, 3)
            elif stage.stage_name == "silver":
                sizes["silver_gb"] = round(stage.output_size_gb, 3)
            elif stage.stage_name == "gold":
                sizes["gold_gb"] = round(stage.output_size_gb, 3)
        # Fallback to config snapshot if stages didn't have sizes
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

    # Datagen stage (if provided)
    if datagen_elapsed > 0:
        dg = StageMetrics(
            stage_name="datagen",
            stage_type="datagen",
            engine="datagen",
            elapsed_seconds=datagen_elapsed,
            output_size_gb=datagen_output_gb,
            output_rows=datagen_output_rows,
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
        # Recompute throughput if input was corrected by fallback
        if input_gb != job.input_size_gb:
            stage.compute_derived()
        stages.append(stage)

    # Streaming stages (continuous mode)
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

            _s_profile = _get_profile(sj.job_type)
            if _s_profile:
                _s_cores = _s_profile["executor_cores"]
                _s_scale = run.config_snapshot.get("scale", 10)
                _s_execs = _get_exec_count(sj.job_type, _s_scale)
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
            unique_rows_processed=sj.unique_rows_processed
            if sj.unique_rows_processed > 0
            else sj.total_rows_processed,
            throughput_rows_per_second=sj.throughput_rps,
            latency_ms=sj.micro_batch_duration_ms,
            freshness_seconds=sj.freshness_seconds,
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
        "silver": "bronze_size_gb",
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
        pipeline_mode="continuous" if run.streaming else "batch",
        start_time=run.start_time,
        end_time=run.end_time,
        stages=stages,
        query_benchmark=run.benchmark,
        config_snapshot=snapshot,
        success=run.success,
    )
    benchmark.compute_aggregates()
    return benchmark


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
        "query_engine": cfg.architecture.query_engine.type.value,
        "continuous": {
            "bronze_trigger_interval": pipeline.continuous.bronze_trigger_interval,
            "silver_trigger_interval": pipeline.continuous.silver_trigger_interval,
            "gold_refresh_interval": pipeline.continuous.gold_refresh_interval,
            "run_duration": pipeline.continuous.run_duration,
            "max_files_per_trigger": pipeline.continuous.max_files_per_trigger,
            "bronze_target_file_size_mb": pipeline.continuous.bronze_target_file_size_mb,
            "silver_target_file_size_mb": pipeline.continuous.silver_target_file_size_mb,
            "gold_target_file_size_mb": pipeline.continuous.gold_target_file_size_mb,
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

    def record_actual_sizes(
        self,
        s3_client: Any,
        bronze_bucket: str,
        silver_bucket: str,
        gold_bucket: str,
    ) -> None:
        """Measure and record actual S3 bucket sizes.

        Uses paginated listing via ``s3_client.get_bucket_size()``
        for accurate measurement.

        Args:
            s3_client: :class:`~lakebench.s3.S3Client` instance
            bronze_bucket: Bronze bucket name
            silver_bucket: Silver bucket name
            gold_bucket: Gold bucket name
        """
        if not self.current_run:
            return

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

        # Parse timing from log timestamps
        time_match = re.search(
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*completed in (\d+\.?\d*)s",
            logs,
        )
        if time_match:
            metrics.elapsed_seconds = float(time_match.group(2))

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
                continue

            # Gold: "Cycle N: aggregating X Silver records"
            m = re.search(r"Cycle (\d+): aggregating ([\d,]+) Silver records", line)
            if m:
                batch_ids.add(int(m.group(1)))
                total_rows += int(m.group(2).replace(",", ""))
                continue

            # Gold: "Cycle N: refreshed ... in X.Xs (Y KPI records)"
            m = re.search(r"Cycle \d+: refreshed .+ in (\d+\.?\d*)s", line)
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
                continue

            # Gold: "Cycle N: data freshness Xs"
            m = re.search(r"Cycle \d+: data freshness (\d+\.?\d*)s", line)
            if m:
                freshness_values.append(float(m.group(1)))
                continue

        metrics.total_batches = len(batch_ids)
        metrics.total_rows_processed = total_rows

        if batch_durations:
            metrics.micro_batch_duration_ms = (sum(batch_durations) / len(batch_durations)) * 1000

        if freshness_values:
            metrics.freshness_seconds = sum(freshness_values) / len(freshness_values)

        if metrics.total_batches > 0:
            metrics.batch_size = total_rows // metrics.total_batches

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
