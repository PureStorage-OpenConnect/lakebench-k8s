"""Metrics storage for Lakebench.

Persists metrics to local JSON files, organised as per-run subdirectories
under a unified output tree::

    lakebench-output/
      runs/
        run-20260204-210211-abc123/
          metrics.json
          report.html          # written by ReportGenerator
        run-20260204-220000-def456/
          metrics.json

Legacy flat layout (``run-{id}.json`` files in a single directory) is
transparently supported for reading: :meth:`load_run` and :meth:`list_runs`
probe both layouts so that old data created before the migration continues
to work.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from lakebench._constants import DEFAULT_OUTPUT_DIR

from .collector import (
    BenchmarkMetrics,
    JobMetrics,
    PipelineBenchmark,
    PipelineMetrics,
    QueryMetrics,
    StageMetrics,
    StreamingJobMetrics,
)

logger = logging.getLogger(__name__)

_DEFAULT_RUNS_DIR = str(Path(DEFAULT_OUTPUT_DIR) / "runs")


class MetricsStorage:
    """Stores metrics to local JSON files.

    New runs are written as ``<runs_dir>/run-<id>/metrics.json``.
    Old-style ``<runs_dir>/run-<id>.json`` files are still readable.
    """

    def __init__(self, metrics_dir: Path | str = _DEFAULT_RUNS_DIR):
        """Initialize metrics storage.

        Args:
            metrics_dir: Directory for storing metrics (parent of per-run dirs)
        """
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Run directory helpers
    # ------------------------------------------------------------------

    def run_dir(self, run_id: str) -> Path:
        """Return the per-run directory for *run_id*, creating it if needed."""
        d = self.metrics_dir / f"run-{run_id}"
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ------------------------------------------------------------------
    # Save / load
    # ------------------------------------------------------------------

    def save_run(self, metrics: PipelineMetrics) -> Path:
        """Save pipeline run metrics.

        Args:
            metrics: PipelineMetrics to save

        Returns:
            Path to saved file
        """
        run = self.run_dir(metrics.run_id)
        filepath = run / "metrics.json"

        with open(filepath, "w") as f:
            json.dump(metrics.to_dict(), f, indent=2)

        logger.info(f"Saved metrics to {filepath}")
        return filepath

    def load_run(self, run_id: str) -> PipelineMetrics | None:
        """Load pipeline run metrics.

        Checks the new per-run directory layout first, then falls back to
        the legacy flat file layout for backward compatibility.

        Args:
            run_id: Run identifier

        Returns:
            PipelineMetrics or None if not found
        """
        # New layout: runs/run-{id}/metrics.json
        new_path = self.metrics_dir / f"run-{run_id}" / "metrics.json"
        if new_path.exists():
            with open(new_path) as f:
                return self._dict_to_metrics(json.load(f))

        # Legacy layout: runs/run-{id}.json
        legacy_path = self.metrics_dir / f"run-{run_id}.json"
        if legacy_path.exists():
            with open(legacy_path) as f:
                return self._dict_to_metrics(json.load(f))

        return None

    def list_runs(self) -> list[dict[str, Any]]:
        """List all saved runs.

        Returns:
            List of run summaries (most recent first)
        """
        runs = []
        seen_ids: set[str] = set()

        # New layout: per-run directories
        for run_dir in sorted(self.metrics_dir.iterdir(), reverse=True):
            metrics_file = run_dir / "metrics.json"
            if run_dir.is_dir() and run_dir.name.startswith("run-") and metrics_file.exists():
                summary = self._read_run_summary(metrics_file)
                if summary and summary["run_id"] not in seen_ids:
                    runs.append(summary)
                    seen_ids.add(summary["run_id"])

        # Legacy layout: flat run-*.json files
        for filepath in sorted(self.metrics_dir.glob("run-*.json"), reverse=True):
            if filepath.is_file():
                summary = self._read_run_summary(filepath)
                if summary and summary["run_id"] not in seen_ids:
                    runs.append(summary)
                    seen_ids.add(summary["run_id"])

        # Re-sort combined list by start_time descending
        runs.sort(key=lambda r: r.get("start_time", ""), reverse=True)
        return runs

    @staticmethod
    def _read_run_summary(filepath: Path) -> dict[str, Any] | None:
        """Read a metrics JSON and return a summary dict."""
        try:
            with open(filepath) as f:
                data = json.load(f)

            pb = data.get("pipeline_benchmark")
            pb_scores = pb.get("scores", {}) if pb else {}
            return {
                "run_id": data.get("run_id"),
                "deployment_name": data.get("deployment_name"),
                "start_time": data.get("start_time"),
                "success": data.get("success"),
                "total_elapsed_seconds": data.get("total_elapsed_seconds"),
                "job_count": len(data.get("jobs", [])),
                "scale": data.get("config_snapshot", {}).get("scale"),
                "processing_pattern": data.get("config_snapshot", {}).get("processing_pattern"),
                "qph": data.get("benchmark", {}).get("qph") if data.get("benchmark") else None,
                "bronze_size_gb": data.get("bronze_size_gb", 0),
                "silver_size_gb": data.get("silver_size_gb", 0),
                "gold_size_gb": data.get("gold_size_gb", 0),
                "streaming_count": len(data.get("streaming", [])),
                "time_to_value_seconds": pb_scores.get("time_to_value_seconds"),
                "pipeline_throughput_gb_per_second": pb_scores.get(
                    "pipeline_throughput_gb_per_second"
                ),
            }
        except (json.JSONDecodeError, KeyError):
            return None

    def get_latest_run(self) -> PipelineMetrics | None:
        """Get the most recent run.

        Returns:
            PipelineMetrics or None if no runs exist
        """
        runs = self.list_runs()
        if not runs:
            return None

        return self.load_run(runs[0]["run_id"])

    def _iter_metrics_files(self):
        """Yield all metrics JSON file paths (new + legacy layouts)."""
        # New layout: per-run directories
        for run_dir in sorted(self.metrics_dir.iterdir(), reverse=True):
            metrics_file = run_dir / "metrics.json"
            if run_dir.is_dir() and run_dir.name.startswith("run-") and metrics_file.exists():
                yield metrics_file
        # Legacy layout: flat run-*.json files
        for filepath in sorted(self.metrics_dir.glob("run-*.json"), reverse=True):
            if filepath.is_file():
                yield filepath

    def _dict_to_metrics(self, data: dict[str, Any]) -> PipelineMetrics:
        """Convert dict to PipelineMetrics.

        Args:
            data: Dict from JSON

        Returns:
            PipelineMetrics instance
        """
        jobs = []
        for job_data in data.get("jobs", []):
            job = JobMetrics(
                job_name=job_data.get("job_name", ""),
                job_type=job_data.get("job_type", ""),
                elapsed_seconds=job_data.get("elapsed_seconds", 0),
                success=job_data.get("success", False),
                error_message=job_data.get("error_message"),
                input_size_gb=job_data.get("input_size_gb", 0),
                output_size_gb=job_data.get("output_size_gb", 0),
                input_rows=job_data.get("input_rows", 0),
                output_rows=job_data.get("output_rows", 0),
                executor_count=job_data.get("executor_count", 0),
                executor_cores=job_data.get("executor_cores", 0),
                executor_memory_gb=job_data.get("executor_memory_gb", 0.0),
                cpu_seconds_requested=job_data.get(
                    "cpu_seconds_requested",
                    job_data.get(
                        "total_cpu_seconds_allocated", job_data.get("total_cpu_time_seconds", 0.0)
                    ),
                ),
                memory_gb_requested=job_data.get(
                    "memory_gb_requested",
                    job_data.get("peak_memory_gb_allocated", job_data.get("peak_memory_gb", 0.0)),
                ),
                throughput_gb_per_second=job_data.get("throughput_gb_per_second", 0),
                throughput_rows_per_second=job_data.get("throughput_rows_per_second", 0),
            )

            if job_data.get("start_time"):
                job.start_time = datetime.fromisoformat(job_data["start_time"])
            if job_data.get("end_time"):
                job.end_time = datetime.fromisoformat(job_data["end_time"])

            jobs.append(job)

        queries = []
        for query_data in data.get("queries", []):
            queries.append(
                QueryMetrics(
                    query_name=query_data.get("query_name", ""),
                    query_text=query_data.get("query_text", ""),
                    elapsed_seconds=query_data.get("elapsed_seconds", 0),
                    rows_returned=query_data.get("rows_returned", 0),
                    success=query_data.get("success", False),
                    error_message=query_data.get("error_message", ""),
                )
            )

        streaming = []
        for s_data in data.get("streaming", []):
            streaming.append(
                StreamingJobMetrics(
                    job_name=s_data.get("job_name", ""),
                    job_type=s_data.get("job_type", ""),
                    throughput_rps=s_data.get("throughput_rps", 0.0),
                    freshness_seconds=s_data.get("freshness_seconds", 0.0),
                    micro_batch_duration_ms=s_data.get("micro_batch_duration_ms", 0.0),
                    batch_size=s_data.get("batch_size", 0),
                    total_batches=s_data.get("total_batches", 0),
                    total_rows_processed=s_data.get("total_rows_processed", 0),
                    elapsed_seconds=s_data.get("elapsed_seconds", 0.0),
                    success=s_data.get("success", False),
                    error_message=s_data.get("error_message"),
                )
            )

        metrics = PipelineMetrics(
            run_id=data.get("run_id", ""),
            deployment_name=data.get("deployment_name", ""),
            start_time=datetime.fromisoformat(data.get("start_time", datetime.now().isoformat())),
            success=data.get("success", False),
            total_elapsed_seconds=data.get("total_elapsed_seconds", 0),
            bronze_size_gb=data.get("bronze_size_gb", 0),
            silver_size_gb=data.get("silver_size_gb", 0),
            gold_size_gb=data.get("gold_size_gb", 0),
            jobs=jobs,
            queries=queries,
            streaming=streaming,
            config_snapshot=data.get("config_snapshot", {}),
        )

        if data.get("end_time"):
            metrics.end_time = datetime.fromisoformat(data["end_time"])

        # Deserialize benchmark if present
        bench_data = data.get("benchmark")
        if bench_data:
            metrics.benchmark = BenchmarkMetrics(
                mode=bench_data.get("mode", "power"),
                cache=bench_data.get("cache", "hot"),
                scale=bench_data.get("scale", 0),
                qph=bench_data.get("qph", 0.0),
                total_seconds=bench_data.get("total_seconds", 0.0),
                queries=bench_data.get("queries", []),
                iterations=bench_data.get("iterations", 1),
                streams=bench_data.get("streams", 1),
                stream_results=bench_data.get("stream_results", []),
            )

        # Deserialize pipeline benchmark if present
        pb_data = data.get("pipeline_benchmark")
        if pb_data:
            pb_stages = []
            for s_data in pb_data.get("stages", []):
                stage = StageMetrics(
                    stage_name=s_data.get("stage_name", ""),
                    stage_type=s_data.get("stage_type", ""),
                    engine=s_data.get("engine", ""),
                    elapsed_seconds=s_data.get("elapsed_seconds", 0.0),
                    success=s_data.get("success", False),
                    error_message=s_data.get("error_message"),
                    input_size_gb=s_data.get("input_size_gb", 0.0),
                    output_size_gb=s_data.get("output_size_gb", 0.0),
                    input_rows=s_data.get("input_rows", 0),
                    output_rows=s_data.get("output_rows", 0),
                    throughput_gb_per_second=s_data.get("throughput_gb_per_second", 0.0),
                    throughput_rows_per_second=s_data.get("throughput_rows_per_second", 0.0),
                    executor_count=s_data.get("executor_count", 0),
                    executor_cores=s_data.get("executor_cores", 0),
                    executor_memory_gb=s_data.get("executor_memory_gb", 0.0),
                    latency_ms=s_data.get("latency_ms", 0.0),
                    freshness_seconds=s_data.get("freshness_seconds", 0.0),
                    total_batches=s_data.get("total_batches", 0),
                    batch_size=s_data.get("batch_size", 0),
                    queries_executed=s_data.get("queries_executed", 0),
                    queries_per_hour=s_data.get("queries_per_hour", 0.0),
                )
                if s_data.get("start_time"):
                    stage.start_time = datetime.fromisoformat(s_data["start_time"])
                if s_data.get("end_time"):
                    stage.end_time = datetime.fromisoformat(s_data["end_time"])
                pb_stages.append(stage)

            # Reconstruct query_benchmark from the nested dict if present
            qb_data = pb_data.get("query_benchmark")
            query_benchmark = None
            if qb_data:
                query_benchmark = BenchmarkMetrics(
                    mode=qb_data.get("mode", "power"),
                    cache=qb_data.get("cache", "hot"),
                    scale=qb_data.get("scale", 0),
                    qph=qb_data.get("qph", 0.0),
                    total_seconds=qb_data.get("total_seconds", 0.0),
                    queries=qb_data.get("queries", []),
                    iterations=qb_data.get("iterations", 1),
                    streams=qb_data.get("streams", 1),
                    stream_results=qb_data.get("stream_results", []),
                )

            scores = pb_data.get("scorecard", pb_data.get("scores", {}))
            metrics.pipeline_benchmark = PipelineBenchmark(
                run_id=pb_data.get("run_id", ""),
                deployment_name=pb_data.get("deployment_name", ""),
                pipeline_mode=pb_data.get("pipeline_mode", "batch"),
                start_time=datetime.fromisoformat(
                    pb_data.get("start_time", datetime.now().isoformat())
                ),
                stages=pb_stages,
                # Batch scores
                total_elapsed_seconds=scores.get("total_elapsed_seconds", 0.0),
                total_data_processed_gb=scores.get("total_data_processed_gb", 0.0),
                pipeline_throughput_gb_per_second=scores.get(
                    "pipeline_throughput_gb_per_second", 0.0
                ),
                time_to_value_seconds=scores.get("time_to_value_seconds", 0.0),
                # Both modes
                compute_efficiency_gb_per_core_hour=scores.get(
                    "compute_efficiency_gb_per_core_hour", 0.0
                ),
                # Batch only
                scale_verified_ratio=scores.get("scale_verified_ratio", 0.0),
                # Continuous scores (default 0 for old batch JSONs)
                data_freshness_seconds=scores.get("data_freshness_seconds", 0.0),
                sustained_throughput_rps=scores.get("sustained_throughput_rps", 0.0),
                stage_latency_profile=scores.get("stage_latency_profile", []),
                total_rows_processed=scores.get("total_rows_processed", 0),
                ingestion_completeness_ratio=scores.get("ingestion_completeness_ratio", 0.0),
                pipeline_saturated=scores.get("pipeline_saturated", False),
                query_benchmark=query_benchmark,
                config_snapshot=pb_data.get("config_snapshot", {}),
                success=pb_data.get("success", False),
            )
            if pb_data.get("end_time"):
                metrics.pipeline_benchmark.end_time = datetime.fromisoformat(pb_data["end_time"])

        return metrics

    def export_csv(self, output_path: Path | str) -> Path:
        """Export all runs to CSV format with per-job columns.

        Args:
            output_path: Output CSV file path

        Returns:
            Path to CSV file
        """
        import csv

        output_path = Path(output_path)

        batch_job_types = ["bronze_verify", "silver_build", "gold_finalize"]
        streaming_job_types = ["bronze_ingest", "silver_stream", "gold_refresh"]
        pipeline_stages = ["datagen", "bronze", "silver", "gold", "query"]

        fieldnames = [
            "run_id",
            "deployment_name",
            "start_time",
            "success",
            "total_elapsed_seconds",
            "job_count",
            "scale",
            "processing_pattern",
            "qph",
            "bronze_size_gb",
            "silver_size_gb",
            "gold_size_gb",
            "streaming_count",
            "time_to_value_seconds",
            "pipeline_throughput_gb_per_second",
        ]
        for jt in batch_job_types:
            fieldnames.extend(
                [
                    f"{jt}_seconds",
                    f"{jt}_input_size_gb",
                    f"{jt}_output_rows",
                ]
            )
        for jt in streaming_job_types:
            fieldnames.extend(
                [
                    f"{jt}_total_batches",
                    f"{jt}_total_rows_processed",
                    f"{jt}_throughput_rps",
                    f"{jt}_freshness_seconds",
                ]
            )
        for ps in pipeline_stages:
            fieldnames.extend(
                [
                    f"pb_{ps}_seconds",
                    f"pb_{ps}_input_gb",
                    f"pb_{ps}_output_gb",
                    f"pb_{ps}_gb_per_s",
                ]
            )

        rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for filepath in self._iter_metrics_files():
            try:
                with open(filepath) as f:
                    data = json.load(f)

                rid = data.get("run_id", "")
                if rid in seen_ids:
                    continue
                seen_ids.add(rid)

                row: dict[str, Any] = {
                    "run_id": data.get("run_id"),
                    "deployment_name": data.get("deployment_name"),
                    "start_time": data.get("start_time"),
                    "success": data.get("success"),
                    "total_elapsed_seconds": data.get("total_elapsed_seconds"),
                    "job_count": len(data.get("jobs", [])),
                    "scale": data.get("config_snapshot", {}).get("scale"),
                    "processing_pattern": data.get("config_snapshot", {}).get("processing_pattern"),
                    "qph": data.get("benchmark", {}).get("qph") if data.get("benchmark") else None,
                    "bronze_size_gb": data.get("bronze_size_gb", 0),
                    "silver_size_gb": data.get("silver_size_gb", 0),
                    "gold_size_gb": data.get("gold_size_gb", 0),
                    "streaming_count": len(data.get("streaming", [])),
                }

                for job_data in data.get("jobs", []):
                    prefix = job_data.get("job_type", "").replace("-", "_")
                    if prefix in batch_job_types:
                        row[f"{prefix}_seconds"] = job_data.get("elapsed_seconds", 0)
                        row[f"{prefix}_input_size_gb"] = job_data.get("input_size_gb", 0)
                        row[f"{prefix}_output_rows"] = job_data.get("output_rows", 0)

                for s_data in data.get("streaming", []):
                    prefix = s_data.get("job_type", "").replace("-", "_")
                    if prefix in streaming_job_types:
                        row[f"{prefix}_total_batches"] = s_data.get("total_batches", 0)
                        row[f"{prefix}_total_rows_processed"] = s_data.get(
                            "total_rows_processed", 0
                        )
                        row[f"{prefix}_throughput_rps"] = s_data.get("throughput_rps", 0)
                        row[f"{prefix}_freshness_seconds"] = s_data.get("freshness_seconds", 0)

                # Pipeline benchmark stage columns
                pb = data.get("pipeline_benchmark")
                if pb:
                    pb_scores = pb.get("scores", {})
                    row["time_to_value_seconds"] = pb_scores.get("time_to_value_seconds")
                    row["pipeline_throughput_gb_per_second"] = pb_scores.get(
                        "pipeline_throughput_gb_per_second"
                    )
                    matrix = pb.get("stage_matrix", {})
                    for ps in pipeline_stages:
                        stage_data = matrix.get(ps, {})
                        row[f"pb_{ps}_seconds"] = stage_data.get("elapsed_seconds")
                        row[f"pb_{ps}_input_gb"] = stage_data.get("input_size_gb")
                        row[f"pb_{ps}_output_gb"] = stage_data.get("output_size_gb")
                        row[f"pb_{ps}_gb_per_s"] = stage_data.get("throughput_gb_per_second")

                rows.append(row)
            except (json.JSONDecodeError, KeyError):
                continue

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"Exported {len(rows)} runs to {output_path}")
        return output_path
