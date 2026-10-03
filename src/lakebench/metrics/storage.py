"""Metrics storage for Lakebench.

Persists metrics to local JSON files, organised as per-run subdirectories
under a unified output tree::

    lakebench-output/
      runs/
        run-20260204-210211-abc123/
          metrics.json
          report.html          # delivered once by write_run_report
        run-20260204-220000-def456/
          metrics.json
      reports/
        report-20260204-210211-abc123-20260205-091401.html
                                # rendered by `lakebench report --render`

The per-run ``report.html`` is the delivered artifact and is written once,
at the end of the run, by :func:`lakebench.cli._helpers.write_run_report`.
Regenerating the HTML from saved metrics goes to a timestamped file under
``lakebench-output/reports/`` (see :mod:`lakebench.reports.generator`) so
the delivered artifact cannot be mutated in place by a later CLI call.

Legacy flat layout (``run-{id}.json`` files in a single directory) is
transparently supported for reading: :meth:`load_run` and :meth:`list_runs`
probe both layouts so that old data created before the migration continues
to work.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from lakebench._constants import DEFAULT_OUTPUT_DIR
from lakebench.benchmark.queries import legacy_query_set_id
from lakebench.metrics.maintenance_policy import recorded_policy

from .collector import (
    BenchmarkMetrics,
    BenchmarkRoundMeta,
    CycleMetrics,
    JobMetrics,
    PipelineBenchmark,
    PipelineMetrics,
    QueryMetrics,
    StageMetrics,
    StreamingJobMetrics,
)

logger = logging.getLogger(__name__)

_DEFAULT_RUNS_DIR = str(Path(DEFAULT_OUTPUT_DIR) / "runs")


# Backward-compat aliases for renamed JobMetrics fields. Read tries the
# canonical name first, then falls back through the alias list. Any
# renamed field lists its OLD names here so runs recorded before the
# rename still load correctly.
_JOB_ALIASES: dict[str, tuple[str, ...]] = {
    "cpu_seconds_requested": ("total_cpu_seconds_allocated", "total_cpu_time_seconds"),
    "memory_gb_requested": ("peak_memory_gb_allocated", "peak_memory_gb"),
}


def _dataclass_from_dict(
    cls: type, data: dict[str, Any], aliases: dict[str, tuple[str, ...]] | None = None
):
    """Reconstruct a dataclass instance from a JSON-dict, iterating fields.

    Class-level fix for the LB-123 defect shape on the LOAD side. Two
    live-caught instances (silver-plan r3 silver_tables + extra_metrics on
    JobMetrics; extra_metrics on StreamingJobMetrics) reached metrics.json
    via asdict() but reverted to defaults on load because the hand-written
    kwargs list at each ctor site drifted behind the dataclass. Iterating
    dataclasses.fields() removes that shape: a new field on the dataclass
    flows automatically on both save (asdict) and load (this helper).

    * datetime fields (start_time / end_time on JobMetrics) are handled by
      the caller AFTER construction, since the dict carries ISO strings.
      This helper skips them.
    * aliases maps canonical -> tuple of legacy names; used for renamed
      fields so pre-rename metrics.json still loads correctly.
    * A missing key falls back to the dataclass default (default value
      or default_factory), matching the behaviour of an omitted kwarg.
    """
    import dataclasses

    kwargs: dict[str, Any] = {}
    field_aliases = aliases or {}
    _DATETIME_FIELDS = frozenset({"start_time", "end_time"})

    for f in dataclasses.fields(cls):
        if f.name in _DATETIME_FIELDS:
            continue  # caller sets these post-construction via fromisoformat
        # Try canonical name, then each alias in turn.
        value = data.get(f.name)
        if value is None:
            for legacy in field_aliases.get(f.name, ()):
                if data.get(legacy) is not None:
                    value = data[legacy]
                    break
        if value is None:
            # Missing on disk. Use the dataclass default so the loaded
            # instance matches what a bare `cls()` would produce.
            if f.default is not dataclasses.MISSING:
                kwargs[f.name] = f.default
            elif f.default_factory is not dataclasses.MISSING:  # type: ignore[misc]
                kwargs[f.name] = f.default_factory()
            else:
                # No default; construction would fail with kwarg omitted,
                # so pass None and let the dataclass complain loudly.
                kwargs[f.name] = None
        else:
            kwargs[f.name] = value

    return cls(**kwargs)


def _sort_instant(value: Any) -> float:
    """A start_time as epoch seconds for ordering runs.

    Naive values (runs before v1.6) are host-local, as they were written;
    aware ones carry their offset. Unparseable or missing sorts oldest.
    """
    try:
        at = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return float("-inf")
    try:
        return at.timestamp()  # naive: local time, like .astimezone()
    except (OverflowError, OSError, ValueError):
        return float("-inf")


def _deserialize_stage_latency_profile(raw: Any) -> list[float]:
    """Deserialize stage_latency_profile from JSON.

    Handles both the v2.0 object format (``{"bronze_ms": ..., ...}``) and
    the legacy v1.x array format (``[bronze, silver, gold]``).
    """
    if isinstance(raw, dict):
        return [
            raw.get("bronze_ms", 0.0),
            raw.get("silver_ms", 0.0),
            raw.get("gold_ms", 0.0),
        ]
    if isinstance(raw, list):
        return [float(v) for v in raw]
    return []


def _deserialize_benchmark_rounds(
    raw_rounds: list[dict[str, Any]], recorded_at: str | None = None
) -> list[BenchmarkMetrics]:
    """Deserialize benchmark_rounds from JSON into BenchmarkMetrics objects."""
    rounds: list[BenchmarkMetrics] = []
    for r in raw_rounds:
        round_meta = None
        rm = r.get("round_meta")
        if rm:
            # Table health fields from v1.1.0
            th = rm.get("table_health", {})
            round_meta = BenchmarkRoundMeta(
                round_index=rm.get("round_index", 0),
                timestamp=(
                    datetime.fromisoformat(rm["timestamp"]) if rm.get("timestamp") else None
                ),
                # Renamed from gold_freshness_seconds: the probe has always
                # measured event-date age, never pipeline freshness.
                gold_event_age_seconds=rm.get(
                    "gold_event_age_seconds", rm.get("gold_freshness_seconds")
                ),
                q9_contention_observed=rm.get("q9_contention_observed", False),
                q9_retry_used=rm.get("q9_retry_used", False),
                silver_data_file_count=th.get(
                    "silver_data_file_count", rm.get("silver_data_file_count")
                ),
                silver_snapshot_count=th.get(
                    "silver_snapshot_count", rm.get("silver_snapshot_count")
                ),
                gold_data_file_count=th.get("gold_data_file_count", rm.get("gold_data_file_count")),
                gold_snapshot_count=th.get("gold_snapshot_count", rm.get("gold_snapshot_count")),
            )
        rounds.append(
            BenchmarkMetrics(
                mode=r.get("mode", "power"),
                cache=r.get("cache", "hot"),
                scale=r.get("scale", 0),
                qph=r.get("qph", 0.0),
                total_seconds=r.get("total_seconds", 0.0),
                queries=r.get("queries", []),
                query_set_id=r.get("query_set_id")
                or legacy_query_set_id(r.get("queries"), recorded_at),
                engine=recorded_engine(r),
                iterations=r.get("iterations", 1),
                streams=r.get("streams", 1),
                stream_results=r.get("stream_results", []),
                round_meta=round_meta,
                round_record=(
                    {k: r.get(k) for k in _ROUND_RECORD_KEYS}
                    if "executed_query_set_id" in r
                    else None
                ),
            )
        )
    return rounds


def recorded_qph_basis(record: Any) -> dict[str, Any] | None:
    """The ``composite_qph_basis`` of a stored metrics.json record, read from
    its in-stream rounds (collector.composite_qph_basis) so a record written
    before the basis was stored gets the same answer in compare, the perf
    gate and reproduce. A record that keeps no rounds gives its stored basis,
    or None."""
    from .collector import composite_qph_basis

    rounds = _recorded_rounds(record)
    if rounds:
        return composite_qph_basis(rounds)[0]
    if not isinstance(record, dict) or "error" in record:
        return None
    pb = record.get("pipeline_benchmark") or {}
    stored = (pb.get("scores") or {}).get("composite_qph_basis") if isinstance(pb, dict) else None
    return stored if isinstance(stored, dict) else None


def recorded_executed_query_set(record: Any) -> str | None:
    """collector.executed_subset_query_set over a stored record's rounds."""
    from .collector import executed_subset_query_set

    when = record.get("start_time") if isinstance(record, dict) else None
    return executed_subset_query_set(_recorded_rounds(record), when)


def _recorded_rounds(record: Any) -> list[BenchmarkMetrics]:
    """A stored record's in-stream rounds, loaded as ``_dict_to_metrics``
    loads them; rounds that are not objects are skipped."""
    if not isinstance(record, dict) or "error" in record:
        return []
    pb = record.get("pipeline_benchmark") or {}
    raw = pb.get("benchmark_rounds") if isinstance(pb, dict) else None
    if not isinstance(raw, list):
        return []
    return _deserialize_benchmark_rounds(
        [r for r in raw if isinstance(r, dict)], record.get("start_time")
    )


#: The keys MetricsCollector.record_round writes beside a round's benchmark.
_ROUND_RECORD_KEYS = (
    "index",
    "started_at",
    "ended_at",
    "executed_queries",
    "executed_query_set_id",
    "investigator_queries",
)


def recorded_engine(bench: dict[str, Any]) -> str | None:
    """The query engine a recorded benchmark ran on. Records from before the
    ``engine`` field stamped ``benchmark_type`` "trino_query" on every
    engine, so it is not trusted; the per-query result fingerprints carried
    the real engine and are used instead."""
    if bench.get("engine"):
        return str(bench["engine"])
    for q in bench.get("queries") or []:
        fp = q.get("result_fingerprint") if isinstance(q, dict) else None
        if isinstance(fp, dict) and fp.get("engine"):
            return str(fp["engine"])
    return None


def _deserialize_cycles(
    raw_cycles: list[dict[str, Any]], recorded_at: str | None = None
) -> list[CycleMetrics]:
    """Deserialize cycle metrics from JSON."""
    cycles: list[CycleMetrics] = []
    for c in raw_cycles:
        jobs = []
        for jd in c.get("jobs", []):
            jobs.append(
                JobMetrics(
                    job_name=jd.get("job_name", ""),
                    job_type=jd.get("job_type", ""),
                    elapsed_seconds=jd.get("elapsed_seconds", 0),
                    success=jd.get("success", False),
                    error_message=jd.get("error_message"),
                    input_size_gb=jd.get("input_size_gb", 0),
                    output_size_gb=jd.get("output_size_gb", 0),
                    input_rows=jd.get("input_rows", 0),
                    output_rows=jd.get("output_rows", 0),
                )
            )
        bench = None
        bd = c.get("benchmark")
        if bd:
            bench = BenchmarkMetrics(
                mode=bd.get("mode", "power"),
                cache=bd.get("cache", "hot"),
                scale=bd.get("scale", 0),
                qph=bd.get("qph", 0.0),
                total_seconds=bd.get("total_seconds", 0.0),
                queries=bd.get("queries", []),
                query_set_id=bd.get("query_set_id")
                or legacy_query_set_id(bd.get("queries"), recorded_at),
                engine=recorded_engine(bd),
                iterations=bd.get("iterations", 1),
            )
        cycles.append(
            CycleMetrics(
                cycle_index=c.get("cycle_index", 0),
                timestamp_start=c.get("timestamp_start", ""),
                timestamp_end=c.get("timestamp_end", ""),
                datagen_elapsed_seconds=c.get("datagen_elapsed_seconds", 0.0),
                datagen_output_gb=c.get("datagen_output_gb", 0.0),
                jobs=jobs,
                benchmark=bench,
                table_health=c.get("table_health", {}),
            )
        )
    return cycles


class RecordExistsError(FileExistsError):
    """``save_run`` found a record at the run's path and was not told to
    replace it (``seal_update``)."""


def _create_exclusive(tmp_path: str, filepath: Path) -> None:
    """Move *tmp_path* to *filepath*, refusing when *filepath* exists.

    ``os.link`` creates the name atomically and fails if it exists, so two
    writers cannot both succeed. A filesystem without hard links falls back
    to an exists check and a rename (not atomic against a racing writer)."""
    try:
        os.link(tmp_path, filepath)
    except FileExistsError:
        raise RecordExistsError(
            f"{filepath} already exists; a record is written once, by the run that owns it"
        ) from None
    except OSError:
        if filepath.exists():
            raise RecordExistsError(
                f"{filepath} already exists; a record is written once, by the run that owns it"
            ) from None
        os.rename(tmp_path, filepath)
        return
    os.unlink(tmp_path)


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
        # Created on first write (run_dir), not here: report, results and
        # compare only read, and a read-only command creates no files.

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

    def save_run(self, metrics: PipelineMetrics, *, seal_update: bool = False) -> Path:
        """Save pipeline run metrics to ``run-<id>/metrics.json``.

        A record is written once. An existing ``metrics.json`` is replaced
        only with ``seal_update=True``, which only the run that owns the
        record passes; every other writer gets ``RecordExistsError`` and
        the file is left as it was. The write goes to a temporary file in
        the run directory first, so a reader never sees half a record.

        Args:
            metrics: PipelineMetrics to save
            seal_update: replace an existing record (the owning run only)

        Returns:
            Path to saved file
        """
        run = self.run_dir(metrics.run_id)
        filepath = run / "metrics.json"

        import tempfile

        fd, tmp_path = tempfile.mkstemp(dir=run, suffix=".json.tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(metrics.to_dict(), f, indent=2)
            if seal_update:
                os.replace(tmp_path, filepath)
            else:
                _create_exclusive(tmp_path, filepath)
        except BaseException:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

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
        runs: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        if not self.metrics_dir.is_dir():
            return runs

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

        # Re-sort combined list by start_time descending. Parsed, not as
        # strings: runs from v1.6 record UTC with an offset, older ones naive
        # host-local time, and the two do not sort as text.
        runs.sort(key=lambda r: _sort_instant(r.get("start_time")), reverse=True)
        return runs

    @staticmethod
    def _read_run_summary(filepath: Path) -> dict[str, Any] | None:
        """Read a metrics JSON and return a summary dict."""
        try:
            with open(filepath) as f:
                data = json.load(f)

            pb = data.get("pipeline_benchmark")
            pb_scores = pb.get("scores", {}) if pb else {}
            # Carry the persisted verdict block through the summary so
            # ``lakebench report --list`` (and any other summary consumer)
            # can prefer verdict.status over raw ``success`` (OD-6).
            verdict = data.get("verdict") if isinstance(data.get("verdict"), dict) else None
            from lakebench.metrics.verdict import verdict_of

            judged = verdict_of(data)
            return {
                "run_id": data.get("run_id"),
                "deployment_name": data.get("deployment_name"),
                "record_kind": data.get("record_kind") or "run",
                "parent_run_id": data.get("parent_run_id"),
                "start_time": data.get("start_time"),
                "success": data.get("success"),
                "verdict": verdict,
                # The strictest of the stored verdict and the one recomputed
                # from the whole record (a summary row cannot recompute).
                "verdict_recomputed": judged["recomputed"],
                "verdict_headline": judged["status"],
                "passed": judged["status"] == "PASSED",
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
        """Get the most recent run record.

        A ``benchmark`` record (``record_kind``) is never the latest run:
        it is a copy of the run it measured, read by its own run id.

        Returns:
            PipelineMetrics or None if no runs exist
        """
        runs = [r for r in self.list_runs() if r.get("record_kind", "run") == "run"]
        if not runs:
            return None

        return self.load_run(runs[0]["run_id"])

    def get_latest_run_for_deployment(
        self, deployment_name: str | None, *, writable: bool = False
    ) -> PipelineMetrics | None:
        """Newest run whose ``deployment_name`` matches *deployment_name*.

        Parallel deployments share a single lakebench-output tree, so an
        unscoped ``get_latest_run()`` reads whichever deployment happened to
        finish last -- a callsite that then rewrites the record corrupts
        another deployment's history (SP-2 owns the deployment_id fix; this
        helper is the interim scope-by-name path).

        Legacy records recorded before v1.6 did not persist deployment_name.
        For READ callers (``writable=False``), when no exact match is found
        this method falls back to the newest record with no recorded
        deployment name so those old runs stay accessible under best-effort
        matching. Records for a DIFFERENT named deployment are never
        returned.

        For WRITE callers (``writable=True``), the legacy fallback is
        DISABLED and this method returns ``None`` when no exact match
        exists. Without the disable, ``lakebench query dep-new.yaml`` on a
        machine with only pre-v1.6 legacy records would pick the newest
        legacy record (from any deployment) and rewrite it, silently
        corrupting another deployment's history -- the very defect this
        helper is meant to prevent. The write callsites in
        ``cli/_query.py`` pass ``writable=True``.

        With ``deployment_name`` empty or ``None`` this reduces to
        :meth:`get_latest_run` (no scoping requested).

        Args:
            deployment_name: The deployment to scope by, or ``None`` for the
                unscoped latest.
            writable: If ``True``, disable the legacy fallback (write path).

        Returns:
            The scoped PipelineMetrics, or ``None`` if nothing matches.
        """
        if not deployment_name:
            return self.get_latest_run()

        legacy_fallback_id: str | None = None
        for info in self.list_runs():
            if info.get("record_kind", "run") != "run":
                # A benchmark record is a copy of a run, not a run (see
                # get_latest_run); compare's config resolution skips it too.
                continue
            recorded = info.get("deployment_name")
            if recorded == deployment_name:
                return self.load_run(info["run_id"])
            if not recorded and legacy_fallback_id is None:
                legacy_fallback_id = info.get("run_id")

        if writable:
            # Write path: never touch a legacy record; return None so the
            # caller can create a fresh scoped record instead of rewriting
            # someone else's history.
            return None
        if legacy_fallback_id is not None:
            return self.load_run(legacy_fallback_id)
        return None

    def _iter_metrics_files(self):
        """Yield all metrics JSON file paths (new + legacy layouts)."""
        if not self.metrics_dir.is_dir():
            return
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
        # When the run was recorded: a benchmark from before query-set ids
        # gets a pinned legacy id only if it is newer than the last SQL change.
        recorded_at = data.get("start_time")
        jobs = []
        for job_data in data.get("jobs", []):
            # Class-level reload: iterate dataclass fields so every JobMetrics
            # field flows automatically. The hand-written kwargs list here
            # previously omitted silver_tables + extra_metrics -- disk showed
            # them via asdict() but load reverted to defaults (LB-123 shape on
            # the READ side, adversarial-review finding 2026-09-28). Same
            # mirror defect on StreamingJobMetrics.extra_metrics, fixed below.
            job = _dataclass_from_dict(JobMetrics, job_data, aliases=_JOB_ALIASES)

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
            streaming.append(_dataclass_from_dict(StreamingJobMetrics, s_data))

        # Deserialize top-level benchmark rounds
        top_rounds = _deserialize_benchmark_rounds(data.get("benchmark_rounds", []), recorded_at)

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
            benchmark_rounds=top_rounds,
            platform_metrics=data.get("platform_metrics"),
            cycles=_deserialize_cycles(data.get("cycles", []), recorded_at),
            datagen_fleet=data.get("datagen_fleet"),
            datagen_stale_bronze=(data.get("datagen") or {}).get("stale_bronze"),
            financial_scoring=data.get("financial_scoring"),
            tm_operations=data.get("tm_operations"),
            c360_correctness=data.get("c360_correctness"),
            maintenance_policy_id=recorded_policy(data),
            provenance=data.get("provenance"),
            benchmark_error=data.get("benchmark_error"),
            failure_reasons=[str(r) for r in data.get("failure_reasons") or []],
            autosize_cuts=data.get("autosize_cuts"),
            job_timeout_seconds=data.get("job_timeout_seconds"),
            benchmark_query_timeout_seconds=data.get("benchmark_query_timeout_seconds"),
            maintenance_outcomes=data.get("maintenance_outcomes"),
            continuous=data.get("continuous"),
            interrupted=data.get("interrupted"),
            abort_reason=data.get("abort_reason"),
            series=data.get("series"),
            record_kind=str(data.get("record_kind") or "run"),
            parent_run_id=data.get("parent_run_id"),
            stage_only=data.get("stage_only"),
            # Kept as written. A record from before the block has none, and its
            # snapshot has no experiment inputs, so it never gets one.
            experiment=data.get("experiment"),
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
                query_set_id=bench_data.get("query_set_id")
                or legacy_query_set_id(bench_data.get("queries"), recorded_at),
                engine=recorded_engine(bench_data),
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
                    timing_source=s_data.get("timing_source") or "",
                    timing_resolution_seconds=s_data.get("timing_resolution_seconds"),
                    submission_failures=list(s_data.get("submission_failures") or []),
                    submission_retry_seconds=s_data.get("submission_retry_seconds") or 0.0,
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
                    latency_ms=s_data.get("latency_ms"),
                    freshness_seconds=s_data.get("freshness_seconds"),
                    freshness_active_seconds=s_data.get("freshness_active_seconds"),
                    trailing_idle_cycles=s_data.get("trailing_idle_cycles", 0),
                    committed_rows=s_data.get("committed_rows"),
                    batch_span_seconds=s_data.get("batch_span_seconds"),
                    ttd_alerts=s_data.get("ttd_alerts"),
                    ttd_unmatched=s_data.get("ttd_unmatched", 0),
                    ttd_late=s_data.get("ttd_late", 0),
                    ttd_unmeasured_cycles=s_data.get("ttd_unmeasured_cycles", 0),
                    ttd_p50_seconds=s_data.get("ttd_p50_seconds"),
                    ttd_p95_seconds=s_data.get("ttd_p95_seconds"),
                    ttd_max_seconds=s_data.get("ttd_max_seconds"),
                    total_batches=s_data.get("total_batches", 0),
                    batch_size=s_data.get("batch_size", 0),
                    unique_rows_processed=s_data.get("unique_rows_processed"),
                    window_input_rows=s_data.get("window_input_rows"),
                    pre_window_input_rows=s_data.get("pre_window_input_rows"),
                    window_commits=s_data.get("window_commits"),
                    window_new_data_cycles=s_data.get("window_new_data_cycles"),
                    last_write_offset_seconds=s_data.get("last_write_offset_seconds"),
                    trickle_start_offset_seconds=s_data.get("trickle_start_offset_seconds"),
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
                    query_set_id=qb_data.get("query_set_id")
                    or legacy_query_set_id(qb_data.get("queries"), recorded_at),
                    engine=recorded_engine(qb_data),
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
                total_core_hours=scores.get("total_core_hours", 0.0),
                compute_efficiency_gb_per_core_hour=scores.get(
                    "compute_efficiency_gb_per_core_hour", 0.0
                ),
                # Batch only
                scale_ratio=scores.get("scale_ratio", scores.get("scale_verified_ratio", 0.0)),
                # Continuous scores (None when unmeasurable)
                data_freshness_seconds=scores.get("data_freshness_seconds"),
                sustained_throughput_rps=scores.get("sustained_throughput_rps", 0.0),
                stage_latency_profile=_deserialize_stage_latency_profile(
                    scores.get("stage_latency_profile", [])
                ),
                total_rows_processed=scores.get("total_rows_processed", 0),
                ingest_ratio=scores.get(
                    "ingest_ratio", scores.get("ingestion_completeness_ratio", 0.0)
                ),
                pipeline_saturated=scores.get("pipeline_saturated", False),
                corpus_drained=scores.get("corpus_drained"),
                intake_limit=scores.get("intake_limit"),
                bronze_busy_fraction=scores.get("bronze_busy_fraction"),
                corpus_drain_seconds=scores.get("corpus_drain_seconds"),
                window_seconds=scores.get("window_seconds"),
                corpus_ingest_ratio=scores.get("corpus_ingest_ratio"),
                released_rows=scores.get("released_rows"),
                arrival_seconds=scores.get("arrival_seconds"),
                window_arrival_fraction=scores.get("window_arrival_fraction"),
                pre_window_rows=scores.get("pre_window_rows"),
                time_to_detect_seconds=scores.get("time_to_detect_seconds"),
                time_to_detect_p95_seconds=scores.get("time_to_detect_p95_seconds"),
                time_to_detect_max_seconds=scores.get("time_to_detect_max_seconds"),
                time_to_detect_alerts=scores.get("time_to_detect_alerts"),
                time_to_detect_late_alerts=scores.get("time_to_detect_late_alerts"),
                time_to_detect_unmeasured_cycles=scores.get("time_to_detect_unmeasured_cycles"),
                total_s3_objects=scores.get("total_s3_objects", 0),
                query_benchmark=query_benchmark,
                config_snapshot=pb_data.get("config_snapshot", {}),
                success=pb_data.get("success", False),
                benchmark_rounds=_deserialize_benchmark_rounds(
                    pb_data.get("benchmark_rounds", []), recorded_at
                ),
                cycles=_deserialize_cycles(pb_data.get("cycles", []), recorded_at),
                qph_degradation_pct=scores.get("qph_degradation_pct"),
                qph_degradation_withheld=scores.get("qph_degradation_withheld"),
                # Maintenance metrics (v1.3)
                maintenance_elapsed_seconds=scores.get("maintenance_elapsed_seconds", 0.0),
                maintenance_stopped=bool(scores.get("maintenance_stopped", False)),
                maintenance_stop_reason=scores.get("maintenance_stop_reason", ""),
                maintenance_live_streams=bool(scores.get("maintenance_live_streams", False)),
                maintenance_live_streams_reason=scores.get("maintenance_live_streams_reason", ""),
                maintenance_pct_of_pipeline=scores.get("maintenance_pct_of_pipeline", 0.0),
                pre_compaction_file_count=scores.get("pre_compaction_file_count", 0),
                post_compaction_file_count=scores.get("post_compaction_file_count", 0),
                compaction_ratio=scores.get("compaction_ratio", 0.0),
                pre_compaction_qph=scores.get("pre_compaction_qph", 0.0),
                post_compaction_qph=scores.get("post_compaction_qph", 0.0),
                maintenance_value_pct=scores.get("maintenance_value_pct"),
                maintenance_paired_queries=scores.get("maintenance_paired_queries", 0),
                maintenance_value_reason=scores.get("maintenance_value_reason", ""),
                pre_compaction_benchmark=pb_data.get("pre_compaction_benchmark"),
                maintenance_settle_seconds=scores.get("maintenance_settle_seconds"),
                maintenance_settled=scores.get("maintenance_settled"),
                maintenance_settle_capped=scores.get("maintenance_settle_capped", False),
                maintenance_settle_verified=scores.get("maintenance_settle_verified"),
                maintenance_settle=pb_data.get("maintenance_settle"),
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
                if (data.get("record_kind") or "run") != "run":
                    continue  # a benchmark record repeats its run's pipeline numbers

                from lakebench.metrics.verdict import passed as _record_passed
                from lakebench.metrics.verdict import verdict_status as _verdict_status

                row: dict[str, Any] = {
                    "run_id": data.get("run_id"),
                    "deployment_name": data.get("deployment_name"),
                    "start_time": data.get("start_time"),
                    # OD-6: 'success' now reflects the verdict when a v1.6
                    # record has one, falling back to raw success for
                    # legacy v1.5 exports. verdict_status is added so a
                    # downstream reader can tell PASSED from REFUSED etc.
                    "success": _record_passed(data),
                    "verdict_status": _verdict_status(data),
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
