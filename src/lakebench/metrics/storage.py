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

import dataclasses
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
    """Reconstruct a dataclass instance from a JSON-dict, iterating fields,
    so a new field on the dataclass loads as it was saved (asdict) without a
    hand-written kwargs list that can fall behind.

    * datetime fields (start_time / end_time on JobMetrics) are handled by
      the caller AFTER construction, since the dict carries ISO strings.
      This helper skips them.
    * aliases maps canonical -> tuple of legacy names; used for renamed
      fields so pre-rename metrics.json still loads correctly.
    * A missing key falls back to the dataclass default (default value
      or default_factory), matching the behaviour of an omitted kwarg.
    """
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
        bench = _benchmark_from_dict(r, recorded_at)
        bench.round_meta = round_meta
        if "executed_query_set_id" in r:
            bench.round_record = {k: r.get(k) for k in _ROUND_RECORD_KEYS}
        rounds.append(bench)
    return rounds


def _benchmark_from_dict(d: dict[str, Any], recorded_at: str | None) -> BenchmarkMetrics:
    """A stored benchmark. One from before query-set ids gets a pinned legacy
    id only if it is newer than the last SQL change (*recorded_at*)."""
    return BenchmarkMetrics(
        mode=d.get("mode", "power"),
        cache=d.get("cache", "hot"),
        scale=d.get("scale", 0),
        qph=d.get("qph", 0.0),
        total_seconds=d.get("total_seconds", 0.0),
        queries=d.get("queries", []),
        query_set_id=d.get("query_set_id") or legacy_query_set_id(d.get("queries"), recorded_at),
        engine=recorded_engine(d),
        iterations=d.get("iterations", 1),
        streams=d.get("streams", 1),
        stream_results=d.get("stream_results", []),
    )


def _job_from_dict(d: dict[str, Any]) -> JobMetrics:
    job = _dataclass_from_dict(JobMetrics, d, aliases=_JOB_ALIASES)
    if d.get("start_time"):
        job.start_time = datetime.fromisoformat(d["start_time"])
    if d.get("end_time"):
        job.end_time = datetime.fromisoformat(d["end_time"])
    return job


def _stage_from_dict(d: dict[str, Any]) -> StageMetrics:
    """A stored stage. Every field written is read back; a stored None stays
    None (``output_rows``: not measured), except the fields an older record
    wrote as null and whose default is not."""
    kwargs: dict[str, Any] = {"stage_name": "", "stage_type": "", "engine": ""}
    for f in dataclasses.fields(StageMetrics):
        if f.name in d and f.name not in ("start_time", "end_time"):
            kwargs[f.name] = d[f.name]
    kwargs["timing_source"] = kwargs.get("timing_source") or ""
    kwargs["submission_failures"] = list(kwargs.get("submission_failures") or [])
    kwargs["submission_retry_seconds"] = kwargs.get("submission_retry_seconds") or 0.0
    stage = StageMetrics(**kwargs)
    if d.get("start_time"):
        stage.start_time = datetime.fromisoformat(d["start_time"])
    if d.get("end_time"):
        stage.end_time = datetime.fromisoformat(d["end_time"])
    return stage


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
    engine, so it is not trusted; the per-query result fingerprints those
    records carry name the real engine and are used instead."""
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
        bd = c.get("benchmark")
        cycles.append(
            CycleMetrics(
                cycle_index=c.get("cycle_index", 0),
                timestamp_start=c.get("timestamp_start", ""),
                timestamp_end=c.get("timestamp_end", ""),
                datagen_elapsed_seconds=c.get("datagen_elapsed_seconds", 0.0),
                datagen_output_gb=c.get("datagen_output_gb", 0.0),
                datagen_skipped=bool(c.get("datagen_skipped", False)),
                datagen_start=c.get("datagen_start", "") or "",
                datagen_end=c.get("datagen_end", "") or "",
                jobs=[_job_from_dict(jd) for jd in c.get("jobs", [])],
                benchmark=_benchmark_from_dict(bd, recorded_at) if bd else None,
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


def _stored_scores(data: dict[str, Any]) -> dict[str, Any] | None:
    scores = (data.get("pipeline_benchmark") or {}).get("scores")
    return scores if isinstance(scores, dict) else None


def _read_run_key(filepath: Path) -> dict[str, Any] | None:
    """The keys a latest-run lookup reads from a metrics JSON."""
    try:
        with open(filepath) as f:
            data = json.load(f)
        return {
            "run_id": data.get("run_id"),
            "deployment_name": data.get("deployment_name"),
            "record_kind": data.get("record_kind") or "run",
            "start_time": data.get("start_time"),
        }
    except (json.JSONDecodeError, KeyError):
        return None


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
        return self._scan(self._read_run_summary)

    def _run_index(self) -> list[dict[str, Any]]:
        """``list_runs`` rows with only the keys the latest-run lookups read
        (run_id, deployment_name, record_kind, start_time), in the same
        order, without judging every record."""
        return self._scan(_read_run_key)

    def _scan(self, read: Any) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for filepath in self._iter_metrics_files():
            summary = read(filepath)
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
            # The stored verdict block, so ``lakebench report --list`` can
            # prefer verdict.status over raw ``success``.
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
                # from the whole record (verdict.verdict_of): a reader of the
                # row cannot recompute, so the row carries it.
                "verdict_recomputed": judged["recomputed"],
                "verdict_headline": judged["status"],
                "passed": judged["status"] == "PASSED",
                "total_elapsed_seconds": data.get("total_elapsed_seconds"),
                "job_count": len(data.get("jobs", [])),
                "scale": data.get("config_snapshot", {}).get("scale"),
                "processing_pattern": data.get("config_snapshot", {}).get("processing_pattern"),
                "qph": data.get("benchmark", {}).get("qph") if data.get("benchmark") else None,
                "bronze_size_gb": data.get("bronze_size_gb"),
                "silver_size_gb": data.get("silver_size_gb"),
                "gold_size_gb": data.get("gold_size_gb"),
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
        runs = [r for r in self._run_index() if r.get("record_kind", "run") == "run"]
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
        another deployment's history. This scopes by deployment name.

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
        for info in self._run_index():
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
        recorded_at = data.get("start_time")
        jobs = [_job_from_dict(job_data) for job_data in data.get("jobs", [])]

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
            bronze_size_gb=data.get("bronze_size_gb"),
            silver_size_gb=data.get("silver_size_gb"),
            gold_size_gb=data.get("gold_size_gb"),
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
            cycle_series=data.get("cycle_series"),
            # Kept as written. A record from before the block has none, and its
            # snapshot has no experiment inputs, so it never gets one.
            experiment=data.get("experiment"),
            stored_verdict=data.get("verdict") if isinstance(data.get("verdict"), dict) else None,
            stored_scores=_stored_scores(data),
            storage_multiple=data.get("storage_multiple"),
        )

        if data.get("end_time"):
            metrics.end_time = datetime.fromisoformat(data["end_time"])

        bench_data = data.get("benchmark")
        if bench_data:
            metrics.benchmark = _benchmark_from_dict(bench_data, recorded_at)

        # Deserialize pipeline benchmark if present
        pb_data = data.get("pipeline_benchmark")
        if pb_data:
            pb_stages = [_stage_from_dict(s_data) for s_data in pb_data.get("stages", [])]
            qb_data = pb_data.get("query_benchmark")
            query_benchmark = _benchmark_from_dict(qb_data, recorded_at) if qb_data else None

            scores = pb_data.get("scorecard", pb_data.get("scores", {}))
            _basis = scores.get("composite_qph_basis")
            metrics.pipeline_benchmark = PipelineBenchmark(
                # A composite recorded before it was over one fixed set:
                # no basis, or a basis without composite_set.
                blended_composite_recorded=bool(scores.get("composite_qph"))
                and not (isinstance(_basis, dict) and "composite_set" in _basis),
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
                time_to_value_datagen_excluded_seconds=scores.get(
                    "time_to_value_datagen_excluded_seconds"
                ),
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
                bronze_log_incomplete=bool(scores.get("bronze_log_incomplete", False)),
                ingest_ratio_unscored_reason=scores.get("ingest_ratio_unscored_reason"),
                stage_capacity=pb_data.get("stage_capacity"),
                query_time_event_age_seconds=(pb_data.get("diagnostics") or {}).get(
                    "query_time_event_age_seconds", 0.0
                ),
                pipeline_saturated=scores.get("pipeline_saturated", False),
                corpus_drained=scores.get("corpus_drained"),
                intake_limit=scores.get("intake_limit"),
                bronze_busy_fraction=scores.get("bronze_busy_fraction"),
                backlog_rows=scores.get("backlog_rows"),
                datagen_ahead=scores.get("datagen_ahead"),
                pace_seconds_per_million_rows=scores.get("pace_seconds_per_million_rows"),
                bronze_pace_seconds_per_million_rows=scores.get(
                    "bronze_pace_seconds_per_million_rows"
                ),
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
                total_s3_objects=scores.get("total_s3_objects"),
                query_benchmark=query_benchmark,
                config_snapshot=pb_data.get("config_snapshot", {}),
                success=pb_data.get("success", False),
                benchmark_rounds=_deserialize_benchmark_rounds(
                    pb_data.get("benchmark_rounds", []), recorded_at
                ),
                cycles=_deserialize_cycles(pb_data.get("cycles", []), recorded_at),
                qph_degradation_pct=scores.get("qph_degradation_pct"),
                qph_degradation_withheld=scores.get("qph_degradation_withheld"),
                maintenance_elapsed_seconds=scores.get("maintenance_elapsed_seconds", 0.0),
                maintenance_stopped=bool(scores.get("maintenance_stopped", False)),
                maintenance_stop_reason=scores.get("maintenance_stop_reason", ""),
                maintenance_live_streams=bool(scores.get("maintenance_live_streams", False)),
                maintenance_live_streams_reason=scores.get("maintenance_live_streams_reason", ""),
                maintenance_pct_of_pipeline=scores.get("maintenance_pct_of_pipeline", 0.0),
                pre_compaction_file_count=scores.get("pre_compaction_file_count", 0),
                post_compaction_file_count=scores.get("post_compaction_file_count", 0),
                compaction_ratio=scores.get("compaction_ratio", 0.0),
                snapshots_expired=scores.get("snapshots_expired", 0),
                orphan_files_removed=scores.get("orphan_files_removed", 0),
                storage_reclaimed_mb=scores.get("storage_reclaimed_mb", 0.0),
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
