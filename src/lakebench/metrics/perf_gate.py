"""Performance-regression gate: pinned configs, a baseline store, a compare step.

A pinned config (``benchmarks/perf/*.yaml``) sets every sizing knob
explicitly. The baseline store (``benchmarks/perf/baselines.yaml``) holds the
accepted numbers for each pinned config, with the run id, git sha and config
hash they came from. ``compare`` checks a new run against its baseline and
refuses to compare anything that is not like for like.

Two hashes guard "like for like":

- ``config_hash`` is the sha256 of the pinned YAML as parsed (comments do
  not count, values do). The baseline records it; if the pinned file has
  changed since, the baseline no longer describes it and compare refuses.
- ``fingerprint`` is the sha256 of the sizing-relevant part of the run's
  recorded ``config_snapshot`` (the resolved values after autosizing and any
  cluster capping). The pinned file's fingerprint is computed the same way,
  so a run that silently ran with 8 Trino workers instead of 2 is refused
  rather than compared. Version 2 (``FINGERPRINT_VERSION``) also hashes the
  job profiles and the Spark conf each job's manifest asks for
  (``metrics/fingerprint_inputs.py``); the version and those inputs are
  stamped into the snapshot when the run starts and read, never rebuilt.

A baseline also records the dependency pinset of its run
(``provenance.deps.pinset_sha256``); a run on another set is refused.

Metric classification and direction come from the metric registry
(``metrics/metric_registry.reproduce_class``), as reproduce's do; this
module adds no second table. Only the performance band is compared here: the correctness signals
(``scale_ratio``, ``corpus_drained``) are guards that refuse a run.

Design and workflow: docs/perf-regression-gate.md.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from lakebench.cli._reproduce import (
    QUERY_QPH_PREFIX,
    _drift_pct,
    _extract_expected_numbers,
    _is_stage_seconds,
)
from lakebench.metrics.experiment import (
    NO_PROVENANCE,
    corpus_problems,
    experiment_of,
    failed_queries,
    identity,
    result_fingerprints,
    results_established,
    stored_identity_refusals,
)
from lakebench.metrics.fingerprint_inputs import FINGERPRINT_VERSION
from lakebench.metrics.maintenance_policy import not_current, policy_mismatch, recorded_policy
from lakebench.metrics.metric_registry import reproduce_class as _classify_direction

# Schema 2 adds fingerprint_version and pinset_sha256 to every baseline.
# load_store reads 1 and 2; save always writes 2.
STORE_SCHEMA_VERSION = 2
_READABLE_STORE_SCHEMAS = (1, 2)

# Refuse batch runs that processed less than this share of the expected
# bronze volume (a scale_ratio of 0 means the volume was not measured).
MIN_SCALE_RATIO = 0.95
# ...and refuse more than this: extra data (a bronze bucket not emptied
# before a regenerate) flatters GB/s and GB/core-hr just as missing data
# flatters time to value.
MAX_SCALE_RATIO = 1.10

# A continuous stage runs for the whole window, so its elapsed seconds is
# the window length. Refuse a run whose window differs from the pinned
# run_duration by more than this (a --duration override is not recorded in
# the snapshot).
WINDOW_TOLERANCE_PCT = 10.0

# Continuous runs: bronze rows over datagen rows above this means data was
# ingested twice (stale or re-ingested corpus), which inflates rows/s. Below
# 1.0 is saturation, a real performance signal, and stays comparable.
MAX_INGEST_RATIO = 1.05

# The datagen fleet numbers come from a sidecar written by the last
# `lakebench generate` in the namespace. One written more than this before
# the run started belongs to an earlier generate: its datagen numbers are
# left out rather than attributed to the run.
MAX_DATAGEN_AGE_HOURS = 24.0
# Before v1.6 start_time is naive local time on the host that ran lakebench,
# and the zone is not recorded (from v1.6 it is UTC with its offset, and the
# age is exact). UTC offsets run from -12h to +14h, so the age is
# taken at its smallest over every zone: a sidecar is only called stale when
# it is stale wherever the run happened, whichever host runs the gate.
_MAX_UTC_OFFSET_HOURS = 14.0

# Metrics that only describe the datagen stage. Nothing else is left out
# with them: time to value and GB/s are recomputed from the pipeline stages
# (see _pipeline_ttv), and GB/core-hr counts batch or continuous stages only
# (collector._compute_batch_scores / _compute_sustained_scores).
_DATAGEN_METRICS = frozenset(
    {"datagen_seconds", "datagen_aggregate_mbps", "datagen_cpu_hr_per_tb", "datagen_mbps_per_pod"}
)
# How time_to_value_seconds was taken: from the pipeline stages' own
# timestamps (datagen excluded by construction), or the scorecard value,
# which falls back to the run's wall clock when stages carry no timestamps
# and so may or may not include a generate.
TTV_FROM_STAGES = "stages"
TTV_FROM_SCORECARD = "scorecard"
# Stages whose presence does not decide comparability: datagen is handled
# by the exclusion above, and a missing query stage is a regression that the
# QpH metrics report as missing.
_OPTIONAL_STAGE_KEYS = frozenset({"datagen_seconds", "query_seconds"})

# Default tolerances. "pct" is percent drift in the bad direction; "abs" is
# an absolute difference in the metric's own unit.
DEFAULT_TOLERANCE: dict[str, float] = {"pct": 10.0}
# One query takes seconds at scale 10, so scheduling noise moves it more
# than it moves a whole-pipeline number.
DEFAULT_QUERY_TOLERANCE: dict[str, float] = {"pct": 20.0}

STATUS_ACCEPTED = "accepted"
STATUS_PENDING = "pending first run"

# Verdicts
PASS = "PASS"
REGRESSION = "REGRESSION"
REFUSED = "REFUSED"
NO_BASELINE = "NO_BASELINE"
# Pre-benchmark maintenance stopped and no QpH metric was left to gate: the
# run can prove neither a pass nor a regression (e.g. scale >= 50, where no
# pre-maintenance benchmark runs). Never a pass.
NOT_COMPARABLE = "NOT_COMPARABLE"

# Placeholders for the ${VAR} references in pinned configs. Only used when
# the variable is unset; values never reach the fingerprint (identity and
# credential fields are excluded from it).
_PLACEHOLDER_ENV = {
    "LAKEBENCH_S3_ENDPOINT": "http://127.0.0.1:9",
    "LAKEBENCH_S3_ACCESS_KEY": "placeholder",
    "LAKEBENCH_S3_SECRET_KEY": "placeholder",
    "LAKEBENCH_POLARIS_CLIENT_SECRET": "placeholder",
}

# Batch stage name in PipelineBenchmark -> executor override key in the
# snapshot. Batch stages carry the executor count the run observed (the
# progress callback's peak, _run.py); continuous stages do not have one:
# collector.build_pipeline_benchmark fills theirs from the same snapshot
# overrides this would compare against, so a continuous check could never
# fail and there is none (docs/perf-regression-gate.md, "Known gaps").
_BATCH_STAGE_OVERRIDE_KEY = {"bronze": "bronze", "silver": "silver", "gold": "gold"}


class PerfGateError(Exception):
    """A pinned config, baseline store or run could not be read."""


# ---------------------------------------------------------------------------
# Hashing and fingerprints
# ---------------------------------------------------------------------------


def _normalise(value: Any) -> Any:
    """Canonical form for hashing: sorted dicts, integral floats as ints."""
    if isinstance(value, Mapping):
        return {str(k): _normalise(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalise(v) for v in value]
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return int(value)
    return value


def _sha(obj: Any) -> str:
    blob = json.dumps(_normalise(obj), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode()).hexdigest()


def config_file_hash(path: Path) -> str:
    """sha256 of a pinned YAML as parsed, before ${VAR} substitution."""
    try:
        data = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as e:
        raise PerfGateError(f"cannot read pinned config {path}: {e}") from None
    return _sha(data)


def normalise_mode(mode: str | None) -> str:
    mode = (mode or "batch").lower()
    return "sustained" if mode == "continuous" else mode


_FINGERPRINT_KEYS = (
    "scale",
    "processing_pattern",
    "catalog",
    "table_format",
    "pipeline_engine",
    "query_engine",
    "workload_schema",
    "spark",
    "datagen",
    "images",
    "benchmark",
    "maintenance",
)


def snapshot_fingerprint_version(snapshot: Mapping[str, Any]) -> int | None:
    """The fingerprint version a snapshot was stamped with; 1 when unstamped.

    None when the stamp is not an integer, which no version reads.
    """
    value = snapshot.get("fingerprint_version", 1)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def snapshot_fingerprint(snapshot: Mapping[str, Any], mode: str) -> dict[str, Any]:
    """The sizing-relevant subset of a ``build_config_snapshot`` dict.

    Identity fields (name, S3 endpoint and buckets) are left out: they differ
    between deployments of the same pinned config. Keys missing from an old
    snapshot become None, so a run recorded before a field existed does not
    match a config that pins it.

    The projection follows the version the snapshot was stamped with, so a
    v1.6 record is read as version 1 and never given a version 2 shape.
    Version 2 drops the Spark driver and executor blocks and the scratch size
    (they sized nothing) and adds the per-job scratch sizes and the stamped
    ``fingerprint_inputs``.
    """
    mode = normalise_mode(mode)
    version = snapshot_fingerprint_version(snapshot)
    fp: dict[str, Any] = {"pipeline_mode": mode}
    for key in _FINGERPRINT_KEYS:
        fp[key] = snapshot.get(key)
    scratch = snapshot.get("scratch") or {}
    if version == FINGERPRINT_VERSION:
        fp["fingerprint_version"] = version
        fp["scratch"] = {k: scratch.get(k) for k in ("enabled", "storage_class", "size_per_job")}
        fp["fingerprint_inputs"] = snapshot.get("fingerprint_inputs")
    else:
        if version != 1:
            fp["fingerprint_version"] = snapshot.get("fingerprint_version")
        fp["scratch"] = {k: scratch.get(k) for k in ("enabled", "storage_class", "size")}
    if snapshot.get("query_engine") == "trino":
        fp["trino"] = snapshot.get("trino")
    if mode == "sustained":
        fp["sustained"] = snapshot.get("sustained")
    return _normalise(fp)


def fingerprint_version_refusal(snapshot: Mapping[str, Any]) -> str | None:
    """Why a run snapshot cannot be fingerprinted as version 2, or None."""
    version = snapshot_fingerprint_version(snapshot)
    if version != FINGERPRINT_VERSION:
        if version == 1:
            return (
                f"run predates fingerprint v{FINGERPRINT_VERSION} (its snapshot has no "
                "fingerprint_version); re-run it with this version"
            )
        return (
            f"run snapshot has fingerprint_version {snapshot.get('fingerprint_version')!r}, "
            f"not {FINGERPRINT_VERSION}"
        )
    inputs = snapshot.get("fingerprint_inputs")
    if not isinstance(inputs, Mapping):
        return "run snapshot has no fingerprint_inputs"
    if inputs.get("error"):
        return f"run fingerprint inputs could not be built: {inputs['error']}"
    return None


def run_pinset(raw: Mapping[str, Any]) -> str | None:
    """The dependency pinset a run recorded (``provenance.deps.pinset_sha256``).

    None when the record has none: every record before the in-deployment
    dependency server writes it.
    """
    provenance = raw.get("provenance")
    deps = provenance.get("deps") if isinstance(provenance, Mapping) else None
    value = deps.get("pinset_sha256") if isinstance(deps, Mapping) else None
    return value if isinstance(value, str) and value else None


def _short(sha: str | None) -> str:
    return sha[:12] if sha else "none recorded"


def fingerprint_hash(fp: Mapping[str, Any]) -> str:
    return _sha(fp)


def _flatten(d: Any, prefix: str = "") -> Iterator[tuple[str, Any]]:
    if isinstance(d, Mapping):
        for k, v in d.items():
            yield from _flatten(v, f"{prefix}.{k}" if prefix else str(k))
    else:
        yield prefix, d


def fingerprint_diff(expected: Mapping[str, Any], actual: Mapping[str, Any]) -> list[str]:
    """Human-readable 'path: pinned X, run Y' lines for differing leaves."""
    exp = dict(_flatten(expected))
    act = dict(_flatten(actual))
    lines = []
    for path in sorted(set(exp) | set(act)):
        if exp.get(path) != act.get(path):
            lines.append(f"{path}: pinned {exp.get(path)!r}, run {act.get(path)!r}")
    return lines


@dataclass
class PinnedConfig:
    name: str
    path: Path
    config_hash: str
    mode: str
    fingerprint: dict[str, Any]
    file_sha256: str = ""

    @property
    def fingerprint_hash(self) -> str:
        return fingerprint_hash(self.fingerprint)


def load_pinned(path: Path, name: str | None = None) -> PinnedConfig:
    """Load a pinned config and compute both of its hashes.

    The fingerprint is taken from ``build_config_snapshot`` after the same
    offline autosizing ``lakebench run`` applies, so it is the snapshot a
    faithful run of this file records (before any cluster capping).
    """
    from lakebench.config import LoadPurpose, load_config
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.metrics.collector import build_config_snapshot

    path = Path(path)
    if not path.is_file():
        raise PerfGateError(f"pinned config {path} not found")
    config_hash = config_file_hash(path)
    added = {k: v for k, v in _PLACEHOLDER_ENV.items() if not os.environ.get(k)}
    os.environ.update(added)
    try:
        # The fingerprint does not depend on the name, so a too-long
        # LAKEBENCH_PERF_NAME left in the environment must not fail the gate.
        # RUN: a pinned config is run with `lakebench run`, so it must load as
        # run loads it (removed keys refused), not as a teardown.
        cfg = load_config(path, purpose=LoadPurpose.RUN, allow_long_names=True)
        resolve_auto_sizing(cfg, None)
    except Exception as e:  # noqa: BLE001 -- any load failure is a gate failure
        raise PerfGateError(f"pinned config {path} does not load: {e}") from None
    finally:
        for k in added:
            os.environ.pop(k, None)
    mode = normalise_mode(cfg.architecture.pipeline.mode.value)
    # A baseline measures the proven profile: a pinned executor count that
    # differs from what the profile asks at the pinned scale, or a driver
    # override, sizes the run some other way.
    from lakebench.modules.pipeline_engines.spark.job import (
        BATCH_JOB_TYPES,
        STREAMING_JOB_TYPES,
        executor_override,
        get_executor_count,
    )

    schema = cfg.architecture.workload.schema_type.value
    scale = cfg.architecture.workload.datagen.get_effective_scale()
    off_profile = []
    for jt in STREAMING_JOB_TYPES if mode == "sustained" else BATCH_JOB_TYPES:
        pinned = executor_override(jt, cfg)
        asks = get_executor_count(jt, scale, schema)
        if pinned is not None and pinned != asks:
            off_profile.append(f"{jt} {pinned} (profile asks {asks})")
    spark_compute = cfg.platform.compute.spark
    if off_profile or spark_compute.driver_cores is not None or spark_compute.driver_memory:
        what = list(off_profile)
        if spark_compute.driver_cores is not None or spark_compute.driver_memory:
            what.append("a driver override")
        raise PerfGateError(
            f"pinned config {path} does not size the run by the proven profile: "
            + "; ".join(what)
            + ". Pin each executor count at the profile's count, or leave it unset"
        )
    if mode == "sustained":
        # The continuous concurrent budget caps unpinned executor counts by
        # cluster size, and the fingerprint records the count before that
        # cap: two clusters would run different counts under one fingerprint.
        # A pinned count beats the budget.
        spark = cfg.platform.compute.spark
        unpinned = [
            key
            for key in (
                "bronze_ingest_executors",
                "silver_stream_executors",
                "gold_refresh_executors",
            )
            if getattr(spark, key) is None
        ]
        if unpinned:
            raise PerfGateError(
                f"pinned config {path}: a continuous pinned config must set "
                f"platform.compute.spark.{', '.join(unpinned)} (the concurrent budget "
                "would otherwise size them by cluster)"
            )
    # Through JSON, as a run's snapshot reaches the gate from metrics.json,
    # so both sides hash the same representation.
    snapshot = json.loads(json.dumps(build_config_snapshot(cfg), default=str))
    inputs = snapshot.get("fingerprint_inputs") or {}
    if inputs.get("error"):
        raise PerfGateError(
            f"pinned config {path}: fingerprint inputs could not be built: {inputs['error']}"
        )
    fp = snapshot_fingerprint(snapshot, mode)
    return PinnedConfig(
        name=name or path.stem,
        path=path,
        config_hash=config_hash,
        mode=mode,
        fingerprint=fp,
        file_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
    )


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


@dataclass
class RunRecord:
    run_id: str
    path: Path
    raw: dict[str, Any]
    metrics: Any  # PipelineMetrics

    @property
    def pb_raw(self) -> dict[str, Any]:
        return self.raw.get("pipeline_benchmark") or {}

    @property
    def mode(self) -> str:
        return normalise_mode(self.pb_raw.get("pipeline_mode"))

    @property
    def snapshot(self) -> dict[str, Any]:
        return self.raw.get("config_snapshot") or self.pb_raw.get("config_snapshot") or {}

    @property
    def fingerprint(self) -> dict[str, Any]:
        return snapshot_fingerprint(self.snapshot, self.mode)

    @property
    def scores(self) -> dict[str, Any]:
        return self.pb_raw.get("scorecard") or self.pb_raw.get("scores") or {}


def _canonical_json(path: Path) -> str:
    try:
        return json.dumps(json.loads(path.read_text()), sort_keys=True)
    except (OSError, json.JSONDecodeError):
        return f"unreadable:{path}"


def load_run(ref: str | Path, runs_dir: Path | list[Path] | None = None) -> RunRecord:
    """Load a run by metrics.json path, run directory, or run id.

    A run id is looked up in each of *runs_dir* in order (one directory or a
    list); the first match wins.
    """
    from lakebench.metrics.storage import MetricsStorage

    candidate = Path(ref)
    if candidate.is_dir():
        candidate = candidate / "metrics.json"
    if not candidate.is_file():
        if runs_dir is None:
            dirs: list[Path] = []
        elif isinstance(runs_dir, (str, Path)):
            dirs = [Path(runs_dir)]
        else:
            dirs = [Path(d) for d in runs_dir]
        if not dirs:
            raise PerfGateError(f"run {ref!r} not found (no runs directory given)")
        run_id = str(ref).removeprefix("run-")
        found = [d / f"run-{run_id}" / "metrics.json" for d in dirs]
        found = [f for f in found if f.is_file()]
        where = ", ".join(str(d) for d in dirs)
        if not found:
            raise PerfGateError(f"run {ref!r} not found under {where}")
        if len({_canonical_json(f) for f in found}) > 1:
            raise PerfGateError(
                f"run {ref!r} exists with different contents in more than one of {where}"
            )
        candidate = found[0]
    try:
        raw = json.loads(candidate.read_text())
    except (OSError, json.JSONDecodeError) as e:
        raise PerfGateError(f"cannot read {candidate}: {e}") from None
    if not isinstance(raw, dict):
        raise PerfGateError(f"{candidate} is not a metrics.json object")
    # Bypass __init__: it creates the runs directory, and reading a run must
    # not write anything. _dict_to_metrics uses no instance state.
    storage = MetricsStorage.__new__(MetricsStorage)
    metrics = storage._dict_to_metrics(raw)
    return RunRecord(
        run_id=str(raw.get("run_id") or candidate.parent.name.removeprefix("run-")),
        path=candidate,
        raw=raw,
        metrics=metrics,
    )


def iter_runs(runs_dir: Path) -> Iterator[RunRecord]:
    """Every readable run under *runs_dir*, newest run id first."""
    if not runs_dir.is_dir():
        return
    for d in sorted(runs_dir.glob("run-*"), reverse=True):
        if (d / "metrics.json").is_file():
            try:
                yield load_run(d)
            except PerfGateError:
                continue


def _is_datagen_stage(stage: Mapping[str, Any]) -> bool:
    return stage.get("stage_name") == "datagen" or stage.get("stage_type") == "datagen"


def _pipeline_ttv(run: RunRecord) -> tuple[float, float | None, float] | None:
    """(time to value, GB/s, summed stage seconds) over the pipeline stages.

    The same rule as ``_compute_batch_scores`` (first start to last end, a
    stage without an end counts at its start), applied only to stages that
    are not datagen, so whether a generate ran inside a single-cycle run does
    not move it (multi-cycle runs, which generate between cycles, are refused
    in run_refusals). GB is summed over every non-datagen stage, timed or not,
    as the scorecard sums it (the query stage has no timestamps but has an
    input size), so GB/s equals the scorecard's whenever the time does. None
    when no pipeline stage records a start time.
    """
    starts: list[datetime] = []
    ends: list[datetime] = []
    gb = 0.0
    timed_seconds = 0.0
    for s in run.pb_raw.get("stages") or []:
        if _is_datagen_stage(s):
            continue
        size = s.get("input_size_gb")
        if isinstance(size, (int, float)) and size > 0:
            gb += float(size)
        try:
            start = datetime.fromisoformat(s["start_time"]) if s.get("start_time") else None
            end = datetime.fromisoformat(s["end_time"]) if s.get("end_time") else None
        except (TypeError, ValueError):
            return None
        if start is None:
            continue
        starts.append(start)
        ends.append(end or start)
        elapsed = s.get("elapsed_seconds")
        if isinstance(elapsed, (int, float)) and elapsed > 0:
            timed_seconds += float(elapsed)
    if not starts:
        return None
    try:
        ttv = (max(ends) - min(starts)).total_seconds()
    except TypeError:  # aware and naive timestamps mixed
        return None
    if ttv <= 0:
        return None
    if gb <= 0:
        # Stages without sizes: the scorecard's GB (datagen reads nothing,
        # so it is the pipeline's) over the recomputed time.
        s_ttv, s_gbps = (
            run.scores.get("time_to_value_seconds"),
            run.scores.get("pipeline_throughput_gb_per_second"),
        )
        if isinstance(s_ttv, (int, float)) and isinstance(s_gbps, (int, float)) and s_ttv > 0:
            gb = float(s_gbps) * float(s_ttv)
    return ttv, (gb / ttv if gb > 0 else None), timed_seconds


def _datagen_stale(run: RunRecord) -> str | None:
    """Why the run's datagen numbers belong to an earlier generate, or None."""
    age = _datagen_age_hours(run)
    if age is not None and age > MAX_DATAGEN_AGE_HOURS:
        return (
            f"datagen metrics written at least {age:.0f}h before the run; from an earlier generate"
        )
    return None


# How batch stage seconds were timed (JobMetrics.timing_source). Before v1.6
# every stage ended on the 15 s job-monitor poll; from v1.6 at the Spark
# application's real end. The two are not comparable (the poll rounds each
# stage up by 0-15 s), so a baseline and a run must share a basis.
STAGE_TIMING_POLL = "poll"
STAGE_TIMING_CLUSTER = "cluster"
STAGE_TIMING_MIXED = "mixed"


def _stage_basis(stage: Mapping[str, Any]) -> str:
    src = str(stage.get("timing_source") or "")
    if src in ("driver_container", "spark_application"):
        return STAGE_TIMING_CLUSTER
    if src == "":
        return STAGE_TIMING_POLL  # pre-v1.6: the 15 s poll
    if src == "poll":
        # A v1.6 poll fallback, at its own interval: not the old 15 s poll.
        res = stage.get("timing_resolution_seconds")
        return f"poll{res:g}s" if isinstance(res, (int, float)) else STAGE_TIMING_POLL
    return src  # local_runner, submit_failed: a basis of their own


def stage_timing_basis(run: RunRecord) -> str | None:
    """The timing basis of the run's batch Spark stages, None with none.

    STAGE_TIMING_MIXED when the stages were not all timed the same way; such
    a run is neither gated nor recorded, since a per-stage basis that differs
    from the baseline's cannot be told apart from a change in the stage.
    """
    kinds = {
        _stage_basis(s) for s in run.pb_raw.get("stages") or [] if s.get("stage_type") == "batch"
    }
    if not kinds:
        return None
    return kinds.pop() if len(kinds) == 1 else STAGE_TIMING_MIXED


def ttv_basis(run: RunRecord) -> str:
    if run.mode == "batch" and _pipeline_ttv(run) is not None:
        return TTV_FROM_STAGES
    return TTV_FROM_SCORECARD


def post_qph_unmeasured(scores: dict) -> str:
    """Why the run's post-maintenance QpH is not a measurement, or ""."""
    parts = []
    if scores.get("maintenance_stopped") is True:
        parts.append(
            "pre-benchmark maintenance stopped before completion ("
            + (scores.get("maintenance_stop_reason") or "unknown")
            + ")"
        )
    if scores.get("maintenance_live_streams") is True:
        parts.append(
            "streams were live during pre-benchmark maintenance ("
            + (scores.get("maintenance_live_streams_reason") or "unknown")
            + ")"
        )
    return "; ".join(parts)


def extract_metrics(run: RunRecord) -> tuple[dict[str, float], dict[str, str]]:
    """Numbers the gate compares, plus the metrics it deliberately left out.

    Starts from ``_extract_expected_numbers`` (the reproduce surface, which
    already leaves out rows/s for a drained corpus) and adds per-query QpH,
    pre-maintenance QpH and datagen MB/s per pod. Correctness-band numbers
    are dropped: they are guards, not performance. For batch runs whose
    stages carry timestamps, time to value and GB/s are recomputed without
    the datagen stage.

    Returns (numbers, excluded) where *excluded* maps a metric name to the
    reason it was not measured.
    """
    numbers = {
        k: v
        for k, v in _extract_expected_numbers(run.metrics).items()
        if _classify_direction(k)[0] == "performance"
    }
    excluded: dict[str, str] = {}
    from lakebench.metrics.storage import recorded_qph_basis

    basis = recorded_qph_basis(run.raw)
    if isinstance(basis, Mapping) and basis.get("blended"):
        excluded["composite_qph"] = "in-stream rounds ran different query sets"

    if run.mode == "sustained":
        # Every continuous stage runs for the whole window, so its seconds
        # is the window length, not a measurement.
        for key in [k for k in numbers if _is_stage_seconds(k)]:
            del numbers[key]

    if run.mode == "sustained":
        from lakebench.metrics.continuous_window import drained_rps_excluded

        why = drained_rps_excluded(
            run.scores.get("corpus_drained"), run.scores.get("window_arrival_fraction")
        )
        if why:
            numbers.pop("sustained_throughput_rps", None)
            excluded["sustained_throughput_rps"] = why

    if run.mode == "batch":
        recomputed = _pipeline_ttv(run)
        if recomputed is not None:
            ttv, gbps, _timed = recomputed
            numbers["time_to_value_seconds"] = ttv
            if gbps is not None:
                numbers["pipeline_throughput_gb_per_second"] = gbps
            else:
                numbers.pop("pipeline_throughput_gb_per_second", None)

    scores = run.scores
    # Pre-maintenance QpH is gated on its own: a write-layout regression
    # lowers it while post-maintenance composite_qph stays flat.
    # maintenance_value_pct is not gated: it is (post - pre) / pre, both of
    # which are gated, and it has no good direction (a better write layout
    # raises pre and lowers it).
    pre_qph = scores.get("pre_compaction_qph")
    if isinstance(pre_qph, (int, float)) and pre_qph > 0:
        numbers["pre_compaction_qph"] = float(pre_qph)

    qb = run.pb_raw.get("query_benchmark") or run.raw.get("benchmark") or {}
    per_query: dict[str, list[float]] = {}
    for q in qb.get("queries") or []:
        name = q.get("name") or q.get("query_name")
        elapsed = q.get("elapsed_seconds") or 0
        if name and q.get("success") and elapsed > 0:
            per_query.setdefault(str(name), []).append(float(elapsed))
    for name, times in per_query.items():
        numbers[f"{QUERY_QPH_PREFIX}{name}"] = 3600.0 / statistics.median(times)

    why = post_qph_unmeasured(scores)
    if why:
        # A stopped maintenance (a rewrite may still have run during the
        # benchmark) or live streams (writers active during it): post-
        # maintenance QpH is not a measurement. Pre-maintenance QpH was taken
        # before and stays gated.
        # Stream apps present at maintenance were present during the
        # pre-maintenance round too, so that number is under load as well.
        live = scores.get("maintenance_live_streams") is True
        for key in [
            k
            for k in numbers
            if k == "composite_qph"
            or k.startswith(QUERY_QPH_PREFIX)
            or (live and k == "pre_compaction_qph")
        ]:
            del numbers[key]
            excluded[key] = why

    fleet = run.raw.get("datagen_fleet") or {}
    pod_mbps = [
        float(p["throughput_mbps"])
        for p in fleet.get("per_pod") or []
        if isinstance(p.get("throughput_mbps"), (int, float)) and p["throughput_mbps"] > 0
    ]
    if pod_mbps:
        numbers["datagen_mbps_per_pod"] = statistics.fmean(pod_mbps)

    stale = _datagen_stale(run)
    if stale:
        # datagen_seconds is the in-run generate time when there was one
        # (`lakebench run --generate`, which writes no sidecar but still
        # attaches an old one); the collector only falls back to the
        # sidecar's wall_elapsed_max_s when the run did not generate. Keep
        # it unless it is the sidecar's number.
        fleet = run.raw.get("datagen_fleet") or {}
        in_run = "datagen_seconds" in numbers and numbers["datagen_seconds"] != fleet.get(
            "wall_elapsed_max_s"
        )
        for key in _DATAGEN_METRICS:
            if key == "datagen_seconds" and in_run:
                continue
            if key in numbers:
                del numbers[key]
                excluded[key] = stale

    return {k: float(v) for k, v in numbers.items() if math.isfinite(float(v))}, excluded


def run_samples_per_query(run: RunRecord) -> int | None:
    """Timed samples per query in the run's scored benchmark round.

    A record written before per-query repeats (LB-150) has no ``samples``
    and reads as 1, whatever its config snapshot says: before then
    ``lakebench run`` never passed ``benchmark.iterations`` to the runner,
    so a snapshot reading 3 still took one sample. None when the run has no
    successful benchmark query.
    """
    from lakebench.benchmark.spread import samples_per_query

    qb = run.pb_raw.get("query_benchmark") or run.raw.get("benchmark") or {}
    return samples_per_query(qb.get("queries") or [])


def benchmark_sample_refusal(run: RunRecord, pinned: PinnedConfig) -> str | None:
    """Why the run's QpH cannot stand against *pinned*'s, or None.

    A median of three samples and a single sample are different estimators:
    with right-skewed query noise the median reads faster, so a QpH drift
    between them is a bias, not a change. The gate refuses rather than warns
    because it exits nonzero on a regression and a biased pass or fail is
    worse than no verdict.
    """
    want = (pinned.fingerprint.get("benchmark") or {}).get("iterations")
    got = run_samples_per_query(run)
    if not isinstance(want, int) or got is None or got == want:
        return None
    return (
        f"benchmark took {got} sample(s) per query but the pinned config scores the "
        f"median of {want}; QpH from different sample counts is not comparable"
        + (" (the run predates per-query repeats, LB-150)" if got == 1 else "")
    )


def run_refusals(run: RunRecord, pinned: PinnedConfig) -> list[str]:
    """Reasons this run cannot stand for *pinned*. Empty means comparable."""
    from lakebench.metrics.verdict import passed as _record_passed

    reasons: list[str] = []
    # Prefer the persisted verdict (OD-6: v1.6 records) and fall back to
    # raw ``success`` for legacy v1.5 records. A verdict of FAILED refuses
    # even when the raw flag is True (LB-044 shape).
    if not _record_passed(run.raw):
        reasons.append("run did not succeed")
    # Only runs under this version's maintenance policy are compared or
    # recorded: m1-legacy covers two different real policies, and a baseline
    # recorded from a legacy run would refuse every current run.
    stale_policy = not_current(recorded_policy(run.raw))
    if stale_policy:
        reasons.append(stale_policy)
    # `lakebench run --local` records every stage ending at the same moment
    # and runs on a workstation; the fingerprint does not include the flag.
    if run.snapshot.get("local"):
        reasons.append("local run (--local); only cluster runs are comparable")
    if run.mode != pinned.mode:
        reasons.append(f"run mode {run.mode} but pinned config is {pinned.mode}")
    if experiment_of(run.raw) is None:
        reasons.append(f"{NO_PROVENANCE} (run {run.run_id} has no experiment block)")
    # A run an evaluation profile or a Lakebench cap bound measures the
    # limit: never a baseline, never compared against one.
    from lakebench.metrics.release_record import bound_problems

    reasons += bound_problems(run.raw)
    # The run's fingerprint is read from its stored snapshot. A snapshot not
    # stamped with this version cannot show what the version 2 inputs were,
    # so it is refused by name rather than diffed field by field.
    version_problem = fingerprint_version_refusal(run.snapshot)
    if version_problem:
        reasons.append(version_problem)
    else:
        run_fp = run.fingerprint
        if fingerprint_hash(run_fp) != pinned.fingerprint_hash:
            diff = fingerprint_diff(pinned.fingerprint, run_fp)
            reasons.append("config fingerprint differs from the pinned config: " + "; ".join(diff))

    # The run must have used the pinned file byte for byte: the fingerprint
    # covers the fields that size work, the file covers every other setting.
    # Version 2 runs record the file's sha256 when they start, so a version 2
    # snapshot without it is refused (fail closed); older runs are refused by
    # their version above.
    recorded = run.snapshot.get("config_sha256")
    if recorded and recorded != pinned.file_sha256:
        reasons.append(
            f"run used a different config file (sha256 {str(recorded)[:12]}, "
            f"pinned {pinned.file_sha256[:12]})"
        )
    elif not recorded and version_problem is None:
        reasons.append("run snapshot records no config_sha256, so its config file is unknown")

    scores = run.scores
    if run.mode == "batch":
        # Cycles 2..N regenerate data between gold and the next bronze, so
        # first start to last end spans datagen, and stage seconds keep only
        # the last cycle's value. `cycles` is not in the snapshot, so it is
        # checked here.
        cycles = run.raw.get("cycles") or run.pb_raw.get("cycles") or []
        names = [
            s.get("stage_name")
            for s in run.pb_raw.get("stages") or []
            if s.get("stage_type") == "batch"
        ]
        if len(cycles) > 1 or len(names) != len(set(names)):
            reasons.append(
                f"multi-cycle batch run ({max(len(cycles), 2)} cycles); the gate compares "
                "single-cycle runs only"
            )
        recomputed = _pipeline_ttv(run)
        if recomputed is not None:
            ttv, _gbps, timed = recomputed
            # Batch stages run one after another, so their timestamps cannot
            # span less than their own durations. A run that does crossed a
            # clock change (naive local timestamps across a DST fall-back).
            if ttv + max(5.0, 0.01 * timed) < timed:
                reasons.append(
                    f"stage timestamps span {ttv:.0f}s, less than the stages' own "
                    f"{timed:.0f}s (a clock change during the run?)"
                )
        sample_problem = benchmark_sample_refusal(run, pinned)
        if sample_problem:
            reasons.append(sample_problem)
        ratio = scores.get("scale_ratio", run.pb_raw.get("scale_ratio"))
        ratio = float(ratio) if isinstance(ratio, (int, float)) else 0.0
        if ratio < MIN_SCALE_RATIO:
            note = " (0 means bronze input volume was not measured)" if ratio == 0 else ""
            reasons.append(f"scale_ratio {ratio:.3f} < {MIN_SCALE_RATIO}{note}")
        elif ratio > MAX_SCALE_RATIO:
            reasons.append(f"scale_ratio {ratio:.3f} > {MAX_SCALE_RATIO}: more data than the scale")
    else:
        ingest = scores.get("ingest_ratio")
        rps = scores.get("sustained_throughput_rps")
        if not isinstance(ingest, (int, float)) or ingest <= 0:
            reasons.append(f"no data flowed (ingest_ratio {ingest!r})")
        elif ingest > MAX_INGEST_RATIO:
            reasons.append(
                f"ingest_ratio {ingest:.2f} > {MAX_INGEST_RATIO}: bronze ingested more rows than "
                "datagen produced, which inflates rows/s"
            )
        elif not isinstance(rps, (int, float)) or rps <= 0:
            reasons.append(f"no data flowed (sustained_throughput_rps {rps!r})")
        if scores.get("data_freshness_seconds") is None:
            reasons.append("data_freshness_seconds was not measured")
        want_window = (pinned.fingerprint.get("sustained") or {}).get("run_duration")
        windows = [
            float(s.get("elapsed_seconds") or 0)
            for s in run.pb_raw.get("stages") or []
            if s.get("stage_type") == "streaming"
        ]
        if want_window and windows:
            window = max(windows)
            if abs(window - want_window) > want_window * WINDOW_TOLERANCE_PCT / 100:
                reasons.append(
                    f"run window {window:.0f}s differs from pinned run_duration {want_window}s "
                    "(a --duration override?)"
                )

    # Realised sizing: what actually ran, not what the snapshot asked for.
    # Batch only; continuous stages have no realised count (see
    # _BATCH_STAGE_OVERRIDE_KEY).
    overrides = ((pinned.fingerprint.get("spark") or {}).get("executor_overrides")) or {}
    stage_keys = _BATCH_STAGE_OVERRIDE_KEY if run.mode == "batch" else {}
    for stage in run.pb_raw.get("stages") or []:
        key = stage_keys.get(stage.get("stage_name", ""))
        want = overrides.get(key) if key else None
        got = stage.get("executor_count") or 0
        if want and got and got != want:
            reasons.append(f"stage {stage['stage_name']} ran {got} executors, pinned {want}")
    fleet = run.raw.get("datagen_fleet") or {}
    want_pods = (pinned.fingerprint.get("datagen") or {}).get("parallelism")
    got_pods = fleet.get("pods_expected")
    if fleet and want_pods and got_pods and got_pods != want_pods:
        reasons.append(f"datagen ran {got_pods} pods, pinned {want_pods}")
    if fleet and fleet.get("data_quality") not in (None, "complete"):
        reasons.append(f"datagen fleet data_quality={fleet.get('data_quality')!r}")
    return reasons


def _datagen_age_hours(run: RunRecord) -> float | None:
    """Smallest possible hours between the datagen sidecar and the run start.

    written_at is UTC-aware. A pre-v1.6 start_time is naive local time on the
    run host with no zone recorded, so it is read as
    UTC and the largest positive offset is subtracted: the result is a lower
    bound on the true age in every zone and does not depend on the gate
    host's zone. An aware start_time is used as is.
    """
    fleet = run.raw.get("datagen_fleet") or {}
    written, started = fleet.get("written_at"), run.raw.get("start_time")
    if not written or not started:
        return None
    try:
        w = datetime.fromisoformat(str(written))
        s = datetime.fromisoformat(str(started))
    except ValueError:
        return None
    if w.tzinfo is None:
        w = w.replace(tzinfo=timezone.utc)
    slack = 0.0
    if s.tzinfo is None:
        s = s.replace(tzinfo=timezone.utc)
        slack = _MAX_UTC_OFFSET_HOURS
    return (s - w).total_seconds() / 3600 - slack


# ---------------------------------------------------------------------------
# Baseline store
# ---------------------------------------------------------------------------


@dataclass
class Baseline:
    name: str
    config: str
    required: bool
    status: str = STATUS_PENDING
    run_id: str | None = None
    git_sha: str | None = None
    config_hash: str | None = None
    fingerprint_hash: str | None = None
    recorded_at: str | None = None
    metrics: dict[str, float] = field(default_factory=dict)
    tolerances: dict[str, dict[str, float]] = field(default_factory=dict)
    notes: str | None = None
    # Continuous runs only: whether the baseline run drained its corpus.
    # A drained run's freshness covers only the cycles that saw data, so a
    # drained and an undrained run are not comparable (LB-145).
    corpus_drained: bool | None = None
    # Batch runs only: TTV_FROM_STAGES or TTV_FROM_SCORECARD.
    ttv_basis: str | None = None
    # Batch runs only: STAGE_TIMING_*. None: recorded before v1.6, when every
    # stage was poll-timed.
    stage_timing: str | None = None
    # Table-maintenance policy of the baseline run (metrics/maintenance_policy).
    # None (a baseline recorded before the field) is the legacy policy.
    maintenance_policy_id: str | None = None
    # Experiment identity (metrics/experiment.identity) and per-query result
    # fingerprints of the baseline run. None: recorded before provenance, so
    # nothing can be gated against it.
    experiment_identity: dict[str, Any] | None = None
    result_fingerprints: dict[str, Any] | None = None
    # Fingerprint version the baseline's fingerprint_hash was taken under.
    # Entries of a schema 1 store are version 1.
    fingerprint_version: int | None = None
    # Dependency pinset of the baseline run (provenance.deps.pinset_sha256).
    pinset_sha256: str | None = None

    @property
    def accepted(self) -> bool:
        return self.status == STATUS_ACCEPTED and bool(self.metrics)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "config": self.config,
            "required": self.required,
            "status": self.status,
        }
        for key in (
            "run_id",
            "git_sha",
            "config_hash",
            "fingerprint_hash",
            "recorded_at",
            "corpus_drained",
            "ttv_basis",
            "stage_timing",
            "maintenance_policy_id",
            "experiment_identity",
            "result_fingerprints",
            "fingerprint_version",
            "pinset_sha256",
        ):
            value = getattr(self, key)
            if value is not None:
                d[key] = value
        if self.metrics:
            d["metrics"] = {k: round(v, 4) for k, v in sorted(self.metrics.items())}
        if self.tolerances:
            d["tolerances"] = self.tolerances
        if self.notes:
            d["notes"] = self.notes
        return d


@dataclass
class BaselineStore:
    path: Path
    baselines: dict[str, Baseline]

    @property
    def root(self) -> Path:
        return self.path.parent

    def pinned(self, name: str) -> PinnedConfig:
        return load_pinned(self.root / self.baselines[name].config, name)

    def save(self) -> None:
        body = {
            "schema_version": STORE_SCHEMA_VERSION,
            "baselines": {n: b.to_dict() for n, b in self.baselines.items()},
        }
        text = (
            "# Accepted performance baselines per pinned config. Written by\n"
            "# scripts/perf_gate.py record; see docs/perf-regression-gate.md.\n"
        ) + yaml.safe_dump(body, sort_keys=False, default_flow_style=False)
        self.path.write_text(text)


def _check_tolerances(name: str, tolerances: Any) -> dict[str, dict[str, float]]:
    if tolerances is None:
        return {}
    if not isinstance(tolerances, dict):
        raise PerfGateError(f"{name}: tolerances must be a mapping")
    out: dict[str, dict[str, float]] = {}
    for metric, spec in tolerances.items():
        if not isinstance(spec, dict) or set(spec) - {"pct", "abs"} or not spec:
            raise PerfGateError(f"{name}: tolerance for {metric} must be {{pct: N}} or {{abs: N}}")
        for k, v in spec.items():
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not v >= 0:
                raise PerfGateError(f"{name}: tolerance {metric}.{k} must be a number >= 0")
        out[str(metric)] = {k: float(v) for k, v in spec.items()}
    return out


def load_store(path: Path) -> BaselineStore:
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError) as e:
        raise PerfGateError(f"cannot read baseline store {path}: {e}") from None
    schema = raw.get("schema_version")
    if isinstance(schema, bool) or schema not in _READABLE_STORE_SCHEMAS:
        raise PerfGateError(
            f"{path}: schema_version {schema!r} is not one of "
            f"{', '.join(str(v) for v in _READABLE_STORE_SCHEMAS)}"
        )
    entries = raw.get("baselines")
    if not isinstance(entries, dict) or not entries:
        raise PerfGateError(f"{path}: 'baselines' must be a non-empty mapping")
    baselines: dict[str, Baseline] = {}
    for name, entry in entries.items():
        if not isinstance(entry, dict) or not entry.get("config"):
            raise PerfGateError(f"{path}: baseline {name!r} needs a 'config' path")
        metrics = entry.get("metrics") or {}
        for k, v in metrics.items():
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
                raise PerfGateError(f"{path}: {name}.metrics.{k} must be a finite number")
        status = entry.get("status", STATUS_PENDING)
        # A schema 1 store predates fingerprint versions: its entries are
        # version 1 whatever they say. In schema 2 the version is written.
        fp_version = 1 if schema == 1 else entry.get("fingerprint_version")
        if status == STATUS_ACCEPTED:
            required_keys = ["run_id", "config_hash", "fingerprint_hash"]
            if schema != 1:
                required_keys.append("fingerprint_version")
                # A version 1 entry carried into a schema 2 store by a save
                # has no pinset; compare refuses it by its version instead.
                if fp_version == FINGERPRINT_VERSION:
                    required_keys.append("pinset_sha256")
            missing = [k for k in required_keys if not entry.get(k)]
            if missing or not metrics:
                raise PerfGateError(
                    f"{path}: accepted baseline {name!r} lacks {', '.join(missing) or 'metrics'}"
                )
        baselines[str(name)] = Baseline(
            name=str(name),
            config=str(entry["config"]),
            required=bool(entry.get("required", False)),
            status=status,
            run_id=entry.get("run_id"),
            git_sha=entry.get("git_sha"),
            config_hash=entry.get("config_hash"),
            fingerprint_hash=entry.get("fingerprint_hash"),
            recorded_at=entry.get("recorded_at"),
            metrics={k: float(v) for k, v in metrics.items()},
            tolerances=_check_tolerances(str(name), entry.get("tolerances")),
            notes=entry.get("notes"),
            corpus_drained=entry.get("corpus_drained"),
            ttv_basis=entry.get("ttv_basis"),
            stage_timing=entry.get("stage_timing"),
            maintenance_policy_id=entry.get("maintenance_policy_id"),
            experiment_identity=entry.get("experiment_identity"),
            result_fingerprints=entry.get("result_fingerprints"),
            fingerprint_version=fp_version,
            pinset_sha256=entry.get("pinset_sha256") if schema != 1 else None,
        )
    return BaselineStore(path=path, baselines=baselines)


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------


def tolerance_for(metric: str, overrides: Mapping[str, Mapping[str, float]]) -> dict[str, float]:
    if metric in overrides:
        return dict(overrides[metric])
    if metric.startswith(QUERY_QPH_PREFIX):
        return dict(DEFAULT_QUERY_TOLERANCE)
    return dict(DEFAULT_TOLERANCE)


def _is_regression(metric: str, actual: float, expected: float, tol: Mapping[str, float]) -> bool:
    """Direction-aware check, directions from ``_classify_direction``."""
    _band, direction = _classify_direction(metric)
    if "abs" in tol:
        delta = actual - expected
        bad = {"lower": delta, "higher": -delta, "exact": abs(delta)}[direction]
        return bad > tol["abs"]
    drift = _drift_pct(actual, expected)
    bad = {"lower": drift, "higher": -drift, "exact": abs(drift)}[direction]
    return bad > tol["pct"]


@dataclass
class Row:
    metric: str
    expected: float | None
    actual: float | None
    drift_pct: float | None
    direction: str
    tolerance: str
    status: str  # ok | regression | improved | missing | excluded | new


@dataclass
class Comparison:
    name: str
    verdict: str
    run_id: str | None = None
    reasons: list[str] = field(default_factory=list)
    rows: list[Row] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.verdict == PASS


def compare_run(store: BaselineStore, name: str, run: RunRecord) -> Comparison:
    """Compare *run* with the accepted baseline of pinned config *name*."""
    if name not in store.baselines:
        raise PerfGateError(f"no pinned config named {name!r} in {store.path}")
    baseline = store.baselines[name]
    pinned = store.pinned(name)
    result = Comparison(name=name, verdict=PASS, run_id=run.run_id)

    if not baseline.accepted:
        result.verdict = NO_BASELINE
        result.reasons.append(f"baseline is '{baseline.status}'")
        return result
    if baseline.fingerprint_version != FINGERPRINT_VERSION:
        # Its fingerprint_hash cannot equal any version 2 hash; say why
        # instead of reporting a changed snapshot.
        result.reasons.append(
            f"baseline predates fingerprint v{FINGERPRINT_VERSION} (recorded under "
            f"version {baseline.fingerprint_version}); re-record it with the v1.7 "
            "re-baseline (scripts/perf_gate.py record --replace)"
        )
    elif baseline.fingerprint_hash != pinned.fingerprint_hash:
        result.reasons.append(
            "pinned config resolves to a different snapshot than when the baseline was "
            "recorded (a schema default, job profile, Spark conf or autosizer change); "
            "record a new baseline"
        )
    if baseline.config_hash != pinned.config_hash:
        result.reasons.append(
            f"pinned config {baseline.config} changed since the baseline was recorded "
            f"(baseline config_hash {(baseline.config_hash or '')[:12]}, now {pinned.config_hash[:12]}); "
            "record a new baseline"
        )
    result.reasons.extend(run_refusals(run, pinned))
    # The pinned config is compared with itself, so every other part of the
    # architecture is equal by construction: a run on another dependency set
    # differs from the baseline in its jars alone, which is not like for like.
    pinset = run_pinset(run.raw)
    if baseline.fingerprint_version == FINGERPRINT_VERSION and pinset != baseline.pinset_sha256:
        result.reasons.append(
            f"dependency set differs from the baseline ({_short(pinset)} vs "
            f"{_short(baseline.pinset_sha256)})"
        )
    run_exp = experiment_of(run.raw)
    if run_exp is not None:
        # Same workload, corpus, seed, scale and mode, and every benchmark
        # query returned what the baseline's did (invariant 2).
        result.reasons.extend(
            stored_identity_refusals(
                baseline.experiment_identity,
                baseline.result_fingerprints,
                run_exp,
                "baseline",
                failed=failed_queries(run.raw),
            )
        )
    policy_problem = policy_mismatch(baseline.maintenance_policy_id, recorded_policy(run.raw))
    if policy_problem:
        result.reasons.append(
            policy_problem + "; record a new baseline (scripts/perf_gate.py record --replace)"
        )
    if run.mode == "sustained":
        drained = run.scores.get("corpus_drained")
        if drained != baseline.corpus_drained:
            result.reasons.append(
                f"corpus_drained is {drained!r} but the baseline's is "
                f"{baseline.corpus_drained!r}; freshness and rows/s mean different things "
                "for drained and undrained runs (LB-145)"
            )
    actual, excluded = extract_metrics(run)
    datagen_differs = ("datagen_seconds" in baseline.metrics) != ("datagen_seconds" in actual)
    if datagen_differs:
        reason = "datagen stage present in only one of the baseline and the run"
        for key in _DATAGEN_METRICS:
            actual.pop(key, None)
            excluded.setdefault(key, reason)
    if run.mode == "batch" and "time_to_value_seconds" in baseline.metrics:
        basis = ttv_basis(run)
        if basis != baseline.ttv_basis:
            result.reasons.append(
                f"time to value taken from the {basis} in the run but the "
                f"{baseline.ttv_basis or 'unrecorded source'} in the baseline; record a new baseline"
            )
        elif basis == TTV_FROM_SCORECARD and (datagen_differs or _datagen_stale(run)):
            # No stage timestamps, so TTV cannot be separated from a generate
            # that may or may not have run inside it.
            result.reasons.append(
                "time to value cannot be separated from the datagen stage (stages carry "
                "no timestamps) and the datagen stage differs from the baseline's or is stale"
            )
    if run.mode == "batch":
        run_timing = stage_timing_basis(run)
        base_timing = baseline.stage_timing or STAGE_TIMING_POLL
        if run_timing == STAGE_TIMING_MIXED:
            result.reasons.append(
                "batch stages are not all timed the same way (some fell back to the poll); "
                "stage seconds and time to value are not like for like with any baseline"
            )
        elif run_timing is not None and run_timing != base_timing:
            result.reasons.append(
                f"batch stage times are {run_timing}-timed in the run but "
                f"{base_timing}-timed in the baseline (v1.6 times stages from the Spark "
                "application's end, not the 15 s poll); stage seconds and time to value "
                "are not like for like: record a new baseline"
            )
    want_stages = {
        k for k in baseline.metrics if _is_stage_seconds(k) and k not in _OPTIONAL_STAGE_KEYS
    }
    got_stages = {k for k in actual if _is_stage_seconds(k) and k not in _OPTIONAL_STAGE_KEYS}
    if run.mode == "batch" and want_stages != got_stages:
        result.reasons.append(
            "stages differ from the baseline run (time to value is not like for like): "
            f"baseline {sorted(want_stages)}, run {sorted(got_stages)}"
        )
    if result.reasons:
        result.verdict = REFUSED
        return result

    regressed = False
    for metric, expected in sorted(baseline.metrics.items()):
        _band, direction = _classify_direction(metric)
        tol = tolerance_for(metric, baseline.tolerances)
        tol_s = f"{tol['abs']:g} abs" if "abs" in tol else f"{tol['pct']:g}%"
        if metric in excluded:
            result.rows.append(
                Row(metric, expected, None, None, direction, tol_s, "excluded: " + excluded[metric])
            )
            continue
        value = actual.get(metric)
        if value is None:
            result.rows.append(Row(metric, expected, None, None, direction, tol_s, "missing"))
            regressed = True
            continue
        drift = _drift_pct(value, expected)
        if _is_regression(metric, value, expected, tol):
            status = "regression"
            regressed = True
        else:
            better = {"lower": value < expected, "higher": value > expected}.get(direction, False)
            status = "improved" if better and abs(drift) > tol.get("pct", 0) else "ok"
        result.rows.append(Row(metric, expected, value, drift, direction, tol_s, status))
    for metric in sorted(set(actual) - set(baseline.metrics) - set(excluded)):
        _band, direction = _classify_direction(metric)
        result.rows.append(Row(metric, None, actual[metric], None, direction, "-", "new"))
    unmeasured = post_qph_unmeasured(run.scores)
    stopped = bool(unmeasured)
    if stopped:
        # Always said, whatever the verdict: post-maintenance QpH was left out.
        result.reasons.append(
            unmeasured + "; post-maintenance QpH is not a measurement and was not gated"
        )
    if regressed:
        result.verdict = REGRESSION
    elif stopped and not any(
        _is_qph_metric(r.metric) and not r.status.startswith("excluded") and r.status != "new"
        for r in result.rows
    ):
        result.verdict = NOT_COMPARABLE
        result.reasons.append(
            "no QpH metric was left to gate; the cause of the stop may itself be a regression"
        )
    return result


def _is_qph_metric(metric: str) -> bool:
    return metric in ("composite_qph", "pre_compaction_qph") or metric.startswith(QUERY_QPH_PREFIX)


def format_comparison(c: Comparison) -> str:
    lines = [f"{c.name}: {c.verdict}" + (f" (run {c.run_id})" if c.run_id else "")]
    for reason in c.reasons:
        lines.append(f"  - {reason}")
    if c.rows:
        width = max(len(r.metric) for r in c.rows)
        lines.append(
            f"  {'metric':{width}}  {'baseline':>12}  {'run':>12}  {'drift':>8}  {'tol':>7}  status"
        )
        for r in c.rows:
            exp = f"{r.expected:.2f}" if r.expected is not None else "-"
            act = f"{r.actual:.2f}" if r.actual is not None else "-"
            drift = f"{r.drift_pct:+.1f}%" if r.drift_pct is not None else "-"
            lines.append(
                f"  {r.metric:{width}}  {exp:>12}  {act:>12}  {drift:>8}  {r.tolerance:>7}  {r.status}"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Record and seed
# ---------------------------------------------------------------------------


def record_baseline(
    store: BaselineStore,
    name: str,
    run: RunRecord,
    git_sha: str | None,
    *,
    replace: bool = False,
) -> Baseline:
    """Accept *run* as the baseline for *name*. Refuses non-comparable runs."""
    if name not in store.baselines:
        raise PerfGateError(f"no pinned config named {name!r} in {store.path}")
    current = store.baselines[name]
    if current.accepted and not replace:
        raise PerfGateError(
            f"{name} already has an accepted baseline (run {current.run_id}); pass --replace"
        )
    pinned = store.pinned(name)
    reasons = run_refusals(run, pinned)
    pinset = run_pinset(run.raw)
    if pinset is None:
        reasons.append(
            "run records no dependency set (provenance.deps.pinset_sha256), so a later run "
            "on other jars could not be told apart"
        )
    basis = ttv_basis(run) if run.mode == "batch" else None
    if run.mode == "batch" and stage_timing_basis(run) == STAGE_TIMING_MIXED:
        reasons.append(
            "batch stages are not all timed the same way (some fell back to the poll); "
            "not recordable as a baseline"
        )
    if basis == TTV_FROM_SCORECARD and _datagen_stale(run):
        reasons.append(
            "time to value cannot be separated from a stale datagen stage (stages carry "
            "no timestamps)"
        )
    if reasons:
        raise PerfGateError(
            f"run {run.run_id} cannot be a baseline for {name}: " + "; ".join(reasons)
        )
    exp = experiment_of(run.raw)
    assert exp is not None  # run_refusals refused a run without one
    established = results_established(exp)
    problems = corpus_problems(exp)
    if established is not True or problems:
        raise PerfGateError(
            f"run {run.run_id} cannot be a baseline for {name}: "
            + "; ".join(
                problems
                + (
                    [f"comparability not established ({established})"]
                    if established is not True
                    else []
                )
            )
        )
    from lakebench.benchmark.fingerprint import usable

    # A failed query (a known upstream failure included) has no result to
    # fingerprint; the gate handles it as a failure, not here.
    failed = failed_queries(run.raw)
    unfp = sorted(
        n for n, f in result_fingerprints(exp).items() if n not in failed and not usable(f)
    )
    if unfp:
        raise PerfGateError(
            f"run {run.run_id} cannot be a baseline for {name}: queries without a usable "
            f"result fingerprint ({', '.join(unfp)}) could never be shown equal to a later run"
        )
    numbers, excluded = extract_metrics(run)
    if not numbers:
        raise PerfGateError(f"run {run.run_id} has no performance numbers")
    unmeasured = post_qph_unmeasured(run.scores)
    if unmeasured:
        raise PerfGateError(
            f"run {run.run_id} cannot be a baseline for {name}: {unmeasured}, "
            "so its post-maintenance QpH is not a measurement"
        )
    # A batch baseline without datagen numbers turns datagen gating off for
    # every later run (they are excluded as "present on one side only"), so
    # it has to come from a run with a fresh generate.
    # Continuous stage seconds, datagen's included, are not gated.
    if run.mode == "batch" and "datagen_seconds" not in numbers:
        why = excluded.get("datagen_seconds") or "the run has no datagen stage"
        raise PerfGateError(
            f"run {run.run_id} cannot be a baseline for {name}: no datagen numbers ({why}); "
            "record a run that generated its own data"
        )
    new = Baseline(
        name=name,
        config=current.config,
        required=current.required,
        status=STATUS_ACCEPTED,
        run_id=run.run_id,
        git_sha=git_sha or "unrecorded",
        config_hash=pinned.config_hash,
        fingerprint_hash=pinned.fingerprint_hash,
        recorded_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        metrics=numbers,
        tolerances=current.tolerances,
        notes=current.notes,
        corpus_drained=(run.scores.get("corpus_drained") if run.mode == "sustained" else None),
        ttv_basis=basis,
        stage_timing=stage_timing_basis(run) if run.mode == "batch" else None,
        maintenance_policy_id=recorded_policy(run.raw),
        experiment_identity=identity(exp),
        result_fingerprints=result_fingerprints(exp),
        fingerprint_version=FINGERPRINT_VERSION,
        pinset_sha256=pinset,
    )
    store.baselines[name] = new
    return new


def find_runs_for(
    pinned: PinnedConfig, runs_dir: Path
) -> tuple[list[RunRecord], list[tuple[RunRecord, list[str]]]]:
    """Runs of *pinned* in *runs_dir*: (comparable, [(near miss, reasons)]).

    A near miss is a run with the same workload, scale and mode whose
    fingerprint or guards failed; the reasons say why it cannot be used.
    """
    good: list[RunRecord] = []
    near: list[tuple[RunRecord, list[str]]] = []
    fp = pinned.fingerprint
    for run in iter_runs(runs_dir):
        rfp = run.fingerprint
        same_shape = (
            run.mode == pinned.mode
            and _normalise(rfp.get("scale")) == fp.get("scale")
            and (rfp.get("workload_schema") or "customer360") == fp.get("workload_schema")
        )
        if not same_shape:
            continue
        reasons = run_refusals(run, pinned)
        if reasons:
            near.append((run, reasons))
        else:
            good.append(run)
    return good, near


def latest_candidate(pinned: PinnedConfig, runs_dir: Path) -> RunRecord | None:
    """Newest successful run in *runs_dir* whose fingerprint matches *pinned*,
    measured under the current maintenance policy.

    Guards (scale_ratio, datagen pods) are not applied here, so a matching
    run that fails them is still returned and then refused by compare.
    """
    from lakebench.metrics.verdict import passed as _record_passed

    for run in iter_runs(runs_dir):
        # Prefer the persisted verdict (OD-6: v1.6) with fallback to raw
        # ``success`` (legacy v1.5). A FAILED verdict disqualifies a run
        # even when its raw success flag was left True. OD-6 also says
        # 'v1.5 records are never a perf baseline'; enforcing that here
        # requires the whole synthetic-fixture surface in tests to add a
        # verdict block, which is bigger than this lane. TODO(follow-up):
        # tighten to ``_has_verdict(run.raw) and _record_passed(...)``
        # once the perf-gate test suite fixtures carry verdict blocks.
        if (
            _record_passed(run.raw)
            and run.mode == pinned.mode
            and fingerprint_hash(run.fingerprint) == pinned.fingerprint_hash
            # A later --skip-maintenance or --local run must not displace the
            # gating run.
            and not not_current(recorded_policy(run.raw))
        ):
            return run
    return None


# ---------------------------------------------------------------------------
# Release gate
# ---------------------------------------------------------------------------


def release_check(
    store: BaselineStore,
    runs_dirs: Path | list[Path] | None,
    explicit_runs: Mapping[str, str] | None = None,
) -> tuple[bool, list[str]]:
    """Gate every pinned config. Returns (passed, one line per config).

    A required config fails the gate when it has no accepted baseline, when
    no run of it can be found, or when its run is refused or regressed. A
    config that is not required is compared when it can be and reported, but
    never fails the gate. Without an explicit run, the newest successful run
    of the pinned config across *runs_dirs* is used.
    """
    if runs_dirs is None:
        dirs: list[Path] = []
    elif isinstance(runs_dirs, Path):
        dirs = [runs_dirs]
    else:
        dirs = list(runs_dirs)
    explicit_runs = dict(explicit_runs or {})
    unknown = set(explicit_runs) - set(store.baselines)
    lines: list[str] = []
    passed = True
    if unknown:
        passed = False
        lines.append(f"FAIL unknown pinned config(s) in run mapping: {', '.join(sorted(unknown))}")
    where = ", ".join(str(d) for d in dirs) or "(no runs directory)"
    for name, baseline in store.baselines.items():
        tag = "required" if baseline.required else "optional"
        try:
            if not baseline.accepted:
                ok, msg = False, f"no baseline ({baseline.status})"
            else:
                pinned = store.pinned(name)
                run: RunRecord | None = None
                if name in explicit_runs:
                    run = load_run(explicit_runs[name], dirs)
                else:
                    found = [r for d in dirs if (r := latest_candidate(pinned, d)) is not None]
                    run = max(found, key=lambda r: r.run_id) if found else None
                    if run is not None:
                        # The same run id in two directories must be one run.
                        run = load_run(run.run_id, dirs)
                if run is None:
                    ok, msg = False, f"no successful run of {baseline.config} in {where}"
                else:
                    c = compare_run(store, name, run)
                    ok = c.ok
                    msg = f"{c.verdict} run {run.run_id}"
                    if c.reasons:
                        msg += ": " + "; ".join(c.reasons)
                    bad = [r.metric for r in c.rows if r.status in ("regression", "missing")]
                    if bad:
                        msg += ": " + ", ".join(bad)
                    # Run ids are timestamp-prefixed, so they order by time.
                    if run.run_id <= (baseline.run_id or ""):
                        ok = False
                        msg = (
                            f"run {run.run_id} is not newer than the baseline run "
                            f"{baseline.run_id}; a gate needs a run made after the baseline"
                        )
        except PerfGateError as e:
            ok, msg = False, str(e)
        if not ok and baseline.required:
            passed = False
        status = "ok  " if ok else ("FAIL" if baseline.required else "warn")
        lines.append(f"{status} {name} [{tag}]: {msg}")
    return passed, lines
