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
  rather than compared.

Metric classification and direction come from ``lakebench.cli._reproduce``
(``_METRIC_TABLE`` and ``_classify_direction``); this module adds no second
table. Only the performance band is compared here: the correctness signals
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
    _classify_direction,
    _drift_pct,
    _extract_expected_numbers,
    _is_stage_seconds,
)

STORE_SCHEMA_VERSION = 1

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
# left out rather than attributed to the run. start_time is naive local
# time, read in the gate host's zone, so a gate on another host is off by
# the zone difference.
MAX_DATAGEN_AGE_HOURS = 24.0

# Metrics that only describe the datagen stage, and the whole-pipeline
# numbers whose value depends on whether the datagen stage is included.
_DATAGEN_METRICS = frozenset(
    {"datagen_seconds", "datagen_aggregate_mbps", "datagen_cpu_hr_per_tb", "datagen_mbps_per_pod"}
)
_DATAGEN_DEPENDENT = frozenset(
    {
        "time_to_value_seconds",
        "pipeline_throughput_gb_per_second",
        "compute_efficiency_gb_per_core_hour",
    }
)
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
# maintenance_value_pct is already a percentage and can sit near zero, so a
# relative drift on it is meaningless; compare in percentage points.
DEFAULT_METRIC_TOLERANCES: dict[str, dict[str, float]] = {
    "maintenance_value_pct": {"abs": 10.0},
}

STATUS_ACCEPTED = "accepted"
STATUS_PENDING = "pending first run"

# Verdicts
PASS = "PASS"
REGRESSION = "REGRESSION"
REFUSED = "REFUSED"
NO_BASELINE = "NO_BASELINE"

# Placeholders for the ${VAR} references in pinned configs. Only used when
# the variable is unset; values never reach the fingerprint (identity and
# credential fields are excluded from it).
_PLACEHOLDER_ENV = {
    "LAKEBENCH_S3_ENDPOINT": "http://127.0.0.1:9",
    "LAKEBENCH_S3_ACCESS_KEY": "placeholder",
    "LAKEBENCH_S3_SECRET_KEY": "placeholder",
    "LAKEBENCH_POLARIS_CLIENT_SECRET": "placeholder",
}

# Stage name in PipelineBenchmark -> executor override key in the snapshot,
# per mode. Continuous runs name their stages bronze/silver/gold too
# (collector._STREAMING_MAP), so the lookup has to know the mode.
_STAGE_OVERRIDE_KEY = {
    "batch": {"bronze": "bronze", "silver": "silver", "gold": "gold"},
    "sustained": {
        "bronze": "bronze_ingest",
        "silver": "silver_stream",
        "gold": "gold_refresh",
        "bronze-ingest": "bronze_ingest",
        "silver-stream": "silver_stream",
        "gold-refresh": "gold_refresh",
    },
}

# Signed percentages: a relative tolerance divides by a value that can be
# negative or near zero, so only absolute tolerances are allowed.
_ABS_ONLY_METRICS = frozenset({"maintenance_value_pct"})


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


def snapshot_fingerprint(snapshot: Mapping[str, Any], mode: str) -> dict[str, Any]:
    """The sizing-relevant subset of a ``build_config_snapshot`` dict.

    Identity fields (name, S3 endpoint and buckets) are left out: they differ
    between deployments of the same pinned config. Keys missing from an old
    snapshot become None, so a run recorded before a field existed does not
    match a config that pins it.
    """
    mode = normalise_mode(mode)
    fp: dict[str, Any] = {"pipeline_mode": mode}
    for key in (
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
    ):
        fp[key] = snapshot.get(key)
    scratch = snapshot.get("scratch") or {}
    fp["scratch"] = {k: scratch.get(k) for k in ("enabled", "storage_class", "size")}
    if snapshot.get("query_engine") == "trino":
        fp["trino"] = snapshot.get("trino")
    if mode == "sustained":
        fp["sustained"] = snapshot.get("sustained")
    return _normalise(fp)


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
    from lakebench.config import load_config
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.metrics.collector import build_config_snapshot

    path = Path(path)
    if not path.is_file():
        raise PerfGateError(f"pinned config {path} not found")
    config_hash = config_file_hash(path)
    added = {k: v for k, v in _PLACEHOLDER_ENV.items() if not os.environ.get(k)}
    os.environ.update(added)
    try:
        cfg = load_config(path)
        resolve_auto_sizing(cfg, None)
    except Exception as e:  # noqa: BLE001 -- any load failure is a gate failure
        raise PerfGateError(f"pinned config {path} does not load: {e}") from None
    finally:
        for k in added:
            os.environ.pop(k, None)
    mode = normalise_mode(cfg.architecture.pipeline.mode.value)
    fp = snapshot_fingerprint(build_config_snapshot(cfg), mode)
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


def load_run(ref: str | Path, runs_dir: Path | None = None) -> RunRecord:
    """Load a run by metrics.json path, run directory, or run id."""
    from lakebench.metrics.storage import MetricsStorage

    candidate = Path(ref)
    if candidate.is_dir():
        candidate = candidate / "metrics.json"
    if not candidate.is_file():
        if runs_dir is None:
            raise PerfGateError(f"run {ref!r} not found (no runs directory given)")
        run_id = str(ref).removeprefix("run-")
        candidate = Path(runs_dir) / f"run-{run_id}" / "metrics.json"
        if not candidate.is_file():
            raise PerfGateError(f"run {ref!r} not found under {runs_dir}")
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


def extract_metrics(run: RunRecord) -> tuple[dict[str, float], dict[str, str]]:
    """Numbers the gate compares, plus the metrics it deliberately left out.

    Starts from ``_extract_expected_numbers`` (the reproduce surface, which
    already leaves out rows/s for a drained corpus) and adds per-query QpH,
    datagen MB/s per pod and maintenance value. Correctness-band numbers are
    dropped: they are guards, not performance.

    Returns (numbers, excluded) where *excluded* maps a metric name to the
    reason it was not measured.
    """
    numbers = {
        k: v
        for k, v in _extract_expected_numbers(run.metrics).items()
        if _classify_direction(k)[0] == "performance"
    }
    excluded: dict[str, str] = {}

    if run.mode == "sustained":
        # Every continuous stage runs for the whole window, so its seconds
        # is the window length, not a measurement.
        for key in [k for k in numbers if _is_stage_seconds(k)]:
            del numbers[key]

    if run.mode == "sustained" and run.scores.get("corpus_drained") is True:
        numbers.pop("sustained_throughput_rps", None)
        excluded["sustained_throughput_rps"] = (
            "corpus drained before the window ended; rows/s is a lower bound (LB-145)"
        )

    scores = run.scores
    # Pre-maintenance QpH is gated on its own: a write-layout regression
    # lowers it and raises maintenance_value_pct, while post-maintenance
    # composite_qph stays flat.
    pre_qph = scores.get("pre_compaction_qph")
    if isinstance(pre_qph, (int, float)) and pre_qph > 0:
        numbers["pre_compaction_qph"] = float(pre_qph)
    if "maintenance_value_pct" in scores or "pre_compaction_qph" in scores:
        value = scores.get("maintenance_value_pct")
        if value is None:
            excluded["maintenance_value_pct"] = "not measured"
        else:
            numbers["maintenance_value_pct"] = float(value)

    qb = run.pb_raw.get("query_benchmark") or run.raw.get("benchmark") or {}
    per_query: dict[str, list[float]] = {}
    for q in qb.get("queries") or []:
        name = q.get("name") or q.get("query_name")
        elapsed = q.get("elapsed_seconds") or 0
        if name and q.get("success") and elapsed > 0:
            per_query.setdefault(str(name), []).append(float(elapsed))
    for name, times in per_query.items():
        numbers[f"{QUERY_QPH_PREFIX}{name}"] = 3600.0 / statistics.median(times)

    fleet = run.raw.get("datagen_fleet") or {}
    pod_mbps = [
        float(p["throughput_mbps"])
        for p in fleet.get("per_pod") or []
        if isinstance(p.get("throughput_mbps"), (int, float)) and p["throughput_mbps"] > 0
    ]
    if pod_mbps:
        numbers["datagen_mbps_per_pod"] = statistics.fmean(pod_mbps)

    age = _datagen_age_hours(run)
    if age is not None and age > MAX_DATAGEN_AGE_HOURS:
        reason = f"datagen metrics written {age:.0f}h before the run; from an earlier generate"
        for key in _DATAGEN_METRICS | _DATAGEN_DEPENDENT:
            if key in numbers:
                del numbers[key]
                excluded[key] = reason

    return {k: float(v) for k, v in numbers.items() if math.isfinite(float(v))}, excluded


def run_refusals(run: RunRecord, pinned: PinnedConfig) -> list[str]:
    """Reasons this run cannot stand for *pinned*. Empty means comparable."""
    reasons: list[str] = []
    if not run.raw.get("success"):
        reasons.append("run did not succeed")
    if run.mode != pinned.mode:
        reasons.append(f"run mode {run.mode} but pinned config is {pinned.mode}")
    run_fp = run.fingerprint
    if fingerprint_hash(run_fp) != pinned.fingerprint_hash:
        diff = fingerprint_diff(pinned.fingerprint, run_fp)
        reasons.append("config fingerprint differs from the pinned config: " + "; ".join(diff))

    # A run that records the sha256 of the config file it used must have used
    # the pinned file byte for byte. Runs that predate the field cannot be
    # checked this way; the fingerprint is the only guard for them.
    recorded = run.snapshot.get("config_sha256")
    if recorded and recorded != pinned.file_sha256:
        reasons.append(
            f"run used a different config file (sha256 {str(recorded)[:12]}, "
            f"pinned {pinned.file_sha256[:12]})"
        )

    scores = run.scores
    if run.mode == "batch":
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
    overrides = ((pinned.fingerprint.get("spark") or {}).get("executor_overrides")) or {}
    stage_keys = _STAGE_OVERRIDE_KEY.get(run.mode, {})
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
    """Hours between the datagen sidecar's written_at and the run start."""
    fleet = run.raw.get("datagen_fleet") or {}
    written, started = fleet.get("written_at"), run.raw.get("start_time")
    if not written or not started:
        return None
    try:
        w = datetime.fromisoformat(str(written))
        s = datetime.fromisoformat(str(started))
    except ValueError:
        return None
    # start_time is naive local time (datetime.now() in the run path); read
    # it in this host's zone. written_at is UTC-aware.
    if s.tzinfo is None:
        s = s.astimezone()
    if w.tzinfo is None:
        w = w.replace(tzinfo=timezone.utc)
    return (s - w).total_seconds() / 3600


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
        if metric in _ABS_ONLY_METRICS and "abs" not in spec:
            raise PerfGateError(f"{name}: {metric} is a signed percentage; use {{abs: N}}")
        out[str(metric)] = {k: float(v) for k, v in spec.items()}
    return out


def load_store(path: Path) -> BaselineStore:
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError) as e:
        raise PerfGateError(f"cannot read baseline store {path}: {e}") from None
    if raw.get("schema_version") != STORE_SCHEMA_VERSION:
        raise PerfGateError(
            f"{path}: schema_version {raw.get('schema_version')!r} is not {STORE_SCHEMA_VERSION}"
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
        if status == STATUS_ACCEPTED:
            missing = [k for k in ("run_id", "config_hash", "fingerprint_hash") if not entry.get(k)]
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
        )
    return BaselineStore(path=path, baselines=baselines)


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------


def tolerance_for(metric: str, overrides: Mapping[str, Mapping[str, float]]) -> dict[str, float]:
    if metric in overrides:
        return dict(overrides[metric])
    if metric in DEFAULT_METRIC_TOLERANCES:
        return dict(DEFAULT_METRIC_TOLERANCES[metric])
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
    if baseline.config_hash != pinned.config_hash:
        result.reasons.append(
            f"pinned config {baseline.config} changed since the baseline was recorded "
            f"(baseline config_hash {(baseline.config_hash or '')[:12]}, now {pinned.config_hash[:12]}); "
            "record a new baseline"
        )
    if baseline.fingerprint_hash != pinned.fingerprint_hash:
        result.reasons.append(
            "pinned config resolves to a different snapshot than when the baseline was "
            "recorded (a schema default or autosizer change); record a new baseline"
        )
    result.reasons.extend(run_refusals(run, pinned))
    if run.mode == "sustained":
        drained = run.scores.get("corpus_drained")
        if drained != baseline.corpus_drained:
            result.reasons.append(
                f"corpus_drained is {drained!r} but the baseline's is "
                f"{baseline.corpus_drained!r}; freshness and rows/s mean different things "
                "for drained and undrained runs (LB-145)"
            )
    actual, excluded = extract_metrics(run)
    if ("datagen_seconds" in baseline.metrics) != ("datagen_seconds" in actual):
        reason = "datagen stage present in only one of the baseline and the run"
        for key in _DATAGEN_METRICS | _DATAGEN_DEPENDENT:
            actual.pop(key, None)
            excluded.setdefault(key, reason)
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
    if regressed:
        result.verdict = REGRESSION
    return result


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
    if reasons:
        raise PerfGateError(
            f"run {run.run_id} cannot be a baseline for {name}: " + "; ".join(reasons)
        )
    numbers, _excluded = extract_metrics(run)
    if not numbers:
        raise PerfGateError(f"run {run.run_id} has no performance numbers")
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
    """Newest successful run in *runs_dir* whose fingerprint matches *pinned*.

    Guards (scale_ratio, datagen pods) are not applied here, so a matching
    run that fails them is still returned and then refused by compare.
    """
    for run in iter_runs(runs_dir):
        if (
            run.raw.get("success")
            and run.mode == pinned.mode
            and fingerprint_hash(run.fingerprint) == pinned.fingerprint_hash
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
                    run = load_run(explicit_runs[name], dirs[0] if dirs else None)
                else:
                    found = [r for d in dirs if (r := latest_candidate(pinned, d)) is not None]
                    run = max(found, key=lambda r: r.run_id) if found else None
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
