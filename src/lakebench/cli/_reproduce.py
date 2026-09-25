"""Reproduce command -- record and verify reproduction packages.

Reproduction packages pin a published benchmark result to a specific commit,
config, and set of expected numbers, so a third party can rerun the pipeline
on their own cluster and check whether their result falls inside the recorded
tolerance band.

Design: see docs/deep-dive/reproduce.md. This module implements the two
modes documented there:

- ``lakebench reproduce --record RUN_ID --write PATH`` reads a saved
  metrics.json and emits a package YAML.
- ``lakebench reproduce PACKAGE`` runs the pipeline against the config the
  package references, then compares actual vs. expected under the recorded
  tolerance bands.

Exit codes:
  0 -- pass (within tolerance)
  1 -- performance drift exceeded
  2 -- correctness violation (missing stages, scale_ratio mismatch, ...)
"""

from __future__ import annotations

import logging
import math
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml
from rich.panel import Panel
from rich.table import Table

from lakebench.cli._helpers import (
    console,
    print_error,
    print_info,
    print_success,
    print_warning,
)

logger = logging.getLogger(__name__)


SCHEMA_VERSION = 1


DEFAULT_TOLERANCES: dict[str, float] = {
    # Performance metrics vary run-to-run with cluster warmth and scheduling.
    # 20% absorbs that without hiding regressions.
    "performance": 20.0,
    # Correctness has no legitimate tolerance -- a scale ratio off means the
    # pipeline did not process the data it was supposed to.
    "correctness": 0.0,
}


# Every enumerated metric carries an explicit (band, direction).
#
# band -- "correctness" or "performance". The verifier looks up the band
#     here, not in the package, so a malformed or hostile package cannot
#     silently downgrade a correctness metric.
# direction -- "lower", "higher", or "exact":
#     lower  -- lower-is-better; positive drift is bad.
#     higher -- higher-is-better; negative drift is bad.
#     exact  -- either direction of drift is bad (correctness signals like
#               scale_ratio and ingest_ratio: 2x is a bug just as much as
#               0.5x is).
#
# Adding a new metric without an entry in this table trips the completeness
# self-check in _classify_direction() rather than silently defaulting to a
# forgiving direction. That's F7 from the adversarial review.
_METRIC_TABLE: dict[str, tuple[str, str]] = {
    # Correctness -- exact match, zero tolerance by default.
    "scale_ratio": ("correctness", "exact"),
    "ingest_ratio": ("correctness", "exact"),
    # Performance -- lower is better (durations).
    "time_to_value_seconds": ("performance", "lower"),
    "data_freshness_seconds": ("performance", "lower"),
    "datagen_cpu_hr_per_tb": ("performance", "lower"),
    # Performance -- higher is better (throughput, efficiency, QpH).
    "pipeline_throughput_gb_per_second": ("performance", "higher"),
    "compute_efficiency_gb_per_core_hour": ("performance", "higher"),
    "composite_qph": ("performance", "higher"),
    "sustained_throughput_rps": ("performance", "higher"),
    "datagen_aggregate_mbps": ("performance", "higher"),
    # Used by the performance-regression gate (lakebench.metrics.perf_gate).
    # _extract_expected_numbers does not emit them, so reproduction packages
    # are unchanged.
    "datagen_mbps_per_pod": ("performance", "higher"),
    "maintenance_value_pct": ("performance", "higher"),
}

# Per-query QpH (3600 / query seconds) lives in an open namespace keyed by
# query name, for example ``query_qph_Q1_full_aggregation_scan``. Higher is
# better.
QUERY_QPH_PREFIX = "query_qph_"


# Per-stage seconds live in an open namespace: the stage name comes from the
# PipelineBenchmark stage list, which uses short names (bronze, silver, gold,
# datagen, query) in batch mode and (bronze-ingest, silver-stream,
# gold-refresh) in sustained mode. Any key matching STAGE_SECONDS_SUFFIX --
# and not already in _METRIC_TABLE -- is classified as (performance, lower).
STAGE_SECONDS_SUFFIX = "_seconds"


def _is_stage_seconds(metric: str) -> bool:
    """True for open-namespace per-stage duration metrics."""
    return metric.endswith(STAGE_SECONDS_SUFFIX) and metric not in _METRIC_TABLE


def _classify_direction(metric: str) -> tuple[str, str]:
    """Return (band, direction) for a metric key.

    Enumerated metrics come from the table. Stage-seconds default to
    (performance, lower). Anything else is treated as (performance, exact)
    -- the safest default: a metric we don't recognise won't fabricate a
    correctness failure, but a divergence in either direction will still
    be caught. This shuts the door on F7's silent higher-is-better default.
    """
    if metric in _METRIC_TABLE:
        return _METRIC_TABLE[metric]
    if metric.startswith(QUERY_QPH_PREFIX):
        return ("performance", "higher")
    if _is_stage_seconds(metric):
        return ("performance", "lower")
    return ("performance", "exact")


# Legacy set kept for tests / callers that reference it directly. The
# verifier now derives band/direction from _METRIC_TABLE.
_CORRECTNESS_METRICS: frozenset[str] = frozenset(
    m for m, (band, _dir) in _METRIC_TABLE.items() if band == "correctness"
)


class ReproduceError(Exception):
    """Raised for package validation or record-mode errors."""


# ---------------------------------------------------------------------------
# Record mode -- build a package from a saved metrics.json
# ---------------------------------------------------------------------------


def _current_commit_sha() -> str | None:
    """Return the current git HEAD short SHA, or None if git is unavailable."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short=7", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return out.stdout.strip() or None
    except (FileNotFoundError, subprocess.SubprocessError):
        return None


def _extract_expected_numbers(metrics: Any) -> dict[str, float]:
    """Pull the reproducible score set out of a PipelineMetrics record.

    Only metrics classified in ``_CORRECTNESS_METRICS`` or
    ``_PERFORMANCE_METRICS`` land here. Anything else is dropped rather than
    silently promoted into a package that a future reproduce would compare.
    """
    pb = getattr(metrics, "pipeline_benchmark", None)
    numbers: dict[str, float] = {}

    if pb is None:
        return numbers

    # Batch scores
    for attr in (
        "time_to_value_seconds",
        "pipeline_throughput_gb_per_second",
        "compute_efficiency_gb_per_core_hour",
        "scale_ratio",
    ):
        value = getattr(pb, attr, 0.0) or 0.0
        if value > 0:
            numbers[attr] = float(value)

    # Sustained scores. R5: only drop data_freshness_seconds when None
    # (never measured). Producers today filter zeros upstream in
    # _compute_sustained_scores, but this side is defensive: if a future
    # collector change ever emits a legit 0.0, dropping it here would
    # silently hide a regression against instant freshness.
    # throughput and ingest_ratio still gate on > 0 because zero there
    # means "no data flowed" -- a broken pipeline, not a fast one.
    freshness = getattr(pb, "data_freshness_seconds", None)
    if freshness is not None:
        numbers["data_freshness_seconds"] = float(freshness)
    # A drained corpus (LB-145) caps rows/s at corpus size / window, so it is
    # not a throughput and is left out of the comparison.
    drained = getattr(pb, "corpus_drained", None) is True
    for attr in ("sustained_throughput_rps", "ingest_ratio"):
        if drained and attr == "sustained_throughput_rps":
            continue
        value = getattr(pb, attr, None)
        if value is not None and value > 0:
            numbers[attr] = float(value)

    # QpH -- prefer post-compaction, then the plain benchmark result
    post_qph = getattr(pb, "post_compaction_qph", 0.0) or 0.0
    if post_qph > 0:
        numbers["composite_qph"] = float(post_qph)
    else:
        qb = getattr(pb, "query_benchmark", None)
        if qb is not None and getattr(qb, "qph", 0) > 0:
            numbers["composite_qph"] = float(qb.qph)

    # Per-stage seconds -- stage names come from live PipelineBenchmark
    # data (bronze/silver/gold/datagen/query for batch;
    # bronze-ingest/silver-stream/gold-refresh for sustained).
    for stage in getattr(pb, "stages", []):
        stage_name = getattr(stage, "stage_name", "")
        elapsed = getattr(stage, "elapsed_seconds", 0.0) or 0.0
        if not stage_name or elapsed <= 0:
            continue
        key = f"{stage_name.replace('-', '_')}_seconds"
        if _is_stage_seconds(key):
            numbers[key] = float(elapsed)

    # Datagen fleet aggregates -- only present when fleet metrics were collected
    fleet = getattr(metrics, "datagen_fleet", None) or {}
    aggregate_mbps = fleet.get("aggregate_mbps") or 0
    if aggregate_mbps > 0:
        numbers["datagen_aggregate_mbps"] = float(aggregate_mbps)
    cpu_hr_per_tb = fleet.get("cpu_hr_per_tb") or 0
    if cpu_hr_per_tb > 0:
        numbers["datagen_cpu_hr_per_tb"] = float(cpu_hr_per_tb)

    return numbers


def _build_package(
    metrics: Any,
    *,
    config_reference: str | None,
    commit_sha: str | None,
) -> dict[str, Any]:
    """Assemble a reproduction package dict from a PipelineMetrics record."""
    numbers = _extract_expected_numbers(metrics)
    if not numbers:
        raise ReproduceError(
            "The source run has no numbers to reproduce -- "
            "pipeline_benchmark is empty. Was the run successful?"
        )

    snapshot = dict(getattr(metrics, "config_snapshot", {}) or {})
    fleet = getattr(metrics, "datagen_fleet", None) or {}

    pipeline_mode = "batch"
    pb = getattr(metrics, "pipeline_benchmark", None)
    if pb is not None:
        pipeline_mode = getattr(pb, "pipeline_mode", "batch") or "batch"

    # F6: a package with no correctness metric would skip the correctness
    # gate entirely on every future reproduce -- a broken source run
    # could silently publish a package that will always exit 0. Require
    # the mode-appropriate signal.
    required = "scale_ratio" if pipeline_mode == "batch" else "ingest_ratio"
    if required not in numbers:
        raise ReproduceError(
            f"The source run has no {required!r} recorded -- refusing to publish "
            f"a package whose correctness gate is empty. Rerun the pipeline and "
            f"verify {required} > 0 in metrics.json before recording."
        )

    package: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "reproduction_metadata": {
            "commit_sha": commit_sha or "unknown",
            "recorded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "source_run_id": getattr(metrics, "run_id", None),
            "deployment_name": getattr(metrics, "deployment_name", None),
            "pipeline_mode": pipeline_mode,
            "config_reference": config_reference,
            "expected_numbers": numbers,
            "tolerance_pct": dict(DEFAULT_TOLERANCES),
            "config_snapshot": snapshot,
            "datagen_fleet_summary": {
                "pods_reported": fleet.get("pods_reported"),
                "data_quality": fleet.get("data_quality"),
            }
            if fleet
            else {},
        },
    }
    return package


def _record(run_id: str, write: Path, config_reference: str | None) -> None:
    """Read a saved run and write a reproduction package."""
    from lakebench.metrics import MetricsStorage

    storage = MetricsStorage()
    metrics = storage.load_run(run_id)
    if metrics is None:
        print_error(f"Run {run_id!r} not found in {storage.metrics_dir}")
        raise typer.Exit(2)

    try:
        package = _build_package(
            metrics,
            config_reference=config_reference,
            commit_sha=_current_commit_sha(),
        )
    except ReproduceError as e:
        print_error(str(e))
        raise typer.Exit(2) from None

    write.parent.mkdir(parents=True, exist_ok=True)
    with write.open("w") as f:
        yaml.safe_dump(package, f, sort_keys=False, default_flow_style=False)

    numbers = package["reproduction_metadata"]["expected_numbers"]
    print_success(f"Wrote reproduction package to {write}")
    print_info(
        f"  {len(numbers)} metrics recorded across mode={package['reproduction_metadata']['pipeline_mode']}"
    )
    if config_reference is None:
        print_warning(
            "No --config-reference supplied. Verify runs will need "
            "--config PATH to point at a live-credentialed config."
        )


# ---------------------------------------------------------------------------
# Verify mode -- run the pipeline and compare actuals against expected
# ---------------------------------------------------------------------------


def _load_package(path: Path) -> dict[str, Any]:
    """Parse a package YAML and validate its top-level shape."""
    if not path.exists():
        raise ReproduceError(f"Package file not found: {path}")
    try:
        with path.open() as f:
            raw = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ReproduceError(f"Package is not valid YAML: {e}") from None

    if not isinstance(raw, dict):
        raise ReproduceError("Package root must be a mapping")

    schema = raw.get("schema_version")
    if schema != SCHEMA_VERSION:
        raise ReproduceError(
            f"Package schema_version={schema!r} is not supported "
            f"(this lakebench understands {SCHEMA_VERSION})"
        )

    meta = raw.get("reproduction_metadata")
    if not isinstance(meta, dict):
        raise ReproduceError("Package missing 'reproduction_metadata' section")

    numbers = meta.get("expected_numbers")
    if not isinstance(numbers, dict) or not numbers:
        raise ReproduceError("Package 'expected_numbers' is empty; there is nothing to verify")
    for key, value in numbers.items():
        # bool is a subclass of int -- reject explicitly so True/False can't
        # sneak through as 1/0.
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ReproduceError(
                f"expected_numbers[{key!r}] must be a number, got {type(value).__name__}"
            )
        # F2: NaN / Infinity pass every drift comparison silently.
        if not math.isfinite(float(value)):
            raise ReproduceError(f"expected_numbers[{key!r}] must be finite, got {value!r}")

    tolerances = meta.get("tolerance_pct")
    if tolerances is not None:
        if not isinstance(tolerances, dict):
            raise ReproduceError(
                f"tolerance_pct must be a mapping, got {type(tolerances).__name__}"
            )
        for key, value in tolerances.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ReproduceError(
                    f"tolerance_pct[{key!r}] must be a number, got {type(value).__name__}"
                )
            if not math.isfinite(float(value)) or float(value) < 0:
                raise ReproduceError(
                    f"tolerance_pct[{key!r}] must be a finite, non-negative number, got {value!r}"
                )
        # R1: a package with tolerance_pct.correctness > 0 would silently
        # rebuild F1 -- correctness would become forgiving again and a
        # broken pipeline would exit 0. The correctness band has no
        # legitimate tolerance; a package that asks for one is malformed.
        corr_tol = tolerances.get("correctness")
        if corr_tol is not None and float(corr_tol) > 0:
            raise ReproduceError(
                f"tolerance_pct.correctness must be 0 (got {corr_tol!r}); "
                "correctness metrics have no legitimate tolerance."
            )

    return raw


def _resolve_config_path(
    package: dict[str, Any], override: Path | None, package_path: Path
) -> Path:
    """Return the config file the verify run should use.

    An explicit --config override always wins -- but must exist (F8: an
    override typo used to blow up minutes later inside the deployer). The
    package's ``config_reference`` is resolved relative to the package file's
    directory when no override is given.
    """
    if override is not None:
        if not override.exists():
            raise ReproduceError(f"--config path {override} does not exist")
        return override

    meta = package["reproduction_metadata"]
    ref = meta.get("config_reference")
    if not ref:
        raise ReproduceError("Package has no config_reference; pass --config PATH to name one")
    candidate = (package_path.parent / ref).resolve()
    if not candidate.exists():
        raise ReproduceError(
            f"config_reference {ref!r} resolves to {candidate}, which does not exist. "
            f"Pass --config PATH to override."
        )
    return candidate


def _measure_actual_numbers(metrics: Any) -> dict[str, float]:
    """Extract the same metric surface as ``_extract_expected_numbers``."""
    return _extract_expected_numbers(metrics)


def _classify(metric: str) -> str:
    """Return 'correctness' or 'performance' for a given metric key."""
    band, _direction = _classify_direction(metric)
    return band


def _drift_pct(actual: float, expected: float) -> float:
    """Signed percentage drift of actual vs expected.

    Positive means actual > expected. When expected is zero and actual is
    zero, drift is zero (exact match, no regression). When expected is zero
    but actual is not, drift is signed infinity so any finite tolerance
    fails -- R5 made expected=0 a legitimate value (e.g. instant freshness),
    so we can't hand back a bare 0.0 and hide a real regression against it.
    """
    if expected == 0:
        if actual == 0:
            return 0.0
        return math.inf if actual > 0 else -math.inf
    return ((actual - expected) / expected) * 100.0


def _is_over_band(metric: str, actual: float, expected: float, tolerance_pct: float) -> bool:
    """Decide whether a metric drifted enough to fail.

    Direction is taken from ``_classify_direction`` -- a single source of
    truth for the whole module. Lower-is-better: only positive drift counts.
    Higher-is-better: only negative drift. Exact: drift in EITHER direction
    counts (F1 -- scale_ratio=2.0 vs expected=1.0 must not pass just because
    higher looks like "more data").
    """
    drift = _drift_pct(actual, expected)
    _band, direction = _classify_direction(metric)
    if direction == "exact":
        return abs(drift) > tolerance_pct
    if direction == "lower":
        return drift > tolerance_pct
    # direction == "higher"
    return -drift > tolerance_pct


def _compare(
    expected: dict[str, float],
    actual: dict[str, float],
    tolerances: dict[str, float],
) -> tuple[list[dict[str, Any]], int]:
    """Compare expected vs actual and return (rows, exit_code).

    Rows are dicts with keys metric, expected, actual, drift_pct, band,
    tolerance_pct, status. Exit code is 0/1/2 per the CLI contract.
    """
    perf_tol = float(tolerances.get("performance", DEFAULT_TOLERANCES["performance"]))
    # R1: correctness tolerance is hard-zero at the compare layer regardless
    # of what the package says. _load_package already rejects packages that
    # try to inflate it, but this belt-and-braces guarantees the invariant
    # for callers who reach _compare through any other path.
    corr_tol = 0.0

    rows: list[dict[str, Any]] = []
    correctness_failed = False
    performance_failed = False

    for metric, expected_value in expected.items():
        band = _classify(metric)
        tol = corr_tol if band == "correctness" else perf_tol
        actual_value = actual.get(metric)
        if actual_value is None:
            rows.append(
                {
                    "metric": metric,
                    "expected": expected_value,
                    "actual": None,
                    "drift_pct": None,
                    "band": band,
                    "tolerance_pct": tol,
                    "status": "missing",
                }
            )
            if band == "correctness":
                correctness_failed = True
            else:
                performance_failed = True
            continue

        drift = _drift_pct(actual_value, expected_value)
        over = _is_over_band(metric, actual_value, expected_value, tol)
        status = "fail" if over else "pass"
        rows.append(
            {
                "metric": metric,
                "expected": expected_value,
                "actual": actual_value,
                "drift_pct": drift,
                "band": band,
                "tolerance_pct": tol,
                "status": status,
            }
        )
        if over:
            if band == "correctness":
                correctness_failed = True
            else:
                performance_failed = True

    if correctness_failed:
        exit_code = 2
    elif performance_failed:
        exit_code = 1
    else:
        exit_code = 0
    return rows, exit_code


def _print_comparison(rows: list[dict[str, Any]], exit_code: int) -> None:
    """Render the comparison table + verdict panel."""
    table = Table(show_header=True, header_style="bold", expand=False)
    table.add_column("Metric", style="cyan")
    table.add_column("Expected", justify="right")
    table.add_column("Actual", justify="right")
    table.add_column("Drift", justify="right")
    table.add_column("Band", justify="center")
    table.add_column("Status", justify="center")

    for row in rows:
        exp_s = f"{row['expected']:.2f}"
        if row["actual"] is None:
            act_s = "-"
            drift_s = "-"
        else:
            act_s = f"{row['actual']:.2f}"
            drift_s = f"{row['drift_pct']:+.1f}%"
        status = row["status"]
        if status == "pass":
            status_s = "[green]pass[/green]"
        elif status == "missing":
            status_s = "[yellow]missing[/yellow]"
        else:
            status_s = "[red]fail[/red]"
        table.add_row(row["metric"], exp_s, act_s, drift_s, row["band"], status_s)

    console.print()
    console.print(table)

    if exit_code == 0:
        verdict = "[green]PASS -- every metric within tolerance[/green]"
    elif exit_code == 1:
        verdict = "[yellow]FAIL -- performance drift over tolerance[/yellow]"
    else:
        verdict = "[red]FAIL -- correctness violation[/red]"
    console.print()
    console.print(Panel(verdict, title="Reproduction verdict", expand=False))


def _find_reproduce_run(storage, deployment_name: str, start_watermark: datetime) -> Any:
    """Find the run this reproduce just produced.

    F4: list_runs is a global view -- a parallel `lakebench run` in another
    shell would poison a simple set-diff. We filter by two attributes we
    control end-to-end: the deployment_name from the config we ran against,
    and a start_time strictly after the watermark we captured before deploy.
    Both are stable across the metrics.json round trip.
    """
    candidates: list[tuple[str, str]] = []
    for row in storage.list_runs():
        if row.get("deployment_name") != deployment_name:
            continue
        start_time = row.get("start_time")
        if not start_time:
            continue
        try:
            run_start = datetime.fromisoformat(start_time)
        except ValueError:
            continue
        # R3: MetricsCollector.start_run uses naive datetime.now() (local
        # time). Labelling that as UTC would shift the timestamp by the
        # host's UTC offset -- on a west-of-UTC host every legitimate run
        # would fall BEFORE the UTC watermark and be dropped. .astimezone()
        # on a naive datetime interprets it as local (Python 3.6+), which
        # is what we want.
        if run_start.tzinfo is None:
            run_start = run_start.astimezone(timezone.utc)
        # The watermark is always constructed tz-aware UTC at the caller;
        # comparison against a tz-aware run_start is safe.
        if run_start >= start_watermark:
            candidates.append((start_time, row["run_id"]))
    if not candidates:
        raise ReproduceError(
            f"No new {deployment_name!r} run recorded after {start_watermark.isoformat()} -- "
            "pipeline may have failed before saving metrics.json"
        )
    # Take the earliest new run for this deployment: that's the one we just
    # started, not a later concurrent one that happened to finish faster.
    candidates.sort()
    return storage.load_run(candidates[0][1])


def _run_pipeline(
    config_file: Path,
    timeout: int | None,
    keep: bool,
) -> Any:
    """Run destroy -> deploy -> generate -> run -> (optional) destroy, then
    return the PipelineMetrics the pipeline just produced.

    Delegates to the existing CLI command functions so the reproduce path
    does not fork the pipeline plumbing.

    F5: --keep controls only the POST-run tear-down. Every reproduce starts
    with a destroy pass so the pipeline runs against empty buckets -- a
    prior --keep run cannot silently contaminate scale_ratio or ingest_ratio.

    F4: the produced run is identified by deployment_name + start-time
    watermark, so a concurrent `lakebench run` in another shell cannot
    poison the comparison.
    """
    from lakebench.cli._deploy import deploy as _deploy_cmd
    from lakebench.cli._destroy import destroy as _destroy_cmd
    from lakebench.cli._generate import generate as _generate_cmd
    from lakebench.cli._run import run as _run_cmd
    from lakebench.config import load_config
    from lakebench.metrics import MetricsStorage

    storage = MetricsStorage()

    # F5: pre-destroy is idempotent-safe. destroy(--force) on a missing
    # namespace returns implicitly (exit 0). A non-zero exit means a
    # component partially failed to clean up (Iceberg drop, S3 multipart,
    # PVC finalizer) -- bronze/silver/gold may still hold stale data.
    # R2: swallowing that would let the pipeline run against contaminated
    # buckets and quietly inflate scale_ratio close to expected, defeating
    # F5's whole purpose. Only exit 0 (or missing exit_code) means "safe
    # to proceed"; anything else is a real destroy failure.
    try:
        _destroy_cmd(config_file=config_file, force=True)
    except typer.Exit as e:
        exit_code = getattr(e, "exit_code", None)
        if exit_code not in (0, None):
            raise ReproduceError(
                f"Pre-run destroy failed with exit code {exit_code}. "
                "Refusing to deploy against a namespace that may still hold "
                "stale data (scale_ratio and ingest_ratio would be unreliable). "
                "Fix the destroy problem, then rerun reproduce."
            ) from None
        logger.info("pre-run destroy exited cleanly (exit_code=%s); continuing", exit_code)

    # Watermark BEFORE deploy, so any run started by this reproduce falls
    # strictly after it. Load config once to get the deployment_name.
    cfg = load_config(config_file)
    deployment_name = cfg.name
    start_watermark = datetime.now(timezone.utc)

    _deploy_cmd(config_file=config_file, yes=True)
    _generate_cmd(config_file=config_file, wait=True, timeout=timeout or 14400, yes=True)
    _run_cmd(config_file=config_file, yes=True, timeout=timeout)

    result = _find_reproduce_run(storage, deployment_name, start_watermark)
    if result is None:
        raise ReproduceError("Could not load the run this reproduce produced")

    if not keep:
        try:
            _destroy_cmd(config_file=config_file, force=True)
        except typer.Exit as e:
            # A destroy failure should not mask a passing reproduce; log it.
            print_warning(f"destroy exited with code {e.exit_code}; continuing")

    return result


def _verify(
    package_path: Path,
    config_override: Path | None,
    timeout: int | None,
    keep: bool,
    dry_run: bool,
    allow_commit_drift: bool,
) -> None:
    """Load a package, run the pipeline, compare, exit with 0/1/2."""
    try:
        package = _load_package(package_path)
    except ReproduceError as e:
        print_error(str(e))
        raise typer.Exit(2) from None

    meta = package["reproduction_metadata"]
    expected = meta["expected_numbers"]
    tolerances = meta.get("tolerance_pct") or DEFAULT_TOLERANCES

    print_info(f"Package: {package_path}")
    print_info(f"  source run: {meta.get('source_run_id')}")
    print_info(f"  commit_sha: {meta.get('commit_sha')}")
    print_info(f"  mode: {meta.get('pipeline_mode')}")
    print_info(f"  metrics recorded: {len(expected)}")

    # F3: commit drift means the code path measured is not the code path
    # the package claims. Exit 2 (correctness) unless the caller opts in.
    # R4: normalise both sides to a 7-char prefix -- a hand-edited package
    # might carry a 40-char full SHA, and _current_commit_sha returns a
    # 7-char short SHA. Direct equality would spuriously fire on the same
    # commit at different SHA lengths.
    current_sha = _current_commit_sha()
    recorded_sha = meta.get("commit_sha")
    current_prefix = current_sha[:7] if current_sha else None
    recorded_prefix = recorded_sha[:7] if isinstance(recorded_sha, str) else None
    if (
        current_prefix
        and recorded_prefix
        and recorded_sha != "unknown"
        and current_prefix != recorded_prefix
    ):
        if allow_commit_drift:
            print_warning(
                f"Commit drift accepted: package recorded at {recorded_sha}, "
                f"HEAD is {current_sha}. --allow-commit-drift set."
            )
        else:
            print_error(
                f"Commit drift: package recorded at {recorded_sha}, HEAD is {current_sha}. "
                "The measured code path is not the one this package claims. "
                f"Check out {recorded_sha} or pass --allow-commit-drift."
            )
            raise typer.Exit(2)

    try:
        config_file = _resolve_config_path(package, config_override, package_path)
    except ReproduceError as e:
        print_error(str(e))
        raise typer.Exit(2) from None
    print_info(f"  config: {config_file}")

    if dry_run:
        print_warning("--dry-run set: package validation only, no pipeline run")
        console.print()
        console.print(Panel("[green]Package parsed cleanly[/green]", title="Dry run", expand=False))
        return

    try:
        metrics = _run_pipeline(config_file, timeout, keep)
    except ReproduceError as e:
        print_error(str(e))
        raise typer.Exit(2) from None

    actual = _measure_actual_numbers(metrics)
    rows, exit_code = _compare(expected, actual, tolerances)
    _print_comparison(rows, exit_code)

    if exit_code != 0:
        raise typer.Exit(exit_code)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def reproduce(
    package: Annotated[
        Path | None,
        typer.Argument(
            help="Path to a reproduction package YAML (verify mode)",
        ),
    ] = None,
    record: Annotated[
        str | None,
        typer.Option(
            "--record",
            help="Record mode: build a package from this saved run ID",
        ),
    ] = None,
    write: Annotated[
        Path | None,
        typer.Option(
            "--write",
            help="Record mode: write the package to this path",
        ),
    ] = None,
    config_reference: Annotated[
        str | None,
        typer.Option(
            "--config-reference",
            help="Record mode: store this relative config path in the package",
        ),
    ] = None,
    config_override: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Verify mode: config YAML to run instead of the package's config_reference",
        ),
    ] = None,
    timeout: Annotated[
        int | None,
        typer.Option(
            "--timeout",
            "-t",
            help="Verify mode: per-job timeout in seconds",
        ),
    ] = None,
    keep: Annotated[
        bool,
        typer.Option(
            "--keep",
            help=(
                "Verify mode: do not destroy the deployment after the run. "
                "Note: reproduce always destroys BEFORE the run to guarantee "
                "fresh buckets; --keep only affects post-run cleanup."
            ),
        ),
    ] = False,
    allow_commit_drift: Annotated[
        bool,
        typer.Option(
            "--allow-commit-drift",
            help=(
                "Verify mode: run even when HEAD differs from the recorded "
                "commit. Default is to refuse with exit 2 -- comparing numbers "
                "across code paths cannot claim to reproduce anything."
            ),
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Verify mode: parse the package and exit without running the pipeline",
        ),
    ] = False,
) -> None:
    """Record or verify a reproduction package.

    Record mode: reads a saved run and emits a package pinning its expected
    numbers, tolerances, commit SHA, and config reference.

        lakebench reproduce --record RUN_ID --write path/to/package.yaml
                            --config-reference examples/my-config.yaml

    Verify mode: loads a package, runs the pipeline against the referenced
    config, and compares actuals against expected under the recorded
    tolerance bands. Exits 0 on pass, 1 on performance drift, 2 on
    correctness violation.

        lakebench reproduce path/to/package.yaml [--config CONFIG]

    See docs/deep-dive/reproduce.md for the full contract.
    """
    # Record mode -- --record and --write must both be present.
    if record is not None or write is not None:
        if record is None or write is None:
            print_error("Record mode requires both --record RUN_ID and --write PATH")
            raise typer.Exit(2)
        if package is not None:
            print_error("Positional PACKAGE cannot be combined with --record")
            raise typer.Exit(2)
        _record(record, write, config_reference)
        return

    # Verify mode -- positional package required.
    if package is None:
        print_error("Verify mode requires a PACKAGE path (or use --record/--write)")
        raise typer.Exit(2)

    _verify(
        package_path=package,
        config_override=config_override,
        timeout=timeout,
        keep=keep,
        dry_run=dry_run,
        allow_commit_drift=allow_commit_drift,
    )
