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

Exit codes (see docs/exit-codes.md):
  0  -- pass (within tolerance)
  14 -- requirement unmet: correctness violation (missing stages,
        scale_ratio mismatch, ...), performance drift outside its band, or
        commit drift without --allow-commit-drift (2 and 1 in 1.6)
  2  -- usage: a package or config that cannot be read or does not match
  1  -- the reproduction could not run or its run could not be found
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
    esc,
    print_error,
    print_info,
    print_success,
    print_warning,
)
from lakebench.exit_codes import ExitCode

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
    "pre_compaction_qph": ("performance", "higher"),
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
    # rows/s of a corpus that drained early is over a short arrival (or, on
    # a record from before the window, corpus size / window) and is left out.
    from lakebench.metrics.continuous_window import drained_rps_excluded

    drained = bool(
        drained_rps_excluded(
            getattr(pb, "corpus_drained", None), getattr(pb, "window_arrival_fraction", None)
        )
    )
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


def _run_query_set(metrics: Any) -> str | None:
    """The query-set id of the run's QpH (the benchmark the QpH came from)."""
    pb = getattr(metrics, "pipeline_benchmark", None)
    for bench in (getattr(pb, "query_benchmark", None), getattr(metrics, "benchmark", None)):
        qs = getattr(bench, "query_set_id", None)
        if qs:
            return qs
    return None


def _run_maintenance_policy(metrics: Any) -> str:
    """The maintenance policy id the run was measured under (legacy if unrecorded)."""
    from lakebench.metrics.maintenance_policy import LEGACY_MAINTENANCE_POLICY_ID

    return getattr(metrics, "maintenance_policy_id", None) or LEGACY_MAINTENANCE_POLICY_ID


def _policy_refusal(meta: dict[str, Any], actual: str | None) -> str | None:
    """Why a run under *actual* policy cannot verify the package, or None.

    Refused, not warned: maintenance changes post-maintenance QpH,
    continuous freshness and throughput and the object count, so a drift
    across policies is not a regression or an improvement of the code.
    A package without the field was recorded under the legacy policy.
    """
    from lakebench.metrics.maintenance_policy import policy_mismatch, recorded_policy

    problem = policy_mismatch(recorded_policy(meta), actual)
    if problem is None:
        return None
    return problem + "; make a new run with this version and record the package from it"


def _run_experiment(metrics: Any) -> dict[str, Any] | None:
    """The run's experiment block (metrics/experiment.py), or None."""
    block = getattr(metrics, "experiment_block", None)
    exp = block() if callable(block) else getattr(metrics, "experiment", None)
    return exp if isinstance(exp, dict) and exp.get("schema") else None


def _failed_queries(metrics: Any) -> set[str]:
    """Benchmark queries the run records as failed."""
    bench = getattr(metrics, "benchmark", None)
    if bench is None:
        bench = getattr(getattr(metrics, "pipeline_benchmark", None), "query_benchmark", None)
    out = set()
    for q in getattr(bench, "queries", None) or []:
        if isinstance(q, dict) and not q.get("success", True):
            name = q.get("name") or q.get("query_name")
            if name:
                out.add(str(name))
    return out


def _experiment_refusal(meta: dict[str, Any], metrics: Any) -> str | None:
    """Why the reproduce run is not the package's experiment, or returned
    different benchmark results, or None."""
    from lakebench.metrics.experiment import stored_identity_refusals

    reasons = stored_identity_refusals(
        meta.get("experiment_identity"),
        meta.get("result_fingerprints"),
        _run_experiment(metrics),
        "package",
        # A failed query is reported once, as a failed number, like the
        # perf gate does, not a second time as a result mismatch.
        failed=_failed_queries(metrics),
    )
    if not reasons:
        return None
    return "The run cannot verify the package: " + "; ".join(reasons) + "."


def _benchmark_samples(metrics: Any) -> int | None:
    """Timed samples per query behind the run's QpH; 1 for pre-LB-150 records."""
    from lakebench.benchmark.spread import samples_per_query

    pb = getattr(metrics, "pipeline_benchmark", None)
    qb = getattr(pb, "query_benchmark", None) if pb is not None else None
    if qb is None:
        qb = getattr(metrics, "benchmark", None)
    return samples_per_query(getattr(qb, "queries", None) or [])


def _sample_mismatch(meta: dict[str, Any], got: int | None) -> str | None:
    """Why a batch QpH measured with *got* samples cannot verify the package.

    A package recorded before per-query repeats has no
    ``benchmark_samples_per_query`` and took one sample. Refused rather than
    warned: a single sample and a median of three are different estimators,
    so the QpH drift between them is a bias and the exit code would be wrong
    either way.
    """
    expected = meta.get("expected_numbers") or {}
    if meta.get("pipeline_mode", "batch") != "batch" or "composite_qph" not in expected:
        return None
    want = meta.get("benchmark_samples_per_query", 1)
    if got is None or got == want:
        return None
    return (
        f"The package's QpH is the median of {want} sample(s) per query but this run took "
        f"{got}; set architecture.benchmark.iterations: {want} in the config to reproduce it."
    )


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
    from lakebench.metrics.maintenance_policy import not_current

    stale_policy = not_current(_run_maintenance_policy(metrics))
    if stale_policy:
        # Such a package could never verify on this version.
        raise ReproduceError(f"The source run cannot be packaged: {stale_policy}.")

    from lakebench.metrics.experiment import NO_PROVENANCE, identity, result_fingerprints

    experiment = _run_experiment(metrics)
    if experiment is None:
        raise ReproduceError(
            f"The source run cannot be packaged: {NO_PROVENANCE} (it predates the "
            "experiment block, so a reproduce could not check it ran the same experiment)."
        )
    from lakebench.benchmark.fingerprint import usable
    from lakebench.metrics.experiment import results_established

    established = results_established(experiment)
    if established is not True:
        raise ReproduceError(
            f"The source run cannot be packaged: comparability not established ({established}); "
            "a reproduce could never show it returned the same results."
        )
    failed = _failed_queries(metrics)
    unfp = sorted(
        n for n, f in result_fingerprints(experiment).items() if n not in failed and not usable(f)
    )
    if unfp:
        raise ReproduceError(
            "The source run cannot be packaged: queries without a usable result fingerprint "
            f"({', '.join(unfp)}) could never be shown equal to a reproduce run."
        )

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
            # QpH is only reproducible over the same query set.
            "query_set_id": _run_query_set(metrics),
            # ...and under the same table-maintenance policy.
            "maintenance_policy_id": _run_maintenance_policy(metrics),
            "benchmark_samples_per_query": _benchmark_samples(metrics) or 1,
            # The experiment (workload, corpus, seed, scale, mode) and what
            # each benchmark query returned: a reproduce must match both.
            "experiment_identity": identity(experiment),
            "result_fingerprints": result_fingerprints(experiment),
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
        raise typer.Exit(ExitCode.USAGE)

    try:
        package = _build_package(
            metrics,
            config_reference=config_reference,
            commit_sha=_current_commit_sha(),
        )
    except ReproduceError as e:
        print_error(str(e))
        raise typer.Exit(ExitCode.USAGE) from None

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
    query_sets: tuple[str | None, str | None] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Compare expected vs actual and return (rows, outcome).

    Rows are dicts with keys metric, expected, actual, drift_pct, band,
    tolerance_pct, status. ``outcome`` is 0 (pass), 1 (performance drift) or
    2 (correctness violation); the command exits 14 for either drift.
    ``query_sets`` is (package, actual) query-set ids; QpH over different or
    unrecorded sets is refused (status ``incomparable``, a performance
    failure) rather than compared.
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

    qph_ok, qph_reason = True, ""
    if query_sets is not None:
        from lakebench.benchmark.queries import qph_comparable

        qph_ok, qph_reason = qph_comparable(*query_sets)

    for metric, expected_value in expected.items():
        band = _classify(metric)
        tol = corr_tol if band == "correctness" else perf_tol
        actual_value = actual.get(metric)
        if "qph" in metric and not qph_ok:
            # Different recorded sets: a performance failure. A package that
            # predates query-set ids: not compared, not failed (re-record it).
            legacy = query_sets is not None and not query_sets[0]
            rows.append(
                {
                    "metric": metric,
                    "expected": expected_value,
                    "actual": actual_value,
                    "drift_pct": None,
                    "band": band,
                    "tolerance_pct": tol,
                    "status": "incomparable",
                    "reason": qph_reason,
                }
            )
            if not legacy:
                performance_failed = True
            continue
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
        outcome = 2
    elif performance_failed:
        outcome = 1
    else:
        outcome = 0
    return rows, outcome


def _print_comparison(rows: list[dict[str, Any]], outcome: int) -> None:
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
        if row["actual"] is None or row.get("drift_pct") is None:
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
        elif status == "incomparable":
            status_s = "[yellow]incomparable[/yellow]"
        else:
            status_s = "[red]fail[/red]"
        table.add_row(row["metric"], exp_s, act_s, drift_s, row["band"], status_s)

    console.print()
    console.print(table)
    for row in rows:
        if row["status"] == "incomparable":
            console.print(
                f"[yellow]{esc(row['metric'])} not compared: {esc(row.get('reason'))}. "
                "Re-record the package on the current query set.[/yellow]"
            )

    if outcome == 0:
        verdict = "[green]PASS -- every metric within tolerance[/green]"
    elif outcome == 1:
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
        # R3: before v1.6 MetricsCollector.start_run used naive
        # datetime.now() (local time; now aware UTC). Labelling that as UTC would shift the timestamp by the
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


def _refuse_existing(cfg: Any, config_file: Path) -> None:
    """Refuse before deploy when anything reproduce would create already exists.

    reproduce destroys only what it created in this invocation, and measures
    against empty buckets, so it creates its namespace and buckets itself:
    an existing namespace or bucket is refused, never destroyed or adopted.
    Every read error stops it (exit 4) rather than reading as absent.
    """
    from lakebench.exit_codes import PrerequisiteError, SafetyRefusal, UsageError
    from lakebench.k8s.target import ContextConflictError

    k8s_cfg = cfg.platform.kubernetes
    s3_cfg = cfg.platform.storage.s3
    if not k8s_cfg.create_namespace:
        raise UsageError(
            "reproduce creates its own namespace, and this config sets "
            "platform.kubernetes.create_namespace: false",
            why="reproduce refuses an existing namespace and destroys only what it created",
            next="set create_namespace: true in the config reproduce runs",
            path="cli.bad_argument",
        )
    if not s3_cfg.create_buckets:
        raise UsageError(
            "reproduce creates its own buckets, and this config sets "
            "platform.storage.s3.create_buckets: false",
            why="reproduce measures against empty buckets it created and refuses existing ones",
            next="set create_buckets: true in the config reproduce runs",
            path="cli.bad_argument",
        )

    namespace = cfg.get_namespace()
    try:
        from lakebench.k8s import get_k8s_client

        # Pins the process to the config's cluster context for every later call.
        present = get_k8s_client(context=k8s_cfg.context, namespace=namespace).namespace_exists(
            namespace
        )
    except ContextConflictError:
        raise
    except Exception as e:  # noqa: BLE001 -- unreadable is not absent
        raise PrerequisiteError(
            f"cannot check whether namespace {namespace} exists: {e}",
            why="reproduce refuses an existing namespace, so it must read it first",
            path="k8s.unreachable",
        ) from e
    if present:
        raise SafetyRefusal(
            f"reproduce needs a new deployment; namespace {namespace} exists",
            why="reproduce destroys only a deployment it created in this run",
            next=f"lakebench destroy {config_file}, then re-run, or give the package's "
            "config a new name",
            path="reproduce.existing_namespace",
        )

    from lakebench.s3 import S3Client

    s3 = S3Client(
        endpoint=s3_cfg.endpoint,
        access_key=s3_cfg.access_key,
        secret_key=s3_cfg.secret_key,
        region=s3_cfg.region,
        path_style=s3_cfg.path_style,
        ca_cert=s3_cfg.ca_cert,
        verify_ssl=s3_cfg.verify_ssl,
    )
    if s3._init_error:
        raise PrerequisiteError(
            f"cannot check the buckets: {s3._init_error}",
            why="reproduce refuses existing buckets, so it must read them first",
            path="s3.unreachable",
        )
    for bucket in dict.fromkeys(
        (s3_cfg.buckets.bronze, s3_cfg.buckets.silver, s3_cfg.buckets.gold)
    ):
        try:
            exists = s3.bucket_exists(bucket)
        except Exception as e:  # noqa: BLE001 -- 403 or unreachable is not absent
            raise PrerequisiteError(
                f"cannot check whether bucket {bucket} exists: {e}",
                why="reproduce refuses existing buckets, so it must read them first",
                path="s3.unreachable",
            ) from e
        if exists:
            raise SafetyRefusal(
                f"reproduce needs new buckets; bucket {bucket} exists",
                why="reproduce measures against empty buckets it created, and destroys "
                "only what it created",
                next="remove the bucket if it is yours, or give the package's config "
                "new bucket names",
                path="reproduce.existing_namespace",
            )


def _own_incarnation(cfg: Any, config_file: Path, own: str) -> str:
    """``uid#own`` when the namespace carries the nonce this reproduce deployed.

    One ``read_namespace``. The comparison is against ``own``, never against
    a value read back, so a deploy that replaced ours between our deploy and
    this read is refused, not taken over.
    """
    from kubernetes import client

    from lakebench.config.deploy_state import read_namespace_identity
    from lakebench.exit_codes import PrerequisiteError, SafetyRefusal

    namespace = cfg.get_namespace()
    try:
        ident = read_namespace_identity(client.CoreV1Api(), namespace)
    except Exception as e:  # noqa: BLE001
        raise PrerequisiteError(
            f"cannot read namespace {namespace} after deploy: {e}",
            why="reproduce confirms the namespace carries its own nonce before it runs "
            "or destroys anything",
            next=f"reproduce generated, ran and destroyed nothing; check "
            f"lakebench status {config_file}",
            path="k8s.unreachable",
        ) from e
    if ident is None or not ident.uid or ident.nonce != own:
        found = "no namespace" if ident is None else (ident.nonce or "no nonce")
        raise SafetyRefusal(
            f"namespace {namespace} does not carry the nonce this reproduce deployed "
            f"({own}); found {found}",
            why="another deploy replaced the deployment after this reproduce made it",
            next="reproduce generated, ran and destroyed nothing; check which deployment "
            "the namespace holds before destroying it",
            path="reproduce.nonce_changed",
        )
    return f"{ident.uid}#{own}"


def _run_pipeline(
    config_file: Path,
    timeout: int | None,
    keep: bool,
    refusals: list[Any] | None = None,
) -> Any:
    """Deploy -> generate -> run -> (optional) destroy, then return the
    PipelineMetrics the pipeline just produced.

    Delegates to the existing CLI command bodies so the reproduce path does
    not fork the pipeline plumbing.

    reproduce destroys only what it created in this invocation. It refuses an
    existing namespace or bucket (that is also what keeps its buckets empty,
    so a prior --keep run cannot contaminate scale_ratio or ingest_ratio),
    deploys with its own nonce and with ``require_new`` (a namespace or bucket
    that appears meanwhile is refused, not adopted), confirms the namespace
    carries that nonce, and passes ``uid#nonce`` to its destroy, which refuses
    any other incarnation. A refused post-run destroy is appended to
    ``refusals``; the caller reports it after the verdict.

    F4: the produced run is identified by deployment_name + start-time
    watermark, so a concurrent `lakebench run` in another shell cannot
    poison the comparison.
    """
    import uuid

    from lakebench.cli._deploy import _deploy_impl
    from lakebench.cli._destroy import _destroy_impl
    from lakebench.cli._generate import generate as _generate_cmd
    from lakebench.cli._helpers import _journal_safe, journal_open
    from lakebench.cli._run import run as _run_cmd
    from lakebench.config import ConfigError, LoadPurpose, load_config
    from lakebench.exit_codes import SafetyRefusal
    from lakebench.journal import EventType
    from lakebench.metrics import MetricsStorage

    storage = MetricsStorage()

    # Load first: deploy and run refuse a nameless config or a removed key,
    # and that refusal comes before any cluster read.
    try:
        cfg = load_config(config_file, purpose=LoadPurpose.RUN)
    except ConfigError as e:
        raise ReproduceError(str(e)) from None

    _refuse_existing(cfg, config_file)
    namespace = cfg.get_namespace()

    # Watermark BEFORE deploy, so any run started by this reproduce falls
    # strictly after it.
    deployment_name = cfg.name
    start_watermark = datetime.now(timezone.utc)

    own = uuid.uuid4().hex
    try:
        recorded = _deploy_impl(config_file, yes=True, nonce=own, require_new=True)
    except BaseException:
        print_warning(
            f"reproduce stopped at deploy and destroyed nothing; check "
            f"`lakebench status {config_file}` before removing namespace {namespace}"
        )
        raise
    if recorded != own:  # _deploy_impl records and stamps the nonce it is given
        raise RuntimeError(f"deploy recorded nonce {recorded!r}, not this reproduce's {own!r}")
    created = _own_incarnation(cfg, config_file, own)
    print_info(f"reproduce created namespace {namespace} as {created}")
    j = journal_open(config_file, config_name=cfg.name)
    _journal_safe(
        j.record,
        EventType.REPRODUCE_CREATED_INCARNATION,
        message=f"reproduce created namespace {namespace}",
        command="reproduce",
        success=True,
        details={"namespace": namespace, "incarnation": created},
    )

    try:
        _generate_cmd(config_file=config_file, timeout=timeout or 14400, yes=True)
        _run_cmd(config_file=config_file, yes=True, timeout=timeout)
    except BaseException:
        print_warning(
            f"reproduce stopped before its destroy; namespace {namespace} ({created}) is "
            f"left: `lakebench destroy {config_file}` removes it"
        )
        raise

    result = _find_reproduce_run(storage, deployment_name, start_watermark)
    if result is None:
        raise ReproduceError("Could not load the run this reproduce produced")

    if not keep:
        try:
            _destroy_impl(config_file, force=True, expected_incarnation=created)
        except SafetyRefusal as e:
            # Nothing was deleted: the namespace is no longer the one we made.
            print_warning(f"destroy refused, namespace {namespace} kept: {e.what}")
            if refusals is not None:
                refusals.append(e)
        except typer.Exit as e:
            # A destroy failure should not mask a passing reproduce; log it.
            if e.exit_code not in (0, None):
                print_warning(f"destroy exited with code {e.exit_code}; continuing")
        except Exception as e:  # noqa: BLE001
            print_warning(
                f"destroy failed ({e}); namespace {namespace} may be left: "
                f"`lakebench destroy {config_file}`"
            )

    return result


def _post_destroy_refusal(refusals: list[Any]) -> None:
    """Exit 3 when reproduce's own destroy was refused: its deployment was
    replaced while it ran, so the measurement may not be its own either."""
    if refusals:
        from lakebench.exit_codes import SafetyRefusal

        raise SafetyRefusal(
            "the deployment this reproduce created was replaced before its destroy; "
            "nothing was destroyed",
            why=str(refusals[0].what),
            next="check which deployment the namespace holds, and re-run reproduce on a "
            "new namespace",
            path="reproduce.nonce_changed",
        )


def _verify(
    package_path: Path,
    config_override: Path | None,
    timeout: int | None,
    keep: bool,
    dry_run: bool,
    allow_commit_drift: bool,
) -> None:
    """Load a package, run the pipeline, compare; exit 0, or 14 outside tolerance."""
    try:
        package = _load_package(package_path)
    except ReproduceError as e:
        print_error(str(e))
        raise typer.Exit(ExitCode.USAGE) from None

    meta = package["reproduction_metadata"]
    expected = meta["expected_numbers"]
    tolerances = meta.get("tolerance_pct") or DEFAULT_TOLERANCES

    print_info(f"Package: {package_path}")
    print_info(f"  source run: {meta.get('source_run_id')}")
    print_info(f"  commit_sha: {meta.get('commit_sha')}")
    print_info(f"  mode: {meta.get('pipeline_mode')}")
    print_info(f"  metrics recorded: {len(expected)}")

    # F3: commit drift means the code path measured is not the code path
    # the package claims. Exit 14 (requirement unmet) unless the caller opts in.
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
            raise typer.Exit(ExitCode.REQUIREMENT_UNMET)  # reproduce.commit_drift

    try:
        config_file = _resolve_config_path(package, config_override, package_path)
    except ReproduceError as e:
        print_error(str(e))
        raise typer.Exit(ExitCode.USAGE) from None
    print_info(f"  config: {config_file}")

    # Refuse before a multi-hour run that would be refused afterwards, and
    # before the pre-run destroy: a config that deploy and run would refuse
    # (no name, a removed key) must not reach that destroy.
    from lakebench.config import ConfigError, LoadPurpose, load_config

    try:
        _iterations = load_config(
            config_file, purpose=LoadPurpose.RUN
        ).architecture.benchmark.iterations
    except ConfigError as e:
        print_error(str(e))
        raise typer.Exit(ExitCode.USAGE) from None
    _mismatch = _sample_mismatch(meta, _iterations)
    if _mismatch:
        print_error(_mismatch)
        raise typer.Exit(ExitCode.USAGE)
    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID

    _mismatch = _policy_refusal(meta, MAINTENANCE_POLICY_ID)
    if _mismatch:
        print_error(_mismatch)
        raise typer.Exit(ExitCode.USAGE)
    if not meta.get("experiment_identity"):
        from lakebench.metrics.experiment import NO_PROVENANCE

        print_error(
            f"The package cannot be verified: {NO_PROVENANCE} (it was recorded before "
            "packages carried an experiment identity); record it again from a current run."
        )
        raise typer.Exit(ExitCode.USAGE)

    if dry_run:
        print_warning("--dry-run set: package validation only, no pipeline run")
        console.print()
        console.print(Panel("[green]Package parsed cleanly[/green]", title="Dry run", expand=False))
        return

    refusals: list[Any] = []
    try:
        metrics = _run_pipeline(config_file, timeout, keep, refusals)
    except ReproduceError as e:
        # The pipeline did not run cleanly, or its run could not be found.
        print_error(str(e))
        raise typer.Exit(ExitCode.FAILED) from None

    _mismatch = (
        _sample_mismatch(meta, _benchmark_samples(metrics))
        or _policy_refusal(meta, _run_maintenance_policy(metrics))
        or _experiment_refusal(meta, metrics)
    )
    if _mismatch:
        # The run that just finished does not match the package's protocol.
        print_error(_mismatch)
        _post_destroy_refusal(refusals)
        raise typer.Exit(ExitCode.REQUIREMENT_UNMET)

    actual = _measure_actual_numbers(metrics)
    rows, outcome = _compare(
        expected, actual, tolerances, (meta.get("query_set_id"), _run_query_set(metrics))
    )
    _print_comparison(rows, outcome)
    _post_destroy_refusal(refusals)

    if outcome != 0:
        # _compare's verdict is 2 (correctness) or 1 (performance); both are
        # a reproduction outside its tolerance, 14 in the exit-code table.
        raise typer.Exit(ExitCode.REQUIREMENT_UNMET)


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
                "reproduce never destroys before the run: it refuses an "
                "existing namespace or bucket, and destroys only what it created."
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
            raise typer.Exit(ExitCode.USAGE)
        if package is not None:
            print_error("Positional PACKAGE cannot be combined with --record")
            raise typer.Exit(ExitCode.USAGE)
        _record(record, write, config_reference)
        return

    # Verify mode -- positional package required.
    if package is None:
        print_error("Verify mode requires a PACKAGE path (or use --record/--write)")
        raise typer.Exit(ExitCode.USAGE)

    _verify(
        package_path=package,
        config_override=config_override,
        timeout=timeout,
        keep=keep,
        dry_run=dry_run,
        allow_commit_drift=allow_commit_drift,
    )
