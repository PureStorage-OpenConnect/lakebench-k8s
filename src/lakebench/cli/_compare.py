"""Compare command for Lakebench CLI.

Runs two configurations sequentially and produces a side-by-side
comparison of their benchmark results.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from lakebench._constants import DEFAULT_OUTPUT_DIR
from lakebench.cli._helpers import EXIT_DECLINED, emit_data, err_console, esc, print_error
from lakebench.config import LoadPurpose, load_config

logger = logging.getLogger(__name__)

console = Console()


def compare(
    config_a: Annotated[Path, typer.Argument(help="First configuration file", exists=True)],
    config_b: Annotated[Path, typer.Argument(help="Second configuration file", exists=True)],
    keep: Annotated[
        bool,
        typer.Option("--keep", help="Keep deployments after comparison (do not destroy)"),
    ] = False,
    scale: Annotated[
        float | None,
        typer.Option("--scale", help="Override scale for both configs"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write comparison report to file"),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: table, json, csv"),
    ] = "table",
    skip_benchmark: Annotated[
        bool,
        typer.Option("--skip-benchmark", help="Skip benchmark phase"),
    ] = False,
    timeout: Annotated[
        int,
        typer.Option("--timeout", help="Per-run timeout in seconds"),
    ] = 7200,
    local: Annotated[
        bool,
        typer.Option("--local", help="Run both configs locally with podman/docker"),
    ] = False,
    generate: Annotated[
        bool,
        typer.Option("--generate", help="Generate data before each run"),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmations"),
    ] = False,
) -> None:
    """Compare two configurations side-by-side.

    Runs each configuration through the full pipeline sequentially,
    then displays a comparison of their benchmark results.

    With --local, both configs run on this host via podman/docker. Each is
    deployed, run, and torn down in turn rather than side by side, so the two
    do not contend for cores -- a comparison where one config ran against the
    other's load would measure the contention, not the configs.
    """
    from lakebench.config import ConfigError

    # --scale here would rewrite only the in-memory configs; the subprocess
    # `run` invocations reload from disk and would benchmark at whatever the
    # file says, contradicting the displayed plan. The option stays parked
    # until it is wired end-to-end. Refuse loudly rather than silently mislead.
    if scale is not None:
        print_error(
            "--scale is not supported by `compare`. Edit "
            "architecture.workload.datagen.scale in each config file so the "
            "subprocess runs see the scale you asked for."
        )
        raise typer.Exit(2)

    # Unknown --format used to silently fall back to JSON via
    # _save_comparison's else branch. `report --render` is the HTML path;
    # `compare` writes JSON, CSV, or the terminal table. Any other value
    # (including typos like 'htlm') is refused loudly.
    _SUPPORTED_FORMATS = {"table", "json", "csv"}
    _requested_format = output_format.lower()
    if _requested_format == "html":
        print_error(
            "--format html is not supported by `compare`. Use "
            "`lakebench report --render` to build an HTML report from a run's "
            "metrics.json (or --format json / --format csv here)."
        )
        raise typer.Exit(2)
    if _requested_format not in _SUPPORTED_FORMATS:
        print_error(
            f"--format {output_format!r} is not supported. "
            f"Use one of: {', '.join(sorted(_SUPPORTED_FORMATS))}."
        )
        raise typer.Exit(2)

    # Load both configs. compare still deploys, runs and destroys each config,
    # so it loads them as MUTATE: a nameless config is refused here rather
    # than reaching the destroy step under a resolved name. LoadPurpose.COMPARE
    # is for the read-only compare over stored records.
    try:
        cfg_a = load_config(config_a, purpose=LoadPurpose.MUTATE)
        cfg_b = load_config(config_b, purpose=LoadPurpose.MUTATE)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1) from None

    # With --format json|csv and no -o, stdout carries only the data: every
    # Panel, header and the child runs' own output go to stderr.
    machine = _requested_format != "table" and output is None
    human = err_console if machine else console
    child_stdout = sys.stderr if machine else None

    # Different workloads, corpora, seeds, scales or modes are different
    # experiments; say so before hours are spent running them. The runs still
    # happen (the evidence is shown), and the command exits 1 at the end.
    differences = config_identity_differences(cfg_a, cfg_b)
    if differences:
        human.print(
            Panel(
                "[bold red]NOT COMPARABLE[/bold red]: the two configs describe different "
                "experiments, so their performance numbers will not be compared.\n  "
                + "\n  ".join(differences),
                border_style="red",
            )
        )
    conditions = config_condition_differences(cfg_a, cfg_b)
    if conditions:
        human.print(
            Panel(
                "[bold yellow]Not like-for-like[/bold yellow]: the configs will execute under "
                "different conditions; matching results will be shown as comparable, not "
                "like-for-like.\n  " + "\n  ".join(conditions),
                border_style="yellow",
            )
        )

    # Show comparison plan
    where = "local (podman/docker)" if local else "Kubernetes"
    human.print(
        Panel(
            f"[bold]Comparing two configurations:[/bold]\n\n"
            f"  A: [cyan]{esc(cfg_a.name)}[/cyan] ({esc(config_a)})\n"
            f"     Recipe: {esc(_recipe_summary(cfg_a))}\n"
            f"     Scale: {esc(cfg_a.architecture.workload.datagen.scale)}\n\n"
            f"  B: [cyan]{esc(cfg_b.name)}[/cyan] ({esc(config_b)})\n"
            f"     Recipe: {esc(_recipe_summary(cfg_b))}\n"
            f"     Scale: {esc(cfg_b.architecture.workload.datagen.scale)}\n\n"
            f"  Target: {esc(where)}, run one after the other\n",
            title="lakebench compare",
            border_style="blue",
        )
    )

    if not yes:
        confirm = typer.confirm("Proceed with comparison?", err=machine)
        if not confirm:
            raise typer.Exit(EXIT_DECLINED)

    if local and cfg_a.name == cfg_b.name:
        # Local stacks are keyed by config name: same name means the same
        # workdir, the same Garage container, and the same buckets, so B would
        # run against A's data and the comparison would be meaningless.
        print_error(
            f"Both configs are named '{cfg_a.name}'. "
            "Local comparison needs distinct names -- they key the workdir, "
            "the container, and the buckets."
        )
        raise typer.Exit(1)

    # Run A
    human.print()
    human.print(f"[bold]== Running configuration A: {esc(cfg_a.name)} ==[/bold]")
    metrics_a = _run_single(
        config_a, timeout, skip_benchmark, keep, local, generate, child_stdout=child_stdout
    )

    # Run B
    human.print()
    human.print(f"[bold]== Running configuration B: {esc(cfg_b.name)} ==[/bold]")
    metrics_b = _run_single(
        config_b, timeout, skip_benchmark, keep, local, generate, child_stdout=child_stdout
    )

    # Destroy failures were captured by _run_single; surface them before
    # building the comparison so they cannot be hidden by a nice-looking
    # table, and set exit non-zero regardless of the comparison verdict.
    destroy_failures: list[str] = []
    for label, m in (("A", metrics_a), ("B", metrics_b)):
        if isinstance(m, dict):
            err = m.pop("_destroy_error", None)
            if err:
                human.print(f"[red]Destroy for run {esc(label)} failed: {esc(err)}[/red]")
                destroy_failures.append(label)

    # Build comparison
    comparison = _build_comparison(cfg_a.name, metrics_a, cfg_b.name, metrics_b)

    # Display. JSON and CSV without -o are machine output on plain stdout;
    # every notice below goes to stderr so the stream stays parseable.
    if _requested_format == "table":
        _print_comparison_table(comparison)
    elif output is None:
        emit_data(_comparison_text(comparison, _requested_format))

    # Save if requested
    if output:
        _save_comparison(comparison, output, _requested_format)

    # Save to standard output directory
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    compare_dir = Path(DEFAULT_OUTPUT_DIR) / "comparisons" / f"compare-{ts}"
    compare_dir.mkdir(parents=True, exist_ok=True)
    with open(compare_dir / "comparison.json", "w") as f:
        json.dump(comparison, f, indent=2)
    err_console.print(f"\n[dim]Comparison saved to {esc(compare_dir)}[/dim]")
    if destroy_failures:
        # A destroy failure leaves cluster or bucket state behind that the
        # next run of the same config will collide with; surfacing this via
        # exit code is what a UAT script or CI job will actually notice.
        print_error(
            f"compare: destroy failed for {', '.join(destroy_failures)}; see messages above."
        )
        raise typer.Exit(1)
    if (
        comparison.get("verdict", "not_comparable" if comparison.get("comparable") is False else "")
        == "not_comparable"
    ):
        # Owner decision 2026-09-26: a non-comparable pair exits non-zero.
        # A pair whose comparability is not established exits 0: nothing
        # shows it differs.
        raise typer.Exit(1)


def config_identity_differences(cfg_a, cfg_b) -> list[str]:
    """Why two configs cannot produce comparable runs (experiment identity),
    known before either runs. The generator digest is only known after."""
    from lakebench.metrics.experiment import identity_differences, planned_experiment

    return identity_differences(planned_experiment(cfg_a), planned_experiment(cfg_b))


def config_condition_differences(cfg_a, cfg_b) -> list[str]:
    """Execution conditions two configs will run under differently (planned
    effective maintenance, query access path): not like-for-like."""
    from lakebench.metrics.experiment import condition_differences, planned_experiment

    return condition_differences(planned_experiment(cfg_a), planned_experiment(cfg_b))


# Repeated local benchmarks on unchanged data measured 0.9% spread (n=5,
# stdev 1.8 QpH on a mean of 472.9), with every query under 1% coefficient of
# variation. 2% leaves headroom over that without hiding real differences.
_NOISE_FLOOR_PCT = 2.0

# Scores where a bigger number is the better result. Everything else -- times,
# sizes, staleness -- is better when smaller.
_HIGHER_IS_BETTER = (
    "qph",
    "throughput",
    "efficiency",
    "rows_processed",
    "ingest_ratio",
)

# Scores that describe the run rather than rate it. A difference here is
# information, not a win or a loss: scale_ratio is best at 1.0 in either
# direction, and the data volume is an input, not a result.
_NEUTRAL = ("scale_ratio", "total_data_processed_gb", "total_s3_objects", "composite_qph_rounds")


def _higher_is_better(metric: str) -> bool:
    """Whether an increase in this score is an improvement.

    Getting this backwards paints a faster engine red, so the default is the
    conservative one: unknown metrics are treated as lower-is-better, matching
    the times and sizes that make up most of the scorecard.
    """
    return any(token in metric.lower() for token in _HIGHER_IS_BETTER)


def _is_neutral(metric: str) -> bool:
    """Whether a change in this score is neither better nor worse."""
    return metric.lower() in _NEUTRAL


def _recipe_summary(cfg) -> str:
    """Build a short recipe description string."""
    arch = cfg.architecture
    return (
        f"{arch.catalog.type.value}-{arch.table_format.type.value}"
        f"-spark-{arch.query_engine.type.value}"
    )


def _run_single(
    config_path: Path,
    timeout: int,
    skip_benchmark: bool,
    keep: bool,
    local: bool = False,
    generate: bool = False,
    child_stdout: Any = None,
) -> dict[str, Any]:
    """Run a single configuration through deploy -> generate -> run -> destroy.

    Returns the metrics dict, or an error dict if the run failed.
    ``child_stdout`` redirects the child commands' stdout (to stderr when the
    caller's stdout carries machine output).
    """
    import subprocess
    import sys

    # Local mode refuses to run without an existing stack, so deploy first.
    # The cluster path does its own deploy inside `run`.
    if local:
        deploy = subprocess.run(
            [sys.executable, "-m", "lakebench", "deploy", str(config_path), "--local", "--yes"],
            stdout=child_stdout,
            timeout=900,
        )
        if deploy.returncode != 0:
            return {"error": f"Local deploy failed with exit code {deploy.returncode}"}

    cmd = [sys.executable, "-m", "lakebench", "run", str(config_path), "--timeout", str(timeout)]
    if skip_benchmark:
        cmd.append("--skip-benchmark")
    if local:
        cmd.append("--local")
    if generate:
        cmd.append("--generate")

    try:
        result = subprocess.run(cmd, stdout=child_stdout, timeout=timeout + 300)
        if result.returncode != 0:
            return {"error": f"Run failed with exit code {result.returncode}"}
    except subprocess.TimeoutExpired:
        return {"error": f"Run timed out after {timeout + 300}s"}

    # Load metrics before destroying: the local teardown removes the workdir,
    # and reading after that would race the deletion.
    metrics = _load_latest_metrics(config_path)

    # Destroy unless --keep
    if not keep:
        destroy_cmd = [sys.executable, "-m", "lakebench", "destroy", str(config_path), "--force"]
        if local:
            # Without --remove-data the buckets survive, and the next run of a
            # config with the same bucket names would read stale data.
            destroy_cmd += ["--local", "--remove-data"]
        try:
            destroy_result = subprocess.run(
                destroy_cmd,
                capture_output=True,
                timeout=600,
            )
            if destroy_result.returncode != 0:
                # Surface it: a silent destroy failure leaves an orphan
                # namespace (or bucket) that a later comparison will collide
                # with. The metrics are still returned so the comparison can
                # be built, but the compare exits non-zero at the end.
                stderr = destroy_result.stderr or b""
                if isinstance(stderr, bytes):
                    stderr_text = stderr.decode(errors="replace")
                else:
                    stderr_text = str(stderr)
                metrics = dict(metrics) if isinstance(metrics, dict) else {"error": str(metrics)}
                metrics["_destroy_error"] = (
                    f"destroy exited {destroy_result.returncode}: "
                    f"{stderr_text.strip()[:800] or 'no stderr'}"
                )
        except subprocess.TimeoutExpired:
            metrics = dict(metrics) if isinstance(metrics, dict) else {"error": str(metrics)}
            metrics["_destroy_error"] = "destroy timed out after 600s"

    return metrics


def _load_latest_metrics(config_path: Path) -> dict[str, Any]:
    """Find and load the most recent metrics.json for a config."""
    from lakebench.config import LoadPurpose, load_config
    from lakebench.metrics.storage import MetricsStorage

    try:
        cfg = load_config(config_path, purpose=LoadPurpose.MUTATE)
        storage = MetricsStorage()
        # list_runs() is already newest-first. Iterating it in reverse would
        # return the config's *first ever* run rather than the one just
        # executed, which is a silent wrong answer rather than an error.
        runs = storage.list_runs()
        for run_info in runs:
            run_dir = storage.run_dir(run_info["run_id"])
            metrics_file = run_dir / "metrics.json"
            if metrics_file.exists():
                with open(metrics_file) as f:
                    data = json.load(f)
                if data.get("deployment_name") == cfg.name:
                    return data
        return {"error": "No metrics found after run"}
    except Exception as e:
        return {"error": f"Failed to load metrics: {e}"}


def _samples_per_query(metrics: dict) -> int | None:
    """Samples per query behind a run's QpH; 1 for records without samples."""
    from lakebench.benchmark.spread import samples_per_query

    if "error" in metrics:
        return None
    qb = (metrics.get("pipeline_benchmark") or {}).get("query_benchmark") or metrics.get(
        "benchmark"
    )
    return samples_per_query((qb or {}).get("queries") or [])


_RENAMED_SCORES = {"query_time_freshness_seconds": "query_time_event_age_seconds"}


def _renamed_scores(scores: dict) -> dict:
    """Score keys under their current names (old metrics.json files)."""
    out = dict(scores)
    for old, new in _RENAMED_SCORES.items():
        if old in out:
            value = out.pop(old)
            out.setdefault(new, value)
    return out


def _build_comparison(
    name_a: str,
    metrics_a: dict,
    name_b: str,
    metrics_b: dict,
) -> dict[str, Any]:
    """Build a structured comparison dict from two metric sets.

    When the runs are different experiments or returned different benchmark
    results (metrics.experiment.refusals), ``comparable`` is False, the
    reasons are under ``refusals``, and every row is marked
    ``not_comparable``: the raw numbers stay visible, but no delta or winner
    is derived from them.
    """
    scores_a = (
        metrics_a.get("pipeline_benchmark", {}).get("scores", metrics_a.get("scorecard", {}))
        if "error" not in metrics_a
        else {}
    )
    scores_b = (
        metrics_b.get("pipeline_benchmark", {}).get("scores", metrics_b.get("scorecard", {}))
        if "error" not in metrics_b
        else {}
    )

    # Pre-v1.6 runs wrote the gold event-date age as a freshness figure.
    scores_a, scores_b = _renamed_scores(scores_a), _renamed_scores(scores_b)

    # Collect all score keys from both
    all_keys = sorted(set(list(scores_a.keys()) + list(scores_b.keys())))

    # QpH is per query set: refuse to compare it across different sets.
    from lakebench.benchmark.queries import qph_comparable

    qs_a, qs_b = _query_set(metrics_a), _query_set(metrics_b)
    qph_ok, qph_reason = qph_comparable(qs_a, qs_b)
    refused = []
    if not qph_ok:
        refused = [k for k in all_keys if "qph" in k.lower()]
        all_keys = [k for k in all_keys if k not in refused]

    from lakebench.metrics.experiment import (
        experiment_of,
        like_for_like,
        results_established,
        support_of,
    )
    from lakebench.metrics.experiment import refusals as experiment_refusals

    provenance_refusals: list[str] = []
    result_refusals: list[str] = []
    result_notes: list[str] = []
    conditions: list[str] = []
    if "error" not in metrics_a and "error" not in metrics_b:
        provenance_refusals, result_refusals, result_notes = experiment_refusals(
            metrics_a, metrics_b
        )
        conditions = like_for_like(metrics_a, metrics_b)
    else:
        # A run that failed has no evidence to compare (invariant 1).
        for label, m in (("A", metrics_a), ("B", metrics_b)):
            if "error" in m:
                provenance_refusals.append(f"run {label} did not complete: {m['error']}")

    # A run whose verdict is FAILED (OD-6) cannot stand for a comparison
    # even when its raw ``success`` flag is True (LB-044 shape: the CLI
    # exited 0, silver never kept pace with the trickle, and no comparable
    # evidence was produced). Legacy v1.5 records with no verdict block
    # fall back to raw ``success``.
    from lakebench.metrics.verdict import passed as _record_passed

    for label, m in (("A", metrics_a), ("B", metrics_b)):
        if "error" in m:
            continue
        if not _record_passed(m):
            provenance_refusals.append(
                f"run {label} did not pass its verdict; a FAILED run has no evidence to compare"
            )
    # Three verdicts. not_comparable: different experiments or different
    # results. not_established: nothing contradicts the pair, but at least
    # one side has no checked results (continuous, --skip-benchmark, a
    # recipe without a query engine), so equivalence is not shown either.
    # comparable: results shown equivalent.
    unestablished: list[str] = []
    if not provenance_refusals and not result_refusals:
        for label, m in (("A", metrics_a), ("B", metrics_b)):
            why = results_established(experiment_of(m))
            if why is not True:
                unestablished.append(f"run {label}: {why}")
    if provenance_refusals or result_refusals:
        verdict = "not_comparable"
    elif unestablished:
        verdict = "not_established"
    else:
        verdict = "comparable"
    comparable = verdict == "comparable"

    rows = []
    for key in all_keys:
        val_a = scores_a.get(key)
        val_b = scores_b.get(key)
        row: dict[str, Any] = {"metric": key, "config_a": val_a, "config_b": val_b}
        if not comparable:
            row["not_comparable"] = True
        rows.append(row)

    warnings = []
    n_a, n_b = _samples_per_query(metrics_a), _samples_per_query(metrics_b)
    if n_a is not None and n_b is not None and n_a != n_b:
        # Warn, not refuse: compare runs two configs the user chose, and the
        # sample count may be the thing being compared. The QpH rows are
        # still different estimators, which the table has to say.
        warnings.append(
            f"QpH for A is the median of {n_a} sample(s) per query and for B of {n_b}; "
            "the QpH rows compare different estimators (set the same "
            "architecture.benchmark.iterations in both configs)"
        )

    r_a, r_b = scores_a.get("composite_qph_rounds"), scores_b.get("composite_qph_rounds")
    if r_a is not None and r_b is not None and r_a != r_b:
        # Also a condition difference (benchmark rounds): not like-for-like.
        warnings.append(
            f"continuous QpH for A is the median of {r_a} in-stream round(s) and for B "
            f"of {r_b}; the QpH rows compare medians over different numbers of rounds"
        )

    if "error" not in metrics_a and "error" not in metrics_b:
        from lakebench.metrics.maintenance_policy import policy_mismatch, recorded_policy

        # Warn, not refuse, for the same reason as the sample count; the perf
        # gate and reproduce refuse.
        policy_problem = policy_mismatch(recorded_policy(metrics_a), recorded_policy(metrics_b))
        if policy_problem:
            warnings.append(policy_problem)

    warnings.extend(result_notes)

    # Caps that bound each side, from the experiment block. Delta rendering
    # uses these to render `capped` instead of a bogus winner when a
    # Lakebench cap held either run.
    def _caps_of(m: dict) -> list[str]:
        exp = experiment_of(m) or {}
        limits = exp.get("limits", {}) or {}
        bound = limits.get("bound", []) or []
        return list(bound) if isinstance(bound, list) else []

    caps_bound_a = _caps_of(metrics_a)
    caps_bound_b = _caps_of(metrics_b)

    return {
        "timestamp": datetime.now().isoformat(),
        "verdict": verdict,
        "comparable": comparable,
        "not_established": unestablished,
        # DESIGN 6.5: comparable pairs whose effective execution conditions
        # differ are shown with those differences and not called like-for-like.
        "like_for_like": comparable and not conditions,
        "condition_differences": conditions,
        "support": {"config_a": support_of(metrics_a), "config_b": support_of(metrics_b)},
        "caps_bound_a": caps_bound_a,
        "caps_bound_b": caps_bound_b,
        "refusals": {"provenance": provenance_refusals, "results": result_refusals},
        "warnings": warnings,
        "config_a": {
            "name": name_a,
            "error": metrics_a.get("error"),
            "run_id": metrics_a.get("run_id"),
        },
        "config_b": {
            "name": name_b,
            "error": metrics_b.get("error"),
            "run_id": metrics_b.get("run_id"),
        },
        "noise_floor_pct": _NOISE_FLOOR_PCT,
        "query_sets": {"config_a": qs_a, "config_b": qs_b},
        "qph_comparable": qph_ok,
        "qph_refused": {"metrics": refused, "reason": qph_reason} if refused else None,
        "metrics": rows,
    }


def _query_set(metrics: dict) -> str | None:
    """The query-set id a run's QpH was measured over, or None."""
    if not isinstance(metrics, dict) or "error" in metrics:
        return None
    from lakebench.benchmark.queries import legacy_query_set_id

    bench = metrics.get("benchmark") or {}
    qb = (metrics.get("pipeline_benchmark") or {}).get("query_benchmark") or {}
    for b in (bench, qb):
        if b.get("query_set_id"):
            return b["query_set_id"]
    # A run that predates query-set ids: the pinned historical id of its
    # query-name set, or "unknown" (never today's SQL, which it may not have
    # run).
    for b in (bench, qb):
        if b.get("queries"):
            return legacy_query_set_id(b["queries"], metrics.get("start_time"))
    return None


def _print_comparison_table(comparison: dict) -> None:
    """Print a Rich comparison table."""
    name_a = comparison["config_a"]["name"]
    name_b = comparison["config_b"]["name"]

    if comparison["config_a"].get("error"):
        console.print(
            f"[red]Config A ({esc(name_a)}) failed: {esc(comparison['config_a']['error'])}[/red]"
        )
    if comparison["config_b"].get("error"):
        console.print(
            f"[red]Config B ({esc(name_b)}) failed: {esc(comparison['config_b']['error'])}[/red]"
        )

    for warning in comparison.get("warnings") or []:
        console.print(f"[yellow]Warning: {esc(warning)}[/yellow]")

    refused = comparison.get("refusals") or {}
    reasons = []
    if refused.get("provenance"):
        reasons.append("the runs are different experiments:")
        reasons.extend(f"  {r}" for r in refused["provenance"])
    if refused.get("results"):
        reasons.append("the benchmark queries returned results not shown equal:")
        reasons.extend(f"  {r}" for r in refused["results"])
    verdict = comparison.get("verdict") or (
        "comparable" if comparison.get("comparable") is not False else "not_comparable"
    )
    not_comparable = verdict == "not_comparable"
    if verdict == "not_established":
        console.print(
            Panel(
                "[bold yellow]COMPARABILITY NOT ESTABLISHED[/bold yellow]: no checked benchmark "
                "results on both sides, so nothing shows the two runs did equivalent work. The "
                "raw numbers are shown; no deltas or winner.\n"
                + "\n".join(f"  {esc(r)}" for r in comparison.get("not_established") or []),
                border_style="yellow",
            )
        )
    elif not_comparable:
        console.print(
            Panel(
                "[bold red]NOT COMPARABLE[/bold red]: the numbers below measure different "
                "work, so no deltas are shown.\n" + "\n".join(reasons),
                border_style="red",
            )
        )
    elif comparison.get("condition_differences"):
        console.print(
            Panel(
                "[bold yellow]COMPARABLE, NOT LIKE-FOR-LIKE[/bold yellow]: the benchmark "
                "results match, but the runs executed under different conditions, so a "
                "difference below may come from those rather than the architecture.\n"
                + "\n".join(f"  {esc(d)}" for d in comparison["condition_differences"]),
                border_style="yellow",
            )
        )
    support = comparison.get("support") or {}
    if support:
        a_state = support.get("config_a", "unknown")
        b_state = support.get("config_b", "unknown")
        meaning = {
            "supported": "validated on the release tree",
            "unverified": "valid, not release-validated: not proof the combination is supported",
            "unsupported": "outside the supported set",
            "unknown": "the record carries no support state",
        }
        explained = "; ".join(
            f"{s}: {meaning.get(s, s)}" for s in dict.fromkeys((a_state, b_state))
        )
        console.print(
            f"Support state: A {esc(a_state)}, B {esc(b_state)} [dim]({esc(explained)})[/dim]"
        )
    if not_comparable:
        title = "Comparison Results -- NOT COMPARABLE"
    elif verdict == "not_established":
        title = "Comparison Results -- comparability not established"
    elif comparison.get("condition_differences"):
        title = "Comparison Results -- comparable, not like-for-like"
    else:
        title = "Comparison Results"
    table = Table(title=title, show_header=True, header_style="bold")
    table.add_column("Metric", style="cyan")
    table.add_column(name_a, justify="right")
    table.add_column(name_b, justify="right")
    table.add_column("Delta", justify="right")

    from lakebench.reports.formatter import (
        DELTA_TOKEN_A_FASTER,
        DELTA_TOKEN_B_FASTER,
        DELTA_TOKEN_CAPPED,
        DELTA_TOKEN_OVERLAP,
        DELTA_TOKEN_WITHHELD,
        delta_token,
    )

    # WCAG 1.4.1: pass/fail and winner/loser are not encoded by colour alone.
    # Each delta cell carries an ASCII glyph and a text token next to the
    # percentage; a screen-reader or a copy-paste of the text still shows
    # which side won, without relying on the red/green pill.
    _TOKEN_GLYPH = {
        DELTA_TOKEN_A_FASTER: "<",
        DELTA_TOKEN_B_FASTER: ">",
        DELTA_TOKEN_OVERLAP: "=",
        DELTA_TOKEN_WITHHELD: "x",
        DELTA_TOKEN_CAPPED: "!",
    }
    # A Lakebench cap on either side makes the comparison unsafe to
    # attribute; the token pipes that through to the delta rendering so
    # the caller sees "capped" instead of a bogus winner.
    _capped_either_side = bool(comparison.get("caps_bound_a")) or bool(
        comparison.get("caps_bound_b")
    )
    for row in comparison["metrics"]:
        val_a = row["config_a"]
        val_b = row["config_b"]
        delta = ""
        if (
            isinstance(val_a, (int, float))
            and isinstance(val_b, (int, float))
            and not isinstance(val_a, bool)
            and not isinstance(val_b, bool)
            and val_a != 0
        ):
            pct = ((val_b - val_a) / abs(val_a)) * 100
            within_noise = abs(pct) < _NOISE_FLOOR_PCT or _is_neutral(row["metric"])
            token = delta_token(
                higher_is_better=_higher_is_better(row["metric"]),
                pct=pct,
                within_noise=within_noise,
                capped=_capped_either_side,
            )
            glyph = _TOKEN_GLYPH.get(token, "=")
            if token == DELTA_TOKEN_CAPPED:
                # A capped delta is not attributable; render as yellow so
                # the reader does not read the number as a win.
                delta = f"[yellow]{glyph} {token} {pct:+.1f}%[/yellow]"
            elif within_noise:
                # Measured spread on an idle host is ~1%. Colouring a smaller
                # difference green or red claims a result the run cannot
                # support, and neutral scores have no better direction.
                delta = f"[dim]{glyph} {token} {pct:+.1f}%[/dim]"
            else:
                better = "green" if _higher_is_better(row["metric"]) == (pct > 0) else "red"
                delta = f"[{better}]{glyph} {token} {pct:+.1f}%[/{better}]"

        if row.get("not_comparable"):
            token = DELTA_TOKEN_WITHHELD
            glyph = _TOKEN_GLYPH[token]
            delta = (
                f"[yellow]{glyph} {token} not established[/yellow]"
                if verdict == "not_established"
                else f"[red]{glyph} {token} not comparable[/red]"
            )
        table.add_row(
            row["metric"],
            _fmt(val_a),
            _fmt(val_b),
            delta,
        )

    console.print(table)
    refused = comparison.get("qph_refused")
    if refused:
        console.print(
            f"[yellow]QpH not compared ({esc(', '.join(refused['metrics']))}): "
            f"{esc(refused['reason'])}. QpH is queries per hour over one query set.[/yellow]"
        )
    console.print(
        f"[dim]Delta is B relative to A. Differences under {_NOISE_FLOOR_PCT:g}% are within "
        "measured run-to-run spread and are not coloured.[/dim]"
    )


def _fmt(val: Any) -> str:
    """Format a value for table display."""
    if val is None:
        return "[dim]--[/dim]"
    if isinstance(val, float):
        return f"{val:,.2f}"
    if isinstance(val, bool):
        return str(val)
    if isinstance(val, int):
        return f"{val:,}"
    return str(val)


def _comparison_text(comparison: dict, fmt: str) -> str:
    """The comparison as JSON, or as CSV when *fmt* is ``csv``."""
    if fmt != "csv":
        return json.dumps(comparison, indent=2)
    import csv
    import io

    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=["metric", "config_a", "config_b", "comparable", "like_for_like"]
    )
    writer.writeheader()
    for row in comparison["metrics"]:
        writer.writerow(
            {
                "metric": row["metric"],
                "config_a": row["config_a"],
                "config_b": row["config_b"],
                "comparable": not row.get("not_comparable"),
                "like_for_like": bool(comparison.get("like_for_like")),
            }
        )
    return buf.getvalue()


def _save_comparison(comparison: dict, path: Path, fmt: str) -> None:
    """Save comparison to file in the requested format (JSON unless ``csv``)."""
    with open(path, "w", newline="") as f:
        f.write(_comparison_text(comparison, fmt))
    err_console.print(f"[green]Comparison saved to {esc(path)}[/green]")
