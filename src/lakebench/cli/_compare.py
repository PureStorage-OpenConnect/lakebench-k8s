"""Compare command for Lakebench CLI.

Runs two configurations sequentially and produces a side-by-side
comparison of their benchmark results.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from lakebench._constants import DEFAULT_OUTPUT_DIR
from lakebench.config import load_config

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
        int | None,
        typer.Option("--scale", help="Override scale for both configs"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write comparison report to file"),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: table, json, csv, html"),
    ] = "table",
    skip_benchmark: Annotated[
        bool,
        typer.Option("--skip-benchmark", help="Skip benchmark phase"),
    ] = False,
    timeout: Annotated[
        int,
        typer.Option("--timeout", help="Per-run timeout in seconds"),
    ] = 7200,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmations"),
    ] = False,
) -> None:
    """Compare two configurations side-by-side.

    Runs each configuration through the full pipeline sequentially,
    then displays a comparison of their benchmark results.
    """
    from lakebench.config import ConfigError

    # Load both configs
    try:
        cfg_a = load_config(config_a)
        cfg_b = load_config(config_b)
    except ConfigError as e:
        console.print(f"[red]Config error: {e}[/red]")
        raise typer.Exit(1) from None

    # Apply scale override
    if scale is not None:
        cfg_a.architecture.workload.datagen.scale = scale
        cfg_b.architecture.workload.datagen.scale = scale

    # Show comparison plan
    console.print(
        Panel(
            f"[bold]Comparing two configurations:[/bold]\n\n"
            f"  A: [cyan]{cfg_a.name}[/cyan] ({config_a})\n"
            f"     Recipe: {_recipe_summary(cfg_a)}\n"
            f"     Scale: {cfg_a.architecture.workload.datagen.scale}\n\n"
            f"  B: [cyan]{cfg_b.name}[/cyan] ({config_b})\n"
            f"     Recipe: {_recipe_summary(cfg_b)}\n"
            f"     Scale: {cfg_b.architecture.workload.datagen.scale}\n",
            title="lakebench compare",
            border_style="blue",
        )
    )

    if not yes:
        confirm = typer.confirm("Proceed with comparison?")
        if not confirm:
            raise typer.Exit(0)

    # Run A
    console.print()
    console.print(f"[bold]== Running configuration A: {cfg_a.name} ==[/bold]")
    metrics_a = _run_single(config_a, timeout, skip_benchmark, keep)

    # Run B
    console.print()
    console.print(f"[bold]== Running configuration B: {cfg_b.name} ==[/bold]")
    metrics_b = _run_single(config_b, timeout, skip_benchmark, keep)

    # Build comparison
    comparison = _build_comparison(cfg_a.name, metrics_a, cfg_b.name, metrics_b)

    # Display
    if output_format == "table" or output is None:
        _print_comparison_table(comparison)

    # Save if requested
    if output:
        _save_comparison(comparison, output, output_format)

    # Save to standard output directory
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    compare_dir = Path(DEFAULT_OUTPUT_DIR) / "comparisons" / f"compare-{ts}"
    compare_dir.mkdir(parents=True, exist_ok=True)
    with open(compare_dir / "comparison.json", "w") as f:
        json.dump(comparison, f, indent=2)
    console.print(f"\n[dim]Comparison saved to {compare_dir}[/dim]")


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
) -> dict[str, Any]:
    """Run a single configuration through deploy -> generate -> run -> destroy.

    Returns the metrics dict, or an error dict if the run failed.
    """
    import subprocess

    cmd = ["lakebench", "run", str(config_path), "--timeout", str(timeout)]
    if skip_benchmark:
        cmd.append("--skip-benchmark")

    try:
        result = subprocess.run(cmd, capture_output=False, timeout=timeout + 300)
        if result.returncode != 0:
            return {"error": f"Run failed with exit code {result.returncode}"}
    except subprocess.TimeoutExpired:
        return {"error": f"Run timed out after {timeout + 300}s"}

    # Load the latest metrics
    metrics = _load_latest_metrics(config_path)

    # Destroy unless --keep
    if not keep:
        subprocess.run(
            ["lakebench", "destroy", str(config_path), "--force"],
            capture_output=True,
            timeout=600,
        )

    return metrics


def _load_latest_metrics(config_path: Path) -> dict[str, Any]:
    """Find and load the most recent metrics.json for a config."""
    from lakebench.config import load_config
    from lakebench.metrics.storage import MetricsStorage

    try:
        cfg = load_config(config_path)
        storage = MetricsStorage()
        runs = storage.list_runs()
        # Find runs matching this deployment name
        for run_info in reversed(runs):
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


def _build_comparison(
    name_a: str,
    metrics_a: dict,
    name_b: str,
    metrics_b: dict,
) -> dict[str, Any]:
    """Build a structured comparison dict from two metric sets."""
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

    # Collect all score keys from both
    all_keys = sorted(set(list(scores_a.keys()) + list(scores_b.keys())))

    rows = []
    for key in all_keys:
        val_a = scores_a.get(key)
        val_b = scores_b.get(key)
        rows.append({"metric": key, "config_a": val_a, "config_b": val_b})

    return {
        "timestamp": datetime.now().isoformat(),
        "config_a": {"name": name_a, "error": metrics_a.get("error")},
        "config_b": {"name": name_b, "error": metrics_b.get("error")},
        "metrics": rows,
    }


def _print_comparison_table(comparison: dict) -> None:
    """Print a Rich comparison table."""
    name_a = comparison["config_a"]["name"]
    name_b = comparison["config_b"]["name"]

    if comparison["config_a"].get("error"):
        console.print(f"[red]Config A ({name_a}) failed: {comparison['config_a']['error']}[/red]")
    if comparison["config_b"].get("error"):
        console.print(f"[red]Config B ({name_b}) failed: {comparison['config_b']['error']}[/red]")

    table = Table(title="Comparison Results", show_header=True, header_style="bold")
    table.add_column("Metric", style="cyan")
    table.add_column(name_a, justify="right")
    table.add_column(name_b, justify="right")
    table.add_column("Delta", justify="right")

    for row in comparison["metrics"]:
        val_a = row["config_a"]
        val_b = row["config_b"]
        delta = ""
        if isinstance(val_a, (int, float)) and isinstance(val_b, (int, float)) and val_a != 0:
            pct = ((val_b - val_a) / abs(val_a)) * 100
            color = "green" if pct < 0 else "red" if pct > 0 else "white"
            delta = f"[{color}]{pct:+.1f}%[/{color}]"

        table.add_row(
            row["metric"],
            _fmt(val_a),
            _fmt(val_b),
            delta,
        )

    console.print(table)


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


def _save_comparison(comparison: dict, path: Path, fmt: str) -> None:
    """Save comparison to file in the requested format."""
    if fmt == "json":
        with open(path, "w") as f:
            json.dump(comparison, f, indent=2)
    elif fmt == "csv":
        import csv

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["metric", "config_a", "config_b"])
            writer.writeheader()
            writer.writerows(comparison["metrics"])
    else:
        # Default to JSON for unsupported formats
        with open(path, "w") as f:
            json.dump(comparison, f, indent=2)
    console.print(f"[green]Comparison saved to {path}[/green]")
