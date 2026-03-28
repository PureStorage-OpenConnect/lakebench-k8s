"""Config subcommands for Lakebench CLI.

Provides ``lakebench config show``, ``lakebench config validate``,
``lakebench config recommend``, and ``lakebench config upgrade``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

logger = logging.getLogger(__name__)

config_app = typer.Typer(
    name="config",
    help="Configuration management commands",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

console = Console()


@config_app.command("show")
def config_show(
    config_file: Annotated[
        Path,
        typer.Argument(help="Configuration file path", exists=True),
    ] = Path("lakebench.yaml"),
) -> None:
    """Show fully resolved configuration with source annotations."""
    from lakebench.config import load_config
    from lakebench.config.loader import load_yaml

    try:
        # Load raw YAML to detect which fields are explicitly set
        raw = load_yaml(config_file)
        raw_keys = set(_flatten_keys(raw))

        # Load fully resolved config
        cfg = load_config(config_file)

        console.print(
            Panel(
                f"[bold]Resolved configuration:[/bold] {config_file}",
                border_style="blue",
            )
        )

        # Display key fields with source annotation
        fields = [
            ("name", cfg.name, _source(raw, "name", raw_keys)),
            (
                "recipe",
                raw.get("recipe", "(none -- using field defaults)"),
                _source(raw, "recipe", raw_keys),
            ),
            (
                "namespace",
                cfg.get_namespace(),
                _source(raw, "namespace", raw_keys, "platform.kubernetes.namespace"),
            ),
            (
                "endpoint",
                cfg.platform.storage.s3.endpoint,
                _source(raw, "endpoint", raw_keys, "platform.storage.s3.endpoint"),
            ),
            (
                "catalog",
                cfg.architecture.catalog.type.value,
                "recipe default" if "architecture" not in raw else "from config",
            ),
            (
                "table_format",
                f"{cfg.architecture.table_format.type.value} {cfg.architecture.table_format.iceberg.version if cfg.architecture.table_format.type.value == 'iceberg' else cfg.architecture.table_format.delta.version}",
                "auto-resolved",
            ),
            (
                "query_engine",
                cfg.architecture.query_engine.type.value,
                "recipe default" if "architecture" not in raw else "from config",
            ),
            (
                "pipeline_mode",
                cfg.architecture.pipeline.mode.value,
                _source(raw, "mode", raw_keys, "architecture.pipeline.mode"),
            ),
            (
                "scale",
                str(cfg.architecture.workload.datagen.scale),
                _source(raw, "scale", raw_keys, "architecture.workload.datagen.scale"),
            ),
            (
                "spark_image",
                cfg.images.spark,
                _source(raw, "spark_image", raw_keys, "images.spark"),
            ),
        ]

        table = Table(show_header=True, header_style="bold")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="white")
        table.add_column("Source", style="dim")

        for field_name, value, source in fields:
            table.add_row(field_name, str(value), source)

        console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@config_app.command("validate")
def config_validate(
    config_file: Annotated[
        Path,
        typer.Argument(help="Configuration file path", exists=True),
    ] = Path("lakebench.yaml"),
) -> None:
    """Validate configuration and test connectivity."""
    # Delegate to the existing validate command
    from lakebench.cli import validate as _validate

    _validate(config_file)


@config_app.command("recommend")
def config_recommend(
    config_file: Annotated[
        Path,
        typer.Argument(help="Configuration file path", exists=True),
    ] = Path("lakebench.yaml"),
) -> None:
    """Show sizing guidance for your cluster."""
    from lakebench.cli import recommend as _recommend

    _recommend(config_file)


@config_app.command("upgrade")
def config_upgrade(
    config_file: Annotated[
        Path,
        typer.Argument(help="v1 configuration file to upgrade", exists=True),
    ] = Path("lakebench.yaml"),
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output path (default: overwrite in place)"),
    ] = None,
) -> None:
    """Upgrade a v1.2 config to v2 flat format."""
    import yaml

    from lakebench.config import load_config

    try:
        cfg = load_config(config_file)
    except Exception as e:
        console.print(f"[red]Error loading config: {e}[/red]")
        raise typer.Exit(1) from None

    # Build v2 flat config
    v2: dict = {"name": cfg.name}

    # Extract flat fields from resolved config
    v2["endpoint"] = cfg.platform.storage.s3.endpoint
    v2["access_key"] = cfg.platform.storage.s3.access_key
    v2["secret_key"] = cfg.platform.storage.s3.secret_key
    v2["scale"] = cfg.architecture.workload.datagen.scale

    # Optional fields (only include if non-default)
    ns = cfg.get_namespace()
    if ns != cfg.name:
        v2["namespace"] = ns

    mode = cfg.architecture.pipeline.mode.value
    if mode != "batch":
        v2["mode"] = mode

    cycles = cfg.architecture.pipeline.cycles
    if cycles != 1:
        v2["cycles"] = cycles

    # Recipe detection
    from lakebench.config.recipes import RECIPES

    for recipe_name, recipe_defaults in RECIPES.items():
        if recipe_name == "default":
            continue
        arch = recipe_defaults.get("architecture", {})
        if (
            arch.get("catalog", {}).get("type") == cfg.architecture.catalog.type.value
            and arch.get("table_format", {}).get("type") == cfg.architecture.table_format.type.value
            and arch.get("query_engine", {}).get("type") == cfg.architecture.query_engine.type.value
        ):
            v2["recipe"] = recipe_name
            break

    # Preserve spark conf overrides from the original config
    from lakebench.config.loader import load_yaml

    raw = load_yaml(config_file)
    spark_conf = raw.get("spark", {}).get("conf")
    if spark_conf:
        v2["spark"] = {"conf": spark_conf}

    out_path = output or config_file
    with open(out_path, "w") as f:
        yaml.safe_dump(v2, f, default_flow_style=False, sort_keys=False)

    console.print(f"[green]Upgraded config written to {out_path}[/green]")
    console.print()
    for k, v in v2.items():
        console.print(f"  [cyan]{k}:[/cyan] {v}")


# -- Helpers -----------------------------------------------------------------


def _flatten_keys(d: dict, prefix: str = "") -> list[str]:
    """Flatten a nested dict into dot-separated key paths."""
    keys = []
    for k, v in d.items():
        full = f"{prefix}.{k}" if prefix else k
        keys.append(full)
        if isinstance(v, dict):
            keys.extend(_flatten_keys(v, full))
    return keys


def _source(raw: dict, flat_key: str, raw_keys: set, nested_key: str = "") -> str:
    """Determine the source of a config field value."""
    if flat_key in raw:
        return "from config (flat)"
    if nested_key and nested_key in raw_keys:
        return "from config (nested)"
    return "default"
