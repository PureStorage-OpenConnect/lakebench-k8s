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
    local: Annotated[
        bool,
        typer.Option("--local", help="Validate for local mode instead of Kubernetes"),
    ] = False,
) -> None:
    """Validate configuration and test connectivity."""
    if local:
        _validate_local(config_file)
        return

    # Delegate to the existing validate command
    from lakebench.cli import validate as _validate

    _validate(config_file)


def _validate_local(config_file: Path) -> None:
    """Validate a config for local mode.

    The Kubernetes checks are actively misleading here: they report missing S3
    credentials that Garage mints itself, missing Stackable operators local
    mode never uses, and a missing Spark Operator it does not need. A user
    following that advice would go looking for a cluster.
    """
    from lakebench.cli._local import LocalModeError, check_local_supported, scale_advisory
    from lakebench.config import load_config
    from lakebench.runtime.container import ContainerRuntimeError, detect_container_cli

    console.print()
    console.print(Panel(f"Validating for local mode:\n{config_file}", expand=False))
    console.print()

    passed, failed = 0, 0

    try:
        cfg = load_config(config_file)
        console.print("  [green]+[/green] Config syntax valid")
        passed += 1
    except Exception as e:
        console.print(f"  [red]x[/red] Config invalid: {e}")
        raise typer.Exit(1)  # noqa: B904

    try:
        check_local_supported(cfg)
        console.print("  [green]+[/green] Table format supported locally (Iceberg)")
        passed += 1
    except LocalModeError as e:
        console.print(f"  [red]x[/red] {e}")
        failed += 1

    try:
        cli = detect_container_cli()
        console.print(f"  [green]+[/green] Container runtime available ({cli})")
        passed += 1
    except ContainerRuntimeError as e:
        console.print(f"  [red]x[/red] {e}")
        failed += 1

    advisory = scale_advisory(cfg)
    if advisory:
        console.print(f"  [yellow]![/yellow] {advisory}")
    else:
        scale = cfg.architecture.workload.datagen.scale
        console.print(f"  [green]+[/green] Scale {scale} is sized for one host")
        passed += 1

    console.print()
    if failed:
        console.print(
            Panel(
                f"[red]{passed} passed, {failed} failed[/red]",
                title="Validation Failed",
                expand=False,
            )
        )
        raise typer.Exit(1)

    console.print(
        Panel(
            f"[green]{passed} passed[/green]\n\nRun: lakebench deploy {config_file} --local",
            title="Ready",
            expand=False,
        )
    )


@config_app.command("storage")
def config_storage(
    config_file: Annotated[
        Path,
        typer.Argument(help="Configuration file path", exists=True),
    ] = Path("lakebench.yaml"),
    full: Annotated[
        bool,
        typer.Option(
            "--full/--no-full",
            help=(
                "Create a temporary bucket for write and multipart checks. "
                "Use --no-full when the account cannot create buckets; those "
                "checks are then skipped rather than failed."
            ),
        ),
    ] = True,
) -> None:
    """Validate that the S3 backend supports the operations lakebench needs.

    Runs a set of graded checks against the configured endpoint and reports
    what the backend does. This command never blocks a deployment: it tells
    you whether the store will work and what to expect if it will not.
    """
    from lakebench.config import load_config
    from lakebench.s3 import KNOWN_BACKENDS, CheckStatus, Severity, run_conformance

    try:
        cfg = load_config(config_file)
    except Exception as e:
        console.print(f"[red]Could not load config: {e}[/red]")
        raise typer.Exit(1) from e

    s3 = cfg.platform.storage.s3
    if not s3.endpoint:
        console.print("[red]No S3 endpoint configured.[/red]")
        raise typer.Exit(1)

    console.print(
        Panel(
            f"[bold]Storage conformance[/bold]\nEndpoint: {s3.endpoint}",
            border_style="blue",
        )
    )

    # A bucket to fall back to when create-bucket is not permitted.
    fallback = ""
    try:
        fallback = s3.buckets.bronze or ""
    except Exception:
        pass

    report = run_conformance(
        endpoint=s3.endpoint,
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
        path_style=s3.path_style,
        existing_bucket=fallback,
        allow_create_bucket=full,
    )

    table = Table(show_header=True, header_style="bold")
    table.add_column("Check")
    table.add_column("Result")
    table.add_column("Detail")
    marks = {
        CheckStatus.PASS: "[green]pass[/green]",
        CheckStatus.FAIL: "[red]FAIL[/red]",
        CheckStatus.SKIP: "[yellow]skip[/yellow]",
    }
    for check in report.checks:
        label = check.name
        if check.severity is Severity.ADVISORY and check.status is CheckStatus.PASS:
            label = f"{check.name} [dim](advisory)[/dim]"
        table.add_row(label, marks[check.status], check.message)
    console.print(table)

    if report.degraded:
        console.print(f"\n[yellow]Degraded run:[/yellow] {report.degraded_reason}")

    for check in report.blocking_failures:
        if check.impact:
            console.print(f"\n[red]Impact:[/red] {check.impact}")

    for check in report.checks:
        if check.status is CheckStatus.PASS and check.severity is Severity.ADVISORY:
            if check.impact:
                console.print(f"\n[yellow]Note:[/yellow] {check.impact}")

    known = KNOWN_BACKENDS.get(report.backend)
    if known and known.get("notes"):
        console.print(f"\n[dim]{known['label']}: {known['notes']}[/dim]")

    console.print()
    if report.passed and not report.degraded:
        console.print(f"[green]Backend supported.[/green] {report.summary()}")
    elif report.passed and report.degraded:
        console.print(
            f"[yellow]No blocking failures, but coverage was partial.[/yellow] {report.summary()}"
        )
    else:
        console.print(f"[red]Backend not usable by lakebench.[/red] {report.summary()}")
        raise typer.Exit(1)


@config_app.command("recommend")
def config_recommend(
    config_file: Annotated[
        Path,
        typer.Argument(help="Configuration file path (used for mode detection)", exists=True),
    ] = Path("lakebench.yaml"),
) -> None:
    """Show sizing guidance for your cluster."""
    from lakebench.cli import recommend as _recommend
    from lakebench.config import load_config

    # Extract pipeline mode from config to pass to recommend
    try:
        cfg = load_config(config_file)
        mode = cfg.architecture.pipeline.mode.value
    except Exception:
        mode = None

    _recommend(mode=mode)


@config_app.command("recipes")
def config_recipes(
    local: Annotated[
        bool,
        typer.Option("--local", help="Show only recipes that run in local mode"),
    ] = False,
    name: Annotated[
        str | None,
        typer.Argument(help="Show full detail for one recipe"),
    ] = None,
) -> None:
    """List architecture recipes and what each one trades off.

    A recipe name says which components are used. This adds what the choice
    costs and what it cannot do, so an architecture can be picked without
    first running it and finding out.
    """
    from lakebench.config.recipes import (
        RECIPE_DESCRIPTIONS,
        RECIPES,
        get_recipe_note,
        local_recipes,
    )

    names = [n for n in sorted(RECIPES) if n != "default"]
    if local:
        names = [n for n in names if n in local_recipes()]

    if name:
        if name not in RECIPES:
            available = ", ".join(sorted(n for n in RECIPES if n != "default"))
            console.print(f"[red]Unknown recipe:[/red] {name}")
            console.print(f"  Available: {available}")
            raise typer.Exit(1)
        _print_recipe_detail(name)
        return

    if not names:
        console.print("[yellow]No recipes match.[/yellow]")
        return

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Recipe", style="cyan", no_wrap=True)
    table.add_column("Choose when")
    table.add_column("Local", justify="center")

    for recipe_name in names:
        note = get_recipe_note(recipe_name)
        table.add_row(
            recipe_name,
            note.when if note else RECIPE_DESCRIPTIONS.get(recipe_name, ""),
            "[green]yes[/green]" if note and note.runs_locally else "[dim]no[/dim]",
        )

    console.print()
    console.print(table)
    console.print()
    console.print("[dim]lakebench config recipes <name> for caveats and detail.[/dim]")
    if not local:
        console.print("[dim]lakebench config recipes --local for what runs on a laptop.[/dim]")


def _print_recipe_detail(name: str) -> None:
    """Print one recipe's components and caveats."""
    from lakebench.config.recipes import RECIPES, get_recipe_note

    recipe = RECIPES[name]
    arch = recipe.get("architecture", {})
    note = get_recipe_note(name)

    lines = [
        f"[bold]{name}[/bold]",
        "",
        f"  Catalog:      {arch.get('catalog', {}).get('type', '-')}",
        f"  Table format: {arch.get('table_format', {}).get('type', '-')}",
        f"  Query engine: {arch.get('query_engine', {}).get('type', '-')}",
    ]
    if note:
        lines += ["", f"  {note.when}"]
        if note.runs_locally:
            lines.append("  Runs locally with --local.")

    console.print()
    console.print(Panel("\n".join(lines), expand=False))

    if note and note.caveats:
        console.print()
        console.print("[bold]Caveats[/bold]")
        for caveat in note.caveats:
            console.print(f"  [yellow]*[/yellow] {caveat}")
    console.print()


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
