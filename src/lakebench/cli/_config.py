"""Config subcommands for Lakebench CLI.

Provides ``lakebench config show``, ``lakebench config validate`` and
``lakebench config recommend``. ``lakebench config upgrade`` is removed and
refuses: it rewrote configs lossily and wrote secrets in plaintext.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from lakebench.cli._helpers import esc, print_error
from lakebench.exit_codes import ExitCode

logger = logging.getLogger(__name__)


def _load_failure_code(exc: BaseException) -> ExitCode:
    """2 for a config that fails to load or validate; 1 for anything else."""
    from lakebench.config import ConfigError

    return ExitCode.USAGE if isinstance(exc, ConfigError) else ExitCode.FAILED


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
    from lakebench.config import LoadPurpose, load_config
    from lakebench.config.loader import load_yaml

    try:
        # Load raw YAML to detect which fields are explicitly set
        raw = load_yaml(config_file)
        raw_keys = set(_flatten_keys(raw))

        # Load fully resolved config
        cfg = load_config(config_file, purpose=LoadPurpose.INSPECT)

        console.print(
            Panel(
                f"[bold]Resolved configuration:[/bold] {esc(config_file)}",
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
                _source(raw, "scale", raw_keys, "workload.datagen.scale")
                if "workload.datagen.scale" in raw_keys
                else _source(raw, "scale", raw_keys, "architecture.workload.datagen.scale"),
            ),
            (
                "spark_image",
                cfg.images.spark,
                _source(raw, "spark_image", raw_keys, "images.spark"),
            ),
        ]

        # Peak requested resources from compute_peak_requirements(), the
        # same figure run's capacity preflight checks. Auto-sizing first, as
        # info and run do, so the co-resident request matches theirs.
        from lakebench.cli import info_peak_request
        from lakebench.config.autosizer import resolve_auto_sizing

        resolve_auto_sizing(cfg)
        from lakebench.config.schema import PipelineMode

        sustained = cfg.architecture.pipeline.mode == PipelineMode.SUSTAINED
        peak, co_cores, co_gb, co_label = info_peak_request(
            cfg, cfg.architecture.workload.datagen.scale, sustained
        )
        fields.append(
            (
                "peak_requested",
                f"{peak.cpu_cores + co_cores} cores / {peak.memory_gb + co_gb} GB memory / "
                f"{peak.scratch_gb} GB scratch",
                f"derived: {peak.driving_job} + {co_label}",
            )
        )
        from lakebench.config.support import support_state_for_config

        support = support_state_for_config(cfg)
        fields.append(
            (
                "support",
                f"{support['state']} ({support['workload']} x "
                f"{support.get('recipe') or 'no recipe'} x {support['mode']})",
                support["basis"],
            )
        )
        if support.get("mode_note"):
            fields.append(("mode_note", support["mode_note"], "workload x mode"))
        if support.get("scale_note"):
            fields.append(("scale_note", support["scale_note"], "datagen scale band"))

        table = Table(show_header=True, header_style="bold")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="white")
        table.add_column("Source", style="dim")

        for field_name, value, source in fields:
            table.add_row(field_name, str(value), source)

        console.print(table)

    except Exception as e:
        print_error(e)
        raise typer.Exit(_load_failure_code(e)) from None


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
    from lakebench.config import LoadPurpose, load_config
    from lakebench.runtime.container import ContainerRuntimeError, detect_container_cli

    console.print()
    console.print(Panel(f"Validating for local mode:\n{esc(config_file)}", expand=False))
    console.print()

    passed, failed = 0, 0

    try:
        # Loads as deploy does, so the check fails where deploy would.
        cfg = load_config(config_file, purpose=LoadPurpose.MUTATE)
        console.print("  [green]+[/green] Config syntax valid")
        passed += 1
    except Exception as e:
        console.print(f"  [red]x[/red] Config invalid: {esc(e)}")
        raise typer.Exit(_load_failure_code(e))  # noqa: B904

    try:
        check_local_supported(cfg)
        console.print("  [green]+[/green] Table format supported locally (Iceberg)")
        passed += 1
    except LocalModeError as e:
        console.print(f"  [red]x[/red] {esc(e)}")
        failed += 1

    try:
        cli = detect_container_cli()
        console.print(f"  [green]+[/green] Container runtime available ({esc(cli)})")
        passed += 1
    except ContainerRuntimeError as e:
        console.print(f"  [red]x[/red] {esc(e)}")
        failed += 1

    advisory = scale_advisory(cfg)
    if advisory:
        console.print(f"  [yellow]![/yellow] {esc(advisory)}")
    else:
        scale = cfg.architecture.workload.datagen.scale
        console.print(f"  [green]+[/green] Scale {esc(scale)} is sized for one host")
        passed += 1

    console.print()
    if failed:
        console.print(
            Panel(
                f"[red]{esc(passed)} passed, {esc(failed)} failed[/red]",
                title="Validation Failed",
                expand=False,
            )
        )
        raise typer.Exit(ExitCode.FAILED)

    console.print(
        Panel(
            f"[green]{esc(passed)} passed[/green]\n\nRun: lakebench deploy {esc(config_file)} --local",
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
    from lakebench.config import LoadPurpose, load_config
    from lakebench.s3 import KNOWN_BACKENDS, CheckStatus, Severity, run_conformance

    try:
        cfg = load_config(config_file, purpose=LoadPurpose.INSPECT)
    except Exception as e:
        print_error(f"Could not load config: {e}")
        raise typer.Exit(_load_failure_code(e)) from e

    s3 = cfg.platform.storage.s3
    if not s3.endpoint:
        print_error("No S3 endpoint configured.")
        raise typer.Exit(ExitCode.USAGE)

    console.print(
        Panel(
            f"[bold]Storage conformance[/bold]\nEndpoint: {esc(s3.endpoint)}",
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
        ca_cert=s3.ca_cert,
        verify_ssl=s3.verify_ssl,
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
        console.print(f"\n[yellow]Degraded run:[/yellow] {esc(report.degraded_reason)}")

    for check in report.blocking_failures:
        if check.impact:
            console.print(f"\n[red]Impact:[/red] {esc(check.impact)}")

    for check in report.checks:
        if check.status is CheckStatus.PASS and check.severity is Severity.ADVISORY:
            if check.impact:
                console.print(f"\n[yellow]Note:[/yellow] {esc(check.impact)}")

    known = KNOWN_BACKENDS.get(report.backend)
    if known and known.get("notes"):
        console.print(f"\n[dim]{esc(known['label'])}: {esc(known['notes'])}[/dim]")

    console.print()
    if report.passed and not report.degraded:
        console.print(f"[green]Backend supported.[/green] {esc(report.summary())}")
    elif report.passed and report.degraded:
        console.print(
            f"[yellow]No blocking failures, but coverage was partial.[/yellow] {esc(report.summary())}"
        )
    else:
        print_error(f"Backend not usable by lakebench. {report.summary()}")
        raise typer.Exit(ExitCode.FAILED)


@config_app.command("recommend")
def config_recommend(
    config_file: Annotated[
        Path,
        typer.Argument(help="Configuration file path (used for mode detection)", exists=True),
    ] = Path("lakebench.yaml"),
) -> None:
    """Show sizing guidance for your cluster."""
    from lakebench.cli import recommend as _recommend
    from lakebench.config import LoadPurpose, load_config

    # Extract pipeline mode from config to pass to recommend
    schema: str | None = None
    try:
        cfg = load_config(config_file, purpose=LoadPurpose.INSPECT)
        mode = cfg.architecture.pipeline.mode.value
        schema = cfg.architecture.workload.schema_type.value
    except Exception:
        mode = None

    _recommend(mode=mode, schema_type=schema)


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
            print_error(f"Unknown recipe: {name}. Available: {available}")
            raise typer.Exit(ExitCode.USAGE)
        _print_recipe_detail(name)
        return

    if not names:
        console.print("[yellow]No recipes match.[/yellow]")
        return

    from lakebench.config.support import MODES, support_matrix, workloads

    states = {(r["recipe"], r["workload"], r["mode"]): r["state"] for r in support_matrix()}
    cols = [(wl, m) for wl in workloads() for m in MODES]
    short = {"customer360": "C360", "financial": "AML"}

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Recipe", style="cyan", no_wrap=True)
    table.add_column("Choose when")
    table.add_column("Local", justify="center")
    for wl, m in cols:
        table.add_column(f"{short.get(wl, wl)} {m}")

    for recipe_name in names:
        note = get_recipe_note(recipe_name)
        table.add_row(
            recipe_name,
            note.when if note else RECIPE_DESCRIPTIONS.get(recipe_name, ""),
            "[green]yes[/green]" if note and note.runs_locally else "[dim]no[/dim]",
            *(_state_markup(states[(recipe_name, wl, m)]) for wl, m in cols),
        )

    console.print()
    console.print(table)
    console.print()
    console.print(
        "[dim]Support: supported = validated on the release tree; unverified = valid, "
        "not release-validated; unsupported = refused at config load.[/dim]"
    )
    console.print("[dim]lakebench config recipes <name> for caveats and detail.[/dim]")
    if not local:
        console.print("[dim]lakebench config recipes --local for what runs on a laptop.[/dim]")


def _state_markup(state: str) -> str:
    color = {"supported": "green", "unverified": "yellow", "unsupported": "red"}.get(state, "dim")
    return f"[{color}]{state}[/{color}]"


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

    from rich.markup import escape

    from lakebench.config.support import WORKLOAD_LABELS, support_matrix

    real = name if name != "default" else "hive-iceberg-spark-trino"
    console.print()
    console.print("[bold]Support[/bold]")
    for row in support_matrix():
        if row["recipe"] != real:
            continue
        label = WORKLOAD_LABELS.get(row["workload"], row["workload"])
        console.print(
            f"  {esc(label)} {esc(row['mode'])}: {_state_markup(row['state'])} -- {escape(row['basis'])}"
        )

    if note and note.caveats:
        console.print()
        console.print("[bold]Caveats[/bold]")
        for caveat in note.caveats:
            console.print(f"  [yellow]*[/yellow] {esc(caveat)}")
    console.print()


@config_app.command(
    "upgrade",
    hidden=True,
    # Any old argument or flag reaches the refusal, so Click never echoes one.
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
)
def config_upgrade(
    config_file: Annotated[
        Path | None,
        # No exists=True: a missing path must reach the refusal, not a Click
        # error that echoes the argument.
        typer.Argument(help="Ignored: the command is removed."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Ignored: the command is removed."),
    ] = None,
) -> None:
    """Removed: refuses before opening any file.

    It rewrote configs lossily, in place by default, and wrote the S3
    secret key into the result in plaintext. The arguments stay declared so
    old invocations get this refusal rather than a usage error; neither is
    read or printed.
    """
    from lakebench.cli._exit import UsageError

    raise UsageError(
        "`config upgrade` is removed: it rewrote configs lossily and wrote secrets in plaintext.",
        next="lakebench init --from OLD.yaml -o NEW.yaml",
        path="config.upgrade_refused",
    )


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
