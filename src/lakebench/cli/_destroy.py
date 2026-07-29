"""Destroy command for Lakebench CLI.

Extracted from cli/__init__.py -- tears down lakehouse infrastructure.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel

from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    load_config,
)
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError

from ._helpers import (
    _journal_safe,
    console,
    journal_open,
    print_error,
    print_info,
    print_success,
    resolve_config_path,
)


def _build_destroy_list(cfg) -> str:
    """Build a config-aware destruction list for confirmation prompt."""
    items = ["PostgreSQL data"]
    cat = cfg.architecture.catalog.type.value
    if cat == "hive":
        items.append("Hive Metastore")
    elif cat == "polaris":
        items.append("Polaris Catalog")
    engine = cfg.architecture.query_engine.type.value
    if engine == "trino":
        items.append("Trino cluster")
    elif engine == "spark-thrift":
        items.append("Spark Thrift Server")
    elif engine == "duckdb":
        items.append("DuckDB")
    if cfg.observability.enabled:
        items.append("Prometheus + Grafana")
    items.append("All secrets, configs, and namespace")
    return "\n".join(f"  - {item}" for item in items)


def _destroy_local_mode(cfg, workdir, remove_data: bool, force: bool) -> None:
    """Tear down the local stack. Raises typer.Exit on failure."""
    import sys

    from lakebench.cli._local import default_workdir, destroy_local, status_local

    resolved = workdir or default_workdir(cfg.name)
    running = status_local(cfg, workdir=resolved)["running"]

    if not force and sys.stdin.isatty():
        detail = (
            f"Containers: {', '.join(str(r) for r in running)}" if running else "Nothing is running"
        )
        data_line = (
            "[red]Generated data and the Ivy cache will be deleted.[/red]"
            if remove_data
            else f"Data is kept in {resolved}. Use --remove-data to delete it."
        )
        console.print(
            Panel(
                f"Tearing down the local stack.\n\n{detail}\n{data_line}",
                title="Confirm Destruction",
                expand=False,
            )
        )
        if not typer.confirm("Proceed?"):
            print_info("Destruction cancelled")
            raise typer.Exit(0)

    try:
        removed, used_workdir = destroy_local(cfg, workdir=resolved, remove_data=remove_data)
    except Exception as e:  # noqa: BLE001 -- container CLI failures vary widely
        print_error(f"Local destroy failed: {e}")
        raise typer.Exit(1)  # noqa: B904

    print_success(f"Removed {removed} container(s)")
    if remove_data:
        print_info(f"Deleted {used_workdir}")
    else:
        print_info(f"Data kept in {used_workdir} (--remove-data to delete)")


def destroy(
    config_file: Annotated[
        Path | None,
        typer.Argument(
            help="Path to configuration YAML file (default: ./lakebench.yaml)",
        ),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option(
            "--file",
            help="Path to configuration YAML file (alternative to positional argument)",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Skip confirmation prompt",
        ),
    ] = False,
    local: Annotated[
        bool,
        typer.Option(
            "--local",
            help="Tear down the local stack instead of Kubernetes",
        ),
    ] = False,
    workdir: Annotated[
        Path | None,
        typer.Option(
            "--workdir",
            help="Host directory for local mode state (default: ~/.lakebench/local/<name>)",
        ),
    ] = None,
    remove_data: Annotated[
        bool,
        typer.Option(
            "--remove-data",
            help="Local mode: also delete generated data and the Ivy cache",
        ),
    ] = False,
) -> None:
    """Tear down lakehouse infrastructure.

    Removes all Lakebench resources from the cluster.
    """
    from lakebench.deploy import DeploymentEngine, DeploymentStatus

    config_file = resolve_config_path(config_file, file_option)

    # Load configuration
    try:
        cfg = load_config(config_file)
    except ConfigFileNotFoundError as e:
        print_error(f"File not found: {e}")
        raise typer.Exit(1)  # noqa: B904
    except ConfigValidationError as e:
        print_error("Config validation failed:")
        for err in e.errors:
            loc = ".".join(str(x) for x in err["loc"])
            console.print(f"  [red]*[/red] {loc}: {err['msg']}")
        raise typer.Exit(1)  # noqa: B904
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    if local:
        _destroy_local_mode(cfg, workdir, remove_data, force)
        return

    namespace = cfg.get_namespace()

    # Confirmation
    if not force:
        import sys

        if sys.stdin.isatty():
            console.print(
                Panel(
                    f"[red]WARNING[/red]: This will destroy all Lakebench resources in namespace [bold]{namespace}[/bold]\n\n"
                    f"This includes:\n{_build_destroy_list(cfg)}",
                    title="Confirm Destruction",
                    expand=False,
                )
            )
            confirm = typer.confirm("Are you sure you want to proceed?")
            if not confirm:
                print_info("Destruction cancelled")
                raise typer.Exit(0)
        else:
            print_error(
                "Refusing to destroy without --force in non-interactive mode. "
                "Pass --force to skip confirmation."
            )
            raise typer.Exit(1)

    console.print(
        Panel(
            f"[bold]{cfg.name}[/bold]  ·  namespace: [bold]{namespace}[/bold]",
            title="Destroy",
            expand=False,
        )
    )
    console.print()

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.DESTROY, {"force": force})

    # Progress callback -- phase headers with timed sub-steps
    _destroy_groups = {
        "spark-jobs": "Cleaning up jobs and pods",
        "spark-pods": "Cleaning up jobs and pods",
        "datagen-jobs": "Cleaning up jobs and pods",
        "iceberg-tables": "Cleaning data",
        "s3-buckets": "Cleaning data",
        "observability": "Removing infrastructure",
        "trino": "Removing infrastructure",
        "spark-thrift": "Removing infrastructure",
        "duckdb": "Removing infrastructure",
        "hive": "Removing infrastructure",
        "polaris": "Removing infrastructure",
        "postgres": "Removing infrastructure",
        "rbac": "Removing infrastructure",
        "scratch-sc": "Removing infrastructure",
        "namespace": "Removing namespace",
    }
    _dg_current_group = ""
    _dg_step_start: dict[str, float] = {}

    def on_progress(component: str, status: DeploymentStatus, message: str) -> None:
        nonlocal _dg_current_group
        group = _destroy_groups.get(component, component)

        if status == DeploymentStatus.IN_PROGRESS:
            if group != _dg_current_group:
                _dg_current_group = group
                console.print(f"  [dim]{group}[/dim]")
            _dg_step_start[component] = time.time()
        elif status == DeploymentStatus.SKIPPED:
            _dg_step_start.pop(component, None)
        elif status == DeploymentStatus.SUCCESS:
            elapsed = time.time() - _dg_step_start.pop(component, time.time())
            console.print(f"    [green]+[/green] {message:<56} [dim]{elapsed:>6.1f}s[/dim]")
            _journal_safe(
                j.record,
                EventType.DESTROY_COMPONENT,
                message=message,
                success=True,
                details={"component": component, "status": "success"},
            )
        elif status == DeploymentStatus.FAILED:
            elapsed = time.time() - _dg_step_start.pop(component, time.time())
            console.print(f"    [red]x[/red] {message:<56} [dim]{elapsed:>6.1f}s[/dim]")
            _journal_safe(
                j.record,
                EventType.DESTROY_COMPONENT,
                message=message,
                success=False,
                details={"component": component, "status": "failed"},
            )

    # Destroy
    destroy_start = time.time()
    try:
        engine = DeploymentEngine(cfg)
        results = engine.destroy_all(progress_callback=on_progress)
    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    # Summary
    destroy_elapsed = int(time.time() - destroy_start)
    console.print()
    passed = sum(1 for r in results if r.status == DeploymentStatus.SUCCESS)
    failed = sum(1 for r in results if r.status == DeploymentStatus.FAILED)

    _journal_safe(
        j.record,
        EventType.DESTROY_COMPLETE,
        message=f"{passed} destroyed, {failed} failed",
        success=failed == 0,
        details={"components_destroyed": passed, "components_failed": failed},
    )
    _journal_safe(j.end_command, success=failed == 0)
    _journal_safe(j.close_session)

    if failed == 0:
        console.print(
            Panel(
                f"[green]{passed} components removed in {destroy_elapsed}s[/green]"
                f"\n\nTo re-deploy: [bold]lakebench deploy[/bold]",
                title="Destroy Complete",
                expand=False,
            )
        )
    else:
        console.print(
            Panel(
                f"[red]{failed} failed[/red], {passed} succeeded\n\n"
                f"Some resources may need manual cleanup",
                title="Destroy Incomplete",
                expand=False,
            )
        )
        raise typer.Exit(1)
