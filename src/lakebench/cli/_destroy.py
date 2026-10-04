"""Destroy command for Lakebench CLI.

Extracted from cli/__init__.py -- tears down lakehouse infrastructure.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel

from lakebench.cli._exit import note_exit_paths, refusal_paths, refused_result_code
from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    LoadPurpose,
    load_config,
)
from lakebench.exit_codes import ExitCode, UsageError
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError

from ._helpers import (
    DEPRECATED_SHORT_F_HELP,
    _journal_safe,
    console,
    deprecated_short_f_force,
    esc,
    journal_open,
    print_error,
    print_info,
    print_success,
    resolve_config_path,
    stdin_is_tty,
)
from ._nameless import NAME_OPTION_HELP, guard_nameless


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
        items.append("This deployment's PodMonitors (the shared Prometheus + Grafana stay)")
    items.append("All secrets, configs, and namespace")
    return "\n".join(f"  - {item}" for item in items)


def _destroy_local_mode(cfg, workdir, remove_data: bool, force: bool) -> None:
    """Tear down the local stack. Raises typer.Exit on failure."""

    from lakebench.cli._local import default_workdir, destroy_local, status_local

    resolved = workdir or default_workdir(cfg.name)
    running = status_local(cfg, workdir=resolved)["running"]

    if not force and stdin_is_tty():
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
            raise typer.Exit(ExitCode.NOT_CONFIRMED)

    try:
        removed, used_workdir = destroy_local(cfg, workdir=resolved, remove_data=remove_data)
    except Exception as e:  # noqa: BLE001 -- container CLI failures vary widely
        print_error(f"Local destroy failed: {e}")
        raise typer.Exit(ExitCode.FAILED)  # noqa: B904

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
            "--yes",
            "-y",
            help="Skip confirmation prompt",
        ),
    ] = False,
    force_short_f: Annotated[
        bool,
        typer.Option("-f", hidden=True, help=DEPRECATED_SHORT_F_HELP),
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
    allow_unverified_cluster: Annotated[
        bool,
        typer.Option(
            "--allow-unverified-cluster",
            help=(
                "Bypass the api-server fingerprint match when it cannot "
                "be computed on one or both sides. Only use when you "
                "know the current kubectl context is correct (dev "
                "environment with a broken kubeconfig, etc)."
            ),
        ),
    ] = False,
    force_legacy: Annotated[
        bool,
        typer.Option(
            "--force-legacy",
            help=(
                "Destroy without tag / annotation proof of ownership. "
                "Covers two cases: (1) legacy pre-ownership namespace "
                "or bucket that carries no lakebench identity "
                "annotation / ownership tag; (2) a bucket on an S3 "
                "backend that does not implement tagging AND does not "
                "match the deployment-name prefix. Caution: another "
                "workload's data may live there. Prefer `lakebench "
                "admin migrate-deployment <namespace>` first (case 1) "
                "or rename the bucket to start with the deployment "
                "name (case 2). Refuses always on foreign-tagged "
                "buckets or namespaces regardless of this flag."
            ),
        ),
    ] = False,
    namespace_timeout: Annotated[
        int,
        typer.Option(
            "--namespace-timeout",
            min=0,
            help=(
                "Seconds to wait for the namespace to finish terminating "
                "after the delete is issued (PVC and pod finalizers can "
                "hold it for minutes). A namespace still terminating at "
                "the deadline is not reported as deleted and destroy exits "
                f"{int(ExitCode.INCOMPLETE)}. 0 skips the wait, so "
                f"destroy exits {int(ExitCode.INCOMPLETE)} unless "
                "the namespace is already gone."
            ),
        ),
    ] = 600,
    name: Annotated[
        str | None,
        typer.Option("--name", help=NAME_OPTION_HELP),
    ] = None,
    keep_buckets: Annotated[
        bool,
        typer.Option(
            "--keep-buckets",
            help=(
                "Empty the S3 buckets but do not delete them. By default "
                "destroy deletes the emptied buckets this deployment "
                "created (listed in the namespace's created-buckets record) "
                "and provably owns (ownership tag, or name prefix on "
                "backends without tagging) when create_buckets is true. "
                "Without tagging, a bucket is emptied only if the namespace "
                "records creating it or adopting it empty."
            ),
        ),
    ] = False,
    expect_incarnation: Annotated[
        str | None,
        typer.Option(
            "--expect-incarnation",
            hidden=True,
            help=(
                "Refuse unless the namespace is this incarnation (UID#NONCE). "
                "For harnesses that deployed it; it only adds a refusal."
            ),
        ),
    ] = None,
) -> None:
    """Tear down lakehouse infrastructure.

    Removes the resources this deployment owns: its namespace and what is
    in it. It empties the S3 buckets it can prove it owns and deletes only
    those it created. Shared operators, CRDs and the scratch StorageClass
    are never removed.
    """
    if force_short_f:
        force = deprecated_short_f_force("--force or -y", force)
    if expect_incarnation is not None:
        _check_incarnation_token(expect_incarnation)
        if local:
            raise UsageError(
                "--expect-incarnation does not apply to --local: a local stack has no "
                "namespace to check"
            )
    _destroy_impl(
        resolve_config_path(config_file, file_option),
        force=force,
        local=local,
        workdir=workdir,
        remove_data=remove_data,
        allow_unverified_cluster=allow_unverified_cluster,
        force_legacy=force_legacy,
        namespace_timeout=namespace_timeout,
        name=name,
        keep_buckets=keep_buckets,
        expected_incarnation=expect_incarnation,
    )


def _check_incarnation_token(token: str) -> None:
    """``UID#NONCE`` with both parts set, or a usage error (exit 2)."""
    uid, sep, nonce = token.partition("#")
    if not sep or not uid or not nonce or "#" in nonce:
        raise UsageError(
            f"--expect-incarnation {token!r} is not UID#NONCE",
            next="pass the namespace's UID and its lakebench.deployment/deploy-nonce, "
            "joined by '#'",
            path="cli.bad_argument",
        )


def _destroy_impl(
    config_file: Path,
    *,
    force: bool = False,
    local: bool = False,
    workdir: Path | None = None,
    remove_data: bool = False,
    allow_unverified_cluster: bool = False,
    force_legacy: bool = False,
    namespace_timeout: int = 600,
    name: str | None = None,
    keep_buckets: bool = False,
    expected_incarnation: str | None = None,
) -> None:
    """The body of ``destroy``, callable with an expected incarnation.

    ``expected_incarnation`` (``uid#nonce``) only adds a refusal: when the
    namespace is not that incarnation, nothing is deleted and
    ``SafetyRefusal`` (``destroy.incarnation_mismatch``, exit 3) is raised,
    whatever ``force`` says. ``reproduce`` passes the incarnation it created.
    """
    from lakebench.deploy import DeploymentEngine, DeploymentStatus

    if expected_incarnation is not None and local:
        raise UsageError(
            "--expect-incarnation does not apply to --local: a local stack has no "
            "namespace to check"
        )

    # Load configuration
    try:
        # A namespace too long to finish deploying (LB-153) still has to be
        # destroyable, so the derived-name length check is skipped here.
        cfg = load_config(config_file, purpose=LoadPurpose.TEARDOWN, name_override=name)
    except ConfigFileNotFoundError as e:
        print_error(f"File not found: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    except ConfigValidationError as e:
        print_error("Config validation failed:")
        for err in e.errors:
            loc = ".".join(str(x) for x in err["loc"])
            console.print(f"  [red]*[/red] {loc}: {err['msg']}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904

    if local:
        _destroy_local_mode(cfg, workdir, remove_data, force)
        return

    namespace = cfg.get_namespace()

    # A nameless config destroys only a deployment it can prove is its own
    # (its state or stamps); the incarnation it proved is the only one destroy may touch.
    verified_incarnation = guard_nameless(cfg, config_file, allow_absent=False)
    caller_expected = expected_incarnation is not None
    if expected_incarnation is not None:
        if verified_incarnation is not None and verified_incarnation != expected_incarnation:
            from lakebench.exit_codes import SafetyRefusal

            raise SafetyRefusal(
                f"Destroy NOT started: namespace {namespace} is {verified_incarnation}, "
                f"not the expected {expected_incarnation}. Nothing was changed.",
                next="check which deployment the namespace now holds before destroying it",
                path="destroy.incarnation_mismatch",
            )
        verified_incarnation = expected_incarnation

    # Confirmation
    if not force:
        if stdin_is_tty():
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
                raise typer.Exit(ExitCode.NOT_CONFIRMED)
        else:
            print_error(
                "Refusing to destroy without --yes in non-interactive mode. "
                "Pass --yes to skip confirmation."
            )
            raise typer.Exit(ExitCode.NOT_CONFIRMED)

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
        "spark-scripts": "Removing infrastructure",
        "rbac": "Removing infrastructure",
        "scratch-sc": "Removing infrastructure",
        "category1": "Removing infrastructure",
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
            if component in _dg_step_start:
                # A repeat for a step already running is a progress line
                # (e.g. waiting for the namespace to terminate); show it and
                # keep the step's start time.
                console.print(f"    [dim]{message}[/dim]")
            else:
                _dg_step_start[component] = time.time()
        elif status == DeploymentStatus.SKIPPED:
            _dg_step_start.pop(component, None)
            if component in ("namespace", "table-cleanup"):
                # The namespace was kept or is still terminating, or tables
                # were left registered (LB-186): never silent.
                console.print(f"    [yellow]![/yellow] {message}")
                _journal_safe(
                    j.record,
                    EventType.DESTROY_COMPONENT,
                    message=message,
                    success=True,
                    details={"component": component, "status": "skipped"},
                )
        elif status == DeploymentStatus.SUCCESS:
            # A step reported done without a start shows 0.0s, not -0.0s.
            elapsed = max(0.0, time.time() - _dg_step_start.pop(component, time.time()))
            console.print(f"    [green]+[/green] {message:<56} [dim]{elapsed:>6.1f}s[/dim]")
            _journal_safe(
                j.record,
                EventType.DESTROY_COMPONENT,
                message=message,
                success=True,
                details={"component": component, "status": "success"},
            )
        elif status == DeploymentStatus.FAILED:
            elapsed = max(0.0, time.time() - _dg_step_start.pop(component, time.time()))
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
        results = engine.destroy_all(
            progress_callback=on_progress,
            allow_unverified_cluster=allow_unverified_cluster,
            force_legacy=force_legacy,
            namespace_wait_timeout=namespace_timeout,
            delete_buckets=not keep_buckets,
            expected_incarnation=verified_incarnation,
        )
    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904  kube config did not load

    mismatch = next((r for r in results if (r.details or {}).get("incarnation_mismatch")), None)
    if mismatch is not None:
        # Nothing was deleted: a refusal (exit 3), not a partial teardown.
        from lakebench.exit_codes import SafetyRefusal

        _journal_safe(j.end_command, success=False, message=mismatch.message)
        _journal_safe(j.close_session)
        raise SafetyRefusal(
            mismatch.message,
            why=(
                "the namespace is not the incarnation the caller expected (another "
                "deploy replaced it, or the expected value is wrong)"
                if caller_expected and (mismatch.details or {}).get("found")
                else "the namespace was redeployed after this command checked it"
                if (mismatch.details or {}).get("found")
                else "the namespace is gone (another destroy may have finished it)"
            ),
            next="check which deployment the namespace now holds before destroying it",
            path="destroy.incarnation_mismatch",
        )

    # Summary
    destroy_elapsed = int(time.time() - destroy_start)
    console.print()
    passed = sum(1 for r in results if r.status == DeploymentStatus.SUCCESS)
    failed = sum(1 for r in results if r.status == DeploymentStatus.FAILED)
    # LB-186: tables destroy left registered because their data is in buckets
    # it does not own (or bucket cleanup was off). Never hidden by the panel.
    left_registered = [
        entry
        for r in results
        if r.component == "table-cleanup"
        for entry in (r.details or {}).get("tables_left_registered", [])
    ]
    for entry in left_registered:
        console.print(
            f"  [yellow]![/yellow] table left registered: {entry['table']} ({entry['reason']})"
        )
    left_note = (
        f"\n[yellow]{len(left_registered)} table(s) left registered[/yellow] (their data "
        "is in buckets this deployment does not own, or bucket cleanup was off)"
        if left_registered
        else ""
    )

    _journal_safe(
        j.record,
        EventType.DESTROY_COMPLETE,
        message=f"{passed} destroyed, {failed} failed"
        + (f", {len(left_registered)} tables left registered" if left_registered else ""),
        success=failed == 0,
        details={
            "components_destroyed": passed,
            "components_failed": failed,
            "tables_left_registered": left_registered,
        },
    )
    _journal_safe(j.end_command, success=failed == 0)
    _journal_safe(j.close_session)

    ns_pending = any(
        r.component == "namespace" and r.details.get("still_terminating") for r in results
    )
    if failed == 0 and ns_pending:
        console.print(
            Panel(
                f"[green]{passed} components removed in {destroy_elapsed}s[/green]"
                f"{left_note}"
                f"\n\n[yellow]Namespace {namespace} is still terminating.[/yellow] "
                f"Wait until `kubectl get ns {namespace}` returns NotFound "
                "before re-deploying under the same name.",
                title="Destroy Incomplete (namespace still terminating)",
                expand=False,
            )
        )
        note_exit_paths(["destroy.namespace_terminating"])
        raise typer.Exit(ExitCode.INCOMPLETE)
    elif failed == 0:
        console.print(
            Panel(
                f"[green]{passed} components removed in {destroy_elapsed}s[/green]"
                f"{left_note}"
                f"\n\nTo re-deploy: [bold]lakebench deploy {esc(config_file)}[/bold]",
                title=(
                    f"Destroy Complete; {len(left_registered)} tables left registered"
                    if left_registered
                    else "Destroy Complete"
                ),
                expand=False,
            )
        )
    else:
        console.print(
            Panel(
                f"[red]{failed} failed[/red], {passed} succeeded{left_note}\n\n"
                f"Some resources may need manual cleanup",
                title="Destroy Incomplete",
                expand=False,
            )
        )
        note_exit_paths(refusal_paths(results))
        raise typer.Exit(refused_result_code(results) or ExitCode.FAILED)
