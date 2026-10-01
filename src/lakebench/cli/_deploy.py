"""Deploy command for Lakebench CLI.

Extracted from ``cli/__init__.py`` -- contains the ``deploy()`` function
and its helper functions ``_preflight_check()`` and ``_build_component_list()``.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel

from lakebench.cli._helpers import (
    _journal_safe,
    check_datagen_scale,
    console,
    journal_open,
    print_error,
    print_info,
    print_warning,
    resolve_config_path,
)
from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    LoadPurpose,
    load_config,
)
from lakebench.exit_codes import LakebenchError
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError

logger = logging.getLogger(__name__)


def _preflight_check(cfg) -> None:
    """Run critical pre-flight checks before deployment.

    Prints warnings for non-critical issues; exits on blockers.
    """
    print_info("Tip: run 'lakebench config validate' for a full prerequisite check")

    # 1. S3 endpoint must be set
    s3 = cfg.platform.storage.s3
    if not s3.endpoint:
        print_error("S3 endpoint not configured (platform.storage.s3.endpoint)")
        print_info("Run 'lakebench config validate' for detailed diagnostics")
        raise typer.Exit(1)

    # 2. S3 credentials must be present (inline or secret_ref)
    has_inline = bool(s3.access_key and s3.secret_key)
    has_ref = bool(getattr(s3, "secret_ref", None))
    if not has_inline and not has_ref:
        print_error("S3 credentials not configured (set access_key/secret_key or secret_ref)")
        raise typer.Exit(1)

    # 3. Check Stackable CRDs if catalog=hive
    if cfg.architecture.catalog.type.value == "hive":
        _missing_stackable: list[str] = []
        try:
            # Check CRDs directly without instantiating a full deployer
            from kubernetes import client as k8s_client

            api_ext = k8s_client.ApiextensionsV1Api()
            crds = api_ext.list_custom_resource_definition()
            crd_names = {crd.metadata.name for crd in crds.items}
            required_crds = {
                "hiveclusters.hive.stackable.tech": "hive-operator",
                "secretclasses.secrets.stackable.tech": "secret-operator",
            }
            _missing_stackable = [op for crd, op in required_crds.items() if crd not in crd_names]
        except Exception:
            logger.debug("Preflight CRD check skipped (K8s not reachable)", exc_info=True)

        if _missing_stackable:
            op_install = getattr(
                getattr(getattr(cfg.architecture.catalog, "hive", None), "operator", None),
                "install",
                False,
            )
            if op_install:
                print_info(
                    f"Stackable operators missing ({', '.join(_missing_stackable)}) "
                    "-- will be auto-installed during deploy (install: true)"
                )
            else:
                print_error(f"Missing Stackable operators: {', '.join(_missing_stackable)}")
                print_info("Install operators first:")
                for op in [
                    "commons-operator",
                    "listener-operator",
                    "secret-operator",
                    "hive-operator",
                ]:
                    console.print(
                        f"  helm install {op} "
                        f"oci://oci.stackable.tech/sdp-charts/{op} "
                        f"--version 25.7.0 --namespace stackable --create-namespace"
                    )
                print_info(
                    "Or switch to a Polaris recipe (no operators needed):\n"
                    "  Set recipe: polaris-iceberg-spark-trino in your config"
                )
                raise typer.Exit(1)


def _build_component_list(cfg) -> str:
    """Build a config-aware component list for confirmation prompts."""
    parts = ["PostgreSQL"]
    cat = cfg.architecture.catalog.type.value
    if cat == "hive":
        parts.append("Hive Metastore")
    elif cat == "polaris":
        parts.append("Polaris Catalog")
    engine = cfg.architecture.query_engine.type.value
    if engine == "trino":
        parts.append("Trino")
    elif engine == "spark-thrift":
        parts.append("Spark Thrift Server")
    elif engine == "duckdb":
        parts.append("DuckDB")
    parts.append("Spark RBAC")
    if cfg.platform.compute.spark.operator.install:
        parts.append("Spark Operator")
    if cfg.observability.enabled:
        if cfg.observability.prometheus_stack_enabled:
            parts.append("Prometheus")
        if cfg.observability.dashboards_enabled:
            parts.append("Grafana")
    return ", ".join(parts)


def _deploy_local_mode(
    cfg,
    config_file: Path,
    workdir: Path | None,
    dry_run: bool,
    yes: bool,
    timeout: int,
) -> None:
    """Deploy the local stack. Raises typer.Exit on failure."""
    from lakebench.cli._local import (
        LocalModeError,
        check_local_supported,
        default_workdir,
        deploy_local,
        print_local_plan,
        scale_advisory,
    )

    try:
        check_local_supported(cfg)
    except LocalModeError as e:
        print_error(str(e))
        raise typer.Exit(1)  # noqa: B904

    resolved_workdir = workdir or default_workdir(cfg.name)
    print_local_plan(cfg, resolved_workdir)

    advisory = scale_advisory(cfg)
    if advisory:
        console.print(f"[yellow]{advisory}[/yellow]")
        console.print()

    if dry_run:
        console.print("[yellow]DRY RUN[/yellow] -- nothing was started.")
        return

    if not yes:
        typer.confirm("Proceed with local deployment?", abort=True)

    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.DEPLOY, {"local": True})
    started = time.time()

    try:
        deployment = deploy_local(cfg, workdir=resolved_workdir, timeout=timeout)
    except LocalModeError as e:
        print_error(str(e))
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904
    except Exception as e:  # noqa: BLE001 -- container CLI failures vary widely
        print_error(f"Local deploy failed: {e}")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    elapsed = int(time.time() - started)
    _journal_safe(
        j.record,
        EventType.DEPLOY_COMPLETE,
        message=f"Local stack ready in {elapsed}s",
        success=True,
        details={"local": True, "endpoint": deployment.endpoint},
    )
    _journal_safe(j.end_command, success=True)

    console.print()
    console.print(
        Panel(
            f"[green]Local stack ready in {elapsed}s[/green]\n\n"
            f"Endpoint: {deployment.endpoint}\n"
            f"Workdir:  {deployment.workdir}\n\n"
            f"Next: [bold]lakebench run {config_file} --local[/bold]",
            title="Deploy complete",
            expand=False,
        )
    )


def _namespace_identity(cfg):
    """One read of the namespace's UID and lakebench annotations (or None)."""
    from kubernetes import client

    from lakebench.config.deploy_state import read_namespace_identity
    from lakebench.exit_codes import PrerequisiteError

    try:
        return read_namespace_identity(client.CoreV1Api(), cfg.get_namespace())
    except Exception as e:  # noqa: BLE001
        raise PrerequisiteError(
            f"cannot read namespace {cfg.get_namespace()}: {e}",
            why="deploy reconciles its recorded nonces with the namespace before any change",
            path="deploy.state_unrecordable",
        ) from e


def _record_deploy_nonce(cfg, config_file: Path, *, dry_run: bool, nonce: str | None) -> str | None:
    """Steps 1 to 4 of SAF-2 (b): lock, reconcile, choose, record.

    Under the directory's state lock: read the state, read the namespace and
    mark the entry it carries confirmed, prepend the new nonce as pending
    (never evicting the carried entry), write the state atomically. Returns
    the nonce. A dry run reads the namespace, prints which entry it carries
    and writes nothing (no lock, no ``.lakebench/``). A state that cannot be
    written stops the deploy with exit 4 before any cluster change.
    """
    import uuid

    from lakebench.config import deploy_state as ds
    from lakebench.exit_codes import PrerequisiteError, SafetyRefusal

    name = cfg.name
    try:
        path = ds.state_path(config_file, name)
    except ds.StateError as e:
        raise PrerequisiteError(
            str(e),
            why="deploy records the nonce in <config dir>/.lakebench/<name>.json (SAF-2)",
            next="give the config a name without '/', a leading '.', or the value 'state'",
            path="deploy.state_unrecordable",
        ) from e
    if dry_run:
        ident = _namespace_identity(cfg)
        try:
            state = ds.read_state_file(path)
        except ds.StateError as e:
            print_warning(f"State: {e}; a real deploy stops here (exit 4)")
            return None
        why_not = ds.not_here(state, config_file) if state is not None else None
        if why_not is not None:
            print_warning(f"State: {path}: {why_not}; a real deploy stops here (exit 3)")
            return None
        carries = state is not None and ident is not None and ident.nonce in state.kept_nonces()
        print_info(
            f"State: {path} "
            + (
                "(none yet)"
                if state is None
                else f"keeps {len(state.nonces)} nonce(s); namespace carries "
                + ("one of them" if carries else "none of them")
            )
            + "; a dry run writes nothing"
        )
        return None
    chosen = nonce or uuid.uuid4().hex
    try:
        with ds.state_lock(config_file, name):
            state = ds.read_state_file(path)
            if state is not None and state.moved_to:
                raise SafetyRefusal(
                    f"this deployment's state moved to {state.moved_to}; deploy from there",
                    where=str(path),
                    path="nameless.moved",
                )
            if state is not None:
                why_not = ds.not_here(state, config_file)
                if why_not is not None:
                    raise SafetyRefusal(
                        f"{path}: {why_not}",
                        why="a copied directory would add its nonces to another "
                        "directory's record of the deployment",
                        where=str(path),
                        next=(
                            "this directory was renamed: run `python -m "
                            "lakebench.config.deploy_state relocate CONFIG NEWDIR` from it"
                            if ds.moved_with_its_directory(state, config_file)
                            else "deploy from the directory that wrote it; if this copy is "
                            f"meant to be a new deployment directory, remove only {path}"
                        ),
                        path="deploy.state_copied",
                    )
                if ds.retarget(state, cfg.get_namespace()):
                    print_warning(
                        f"State: the config now targets namespace {state.namespace}; "
                        "the nonces recorded for the old one are dropped"
                    )
                if state.config_dir_id is None:
                    state.config_dir_id = ds.new_state(
                        config_file, name, state.namespace
                    ).config_dir_id
            else:
                state = ds.new_state(config_file, name, cfg.get_namespace())
            ident = _namespace_identity(cfg)
            carried = ds.reconcile(state, ident)
            ds.record_pending(state, chosen, carried)
            from lakebench.k8s.target import active_target

            pinned = active_target()
            state.api_server = pinned.api_server if pinned is not None else state.api_server
            ds.write_state(path, state)
    except (OSError, ds.StateError) as e:
        raise PrerequisiteError(
            f"cannot record the deploy nonce in {path}: {e}",
            why="deploy records the nonce before the namespace gets it (SAF-2)",
            where=str(path),
            next=(
                "check or move the state file aside, then deploy again"
                if isinstance(e, ds.StateError)
                else "make the config's directory writable, or deploy from a local disk"
            ),
            path="deploy.state_unrecordable",
        ) from e
    return chosen


def _confirm_deploy_nonce(cfg, config_file: Path, nonce: str) -> None:
    """Step 7: once the namespace carries ``nonce``, mark it confirmed.

    Runs whether the deploy succeeded or failed. Best effort: an entry left
    pending is still accepted by the nameless checks and confirmed by the
    next deploy's reconcile.
    """
    from lakebench.config import deploy_state as ds

    try:
        with ds.state_lock(config_file, cfg.name):
            path = ds.state_path(config_file, cfg.name)
            state = ds.read_state_file(path)
            if state is None:
                return
            if ds.confirm(state, nonce, _namespace_identity(cfg)):
                ds.write_state(path, state)
    except Exception as e:  # noqa: BLE001 -- the entry stays pending
        logger.debug("could not confirm deploy nonce %s: %s", nonce, e)


def deploy(
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
            "-f",
            help="Path to configuration YAML file (alternative to positional argument)",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be deployed without making changes",
        ),
    ] = False,
    include_observability: Annotated[
        bool,
        typer.Option(
            "--include-observability",
            help="Deploy Prometheus and Grafana monitoring stack",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip confirmation prompt",
        ),
    ] = False,
    timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            "-t",
            help="Global deployment timeout in seconds (0 = no timeout)",
        ),
    ] = 3600,
    local: Annotated[
        bool,
        typer.Option(
            "--local",
            help="Deploy locally with podman/docker instead of Kubernetes",
        ),
    ] = False,
    workdir: Annotated[
        Path | None,
        typer.Option(
            "--workdir",
            help="Host directory for local mode state (default: ~/.lakebench/local/<name>)",
        ),
    ] = None,
    force_legacy: Annotated[
        bool,
        typer.Option(
            "--force-legacy",
            help=(
                "Claim ownership without tag proof. Covers two cases: "
                "(1) a pre-1.5 annotation-less namespace or untagged "
                "bucket being migrated; (2) a bucket on a backend that "
                "does not implement bucket tagging AND does not match "
                "the deployment-name prefix. Use only when you have "
                "confirmed the resources are yours -- a mistake can "
                "silently take over another team's storage."
            ),
        ),
    ] = False,
) -> None:
    """Deploy lakehouse infrastructure.

    Deploys all components in this order:
    1. Namespace, secrets and S3 buckets
    2. Scratch StorageClass check (it must already exist; a cluster admin
       installs it with `lakebench admin install-scratch-storage-class`)
    3. PostgreSQL
    4. Catalog (Hive Metastore or Polaris)
    5. Spark RBAC (then Unity Catalog, only if catalog.type is unity)
    6. Spark Operator check and watch-list entry for the namespace (always
       runs; operator.install: true also installs a missing operator)
    7. Query Engine (Trino / Spark Thrift / DuckDB)
    8. Observability (if enabled)
    """
    _deploy_impl(
        resolve_config_path(config_file, file_option),
        dry_run=dry_run,
        include_observability=include_observability,
        yes=yes,
        timeout=timeout,
        local=local,
        workdir=workdir,
        force_legacy=force_legacy,
    )


def _deploy_impl(
    config_file: Path,
    *,
    dry_run: bool = False,
    include_observability: bool = False,
    yes: bool = False,
    timeout: int = 3600,
    local: bool = False,
    workdir: Path | None = None,
    force_legacy: bool = False,
    nonce: str | None = None,
) -> str | None:
    """The body of ``deploy``, callable with a nonce the caller chose.

    ``reproduce`` passes its own ``nonce`` (SAF-1). Returns the nonce this
    deploy recorded and stamped, or None for a dry run or local mode.
    """
    from lakebench.deploy import DeploymentEngine, DeploymentStatus

    # Load configuration
    try:
        cfg = load_config(config_file, purpose=LoadPurpose.MUTATE)
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

    # Enable observability if flag is set
    if include_observability:
        cfg.observability.enabled = True

    if not local:
        # A name the deploy state cannot record stops here, before the
        # prompt and preflight, not after them (SAF-2).
        from lakebench.config import deploy_state as _ds
        from lakebench.exit_codes import PrerequisiteError

        try:
            _ds.state_path(config_file, cfg.name)
        except _ds.StateError as e:
            raise PrerequisiteError(
                str(e),
                why="deploy records the nonce in <config dir>/.lakebench/<name>.json (SAF-2)",
                next="give the config a name without '/', a leading '.', or the value 'state'",
                path="deploy.state_unrecordable",
            ) from e

    # Local mode has its own path: no namespace, no operator, no preflight
    # against a cluster that is not there.
    if local:
        _deploy_local_mode(cfg, config_file, workdir, dry_run, yes, timeout)
        return None

    check_datagen_scale(cfg)

    namespace = cfg.get_namespace()

    # Pre-flight validation (BUG-012)
    if not dry_run:
        _preflight_check(cfg)

    # Confirmation prompt (mirrors destroy command pattern)
    if not yes and not dry_run:
        components = _build_component_list(cfg)
        console.print(
            Panel(
                f"Deploying to namespace [bold]{namespace}[/bold]\n\nComponents: {components}",
                title="Confirm Deployment",
                expand=False,
            )
        )
        typer.confirm("Proceed with deployment?", abort=True)

    # Display deployment info
    components = _build_component_list(cfg)
    header = f"[bold]{cfg.name}[/bold]  ·  namespace: [bold]{namespace}[/bold]"
    if dry_run:
        header = f"[yellow]DRY RUN[/yellow]  ·  {header}"
    console.print(
        Panel(
            f"{header}\nConfig: {config_file.resolve()}\nComponents: {components}",
            title="Deploy",
            expand=False,
        )
    )
    console.print()

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.DEPLOY, {"dry_run": dry_run})

    # Deploy
    deploy_start = time.time()
    recorded: str | None = None
    try:
        engine = DeploymentEngine(cfg, dry_run=dry_run)
        # SAF-2: record the nonce in the directory's state before the
        # namespace gets it, so a crash between the two cannot orphan the
        # deployment. A dry run only reads.
        recorded = _record_deploy_nonce(cfg, config_file, dry_run=dry_run, nonce=nonce)
        engine.deploy_nonce = recorded

        # Progress callback -- columnar output with version info
        _step_start: dict[str, float] = {}

        def _fmt_deploy_line(icon: str, color: str, elapsed: float) -> str:
            """Format a deploy progress line from the latest engine result."""
            r = engine.results[-1] if engine.results else None
            if r and r.label:
                return (
                    f"  [{color}]{icon}[/{color}] {r.label:<22} "
                    f"{r.detail:<30} [dim]{elapsed:>6.1f}s[/dim]"
                )
            # Fallback for results without label/detail
            return f"  [{color}]{icon}[/{color}] {r.message if r else '':<54} [dim]{elapsed:>6.1f}s[/dim]"

        def on_progress(component: str, status: DeploymentStatus, message: str) -> None:
            if status == DeploymentStatus.IN_PROGRESS:
                _step_start[component] = time.time()
            elif status == DeploymentStatus.SKIPPED:
                _step_start.pop(component, None)
            elif status == DeploymentStatus.SUCCESS:
                elapsed = time.time() - _step_start.pop(component, time.time())
                console.print(_fmt_deploy_line("+", "green", elapsed))
            elif status == DeploymentStatus.FAILED:
                elapsed = time.time() - _step_start.pop(component, time.time())
                console.print(_fmt_deploy_line("x", "red", elapsed))

        try:
            results = engine.deploy_all(
                progress_callback=on_progress,
                timeout=timeout,
                force_legacy=force_legacy,
            )
        finally:
            if recorded:
                _confirm_deploy_nonce(cfg, config_file, recorded)

        # Record each component result in journal
        for r in results:
            _journal_safe(
                j.record,
                EventType.DEPLOY_COMPONENT,
                message=r.message,
                success=r.status == DeploymentStatus.SUCCESS,
                details={
                    "component": r.component,
                    "status": r.status.value,
                    "elapsed_seconds": r.elapsed_seconds,
                },
            )
    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904
    except LakebenchError as e:
        _journal_safe(j.end_command, success=False, message=str(e))
        raise

    # Summary
    console.print()
    passed = sum(1 for r in results if r.status == DeploymentStatus.SUCCESS)
    failed = sum(1 for r in results if r.status == DeploymentStatus.FAILED)
    skipped = sum(1 for r in results if r.status == DeploymentStatus.SKIPPED)

    _journal_safe(
        j.record,
        EventType.DEPLOY_COMPLETE,
        message=f"{passed} succeeded, {failed} failed",
        success=failed == 0,
        details={
            "components_succeeded": passed,
            "components_failed": failed,
            "components_skipped": skipped,
            "dry_run": dry_run,
        },
    )
    _journal_safe(j.end_command, success=failed == 0)

    deploy_elapsed = int(time.time() - deploy_start)

    if failed == 0:
        # Build success message
        success_msg = f"[green]{passed} components deployed in {deploy_elapsed}s[/green]"

        # Add monitoring access info if observability was deployed
        obs_result = next(
            (
                r
                for r in results
                if r.component == "observability" and r.status == DeploymentStatus.SUCCESS
            ),
            None,
        )
        if obs_result and obs_result.details:
            # The stack is shared and lives in its own namespace.
            obs_ns = obs_result.details.get("release_namespace") or cfg.get_namespace()
            success_msg += (
                f"\n\n[bold]Monitoring (shared stack in namespace {obs_ns}):[/bold]"
                f"\n  Services: [cyan]kubectl get svc -n {obs_ns} -l release=lakebench-observability[/cyan]"
                f"\n  Grafana login: admin / lakebench"
                f"\n  Local access: [cyan]kubectl port-forward -n {obs_ns} svc/<grafana service> 3000:80[/cyan]"
            )

        success_msg += (
            "\n\nNext: [bold]lakebench generate[/bold]      to create test data"
            "\n      [bold]lakebench run --generate[/bold]  to generate data and run the pipeline"
            "\n      [bold]lakebench status[/bold]          to check deployment"
        )

        console.print(
            Panel(
                success_msg,
                title="Deployment Successful",
                expand=False,
            )
        )
    else:
        failed_component = next((r for r in results if r.status == DeploymentStatus.FAILED), None)
        guidance = "Check the errors above, then re-run 'lakebench deploy'."
        if failed_component:
            guidance = (
                f"Failed at: {failed_component.component}\n"
                "Fix the issue above, then re-run 'lakebench deploy'.\n"
                "Successful steps will be skipped on retry."
            )
        console.print(
            Panel(
                f"[red]{failed} failed[/red], {passed} succeeded" + f"\n\n{guidance}",
                title="Deployment Failed",
                expand=False,
            )
        )
        raise typer.Exit(1)
    return recorded
