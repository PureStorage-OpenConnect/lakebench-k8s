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
    console,
    journal_open,
    print_error,
    print_info,
    resolve_config_path,
)
from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    load_config,
)
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
) -> None:
    """Deploy lakehouse infrastructure.

    Deploys all components in the correct order:
    1. Namespace + Secrets + Scratch StorageClass
    2. PostgreSQL
    3. Catalog (Hive Metastore or Polaris)
    4. Spark RBAC
    5. Spark Operator (if operator.install: true)
    6. Query Engine (Trino / Spark Thrift / DuckDB)
    7. Observability (if enabled)
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

    # Enable observability if flag is set
    if include_observability:
        cfg.observability.enabled = True

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
    try:
        engine = DeploymentEngine(cfg, dry_run=dry_run)

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

        results = engine.deploy_all(progress_callback=on_progress, timeout=timeout)

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
            namespace = cfg.get_namespace()
            success_msg += (
                f"\n\n[bold]Monitoring (in-cluster):[/bold]"
                f"\n  Grafana:    http://lakebench-grafana.{namespace}.svc:3000 (admin / lakebench)"
                f"\n  Prometheus: http://lakebench-prometheus.{namespace}.svc:9090"
                f"\n\n[bold]Local access via port-forward:[/bold]"
                f"\n  [cyan]kubectl port-forward svc/lakebench-grafana 3000:3000 -n {namespace}[/cyan]"
                f"\n  [cyan]kubectl port-forward svc/lakebench-prometheus 9090:9090 -n {namespace}[/cyan]"
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
