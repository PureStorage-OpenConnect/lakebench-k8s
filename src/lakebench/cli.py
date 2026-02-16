"""Lakebench CLI."""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from lakebench import __version__
from lakebench._constants import DEFAULT_OUTPUT_DIR
from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    generate_example_config_yaml,
    load_config,
    parse_spark_memory,
)
from lakebench.journal import DEFAULT_JOURNAL_DIR, CommandName, EventType, Journal
from lakebench.k8s import K8sConnectionError, PlatformType, SecurityVerifier, get_k8s_client
from lakebench.s3 import test_s3_connectivity

# Default config file name for auto-discovery
DEFAULT_CONFIG = "lakebench.yaml"

logger = logging.getLogger(__name__)

# Global journal instance (lazy-initialized)
_journal: Journal | None = None

# ANSI escape code stripper for log output
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    return _ANSI_RE.sub("", text)


app = typer.Typer(
    name="lakebench",
    help="Deploy and benchmark lakehouse architectures on Kubernetes",
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="rich",
    epilog="[dim]Workflow: init -> validate -> deploy -> generate -> run -> report -> destroy[/dim]",
)

console = Console()


# =============================================================================
# Helper Functions
# =============================================================================


def resolve_config_path(
    config_file: Path | None,
    file_option: Path | None = None,
) -> Path:
    """Resolve config file path, using ./lakebench.yaml as default.

    Supports both positional argument and --file/-f option.
    If both are provided, --file takes precedence.
    """
    path = file_option or config_file
    if path is not None:
        return path

    default = Path(DEFAULT_CONFIG)
    if default.exists():
        return default

    console.print(f"[red]ERROR[/red] No config file specified and ./{DEFAULT_CONFIG} not found")
    console.print("[blue]INFO[/blue] Create one with: lakebench init")
    raise typer.Exit(1)


def get_journal() -> Journal:
    """Get or create the global journal instance."""
    global _journal
    if _journal is None:
        _journal = Journal()
    return _journal


def journal_open(config_path: Path | None, config_name: str = "") -> Journal:
    """Open journal session for a command that loads config."""
    j = get_journal()
    j.open_session(config_path=config_path, config_name=config_name)
    return j


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[green]OK[/green] {message}")


def print_error(message: str) -> None:
    """Print an error message."""
    console.print(f"[red]ERROR[/red] {message}")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[yellow]WARN[/yellow] {message}")


def print_info(message: str) -> None:
    """Print an info message."""
    console.print(f"[blue]...[/blue] {message}")


_journal_warned = False


def _journal_safe(fn, *args, **kwargs) -> None:
    """Call a journal function, logging failures instead of silently dropping them."""
    global _journal_warned
    try:
        fn(*args, **kwargs)
    except Exception:
        if not _journal_warned:
            logger.debug("Journal write failed (further warnings suppressed)", exc_info=True)
            _journal_warned = True


def _preflight_check(cfg) -> None:
    """Run critical pre-flight checks before deployment.

    Prints warnings for non-critical issues; exits on blockers.
    """
    # 1. S3 endpoint must be set
    s3 = cfg.platform.storage.s3
    if not s3.endpoint:
        print_error("S3 endpoint not configured (platform.storage.s3.endpoint)")
        print_info("Run 'lakebench validate' for detailed diagnostics")
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
            required = {
                "hiveclusters.hive.stackable.tech": "hive-operator",
                "secretclasses.secrets.stackable.tech": "secret-operator",
            }
            _missing_stackable = [op for crd, op in required.items() if crd not in crd_names]
        except Exception:
            logger.debug("Preflight CRD check skipped (K8s not reachable)", exc_info=True)

        if _missing_stackable:
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
                "  Set recipe: polaris-iceberg-trino in your config"
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
    if cfg.observability.enabled:
        if cfg.observability.prometheus_stack_enabled:
            parts.append("Prometheus")
        if cfg.observability.dashboards_enabled:
            parts.append("Grafana")
    return ", ".join(parts)


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


# =============================================================================
# CLI Commands
# =============================================================================


@app.command()
def version() -> None:
    """Show version information."""
    console.print(f"Lakebench version {__version__}")


@app.command()
def init(
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output file path for configuration",
        ),
    ] = Path("lakebench.yaml"),
    name: Annotated[
        str,
        typer.Option(
            "--name",
            "-n",
            help="Deployment name",
        ),
    ] = "my-lakehouse",
    scale: Annotated[
        int,
        typer.Option(
            "--scale",
            "-s",
            help="Scale factor (1 = ~10 GB, 100 = ~1 TB, 1000 = ~10 TB)",
        ),
    ] = 10,
    endpoint: Annotated[
        str,
        typer.Option(
            "--endpoint",
            help="S3 endpoint URL (e.g. http://your-s3-endpoint:80)",
        ),
    ] = "",
    access_key: Annotated[
        str,
        typer.Option(
            "--access-key",
            help="S3 access key",
        ),
    ] = "",
    secret_key: Annotated[
        str,
        typer.Option(
            "--secret-key",
            help="S3 secret key",
        ),
    ] = "",
    namespace: Annotated[
        str,
        typer.Option(
            "--namespace",
            help="Kubernetes namespace (default: same as deployment name)",
        ),
    ] = "",
    recipe: Annotated[
        str,
        typer.Option(
            "--recipe",
            "-r",
            help="Architecture recipe (e.g. hive-iceberg-trino, default)",
        ),
    ] = "",
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Guided setup -- prompts for all required values",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Overwrite existing file",
        ),
    ] = False,
) -> None:
    """Generate a starter configuration file.

    Creates a well-documented YAML configuration file with sensible defaults
    that you can customize for your environment.

    Use --interactive for guided setup, or pass values directly with flags:

        lakebench init --endpoint http://your-s3-endpoint:80 --access-key AAA --secret-key BBB

    Use 'lakebench recommend' first to find the right scale for your cluster.
    """
    if output.exists() and not force:
        print_error(f"File already exists: {output}")
        print_info("Use --force to overwrite")
        raise typer.Exit(1)

    # Interactive mode: prompt for values not already provided via flags
    if interactive:
        console.print(Panel("Lakebench Configuration Setup", expand=False))
        console.print()

        if not name or name == "my-lakehouse":
            name = typer.prompt("Deployment name", default="my-lakehouse")

        # Recipe selection
        if not recipe:
            from lakebench.config.recipes import RECIPE_DESCRIPTIONS

            console.print()
            console.print("[bold]Choose a recipe:[/bold]")
            choices = list(RECIPE_DESCRIPTIONS.items())
            for i, (rname, desc) in enumerate(choices, 1):
                console.print(f"  [cyan]{i}[/cyan]. {rname:30s} {desc}")
            console.print(
                f"  [cyan]{len(choices) + 1}[/cyan]. {'custom':30s} Pick components individually"
            )
            console.print()
            choice = typer.prompt(
                "Recipe number",
                default=1,
                type=int,
            )
            if 1 <= choice <= len(choices):
                recipe = choices[choice - 1][0]
            # else: custom -- no recipe set

        console.print()
        console.print("[dim]S3 endpoint examples:[/dim]")
        console.print("[dim]  FlashBlade: http://your-s3-endpoint:80[/dim]")
        console.print("[dim]  MinIO:      http://minio:9000[/dim]")
        console.print("[dim]  AWS S3:     https://s3.us-east-1.amazonaws.com[/dim]")
        if not endpoint:
            endpoint = typer.prompt("S3 endpoint URL")

        if not access_key:
            access_key = typer.prompt("S3 access key")

        if not secret_key:
            secret_key = typer.prompt("S3 secret key", hide_input=True)

        if not namespace:
            namespace = typer.prompt("Kubernetes namespace", default=name)

        console.print()
        console.print("[dim]Scale factor: 1 unit = ~10 GB bronze data[/dim]")
        console.print("[dim]  1 = ~10 GB, 10 = ~100 GB, 100 = ~1 TB, 1000 = ~10 TB[/dim]")
        if scale == 10:  # only prompt if default wasn't overridden by flag
            scale = typer.prompt("Scale factor", default=10, type=int)

        console.print()

    # Generate config with substitutions
    import re

    config_content = generate_example_config_yaml()
    config_content = config_content.replace("name: my-lakehouse", f"name: {name}")
    config_content = re.sub(r"scale:\s*\d+", f"scale: {scale}", config_content)

    # Inject recipe line after name
    if recipe:
        config_content = config_content.replace(
            f"name: {name}",
            f"name: {name}\nrecipe: {recipe}",
        )

    # Fill in S3 and namespace values if provided
    if endpoint:
        config_content = config_content.replace('endpoint: ""', f'endpoint: "{endpoint}"', 1)
    if access_key:
        config_content = config_content.replace('access_key: ""', f'access_key: "{access_key}"')
    if secret_key:
        config_content = config_content.replace('secret_key: ""', f'secret_key: "{secret_key}"')
    if namespace:
        config_content = config_content.replace('namespace: ""', f'namespace: "{namespace}"', 1)

    output.write_text(config_content)
    print_success(f"Created configuration file: {output}")
    print_info(f"Scale: {scale} (~{scale * 10} GB)")
    if not endpoint:
        print_info("Edit the file to configure your S3 endpoint and credentials")
    print_info("Then run: lakebench validate")
    print_info("Run 'lakebench recommend' to find the optimal scale for your cluster")


@app.command()
def validate(
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
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Show detailed validation output",
        ),
    ] = False,
) -> None:
    """Validate configuration and test connectivity.

    This command performs the following checks:
    - YAML syntax is valid
    - Required fields are present
    - S3 endpoint is reachable
    - S3 credentials are valid
    - Kubernetes context is accessible
    - Kubernetes namespace is accessible or can be created
    """
    config_file = resolve_config_path(config_file, file_option)
    console.print(Panel(f"Validating: [bold]{config_file}[/bold]", expand=False))

    # Journal (opened early so we can record config_loaded)
    j = journal_open(config_file, config_name="")
    j.begin_command(CommandName.VALIDATE, {"verbose": verbose})

    # Track validation results
    checks_passed = 0
    checks_failed = 0
    checks_warned = 0

    # 1. Load and validate config
    console.print("\n[bold]Configuration[/bold]")
    try:
        cfg = load_config(config_file)
        print_success("Config syntax valid")
        checks_passed += 1

        if verbose:
            console.print(f"  Name: {cfg.name}")
            console.print(f"  Namespace: {cfg.get_namespace()}")
    except ConfigFileNotFoundError as e:
        print_error(f"File not found: {e}")
        raise typer.Exit(1)  # noqa: B904
    except ConfigValidationError as e:
        print_error("Config validation failed:")
        for err in e.errors:
            loc = ".".join(str(x) for x in err["loc"])
            console.print(f"  [red]•[/red] {loc}: {err['msg']}")
        raise typer.Exit(1)  # noqa: B904
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    # 1b. Check required CLI tools
    console.print("\n[bold]CLI Tools[/bold]")
    for tool in ["kubectl", "helm"]:
        if shutil.which(tool):
            print_success(f"{tool} found on PATH")
            checks_passed += 1
        else:
            print_error(f"{tool} not found on PATH")
            checks_failed += 1

    if not cfg.platform.storage.s3.endpoint:
        print_warning("S3 endpoint not configured")
        checks_failed += 1
    else:
        print_success(f"S3 endpoint configured: {cfg.platform.storage.s3.endpoint}")
        checks_passed += 1

    if not cfg.has_inline_s3_credentials() and not cfg.has_s3_secret_ref():
        print_warning("S3 credentials not configured (neither inline nor secret_ref)")
        checks_failed += 1
    else:
        print_success("S3 credentials configured")
        checks_passed += 1

    # 2. Test S3 connectivity
    console.print("\n[bold]S3 Connectivity[/bold]")
    s3 = cfg.platform.storage.s3

    if not s3.endpoint:
        print_warning("Skipping S3 tests (no endpoint configured)")
    elif not cfg.has_inline_s3_credentials():
        print_warning("Skipping S3 tests (no inline credentials)")
    else:
        results = test_s3_connectivity(
            endpoint=s3.endpoint,
            access_key=s3.access_key,
            secret_key=s3.secret_key,
            region=s3.region,
            path_style=s3.path_style,
        )

        if results["endpoint_reachable"]:
            print_success(results["endpoint_message"])
            checks_passed += 1
        else:
            print_error(results["endpoint_message"])
            checks_failed += 1

        if results["credentials_valid"]:
            print_success(results["credentials_message"])
            checks_passed += 1

            if verbose and results["buckets"]:
                console.print(f"  Existing buckets: {', '.join(results['buckets'])}")
        elif results["endpoint_reachable"]:
            print_error(results["credentials_message"])
            checks_failed += 1

    # 2.5. Validate S3 secret reference if configured
    if cfg.has_s3_secret_ref():
        secret_name = cfg.platform.storage.s3.secret_ref
        console.print("\n[bold]S3 Secret Reference[/bold]")
        try:
            k8s = get_k8s_client(
                context=cfg.platform.kubernetes.context,
                namespace=cfg.get_namespace(),
            )
            if k8s.secret_exists(secret_name, cfg.get_namespace()):
                print_success(f"S3 secret '{secret_name}' exists in namespace")
                checks_passed += 1
            else:
                print_error(
                    f"S3 secret '{secret_name}' not found in namespace '{cfg.get_namespace()}'"
                )
                checks_failed += 1
        except K8sConnectionError:
            print_warning("Cannot verify S3 secret (K8s not connected yet)")
        except Exception as e:
            print_warning(f"Cannot verify S3 secret: {e}")

    # 2.6. Validate bucket name overlap
    bucket_names = [s3.buckets.bronze, s3.buckets.silver, s3.buckets.gold]
    if len(set(bucket_names)) < 3:
        print_warning("Bronze, silver, and gold use overlapping bucket names")
        checks_warned += 1

    # 3. Test Kubernetes connectivity
    console.print("\n[bold]Kubernetes Connectivity[/bold]")
    try:
        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )

        # Test connectivity
        connected, msg = k8s.test_connectivity()
        if connected:
            print_success(msg)
            checks_passed += 1
        else:
            print_error(msg)
            checks_failed += 1

        # Show context info
        ctx = k8s.get_current_context()
        if ctx and verbose:
            console.print(f"  Context: {ctx.name}")
            console.print(f"  Cluster: {ctx.cluster}")

        ns = cfg.get_namespace()
        if k8s.namespace_exists(ns):
            print_success(f"Namespace '{ns}' exists")
            checks_passed += 1
        else:
            can_create, msg = k8s.can_create_namespace(ns)
            if can_create:
                print_success(f"Namespace '{ns}' can be created")
                checks_passed += 1
            else:
                print_error(f"Cannot create namespace '{ns}': {msg}")
                checks_failed += 1

        # 4. Platform security verification
        console.print("\n[bold]Platform Security[/bold]")
        verifier = SecurityVerifier(k8s)
        platform = verifier.detect_platform()
        platform_version = verifier.get_platform_version()

        platform_info = f"{platform.value}"
        if platform_version:
            platform_info += f" {platform_version}"
        print_success(f"Platform detected: {platform_info}")
        checks_passed += 1

        # Verify security requirements
        security_result = verifier.verify_security(ns)

        if platform == PlatformType.OPENSHIFT:
            # Show SCC status on OpenShift
            for scc in security_result.scc_status:
                if scc.assigned:
                    print_success(f"SCC '{scc.name}' assigned to '{scc.service_account}'")
                else:
                    print_warning(f"SCC '{scc.name}' not assigned to '{scc.service_account}'")
                    checks_warned += 1
                    if verbose:
                        console.print(
                            f"  Fix: oc adm policy add-scc-to-user {scc.name} -z {scc.service_account} -n {ns}"
                        )

        # Report issues and warnings
        for issue in security_result.issues:
            if verbose:
                print_warning(issue)

        for warning in security_result.warnings:
            if verbose:
                print_info(warning)

        if security_result.passed:
            print_success("Platform security checks passed")
            checks_passed += security_result.checks_passed
        else:
            print_warning(
                f"Platform security: {security_result.checks_passed} passed, {security_result.checks_failed} need attention"
            )
            if platform == PlatformType.OPENSHIFT:
                print_info("SCC will be configured automatically during deploy")
            checks_passed += security_result.checks_passed
            # Don't fail on security warnings - they'll be fixed during deploy

        if verbose and security_result.recommendations:
            console.print("\n[dim]Recommendations:[/dim]")
            for rec in security_result.recommendations:
                console.print(f"  [dim]• {rec}[/dim]")

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        checks_failed += 1

    # 5. Storage Class Validation
    console.print("\n[bold]Storage Classes[/bold]")
    try:
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        storage_v1 = k8s_client.StorageV1Api()

        storage_classes = []
        scratch_cfg = cfg.platform.storage.scratch
        if scratch_cfg.enabled and scratch_cfg.storage_class:
            # If create_storage_class is True, it will be created during deploy
            storage_classes.append(
                (scratch_cfg.storage_class, "scratch", not scratch_cfg.create_storage_class)
            )

        pg_sc = cfg.platform.compute.postgres.storage_class
        if pg_sc:
            storage_classes.append((pg_sc, "postgres", True))

        if not storage_classes:
            print_info("No custom storage classes configured (using cluster defaults)")
        else:
            for sc_name, purpose, required in storage_classes:
                try:
                    storage_v1.read_storage_class(sc_name)
                    print_success(f"StorageClass '{sc_name}' exists ({purpose})")
                    checks_passed += 1
                except ApiException as e:
                    if e.status == 404:
                        if required:
                            print_error(f"StorageClass '{sc_name}' not found ({purpose})")
                            checks_failed += 1
                        else:
                            print_warning(
                                f"StorageClass '{sc_name}' will be created during deploy ({purpose})"
                            )
                            checks_passed += 1
                            checks_warned += 1
                    else:
                        print_warning(f"Could not check StorageClass '{sc_name}': {e.reason}")
    except Exception as e:
        print_warning(f"Could not validate storage classes: {e}")

    # 5b. Catalog Prerequisites (Hive needs Stackable operators)
    if cfg.architecture.catalog.type.value == "hive":
        console.print("\n[bold]Catalog Prerequisites[/bold]")
        try:
            from kubernetes import client as k8s_client

            api_ext = k8s_client.ApiextensionsV1Api()
            crds = api_ext.list_custom_resource_definition()
            crd_names = {crd.metadata.name for crd in crds.items}
            required = {
                "hiveclusters.hive.stackable.tech": "hive-operator",
                "secretclasses.secrets.stackable.tech": "secret-operator",
            }
            missing = [op for crd, op in required.items() if crd not in crd_names]
            if missing:
                print_error(f"Missing Stackable operators: {', '.join(missing)}")
                print_info("Install with:")
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
                checks_failed += 1
            else:
                print_success("Stackable operators installed (Hive catalog)")
                checks_passed += 1
        except Exception:
            print_warning("Could not verify Stackable operators (K8s not reachable)")
    elif cfg.architecture.catalog.type.value == "polaris":
        console.print("\n[bold]Catalog Prerequisites[/bold]")
        print_success("Polaris catalog (no external operators needed)")
        checks_passed += 1

    # 6. Spark Operator Status
    console.print("\n[bold]Spark Operator[/bold]")
    try:
        from lakebench.spark import SparkOperatorManager

        spark_op_cfg = cfg.platform.compute.spark.operator
        operator = SparkOperatorManager(
            namespace=spark_op_cfg.namespace,
            version=spark_op_cfg.version,
            job_namespace=cfg.get_namespace(),
        )
        status = operator.check_status()

        if status.ready:
            version_info = f" (v{status.version})" if status.version else ""
            print_success(f"Spark Operator ready in '{status.namespace}'{version_info}")
            checks_passed += 1

            # Check namespace watching
            if status.watching_namespace is False:
                existing = status.watched_namespaces or []
                new_list = ",".join(existing + [cfg.get_namespace()])
                fix_cmd = (
                    f"helm upgrade {operator.HELM_RELEASE_NAME} "
                    f"{operator.HELM_CHART_NAME} "
                    f"-n {spark_op_cfg.namespace} --reuse-values "
                    f"--set 'spark.jobNamespaces={{{new_list}}}'"
                )
                if spark_op_cfg.install:
                    print_warning(
                        f"Spark Operator does NOT watch namespace '{cfg.get_namespace()}'"
                    )
                    print_info(f"Currently watching: {existing}")
                    print_info("Namespace will be added automatically during run (install: true)")
                    checks_warned += 1
                else:
                    print_error(f"Spark Operator does NOT watch namespace '{cfg.get_namespace()}'")
                    print_info(f"Currently watching: {existing}")
                    print_info(f"SparkApplications will hang. Fix with:\n  {fix_cmd}")
                    checks_failed += 1
            elif status.watching_namespace is None:
                print_info("Could not verify namespace watching (helm values unavailable)")
        elif status.installed:
            print_warning(f"Spark Operator installed but not ready: {status.message}")
            if spark_op_cfg.install:
                print_info("Operator will be repaired during deploy")
            checks_passed += 1  # Non-blocking warning
            checks_warned += 1
        else:
            if spark_op_cfg.install:
                print_warning(f"Spark Operator not installed: {status.message}")
                print_info("Operator will be auto-installed during deploy (install: true)")
                checks_passed += 1  # Non-blocking if install=True
                checks_warned += 1
            else:
                ns = cfg.get_namespace()
                print_error("Spark Operator not installed")
                print_info(
                    "Option 1: Set platform.compute.spark.operator.install: true (auto-install)"
                )
                print_info("Option 2: Install manually:")
                console.print(
                    f"  helm repo add spark-operator https://kubeflow.github.io/spark-operator\n"
                    f"  helm install spark-operator spark-operator/spark-operator \\\n"
                    f"    --namespace spark-operator --create-namespace \\\n"
                    f"    --set 'spark.jobNamespaces={{{ns}}}' \\\n"
                    f"    --set webhook.enable=true"
                )
                checks_failed += 1
    except Exception as e:
        print_warning(f"Could not check Spark Operator status: {e}")

    # Compute vs Scale validation
    console.print("\n[bold]Compute Adequacy[/bold]")
    try:
        from lakebench.config.scale import compute_guidance as _cg

        scale = cfg.architecture.workload.datagen.get_effective_scale()
        dims = cfg.get_scale_dimensions()
        guidance = _cg(scale)

        actual_executors = cfg.platform.compute.spark.executor.instances
        actual_memory = cfg.platform.compute.spark.executor.memory
        actual_mem_bytes = parse_spark_memory(actual_memory)
        rec_mem_bytes = parse_spark_memory(guidance.recommended_memory)
        min_mem_bytes = parse_spark_memory(guidance.min_memory)

        # Executors
        if actual_executors >= guidance.recommended_executors:
            print_success(
                f"Executors: {actual_executors} "
                f"(recommended {guidance.recommended_executors} for scale {scale})"
            )
            checks_passed += 1
        elif actual_executors >= guidance.min_executors:
            console.print(
                f"  [yellow]![/yellow] Executors: {actual_executors} "
                f"(min {guidance.min_executors}, rec {guidance.recommended_executors} "
                f"for scale {scale})"
            )
            checks_passed += 1  # warning, not failure
            checks_warned += 1
        else:
            print_error(
                f"Executors: {actual_executors} below minimum "
                f"{guidance.min_executors} for scale {scale}"
            )
            checks_failed += 1

        # Memory
        if actual_mem_bytes >= rec_mem_bytes:
            print_success(
                f"Memory: {actual_memory} "
                f"(recommended {guidance.recommended_memory} for scale {scale})"
            )
            checks_passed += 1
        elif actual_mem_bytes >= min_mem_bytes:
            console.print(
                f"  [yellow]![/yellow] Memory: {actual_memory} "
                f"(min {guidance.min_memory}, rec {guidance.recommended_memory} "
                f"for scale {scale})"
            )
            checks_passed += 1
            checks_warned += 1
        else:
            print_error(
                f"Memory: {actual_memory} below minimum {guidance.min_memory} for scale {scale}"
            )
            checks_failed += 1

        print_info(
            f"Scale {scale}: ~{dims.approx_bronze_gb:.0f} GB bronze, "
            f"{dims.customers:,} customers, {dims.approx_rows:,} rows "
            f"[{guidance.tier_name} tier]"
        )

        if guidance.warning:
            console.print(f"  [yellow]{guidance.warning}[/yellow]")

    except Exception as e:
        console.print(f"  [yellow]Could not validate compute adequacy: {e}[/yellow]")

    # Summary
    console.print("\n[bold]Summary[/bold]")
    if checks_failed == 0:
        if checks_warned > 0:
            warn_s = "s" if checks_warned > 1 else ""
            _journal_safe(
                j.end_command,
                success=True,
                message=f"{checks_passed} passed, {checks_warned} warning{warn_s}",
            )
            console.print(
                Panel(
                    f"[green]{checks_passed} passed[/green], "
                    f"[yellow]{checks_warned} warning{warn_s}[/yellow]\n"
                    f"Run [bold]lakebench deploy[/bold] to deploy",
                    title="Validation Passed",
                    expand=False,
                )
            )
        else:
            _journal_safe(j.end_command, success=True, message=f"All {checks_passed} checks passed")
            console.print(
                Panel(
                    f"[green]All {checks_passed} checks passed[/green]\n"
                    f"Run [bold]lakebench deploy[/bold] to deploy",
                    title="Validation Successful",
                    expand=False,
                )
            )
    else:
        _journal_safe(
            j.end_command, success=False, message=f"{checks_passed} passed, {checks_failed} failed"
        )
        console.print(
            Panel(
                f"[green]{checks_passed} passed[/green], [red]{checks_failed} failed[/red]\n"
                f"Fix the issues above before deploying",
                title="Validation Failed",
                expand=False,
            )
        )
        raise typer.Exit(1)


@app.command()
def status(
    config_file: Annotated[
        Path | None,
        typer.Argument(
            help="Path to configuration YAML file (optional)",
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
    namespace: Annotated[
        str | None,
        typer.Option(
            "--namespace",
            "-n",
            help="Kubernetes namespace to check",
        ),
    ] = None,
) -> None:
    """Show deployment status.

    Shows the current status of Lakebench components in the cluster.
    """
    # Determine namespace
    ns = namespace
    if not ns:
        config_file = resolve_config_path(config_file, file_option)
    if config_file:
        try:
            cfg = load_config(config_file)
            ns = ns or cfg.get_namespace()
        except ConfigError as e:
            print_error(f"Config error: {e}")
            raise typer.Exit(1)  # noqa: B904

    if not ns:
        print_error("Specify --namespace or provide a config file")
        raise typer.Exit(1)

    console.print(Panel(f"Status for namespace: [bold]{ns}[/bold]", expand=False))

    try:
        k8s = get_k8s_client(namespace=ns)

        # Check if namespace exists
        if not k8s.namespace_exists(ns):
            print_warning(f"Namespace '{ns}' does not exist")
            print_info("Run 'lakebench deploy' to create the deployment")
            return

        # Check components
        from kubernetes import client as k8s_client

        apps_v1 = k8s_client.AppsV1Api()

        table = Table(title="Components")
        table.add_column("Component", style="cyan")
        table.add_column("Type", style="dim")
        table.add_column("Status", style="bold")
        table.add_column("Ready", justify="center")

        # Build component list -- config-aware when a config file was loaded
        if config_file:
            components: list[tuple[str, str]] = [("lakebench-postgres", "StatefulSet")]
            cat = cfg.architecture.catalog.type.value
            if cat == "hive":
                components.append(("lakebench-hive-metastore-default", "StatefulSet"))
            elif cat == "polaris":
                components.append(("lakebench-polaris", "Deployment"))
            engine = cfg.architecture.query_engine.type.value
            if engine == "trino":
                components.append(("lakebench-trino-coordinator", "Deployment"))
                components.append(("lakebench-trino-worker", "StatefulSet"))
            elif engine == "spark-thrift":
                components.append(("lakebench-spark-thrift", "Deployment"))
            elif engine == "duckdb":
                components.append(("lakebench-duckdb", "Deployment"))
            if cfg.observability.enabled:
                components.append(("lakebench-observability-prometheus", "StatefulSet"))
                components.append(("lakebench-observability-grafana", "Deployment"))
        else:
            # Namespace-only mode (no config loaded) -- show all possible components
            components = [
                ("lakebench-postgres", "StatefulSet"),
                ("lakebench-hive-metastore-default", "StatefulSet"),
                ("lakebench-polaris", "Deployment"),
                ("lakebench-trino-coordinator", "Deployment"),
                ("lakebench-trino-worker", "StatefulSet"),
                ("lakebench-spark-thrift", "Deployment"),
                ("lakebench-duckdb", "Deployment"),
                ("lakebench-observability-prometheus", "StatefulSet"),
                ("lakebench-observability-grafana", "Deployment"),
            ]

        for name, kind in components:
            try:
                if kind == "StatefulSet":
                    sts = apps_v1.read_namespaced_stateful_set(name, ns)
                    ready = sts.status.ready_replicas or 0
                    desired = sts.spec.replicas or 1
                    status_str = f"{ready}/{desired} replicas"
                    ready_str = "[green]OK[/green]" if ready >= desired else "[yellow]--[/yellow]"
                else:
                    dep = apps_v1.read_namespaced_deployment(name, ns)
                    ready = dep.status.ready_replicas or 0
                    desired = dep.spec.replicas or 1
                    status_str = f"{ready}/{desired} replicas"
                    ready_str = "[green]OK[/green]" if ready >= desired else "[yellow]--[/yellow]"

                table.add_row(name, kind, status_str, ready_str)
            except k8s_client.rest.ApiException as e:
                if e.status == 404:
                    table.add_row(name, kind, "Not found", "[dim]-[/dim]")
                else:
                    table.add_row(name, kind, f"Error: {e.reason}", "[red]ERROR[/red]")

        console.print(table)

        # Check for datagen job status
        try:
            batch_v1 = k8s_client.BatchV1Api()
            job = batch_v1.read_namespaced_job("lakebench-datagen", ns)
            active = job.status.active or 0
            succeeded = job.status.succeeded or 0
            completions = job.spec.completions or 1
            if active > 0 or succeeded < completions:
                console.print()
                console.print(
                    f"[bold]Datagen:[/bold] {succeeded}/{completions} pods completed, "
                    f"{active} active"
                )
        except k8s_client.rest.ApiException as e:
            if e.status != 404:
                logger.debug("Could not check datagen job: %s", e)

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        raise typer.Exit(1)  # noqa: B904


@app.command()
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
) -> None:
    """Deploy lakehouse infrastructure.

    Deploys all components in the correct order:
    1. Namespace + Secrets
    2. PostgreSQL
    3. Hive Metastore
    4. Trino
    5. Spark RBAC
    6. Prometheus (if --include-observability or config enabled)
    7. Grafana (if --include-observability or config enabled)
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
    if dry_run:
        console.print(
            Panel(f"[yellow]DRY RUN[/yellow] - Deploying: [bold]{cfg.name}[/bold]", expand=False)
        )
    else:
        console.print(Panel(f"Deploying: [bold]{cfg.name}[/bold]", expand=False))

    console.print(f"Namespace: {namespace}")
    console.print(f"Config: {config_file.resolve()}")
    if cfg.observability.enabled:
        console.print("Observability: [green]Prometheus + Grafana[/green]")
    console.print()

    # Progress callback
    def on_progress(component: str, status: DeploymentStatus, message: str) -> None:
        if status == DeploymentStatus.IN_PROGRESS:
            console.print(f"[blue]...[/blue] {message}")
        elif status == DeploymentStatus.SUCCESS:
            print_success(message)
        elif status == DeploymentStatus.FAILED:
            print_error(message)
        elif status == DeploymentStatus.SKIPPED:
            print_warning(message)

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.DEPLOY, {"dry_run": dry_run})

    # Deploy
    try:
        engine = DeploymentEngine(cfg, dry_run=dry_run)
        results = engine.deploy_all(progress_callback=on_progress)

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

    if failed == 0:
        # Build success message
        success_msg = f"[green]{passed} components deployed[/green]" + (
            f", {skipped} skipped" if skipped else ""
        )

        # Add monitoring access info if observability was deployed
        grafana_result = next(
            (
                r
                for r in results
                if r.component == "grafana" and r.status == DeploymentStatus.SUCCESS
            ),
            None,
        )
        next(
            (
                r
                for r in results
                if r.component == "prometheus" and r.status == DeploymentStatus.SUCCESS
            ),
            None,
        )
        if grafana_result and grafana_result.details:
            namespace = cfg.get_namespace()
            success_msg += (
                f"\n\n[bold]Monitoring (in-cluster):[/bold]"
                f"\n  Grafana:    http://lakebench-grafana.{namespace}.svc:3000 (admin / lakebench)"
                f"\n  Prometheus: http://lakebench-prometheus.{namespace}.svc:9090"
                f"\n\n[bold]Local access via port-forward:[/bold]"
                f"\n  [cyan]kubectl port-forward svc/lakebench-grafana 3000:3000 -n {namespace}[/cyan]"
                f"\n  [cyan]kubectl port-forward svc/lakebench-prometheus 9090:9090 -n {namespace}[/cyan]"
            )

        success_msg += "\n\nRun [bold]lakebench status[/bold] to check deployment"

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
                "Previously successful steps are idempotent and will be skipped on retry."
            )
        console.print(
            Panel(
                f"[red]{failed} failed[/red], {passed} succeeded"
                + (f", {skipped} skipped" if skipped else "")
                + f"\n\n{guidance}",
                title="Deployment Failed",
                expand=False,
            )
        )
        raise typer.Exit(1)


@app.command()
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

    namespace = cfg.get_namespace()

    # Confirmation
    if not force:
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

    console.print(Panel(f"Destroying: [bold]{cfg.name}[/bold]", expand=False))
    console.print(f"Config: {config_file.resolve()}")

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.DESTROY, {"force": force})

    # Progress callback
    def on_progress(component: str, status: DeploymentStatus, message: str) -> None:
        if status == DeploymentStatus.IN_PROGRESS:
            console.print(f"[blue]...[/blue] {message}")
        elif status == DeploymentStatus.SUCCESS:
            print_success(message)
            _journal_safe(
                j.record,
                EventType.DESTROY_COMPONENT,
                message=message,
                success=True,
                details={"component": component, "status": "success"},
            )
        elif status == DeploymentStatus.FAILED:
            print_error(message)
            _journal_safe(
                j.record,
                EventType.DESTROY_COMPONENT,
                message=message,
                success=False,
                details={"component": component, "status": "failed"},
            )

    # Destroy
    try:
        engine = DeploymentEngine(cfg)
        results = engine.destroy_all(progress_callback=on_progress)
    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    # Summary
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
                f"[green]All {passed} components destroyed[/green]",
                title="Destruction Complete",
                expand=False,
            )
        )
    else:
        console.print(
            Panel(
                f"[red]{failed} failed[/red], {passed} succeeded\n\n"
                f"Some resources may need manual cleanup",
                title="Destruction Incomplete",
                expand=False,
            )
        )
        raise typer.Exit(1)


# Valid clean targets
CLEAN_TARGETS = ["bronze", "silver", "gold", "data", "metrics", "journal"]


@app.command()
def clean(
    target: Annotated[
        str,
        typer.Argument(
            help=f"What to clean: {', '.join(CLEAN_TARGETS)}",
        ),
    ],
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
    metrics_dir: Annotated[
        Path,
        typer.Option(
            "--metrics-dir",
            "-m",
            help="Metrics/runs directory (for 'metrics' target)",
        ),
    ] = Path(DEFAULT_OUTPUT_DIR) / "runs",
) -> None:
    """Delete data without destroying infrastructure.

    Granular data cleanup for re-running pipeline stages.

    Targets:
      bronze  - Empty the bronze S3 bucket
      silver  - Empty the silver S3 bucket
      gold    - Empty the gold S3 bucket
      data    - Empty all three buckets (bronze + silver + gold)
      metrics - Delete local metrics/runs directory
      journal - Delete all journal session files
    """
    target = target.lower().strip()
    if target not in CLEAN_TARGETS:
        print_error(f"Invalid target: '{target}'. Must be one of: {', '.join(CLEAN_TARGETS)}")
        raise typer.Exit(1)

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

    s3_cfg = cfg.platform.storage.s3

    # Determine which buckets to clean
    if target == "data":
        bucket_targets = {
            "bronze": s3_cfg.buckets.bronze,
            "silver": s3_cfg.buckets.silver,
            "gold": s3_cfg.buckets.gold,
        }
    elif target in ("metrics", "journal"):
        bucket_targets = {}
    else:
        bucket_targets = {target: getattr(s3_cfg.buckets, target)}

    # Build description of what will be cleaned
    descriptions = []
    if bucket_targets:
        for layer, bucket in bucket_targets.items():
            descriptions.append(f"  - {layer}: s3://{bucket}/ (all objects)")
    if target == "metrics":
        descriptions.append(f"  - metrics: {metrics_dir}/ (all files)")
    if target == "journal":
        descriptions.append(f"  - journal: {DEFAULT_JOURNAL_DIR}/ (all session files)")

    # Confirmation
    if not force:
        console.print(
            Panel(
                "[yellow]WARNING[/yellow]: This will delete the following data:\n\n"
                + "\n".join(descriptions)
                + "\n\n"
                "Infrastructure (K8s, catalog) will NOT be affected.",
                title="Confirm Clean",
                expand=False,
            )
        )
        confirm = typer.confirm("Are you sure you want to proceed?")
        if not confirm:
            print_info("Clean cancelled")
            raise typer.Exit(0)

    console.print(Panel(f"Cleaning: [bold]{target}[/bold]", expand=False))

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.CLEAN, {"target": target})

    total_deleted = 0
    errors = []

    # Check for active datagen before cleaning S3 buckets
    if bucket_targets:
        try:
            from kubernetes import client as k8s_client

            ns = cfg.get_namespace()
            batch_v1 = k8s_client.BatchV1Api()
            job = batch_v1.read_namespaced_job("lakebench-datagen", ns)
            active_pods = job.status.active or 0
            if active_pods > 0:
                print_warning(f"Datagen job has {active_pods} active pod(s)")
                if not force:
                    typer.confirm("Data generation is running. Clean anyway?", abort=True)
        except Exception:
            pass  # No datagen job or K8s unavailable -- safe to proceed

    # Clean S3 buckets
    if bucket_targets:
        try:
            from lakebench.s3 import S3Client

            s3 = S3Client(
                endpoint=s3_cfg.endpoint,
                access_key=s3_cfg.access_key,
                secret_key=s3_cfg.secret_key,
                region=s3_cfg.region,
                path_style=s3_cfg.path_style,
            )

            def _clean_progress(bkt: str, count: int) -> None:
                console.print(f"  Deleting from s3://{bkt}/... ({count:,} objects so far)")

            for layer, bucket in bucket_targets.items():
                try:
                    deleted = s3.empty_bucket(bucket, progress_callback=_clean_progress)
                    total_deleted += deleted
                    if deleted > 0:
                        print_success(
                            f"Cleaned {layer}: {deleted:,} objects deleted from s3://{bucket}/"
                        )
                    else:
                        print_info(f"Cleaned {layer}: bucket s3://{bucket}/ already empty")
                except Exception as e:
                    errors.append(f"{layer}: {e}")
                    print_error(f"Failed to clean {layer}: {e}")

        except Exception as e:
            errors.append(f"S3 connection: {e}")
            print_error(f"S3 connection failed: {e}")

    # Clean metrics directory
    if target == "metrics":
        import shutil

        if metrics_dir.exists():
            file_count = sum(1 for _ in metrics_dir.rglob("*") if _.is_file())
            shutil.rmtree(metrics_dir)
            total_deleted += file_count
            print_success(f"Cleaned metrics: {file_count} files deleted from {metrics_dir}/")
        else:
            print_info(f"Metrics directory {metrics_dir}/ does not exist")

    # Clean journal files
    if target == "journal":
        journal_path = Path(DEFAULT_JOURNAL_DIR)
        if journal_path.exists():
            purge_journal = Journal(journal_dir=journal_path)
            deleted = purge_journal.purge()
            total_deleted += deleted
            if deleted > 0:
                print_success(
                    f"Cleaned journal: {deleted} session files deleted from {journal_path}/"
                )
            else:
                print_info(f"Journal directory {journal_path}/ has no session files")
        else:
            print_info(f"Journal directory {journal_path}/ does not exist")

    # Journal recording
    _journal_safe(
        j.record,
        EventType.CLEAN_TARGET,
        message=f"Cleaned {target}: {total_deleted:,} objects",
        success=len(errors) == 0,
        details={
            "target": target,
            "objects_deleted": total_deleted,
            "buckets": list(bucket_targets.keys()) if bucket_targets else [],
        },
    )
    _journal_safe(j.end_command, success=len(errors) == 0)
    if target == "data":
        _journal_safe(j.close_session)

    # Summary
    console.print()
    if not errors:
        console.print(
            Panel(
                f"[green]Clean complete[/green]: {total_deleted:,} objects deleted",
                title="Clean Complete",
                expand=False,
            )
        )
    else:
        console.print(
            Panel(
                f"[red]{len(errors)} error(s)[/red] during clean\n\n"
                + "\n".join(f"  - {e}" for e in errors),
                title="Clean Incomplete",
                expand=False,
            )
        )
        raise typer.Exit(1)


@app.command()
def generate(
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
    wait: Annotated[
        bool,
        typer.Option(
            "--wait",
            "-w",
            help="Wait for data generation to complete",
        ),
    ] = True,
    timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            "-t",
            help="Timeout in seconds when waiting for completion",
        ),
    ] = 7200,
    resume: Annotated[
        bool,
        typer.Option(
            "--resume",
            help="Resume from checkpoint if previous generation was interrupted",
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
) -> None:
    """Generate synthetic data to bronze bucket.

    Runs the datagen job to populate the bronze bucket with synthetic data.
    Uses parallel Kubernetes Jobs for efficient generation.

    Use --resume to continue from a previous interrupted generation.
    """
    from lakebench.deploy import DatagenDeployer, DeploymentEngine, DeploymentStatus

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

    # Auto-size resources based on scale + cluster capacity
    from lakebench.config.autosizer import resolve_auto_sizing

    try:
        k8s_for_cap = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        cluster_cap = k8s_for_cap.get_cluster_capacity()
    except Exception as e:
        logger.warning("Could not get cluster capacity for auto-sizing: %s", e)
        cluster_cap = None
    resolve_auto_sizing(cfg, cluster_cap)

    workload = cfg.architecture.workload
    datagen_cfg = workload.datagen
    dims = cfg.get_scale_dimensions()
    console.print(
        Panel(
            f"Generating data for: [bold]{cfg.name}[/bold]\n\n"
            f"Scale: {dims.scale} (~{dims.approx_bronze_gb:.0f} GB)\n"
            f"Customers: {dims.customers:,}\n"
            f"Parallelism: {datagen_cfg.parallelism} pods\n"
            f"Bucket: {cfg.platform.storage.s3.buckets.bronze}",
            expand=False,
        )
    )

    if not yes:
        typer.confirm("Start data generation?", default=True, abort=True)

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(
        CommandName.GENERATE,
        {
            "wait": wait,
            "timeout": timeout,
            "resume": resume,
        },
    )

    try:
        engine = DeploymentEngine(cfg)
        datagen = DatagenDeployer(engine)

        # Submit job
        if resume:
            print_info("Submitting datagen job with checkpoint resume...")
        else:
            print_info("Submitting datagen job...")
        result = datagen.deploy(resume=resume)

        if result.status != DeploymentStatus.SUCCESS:
            print_error(f"Failed to submit job: {result.message}")
            _journal_safe(j.end_command, success=False, message=result.message)
            raise typer.Exit(1)

        print_success("Datagen job submitted")
        console.print(f"  Parallelism: {result.details.get('parallelism', '?')} pods")
        target_tb = float(result.details.get("target_tb", 0))
        console.print(f"  Target: {target_tb * 1024:.0f} GB")

        _journal_safe(
            j.record,
            EventType.GENERATE_START,
            message="Data generation started",
            details={
                "scale": dims.scale,
                "parallelism": datagen_cfg.parallelism,
                "target_gb": round(dims.approx_bronze_gb, 1),
                "bucket": cfg.platform.storage.s3.buckets.bronze,
            },
        )

        if not wait:
            print_info("Use 'lakebench status' to check progress")
            _journal_safe(j.end_command, success=True, message="Job submitted (no-wait)")
            return

        # Wait for completion with progress bar
        import time

        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )

        start = time.time()

        # Get initial progress to determine total completions
        initial_progress = datagen.get_progress()
        total_completions = initial_progress.get("completions", 1)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total} pods"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress_bar:
            task = progress_bar.add_task("Generating data", total=total_completions)

            while time.time() - start < timeout:
                prog = datagen.get_progress()

                if not prog.get("running", False):
                    if prog.get("error"):
                        progress_bar.stop()
                        print_error(prog["error"])
                        raise typer.Exit(1)
                    # Mark complete
                    progress_bar.update(task, completed=total_completions)
                    break

                # Surface pod failures early
                if prog.get("oom_pods"):
                    progress_bar.stop()
                    print_error(f"OOMKilled: {', '.join(prog['oom_pods'])}")
                    print_info("Increase datagen memory or reduce parallelism")
                    _journal_safe(j.end_command, success=False, message="OOMKilled pods detected")
                    raise typer.Exit(1)
                if prog.get("pending_pods"):
                    progress_bar.console.print(
                        f"  [yellow]{len(prog['pending_pods'])} pod(s) pending[/yellow]"
                    )

                succeeded = prog.get("succeeded", 0)
                progress_bar.update(task, completed=succeeded)

                time.sleep(30)

        # Final result
        completion_result = datagen.wait_for_completion(timeout_seconds=10)

        console.print()
        if completion_result.status == DeploymentStatus.SUCCESS:
            _journal_safe(
                j.record,
                EventType.GENERATE_COMPLETE,
                message="Data generation complete",
                success=True,
                details={
                    "succeeded_pods": completion_result.details.get("succeeded", 0),
                    "failed_pods": completion_result.details.get("failed", 0),
                    "elapsed_seconds": completion_result.elapsed_seconds,
                },
            )
            _journal_safe(j.end_command, success=True)

            console.print(
                Panel(
                    f"[green]Data generation complete![/green]\n\n"
                    f"Succeeded: {completion_result.details.get('succeeded', '?')} pods\n"
                    f"Elapsed: {completion_result.elapsed_seconds:.0f}s\n\n"
                    f"Data written to: s3://{cfg.platform.storage.s3.buckets.bronze}/{cfg.architecture.pipeline.medallion.bronze.path_template}",
                    title="Generation Complete",
                    expand=False,
                )
            )
        else:
            _journal_safe(
                j.record,
                EventType.GENERATE_COMPLETE,
                message=completion_result.message,
                success=False,
                details={
                    "succeeded_pods": completion_result.details.get("succeeded", 0),
                    "failed_pods": completion_result.details.get("failed", 0),
                    "elapsed_seconds": completion_result.elapsed_seconds,
                },
            )
            _journal_safe(j.end_command, success=False, message=completion_result.message)

            console.print(
                Panel(
                    f"[red]Data generation failed![/red]\n\n{completion_result.message}",
                    title="Generation Failed",
                    expand=False,
                )
            )
            raise typer.Exit(1)

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904


@app.command()
def run(
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
    stage: Annotated[
        str | None,
        typer.Option(
            "--stage",
            "-s",
            help="Run specific stage only (bronze-verify, silver-build, gold-finalize)",
        ),
    ] = None,
    timeout: Annotated[
        int | None,
        typer.Option(
            "--timeout",
            "-t",
            help="Timeout per job in seconds (auto-scaled from data scale if omitted)",
        ),
    ] = None,
    skip_benchmark: Annotated[
        bool,
        typer.Option(
            "--skip-benchmark",
            help="Skip the query benchmark after pipeline completion",
        ),
    ] = False,
    continuous: Annotated[
        bool,
        typer.Option(
            "--continuous",
            help="Run in continuous streaming mode (bronze-ingest → silver-stream → gold-refresh)",
        ),
    ] = False,
    duration: Annotated[
        int | None,
        typer.Option(
            "--duration",
            help="Streaming run duration in seconds (default: from config, typically 1800)",
        ),
    ] = None,
    include_datagen: Annotated[
        bool,
        typer.Option(
            "--include-datagen",
            help="Run datagen before pipeline stages (full end-to-end measurement)",
        ),
    ] = False,
) -> None:
    """Execute the data pipeline.

    Runs the medallion pipeline (bronze → silver → gold).
    Each stage is a separate Spark job submitted to the cluster.
    Metrics are automatically collected and saved for reporting.
    After gold finalize, runs a query benchmark (QpH).
    Use --skip-benchmark to skip the benchmark stage.

    With --continuous, runs the streaming pipeline instead:
    starts datagen, then launches bronze-ingest, silver-stream,
    and gold-refresh as concurrent streaming jobs. Monitors for
    the configured duration, then stops streaming and runs benchmark.

    With --include-datagen, runs data generation first, measuring
    ingest time as the first stage in the pipeline benchmark.
    """
    import uuid

    from lakebench.metrics import JobMetrics, MetricsCollector, MetricsStorage
    from lakebench.spark import SparkJobManager, SparkJobMonitor, SparkOperatorManager
    from lakebench.spark.job import JobState, JobType, get_executor_count, get_job_profile

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

    # Auto-size resources based on scale + cluster capacity
    from lakebench.config.autosizer import resolve_auto_sizing

    try:
        k8s_for_cap = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        cluster_cap = k8s_for_cap.get_cluster_capacity()
    except Exception as e:
        logger.warning("Could not get cluster capacity for auto-sizing: %s", e)
        cluster_cap = None
    resolve_auto_sizing(cfg, cluster_cap)

    # Auto-scale timeout if not explicitly set
    if timeout is None:
        scale = cfg.architecture.workload.datagen.get_effective_scale()
        timeout = max(3600, scale * 60)
        if scale >= 50:
            print_info(f"Per-job timeout: {timeout}s (auto-scaled for scale {scale})")

    # Branch: continuous streaming pipeline
    if continuous:
        _run_continuous(cfg, config_file, timeout, skip_benchmark, duration)
        return

    console.print(
        Panel(
            f"Running pipeline for: [bold]{cfg.name}[/bold]\n\n"
            f"Stages: bronze-verify → silver-build → gold-finalize",
            expand=False,
        )
    )

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.RUN, {"stage": stage, "timeout": timeout})

    # Initialize metrics collection
    collector = MetricsCollector()
    metrics_storage = MetricsStorage()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    from lakebench.metrics import build_config_snapshot

    config_snapshot = build_config_snapshot(cfg)
    collector.start_run(run_id, cfg.name, config_snapshot)

    pipeline_success = True
    _datagen_elapsed = 0.0
    _datagen_output_gb = 0.0
    _datagen_output_rows = 0

    try:
        # Check Spark operator
        print_info("Checking Spark Operator...")
        spark_op_cfg = cfg.platform.compute.spark.operator
        operator = SparkOperatorManager(
            namespace=spark_op_cfg.namespace,
            version=spark_op_cfg.version if spark_op_cfg.install else None,
            job_namespace=cfg.get_namespace(),
        )
        status = operator.ensure_installed() if spark_op_cfg.install else operator.check_status()

        if not status.ready:
            print_error(f"Spark Operator not ready: {status.message}")
            pipeline_success = False
            raise typer.Exit(1)

        # Ensure operator watches the target namespace (self-heal if install=true)
        ns_status = operator.ensure_namespace_watched(can_heal=spark_op_cfg.install)
        if ns_status.watching_namespace is False:
            print_error(ns_status.message)
            pipeline_success = False
            raise typer.Exit(1)

        print_success(f"Spark Operator ready (version: {status.version or 'unknown'})")

        # Initialize job manager
        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )

        job_manager = SparkJobManager(cfg, k8s)
        monitor = SparkJobMonitor(cfg, k8s)

        # Deploy scripts ConfigMap -- must succeed or pipeline jobs will fail
        print_info("Deploying Spark scripts...")
        if not job_manager.deploy_scripts_configmap():
            print_error("Failed to deploy Spark scripts ConfigMap -- pipeline cannot proceed")
            _journal_safe(j.end_command, success=False, message="Scripts ConfigMap deploy failed")
            raise typer.Exit(1)
        print_success("Spark scripts deployed")

        # Run datagen if requested (full end-to-end measurement)
        if include_datagen:
            console.print()
            console.print("[bold]Stage: datagen (ingest)[/bold]")
            print_info("Generating data for pipeline benchmark...")
            datagen_start = datetime.now()
            try:
                from lakebench.deploy import DatagenDeployer, DeploymentEngine

                dg_engine = DeploymentEngine(cfg)
                datagen_deployer = DatagenDeployer(dg_engine)
                datagen_deployer.deploy()
                datagen_deployer.wait_for_completion(timeout_seconds=timeout)
                datagen_end = datetime.now()
                _datagen_elapsed = (datagen_end - datagen_start).total_seconds()
                print_success(f"Datagen completed in {_datagen_elapsed:.0f}s")

                # Measure bronze bucket after datagen
                try:
                    from lakebench.s3 import S3Client

                    s3_cfg = cfg.platform.storage.s3
                    _s3_dg = S3Client(
                        endpoint=s3_cfg.endpoint,
                        access_key=s3_cfg.access_key,
                        secret_key=s3_cfg.secret_key,
                        region=s3_cfg.region,
                        path_style=s3_cfg.path_style,
                    )
                    dg_info = _s3_dg.get_bucket_size(s3_cfg.buckets.bronze)
                    if dg_info.size_bytes:
                        _datagen_output_gb = dg_info.size_bytes / (1024**3)
                    if dg_info.object_count:
                        # Estimate rows from scale factor (1.5M rows per scale unit)
                        _datagen_output_rows = cfg.architecture.workload.datagen.scale * 1_500_000
                except Exception as e:
                    logger.warning("Could not measure bronze bucket size: %s", e)
            except Exception as e:
                print_error(f"Datagen failed: {e}")
                pipeline_success = False
                raise typer.Exit(1)  # noqa: B904

        # Determine stages to run
        all_stages = [
            (JobType.BRONZE_VERIFY, "bronze-verify", "Verifying bronze data"),
            (JobType.SILVER_BUILD, "silver-build", "Building silver layer"),
            (JobType.GOLD_FINALIZE, "gold-finalize", "Finalizing gold layer"),
        ]

        if stage:
            stages = [(jt, name, desc) for jt, name, desc in all_stages if name == stage]
            if not stages:
                print_error(f"Unknown stage: {stage}")
                print_info("Valid stages: bronze-verify, silver-build, gold-finalize")
                raise typer.Exit(1)
        else:
            stages = all_stages

        # Run each stage
        results = []
        for job_type, stage_name, description in stages:
            console.print()
            console.print(f"[bold]Stage: {stage_name}[/bold]")
            print_info(description)

            job_start = datetime.now()

            # Submit job
            job_status = job_manager.submit_job(job_type)
            if job_status.state == JobState.FAILED:
                print_error(f"Failed to submit job: {job_status.message}")
                collector.record_job(
                    JobMetrics(
                        job_name=f"lakebench-{stage_name}",
                        job_type=stage_name,
                        start_time=job_start,
                        end_time=datetime.now(),
                        elapsed_seconds=(datetime.now() - job_start).total_seconds(),
                        success=False,
                        error_message=job_status.message,
                    )
                )
                pipeline_success = False
                raise typer.Exit(1)

            print_success(f"Job submitted: lakebench-{stage_name}")

            # Wait for completion -- capture max executor count seen
            _max_executors = 0
            _last_reported_executors = -1

            def on_progress(status):
                nonlocal _max_executors, _last_reported_executors
                if status.state == JobState.RUNNING:
                    _max_executors = max(_max_executors, status.executor_count)
                    if status.executor_count != _last_reported_executors:
                        console.print(f"  Running... (executors: {status.executor_count})")
                        _last_reported_executors = status.executor_count

            result = monitor.wait_for_completion(
                f"lakebench-{stage_name}",
                timeout_seconds=timeout,
                poll_interval=15,
                progress_callback=on_progress,
            )

            job_end = datetime.now()

            # Build job metrics
            job_metrics = JobMetrics(
                job_name=f"lakebench-{stage_name}",
                job_type=stage_name,
                start_time=job_start,
                end_time=job_end,
                elapsed_seconds=result.elapsed_seconds,
                success=result.success,
                error_message=result.message if not result.success else None,
                executor_count=_max_executors,
            )

            # Parse driver logs for data metrics if available
            if result.driver_logs:
                parsed = collector.parse_driver_logs(result.driver_logs, stage_name)
                job_metrics.input_size_gb = parsed.input_size_gb
                job_metrics.output_size_gb = parsed.output_size_gb
                job_metrics.input_rows = parsed.input_rows
                job_metrics.output_rows = parsed.output_rows
                job_metrics.throughput_gb_per_second = parsed.throughput_gb_per_second
                job_metrics.throughput_rows_per_second = parsed.throughput_rows_per_second

            # Populate resource metrics from job profile
            _profile = get_job_profile(stage_name)
            if _profile:
                _scale = cfg.architecture.workload.datagen.get_effective_scale()
                _expected_executors = get_executor_count(stage_name, _scale)

                # Check per-job executor override
                _override_map = {
                    "bronze-verify": cfg.platform.compute.spark.bronze_executors,
                    "silver-build": cfg.platform.compute.spark.silver_executors,
                    "gold-finalize": cfg.platform.compute.spark.gold_executors,
                }
                _override = _override_map.get(stage_name)
                if _override is not None:
                    _expected_executors = _override

                # Use deterministic count when progress callback didn't capture
                if job_metrics.executor_count == 0:
                    job_metrics.executor_count = _expected_executors

                job_metrics.executor_cores = _profile["executor_cores"]
                _mem_gb = parse_spark_memory(_profile["executor_memory"]) / (1024**3)
                _overhead_gb = parse_spark_memory(_profile["executor_memory_overhead"]) / (1024**3)
                job_metrics.executor_memory_gb = _mem_gb

                # Requested CPU-seconds and peak memory
                job_metrics.cpu_seconds_requested = (
                    job_metrics.executor_count
                    * job_metrics.executor_cores
                    * job_metrics.elapsed_seconds
                )
                job_metrics.memory_gb_requested = job_metrics.executor_count * (
                    _mem_gb + _overhead_gb
                )

            # Gold input fallback: gold reads from silver
            if (
                stage_name == "gold-finalize"
                and job_metrics.input_size_gb == 0.0
                and collector.current_run
            ):
                for _prev in collector.current_run.jobs:
                    if _prev.job_type == "silver-build" and _prev.output_size_gb > 0:
                        job_metrics.input_size_gb = _prev.output_size_gb
                        if job_metrics.elapsed_seconds > 0:
                            job_metrics.throughput_gb_per_second = (
                                job_metrics.input_size_gb / job_metrics.elapsed_seconds
                            )
                        break

            # Measure per-stage S3 output size
            _stage_bucket_map = {
                "bronze-verify": cfg.platform.storage.s3.buckets.bronze,
                "silver-build": cfg.platform.storage.s3.buckets.silver,
                "gold-finalize": cfg.platform.storage.s3.buckets.gold,
            }
            if stage_name in _stage_bucket_map and job_metrics.output_size_gb == 0:
                try:
                    from lakebench.s3 import S3Client

                    s3_cfg = cfg.platform.storage.s3
                    _s3 = S3Client(
                        endpoint=s3_cfg.endpoint,
                        access_key=s3_cfg.access_key,
                        secret_key=s3_cfg.secret_key,
                        region=s3_cfg.region,
                        path_style=s3_cfg.path_style,
                    )
                    _bucket_info = _s3.get_bucket_size(_stage_bucket_map[stage_name])
                    if _bucket_info.size_bytes:
                        job_metrics.output_size_gb = _bucket_info.size_bytes / (1024**3)
                except Exception as e:
                    logger.warning("Could not measure %s bucket size: %s", stage_name, e)

            collector.record_job(job_metrics)

            if result.success:
                print_success(f"{stage_name} completed in {result.elapsed_seconds:.0f}s")
                results.append((stage_name, True, result.elapsed_seconds))
                _journal_safe(
                    j.record,
                    EventType.PIPELINE_STAGE,
                    message=f"{stage_name} completed",
                    success=True,
                    details={
                        "stage": stage_name,
                        "success": True,
                        "elapsed_seconds": result.elapsed_seconds,
                        "input_gb": job_metrics.input_size_gb,
                        "output_rows": job_metrics.output_rows,
                    },
                )
            else:
                print_error(f"{stage_name} failed: {result.message}")
                if result.driver_logs:
                    console.print("[dim]Driver logs (last 20 lines):[/dim]")
                    for line in result.driver_logs.split("\n")[-20:]:
                        console.print(f"  {line}")
                results.append((stage_name, False, result.elapsed_seconds))
                _journal_safe(
                    j.record,
                    EventType.PIPELINE_STAGE,
                    message=f"{stage_name} failed: {result.message}",
                    success=False,
                    details={
                        "stage": stage_name,
                        "success": False,
                        "elapsed_seconds": result.elapsed_seconds,
                    },
                )
                pipeline_success = False
                raise typer.Exit(1)

        # Summary
        console.print()
        total_time = sum(r[2] for r in results)

        stages_succeeded = sum(1 for _, ok, _ in results if ok)
        stages_failed = sum(1 for _, ok, _ in results if not ok)
        _journal_safe(
            j.record,
            EventType.PIPELINE_COMPLETE,
            message=f"Pipeline complete: {stages_succeeded} stages succeeded",
            success=stages_failed == 0,
            details={
                "run_id": run_id,
                "stages_succeeded": stages_succeeded,
                "stages_failed": stages_failed,
                "total_seconds": total_time,
            },
        )

        # Run Trino benchmark after pipeline completes
        benchmark_qph = None
        if not skip_benchmark:
            try:
                from lakebench.benchmark import BenchmarkRunner
                from lakebench.metrics import BenchmarkMetrics

                console.print()
                console.print("[bold]Stage: benchmark[/bold]")
                print_info("Running query benchmark (hot cache, power)...")

                _journal_safe(
                    j.record,
                    EventType.BENCHMARK_START,
                    message="Benchmark started",
                    details={"mode": "power", "cache": "hot"},
                )

                bench_runner = BenchmarkRunner(cfg)
                bench_result = bench_runner.run_power(cache="hot")

                # Display per-query results
                for qr in bench_result.queries:
                    status_str = "[green]PASS[/green]" if qr.success else "[red]FAIL[/red]"
                    console.print(
                        f"  {qr.query.name[:4]}  {qr.query.display_name:<34} "
                        f"{qr.elapsed_seconds:>7.2f}s   "
                        f"{qr.rows_returned:>6} rows   {status_str}"
                    )

                console.print(f"\n  Total: {bench_result.total_seconds:.2f}s")
                console.print(f"  [bold]QpH:   {bench_result.qph:.1f}[/bold]")
                benchmark_qph = bench_result.qph

                # Record in metrics
                bench_metrics = BenchmarkMetrics(
                    mode=bench_result.mode,
                    cache=bench_result.cache,
                    scale=bench_result.scale,
                    qph=bench_result.qph,
                    total_seconds=bench_result.total_seconds,
                    queries=[q.to_dict() for q in bench_result.queries],
                    iterations=bench_result.iterations,
                )
                collector.record_benchmark(bench_metrics)

                _journal_safe(
                    j.record,
                    EventType.BENCHMARK_COMPLETE,
                    message=f"Benchmark complete: QpH={bench_result.qph:.1f}",
                    success=True,
                    details={
                        "qph": round(bench_result.qph, 1),
                        "total_seconds": round(bench_result.total_seconds, 2),
                        "queries_passed": sum(1 for q in bench_result.queries if q.success),
                        "queries_total": len(bench_result.queries),
                    },
                )

            except Exception as e:
                print_warning(f"Benchmark failed: {e}")
                print_info(
                    "Pipeline results are still valid. Run 'lakebench benchmark' separately."
                )

        # Build summary with optional QpH
        qph_line = f"\nQpH: {benchmark_qph:.1f}" if benchmark_qph is not None else ""

        console.print(
            Panel(
                "[green]Pipeline completed successfully![/green]\n\n"
                + "\n".join([f"  {name}: {elapsed:.0f}s" for name, _, elapsed in results])
                + f"\n\nTotal time: {total_time:.0f}s{qph_line}\n\n"
                f"Query results: lakebench query --example count\n"
                f"Generate report: lakebench report",
                title="Pipeline Complete",
                expand=False,
            )
        )

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        pipeline_success = False
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904
    finally:
        # Measure actual S3 bucket sizes before saving metrics
        try:
            from lakebench.s3 import S3Client

            s3_cfg = cfg.platform.storage.s3
            s3_client = S3Client(
                endpoint=s3_cfg.endpoint,
                access_key=s3_cfg.access_key,
                secret_key=s3_cfg.secret_key,
                region=s3_cfg.region,
                path_style=s3_cfg.path_style,
            )
            print_info("Measuring actual S3 bucket sizes...")
            collector.record_actual_sizes(
                s3_client,
                s3_cfg.buckets.bronze,
                s3_cfg.buckets.silver,
                s3_cfg.buckets.gold,
            )
        except Exception as e:
            console.print(f"  [yellow]Could not measure S3 sizes: {e}[/yellow]")

        # Always save metrics, even on failure
        run_metrics = collector.end_run(success=pipeline_success)
        if run_metrics:
            # Build pipeline benchmark (stage-matrix view)
            try:
                from lakebench.metrics import build_pipeline_benchmark

                pb = build_pipeline_benchmark(
                    run_metrics,
                    datagen_elapsed=_datagen_elapsed,
                    datagen_output_gb=_datagen_output_gb,
                    datagen_output_rows=_datagen_output_rows,
                )
                run_metrics.pipeline_benchmark = pb
                if pb.pipeline_mode == "continuous":
                    if pb.sustained_throughput_rps > 0:
                        latency_str = (
                            "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
                            if pb.stage_latency_profile
                            else "n/a"
                        )
                        print_info(
                            f"Pipeline Score: {pb.data_freshness_seconds:.1f}s freshness"
                            f" | {pb.sustained_throughput_rps:,.0f} rows/s sustained"
                            f" | {latency_str}ms latency (b/s/g)"
                        )
                elif pb.time_to_value_seconds > 0:
                    print_info(
                        f"Pipeline Score: {pb.time_to_value_seconds:.1f}s time-to-value"
                        f" | {pb.pipeline_throughput_gb_per_second:.3f} GB/s throughput"
                    )
            except Exception as e:
                console.print(f"  [yellow]Could not build pipeline benchmark: {e}[/yellow]")

            metrics_path = metrics_storage.save_run(run_metrics)
            print_info(f"Metrics saved to {metrics_path}")
            print_info(f"Run ID: {run_id}")

            _journal_safe(
                j.record,
                EventType.METRICS_SAVED,
                message=f"Metrics saved for run {run_id}",
                details={"run_id": run_id, "metrics_path": str(metrics_path)},
            )

        _journal_safe(j.end_command, success=pipeline_success)


def _run_continuous(
    cfg,
    config_file: Path,
    timeout: int,
    skip_benchmark: bool,
    duration: int | None,
) -> None:
    """Run the continuous streaming pipeline.

    Starts datagen concurrently with bronze-ingest, silver-stream, and
    gold-refresh streaming SparkApplications. Monitors for the configured
    duration, then stops streaming jobs and optionally runs the benchmark.
    """
    import uuid

    from lakebench.deploy import DatagenDeployer, DeploymentEngine, DeploymentStatus
    from lakebench.metrics import MetricsCollector, MetricsStorage, StreamingJobMetrics
    from lakebench.spark import SparkJobManager, SparkJobMonitor, SparkOperatorManager
    from lakebench.spark.job import JobState, JobType

    run_duration = duration or cfg.architecture.pipeline.continuous.run_duration

    console.print(
        Panel(
            f"Running continuous pipeline for: [bold]{cfg.name}[/bold]\n\n"
            f"Stages: bronze-ingest + silver-stream + gold-refresh (concurrent)\n"
            f"Duration: {run_duration}s ({run_duration / 60:.0f} min)",
            expand=False,
        )
    )

    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.RUN, {"continuous": True, "duration": run_duration})

    collector = MetricsCollector()
    metrics_storage = MetricsStorage()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    from lakebench.metrics import build_config_snapshot

    config_snapshot = build_config_snapshot(cfg)
    collector.start_run(run_id, cfg.name, config_snapshot)

    pipeline_success = True
    _datagen_output_gb = 0.0
    _datagen_output_rows = 0
    streaming_jobs = [
        (JobType.BRONZE_INGEST, "bronze-ingest"),
        (JobType.SILVER_STREAM, "silver-stream"),
        (JobType.GOLD_REFRESH, "gold-refresh"),
    ]

    try:
        # Check Spark operator
        print_info("Checking Spark Operator...")
        spark_op_cfg = cfg.platform.compute.spark.operator
        operator = SparkOperatorManager(
            namespace=spark_op_cfg.namespace,
            version=spark_op_cfg.version if spark_op_cfg.install else None,
            job_namespace=cfg.get_namespace(),
        )
        status = operator.ensure_installed() if spark_op_cfg.install else operator.check_status()
        if not status.ready:
            print_error(f"Spark Operator not ready: {status.message}")
            pipeline_success = False
            raise typer.Exit(1)

        # Ensure operator watches the target namespace (self-heal if install=true)
        ns_status = operator.ensure_namespace_watched(can_heal=spark_op_cfg.install)
        if ns_status.watching_namespace is False:
            print_error(ns_status.message)
            pipeline_success = False
            raise typer.Exit(1)

        print_success(f"Spark Operator ready (version: {status.version or 'unknown'})")

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        job_manager = SparkJobManager(cfg, k8s)
        monitor = SparkJobMonitor(cfg, k8s)

        # Deploy scripts ConfigMap (includes streaming scripts) -- must succeed
        print_info("Deploying Spark scripts...")
        if not job_manager.deploy_scripts_configmap():
            print_error("Failed to deploy Spark scripts ConfigMap -- pipeline cannot proceed")
            _journal_safe(j.end_command, success=False, message="Scripts ConfigMap deploy failed")
            raise typer.Exit(1)
        print_success("Spark scripts deployed")

        # Start datagen (runs concurrently with streaming jobs)
        console.print()
        console.print("[bold]Starting datagen...[/bold]")
        engine = DeploymentEngine(cfg)
        datagen = DatagenDeployer(engine)
        datagen_result = datagen.deploy()
        if datagen_result.status != DeploymentStatus.SUCCESS:
            print_error(f"Failed to start datagen: {datagen_result.message}")
            pipeline_success = False
            raise typer.Exit(1)
        print_success("Datagen started (continuous mode)")
        dims = cfg.get_scale_dimensions()
        console.print(f"  Scale: {dims.scale} (~{dims.approx_bronze_gb:.0f} GB)")
        console.print(f"  Parallelism: {cfg.architecture.workload.datagen.parallelism} pods")
        _journal_safe(
            j.record,
            EventType.GENERATE_START,
            message="Datagen started for continuous pipeline",
            details={
                "scale": dims.scale,
                "parallelism": cfg.architecture.workload.datagen.parallelism,
                "target_gb": round(dims.approx_bronze_gb, 1),
            },
        )

        # Launch all streaming jobs concurrently
        console.print()
        console.print("[bold]Launching streaming jobs...[/bold]")

        _journal_safe(
            j.record,
            EventType.STREAMING_START,
            message="Starting streaming pipeline",
            details={"jobs": [name for _, name in streaming_jobs]},
        )

        submitted = []
        for job_type, job_name in streaming_jobs:
            job_status = job_manager.submit_job(job_type)
            if job_status.state == JobState.FAILED:
                print_error(f"Failed to submit {job_name}: {job_status.message}")
                pipeline_success = False
                raise typer.Exit(1)
            print_success(f"Submitted: lakebench-{job_name}")
            submitted.append((job_type, job_name))

        # Monitor for configured duration
        console.print()
        print_info(
            f"Streaming pipeline running for {run_duration}s ({run_duration / 60:.0f} min)..."
        )
        start = time.time()
        check_interval = 30

        while time.time() - start < run_duration:
            remaining = run_duration - (time.time() - start)
            sleep_time = min(check_interval, remaining)
            if sleep_time > 0:
                time.sleep(sleep_time)

            # Periodic health check
            elapsed = time.time() - start
            console.print(
                f"  [{elapsed:.0f}s / {run_duration}s] "
                f"Streaming jobs running... ({remaining:.0f}s remaining)"
            )

            _journal_safe(
                j.record,
                EventType.STREAMING_HEALTH,
                message="Health check",
                details={"elapsed_seconds": elapsed, "remaining_seconds": remaining},
            )

        # Measure bronze bucket for datagen stats (datagen runs concurrently,
        # so we measure after the monitoring window to capture all output)
        try:
            from lakebench.s3 import S3Client

            s3_cfg = cfg.platform.storage.s3
            _s3_dg = S3Client(
                endpoint=s3_cfg.endpoint,
                access_key=s3_cfg.access_key,
                secret_key=s3_cfg.secret_key,
                region=s3_cfg.region,
                path_style=s3_cfg.path_style,
            )
            dg_info = _s3_dg.get_bucket_size(s3_cfg.buckets.bronze)
            if dg_info.size_bytes:
                _datagen_output_gb = dg_info.size_bytes / (1024**3)
            if dg_info.object_count:
                _datagen_output_rows = cfg.architecture.workload.datagen.scale * 1_500_000
        except Exception as e:
            logger.warning("Could not measure streaming bronze bucket size: %s", e)

        # Capture driver logs BEFORE stopping jobs (pods are deleted on stop)
        console.print()
        console.print("[bold]Collecting streaming metrics...[/bold]")
        namespace = cfg.get_namespace()
        driver_logs: dict[str, str | None] = {}
        for _job_type, job_name in submitted:
            try:
                logs = monitor._get_driver_logs(
                    f"lakebench-{job_name}",
                    tail_lines=None,
                )
                driver_logs[job_name] = logs
                # Diagnostic: report log capture status
                if logs is None:
                    print_warning(f"{job_name}: no driver logs (pod not found)")
                elif len(logs) == 0:
                    print_warning(f"{job_name}: driver logs empty (0 bytes)")
                else:
                    lines = logs.strip().split("\n")
                    console.print(
                        f"  {job_name}: captured {len(logs):,} bytes ({len(lines)} lines)"
                    )
                    # Show first line to verify format
                    if lines:
                        first = lines[0][:80] + "..." if len(lines[0]) > 80 else lines[0]
                        console.print(f"    first: {first}")
            except Exception as e:
                driver_logs[job_name] = None
                print_warning(f"{job_name}: log capture failed: {e}")

        # Stop streaming jobs
        console.print("[bold]Stopping streaming jobs...[/bold]")
        for _job_type, job_name in submitted:
            try:
                k8s.delete_custom_resource(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    plural="sparkapplications",
                    name=f"lakebench-{job_name}",
                    namespace=namespace,
                )
                print_success(f"Stopped: lakebench-{job_name}")
            except Exception as e:
                print_warning(f"Could not stop {job_name}: {e}")

        _journal_safe(
            j.record,
            EventType.STREAMING_STOP,
            message="Streaming pipeline stopped",
            details={"duration_seconds": run_duration},
        )

        # Record streaming metrics (from pre-captured driver logs)
        console.print()
        console.print("[bold]Parsing streaming metrics...[/bold]")
        for _job_type, job_name in submitted:
            logs = driver_logs.get(job_name)
            if logs:
                try:
                    streaming_metrics = collector.parse_streaming_logs(
                        logs,
                        job_name,
                    )
                    # Diagnostic: report what was parsed
                    console.print(
                        f"  {job_name}: {streaming_metrics.total_batches} batches, "
                        f"{streaming_metrics.total_rows_processed:,} rows"
                    )
                    if streaming_metrics.total_batches == 0:
                        # Show a sample of the logs to debug pattern mismatch
                        lines = [line for line in logs.split("\n") if line.strip()]
                        if lines:
                            console.print("    [dim]no batches parsed -- sample lines:[/dim]")
                            for sample in lines[:3]:
                                truncated = sample[:100] + "..." if len(sample) > 100 else sample
                                console.print(f"      {truncated}")
                except Exception as e:
                    print_warning(f"{job_name}: parse failed: {e}")
                    streaming_metrics = StreamingJobMetrics(
                        job_name=f"lakebench-{job_name}",
                        job_type=job_name,
                    )
            else:
                console.print(f"  {job_name}: no logs to parse")
                streaming_metrics = StreamingJobMetrics(
                    job_name=f"lakebench-{job_name}",
                    job_type=job_name,
                )

            streaming_metrics.elapsed_seconds = run_duration
            streaming_metrics.success = pipeline_success
            if streaming_metrics.elapsed_seconds > 0 and streaming_metrics.total_rows_processed > 0:
                streaming_metrics.throughput_rps = (
                    streaming_metrics.total_rows_processed / streaming_metrics.elapsed_seconds
                )
            collector.record_streaming(streaming_metrics)

        # Run benchmark if requested
        if not skip_benchmark:
            try:
                from lakebench.benchmark import BenchmarkRunner
                from lakebench.metrics import BenchmarkMetrics

                console.print()
                console.print("[bold]Stage: benchmark[/bold]")
                print_info("Running query benchmark (hot cache, power)...")

                bench_runner = BenchmarkRunner(cfg)
                bench_result = bench_runner.run_power(cache="hot")

                for qr in bench_result.queries:
                    status_str = "[green]PASS[/green]" if qr.success else "[red]FAIL[/red]"
                    console.print(
                        f"  {qr.query.name[:4]}  {qr.query.display_name:<34} "
                        f"{qr.elapsed_seconds:>7.2f}s   "
                        f"{qr.rows_returned:>6} rows   {status_str}"
                    )

                console.print(f"\n  Total: {bench_result.total_seconds:.2f}s")
                console.print(f"  [bold]QpH:   {bench_result.qph:.1f}[/bold]")

                bench_metrics = BenchmarkMetrics(
                    mode=bench_result.mode,
                    cache=bench_result.cache,
                    scale=bench_result.scale,
                    qph=bench_result.qph,
                    total_seconds=bench_result.total_seconds,
                    queries=[q.to_dict() for q in bench_result.queries],
                    iterations=bench_result.iterations,
                )
                collector.record_benchmark(bench_metrics)
            except Exception as e:
                print_warning(f"Benchmark failed: {e}")

        # Summary
        console.print(
            Panel(
                f"[green]Continuous pipeline completed![/green]\n\n"
                f"  Duration: {run_duration}s ({run_duration / 60:.0f} min)\n"
                f"  Streaming jobs: {len(submitted)}\n\n"
                f"Query results: lakebench query --example count\n"
                f"Generate report: lakebench report",
                title="Continuous Pipeline Complete",
                expand=False,
            )
        )

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        pipeline_success = False
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904
    finally:
        # Measure actual S3 bucket sizes before saving metrics
        try:
            from lakebench.s3 import S3Client

            s3_cfg = cfg.platform.storage.s3
            s3_client = S3Client(
                endpoint=s3_cfg.endpoint,
                access_key=s3_cfg.access_key,
                secret_key=s3_cfg.secret_key,
                region=s3_cfg.region,
                path_style=s3_cfg.path_style,
            )
            print_info("Measuring actual S3 bucket sizes...")
            collector.record_actual_sizes(
                s3_client,
                s3_cfg.buckets.bronze,
                s3_cfg.buckets.silver,
                s3_cfg.buckets.gold,
            )
        except Exception as e:
            console.print(f"  [yellow]Could not measure S3 sizes: {e}[/yellow]")

        run_metrics = collector.end_run(success=pipeline_success)
        if run_metrics:
            # Build pipeline benchmark (stage-matrix view)
            try:
                from lakebench.metrics import build_pipeline_benchmark

                pb = build_pipeline_benchmark(
                    run_metrics,
                    datagen_output_gb=_datagen_output_gb,
                    datagen_output_rows=_datagen_output_rows,
                )
                run_metrics.pipeline_benchmark = pb
                if pb.pipeline_mode == "continuous":
                    if pb.sustained_throughput_rps > 0:
                        latency_str = (
                            "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
                            if pb.stage_latency_profile
                            else "n/a"
                        )
                        print_info(
                            f"Pipeline Score: {pb.data_freshness_seconds:.1f}s freshness"
                            f" | {pb.sustained_throughput_rps:,.0f} rows/s sustained"
                            f" | {latency_str}ms latency (b/s/g)"
                        )
                elif pb.time_to_value_seconds > 0:
                    print_info(
                        f"Pipeline Score: {pb.time_to_value_seconds:.1f}s time-to-value"
                        f" | {pb.pipeline_throughput_gb_per_second:.3f} GB/s throughput"
                    )
            except Exception as e:
                console.print(f"  [yellow]Could not build pipeline benchmark: {e}[/yellow]")

            metrics_path = metrics_storage.save_run(run_metrics)
            print_info(f"Metrics saved to {metrics_path}")
            print_info(f"Run ID: {run_id}")

        _journal_safe(j.end_command, success=pipeline_success)


@app.command()
def stop(
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
) -> None:
    """Stop running streaming jobs.

    Deletes all streaming SparkApplications (bronze-ingest, silver-stream,
    gold-refresh) from the cluster.
    """

    config_file = resolve_config_path(config_file, file_option)
    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    namespace = cfg.get_namespace()
    k8s = get_k8s_client(
        context=cfg.platform.kubernetes.context,
        namespace=namespace,
    )

    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.STOP, {})

    streaming_names = ["bronze-ingest", "silver-stream", "gold-refresh"]
    stopped = 0

    console.print(
        Panel(
            f"Stopping streaming jobs for: [bold]{cfg.name}[/bold]",
            expand=False,
        )
    )

    for name in streaming_names:
        try:
            k8s.delete_custom_resource(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                plural="sparkapplications",
                name=f"lakebench-{name}",
                namespace=namespace,
            )
            print_success(f"Stopped: lakebench-{name}")
            stopped += 1
        except Exception as e:
            print_info(f"lakebench-{name}: not running ({e})")

    if stopped > 0:
        print_success(f"Stopped {stopped} streaming job(s)")
    else:
        print_info("No streaming jobs were running")

    _journal_safe(
        j.record,
        EventType.STREAMING_STOP,
        message=f"Stopped {stopped} streaming jobs",
        details={"stopped": stopped},
    )
    _journal_safe(j.end_command, success=True)


@app.command()
def info(
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
) -> None:
    """Show configuration summary with scale dimensions and compute guidance.

    Displays a concise overview of the recipe configuration including
    schema, scale factor, derived dimensions, compute guidance,
    catalog, table format, and query engine.
    """
    config_file = resolve_config_path(config_file, file_option)
    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    # Auto-size resources based on scale (tier guidance only)
    from lakebench.config.autosizer import resolve_auto_sizing

    resolve_auto_sizing(cfg)

    from lakebench.config.autosizer import _resolve_datagen_mode
    from lakebench.config.scale import compute_guidance as _compute_guidance
    from lakebench.spark.job import _JOB_PROFILES, _scale_executor_count

    spark = cfg.platform.compute.spark
    s3 = cfg.platform.storage.s3
    arch = cfg.architecture
    workload = arch.workload
    scale = workload.datagen.get_effective_scale()
    dims = cfg.get_scale_dimensions()
    guidance = _compute_guidance(scale)

    # Derive workload profile: {schema}-{mode}
    effective_mode = _resolve_datagen_mode(cfg)
    workload_profile = f"{workload.schema_type.value}-{effective_mode}"

    # Per-job executor counts with auto/override labels
    override_map = {
        "bronze-verify": spark.bronze_executors,
        "silver-build": spark.silver_executors,
        "gold-finalize": spark.gold_executors,
    }
    executor_parts = []
    for job_name, override_val in override_map.items():
        auto_count = _scale_executor_count(_JOB_PROFILES[job_name], scale)
        if override_val is not None:
            executor_parts.append(f"{job_name}={override_val} (override)")
        else:
            executor_parts.append(f"{job_name}={auto_count} (auto)")

    # Streaming executor counts
    streaming_override_map = {
        "bronze-ingest": spark.bronze_ingest_executors,
        "silver-stream": spark.silver_stream_executors,
        "gold-refresh": spark.gold_refresh_executors,
    }
    streaming_executor_parts = []
    for job_name, override_val in streaming_override_map.items():
        auto_count = _scale_executor_count(_JOB_PROFILES[job_name], scale)
        if override_val is not None:
            streaming_executor_parts.append(f"{job_name}={override_val} (override)")
        else:
            streaming_executor_parts.append(f"{job_name}={auto_count} (auto)")

    # Build info lines
    lines = [
        ("Deployment", cfg.name),
        ("Namespace", cfg.get_namespace()),
        ("Workload", workload_profile),
        ("Schema", workload.schema_type.value),
        ("Scale", f"{scale} (~{dims.approx_bronze_gb:.0f} GB bronze)"),
        ("Customers", f"{dims.customers:,}"),
        ("Events/customer", f"~{dims.events_per_customer}"),
        ("Date range", f"{dims.date_range_days} days"),
        ("Approx rows", f"{dims.approx_rows:,}"),
        ("Processing", f"{arch.pipeline.pattern.value} (bronze > silver > gold)"),
        ("Catalog", f"{arch.catalog.type.value}"),
        ("Table format", f"{arch.table_format.type.value} {arch.table_format.iceberg.version}"),
        ("Query engine", f"{arch.query_engine.type.value}"),
        ("Parallelism", str(workload.datagen.parallelism)),
        (
            "Compute tier",
            f"{guidance.tier_name} (rec: {guidance.recommended_executors} executors, {guidance.recommended_memory})",
        ),
        ("Executors", ", ".join(executor_parts)),
        ("Streaming", ", ".join(streaming_executor_parts)),
        ("S3 endpoint", s3.endpoint or "(not set)"),
        ("Buckets", f"{s3.buckets.bronze}, {s3.buckets.silver}, {s3.buckets.gold}"),
    ]

    # Add monitoring info if enabled
    obs = cfg.observability
    if obs.enabled:
        prom_status = "enabled" if obs.prometheus_stack_enabled else "disabled"
        graf_status = "enabled" if obs.dashboards_enabled else "disabled"
        lines.append(
            (
                "Prometheus",
                f"{prom_status} (retention={obs.retention}, storage={obs.storage})",
            )
        )
        lines.append(("Grafana", graf_status))

    # Format as panel
    max_label = max(len(label) for label, _ in lines)
    formatted = "\n".join(
        f"[dim]{label + ':':<{max_label + 1}}[/dim] [bold]{value}[/bold]" for label, value in lines
    )

    console.print(Panel(formatted, title=f"Lakebench: {cfg.name}", expand=False))

    if guidance.warning:
        console.print(f"  [yellow]Warning: {guidance.warning}[/yellow]")

    # Check cluster feasibility
    try:
        k8s = get_k8s_client()
        cap = k8s.get_cluster_capacity()
        if cap is None:
            raise ValueError("Could not detect cluster capacity")
        cluster_cores = cap.total_cpu_millicores // 1000

        # Quick feasibility check using the recommend logic
        from lakebench.config.scale import full_compute_guidance as _full_guidance

        full_g = _full_guidance(scale)

        # Calculate requirements (simplified version of recommend's logic)
        spark_cores = full_g.spark.recommended_executors * full_g.spark.recommended_cores
        datagen_cores = full_g.datagen.parallelism * int(full_g.datagen.cpu)
        trino_cores = int(full_g.trino.coordinator_cpu) + full_g.trino.worker_replicas * int(
            full_g.trino.worker_cpu
        )
        peak_cores = max(spark_cores, datagen_cores) + trino_cores + 4
        needed_cores = int(peak_cores * 1.15)

        if cluster_cores >= needed_cores:
            console.print(
                f"  [green]Cluster OK:[/green] {cluster_cores} cores available, {needed_cores} needed"
            )
        else:
            console.print(
                f"  [red]Cluster undersized:[/red] {cluster_cores} cores available, {needed_cores} needed"
            )
            console.print("  [dim]Run 'lakebench recommend' to find max feasible scale[/dim]")
    except Exception as e:
        logger.debug("Could not check cluster feasibility: %s", e)


# =============================================================================
# Enterprise Query Examples
# =============================================================================

EXAMPLE_QUERIES: dict[str, tuple[str, str]] = {
    "count": (
        "Row counts per table",
        """SELECT 'silver.customer_interactions_enriched' AS table_name, count(*) AS row_count
FROM lakehouse.silver.customer_interactions_enriched
UNION ALL
SELECT 'gold.customer_executive_dashboard', count(*)
FROM lakehouse.gold.customer_executive_dashboard""",
    ),
    "revenue": (
        "Daily revenue with 7-day moving average",
        """SELECT
  interaction_date,
  total_daily_revenue,
  ROUND(avg_transaction_value, 2) AS avg_txn,
  daily_active_customers AS dau,
  ROUND(AVG(total_daily_revenue) OVER (
    ORDER BY interaction_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS revenue_ma7
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
    "channels": (
        "Channel revenue breakdown",
        """SELECT
  interaction_date,
  web_revenue,
  mobile_revenue,
  store_revenue,
  call_center_revenue,
  total_daily_revenue,
  web_interactions + mobile_interactions + store_interactions AS total_interactions,
  conversions
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
    "engagement": (
        "Engagement and churn risk summary",
        """SELECT
  interaction_date,
  daily_active_customers,
  ROUND(avg_engagement_score, 2) AS avg_engagement,
  high_churn_risk_count,
  medium_churn_risk_count,
  support_tickets_created,
  ROUND(avg_satisfaction_score, 2) AS avg_satisfaction,
  loyalty_member_interactions,
  total_points_earned
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
    "funnel": (
        "Daily conversion funnel",
        """SELECT
  interaction_date,
  awareness_interactions,
  consideration_interactions,
  conversions,
  retention_interactions,
  ROUND(CAST(conversions AS DOUBLE) / NULLIF(awareness_interactions, 0) * 100, 2) AS conversion_rate_pct
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
    "clv": (
        "Lifetime value estimates by day",
        """SELECT
  interaction_date,
  daily_active_customers,
  ROUND(total_estimated_ltv, 2) AS total_ltv,
  ROUND(avg_estimated_ltv, 2) AS avg_ltv,
  total_transactions,
  ROUND(avg_transaction_value, 2) AS avg_txn_value,
  ROUND(largest_transaction, 2) AS max_txn
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
}


def _run_query_repl(
    config_file: Path,
    timeout: int,
    output_format: str,
) -> None:
    """Interactive SQL REPL for the configured query engine."""
    import sys

    from rich.prompt import Prompt

    from lakebench.benchmark.executor import get_executor

    if not sys.stdin.isatty():
        print_error("Interactive mode requires a terminal")
        raise typer.Exit(1)

    config_file = resolve_config_path(config_file)
    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    namespace = cfg.get_namespace()

    try:
        executor = get_executor(cfg, namespace)
    except ValueError as e:
        print_error(str(e))
        raise typer.Exit(1)  # noqa: B904

    console.print(
        Panel(
            f"Lakebench SQL REPL ({executor.engine_name()})\n"
            f"Namespace: {namespace}\n"
            f"Format: {output_format} | Timeout: {timeout}s\n"
            f"Type 'exit', 'quit', or Ctrl+D to quit",
            expand=False,
        )
    )

    query_count = 0
    while True:
        try:
            sql = Prompt.ask("\n[bold cyan]SQL[/bold cyan]")
        except (EOFError, KeyboardInterrupt):
            break

        sql = sql.strip()

        # Exit commands
        if sql.lower() in ("exit", "quit", ".quit", "\\q"):
            break

        # Skip empty input
        if not sql:
            continue

        # Strip trailing semicolon
        if sql.endswith(";"):
            sql = sql[:-1]

        query_count += 1

        try:
            result = executor.execute_query(sql, timeout=timeout)
        except RuntimeError as e:
            print_error(str(e))
            continue

        if not result.success:
            print_error(result.error or "Query failed")
            continue

        # Display results
        output = result.raw_output
        rows = output.split("\n") if output else []
        row_count = result.rows_returned

        if rows:
            if output_format == "json":
                import json

                data = [row.split("\t") for row in rows]
                console.print(json.dumps({"rows": data, "count": len(data)}, indent=2))
            elif output_format == "csv":
                import csv
                import io

                buf = io.StringIO()
                writer = csv.writer(buf)
                for row in rows:
                    writer.writerow(row.split("\t"))
                console.print(buf.getvalue().strip())
            else:  # table
                for line in rows[:50]:
                    console.print(line)
                if row_count > 50:
                    console.print(f"[dim]... ({row_count - 50} more rows)[/dim]")

        console.print(f"[green]{row_count} rows in {result.duration_seconds:.2f}s[/green]")

    console.print(f"\n[dim]Executed {query_count} queries. Goodbye![/dim]")


@app.command()
def query(
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
    sql: Annotated[
        str | None,
        typer.Option(
            "--sql",
            "-q",
            help="SQL query to execute",
        ),
    ] = None,
    example: Annotated[
        str | None,
        typer.Option(
            "--example",
            "-e",
            help="Run a built-in example query (count, revenue, channels, engagement, funnel, clv)",
        ),
    ] = None,
    file: Annotated[
        Path | None,
        typer.Option(
            "--file",
            help="Read SQL from file (use '-' for stdin)",
        ),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Start interactive SQL shell (REPL)",
        ),
    ] = False,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-o",
            help="Output format: table (default), json, csv",
        ),
    ] = "table",
    show_query: Annotated[
        bool,
        typer.Option(
            "--show-query",
            help="Show the SQL query before executing",
        ),
    ] = False,
    query_timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            "-t",
            help="Query timeout in seconds",
        ),
    ] = 120,
) -> None:
    """Execute SQL queries against the configured query engine.

    Run custom SQL, built-in examples, read from file, or start interactive shell.
    Results can be displayed as table, JSON, or CSV.

    Examples:

        lakebench query --example count

        lakebench query --sql "SELECT count(*) FROM lakehouse.gold.customer_executive_dashboard"

        lakebench query --file query.sql

        lakebench query --file - < query.sql

        lakebench query --interactive

        lakebench query --example count --format json
    """
    import sys

    from lakebench.metrics import QueryMetrics

    config_file = resolve_config_path(config_file, file_option)

    # Validate mutually exclusive options
    sources = sum(1 for x in [sql, example, file, interactive] if x)
    if sources > 1:
        print_error("Specify only one of: --sql, --example, --file, --interactive")
        raise typer.Exit(1)

    # Handle interactive mode early
    if interactive:
        _run_query_repl(config_file, query_timeout, output_format)
        return

    # Handle file input
    query_name = "custom"
    if file is not None:
        if str(file) == "-":
            if sys.stdin.isatty():
                print_error("No input from stdin (pipe SQL or use --sql/--example)")
                raise typer.Exit(1)
            sql = sys.stdin.read().strip()
            query_name = "stdin"
        else:
            try:
                sql = file.read_text().strip()
                query_name = file.stem
            except FileNotFoundError:
                print_error(f"File not found: {file}")
                raise typer.Exit(1)  # noqa: B904
            except Exception as e:
                print_error(f"Error reading file: {e}")
                raise typer.Exit(1)  # noqa: B904
    elif example:
        if example not in EXAMPLE_QUERIES:
            print_error(f"Unknown example: {example}")
            print_info(f"Available: {', '.join(EXAMPLE_QUERIES.keys())}")
            raise typer.Exit(1)
        query_name = example
        _, sql = EXAMPLE_QUERIES[example]
    elif not sql:
        # No input specified - show help
        console.print(Panel("Built-in Enterprise Query Examples", expand=False))
        table = Table()
        table.add_column("Name", style="cyan")
        table.add_column("Description")
        for name, (desc, _) in EXAMPLE_QUERIES.items():
            table.add_row(name, desc)
        console.print(table)
        print_info("Usage: lakebench query <config> --example <name>")
        print_info('Usage: lakebench query <config> --sql "SELECT ..."')
        print_info("Usage: lakebench query <config> --file query.sql")
        print_info("Usage: lakebench query <config> --interactive")
        return

    if not sql:
        print_error("No SQL query provided")
        raise typer.Exit(1)

    # Load config
    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    namespace = cfg.get_namespace()

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.QUERY, {"query_name": query_name})

    if show_query or example:
        console.print(f"\n[dim]Query ({query_name}):[/dim]")
        console.print(f"[dim]{sql}[/dim]\n")

    # Execute via QueryExecutor
    from lakebench.benchmark.executor import get_executor

    try:
        executor = get_executor(cfg, namespace)
    except ValueError as e:
        print_error(str(e))
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    try:
        result = executor.execute_query(sql, timeout=query_timeout)
    except FileNotFoundError:
        print_error("kubectl not found on PATH")
        _journal_safe(j.end_command, success=False, message="kubectl not found")
        raise typer.Exit(1)  # noqa: B904
    except RuntimeError as e:
        print_error(str(e))
        print_info("Is the query engine deployed? Run: lakebench status")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    elapsed = result.duration_seconds

    if not result.success:
        print_error(f"Query failed ({elapsed:.2f}s)")
        if result.error:
            console.print(f"[red]{result.error}[/red]")
        _journal_safe(
            j.record,
            EventType.QUERY_EXECUTED,
            message=f"Query '{query_name}' failed",
            success=False,
            details={
                "query_name": query_name,
                "elapsed_seconds": round(elapsed, 3),
                "success": False,
            },
        )
        _journal_safe(j.end_command, success=False, message="Query failed")
        raise typer.Exit(1)

    # Parse and display results
    output = result.raw_output
    rows = output.split("\n") if output else []
    row_count = result.rows_returned

    if rows:
        if output_format == "json":
            import json

            parsed = [row.split("\t") for row in rows if row.strip()]
            if len(parsed) > 1:
                headers = [h.strip().strip('"') for h in parsed[0]]
                data = [
                    dict(zip(headers, [v.strip().strip('"') for v in r], strict=False))
                    for r in parsed[1:]
                ]
            else:
                data = parsed
            console.print(json.dumps({"rows": data, "count": len(data)}, indent=2))
        elif output_format == "csv":
            import csv
            import io

            buf = io.StringIO()
            writer = csv.writer(buf)
            for row in rows:
                if row.strip():
                    writer.writerow([c.strip().strip('"') for c in row.split("\t")])
            console.print(buf.getvalue().strip())
        else:  # table (default)
            console.print()
            for line in rows[:50]:
                console.print(line)
            if row_count > 50:
                console.print(f"[dim]... ({row_count - 50} more rows)[/dim]")

    console.print(f"\n[green]{row_count} rows in {elapsed:.2f}s[/green]")

    _journal_safe(
        j.record,
        EventType.QUERY_EXECUTED,
        message=f"Query '{query_name}' returned {row_count} rows in {elapsed:.2f}s",
        success=True,
        details={
            "query_name": query_name,
            "elapsed_seconds": round(elapsed, 3),
            "rows": row_count,
            "success": True,
        },
    )
    _journal_safe(j.end_command, success=True)

    # Record query metrics
    query_metrics = QueryMetrics(
        query_name=query_name,
        query_text=sql,
        elapsed_seconds=elapsed,
        rows_returned=row_count,
        success=True,
    )

    # Try to append to latest run's metrics
    from lakebench.metrics import MetricsStorage

    storage = MetricsStorage()
    latest_run = storage.get_latest_run()
    if latest_run:
        latest_run.queries.append(query_metrics)
        storage.save_run(latest_run)
        print_info(f"Query metrics appended to run {latest_run.run_id}")


@app.command()
def benchmark(
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
    mode: Annotated[
        str | None,
        typer.Option(
            "--mode",
            "-m",
            help="Benchmark mode: power, throughput, or composite (overrides config)",
        ),
    ] = None,
    streams: Annotated[
        int | None,
        typer.Option(
            "--streams",
            "-s",
            help="Number of concurrent query streams for throughput/composite (overrides config)",
        ),
    ] = None,
    cold: Annotated[
        bool,
        typer.Option(
            "--cold",
            help="Flush Iceberg metadata cache before each query (cold run)",
        ),
    ] = False,
    iterations: Annotated[
        int,
        typer.Option(
            "--iterations",
            "-n",
            help="Number of iterations per query (>1 uses median)",
        ),
    ] = 1,
    query_class: Annotated[
        str | None,
        typer.Option(
            "--class",
            "-c",
            help="Run only queries of a specific class (scan, analytics, gold)",
        ),
    ] = None,
) -> None:
    """Run query benchmark and compute QpH.

    Executes 8 analytical queries against the Customer 360 pipeline
    and reports Queries per Hour (QpH) throughput.

    Modes:

        power       Single sequential query stream (default)

        throughput   N concurrent query streams

        composite    Power + throughput, geometric mean QpH

    Examples:

        lakebench benchmark test-config.yaml

        lakebench benchmark test-config.yaml --cold

        lakebench benchmark test-config.yaml --iterations 5

        lakebench benchmark test-config.yaml --mode throughput --streams 8

        lakebench benchmark test-config.yaml --mode composite --streams 4

        lakebench benchmark test-config.yaml --class scan
    """
    from lakebench.benchmark import BenchmarkRunner
    from lakebench.metrics import BenchmarkMetrics, MetricsStorage

    config_file = resolve_config_path(config_file, file_option)

    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    scale = cfg.architecture.workload.datagen.get_effective_scale()
    dims = cfg.get_scale_dimensions()
    cache_mode = "cold" if cold else None  # None = let runner use config default

    # Resolve effective mode for display
    bench_cfg = cfg.architecture.benchmark
    effective_mode = mode or bench_cfg.mode.value
    effective_streams = streams if streams is not None else bench_cfg.streams
    effective_cache = cache_mode or bench_cfg.cache

    console.print(
        Panel(
            f"Lakebench Query Benchmark\n"
            f"{'=' * 25}\n"
            f"Scale: {scale} (~{dims.approx_bronze_gb:.0f} GB Bronze)\n"
            f"Mode: {effective_mode}"
            + (f" ({effective_streams} streams)" if effective_mode != "power" else "")
            + f" ({iterations} iteration{'s' if iterations > 1 else ''})\n"
            f"Cache: {effective_cache}" + (f"\nClass: {query_class}" if query_class else ""),
            expand=False,
        )
    )

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(
        CommandName.BENCHMARK,
        {
            "mode": effective_mode,
            "streams": effective_streams,
            "cold": cold,
            "iterations": iterations,
            "query_class": query_class,
        },
    )

    _journal_safe(
        j.record,
        EventType.BENCHMARK_START,
        message="Benchmark started",
        details={"mode": effective_mode, "cache": effective_cache, "scale": scale},
    )

    try:
        runner = BenchmarkRunner(cfg)
        run_result = runner.run(
            mode=mode,
            cache=cache_mode,
            iterations=iterations,
            streams=streams,
            query_class=query_class,
        )
    except RuntimeError as e:
        print_error(str(e))
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    # Handle composite (returns tuple) vs single result
    if isinstance(run_result, tuple):
        power_result, throughput_result, composite_result = run_result
        _display_power_results(power_result)
        _display_throughput_results(throughput_result)
        console.print(
            f"\n  [bold]Composite QpH: {composite_result.qph:.1f}[/bold]  "
            f"(geometric mean of power {power_result.qph:.1f} "
            f"and throughput {throughput_result.qph:.1f})"
        )
        primary_result = composite_result
    else:
        if run_result.mode == "throughput":
            _display_throughput_results(run_result)
        else:
            _display_power_results(run_result)
        primary_result = run_result

    # Save to latest metrics if available
    storage = MetricsStorage()
    latest_run = storage.get_latest_run()
    if latest_run:
        bench_metrics = BenchmarkMetrics(
            mode=primary_result.mode,
            cache=primary_result.cache,
            scale=primary_result.scale,
            qph=primary_result.qph,
            total_seconds=primary_result.total_seconds,
            queries=[q.to_dict() for q in primary_result.queries],
            iterations=primary_result.iterations,
            streams=primary_result.streams,
            stream_results=[s.to_dict() for s in primary_result.stream_results],
        )
        latest_run.benchmark = bench_metrics
        storage.save_run(latest_run)
        print_info(f"Benchmark metrics appended to run {latest_run.run_id}")

    _journal_safe(
        j.record,
        EventType.BENCHMARK_COMPLETE,
        message=f"Benchmark complete: QpH={primary_result.qph:.1f}",
        success=True,
        details={
            "mode": primary_result.mode,
            "qph": round(primary_result.qph, 1),
            "total_seconds": round(primary_result.total_seconds, 2),
            "queries_passed": sum(1 for q in primary_result.queries if q.success),
            "queries_total": len(primary_result.queries),
            "streams": primary_result.streams,
        },
    )
    _journal_safe(j.end_command, success=True)


def _display_power_results(result: Any) -> None:
    """Display power benchmark results."""
    console.print()
    for qr in result.queries:
        status = "[green]PASS[/green]" if qr.success else "[red]FAIL[/red]"
        name_padded = f"{qr.query.name[:4]}  {qr.query.display_name}"
        console.print(
            f"  {name_padded:<40} {qr.elapsed_seconds:>7.2f}s   "
            f"{qr.rows_returned:>6} rows   {status}"
        )

    console.print(f"\n  Total: {result.total_seconds:.2f}s")
    console.print(f"  [bold]Power QpH: {result.qph:.1f}[/bold]")


def _display_throughput_results(result: Any) -> None:
    """Display throughput benchmark results."""
    console.print()
    console.print(f"  [bold]Throughput run: {result.streams} streams[/bold]")
    for sr in result.stream_results:
        status = "[green]PASS[/green]" if sr.success else "[red]FAIL[/red]"
        console.print(
            f"    Stream {sr.stream_id}:  {len(sr.queries):>2} queries  "
            f"{sr.total_seconds:>7.1f}s  {status}"
        )
    console.print(f"\n  Wall clock: {result.total_seconds:.1f}s")
    console.print(f"  [bold]Throughput QpH: {result.qph:.1f}[/bold]")


@app.command()
def report(
    metrics_dir: Annotated[
        Path,
        typer.Option(
            "--metrics",
            "-m",
            help="Directory containing run subdirectories",
        ),
    ] = Path(DEFAULT_OUTPUT_DIR) / "runs",
    run_id: Annotated[
        str | None,
        typer.Option(
            "--run",
            "-r",
            help="Specific run ID to report on (default: latest)",
        ),
    ] = None,
    list_runs: Annotated[
        bool,
        typer.Option(
            "--list",
            "-l",
            help="List available runs instead of generating report",
        ),
    ] = False,
) -> None:
    """Generate benchmark report from collected metrics.

    Creates an HTML report inside the per-run directory.
    """
    from lakebench.metrics import MetricsStorage
    from lakebench.reports import ReportGenerator

    storage = MetricsStorage(metrics_dir)

    # List runs mode
    if list_runs:
        runs = storage.list_runs()
        if not runs:
            print_warning(f"No runs found in {metrics_dir}")
            return

        console.print(Panel(f"Available runs in [bold]{metrics_dir}[/bold]", expand=False))

        from rich.table import Table

        table = Table()
        table.add_column("Run ID", style="cyan")
        table.add_column("Deployment")
        table.add_column("Date")
        table.add_column("Status")
        table.add_column("Duration")

        for r in runs:
            status = "[green]Passed[/green]" if r.get("success") else "[red]Failed[/red]"
            elapsed = f"{r.get('total_elapsed_seconds', 0):.1f}s"
            date = r.get("start_time", "")[:10] if r.get("start_time") else ""
            table.add_row(
                r.get("run_id", ""),
                r.get("deployment_name", ""),
                date,
                status,
                elapsed,
            )

        console.print(table)
        return

    # Generate report mode
    try:
        generator = ReportGenerator(metrics_dir)
        report_path = generator.generate_report(run_id)

        console.print(
            Panel(
                f"[green]Report generated successfully![/green]\n\n"
                f"Output: {report_path}\n\n"
                f"Open in browser to view.",
                title="Report Generated",
                expand=False,
            )
        )

    except ValueError as e:
        print_error(str(e))
        print_info("Use 'lakebench report --list' to see available runs")
        raise typer.Exit(1)  # noqa: B904


@app.command()
def results(
    metrics_dir: Annotated[
        Path,
        typer.Option(
            "--metrics",
            "-m",
            help="Directory containing run subdirectories",
        ),
    ] = Path(DEFAULT_OUTPUT_DIR) / "runs",
    run_id: Annotated[
        str | None,
        typer.Option(
            "--run",
            "-r",
            help="Specific run ID (default: latest)",
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Output format: table, json, csv",
        ),
    ] = "table",
) -> None:
    """Display pipeline benchmark results.

    Shows the stage-matrix view of pipeline performance -- each stage
    as a column with consistent metrics as rows. Use --format json or
    --format csv for machine-readable output.
    """
    import json as _json

    from lakebench.metrics import MetricsStorage

    storage = MetricsStorage(metrics_dir)

    if run_id:
        metrics = storage.load_run(run_id)
    else:
        metrics = storage.get_latest_run()

    if metrics is None:
        print_error("No run found" + (f" with ID {run_id}" if run_id else ""))
        print_info("Use 'lakebench report --list' to see available runs")
        raise typer.Exit(1)

    pb = metrics.pipeline_benchmark
    if pb is None:
        print_warning("This run does not have pipeline benchmark data.")
        print_info("Pipeline benchmark is generated for runs after this feature was added.")
        raise typer.Exit(1)

    if output_format == "json":
        console.print(_json.dumps(pb.to_dict(), indent=2))
        return

    if output_format == "csv":
        import csv
        import io

        matrix = pb.to_matrix()
        if not matrix:
            print_warning("No stages in pipeline benchmark.")
            return
        # Build CSV: rows are metrics, columns are stages
        metric_keys = list(next(iter(matrix.values())).keys())
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["metric"] + list(matrix.keys()))
        for key in metric_keys:
            row = [key] + [matrix[stage].get(key, "") for stage in matrix]
            writer.writerow(row)
        console.print(buf.getvalue())
        return

    # Table format (default)
    console.print()
    console.print(
        Panel(
            f"[bold]Pipeline Benchmark:[/bold] {pb.deployment_name} (run {pb.run_id})\n"
            f"Mode: {pb.pipeline_mode} | "
            f"Time-to-Value: {pb.time_to_value_seconds:.1f}s | "
            f"Throughput: {pb.pipeline_throughput_gb_per_second:.3f} GB/s",
            expand=False,
        )
    )

    table = Table(show_header=True, header_style="bold")
    table.add_column("Stage", style="cyan")
    table.add_column("Engine")
    table.add_column("Time(s)", justify="right")
    table.add_column("In(GB)", justify="right")
    table.add_column("Out(GB)", justify="right")
    table.add_column("In Rows", justify="right")
    table.add_column("Out Rows", justify="right")
    table.add_column("GB/s", justify="right")
    table.add_column("Rows/s", justify="right")
    table.add_column("Execs", justify="right")
    table.add_column("Status")

    for stage in pb.stages:
        status = "[green]OK[/green]" if stage.success else "[red]FAIL[/red]"
        table.add_row(
            stage.stage_name,
            stage.engine,
            f"{stage.elapsed_seconds:.1f}",
            f"{stage.input_size_gb:.3f}" if stage.input_size_gb > 0 else "-",
            f"{stage.output_size_gb:.3f}" if stage.output_size_gb > 0 else "-",
            f"{stage.input_rows:,}" if stage.input_rows > 0 else "-",
            f"{stage.output_rows:,}" if stage.output_rows > 0 else "-",
            f"{stage.throughput_gb_per_second:.4f}" if stage.throughput_gb_per_second > 0 else "-",
            f"{stage.throughput_rows_per_second:.0f}"
            if stage.throughput_rows_per_second > 0
            else "-",
            str(stage.executor_count) if stage.executor_count > 0 else "-",
            status,
        )

    console.print(table)

    # Query stage detail
    if pb.query_benchmark:
        qb = pb.query_benchmark
        console.print(
            f"\n  Query Benchmark: {qb.mode} mode | QpH: {qb.qph:.1f} | {qb.total_seconds:.1f}s"
        )

    console.print(
        f"\n  Pipeline: {pb.total_elapsed_seconds:.1f}s total"
        f" | {pb.time_to_value_seconds:.1f}s time-to-value"
        f" | {pb.pipeline_throughput_gb_per_second:.3f} GB/s"
    )
    console.print()


@app.command()
def logs(
    component: Annotated[
        str,
        typer.Argument(
            help="Component to show logs for (postgres, hive, polaris, trino, spark-driver)",
        ),
    ],
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
    follow: Annotated[
        bool,
        typer.Option(
            "--follow",
            "-f",
            help="Follow log output",
        ),
    ] = False,
    lines: Annotated[
        int,
        typer.Option(
            "--lines",
            "-n",
            help="Number of lines to show",
        ),
    ] = 100,
) -> None:
    """Stream logs from a component.

    Shows logs from the specified Lakebench component.

    Valid components: postgres, hive, polaris, trino, spark-driver
    """
    # Map component names to pod selectors
    COMPONENT_SELECTORS = {
        "postgres": ("app.kubernetes.io/component=postgres", None),
        "hive": ("app.kubernetes.io/component=metastore", None),
        "polaris": ("app.kubernetes.io/component=polaris", None),
        "trino": ("app.kubernetes.io/component=trino-coordinator", None),
        "spark-driver": ("spark-role=driver", None),
    }

    if component not in COMPONENT_SELECTORS:
        print_error(f"Unknown component: {component}")
        print_info(f"Valid components: {', '.join(COMPONENT_SELECTORS.keys())}")
        raise typer.Exit(1)

    config_file = resolve_config_path(config_file, file_option)

    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    namespace = cfg.get_namespace()
    label_selector, container = COMPONENT_SELECTORS[component]

    console.print(
        f"Fetching logs for [bold]{component}[/bold] in namespace [bold]{namespace}[/bold]"
    )

    # Build kubectl logs command
    cmd = [
        "kubectl",
        "logs",
        "-l",
        label_selector,
        "-n",
        namespace,
        f"--tail={lines}",
    ]
    if container:
        cmd.extend(["-c", container])
    if follow:
        cmd.append("-f")

    try:
        if follow:
            # Stream logs
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                if process.stdout:
                    for line in iter(process.stdout.readline, ""):
                        console.print(_strip_ansi(line), end="")
            except KeyboardInterrupt:
                process.terminate()
                print_info("\nLog streaming stopped")
        else:
            # One-shot log fetch
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                if "not found" in result.stderr.lower() or not result.stderr.strip():
                    print_warning(f"No pods found for {component}")
                    print_info("Is the component deployed? Run: lakebench status")
                else:
                    print_error(result.stderr.strip())
                return

            if result.stdout:
                console.print(_strip_ansi(result.stdout))
            else:
                print_warning(f"No logs available for {component}")

    except subprocess.TimeoutExpired:
        print_error("Timed out fetching logs")
    except FileNotFoundError:
        print_error("kubectl not found on PATH")


@app.command()
def journal(
    session_id: Annotated[
        str | None,
        typer.Option(
            "--session",
            "-s",
            help="Show events for a specific session",
        ),
    ] = None,
    last: Annotated[
        int,
        typer.Option(
            "--last",
            "-n",
            help="Show last N sessions",
        ),
    ] = 10,
    journal_dir: Annotated[
        Path,
        typer.Option(
            "--dir",
            help="Journal directory",
        ),
    ] = Path(DEFAULT_OUTPUT_DIR) / "journal",
) -> None:
    """View command and execution provenance journal.

    Shows the history of all lakebench operations including deploys,
    data generation, pipeline runs, and teardowns.

    Examples:

        lakebench journal

        lakebench journal --session 20260129-120000-a1b2c3
    """
    j = Journal(journal_dir)

    if session_id:
        events = j.load_session_events(session_id)
        if not events:
            print_warning(f"No events found for session {session_id}")
            return

        console.print(Panel(f"Session: [bold]{session_id}[/bold]", expand=False))
        table = Table()
        table.add_column("Time", style="dim", width=19)
        table.add_column("Event", style="cyan")
        table.add_column("Command", style="bold")
        table.add_column("Message")
        table.add_column("Status", justify="center")

        for event in events:
            ts = event.get("timestamp", "")[:19]
            etype = event.get("event_type", "").split(".")[-1]
            cmd = event.get("command", "") or ""
            msg = event.get("message", "")
            success = event.get("success")
            status = ""
            if success is True:
                status = "[green]OK[/green]"
            elif success is False:
                status = "[red]FAIL[/red]"
            table.add_row(ts, etype, cmd, msg, status)

        console.print(table)
    else:
        sessions = j.list_sessions()
        if not sessions:
            print_warning(f"No journal sessions found in {journal_dir}")
            return

        console.print(Panel("Lakebench Journal Sessions", expand=False))
        table = Table()
        table.add_column("Session ID", style="cyan")
        table.add_column("Config")
        table.add_column("Started", style="dim")
        table.add_column("Events", justify="right")
        table.add_column("Commands")
        table.add_column("Status")

        for s in sessions[:last]:
            status = "[green]closed[/green]" if s["closed"] else "[yellow]active[/yellow]"
            cmds = ", ".join(s.get("commands", []))
            table.add_row(
                s["session_id"],
                s.get("config_name", ""),
                s.get("started", "")[:19],
                str(s.get("event_count", 0)),
                cmds,
                status,
            )

        console.print(table)


# =============================================================================
# Cluster Sizing Guidance
# =============================================================================


@app.command()
def recommend(
    cluster_cores: Annotated[
        int | None,
        typer.Option(
            "--cores",
            "-c",
            help="Total cluster CPU cores (auto-detects from connected cluster if not set)",
        ),
    ] = None,
    cluster_memory_gb: Annotated[
        int | None,
        typer.Option(
            "--memory",
            "-m",
            help="Total cluster memory in GB (auto-detects from connected cluster if not set)",
        ),
    ] = None,
    target_scale: Annotated[
        int | None,
        typer.Option(
            "--scale",
            "-s",
            help="Target scale factor to check requirements for",
        ),
    ] = None,
    extended: Annotated[
        bool,
        typer.Option(
            "--extended",
            "-e",
            help="Show extended scale (slower datagen, same Spark). Reduces datagen parallelism to fit cluster.",
        ),
    ] = False,
) -> None:
    """Show cluster sizing guidance for lakebench workloads.

    This command helps answer two questions:
    - "I have cluster X -- what scale can I run?"
    - "I want to run scale X -- what cluster do I need?"

    Without arguments, auto-detects connected cluster capacity and shows
    the maximum feasible scale. Use --scale to check requirements for
    a specific target (any scale from 1 to 100,000,000+ is supported).

    Scale maps linearly to data volume: scale 1 = ~10 GB, scale 100 = ~1 TB,
    scale 100,000 = ~1 PB.

    Examples:

        lakebench recommend                     # auto-detect cluster, find max scale
        lakebench recommend --cores 64 --memory 256
        lakebench recommend --scale 100         # what do I need for scale 100?
        lakebench recommend --scale 100000      # what do I need for 1 PB?
    """
    from lakebench.config.scale import customer360_dimensions, full_compute_guidance

    def format_data_size(gb: float) -> str:
        """Format data size in human-readable units."""
        if gb >= 1_000_000_000:  # 1 EB = 10^9 GB
            return f"{gb / 1_000_000_000:.1f} EB"
        if gb >= 1_000_000:  # 1 PB = 10^6 GB
            return f"{gb / 1_000_000:.1f} PB"
        if gb >= 1_000:
            return f"{gb / 1_000:.1f} TB"
        return f"{gb:.0f} GB"

    def compute_cluster_requirements(scale: int) -> dict:
        """Compute minimum cluster requirements for a given scale."""
        dims = customer360_dimensions(scale)
        guidance = full_compute_guidance(scale)

        # Calculate total resource requirements
        # Spark (batch mode uses sequential phases, so just max per phase)
        spark_cores = guidance.spark.recommended_executors * guidance.spark.recommended_cores
        spark_mem_gi = guidance.spark.recommended_executors * int(
            guidance.spark.recommended_memory.rstrip("g")
        )

        # Datagen (runs concurrently with Trino in batch mode)
        datagen_cores = guidance.datagen.parallelism * int(guidance.datagen.cpu)
        datagen_mem_gi = guidance.datagen.parallelism * int(
            guidance.datagen.memory.rstrip("Gi").rstrip("gi")
        )

        # Trino (always running)
        trino_cores = int(guidance.trino.coordinator_cpu) + guidance.trino.worker_replicas * int(
            guidance.trino.worker_cpu
        )
        trino_mem_gi = int(
            guidance.trino.coordinator_memory.rstrip("Gi")
        ) + guidance.trino.worker_replicas * int(guidance.trino.worker_memory.rstrip("Gi"))

        # Infra overhead (Hive metastore, Postgres) ~4 cores, 8 Gi
        infra_cores = 4
        infra_mem_gi = 8

        # Batch mode: datagen runs first, then Spark. They don't overlap.
        # So peak is max(datagen, spark) + trino + infra
        peak_workload_cores = max(spark_cores, datagen_cores)
        peak_workload_mem = max(spark_mem_gi, datagen_mem_gi)

        total_cores = peak_workload_cores + trino_cores + infra_cores
        total_mem_gi = peak_workload_mem + trino_mem_gi + infra_mem_gi

        # Add 15% headroom for system pods
        total_cores = int(total_cores * 1.15)
        total_mem_gi = int(total_mem_gi * 1.15)

        return {
            "scale": scale,
            "data_gb": dims.approx_bronze_gb,
            "tier": guidance.spark.tier_name,
            "spark_executors": guidance.spark.recommended_executors,
            "spark_cores_per_exec": guidance.spark.recommended_cores,
            "spark_mem_per_exec": guidance.spark.recommended_memory,
            "datagen_pods": guidance.datagen.parallelism,
            "trino_workers": guidance.trino.worker_replicas,
            "total_cores": total_cores,
            "total_mem_gi": total_mem_gi,
        }

    def is_feasible(scale: int, cores: int, mem_gb: int) -> bool:
        """Check if a scale is feasible on given cluster resources."""
        reqs = compute_cluster_requirements(scale)
        return cores >= reqs["total_cores"] and mem_gb >= reqs["total_mem_gi"]

    def compute_spark_only_requirements(scale: int) -> dict:
        """Compute requirements assuming datagen runs with reduced parallelism.

        In 'extended' mode, datagen parallelism is reduced to fit the cluster.
        This means datagen takes longer, but Spark and Trino requirements remain.
        The limiting factor becomes Spark phase, not datagen phase.
        """
        dims = customer360_dimensions(scale)
        guidance = full_compute_guidance(scale)

        # Spark requirements (the actual compute workload)
        spark_cores = guidance.spark.recommended_executors * guidance.spark.recommended_cores
        spark_mem_gi = guidance.spark.recommended_executors * int(
            guidance.spark.recommended_memory.rstrip("g")
        )

        # Trino (always running)
        trino_cores = int(guidance.trino.coordinator_cpu) + guidance.trino.worker_replicas * int(
            guidance.trino.worker_cpu
        )
        trino_mem_gi = int(
            guidance.trino.coordinator_memory.rstrip("Gi")
        ) + guidance.trino.worker_replicas * int(guidance.trino.worker_memory.rstrip("Gi"))

        # Infra
        infra_cores = 4
        infra_mem_gi = 8

        # In extended mode, Spark phase is the constraint (datagen runs slower)
        total_cores = spark_cores + trino_cores + infra_cores
        total_mem_gi = spark_mem_gi + trino_mem_gi + infra_mem_gi

        # Add 15% headroom
        total_cores = int(total_cores * 1.15)
        total_mem_gi = int(total_mem_gi * 1.15)

        return {
            "scale": scale,
            "data_gb": dims.approx_bronze_gb,
            "tier": guidance.spark.tier_name,
            "spark_executors": guidance.spark.recommended_executors,
            "spark_cores_per_exec": guidance.spark.recommended_cores,
            "spark_mem_per_exec": guidance.spark.recommended_memory,
            "datagen_pods": guidance.datagen.parallelism,
            "trino_workers": guidance.trino.worker_replicas,
            "total_cores": total_cores,
            "total_mem_gi": total_mem_gi,
        }

    def is_feasible_extended(scale: int, cores: int, mem_gb: int) -> bool:
        """Check feasibility in extended mode (Spark-limited, not datagen-limited)."""
        reqs = compute_spark_only_requirements(scale)
        return cores >= reqs["total_cores"] and mem_gb >= reqs["total_mem_gi"]

    def find_max_scale(cores: int, mem_gb: int, use_extended: bool = False) -> int:
        """Binary search to find max feasible scale."""
        check_fn = is_feasible_extended if use_extended else is_feasible
        if not check_fn(1, cores, mem_gb):
            return 0

        # Binary search between 1 and 100 million (1 EB)
        lo, hi = 1, 100_000_000
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if check_fn(mid, cores, mem_gb):
                lo = mid
            else:
                hi = mid - 1
        return lo

    # Case 1: User wants to know requirements for a specific scale
    if target_scale is not None:
        reqs = compute_cluster_requirements(target_scale)
        dims = customer360_dimensions(target_scale)

        console.print(
            Panel(
                f"[bold]Scale {target_scale:,}[/bold] ({format_data_size(reqs['data_gb'])})\n\n"
                f"[dim]Tier:[/dim]             {reqs['tier']}\n"
                f"[dim]Rows:[/dim]             {dims.approx_rows:,}\n\n"
                f"[yellow]Minimum Cluster Requirements:[/yellow]\n"
                f"  CPU cores:       [bold]{reqs['total_cores']:,}[/bold]\n"
                f"  Memory:          [bold]{reqs['total_mem_gi']:,} GB[/bold]\n\n"
                f"[dim]Breakdown:[/dim]\n"
                f"  Spark:           {reqs['spark_executors']} × {reqs['spark_cores_per_exec']} cores × {reqs['spark_mem_per_exec']}\n"
                f"  Datagen:         {reqs['datagen_pods']} pods\n"
                f"  Trino:           {reqs['trino_workers']} workers\n"
                f"  + infra overhead",
                title="Cluster Requirements",
                expand=False,
            )
        )
        return

    # Case 2: Auto-detect cluster or use provided specs
    detected_cores = cluster_cores
    detected_mem = cluster_memory_gb
    cluster_source = "user-provided"

    if detected_cores is None or detected_mem is None:
        # Try to detect from connected cluster
        try:
            k8s = get_k8s_client()
            cap = k8s.get_cluster_capacity()
            if cap is not None:
                if detected_cores is None:
                    detected_cores = cap.total_cpu_millicores // 1000
                if detected_mem is None:
                    detected_mem = int(cap.total_memory_bytes / (1024**3))
                cluster_source = f"detected ({cap.node_count} nodes)"
        except Exception as e:
            console.print(f"[yellow]Could not detect cluster capacity: {e}[/yellow]")
            console.print("[dim]Use --cores and --memory to specify manually[/dim]\n")

    if detected_cores is None or detected_mem is None:
        # Show reference table without cluster info
        console.print("[bold]Cluster Sizing Reference[/bold]\n")
        console.print(
            "[dim]Tip: Connect to a cluster or use --cores/--memory for max scale calculation[/dim]\n"
        )
        console.print("[dim]Use --scale N to see requirements for any specific scale[/dim]\n")

        # Show common reference points
        table = Table(title="Common Scale Points")
        table.add_column("Scale", justify="right", style="cyan")
        table.add_column("Data Size", justify="right")
        table.add_column("Min Cores", justify="right")
        table.add_column("Min Memory", justify="right")

        for scale in [1, 10, 100, 500, 1000, 10000, 100000]:
            reqs = compute_cluster_requirements(scale)
            table.add_row(
                f"{scale:,}",
                format_data_size(reqs["data_gb"]),
                f"{reqs['total_cores']:,}",
                f"{reqs['total_mem_gi']:,} GB",
            )

        console.print(table)
        return

    # Case 3: Find max feasible scale for this cluster
    max_scale = find_max_scale(detected_cores, detected_mem, use_extended=extended)
    max_scale_standard = find_max_scale(detected_cores, detected_mem, use_extended=False)
    max_scale_extended = find_max_scale(detected_cores, detected_mem, use_extended=True)

    console.print(f"[bold]Cluster Capacity[/bold] ({cluster_source})")
    console.print(f"  CPU cores: [bold]{detected_cores}[/bold]")
    console.print(f"  Memory:    [bold]{detected_mem} GB[/bold]\n")

    if max_scale == 0:
        console.print("[yellow]Cluster is below minimum requirements for scale 1.[/yellow]")
        reqs = compute_cluster_requirements(1)
        console.print(
            f"[dim]Minimum for scale 1: {reqs['total_cores']} cores, {reqs['total_mem_gi']} GB RAM[/dim]"
        )
        return

    # Show both modes
    std_reqs = compute_cluster_requirements(max_scale_standard)
    ext_reqs = compute_spark_only_requirements(max_scale_extended)

    if extended:
        console.print(
            "[bold yellow]Extended mode:[/bold yellow] Datagen runs with reduced parallelism (slower, same resources)\n"
        )
        console.print(
            f"[green]Maximum scale (extended):[/green] [bold]{max_scale_extended:,}[/bold] ({format_data_size(ext_reqs['data_gb'])})"
        )
        console.print(
            f"[dim]Standard mode max:         {max_scale_standard:,} ({format_data_size(std_reqs['data_gb'])})[/dim]\n"
        )
    else:
        console.print(
            f"[green]Maximum scale (standard):[/green] [bold]{max_scale_standard:,}[/bold] ({format_data_size(std_reqs['data_gb'])})"
        )
        if max_scale_extended > max_scale_standard:
            console.print(
                f"[dim]Extended mode max:         {max_scale_extended:,} ({format_data_size(ext_reqs['data_gb'])}) -- use --extended[/dim]"
            )
        console.print()

    # Use appropriate requirements function based on mode
    req_fn = compute_spark_only_requirements if extended else compute_cluster_requirements
    check_fn = is_feasible_extended if extended else is_feasible

    # Build a cleaner table: only show FEASIBLE milestones + max + next infeasible tier
    # This avoids confusion from non-monotonic tier boundaries
    scale_points = []
    milestones = [1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000]

    # Only include milestones that are actually feasible
    for s in milestones:
        if s < max_scale and check_fn(s, detected_cores, detected_mem):
            scale_points.append(s)

    # Always include the max
    scale_points.append(max_scale)

    # Find the next milestone above max that's NOT feasible (to show "what you'd need")
    for s in milestones:
        if s > max_scale:
            scale_points.append(s)
            break
    # If no milestone above, use 2x max
    if (
        len(scale_points)
        == len(
            [s for s in milestones if s < max_scale and check_fn(s, detected_cores, detected_mem)]
        )
        + 1
    ):
        scale_points.append(max_scale * 2)

    table = Table(title="Scale Options")
    table.add_column("Scale", justify="right", style="cyan")
    table.add_column("Data", justify="right")
    table.add_column("Cores", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("Status")

    for scale in scale_points:
        reqs = req_fn(scale)
        feasible = check_fn(scale, detected_cores, detected_mem)

        if scale == max_scale:
            status = "[green bold]← MAX[/green bold]"
        elif feasible:
            status = "[green]OK[/green]"
        else:
            cores_need = max(0, reqs["total_cores"] - detected_cores)
            mem_need = max(0, reqs["total_mem_gi"] - detected_mem)
            parts = []
            if cores_need > 0:
                parts.append(f"+{cores_need:,} cores")
            if mem_need > 0:
                parts.append(f"+{mem_need:,} GB")
            status = f"[red]needs {', '.join(parts)}[/red]"

        table.add_row(
            f"{scale:,}",
            format_data_size(reqs["data_gb"]),
            f"{reqs['total_cores']:,}",
            f"{reqs['total_mem_gi']:,} GB",
            status,
        )

    console.print(table)

    # Show next steps
    console.print()
    if extended:
        console.print("[dim]Extended mode: datagen runs slower to fit cluster resources[/dim]")
    console.print(f"[dim]Next: lakebench init --scale {max_scale}[/dim]")


# =============================================================================
# Entry Point
# =============================================================================


def main() -> None:
    """Main entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
