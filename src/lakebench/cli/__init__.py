"""Lakebench CLI."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer
from kubernetes.config import ConfigException
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table

from lakebench import __version__
from lakebench._constants import DEFAULT_OUTPUT_DIR
from lakebench.cli._exit import LakebenchGroup
from lakebench.cli._helpers import (
    DEFAULT_CONFIG as DEFAULT_CONFIG,
)
from lakebench.cli._helpers import (
    DEPRECATED_SHORT_F_HELP,
    _journal_safe,
    _strip_ansi,
    console,
    emit_data,
    esc,
    journal_open,
    markup,
    print_error,
    print_info,
    print_success,
    print_warning,
    resolve_config_path,
    warn_deprecated_short_f,
)
from lakebench.cli._helpers import (
    get_journal as get_journal,
)
from lakebench.cli._json import json_option
from lakebench.cli._nameless import NAME_OPTION_HELP, guard_nameless
from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    LoadPurpose,
    load_config,
)
from lakebench.config.schema import is_continuous_mode
from lakebench.exit_codes import ExitCode
from lakebench.journal import DEFAULT_JOURNAL_DIR as DEFAULT_JOURNAL_DIR
from lakebench.journal import CommandName, EventType, Journal
from lakebench.k8s import (
    K8sConnectionError,
    PlatformType,
    SecurityVerifier,
    get_k8s_client,
)
from lakebench.s3 import test_s3_connectivity

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig

logger = logging.getLogger(__name__)


app = typer.Typer(
    name="lakebench",
    cls=LakebenchGroup,
    pretty_exceptions_enable=False,
    help="Deploy and benchmark lakehouse architectures on Kubernetes",
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="[dim]Workflow: init -> run -> results -> destroy[/dim]",
)


def _version_callback(value: bool) -> None:
    """Print version and exit. Invoked eagerly so `--version` short-
    circuits subcommand dispatch (and works before any subcommand-
    validation errors would fire)."""
    if value:
        console.print(f"Lakebench version {esc(__version__)}")
        raise typer.Exit()


@app.callback()
def _main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Root callback. Exists so `--version` is a top-level flag.

    Priya (roleplay P2) tried ``lakebench --version`` and got
    ``No such option``; the ``version`` subcommand is idiomatic but
    the flag form is muscle memory for anyone coming from any other
    CLI. Both work now."""


# Config subcommand group
from lakebench.cli._admin import admin_app  # noqa: E402
from lakebench.cli._config import config_app  # noqa: E402
from lakebench.cli._financial import financial_app  # noqa: E402

app.add_typer(config_app)
app.add_typer(financial_app)
app.add_typer(admin_app)

# Compare command (registered from separate module)
from lakebench.cli._compare import compare as _compare_fn  # noqa: E402

app.command(name="compare")(_compare_fn)

# Extracted commands (registered from separate modules)
from lakebench.cli._clean import clean as _clean_fn  # noqa: E402
from lakebench.cli._deploy import deploy as _deploy_fn  # noqa: E402
from lakebench.cli._destroy import destroy as _destroy_fn  # noqa: E402
from lakebench.cli._generate import generate as _generate_fn  # noqa: E402
from lakebench.cli._plan import plan as _plan_fn  # noqa: E402
from lakebench.cli._query import benchmark as _benchmark_fn  # noqa: E402
from lakebench.cli._query import query as _query_fn  # noqa: E402
from lakebench.cli._reproduce import reproduce as _reproduce_fn  # noqa: E402
from lakebench.cli._run import run as _run_fn  # noqa: E402

app.command(name="deploy")(_deploy_fn)
app.command(name="destroy")(_destroy_fn)
app.command(name="clean")(_clean_fn)
app.command(name="generate")(_generate_fn)
app.command(name="run")(_run_fn)
app.command(name="query")(_query_fn)
app.command(name="benchmark")(_benchmark_fn)
app.command(name="reproduce")(_reproduce_fn)
app.command(name="plan")(_plan_fn)

from lakebench.cli import _aliases  # noqa: E402

_aliases.register(app)


# Re-exports for backward compatibility (tests import these from lakebench.cli)
from lakebench.cli._deploy import _preflight_check as _preflight_check  # noqa: E402
from lakebench.cli._run import (  # noqa: E402
    _run_preflight_infra_check as _run_preflight_infra_check,
)
from lakebench.cli._sustained import (  # noqa: E402
    _collect_platform_metrics as _collect_platform_metrics,
)
from lakebench.cli._sustained import (  # noqa: E402
    _parse_spark_interval as _parse_spark_interval,
)
from lakebench.cli._sustained import (  # noqa: E402
    _run_iceberg_compaction as _run_iceberg_compaction,
)
from lakebench.cli._sustained import (  # noqa: E402
    _run_iceberg_maintenance as _run_iceberg_maintenance,
)


# Re-export for backward compatibility (report command references this)
def _note_benchmark_record(metrics) -> None:
    """One stderr line when *metrics* is a ``lakebench benchmark`` record: its
    pipeline numbers are its parent run's, its benchmark is its own."""
    if getattr(metrics, "record_kind", "run") == "benchmark":
        print_info(
            f"Run {metrics.run_id} is a benchmark record of run "
            f"{metrics.parent_run_id or 'unknown'}: the benchmark is its own, the "
            "pipeline numbers are that run's"
        )


def _print_report_summary(metrics) -> None:
    """Print key scores from saved metrics to the terminal."""
    _note_benchmark_record(metrics)
    pb = metrics.pipeline_benchmark
    if pb is None:
        print_warning("No pipeline benchmark data in this run.")
        return

    # Header
    status = "[green]Passed[/green]" if pb.success else "[red]Failed[/red]"
    header = (
        f"[bold]{pb.deployment_name}[/bold]  run {pb.run_id}\n"
        f"Mode: {pb.pipeline_mode} | Status: {status}"
    )

    # Stage table
    table = Table(show_header=True, header_style="bold", expand=False)
    table.add_column("Stage", style="cyan")
    table.add_column("Time", justify="right")
    table.add_column("In (GB)", justify="right")
    table.add_column("Out (GB)", justify="right")
    table.add_column("GB/s", justify="right")
    table.add_column("Executors", justify="right")

    for stage in pb.stages:
        table.add_row(
            stage.stage_name,
            f"{stage.elapsed_seconds:.0f}s",
            f"{stage.input_size_gb:.1f}" if stage.input_size_gb > 0 else "-",
            f"{stage.output_size_gb:.1f}" if stage.output_size_gb > 0 else "-",
            f"{stage.throughput_gb_per_second:.3f}" if stage.throughput_gb_per_second > 0 else "-",
            str(stage.executor_count) if stage.executor_count > 0 else "-",
        )

    # Scores
    scores: list[str] = []
    if is_continuous_mode(pb.pipeline_mode):
        if (pb.data_freshness_seconds or 0) > 0:
            scores.append(f"Freshness:       {pb.data_freshness_seconds:>8.1f}s")
        if pb.sustained_throughput_rps > 0:
            from lakebench.metrics.bounds import trickle_note

            scores.append(
                f"Throughput:      {pb.sustained_throughput_rps:>8,.0f} rows/s"
                f"{trickle_note(metrics)}"
            )
        if pb.stage_latency_profile:
            lat = "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
            scores.append(f"Latency (b/s/g): {lat}ms")
        if pb.ingest_ratio is None:
            scores.append("Completeness:    [dim]unmeasured[/dim]")
        elif pb.ingest_ratio > 0:
            pct = pb.ingest_ratio * 100
            scores.append(f"Completeness:    {pct:>7.1f}%")
        # pipeline_saturated is bool | None. None means unmeasurable and must
        # not be silently coerced to "not saturated" -- that would suppress
        # the warning under the same condition it was meant to fire.
        if pb.pipeline_saturated is True:
            scores.append("[yellow]Pipeline saturated (completeness < 95%)[/yellow]")
            if pb.intake_limit == "bronze_capacity":
                scores.append("[dim]Bronze ran back to back: its processing is the limit[/dim]")
        trickle = pb.trickle_summary()
        if trickle:
            scores.append(f"[dim]{trickle}[/dim]")
        if pb.time_to_detect_seconds is not None:
            scores.append(
                f"Time to detect:  {pb.time_to_detect_seconds:>8.1f}s p50, "
                f"{pb.time_to_detect_p95_seconds or 0:.0f}s p95"
            )
    else:
        if pb.time_to_value_seconds > 0:
            scores.append(f"Time to Value:   {pb.time_to_value_seconds:>8.1f}s")
        if pb.pipeline_throughput_gb_per_second > 0:
            scores.append(f"Throughput:      {pb.pipeline_throughput_gb_per_second:>8.3f} GB/s")
        if pb.total_data_processed_gb > 0:
            scores.append(f"Data Processed:  {pb.total_data_processed_gb:>8.1f} GB")
        if pb.compute_efficiency_gb_per_core_hour > 0:
            scores.append(
                f"Efficiency:      {pb.compute_efficiency_gb_per_core_hour:>8.3f} GB/core-hr"
            )
        if pb.scale_ratio > 0:
            pct = pb.scale_ratio * 100
            label = "[green]verified[/green]" if pct >= 95 else "[yellow]incomplete[/yellow]"
            scores.append(f"Scale:           {pct:>7.1f}% {label}")

    if pb.query_benchmark:
        scores.append(f"QpH:             {pb.query_benchmark.qph:>8,.1f}")

    console.print()
    console.print(Panel(header, title="Benchmark Summary", expand=False))
    console.print(table)
    if scores:
        console.print()
        for s in scores:
            console.print(f"  {markup(s)}")
    console.print()


# =============================================================================
# CLI Commands (kept inline -- smaller commands)
# =============================================================================


@app.command()
def version() -> None:
    """Show version information."""
    console.print(f"Lakebench version {esc(__version__)}")


# init lives in cli/_init.py; registered here to keep its place in --help.
from lakebench.cli._init import init as _init_fn  # noqa: E402

app.command(name="init")(_init_fn)


def can_edit_operator_release(operator_namespace: str) -> bool | None:
    """Whether these credentials can run the watch-list ``helm upgrade``.

    Helm keeps the release in Secrets and the upgrade patches the operator
    Deployment, both in the operator namespace. Returns None when the
    access review itself cannot be made.
    """
    try:
        from kubernetes import client as _kc

        api = _kc.AuthorizationV1Api()
        for group, resource, verb in (
            ("", "secrets", "list"),
            ("", "secrets", "create"),
            ("", "secrets", "update"),
            ("apps", "deployments", "patch"),
        ):
            review = _kc.V1SelfSubjectAccessReview(
                spec=_kc.V1SelfSubjectAccessReviewSpec(
                    resource_attributes=_kc.V1ResourceAttributes(
                        namespace=operator_namespace, group=group, resource=resource, verb=verb
                    )
                )
            )
            if not api.create_self_subject_access_review(review).status.allowed:
                return False
        return True
    except Exception:  # noqa: BLE001
        return None


def operator_watch_verdict(
    namespace: str,
    watched: list[str],
    *,
    namespace_exists: bool | None,
    can_edit_release: bool | None = None,
    operator_namespace: str = "spark-operator",
) -> tuple[str, str, str]:
    """Validate's verdict for a namespace missing from ``spark.jobNamespaces``.

    Returns ``(level, message, hint)``. ``deploy`` adds the namespace under
    the ``lakebench-cluster-lock`` lease, and ``run`` re-adds it before submitting jobs, so a missing entry is not
    itself a blocker: before a deploy (namespace absent) it is expected, on
    an existing deployment it is drift worth a warning. It fails only when
    these credentials cannot make that add (``can_edit_release`` False), in
    which case deploy would build the stack and then stop at the operator
    step. The hint never suggests a raw ``helm upgrade``: that bypasses the
    lease.
    """
    from lakebench.modules.pipeline_engines.spark.operator import watch_list_fix_hint

    if can_edit_release is False:
        return (
            "fail",
            f"Cannot add namespace '{namespace}' to the Spark Operator watch list",
            f"Currently watching: {watched}. deploy and run add it by upgrading "
            f"the operator's Helm release in '{operator_namespace}', which "
            "needs list/create/update on secrets and patch on deployments there; these "
            "credentials lack that, so deploy would stop at the spark-operator "
            "step after creating the namespace. Ask a cluster admin for that "
            "access, or to run the deploy.",
        )
    if namespace_exists is False:
        return (
            "ok",
            f"Namespace '{namespace}' not yet watched; deploy adds it",
            f"Currently watching: {watched}. 'lakebench deploy' adds the "
            "namespace under the lakebench-cluster-lock lease.",
        )
    return (
        "warn",
        f"Does not watch namespace '{namespace}'",
        f"Currently watching: {watched}\n"
        "'lakebench deploy' and 'lakebench run' add it under the cluster "
        "lease before submitting jobs.\n" + watch_list_fix_hint(),
    )


@app.command(
    help="Validate configuration and test cluster + S3 connectivity. "
    "Equivalent to `lakebench config validate`."
)
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
    console.print(Panel(f"Validating: [bold]{esc(config_file)}[/bold]", expand=False))

    # validate is read-only: it opens no journal and writes no file.

    # Track validation results
    checks_passed = 0
    checks_failed = 0
    checks_warned = 0

    # --- Helpers for grouped section output ---
    _section_items: list[tuple[str, str, str | None]] = []  # (status, msg, hint)

    def _section_start(title: str) -> None:
        _section_items.clear()
        console.print(f"\n [bold]{esc(title)}[/bold]")

    def _check_ok(msg: str, *, hint: str | None = None) -> None:
        _section_items.append(("ok", msg, hint))

    def _check_fail(msg: str, *, hint: str | None = None) -> None:
        _section_items.append(("fail", msg, hint))

    def _check_warn(msg: str, *, hint: str | None = None) -> None:
        _section_items.append(("warn", msg, hint))

    def _section_end() -> tuple[int, int, int]:
        """Flush section items. Returns (passed, failed, warned)."""
        ok = fail = warn = 0
        for status, msg, hint in _section_items:
            if status == "ok":
                console.print(f"   [green]+[/green] {esc(msg)}")
                ok += 1
            elif status == "fail":
                console.print(f"   [red]x[/red] {esc(msg)}")
                fail += 1
            else:
                console.print(f"   [yellow]![/yellow] {esc(msg)}")
                warn += 1
            if hint:
                for line in hint.split("\n"):
                    console.print(f"     [dim]{esc(line)}[/dim]")
        _section_items.clear()
        return ok, fail, warn

    # 1. Load and validate config
    _section_start("Configuration")
    try:
        # validate answers "will deploy and run accept this config", so it
        # loads as they do (MUTATE): a nameless config or a removed key fails
        # here, not at deploy. Loading writes nothing either way.
        cfg = load_config(config_file, purpose=LoadPurpose.MUTATE)
        _check_ok("Config syntax valid")
        if verbose:
            _check_ok(f"Name: {cfg.name}, Namespace: {cfg.get_namespace()}")
    except ConfigFileNotFoundError as e:
        print_error(f"File not found: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    except ConfigValidationError as e:
        print_error("Config validation failed:")
        for err in e.errors:
            loc = ".".join(str(x) for x in err["loc"])
            console.print(f"  [red]x[/red] {esc(loc)}: {esc(err['msg'])}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    p, f, w = _section_end()
    checks_passed += p
    checks_failed += f
    checks_warned += w

    # 2. CLI tools
    _section_start("CLI Tools")
    for tool in ["kubectl", "helm"]:
        if shutil.which(tool):
            _check_ok(f"{tool} found")
        else:
            _check_fail(f"{tool} not found on PATH")
    p, f, w = _section_end()
    checks_passed += p
    checks_failed += f
    checks_warned += w

    # 3. S3 storage (endpoint + credentials + connectivity -- single group)
    _section_start("S3 Storage")
    s3 = cfg.platform.storage.s3

    if not s3.endpoint:
        _check_fail("Endpoint not configured")
    else:
        _check_ok(f"Endpoint: {s3.endpoint}")

    if not cfg.has_inline_s3_credentials():
        _check_fail("Credentials not configured (set access_key and secret_key)")
    else:
        _check_ok("Credentials configured")

    # Live connectivity
    if s3.endpoint and cfg.has_inline_s3_credentials():
        results = test_s3_connectivity(
            endpoint=s3.endpoint,
            access_key=s3.access_key,
            secret_key=s3.secret_key,
            region=s3.region,
            path_style=s3.path_style,
            ca_cert=s3.ca_cert,
            verify_ssl=s3.verify_ssl,
        )

        if results["endpoint_reachable"]:
            _check_ok("Endpoint reachable")
        else:
            _check_fail(results["endpoint_message"])

        if results["credentials_valid"]:
            _check_ok("Credentials valid")
            if verbose and results["buckets"]:
                _check_ok(f"Buckets: {', '.join(results['buckets'])}")
        elif results["endpoint_reachable"]:
            _check_fail(results["credentials_message"])

    # Bucket name overlap
    bucket_names = [s3.buckets.bronze, s3.buckets.silver, s3.buckets.gold]
    if len(set(bucket_names)) < 3:
        _check_warn("Bronze, silver, and gold use overlapping bucket names")

    p, f, w = _section_end()
    checks_passed += p
    checks_failed += f
    checks_warned += w

    # 4. Kubernetes cluster (connectivity + namespace + platform security)
    _section_start("Kubernetes")
    try:
        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )

        connected, msg = k8s.test_connectivity()
        if connected:
            _check_ok(msg)
        else:
            _check_fail(msg)

        ctx = k8s.get_current_context()
        if ctx and verbose:
            _check_ok(f"Context: {ctx.name} / Cluster: {ctx.cluster}")

        ns = cfg.get_namespace()
        if k8s.namespace_exists(ns):
            _check_ok(f"Namespace '{ns}' exists")
        else:
            can_create, msg = k8s.can_create_namespace(ns)
            if can_create:
                _check_ok(f"Namespace '{ns}' can be created")
            else:
                _check_fail(f"Cannot create namespace '{ns}': {msg}")

        # Platform security
        verifier = SecurityVerifier(k8s)
        platform = verifier.detect_platform()
        platform_version = verifier.get_platform_version()

        platform_info = f"{platform.value}"
        if platform_version:
            platform_info += f" {platform_version}"
        _check_ok(f"Platform: {platform_info}")

        security_result = verifier.verify_security(ns)

        if platform == PlatformType.OPENSHIFT:
            for scc in security_result.scc_status:
                if scc.assigned:
                    _check_ok(f"SCC '{scc.name}' assigned to '{scc.service_account}'")
                else:
                    fix_hint = (
                        f"Fix: oc adm policy add-scc-to-user {scc.name} "
                        f"-z {scc.service_account} -n {ns}"
                    )
                    _check_warn(
                        f"SCC '{scc.name}' not assigned to '{scc.service_account}'",
                        hint="Auto-configured during deploy" if not verbose else fix_hint,
                    )

        if security_result.passed:
            checks_passed += security_result.checks_passed
        else:
            if platform == PlatformType.OPENSHIFT:
                _check_warn(
                    f"Security: {security_result.checks_failed} item(s) need attention",
                    hint="SCC will be configured automatically during deploy",
                )
            checks_passed += security_result.checks_passed

        if verbose and security_result.recommendations:
            for rec in security_result.recommendations:
                _check_ok(f"Tip: {rec}")

    except K8sConnectionError as e:
        _check_fail(f"Connection failed: {e}")

    p, f, w = _section_end()
    checks_passed += p
    checks_failed += f
    checks_warned += w

    # 5. Storage classes
    _section_start("Storage Classes")
    try:
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        storage_v1 = k8s_client.StorageV1Api()

        storage_classes = []
        scratch_cfg = cfg.platform.storage.scratch
        if scratch_cfg.enabled and scratch_cfg.storage_class:
            # StorageClass is Category 2 shared infrastructure: preflight
            # requires it to exist before deploy runs.
            storage_classes.append((scratch_cfg.storage_class, "scratch", True))

        pg_sc = cfg.platform.compute.postgres.storage_class
        if pg_sc:
            storage_classes.append((pg_sc, "postgres", True))

        if not storage_classes:
            _check_ok("Using cluster defaults (no custom storage classes)")
        else:
            for sc_name, purpose, required in storage_classes:
                try:
                    storage_v1.read_storage_class(sc_name)
                    _check_ok(f"{sc_name} ({purpose})")
                except ApiException as e:
                    if e.status == 404:
                        if required:
                            _check_fail(f"{sc_name} not found ({purpose})")
                        else:
                            _check_warn(
                                f"{sc_name} will be created during deploy ({purpose})",
                            )
                    else:
                        _check_warn(f"Could not check {sc_name}: {e.reason}")
    except Exception as e:
        _check_warn(f"Could not validate storage classes: {e}")
    p, f, w = _section_end()
    checks_passed += p
    checks_failed += f
    checks_warned += w

    # 6. Catalog prerequisites
    _section_start("Catalog")
    if cfg.architecture.catalog.type.value == "hive":
        try:
            from kubernetes import client as k8s_client

            api_ext = k8s_client.ApiextensionsV1Api()
            crds = api_ext.list_custom_resource_definition()
            crd_names = {crd.metadata.name for crd in crds.items}
            required_crds = {
                "hiveclusters.hive.stackable.tech": "hive-operator",
                "secretclasses.secrets.stackable.tech": "secret-operator",
            }
            missing = [op for crd, op in required_crds.items() if crd not in crd_names]
            if missing:
                _check_fail(
                    f"Missing Stackable operators: {', '.join(missing)}",
                    hint="A cluster admin installs them once:\n"
                    "  lakebench admin install --component stackable <config>",
                )
            else:
                _check_ok("Stackable operators installed (Hive)")
        except Exception:
            _check_warn("Could not verify Stackable operators (K8s not reachable)")
    elif cfg.architecture.catalog.type.value == "polaris":
        _check_ok("Polaris catalog (no external operators needed)")
    else:
        _check_ok(f"Catalog type: {cfg.architecture.catalog.type.value}")
    p, f, w = _section_end()
    checks_passed += p
    checks_failed += f
    checks_warned += w

    # 7. Spark Operator
    _section_start("Spark Operator")
    try:
        from lakebench.spark import SparkOperatorManager

        spark_op_cfg = cfg.platform.compute.spark.operator
        operator = SparkOperatorManager(
            namespace=spark_op_cfg.namespace,
            job_namespace=cfg.get_namespace(),
            kube_context=cfg.platform.kubernetes.context,
        )
        status = operator.check_status()

        if status.ready:
            version_info = f" v{status.version}" if status.version else ""
            _check_ok(f"Ready in '{status.namespace}'{version_info}")

            # Check namespace watching. Never a failure: deploy and run
            # both add the namespace under the cluster lease.
            if status.watching_namespace is False:
                ns_now = cfg.get_namespace()
                try:
                    from lakebench.k8s import get_k8s_client as _gkc

                    ns_exists: bool | None = _gkc(
                        context=cfg.platform.kubernetes.context, namespace=ns_now
                    ).namespace_exists(ns_now)
                except Exception:
                    ns_exists = None
                op_ns = status.namespace or spark_op_cfg.namespace
                level, msg, hint = operator_watch_verdict(
                    ns_now,
                    status.watched_namespaces or [],
                    namespace_exists=ns_exists,
                    can_edit_release=can_edit_operator_release(op_ns),
                    operator_namespace=op_ns,
                )
                if level == "ok":
                    _check_ok(msg, hint=hint)
                elif level == "fail":
                    _check_fail(msg, hint=hint)
                else:
                    _check_warn(msg, hint=hint)
            elif status.watching_namespace is None:
                # deploy and run stop on an unreadable watch list
                _check_warn(
                    "Namespace watching unverified (watch list unreadable)",
                    hint="deploy and run refuse until the operator's watch list can be read",
                )
        elif status.installed is None:
            _check_warn(f"Could not check the Spark Operator: {status.message}")
        elif status.installed:
            _check_warn(f"Installed but not ready: {status.message}")
        else:
            _check_fail(
                "Not installed",
                hint="A cluster admin installs it once (takes the cluster lock):\n"
                "  lakebench admin install --component spark-operator <config>",
            )
    except Exception as e:
        _check_warn(f"Could not check status: {e}")
    p, f, w = _section_end()
    checks_passed += p
    checks_failed += f
    checks_warned += w

    # 8. Scale. Executor counts and per-executor sizing come from the job
    # profiles at manifest build, so there is no executor setting to grade;
    # the tier's executor advice is not repeated (it named removed settings).
    _section_start("Scale")
    try:
        from lakebench.config.scale import compute_guidance as _cg

        scale = cfg.architecture.workload.datagen.get_effective_scale()
        dims = cfg.get_scale_dimensions()
        guidance = _cg(scale)

        _check_ok(f"Scale {scale}: {dims.customers:,} customers, {dims.approx_rows:,} rows")
        if guidance.tier_name == "extreme":
            _check_warn(f"Scale {scale} is the extreme tier: it needs a large cluster")

    except Exception as e:
        _check_warn(f"Could not read the scale: {e}")
    p, f, w = _section_end()
    checks_passed += p
    checks_failed += f
    checks_warned += w

    # Summary
    console.print()
    if checks_failed == 0:
        if checks_warned > 0:
            warn_s = "s" if checks_warned > 1 else ""
            console.print(
                Panel(
                    f"[green]{esc(checks_passed)} passed[/green], "
                    f"[yellow]{esc(checks_warned)} warning{esc(warn_s)}[/yellow]\n"
                    f"Run [bold]lakebench deploy[/bold] to deploy",
                    title="Validation Passed",
                    expand=False,
                )
            )
        else:
            console.print(
                Panel(
                    f"[green]All {esc(checks_passed)} checks passed[/green]\n"
                    f"Run [bold]lakebench deploy[/bold] to deploy",
                    title="Validation Successful",
                    expand=False,
                )
            )
    else:
        console.print(
            Panel(
                f"[green]{esc(checks_passed)} passed[/green], [red]{esc(checks_failed)} failed[/red]\n"
                f"Fix the issues above before deploying",
                title="Validation Failed",
                expand=False,
            )
        )
        raise typer.Exit(ExitCode.FAILED)


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
    local: Annotated[
        bool,
        typer.Option(
            "--local",
            help="Show local mode status instead of Kubernetes",
        ),
    ] = False,
    workdir: Annotated[
        Path | None,
        typer.Option(
            "--workdir",
            help="Host directory for local mode state (default: ~/.lakebench/local/<name>)",
        ),
    ] = None,
    name: Annotated[
        str | None,
        typer.Option("--name", help=NAME_OPTION_HELP),
    ] = None,
    as_json: Annotated[bool, json_option()] = False,
) -> None:
    """Show deployment status.

    Shows the current status of Lakebench components in the cluster.
    """
    # Determine namespace
    cfg = None
    ns = namespace
    if not ns or local:
        config_file = resolve_config_path(config_file, file_option)
    if config_file:
        try:
            cfg = load_config(config_file, purpose=LoadPurpose.READ, name_override=name)
            ns = ns or cfg.get_namespace()
        except ConfigError as e:
            print_error(f"Config error: {e}")
            raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    if cfg is not None and config_file and not local:
        # A nameless config reads only a deployment it can prove is its own.
        guard_nameless(cfg, config_file, allow_absent=True)

    if local:
        from lakebench.cli._local import print_local_status, status_local

        if as_json:
            print_error("--json covers cluster status; it does not combine with --local")
            raise typer.Exit(ExitCode.USAGE)
        if cfg is None:
            print_error("Local status needs a config file")
            raise typer.Exit(ExitCode.USAGE)
        print_local_status(status_local(cfg, workdir=workdir))
        return

    if not ns:
        print_error("Specify --namespace or provide a config file")
        raise typer.Exit(ExitCode.USAGE)

    console.print(Panel(f"Status for namespace: [bold]{esc(ns)}[/bold]", expand=False))

    from kubernetes import client as k8s_client

    from lakebench.cli import _cluster_ops as ops
    from lakebench.exit_codes import LakebenchError

    def _unreachable(detail: object) -> typer.Exit:
        print_error(f"Kubernetes connection failed: {detail}")
        return typer.Exit(ExitCode.PREREQUISITE)

    def _unreadable(detail: object) -> typer.Exit:
        print_error(f"Kubernetes API error: {detail}")
        return typer.Exit(ExitCode.PREREQUISITE)

    try:
        # The config's context, or (status --namespace with no config) the
        # kubeconfig's current context resolved by name and printed.
        # get_k8s_client pins the process; the API objects below follow it.
        if cfg is not None:
            get_k8s_client(context=cfg.platform.kubernetes.context, namespace=ns)
        else:
            from lakebench.k8s.target import ClusterTarget

            target = ClusterTarget.current()
            print_info(f"Cluster context: {escape(target.label)}")
            get_k8s_client(target=target, namespace=ns)
    except (K8sConnectionError, ConfigException) as e:
        raise _unreachable(e)  # noqa: B904

    deploy_hint = f"lakebench deploy {config_file}" if config_file else "lakebench deploy CONFIG"
    try:
        exists = ops.namespace_exists(k8s_client.CoreV1Api(), ns)
    except ops.ClusterReadError as e:
        raise _unreadable(e)  # noqa: B904
    if not exists:
        raise LakebenchError(
            f"namespace {ns} does not exist",
            next=deploy_hint,
            path="status.namespace_missing",
            code=ExitCode.FAILED,
        )

    apps_v1 = k8s_client.AppsV1Api()

    table = Table(title="Components")
    table.add_column("Component", style="cyan")
    table.add_column("Type", style="dim")
    table.add_column("Status", style="bold")
    table.add_column("Ready", justify="center")

    # Build component list -- config-aware when a config file was loaded
    if cfg is not None:
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
        # Observability is a shared stack in its own namespace
        # (deploy/observability.py), not a component of this deployment.
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
            ("prometheus-lakebench-observability-ku-prometheus", "StatefulSet"),
            ("lakebench-observability-grafana", "Deployment"),
        ]

    marks = {
        "ok": "[green]OK[/green]",
        "unready": "[yellow]--[/yellow]",
        "missing": "[dim]-[/dim]",
        "error": "[red]ERROR[/red]",
    }
    rows = []
    try:
        for obj_name, kind in components:
            row = ops.read_component(apps_v1, ns, obj_name, kind)
            rows.append(row)
            table.add_row(obj_name, kind, esc(row.detail), marks[row.state])
    except ops.ClusterReadError as e:
        raise _unreachable(e)  # noqa: B904

    console.print(table)

    # Datagen job progress, while it runs (informational, never drift)
    datagen: dict[str, int] | None = None
    try:
        batch_v1 = k8s_client.BatchV1Api()
        job = batch_v1.read_namespaced_job(
            "lakebench-datagen", ns, _request_timeout=ops.API_TIMEOUT
        )
        active = job.status.active or 0
        succeeded = job.status.succeeded or 0
        completions = job.spec.completions or 1
        if active > 0 or succeeded < completions:
            datagen = {"succeeded": succeeded, "completions": completions, "active": active}
            console.print()
            console.print(
                f"[bold]Datagen:[/bold] {esc(succeeded)}/{esc(completions)} pods completed, "
                f"{esc(active)} active"
            )
    except k8s_client.rest.ApiException as e:
        if e.status != 404:
            logger.debug("Could not check datagen job: %s", e)
    except Exception as e:  # noqa: BLE001 -- informational line only
        logger.debug("Could not check datagen job: %s", e)

    verdict, names = ops.status_exit(rows, config_known=cfg is not None)
    from lakebench.cli import _json

    _json.set_data(
        {
            "namespace": ns,
            "verdict": verdict,
            "components": [
                {"name": r.name, "kind": r.kind, "state": r.state, "detail": r.detail} for r in rows
            ],
            "datagen": datagen,
        }
    )
    if verdict == "drift":
        print_error(f"Drift: {', '.join(names)} not ready or not found")
        log_names = [ops.STATUS_LOG_COMPONENT[n] for n in names if n in ops.STATUS_LOG_COMPONENT]
        cfg_arg = str(config_file) if config_file else "CONFIG"
        name_arg = f" --name {name}" if name else ""
        if log_names:
            print_info(f"Next: lakebench logs {cfg_arg} {log_names[0]}{name_arg}, or {deploy_hint}")
        else:
            print_info(f"Next: {deploy_hint}")
        raise typer.Exit(ExitCode.FAILED)
    if verdict == "unverified":
        raise _unreadable(f"could not read {', '.join(names)}")
    print_success(
        "Every listed component is ready" if cfg is not None else "Every component found is ready"
    )


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
    name: Annotated[
        str | None,
        typer.Option("--name", help=NAME_OPTION_HELP),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="List what would be stopped without deleting anything",
        ),
    ] = False,
) -> None:
    """Stop every job Lakebench started in the deployment.

    Deletes every SparkApplication named lakebench-* in the namespace that
    has not finished (the continuous streams and any batch stage left
    running) and the datagen Job while it runs. Finished ones stay, with
    their logs. Exits 1 when anything could not be deleted.
    """
    from kubernetes import client as k8s_client

    from lakebench.cli import _cluster_ops as ops

    config_file = resolve_config_path(config_file, file_option)
    try:
        cfg = load_config(
            config_file, purpose=LoadPurpose.TEARDOWN, name_override=name
        )  # no name-length check; stops only
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904

    # A nameless config stops only a deployment it can prove is its own.
    guard_nameless(cfg, config_file, allow_absent=False)
    namespace = cfg.get_namespace()
    try:
        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=namespace,
        )
    except (K8sConnectionError, ConfigException) as e:
        print_error(f"Kubernetes connection failed: {e}")
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904

    verb = "Would stop" if dry_run else "Stopping"
    console.print(Panel(f"{esc(verb)} jobs for: [bold]{esc(cfg.name)}[/bold]", expand=False))

    try:
        exists = ops.namespace_exists(k8s_client.CoreV1Api(), namespace)
    except ops.ClusterReadError as e:
        print_error(f"Kubernetes API error: {e}")
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904
    if not exists:
        print_info(f"Namespace {namespace} does not exist; nothing to stop")
        return

    custom = k8s_client.CustomObjectsApi()
    batch = k8s_client.BatchV1Api()
    out = ops.StopOutcome()
    try:
        ops.stop_targets(custom, batch, namespace, out)
    except ops.ClusterReadError as e:
        print_error(f"Kubernetes connection failed: {e}")
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904

    for ref in out.finished:
        print_info(f"Already finished, left in place: {ref}")
    if dry_run:
        for ref in out.found:
            print_info(f"Would delete {ref}")
        if not out.found:
            print_info("Nothing is running")
        for failure in out.failures:
            print_error(failure)
        raise typer.Exit(ExitCode.FAILED if out.failures else ExitCode.OK)

    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.STOP, {})

    if out.found:
        try:
            ops.pre_stop(cfg, k8s)
        except Exception as e:  # noqa: BLE001 -- a failed drain must not block the stop
            print_warning(f"Pre-stop step failed, stopping anyway: {e}")
        ops.stop_delete(custom, batch, namespace, out)

    for ref in out.deleted:
        print_success(f"Stopped: {ref}")
    for ref in out.not_running:
        print_info(f"{ref}: not running")
    for failure in out.failures:
        print_error(failure)
    if out.deleted:
        print_success(f"Stopped {len(out.deleted)} job(s)")
    elif not out.failures:
        print_info("Nothing was running")

    _journal_safe(
        j.record,
        EventType.STREAMING_STOP,
        message=f"Stopped {len(out.deleted)} jobs",
        details={
            "stopped": len(out.deleted),
            "deleted": out.deleted,
            "finished": out.finished,
            "failures": out.failures,
        },
    )
    _journal_safe(j.end_command, success=not out.failures)
    if out.failures:
        raise typer.Exit(ExitCode.FAILED)


# The customer360 generator's own timestamp defaults (datagen_rs generate.rs).
_C360_TIMESTAMP_DEFAULTS = ("2024-01-01", "2025-01-01")


def info_datagen_mode(cfg: LakebenchConfig) -> str:
    """The datagen mode line ``info`` shows: the delivery mode the run uses,
    where it came from, and, for a continuous pipeline, how the corpus
    reaches bronze.

    DatagenMode is a delivery pattern since Wave 2 D1 (2026-09-28): batch
    buffers whole files and PUTs, continuous streams row-groups through S3
    multipart. Same corpus, different write pipeline. Auto resolves to
    continuous at every scale (Wave 2 D-wave adv-review fix).
    """
    from lakebench.config.autosizer import _resolve_datagen_mode
    from lakebench.config.schema import DatagenMode

    datagen = cfg.architecture.workload.datagen
    mode = _resolve_datagen_mode(cfg)
    source = "auto" if datagen.mode == DatagenMode.AUTO else "set in config"
    line = f"{mode} delivery ({source})"
    if is_continuous_mode(cfg.architecture.pipeline.mode):
        line += "; corpus written up front, trickled to bronze by the pipeline"
    return line


def info_date_range(cfg: LakebenchConfig, scale_days: int) -> str:
    """The event date range the generator will write, as ``info`` shows it.

    For customer360 the generator spans datagen.timestamp_start to
    timestamp_end (defaults 2024-01-01 to 2025-01-01), whatever the scale
    table says; showing the scale's 365 days for a 14-day config misled
    (lb16-checks2). Other schemas, and a range that does not parse, show
    the scale dimensions' figure.
    """
    from datetime import date

    datagen = cfg.architecture.workload.datagen
    if cfg.architecture.workload.schema_type.value != "customer360" or not (
        datagen.timestamp_start or datagen.timestamp_end
    ):
        return f"{scale_days} days"
    start = datagen.timestamp_start or _C360_TIMESTAMP_DEFAULTS[0]
    end = datagen.timestamp_end or _C360_TIMESTAMP_DEFAULTS[1]
    try:
        days = (date.fromisoformat(end) - date.fromisoformat(start)).days
    except ValueError:
        return f"{scale_days} days"
    return f"{days} days ({start} to {end})"


@app.command(hidden=True, deprecated=True)
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
        cfg = load_config(config_file, purpose=LoadPurpose.INSPECT)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904

    # Auto-size resources based on scale (tier guidance only)
    from lakebench.config.autosizer import resolve_auto_sizing

    resolve_auto_sizing(cfg)

    from lakebench.config.scale import compute_guidance as _compute_guidance
    from lakebench.modules.pipeline_engines.spark.job import (
        _JOB_PROFILES,
        _resolve_job_profile,
        _scale_executor_count,
    )

    s3 = cfg.platform.storage.s3
    arch = cfg.architecture
    workload = arch.workload
    scale = workload.datagen.get_effective_scale()
    dims = cfg.get_scale_dimensions()
    guidance = _compute_guidance(scale)

    # Workload profile: {schema}-{pipeline mode}. The datagen mode (batch or
    # continuous generator) is its own line: a continuous pipeline at small
    # scale runs the batch generator, and naming the workload after it read
    # as "customer360-batch" for a continuous config (lb16-checks2).
    workload_profile = f"{workload.schema_type.value}-{arch.pipeline.mode.value}"

    is_sustained = is_continuous_mode(arch.pipeline.mode)

    # Schema-resolved profiles: what the manifests deploy (AML overrides
    # bronze-verify and bronze-ingest), so info matches the capacity check.
    _profiles = {
        j: _resolve_job_profile(j, workload.schema_type.value) or _JOB_PROFILES[j]
        for j in _JOB_PROFILES
    }
    # Per-job executor counts with auto/override labels
    from lakebench.modules.pipeline_engines.spark.job import executor_override

    override_map = {
        j: executor_override(j, cfg) for j in ("bronze-verify", "silver-build", "gold-finalize")
    }
    executor_parts = []
    for job_name, override_val in override_map.items():
        auto_count = _scale_executor_count(_profiles[job_name], scale)
        if override_val is not None:
            executor_parts.append(f"{job_name}={override_val} (override)")
        else:
            executor_parts.append(f"{job_name}={auto_count} (auto)")

    # Streaming executor counts
    streaming_override_map = {
        j: executor_override(j, cfg) for j in ("bronze-ingest", "silver-stream", "gold-refresh")
    }
    streaming_executor_parts = []
    for job_name, override_val in streaming_override_map.items():
        auto_count = _scale_executor_count(_profiles[job_name], scale)
        if override_val is not None:
            streaming_executor_parts.append(f"{job_name}={override_val} (override)")
        else:
            # info does not read the cluster, so this is the profile count;
            # the run caps it to the concurrent budget and warns when it does.
            streaming_executor_parts.append(
                f"{job_name}={auto_count} (auto, before cluster budget)"
            )

    # Build info lines
    lines = [
        ("Deployment", cfg.name),
        ("Namespace", cfg.get_namespace()),
        ("Workload", workload_profile),
        ("Schema", workload.schema_type.value),
        ("Scale", f"{scale}"),
        ("Customers", f"{dims.customers:,}"),
        ("Events/customer", f"~{dims.events_per_customer}"),
        ("Date range", info_date_range(cfg, dims.date_range_days)),
        ("Approx rows", f"{dims.approx_rows:,}"),
        ("Pipeline mode", f"{arch.pipeline.mode.value}"),
        ("Datagen mode", info_datagen_mode(cfg)),
        ("Processing", f"{arch.pipeline.pattern.value} (bronze > silver > gold)"),
        ("Catalog", f"{arch.catalog.type.value}"),
        (
            "Table format",
            f"{arch.table_format.type.value} {arch.table_format.delta.version if arch.table_format.type.value == 'delta' else arch.table_format.iceberg.version}",
        ),
        ("Query engine", f"{arch.query_engine.type.value}"),
        ("Parallelism", str(workload.datagen.parallelism)),
        # compute_guidance() is advisory; its executor/memory "rec" disagreed
        # with what the jobs request, so only the tier name is shown.
        ("Compute tier", guidance.tier_name),
    ]

    if is_sustained:
        sustained_cfg = arch.pipeline.sustained
        lines += [
            ("Executors", ", ".join(streaming_executor_parts)),
            (
                "Trigger intervals",
                f"bronze={sustained_cfg.bronze_trigger_interval}, silver={sustained_cfg.silver_trigger_interval}, gold={sustained_cfg.gold_refresh_interval}",
            ),
            ("Run duration", f"{sustained_cfg.run_duration}s"),
        ]
    else:
        lines += [
            ("Executors", ", ".join(executor_parts)),
        ]

    # Peak requested resources from the one sizing source: the same
    # plan_requirements() that config show, recommend and run's capacity
    # preflight use. Offline, so batch datagen is before cluster scaling.
    from lakebench.config.sizing import breakdown_text, floor_text, plan_requirements

    plan = plan_requirements(cfg)
    lines += [
        ("Peak requested", floor_text(plan)),
        ("  of which", breakdown_text(plan)),
    ]

    lines += [
        ("S3 endpoint", s3.endpoint or "(not set)"),
        ("Buckets", f"{s3.buckets.bronze}, {s3.buckets.silver}, {s3.buckets.gold}"),
    ]

    # Add monitoring info if enabled
    obs = cfg.observability
    if obs.enabled:
        prom_status = "enabled"
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

    console.print(Panel(formatted, title=f"Lakebench: {esc(cfg.name)}", expand=False))

    if guidance.warning:
        console.print(f"  [yellow]Warning: {esc(guidance.warning)}[/yellow]")

    # Check cluster feasibility, on the cluster the config names, as run does
    try:
        k8s = get_k8s_client(context=cfg.platform.kubernetes.context)
        cap = k8s.get_cluster_capacity()
        if cap is None:
            raise ValueError("Could not detect cluster capacity")
        cluster_cores = cap.total_cpu_millicores // 1000
        cluster_gb = cap.total_memory_bytes // (1024**3)
        # The run preflight's decision for run --generate: datagen and
        # Trino sized against this cluster, as run sizes them. A plain batch
        # run creates no datagen pod and is checked without one.
        from lakebench.config.sizing import check_capacity

        verdict = check_capacity(cfg, cap)
        fitted = verdict.plan.floor
        request = (
            f"{cluster_cores} cores / {cluster_gb} GB allocatable; "
            f"peak request {fitted.cpu_cores} cores / {fitted.memory_gb} GB on this cluster "
            "(with datagen)"
        )
        if verdict.status == "fits":
            console.print(f"  [green]Cluster OK:[/green] {esc(request)}")
        elif verdict.status == "degraded":
            capped = verdict.capped_request or fitted
            console.print(
                f"  [yellow]Cluster below the full request:[/yellow] {esc(request)}; runs degraded "
                f"at ~{esc(capped.cpu_cores)} cores / {esc(capped.memory_gb)} GB with capped streams"
            )
        else:
            console.print(f"  [red]Cluster undersized:[/red] {esc(request)}")
            console.print(
                "  [dim]Run 'lakebench config recommend' to find max feasible scale[/dim]"
            )
        for note in (*verdict.plan.cuts, *verdict.warnings):
            console.print(f"  [yellow]{esc(note)}[/yellow]")
    except Exception as e:
        logger.debug("Could not check cluster feasibility: %s", e)


_REPORT_FORMATS = ("table", "json", "csv")


def _report_target(
    target: str | None, run_option: str | None, *, use_default: bool
) -> tuple[Path | None, str | None]:
    """``report``'s positional as ``(config, run id)``.

    An existing file, or a name ending in .yaml or .yml, is a config; any
    other value is a run id (a leading ``run-`` is dropped); a run directory
    stands for its run id. With no
    positional and no ``--run``, ``./lakebench.yaml`` is the config when it
    exists and *use_default* is set (not for ``--list``, which lists every
    deployment)."""
    if target is None:
        default = Path(DEFAULT_CONFIG)
        if run_option is None and use_default and default.is_file():
            return default, None
        return None, None
    path = Path(target)
    if path.is_dir() and (path / "metrics.json").is_file():
        target = path.resolve().name  # a run directory: its id
    elif path.is_file() or path.suffix in (".yaml", ".yml"):
        if not path.is_file():
            print_error(f"Config file not found: {target}")
            raise typer.Exit(ExitCode.USAGE)
        return path, None
    run = target.removeprefix("run-")
    if run_option is not None and run_option != run:
        print_error(f"Two runs given: {run} and --run {run_option}; pass one")
        raise typer.Exit(ExitCode.USAGE)
    return None, run


def _print_stage_matrix(metrics, output_format: str) -> None:
    """The stage matrix of *metrics*: a table, the pipeline benchmark block
    as JSON, or the matrix as CSV. Exits 1 when the run has no pipeline
    benchmark."""
    import json as _json

    _note_benchmark_record(metrics)
    pb = metrics.pipeline_benchmark
    if pb is None:
        print_warning("This run does not have pipeline benchmark data.")
        print_info("Pipeline benchmark is generated for runs after this feature was added.")
        raise typer.Exit(ExitCode.FAILED)

    # Machine-readable formats go to plain stdout (emit_data): Rich wraps
    # long lines at the terminal width and parses markup, which breaks parsers.
    if output_format == "json":
        emit_data(_json.dumps(pb.to_dict(), indent=2))
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
        emit_data(buf.getvalue())
        return

    # Table format (default)
    console.print()
    console.print(
        Panel(
            f"[bold]Pipeline Benchmark:[/bold] {esc(pb.deployment_name)} (run {esc(pb.run_id)})\n"
            f"Mode: {esc(pb.pipeline_mode)} | "
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
            f"{stage.output_rows:,}" if stage.output_rows else "-",
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
            f"\n  Query Benchmark: {esc(qb.mode)} mode | QpH: {qb.qph:.1f} | {qb.total_seconds:.1f}s"
        )

    console.print(
        f"\n  Pipeline: {pb.total_elapsed_seconds:.1f}s total"
        f" | {pb.time_to_value_seconds:.1f}s time-to-value"
        f" | {pb.pipeline_throughput_gb_per_second:.3f} GB/s"
    )
    console.print()


def _report_list_row(r: dict) -> dict:
    """A ``report --list`` row for ``--json`` (cli/_json.ReportListRow)."""
    from lakebench.metrics.verdict import verdict_status

    return {
        "run_id": r.get("run_id"),
        "record_kind": r.get("record_kind") or "run",
        "parent_run_id": r.get("parent_run_id"),
        "deployment_name": r.get("deployment_name"),
        "start_time": r.get("start_time"),
        "verdict": verdict_status(r) or r.get("verdict_status"),
        "total_elapsed_seconds": r.get("total_elapsed_seconds"),
    }


def _report_run_data(
    metrics, runs_dir: Path, delivered: Path | None, requested: str | None = None
) -> dict:
    """The run ``report`` shows, for ``--json`` (cli/_json.ReportRun), from
    the record as stored (the per-run file, else the legacy flat one, as
    ``load_run`` reads them): its verdict and scores are never recomputed;
    a record that cannot be read as stored gives null for both."""
    import json as _json_mod

    record: dict = {}
    rid = requested or metrics.run_id  # the id load_run was given, when one was
    for path in (runs_dir / f"run-{rid}" / "metrics.json", runs_dir / f"run-{rid}.json"):
        try:
            record = _json_mod.loads(path.read_text())
            break
        except (OSError, ValueError):
            continue
    pbd = record.get("pipeline_benchmark") or {}
    return {
        "run_id": metrics.run_id,
        "record_kind": record.get("record_kind") or metrics.record_kind,
        "parent_run_id": record.get("parent_run_id", metrics.parent_run_id),
        "deployment_name": record.get("deployment_name", metrics.deployment_name),
        "start_time": record.get("start_time"),
        "verdict": (record.get("verdict") or {}).get("status"),
        "pipeline_mode": pbd.get("pipeline_mode"),
        "scores": pbd.get("scores") if record else None,
        "stages": [
            {
                "stage_name": st.get("stage_name"),
                "stage_type": st.get("stage_type"),
                "elapsed_seconds": st.get("elapsed_seconds"),
                "input_size_gb": st.get("input_size_gb"),
                "output_size_gb": st.get("output_size_gb"),
                "throughput_gb_per_second": st.get("throughput_gb_per_second"),
                "executor_count": st.get("executor_count"),
            }
            for st in pbd.get("stages") or []
        ],
        "delivered_report": str(delivered) if delivered is not None else None,
    }


@app.command()
def report(
    target: Annotated[
        str | None,
        typer.Argument(
            metavar="[RUN|CONFIG]",
            help=(
                "A run id, or a configuration YAML file: its deployment's latest "
                "run record, so a parallel deployment's newer run is not reported "
                "by mistake. Default: ./lakebench.yaml when it exists, otherwise "
                "the latest run of any deployment."
            ),
            show_default=False,
        ),
    ] = None,
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
            help="List available runs instead of reporting on one",
        ),
    ] = False,
    render: Annotated[
        bool,
        typer.Option(
            "--render",
            help=(
                "Regenerate HTML. Writes a fresh timestamped file at "
                "lakebench-output/reports/report-<run_id>-<ts>.html without "
                "touching the delivered run-<id>/report.html."
            ),
        ),
    ] = False,
    output_path: Annotated[
        Path | None,
        typer.Option(
            "--output",
            help=(
                "Explicit output path for --render. Refuses to overwrite an "
                "existing file at this path unless --force is also given."
            ),
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help=(
                "Allow --render to overwrite an existing file at --output. "
                "Requires both --render and --output."
            ),
        ),
    ] = False,
    summary: Annotated[
        bool,
        typer.Option(
            "--summary",
            "-s",
            help="Also print the key scores when rendering (default action already prints them).",
        ),
    ] = False,
    output_format: Annotated[
        str | None,
        typer.Option(
            "--format",
            "-o",
            help=(
                "Print the run's stage matrix instead of the summary: table, json "
                "or csv (json is the pipeline benchmark block)"
            ),
        ),
    ] = None,
    as_json: Annotated[bool, json_option()] = False,
) -> None:
    """Report on a saved benchmark run.

    Default behaviour is to print the summary of the requested run (or the
    latest one) and point at the delivered ``run-<id>/report.html`` without
    modifying it. Pass ``--render`` to regenerate a fresh HTML file; the
    output goes to ``lakebench-output/reports/report-<run_id>-<ts>.html`` so
    the delivered artifact is never rewritten silently. Pass ``--list`` to
    show all saved runs, and ``--format`` for the stage matrix (what
    ``results`` printed).
    """
    from lakebench.cli import _json
    from lakebench.metrics import MetricsStorage
    from lakebench.reports import ReportGenerator

    storage = MetricsStorage(metrics_dir)

    # Scope the "latest run" lookup to a specific deployment when a config
    # file is given. Under parallel deployments the shared runs/ tree can
    # have another deployment's newer record on top; without scoping,
    # `report` would display it (SP-2 owns the durable deployment_id fix).
    if output_format is not None and output_format not in _REPORT_FORMATS:
        print_error(f"--format must be one of {', '.join(_REPORT_FORMATS)}, not {output_format}")
        raise typer.Exit(ExitCode.USAGE)
    if output_format is not None and as_json:
        print_error("--json and --format are two outputs; pass one")
        raise typer.Exit(ExitCode.USAGE)
    if output_format is not None and (render or list_runs):
        print_error("--format prints a stage matrix; it does not combine with --render or --list")
        raise typer.Exit(ExitCode.USAGE)
    config_file, target_run = _report_target(target, run_id, use_default=not list_runs)
    run_id = target_run or run_id
    deployment_name: str | None = None
    if config_file is not None:
        try:
            deployment_name = load_config(config_file, purpose=LoadPurpose.READ).name
        except ConfigError as e:
            print_error(f"Config error: {e}")
            raise typer.Exit(ExitCode.USAGE)  # noqa: B904
        if target is None and run_id is None:
            print_info(
                f"Showing the latest record of deployment {deployment_name} "
                f"(./{DEFAULT_CONFIG}); pass a run id or another config to choose"
            )

    # List runs mode
    if list_runs:
        runs = storage.list_runs()
        _json.set_data({"runs": [_report_list_row(r) for r in runs]})
        if not runs:
            print_warning(f"No runs found in {metrics_dir}")
            return

        console.print(Panel(f"Available runs in [bold]{esc(metrics_dir)}[/bold]", expand=False))

        table = Table()
        table.add_column("Run ID", style="cyan")
        table.add_column("Kind")
        table.add_column("Deployment")
        table.add_column("Date")
        table.add_column("Status")
        table.add_column("Duration")

        from lakebench.metrics.verdict import passed as _record_passed
        from lakebench.metrics.verdict import verdict_status as _verdict_status

        for r in runs:
            # Prefer the persisted verdict (OD-6: v1.6 records) and fall
            # back to raw ``success`` for legacy v1.5 records.
            if _record_passed(r):
                status = "[green]Passed[/green]"
            elif (_verdict_status(r) or r.get("verdict_status")) == "INTERRUPTED":
                status = "[yellow]Interrupted[/yellow]"
            else:
                status = "[red]Failed[/red]"
            elapsed = f"{r.get('total_elapsed_seconds', 0):.1f}s"
            date = r.get("start_time", "")[:10] if r.get("start_time") else ""
            kind = r.get("record_kind") or "run"
            if kind == "benchmark" and r.get("parent_run_id"):
                kind = f"benchmark of {r['parent_run_id']}"
            table.add_row(
                r.get("run_id", ""),
                kind,
                r.get("deployment_name", ""),
                date,
                status,
                elapsed,
            )

        console.print(table)
        return

    # Guard rails on the option combinations. --force and --output only
    # make sense with --render: they are opt-ins to a regenerate action.
    if force and not render:
        print_error("--force requires --render")
        raise typer.Exit(ExitCode.USAGE)
    if output_path is not None and not render:
        print_error("--output requires --render")
        raise typer.Exit(ExitCode.USAGE)
    # --force only makes sense with --output: the default timestamped path
    # is collision-free in practice, so --force there is a no-op that only
    # confuses the caller. Matches the help text.
    if force and output_path is None:
        print_error("--force requires --output (the default timestamped path is collision-free)")
        raise typer.Exit(ExitCode.USAGE)

    if render:
        try:
            generator = ReportGenerator(metrics_dir)
            report_path = generator.generate_report(
                run_id,
                output_path=output_path,
                force=force,
                deployment_name=deployment_name,
            )
        except FileExistsError as e:
            print_error(str(e))
            # Only mention --output in the hint when the user actually set it;
            # the default timestamped path never collides in practice.
            if output_path is not None:
                print_info("Pass --force to overwrite the file at --output.")
            else:
                print_info("Retry in a moment; the timestamp will differ.")
            # An existing --output file is a usage error; a default-path
            # collision is a transient failure.
            code = ExitCode.USAGE if output_path is not None else ExitCode.FAILED
            raise typer.Exit(code)  # noqa: B904
        except ValueError as e:
            print_error(str(e))
            print_info("Use 'lakebench report --list' to see available runs")
            # An unknown run id is a bad argument; no runs at all is a failed lookup.
            raise typer.Exit(ExitCode.USAGE if run_id else ExitCode.FAILED)  # noqa: B904

        _json.set_data({"run_id": run_id, "report": str(report_path)})
        # Also print the summary when asked; keep the default quiet so
        # scripts that watch stdout for the path have a clean output.
        if summary:
            resolved = (
                storage.load_run(run_id)
                if run_id
                else storage.get_latest_run_for_deployment(deployment_name)
            )
            if resolved:
                _print_report_summary(resolved)

        console.print(
            Panel(
                f"[green]Report rendered[/green]\n\n"
                f"Output: {esc(report_path)}\n\n"
                f"The delivered run directory report.html is unchanged.",
                title="Report Rendered",
                expand=False,
            )
        )
        return

    # Default action: print the summary; do not regenerate the HTML.
    metrics = (
        storage.load_run(run_id)
        if run_id
        else storage.get_latest_run_for_deployment(deployment_name)
    )
    if metrics is None:
        if target_run is not None:
            print_error(f"No run {run_id} in {metrics_dir}, and no file {target}")
        else:
            print_error("No run found" + (f" with ID {run_id}" if run_id else ""))
        print_info("Use 'lakebench report --list' to see available runs")
        # An unknown run id is a bad argument; no runs at all is a failed lookup.
        raise typer.Exit(ExitCode.USAGE if run_id else ExitCode.FAILED)

    if output_format is not None:
        _print_stage_matrix(metrics, output_format)
        return

    _print_report_summary(metrics)

    # Not run_dir(): that creates the directory, and report only reads here.
    delivered = storage.metrics_dir / f"run-{run_id or metrics.run_id}" / "report.html"
    _json.set_data(
        _report_run_data(
            metrics, storage.metrics_dir, delivered if delivered.exists() else None, run_id
        )
    )
    if delivered.exists():
        console.print(f"[dim]Delivered report: {esc(delivered)}[/dim]")
        console.print(
            "[dim]Run 'lakebench report --render' to write a fresh HTML "
            "at lakebench-output/reports/.[/dim]"
        )
    else:
        console.print(
            f"[dim]No delivered report at {esc(delivered)}. "
            "Run 'lakebench report --render' to generate one.[/dim]"
        )


_LOGS_HELP_COMPONENTS = (
    "datagen, a stage (bronze-verify, silver-build, gold-finalize, bronze-ingest, "
    "silver-stream, gold-refresh, score-financial, ...), spark-driver, trino, "
    "trino-worker, thrift, duckdb, hive, polaris, postgres"
)


@app.command()
def logs(
    first: Annotated[
        str | None,
        typer.Argument(
            metavar="CONFIG",
            help="Path to configuration YAML file (default: ./lakebench.yaml)",
            show_default=False,
        ),
    ] = None,
    second: Annotated[
        str | None,
        typer.Argument(
            metavar="COMPONENT",
            help=f"Component to read: {_LOGS_HELP_COMPONENTS}",
            show_default=False,
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
            "-F",
            help="Follow log output (the newest matching pod)",
        ),
    ] = False,
    follow_short_f: Annotated[
        bool,
        typer.Option("-f", hidden=True, help=DEPRECATED_SHORT_F_HELP),
    ] = False,
    lines: Annotated[
        int,
        typer.Option(
            "--lines",
            "-n",
            min=1,
            help="Number of lines to show per pod",
        ),
    ] = 100,
    name: Annotated[
        str | None,
        typer.Option("--name", help=NAME_OPTION_HELP),
    ] = None,
    previous: Annotated[
        bool,
        typer.Option(
            "--previous",
            help="Read the previous (crashed or restarted) container instead",
        ),
    ] = False,
) -> None:
    """Show logs from a component of the deployment.

    `lakebench logs CONFIG COMPONENT`. The 1.6 order, COMPONENT first, still
    works. Log text goes to stdout, unformatted; pod headers and notices go
    to stderr.
    """
    from kubernetes import client as k8s_client

    from lakebench.cli import _cluster_ops as ops
    from lakebench.exit_codes import LakebenchError

    if follow_short_f:
        warn_deprecated_short_f("--follow / -F")
        follow = True

    try:
        component, config_arg, legacy = ops.resolve_logs_args(
            first, second, file_option is not None
        )
    except ValueError as e:
        print_error(str(e))
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    valid = ", ".join(ops.LOG_COMPONENTS)
    if component is None:
        print_error("Name a component: lakebench logs CONFIG COMPONENT")
        print_info(f"Valid components: {valid}")
        raise typer.Exit(ExitCode.USAGE)
    if component not in ops.LOG_COMPONENTS:
        print_error(f"Unknown component: {component}")
        print_info(f"Valid components: {valid}")
        raise typer.Exit(ExitCode.USAGE)
    if legacy:
        print_warning(
            f"`logs {component} {config_arg}` is the 1.6 argument order; "
            f"use `lakebench logs {config_arg} {component}`"
        )

    config_file = resolve_config_path(Path(config_arg) if config_arg else None, file_option)
    try:
        cfg = load_config(config_file, purpose=LoadPurpose.READ, name_override=name)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904

    # A nameless config reads only a deployment it can prove is its own.
    guard_nameless(cfg, config_file, allow_absent=True)
    namespace = cfg.get_namespace()
    source = ops.LOG_COMPONENTS[component]
    try:
        get_k8s_client(context=cfg.platform.kubernetes.context, namespace=namespace)
    except (K8sConnectionError, ConfigException) as e:
        print_error(f"Kubernetes connection failed: {e}")
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904
    core = k8s_client.CoreV1Api()

    try:
        pods = ops.list_pods(core, namespace, source.selector)
    except ops.ClusterReadError as e:
        print_error(f"Kubernetes API error: {e}")
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904
    if not pods:
        raise LakebenchError(
            f"no pod for {component} ({source.what}) in namespace {namespace}",
            next=f"lakebench status {config_file}" + (f" --name {name}" if name else ""),
            path="logs.no_pod",
            code=ExitCode.FAILED,
        )
    if follow and len(pods) > 1:
        print_info(
            f"Following {pods[-1].metadata.name}, the newest of {len(pods)} pods; "
            "leave out --follow to read them all"
        )

    try:
        outcome = ops.read_logs(
            core,
            namespace,
            pods,
            source,
            lines=lines,
            previous=previous,
            follow=follow,
            write=lambda text: emit_data(_strip_ansi(text)),
            header=print_info,
        )
    except ops.ClusterReadError as e:
        print_error(f"Kubernetes API error: {e}")
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904
    except KeyboardInterrupt:
        print_info("Log streaming stopped")
        raise typer.Exit(ExitCode.INTERRUPTED)  # noqa: B904

    for pod_name in outcome.empty:
        print_warning(f"No log output from pod {pod_name}")
    for line in outcome.unavailable:
        print_warning(f"No log: {line}")
    for error in outcome.errors:
        print_error(f"Kubernetes API error: {error}")
    if outcome.errors:
        raise typer.Exit(ExitCode.PREREQUISITE)
    if not outcome.pods:
        raise LakebenchError(
            f"no pod of {component} has a log to read"
            + (" from a previous container" if previous else ""),
            next=f"lakebench status {config_file}" + (f" --name {name}" if name else ""),
            path="logs.no_pod",
            code=ExitCode.FAILED,
        )


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

        console.print(Panel(f"Session: [bold]{esc(session_id)}[/bold]", expand=False))
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


@app.command(hidden=True, deprecated=True)
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
            min=1,
            help="Target scale factor to check requirements for",
        ),
    ] = None,
    extended: Annotated[
        bool,
        typer.Option(
            "--extended",
            "-e",
            help="[deprecated: use --slow-datagen] Reduces datagen parallelism to fit cluster.",
            hidden=True,
        ),
    ] = False,
    slow_datagen: Annotated[
        bool,
        typer.Option(
            "--slow-datagen",
            help="Ignored: datagen pods that do not fit queue, so datagen never limits the scale.",
        ),
    ] = False,
    mode: Annotated[
        str | None,
        typer.Option(
            "--mode",
            help="Pipeline mode: batch (sequential phases) or continuous (datagen and pipeline run concurrently; 'sustained' is accepted as an alias). Default: batch.",
        ),
    ] = None,
    schema_type: Annotated[
        str | None,
        typer.Option(
            "--schema",
            help="Workload schema (customer360 or financial). Default: customer360.",
        ),
    ] = None,
) -> None:
    """Show cluster sizing guidance for lakebench workloads.

    This command helps answer two questions:
    - "I have cluster X -- what scale can I run?"
    - "I want to run scale X -- what cluster do I need?"

    Every figure comes from the one sizing source that ``run``'s capacity
    preflight and ``config show`` use, for the default recipe
    (hive-iceberg-spark-trino) of the workload and mode. Without
    arguments it auto-detects the connected cluster and shows the largest
    scale that fits, up to the workload's datagen ceiling. Use --scale to
    see what one scale requests.

    Examples:

        lakebench recommend                     # auto-detect cluster, find max scale
        lakebench recommend --cores 64 --memory 256
        lakebench recommend --scale 100         # what do I need for scale 100?
    """
    from lakebench.cli._recommend import recommend_impl

    if extended:
        console.print("[yellow]--extended is deprecated, use --slow-datagen instead[/yellow]\n")

    def _detect():
        from lakebench.k8s.target import ClusterTarget

        target = ClusterTarget.current()
        console.print(f"[dim]Cluster context: {esc(target.label)}[/dim]")
        return get_k8s_client(target=target).get_cluster_capacity()

    code = recommend_impl(
        cluster_cores=cluster_cores,
        cluster_memory_gb=cluster_memory_gb,
        target_scale=target_scale,
        slow_datagen=slow_datagen or extended,
        mode=mode,
        schema_type=schema_type,
        detect_capacity=_detect,
    )
    if code:
        raise typer.Exit(code)


# =============================================================================
# Entry Point
# =============================================================================


def main() -> None:
    """Main entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
