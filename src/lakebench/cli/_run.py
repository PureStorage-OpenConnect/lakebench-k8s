"""Run command implementation -- extracted from cli/__init__.py."""

from __future__ import annotations

import logging
from datetime import datetime
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
    print_success,
    print_warning,
    resolve_config_path,
)
from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    load_config,
    parse_spark_memory,
)
from lakebench.config.schema import PipelineMode
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError

logger = logging.getLogger(__name__)


def _print_pipeline_scorecard(
    pb,
    stage_results: list[tuple[str, bool, float]],
    datagen_elapsed: float,
    benchmark_qph: float | None,
) -> None:
    """Print the pipeline complete panel with full scorecard."""
    # Stage timing lines
    lines: list[str] = []
    if datagen_elapsed > 0:
        lines.append(f"  data-generation {datagen_elapsed:>8.0f}s")
    for name, _ok, elapsed in stage_results:
        lines.append(f"  {name:<16}{elapsed:>8.0f}s")
    total_time = datagen_elapsed + sum(r[2] for r in stage_results)

    body = "[green]Pipeline completed successfully[/green]\n\n"
    body += "\n".join(lines)

    # Scores section
    scores: list[str] = []
    if pb.pipeline_mode == PipelineMode.SUSTAINED.value:
        if (pb.data_freshness_seconds or 0) > 0:
            scores.append(f"  Freshness:      {pb.data_freshness_seconds:>8.1f}s")
        if pb.sustained_throughput_rps > 0:
            scores.append(f"  Throughput:     {pb.sustained_throughput_rps:>8,.0f} rows/s")
        if pb.stage_latency_profile:
            lat = "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
            scores.append(f"  Latency (b/s/g):  {lat}ms")
        if pb.ingest_ratio > 0:
            pct = pb.ingest_ratio * 100
            scores.append(f"  Completeness:   {pct:>7.1f}%")
        if pb.pipeline_saturated:
            scores.append("  [yellow]Pipeline saturated (completeness < 95%)[/yellow]")
    else:
        if pb.time_to_value_seconds > 0:
            scores.append(f"  Time to Value:  {pb.time_to_value_seconds:>8.1f}s")
        if pb.pipeline_throughput_gb_per_second > 0:
            scores.append(f"  Throughput:     {pb.pipeline_throughput_gb_per_second:>8.3f} GB/s")
        if pb.total_data_processed_gb > 0:
            scores.append(f"  Data Processed: {pb.total_data_processed_gb:>8.1f} GB")
        if pb.compute_efficiency_gb_per_core_hour > 0:
            scores.append(
                f"  Efficiency:     {pb.compute_efficiency_gb_per_core_hour:>8.3f} GB/core-hr"
            )
        if pb.scale_ratio > 0:
            pct = pb.scale_ratio * 100
            label = "[green]verified[/green]" if pct >= 95 else "[yellow]incomplete[/yellow]"
            scores.append(f"  Scale:          {pct:>7.1f}% {label}")

    if benchmark_qph is not None:
        scores.append(f"  QpH:            {benchmark_qph:>8,.1f}")

    if scores:
        body += "\n\n[bold]Scores[/bold]\n" + "\n".join(scores)

    body += f"\n\nTotal: {total_time:.0f}s\n\nFull report: [bold]lakebench report[/bold]"

    console.print()
    console.print(Panel(body, title="Pipeline Complete", expand=False))


def _run_preflight_infra_check(cfg) -> None:
    """Check that required infrastructure is deployed before running the pipeline.

    Verifies the namespace, Postgres, configured catalog, and configured query
    engine are present and ready. Exits with actionable guidance if anything is
    missing.
    """
    ns = cfg.get_namespace()

    try:
        # Import via lakebench.cli so tests can patch lakebench.cli.get_k8s_client
        import lakebench.cli as _cli_mod

        k8s = _cli_mod.get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=ns,
        )
    except K8sConnectionError as e:
        print_error(f"Cannot connect to Kubernetes: {e}")
        print_info("Check your kubectl context and cluster connectivity")
        raise typer.Exit(1) from None

    # 1. Namespace must exist
    if not k8s.namespace_exists(ns):
        print_error(f"Namespace '{ns}' does not exist")
        print_info("Run 'lakebench deploy' to create the deployment first")
        raise typer.Exit(1)

    # 2. Build config-aware list of required components
    from kubernetes import client as k8s_client

    apps_v1 = k8s_client.AppsV1Api()

    required: list[tuple[str, str, str]] = [
        ("lakebench-postgres", "StatefulSet", "PostgreSQL"),
    ]
    cat = cfg.architecture.catalog.type.value
    if cat == "hive":
        required.append(("lakebench-hive-metastore-default", "StatefulSet", "Hive Metastore"))
    elif cat == "polaris":
        required.append(("lakebench-polaris", "Deployment", "Polaris Catalog"))

    engine = cfg.architecture.query_engine.type.value
    if engine == "trino":
        required.append(("lakebench-trino-coordinator", "Deployment", "Trino coordinator"))
        required.append(("lakebench-trino-worker", "StatefulSet", "Trino workers"))
    elif engine == "spark-thrift":
        required.append(("lakebench-spark-thrift", "Deployment", "Spark Thrift Server"))
    elif engine == "duckdb":
        required.append(("lakebench-duckdb", "Deployment", "DuckDB"))

    # 3. Check each component
    missing: list[str] = []
    not_ready: list[str] = []
    for name, kind, label in required:
        try:
            if kind == "StatefulSet":
                obj = apps_v1.read_namespaced_stateful_set(name, ns)
                ready = obj.status.ready_replicas or 0
                desired = obj.spec.replicas or 1
            else:
                obj = apps_v1.read_namespaced_deployment(name, ns)
                ready = obj.status.ready_replicas or 0
                desired = obj.spec.replicas or 1
            if ready < desired:
                not_ready.append(f"{label} ({ready}/{desired} replicas ready)")
        except k8s_client.rest.ApiException as e:
            if e.status == 404:
                missing.append(label)
            else:
                logger.warning("Preflight check error for %s: %s", name, e.reason)
                not_ready.append(f"{label} (API error: {e.reason})")

    # 4. Report and block
    if missing or not_ready:
        print_error("Infrastructure not ready -- cannot run pipeline")
        if missing:
            console.print(f"  [red]Missing:[/red] {', '.join(missing)}")
        if not_ready:
            console.print(f"  [yellow]Not ready:[/yellow] {', '.join(not_ready)}")
        console.print()
        if missing:
            print_info("Run 'lakebench deploy' to create the missing components")
        else:
            print_info("Wait for components to become ready, or check 'lakebench status'")
        # Show config mismatch hint if catalog/engine might be wrong
        console.print(f"  Config expects: catalog={cat}, query_engine={engine} (namespace: {ns})")
        raise typer.Exit(1)

    print_success("Infrastructure check passed")


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
    sustained: Annotated[
        bool,
        typer.Option(
            "--sustained",
            help="Run in sustained streaming mode (bronze-ingest -> silver-stream -> gold-refresh)",
        ),
    ] = False,
    continuous: Annotated[
        bool,
        typer.Option(
            "--continuous",
            help="Deprecated alias for --sustained",
            hidden=True,
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
            "--generate",
            help="Run datagen before pipeline stages (batch mode only; sustained always runs datagen)",
        ),
    ] = False,
    skip_deploy: Annotated[
        bool,
        typer.Option(
            "--skip-preflight",
            "--skip-deploy",
            help="Skip prerequisite checks and infrastructure validation",
        ),
    ] = False,
    skip_generate: Annotated[
        bool,
        typer.Option(
            "--skip-generate",
            help="Assume data already exists in bronze bucket",
        ),
    ] = False,
    skip_maintenance: Annotated[
        bool,
        typer.Option(
            "--skip-maintenance",
            help="Skip pre-benchmark maintenance (compaction, snapshot expiry)",
        ),
    ] = False,
    deploy_only: Annotated[
        bool,
        typer.Option(
            "--deploy-only",
            help="Deploy infrastructure and exit (do not generate or run pipeline)",
        ),
    ] = False,
    generate_only: Annotated[
        bool,
        typer.Option(
            "--generate-only",
            help="Deploy + generate data and exit (do not run pipeline)",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip all confirmation prompts",
        ),
    ] = False,
) -> None:
    """Execute the data pipeline.

    Runs the medallion pipeline (bronze -> silver -> gold).
    Each stage is a separate Spark job submitted to the cluster.
    Metrics are automatically collected and saved for reporting.
    After gold finalize, runs a query benchmark (QpH).
    Use --skip-benchmark to skip the benchmark stage.

    With --generate (batch mode), generates data first, then runs the full
    pipeline. Sustained mode always runs datagen automatically.

    With --sustained, runs the streaming pipeline instead:
    starts datagen, then launches bronze-ingest, silver-stream,
    and gold-refresh as concurrent streaming jobs. Monitors for
    the configured duration, then stops streaming and runs benchmark.
    """
    import uuid

    from lakebench.cli._sustained import (
        _collect_platform_metrics,
        _probe_table_health,
        _run_iceberg_compaction,
        _run_iceberg_maintenance,
        _run_sustained,
        _wait_for_query_engine_ready,
    )
    from lakebench.engine import get_engine
    from lakebench.metrics import JobMetrics, MetricsCollector, MetricsStorage
    from lakebench.spark import SparkJobMonitor, SparkOperatorManager
    from lakebench.spark.job import (
        JobState,
        JobType,
        SparkJobManager,
        get_executor_count,
        get_job_profile,
    )

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
        from lakebench.k8s import get_k8s_client

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
        timeout = max(3600, scale * 120)
        if scale >= 50:
            print_info(f"Per-job timeout: {timeout}s (auto-scaled for scale {scale})")

    # Flag mutual exclusivity
    if deploy_only and generate_only:
        print_error("--deploy-only and --generate-only are mutually exclusive")
        raise typer.Exit(1)

    # deploy_only: deploy infrastructure and exit
    if deploy_only:
        from lakebench.cli import deploy

        print_info("--deploy-only: deploying infrastructure...")
        deploy(config_file=config_file, yes=yes)
        return

    # generate_only: deploy + generate and exit
    if generate_only:
        from lakebench.cli import deploy, generate

        print_info("--generate-only: deploying and generating data...")
        deploy(config_file=config_file, yes=yes)
        generate(config_file=config_file, wait=True, timeout=timeout or 14400, yes=yes)
        return

    # -- Phase 1/7: Prerequisites ------------------------------------------------
    console.print()
    console.print("[bold dim]Phase 1/7: Prerequisites[/bold dim]")

    if not skip_deploy:
        from lakebench.cli._prerequisites import run_prerequisites

        prereq_report = run_prerequisites(cfg)
        for check in prereq_report.checks:
            icon = "[green]\u2713[/green]" if check.passed else "[red]\u2717[/red]"
            console.print(f"  {icon} {check.name}: {check.message}")
            if not check.passed and check.hint:
                for line in check.hint.split("\n"):
                    console.print(f"      [dim]{line}[/dim]")

        if not prereq_report.all_passed:
            print_error("Prerequisites not met -- cannot proceed")
            raise typer.Exit(1)
        print_success("All prerequisites passed")

        # Also run infrastructure readiness check
        # If namespace doesn't exist and --yes is set, auto-deploy first
        ns = cfg.get_namespace()
        try:
            from lakebench.k8s import get_k8s_client

            _k8s_check = get_k8s_client(
                context=cfg.platform.kubernetes.context,
                namespace=ns,
            )
            if not _k8s_check.namespace_exists(ns):
                if yes:
                    from lakebench.cli import deploy

                    print_info(f"Namespace '{ns}' not found -- auto-deploying...")
                    deploy(config_file=config_file, yes=True)
                else:
                    print_error(f"Namespace '{ns}' does not exist")
                    print_info("Run 'lakebench deploy' first, or use --yes to auto-deploy")
                    raise typer.Exit(1)
        except K8sConnectionError:
            pass  # preflight will catch this

        _run_preflight_infra_check(cfg)
    else:
        print_info("Skipping prerequisites (--skip-preflight)")

    # Branch: sustained streaming pipeline (CLI flag overrides config)
    use_sustained = sustained or continuous or cfg.architecture.pipeline.mode == "sustained"
    if use_sustained:
        _run_sustained(cfg, config_file, timeout, skip_benchmark, duration)
        return

    # -- Phase 2/7: Deploy (handled by prerequisite check above) ---------------
    console.print()
    console.print("[bold dim]Phase 2/7: Infrastructure[/bold dim]")
    print_success("Infrastructure verified (deploy with 'lakebench deploy' if needed)")

    console.print(
        Panel(
            f"Running pipeline for: [bold]{cfg.name}[/bold]\n\n"
            f"Stages: bronze-verify \u2192 silver-build \u2192 gold-finalize",
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
    results: list[tuple[str, bool, float]] = []
    benchmark_qph: float | None = None

    try:
        # Check Spark operator
        print_info("Checking Spark Operator...")
        spark_op_cfg = cfg.platform.compute.spark.operator
        operator = SparkOperatorManager(
            namespace=spark_op_cfg.namespace,
            version=spark_op_cfg.version if spark_op_cfg.install else None,
            job_namespace=cfg.get_namespace(),
        )
        status = operator.check_status()

        if not status.ready:
            hint = ""
            if not status.installed and spark_op_cfg.install:
                hint = " -- run 'lakebench deploy' first to install it"
            print_error(f"Spark Operator not ready: {status.message}{hint}")
            pipeline_success = False
            raise typer.Exit(1)

        # Ensure operator watches the target namespace (always try to heal)
        ns_status = operator.ensure_namespace_watched(can_heal=True)
        if ns_status.watching_namespace is False:
            print_error(ns_status.message)
            pipeline_success = False
            raise typer.Exit(1)

        print_success(f"Spark Operator ready (version: {status.version or 'unknown'})")

        # Initialize job manager
        from lakebench.k8s import get_k8s_client

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )

        job_manager: SparkJobManager = get_engine(cfg, k8s)  # type: ignore[assignment]
        monitor = SparkJobMonitor(cfg, k8s)

        # Deploy scripts ConfigMap -- must succeed or pipeline jobs will fail
        print_info("Deploying Spark scripts...")
        if not job_manager.deploy_scripts_configmap():
            print_error("Failed to deploy Spark scripts ConfigMap -- pipeline cannot proceed")
            _journal_safe(j.end_command, success=False, message="Scripts ConfigMap deploy failed")
            raise typer.Exit(1)
        print_success("Spark scripts deployed")

        # -- Phase 3/7: Generate data -----------------------------------------------
        console.print()
        console.print("[bold dim]Phase 3/7: Generate[/bold dim]")
        if include_datagen and not skip_generate:
            console.print("[bold]Stage: datagen (ingest)[/bold]")
            print_info("Generating data for pipeline benchmark...")
            datagen_start = datetime.now()
            try:
                import time as _time

                from rich.progress import (
                    BarColumn,
                    Progress,
                    SpinnerColumn,
                    TextColumn,
                    TimeElapsedColumn,
                    TimeRemainingColumn,
                )

                from lakebench.deploy import DatagenDeployer, DeploymentEngine

                dg_engine = DeploymentEngine(cfg)
                datagen_deployer = DatagenDeployer(dg_engine)
                datagen_deployer.deploy()

                # Progress bar (same as standalone generate command)
                _dg_start = _time.time()
                _initial = datagen_deployer.get_progress()
                _total_pods = _initial.get("completions", 1)

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("{task.completed}/{task.total} pods"),
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    console=console,
                ) as _dg_bar:
                    _dg_task = _dg_bar.add_task("Generating data", total=_total_pods)
                    while _time.time() - _dg_start < timeout:
                        _dg_prog = datagen_deployer.get_progress()
                        if not _dg_prog.get("running", False):
                            if _dg_prog.get("error"):
                                _dg_bar.stop()
                                print_error(_dg_prog["error"])
                                raise typer.Exit(1)
                            _dg_bar.update(_dg_task, completed=_total_pods)
                            break
                        if _dg_prog.get("oom_pods"):
                            _dg_bar.stop()
                            print_error(f"OOMKilled: {', '.join(_dg_prog['oom_pods'])}")
                            raise typer.Exit(1)
                        _dg_bar.update(_dg_task, completed=_dg_prog.get("succeeded", 0))
                        _time.sleep(15)

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
        else:
            print_info("Skipped (use --generate to include datagen)")

        # -- Phase 4/7: Pipeline stages ---------------------------------------------
        console.print()
        console.print("[bold dim]Phase 4/7: Pipeline[/bold dim]")
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

        # Multi-cycle batch support (v1.1.0)
        total_cycles = cfg.architecture.pipeline.cycles

        for cycle_idx in range(total_cycles):
            # Track per-cycle metrics (v1.1.0)
            _cycle_start = datetime.now()
            _cycle_jobs: list[JobMetrics] = []
            _cycle_ts_start = ""
            _cycle_ts_end = ""
            _cycle_dg_elapsed = 0.0

            # Cycle header for multi-cycle runs
            if total_cycles > 1:
                console.print()
                console.print(f"[bold cyan]Cycle {cycle_idx + 1}/{total_cycles}[/bold cyan]")

                # Run datagen for this cycle's time window
                try:
                    from lakebench.deploy import (
                        DatagenDeployer,
                        DeploymentEngine,
                        DeploymentStatus,
                    )

                    _cycle_engine = DeploymentEngine(cfg)
                    _cycle_datagen = DatagenDeployer(_cycle_engine)
                    datagen_result = _cycle_datagen.deploy_cycle(cycle_idx, total_cycles)
                    if datagen_result.status != DeploymentStatus.SUCCESS:
                        print_warning(
                            f"Datagen cycle {cycle_idx + 1} failed: {datagen_result.message}"
                        )
                    else:
                        ts_start = datagen_result.details.get("timestamp_start", "")
                        ts_end = datagen_result.details.get("timestamp_end", "")
                        _cycle_ts_start = ts_start
                        _cycle_ts_end = ts_end
                        print_info(f"Datagen: {ts_start} to {ts_end}")

                        # Wait for datagen completion
                        _dg_start = datetime.now()
                        dg_wait = _cycle_datagen.wait_for_completion(timeout_seconds=timeout)
                        _cycle_dg_elapsed = (datetime.now() - _dg_start).total_seconds()
                        if dg_wait.status != DeploymentStatus.SUCCESS:
                            print_warning(f"Datagen did not complete: {dg_wait.message}")
                except Exception as e:
                    print_warning(f"Cycle datagen failed: {e}")

            # Cycle env vars for incremental mode (cycles 2+)
            cycle_env: dict[str, str] | None = None
            if cycle_idx > 0:
                cycle_env = {
                    "LB_SILVER_INCREMENTAL": "true",
                    "LB_GOLD_INCREMENTAL": "true",
                }

            # Run each stage
            for job_type, stage_name, description in stages:
                console.print()
                if total_cycles > 1:
                    console.print(f"[bold]Stage: {stage_name} (cycle {cycle_idx + 1})[/bold]")
                else:
                    console.print(f"[bold]Stage: {stage_name}[/bold]")
                print_info(description)

                job_start = datetime.now()

                # Submit job
                job_status = job_manager.submit_job(job_type, cycle_env=cycle_env)
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

                _last_heartbeat_ts = job_start

                def on_progress(status, _start=job_start, _hb=[job_start]):  # noqa: B006
                    nonlocal _max_executors, _last_reported_executors
                    if status.state == JobState.RUNNING:
                        _max_executors = max(_max_executors, status.executor_count)
                        elapsed = (datetime.now() - _start).total_seconds()
                        if status.executor_count != _last_reported_executors:
                            console.print(f"  Running... (executors: {status.executor_count})")
                            _last_reported_executors = status.executor_count
                            _hb[0] = datetime.now()
                        elif (datetime.now() - _hb[0]).total_seconds() >= 60:
                            console.print(f"  Running... ({int(elapsed)}s elapsed)")
                            _hb[0] = datetime.now()

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
                    _overhead_gb = parse_spark_memory(_profile["executor_memory_overhead"]) / (
                        1024**3
                    )
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
                _cycle_jobs.append(job_metrics)

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

            # Record CycleMetrics after all stages for this cycle (v1.1.0)
            if total_cycles > 1:
                _cycle_health: dict[str, int] = {}
                try:
                    _cycle_health = _probe_table_health(cfg, k8s)
                except Exception:
                    pass
                from lakebench.metrics.collector import CycleMetrics as _CM

                _cm = _CM(
                    cycle_index=cycle_idx,
                    timestamp_start=_cycle_ts_start,
                    timestamp_end=_cycle_ts_end,
                    datagen_elapsed_seconds=_cycle_dg_elapsed,
                    jobs=list(_cycle_jobs),
                    table_health=_cycle_health,
                )
                if collector.current_run is not None:
                    collector.current_run.cycles.append(_cm)

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

        # -- Phase 5/7: Maintenance ------------------------------------------------
        console.print()
        console.print("[bold dim]Phase 5/7: Maintenance[/bold dim]")
        # Flow: pre-compaction benchmark -> maintenance -> post-compaction benchmark
        # The pre/post delta quantifies the value of table maintenance.
        pre_compaction_qph = 0.0
        pre_file_count = 0
        post_file_count = 0
        maint_elapsed = 0.0

        do_maintenance = (
            not skip_benchmark
            and not skip_maintenance
            and cfg.architecture.pipeline.pre_benchmark_maintenance
        )

        if not do_maintenance:
            if skip_benchmark:
                print_info("Skipped (no query engine)")
            elif skip_maintenance:
                print_info("Skipped (--skip-maintenance)")

        if do_maintenance:
            from lakebench.benchmark import BenchmarkRunner as _BR

            def _bench_progress(idx, total, name, phase, **kwargs):
                if phase == "start":
                    console.print(f"  [{idx}/{total}] {name}...", end=" ")
                elif phase == "done":
                    elapsed = kwargs.get("elapsed", 0)
                    success = kwargs.get("success", False)
                    error = kwargs.get("error", "")
                    if success:
                        console.print(f"[green]{elapsed:.1f}s OK[/green]")
                    else:
                        short_err = (error[:60] + "...") if len(error) > 60 else error
                        console.print(f"[red]FAIL[/red] ({short_err})")

            try:
                # 1. Pre-compaction file count (probe first to decide if benchmark is feasible)
                try:
                    _pre_health = _probe_table_health(cfg, k8s)
                    pre_file_count = sum(
                        v
                        for k, v in _pre_health.items()
                        if "file_count" in k and isinstance(v, int)
                    )
                except Exception:
                    pass

                # 2. Pre-compaction benchmark (skip if too many files)
                console.print()
                console.print("[bold]Pre-compaction benchmark[/bold]")
                if pre_file_count > 200_000:
                    print_warning(
                        f"Skipping pre-compaction benchmark ({pre_file_count:,} uncompacted files) "
                        "-- too many files for reliable measurement"
                    )
                else:
                    print_info("Benchmarking before maintenance (uncompacted data)...")
                    _pre_runner = _BR(cfg)
                    _pre_result = _pre_runner.run_power(
                        cache="hot",
                        progress_callback=_bench_progress,
                        query_timeout=60,
                    )
                    pre_compaction_qph = _pre_result.qph
                    _succeeded = sum(1 for q in _pre_result.queries if q.success)
                    console.print(
                        f"  Pre-compaction QpH: {pre_compaction_qph:.1f} "
                        f"({_succeeded}/{len(_pre_result.queries)} queries succeeded)"
                    )

                # 3. Run maintenance
                console.print()
                console.print("[bold]Running maintenance[/bold]")
                _maint_start = datetime.now()
                _run_iceberg_maintenance(cfg, k8s, console, j, retention_threshold="0s")
                _run_iceberg_compaction(cfg, k8s, console, j)
                _wait_for_query_engine_ready(cfg, k8s, console, timeout=120)
                maint_elapsed = (datetime.now() - _maint_start).total_seconds()

                # 4. Post-compaction file count
                try:
                    _post_health = _probe_table_health(cfg, k8s)
                    post_file_count = sum(
                        v
                        for k, v in _post_health.items()
                        if "file_count" in k and isinstance(v, int)
                    )
                except Exception:
                    pass

            except Exception as e:
                print_warning(f"Maintenance cost measurement failed (non-fatal): {e}")

        # -- Phase 6/7: Benchmark --------------------------------------------------
        console.print()
        console.print("[bold dim]Phase 6/7: Benchmark[/bold dim]")
        # Run post-compaction benchmark (or the only benchmark if maintenance skipped)
        if skip_benchmark:
            print_info("Skipped (no query engine)")
        if not skip_benchmark:
            try:
                from lakebench.benchmark import BenchmarkRunner
                from lakebench.metrics import BenchmarkMetrics

                console.print()
                console.print("[bold]Stage: benchmark[/bold]")
                label = "post-compaction " if do_maintenance else ""
                print_info(f"Running {label}query benchmark (hot cache, power)...")

                _journal_safe(
                    j.record,
                    EventType.BENCHMARK_START,
                    message="Benchmark started",
                    details={"mode": "power", "cache": "hot"},
                )

                def _post_bench_progress(idx, total, name, phase, **kwargs):
                    if phase == "start":
                        console.print(f"  [{idx}/{total}] {name}...", end=" ")
                    elif phase == "done":
                        elapsed = kwargs.get("elapsed", 0)
                        success = kwargs.get("success", False)
                        error = kwargs.get("error", "")
                        if success:
                            console.print(f"[green]{elapsed:.1f}s OK[/green]")
                        else:
                            short_err = (error[:60] + "...") if len(error) > 60 else error
                            console.print(f"[red]FAIL[/red] ({short_err})")

                bench_runner = BenchmarkRunner(cfg)
                bench_result = bench_runner.run_power(
                    cache="hot",
                    progress_callback=_post_bench_progress,
                )

                console.print(f"\n  Total: {bench_result.total_seconds:.2f}s")
                console.print(f"  [bold]QpH:   {bench_result.qph:.1f}[/bold]")
                benchmark_qph = bench_result.qph

                # Maintenance cost summary (v1.3)
                # Only show when maintenance actually ran (maint_elapsed > 0)
                if pre_compaction_qph > 0 and benchmark_qph > 0 and maint_elapsed > 0:
                    improvement = ((benchmark_qph - pre_compaction_qph) / pre_compaction_qph) * 100
                    console.print(
                        f"  [bold]Maintenance value: {improvement:+.1f}% QpH improvement[/bold]"
                    )
                    if pre_file_count > 0:
                        console.print(
                            f"  Files: {pre_file_count} -> {post_file_count} "
                            f"({pre_file_count / max(post_file_count, 1):.1f}x compaction)"
                        )

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

        # Summary panel is printed in the finally block (after pipeline
        # benchmark scores are computed) so it can include the full scorecard.

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        pipeline_success = False
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904
    finally:
        # -- Phase 7/7: Results ----------------------------------------------------
        console.print()
        console.print("[bold dim]Phase 7/7: Results[/bold dim]")
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
                ca_cert=s3_cfg.ca_cert,
                verify_ssl=s3_cfg.verify_ssl,
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
            # Collect platform metrics from Prometheus (best-effort)
            _collect_platform_metrics(cfg, run_metrics)

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

                # Populate maintenance cost metrics (v1.3)
                if maint_elapsed > 0:
                    pb.maintenance_elapsed_seconds = maint_elapsed
                    if pb.total_elapsed_seconds > 0:
                        pb.maintenance_pct_of_pipeline = (
                            maint_elapsed / pb.total_elapsed_seconds
                        ) * 100
                if pre_file_count > 0:
                    pb.pre_compaction_file_count = pre_file_count
                    pb.post_compaction_file_count = post_file_count
                    pb.compaction_ratio = pre_file_count / max(post_file_count, 1)
                if pre_compaction_qph > 0:
                    pb.pre_compaction_qph = pre_compaction_qph
                    pb.post_compaction_qph = benchmark_qph
                    if pre_compaction_qph > 0:
                        pb.maintenance_value_pct = (
                            (benchmark_qph - pre_compaction_qph) / pre_compaction_qph
                        ) * 100

                # Print full scorecard panel
                if pipeline_success:
                    _print_pipeline_scorecard(pb, results, _datagen_elapsed, benchmark_qph)
            except Exception as e:
                console.print(f"  [yellow]Could not build pipeline benchmark: {e}[/yellow]")
                # Fallback summary if scorecard build failed
                if pipeline_success and results:
                    _total = sum(r[2] for r in results)
                    _qph = f"\nQpH: {benchmark_qph:.1f}" if benchmark_qph else ""
                    console.print(
                        Panel(
                            "[green]Pipeline complete[/green]\n\n"
                            + "\n".join(f"  {n}: {el:.0f}s" for n, _, el in results)
                            + f"\n\nTotal: {_total:.0f}s{_qph}"
                            + "\n\nFull report: [bold]lakebench report[/bold]",
                            title="Pipeline Complete",
                            expand=False,
                        )
                    )

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
