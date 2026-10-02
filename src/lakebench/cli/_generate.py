"""Generate command for Lakebench CLI."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel

from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    LoadPurpose,
    load_config,
)
from lakebench.exit_codes import ExitCode
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError, get_k8s_client

from ._helpers import (
    _journal_safe,
    check_datagen_scale,
    console,
    enforce_bronze_regenerate,
    esc,
    journal_open,
    print_error,
    print_info,
    print_success,
    print_warning,
    resolve_config_path,
)

logger = logging.getLogger(__name__)


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
    timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            "-t",
            help=(
                "Timeout in seconds when waiting for completion. "
                "0 (default) auto-computes from scale, parallelism and a "
                "conservative per-pod throughput; pass a positive int to "
                "override."
            ),
        ),
    ] = 0,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip confirmation prompt",
        ),
    ] = False,
    regenerate: Annotated[
        bool,
        typer.Option(
            "--regenerate",
            help=(
                "Empty the bronze bucket before generating. Without this "
                "flag, a non-empty bronze prefix is refused (exit 3) so "
                "existing datagen output is never overwritten silently."
            ),
        ),
    ] = False,
) -> None:
    """Generate synthetic data to bronze bucket.

    Runs the datagen job to populate the bronze bucket with synthetic data.
    Uses parallel Kubernetes Jobs. Interrupted runs are re-run from the
    start; the Rust generator has no checkpoint-resume.
    """
    from lakebench.deploy import DatagenDeployer, DeploymentEngine, DeploymentStatus

    config_file = resolve_config_path(config_file, file_option)

    # Load configuration
    try:
        cfg = load_config(config_file, purpose=LoadPurpose.MUTATE)
    except ConfigFileNotFoundError as e:
        print_error(f"File not found: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    except ConfigValidationError as e:
        print_error("Config validation failed:")
        for err in e.errors:
            loc = ".".join(str(x) for x in err["loc"])
            console.print(f"  [red]*[/red] {esc(loc)}: {esc(err['msg'])}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904

    check_datagen_scale(cfg)

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
    # Cuts to fit the cluster are shown with their reason, never silent (LB-160).
    for cut in resolve_auto_sizing(cfg, cluster_cap) or []:
        print_warning(f"Auto-sizing: {cut}")

    workload = cfg.architecture.workload
    datagen_cfg = workload.datagen
    dims = cfg.get_scale_dimensions()

    # Auto-compute --timeout when the operator passed 0 (the new default).
    # Anchored on live UAT: scale 100 (~730 GB) with 30 pods took ~4500 s,
    # so per-pod effective throughput is ~5 MB/s to S3-A after row build,
    # partitioning, and parquet encoding. Formula:
    #   time = (gb * 1024) / (pods * 5 MB/s) * 2   (100% headroom)
    # Floor 900 s so small scales still have room for pod scheduling and
    # image pull; ceiling 86400 s (24 h) to keep a typo from parking a
    # runaway wait forever. If `approx_bronze_gb` isn't populated the
    # formula cannot estimate wall time -- fall back to a fixed 7200 s
    # default (matching the pre-LB-111 hard-coded value) rather than
    # let it clamp to the 900 s floor and declare a 75-minute job
    # failed after 15 minutes.
    if timeout <= 0:
        pods = max(1, int(datagen_cfg.parallelism or 1))
        gb = max(0.0, float(dims.approx_bronze_gb or 0.0))
        if gb <= 0:
            timeout = 7200
            print_info(
                "--timeout auto: approx_bronze_gb not populated by scale "
                "dimensions; falling back to 7200s. Pass --timeout N to "
                "override for jobs longer than 2 hours."
            )
        else:
            auto = int((gb * 1024.0 / (pods * 5.0)) * 2.0)
            timeout = max(900, min(auto, 86400))
            if auto > 86400:
                # Real datagens can plausibly exceed 24 hours at scale
                # >= 500. Warn LOUD so the operator explicitly picks
                # --timeout, rather than watching the wait declare
                # failure at 24h while pods keep succeeding.
                print_info(
                    f"--timeout auto WARNING: computed {auto}s exceeds the "
                    f"86400s (24h) safety cap. Clamped to 86400s -- pass "
                    f"--timeout {auto} explicitly if the job really needs "
                    f"the full estimate."
                )
            print_info(
                f"--timeout auto={timeout}s "
                f"(scale~{gb:.0f} GB / {pods} pods @ 5 MB/s/pod, 2x headroom)"
            )

    console.print(
        Panel(
            f"Generating data for: [bold]{esc(cfg.name)}[/bold]\n\n"
            f"Scale: {esc(dims.scale)}\n"
            f"Customers: {dims.customers:,}\n"
            f"Parallelism: {esc(datagen_cfg.parallelism)} pods\n"
            f"Bucket: {esc(cfg.platform.storage.s3.buckets.bronze)}",
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
            "timeout": timeout,
        },
    )

    try:
        # Share a run id with the datagen pods (live-observability grouping
        # label) so a standalone `generate` is correlatable in Grafana and a
        # re-run does not read as the previous run's series. Respect an id an
        # enclosing run flow already set.
        if not os.environ.get("LB_RUN_ID"):
            import uuid as _uuid
            from datetime import datetime as _dt

            os.environ["LB_RUN_ID"] = (
                _dt.now().strftime("%Y%m%d-%H%M%S") + "-" + _uuid.uuid4().hex[:6]
            )

        # A4 (v1.6): refuse to over-write an existing bronze prefix unless
        # --regenerate was passed; with --regenerate, empty the bucket first.
        enforce_bronze_regenerate(cfg, regenerate)

        engine = DeploymentEngine(cfg)
        datagen = DatagenDeployer(engine)

        # Submit job
        print_info("Submitting datagen job...")
        result = datagen.deploy()

        if result.status != DeploymentStatus.SUCCESS:
            print_error(f"Failed to submit job: {result.message}")
            _journal_safe(j.end_command, success=False, message=result.message)
            raise typer.Exit(ExitCode.FAILED)

        print_success("Datagen job submitted")
        console.print(f"  Parallelism: {esc(result.details.get('parallelism', '?'))} pods")
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
                        raise typer.Exit(ExitCode.FAILED)
                    # Mark complete
                    progress_bar.update(task, completed=total_completions)
                    break

                # Surface pod failures early
                if prog.get("oom_pods"):
                    progress_bar.stop()
                    print_error(f"OOMKilled: {', '.join(prog['oom_pods'])}")
                    print_info("Increase datagen memory or reduce parallelism")
                    _journal_safe(j.end_command, success=False, message="OOMKilled pods detected")
                    raise typer.Exit(ExitCode.FAILED)
                if prog.get("crash_pods"):
                    # A crash-looping generator never finishes; waiting out the
                    # timeout (hours at large scale) only hides the failure.
                    progress_bar.stop()
                    details = prog.get("crash_details", {})
                    for pod in prog["crash_pods"]:
                        print_error(f"Datagen pod crash-looping: {pod} {details.get(pod, '')}")
                    ns = cfg.get_namespace()
                    print_info(
                        f"See why with: kubectl logs -n {ns} {prog['crash_pods'][0]} --previous"
                    )
                    _journal_safe(
                        j.end_command, success=False, message="Datagen pods crash-looping"
                    )
                    raise typer.Exit(ExitCode.FAILED)
                if prog.get("pending_pods"):
                    progress_bar.console.print(
                        f"  [yellow]{len(prog['pending_pods'])} pod(s) pending[/yellow]"
                    )

                succeeded = prog.get("succeeded", 0)
                progress_bar.update(task, completed=succeeded)

                time.sleep(30)

        # Final result. Give the finalizer whatever budget remains under
        # the operator's --timeout so a job that was still running when
        # the polling loop's outer timeout hit gets one more real check
        # before being declared failed. Prior code hard-coded 10s here,
        # which meant --timeout 3600 on a real UAT declared a still-
        # running datagen a failure ~10s after the polling loop's own
        # timeout expired -- exit code 0 but "Generation Failed" logged
        # while the pods kept generating for another half hour.
        elapsed = time.time() - start
        final_budget = max(30, int(timeout - elapsed))
        completion_result = datagen.wait_for_completion(timeout_seconds=final_budget)

        console.print()
        if completion_result.status == DeploymentStatus.SUCCESS:
            # Collect per-pod metrics from the datagen pod logs. Failure to
            # collect is not a hard failure: the metrics are for the pipeline
            # scorecard, not for correctness of the data. Log a warning and
            # continue.
            fleet_dict: dict | None = None
            try:
                from lakebench._constants import DEFAULT_OUTPUT_DIR
                from lakebench.metrics.datagen_aggregator import collect_from_k8s

                fleet = collect_from_k8s(
                    namespace=cfg.get_namespace(),
                    job_completions=int(
                        completion_result.details.get(
                            "completions", completion_result.details.get("succeeded", 0)
                        )
                        or 0
                    ),
                )
                fleet_dict = fleet.to_dict()
                # Sidecar file keyed by namespace so parallel UAT runs in
                # different namespaces do NOT overwrite each other. The
                # payload also carries `namespace` so a run in ns-A that
                # accidentally reads ns-B's sidecar is caught downstream.
                # Wall-clock timestamp lets `lakebench run` reject a
                # sidecar that predates the pipeline invocation.
                from datetime import datetime, timezone

                ns = cfg.get_namespace()
                fleet_dict["namespace"] = ns
                fleet_dict["written_at"] = datetime.now(timezone.utc).isoformat()
                out_dir = Path(DEFAULT_OUTPUT_DIR) / "datagen"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{ns}-datagen-metrics.json"
                import json as _json

                out_path.write_text(_json.dumps(fleet_dict, indent=2))
                print_info(
                    f"Datagen metrics: {fleet.pods_reported}/{fleet.pods_expected} pods "
                    f"reported, aggregate {fleet.aggregate_mbps:.1f} MB/s, "
                    f"{fleet.cpu_hr_per_tb:.2f} CPU-hr/TB"
                    if fleet.cpu_hr_per_tb is not None
                    else f"Datagen metrics: {fleet.pods_reported}/{fleet.pods_expected} pods reported"
                )
                print_info(f"  written to {out_path}")
            except Exception as e:
                logger.warning("failed to collect per-pod datagen metrics: %s", e)

            _journal_safe(
                j.record,
                EventType.GENERATE_COMPLETE,
                message="Data generation complete",
                success=True,
                details={
                    "succeeded_pods": completion_result.details.get("succeeded", 0),
                    "failed_pods": completion_result.details.get("failed", 0),
                    "elapsed_seconds": completion_result.elapsed_seconds,
                    "datagen_metrics": fleet_dict,
                },
            )
            _journal_safe(j.end_command, success=True)

            # The prefix DatagenDeployer wrote under.
            from lakebench.deploy.datagen import bronze_datagen_prefix

            written_prefix = bronze_datagen_prefix(cfg)
            console.print(
                Panel(
                    f"[green]Data generation complete![/green]\n\n"
                    f"Succeeded: {esc(completion_result.details.get('succeeded', '?'))} pods\n"
                    # Whole wait, not the finalizer's own check (which is ~0s
                    # when the polling loop already saw the job finish).
                    f"Elapsed: {time.time() - start:.0f}s\n\n"
                    f"Data written to: s3://{esc(cfg.platform.storage.s3.buckets.bronze)}/{esc(written_prefix)}"
                    f"\n\nNext: [bold]lakebench run[/bold]  to execute the pipeline",
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
                    f"[red]Data generation failed![/red]\n\n{esc(completion_result.message)}",
                    title="Generation Failed",
                    expand=False,
                )
            )
            raise typer.Exit(ExitCode.FAILED)

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904  kube config did not load
