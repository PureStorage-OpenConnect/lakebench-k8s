"""Local mode execution for ``deploy --local`` and ``run --local``.

This is a separate path rather than a branch inside the Kubernetes commands.
``_run.py`` is entangled with operator readiness checks, script ConfigMaps, and
datagen Jobs, none of which exist locally; threading a flag through it would
mean a conditional at a dozen points for a path that shares almost none of the
same steps. The design doc's constraint applies: the local path is additive and
the Kubernetes path is untouched.

What is shared is everything above the substrate -- config loading, validation,
the S3 client, and the scale factor.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from rich.panel import Panel
from rich.table import Table

from lakebench.cli._helpers import console, print_error, print_info, print_success
from lakebench.config.scale import get_dimensions
from lakebench.config.schema import LakebenchConfig
from lakebench.deploy.local import SKIPPED_COMPONENTS, LocalDeployer, LocalDeployment
from lakebench.modules.pipeline_engines.spark.job import (
    LOCAL_SCALE_ADVISORY_MAX,
    local_peak_memory_gb,
)
from lakebench.modules.pipeline_engines.spark.local_job import (
    LocalSparkConfig,
    LocalSparkRunner,
)

# Batch jobs, in order. Sustained mode has no local profile: streaming needs a
# long-lived cluster, and the small-file proliferation described in gotcha 17
# makes it a poor fit for a laptop.
LOCAL_JOB_ORDER: tuple[str, ...] = ("bronze-verify", "silver-build", "gold-finalize")


class LocalModeError(RuntimeError):
    """Local mode cannot run with the given configuration."""


@dataclass
class LocalRunResult:
    """Outcome of a local pipeline run."""

    success: bool
    elapsed_seconds: float
    stages: list[tuple[str, bool, float]]

    @property
    def failed_stage(self) -> str:
        for name, ok, _ in self.stages:
            if not ok:
                return name
        return ""


def default_workdir(config_name: str) -> Path:
    """Host directory for a local deployment.

    Kept under the user's home rather than /tmp: the Ivy cache is ~1.2 GB and
    re-downloading it dominates a cold run, so it must survive a reboot.
    """
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in config_name)
    return Path.home() / ".lakebench" / "local" / (safe or "default")


def check_local_supported(cfg: LakebenchConfig) -> None:
    """Raise if the config asks for something local mode cannot do.

    Local mode is Iceberg-only. DuckDB's delta extension uses delta-kernel-rs,
    which ignores DuckDB's S3 settings and hangs on AWS IMDS against a non-AWS
    endpoint (gotcha 18), and Trino would reintroduce a catalog service and
    PostgreSQL, defeating the two-container model.
    """
    table_format = cfg.architecture.table_format.type
    fmt = getattr(table_format, "value", str(table_format))
    if fmt != "iceberg":
        raise LocalModeError(
            f"Local mode supports Iceberg only, config requests {fmt!r}. "
            "DuckDB cannot read Delta on a non-AWS S3 endpoint."
        )


def scale_advisory(cfg: LakebenchConfig) -> str:
    """Return a warning if the configured scale is large for one host, else ''."""
    scale = cfg.architecture.workload.datagen.scale
    if scale <= LOCAL_SCALE_ADVISORY_MAX:
        return ""
    dims = get_dimensions(cfg.architecture.workload.schema_type, scale)
    return (
        f"Scale {scale} is ~{dims.approx_bronze_gb:.0f} GB of bronze on one host. "
        f"Local mode is sized for scale {LOCAL_SCALE_ADVISORY_MAX} and below; "
        "expect this to take a long time."
    )


def deploy_local(
    cfg: LakebenchConfig,
    workdir: Path | None = None,
    timeout: int = 180,
    cli: str = "",
) -> LocalDeployment:
    """Stand up the local stack and report what was skipped."""
    check_local_supported(cfg)

    workdir = workdir or default_workdir(cfg.name)
    buckets = (
        cfg.platform.storage.s3.buckets.bronze,
        cfg.platform.storage.s3.buckets.silver,
        cfg.platform.storage.s3.buckets.gold,
    )

    deployer = LocalDeployer(
        workdir=workdir,
        namespace=cfg.get_namespace(),
        region=cfg.platform.storage.s3.region,
        buckets=buckets,
        cli=cli,
    )

    print_info(f"Starting local stack in {workdir}")
    started = time.time()
    deployment = deployer.deploy(timeout=timeout)
    elapsed = time.time() - started

    print_success(f"Garage ready on {deployment.endpoint} in {elapsed:.1f}s")
    _print_skipped()
    return deployment


def _print_skipped() -> None:
    """Show what a Kubernetes deploy would have done and local mode does not."""
    table = Table(title="Not deployed locally", show_header=True, header_style="dim", box=None)
    table.add_column("Component", style="cyan")
    table.add_column("Reason", style="dim")
    for name, reason in SKIPPED_COMPONENTS:
        table.add_row(name, reason)
    console.print()
    console.print(table)


def run_local(
    cfg: LakebenchConfig,
    deployment: LocalDeployment,
    workdir: Path | None = None,
    timeout: int = 3600,
    stages: tuple[str, ...] = LOCAL_JOB_ORDER,
) -> LocalRunResult:
    """Run the batch pipeline locally, stage by stage.

    Stops at the first failure: silver cannot build on a bronze that did not
    verify, and continuing would bury the real error under a second one.
    """
    check_local_supported(cfg)
    workdir = workdir or default_workdir(cfg.name)

    spark_config = LocalSparkConfig(
        endpoint=deployment.endpoint,
        access_key=deployment.credentials.access_key,
        secret_key=deployment.credentials.secret_key,
        region=deployment.credentials.region,
        bronze_bucket=cfg.platform.storage.s3.buckets.bronze,
        silver_bucket=cfg.platform.storage.s3.buckets.silver,
        gold_bucket=cfg.platform.storage.s3.buckets.gold,
    )
    runner = LocalSparkRunner(spark_config, workdir=workdir, cli=deployment.runtime_cli)

    results: list[tuple[str, bool, float]] = []
    started = time.time()

    for stage in stages:
        console.print(f"[bold]Stage: {stage}[/bold]")
        outcome = runner.run_job(stage, timeout=timeout)
        results.append((stage, outcome.success, outcome.elapsed_seconds))

        if outcome.success:
            print_success(f"{stage} completed in {outcome.elapsed_seconds:.1f}s")
            continue

        print_error(f"{stage} failed after {outcome.elapsed_seconds:.1f}s")
        if outcome.error_message:
            console.print(f"  [dim]{outcome.error_message}[/dim]")
        break

    return LocalRunResult(
        success=all(ok for _, ok, _ in results) and len(results) == len(stages),
        elapsed_seconds=time.time() - started,
        stages=results,
    )


def print_local_summary(result: LocalRunResult) -> None:
    """Print the per-stage timing table after a local run."""
    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Stage", style="cyan")
    table.add_column("Result")
    table.add_column("Elapsed", justify="right")

    for name, ok, elapsed in result.stages:
        marker = "[green]ok[/green]" if ok else "[red]failed[/red]"
        table.add_row(name, marker, f"{elapsed:.1f}s")

    console.print()
    console.print(table)
    console.print()

    if result.success:
        console.print(
            Panel(
                f"[green]Pipeline completed in {result.elapsed_seconds:.1f}s[/green]",
                title="Local run",
                expand=False,
            )
        )
    else:
        console.print(
            Panel(
                f"[red]Pipeline failed at {result.failed_stage}[/red]",
                title="Local run",
                expand=False,
            )
        )


def print_local_plan(cfg: LakebenchConfig, workdir: Path) -> None:
    """Show what local mode will do, for --dry-run and the confirmation prompt."""
    scale = cfg.architecture.workload.datagen.scale
    dims = get_dimensions(cfg.architecture.workload.schema_type, scale)
    console.print(
        Panel(
            f"[bold]{cfg.name}[/bold]  ·  local mode\n"
            f"Workdir: {workdir}\n"
            f"Scale: {scale} (~{dims.approx_bronze_gb:.1f} GB bronze, "
            f"{dims.customers:,} customers)\n"
            f"Peak memory: ~{local_peak_memory_gb()} GB (jobs run sequentially)\n"
            f"Containers: Garage (object store) + Spark local[*] per stage",
            title="Local deploy",
            expand=False,
        )
    )
