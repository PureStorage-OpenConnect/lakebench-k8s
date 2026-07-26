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

import logging
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from rich.panel import Panel
from rich.table import Table

from lakebench.cli._helpers import console, print_error, print_info, print_success
from lakebench.config.scale import get_dimensions
from lakebench.config.schema import LakebenchConfig
from lakebench.deploy.garage import DEFAULT_S3_PORT as _DEFAULT_S3_PORT
from lakebench.deploy.garage import GarageCredentials
from lakebench.deploy.local import SKIPPED_COMPONENTS, LocalDeployer, LocalDeployment
from lakebench.modules.pipeline_engines.spark.job import (
    LOCAL_SCALE_ADVISORY_MAX,
    local_peak_memory_gb,
)
from lakebench.modules.pipeline_engines.spark.local_job import (
    LocalSparkConfig,
    LocalSparkRunner,
)

logger = logging.getLogger(__name__)

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


def destroy_local(
    cfg: LakebenchConfig,
    workdir: Path | None = None,
    remove_data: bool = False,
    cli: str = "",
) -> tuple[int, Path]:
    """Tear down the local stack.

    Returns (containers removed, workdir). Data is kept unless ``remove_data``
    is set: the Ivy cache alone is ~1.2 GB and re-downloading it dominates the
    next cold run, and generated data can represent hours of work.
    """
    workdir = workdir or default_workdir(cfg.name)
    deployer = LocalDeployer(
        workdir=workdir,
        namespace=cfg.get_namespace(),
        region=cfg.platform.storage.s3.region,
        cli=cli,
    )
    return deployer.destroy(remove_data=remove_data), workdir


def status_local(
    cfg: LakebenchConfig,
    workdir: Path | None = None,
    cli: str = "",
) -> dict[str, object]:
    """Report what is running locally and what data exists.

    Never raises for a stack that is simply not deployed -- "nothing running"
    is a legitimate answer to a status question, not an error.
    """
    workdir = workdir or default_workdir(cfg.name)
    s3 = cfg.platform.storage.s3
    info: dict[str, object] = {
        "workdir": workdir,
        "workdir_exists": workdir.exists(),
        "running": [],
        "endpoint": "",
        "buckets": {},
        "error": "",
    }

    try:
        deployer = LocalDeployer(
            workdir=workdir,
            namespace=cfg.get_namespace(),
            region=s3.region,
            cli=cli,
        )
        info["running"] = deployer.status()
    except Exception as e:  # noqa: BLE001 -- a missing container CLI is a status answer
        info["error"] = str(e)
        return info

    if not info["running"]:
        return info

    info["endpoint"] = f"http://localhost:{_DEFAULT_S3_PORT}"
    creds = _running_credentials(cfg, workdir, cli)
    if creds:
        info["buckets"] = _measure_layers(cfg, creds)
    return info


def _running_credentials(cfg: LakebenchConfig, workdir: Path, cli: str) -> GarageCredentials | None:
    """Read credentials back from a running Garage without redeploying it.

    ``deploy_local`` would also return them, but calling it from ``status``
    would start containers as a side effect of asking a question.
    """
    from lakebench.deploy.garage import GarageDeployer
    from lakebench.runtime.container import ContainerRuntime, detect_container_cli

    try:
        runtime = ContainerRuntime(cli=cli or detect_container_cli(), namespace=cfg.get_namespace())
        deployer = GarageDeployer(
            runtime,
            config_dir=str(workdir / "garage"),
            region=cfg.platform.storage.s3.region,
        )
        return deployer.read_credentials()
    except Exception as e:  # noqa: BLE001
        logger.debug("Could not read local credentials: %s", e)
        return None


def _measure_layers(cfg: LakebenchConfig, creds: GarageCredentials) -> dict[str, dict[str, float]]:
    """Size each medallion layer, accounting for the shared warehouse root."""
    from lakebench.modules.pipeline_engines.spark.local_job import local_layer_prefix
    from lakebench.s3 import S3Client

    buckets = {
        "bronze": cfg.platform.storage.s3.buckets.bronze,
        "silver": cfg.platform.storage.s3.buckets.silver,
        "gold": cfg.platform.storage.s3.buckets.gold,
    }
    client = S3Client(
        endpoint=creds.endpoint,
        access_key=creds.access_key,
        secret_key=creds.secret_key,
        region=creds.region,
        path_style=True,
    )

    measured: dict[str, dict[str, float]] = {}
    for layer in ("bronze", "silver", "gold"):
        bucket_layer, prefix = local_layer_prefix(layer)
        try:
            info = client.get_bucket_size(buckets[bucket_layer], prefix=prefix)
            measured[layer] = {
                "objects": info.object_count or 0,
                "size_gb": (info.size_bytes or 0) / (1024**3),
            }
        except Exception as e:  # noqa: BLE001
            logger.debug("Could not measure %s: %s", layer, e)
            measured[layer] = {"objects": 0, "size_gb": 0.0}
    return measured


def print_local_status(info: dict[str, object]) -> None:
    """Render what status_local() found."""
    if info["error"]:
        print_error(str(info["error"]))
        return

    running = info["running"]
    if not running:
        console.print()
        console.print("[yellow]Nothing running locally.[/yellow]")
        if info["workdir_exists"]:
            console.print(f"  Workdir still present: {info['workdir']}")
            console.print("  [dim]Data is kept across destroy unless --remove-data.[/dim]")
        console.print("  [dim]Start it with: lakebench deploy <config> --local[/dim]")
        console.print()
        return

    console.print()
    console.print(
        Panel(
            f"[green]Running:[/green] {', '.join(str(r) for r in running)}\n"
            f"Endpoint: {info['endpoint']}\n"
            f"Workdir:  {info['workdir']}",
            title="Local status",
            expand=False,
        )
    )

    buckets = info["buckets"]
    if not buckets:
        return

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Layer", style="cyan")
    table.add_column("Objects", justify="right")
    table.add_column("Size", justify="right")
    for layer in ("bronze", "silver", "gold"):
        row = buckets.get(layer, {})
        table.add_row(
            layer,
            f"{int(row.get('objects', 0)):,}",
            f"{row.get('size_gb', 0.0):.3f} GB",
        )
    console.print()
    console.print(table)
    console.print()


def generate_local(
    cfg: LakebenchConfig,
    deployment: LocalDeployment,
    timeout: int = 3600,
    replace: bool = True,
) -> bool:
    """Generate bronze data with the datagen image, against the local store.

    Uses the same published image as the Kubernetes path, so local data has the
    schema the pipeline scripts expect rather than something approximated here.

    Bronze is emptied first by default. Datagen writes part-NNNNNN keys that do
    not collide across runs, so without this a second ``--generate`` adds to the
    first rather than replacing it, and the pipeline silently processes a
    multiple of the configured scale. That surfaces as an OOM in silver-build,
    which points at the sizing rather than at the real cause.
    """
    if replace and not _empty_bronze(cfg, deployment):
        return False

    dims = get_dimensions(
        cfg.architecture.workload.schema_type, cfg.architecture.workload.datagen.scale
    )
    target_tb = max(dims.approx_bronze_gb / 1024.0, 0.0001)
    datagen = cfg.architecture.workload.datagen

    cmd = [
        deployment.runtime_cli,
        "run",
        "--rm",
        "--network",
        "host",
        "-e",
        f"S3_ENDPOINT={deployment.endpoint}",
        # The image reads AWS_* names, not S3_ACCESS_KEY.
        "-e",
        f"AWS_ACCESS_KEY_ID={deployment.credentials.access_key}",
        "-e",
        f"AWS_SECRET_ACCESS_KEY={deployment.credentials.secret_key}",
        "-e",
        f"AWS_REGION={deployment.credentials.region}",
        cfg.images.datagen,
        "--bucket",
        cfg.platform.storage.s3.buckets.bronze,
        "--target-tb",
        f"{target_tb:.6f}",
        # Small files locally: one 512 MB part would be most of the dataset and
        # would leave Spark a single partition to work with.
        "--file-size-mb",
        "16",
        "--mode",
        "batch",
    ]
    if datagen.timestamp_start:
        cmd += ["--timestamp-start", str(datagen.timestamp_start)]
    if datagen.timestamp_end:
        cmd += ["--timestamp-end", str(datagen.timestamp_end)]

    print_info(
        f"Generating ~{dims.approx_bronze_gb:.1f} GB into {cfg.platform.storage.s3.buckets.bronze}"
    )
    started = time.time()
    proc = subprocess.run(  # noqa: S603
        cmd, capture_output=True, text=True, timeout=timeout, check=False
    )
    elapsed = time.time() - started

    if proc.returncode != 0:
        output = (proc.stdout or "") + (proc.stderr or "")
        print_error(f"Datagen failed after {elapsed:.1f}s")
        for line in output.strip().splitlines()[-3:]:
            console.print(f"  [dim]{line[:200]}[/dim]")
        return False

    print_success(f"Data generated in {elapsed:.1f}s")
    return True


def benchmark_local(
    cfg: LakebenchConfig,
    deployment: LocalDeployment,
    workdir: Path | None = None,
    timeout: int = 300,
) -> tuple[list[tuple[str, bool, float]], float]:
    """Run the query benchmark locally with DuckDB.

    Returns (per-query results, queries per hour). QpH uses the same definition
    as the cluster path -- 3600 / mean successful query time * query count --
    so a local number is comparable in kind, though obviously not in magnitude.
    """
    from lakebench.benchmark import BENCHMARK_QUERIES
    from lakebench.modules.pipeline_engines.spark.local_job import LOCAL_WAREHOUSE_LAYER
    from lakebench.modules.query_engines.duckdb.local_executor import LocalDuckDBExecutor

    workdir = workdir or default_workdir(cfg.name)
    duckdb_dir = workdir / "duckdb"
    duckdb_dir.mkdir(parents=True, exist_ok=True)
    duckdb_dir.chmod(0o777)

    buckets = cfg.platform.storage.s3.buckets
    executor = LocalDuckDBExecutor(
        endpoint=deployment.endpoint,
        access_key=deployment.credentials.access_key,
        secret_key=deployment.credentials.secret_key,
        warehouse_bucket=getattr(buckets, LOCAL_WAREHOUSE_LAYER),
        region=deployment.credentials.region,
        table_names={
            "silver": "silver.customer_interactions_enriched",
            "gold": "gold.customer_executive_dashboard",
        },
        cli=deployment.runtime_cli,
        workdir=str(duckdb_dir),
    )

    print_info("Warming DuckDB (first query installs extensions)")
    if not executor.health_check():
        print_error("DuckDB could not start; skipping benchmark")
        return [], 0.0

    silver_table = "silver.customer_interactions_enriched"
    gold_table = "gold.customer_executive_dashboard"

    results: list[tuple[str, bool, float]] = []
    durations: list[float] = []
    for query in BENCHMARK_QUERIES:
        # Queries are templates: substitute the table names first, then let the
        # executor rewrite them to iceberg_scan paths. Skipping the format step
        # leaves literal "{catalog}" in the SQL and DuckDB fails on the brace.
        sql = query.sql.format(
            catalog=executor.catalog_name,
            silver_table=silver_table,
            gold_table=gold_table,
        )
        outcome = executor.execute_query(executor.adapt_query(sql), timeout=timeout)
        results.append((query.name, outcome.success, outcome.duration_seconds))
        if outcome.success:
            durations.append(outcome.duration_seconds)
            print_success(f"{query.name} ({outcome.duration_seconds:.2f}s)")
        else:
            print_error(f"{query.name}: {outcome.error}")

    qph = 0.0
    if durations:
        qph = 3600.0 / (sum(durations) / len(durations))
    return results, qph


def print_local_benchmark(results: list[tuple[str, bool, float]], qph: float) -> None:
    """Render the local benchmark results."""
    if not results:
        return

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Query", style="cyan")
    table.add_column("Result")
    table.add_column("Elapsed", justify="right")
    for name, ok, elapsed in results:
        marker = "[green]ok[/green]" if ok else "[red]failed[/red]"
        table.add_row(name, marker, f"{elapsed:.2f}s")

    passed = sum(1 for _, ok, _ in results if ok)
    console.print()
    console.print(table)
    console.print()
    console.print(
        Panel(
            f"{passed}/{len(results)} queries passed\n"
            f"[bold]{qph:,.0f} QpH[/bold] (queries per hour, single host)",
            title="Local benchmark",
            expand=False,
        )
    )


def _empty_bronze(cfg: LakebenchConfig, deployment: LocalDeployment) -> bool:
    """Clear the bronze bucket before regenerating into it."""
    from lakebench.s3 import S3Client

    bucket = cfg.platform.storage.s3.buckets.bronze
    try:
        client = S3Client(
            endpoint=deployment.endpoint,
            access_key=deployment.credentials.access_key,
            secret_key=deployment.credentials.secret_key,
            region=deployment.credentials.region,
            path_style=True,
        )
        existing = client.get_bucket_size(bucket)
        if existing.object_count:
            print_info(f"Clearing {existing.object_count:,} existing objects from {bucket}")
            client.empty_bucket(bucket)
        return True
    except Exception as e:  # noqa: BLE001
        print_error(f"Could not clear {bucket}: {e}")
        return False


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
