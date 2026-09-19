"""Financial (FinServ-Crime, AML) operator subcommands.

Grouped under ``lakebench financial <verb>`` so the operator-facing
Financial ops surface stays discoverable via ``lakebench financial --help``
and out of the top-level command namespace.

Three verbs shipped in ENG-2C.3:

- ``replay``    -- W8 historical replay of a detection rule (SparkApplication).
- ``reproduce`` -- W10 time-travel reproduction of a specific alert.
- ``score``     -- Compute recall from datagen manifest + gold.alerts.

Each verb loads the config, gets the shared k8s client + SparkJobManager,
ensures the scripts ConfigMap is deployed (idempotent), then submits the
appropriate JobType with CLI --arguments passed through to the Python
script.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

logger = logging.getLogger(__name__)

financial_app = typer.Typer(
    name="financial",
    help="Financial (FinServ-Crime, AML) operator actions.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()


def _assert_financial_schema(cfg) -> None:
    from lakebench.config.schema import WorkloadSchema

    schema = cfg.architecture.workload.schema_type
    if schema != WorkloadSchema.FINANCIAL:
        raise typer.BadParameter(
            f"lakebench financial subcommands require workload.schema=financial "
            f"(config has schema={schema.value})."
        )


def _load_config(config_path: Path):
    from lakebench.config import load_config

    cfg = load_config(str(config_path))
    _assert_financial_schema(cfg)
    return cfg


def _get_job_manager(cfg):
    """Build k8s client + SparkJobManager + ensure scripts ConfigMap is
    up-to-date. Matches the pattern in cli/_run.py so replay/reproduce/score
    use the same script-mount path as bronze_verify/silver_build/gold_finalize.
    """
    from lakebench.engine import get_engine
    from lakebench.k8s import get_k8s_client

    k8s = get_k8s_client(
        context=cfg.platform.kubernetes.context,
        namespace=cfg.get_namespace(),
    )
    job_manager = get_engine(cfg, k8s)
    if not job_manager.deploy_scripts_configmap():
        raise typer.Exit("Failed to deploy Spark scripts ConfigMap")
    return job_manager


def _wait_for_sparkapp(namespace: str, name: str, timeout: int = 1800) -> str:
    """Poll a SparkApplication until it reaches a terminal state, return state."""
    from kubernetes import client
    from kubernetes.client.rest import ApiException

    api = client.CustomObjectsApi()
    start = time.time()
    last_state = ""
    while time.time() - start < timeout:
        try:
            obj = api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=name,
            )
        except ApiException as e:
            if e.status == 404:
                time.sleep(5)
                continue
            raise
        state = (obj.get("status", {}) or {}).get("applicationState", {}).get("state", "")
        if state != last_state:
            console.print(f"  [dim]{name}: {state}[/dim]")
            last_state = state
        if state in ("COMPLETED", "FAILED"):
            return state
        time.sleep(10)
    return "TIMEOUT"


@financial_app.command("replay")
def replay(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    rule: Annotated[str, typer.Option(help="Rule id, e.g. W2_structuring")],
    depth_months: Annotated[int, typer.Option(help="Snapshot depth in months")] = 60,
    threshold: Annotated[
        float | None, typer.Option(help="Rule-specific threshold override")
    ] = None,
    output_alerts: Annotated[
        str,
        typer.Option(
            help=(
                "Fully-qualified output alerts table (catalog.namespace.table). "
                "Defaults to the config's gold_alerts table so score_financial's "
                "join finds the rows without extra plumbing. Multiple rules can "
                "share the same table -- replay does DELETE WHERE rule_id=X "
                "before appending, so each rule owns its rows."
            ),
        ),
    ] = "",
    wait: Annotated[bool, typer.Option(help="Wait for job completion")] = True,
) -> None:
    """Rerun a detection rule against a historical Iceberg snapshot (W8)."""
    from lakebench.modules.pipeline_engines.spark.job import JobType

    cfg = _load_config(config)

    # Default target = config's gold_alerts, fully qualified with the active
    # catalog. score_financial reads exactly this table, so defaulting here
    # removes the "why is my recall 0" foot-gun of writing to a different
    # table than what score reads.
    if not output_alerts:
        catalog = cfg.architecture.query_engine.trino.catalog_name
        output_alerts = f"{catalog}.{cfg.architecture.tables.gold_alerts}"

    args = [
        "--rule", rule,
        "--depth-months", str(depth_months),
        "--output-alerts", output_alerts,
    ]
    if threshold is not None:
        args += ["--threshold", str(threshold)]

    console.print(
        f"[bold]lakebench financial replay[/bold] rule={rule} depth={depth_months}mo"
    )
    job_manager = _get_job_manager(cfg)
    status = job_manager.submit_job(JobType.REPLAY_FINANCIAL, arguments=args)
    console.print(f"  submitted: {status.message}")

    if wait:
        result = _wait_for_sparkapp(cfg.get_namespace(), "lakebench-replay-financial")
        console.print(f"[bold]replay result:[/bold] {result}")
        if result != "COMPLETED":
            raise typer.Exit(1)


@financial_app.command("reproduce")
def reproduce(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    alert_id: Annotated[str, typer.Option(help="Alert id to reproduce")],
    wait: Annotated[bool, typer.Option(help="Wait for job completion")] = True,
) -> None:
    """Reproduce a specific past alert via Iceberg time-travel (W10)."""
    from lakebench.modules.pipeline_engines.spark.job import JobType

    cfg = _load_config(config)
    console.print(f"[bold]lakebench financial reproduce[/bold] alert_id={alert_id}")
    job_manager = _get_job_manager(cfg)
    status = job_manager.submit_job(
        JobType.REPRODUCE_FINANCIAL, arguments=["--alert-id", alert_id],
    )
    console.print(f"  submitted: {status.message}")

    if wait:
        result = _wait_for_sparkapp(cfg.get_namespace(), "lakebench-reproduce-financial")
        console.print(f"[bold]reproduce result:[/bold] {result}")
        if result != "COMPLETED":
            raise typer.Exit(1)


@financial_app.command("score")
def score(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    manifest: Annotated[str, typer.Option(help="S3 URI to datagen manifest.parquet")],
    output: Annotated[str, typer.Option(help="S3 URI for recall.parquet output")],
    wait: Annotated[bool, typer.Option(help="Wait for job completion")] = True,
) -> None:
    """Compute recall from datagen manifest and gold.alerts."""
    from lakebench.modules.pipeline_engines.spark.job import JobType

    cfg = _load_config(config)
    console.print("[bold]lakebench financial score[/bold]")
    job_manager = _get_job_manager(cfg)
    status = job_manager.submit_job(
        JobType.SCORE_FINANCIAL,
        arguments=["--manifest", manifest, "--output", output],
    )
    console.print(f"  submitted: {status.message}")

    if wait:
        result = _wait_for_sparkapp(cfg.get_namespace(), "lakebench-score-financial")
        console.print(f"[bold]score result:[/bold] {result}")
        if result != "COMPLETED":
            raise typer.Exit(1)
