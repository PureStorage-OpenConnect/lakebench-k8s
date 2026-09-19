"""Financial (FinServ-Crime, AML) operator subcommands.

Grouped under ``lakebench financial <verb>`` so the operator-facing
Financial ops surface stays discoverable via ``lakebench financial --help``
and out of the top-level command namespace.

Three verbs shipped in ENG-2C.3:

- ``replay``    -- W8 historical replay of a detection rule (SparkApplication).
- ``reproduce`` -- W10 time-travel reproduction of a specific alert.
- ``score``     -- Compute recall from datagen manifest + gold.alerts.

Each verb loads the config, resolves the schema-aware ComponentSpec for
its JobType, and submits it via the active runtime (K8s for cluster
deployments, ContainerRuntime for ``--local``).
"""

from __future__ import annotations

import logging
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
) -> None:
    """Rerun a detection rule against a historical Iceberg snapshot (W8)."""
    from lakebench.k8s.client import KubernetesClient
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    cfg = _load_config(config)
    k8s = KubernetesClient(context=None)
    mgr = SparkJobManager(cfg, k8s)

    # Default to the config's gold_alerts table, fully qualified with the
    # active catalog. score_financial reads exactly this table, so
    # defaulting here removes the "why is my recall 0.0" foot-gun of
    # writing to a different table than what score reads.
    if not output_alerts:
        catalog = cfg.architecture.query_engine.trino.catalog_name
        output_alerts = f"{catalog}.{cfg.architecture.tables.gold_alerts}"

    extra_args = [
        "--rule",
        rule,
        "--depth-months",
        str(depth_months),
        "--output-alerts",
        output_alerts,
    ]
    if threshold is not None:
        extra_args += ["--threshold", str(threshold)]

    console.print(f"[bold]lakebench financial replay[/bold] rule={rule} depth={depth_months}mo")
    mgr.submit(JobType.REPLAY_FINANCIAL, arguments=extra_args)


@financial_app.command("reproduce")
def reproduce(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    alert_id: Annotated[str, typer.Option(help="Alert id to reproduce")],
) -> None:
    """Reproduce a specific past alert via Iceberg time-travel (W10)."""
    from lakebench.k8s.client import KubernetesClient
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    cfg = _load_config(config)
    k8s = KubernetesClient(context=None)
    mgr = SparkJobManager(cfg, k8s)

    console.print(f"[bold]lakebench financial reproduce[/bold] alert_id={alert_id}")
    mgr.submit(JobType.REPRODUCE_FINANCIAL, arguments=["--alert-id", alert_id])


@financial_app.command("score")
def score(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    manifest: Annotated[str, typer.Option(help="S3 URI to datagen manifest.parquet")],
    output: Annotated[str, typer.Option(help="S3 URI for recall.parquet output")],
) -> None:
    """Compute recall from datagen manifest and gold.alerts."""
    from lakebench.k8s.client import KubernetesClient
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    cfg = _load_config(config)
    k8s = KubernetesClient(context=None)
    mgr = SparkJobManager(cfg, k8s)

    console.print("[bold]lakebench financial score[/bold]")
    mgr.submit(JobType.SCORE_FINANCIAL, arguments=["--manifest", manifest, "--output", output])
