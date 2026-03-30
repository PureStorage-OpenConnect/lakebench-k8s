"""Clean command implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel

from lakebench._constants import DEFAULT_OUTPUT_DIR
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
)
from lakebench.journal import DEFAULT_JOURNAL_DIR, CommandName, EventType, Journal

# Valid clean targets
CLEAN_TARGETS = ["bronze", "silver", "gold", "data", "metrics", "journal"]


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
                ca_cert=s3_cfg.ca_cert,
                verify_ssl=s3_cfg.verify_ssl,
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
