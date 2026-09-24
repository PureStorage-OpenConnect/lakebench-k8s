"""Clean command implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel

from lakebench._constants import DEFAULT_OUTPUT_DIR
from lakebench.cli._helpers import (
    DEPRECATED_SHORT_F_HELP,
    _journal_safe,
    console,
    deprecated_short_f_force,
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
            "--yes",
            "-y",
            help="Skip confirmation prompt",
        ),
    ] = False,
    force_short_f: Annotated[
        bool,
        typer.Option("-f", hidden=True, help=DEPRECATED_SHORT_F_HELP),
    ] = False,
    force_legacy: Annotated[
        bool,
        typer.Option(
            "--force-legacy",
            help=(
                "Clean a bucket that has no lakebench ownership tag "
                "(legacy). Caution: another team's data may live in an "
                "untagged bucket. Refuses always on foreign-tagged "
                "buckets regardless of this flag."
            ),
        ),
    ] = False,
    allow_unverified_cluster: Annotated[
        bool,
        typer.Option(
            "--allow-unverified-cluster",
            help=(
                "Proceed when the kubeconfig cannot prove which cluster it "
                "points at (no CA data for the api-server fingerprint). Same "
                "meaning as on destroy."
            ),
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
    if force_short_f:
        force = deprecated_short_f_force("--force or -y", force)
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

    # Check for writers still active before cleaning S3 buckets. The prompt
    # sits outside the try: typer.confirm(abort=True) raises click.Abort,
    # which subclasses RuntimeError, so inside `except Exception` a "no"
    # was swallowed and the clean went ahead.
    # Ownership gate, shared with destroy (check_data_ownership): clean must
    # not be a bypass. Load the configured kube context first; every K8s call
    # below (including the active-writer check) depends on it.
    if bucket_targets:
        from lakebench.deploy.ownership import (
            IdentityVerdict,
            build_identity_from_config,
            check_data_ownership,
            verify_namespace_identity,
        )

        ns = cfg.get_namespace()
        kube_ctx = cfg.platform.kubernetes.context or ""
        core_v1 = None
        ns_present = False
        ns_verified = False
        try:
            from kubernetes import client as _k8s
            from kubernetes.client.rest import ApiException

            from lakebench.k8s import get_k8s_client

            get_k8s_client(context=kube_ctx, namespace=ns)
            core_v1 = _k8s.CoreV1Api()
            try:
                core_v1.read_namespace(ns)
                ns_present = True
            except ApiException as e:
                if e.status != 404:
                    raise
            if ns_present:
                identity = build_identity_from_config(cfg, context=kube_ctx)
                v = verify_namespace_identity(
                    core_v1,
                    ns,
                    identity.name,
                    identity.api_server,
                    allow_unverified_cluster=allow_unverified_cluster,
                )
                if v.verdict is IdentityVerdict.MISMATCH:
                    print_error(f"Refusing to clean: {v.hint}")
                    raise typer.Exit(1)
                ns_verified = v.verdict is IdentityVerdict.MATCH or (
                    v.verdict is IdentityVerdict.ABSENT and force_legacy
                )
        except typer.Exit:
            raise
        except Exception as e:  # noqa: BLE001
            print_warning(f"Could not reach the cluster to verify ownership: {e}")
            core_v1 = None
        decision = check_data_ownership(
            core_v1,
            namespace=ns,
            deployment_name=cfg.name,
            namespace_present=ns_present,
            namespace_verified=ns_verified,
            force_legacy=force_legacy,
            context_name=kube_ctx,
        )
        if not decision.allowed:
            print_error(decision.hint)
            errors.append(decision.hint)
            bucket_targets = {}
        elif decision.hint:
            print_warning(decision.hint)

    if bucket_targets:
        active_writers: list[str] = []
        try:
            from kubernetes import client as k8s_client

            ns = cfg.get_namespace()
            try:
                job = k8s_client.BatchV1Api().read_namespaced_job("lakebench-datagen", ns)
                active_pods = job.status.active or 0
                if active_pods > 0:
                    active_writers.append(f"datagen job ({active_pods} active pod(s))")
            except Exception:
                pass  # no datagen job
            try:
                apps = k8s_client.CustomObjectsApi().list_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=ns,
                    plural="sparkapplications",
                )
                for app in apps.get("items", []):
                    state = ((app.get("status") or {}).get("applicationState") or {}).get(
                        "state", ""
                    )
                    # SUBMISSION_FAILED is not final: the operator resubmits
                    # it (restartPolicy), so the app may start writing.
                    if state not in ("COMPLETED", "FAILED"):
                        active_writers.append(
                            f"Spark application {app['metadata']['name']} ({state or 'pending'})"
                        )
            except Exception:
                pass  # no Spark Operator CRD or K8s unavailable
        except Exception:
            pass  # K8s client unavailable -- nothing to check
        if active_writers:
            for w in active_writers:
                print_warning(f"Still writing: {w}")
            if not force:
                typer.confirm("Jobs are still writing to these buckets. Clean anyway?", abort=True)

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
            # F1-A: bail on S3 init failure with a clean refusal.
            # Without this guard, `raw_client` returns None and every
            # verify_bucket_ownership call raises AttributeError.
            if s3._init_error:
                print_error(f"S3 client init failed: {s3._init_error}")
                raise typer.Exit(1)

            def _clean_progress(bkt: str, count: int) -> None:
                console.print(f"  Deleting from s3://{bkt}/... ({count:,} objects so far)")

            # F-1: ownership check per bucket before touching contents.
            # Foreign-tagged buckets always refuse; legacy (untagged)
            # buckets need --force-legacy. This mirrors the deploy and
            # destroy paths so `clean` cannot be used as a bypass.
            from lakebench.deploy.ownership import (
                IdentityVerdict,
                bucket_name_matches_deployment,
                list_lakebench_deployment_names,
                verify_bucket_ownership,
            )

            # Backends without bucket tagging (FlashBlade) report every bucket
            # as UNSUPPORTED. Fall back to the same longest-prefix name check
            # destroy uses, which needs the other deployments' names. None
            # means the enumeration failed, and the fallback then refuses.
            other_deployments: list[str] | None
            try:
                from kubernetes import client as _k8s

                from lakebench.k8s import get_k8s_client

                get_k8s_client(
                    context=cfg.platform.kubernetes.context or "",
                    namespace=cfg.get_namespace(),
                )
                other_deployments = list_lakebench_deployment_names(
                    _k8s.CoreV1Api(), exclude=cfg.get_namespace()
                )
            except Exception:
                other_deployments = None

            for layer, bucket in bucket_targets.items():
                try:
                    v = verify_bucket_ownership(s3.raw_client, bucket, cfg.name)
                    if v.verdict is IdentityVerdict.MISMATCH:
                        errors.append(f"{layer}: {v.hint}")
                        print_error(
                            f"Refusing to clean {layer}: bucket "
                            f"{bucket!r} is owned by another lakebench "
                            f"deployment. {v.hint}"
                        )
                        continue
                    if v.verdict is IdentityVerdict.ABSENT and not force_legacy:
                        errors.append(f"{layer}: legacy untagged bucket, --force-legacy required")
                        print_error(
                            f"Refusing to clean {layer}: bucket "
                            f"{bucket!r} has no lakebench ownership tag. "
                            "Pass --force-legacy to clean (caution: this "
                            "may collide with another team's data)."
                        )
                        continue
                    if v.verdict is IdentityVerdict.NOT_FOUND:
                        print_info(f"{layer}: bucket {bucket!r} does not exist")
                        continue
                    if v.verdict is IdentityVerdict.UNSUPPORTED:
                        prefix_ok = other_deployments is not None and (
                            bucket_name_matches_deployment(bucket, cfg.name, other_deployments)
                        )
                        if not prefix_ok and not force_legacy:
                            reason = (
                                "could not list other lakebench deployments to check name ownership"
                                if other_deployments is None
                                else "the bucket name does not match this deployment, "
                                "or another deployment has a longer-prefix claim"
                            )
                            errors.append(f"{layer}: ownership unverifiable ({reason})")
                            print_error(
                                f"Refusing to clean {layer}: bucket {bucket!r} is on a "
                                f"backend without bucket tagging and {reason}. Pass "
                                "--force-legacy only if you have confirmed it is yours."
                            )
                            continue

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
