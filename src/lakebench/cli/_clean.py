"""Clean command implementation."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel

from lakebench.cli._helpers import (
    DEPRECATED_SHORT_F_HELP,
    _journal_safe,
    console,
    deprecated_short_f_force,
    esc,
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
    LoadPurpose,
    load_config,
)
from lakebench.exit_codes import ExitCode
from lakebench.journal import CommandName, EventType
from lakebench.k8s.target import ContextConflictError

# Valid clean targets. bronze, data, metrics and journal are refused
# (cli/_aliases.REFUSED): a run regenerates its own corpus, and evidence is
# not deleted by the CLI.
CLEAN_TARGETS = ["silver", "gold"]


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
) -> None:
    """Delete data without destroying infrastructure.

    Granular data cleanup for re-running pipeline stages.

    Targets:
      silver  - Empty the silver S3 bucket
      gold    - Empty the gold S3 bucket

    bronze and data are refused: `lakebench run CONFIG --generate
    --regenerate` regenerates the corpus. metrics and journal are refused:
    evidence is not deleted by the CLI.
    """
    if force_short_f:
        force = deprecated_short_f_force("--force or -y", force)
    target = target.lower().strip()
    from lakebench.cli._aliases import REFUSED, refusal

    if f"clean {target}" in REFUSED:
        # Before the config is read: nothing the caller passed is echoed.
        raise refusal(f"clean {target}")
    if target not in CLEAN_TARGETS:
        print_error(f"Invalid target: '{target}'. Must be one of: {', '.join(CLEAN_TARGETS)}")
        raise typer.Exit(ExitCode.USAGE)

    config_file = resolve_config_path(config_file, file_option)

    # Load configuration
    try:
        cfg = load_config(
            config_file, purpose=LoadPurpose.MUTATE, allow_long_names=True
        )  # LB-153: cleanup path
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

    s3_cfg = cfg.platform.storage.s3

    # The bucket to clean
    bucket_targets = {target: getattr(s3_cfg.buckets, target)}

    # Build description of what will be cleaned
    descriptions = []
    if bucket_targets:
        for layer, bucket in bucket_targets.items():
            descriptions.append(f"  - {layer}: s3://{bucket}/ (all objects)")

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
            raise typer.Exit(ExitCode.NOT_CONFIRMED)

    console.print(Panel(f"Cleaning: [bold]{esc(target)}[/bold]", expand=False))

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.CLEAN, {"target": target})

    total_deleted = 0
    errors = []
    # How many of ``errors`` are ownership refusals: when all are, clean
    # exits 3 (refused), not 1, as the exit-code table says.
    refusals = 0

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
                    raise typer.Exit(ExitCode.REFUSED)
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
            # Could not check (cluster unreachable, namespace list unreadable)
            # is not a refusal.
            if core_v1 is not None and not decision.unverifiable:
                refusals += 1
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
                raise typer.Exit(ExitCode.PREREQUISITE)

            def _clean_progress(bkt: str, count: int) -> None:
                console.print(f"  Deleting from s3://{esc(bkt)}/... ({count:,} objects so far)")

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
            except ContextConflictError:
                raise
            except Exception:
                other_deployments = None

            # This cluster's stamp, and the namespace's record of the
            # buckets it created or adopted while empty.
            from lakebench.deploy.ownership import (
                api_server_fingerprint,
                read_created_buckets,
            )

            my_cluster = api_server_fingerprint(cfg.platform.kubernetes.context or "")
            try:
                from kubernetes import client as _k8s_record

                _core = _k8s_record.CoreV1Api()
                # The created record only (not 1.6's adopted-empty one).
                ns_record = read_created_buckets(_core, cfg.get_namespace())
            except Exception:  # noqa: BLE001 -- unreadable: nothing is proven by it
                ns_record = set()

            for layer, bucket in bucket_targets.items():
                try:
                    v = verify_bucket_ownership(
                        s3.raw_client,
                        bucket,
                        cfg.name,
                        expected_cluster=my_cluster,
                        created_record=ns_record,
                    )
                    if v.verdict is IdentityVerdict.LEGACY_PROVEN:
                        # Row 3: a tagged one is ours as a MATCH is; a
                        # tagless one takes the record branch below.
                        v = dataclasses.replace(
                            v,
                            verdict=(
                                IdentityVerdict.MATCH if v.tagged else IdentityVerdict.UNSUPPORTED
                            ),
                        )
                    if v.verdict in (
                        IdentityVerdict.FOREIGN_CLUSTER,
                        IdentityVerdict.LEGACY_UNPROVEN,
                        IdentityVerdict.UNVERIFIED_CLUSTER,
                    ):
                        errors.append(f"{layer}: {v.hint}")
                        refusals += 1
                        print_error(f"Refusing to clean {layer}: {v.hint}")
                        continue
                    if v.verdict is IdentityVerdict.MISMATCH:
                        errors.append(f"{layer}: {v.hint}")
                        refusals += 1
                        print_error(
                            f"Refusing to clean {layer}: bucket "
                            f"{bucket!r} is owned by another lakebench "
                            f"deployment. {v.hint}"
                        )
                        continue
                    if v.verdict is IdentityVerdict.ABSENT and not force_legacy:
                        errors.append(f"{layer}: legacy untagged bucket, --force-legacy required")
                        refusals += 1
                        print_error(
                            f"Refusing to clean {layer}: bucket "
                            f"{bucket!r} has no lakebench ownership tag. "
                            "FIRST verify your cluster context: run "
                            "`oc whoami && kubectl config current-context` "
                            "and confirm the output matches this "
                            "deployment's expected cluster. Only after "
                            "that check, if you have confirmed this is "
                            "yours, pass --force-legacy to clean as a "
                            "last resort (caution: this may collide with "
                            "another team's data)."
                        )
                        continue
                    if v.verdict is IdentityVerdict.NOT_FOUND:
                        print_info(f"{layer}: bucket {bucket!r} does not exist")
                        continue
                    if v.verdict is IdentityVerdict.UNSUPPORTED:
                        prefix_ok = other_deployments is not None and (
                            bucket_name_matches_deployment(bucket, cfg.name, other_deployments)
                        )
                        # The name is not proof: deploy adopts a pre-existing
                        # matching bucket on these backends. Same rule as
                        # destroy: the namespace must record that lakebench
                        # created it or adopted it empty.
                        if prefix_ok and not force_legacy:
                            try:
                                from kubernetes import client as _k8s_rec

                                from lakebench.deploy.ownership import (
                                    tagless_contents_are_ours,
                                )

                                recorded = tagless_contents_are_ours(
                                    _k8s_rec.CoreV1Api(), cfg.get_namespace(), bucket
                                )
                            except Exception:  # noqa: BLE001
                                recorded = False
                            if not recorded:
                                errors.append(f"{layer}: not recorded as created or adopted empty")
                                refusals += 1
                                print_error(
                                    f"Refusing to clean {layer}: bucket {bucket!r} is on a "
                                    "backend without bucket tagging and this deployment's "
                                    "namespace does not record creating it or adopting it "
                                    "empty, so its data may not be lakebench's. Pass "
                                    "--force-legacy only if you have confirmed it is yours."
                                )
                                continue
                        if not prefix_ok and not force_legacy:
                            reason = (
                                "could not list other lakebench deployments to check name ownership"
                                if other_deployments is None
                                else "the bucket name does not match this deployment, "
                                "or another deployment has a longer-prefix claim"
                            )
                            errors.append(f"{layer}: ownership unverifiable ({reason})")
                            # A sibling list that could not be read is a
                            # permission gap, not a refusal.
                            if other_deployments is not None:
                                refusals += 1
                            print_error(
                                f"Refusing to clean {layer}: bucket {bucket!r} is on a "
                                f"backend without bucket tagging and {reason}. Pass "
                                "--force-legacy only if you have confirmed it is yours."
                            )
                            continue

                    # The layer's catalog entries go first, while their
                    # metadata is still there: left behind, the next run met
                    # tables whose files were gone and failed on them.
                    if not _unregister_before_empty(cfg, layer, bucket, errors):
                        print_error(
                            f"Not emptying {layer}: its tables could not all be unregistered "
                            "(see above); re-run clean once they can"
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
                + "\n".join(f"  - {esc(e)}" for e in errors),
                title="Clean Incomplete",
                expand=False,
            )
        )
        raise typer.Exit(ExitCode.REFUSED if refusals == len(errors) else ExitCode.FAILED)


def _unregister_before_empty(cfg, layer: str, bucket: str, errors: list[str]) -> bool:
    """Unregister ``layer``'s tables before its bucket is emptied; report.
    Returns whether the bucket may be emptied.

    A table still registered with its files in place is an error and keeps
    the bucket, so a re-run can finish (emptying it first left an entry no
    engine could drop). An entry whose files are already gone is an error
    too, but the bucket is emptied. No engine pod is a warning, as before
    this step existed.
    """
    try:
        from lakebench.deploy.unregister import unregister_layer_tables
        from lakebench.k8s import get_k8s_client

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context or "", namespace=cfg.get_namespace()
        )
        res = unregister_layer_tables(cfg, layer, bucket, k8s)
    except Exception as e:  # noqa: BLE001
        errors.append(f"{layer}: could not unregister its tables ({e})")
        print_error(f"{layer}: could not unregister its tables before emptying: {e}")
        return False
    if res.skipped:
        print_warning(
            f"{layer}: {res.skipped}, so its tables stay registered with no files; "
            "the next run may fail on them"
        )
    for table in res.unregistered:
        print_info(f"{layer}: unregistered {table}")
    for table, why in res.kept:
        print_info(f"{layer}: kept {table} registered: {why}")
    for table, why in res.failed:
        errors.append(f"{layer}: {table} still registered ({why})")
        print_error(f"{layer}: could not unregister {table}: {why}")
    for table, why in res.stuck:
        errors.append(f"{layer}: {table} still registered with its files gone ({why})")
        print_error(
            f"{layer}: {table} is registered but its files are already gone, and the "
            f"engine could not drop it ({why}); remove the entry with Trino's "
            "CALL <catalog>.system.unregister_table"
        )
    return res.may_empty
