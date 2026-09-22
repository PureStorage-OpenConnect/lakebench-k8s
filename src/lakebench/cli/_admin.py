"""``lakebench admin`` subcommand tree.

Admin commands mutate Category 3 (installed operators) and Category 4
(shared mutable state) resources -- things the ordinary ``deploy`` and
``destroy`` paths refuse to touch. A cluster admin runs these once;
developers run ``deploy`` many times without them.

Every mutating admin command acquires the cluster-wide
``lakebench-cluster-lock`` lease so concurrent invocations across
different workstations cannot race the same Helm upgrade or the same
cluster-scoped resource rename. Non-mutating commands (``status``,
``doctor``) do not take the lease.

See ``dev-artifacts/DESIGN-namespace-isolation.md`` for the design
rationale and the commit-sequence context.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel
from rich.table import Table

from lakebench.cli._helpers import (
    console,
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

logger = logging.getLogger(__name__)


admin_app = typer.Typer(
    name="admin",
    help="Cluster-admin operations for shared operator installs and lease recovery.",
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_core_v1():
    """Return a kubernetes CoreV1Api, printing a clean refusal on failure."""
    try:
        from kubernetes import client as k8s_client
        from kubernetes import config as k8s_config

        try:
            k8s_config.load_kube_config()
        except Exception:  # noqa: BLE001
            k8s_config.load_incluster_config()
        return k8s_client.CoreV1Api()
    except Exception as e:  # noqa: BLE001
        print_error(f"cannot open Kubernetes client: {e}")
        raise typer.Exit(1) from e


def _load_cfg(config_file: Path | None, file_option: Path | None):
    """Load a lakebench config with the same error path other commands use."""
    path = resolve_config_path(config_file, file_option)
    try:
        return load_config(path)
    except ConfigFileNotFoundError as e:
        print_error(f"File not found: {e}")
        raise typer.Exit(1) from e
    except ConfigValidationError as e:
        print_error("Config validation failed:")
        for err in e.errors:
            loc = ".".join(str(x) for x in err["loc"])
            console.print(f"  [red]*[/red] {loc}: {err['msg']}")
        raise typer.Exit(1) from e
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1) from e


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@admin_app.command("status")
def status() -> None:
    """Show installed operators, lease state, and lakebench-annotated namespaces."""
    from lakebench.deploy.cluster_lock import LOCK_NAMESPACE, read_cluster_lock
    from lakebench.deploy.ownership import ANNOTATION_DEPLOYMENT_NAME

    core_v1 = _get_core_v1()

    # Lease state.
    console.print(Panel("Cluster lease", expand=False))
    try:
        state = read_cluster_lock(core_v1)
    except Exception as e:  # noqa: BLE001
        print_warning(f"cannot read lease: {e}")
        state = None
    if state is None:
        print_info(f"No lease held (namespace {LOCK_NAMESPACE} may not exist yet)")
    else:
        expired = "EXPIRED" if state.is_expired() else "active"
        console.print(
            f"  holder:       {state.holder}\n"
            f"  acquired-at:  {state.acquired_at}\n"
            f"  ttl-seconds:  {state.ttl_seconds}\n"
            f"  status:       {expired}"
        )

    # Lakebench-annotated namespaces.
    console.print()
    console.print(Panel("Lakebench-annotated namespaces", expand=False))
    try:
        ns_list = core_v1.list_namespace()
    except Exception as e:  # noqa: BLE001
        print_error(f"cannot list namespaces: {e}")
        raise typer.Exit(1) from e

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("namespace")
    table.add_column("deployment name")
    table.add_column("api-server fingerprint")
    table.add_column("committed sha")
    found_any = False
    for ns in ns_list.items:
        anns = ns.metadata.annotations or {}
        name = anns.get(ANNOTATION_DEPLOYMENT_NAME)
        if not name:
            continue
        found_any = True
        table.add_row(
            ns.metadata.name,
            name,
            anns.get("lakebench.deployment/api-server", ""),
            anns.get("lakebench.deployment/committed-sha", ""),
        )
    if found_any:
        console.print(table)
    else:
        print_info("No lakebench-annotated namespaces on this cluster.")


# ---------------------------------------------------------------------------
# doctor (read-only)
# ---------------------------------------------------------------------------


@admin_app.command("doctor")
def doctor(
    config_file: Annotated[
        Path | None,
        typer.Argument(help="Config file to check against (optional)."),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option("--file", help="Alternative to positional argument."),
    ] = None,
) -> None:
    """Read-only preflight report for Category 2/3/4 shared cluster state."""
    from lakebench.deploy.cluster_lock import read_cluster_lock

    core_v1 = _get_core_v1()

    cfg = None
    if config_file is not None or file_option is not None:
        cfg = _load_cfg(config_file, file_option)

    console.print(Panel("lakebench admin doctor", expand=False))

    # StorageClass check. ADR-F8: when no config is passed we still
    # check the default `px-csi-scratch` name so a bare
    # `lakebench admin doctor` on a fresh cluster does not silently
    # skip the check that would catch "SC missing" -- that is exactly
    # the class of oversight the command exists to surface.
    if cfg is not None and cfg.platform.storage.scratch.enabled:
        sc_name = cfg.platform.storage.scratch.storage_class
        sc_context = "from config"
    elif cfg is None:
        sc_name = "px-csi-scratch"
        sc_context = "default (no config supplied)"
    else:
        sc_name = None
        sc_context = ""
    if sc_name is not None:
        try:
            from kubernetes import client as k8s_client

            k8s_client.StorageV1Api().read_storage_class(sc_name)
            print_success(f"StorageClass {sc_name!r} present ({sc_context})")
        except Exception as e:  # noqa: BLE001
            print_error(
                f"StorageClass {sc_name!r} missing ({sc_context}): {e}. "
                "Install with: lakebench admin install-scratch-storage-class"
            )

    # Spark Operator CRD check.
    try:
        from kubernetes import client as k8s_client

        apiext = k8s_client.ApiextensionsV1Api()
        apiext.read_custom_resource_definition("sparkapplications.sparkoperator.k8s.io")
        print_success("Spark Operator CRD present")
    except Exception:  # noqa: BLE001
        print_error(
            "Spark Operator CRD missing. Install with: lakebench admin install-spark-operator"
        )

    # Lease state.
    try:
        state = read_cluster_lock(core_v1)
    except Exception as e:  # noqa: BLE001
        print_warning(f"cannot read cluster lease: {e}")
        state = None
    if state is None:
        print_info("Cluster lease: not held")
    elif state.is_expired():
        print_warning(
            f"Cluster lease held by {state.holder!r} is EXPIRED "
            f"(acquired {state.acquired_at}); reclaim with "
            "`lakebench admin release-lock --expired-only`"
        )
    else:
        print_info(f"Cluster lease active (holder={state.holder}, acquired={state.acquired_at})")


# ---------------------------------------------------------------------------
# release-lock
# ---------------------------------------------------------------------------


@admin_app.command("release-lock")
def release_lock(
    expired_only: Annotated[
        bool,
        typer.Option(
            "--expired-only",
            help="Refuse to release a lease still within its TTL.",
        ),
    ] = True,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Release even a live lease. Use only when you are certain "
            "the prior holder crashed and cannot release itself.",
        ),
    ] = False,
) -> None:
    """Force-release a stale cluster lease.

    Default is ``--expired-only``: a live lease is left alone. Pass
    ``--force`` (which flips off ``--expired-only``) only when you are
    certain the prior holder crashed.
    """
    from lakebench.deploy.cluster_lock import (
        ClusterLockHeld,
        force_release_cluster_lock,
    )

    core_v1 = _get_core_v1()
    effective_expired_only = expired_only and not force
    try:
        state = force_release_cluster_lock(core_v1, expired_only=effective_expired_only)
    except ClusterLockHeld as e:
        print_error(
            f"lease is held by {e.holder!r} and still within TTL "
            f"(expires {e.expires_at}). Pass --force to release anyway."
        )
        raise typer.Exit(1) from e

    if state is None:
        print_info("no lease was held")
        return
    print_success(
        f"released lease previously held by {state.holder!r} (acquired {state.acquired_at})"
    )


# ---------------------------------------------------------------------------
# install-scratch-storage-class
# ---------------------------------------------------------------------------


@admin_app.command("install-scratch-storage-class")
def install_scratch_storage_class(
    config_file: Annotated[
        Path | None,
        typer.Argument(help="Config providing scratch settings (default: ./lakebench.yaml)."),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option("--file", help="Alternative to positional argument."),
    ] = None,
) -> None:
    """Install the scratch StorageClass named by config.

    Cluster-admin operation. Uses the settings under
    ``platform.storage.scratch`` (``storage_class`` name, ``provisioner``,
    ``parameters``). Runs under the cluster lease.
    """
    from lakebench.deploy.cluster_lock import (
        ClusterLockError,
        ClusterLockHeld,
        cluster_lock,
    )

    cfg = _load_cfg(config_file, file_option)
    scratch = cfg.platform.storage.scratch
    core_v1 = _get_core_v1()

    from kubernetes import client as k8s_client
    from kubernetes.client.exceptions import ApiException

    storage_v1 = k8s_client.StorageV1Api()

    # ADR-F9: both the read and the create happen inside the lease so
    # two admins racing this command produce one "created" + one
    # "already exists no-op" instead of a spurious 409 for whichever
    # loses the race.
    try:
        with cluster_lock(core_v1, timeout=30):
            try:
                storage_v1.read_storage_class(scratch.storage_class)
                print_info(f"StorageClass {scratch.storage_class!r} already exists; no-op")
                return
            except ApiException as e:
                if e.status != 404:
                    print_error(f"cannot read StorageClass: {e}")
                    raise typer.Exit(1) from e

            manifest = {
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {
                    "name": scratch.storage_class,
                    "labels": {"app.kubernetes.io/managed-by": "lakebench-admin"},
                },
                "provisioner": scratch.provisioner,
                "reclaimPolicy": "Delete",
                "volumeBindingMode": "WaitForFirstConsumer",
                "parameters": scratch.parameters,
            }
            try:
                storage_v1.create_storage_class(body=manifest)
            except ApiException as e:
                if e.status == 409:
                    # A third writer sneaked in (or a stale-cache read
                    # earlier missed the object). Treat as no-op.
                    print_info(f"StorageClass {scratch.storage_class!r} already exists; no-op")
                    return
                print_error(f"cannot create StorageClass: {e}")
                raise typer.Exit(1) from e
    except ClusterLockHeld as e:
        print_error(str(e))
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"could not acquire cluster lock: {e}")
        raise typer.Exit(1) from e

    print_success(
        f"created StorageClass {scratch.storage_class!r} "
        f"(provisioner {scratch.provisioner}, parameters {scratch.parameters})"
    )


# ---------------------------------------------------------------------------
# install-spark-operator
# ---------------------------------------------------------------------------


@admin_app.command("install-spark-operator")
def install_spark_operator(
    config_file: Annotated[
        Path | None,
        typer.Argument(help="Config providing operator namespace/version (optional)."),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option("--file", help="Alternative to positional argument."),
    ] = None,
    version: Annotated[
        str | None,
        typer.Option("--version", help="Chart version to install/upgrade to."),
    ] = None,
    operator_namespace: Annotated[
        str,
        typer.Option("--operator-namespace", help="Namespace to install into."),
    ] = "spark-operator",
) -> None:
    """Install or upgrade the shared Spark Operator Helm release.

    Runs under the cluster lease so concurrent admins cannot race the
    same Helm upgrade.
    """
    from lakebench.deploy.cluster_lock import (
        ClusterLockError,
        ClusterLockHeld,
        cluster_lock,
    )
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    core_v1 = _get_core_v1()

    ns = operator_namespace
    v = version
    if config_file is not None or file_option is not None:
        cfg = _load_cfg(config_file, file_option)
        ns = cfg.platform.compute.spark.operator.namespace or ns
        v = v or cfg.platform.compute.spark.operator.version

    try:
        with cluster_lock(core_v1, timeout=30):
            mgr = SparkOperatorManager(namespace=ns, version=v)
            ok = mgr.install()
    except ClusterLockHeld as e:
        print_error(str(e))
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"could not acquire cluster lock: {e}")
        raise typer.Exit(1) from e

    if not ok:
        print_error("Spark Operator install/upgrade failed. See logs above.")
        raise typer.Exit(1)
    print_success(f"Spark Operator install/upgrade ok (namespace={ns}, version={v})")


# ---------------------------------------------------------------------------
# migrate-deployment
# ---------------------------------------------------------------------------


@admin_app.command("migrate-deployment")
def migrate_deployment(
    namespace: Annotated[
        str,
        typer.Argument(help="Namespace to migrate."),
    ],
    config_file: Annotated[
        Path | None,
        typer.Argument(help="Config for this deployment (optional but recommended)."),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option("--file", help="Alternative to positional argument."),
    ] = None,
    api_server: Annotated[
        str | None,
        typer.Option(
            "--api-server-fingerprint",
            help="Override the api-server fingerprint (advanced).",
        ),
    ] = None,
) -> None:
    """Stamp deployment-identity annotations on a legacy namespace.

    A pre-revision-2 lakebench deployment has no ownership annotations
    and its Stackable SecretClasses carry the old cluster-wide fixed
    names. This command:

    1. Acquires the cluster lease.
    2. Verifies the namespace exists and has no lakebench annotations.
    3. Renames the legacy SecretClasses to the new
       ``lakebench-s3-credentials-<namespace>`` /
       ``lakebench-s3-ca-cert-<namespace>`` form (cluster-scoped -- must
       be done before another deployment claims the fixed names).
    4. Stamps the namespace annotations (name / api-server / committed-sha).

    Mandatory before ``destroy`` on any pre-revision-2 namespace --
    ``destroy`` refuses without the annotations.
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.exceptions import ApiException

    from lakebench.deploy.cluster_lock import (
        ClusterLockError,
        ClusterLockHeld,
        cluster_lock,
    )
    from lakebench.deploy.ownership import (
        ANNOTATION_API_SERVER,
        ANNOTATION_COMMITTED_SHA,
        ANNOTATION_DEPLOYMENT_NAME,
        api_server_fingerprint,
        build_identity_from_config,
        stamp_namespace,
    )

    core_v1 = _get_core_v1()

    # Namespace must exist.
    try:
        ns_obj = core_v1.read_namespace(namespace)
    except ApiException as e:
        if e.status == 404:
            print_error(f"namespace {namespace!r} does not exist")
        else:
            print_error(f"cannot read namespace {namespace!r}: {e}")
        raise typer.Exit(1) from e

    # Already migrated?
    anns = ns_obj.metadata.annotations or {}
    if ANNOTATION_DEPLOYMENT_NAME in anns:
        print_info(
            f"namespace {namespace!r} already stamped as "
            f"{anns[ANNOTATION_DEPLOYMENT_NAME]!r}; no-op"
        )
        return

    # Deployment identity to stamp.
    if config_file is not None or file_option is not None:
        cfg = _load_cfg(config_file, file_option)
        identity = build_identity_from_config(cfg, context=cfg.platform.kubernetes.context or None)
        expected_name = identity.name
        expected_api = api_server or identity.api_server
        committed = identity.committed_sha
    else:
        expected_name = namespace  # default assumption
        expected_api = api_server or api_server_fingerprint()
        committed = None

    if not expected_api:
        print_error(
            "cannot determine api-server fingerprint (kubeconfig missing?). "
            "Pass --api-server-fingerprint explicitly."
        )
        raise typer.Exit(1)

    custom_api = k8s_client.CustomObjectsApi()

    try:
        with cluster_lock(core_v1, timeout=30):
            # Rename legacy SecretClasses. On a clean cluster where the
            # legacy names never existed, both branches 404 cleanly.
            _migrate_secretclass(
                custom_api,
                legacy_name="lakebench-s3-credentials-class",
                new_name=f"lakebench-s3-credentials-{namespace}",
            )
            _migrate_secretclass(
                custom_api,
                legacy_name="lakebench-s3-ca-cert-class",
                new_name=f"lakebench-s3-ca-cert-{namespace}",
            )

            # Stamp annotations. force_legacy=True is the whole point of
            # migrate-deployment: the namespace has no lakebench
            # annotations yet, and we accept it because the operator has
            # explicitly consented via this command.
            report = stamp_namespace(
                core_v1,
                namespace,
                deployment_name=expected_name,
                api_server=expected_api,
                committed_sha=committed,
                force_legacy=True,
            )
    except ClusterLockHeld as e:
        print_error(str(e))
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"could not acquire cluster lock: {e}")
        raise typer.Exit(1) from e

    from lakebench.deploy.ownership import IdentityVerdict

    if report.verdict is IdentityVerdict.MISMATCH:
        print_error(
            f"stamp refused: another deployment claimed {namespace!r} "
            f"during migration: {report.hint}"
        )
        raise typer.Exit(1)
    print_success(
        f"migrated {namespace!r}: stamped {ANNOTATION_DEPLOYMENT_NAME}={expected_name}, "
        f"{ANNOTATION_API_SERVER}={expected_api}, {ANNOTATION_COMMITTED_SHA}={committed}"
    )


def _migrate_secretclass(custom_api, legacy_name: str, new_name: str) -> None:
    """Copy a legacy cluster-scoped SecretClass to the new namespaced name.

    Idempotent: 404 on the legacy name is a no-op (already migrated or
    never existed), and an already-present new_name is left alone. The
    legacy object is left in place -- another deployment still using it
    would break if we deleted it here. Cleanup is on the operator once
    every deployment has migrated.
    """
    from kubernetes.client.exceptions import ApiException

    try:
        legacy = custom_api.get_cluster_custom_object(
            group="secrets.stackable.tech",
            version="v1alpha1",
            plural="secretclasses",
            name=legacy_name,
        )
    except ApiException as e:
        if e.status == 404:
            return
        raise

    # ADR-F4/F4b: if the new_name already exists we must confirm it
    # is functionally the same as the legacy before treating it as
    # "already migrated". A pre-existing new_name pointing at a
    # different backend (aborted earlier migration, a manual admin
    # experiment, a foreign deployment's leftovers) would silently
    # bind Hive/Spark to the wrong S3 credentials -- with FlashBlade
    # that means writing to another team's account.
    #
    # We compare only ``spec.backend`` (the field that determines
    # WHICH secrets get looked up). Server-side defaulting and label
    # normalisation can add top-level ``spec`` fields after create
    # that are not present on the legacy read, so a raw dict compare
    # false-refuses legitimate re-migration.
    try:
        existing = custom_api.get_cluster_custom_object(
            group="secrets.stackable.tech",
            version="v1alpha1",
            plural="secretclasses",
            name=new_name,
        )
        legacy_backend = legacy.get("spec", {}).get("backend")
        existing_backend = existing.get("spec", {}).get("backend")
        if legacy_backend != existing_backend:
            raise RuntimeError(
                f"SecretClass {new_name!r} already exists but its "
                f"spec.backend does not match the legacy {legacy_name!r} "
                "it should have been copied from. Refusing to proceed --"
                " another migration attempt or a manual edit left "
                "divergent state. Delete or reconcile it manually and "
                "re-run."
            )
        logger.info(
            "SecretClass %s already exists and backend matches legacy; no-op",
            new_name,
        )
        return
    except ApiException as e:
        if e.status != 404:
            raise

    # Copy spec verbatim under the new name.
    body = {
        "apiVersion": "secrets.stackable.tech/v1alpha1",
        "kind": "SecretClass",
        "metadata": {
            "name": new_name,
            "labels": legacy.get("metadata", {}).get("labels", {}),
        },
        "spec": legacy.get("spec", {}),
    }
    custom_api.create_cluster_custom_object(
        group="secrets.stackable.tech",
        version="v1alpha1",
        plural="secretclasses",
        body=body,
    )


# ---------------------------------------------------------------------------
# repair-operator
# ---------------------------------------------------------------------------


@admin_app.command("repair-operator")
def repair_operator(
    config_file: Annotated[
        Path | None,
        typer.Argument(help="Config providing operator namespace/version (optional)."),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option("--file", help="Alternative to positional argument."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show the reconciled watch list without applying it."),
    ] = False,
) -> None:
    """Reconcile Spark Operator watch list against live annotated namespaces.

    When ``destroy`` fails partway through the watch-list mutation the
    operator can be left with stale entries. This command reads the
    live watch list and the live set of namespaces, keeps only the
    entries whose namespace exists AND carries a
    ``lakebench.deployment/name`` annotation (plus ``default``), and
    Helm-upgrades the operator to that reconciled list under the
    cluster lease.
    """
    from lakebench.deploy.cluster_lock import (
        ClusterLockError,
        ClusterLockHeld,
        cluster_lock,
    )
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    core_v1 = _get_core_v1()

    ns = "spark-operator"
    v: str | None = None
    if config_file is not None or file_option is not None:
        cfg = _load_cfg(config_file, file_option)
        ns = cfg.platform.compute.spark.operator.namespace or ns
        v = cfg.platform.compute.spark.operator.version

    mgr = SparkOperatorManager(namespace=ns, version=v)
    watched = mgr._get_watched_namespaces()  # noqa: SLF001 -- reconciliation needs live state
    if watched is None:
        print_info("Spark Operator watches all namespaces; nothing to reconcile")
        return

    # ADR-F2/F2b: keep every watch entry whose namespace still exists
    # AND is ``Active``, annotated or not. Legacy pre-PR-1 lakebench
    # deployments (or any third-party ns the operator happens to
    # watch) have no lakebench annotation but ARE running Spark
    # workloads there; pruning them would silently stall
    # reconciliation. Only namespaces that have been deleted, or are
    # in ``Terminating`` (about to be gone) are safe to drop -- that
    # is exactly the crash-loop this command is written to prevent.
    # Iterate ``list_namespace`` via ``_continue`` in case a very
    # large multi-tenant cluster exceeds the default page size:
    # missing a page silently unwatches its tenants.
    live_active_names: set[str] = set()
    cont: str | None = None
    while True:
        page = core_v1.list_namespace(_continue=cont) if cont else core_v1.list_namespace()
        for n in page.items:
            phase = getattr(n.status, "phase", None) if n.status else None
            if phase == "Active":
                live_active_names.add(n.metadata.name)
        cont = getattr(page.metadata, "_continue", None) or getattr(
            page.metadata, "continue_", None
        )
        # Guard against test mocks where `_continue` is itself a
        # MagicMock (truthy but not a real cursor). Only a non-empty
        # string is a real continuation token.
        if not cont or not isinstance(cont, str):
            break

    # ``default`` is preserved even when absent because the chart
    # rejects an empty jobNamespaces list.
    reconciled = sorted({n for n in watched if n in live_active_names} | {"default"})

    if reconciled == sorted(watched):
        print_info("Watch list already reconciled; no changes needed")
        return

    console.print("Reconciled watch list:")
    console.print(f"  before: {sorted(watched)}")
    console.print(f"  after:  {reconciled}")

    if dry_run:
        print_info("--dry-run set; not applying")
        return

    # Take the lease and apply. We mutate by removing entries not in
    # ``reconciled``. The strict path already lease-gates its own
    # per-namespace helm upgrade; we call the underlying non-strict
    # helper inside a single lease scope so the whole reconcile is one
    # atomic mutation.
    try:
        with cluster_lock(core_v1, timeout=60):
            to_drop = [n for n in watched if n not in reconciled]
            for n in to_drop:
                if not mgr._remove_namespace_from_watch_impl(n):  # noqa: SLF001
                    print_error(
                        f"could not drop {n!r} from watch list; the operator "
                        "may still be inconsistent. Retry after investigating "
                        "Helm state."
                    )
                    raise typer.Exit(1)
    except ClusterLockHeld as e:
        print_error(str(e))
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"could not acquire cluster lock: {e}")
        raise typer.Exit(1) from e

    print_success(f"reconciled Spark Operator watch list: {reconciled}")


# ---------------------------------------------------------------------------
# reclaim-bucket
# ---------------------------------------------------------------------------


@admin_app.command("reclaim-bucket")
def reclaim_bucket(
    bucket: Annotated[
        str,
        typer.Argument(help="Bucket to reclaim."),
    ],
    config_file: Annotated[
        Path | None,
        typer.Argument(help="Config providing S3 endpoint/credentials + target deployment name."),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option("--file", help="Alternative to positional argument."),
    ] = None,
    force_nonempty: Annotated[
        bool,
        typer.Option(
            "--force-nonempty",
            help="Rewrite the ownership tag even if the bucket has objects. "
            "Dangerous: another team's data may live there. Default is to "
            "refuse when objects are present.",
        ),
    ] = False,
) -> None:
    """Rewrite bucket ownership tag to the caller's deployment.

    For a bucket previously owned by another deployment (or untagged
    legacy). Default is to refuse when the bucket contains objects;
    an operator taking over a live dataset must acknowledge with
    ``--force-nonempty``.
    """
    from lakebench.deploy.cluster_lock import (
        ClusterLockError,
        ClusterLockHeld,
        cluster_lock,
    )
    from lakebench.deploy.ownership import (
        BucketTaggingUnsupported,
        bucket_name_matches_deployment,
        list_lakebench_deployment_names,
        write_bucket_ownership_tag,
    )
    from lakebench.s3 import S3Client

    cfg = _load_cfg(config_file, file_option)
    s3_cfg = cfg.platform.storage.s3
    core_v1 = _get_core_v1()

    s3 = S3Client(
        endpoint=s3_cfg.endpoint,
        access_key=s3_cfg.access_key,
        secret_key=s3_cfg.secret_key,
        region=s3_cfg.region,
        path_style=s3_cfg.path_style,
        ca_cert=s3_cfg.ca_cert,
        verify_ssl=s3_cfg.verify_ssl,
    )
    if s3._init_error:  # noqa: SLF001
        print_error(f"S3 client init failed: {s3._init_error}")
        raise typer.Exit(1)

    workload_schema = getattr(cfg, "workload_schema", None)
    try:
        with cluster_lock(core_v1, timeout=30):
            # ADR-F3: object-count check MUST run inside the lease.
            # Two admins racing this command without --force-nonempty
            # can each observe 0 keys before the first commits any
            # data; if the check ran outside the lease the second
            # would overwrite the first's tag, and a subsequent
            # destroy by the second would empty the first's live
            # bucket. Inside the lease every reclaim serialises.
            if not force_nonempty:
                try:
                    r = s3.raw_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                except Exception as e:  # noqa: BLE001
                    print_error(f"cannot list {bucket!r}: {e}")
                    raise typer.Exit(1) from e
                if r.get("KeyCount", 0) > 0 or r.get("Contents"):
                    print_error(
                        f"bucket {bucket!r} has objects; refusing to rewrite "
                        "ownership tag. Pass --force-nonempty to acknowledge that "
                        "another team's data may be under this name."
                    )
                    raise typer.Exit(1)
            try:
                write_bucket_ownership_tag(
                    s3.raw_client,
                    bucket=bucket,
                    deployment_name=cfg.name,
                    workload_schema=workload_schema,
                )
            except BucketTaggingUnsupported:
                # Backend does not support bucket tagging (e.g.
                # FlashBlade). There is no tag to write, so
                # reclaim-bucket is a no-op on this backend. Report
                # honestly and direct the operator to the name-prefix
                # fallback rather than crash. F1 (round-3): pass the
                # sibling list so the answer here matches what deploy /
                # destroy will do on the same bucket. Without this the
                # success message ("destroy will proceed on the
                # name-prefix fallback") is a lie whenever a
                # longer-prefix sibling deployment exists.
                others = list_lakebench_deployment_names(core_v1, exclude=cfg.get_namespace())
                if others is None:
                    print_error(
                        f"bucket {bucket!r}: backend does not support "
                        "bucket tagging, and lakebench could not "
                        "enumerate other deployments on the cluster "
                        "to check for sibling-collision risk. Grant "
                        "cluster-wide `list namespaces` to this token "
                        "and try again."
                    )
                    raise typer.Exit(1) from None
                if bucket_name_matches_deployment(bucket, cfg.name, others):
                    print_success(
                        f"bucket {bucket!r} on a backend that does not "
                        "support tagging: nothing to write, and the "
                        f"name grants deployment {cfg.name!r} a "
                        "longest-prefix claim over any sibling "
                        "deployment on the cluster. Destroy will "
                        "proceed on the name-prefix fallback."
                    )
                    raise typer.Exit(0) from None
                print_error(
                    f"bucket {bucket!r}: backend does not support "
                    "bucket tagging, and the name does not grant "
                    f"deployment {cfg.name!r} a longest-prefix claim "
                    "(name mismatch, or a sibling deployment on the "
                    "cluster has a longer prefix). Rename the bucket "
                    f"to start with {cfg.name}- to get destroy safety, "
                    "or pass --force-legacy on destroy (last-resort "
                    "operator assertion)."
                )
                raise typer.Exit(1) from None
    except ClusterLockHeld as e:
        print_error(str(e))
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"could not acquire cluster lock: {e}")
        raise typer.Exit(1) from e

    print_success(
        f"bucket {bucket!r} now owned by deployment {cfg.name!r} (workload={workload_schema})"
    )
