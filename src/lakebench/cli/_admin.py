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

See ``docs/design/namespace-isolation.md`` for the design
rationale.
"""

from __future__ import annotations

import logging
import subprocess
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
    LoadPurpose,
    load_config,
)
from lakebench.modules.pipeline_engines.spark.operator_scratch import (
    DEFAULT_CONTROLLER_TMP_SIZE,
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


def _get_core_v1(context: str | None = None):
    """Return a kubernetes CoreV1Api, printing a clean refusal on failure.

    ``context`` selects the kubeconfig context (the config's
    ``platform.kubernetes.context``); None means the current context. Every
    client a command uses must come from the same context, or it reads one
    cluster and changes another.
    """
    try:
        from kubernetes import client as k8s_client
        from kubernetes import config as k8s_config

        try:
            k8s_config.load_kube_config(context=context or None)
        except Exception:  # noqa: BLE001
            k8s_config.load_incluster_config()
        return k8s_client.CoreV1Api()
    except Exception as e:  # noqa: BLE001
        print_error(f"cannot open Kubernetes client: {e}")
        raise typer.Exit(1) from e


def _load_cfg(config_file: Path | None, file_option: Path | None):
    """Load a lakebench config with the same error path other commands use.

    Admin commands repair or reclaim existing deployments, so they skip the
    derived-name length check (LB-153) that deploy relies on.
    """
    path = resolve_config_path(config_file, file_option)
    try:
        return load_config(path, purpose=LoadPurpose.TEARDOWN)
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


def _read_operator_scratch(core_v1, operator_ns: str):
    """Diagnose the Spark Operator controller's /tmp (read-only API calls)."""
    from kubernetes import client as k8s_client

    from lakebench.modules.pipeline_engines.spark.operator_scratch import diagnose

    ser = k8s_client.ApiClient().sanitize_for_serialization
    dep = k8s_client.AppsV1Api().read_namespaced_deployment(
        "spark-operator-controller", operator_ns
    )
    pods = core_v1.list_namespaced_pod(
        operator_ns,
        label_selector="app.kubernetes.io/name=spark-operator,app.kubernetes.io/component=controller",
    )
    events = core_v1.list_namespaced_event(operator_ns, field_selector="reason=Evicted")
    return diagnose(
        ser(dep),
        [ser(p) for p in pods.items or []],
        [ser(e) for e in events.items or []],
    )


def _print_operator_scratch(core_v1, operator_ns: str) -> bool:
    """Print the controller /tmp diagnosis; False when it found a problem."""
    from lakebench.modules.pipeline_engines.spark.operator_scratch import repair_hint

    try:
        diag = _read_operator_scratch(core_v1, operator_ns)
    except Exception as e:  # noqa: BLE001 -- a report, not a gate
        print_warning(f"cannot read the Spark Operator controller in {operator_ns!r}: {e}")
        return True
    vol = diag.volume
    if not vol.found:
        size = "no 'tmp' volume"
    elif not vol.is_empty_dir:
        size = "'tmp' is not an emptyDir"
    else:
        size = vol.size_limit or "unbounded"
    if diag.healthy:
        print_success(f"Spark Operator controller /tmp: {size}; no storage evictions at this size")
    else:
        for problem in diag.problems:
            print_error(f"Spark Operator: {problem}")
        print_info(repair_hint())
    if diag.past_storage_evictions:
        print_info(
            f"{diag.past_storage_evictions} earlier controller pod(s) evicted for storage under "
            "an earlier /tmp size (history; delete the Failed pods to clear it)"
        )
    if diag.other_evictions:
        print_warning(f"{diag.other_evictions} controller pod(s) evicted for other reasons")
    if diag.container_restarts:
        print_info(f"controller container restarts: {diag.container_restarts}")
    return diag.healthy


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@admin_app.command("status")
def status(
    operator_namespace: Annotated[
        str,
        typer.Option("--operator-namespace", help="Namespace of the Spark Operator."),
    ] = "spark-operator",
) -> None:
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

    console.print()
    console.print(Panel("Spark Operator controller", expand=False))
    _print_operator_scratch(core_v1, operator_namespace)


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
        typer.Option("--file", "-f", help="Alternative to positional argument."),
    ] = None,
) -> None:
    """Read-only preflight report for Category 2/3/4 shared cluster state."""
    from lakebench.deploy.cluster_lock import read_cluster_lock

    cfg = None
    kube_ctx: str | None = None
    if config_file is not None or file_option is not None:
        cfg = _load_cfg(config_file, file_option)
        kube_ctx = cfg.platform.kubernetes.context or None
    core_v1 = _get_core_v1(context=kube_ctx)

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
    else:
        op_ns = "spark-operator"
        if cfg is not None:
            op_ns = cfg.platform.compute.spark.operator.namespace or op_ns
        _print_operator_scratch(core_v1, op_ns)

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
            "`lakebench admin release-lock`"
        )
    else:
        print_info(f"Cluster lease active (holder={state.holder}, acquired={state.acquired_at})")


# ---------------------------------------------------------------------------
# release-lock
# ---------------------------------------------------------------------------


@admin_app.command("release-lock")
def release_lock(
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

    Only an expired lease is released: a live lease is left alone. Pass
    ``--force`` to release a live one, only when you are certain the prior
    holder crashed.
    """
    from lakebench.deploy.cluster_lock import (
        ClusterLockError,
        ClusterLockHeld,
        force_release_cluster_lock,
    )

    core_v1 = _get_core_v1()
    try:
        state = force_release_cluster_lock(core_v1, expired_only=not force)
    except ClusterLockHeld as e:
        print_error(
            f"lease is held by {e.holder!r} and still within TTL "
            f"(expires {e.expires_at}). Refusing to release. "
            "First check who the holder is (namespace, session, or lane) "
            "and whether they are still running: releasing a live holder "
            "can corrupt a concurrent deploy or destroy. "
            "Only if you have confirmed the holder crashed and cannot "
            "release itself, re-run with --force as a last resort and "
            "type y/N to confirm."
        )
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"cannot release lease: {e}")
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
        typer.Option("--file", "-f", help="Alternative to positional argument."),
    ] = None,
) -> None:
    """Install the scratch StorageClass named by config.

    Cluster-admin operation. Uses the settings under
    ``platform.storage.scratch`` (``storage_class`` name, ``provisioner``,
    ``parameters``). Runs under the cluster lease.
    """
    from lakebench.deploy.cluster_lock import (
        ADMIN_MAX_HOLD_S,
        LEASE_REQUEST_TIMEOUT,
        ClusterLockError,
        ClusterLockHeld,
        LeaseHoldExceeded,
        cluster_lock,
    )

    cfg = _load_cfg(config_file, file_option)
    scratch = cfg.platform.storage.scratch
    # Pin every K8s call to the configured context (LB-070 class:
    # cluster-scoped StorageClass write on a stale KUBECONFIG hits the
    # wrong cluster). StorageV1Api reuses whatever context the SDK
    # loaded in _get_core_v1(context=...).
    core_v1 = _get_core_v1(context=cfg.platform.kubernetes.context or None)

    from kubernetes import client as k8s_client
    from kubernetes.client.exceptions import ApiException

    storage_v1 = k8s_client.StorageV1Api()

    # ADR-F9: both the read and the create happen inside the lease so
    # two admins racing this command produce one "created" + one
    # "already exists no-op" instead of a spurious 409 for whichever
    # loses the race.
    try:
        with cluster_lock(core_v1, timeout=600, max_hold_s=ADMIN_MAX_HOLD_S):
            try:
                storage_v1.read_storage_class(
                    scratch.storage_class, _request_timeout=LEASE_REQUEST_TIMEOUT
                )
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
                storage_v1.create_storage_class(
                    body=manifest, _request_timeout=LEASE_REQUEST_TIMEOUT
                )
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
    except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
        # A command inside the lease ran out of its hold budget; the lease
        # has been released.
        print_error(str(e))
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
        typer.Option("--file", "-f", help="Alternative to positional argument."),
    ] = None,
    version: Annotated[
        str | None,
        typer.Option("--version", help="Chart version to install/upgrade to."),
    ] = None,
    operator_namespace: Annotated[
        str,
        typer.Option("--operator-namespace", help="Namespace to install into."),
    ] = "spark-operator",
    controller_tmp_size: Annotated[
        str | None,
        typer.Option(
            "--controller-tmp-size",
            help="sizeLimit of the controller's /tmp emptyDir, which holds "
            "spark-submit's Ivy jar cache (chart default 1Gi is too small). "
            f"Default {DEFAULT_CONTROLLER_TMP_SIZE}; an upgrade without the flag "
            "keeps a larger size already set.",
        ),
    ] = None,
) -> None:
    """Install or upgrade the shared Spark Operator Helm release.

    Runs under the cluster lease so concurrent admins cannot race the
    same Helm upgrade. An existing release is upgraded with
    its stored values (the watch list is kept) and stays on its chart
    unless ``--version`` or the config names one. The controller's /tmp
    emptyDir is sized to ``--controller-tmp-size``.
    """
    from lakebench.modules.pipeline_engines.spark.operator_scratch import validate_size

    try:
        if controller_tmp_size is not None:
            validate_size(controller_tmp_size)
    except ValueError as e:
        print_error(f"--controller-tmp-size: {e}")
        raise typer.Exit(2) from e
    from lakebench.deploy.cluster_lock import (
        ADMIN_MAX_HOLD_S,
        ClusterLockError,
        ClusterLockHeld,
        LeaseHoldExceeded,
        cluster_lock,
    )
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    # Load cfg first so the K8s client and the operator manager both pin
    # the configured context. Without this, a stale KUBECONFIG plus a
    # `--file prod.yaml` invocation would helm-upgrade the shared spark-
    # operator release on the wrong cluster (LB-070 class).
    ns = operator_namespace
    v = version
    kube_ctx: str | None = None
    if config_file is not None or file_option is not None:
        cfg = _load_cfg(config_file, file_option)
        ns = cfg.platform.compute.spark.operator.namespace or ns
        v = v or cfg.platform.compute.spark.operator.version
        kube_ctx = cfg.platform.kubernetes.context or None
    core_v1 = _get_core_v1(context=kube_ctx)

    try:
        with cluster_lock(core_v1, timeout=600, max_hold_s=ADMIN_MAX_HOLD_S):
            mgr = SparkOperatorManager(namespace=ns, version=v, kube_context=kube_ctx)
            ok = mgr.install(version=v, tmp_size=controller_tmp_size)
    except ClusterLockHeld as e:
        print_error(str(e))
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"could not acquire cluster lock: {e}")
        raise typer.Exit(1) from e
    except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
        # A command inside the lease ran out of its hold budget; the lease
        # has been released.
        print_error(str(e))
        raise typer.Exit(1) from e

    if not ok:
        print_error("Spark Operator install/upgrade failed. See logs above.")
        raise typer.Exit(1)
    print_success(
        f"Spark Operator install/upgrade ok (namespace={ns}, "
        f"version={v or 'installed chart'}, "
        f"controller /tmp={controller_tmp_size or 'default or larger existing'})"
    )


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
        typer.Option("--file", "-f", help="Alternative to positional argument."),
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
        ADMIN_MAX_HOLD_S,
        ClusterLockError,
        ClusterLockHeld,
        LeaseHoldExceeded,
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

    # Load cfg first so the K8s client pins the configured context; a
    # stale KUBECONFIG plus a `migrate-deployment ns --file prod.yaml`
    # would otherwise stamp the wrong cluster's namespace.
    cfg = None
    kube_ctx: str | None = None
    if config_file is not None or file_option is not None:
        cfg = _load_cfg(config_file, file_option)
        kube_ctx = cfg.platform.kubernetes.context or None
    core_v1 = _get_core_v1(context=kube_ctx)

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
    if cfg is not None:
        identity = build_identity_from_config(cfg, context=kube_ctx)
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
        with cluster_lock(core_v1, timeout=600, max_hold_s=ADMIN_MAX_HOLD_S):
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
    except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
        # A command inside the lease ran out of its hold budget; the lease
        # has been released.
        print_error(str(e))
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

    from lakebench.k8s.lease_state import request_timeout_kw

    try:
        legacy = custom_api.get_cluster_custom_object(
            group="secrets.stackable.tech",
            version="v1alpha1",
            plural="secretclasses",
            name=legacy_name,
            **request_timeout_kw(),
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
            **request_timeout_kw(),
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
        **request_timeout_kw(),
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
        typer.Option("--file", "-f", help="Alternative to positional argument."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show the repairs without applying them."),
    ] = False,
    controller_tmp_size: Annotated[
        str,
        typer.Option(
            "--controller-tmp-size",
            help="Raise the controller's /tmp emptyDir sizeLimit to this when it is smaller.",
        ),
    ] = DEFAULT_CONTROLLER_TMP_SIZE,
) -> None:
    """Repair the shared Spark Operator: stale watch entries and a small /tmp.

    When ``destroy`` fails partway through the watch-list mutation the
    operator can be left with stale entries. This command reads the
    live watch list and the live set of namespaces, keeps only the
    entries whose namespace exists and is Active (plus ``default``), and
    Helm-upgrades the operator to that reconciled list under the
    cluster lease.

    It also raises the controller's /tmp emptyDir sizeLimit to
    ``--controller-tmp-size`` when it is smaller. spark-submit runs in the
    controller and fills /tmp with the Ivy jar cache; at the chart's 1Gi
    the kubelet evicts the controller. The resize keeps every stored value
    and the installed chart version, and rolls the controller once.
    """
    from lakebench.deploy.cluster_lock import (
        ADMIN_MAX_HOLD_S,
        ClusterLockError,
        ClusterLockHeld,
        LeaseHoldExceeded,
        cluster_lock,
    )
    from lakebench.modules.pipeline_engines.spark.operator import (
        SparkOperatorManager,
        _DeploymentReadError,
        _WatchListReadError,
    )
    from lakebench.modules.pipeline_engines.spark.operator_scratch import (
        parse_quantity,
        validate_size,
    )

    try:
        validate_size(controller_tmp_size)
    except ValueError as e:
        print_error(f"--controller-tmp-size: {e}")
        raise typer.Exit(2) from e

    ns = "spark-operator"
    v: str | None = None
    kube_ctx: str | None = None
    if config_file is not None or file_option is not None:
        cfg = _load_cfg(config_file, file_option)
        ns = cfg.platform.compute.spark.operator.namespace or ns
        v = cfg.platform.compute.spark.operator.version
        kube_ctx = cfg.platform.kubernetes.context or None

    core_v1 = _get_core_v1(context=kube_ctx)
    mgr = SparkOperatorManager(namespace=ns, version=v, kube_context=kube_ctx)

    # Controller /tmp: resize only a bounded emptyDir smaller than asked.
    resize_from: str | None = None
    try:
        vol = mgr.controller_tmp_volume()
    except _DeploymentReadError as e:
        print_warning(f"cannot read the controller /tmp volume, not resizing it: {e}")
    else:
        want = parse_quantity(controller_tmp_size) or 0
        if not vol.found or not vol.is_empty_dir:
            print_warning(
                "controller has no /tmp emptyDir named 'tmp'; not resizing "
                "(the chart layout differs from 2.5.1)"
            )
        elif vol.size_limit is not None and (vol.limit_bytes or 0) < want:
            resize_from = vol.size_limit

    try:
        watched = mgr._get_watched_namespaces()  # noqa: SLF001 -- reconciliation needs live state
    except _WatchListReadError as e:
        print_error(f"Cannot read the Spark Operator watch list: {e}")
        raise typer.Exit(1) from e

    to_drop: list[str] = []
    reconciled: list[str] = []
    if watched is None:
        print_info("Spark Operator watches all namespaces; no watch list to reconcile")
    else:
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
        else:
            to_drop = [n for n in watched if n not in reconciled]
            console.print("Reconciled watch list:")
            console.print(f"  before: {sorted(watched)}")
            console.print(f"  after:  {reconciled}")

    if resize_from is not None:
        console.print(f"Controller /tmp emptyDir: {resize_from} -> {controller_tmp_size}")
    elif not to_drop:
        return

    if dry_run:
        print_info("--dry-run set; not applying")
        return

    # Take the lease and apply. We mutate by removing entries not in
    # ``reconciled``. The strict path already lease-gates its own
    # per-namespace helm upgrade; we call the underlying non-strict
    # helper inside a single lease scope so the whole reconcile is one
    # atomic mutation.
    try:
        with cluster_lock(core_v1, timeout=600, max_hold_s=ADMIN_MAX_HOLD_S):
            for n in to_drop:
                if not mgr._remove_namespace_from_watch_impl(n):  # noqa: SLF001
                    print_error(
                        f"could not drop {n!r} from watch list; the operator "
                        "may still be inconsistent. Retry after investigating "
                        "Helm state."
                    )
                    raise typer.Exit(1)
            if resize_from is not None and not mgr.apply_controller_tmp_size(controller_tmp_size):
                print_error(
                    "could not resize the controller /tmp emptyDir; see the log above. "
                    "The watch list repair (if any) was applied."
                )
                raise typer.Exit(1)
    except ClusterLockHeld as e:
        print_error(str(e))
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"could not acquire cluster lock: {e}")
        raise typer.Exit(1) from e
    except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
        # A command inside the lease ran out of its hold budget; the lease
        # has been released.
        print_error(str(e))
        raise typer.Exit(1) from e

    if to_drop:
        print_success(f"reconciled Spark Operator watch list: {reconciled}")
    if resize_from is not None:
        print_success(f"controller /tmp emptyDir sizeLimit is now {controller_tmp_size}")


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
        typer.Option("--file", "-f", help="Alternative to positional argument."),
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
        ADMIN_MAX_HOLD_S,
        ClusterLockError,
        ClusterLockHeld,
        LeaseHoldExceeded,
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
    # Pin the K8s client to the configured context so reclaim-bucket
    # can only list this cluster's lakebench deployments, not the
    # ambient KUBECONFIG's.
    core_v1 = _get_core_v1(context=cfg.platform.kubernetes.context or None)

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
        with cluster_lock(core_v1, timeout=600, max_hold_s=ADMIN_MAX_HOLD_S):
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
                        "deployment on the cluster. Destroy empties and "
                        "deletes it only if this deployment's "
                        "created-buckets record lists it (deploy "
                        "created it), and empties it without deleting "
                        "if deploy recorded adopting it empty; otherwise "
                        "destroy leaves it in place. Before considering "
                        "--force-legacy as a last resort, first check "
                        "your cluster context with `oc whoami && kubectl "
                        "config current-context` and confirm it matches "
                        "the deployment's expected cluster."
                    )
                    raise typer.Exit(0) from None
                print_error(
                    f"bucket {bucket!r}: backend does not support "
                    "bucket tagging, and the name does not grant "
                    f"deployment {cfg.name!r} a longest-prefix claim "
                    "(name mismatch, or a sibling deployment on the "
                    "cluster has a longer prefix). Rename the bucket "
                    f"to start with {cfg.name}- to get destroy safety. "
                    "Only as a last resort, and only after verifying "
                    "your cluster context with `oc whoami && kubectl "
                    "config current-context`, pass --force-legacy on "
                    "destroy (operator assertion)."
                )
                raise typer.Exit(1) from None
    except ClusterLockHeld as e:
        print_error(str(e))
        raise typer.Exit(1) from e
    except ClusterLockError as e:
        print_error(f"could not acquire cluster lock: {e}")
        raise typer.Exit(1) from e
    except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
        # A command inside the lease ran out of its hold budget; the lease
        # has been released.
        print_error(str(e))
        raise typer.Exit(1) from e

    print_success(
        f"bucket {bucket!r} now owned by deployment {cfg.name!r} (workload={workload_schema})"
    )
