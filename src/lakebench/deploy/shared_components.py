"""Shared cluster components and ``lakebench admin install``.

Four components exist once per cluster and serve every deployment on it: the
scratch StorageClass, the Kubeflow Spark Operator, the Stackable operators
behind the Hive catalog, and the kube-prometheus-stack observability release.
``deploy`` only verifies them; a cluster admin installs them with ``lakebench
admin install --component <name>``, which runs here.

The command is built so that an installed cluster is never changed by it:

- Every requested component's status is read first, outside the lease. A
  status that cannot be read, a release in a state other than ``deployed``,
  a second copy of the component Lakebench can see (a Spark Operator in
  another namespace, another kube-prometheus-stack release, Stackable
  releases split across namespaces) or CRDs left with no operator refuses
  the whole invocation before anything changes. An operator installed
  without Helm and without the labels these reads use is not seen.
- The version to install is ``--version C=V`` when given, else the config's
  pin for a component that is not installed. An installed component keeps
  its version: a config pin never moves it, and a requested change is
  refused (exit 2 without ``--allow-version-change``, exit 3 with it). Helm
  never upgrades the CRDs a chart ships in ``crds/``, and all three charts do,
  so v1.7 does not automate a version change.
- When every component is already installed, nothing is mutated and the
  lease is not taken (the shared Grafana dashboard ConfigMap is the one
  thing re-applied, when it differs from this Lakebench's).
- Otherwise one cluster lease is held for the whole invocation, each
  component's plan is recomputed inside it (another admin may have installed
  it meanwhile), and a fresh install uses ``helm install``, never ``upgrade
  --install``, so helm itself refuses a release that appeared since the read.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from lakebench.exit_codes import ExitCode
from lakebench.k8s import pinned_helm
from lakebench.k8s.lease_state import LEASE_REQUEST_TIMEOUT

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig
    from lakebench.config.schema import (
        HiveConfig,
        ObservabilityConfig,
        ScratchStorageConfig,
        SparkOperatorConfig,
    )

logger = logging.getLogger(__name__)

PROMETHEUS_REPO_URL = "https://prometheus-community.github.io/helm-charts"

SCRATCH = "scratch-storage-class"
SPARK_OPERATOR = "spark-operator"
STACKABLE = "stackable"
OBSERVABILITY = "observability"

#: Every component, in install order.
COMPONENTS: tuple[str, ...] = (SCRATCH, SPARK_OPERATOR, STACKABLE, OBSERVABILITY)

#: Exit codes: a failed or unreadable step; a request that needs a flag or is
#: malformed (nothing ran); a request the safety model refuses.
EXIT_FAILED = ExitCode.FAILED
EXIT_USAGE = ExitCode.USAGE
EXIT_REFUSED = ExitCode.REFUSED

#: How long ``admin install`` waits for the cluster lease. Deploys and destroys
#: hold it for a watch-list change (a few minutes at most).
LEASE_WAIT_S = 600

#: ``--version`` takes an exact chart version, never a range.
_EXACT_VERSION = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]+)?$")


class StatusUnknown(Exception):
    """A component's state could not be read."""


@dataclass(frozen=True)
class ComponentStatus:
    """What the cluster has of one component.

    ``installed`` is None when it could not be read. ``blocked`` is set when the
    cluster's state means ``admin install`` must not touch the component (a
    release mid-operation, an operator Lakebench did not install, leftover
    CRDs); it is ``(exit code, message)``.
    """

    installed: bool | None
    version: str | None = None
    ready: bool = False
    detail: str = ""
    blocked: tuple[int, str] | None = None
    warnings: tuple[str, ...] = ()
    #: Where the component runs, when it has one namespace.
    namespace: str | None = None


def _unknown(detail: str) -> ComponentStatus:
    return ComponentStatus(installed=None, detail=detail)


@dataclass(frozen=True)
class Settings:
    """What the components need from a config, or the schema defaults."""

    kube_context: str | None
    scratch: ScratchStorageConfig
    spark_operator: SparkOperatorConfig
    #: The Hive catalog block; its ``operator`` is the Stackable install.
    hive: HiveConfig
    observability: ObservabilityConfig
    #: ``--controller-tmp-size``: the Spark Operator controller's /tmp on a
    #: fresh install. Refused for an installed operator (repair-operator
    #: resizes one).
    controller_tmp_size: str | None = None

    @classmethod
    def from_config(
        cls,
        cfg: LakebenchConfig | None,
        *,
        controller_tmp_size: str | None = None,
        spark_operator_namespace: str | None = None,
    ) -> Settings:
        from lakebench.config.schema import (
            HiveConfig,
            ObservabilityConfig,
            ScratchStorageConfig,
            SparkOperatorConfig,
        )

        if cfg is None:
            spark = SparkOperatorConfig()
            settings = cls(
                kube_context=None,
                scratch=ScratchStorageConfig(),
                spark_operator=spark,
                hive=HiveConfig(),
                observability=ObservabilityConfig(),
                controller_tmp_size=controller_tmp_size,
            )
        else:
            spark = cfg.platform.compute.spark.operator
            settings = cls(
                kube_context=cfg.platform.kubernetes.context or None,
                scratch=cfg.platform.storage.scratch,
                spark_operator=spark,
                hive=cfg.architecture.catalog.hive,
                observability=cfg.observability,
                controller_tmp_size=controller_tmp_size,
            )
        if spark_operator_namespace and spark_operator_namespace != spark.namespace:
            settings = cls(
                kube_context=settings.kube_context,
                scratch=settings.scratch,
                spark_operator=spark.model_copy(update={"namespace": spark_operator_namespace}),
                hive=settings.hive,
                observability=settings.observability,
                controller_tmp_size=controller_tmp_size,
            )
        return settings


def components_for_config(cfg: LakebenchConfig) -> list[str]:
    """What ``--component all`` means for a config: the components it uses."""
    from lakebench.config.autosizer import scratch_will_be_enabled

    out = []
    if scratch_will_be_enabled(cfg):
        out.append(SCRATCH)
    out.append(SPARK_OPERATOR)
    if cfg.architecture.catalog.type.value == "hive":
        out.append(STACKABLE)
    if cfg.observability.enabled:
        out.append(OBSERVABILITY)
    return out


# ---------------------------------------------------------------------------
# Cluster reads shared by the components
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReleaseInfo:
    name: str
    namespace: str
    status: str
    chart_version: str | None
    chart: str = ""


def _chart_version(chart: str) -> str | None:
    m = re.search(r"-(\d+\.\d+\.\d+[0-9A-Za-z.+-]*)$", chart or "")
    return m.group(1) if m else None


def helm_releases(context: str | None, names: Iterable[str]) -> dict[str, list[ReleaseInfo]]:
    """Every release with one of ``names``, in any namespace and any state.

    ``--all`` matters: without it helm hides pending and uninstalling
    releases, and a release mid-operation would read as absent. Raises
    :class:`StatusUnknown` when helm cannot list.
    """
    wanted = sorted(set(names))
    pattern = "^(" + "|".join(re.escape(n) for n in wanted) + ")$"
    out: dict[str, list[ReleaseInfo]] = {n: [] for n in wanted}
    for rel in _list_releases(context, pattern):
        if rel.name in out:
            out[rel.name].append(rel)
    return out


def _list_releases(context: str | None, pattern: str | None) -> list[ReleaseInfo]:
    """``helm list -A --all`` (every state), optionally filtered by name.

    ``--max 0`` lifts helm's default page of 256 releases, past which a
    release on a busy cluster would read as absent.
    """
    args = ["list", "--all-namespaces", "--all", "--max", "0", "--output", "json"]
    if pattern:
        args[3:3] = ["--filter", pattern]
    try:
        result = pinned_helm(context, args, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as e:
        raise StatusUnknown(f"helm list failed: {e}") from e
    if result.returncode != 0:
        raise StatusUnknown(
            f"helm list failed: {(result.stderr or '').strip()[:300] or 'unknown error'}"
        )
    try:
        rows = json.loads(result.stdout or "[]") or []
    except ValueError as e:
        raise StatusUnknown(f"helm list returned unreadable output: {e}") from e
    return [
        ReleaseInfo(
            name=str(r["name"]),
            namespace=str(r["namespace"]),
            status=str(r.get("status") or ""),
            chart_version=_chart_version(str(r.get("chart") or "")),
            chart=str(r.get("chart") or ""),
        )
        for r in rows
        if r.get("name") and r.get("namespace")
    ]


def _crd_present(name: str) -> bool:
    """Whether a CRD exists; raises StatusUnknown when it cannot be read."""
    from kubernetes import client as k8s_client
    from kubernetes.client.exceptions import ApiException

    try:
        k8s_client.ApiextensionsV1Api().read_custom_resource_definition(
            name, _request_timeout=LEASE_REQUEST_TIMEOUT
        )
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise StatusUnknown(f"cannot read CRD {name}: {e.status} {e.reason}") from e
    except Exception as e:  # noqa: BLE001 -- transport errors are unknown, not absent
        raise StatusUnknown(f"cannot read CRD {name}: {e}") from e


def _running_pod_namespaces(label_selector: str) -> set[str]:
    """Namespaces with a Running pod matching the selector; raises StatusUnknown."""
    from kubernetes import client as k8s_client

    try:
        pods = k8s_client.CoreV1Api().list_pod_for_all_namespaces(
            label_selector=label_selector,
            field_selector="status.phase=Running",
            _request_timeout=LEASE_REQUEST_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001
        raise StatusUnknown(f"cannot list pods ({label_selector}): {e}") from e
    return {p.metadata.namespace for p in pods.items or [] if p.metadata}


def _not_deployed(rel: ReleaseInfo) -> tuple[int, str]:
    return (
        EXIT_FAILED,
        f"Helm release {rel.name!r} in namespace {rel.namespace!r} is {rel.status!r}, not "
        "'deployed'; admin install does not touch a release mid-operation or failed. A "
        f"cluster admin inspects it with 'helm status {rel.name} -n {rel.namespace}' "
        "(an interrupted upgrade may need 'lakebench admin repair-operator' or a rollback).",
    )


# ---------------------------------------------------------------------------
# The components
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WatchUsers:
    """The deployments a Spark Operator version change would restart."""

    live: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    known: bool = True
    watches_all: bool = False


@dataclass
class InstallResult:
    ok: bool
    message: str
    #: Work to run after the lease is released (readiness waits); returns a
    #: problem or None.
    after_lease: Callable[[], str | None] | None = None


class SharedComponent(Protocol):
    name: str
    versioned: bool

    def status(self, s: Settings) -> ComponentStatus: ...

    def config_version(self, s: Settings) -> str | None: ...

    def install(self, s: Settings, version: str | None) -> InstallResult: ...


class ScratchStorageClass:
    """The scratch StorageClass named by ``platform.storage.scratch``."""

    name = SCRATCH
    versioned = False

    def config_version(self, s: Settings) -> str | None:
        return None

    def status(self, s: Settings) -> ComponentStatus:
        from kubernetes import client as k8s_client
        from kubernetes.client.exceptions import ApiException

        name = s.scratch.storage_class
        try:
            sc = k8s_client.StorageV1Api().read_storage_class(
                name, _request_timeout=LEASE_REQUEST_TIMEOUT
            )
        except ApiException as e:
            if e.status == 404:
                return ComponentStatus(installed=False, detail=f"StorageClass {name} not found")
            return _unknown(f"cannot read StorageClass {name}: {e.status} {e.reason}")
        except Exception as e:  # noqa: BLE001
            return _unknown(f"cannot read StorageClass {name}: {e}")
        diff = self.parameter_diff(s, sc)
        detail = f"StorageClass {name} ({getattr(sc, 'provisioner', '')})"
        return ComponentStatus(
            installed=True,
            ready=True,
            detail=detail,
            warnings=(diff,) if diff else (),
        )

    @staticmethod
    def parameter_diff(s: Settings, sc: Any) -> str:
        """How the live class differs from the config, on the keys the config sets."""
        want_params = {str(k): str(v) for k, v in (s.scratch.parameters or {}).items()}
        have_params = {str(k): str(v) for k, v in (getattr(sc, "parameters", None) or {}).items()}
        diffs = []
        have_prov = getattr(sc, "provisioner", None)
        if have_prov != s.scratch.provisioner:
            diffs.append(f"provisioner {have_prov!r} (config {s.scratch.provisioner!r})")
        for key, want in sorted(want_params.items()):
            have = have_params.get(key)
            if have != want:
                diffs.append(f"{key}={have!r} (config {want!r})")
        if not diffs:
            return ""
        return (
            f"StorageClass {s.scratch.storage_class} exists with "
            + ", ".join(diffs)
            + ". StorageClass parameters cannot be changed in place; recreating it is an "
            "owner action outside Lakebench (every PVC of every deployment uses it)."
        )

    def install(self, s: Settings, version: str | None) -> InstallResult:
        from kubernetes import client as k8s_client
        from kubernetes.client.exceptions import ApiException

        scratch = s.scratch
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
            k8s_client.StorageV1Api().create_storage_class(
                body=manifest, _request_timeout=LEASE_REQUEST_TIMEOUT
            )
        except ApiException as e:
            if e.status == 409:
                return InstallResult(
                    False,
                    f"StorageClass {scratch.storage_class} appeared while installing; "
                    "re-run admin install to check it",
                )
            return InstallResult(False, f"cannot create StorageClass: {e.status} {e.reason}")
        except Exception as e:  # noqa: BLE001
            return InstallResult(False, f"cannot create StorageClass: {e}")
        return InstallResult(
            True,
            f"created StorageClass {scratch.storage_class} (provisioner "
            f"{scratch.provisioner}, parameters {scratch.parameters})",
        )


class SparkOperator:
    """The Kubeflow Spark Operator Helm release ``spark-operator``."""

    name = SPARK_OPERATOR
    versioned = True
    RELEASE = "spark-operator"
    CRD = "sparkapplications.sparkoperator.k8s.io"
    SELECTOR = "app.kubernetes.io/name=spark-operator"

    def config_version(self, s: Settings) -> str | None:
        return s.spark_operator.version

    def _manager(self, s: Settings) -> Any:
        from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

        return SparkOperatorManager(
            namespace=s.spark_operator.namespace, kube_context=s.kube_context
        )

    @staticmethod
    def _deployments() -> list[Any]:
        from kubernetes import client as k8s_client

        try:
            return list(
                k8s_client.AppsV1Api()
                .list_deployment_for_all_namespaces(
                    label_selector=SparkOperator.SELECTOR, _request_timeout=LEASE_REQUEST_TIMEOUT
                )
                .items
                or []
            )
        except Exception as e:  # noqa: BLE001
            raise StatusUnknown(f"cannot list Spark Operator Deployments: {e}") from e

    def status(self, s: Settings) -> ComponentStatus:
        ns = s.spark_operator.namespace
        try:
            releases = helm_releases(s.kube_context, [self.RELEASE])[self.RELEASE]
            deps = self._deployments()
        except StatusUnknown as e:
            return _unknown(str(e))
        here = [r for r in releases if r.namespace == ns]
        elsewhere = sorted(
            {r.namespace for r in releases if r.namespace != ns}
            | {d.metadata.namespace for d in deps if d.metadata.namespace != ns}
        )
        controllers = [
            d
            for d in deps
            if d.metadata.namespace == ns
            and (d.metadata.labels or {}).get("app.kubernetes.io/component") == "controller"
        ]
        ready = any(((d.status.ready_replicas if d.status else 0) or 0) > 0 for d in controllers)
        if elsewhere and not here and not controllers:
            return ComponentStatus(
                installed=True,
                detail=f"a Spark Operator runs in {', '.join(elsewhere)}",
                blocked=(
                    EXIT_REFUSED,
                    f"A Spark Operator already runs in namespace(s) {', '.join(elsewhere)}, not "
                    f"in {ns!r}; a second one would reconcile the same SparkApplications. Set "
                    "platform.compute.spark.operator.namespace to its namespace.",
                ),
            )
        if here:
            rel = here[0]
            if rel.status != "deployed":
                return ComponentStatus(
                    installed=True,
                    version=rel.chart_version,
                    detail=f"release {rel.status}",
                    blocked=_not_deployed(rel),
                )
            detail = f"release {self.RELEASE} in {ns}" + ("" if ready else ", controller not ready")
            if not rel.chart_version:
                detail += ", chart version unreadable"
            return ComponentStatus(
                installed=True, version=rel.chart_version, ready=ready, detail=detail
            )
        if controllers:
            return ComponentStatus(
                installed=True,
                ready=ready,
                detail=f"controller in {ns} is not managed by a Helm release named {self.RELEASE}",
            )
        try:
            crd = _crd_present(self.CRD)
        except StatusUnknown as e:
            return _unknown(str(e))
        if crd:
            return ComponentStatus(
                installed=False,
                detail=f"CRD {self.CRD} exists with no operator",
                blocked=(
                    EXIT_REFUSED,
                    f"The CRD {self.CRD} exists but no Spark Operator runs: a leftover of an "
                    "uninstall, or an operator Lakebench cannot see. A fresh install would keep "
                    "the old CRDs (helm never replaces them). A cluster admin removes the "
                    "sparkoperator.k8s.io CRDs once nothing uses them, then re-runs admin install.",
                ),
            )
        return ComponentStatus(installed=False, detail=f"no Spark Operator in {ns}")

    def watch_list_users(self, s: Settings) -> WatchUsers:
        """Who a version change would restart: the watched namespaces that
        exist (live) and the entries whose namespace is gone (stale), minus
        the chart's ``default`` placeholder. ``known`` is False when the
        list or a namespace could not be read; ``watches_all`` when the
        controller has no ``--namespaces`` (every namespace is a user)."""
        from kubernetes import client as k8s_client
        from kubernetes.client.exceptions import ApiException

        try:
            watched = self._manager(s)._get_active_namespaces()
        except Exception:  # noqa: BLE001
            return WatchUsers(known=False)
        if watched is None:
            return WatchUsers(watches_all=True)
        live: list[str] = []
        stale: list[str] = []
        core = k8s_client.CoreV1Api()
        for ns in watched:
            if ns == "default":
                continue
            try:
                core.read_namespace(ns, _request_timeout=LEASE_REQUEST_TIMEOUT)
                live.append(ns)
            except ApiException as e:
                if e.status != 404:
                    return WatchUsers(known=False)
                stale.append(ns)
            except Exception:  # noqa: BLE001
                return WatchUsers(known=False)
        return WatchUsers(live=live, stale=stale)

    def prepare(self, s: Settings) -> str | None:
        """Refresh the chart repo before the lease is taken (a slow repo must
        not hold every deploy's watch-list change); a problem, or None."""
        return self._manager(s).refresh_chart_repo()

    def install(self, s: Settings, version: str | None) -> InstallResult:
        mgr = self._manager(s)
        ok = mgr.install(version=version, tmp_size=s.controller_tmp_size, add_repo=False)
        if not ok:
            ns = s.spark_operator.namespace
            return InstallResult(
                False,
                "Spark Operator install failed (see the log above). If the release was "
                f"created, it is left as it is: check it with 'helm status spark-operator -n "
                f"{ns}' and 'lakebench admin doctor'. To start again, a cluster admin runs "
                f"'helm uninstall spark-operator -n {ns}', removes the sparkoperator.k8s.io "
                "CRDs once nothing uses them, and re-runs admin install.",
            )
        return InstallResult(
            True,
            f"installed Spark Operator {version} in {s.spark_operator.namespace} "
            f"(controller /tmp {s.controller_tmp_size or 'default'})",
        )


class Stackable:
    """The four Stackable operators the Hive catalog needs."""

    name = STACKABLE
    versioned = True
    #: Install order: commons and listener first, as the SDP docs order them.
    OPERATORS = ("commons-operator", "listener-operator", "secret-operator", "hive-operator")
    #: The CRDs a HiveCluster needs, and the operator that serves each.
    CRDS = {
        "hiveclusters.hive.stackable.tech": "hive-operator",
        "secretclasses.secrets.stackable.tech": "secret-operator",
    }
    CHART_REPO = "oci://oci.stackable.tech/sdp-charts"
    _HELM_TIMEOUT_S = 120
    _READY_WAIT_S = 300

    def config_version(self, s: Settings) -> str | None:
        return s.hive.operator.version

    @staticmethod
    def running_operators() -> dict[str, set[str]]:
        """Operator -> namespaces with a Running pod; raises StatusUnknown."""
        return {
            op: _running_pod_namespaces(f"app.kubernetes.io/name={op}")
            for op in Stackable.OPERATORS
        }

    def status(self, s: Settings) -> ComponentStatus:
        ns = s.hive.operator.namespace
        try:
            releases = helm_releases(s.kube_context, self.OPERATORS)
            running = self.running_operators()
            crds = {crd: _crd_present(crd) for crd in self.CRDS}
        except StatusUnknown as e:
            return _unknown(str(e))
        present = {op: rels for op, rels in releases.items() if rels}
        for rels in present.values():
            if len(rels) > 1:
                where = ", ".join(r.namespace for r in rels)
                return ComponentStatus(
                    installed=True,
                    detail=f"{rels[0].name} installed more than once ({where})",
                    blocked=(
                        EXIT_REFUSED,
                        f"Stackable {rels[0].name} is installed in more than one namespace "
                        f"({where}); two operators reconcile the same objects. A cluster admin "
                        "removes the extra one.",
                    ),
                )
            if rels[0].status != "deployed":
                return ComponentStatus(
                    installed=True,
                    detail=f"{rels[0].name} {rels[0].status}",
                    blocked=_not_deployed(rels[0]),
                )
        serving = all(running[op] for op in ("hive-operator", "secret-operator"))
        ready = all(crds.values()) and serving
        if len(present) == len(self.OPERATORS):
            homes = sorted({rels[0].namespace for rels in present.values()})
            charts = {rels[0].chart_version for rels in present.values()}
            version = charts.pop() if len(charts) == 1 else None
            detail = f"releases in {', '.join(homes)}"
            if version is None:
                detail += ", chart versions differ or are unreadable"
            if not ready:
                missing = [c for c, ok in crds.items() if not ok]
                detail += (
                    f", missing CRDs {missing}"
                    if missing
                    else ", hive or secret operator not running"
                )
            return ComponentStatus(installed=True, version=version, ready=ready, detail=detail)
        if not present:
            if ready:
                return ComponentStatus(
                    installed=True,
                    ready=True,
                    detail="operators running without Helm releases Lakebench can see",
                )
            leftovers = [c for c, ok in crds.items() if ok]
            running_any = sorted(op for op, where in running.items() if where)
            if leftovers or running_any:
                return ComponentStatus(
                    installed=False,
                    detail="partial Stackable state without Helm releases",
                    blocked=(
                        EXIT_REFUSED,
                        "Stackable is partly present with no Helm releases Lakebench can see "
                        f"(CRDs {leftovers or 'none'}, running {running_any or 'none'}). A fresh "
                        "install would run on the old CRDs (helm never replaces them). A cluster "
                        "admin removes the leftovers or completes the install by hand.",
                    ),
                )
            return ComponentStatus(installed=False, detail="no Stackable operators")
        # Some releases: a partial install, which admin install completes only
        # when it is unambiguous (same namespace, same version, nothing of the
        # missing operators running elsewhere, and no CRD of a missing
        # operator left behind: helm would keep the old one).
        rels = [r[0] for r in present.values()]
        missing = [op for op in self.OPERATORS if op not in present]
        versions = {r.chart_version for r in rels}
        namespaces = {r.namespace for r in rels}
        stray = [op for op in missing if running[op]]
        stray += [f"CRD {crd}" for crd, op in self.CRDS.items() if op in missing and crds[crd]]
        if namespaces != {ns} or len(versions) != 1 or None in versions or stray:
            return ComponentStatus(
                installed=False,
                detail=f"partial: {sorted(present)} present, {missing} missing",
                blocked=(
                    EXIT_REFUSED,
                    f"Stackable is partly installed ({sorted(present)} in {sorted(namespaces)} at "
                    f"{sorted(str(v) for v in versions)}; missing {missing}"
                    + (f", left behind or running elsewhere: {stray}" if stray else "")
                    + "). admin install completes a partial install only in "
                    f"{ns!r} at one readable version.",
                ),
            )
        return ComponentStatus(
            installed=False,
            version=versions.pop(),
            detail=f"partial: missing {missing}",
        )

    def install(self, s: Settings, version: str | None) -> InstallResult:
        ns = s.hive.operator.namespace
        try:
            installed = {
                op for op, rels in helm_releases(s.kube_context, self.OPERATORS).items() if rels
            }
        except StatusUnknown as e:
            return InstallResult(False, str(e))
        done: list[str] = []
        for op in self.OPERATORS:
            if op in installed:
                continue
            cmd = [
                "install",
                op,
                f"{self.CHART_REPO}/{op}",
                "--version",
                str(version),
                "--namespace",
                ns,
                # Bounds helm's own waits (hooks) below the subprocess kill;
                # without --wait it does not bound resource creation, so a
                # killed install can still leave a pending-install release,
                # which the next status read refuses with the inspect command.
                "--timeout",
                "90s",
            ]
            if not done and not installed:
                cmd.append("--create-namespace")
            # No --wait: the release record exists when this returns, so the
            # lease is not held through image pulls; readiness is awaited
            # after the lease is released.
            try:
                result = pinned_helm(
                    s.kube_context,
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self._HELM_TIMEOUT_S,
                )
            except (OSError, subprocess.SubprocessError) as e:
                return InstallResult(
                    False, f"helm install {op} failed: {e}; installed so far: {done}"
                )
            if result.returncode != 0:
                return InstallResult(
                    False,
                    f"helm install {op} failed: {(result.stderr or '').strip()[:300]}; "
                    f"installed so far: {done}",
                )
            done.append(op)
        return InstallResult(
            True,
            f"installed Stackable {', '.join(done)} {version} in {ns}",
            after_lease=self._wait_ready,
        )

    def _wait_ready(self) -> str | None:
        deadline = time.monotonic() + self._READY_WAIT_S
        last = ""
        while True:
            try:
                crds = {crd: _crd_present(crd) for crd in self.CRDS}
                running = self.running_operators()
                if all(crds.values()) and all(running[op] for op in self.OPERATORS):
                    return None
                last = f"CRDs {crds}, running {sorted(op for op, w in running.items() if w)}"
            except StatusUnknown as e:
                last = str(e)
            if time.monotonic() >= deadline:
                return f"Stackable operators not ready after {self._READY_WAIT_S}s ({last})"
            time.sleep(5)


class Observability:
    """The shared kube-prometheus-stack release ``lakebench-observability``."""

    name = OBSERVABILITY
    versioned = True

    def config_version(self, s: Settings) -> str | None:
        return s.observability.chart_version

    def status(self, s: Settings) -> ComponentStatus:
        from lakebench.deploy.observability import HELM_RELEASE_NAME, OBSERVABILITY_NAMESPACE

        try:
            every = _list_releases(s.kube_context, None)
        except StatusUnknown as e:
            return _unknown(str(e))
        rels = [r for r in every if r.name == HELM_RELEASE_NAME]
        # Another kube-prometheus-stack under another name carries the same
        # cluster-scoped CRDs and webhooks; a second install would fight it.
        others = sorted(
            f"{r.name} in {r.namespace}"
            for r in every
            if r.name != HELM_RELEASE_NAME and r.chart.startswith("kube-prometheus-stack-")
        )
        if not rels and others:
            return ComponentStatus(
                installed=True,
                detail=f"another kube-prometheus-stack: {', '.join(others)}",
                blocked=(
                    EXIT_REFUSED,
                    f"A kube-prometheus-stack release already exists ({', '.join(others)}); a "
                    f"second one ({HELM_RELEASE_NAME}) would install the same cluster-wide CRDs "
                    "and webhooks. Lakebench uses only its own release; set observability."
                    "enabled: false, or remove the other one when nothing uses it.",
                ),
            )
        if not rels:
            return ComponentStatus(installed=False, detail="no observability release")
        shared = [r for r in rels if r.namespace == OBSERVABILITY_NAMESPACE]
        rel = shared[0] if shared else sorted(rels, key=lambda r: r.namespace)[0]
        if rel.status != "deployed":
            return ComponentStatus(
                installed=True,
                version=rel.chart_version,
                detail=f"release {rel.status}",
                blocked=_not_deployed(rel),
            )
        warnings: list[str] = (
            [f"another kube-prometheus-stack release exists too: {', '.join(others)}"]
            if others
            else []
        )
        if rel.namespace != OBSERVABILITY_NAMESPACE:
            warnings.append(
                f"An observability release from an older lakebench exists in namespace "
                f"{rel.namespace!r}; it scrapes only that namespace. admin install leaves it "
                "alone; remove it when no deployment uses it, then re-run admin install."
            )
        from lakebench.deploy.observability import _wait_for_prometheus

        # One readiness read, no wait: a deployed release whose Prometheus
        # never came up is installed but not ready (exit 1, not a no-op).
        problem = _wait_for_prometheus(rel.namespace, timeout_s=0, context=s.kube_context)
        return ComponentStatus(
            installed=True,
            version=rel.chart_version,
            ready=not problem,
            detail=f"release in {rel.namespace}" + (f"; {problem}" if problem else ""),
            warnings=tuple(warnings),
            namespace=rel.namespace,
        )

    def prepare(self, s: Settings) -> str | None:
        """Refresh the chart repo before the lease is taken; a problem, or None."""
        for args, timeout in (
            (["repo", "add", "prometheus-community", PROMETHEUS_REPO_URL], 30),
            (["repo", "update", "prometheus-community"], 60),
        ):
            try:
                r = pinned_helm(
                    s.kube_context, args, capture_output=True, text=True, timeout=timeout
                )
            except (OSError, subprocess.SubprocessError) as e:
                return f"helm {' '.join(args[:2])} failed: {e}"
            if r.returncode != 0:
                return f"helm {' '.join(args[:2])} failed: {(r.stderr or '').strip()[:300]}"
        return None

    @staticmethod
    def dashboard_manifests() -> list[dict[str, Any]]:
        """The rendered shared Grafana dashboard ConfigMap(s)."""
        import yaml

        from lakebench.deploy.engine import TemplateRenderer
        from lakebench.deploy.observability import DASHBOARD_TEMPLATE, OBSERVABILITY_NAMESPACE

        text = TemplateRenderer().render(
            DASHBOARD_TEMPLATE, {"observability_namespace": OBSERVABILITY_NAMESPACE}
        )
        return [d for d in yaml.safe_load_all(text) if d]

    def dashboard_stale(self, s: Settings) -> bool:
        """Whether the shared dashboard is missing or differs from this release's."""
        from kubernetes import client as k8s_client
        from kubernetes.client.exceptions import ApiException

        from lakebench.deploy.observability import OBSERVABILITY_NAMESPACE

        if not s.observability.dashboards_enabled:
            return False
        core = k8s_client.CoreV1Api()
        for doc in self.dashboard_manifests():
            name = (doc.get("metadata") or {}).get("name")
            try:
                live = core.read_namespaced_config_map(
                    name, OBSERVABILITY_NAMESPACE, _request_timeout=LEASE_REQUEST_TIMEOUT
                )
            except ApiException as e:
                if e.status == 404:
                    return True
                raise StatusUnknown(f"cannot read dashboard ConfigMap {name}: {e.status}") from e
            except Exception as e:  # noqa: BLE001
                raise StatusUnknown(f"cannot read dashboard ConfigMap {name}: {e}") from e
            if (live.data or {}) != {k: str(v) for k, v in (doc.get("data") or {}).items()}:
                return True
        return False

    def apply_dashboard(self, s: Settings) -> InstallResult:
        from lakebench.deploy.observability import OBSERVABILITY_NAMESPACE
        from lakebench.k8s import get_k8s_client

        if not s.observability.dashboards_enabled:
            return InstallResult(True, "dashboards disabled")
        k8s = get_k8s_client(context=s.kube_context or "", namespace=OBSERVABILITY_NAMESPACE)
        for doc in self.dashboard_manifests():
            try:
                k8s.apply_manifest(doc, namespace=OBSERVABILITY_NAMESPACE)
            except Exception as e:  # noqa: BLE001
                return InstallResult(False, f"cannot apply the Grafana dashboard: {e}")
        return InstallResult(True, "applied the shared Grafana dashboard")

    def install(self, s: Settings, version: str | None) -> InstallResult:
        from lakebench.deploy.observability import (
            HELM_CHART,
            HELM_RELEASE_NAME,
            OBSERVABILITY_NAMESPACE,
            build_helm_values,
            is_openshift,
            write_openshift_values_file,
        )

        ctx = s.kube_context
        cmd = [
            "install",
            HELM_RELEASE_NAME,
            HELM_CHART,
            "--version",
            str(version),
            "--namespace",
            OBSERVABILITY_NAMESPACE,
            "--create-namespace",
            # No --wait: Prometheus readiness is awaited after the lease.
            "--timeout",
            "5m",
        ]
        for key, val in build_helm_values(s.observability).items():
            cmd.extend(["--set", f"{key}={val}"])
        if is_openshift(ctx):
            cmd.extend(["-f", write_openshift_values_file()])
        try:
            result = pinned_helm(ctx, cmd, capture_output=True, text=True, timeout=360)
        except (OSError, subprocess.SubprocessError) as e:
            return InstallResult(False, f"helm install {HELM_RELEASE_NAME} failed: {e}")
        if result.returncode != 0:
            lines = (result.stderr or "").strip().splitlines()
            errors = [ln for ln in lines if not ln.lstrip().startswith(("I0", "W0", '"Warning'))]
            return InstallResult(
                False,
                f"helm install {HELM_RELEASE_NAME} failed: "
                f"{chr(10).join(errors).strip()[:500] or 'unknown error'}",
            )
        dash = self.apply_dashboard(s)
        if not dash.ok:
            logger.warning(dash.message)

        def _ready() -> str | None:
            from lakebench.deploy.observability import _wait_for_prometheus

            return _wait_for_prometheus(OBSERVABILITY_NAMESPACE, context=ctx)

        return InstallResult(
            True,
            f"installed kube-prometheus-stack {version} as {HELM_RELEASE_NAME} in "
            f"{OBSERVABILITY_NAMESPACE}" + ("" if dash.ok else f" ({dash.message})"),
            after_lease=_ready,
        )


REGISTRY: dict[str, SharedComponent] = {
    SCRATCH: ScratchStorageClass(),
    SPARK_OPERATOR: SparkOperator(),
    STACKABLE: Stackable(),
    OBSERVABILITY: Observability(),
}


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------

NOOP = "noop"
INSTALL = "install"
DASHBOARD = "update-dashboard"
REFUSE = "refuse"


@dataclass
class Plan:
    component: str
    action: str
    status: ComponentStatus
    target: str | None = None
    code: int = 0
    message: str = ""
    warnings: list[str] = field(default_factory=list)

    @property
    def mutates(self) -> bool:
        return self.action in (INSTALL, DASHBOARD)


def parse_versions(pairs: Iterable[str], requested: Iterable[str]) -> dict[str, str]:
    """``--version C=V`` pairs; raises ValueError with the message to print."""
    requested = list(requested)
    out: dict[str, str] = {}
    for pair in pairs:
        comp, sep, ver = pair.partition("=")
        comp, ver = comp.strip(), ver.strip()
        if not sep or not comp or not ver:
            raise ValueError(f"--version {pair!r}: expected COMPONENT=VERSION")
        if comp not in COMPONENTS:
            raise ValueError(f"--version {pair!r}: unknown component {comp!r}")
        if comp not in requested:
            raise ValueError(f"--version {pair!r}: {comp} is not a requested --component")
        if not REGISTRY[comp].versioned:
            raise ValueError(f"--version {pair!r}: {comp} has no version")
        if not _EXACT_VERSION.match(ver):
            raise ValueError(
                f"--version {pair!r}: give an exact chart version such as 2.5.1, not a range"
            )
        if comp in out and out[comp] != ver:
            raise ValueError(f"--version names {comp} twice")
        out[comp] = ver
    return out


def plan_component(
    name: str,
    s: Settings,
    *,
    requested_version: str | None = None,
    allow_version_change: bool = False,
) -> Plan:
    """What ``admin install`` would do for one component; reads only."""
    comp = REGISTRY[name]
    st = comp.status(s)
    warnings = list(st.warnings)
    if st.installed is None:
        return Plan(
            name,
            REFUSE,
            st,
            code=EXIT_FAILED,
            message=f"cannot tell whether {name} is installed ({st.detail}); nothing was changed",
        )
    if st.blocked:
        code, msg = st.blocked
        return Plan(name, REFUSE, st, code=code, message=msg)
    if name == SCRATCH and st.installed and warnings:
        # The config's parameters cannot be met: a StorageClass is immutable.
        return Plan(name, REFUSE, st, code=EXIT_REFUSED, message=warnings[0])
    if name == SPARK_OPERATOR and st.installed and s.controller_tmp_size:
        return Plan(
            name,
            REFUSE,
            st,
            code=EXIT_USAGE,
            message=(
                "--controller-tmp-size applies to a fresh install only; the Spark Operator is "
                "installed. Resize its controller /tmp with 'lakebench admin repair-operator "
                f"--controller-tmp-size {s.controller_tmp_size}'."
            ),
        )
    if not st.installed:
        target = requested_version or st.version or comp.config_version(s)
        if requested_version and st.version and requested_version != st.version:
            # A partial Stackable install completes at its present version only.
            return Plan(
                name,
                REFUSE,
                st,
                code=EXIT_REFUSED,
                message=(
                    f"{name} is partly installed at {st.version}; admin install completes it "
                    f"at that version only, not {requested_version}."
                ),
            )
        return Plan(name, INSTALL, st, target=target, warnings=warnings)
    if comp.versioned and requested_version:
        if st.version is None:
            return Plan(
                name,
                REFUSE,
                st,
                code=EXIT_FAILED,
                message=(
                    f"cannot read the installed version of {name} ({st.detail}); not comparing "
                    f"it with {requested_version}. Nothing was changed."
                ),
            )
        if requested_version != st.version:
            return _version_change_refusal(name, s, st, requested_version, allow_version_change)
    pin = comp.config_version(s)
    if comp.versioned and not requested_version and pin and st.version and pin != st.version:
        # The installed version stays; say so, because run evidence that
        # reads the config's pin would name a version that is not running.
        warnings.append(
            f"installed at {st.version}, the config (or Lakebench default) names {pin}; the "
            "installed version is kept and is what runs"
        )
    if name == OBSERVABILITY:
        try:
            stale = REGISTRY[OBSERVABILITY].dashboard_stale(s)  # type: ignore[attr-defined]
        except StatusUnknown as e:
            warnings.append(f"{e}; dashboard not checked")
            stale = False
        from lakebench.deploy.observability import OBSERVABILITY_NAMESPACE

        if stale and st.namespace == OBSERVABILITY_NAMESPACE:
            return Plan(name, DASHBOARD, st, target=st.version, warnings=warnings)
    return Plan(name, NOOP, st, target=st.version, warnings=warnings)


def _version_change_refusal(
    name: str, s: Settings, st: ComponentStatus, target: str, allow: bool
) -> Plan:
    head = f"{name} is installed at {st.version}; the target is {target}."
    if not allow:
        return Plan(
            name,
            REFUSE,
            st,
            code=EXIT_USAGE,
            message=(
                f"{head} --version never moves an installed component. Pass "
                "--allow-version-change to 'lakebench admin install' to see what a change needs."
            ),
        )
    manual = (
        "Lakebench does not automate the change: helm upgrade leaves the CRDs this chart "
        "ships in crds/ at the installed version."
    )
    if name != SPARK_OPERATOR:
        why = (
            f"{manual} With no deployment using it and no deploy or destroy in flight, a "
            "cluster admin upgrades it by hand following the chart's upgrade notes (apply the "
            "new CRDs, then upgrade the releases), then re-runs admin install to verify."
        )
        return Plan(name, REFUSE, st, code=EXIT_REFUSED, message=f"{head} {why}")
    users = REGISTRY[SPARK_OPERATOR].watch_list_users(s)  # type: ignore[attr-defined]
    if not users.known:
        why = (
            f"{manual} The operator's watch list could not be read, so the deployments using "
            "it are unknown: do not change its version until 'lakebench admin doctor' reads it."
        )
    elif users.watches_all:
        why = (
            f"{manual} The operator watches every namespace, so a change restarts it for every "
            "deployment on the cluster."
        )
    elif users.live:
        why = (
            f"{manual} {len(users.live)} deployment(s) use {name}: {users.live}; destroy them "
            "first. A version change restarts it for every one of them."
        )
    else:
        why = (
            f"{manual} No deployment uses it. With no deploy or destroy in flight, a cluster "
            "admin upgrades it by hand following the chart's upgrade notes (apply the new CRDs, "
            "then upgrade the release with its stored values), then re-runs admin install to "
            "verify."
        )
    if users.stale:
        why += (
            f" The watch list also names deleted namespaces {users.stale}: run 'lakebench admin "
            "repair-operator' first, or the restarted operator crash-loops."
        )
    return Plan(name, REFUSE, st, code=EXIT_REFUSED, message=f"{head} {why}")


# ---------------------------------------------------------------------------
# Running an install
# ---------------------------------------------------------------------------


@dataclass
class Report:
    code: int
    plans: list[Plan]
    final: dict[str, ComponentStatus]
    lease_taken: bool


Emit = Callable[[str, str], None]  # (level: info|ok|warn|error, message)


def run_install(
    s: Settings,
    names: list[str],
    *,
    versions: dict[str, str] | None = None,
    allow_version_change: bool = False,
    dry_run: bool = False,
    confirm: Callable[[str], bool] | None = None,
    core_v1: Any = None,
    emit: Emit | None = None,
) -> Report:
    """Plan, then install what is missing under one lease. Never raises for a
    cluster state; the report's ``code`` is the exit code.

    Chart repos are refreshed before the lease, readiness is awaited after
    it, and the final status table is read after it too (reads only).
    """
    from lakebench.deploy.cluster_lock import (
        ADMIN_MAX_HOLD_S,
        ClusterLockError,
        LeaseHoldExceeded,
        cluster_lock,
    )

    say: Emit = emit or (lambda level, msg: logger.info("%s: %s", level, msg))
    versions = dict(versions or {})
    order = [c for c in COMPONENTS if c in names]

    def _plan(name: str) -> Plan:
        return plan_component(
            name,
            s,
            requested_version=versions.get(name),
            allow_version_change=allow_version_change,
        )

    plans = [_plan(n) for n in order]
    for p in plans:
        for w in p.warnings:
            say("warn", f"{p.component}: {w}")
    refusals = [p for p in plans if p.action == REFUSE]
    if refusals:
        for p in refusals:
            say("error", f"{p.component}: {p.message}")
        return Report(max(p.code for p in refusals), plans, {}, False)

    todo = [p for p in plans if p.mutates]
    if not todo:
        final = {p.component: p.status for p in plans}
        not_ready = [n for n, st in final.items() if not st.ready]
        if not_ready:
            say("error", f"installed but not ready: {', '.join(not_ready)}")
            return Report(EXIT_FAILED, plans, final, False)
        say("ok", "every requested component is installed; nothing to change")
        return Report(0, plans, final, False)

    for p in todo:
        what = "update the shared dashboard of" if p.action == DASHBOARD else "install"
        say("info", f"would {what} {p.component} {p.target or ''}".rstrip())
    if dry_run:
        return Report(0, plans, {p.component: p.status for p in plans}, False)
    prompt = (
        "admin install holds the cluster lease while it installs; deploys and destroys on this "
        f"cluster wait for it, up to {LEASE_WAIT_S // 60} minutes each, and one that waits "
        "longer fails without changing anything shared. Continue?"
    )
    if confirm is not None and not confirm(prompt):
        say("error", "aborted; nothing was changed")
        return Report(EXIT_FAILED, plans, {}, False)

    for p in todo:
        prepare = getattr(REGISTRY[p.component], "prepare", None)
        problem = prepare(s) if p.action == INSTALL and prepare is not None else None
        if problem:
            say("error", f"{p.component}: {problem}; nothing was changed")
            return Report(EXIT_FAILED, plans, {}, False)

    after: list[tuple[str, Callable[[], str | None]]] = []
    failures: list[str] = []
    code = 0
    try:
        with cluster_lock(core_v1, timeout=LEASE_WAIT_S, max_hold_s=ADMIN_MAX_HOLD_S):
            for p in todo:
                fresh = _plan(p.component)
                if fresh.action == REFUSE:
                    say("error", f"{p.component}: {fresh.message} (read inside the lease)")
                    code = max(code, fresh.code)
                    failures.append(p.component)
                    break
                if fresh.action == NOOP:
                    say("info", f"{p.component}: installed meanwhile; nothing to do")
                    continue
                comp = REGISTRY[p.component]
                if fresh.action == DASHBOARD:
                    result = REGISTRY[OBSERVABILITY].apply_dashboard(s)  # type: ignore[attr-defined]
                else:
                    say("info", f"installing {p.component} {fresh.target or ''}".rstrip())
                    result = comp.install(s, fresh.target)
                if result.ok:
                    say("ok", f"{p.component}: {result.message}")
                    if result.after_lease is not None:
                        after.append((p.component, result.after_lease))
                else:
                    say("error", f"{p.component}: {result.message}")
                    failures.append(p.component)
                    code = max(code, EXIT_FAILED)
                    break
    except ClusterLockError as e:
        say("error", f"could not take the cluster lease: {e}; nothing was changed")
        return Report(EXIT_FAILED, plans, {}, False)
    except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
        # A command inside the lease ran out of the admin hold budget; the
        # lease has been released. The message names the recovery.
        say("error", f"{e}; the lease was released")
        return Report(EXIT_FAILED, plans, {}, True)

    for comp_name, wait in after:
        problem = wait()
        if problem:
            say("error", f"{comp_name}: {problem}")
            code = max(code, EXIT_FAILED)
    after_install = {name: REGISTRY[name].status(s) for name in order}
    not_ready = [n for n, st in after_install.items() if not st.ready]
    if not_ready:
        say("error", f"not ready: {', '.join(not_ready)}")
        code = max(code, EXIT_FAILED)
    return Report(code, plans, after_install, True)
