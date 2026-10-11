"""The one list of cluster prerequisites.

``PREREQS`` names everything a deployment needs from the cluster before
``deploy`` can succeed. Each entry carries a read-only check, the fix, and the
text ``docs/prerequisites.md`` is generated from
(``scripts/gen_prereq_docs.py``), so the page and the checks cannot drift:
``scripts/gen_docs.py --check`` fails on a hand edit or a registry change
without a regenerate. ``plan`` runs the same checks through
:func:`run_prereqs`.

Checks never write. They read through a :class:`ClusterReader`, which tests
replace with a fake. A change that adds a prerequisite adds its entry here in
the same change.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig


class PrereqStatus(Enum):
    OK = "ok"
    WARN = "warn"
    FAIL = "fail"  # checked and missing or broken: the fix applies
    UNKNOWN = "unknown"  # the check itself could not run (API error, no rights)
    SKIPPED = "skipped"  # not needed for this config, or not this platform
    INFO = "info"  # needed, listed for the reader, not probed


class ClusterUnreachable(Exception):
    """No usable kubeconfig or in-cluster config; nothing was checked."""


@dataclass(frozen=True)
class PrereqResult:
    status: PrereqStatus
    message: str


@dataclass(frozen=True)
class DeploymentView:
    """What a check needs to know about one Deployment."""

    namespace: str
    name: str
    ready_replicas: int
    labels: dict[str, str] = field(default_factory=dict)


class ClusterReader(Protocol):
    """Read-only cluster access for the checks."""

    def crd_names(self) -> set[str]: ...

    def storage_class_names(self) -> set[str]: ...

    def default_storage_class_names(self) -> set[str]: ...

    def pvc_storage_class(self, namespace: str, name: str) -> str | None:
        """The StorageClass of an existing PVC ("" for none), or None when
        there is no such PVC."""
        ...

    def deployments(
        self, label_selector: str, namespace: str | None = None
    ) -> list[DeploymentView]: ...

    def running_pod_exists(self, label_selector: str) -> bool: ...

    def cluster_role_exists(self, name: str) -> bool | None: ...

    def is_openshift(self) -> bool: ...

    def s3_probe(self, cfg: LakebenchConfig) -> tuple[bool, str]: ...


@dataclass(frozen=True)
class Prereq:
    """One prerequisite. ``applies`` needs only the config, so an offline
    caller (``plan --offline``) can list what a config needs without a
    cluster. ``component`` names the shared component a cluster admin
    installs for it with ``admin install --component``, if any."""

    id: str
    title: str
    when: str
    applies: Callable[[LakebenchConfig], bool]
    check: Callable[[LakebenchConfig, ClusterReader], PrereqResult]
    fix: str
    doc: str
    component: str | None = None
    # "deploy": only deploy depends on it, so the run preflight leaves it to
    # deploy's own step (a run on a deployed system must not fail on it).
    phase: str = "run"


@dataclass(frozen=True)
class PrereqOutcome:
    prereq: Prereq
    result: PrereqResult


def _ok(msg: str) -> PrereqResult:
    return PrereqResult(PrereqStatus.OK, msg)


def _warn(msg: str) -> PrereqResult:
    return PrereqResult(PrereqStatus.WARN, msg)


def _fail(msg: str) -> PrereqResult:
    return PrereqResult(PrereqStatus.FAIL, msg)


# -- scratch StorageClass ------------------------------------------------------


def _scratch_applies(cfg: LakebenchConfig) -> bool:
    from lakebench.config.autosizer import scratch_will_be_enabled

    return bool(scratch_will_be_enabled(cfg) and cfg.platform.storage.scratch.storage_class)


def _check_scratch(cfg: LakebenchConfig, r: ClusterReader) -> PrereqResult:
    name = cfg.platform.storage.scratch.storage_class
    if name in r.storage_class_names():
        return _ok(f"StorageClass {name} exists")
    return _fail(f"StorageClass {name} not found")


# -- dependency server --------------------------------------------------------

_DEFAULT_SC_ANNOTATIONS = (
    "storageclass.kubernetes.io/is-default-class",
    "storageclass.beta.kubernetes.io/is-default-class",
)


def _refused(e: Exception) -> bool:
    return getattr(e, "status", None) == 403


def _check_deps_storage_class(cfg: LakebenchConfig, r: ClusterReader) -> PrereqResult:
    from lakebench.deps.manifest import PVC_NAME

    name = cfg.platform.deps.storage_class
    try:
        # The class is read only when the PVC is created: an existing PVC
        # keeps the set where it is, whatever the cluster's classes are now.
        have = r.pvc_storage_class(cfg.get_namespace(), PVC_NAME)
    except Exception as e:  # noqa: BLE001 -- only a refused read is softened
        if not _refused(e):
            raise
        have = None  # unreadable: check the class as if the PVC were new
    if have is not None:
        if name and name != have:
            return _warn(
                f"PVC {PVC_NAME} exists on StorageClass {have or '(none)'}, not "
                f"{name}; the set stays there until the PVC is deleted"
            )
        return _ok(f"PVC {PVC_NAME} exists on StorageClass {have or '(none)'}")
    try:
        if name:
            if name in r.storage_class_names():
                return _ok(f"StorageClass {name} exists (platform.deps.storage_class)")
            return _fail(f"StorageClass {name} not found (platform.deps.storage_class)")
        defaults = sorted(r.default_storage_class_names())
    except Exception as e:  # noqa: BLE001 -- only a refused read is softened
        if not _refused(e):
            raise
        return _warn("cannot read StorageClasses (403); deploy checks the class")
    if not defaults:
        return _fail(
            "platform.deps.storage_class is empty and the cluster has no default StorageClass"
        )
    if len(defaults) > 1:
        return _warn(
            f"several default StorageClasses ({', '.join(defaults)}): Kubernetes 1.26 and "
            "later give the PVC the newest, older releases refuse it; set "
            "platform.deps.storage_class"
        )
    return _ok(f"cluster default StorageClass {defaults[0]}")


def _check_egress_hosts(cfg: LakebenchConfig, r: ClusterReader) -> PrereqResult:
    # Reachability from the cluster can only be seen from a pod there, and
    # checks never create one. The resolve itself reports an unreachable host.
    from lakebench.deps.request import egress_hosts

    try:
        hosts = ", ".join(egress_hosts(cfg))
    except ValueError as e:
        return PrereqResult(PrereqStatus.INFO, f"not probed; hosts not listed: {e}")
    return PrereqResult(PrereqStatus.INFO, f"not probed; the lb-deps resolve reads {hosts}")


# -- Spark Operator ------------------------------------------------------------

SPARK_APP_CRD = "sparkapplications.sparkoperator.k8s.io"
_SPARK_OPERATOR_SELECTOR = "app.kubernetes.io/name=spark-operator"
SUPPORTED_SPARK_OPERATOR_MAJOR = 2


def _chart_version(labels: dict[str, str]) -> str | None:
    m = re.search(r"-(\d+\.\d+\.\d+[0-9A-Za-z.+-]*)$", labels.get("helm.sh/chart", ""))
    if m:
        return m.group(1)
    return labels.get("app.kubernetes.io/version") or None


def _check_spark_operator(cfg: LakebenchConfig, r: ClusterReader) -> PrereqResult:
    op = cfg.platform.compute.spark.operator
    if SPARK_APP_CRD not in r.crd_names():
        return _fail("SparkApplication CRD not found")
    # The operator deploy and the watch-list edits use: the configured
    # namespace only, never a look-alike release elsewhere on the cluster.
    deps = r.deployments(_SPARK_OPERATOR_SELECTOR, namespace=op.namespace)
    # The 2.x chart labels the controller (charts/.../controller/_helpers.tpl).
    controllers = [d for d in deps if d.labels.get("app.kubernetes.io/component") == "controller"]
    if not controllers:
        return _fail(
            f"SparkApplication CRD found but no Spark Operator controller Deployment in "
            f"namespace {op.namespace} (platform.compute.spark.operator.namespace)"
        )
    ready = [d for d in controllers if d.ready_replicas > 0]
    if not ready:
        names = ", ".join(f"{d.namespace}/{d.name}" for d in controllers)
        return _fail(f"Spark Operator controller not ready ({names})")
    d = ready[0]
    version = _chart_version(d.labels)
    where = f"{d.namespace}/{d.name}"
    if version is None:
        return _ok(f"Spark Operator ready ({where}, chart version unknown)")
    major = version.split(".", 1)[0]
    if not major.isdigit() or int(major) != SUPPORTED_SPARK_OPERATOR_MAJOR:
        return _fail(f"Spark Operator chart {version} at {where} is not a supported 2.x release")
    return _ok(f"Spark Operator {version} ready ({where})")


# -- Stackable -----------------------------------------------------------------

STACKABLE_CRDS: dict[str, str] = {
    "hiveclusters.hive.stackable.tech": "hive-operator",
    "secretclasses.secrets.stackable.tech": "secret-operator",
}
# Installed with them (hive deployer _INSTALL_ORDER); a missing one is a
# warning, because their pod labels are not checked on every SDP release.
STACKABLE_SUPPORT_OPERATORS = ("listener-operator", "commons-operator")


def _hive_applies(cfg: LakebenchConfig) -> bool:
    return bool(cfg.architecture.catalog.type.value == "hive")


def _check_stackable(cfg: LakebenchConfig, r: ClusterReader) -> PrereqResult:
    crds = r.crd_names()
    missing_crd = [op for crd, op in STACKABLE_CRDS.items() if crd not in crds]
    if missing_crd:
        return _fail(f"Stackable CRDs missing for {', '.join(missing_crd)}")
    not_running = [
        op
        for op in STACKABLE_CRDS.values()
        if not r.running_pod_exists(f"app.kubernetes.io/name={op}")
    ]
    if not_running:
        return _fail(
            f"Stackable CRDs found but no running pod for {', '.join(not_running)} "
            "(helm leaves CRDs behind after an uninstall)"
        )
    support_missing = [
        op
        for op in STACKABLE_SUPPORT_OPERATORS
        if not r.running_pod_exists(f"app.kubernetes.io/name={op}")
    ]
    if support_missing:
        return _warn(
            "hive-operator and secret-operator running; no running pod found for "
            f"{', '.join(support_missing)}"
        )
    return _ok("Stackable hive, secret, listener and commons operators running")


# -- observability stack -------------------------------------------------------

OBSERVABILITY_RELEASE = "lakebench-observability"


def _observability_applies(cfg: LakebenchConfig) -> bool:
    return bool(cfg.observability.enabled)


def _check_observability(cfg: LakebenchConfig, r: ClusterReader) -> PrereqResult:
    deps = r.deployments(f"release={OBSERVABILITY_RELEASE}")
    if not deps:
        return _fail(f"no {OBSERVABILITY_RELEASE} release found")
    namespaces = ", ".join(sorted({d.namespace for d in deps}))
    not_ready = [d.name for d in deps if d.ready_replicas < 1]
    if not_ready:
        return _warn(
            f"shared observability stack in {namespaces} has no ready replica in "
            f"{', '.join(not_ready)}"
        )
    return _ok(f"shared observability stack ready in {namespaces}")


# -- OpenShift SCC -------------------------------------------------------------

ANYUID_CLUSTER_ROLE = "system:openshift:scc:anyuid"


def _check_scc_clusterrole(cfg: LakebenchConfig, r: ClusterReader) -> PrereqResult:
    if not r.is_openshift():
        return PrereqResult(PrereqStatus.SKIPPED, "not OpenShift")
    exists = r.cluster_role_exists(ANYUID_CLUSTER_ROLE)
    if exists is True:
        return _ok(f"ClusterRole {ANYUID_CLUSTER_ROLE} exists")
    if exists is None:
        return _warn(
            f"cannot read ClusterRole {ANYUID_CLUSTER_ROLE}; deploy still binds it, and "
            "fails with the admin command if the bind is refused"
        )
    return _fail(
        f"ClusterRole {ANYUID_CLUSTER_ROLE} not found; OpenShift before 4.10 is not "
        "supported, and deploy cannot grant the anyuid SCC without it"
    )


# -- S3 ------------------------------------------------------------------------


def _check_s3(cfg: LakebenchConfig, r: ClusterReader) -> PrereqResult:
    s3 = cfg.platform.storage.s3
    if not s3.endpoint:
        return _fail("platform.storage.s3.endpoint is not set")
    if not (s3.access_key and s3.secret_key):
        # secret_ref never supplies credentials: deploy writes the S3 Secret
        # from the inline keys only, and the loader refuses secret_ref alone.
        return _fail("no S3 credentials (access_key and secret_key)")
    ok, msg = r.s3_probe(cfg)
    return _ok(msg) if ok else _fail(msg)


def _schema_default(model: str) -> str:
    from lakebench.config import schema

    return str(getattr(schema, model)().version)


_SPARK_OPERATOR_DEFAULT = _schema_default("SparkOperatorConfig")
_STACKABLE_DEFAULT = _schema_default("StackableOperatorConfig")

PREREQS: tuple[Prereq, ...] = (
    Prereq(
        id="scratch-storage-class",
        component="scratch-storage-class",
        title="Scratch StorageClass",
        when="`platform.storage.scratch.enabled: true`, or a batch run at scale 50 and above with it unset",
        applies=_scratch_applies,
        check=_check_scratch,
        fix=(
            "A cluster admin runs `lakebench admin install --component scratch-storage-class "
            "<config>` once per cluster, or set `platform.storage.scratch.enabled: false`."
        ),
        doc=(
            "Spark executors put shuffle and spill on per-executor PVCs from the StorageClass "
            "named by `platform.storage.scratch.storage_class` (default `px-csi-scratch`, "
            "Portworx with one replica). The StorageClass is shared cluster infrastructure: "
            "Lakebench uses it and never creates or deletes it during `deploy` or `destroy`."
        ),
    ),
    Prereq(
        id="spark-operator",
        component="spark-operator",
        title="Kubeflow Spark Operator 2.x",
        when="Always",
        applies=lambda cfg: True,
        check=_check_spark_operator,
        fix=(
            "A cluster admin runs `lakebench admin install --component spark-operator "
            "<config>` once per cluster. Do not install or upgrade it with a raw `helm` "
            "command: the managed path holds the cluster lease and never resets the "
            "operator's namespace watch list."
        ),
        doc=(
            "Spark jobs are `SparkApplication` resources run by one shared Kubeflow Spark "
            "Operator. The check needs the `SparkApplication` CRD, a controller Deployment "
            f"with a ready replica, and a 2.x chart ({_SPARK_OPERATOR_DEFAULT} is the tested "
            "release; 1.x cannot mount the scripts volume). `deploy` adds its namespace to the "
            "operator's watch "
            "list under the cluster lease; never edit `spark.jobNamespaces` by hand."
        ),
    ),
    Prereq(
        id="stackable",
        component="stackable",
        title="Stackable operators (Hive catalog)",
        when="`architecture.catalog.type: hive`",
        applies=_hive_applies,
        check=_check_stackable,
        fix=(
            "A cluster admin runs `lakebench admin install --component stackable <config>` "
            f"once per cluster (the commons, listener, secret and hive operators, SDP "
            f"{_STACKABLE_DEFAULT}), or use a Polaris recipe, which needs no operator."
        ),
        doc=(
            "The Hive Metastore is a Stackable `HiveCluster`. The check needs the "
            "`HiveCluster` and `SecretClass` CRDs and a running hive-operator and "
            "secret-operator pod. CRDs alone are not enough, because helm leaves them "
            "behind when an operator is uninstalled."
        ),
    ),
    Prereq(
        id="observability-stack",
        component="observability",
        title="Shared observability stack",
        when="`observability.enabled: true`",
        applies=_observability_applies,
        check=_check_observability,
        fix=(
            "A cluster admin runs `lakebench admin install --component observability "
            "<config>` once per cluster, or set `observability.enabled: false`. To remove the "
            f"stack later, a cluster admin runs `helm uninstall {OBSERVABILITY_RELEASE} -n "
            f"{OBSERVABILITY_RELEASE}` once no deployment uses it."
        ),
        doc=(
            "Metrics go to one shared Prometheus and Grafana (kube-prometheus-stack, release "
            f"`{OBSERVABILITY_RELEASE}`). Each deployment adds only its own PodMonitors; "
            "`destroy` never removes the shared release."
        ),
    ),
    Prereq(
        id="openshift-scc-clusterrole",
        title="OpenShift anyuid SCC",
        when="OpenShift",
        applies=lambda cfg: True,
        check=_check_scc_clusterrole,
        fix=(
            "If `deploy` stops with `cannot grant SCC anyuid to SA <sa> in namespace <ns>`, "
            "a cluster admin runs `oc adm policy add-scc-to-user anyuid -z <sa> -n <ns>` "
            "for `lakebench-spark-runner` and `lakebench-postgres`, then `deploy` is re-run."
        ),
        doc=(
            "Spark pods run as UID 185 and PostgreSQL as UID 999, so on OpenShift both "
            "ServiceAccounts need the `anyuid` SCC. The Spark Operator's ServiceAccounts get it "
            "at operator install.\n\n"
            "- `deploy` grants it as `oc adm policy add-scc-to-user` does on OpenShift 4.10 "
            f"and later: the RoleBinding `{ANYUID_CLUSTER_ROLE}` in the ServiceAccount's "
            "namespace, bound to the ClusterRole of the same name.\n"
            "- Before writing, a LocalSubjectAccessReview checks whether an admin already "
            "granted it. Afterwards `deploy` checks that the grant took effect.\n"
            "- The deploying user needs to create RoleBindings in the namespace and to bind "
            "that ClusterRole.\n"
            "- A grant that cannot be made fails the deploy step. It is never a warning.\n"
            "- OpenShift before 4.10 has no such ClusterRole and is not supported."
        ),
    ),
    Prereq(
        id="deps-storage-class",
        title="Dependency server StorageClass",
        when="Always",
        phase="deploy",
        applies=lambda cfg: True,
        check=_check_deps_storage_class,
        fix=(
            "Set `platform.deps.storage_class` to an existing StorageClass, or have a cluster "
            "admin mark one as the cluster default. Prefer a replicated one."
        ),
        doc=(
            "Each deployment runs its own dependency server, `lb-deps`. It keeps the resolved "
            "jars, wheels and DuckDB extensions on a 5Gi ReadWriteOnce PVC, `lb-deps-data`.\n\n"
            "- The class is `platform.deps.storage_class`, or the cluster default when that "
            "is empty.\n"
            "- The volume must be writable by UID 185 through `fsGroup`.\n"
            "- Use a replicated class. If the volume is lost with its node, the server pod "
            "stays Pending and every `run` stops. Delete the PVC and re-run `deploy`, which "
            "resolves the set again.\n"
            "- The class is read only when the PVC is created. To move a set, delete the PVC "
            "and re-run `deploy`.\n"
            "- When the PVC exists, the check reports its class and nothing else."
        ),
    ),
    Prereq(
        id="egress-hosts",
        title="Egress for the dependency resolve",
        when="Always",
        phase="deploy",
        applies=lambda cfg: True,
        check=_check_egress_hosts,
        fix=(
            "Allow egress from the deployment's namespace to the listed hosts during "
            "`deploy`, or point `platform.deps.maven_repository`, `platform.deps.pypi_index` "
            "and `platform.deps.duckdb_extension_repository` at mirrors the cluster can reach."
        ),
        doc=(
            "`deploy` resolves every jar, wheel and DuckDB extension once, in the `lb-deps` "
            "pod. After that no pod fetches a dependency from outside the deployment.\n\n"
            "- Sources: Maven Central and its Google mirror; PyPI (pypi.org and "
            "files.pythonhosted.org) for the AML reference and DuckDB wheels; "
            "extensions.duckdb.org for DuckDB.\n"
            "- Spark jobs, Spark Thrift and DuckDB read the set from `lb-deps`.\n"
            "- Egress is needed only at a deploy that resolves again: a new image, version or "
            "mirror, or a new, lost or damaged set.\n"
            "- The check lists the hosts this config reads and does not probe them.\n"
            "- An unreachable host fails the `deps` step of `deploy`, naming the repository "
            "and the mirror keys. A proxy that answers with an error fails it, naming the "
            "artifact and the repository.\n\n"
            "On a cluster without that egress, set the mirror keys under `platform.deps`:\n\n"
            "- `maven_repository` becomes the only Maven repository.\n"
            "- `pypi_index` replaces pypi.org as a PyPI simple index.\n"
            "- `duckdb_extension_repository` replaces extensions.duckdb.org.\n\n"
            "Mirrors are read anonymously, over HTTP or over HTTPS with a publicly trusted "
            "certificate. Mirror credentials and a private CA are not supported. Changing a "
            "mirror re-resolves at the next `deploy`. A mirror that serves the same bytes "
            "gives the same set hash, so runs stay like-for-like. Other bytes give a different "
            "set, and those runs are not like-for-like.\n\n"
            "Image pulls are separate: the nodes pull the images named under `images` (and "
            "the Stackable Hive image for a Hive catalog) from their registries at every "
            "pod start."
        ),
    ),
    Prereq(
        id="s3-reachable-and-credentials",
        title="S3 endpoint and credentials",
        when="Always",
        applies=lambda cfg: True,
        check=_check_s3,
        fix=(
            "Set `platform.storage.s3.endpoint`, `access_key` and `secret_key`, and "
            "check the endpoint is reachable from this machine; "
            "`lakebench config storage <config>` probes the backend."
        ),
        doc=(
            "Lakebench needs an S3-compatible endpoint and credentials that can list, create "
            "and write buckets. The check lists buckets with the configured credentials."
        ),
    ),
)


def run_prereqs(
    cfg: LakebenchConfig, reader: ClusterReader | None = None, *, for_run: bool = False
) -> list[PrereqOutcome]:
    """Run every applicable check, read-only. A check that raises is UNKNOWN
    ("could not check: ..."); one that does not apply is SKIPPED, and so is a
    deploy-phase entry when ``for_run`` (the run preflight). Raises
    :class:`ClusterUnreachable` when no reader is given and none can be built."""
    if reader is None:
        reader = KubeClusterReader(cfg)
    out: list[PrereqOutcome] = []
    for p in PREREQS:
        if not p.applies(cfg):
            res = PrereqResult(PrereqStatus.SKIPPED, "not needed for this config")
        elif for_run and p.phase == "deploy":
            res = PrereqResult(PrereqStatus.SKIPPED, "checked by deploy")
        else:
            try:
                res = p.check(cfg, reader)
            except Exception as e:  # noqa: BLE001
                text = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
                res = PrereqResult(PrereqStatus.UNKNOWN, f"could not check: {text}")
        out.append(PrereqOutcome(p, res))
    return out


class KubeClusterReader:
    """:class:`ClusterReader` over the Kubernetes API and an S3 probe.

    Pins the client config through ``ClusterTarget`` as ``K8sClient`` does
    (the configured context, else the kubeconfig's current context by name,
    in-cluster only without a kubeconfig), so both talk to the same cluster.
    Every call is a read with a timeout.
    """

    TIMEOUT_S = 15

    def __init__(self, cfg: LakebenchConfig, *, load_config: bool = True):
        """``load_config=False`` when the caller already loaded the client
        config (for example through ``get_k8s_client``)."""
        from kubernetes import client

        from lakebench.k8s.target import ClusterTarget, ContextConflictError

        try:
            if load_config:
                ClusterTarget.resolve(cfg).activate()
        except ContextConflictError:
            raise  # the kubeconfig changed under the command: a refusal
        except Exception as e:  # noqa: BLE001
            raise ClusterUnreachable(f"no usable Kubernetes config: {e}") from None
        self._client = client
        self._crds: set[str] | None = None

    def crd_names(self) -> set[str]:
        if self._crds is None:
            api = self._client.ApiextensionsV1Api()
            items = api.list_custom_resource_definition(_request_timeout=self.TIMEOUT_S).items
            self._crds = {c.metadata.name for c in items}
        return self._crds

    def storage_class_names(self) -> set[str]:
        api = self._client.StorageV1Api()
        return {
            sc.metadata.name for sc in api.list_storage_class(_request_timeout=self.TIMEOUT_S).items
        }

    def default_storage_class_names(self) -> set[str]:
        api = self._client.StorageV1Api()
        return {
            sc.metadata.name
            for sc in api.list_storage_class(_request_timeout=self.TIMEOUT_S).items
            if any(
                (sc.metadata.annotations or {}).get(a) == "true" for a in _DEFAULT_SC_ANNOTATIONS
            )
        }

    def pvc_storage_class(self, namespace: str, name: str) -> str | None:
        from kubernetes.client.rest import ApiException

        try:
            pvc = self._client.CoreV1Api().read_namespaced_persistent_volume_claim(
                name, namespace, _request_timeout=self.TIMEOUT_S
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise
        return str(pvc.spec.storage_class_name or "")

    def deployments(
        self, label_selector: str, namespace: str | None = None
    ) -> list[DeploymentView]:
        api = self._client.AppsV1Api()
        if namespace:
            items = api.list_namespaced_deployment(
                namespace, label_selector=label_selector, _request_timeout=self.TIMEOUT_S
            ).items
        else:
            items = api.list_deployment_for_all_namespaces(
                label_selector=label_selector, _request_timeout=self.TIMEOUT_S
            ).items
        return [
            DeploymentView(
                namespace=d.metadata.namespace,
                name=d.metadata.name,
                ready_replicas=int((d.status and d.status.ready_replicas) or 0),
                labels=dict(d.metadata.labels or {}),
            )
            for d in items
        ]

    def running_pod_exists(self, label_selector: str) -> bool:
        pods = self._client.CoreV1Api().list_pod_for_all_namespaces(
            label_selector=label_selector,
            field_selector="status.phase=Running",
            limit=1,
            _request_timeout=self.TIMEOUT_S,
        )
        return bool(pods.items)

    def cluster_role_exists(self, name: str) -> bool | None:
        from kubernetes.client.rest import ApiException

        try:
            self._client.RbacAuthorizationV1Api().read_cluster_role(
                name, _request_timeout=self.TIMEOUT_S
            )
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            if e.status == 403:
                return None
            raise

    def is_openshift(self) -> bool:
        groups = self._client.ApisApi().get_api_versions(_request_timeout=self.TIMEOUT_S).groups
        return any(g.name == "security.openshift.io" for g in groups or [])

    def s3_probe(self, cfg: LakebenchConfig) -> tuple[bool, str]:
        from lakebench.s3 import test_s3_connectivity

        s3 = cfg.platform.storage.s3
        result: dict[str, Any] = test_s3_connectivity(
            endpoint=s3.endpoint,
            access_key=s3.access_key,
            secret_key=s3.secret_key,
            region=s3.region,
            path_style=s3.path_style,
            ca_cert=s3.ca_cert,
            verify_ssl=s3.verify_ssl,
        )
        if result.get("overall_success"):
            return True, "S3 endpoint reachable and credentials valid (ListBuckets OK)"
        msg = result.get("credentials_message") or result.get("endpoint_message") or "unknown"
        return False, f"S3 check failed: {msg}"


DOC_HEADER = """\
# Prerequisites

<!-- Generated by scripts/gen_prereq_docs.py from src/lakebench/deploy/prereqs.py.
     Do not edit by hand: scripts/gen_docs.py --check fails on any difference. -->

What a cluster needs before `lakebench deploy` can succeed. Each entry is a
read-only check in `src/lakebench/deploy/prereqs.py`, and the preflight of
`lakebench run` runs these same checks, except the ones marked as checked at
deploy, so this page and the checks cannot disagree. Besides these, `kubectl` and `helm` must be on `PATH`. `oc` is not
needed: Lakebench makes its OpenShift SCC grants through the Kubernetes API.
"""


def render_markdown() -> str:
    """``docs/prerequisites.md``, from the registry."""
    lines = [DOC_HEADER]
    lines.append("| Check | Needed when |\n|---|---|")
    for p in PREREQS:
        lines.append(f"| [{p.title}](#{p.id}) | {p.when} |")
    lines.append("")
    for p in PREREQS:
        lines.append(f'<a id="{p.id}"></a>\n\n## {p.title}\n')
        at = " Checked at deploy, not by the `run` preflight." if p.phase == "deploy" else ""
        lines.append(f"Check id `{p.id}`. Needed when: {p.when}.{at}\n")
        lines.append(f"{p.doc}\n")
        lines.append(f"**Fix:** {p.fix}\n")
    return "\n".join(lines).rstrip() + "\n"
