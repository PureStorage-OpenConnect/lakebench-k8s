"""The Category-1 teardown registry (cluster-safety 14, DESIGN ch01 section 11).

With ``create_namespace: false`` the namespace survives ``destroy``, so every
object Lakebench creates in it must be deleted by name or by a selector, or
it outlives the deployment. ``CATEGORY1_OBJECTS`` lists each one:

- an entry whose ``step`` is a component (``postgres``, ``trino``, ...) is
  deleted by that component's own teardown in ``deploy/destroy.py``; the
  entry records it so the registry is complete without moving that code;
- an entry whose ``step`` is ``category1`` is deleted by the ``category1``
  destroy step, after every component step and before the namespace step,
  in registry order (a 404 is success).

``CATEGORY1_ANNOTATIONS`` are the namespace annotations that step removes
when the namespace survives. The deployment's identity annotations
(``lakebench.deployment/name``, the deploy nonce, the created-buckets
record) are not among them: a destroy that stops half way and is re-run
needs them.

``KEPT_ON_DESTROY`` names objects that must outlive a destroy of a
surviving namespace, each with its reason.

Every WI that makes a namespaced object adds its entry in the same change;
``tests/test_category1_teardown.py`` runs deploy and the run-time creators
under the recording fake and fails on an object nothing deletes. Objects
Spark creates at run time (driver Services, ``spark-drv-*`` ConfigMaps,
OnDemand scratch PVCs) carry ownerReferences to the driver pod and are
garbage collected after the SparkApplication delete; they are not listed.

Selectors in ``category1`` entries name the deployment with
``app.kubernetes.io/instance={name}``, the label every Lakebench template
already carries (v1.6 objects have no ``lakebench.io/deployment`` label).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = [
    "CATEGORY1_ANNOTATIONS",
    "CATEGORY1_OBJECTS",
    "CATEGORY1_STEP",
    "KEPT_ON_DESTROY",
    "Cat1Entry",
    "KeptObject",
]

CATEGORY1_STEP = "category1"


@dataclass(frozen=True)
class Cat1Entry:
    """One kind of object Lakebench creates in the deployment's namespace.

    ``api`` and ``kind`` name the client and plural (``core_v1`` and
    ``configmaps``; ``custom`` with ``group`` and ``version``). Exactly one
    of ``name`` and ``label_selector`` is set; a selector may use ``{name}``
    for the deployment name. In a component step's entry ``name`` may be a
    pattern that step matches (``data-lakebench-postgres-<n>``, ``*`` for
    every object of the kind). ``owner_wi`` is the work item that creates
    the object (``pre-1.7`` for objects older than the registry), ``step``
    the destroy step that deletes it.
    """

    api: str
    kind: str
    name: str | None = None
    label_selector: str | None = None
    owner_wi: str = ""
    step: str = ""
    # Custom objects only (``api="custom"``).
    group: str = ""
    version: str = ""
    # The config condition under which deploy creates the object: "" always,
    # "observability" when observability.enabled. The category1 step still
    # tries an entry whose condition is off (it may have been on at deploy),
    # but ignores a 403 for it: a user without rights on a kind the
    # deployment never used (PodMonitor) must not fail every destroy.
    when: str = ""

    def matches(self, kind: str, name: str, labels: dict[str, str] | None, deployment: str) -> bool:
        """Whether this entry covers the object ``kind``/``name`` with ``labels``.

        ``name`` may be exact, ``*`` (every object of the kind) or carry
        ``<n>`` for an ordinal; a selector is ``k=v`` pairs, ``{name}`` the
        deployment.
        """
        if kind != self.kind:
            return False
        if self.name is not None:
            if self.name == "*":
                return True
            pattern = re.escape(self.name).replace(re.escape("<n>"), r"\d+")
            return re.fullmatch(pattern, name) is not None
        assert self.label_selector is not None
        have = labels or {}
        for term in self.label_selector.format(name=deployment).split(","):
            key, _, value = term.partition("=")
            if have.get(key.strip()) != value.strip():
                return False
        return True


@dataclass(frozen=True)
class KeptObject:
    """An object a destroy of a surviving namespace leaves in place, and why."""

    kind: str
    name: str
    owner_wi: str
    reason: str


_PRE = "pre-1.7"
# The work item that adds the in-namespace dependency server (v1.7).
_DEPS = "deps-server"


def _named(api: str, kind: str, step: str, *names: str, owner: str = _PRE) -> list[Cat1Entry]:
    return [Cat1Entry(api, kind, name=n, owner_wi=owner, step=step) for n in names]


CATEGORY1_OBJECTS: tuple[Cat1Entry, ...] = (
    # spark-jobs: every SparkApplication in the namespace (Spark's runtime
    # objects are garbage collected after it).
    Cat1Entry("custom", "sparkapplications", name="*", owner_wi=_PRE, step="spark-jobs"),
    # datagen-jobs: Lakebench's Jobs by label.
    Cat1Entry(
        "batch_v1",
        "jobs",
        label_selector="app.kubernetes.io/managed-by=lakebench",
        owner_wi=_PRE,
        step="datagen-jobs",
    ),
    # Query engines.
    *_named("apps_v1", "deployments", "trino", "lakebench-trino-coordinator"),
    *_named("apps_v1", "statefulsets", "trino", "lakebench-trino-worker"),
    *_named("core_v1", "services", "trino", "lakebench-trino", "lakebench-trino-worker"),
    *_named("core_v1", "configmaps", "trino", "lakebench-trino-config"),
    # The worker StatefulSet's claims, made by its controller when a worker
    # storage class is set; the trino step selects them by component.
    Cat1Entry(
        "core_v1",
        "persistentvolumeclaims",
        label_selector="app.kubernetes.io/component=trino-worker",
        owner_wi=_PRE,
        step="trino",
    ),
    *_named("apps_v1", "deployments", "spark-thrift", "lakebench-spark-thrift"),
    *_named("core_v1", "services", "spark-thrift", "lakebench-spark-thrift"),
    *_named("apps_v1", "deployments", "duckdb", "lakebench-duckdb"),
    *_named("core_v1", "services", "duckdb", "lakebench-duckdb"),
    # Catalogs.
    Cat1Entry(
        "custom",
        "hiveclusters",
        name="lakebench-hive",
        owner_wi=_PRE,
        step="hive",
        group="hive.stackable.tech",
        version="v1alpha1",
    ),
    *_named("core_v1", "services", "hive", "lakebench-hive-metastore"),
    *_named("apps_v1", "deployments", "hive", "lakebench-hive-metastore"),
    *_named("apps_v1", "deployments", "polaris", "lakebench-polaris"),
    *_named("core_v1", "services", "polaris", "lakebench-polaris"),
    *_named("batch_v1", "jobs", "polaris", "lakebench-polaris-bootstrap"),
    *_named("core_v1", "configmaps", "polaris", "lakebench-polaris-config"),
    *_named("apps_v1", "deployments", "unity", "lakebench-unity"),
    *_named("core_v1", "services", "unity", "lakebench-unity"),
    *_named("batch_v1", "jobs", "unity", "lakebench-unity-bootstrap"),
    *_named("core_v1", "configmaps", "unity", "lakebench-unity"),
    # PostgreSQL (its claims by exact name: the claim template has no labels).
    *_named("apps_v1", "statefulsets", "postgres", "lakebench-postgres"),
    *_named("core_v1", "services", "postgres", "lakebench-postgres"),
    *_named(
        "core_v1",
        "persistentvolumeclaims",
        "postgres",
        "data-lakebench-postgres-<n>",
        owner="SD-14",
    ),
    # Scripts ConfigMaps: the per-role maps and the v1.6 single map, by
    # scripts_maps.scripts_label_selector, which keys on
    # app.kubernetes.io/instance (v1.6 maps carry no lakebench.io/deployment
    # label).
    Cat1Entry(
        "core_v1",
        "configmaps",
        label_selector=(
            "app.kubernetes.io/component=spark-scripts,"
            "app.kubernetes.io/managed-by=lakebench,"
            "app.kubernetes.io/instance={name}"
        ),
        owner_wi="SD-8",
        step="spark-scripts",
    ),
    # RBAC and Secrets.
    *_named("core_v1", "serviceaccounts", "rbac", "lakebench-spark-runner"),
    *_named("rbac_v1", "roles", "rbac", "lakebench-spark-runner"),
    *_named("rbac_v1", "rolebindings", "rbac", "lakebench-spark-runner"),
    # The rbac step keeps the three DB and Polaris client Secrets while the
    # Postgres PVC survives, because they belong to the data in it.
    *_named(
        "core_v1",
        "secrets",
        "rbac",
        "lakebench-s3-credentials",
        "lakebench-postgres-secret",
        "lakebench-ca-certificate",
        "lakebench-polaris-db",
        "lakebench-polaris-client",
    ),
    # The category1 step: objects no component step deleted. The
    # deletes do not wait; a claim still mounted by a terminating pod is held
    # by the pvc-protection finalizer until the pod is gone.
    *_named("core_v1", "serviceaccounts", CATEGORY1_STEP, "lakebench-postgres"),
    *(
        Cat1Entry(
            api,
            kind,
            name="lakebench-pushgateway",
            owner_wi=_PRE,
            step=CATEGORY1_STEP,
            when="observability",
        )
        for api, kind in (
            ("apps_v1", "deployments"),
            ("core_v1", "services"),
            ("core_v1", "persistentvolumeclaims"),
        )
    ),
    Cat1Entry(
        "core_v1",
        "configmaps",
        name="lakebench-prometheus-config",
        owner_wi=_PRE,
        step=CATEGORY1_STEP,
        when="observability",
    ),
    *(
        Cat1Entry(
            "custom",
            "podmonitors",
            name=n,
            owner_wi=_PRE,
            step=CATEGORY1_STEP,
            group="monitoring.coreos.com",
            version="v1",
            when="observability",
        )
        for n in (
            "lakebench-spark-driver",
            "lakebench-spark-executor",
            "lakebench-trino-coordinator",
            "lakebench-trino-worker",
            "lakebench-pushgateway",
        )
    ),
    # The dependency server's objects: last in the category1 step, and the
    # Deployment before its claim so pvc-protection lets the claim go once
    # the pod is gone.
    *_named("apps_v1", "deployments", CATEGORY1_STEP, "lb-deps", owner=_DEPS),
    *_named("core_v1", "services", CATEGORY1_STEP, "lb-deps", owner=_DEPS),
    *_named("core_v1", "configmaps", CATEGORY1_STEP, "lb-deps-manifest", owner=_DEPS),
    # The tools maps are named by their request (lb-deps-tools-<16 hex>).
    Cat1Entry(
        "core_v1",
        "configmaps",
        label_selector=(
            "app.kubernetes.io/component=deps,lakebench.io/deps-role=tools,"
            "app.kubernetes.io/instance={name}"
        ),
        owner_wi=_DEPS,
        step=CATEGORY1_STEP,
    ),
    *_named("core_v1", "persistentvolumeclaims", CATEGORY1_STEP, "lb-deps-data", owner=_DEPS),
)

# Namespace annotations the category1 step removes from a surviving
# namespace. ``state-schema`` is written by deploy beside the deploy nonce
# (DESIGN ch01 d3 N3); removing an absent key is a no-op.
CATEGORY1_ANNOTATIONS: tuple[str, ...] = (
    "lakebench.deployment/state-schema",
    "lakebench.deployment/deps-set",  # the dependency server's verified set
)

KEPT_ON_DESTROY: tuple[KeptObject, ...] = (
    KeptObject(
        "configmaps",
        "lakebench-silver-state",
        _PRE,
        "holds the silver rebuild-epoch counters, which must never go back while "
        "table data written under them can outlive destroy (bucket cleanup off, or "
        "buckets this deployment does not own); a reset counter makes Delta skip "
        "writes as already committed. Deploy backfills it and keeps its values. "
        "Its bronze_data_clock survives too, so a later deploy's silver stages read "
        "the old clock until bronze-verify rewrites it",
    ),
)
