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


@dataclass(frozen=True)
class KeptObject:
    """An object a destroy of a surviving namespace leaves in place, and why."""

    kind: str
    name: str
    owner_wi: str
    reason: str


_PRE = "pre-1.7"


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
    *_named("apps_v1", "deployments", "polaris", "lakebench-polaris"),
    *_named("core_v1", "services", "polaris", "lakebench-polaris"),
    *_named("batch_v1", "jobs", "polaris", "lakebench-polaris-bootstrap"),
    *_named("core_v1", "configmaps", "polaris", "lakebench-polaris-config"),
    *_named("apps_v1", "deployments", "unity", "lakebench-unity"),
    *_named("core_v1", "services", "unity", "lakebench-unity"),
    *_named("batch_v1", "jobs", "unity", "lakebench-unity-bootstrap"),
    *_named("core_v1", "configmaps", "unity", "lakebench-unity"),
    # PostgreSQL (its claims by exact name, LB-187).
    *_named("apps_v1", "statefulsets", "postgres", "lakebench-postgres"),
    *_named("core_v1", "services", "postgres", "lakebench-postgres"),
    *_named(
        "core_v1",
        "persistentvolumeclaims",
        "postgres",
        "data-lakebench-postgres-<n>",
        owner="SD-14",
    ),
    # Scripts ConfigMaps (DEP-1): the role maps and the v1.6 single map. The
    # selector keys on app.kubernetes.io/instance (v1.6 maps carry no
    # lakebench.io/deployment label).
    Cat1Entry(
        "core_v1",
        "configmaps",
        label_selector="app.kubernetes.io/instance={name},app.kubernetes.io/component=spark-scripts",
        owner_wi="SD-8",
        step="spark-scripts",
    ),
    # RBAC and Secrets.
    *_named("core_v1", "serviceaccounts", "rbac", "lakebench-spark-runner"),
    *_named("rbac_v1", "roles", "rbac", "lakebench-spark-runner"),
    *_named("rbac_v1", "rolebindings", "rbac", "lakebench-spark-runner"),
    *_named("core_v1", "secrets", "rbac", "lakebench-s3-credentials", "lakebench-postgres-secret"),
    # The category1 step (SD-21): objects no component step deleted. Order
    # matters: a Deployment goes before the claim its pod mounts.
    *_named("core_v1", "serviceaccounts", CATEGORY1_STEP, "lakebench-postgres"),
    *_named("apps_v1", "deployments", CATEGORY1_STEP, "lakebench-pushgateway"),
    *_named("core_v1", "services", CATEGORY1_STEP, "lakebench-pushgateway"),
    *_named("core_v1", "persistentvolumeclaims", CATEGORY1_STEP, "lakebench-pushgateway"),
    *_named("core_v1", "configmaps", CATEGORY1_STEP, "lakebench-prometheus-config"),
    *(
        Cat1Entry(
            "custom",
            "podmonitors",
            name=n,
            owner_wi=_PRE,
            step=CATEGORY1_STEP,
            group="monitoring.coreos.com",
            version="v1",
        )
        for n in (
            "lakebench-spark-driver",
            "lakebench-spark-executor",
            "lakebench-trino-coordinator",
            "lakebench-trino-worker",
            "lakebench-pushgateway",
        )
    ),
    # DEP-2 entries (SD-6) sort last in the category1 step.
)

# Namespace annotations the category1 step removes from a surviving
# namespace. ``state-schema`` is written by CC-2 beside the deploy nonce
# (DESIGN ch01 d3 N3); removing an absent key is a no-op.
CATEGORY1_ANNOTATIONS: tuple[str, ...] = ("lakebench.deployment/state-schema",)

KEPT_ON_DESTROY: tuple[KeptObject, ...] = (
    KeptObject(
        "configmaps",
        "lakebench-silver-state",
        _PRE,
        "holds the silver rebuild-epoch counters, which must never go back while "
        "table data written under them can outlive destroy (bucket cleanup off, or "
        "buckets this deployment does not own); a reset counter makes Delta skip "
        "writes as already committed. Deploy backfills it and keeps its values",
    ),
)
