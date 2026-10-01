"""What ships to Spark pods as scripts, and how.

This module is the only definition of the files Lakebench mounts at
``/opt/spark/scripts``. They ship in one ConfigMap per role, so no single
ConfigMap approaches the 1 MiB Kubernetes limit, and every pod projects the
roles its job type needs into one flat directory. The flat layout is what the
scripts expect: ``local:///opt/spark/scripts/<script>`` main files, bare
``from common import ...`` imports with ``PYTHONPATH=/opt/spark/scripts``, and
the AML JSON read by file name.

Every file is listed explicitly; there are no globs. The builder measures the
bytes it is about to apply (UTF-8 key plus value bytes over ``data``; the API
server counts only the values, so this is slightly conservative) and refuses
a map over :data:`MAP_BUDGET_BYTES`. A listed file that is not in the package
raises instead of being skipped.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

# job.py imports this module lazily (inside functions) because JobType lives
# there; keep it that way, or move JobType to a leaf module first.
from lakebench.modules.pipeline_engines.spark.job import JobType

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig

# Kubernetes rejects a ConfigMap whose data keys plus values exceed 1 MiB.
CONFIGMAP_LIMIT_BYTES = 1_048_576
# Each map stays under 80% of that, so growth fails a test, not a deploy.
MAP_BUDGET_BYTES = int(0.8 * CONFIGMAP_LIMIT_BYTES)  # 838_860

MAP_NAME_PREFIX = "lakebench-scripts-"
# The single map v1.6 and earlier applied. Deleted after the role maps apply.
LEGACY_MAP_NAME = "lakebench-spark-scripts"
COMPONENT_LABEL = "spark-scripts"
SCRIPTS_SHA256_ANNOTATION = "lakebench.io/scripts-sha256"
ROLE_LABEL = "lakebench.io/scripts-role"
DEPLOYMENT_LABEL = "lakebench.io/deployment"
SCRIPTS_MOUNT_PATH = "/opt/spark/scripts"
SCRIPTS_VOLUME_NAME = "spark-scripts"


class ScriptsMapError(Exception):
    """The scripts ConfigMaps cannot be built. The message is one line that
    names the file or map; the CLI prints it and exits 1."""


class ScriptsManifestError(ScriptsMapError):
    """The script manifest names a file the package does not have or cannot
    read, or two roles ship the same key."""


class ScriptsApplyError(ScriptsMapError):
    """The maps could not be applied or verified, or replacing them is
    refused (another deployment's map, or one a live job mounts)."""


class ScriptsBudgetError(ScriptsMapError):
    """A role's ConfigMap is over :data:`MAP_BUDGET_BYTES`."""

    def __init__(self, role: str, size: int, budget: int = MAP_BUDGET_BYTES):
        self.role = role
        self.size = size
        self.budget = budget
        super().__init__(
            f"scripts ConfigMap {map_name(role)} is {size:,} bytes, over its "
            f"{budget:,}-byte budget (80% of the 1 MiB ConfigMap limit); move "
            "a file to a smaller role in scripts_maps.SCRIPT_MAPS"
        )


@dataclass(frozen=True)
class ScriptSource:
    """One shipped file: its path relative to the ``lakebench`` package and
    its flat ConfigMap key (the file name under ``/opt/spark/scripts``)."""

    path: str
    key: str


def _src(path: str) -> ScriptSource:
    return ScriptSource(path=path, key=PurePosixPath(path).name)


def _scripts(*names: str) -> tuple[ScriptSource, ...]:
    return tuple(_src(f"spark/scripts/{n}") for n in names)


# Role -> files, in apply order. Sizes at integrate 46cc3f4 (UTF-8 bytes):
# common 108 KB, c360 155 KB, aml-rules 229 KB, aml-jobs 287 KB,
# aml-gate 178 KB, aml-data 47 KB; the largest is 27% of 1 MiB.
SCRIPT_MAPS: dict[str, tuple[ScriptSource, ...]] = {
    "common": _scripts("common.py"),
    "c360": _scripts(
        "bronze_verify.py",
        "silver_build.py",
        "gold_finalize.py",
        "bronze_ingest.py",
        "silver_stream.py",
        "gold_refresh.py",
        # Delta variants (same pipeline logic, Delta write API)
        "silver_build_delta.py",
        "gold_finalize_delta.py",
        "gold_refresh_delta.py",
        "bronze_ingest_delta.py",
        "silver_stream_delta.py",
    ),
    # Library modules the AML jobs import: detection rules (replay, gold) and
    # the P10 operations layer (gold, and the executors' workflow replay).
    "aml-rules": _scripts("detection_rules.py", "tm_operations.py"),
    "aml-jobs": _scripts(
        "bronze_verify_financial.py",
        "silver_build_financial.py",
        "gold_finalize_financial.py",
        "bronze_ingest_financial.py",
        "silver_stream_financial.py",
        "gold_refresh_financial.py",
        "replay_financial.py",
        "reproduce_financial.py",
        "score_financial.py",
    ),
    # The reference detector and the pre-registered gate. reference_score,
    # fidelity_gate and the seed guard live outside spark/scripts (they are
    # unit-tested as lakebench modules) and ship flat because the Spark image
    # has no lakebench install.
    "aml-gate": (
        *_scripts("score_financial_reference.py", "aml_features.py"),
        _src("aml/reference_score.py"),
        _src("aml/fidelity_gate.py"),
        _src("config/datagen_seed.py"),
    ),
    # AML reference data, read by file name from the scripts directory.
    "aml-data": tuple(
        _src(f"spark/data/aml/{n}")
        for n in (
            "aml_level2_predictions.json",
            "aml_preregistration.json",
            "aml_registered_looks.json",
            "high_risk_jurisdictions.json",
            "synthetic_corridors.json",
        )
    ),
}

ROLES: tuple[str, ...] = tuple(SCRIPT_MAPS)

# Files under spark/data/aml/ deliberately not shipped. Every other JSON there
# must be listed in aml-data (test_every_aml_json_listed_or_excluded).
NOT_SHIPPED: tuple[str, ...] = ()

# Which roles each job type's pods project. Every row is every role today
# (v1.6 mounted everything everywhere); a restricted row is for a job type
# that must not see a role (the v1.8 ML loop). A job type with no row fails
# test_mounts_table_covers_every_job_type, so none can mount nothing.
MOUNTS_BY_JOB_TYPE: dict[JobType, tuple[str, ...]] = {
    JobType.BRONZE_VERIFY: ROLES,
    JobType.SILVER_BUILD: ROLES,
    JobType.GOLD_FINALIZE: ROLES,
    JobType.BRONZE_INGEST: ROLES,
    JobType.SILVER_STREAM: ROLES,
    JobType.GOLD_REFRESH: ROLES,
    JobType.REPLAY_FINANCIAL: ROLES,
    JobType.REPRODUCE_FINANCIAL: ROLES,
    JobType.SCORE_FINANCIAL: ROLES,
    JobType.SCORE_FINANCIAL_REFERENCE: ROLES,
}


def map_name(role: str) -> str:
    """ConfigMap name of a role."""
    return f"{MAP_NAME_PREFIX}{role}"


def map_data_size(data: Mapping[str, str]) -> int:
    """UTF-8 key plus value bytes, summed over ``data``. The API server's
    1 MiB check counts the values only, so this is the larger figure."""
    return sum(len(k.encode("utf-8")) + len(v.encode("utf-8")) for k, v in data.items())


def data_sha256(data: Mapping[str, str]) -> str:
    """sha256 of the canonical JSON of a map's data (keys sorted)."""
    blob = json.dumps(dict(data), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def scripts_sha256(maps: Iterable[Mapping[str, Any]]) -> str:
    """One hash over every map: sha256 of the sorted ``(name, scripts-sha256)``
    pairs. For ``provenance.scripts_sha256`` in the run record."""
    pairs = sorted(
        (m["metadata"]["name"], m["metadata"]["annotations"][SCRIPTS_SHA256_ANNOTATION])
        for m in maps
    )
    blob = json.dumps(pairs, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _read_role(role: str, package_dir: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for src in SCRIPT_MAPS[role]:
        path = package_dir / src.path
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            raise ScriptsManifestError(
                f"{src.path} is listed for map {role} but is not in the package"
            ) from None
        except OSError as e:
            raise ScriptsManifestError(
                f"{src.path} (map {role}) cannot be read: {e.strerror or e}"
            ) from None
        try:
            data[src.key] = raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ScriptsManifestError(f"{src.path} (map {role}) is not UTF-8: {e}") from None
    return data


def build_script_configmaps(
    cfg: LakebenchConfig,
    namespace: str,
    *,
    package_dir: Path | None = None,
    budget: int = MAP_BUDGET_BYTES,
) -> list[dict[str, Any]]:
    """Render every role's ConfigMap manifest, in :data:`ROLES` order.

    Every role is always built, whatever the workload, so a pod can never wait
    in ContainerCreating on a map that was not applied. Raises
    :class:`ScriptsManifestError` for a missing or non-UTF-8 listed file or a
    key shipped by two roles, and :class:`ScriptsBudgetError` for a map over
    ``budget``. Nothing touches the cluster.
    """
    if package_dir is None:
        from lakebench import _resources

        package_dir = _resources._package_dir()

    seen: dict[str, str] = {}
    for role, sources in SCRIPT_MAPS.items():
        for src in sources:
            if src.key in seen:
                raise ScriptsManifestError(
                    f"key {src.key} is listed for map {role} and map {seen[src.key]}; "
                    "in one projected volume the later map would silently shadow the earlier"
                )
            seen[src.key] = role

    maps: list[dict[str, Any]] = []
    for role in ROLES:
        data = _read_role(role, package_dir)
        size = map_data_size(data)
        if size > budget:
            raise ScriptsBudgetError(role, size, budget)
        maps.append(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": map_name(role),
                    "namespace": namespace,
                    "labels": {
                        "app.kubernetes.io/name": "lakebench",
                        "app.kubernetes.io/instance": cfg.name,
                        "app.kubernetes.io/component": COMPONENT_LABEL,
                        "app.kubernetes.io/managed-by": "lakebench",
                        DEPLOYMENT_LABEL: cfg.name,
                        ROLE_LABEL: role,
                    },
                    "annotations": {SCRIPTS_SHA256_ANNOTATION: data_sha256(data)},
                },
                "data": data,
            }
        )
    return maps


def scripts_volume(job_type: JobType) -> dict[str, Any]:
    """The ``spark-scripts`` pod-template volume for a job type: one projected
    volume with a ``configMap`` source per role it mounts, none optional.

    Kubelet merges a projected volume's sources path by path and the last
    source wins, so a key in two mounted roles would shadow silently; that is
    refused here as well as in the builder.
    """
    roles = MOUNTS_BY_JOB_TYPE[job_type]
    keys = [s.key for role in roles for s in SCRIPT_MAPS[role]]
    if len(keys) != len(set(keys)):
        dup = sorted({k for k in keys if keys.count(k) > 1})
        raise ScriptsManifestError(f"{job_type.value} mounts {dup} from more than one map")
    return {
        "name": SCRIPTS_VOLUME_NAME,
        "projected": {
            "sources": [
                {"configMap": {"name": map_name(role), "optional": False}} for role in roles
            ]
        },
    }


def scripts_label_selector(deployment_name: str) -> str:
    """Selects this deployment's scripts ConfigMaps, the role maps and the
    legacy single map alike.

    It keys on ``app.kubernetes.io/instance``, not ``lakebench.io/deployment``:
    the v1.6 map carries only the ``app.kubernetes.io/*`` labels, and a
    deployment made by 1.6 and destroyed by 1.7 without a 1.7 run must still
    lose it. The Category-1 registry keeps this selector as is.
    """
    return (
        f"app.kubernetes.io/component={COMPONENT_LABEL},"
        "app.kubernetes.io/managed-by=lakebench,"
        f"app.kubernetes.io/instance={deployment_name}"
    )
