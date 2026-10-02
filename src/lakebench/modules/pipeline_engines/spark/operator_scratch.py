"""The Spark Operator controller's ``/tmp`` scratch volume.

The operator runs spark-submit inside its controller pod, and spark-submit
resolves ``spark.jars.packages`` with Ivy into ``spark.jars.ivy`` (before 1.7,
``/tmp/.ivy2`` for every lakebench job; since 1.7 the jobs name lb-deps jar
URLs and set no packages, but other tenants' applications may). The
controller's root filesystem is
read-only (chart ``controller.securityContext.readOnlyRootFilesystem``), so
``/tmp`` is the chart's ``tmp`` emptyDir, and chart 2.5.1 (like 2.4.0) caps
it at ``sizeLimit: 1Gi``. The Iceberg or Delta runtime, hadoop-aws and the
AWS SDK bundle for one Spark line come to about 1.2 GB, so the kubelet
evicts the controller once the cache fills: 19 times in 100 minutes on
2026-09-27. Each eviction costs leader election, a cold Ivy cache and a
SUBMISSION_FAILED retry for any job mid-submission, on every tenant.

This module holds the pure parts: the Helm values that size the volume, and
the diagnosis ``lakebench admin doctor`` / ``status`` print. Reading and
changing the release is in ``operator.py`` (under the cluster lease).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# Name of the chart's controller scratch volume (values.yaml
# ``controller.volumes[0]``, mounted at /tmp by ``controller.volumeMounts``).
CONTROLLER_TMP_VOLUME = "tmp"

# What ``admin install --component spark-operator`` and ``admin repair-operator`` set.
# One Spark line's jars are ~1.2 GB; the UAT matrix resolves three Spark
# minors (each with its own AWS SDK bundle) plus Iceberg and Delta, so a
# controller that lives through a full matrix holds 3-4 GB.
DEFAULT_CONTROLLER_TMP_SIZE = "8Gi"

# Below this the cache for a single Spark line plus a second submission's
# concurrent resolve can fill it; doctor reports it as undersized.
MIN_CONTROLLER_TMP_BYTES = 4 * 1024**3

_QUANTITY = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGTPE]i?|k|m)?\s*$")
_FACTORS = {
    None: 1,
    "k": 1000,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
    "P": 1000**5,
    "E": 1000**6,
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
    "Ei": 1024**6,
}

# Kubelet eviction messages for this failure, e.g. 'Usage of EmptyDir volume
# "tmp" exceeds the limit "1Gi". ' or 'Pod ephemeral local storage usage
# exceeds the total limit of containers ...'.
_STORAGE_EVICTION = re.compile(r"emptydir volume|ephemeral(?:-| local )storage", re.IGNORECASE)


def parse_quantity(value: str | None) -> int | None:
    """Bytes in a Kubernetes quantity ("1Gi", "500Mi", "4G"), None if unparseable."""
    if not value:
        return None
    m = _QUANTITY.match(str(value))
    if not m:
        return None
    unit = m.group(2)
    if unit == "m":  # milli-bytes: legal, never meant for storage
        return int(float(m.group(1)) / 1000)
    return int(float(m.group(1)) * _FACTORS[unit])


def validate_size(value: str) -> str:
    """*value* when it is a binary quantity of at least the floor, else ValueError."""
    size = parse_quantity(value)
    if size is None or not re.fullmatch(r"\d+(Mi|Gi|Ti)", value):
        raise ValueError(f"{value!r} is not a size such as 8Gi")
    if size < MIN_CONTROLLER_TMP_BYTES:
        raise ValueError(f"{value} is below the {MIN_CONTROLLER_TMP_BYTES // 1024**3}Gi floor")
    return value


def helm_set_args(size: str = DEFAULT_CONTROLLER_TMP_SIZE) -> list[str]:
    """``--set`` args that size the controller's /tmp emptyDir.

    Helm replaces a list wholesale, so the element is written in full: an
    index-only ``controller.volumes[0].emptyDir.sizeLimit`` would leave a
    volume with no name and the chart's /tmp mount would fail to render a
    matching volume. ``controller.volumeMounts`` is left at the chart
    default (``tmp`` at /tmp).
    """
    return [
        "--set",
        f"controller.volumes[0].name={CONTROLLER_TMP_VOLUME}",
        "--set",
        f"controller.volumes[0].emptyDir.sizeLimit={size}",
    ]


@dataclass
class TmpVolume:
    """The controller's /tmp volume as the Deployment spec has it."""

    found: bool  # a volume named ``tmp`` exists
    size_limit: str | None  # its emptyDir sizeLimit; None = no limit
    is_empty_dir: bool = True

    @property
    def limit_bytes(self) -> int | None:
        return parse_quantity(self.size_limit)

    @property
    def undersized(self) -> bool:
        """True when a limit is set and is below the floor."""
        if not self.found or not self.is_empty_dir or self.size_limit is None:
            return False
        size = self.limit_bytes
        return size is None or size < MIN_CONTROLLER_TMP_BYTES


def tmp_volume(deployment: dict[str, Any]) -> TmpVolume:
    """The /tmp volume of a controller Deployment (a JSON-shaped dict)."""
    spec = ((deployment.get("spec") or {}).get("template") or {}).get("spec") or {}
    for vol in spec.get("volumes") or []:
        if vol.get("name") != CONTROLLER_TMP_VOLUME:
            continue
        empty_dir = vol.get("emptyDir")
        if empty_dir is None:
            return TmpVolume(found=True, size_limit=None, is_empty_dir=False)
        return TmpVolume(found=True, size_limit=(empty_dir or {}).get("sizeLimit"))
    return TmpVolume(found=False, size_limit=None)


@dataclass
class ScratchDiagnosis:
    """What doctor and status say about the controller's scratch space."""

    volume: TmpVolume
    # Controller pods the kubelet evicted for storage at the current /tmp
    # size: [{"pod", "at", "message"}].
    storage_evictions: list[dict[str, str]] = field(default_factory=list)
    # Storage evictions under an earlier /tmp size (before a resize). Evicted
    # pods stay as Failed objects until deleted, so these are history.
    past_storage_evictions: int = 0
    other_evictions: int = 0
    container_restarts: int = 0

    @property
    def problems(self) -> list[str]:
        out: list[str] = []
        if self.volume.undersized:
            out.append(
                f"controller /tmp emptyDir sizeLimit is {self.volume.size_limit}; "
                f"spark-submit's Ivy cache needs at least "
                f"{MIN_CONTROLLER_TMP_BYTES // 1024**3}Gi"
            )
        if self.storage_evictions:
            last = self.storage_evictions[-1]
            out.append(
                f"{len(self.storage_evictions)} controller pod(s) evicted for storage at "
                f"the current /tmp size (latest {last['pod']}"
                f"{' at ' + last['at'] if last['at'] else ''}: {last['message'].strip()[:160]})"
            )
        return out

    @property
    def healthy(self) -> bool:
        return not self.problems


def repair_hint(size: str = DEFAULT_CONTROLLER_TMP_SIZE) -> str:
    return (
        f"Fix: 'lakebench admin repair-operator' raises the controller /tmp emptyDir to "
        f"{size} under the cluster lease (keeps the watch list and chart version). "
        "It rolls the controller once."
    )


def _pod_tmp_limit(pod: dict[str, Any]) -> str | None:
    """The /tmp sizeLimit in a pod's own spec ("" when it has no limit)."""
    for vol in (pod.get("spec") or {}).get("volumes") or []:
        if vol.get("name") == CONTROLLER_TMP_VOLUME:
            return str((vol.get("emptyDir") or {}).get("sizeLimit") or "")
    return None


def diagnose(
    deployment: dict[str, Any] | None,
    pods: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> ScratchDiagnosis:
    """Diagnose the controller's scratch space from JSON-shaped API objects.

    *pods* are the controller's pods (evicted ones stay as Failed pods until
    deleted); *events* are the operator namespace's Evicted events (kept
    about an hour). An eviction seen in both is counted once.

    An evicted pod whose own /tmp sizeLimit differs from the Deployment's is
    history (it was evicted under an earlier size, before a resize). Keying
    on the size rather than the ReplicaSet matters: every watch-list edit
    changes the controller args and so the ReplicaSet, which would file
    evictions that keep happening as history. An eviction known only from
    an event has no pod spec; it counts as current when it is newer than
    the oldest running controller pod (or there is none), so events left
    after a resize and a cleanup of the Failed pods are history.
    """
    volume = tmp_volume(deployment or {})
    current_limit = volume.size_limit or ""
    storage: dict[str, dict[str, str]] = {}  # pod -> record (+ "limit")
    other: set[str] = set()
    restarts = 0
    live_since = ""  # creationTimestamp of the oldest pod that is not evicted
    for pod in pods:
        meta = pod.get("metadata") or {}
        status = pod.get("status") or {}
        name = str(meta.get("name") or "")
        for cs in status.get("containerStatuses") or []:
            restarts += int(cs.get("restartCount") or 0)
        if status.get("reason") != "Evicted":
            created = str(meta.get("creationTimestamp") or "")
            if created and (not live_since or created < live_since):
                live_since = created
            continue
        message = str(status.get("message") or "")
        if _STORAGE_EVICTION.search(message):
            # A pod carries no eviction time; an event, when kept, supplies it.
            limit = _pod_tmp_limit(pod)
            storage[name] = {
                "pod": name,
                "at": "",
                "message": message,
                "limit": current_limit if limit is None else limit,
            }
        else:
            other.add(name)
    for ev in events:
        if ev.get("reason") != "Evicted":
            continue
        obj = ev.get("involvedObject") or {}
        name = str(obj.get("name") or "")
        if obj.get("kind") != "Pod" or "controller" not in name:
            continue
        message = str(ev.get("message") or "")
        at = str(ev.get("lastTimestamp") or ev.get("eventTime") or "")
        if _STORAGE_EVICTION.search(message):
            if name in storage:
                limit = storage[name]["limit"]
            elif live_since and at and at < live_since:
                limit = "<before the running controller>"
            else:
                limit = current_limit
            storage[name] = {"pod": name, "at": at, "message": message, "limit": limit}
            other.discard(name)
        elif name not in storage:
            other.add(name)
    current = [r for r in storage.values() if r["limit"] == current_limit]
    ordered = sorted(
        ({k: v for k, v in r.items() if k != "limit"} for r in current),
        key=lambda e: e["at"],
    )
    return ScratchDiagnosis(
        volume=volume,
        storage_evictions=ordered,
        past_storage_evictions=len(storage) - len(current),
        other_evictions=len(other),
        container_restarts=restarts,
    )
