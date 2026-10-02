"""Code and run provenance stamped into every run's metrics.json.

Provenance says what produced a run; it is never part of the experiment
identity. The ``provenance`` block holds:

- ``lakebench_version``, ``git_sha``, ``git_dirty``, ``install`` and
  ``tree_sha256`` (a hash of the package files on disk): which lakebench
  ran. From a git checkout (an editable install) the commit and
  whether the package had uncommitted changes; from a wheel, the commit the
  wheel was built from, written into ``lakebench/_build_info.py`` by the
  hatch build hook (``hatch_build.py``). ``install`` is ``checkout``,
  ``wheel`` or ``unknown`` (a PyInstaller binary, or a wheel built without
  the hook).
- ``end_sample``: the same fields read again from disk when the run ends,
  plus ``code_changed_during_run``.
- ``config_sha256`` (the config snapshot's value, one source) and
  ``config_path``.
- ``scripts_sha256``, ``scripts_maps`` and ``scripts_files_sha256``: the
  Spark scripts ConfigMaps the run applied and read back.
- ``deps``: the dependency set the job manager recorded, or
  ``"not_recorded"``.
- ``images_observed``: the image digests this run's pods actually ran
  (``status.containerStatuses[].imageID``), first seen per role, with any
  later different value in ``images_observed_changed`` and any batch stage
  whose driver or executor was not seen in ``images_observed_missing``.
- ``scratch_as_ran``: per job type, the scratch PVC size and storage class
  in the SparkApplication as the cluster holds it.

The experiment block copies the code fields, ``deps`` and
``images_observed`` (``experiment_lakebench``); the experiment identity
reads the dependency pinset and the observed image digests from that copy
(metrics/comparability.py). Everything else here is provenance only.
"""

from __future__ import annotations

import ast
import hashlib
import os
import re
import subprocess
import time
from collections.abc import Callable, Collection, Mapping
from pathlib import Path
from typing import Any

# Inherited from a git hook or `rebase --exec`, these would point git at
# another repository whatever the working directory.
_GIT_ENV_OVERRIDES = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR")

#: Written by hatch_build.py into the wheel (and the sdist); never tracked.
BUILD_INFO_FILE = "_build_info.py"

INSTALL_CHECKOUT = "checkout"
INSTALL_WHEEL = "wheel"
INSTALL_UNKNOWN = "unknown"

#: The fields that say which code ran.
CODE_KEYS = ("lakebench_version", "git_sha", "git_dirty", "install", "tree_sha256")

#: Value of ``provenance.deps`` when the job manager recorded no set.
NOT_RECORDED = "not_recorded"
#: ``images_observed`` in the experiment block when no digest was read.
NOT_OBSERVED = "not_observed"

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_VERSION_RE = re.compile(r"""^__version__\s*=\s*["']([^"']+)["']""", re.MULTILINE)


def _package_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def _git(args: list[str], cwd: Path) -> str | None:
    env = {k: v for k, v in os.environ.items() if k not in _GIT_ENV_OVERRIDES}
    try:
        r = subprocess.run(
            # --no-optional-locks: never take index.lock, so a commit in the
            # same checkout at the moment a run starts cannot fail on it.
            ["git", "--no-optional-locks", *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.SubprocessError, OSError):
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def read_build_info(path: Path) -> dict[str, Any] | None:
    """``{"git_sha", "git_dirty"}`` from a ``_build_info.py`` file, or None
    when there is no file. The file is parsed, never imported, so the value
    read at run end is what is on disk then. A malformed file gives None
    values (a wheel whose commit is unknown)."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    values: dict[str, Any] = {}
    try:
        for node in ast.parse(text).body:
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                values[node.targets[0].id] = ast.literal_eval(node.value)
    except (SyntaxError, ValueError):
        values = {}
    sha = values.get("GIT_SHA")
    dirty = values.get("GIT_DIRTY")
    return {
        "git_sha": sha if isinstance(sha, str) and _SHA_RE.match(sha) else None,
        "git_dirty": dirty if isinstance(dirty, bool) else None,
    }


def _disk_version(pkg_dir: Path) -> str:
    """``__version__`` as ``lakebench/__init__.py`` on disk says it, or the
    imported value when the file cannot be read (a PyInstaller binary)."""
    try:
        m = _VERSION_RE.search((pkg_dir / "__init__.py").read_text(encoding="utf-8"))
    except OSError:
        m = None
    if m:
        return m.group(1)
    from lakebench import __version__

    return __version__


def tree_sha256(pkg_dir: Path) -> str | None:
    """sha256 over the package's files on disk (relative path and bytes,
    sorted; bytecode caches left out), or None when they cannot be read.
    Sees an edit git cannot: in an installed wheel, or in a checkout that
    was already modified."""
    h = hashlib.sha256()
    try:
        for root, dirs, files in os.walk(pkg_dir):
            dirs[:] = sorted(d for d in dirs if d != "__pycache__")
            for name in sorted(files):
                if name.endswith((".pyc", ".pyo")):
                    continue
                path = Path(root) / name
                h.update(path.relative_to(pkg_dir).as_posix().encode("utf-8") + b"\0")
                h.update(path.read_bytes())
    except OSError:
        return None
    return h.hexdigest()


def sample() -> dict[str, Any]:
    """Which lakebench is on disk now: ``{lakebench_version, git_sha,
    git_dirty, install, tree_sha256}``. Uncached.

    A checkout is found from the package's own location, not the working
    directory, and must track the package: a wheel in a virtualenv inside
    some other checkout would otherwise record that checkout's commit. Any
    other install reads the commit the build hook wrote into the package.
    """
    pkg_dir = _package_dir()
    sha: str | None = None
    dirty: bool | None = None
    install = INSTALL_UNKNOWN
    if _git(["ls-files", "--error-unmatch", str(pkg_dir / "__init__.py")], pkg_dir) is not None:
        install = INSTALL_CHECKOUT
        sha = _git(["rev-parse", "HEAD"], pkg_dir)
        if sha:
            # Dirty means any change under the package, untracked files
            # included (an untracked module the run imports is at no commit).
            status = _git(["status", "--porcelain", "--", str(pkg_dir)], pkg_dir)
            dirty = None if status is None else bool(status)
    else:
        info = read_build_info(pkg_dir / BUILD_INFO_FILE)
        if info is not None:
            install = INSTALL_WHEEL
            sha, dirty = info["git_sha"], info["git_dirty"]
    return {
        "lakebench_version": _disk_version(pkg_dir),
        "git_sha": sha,
        "git_dirty": dirty,
        "install": install,
        "tree_sha256": tree_sha256(pkg_dir),
    }


def run_provenance() -> dict[str, Any]:
    """The code fields at run start. Sampled afresh on every call, so each
    run of a process (``run --repeat``) records its own start."""
    return sample()


def code_changed(start: Mapping[str, Any], end: Mapping[str, Any]) -> bool:
    """Whether *end* names other code than *start*. The file hash decides
    when both sides have one (a git read that failed at one end is then
    not a change); a commit, version or install that differs is a change
    either way. Without file hashes every code field is compared, so a
    failed read counts as a change: the record claims less, never more."""
    if start.get("tree_sha256") and end.get("tree_sha256"):
        if start["tree_sha256"] != end["tree_sha256"]:
            return True
        if start.get("git_sha") and end.get("git_sha") and start["git_sha"] != end["git_sha"]:
            return True
        return any(start.get(k) != end.get(k) for k in ("lakebench_version", "install"))
    return any(start.get(k) != end.get(k) for k in CODE_KEYS)


def end_sample(start: Mapping[str, Any]) -> dict[str, Any]:
    """The code fields read again at run end, and whether they name other
    code than *start* (``code_changed``)."""
    now = sample()
    now["code_changed_during_run"] = code_changed(start, now)
    return now


def experiment_lakebench(provenance: Mapping[str, Any] | None) -> dict[str, Any]:
    """The ``lakebench`` part of the experiment block: the code fields,
    ``deps`` and the observed image digests, which the identity reads
    (dependency pinset, observed image digests). ``images_observed`` is the
    role-to-digest map, or the constant ``"not_observed"`` when no digest
    was read: the reason text names the namespace and must not split two
    identities."""
    prov = provenance or {}
    out = {k: prov[k] for k in CODE_KEYS if k in prov}
    if "deps" in prov:
        out["deps"] = prov["deps"]
    if "images_observed" in prov:
        observed = prov["images_observed"]
        roles = (
            {r: v for r, v in observed.items() if r in IMAGE_ROLES}
            if isinstance(observed, Mapping)
            else {}
        )
        out["images_observed"] = roles or NOT_OBSERVED
    return out


def config_path_of(config_path: str | os.PathLike[str] | None) -> str | None:
    """The config path as given, made absolute (symlinks kept)."""
    return None if config_path is None else os.path.abspath(os.fspath(config_path))


def job_manager_fields(job_manager: Any) -> dict[str, Any]:
    """The scripts ConfigMaps and dependency set a job manager recorded.

    ``scripts_provenance`` is set by ``deploy_scripts_configmap`` once the
    maps are applied and read back; ``deps`` by the dependency check when it
    exists. Either may be missing; neither raises.
    """
    out: dict[str, Any] = {}
    scripts = getattr(job_manager, "scripts_provenance", None)
    if isinstance(scripts, Mapping):
        out["scripts_sha256"] = scripts.get("scripts_sha256")
        out["scripts_maps"] = dict(scripts.get("scripts_maps") or {})
        out["scripts_files_sha256"] = dict(scripts.get("files_sha256") or {})
    deps = getattr(job_manager, "deps", None)
    out["deps"] = dict(deps) if isinstance(deps, Mapping) else NOT_RECORDED
    return out


# --- images the pods ran ----------------------------------------------------

#: role -> container whose imageID is the role's image.
IMAGE_ROLES: dict[str, str] = {
    "spark_driver": "spark-kubernetes-driver",
    "spark_executor": "spark-kubernetes-executor",
    "trino_coordinator": "trino",
    "thrift": "spark-thrift",
}
#: The roles every batch stage and stream has.
SPARK_ROLES = frozenset({"spark_driver", "spark_executor"})
#: The label the Spark Operator puts on an application's driver and executors.
APP_LABEL = "sparkoperator.k8s.io/app-name"

#: Seconds one pod list may take before the observation is given up.
POD_LIST_TIMEOUT_S = 10


def pod_role(labels: Mapping[str, str] | None) -> str | None:
    """Which IMAGE_ROLES role a pod plays, from its labels."""
    labels = labels or {}
    if labels.get("app.kubernetes.io/component") == "spark-thrift-server":
        return "thrift"
    spark_role = labels.get("spark-role")
    if spark_role == "driver":
        return "spark_driver"
    if spark_role == "executor":
        return "spark_executor"
    if labels.get("app") == "lakebench-trino" and labels.get("component") == "coordinator":
        return "trino_coordinator"
    return None


def images_in(pods: Any, apps: Collection[str]) -> list[tuple[str, str, str]]:
    """``(role, pod name, imageID)`` for this run's pods: Spark drivers and
    executors of the applications in *apps* only (a namespace keeps earlier
    runs' finished drivers, and streams another run left), and the running
    Trino coordinator and Thrift server. A pod being deleted, or whose main
    container has no imageID yet (not pulled), is left out. Sorted by pod
    name, so the first-seen choice is stable."""
    out: list[tuple[str, str, str]] = []
    for pod in getattr(pods, "items", None) or []:
        meta = getattr(pod, "metadata", None)
        labels = getattr(meta, "labels", None) or {}
        role = pod_role(labels)
        if role is None or getattr(meta, "deletion_timestamp", None) is not None:
            continue
        status = getattr(pod, "status", None)
        if role in SPARK_ROLES:
            if labels.get(APP_LABEL) not in apps:
                continue
        elif getattr(status, "phase", None) != "Running":
            continue
        for cs in getattr(status, "container_statuses", None) or []:
            if getattr(cs, "name", None) == IMAGE_ROLES[role] and getattr(cs, "image_id", None):
                out.append((role, str(getattr(meta, "name", "")), str(cs.image_id)))
    return sorted(out, key=lambda t: (t[1], t[0]))


def merge_images(
    provenance: dict[str, Any], seen: list[tuple[str, str, str]], at: str, observed_at: str
) -> None:
    """Fold one observation into *provenance*: the first imageID seen for a
    role is kept; a later different one is appended to
    ``images_observed_changed``."""
    observed = provenance.get("images_observed")
    if not isinstance(observed, dict) or "not_observed" in observed:
        observed = {}
    changed: list[dict[str, Any]] = list(provenance.get("images_observed_changed") or [])
    for role, pod, image_id in seen:
        first = observed.get(role)
        if first is None:
            observed[role] = image_id
        elif first != image_id and not any(
            c["role"] == role and c["image_id"] == image_id for c in changed
        ):
            changed.append(
                {
                    "role": role,
                    "image_id": image_id,
                    "pod": pod,
                    "at": at,
                    "observed_at": observed_at,
                }
            )
    provenance["images_observed"] = observed
    if changed:
        provenance["images_observed_changed"] = changed


def observe_images(
    provenance: dict[str, Any],
    namespace: str,
    at: str,
    observed_at: str,
    apps: Collection[str],
) -> set[str]:
    """List the namespace's pods once and fold this run's Spark (of *apps*),
    Trino coordinator and Thrift images into *provenance*. Returns the
    roles seen. A failed list (RBAC, timeout) records ``{"not_observed":
    reason}`` unless an earlier observation succeeded; it never raises."""
    try:
        from kubernetes import client as k8s_client

        pods = k8s_client.CoreV1Api().list_namespaced_pod(
            namespace, _request_timeout=POD_LIST_TIMEOUT_S
        )
    except Exception as e:  # noqa: BLE001 -- provenance never fails a run
        reason = getattr(e, "reason", None) or type(e).__name__
        status = getattr(e, "status", None)
        why = f"pod list in {namespace} failed at {at}: " + (
            f"HTTP {status} {reason}" if status else str(reason)
        )
        observed = provenance.get("images_observed")
        if not (isinstance(observed, dict) and observed and "not_observed" not in observed):
            provenance["images_observed"] = {"not_observed": why}
        return set()
    seen = images_in(pods, apps)
    merge_images(provenance, seen, at, observed_at)
    return {role for role, _pod, _image in seen}


class StageImageWatch:
    """Reads one batch stage's images while it runs.

    Batch executors are deleted when the stage ends, and an executor that
    the operator already counts may not have pulled its image yet, so the
    pods are read on RUNNING polls that report executors, at most
    ``MAX_READS`` times and ``MIN_GAP_S`` apart, until both a driver and an
    executor digest are seen. ``finish`` reads once more when the driver
    was not seen (its pod stays after the stage) and returns the Spark
    roles never seen.
    """

    MAX_READS = 4
    MIN_GAP_S = 15.0

    def __init__(
        self, observe: Callable[[], set[str]], clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._observe = observe
        self._clock = clock
        self.seen: set[str] = set()
        self.reads = 0
        self._last: float | None = None

    @property
    def done(self) -> bool:
        return SPARK_ROLES <= self.seen

    def _read(self) -> None:
        self.reads += 1
        self._last = self._clock()
        self.seen |= self._observe()

    def on_status(self, running: bool, executor_count: int) -> None:
        if self.done or not running or not executor_count or self.reads >= self.MAX_READS:
            return
        if self._last is not None and self._clock() - self._last < self.MIN_GAP_S:
            return
        self._read()

    def finish(self) -> list[str]:
        if "spark_driver" not in self.seen:
            self._read()
        return sorted(SPARK_ROLES - self.seen)


# --- scratch as ran ----------------------------------------------------------

_SCRATCH_PREFIX = "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1."
SCRATCH_SIZE_KEY = _SCRATCH_PREFIX + "options.sizeLimit"
SCRATCH_CLASS_KEY = _SCRATCH_PREFIX + "options.storageClass"


def scratch_from_spark_conf(spark_conf: Mapping[str, Any] | None) -> dict[str, Any]:
    """``{"size_limit", "storage_class"}`` of the executor scratch PVC in a
    SparkApplication's ``spec.sparkConf``; both None when the job had no
    scratch PVC."""
    conf = spark_conf or {}
    size, sclass = conf.get(SCRATCH_SIZE_KEY), conf.get(SCRATCH_CLASS_KEY)
    return {
        "size_limit": None if size is None else str(size),
        "storage_class": None if sclass is None else str(sclass),
    }


def record_scratch(provenance: dict[str, Any], job_type: str, status: Any) -> None:
    """Record *job_type*'s scratch as the cluster held it, from the
    SparkApplication status the monitor last read (``JobStatus.scratch``).
    The first value per job type is kept; a status without the spec records
    ``{"not_recorded": reason}`` unless a value is already there."""
    table = provenance.setdefault("scratch_as_ran", {})
    scratch = getattr(status, "scratch", None) if status is not None else None
    current = table.get(job_type)
    if isinstance(scratch, Mapping):
        if current is None or "not_recorded" in current:
            table[job_type] = dict(scratch)
    elif current is None:
        table[job_type] = {"not_recorded": "the SparkApplication spec was not read"}
