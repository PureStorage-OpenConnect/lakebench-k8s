"""Spark Operator management for Lakebench.

Handles detection and installation of the Kubeflow Spark Operator.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NamedTuple

from lakebench.k8s import pinned_helm, pinned_kubectl, pinned_oc
from lakebench.k8s.lease_state import LeaseHoldExceeded, lease_clamp, lease_held
from lakebench.modules.pipeline_engines.spark.operator_scratch import (
    DEFAULT_CONTROLLER_TMP_SIZE,
    TmpVolume,
    tmp_volume,
)
from lakebench.modules.pipeline_engines.spark.operator_scratch import (
    helm_set_args as controller_tmp_helm_set_args,
)

logger = logging.getLogger(__name__)


class _DeploymentReadError(Exception):
    """Raised when the controller deployment spec cannot be read."""


# The watch-list hold budget, phase by phase: the design's split of the
# lease's 750 s hold. Each phase is one deadline shared by its steps and is
# also bounded by what the hold has left, so the hold budget, not this table,
# is what stops a long sequence: a step with too little left fails closed
# before it starts. WATCH_POD_POLL_S is destroy's _OPERATOR_POD_WAIT_S and
# WATCH_RECOVERY_S is k8s._pinned's HELM_RECOVERY_RESERVE_S (a test ties them).
WATCH_HELM_PHASE_S = 180  # the helm upgrade, conflict retries included
WATCH_ROLLOUT_PHASE_S = 180  # awaiting the OpenShift patch rollout, both Deployments
WATCH_RESTART_PHASE_S = 180  # the restart and both rollout waits
WATCH_POD_POLL_S = 120  # destroy's in-lease operator pod poll (deploy/destroy.py)
WATCH_RECOVERY_S = 60  # kept back by k8s._pinned after a killed helm call
WATCH_MISC_S = 30  # verify, SCC assign, the namespace delete
WATCH_PHASES_S = (
    WATCH_HELM_PHASE_S,
    WATCH_ROLLOUT_PHASE_S,
    WATCH_RESTART_PHASE_S,
    WATCH_POD_POLL_S,
    WATCH_RECOVERY_S,
    WATCH_MISC_S,
)
# One helm attempt: at most this long, and not started with less than the
# minimum left in its phase.
_HELM_ATTEMPT_MAX_S = 120
_HELM_ATTEMPT_MIN_S = 60

# How long a watch-list mutation waits for the cluster lease: three holders at
# the watch-list budget (deploy.cluster_lock.LEASE_MAX_HOLD_S = 750) ahead of
# it, the four-deployment limit. A typical hold is under a minute; an admin
# hold (ADMIN_MAX_HOLD_S) or a holder that is overtaken repeatedly can still
# outlast it, and the waiter then fails with nothing changed.
_WATCH_LIST_LOCK_TIMEOUT_S = 3 * 750


class _Phase:
    """One deadline shared by the steps of a phase, inside the hold budget."""

    def __init__(self, budget_s: float) -> None:
        self.deadline = time.monotonic() + lease_clamp(float(budget_s))

    def remaining(self) -> float:
        return self.deadline - time.monotonic()

    def wait_s(self, what: str) -> int:
        """Whole seconds left for a wait; raises when under one second, so a
        spent budget never turns into ``--timeout=0s`` (wait forever)."""
        left = int(self.remaining())
        if left < 1:
            raise LeaseHoldExceeded(f"{what}: no time left in its phase; not started")
        return left

    def helm_attempt_s(self, what: str) -> float | None:
        """The subprocess timeout for one helm attempt, or raise when less than
        ``_HELM_ATTEMPT_MIN_S`` is left (a killed upgrade is worse than one
        never started). None outside the lease: there k8s._pinned does not
        stop helm with SIGTERM first or bound its own ``--timeout``, and a
        bare SIGKILL would leave the release pending for every deployment."""
        if not lease_held():
            return None
        left = self.remaining()
        if left < _HELM_ATTEMPT_MIN_S:
            raise LeaseHoldExceeded(
                f"{what}: {max(left, 0):.0f} s left in the helm phase; not started"
            )
        return min(float(_HELM_ATTEMPT_MAX_S), left)


class _WatchListReadError(Exception):
    """The operator's watch list could not be read.

    Distinct from "watches nothing": callers that treated a failed read as an
    empty list would report a strict remove as done without checking it (the
    namespace is then deleted while still watched, crash-looping the operator
    for every tenant), or overwrite the list with only their own namespace.
    """


class WatchListMutationError(RuntimeError):
    """Raised when a strict watch-list mutation fails.

    Destroy paths raise this rather than warning so a crash-looping
    operator is reported to the user explicitly, not swallowed as a
    successful destroy. The message names ``admin repair-operator`` for
    recovery.
    """


@dataclass
class OperatorStatus:
    """Status of Spark Operator."""

    installed: bool | None  # None: could not be determined
    version: str | None
    namespace: str | None
    ready: bool
    message: str
    watching_namespace: bool | None = None  # None = could not determine
    watched_namespaces: list[str] | None = None  # None = watches all


class ReleaseState(NamedTuple):
    """``helm status`` of the operator release."""

    status: str  # deployed, pending-upgrade, ... or "absent"
    revision: int


def _rfc3339(value: object) -> float | None:
    """Epoch seconds of an RFC 3339 time (nanosecond fraction allowed)."""
    from datetime import datetime

    if not isinstance(value, str) or not value:
        return None
    m = re.match(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:\d\d)$", value.strip())
    if not m:
        return None
    # Python 3.10's fromisoformat takes exactly six fraction digits.
    frac = "." + (m.group(2) or ".")[1:7].ljust(6, "0")
    tz = "+00:00" if m.group(3) == "Z" else m.group(3)
    try:
        return datetime.fromisoformat(m.group(1) + frac + tz).timestamp()
    except ValueError:
        return None


def watch_list_fix_hint() -> str:
    """User-facing remedy for a namespace missing from ``spark.jobNamespaces``.

    Never suggests a raw ``helm upgrade --reuse-values``: that bypasses the
    ``lakebench-cluster-lock`` lease (category 4 shared state) and a user
    who copies it with a stale list overwrites other deployments' entries.
    ``lakebench deploy`` adds the namespace under the lease.
    """
    return (
        "Fix: re-run 'lakebench deploy <config>'; it adds the namespace to "
        "the watch list under the lakebench-cluster-lock lease. If the lease "
        "is held, check 'lakebench admin status'. If the watch list carries "
        "entries for deleted namespaces, run 'lakebench admin repair-operator' "
        "first. Do not edit spark.jobNamespaces with helm directly."
    )


class SparkOperatorManager:
    """Manages Spark Operator installation and status."""

    # Helm chart settings
    HELM_REPO_NAME = "spark-operator"
    HELM_REPO_URL = "https://kubeflow.github.io/spark-operator"
    HELM_CHART_NAME = "spark-operator/spark-operator"
    HELM_RELEASE_NAME = "spark-operator"
    DEFAULT_NAMESPACE = "spark-operator"
    CONTROLLER_DEPLOYMENT = "spark-operator-controller"
    WEBHOOK_DEPLOYMENT = "spark-operator-webhook"

    # ``spark.jobNamespaces`` is shared cluster state, so concurrent deploys
    # contend for it.  Helm rejects an upgrade while another is in flight;
    # retrying with a fresh read is what makes the add safe under parallelism.
    _HELM_CONFLICT_RETRIES = 5
    _HELM_CONFLICT_BACKOFF = 3.0

    # Substrings Helm uses when an upgrade loses a race against another
    # writer.  These are contention, not misconfiguration, so they are worth
    # retrying; anything else is a real failure and is surfaced immediately.
    _HELM_CONFLICT_MARKERS = (
        "already exists",
        "another operation",
        "in progress",
        "is in a pending state",
        "operation cannot be fulfilled",
        "the object has been modified",
        # A concurrent writer can advance the release's Secret-backed history
        # past the revision this upgrade's --reuse-values read expected,
        # between the read and the write. Helm reports this as a missing
        # release Secret rather than a generic conflict, e.g. `secrets
        # "sh.helm.release.v1.spark-operator.v32" not found`. Live-verified
        # 2026-07-27 under concurrent UAT: two deploys adding different
        # namespaces to the shared spark-operator release both raced this
        # error, and a same-revision retry on each succeeded on the next
        # attempt. The Secret name itself is transient by design (`v32` was
        # gone from `kubectl get secrets` minutes later, well within normal
        # Helm history churn), so matching on the literal release name would
        # be wrong -- `sh.helm.release.v1.` is the stable, chart-agnostic
        # substring that identifies this failure class.
        'secrets "sh.helm.release.v1.',
    )

    # ``helm upgrade --reuse-values`` carries forward only the keys already
    # present in the *stored release values* -- it does not merge in a newer
    # chart's values.yaml defaults for keys that key never had. The 2.4.0
    # chart has no ``prometheus.metrics.jobSubmitLatencyBuckets`` key at all
    # (only ``jobStartLatencyBuckets``); 2.5.1 added it and templates it
    # straight into ``--metrics-job-submit-latency-buckets``. Reusing a
    # pre-2.5.0 release's values therefore renders that flag empty, which the
    # controller's own flag parser rejects at startup: `invalid argument ""
    # for "--metrics-job-submit-latency-buckets" flag: strconv.ParseFloat:
    # parsing "": invalid syntax` -- a crash loop, not a webhook/RBAC issue.
    # Verified live 2026-07-26 upgrading an existing 2.4.0 release to 2.5.1.
    # Backfilling this explicitly on every version-pinned upgrade closes the
    # gap regardless of what the stored release happens to already have.
    #
    # Helm's own --set parser splits on unescaped commas to separate keys
    # (`--set` docs: "key1=val1,key2=val2") -- this is Helm's parser, not
    # shell word-splitting, so passing the raw value through subprocess.run's
    # argv list does NOT avoid it. A first attempt at this fix passed the
    # bare comma-separated bucket list and failed live with `Error: failed
    # parsing --set data: key "1" has no value (cannot end with ,)` --
    # every comma must be backslash-escaped.
    _JOB_SUBMIT_LATENCY_BUCKETS_DEFAULT = r"0.5\,1\,2\,4\,8\,16\,32\,64\,128\,256"

    def _reuse_values_backfill(
        self, version: str | None, tmp_size: str = DEFAULT_CONTROLLER_TMP_SIZE
    ) -> list[str]:
        """``--set`` args for an admin ``--reuse-values`` upgrade of the release.

        The latency buckets are only needed when the upgrade pins a version
        (a jump from a pre-2.5.0 release). The controller /tmp size is set
        every time: a release installed before lakebench sized it has no
        ``controller.volumes`` in its stored values, so ``--reuse-values``
        would keep the chart's 1Gi (see operator_scratch). The watch-list
        edits do not call this; their ``--reuse-values`` carries the size
        forward once an admin install or repair has stored it.
        """
        args: list[str] = []
        if version:
            args += [
                "--set",
                "prometheus.metrics.jobSubmitLatencyBuckets="
                f"{self._JOB_SUBMIT_LATENCY_BUCKETS_DEFAULT}",
            ]
        # "" means keep the stored /tmp volume (an unbounded one) untouched.
        return args + (controller_tmp_helm_set_args(tmp_size) if tmp_size else [])

    @staticmethod
    def _watch_list_hash(namespaces: list[str] | None) -> str:
        """Content hash of a watch list.

        Order-independent so a re-read that returns the same set in a
        different order compares equal. ``None`` means "watch all" and
        gets a distinct sentinel so no concrete list ever collides with
        it. Whitespace is trimmed and empty entries are dropped so a
        stray "" or " ns " does not falsely differ from "ns".

        Used by the add/remove watch-list paths to short-circuit
        ``helm upgrade`` when the current spec already matches the
        desired one -- a helm upgrade against the shared release is
        every deploy's slowest step and every parallel destroy's
        largest contention point, so skipping when nothing changed is
        both a correctness win (no chance to lose the race) and a wall
        clock win.
        """
        if namespaces is None:
            return "sha256:watch-all"
        cleaned = sorted({n.strip() for n in namespaces if n and n.strip()})
        digest = hashlib.sha256("\n".join(cleaned).encode("utf-8")).hexdigest()
        return f"sha256:{digest}"

    def _watch_list_pin(self) -> list[str] | None:
        """``--version``/backfill args for a watch-list-only ``helm upgrade``.

        Adding or removing a namespace must not change the shared operator's
        chart. Without ``--version`` Helm resolves whatever the local repo
        serves (a silent upgrade for every deployment on the cluster); with
        the config's version a developer's older pin would downgrade an
        admin's newer install. So pin the installed release's chart, read
        afresh here (the callers hold the cluster lease, so it cannot change
        before the upgrade). None when it cannot be read: the caller refuses
        the upgrade rather than guess a version.
        """
        pin = self._get_helm_version()
        if not pin:
            logger.error(
                "Cannot read the installed Spark Operator chart version; refusing a watch-list "
                "upgrade that could move the shared operator to another chart"
            )
            return None
        return [
            "--version",
            pin,
            "--set",
            f"prometheus.metrics.jobSubmitLatencyBuckets={self._JOB_SUBMIT_LATENCY_BUCKETS_DEFAULT}",
        ]

    def __init__(
        self,
        namespace: str | None = None,
        version: str | None = None,
        job_namespace: str | None = None,
        kube_context: str | None = None,
    ):
        """Initialize Spark Operator manager.

        Args:
            namespace: Namespace for operator (default: spark-operator)
            version: Helm chart version to install (default: latest)
            job_namespace: Namespace where SparkApplications will be created.
                Passed to the Helm chart as ``spark.jobNamespaces``.
                If not set, the chart default (``default``) is used.
        """
        self.namespace = namespace or self.DEFAULT_NAMESPACE
        # The chart a fresh install uses (admin install only). Watch-list edits
        # never use it: they pin the installed chart (see _watch_list_pin).
        self.target_version = version
        self.job_namespace = job_namespace
        # The config's kubeconfig context. helm and kubectl otherwise use the
        # ambient current context, so a stale or different current context
        # would read (and change) another cluster's operator.
        self.kube_context = kube_context or None

    def _with_context(self, cmd: list[str]) -> list[str]:
        """Add the configured kube context to a helm/kubectl/oc command.

        Retained for tests that exercise the flag placement directly.
        Callers go through :meth:`_run`, which routes each tool through
        the pinned helper in :mod:`lakebench.k8s._pinned`.
        """
        if not self.kube_context or not cmd:
            return cmd
        if cmd[0] == "helm":
            return [cmd[0], "--kube-context", self.kube_context, *cmd[1:]]
        if cmd[0] in ("kubectl", "oc"):
            return [cmd[0], "--context", self.kube_context, *cmd[1:]]
        return cmd

    def _run(self, cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        """Run a kubectl/helm/oc command through the pinned helpers.

        Every subprocess site in this module goes through here so that a
        stale ambient kube-context can never touch the wrong cluster.
        The lint in ``tests/test_pinned_kubectl_helper.py`` whitelists
        exactly this method (plus the helpers themselves).
        """
        if not cmd:
            raise ValueError("empty command")
        tool = cmd[0]
        args = list(cmd[1:])
        if tool != os.path.basename(tool) and os.path.basename(tool) in ("kubectl", "helm", "oc"):
            # A path would skip the pinned-context helpers below.
            raise ValueError(f"call {os.path.basename(tool)} by name, not as {tool!r}")
        ctx = self.kube_context
        if tool == "kubectl":
            return pinned_kubectl(ctx, args, **kwargs)
        if tool == "helm":
            return pinned_helm(ctx, args, **kwargs)
        if tool == "oc":
            return pinned_oc(ctx, args, **kwargs)
        # A non-cluster tool (e.g. ``python`` in a debug branch) has no
        # kube context to pin, so fall back to a plain subprocess.run.
        return subprocess.run(cmd, **kwargs)  # noqa: S603

    def check_status(self) -> OperatorStatus:
        """Check if Spark Operator is installed and ready.

        Returns:
            OperatorStatus with current state
        """
        try:
            # Check if CRD exists
            result = self._run(
                ["kubectl", "get", "crd", "sparkapplications.sparkoperator.k8s.io"],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                err = (result.stderr or "").strip()
                if "notfound" in err.lower().replace(" ", ""):
                    return OperatorStatus(
                        installed=False,
                        version=None,
                        namespace=None,
                        ready=False,
                        message="SparkApplication CRD not found",
                    )
                # An API that is down, a refused read or a bad context is not
                # "not installed": nobody should be told to install over it.
                return OperatorStatus(
                    installed=None,
                    version=None,
                    namespace=None,
                    ready=False,
                    message=f"could not read the SparkApplication CRD: {err or 'kubectl failed'}",
                )

            # Check if operator deployment exists
            result = self._run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    "-A",
                    "-l",
                    "app.kubernetes.io/name=spark-operator",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                return OperatorStatus(
                    installed=None,
                    version=None,
                    namespace=None,
                    ready=False,
                    message=(
                        "could not list the operator Deployments: "
                        f"{(result.stderr or '').strip() or 'kubectl failed'}"
                    ),
                )
            if "No resources" in result.stdout:
                return OperatorStatus(
                    installed=True,
                    version=None,
                    namespace=None,
                    ready=False,
                    message="CRD exists but operator deployment not found",
                )

            # Parse operator namespace
            lines = result.stdout.strip().split("\n")
            if len(lines) < 2:
                return OperatorStatus(
                    installed=True,
                    version=None,
                    namespace=None,
                    ready=False,
                    message="Could not parse operator deployment",
                )

            # First column is namespace
            parts = lines[1].split()
            operator_ns = parts[0] if parts else self.namespace

            # Check if operator is ready
            result = self._run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    "-n",
                    operator_ns,
                    "-l",
                    "app.kubernetes.io/name=spark-operator",
                    "-o",
                    "jsonpath={.items[0].status.readyReplicas}",
                ],
                capture_output=True,
                text=True,
            )

            ready_replicas = int(result.stdout.strip() or "0")
            is_ready = ready_replicas > 0

            # Get version from Helm release if possible
            version = self._get_helm_version()

            # Check namespace watching -- use the deployment spec args as
            # ground truth, NOT Helm values (which can be out of sync after
            # a failed or partial helm upgrade).
            watching_namespace = None
            watched_namespaces = None
            if is_ready and self.job_namespace:
                try:
                    watched = self._get_active_namespaces(operator_ns)
                except _DeploymentReadError:
                    # Could not read deployment spec, fall back to Helm values
                    try:
                        watched = self._get_watched_namespaces()
                    except _WatchListReadError as e:
                        logger.warning("Spark Operator watch list unreadable: %s", e)
                        watched = []  # reported below as "could not determine"
                if watched is None:
                    # Watches all namespaces (empty or unset)
                    watching_namespace = True
                elif len(watched) == 0:
                    # Could not determine (helm error)
                    watching_namespace = None
                else:
                    watched_namespaces = watched
                    watching_namespace = self.job_namespace in watched

            if is_ready and watching_namespace is False:
                message = (
                    f"Spark Operator is ready but does NOT watch namespace "
                    f"'{self.job_namespace}'. Watched: {watched_namespaces}"
                )
            elif is_ready:
                message = "Spark Operator is ready"
            else:
                message = "Spark Operator not ready"

            return OperatorStatus(
                installed=True,
                version=version,
                namespace=operator_ns,
                ready=is_ready,
                message=message,
                watching_namespace=watching_namespace,
                watched_namespaces=watched_namespaces,
            )

        except Exception as e:
            # Unknown, not absent: nothing may install or report "missing"
            # on a read that failed.
            return OperatorStatus(
                installed=None,
                version=None,
                namespace=None,
                ready=False,
                message=f"Error checking operator status: {e}",
            )

    def _get_helm_version(self) -> str | None:
        """Chart version of this manager's Spark Operator release, or None.

        Looks only in ``self.namespace`` and matches the release name
        exactly: the watch-list edits pin ``--version`` to this value, so a
        look-alike release elsewhere (``my-spark-operator``) must never be
        read. A chart string that does not end in a version yields None,
        and the watch-list edits then refuse rather than guess a version.
        """
        import json
        import re

        try:
            result = self._run(
                [
                    "helm",
                    "list",
                    "-n",
                    self.namespace,
                    "--all",
                    "-f",
                    f"^{self.HELM_RELEASE_NAME}$",
                    "-o",
                    "json",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                return None
            for rel in json.loads(result.stdout) or []:
                if rel.get("name") != self.HELM_RELEASE_NAME:
                    continue
                m = re.search(r"-(\d+\.\d+\.\d+[0-9A-Za-z.+-]*)$", rel.get("chart", ""))
                return m.group(1) if m else None
            return None
        except Exception:
            return None

    def _get_active_namespaces(
        self, operator_ns: str | None = None, deployment: str | None = None
    ) -> list[str] | None:
        """Get namespaces from an operator Deployment's spec (the controller
        unless *deployment* names another, such as the webhook).

        Reads the ``--namespaces=...`` arg from the deployment's pod template.
        This is the ground truth -- what the controller will actually watch
        when its pods start.  Helm values can be out of sync after a failed
        or partial upgrade.

        Returns:
            List of namespace strings if ``--namespaces`` is set,
            None if the arg is absent or empty (watches all namespaces).
            Raises ``_DeploymentReadError`` on error so the caller can
            distinguish "watches all" from "could not read".
        """
        ns = operator_ns or self.namespace
        try:
            result = self._run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    deployment or self.CONTROLLER_DEPLOYMENT,
                    "-n",
                    ns,
                    "-o",
                    "jsonpath={.spec.template.spec.containers[0].args}",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0 or not result.stdout:
                raise _DeploymentReadError("kubectl failed or empty output")

            import json

            try:
                args = json.loads(result.stdout)
            except (json.JSONDecodeError, ValueError) as exc:
                raise _DeploymentReadError(f"bad JSON: {result.stdout!r}") from exc

            for arg in args:
                if isinstance(arg, str) and arg.startswith("--namespaces="):
                    ns_str = arg.split("=", 1)[1].strip('"').strip("'")
                    if not ns_str:
                        return None  # Empty -- watches all
                    return [
                        n.strip().strip('"').strip("'")
                        for n in ns_str.split(",")
                        if n.strip().strip('"').strip("'")
                    ]

            return None  # No --namespaces arg -- watches all

        except _DeploymentReadError:
            raise
        except Exception as e:
            raise _DeploymentReadError(str(e)) from e

    def _no_release_watch_list(self) -> list[str]:
        """Answer for "helm has no release": [] only if no operator runs.

        A controller Deployment can exist without this Helm release (OLM, a
        different release name). Its watch list cannot be read or changed
        through Helm, so that case, and an unanswerable check, raise rather
        than report "nothing watched".
        """
        probe = self._run(
            ["kubectl", "get", "deployment", "spark-operator-controller", "-n", self.namespace],
            capture_output=True,
            text=True,
        )
        if probe.returncode == 0:
            raise _WatchListReadError(
                f"Helm release {self.HELM_RELEASE_NAME!r} not found in {self.namespace!r}, "
                "but a spark-operator-controller Deployment exists there; the "
                "operator is managed outside this Helm release, so lakebench "
                "cannot read or change its watch list."
            )
        if "notfound" in (probe.stderr or "").lower().replace(" ", ""):
            return []
        raise _WatchListReadError(
            f"could not confirm whether a Spark Operator runs in {self.namespace!r}: "
            f"{(probe.stderr or '').strip()}"
        )

    def _get_watched_namespaces(self, revision: int | None = None) -> list[str] | None:
        """Get the namespaces the Spark Operator is configured to watch.

        Uses ``--all`` to include chart defaults (the chart defaults
        ``spark.jobNamespaces`` to ``["default"]``, NOT "all namespaces").

        Returns:
            List of namespace strings if jobNamespaces is set, None if the
            operator watches all namespaces, or ``[]`` if the Helm release
            does not exist (no operator, so nothing is watched).

        Raises:
            _WatchListReadError: the values could not be read for any other
                reason (helm missing, RBAC, unparseable output).
        """
        try:
            import json

            result = self._run(
                [
                    "helm",
                    "get",
                    "values",
                    self.HELM_RELEASE_NAME,
                    "-n",
                    self.namespace,
                    "--all",
                    "-o",
                    "json",
                    *(["--revision", str(revision)] if revision is not None else []),
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                # helm prints "Error: release: not found" (older versions
                # omit the colon). No release means no operator, so nothing
                # is watched; that is a real answer, not a read failure.
                if re.search(r"release:? not found", (result.stderr or "").lower()):
                    return self._no_release_watch_list()
                raise _WatchListReadError(
                    f"helm get values {self.HELM_RELEASE_NAME} failed: "
                    f"{(result.stderr or '').strip()}"
                )

            values = json.loads(result.stdout)
            ns_value = values.get("spark", {}).get("jobNamespaces", None)

            if ns_value is None:
                return None  # Not set -- watches all
            if isinstance(ns_value, str):
                if ns_value == "":
                    return None  # Empty string -- watches all
                return [ns_value]
            if isinstance(ns_value, list):
                if "" in ns_value:
                    return None  # the chart renders --namespaces="" (all)
                filtered = [ns for ns in ns_value if ns]
                return filtered if filtered else None
            return None  # Unknown type -- assume watches all

        except _WatchListReadError:
            raise
        except Exception as e:
            raise _WatchListReadError(f"error reading Helm values: {e}") from e

    # Delays before each namespace read in _namespace_is_terminating (s).
    _NS_READ_BACKOFF = (0.0, 0.5, 1.5)

    def _namespace_is_terminating(self, namespace: str) -> bool:
        """True unless the namespace provably exists and is not being deleted.

        Gone counts as terminating: a destroy can finish deleting the
        namespace while a deploy waits for the lease, and adding a namespace
        that does not exist crash-loops the operator for every deployment.
        Any read failure (429, 5xx, transport, no client) also refuses: under
        API throttling a "probably fine" add is exactly the crash-loop route.
        """
        from kubernetes.client.rest import ApiException

        ns = None
        for attempt, delay in enumerate(self._NS_READ_BACKOFF, start=1):
            if delay:
                time.sleep(delay)
            try:
                from kubernetes import client as k8s_client

                from lakebench.k8s.lease_state import request_timeout_kw

                ns = k8s_client.CoreV1Api().read_namespace(namespace, **request_timeout_kw())
                break
            except ApiException as e:
                if e.status == 404:
                    return True
                logger.warning(
                    "Could not read namespace %s before adding it (attempt %d): %s",
                    namespace,
                    attempt,
                    e,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Could not read namespace %s before adding it (attempt %d): %s",
                    namespace,
                    attempt,
                    e,
                )
        if ns is None:
            # A transient 429/5xx is retried above; a persistent one refuses.
            return True
        meta = getattr(ns, "metadata", None)
        ts = getattr(meta, "deletion_timestamp", None)
        # The client deserialises it as a datetime; anything else (absent,
        # or a test double) is not proof of deletion.
        import datetime as _dt

        return isinstance(ts, (_dt.datetime, str)) and bool(ts)

    def _filter_existing_namespaces(self, namespaces: list[str]) -> list[str]:
        """Return only namespaces that exist on the cluster.

        Stale namespaces from previous test runs cause ``helm upgrade`` to
        fail when the operator tries to create resources in non-existent
        namespaces.
        """
        try:
            from kubernetes import client as k8s_client

            from lakebench.k8s.lease_state import request_timeout_kw

            core_v1 = k8s_client.CoreV1Api()
            existing = {
                ns.metadata.name for ns in core_v1.list_namespace(**request_timeout_kw()).items
            }
            live = [ns for ns in namespaces if ns in existing]
            removed = set(namespaces) - set(live)
            if removed:
                logger.info(
                    "Pruning stale namespaces from spark.jobNamespaces: %s",
                    removed,
                )
            return live
        except Exception as e:
            logger.debug("Could not list namespaces, keeping all: %s", e)
            return namespaces

    def recreate_namespace_rbac(self, namespace: str) -> bool:
        """Force-recreate RBAC for a namespace the operator already watches.

        After ``lakebench destroy`` deletes a namespace and ``deploy``
        recreates it, the operator's per-namespace Role/RoleBinding are
        lost.  ``_add_namespace_to_watch`` exits early because the
        namespace is already in ``spark.jobNamespaces``.

        This method forces Helm to regenerate the RBAC by removing the
        namespace and re-adding it in two ``helm upgrade`` calls, then
        applies the OpenShift SCC patch and restarts the controller.

        Args:
            namespace: The namespace whose RBAC should be recreated.

        Returns:
            True if the RBAC was successfully recreated.
        """
        # The remove-then-re-add is a read-modify-write of the shared
        # watch list, so the whole sequence runs under one lease: an
        # unlocked step 1 writing a list read before a concurrent add
        # would drop that deployment's namespace.
        lease_cm, mode = self._acquire_watch_lease()
        try:
            if mode == "refuse":
                logger.error(
                    "spark-operator watch-list: refusing to recreate RBAC for %r -- "
                    "cluster lease is held or lease RBAC denied",
                    namespace,
                )
                return False
            try:
                return self._recreate_namespace_rbac_impl(namespace)
            except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
                logger.error(
                    "Recreating RBAC for %s stopped inside the cluster lease: %s", namespace, e
                )
                return False
        finally:
            if lease_cm is not None:
                try:
                    lease_cm.__exit__(None, None, None)
                except Exception as e:  # noqa: BLE001
                    logger.warning("spark-operator watch-list: lease release failed: %s", e)

    def _recreate_namespace_rbac_impl(self, namespace: str) -> bool:
        """Body of ``recreate_namespace_rbac``; caller holds the lease."""
        try:
            watched = self._get_watched_namespaces()
        except _WatchListReadError as e:
            logger.error("Cannot recreate RBAC for %s: %s", namespace, e)
            return False
        if watched is None:
            return True
        if namespace not in watched:
            # Not in the watch list -- delegate to normal add flow
            return self._add_namespace_to_watch_impl(namespace)

        # Step 1: Remove the namespace so Helm deletes the Role/RoleBinding
        without_ns = [ns for ns in watched if ns != namespace]
        ns_set_without = ",".join(without_ns) if without_ns else "default"
        cmd = [
            "helm",
            "upgrade",
            self.HELM_RELEASE_NAME,
            self.HELM_CHART_NAME,
            "-n",
            self.namespace,
            "--reuse-values",
            "--set",
            f"spark.jobNamespaces={{{ns_set_without}}}",
        ]
        # Pin the installed chart (see _watch_list_pin); with no readable
        # version the upgrade would let Helm pick the repo's latest chart.
        pin = self._watch_list_pin()
        if pin is None:
            return False
        cmd.extend(pin)
        result = self._run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_Phase(WATCH_HELM_PHASE_S).helm_attempt_s("helm upgrade (RBAC recreate)"),
        )
        if result.returncode != 0:
            logger.error(
                "helm upgrade (remove namespace) failed: %s",
                result.stderr,
            )
            return False

        logger.info(
            "Removed namespace '%s' from spark.jobNamespaces to force RBAC recreation",
            namespace,
        )

        # Step 2: Re-add the namespace -- Helm will create fresh RBAC
        return self._add_namespace_to_watch_impl(namespace)

    def remove_namespace_from_watch(
        self,
        namespace: str,
        *,
        strict: bool = False,
        precondition: Callable[[], None] | None = None,
        then: Callable[[], None] | None = None,
    ) -> bool:
        """Drop a namespace from the operator's watch list before deleting it.

        A watched namespace that does not exist is not a harmless leftover:
        the controller cannot establish a Pod watch on it, so its cache never
        syncs and it crash-loops with ``failed to wait for
        spark-application-controller caches to sync``. That takes down
        SparkApplication reconciliation for *every* namespace on the cluster,
        not just the one being destroyed, and the symptom appears later and
        elsewhere.

        Safe to call when the operator is absent or watches all namespaces --
        both are reported as success, since there is nothing to remove.

        Args:
            namespace: The namespace about to be deleted.
            strict: When True, the call is lease-gated against the
                cluster-wide ``lakebench-cluster-lock`` (serialises
                parallel destroys mutating the shared watch list) and
                any failure raises ``WatchListMutationError`` instead of
                returning False. Destroy paths use strict=True; older
                internal callers keep the historical bool contract.
            precondition: Called right before the watch list is read and
                changed; with ``strict`` it runs while the cluster lease is
                held. Raising aborts the call with the watch list untouched
                and the exception propagates unchanged. Destroy uses it to
                re-check the namespace UID inside the lease, so a same-named
                redeploy (whose watch-list add takes the same lease) can never
                have its entry removed by a slow destroy of the old one.
            then: Called after a successful removal, still under the lease
                with ``strict``. Destroy issues the namespace delete here, so
                a concurrent deploy's add (same lease) sees the namespace
                Terminating and refuses instead of re-adding it.

        Returns:
            True if the watch list no longer contains the namespace.
        """
        if strict:
            return self._remove_namespace_from_watch_locked(namespace, precondition, then)
        if precondition is not None:
            precondition()
        ok = self._remove_namespace_from_watch_unlocked(namespace)
        if ok and then is not None:
            then()
        return ok

    def _remove_namespace_from_watch_unlocked(self, namespace: str) -> bool:
        """Historical non-strict body: read, drop, helm upgrade, retry."""
        helm_phase = _Phase(WATCH_HELM_PHASE_S)
        for attempt in range(self._HELM_CONFLICT_RETRIES):
            try:
                watched = self._get_watched_namespaces()
            except _WatchListReadError as e:
                # Cannot prove the namespace is gone from the list. Reporting
                # success here would let destroy delete a watched namespace.
                logger.error("Cannot remove %s from the watch list: %s", namespace, e)
                return False
            if watched is None:
                # Watches all namespaces -- nothing namespace-specific to drop.
                return True
            if namespace not in watched:
                return True

            remaining = [ns for ns in watched if ns != namespace]
            # The chart rejects an empty list, and an empty jobNamespaces means
            # "watch all" rather than "watch none". Fall back to the chart's
            # own default so removing the last namespace does not silently
            # widen the operator's scope to the whole cluster.
            ns_set = ",".join(remaining) if remaining else "default"

            # LB-ux-safety C2: skip the helm upgrade when the desired
            # list already matches the current one. The remove path
            # already short-circuits when the namespace is absent
            # above; this catches the case where the desired list
            # collapses to the same effective content (chart-default
            # "default" fallback) as what is already there.
            desired = remaining or ["default"]
            if self._watch_list_hash(desired) == self._watch_list_hash(watched):
                logger.info(
                    "Spark Operator watch list already omits '%s' (%s); skipping helm upgrade",
                    namespace,
                    desired,
                )
                return True

            cmd = [
                "helm",
                "upgrade",
                self.HELM_RELEASE_NAME,
                self.HELM_CHART_NAME,
                "-n",
                self.namespace,
                "--reuse-values",
                "--set",
                f"spark.jobNamespaces={{{ns_set}}}",
            ]
            pin = self._watch_list_pin()
            if pin is None:
                return False
            cmd.extend(pin)

            try:
                result = self._run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=helm_phase.helm_attempt_s("helm upgrade (remove namespace)"),
                )
            except FileNotFoundError:
                logger.warning("helm not found on PATH -- cannot remove namespace from watch")
                return False

            if result.returncode == 0:
                logger.info(
                    "Removed namespace '%s' from spark.jobNamespaces (now: %s)",
                    namespace,
                    remaining or ["default"],
                )
                if self._is_openshift():
                    self._assign_openshift_scc()
                    self._patch_openshift_deployments()
                    # The patch rolls the pods; await it rather than let the
                    # restart supersede a rollout still in progress.
                    if not self._wait_for_rollout():
                        logger.error("Operator rollout after the OpenShift patch did not finish")
                        return False
                # A restart that fails leaves pods that may still list the
                # namespace: the strict remove fails and destroy keeps it.
                return self._restart_operator()

            if self._is_helm_conflict(result.stderr) and attempt + 1 < self._HELM_CONFLICT_RETRIES:
                delay = self._HELM_CONFLICT_BACKOFF * (attempt + 1)
                logger.warning(
                    "helm upgrade conflicted removing namespace '%s' "
                    "(attempt %d/%d), retrying in %.1fs",
                    namespace,
                    attempt + 1,
                    self._HELM_CONFLICT_RETRIES,
                    delay,
                )
                time.sleep(delay)
                continue

            logger.warning(
                "helm upgrade failed to remove namespace '%s': %s",
                namespace,
                result.stderr,
            )
            return False

        return False

    def _remove_namespace_from_watch_locked(
        self,
        namespace: str,
        precondition: Callable[[], None] | None = None,
        then: Callable[[], None] | None = None,
    ) -> bool:
        """Strict variant of ``remove_namespace_from_watch``.

        Acquires the cluster-wide lease so parallel destroys cannot race
        the same Helm upgrade, then delegates to the non-strict helper.
        Failure raises ``WatchListMutationError`` naming
        ``admin repair-operator`` as the recovery path. Success returns
        True to match the non-strict contract callers already expect.
        """
        from kubernetes import client as _kclient
        from kubernetes.client.exceptions import ApiException

        from lakebench.deploy.cluster_lock import (
            ClusterLockError,
            ClusterLockHeld,
            cluster_lock,
        )
        from lakebench.k8s.target import ContextConflictError, cli_args

        try:
            core_v1 = _kclient.CoreV1Api()
        except Exception as e:  # noqa: BLE001
            raise WatchListMutationError(
                f"cannot open Kubernetes client to acquire cluster lock: {e}. "
                "The operator's watch list was NOT modified; run "
                "`lakebench admin repair-operator` after the cluster is reachable."
            ) from e

        try:
            with cluster_lock(core_v1, timeout=_WATCH_LIST_LOCK_TIMEOUT_S):
                try:
                    # The pinned-context check once the lease is held
                    # and before the first mutation. A rewrite after it can
                    # still stop a later tool call; that is mapped below.
                    cli_args("helm", self.kube_context)
                except ContextConflictError as e:
                    raise WatchListMutationError(
                        f"{e}. The operator's watch list was NOT modified; run "
                        "destroy again once the kubeconfig names the deployment's "
                        "cluster."
                    ) from e
                if precondition is not None:
                    precondition()
                ok = self._remove_namespace_from_watch_impl(namespace)
                if ok and then is not None:
                    then()
        except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
            raise WatchListMutationError(
                f"a command inside the cluster lease ran out of time: {e}. The "
                "watch list may be partly changed; the namespace was NOT deleted. "
                "Run `lakebench admin repair-operator`, then destroy again."
            ) from e
        except ClusterLockHeld as e:
            raise WatchListMutationError(
                f"another lakebench process holds the cluster lock ({e.holder}); "
                "wait for it, or run `lakebench admin release-lock` once its lease has expired. "
                "The operator's watch list was NOT modified."
            ) from e
        except ClusterLockError as e:
            raise WatchListMutationError(
                f"could not acquire cluster lock: {e}. The operator's watch "
                "list was NOT modified; run `lakebench admin repair-operator` "
                "after clearing the underlying issue."
            ) from e
        except ApiException as e:
            raise WatchListMutationError(
                f"Kubernetes API error while acquiring cluster lock: {e}. "
                "The operator's watch list was NOT modified; run "
                "`lakebench admin repair-operator` after the cluster is reachable."
            ) from e
        except ContextConflictError as e:
            raise WatchListMutationError(
                f"{e}. The operator's watch list may be partly modified; once "
                "the kubeconfig names the deployment's cluster again, run "
                "`lakebench admin repair-operator` against it."
            ) from e

        if not ok:
            raise WatchListMutationError(
                f"failed to remove namespace '{namespace}' from the Spark "
                "Operator watch list. The operator may crash-loop on the "
                "stale entry, which would break SparkApplication reconciliation "
                "for every namespace on the cluster. The namespace was NOT "
                "deleted. Run `lakebench admin repair-operator` to reconcile the "
                "watch list against live namespaces (when the restart after the "
                "removal failed the list is already right), then destroy again "
                "once the operator pods are Ready."
            )
        return True

    def _remove_namespace_from_watch_impl(self, namespace: str) -> bool:
        """Non-strict watch-list drop reused by ``_locked``.

        Kept as a thin re-entry into the historical implementation to
        avoid duplicating the retry/backoff logic. Returns False on
        failure so the ``_locked`` wrapper can raise with a specific
        error message.
        """
        return self._remove_namespace_from_watch_unlocked(namespace)

    @classmethod
    def _is_helm_conflict(cls, stderr: str) -> bool:
        """Return True when Helm stderr indicates contention, not misconfig.

        Only contention is worth retrying.  A genuine error (bad chart, no
        such release, RBAC denial) should fail fast rather than repeat five
        times and then report the same thing several seconds later.
        """
        lowered = (stderr or "").lower()
        return any(marker in lowered for marker in cls._HELM_CONFLICT_MARKERS)

    def _add_namespace_to_watch(self, namespace: str, _retry_on_eviction: bool = True) -> bool:
        """Add a namespace to the Spark Operator's watched namespaces.

        Uses ``helm upgrade --reuse-values`` to preserve existing config.

        ``spark.jobNamespaces`` is cluster-scoped state shared by every
        lakebench deployment, and adding to it is a read-modify-write.  Two
        deploys running at once will each read the list before the other
        writes, so the second upgrade silently drops the first one's
        namespace -- a deploy that reported success ends up unwatched and its
        SparkApplications are never reconciled.  Helm serialises upgrades
        against a release, so the conflict surfaces either as an outright
        failure or as a lost update.  Both are handled by re-reading the list
        and retrying rather than by assuming the first read is still valid.

        ADR-F5: this method is lease-gated (symmetric with the strict
        remove path). Without the lease a concurrent strict destroy of
        namespace Y racing this add of namespace X can produce a
        watch-list that includes Y even after Y's destroy dropped it --
        Y then gets deleted, and the operator crash-loops on the stale
        entry the add path re-introduced. The lease is best-effort: on
        infrastructure failure (workstation with no cluster, unit
        tests) we log-warn and proceed unlocked; production always has
        access to the lease namespace.

        Args:
            namespace: The namespace to add.
            _retry_on_eviction: Internal. Allows exactly one re-add when a
                concurrent writer drops this namespace after a successful
                upgrade. Bounded to one to avoid two deploys ping-ponging.

        Returns:
            True if helm upgrade succeeded. False if the shared cluster
            lease is held by another process (real contention) or the
            lease infrastructure denied RBAC access -- in both cases,
            proceeding unlocked would reopen the exact race F5 closes,
            so we refuse rather than press on.
        """
        # ADR-F5b: distinguish lease-infra-genuinely-absent (workstation
        # without a cluster, unit tests) from real contention. The
        # first case is safe to proceed unlocked -- there is no other
        # writer to race with. The second case must NOT proceed
        # unlocked; if we did, a concurrent strict destroy holding the
        # lease could drop namespace Y while our unlocked add of X
        # writes back a list that still contains Y, and the ns delete
        # that follows crashes the operator globally.
        lease_cm, mode = self._acquire_watch_lease()
        try:
            if mode == "refuse":
                logger.error(
                    "spark-operator watch-list: refusing to add %r -- cluster "
                    "lease is held (concurrent destroy) or lease RBAC denied. "
                    "Retry when the holder releases; if the operator was left "
                    "inconsistent by a prior failure, run "
                    "`lakebench admin repair-operator`.",
                    namespace,
                )
                return False
            from lakebench.k8s.target import ContextConflictError, cli_args

            try:
                # The pinned-context check before the first mutation.
                cli_args("helm", self.kube_context)
            except ContextConflictError as e:
                logger.error("spark-operator watch-list: refusing to add %r -- %s", namespace, e)
                return False
            try:
                return self._add_namespace_to_watch_impl(namespace, _retry_on_eviction)
            except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
                # A command ran out of the lease's time: fail closed, the
                # namespace is not proven watched.
                logger.error(
                    "Adding %s to the watch list stopped inside the cluster lease: %s", namespace, e
                )
                return False
            except ContextConflictError as e:
                # The kubeconfig changed during the sequence: if it was after
                # the helm upgrade, the operator may be upgraded without its
                # OpenShift patches or restart.
                logger.error(
                    "spark-operator watch-list: adding %r stopped -- %s. The operator's "
                    "watch list may be partly modified; once the kubeconfig names the "
                    "deployment's cluster again, run `lakebench admin repair-operator` "
                    "against it.",
                    namespace,
                    e,
                )
                return False
        finally:
            if lease_cm is not None:
                try:
                    lease_cm.__exit__(None, None, None)
                except Exception as e:  # noqa: BLE001
                    logger.warning("spark-operator watch-list: lease release failed: %s", e)

    def _acquire_watch_lease(self):
        """Return (lease_ctx | None, mode).

        - mode == "locked":   we hold the lease; lease_ctx is the entered
          context to __exit__ later.
        - mode == "unlocked": there is no cluster to acquire against (no
          kubeconfig, or connection refused). Safe to proceed -- no other
          writer can be racing us. lease_ctx is None.
        - mode == "refuse":   the lease is held by another process or the
          API denied access. lease_ctx is None. Caller must NOT proceed.
        """
        if getattr(self, "_bypass_cluster_lock", False):
            return None, "unlocked"

        from kubernetes import client as _kclient

        from lakebench.deploy.cluster_lock import (
            ClusterLockError,
            ClusterLockHeld,
            cluster_lock,
        )

        try:
            core_v1 = _kclient.CoreV1Api()
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "spark-operator watch-list: no k8s client, proceeding unlocked: %s",
                e,
            )
            return None, "unlocked"

        lease_cm = cluster_lock(core_v1, timeout=_WATCH_LIST_LOCK_TIMEOUT_S)
        try:
            lease_cm.__enter__()
            return lease_cm, "locked"
        except ClusterLockHeld as e:
            logger.warning("spark-operator watch-list: lease held by %s", e.holder)
            return None, "refuse"
        except ClusterLockError as e:
            # RBAC denial, API failure managing the lease namespace,
            # unreachable cluster with a *loaded* kubeconfig -- any of
            # these mean either (a) production integrity requires the
            # lease and it's broken, or (b) we can't tell whether
            # someone else is mutating the same state. Either way,
            # refuse rather than paper over.
            #
            # LB-ux-safety C2: "timed out" used to fall through to
            # "unlocked" here. That is exactly the case where another
            # holder is likely doing something with the shared watch
            # list, so proceeding unlocked would silently overwrite
            # their edit. Failing closed refuses the request instead.
            # "Connection refused" and DNS-resolution failures remain
            # unlocked: those are workstation-with-no-cluster, where
            # there is no other writer to race by definition.
            msg = str(e).lower()
            if "connection refused" in msg or "not resolve" in msg:
                logger.warning(
                    "spark-operator watch-list: cluster unreachable, proceeding unlocked: %s",
                    e,
                )
                return None, "unlocked"
            logger.warning(
                "spark-operator watch-list: lease acquire failed, refusing: %s",
                e,
            )
            return None, "refuse"
        except Exception as e:  # noqa: BLE001
            # urllib3 transport errors on a broken kubeconfig with no
            # reachable API server (ConnectionError, MaxRetryError, DNS
            # resolution failures) look like workstation-no-cluster:
            # proceed unlocked because there is no other writer.
            # Anything else -- including a bare timeout while the API
            # server is reachable -- refuses: we cannot tell whether
            # someone else is holding the lease and about to write the
            # watch list, and papering over that is the exact race F5
            # exists to close.
            msg = str(e).lower()
            if "connection refused" in msg or "not resolve" in msg or "no route to host" in msg:
                logger.warning(
                    "spark-operator watch-list: cluster unreachable, proceeding unlocked: %s",
                    e,
                )
                return None, "unlocked"
            logger.warning(
                "spark-operator watch-list: lease unavailable, refusing: %s",
                e,
            )
            return None, "refuse"

    def _add_namespace_to_watch_impl(self, namespace: str, _retry_on_eviction: bool = True) -> bool:
        """Non-lease-gated body of ``_add_namespace_to_watch``.

        Split out so ADR-F5's lease acquisition wraps only the mutation
        loop, and existing test callers that patch the mutation body
        keep working. Callers on the deploy path must go through
        ``_add_namespace_to_watch`` (which lease-gates); this variant is
        internal.
        """
        new_list: list[str] = []

        helm_phase = _Phase(WATCH_HELM_PHASE_S)
        for attempt in range(self._HELM_CONFLICT_RETRIES):
            # Re-read on every attempt.  A retry exists precisely because
            # another writer may have changed the list since the last read,
            # so reusing the earlier value would re-introduce the lost update.
            try:
                watched = self._get_watched_namespaces()
            except _WatchListReadError as e:
                # Writing [namespace] now would drop every other deployment's
                # namespace from the list.
                logger.error("Cannot add %s to the watch list: %s", namespace, e)
                return False
            if watched is None:
                # Already watches all namespaces
                return True
            if namespace in watched:
                return True
            if self._namespace_is_terminating(namespace):
                # A destroy dropped it from the list and deleted it (both
                # under this lease). Re-adding it would leave the operator
                # watching a namespace about to vanish, which crash-loops it
                # for every deployment on the cluster.
                logger.error(
                    "Refusing to add %s to the watch list: the namespace is being deleted, "
                    "gone, or could not be read",
                    namespace,
                )
                return False

            # Filter out stale namespaces that no longer exist on the cluster
            live_namespaces = self._filter_existing_namespaces(watched)
            new_list = live_namespaces + [namespace]

            # LB-ux-safety C2: if the desired list already matches the
            # current one, skip helm upgrade entirely -- and the
            # operator restart that would otherwise follow. The chart
            # is rendered from stored values plus --set, so an
            # "upgrade" with no change still churns the release
            # history, still takes the release-level lock, still
            # rolls the controller pods, and still contends with every
            # other parallel deploy. Compare by content hash so a
            # reordered read (kubectl ordering is not stable) is
            # treated as equal.
            if self._watch_list_hash(new_list) == self._watch_list_hash(watched):
                logger.info(
                    "Spark Operator watch list already includes '%s' (%s); skipping helm upgrade",
                    namespace,
                    new_list,
                )
                return True

            ns_set = ",".join(new_list)

            cmd = [
                "helm",
                "upgrade",
                self.HELM_RELEASE_NAME,
                self.HELM_CHART_NAME,
                "-n",
                self.namespace,
                "--reuse-values",
                "--set",
                f"spark.jobNamespaces={{{ns_set}}}",
            ]
            # --reuse-values carries values forward but NOT the chart version,
            # so omitting this lets Helm re-resolve to whatever the repo now
            # serves.  A namespace add would then silently upgrade the
            # operator out from under a pinned config.
            pin = self._watch_list_pin()
            if pin is None:
                return False
            cmd.extend(pin)

            try:
                result = self._run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=helm_phase.helm_attempt_s("helm upgrade (add namespace)"),
                )
            except FileNotFoundError:
                logger.error("helm not found on PATH -- cannot add namespace")
                return False

            if result.returncode == 0:
                break

            if self._is_helm_conflict(result.stderr) and attempt + 1 < self._HELM_CONFLICT_RETRIES:
                delay = self._HELM_CONFLICT_BACKOFF * (attempt + 1)
                logger.warning(
                    "helm upgrade conflicted adding namespace '%s' "
                    "(attempt %d/%d), retrying in %.1fs: %s",
                    namespace,
                    attempt + 1,
                    self._HELM_CONFLICT_RETRIES,
                    delay,
                    result.stderr.strip(),
                )
                time.sleep(delay)
                continue

            logger.error(
                "helm upgrade failed to add namespace '%s': %s",
                namespace,
                result.stderr,
            )
            return False
        else:
            logger.error(
                "helm upgrade still conflicting after %d attempts for namespace '%s'",
                self._HELM_CONFLICT_RETRIES,
                namespace,
            )
            return False

        logger.info(
            "Added namespace '%s' to spark.jobNamespaces (now: %s)",
            namespace,
            new_list,
        )

        # On OpenShift, ``helm upgrade`` regenerates deployment manifests
        # from the chart template, which re-introduces the hardcoded fsGroup
        # and seccompProfile that were patched out during install.  Re-apply
        # the patches before restarting.
        if self._is_openshift():
            self._assign_openshift_scc()
            self._patch_openshift_deployments()
            if not self._wait_for_rollout():
                logger.error("Operator rollout after the OpenShift patch did not finish")
                return False

        # The Spark Operator reads jobNamespaces at startup and does not
        # watch for config changes.  Restart so it picks up the new list.
        if not self._restart_operator():
            logger.error("Operator restart failed after helm upgrade")
            return False

        # Verify the deployment spec includes the new namespace.  A successful
        # upgrade is not proof of a durable result: a concurrent deploy that
        # read the list before this write can land afterwards and drop this
        # namespace again.  That eviction is silent -- the operator simply
        # never reconciles this namespace's SparkApplications -- so treat a
        # missing namespace here as contention to be retried, not as a
        # terminal error.
        if not self._verify_namespace_watched(namespace, timeout=15):
            if _retry_on_eviction:
                logger.warning(
                    "Namespace '%s' was dropped from spark.jobNamespaces after a "
                    "successful upgrade -- a concurrent deploy overwrote it. Re-adding.",
                    namespace,
                )
                # We already hold the cluster lease; do not re-acquire.
                return self._add_namespace_to_watch_impl(namespace, _retry_on_eviction=False)
            logger.error(
                "Operator restarted but deployment spec does not include namespace '%s'",
                namespace,
            )
            return False

        return True

    def _restart_operator(self) -> bool:
        """Restart the Spark Operator to pick up config changes.

        The operator reads ``spark.jobNamespaces`` at startup only, so a
        ``helm upgrade`` alone is not enough -- both the controller and
        webhook pods must be recycled. The restart and both rollout waits
        share one ``WATCH_RESTART_PHASE_S`` budget (inside the lease's hold
        budget); a wait with no time left raises ``LeaseHoldExceeded``.

        Returns:
            True if both deployments restarted and rolled out successfully.
        """
        deployments = [self.CONTROLLER_DEPLOYMENT, self.WEBHOOK_DEPLOYMENT]
        phase = _Phase(WATCH_RESTART_PHASE_S)

        for deploy in deployments:
            result = self._run(
                [
                    "kubectl",
                    "rollout",
                    "restart",
                    f"deployment/{deploy}",
                    "-n",
                    self.namespace,
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning("Failed to restart %s: %s", deploy, result.stderr)
                return False

        logger.info("Restarting Spark Operator deployments to apply namespace changes")
        return self._await_rollouts(deployments, phase)

    def _await_rollouts(self, deployments: list[str], phase: _Phase) -> bool:
        """``kubectl rollout status`` for each Deployment, within one phase."""
        for deploy in deployments:
            wait = phase.wait_s(f"rollout of {deploy}")
            result = self._run(
                [
                    "kubectl",
                    "rollout",
                    "status",
                    f"deployment/{deploy}",
                    "-n",
                    self.namespace,
                    f"--timeout={wait}s",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning("Rollout of %s did not complete: %s", deploy, result.stderr)
                return False
        return True

    def _verify_namespace_watched(self, namespace: str, timeout: int = 60) -> bool:
        """Verify the operator controller deployment includes a namespace.

        Checks the deployment spec's container args for the target
        namespace in ``--namespaces=...``.  Uses the deployment spec
        (not running pods) because after a rollout restart there can be
        multiple pods and ``items[0]`` may hit the old terminating pod.

        Args:
            namespace: The namespace that should appear in the arg list.
            timeout: Seconds to wait for verification.

        Returns:
            True if the deployment spec includes the namespace.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                watched = self._get_active_namespaces()
            except _DeploymentReadError:
                time.sleep(3)
                continue

            if watched is None:
                # Watches all namespaces
                logger.info(
                    "Verified operator controller watches all namespaces (includes '%s')",
                    namespace,
                )
                return True
            if namespace in watched:
                logger.info(
                    "Verified operator controller is watching '%s'",
                    namespace,
                )
                return True
            time.sleep(3)

        logger.warning(
            "Could not verify operator is watching '%s' after %ds",
            namespace,
            timeout,
        )
        return False

    def _is_openshift(self) -> bool:
        """Detect whether we are running on an OpenShift cluster."""
        result = self._run(
            ["kubectl", "api-resources", "--api-group=security.openshift.io"],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0 and "security.openshift.io" in result.stdout

    def _assign_openshift_scc(self, strict: bool = False) -> None:
        """Grant anyuid to the Spark Operator ServiceAccounts on OpenShift.

        The controller and webhook pods need fsGroup 185, which the default
        restricted-v2 SCC rejects. The grant is the namespaced RoleBinding
        ``system:openshift:scc:anyuid`` in the operator namespace, made through
        the RBAC API (``ensure_scc_rolebinding``); no ``oc`` is run, and an
        existing grant is a read with no write.

        ``strict`` (install): a grant that cannot be made raises
        ``SCCGrantError``. Otherwise (the watch-list edits a deploy or destroy
        makes on an operator that is already running) a failure is logged: the
        grant was made at install, and the deploying user may have no rights
        in the operator namespace.
        """
        from kubernetes import client as k8s_client

        from lakebench.k8s.security import SCCGrantError, ensure_scc_rolebinding

        rbac_api = k8s_client.RbacAuthorizationV1Api()
        for sa in ("spark-operator-controller", "spark-operator-webhook"):
            try:
                ensure_scc_rolebinding(rbac_api, self.namespace, sa, "anyuid")
            except SCCGrantError as e:
                if strict:
                    raise
                logger.warning("Spark Operator SCC not re-checked: %s", e)
        logger.info("Spark Operator service accounts hold the anyuid SCC")

    def _patch_openshift_deployments(self) -> None:
        """Patch Spark Operator deployments for OpenShift compatibility.

        The Helm chart hardcodes fsGroup=185 and seccompProfile=RuntimeDefault
        in the pod/container security contexts.  These cannot be overridden via
        Helm values (deep merge behavior).  On OpenShift the restricted-v2 SCC
        rejects both, so we patch them out after install.
        """
        patch = [
            {"op": "remove", "path": "/spec/template/spec/securityContext/fsGroup"},
            {
                "op": "remove",
                "path": "/spec/template/spec/containers/0/securityContext/seccompProfile",
            },
        ]
        import json

        patch_json = json.dumps(patch)

        for deploy in ("spark-operator-controller", "spark-operator-webhook"):
            result = self._run(
                [
                    "kubectl",
                    "patch",
                    "deployment",
                    deploy,
                    "-n",
                    self.namespace,
                    "--type=json",
                    f"-p={patch_json}",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning("Failed to patch %s for OpenShift: %s", deploy, result.stderr)
            else:
                logger.info("Patched %s for OpenShift compatibility", deploy)

    def _release_exists(self) -> bool | None:
        """Whether this manager's Helm release exists; None when unreadable."""
        try:
            result = self._run(
                ["helm", "status", self.HELM_RELEASE_NAME, "-n", self.namespace],
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode == 0:
            return True
        if re.search(r"release:? not found", (result.stderr or "").lower()):
            return False
        return None

    def release_state(self) -> ReleaseState | None:
        """Status and revision of this manager's release; ``("absent", 0)``
        when there is none; None when unreadable."""
        import json

        try:
            result = self._run(
                ["helm", "status", self.HELM_RELEASE_NAME, "-n", self.namespace, "-o", "json"],
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            if re.search(r"release:? not found", (result.stderr or "").lower()):
                return ReleaseState("absent", 0)
            return None
        try:
            doc = json.loads(result.stdout or "{}")
            info = doc.get("info") or {}
            return ReleaseState(str(info.get("status") or ""), int(doc.get("version") or 0))
        except (ValueError, TypeError, AttributeError):
            return None

    def revision_created(self, revision: int) -> float | None:
        """When the API server created the release Secret of *revision* (helm
        writes it as the operation starts), as epoch seconds; None when it
        cannot be read. Server time, unlike helm's ``last_deployed``, which
        the helm client stamps from its own clock."""
        try:
            result = self._run(
                [
                    "kubectl",
                    "get",
                    "secret",
                    f"sh.helm.release.v1.{self.HELM_RELEASE_NAME}.v{revision}",
                    "-n",
                    self.namespace,
                    "-o",
                    "jsonpath={.metadata.creationTimestamp}",
                ],
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            return None
        return _rfc3339((result.stdout or "").strip())

    def good_revisions(self, before: int) -> list[int] | None:
        """Revisions below *before* that were deployed (``deployed`` or
        ``superseded``), newest first, from ``helm history``; None when
        unreadable."""
        import json

        try:
            result = self._run(
                ["helm", "history", self.HELM_RELEASE_NAME, "-n", self.namespace, "-o", "json"],
                capture_output=True,
                text=True,
            )
            rows = json.loads(result.stdout or "[]") if result.returncode == 0 else None
            if not isinstance(rows, list):
                return None
            good = [
                int(r.get("revision") or 0)
                for r in rows
                if isinstance(r, dict)
                and str(r.get("status") or "") in ("deployed", "superseded")
                and int(r.get("revision") or 0) < before
            ]
        except (OSError, subprocess.SubprocessError, ValueError, TypeError):
            return None
        return sorted(good, reverse=True)

    def rollback_to(self, revision: int) -> bool:
        """``helm rollback`` to *revision* (the caller holds the lease and
        checked the revision's watch list), then the OpenShift patch and the
        rollout wait. A pending release blocks every later upgrade until this
        or a manual rollback runs."""
        pin = self._get_helm_version()
        result = self._run(
            ["helm", "rollback", self.HELM_RELEASE_NAME, str(revision), "-n", self.namespace],
            capture_output=True,
            text=True,
            timeout=_Phase(WATCH_HELM_PHASE_S).helm_attempt_s("helm rollback"),
        )
        if result.returncode != 0:
            logger.error("helm rollback to revision %s failed: %s", revision, result.stderr)
            return False
        logger.info(
            "Rolled release %s back to revision %s (chart %s)",
            self.HELM_RELEASE_NAME,
            revision,
            pin,
        )
        if self._is_openshift():
            self._assign_openshift_scc()
            self._patch_openshift_deployments()
        return self._wait_for_rollout()

    def _set_watch_list_impl(self, namespaces: list[str]) -> bool:
        """Set ``spark.jobNamespaces`` to exactly *namespaces* in one upgrade.

        The caller holds the lease and computed the list (repair-operator).
        An empty list is refused: the chart reads ``{}`` as "watch every
        namespace". After the upgrade: the OpenShift patch and its rollout,
        the restart, then a check that the controller's ``--namespaces``
        equals the list.
        """
        wanted = sorted({n for n in namespaces if n})
        if not wanted:
            raise ValueError("refusing an empty watch list (the chart would watch every namespace)")
        pin = self._watch_list_pin()
        if pin is None:
            return False
        cmd = [
            "helm",
            "upgrade",
            self.HELM_RELEASE_NAME,
            self.HELM_CHART_NAME,
            "-n",
            self.namespace,
            "--reuse-values",
            "--set",
            f"spark.jobNamespaces={{{','.join(wanted)}}}",
            *pin,
        ]
        result = self._run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_Phase(WATCH_HELM_PHASE_S).helm_attempt_s("helm upgrade (set watch list)"),
        )
        if result.returncode != 0:
            logger.error("helm upgrade (set watch list) failed: %s", result.stderr)
            return False
        if self._is_openshift():
            self._assign_openshift_scc()
            self._patch_openshift_deployments()
            if not self._wait_for_rollout():
                return False
        if not self._restart_operator():
            return False
        deadline = time.monotonic() + lease_clamp(15.0)
        while True:
            got: dict[str, list[str] | None] = {}
            for deploy in (self.CONTROLLER_DEPLOYMENT, self.WEBHOOK_DEPLOYMENT):
                try:
                    got[deploy] = self._get_active_namespaces(deployment=deploy)
                except _DeploymentReadError as e:
                    got[deploy] = None
                    logger.warning("cannot read %s args after the upgrade: %s", deploy, e)
            if all(v is not None and sorted(set(v)) == wanted for v in got.values()):
                return True
            if time.monotonic() >= deadline:
                logger.error(
                    "operator Deployments watch %s after the upgrade, wanted %s", got, wanted
                )
                return False
            time.sleep(1)

    def _controller_deployment(self) -> dict[str, Any]:
        """The controller Deployment as JSON; raises _DeploymentReadError."""
        import json

        try:
            result = self._run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    self.CONTROLLER_DEPLOYMENT,
                    "-n",
                    self.namespace,
                    "-o",
                    "json",
                ],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as e:
            raise _DeploymentReadError(f"kubectl not found: {e}") from e
        if result.returncode != 0:
            raise _DeploymentReadError((result.stderr or "").strip() or "kubectl get failed")
        try:
            return dict(json.loads(result.stdout))
        except ValueError as e:
            raise _DeploymentReadError(f"unparseable deployment JSON: {e}") from e

    def _stored_controller_volumes(self) -> list[Any]:
        """``controller.volumes`` in the release's user-supplied values.

        Raises _DeploymentReadError when the values cannot be read.
        """
        import json

        try:
            result = self._run(
                [
                    "helm",
                    "get",
                    "values",
                    self.HELM_RELEASE_NAME,
                    "-n",
                    self.namespace,
                    "-o",
                    "json",
                ],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as e:
            raise _DeploymentReadError(f"helm not found: {e}") from e
        if result.returncode != 0:
            raise _DeploymentReadError((result.stderr or "").strip() or "helm get values failed")
        text = (result.stdout or "").strip()
        try:
            values = json.loads(text) if text else {}
        except ValueError as e:
            raise _DeploymentReadError(f"unparseable helm values: {e}") from e
        vols = ((values or {}).get("controller") or {}).get("volumes") or []
        return list(vols) if isinstance(vols, list) else []

    def _upgrade_tmp_size(self, requested: str | None) -> str | None:
        """The size an upgrade of the existing release should set, or None to refuse.

        Helm replaces lists wholesale, so writing controller.volumes[0] would
        drop any other stored controller volume and leave its mount dangling
        (a failed release). Refused then. An unrequested upgrade never lowers
        a larger size an admin set earlier.
        """
        from lakebench.modules.pipeline_engines.spark.operator_scratch import parse_quantity

        try:
            stored = self._stored_controller_volumes()
        except _DeploymentReadError as e:
            logger.error("Cannot read the release's stored controller volumes: %s", e)
            return None
        others = [v for v in stored if not isinstance(v, dict) or v.get("name") != "tmp"]
        if others:
            logger.error(
                "The release stores controller volumes besides 'tmp' (%s); setting the /tmp "
                "size would drop them. Resize by hand with the full controller.volumes list.",
                others,
            )
            return None
        size = requested or DEFAULT_CONTROLLER_TMP_SIZE
        if requested is None:
            try:
                current = self.controller_tmp_volume()
            except _DeploymentReadError:
                current = None
            stored_tmp = [v for v in stored if isinstance(v, dict) and v.get("name") == "tmp"]
            if (
                stored_tmp
                and isinstance(stored_tmp[0].get("emptyDir"), dict)
                and not stored_tmp[0]["emptyDir"].get("sizeLimit")
            ):
                # The release itself stores an unbounded /tmp (a hand patch of
                # the Deployment alone would be reverted by the upgrade, so
                # only the stored values count): keep it.
                return ""
            cur = current.limit_bytes if current is not None else None
            if current is not None and cur is not None and cur > (parse_quantity(size) or 0):
                size = str(current.size_limit)
        return size

    def controller_tmp_volume(self) -> TmpVolume:
        """The controller's /tmp volume; raises _DeploymentReadError."""
        return tmp_volume(self._controller_deployment())

    def _wait_for_rollout(self, timeout_s: int = WATCH_ROLLOUT_PHASE_S) -> bool:
        """Wait for both operator Deployments to finish rolling out, within one
        ``timeout_s`` budget for the two (bounded by the lease's hold budget)."""
        return self._await_rollouts(
            [self.CONTROLLER_DEPLOYMENT, self.WEBHOOK_DEPLOYMENT], _Phase(timeout_s)
        )

    def _verify_tmp_size(self, tmp_size: str) -> bool:
        """True when the controller spec carries a /tmp at least *tmp_size*."""
        from lakebench.modules.pipeline_engines.spark.operator_scratch import parse_quantity

        try:
            vol = self.controller_tmp_volume()
        except _DeploymentReadError as e:
            logger.error("Cannot read the controller /tmp volume after the upgrade: %s", e)
            return False
        want = parse_quantity(tmp_size) or 0
        if vol.found and vol.is_empty_dir and vol.size_limit is None:
            return True  # unbounded emptyDir
        if not tmp_size:
            logger.error(
                "Controller /tmp is %s after the upgrade; the release stores an unbounded one",
                vol.size_limit if vol.found else "missing",
            )
            return False
        got = vol.limit_bytes
        if not vol.found or got is None or got < want:
            logger.error(
                "Controller /tmp volume is %s after the upgrade, wanted sizeLimit %s",
                vol.size_limit if vol.found else "missing",
                tmp_size,
            )
            return False
        logger.info("Controller /tmp emptyDir sizeLimit is %s", vol.size_limit)
        return True

    def apply_controller_tmp_size(self, tmp_size: str | None = None) -> bool:
        """Resize the controller's /tmp emptyDir on the installed release.

        The caller must hold the ``lakebench-cluster-lock`` lease (admin
        repair-operator does). ``--reuse-values`` keeps the watch list and
        every other stored value, and the installed chart version is pinned
        so the resize never upgrades the operator. Rolls the controller.
        """
        pin = self._get_helm_version()
        if not pin:
            # Without --version Helm would move the shared release to the
            # repo's latest chart (gotcha 3b); the config's pin may differ
            # from what is installed.
            logger.error(
                "Cannot read the installed chart version; not resizing the controller /tmp"
            )
            return False
        size = self._upgrade_tmp_size(tmp_size)
        if size is None:
            return False
        tmp_size = size
        cmd = [
            "helm",
            "upgrade",
            self.HELM_RELEASE_NAME,
            self.HELM_CHART_NAME,
            "-n",
            self.namespace,
            "--reuse-values",
        ]
        cmd += ["--version", pin]
        cmd += self._reuse_values_backfill(pin, tmp_size)
        try:
            result = self._run(
                cmd,
                capture_output=True,
                text=True,
                timeout=_Phase(WATCH_HELM_PHASE_S).helm_attempt_s("helm upgrade (/tmp resize)"),
            )
        except FileNotFoundError:
            logger.error("helm not found on PATH -- cannot resize the controller /tmp")
            return False
        if result.returncode != 0:
            logger.error("helm upgrade failed resizing the controller /tmp: %s", result.stderr)
            return False
        if self._is_openshift():
            self._assign_openshift_scc()
            self._patch_openshift_deployments()
        if not self._wait_for_rollout():
            return False
        return self._verify_tmp_size(tmp_size)

    def refresh_chart_repo(self) -> str | None:
        """Add and update the chart repo (admin install runs this before it
        takes the lease); a problem, or None. helm returns 0 for a repo
        already added with the same URL; a non-zero "already exists" means the
        name points at another URL, which must not supply the shared chart."""
        for args, timeout in (
            (["helm", "repo", "add", self.HELM_REPO_NAME, self.HELM_REPO_URL], 60),
            (["helm", "repo", "update", self.HELM_REPO_NAME], 120),
        ):
            try:
                r = self._run(args, capture_output=True, text=True, timeout=timeout)
            except (OSError, subprocess.SubprocessError) as e:
                return f"{' '.join(args[:3])} failed: {e}"
            if r.returncode != 0:
                return f"{' '.join(args[:3])} failed: {(r.stderr or '').strip()[:300]}"
        return None

    def install(
        self,
        version: str | None = None,
        values: dict[str, Any] | None = None,
        tmp_size: str | None = None,
        add_repo: bool = True,
    ) -> bool:
        """Fresh install of the Spark Operator via Helm; never an upgrade.

        Called only by ``lakebench admin install --component spark-operator``
        (``deploy/shared_components.py``), under the cluster lease, after it
        found no release. ``helm install`` (not ``upgrade --install``) makes
        helm itself refuse a release that appeared since that read, so an
        installed operator's version and watch list are never touched here.
        With no ``job_namespace`` the chart's ``["default"]`` watch list is
        kept; deploys add their namespaces under the lease.

        On OpenShift, automatically:
        - Uses webhook port 9443 (non-root can't bind 443)
        - Assigns the anyuid SCC to operator service accounts
        - Patches deployments to remove fsGroup and seccompProfile
          (the Helm chart hardcodes these and they can't be overridden
          via values due to deep merge behavior)

        Args:
            version: Chart version (default: the manager's target version)
            values: Custom Helm values
            tmp_size: Controller /tmp emptyDir sizeLimit (default 8Gi)
            add_repo: Add and refresh the chart repo first (admin install does
                that before taking the lease)

        Returns:
            True if installation succeeded
        """
        logger.info(f"Installing Spark Operator to namespace {self.namespace}")
        version = version or self.target_version
        if not version:
            logger.error("No chart version given; refusing an unpinned Spark Operator install")
            return False

        exists = self._release_exists()
        if exists is not False:
            logger.error(
                "Helm release %s in %s %s; admin install only installs a missing operator",
                self.HELM_RELEASE_NAME,
                self.namespace,
                "already exists" if exists else "cannot be read",
            )
            return False

        is_openshift = self._is_openshift()
        if is_openshift:
            logger.info("OpenShift detected -- will assign anyuid SCC after install")

        try:
            if add_repo:
                self._run(
                    ["helm", "repo", "add", self.HELM_REPO_NAME, self.HELM_REPO_URL],
                    capture_output=True,
                    check=True,
                    timeout=60,
                )
                self._run(
                    ["helm", "repo", "update", self.HELM_REPO_NAME],
                    capture_output=True,
                    check=True,
                    timeout=120,
                )

            # On OpenShift, use a non-privileged port for the webhook
            # (non-root can't bind to port 443).
            webhook_port = "9443" if is_openshift else "443"

            cmd = [
                "helm",
                "install",
                self.HELM_RELEASE_NAME,
                self.HELM_CHART_NAME,
                "--namespace",
                self.namespace,
                "--create-namespace",
                "--version",
                version,
                # Bounds helm's own waits (hooks) below the subprocess kill;
                # without --wait it does not bound resource creation, so a
                # killed install can still leave a pending-install release,
                # which admin install's next status read refuses.
                "--timeout",
                "150s",
                "--set",
                "webhook.enable=true",
                "--set",
                f"webhook.port={webhook_port}",
            ]
            # Tell the operator which namespace(s) to watch
            if self.job_namespace:
                cmd.extend(["--set", f"spark.jobNamespaces={{{self.job_namespace}}}"])
            tmp_size = tmp_size or DEFAULT_CONTROLLER_TMP_SIZE
            cmd.extend(controller_tmp_helm_set_args(tmp_size))

            # Add custom values
            if values:
                for key, value in values.items():
                    cmd.extend(["--set", f"{key}={value}"])

            result = self._run(cmd, capture_output=True, text=True, timeout=180)

            if result.returncode != 0:
                logger.error(f"Helm install failed: {result.stderr}")
                return False

            # On OpenShift, assign SCCs and patch security contexts so
            # operator pods can start under the restricted-v2 SCC. At install a
            # grant that cannot be made fails the install.
            if is_openshift:
                from lakebench.k8s.security import SCCGrantError

                try:
                    self._assign_openshift_scc(strict=True)
                except SCCGrantError as e:
                    logger.error("Spark Operator install: %s", e)
                    return False
                self._patch_openshift_deployments()

            if not self._wait_for_ready(timeout=120):
                return False
            return self._verify_tmp_size(tmp_size)

        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as e:
            logger.error(f"Failed to install Spark Operator: {e}")
            return False

    def _wait_for_ready(self, timeout: int = 120) -> bool:
        """Wait for Spark Operator to become ready.

        Args:
            timeout: Maximum wait time in seconds

        Returns:
            True if operator becomes ready
        """
        start = time.time()

        while time.time() - start < timeout:
            status = self.check_status()
            if status.ready:
                logger.info("Spark Operator is ready")
                return True
            time.sleep(5)

        logger.error(f"Spark Operator not ready after {timeout}s")
        return False

    @staticmethod
    def _watch_list_unreadable(status: OperatorStatus) -> OperatorStatus:
        status.ready = False
        status.message = (
            "the Spark Operator's watch list could not be read; SparkApplications may "
            "never reconcile, so this stops rather than submit them. Check "
            "`helm status spark-operator` and the operator pods in the operator's "
            "namespace; a cluster admin runs `lakebench admin doctor`."
        )
        return status

    def ensure_namespace_watched(self, *, can_heal: bool = False) -> OperatorStatus:
        """Ensure the Spark Operator watches the target namespace.

        When ``can_heal`` is True and the operator is not watching the target
        namespace, adds it under the cluster lease (``_add_namespace_to_watch``).
        When False, or when the add fails, returns status with a remedy that
        routes through ``lakebench deploy`` (see ``watch_list_fix_hint``).

        Args:
            can_heal: If True, attempt to add the namespace under the lease.

        Returns:
            OperatorStatus reflecting the namespace watching state.
        """
        if not self.job_namespace:
            return self.check_status()

        status = self.check_status()

        if not status.ready:
            return status

        # Unknown is not watching: a namespace the operator may not watch
        # would leave every SparkApplication unreconciled.
        if status.watching_namespace is None:
            return self._watch_list_unreadable(status)
        if status.watching_namespace:
            return status

        # Operator is NOT watching the target namespace
        if can_heal:
            logger.info(
                "Adding namespace '%s' to spark.jobNamespaces",
                self.job_namespace,
            )
            if self._add_namespace_to_watch(self.job_namespace):
                healed = self.check_status()
                if healed.ready and healed.watching_namespace is None:
                    return self._watch_list_unreadable(healed)
                return healed
            # Heal failed -- fall through to provide the remedy

        existing = status.watched_namespaces or []
        status.message = (
            f"Spark Operator does not watch namespace '{self.job_namespace}'. "
            f"Currently watching: {existing}. "
            f"SparkApplications will not be reconciled.\n"
            f"{watch_list_fix_hint()}"
        )
        return status
