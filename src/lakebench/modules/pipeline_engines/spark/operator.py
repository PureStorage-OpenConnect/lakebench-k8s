"""Spark Operator management for Lakebench.

Handles detection and installation of the Kubeflow Spark Operator.
"""

from __future__ import annotations

import hashlib
import logging
import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from lakebench.deploy import deadline as deploy_deadline
from lakebench.k8s import pinned_helm, pinned_kubectl, pinned_oc
from lakebench.k8s.lease_state import LeaseHoldExceeded
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


# How long a watch-list mutation waits for the cluster lease. A holder keeps it
# across a helm upgrade, two rollout waits (120 s each) and a verify, so up to
# about 5 minutes; a 30 s acquire made concurrent deploys and destroys fail
# instead of queueing, which parallel UAT hit.
_WATCH_LIST_LOCK_TIMEOUT_S = 600
# Waits after a shared mutation is committed (helm upgrade done, operator
# restarted). They are bounded by their own timeout, never cut by the deploy
# deadline (DEP-6): stopping half way would release the lease with the shared
# operator mid-restart and its watch list unverified, which every other
# deployment then works against. Today only these bounds and the lease TTL
# hold them; SD-12 adds a lease hold budget.
_POST_UPGRADE_ROLLOUT_S = 180
_POST_UPGRADE_RESTART_S = 120
_POST_UPGRADE_READY_S = 120
_POST_UPGRADE_VERIFY_S = 15


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

    installed: bool
    version: str | None
    namespace: str | None
    ready: bool
    message: str
    watching_namespace: bool | None = None  # None = could not determine
    watched_namespaces: list[str] | None = None  # None = watches all


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

    def _watch_list_pin(self) -> list[str]:
        """``--version``/backfill args for a watch-list-only ``helm upgrade``.

        Adding or removing a namespace must not change the shared operator's
        chart. Without ``--version`` Helm resolves whatever the local repo
        serves (a silent upgrade for every deployment on the cluster); with
        the config's version a developer's older pin would downgrade an
        admin's newer install. So pin the installed release's chart when
        check_status() has read it, and fall back to the configured version.
        """
        pin = self._installed_version or self.target_version
        if not pin:
            return []
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
        self.target_version = version  # Version to install if not present
        # Chart version of the running release, cached by check_status().
        # Watch-list edits pin it so adding or removing a namespace never
        # moves the shared operator to another chart (see _watch_list_pin).
        self._installed_version: str | None = None
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
                return OperatorStatus(
                    installed=False,
                    version=None,
                    namespace=None,
                    ready=False,
                    message="SparkApplication CRD not found",
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

            if result.returncode != 0 or "No resources" in result.stdout:
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
            if version:
                self._installed_version = version

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
            return OperatorStatus(
                installed=False,
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
        and callers fall back to the configured version.
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

    def _get_active_namespaces(self, operator_ns: str | None = None) -> list[str] | None:
        """Get namespaces from the running controller deployment spec.

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
                    "spark-operator-controller",
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

    def _get_watched_namespaces(self) -> list[str] | None:
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
        deploy_deadline.check("helm upgrade of the Spark Operator watch list")
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
        # Pin the installed chart (see _watch_list_pin). With no version
        # known, Helm resolves whatever the repo serves while --reuse-values
        # carries forward only the stored values, so backfill regardless.
        pin = self._watch_list_pin()
        cmd.extend(
            pin
            or [
                "--set",
                f"prometheus.metrics.jobSubmitLatencyBuckets="
                f"{self._JOB_SUBMIT_LATENCY_BUCKETS_DEFAULT}",
            ]
        )
        result = self._run(cmd, capture_output=True, text=True)
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

        # Step 2: Re-add the namespace -- Helm will create fresh RBAC. Mid-
        # sequence after the removal, so the deploy deadline does not cut it.
        return self._add_namespace_to_watch_impl(namespace, _deadline_gate=False)

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
            cmd.extend(self._watch_list_pin())

            try:
                result = self._run(cmd, capture_output=True, text=True)
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
                self._restart_operator()
                return True

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

        if not ok:
            raise WatchListMutationError(
                f"failed to remove namespace '{namespace}' from the Spark "
                "Operator watch list. The operator may crash-loop on the "
                "stale entry, which would break SparkApplication reconciliation "
                "for every namespace on the cluster. Run "
                "`lakebench admin repair-operator` to reconcile the watch "
                "list against live namespaces."
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
            try:
                return self._add_namespace_to_watch_impl(namespace, _retry_on_eviction)
            except (subprocess.TimeoutExpired, LeaseHoldExceeded) as e:
                # A command ran out of the lease's time: fail closed, the
                # namespace is not proven watched.
                logger.error(
                    "Adding %s to the watch list stopped inside the cluster lease: %s", namespace, e
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

        # No shared mutation starts once the deploy deadline has passed, and
        # the wait for the lease counts against it (DEP-6).
        deploy_deadline.check("the cluster lease for the Spark Operator watch list")
        lease_cm = cluster_lock(core_v1, timeout=deploy_deadline.clamp(_WATCH_LIST_LOCK_TIMEOUT_S))
        try:
            lease_cm.__enter__()
            return lease_cm, "locked"
        except ClusterLockHeld as e:
            logger.warning("spark-operator watch-list: lease held by %s", e.holder)
            deploy_deadline.check(
                "the cluster lease for the Spark Operator watch list", f"held by {e.holder}"
            )
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

    def _add_namespace_to_watch_impl(
        self, namespace: str, _retry_on_eviction: bool = True, _deadline_gate: bool = True
    ) -> bool:
        """Non-lease-gated body of ``_add_namespace_to_watch``.

        Split out so ADR-F5's lease acquisition wraps only the mutation
        loop, and existing test callers that patch the mutation body
        keep working. Callers on the deploy path must go through
        ``_add_namespace_to_watch`` (which lease-gates); this variant is
        internal.
        """
        new_list: list[str] = []

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

            if _deadline_gate:
                # Before the shared mutation, never after it (DEP-6).
                deploy_deadline.check("helm upgrade of the Spark Operator watch list")

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
            cmd.extend(self._watch_list_pin())

            try:
                result = self._run(cmd, capture_output=True, text=True)
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
        if not self._verify_namespace_watched(namespace, timeout=_POST_UPGRADE_VERIFY_S):
            if _retry_on_eviction:
                logger.warning(
                    "Namespace '%s' was dropped from spark.jobNamespaces after a "
                    "successful upgrade -- a concurrent deploy overwrote it. Re-adding.",
                    namespace,
                )
                # We already hold the cluster lease; do not re-acquire.
                # Mid-sequence: the deadline does not cut a re-add.
                return self._add_namespace_to_watch_impl(
                    namespace, _retry_on_eviction=False, _deadline_gate=False
                )
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
        webhook pods must be recycled.

        Returns:
            True if both deployments restarted and rolled out successfully.
        """
        deployments = ["spark-operator-controller", "spark-operator-webhook"]

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

        for deploy in deployments:
            result = self._run(
                [
                    "kubectl",
                    "rollout",
                    "status",
                    f"deployment/{deploy}",
                    "-n",
                    self.namespace,
                    f"--timeout={_POST_UPGRADE_RESTART_S}s",
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
        except FileNotFoundError:
            return None
        if result.returncode == 0:
            return True
        if re.search(r"release:? not found", (result.stderr or "").lower()):
            return False
        return None

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

    def _rollout_status_after_upgrade(self) -> bool:
        """Wait for both operator Deployments to finish rolling out after a
        committed helm upgrade: bounded by _POST_UPGRADE_ROLLOUT_S, never cut
        by the deploy deadline (DEP-6)."""
        for deploy in (self.CONTROLLER_DEPLOYMENT, "spark-operator-webhook"):
            result = self._run(
                [
                    "kubectl",
                    "rollout",
                    "status",
                    f"deployment/{deploy}",
                    "-n",
                    self.namespace,
                    f"--timeout={_POST_UPGRADE_ROLLOUT_S}s",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.error("Rollout of %s did not complete: %s", deploy, result.stderr)
                return False
        return True

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
            result = self._run(cmd, capture_output=True, text=True)
        except FileNotFoundError:
            logger.error("helm not found on PATH -- cannot resize the controller /tmp")
            return False
        if result.returncode != 0:
            logger.error("helm upgrade failed resizing the controller /tmp: %s", result.stderr)
            return False
        if self._is_openshift():
            self._assign_openshift_scc()
            self._patch_openshift_deployments()
        if not self._rollout_status_after_upgrade():
            return False
        return self._verify_tmp_size(tmp_size)

    def install(
        self,
        version: str | None = None,
        values: dict[str, Any] | None = None,
        tmp_size: str | None = None,
    ) -> bool:
        """Install or upgrade the Spark Operator via Helm.

        On OpenShift, automatically:
        - Uses webhook port 9443 (non-root can't bind 443)
        - Assigns the anyuid SCC to operator service accounts
        - Patches deployments to remove fsGroup and seccompProfile
          (the Helm chart hardcodes these and they can't be overridden
          via values due to deep merge behavior)

        An existing release is upgraded with ``--reuse-values`` and the
        backfill, never re-installed from defaults: a plain ``upgrade
        --install`` resets ``spark.jobNamespaces`` to the chart's
        ``["default"]`` and unwatches every tenant. With no version given it
        stays on the installed chart. The controller's /tmp emptyDir is
        sized to *tmp_size* on both paths (operator_scratch).

        Args:
            version: Chart version (default: the manager's target version,
                then the installed chart, then the repo's latest)
            values: Custom Helm values
            tmp_size: Controller /tmp emptyDir sizeLimit (default 8Gi; an
                upgrade without it keeps a larger size already set)

        Returns:
            True if installation succeeded
        """
        logger.info(f"Installing Spark Operator to namespace {self.namespace}")
        version = version or self.target_version

        exists = self._release_exists()
        if exists is None:
            logger.error(
                "Cannot tell whether Helm release %s exists in %s; refusing to install "
                "over it (a fresh install would reset the watch list)",
                self.HELM_RELEASE_NAME,
                self.namespace,
            )
            return False

        is_openshift = self._is_openshift()
        if is_openshift:
            logger.info("OpenShift detected -- will assign anyuid SCC after install")

        # A fresh install or upgrade of the shared operator does not start
        # after the deploy deadline (DEP-6).
        deploy_deadline.check("helm install of the Spark Operator")
        try:
            # Add Helm repo
            self._run(
                ["helm", "repo", "add", self.HELM_REPO_NAME, self.HELM_REPO_URL],
                capture_output=True,
                check=True,
            )

            self._run(
                ["helm", "repo", "update"],
                capture_output=True,
                check=True,
            )

            # On OpenShift, use a non-privileged port for the webhook
            # (non-root can't bind to port 443).
            webhook_port = "9443" if is_openshift else "443"

            # Build Helm install command
            cmd = [
                "helm",
                "upgrade",
                "--install",
                self.HELM_RELEASE_NAME,
                self.HELM_CHART_NAME,
                "--namespace",
                self.namespace,
                "--create-namespace",
                "--set",
                "webhook.enable=true",
                "--set",
                f"webhook.port={webhook_port}",
            ]

            if exists:
                # Keep the stored values (the watch list above all) and the
                # installed chart unless a version was asked for.
                pin = version or self._get_helm_version()
                if not pin:
                    logger.error(
                        "Cannot read the installed chart version and none was given; "
                        "refusing an unpinned upgrade of the shared operator"
                    )
                    return False
                size = self._upgrade_tmp_size(tmp_size)
                if size is None:
                    return False
                tmp_size = size
                cmd.append("--reuse-values")
                cmd.extend(["--version", pin])
                cmd.extend(self._reuse_values_backfill(pin, tmp_size))
            else:
                # Tell the operator which namespace(s) to watch
                if self.job_namespace:
                    cmd.extend(["--set", f"spark.jobNamespaces={{{self.job_namespace}}}"])
                if version:
                    cmd.extend(["--version", version])
                tmp_size = tmp_size or DEFAULT_CONTROLLER_TMP_SIZE
                cmd.extend(controller_tmp_helm_set_args(tmp_size))

            # Add custom values
            if values:
                for key, value in values.items():
                    cmd.extend(["--set", f"{key}={value}"])

            # Run install; not after the deadline, which the repo update above
            # may have used up.
            deploy_deadline.check("helm install of the Spark Operator")
            result = self._run(cmd, capture_output=True, text=True)

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

            # readyReplicas alone can still count the old pod during an
            # upgrade; wait for the new ReplicaSets.
            if exists and not self._rollout_status_after_upgrade():
                return False
            if not self._wait_for_ready(timeout=_POST_UPGRADE_READY_S, cut_by_deadline=False):
                return False
            return self._verify_tmp_size(tmp_size)

        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.error(f"Failed to install Spark Operator: {e}")
            return False

    def _wait_for_ready(self, timeout: float = 120, cut_by_deadline: bool = True) -> bool:
        """Wait for Spark Operator to become ready.

        Args:
            timeout: Maximum wait time in seconds

        Returns:
            True if operator becomes ready
        """
        start = time.time()
        last = ""

        while time.time() - start < timeout:
            status = self.check_status()
            if status.ready:
                logger.info("Spark Operator is ready")
                return True
            last = status.message
            time.sleep(5)

        if cut_by_deadline:  # not after a committed install
            deploy_deadline.check("Spark Operator ready", last)
        logger.error(f"Spark Operator not ready after {timeout}s")
        return False

    def ensure_installed(self, _after_wait: bool = False) -> OperatorStatus:
        """Ensure Spark Operator is installed and ready.

        If not installed, installs it automatically.
        If installed but not watching the target namespace, adds it.

        Returns:
            OperatorStatus after ensuring installation
        """
        status = self.check_status()

        if status.ready:
            # Operator is running -- ensure it watches our namespace
            if self.job_namespace and status.watching_namespace is False:
                logger.info(
                    "Spark Operator not watching '%s' -- adding via helm upgrade",
                    self.job_namespace,
                )
                if not self._add_namespace_to_watch(self.job_namespace):
                    return OperatorStatus(
                        installed=True,
                        version=status.version,
                        namespace=status.namespace,
                        ready=False,
                        message=(
                            f"Failed to add namespace '{self.job_namespace}' "
                            f"to spark.jobNamespaces via helm upgrade"
                        ),
                    )
                status = self.check_status()
            return status

        if not status.installed:
            logger.info("Spark Operator not found, installing...")
            if self.install(version=self.target_version):
                return self.check_status()
            else:
                return OperatorStatus(
                    installed=False,
                    version=None,
                    namespace=None,
                    ready=False,
                    message="Failed to install Spark Operator",
                )

        # CRD exists but the operator is not ready. With a release in place
        # this is usually a controller restart (an eviction, a watch-list
        # rollout): wait for it. Upgrading here would mutate the shared
        # operator outside the cluster lease and pin it to this tenant's
        # config version, so an existing release is left to the admin
        # commands.
        exists = self._release_exists()
        if exists is False:
            logger.info("Spark Operator release missing, installing...")
            if self.install(version=self.target_version):
                return self.check_status()
            return status
        logger.info("Spark Operator not ready, waiting for it to recover...")
        if self._wait_for_ready(timeout=deploy_deadline.clamp(120)):
            status = self.check_status()
            if (
                not _after_wait
                and status.ready
                and self.job_namespace
                and status.watching_namespace is False
            ):
                # The ready branch above adds the namespace under the lease;
                # at most once, so a flapping controller cannot recurse.
                return self.ensure_installed(_after_wait=True)
            return status
        status.message = (
            f"{status.message}. lakebench does not reinstall a shared operator from "
            "deploy; a cluster admin can run 'lakebench admin doctor' and "
            "'lakebench admin repair-operator'."
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

        # If watching or unknown, accept it
        if status.watching_namespace is not False:
            return status

        # Operator is NOT watching the target namespace
        if can_heal:
            logger.info(
                "Adding namespace '%s' to spark.jobNamespaces",
                self.job_namespace,
            )
            if self._add_namespace_to_watch(self.job_namespace):
                return self.check_status()
            # Heal failed -- fall through to provide the remedy

        existing = status.watched_namespaces or []
        status.message = (
            f"Spark Operator does not watch namespace '{self.job_namespace}'. "
            f"Currently watching: {existing}. "
            f"SparkApplications will not be reconciled.\n"
            f"{watch_list_fix_hint()}"
        )
        return status
