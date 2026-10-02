"""`logs`, `stop` and `status` against the Kubernetes API.

The commands in ``lakebench/cli/__init__.py`` load the config, pin the
cluster context (one context per process) and build the API objects; the functions here do the
reads and deletes, so tests drive them with fakes. Nothing on these paths runs
``kubectl``.

Exit codes (``lakebench.exit_codes``): ``logs`` with no matching pod is 1
(``logs.no_pod``), an API error or an unreachable cluster 4, an unknown
component 2. ``stop`` that could not delete something is 1
(``stop.api_error``). ``status`` on a missing namespace is 1
(``status.namespace_missing``), on a component not ready or missing 1
(``status.drift``), on an unreachable cluster 4.
"""

from __future__ import annotations

import codecs
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError as Urllib3HTTPError

# -- logs ----------------------------------------------------------------------

SPARK_GROUP = "sparkoperator.k8s.io"
SPARK_VERSION = "v1beta2"
SPARK_PLURAL = "sparkapplications"
DATAGEN_JOB = "lakebench-datagen"
APP_PREFIX = "lakebench-"
# Spark's own name for the driver container (also in job.py's pod template).
SPARK_DRIVER_CONTAINER = "spark-kubernetes-driver"

# Every SparkApplication lakebench submits is ``lakebench-<JobType value>``
# (``spark/job.py`` submit_job). ``tests/test_cli_cluster_ops.py`` checks this
# tuple against ``JobType`` so a new job type cannot be left out of `logs`.
STAGES: tuple[str, ...] = (
    "bronze-verify",
    "silver-build",
    "gold-finalize",
    "bronze-ingest",
    "silver-stream",
    "gold-refresh",
    "replay-financial",
    "reproduce-financial",
    "score-financial",
    "score-financial-reference",
)


@dataclass(frozen=True)
class LogSource:
    """Where `logs COMPONENT` reads: a pod label selector and a container."""

    selector: str
    container: str | None = None
    what: str = ""


def _stage_source(stage: str) -> LogSource:
    return LogSource(
        f"spark-role=driver,sparkoperator.k8s.io/app-name={APP_PREFIX}{stage}",
        SPARK_DRIVER_CONTAINER,
        f"the {stage} Spark driver",
    )


LOG_COMPONENTS: dict[str, LogSource] = {
    "datagen": LogSource(f"job-name={DATAGEN_JOB}", "datagen", "the datagen Job pods"),
    **{stage: _stage_source(stage) for stage in STAGES},
    "spark-driver": LogSource("spark-role=driver", SPARK_DRIVER_CONTAINER, "every Spark driver"),
    "trino": LogSource("app.kubernetes.io/component=trino-coordinator", None, "Trino coordinator"),
    "trino-worker": LogSource("app.kubernetes.io/component=trino-worker", None, "Trino workers"),
    "thrift": LogSource(
        "app.kubernetes.io/component=spark-thrift-server", None, "Spark Thrift Server"
    ),
    "duckdb": LogSource("app.kubernetes.io/component=duckdb", None, "DuckDB"),
    "hive": LogSource("app.kubernetes.io/component=metastore", None, "Hive Metastore"),
    "polaris": LogSource("app.kubernetes.io/component=polaris", None, "Polaris"),
    "postgres": LogSource("app.kubernetes.io/component=postgres", None, "PostgreSQL"),
}


# Seconds for one API request attempt, so a server that accepts the
# connection and never answers does not hang `logs`, `stop` or `status`.
# urllib3's default retries still apply (the kubernetes client sets none),
# so a call that stalls on every attempt takes a few multiples of this. A
# followed log gets the connect half only: it is meant to stay open.
API_TIMEOUT = 30
FOLLOW_TIMEOUT = (API_TIMEOUT, None)


class ClusterReadError(Exception):
    """An API error or an unreachable cluster on a read (exit 4).

    The kubernetes client raises urllib3's errors (``MaxRetryError``,
    ``ProtocolError``) when it cannot connect, not an ``ApiException``; both
    end up here.
    """


def api_message(e: ApiException) -> str:
    """``"<status> <reason>: <message>"`` from an ApiException.

    The API server's own message is in the JSON body (for example "container
    ... is waiting to start: ContainerCreating"); the reason alone ("Bad
    Request") does not say what to do.
    """
    head = f"{e.status} {e.reason}".strip()
    body = getattr(e, "body", None)
    if isinstance(body, bytes):
        body = body.decode("utf-8", "replace")
    message = ""
    if isinstance(body, str) and body:
        try:
            parsed = json.loads(body)
            message = str(parsed.get("message") or "") if isinstance(parsed, dict) else ""
        except ValueError:
            message = body.strip().splitlines()[0] if body.strip() else ""
    return f"{head}: {message}" if message else head


def _looks_like_config(arg: str) -> bool:
    if arg.endswith((".yaml", ".yml")):
        return True
    try:
        return Path(arg).is_file()
    except OSError:  # a name too long for the filesystem is not a file
        return False


def resolve_logs_args(
    first: str | None, second: str | None, file_given: bool
) -> tuple[str | None, str | None, bool]:
    """Return ``(component, config_path, legacy_order)`` from the positionals.

    The v1.7 order is ``logs CONFIG COMPONENT``. ``logs COMPONENT`` alone
    (the config defaults to ``./lakebench.yaml`` or ``--file``) and the 1.6
    order ``logs COMPONENT CONFIG`` are accepted; ``legacy_order`` is True
    for the latter so the command can say so. The component is returned
    unvalidated; the caller refuses an unknown name with the valid list.
    """
    names = LOG_COMPONENTS
    if file_given:
        if second is not None:
            raise ValueError("give the config once: as --file or as an argument, not both")
        return first, None, False
    if second is None:
        return first, None, False
    if second in names:
        return second, first, False
    if first in names or (
        first is not None and not _looks_like_config(first) and _looks_like_config(second)
    ):
        # The 1.6 order; a mistyped component then reads as the component.
        return first, second, True
    return second, first, False


def _created(pod: Any) -> Any:
    ts = getattr(pod.metadata, "creation_timestamp", None)
    return (ts is None, ts.timestamp() if ts is not None else 0.0, pod.metadata.name or "")


def list_pods(core: Any, namespace: str, selector: str) -> list[Any]:
    """Pods matching *selector*, oldest first. Raises ClusterReadError."""
    try:
        items = (
            core.list_namespaced_pod(
                namespace, label_selector=selector, _request_timeout=API_TIMEOUT
            ).items
            or []
        )
    except ApiException as e:
        raise ClusterReadError(f"listing pods ({selector}): {api_message(e)}") from e
    except Urllib3HTTPError as e:
        raise ClusterReadError(f"cluster unreachable: {e}") from e
    return sorted(items, key=_created)


def pod_container(pod: Any, preferred: str | None) -> str | None:
    """The container to read: *preferred* when the pod has it, else the
    ``kubectl.kubernetes.io/default-container`` annotation, else the first.

    Naming the container always avoids the API's 400 for a pod with more
    than one; lakebench's own pods run one container today, so this guards a
    sidecar added later (a Spark pod template, a Stackable logging agent).
    """
    containers = [c.name for c in (getattr(pod.spec, "containers", None) or [])]
    if preferred and preferred in containers:
        return preferred
    annotations = getattr(pod.metadata, "annotations", None) or {}
    default = annotations.get("kubectl.kubernetes.io/default-container")
    if default and default in containers:
        return str(default)
    return containers[0] if containers else preferred


def _decoded_lines(chunks: Iterable[bytes]) -> Iterable[str]:
    """Bytes chunks to text lines; bad bytes become U+FFFD, never an error."""
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    pending = ""
    for chunk in chunks:
        pending += decoder.decode(chunk)
        *lines, pending = pending.split("\n")
        for line in lines:
            yield line + "\n"
    pending += decoder.decode(b"", final=True)
    if pending:
        yield pending


@dataclass
class LogsOutcome:
    """What a `logs` read did.

    ``unavailable``: pods the API had no log for (400: a container still
    waiting to start, or no previous container for ``--previous``), each with
    the API's message. ``errors``: any other API error (403, 5xx), exit 4.
    """

    pods: list[str] = field(default_factory=list)
    empty: list[str] = field(default_factory=list)
    unavailable: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def read_logs(
    core: Any,
    namespace: str,
    pods: list[Any],
    source: LogSource,
    *,
    lines: int,
    previous: bool,
    follow: bool,
    write: Callable[[str], None],
    header: Callable[[str], None],
) -> LogsOutcome:
    """Write the logs of *pods* through *write*; one *header* per pod when
    there are several. With *follow* only the newest pod is followed (the
    caller says so). API errors are collected per pod, not raised, except a
    transport failure, which raises ClusterReadError.
    """
    out = LogsOutcome()
    targets = pods[-1:] if follow else pods
    for pod in targets:
        name = pod.metadata.name
        container = pod_container(pod, source.container)
        if len(targets) > 1:
            header(f"pod {name}" + (f" (container {container})" if container else ""))
        kwargs: dict[str, Any] = {
            "tail_lines": lines,
            "previous": previous,
            "_preload_content": False,
            "_request_timeout": FOLLOW_TIMEOUT if follow else API_TIMEOUT,
        }
        if container:
            kwargs["container"] = container
        if follow:
            kwargs["follow"] = True
        try:
            resp = core.read_namespaced_pod_log(name, namespace, **kwargs)
        except ApiException as e:
            bucket = out.unavailable if e.status == 400 else out.errors
            bucket.append(f"pod {name}: {api_message(e)}")
            continue
        except Urllib3HTTPError as e:
            raise ClusterReadError(f"cluster unreachable: {e}") from e
        wrote = False
        try:
            chunks = resp.stream(4096) if follow else [resp.data or b""]
            for text in _decoded_lines(chunks):
                write(text)
                wrote = True
        except Urllib3HTTPError as e:
            raise ClusterReadError(f"log stream from pod {name} broke: {e}") from e
        finally:
            release = getattr(resp, "release_conn", None)
            if callable(release):
                release()
        out.pods.append(name)
        if not wrote:
            out.empty.append(name)
    return out


# -- stop ----------------------------------------------------------------------


@dataclass
class StopOutcome:
    """What `stop` found and did.

    ``found`` holds what is to be deleted ("SparkApplication/x", "Job/y").
    ``finished`` holds what had already finished and is left in place, with
    its state: deleting a finished SparkApplication or Job also deletes its
    driver or worker pods, and with them the logs of a failed stage.
    """

    found: list[str] = field(default_factory=list)
    finished: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    not_running: list[str] = field(default_factory=list)  # vanished before the delete (404)
    failures: list[str] = field(default_factory=list)


# Spark Operator (v2.5.1) applicationState values after which the operator
# never submits the application again: its retry decision is taken in
# FAILING and SUCCEEDING, before these. SUBMISSION_FAILED is not here:
# lakebench submits streams with restartPolicy Always and batch stages with
# OnFailure and onSubmissionFailureRetries 5 (spark/job.py), so the operator
# resubmits from it. A stage in FAILING is deleted, and with it the driver's
# logs, in the window before the operator moves it to FAILED or a retry.
FINISHED_APP_STATES = frozenset({"COMPLETED", "FAILED"})


def _app_state(item: dict[str, Any]) -> str:
    status = item.get("status") or {}
    return str((status.get("applicationState") or {}).get("state") or "")


def _job_finished(job: Any) -> str:
    """ "Complete" or "Failed" when the Job has finished, else ""."""
    for cond in getattr(job.status, "conditions", None) or []:
        if cond.type in ("Complete", "Failed") and str(cond.status) == "True":
            return str(cond.type)
    return ""


def namespace_exists(core: Any, namespace: str) -> bool:
    """True when *namespace* exists. A 404 is False; any other API error and
    a transport failure raise ClusterReadError. Bounded by API_TIMEOUT.
    """
    try:
        core.read_namespace(namespace, _request_timeout=API_TIMEOUT)
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise ClusterReadError(f"cannot read namespace {namespace}: {api_message(e)}") from e
    except Urllib3HTTPError as e:
        raise ClusterReadError(f"cluster unreachable: {e}") from e


def stop_targets(custom: Any, batch: Any, namespace: str, out: StopOutcome) -> None:
    """Sort every ``lakebench-*`` SparkApplication and the datagen Job in
    *namespace* into ``out.found`` (still running or not started) and
    ``out.finished``. A list that fails goes into ``out.failures``; a
    transport failure raises ClusterReadError.
    """
    try:
        listing = custom.list_namespaced_custom_object(
            SPARK_GROUP, SPARK_VERSION, namespace, SPARK_PLURAL, _request_timeout=API_TIMEOUT
        )
        items = sorted(
            (item for item in (listing or {}).get("items") or []),
            key=lambda item: str((item.get("metadata") or {}).get("name") or ""),
        )
        for item in items:
            name = str((item.get("metadata") or {}).get("name") or "")
            if not name.startswith(APP_PREFIX):
                continue
            state = _app_state(item)
            if state in FINISHED_APP_STATES:
                out.finished.append(f"SparkApplication/{name} ({state})")
            else:
                out.found.append(f"SparkApplication/{name}")
    except ApiException as e:
        if e.status != 404:  # 404: the SparkApplication CRD is not installed
            out.failures.append(f"listing SparkApplications: {api_message(e)}")
    except Urllib3HTTPError as e:
        raise ClusterReadError(f"cluster unreachable: {e}") from e
    try:
        job = batch.read_namespaced_job(DATAGEN_JOB, namespace, _request_timeout=API_TIMEOUT)
        done = _job_finished(job)
        if done:
            out.finished.append(f"Job/{DATAGEN_JOB} ({done})")
        else:
            out.found.append(f"Job/{DATAGEN_JOB}")
    except ApiException as e:
        if e.status != 404:
            out.failures.append(f"reading Job {DATAGEN_JOB}: {api_message(e)}")
    except Urllib3HTTPError as e:
        raise ClusterReadError(f"cluster unreachable: {e}") from e


def stop_delete(custom: Any, batch: Any, namespace: str, out: StopOutcome) -> None:
    """Delete everything in ``out.found``. Every deletion is attempted; a
    404 is "not running", any other error goes into ``out.failures``.
    """
    from kubernetes import client as k8s_client

    for ref in out.found:
        kind, name = ref.split("/", 1)
        try:
            if kind == "Job":
                batch.delete_namespaced_job(
                    name,
                    namespace,
                    body=k8s_client.V1DeleteOptions(propagation_policy="Foreground"),
                    _request_timeout=API_TIMEOUT,
                )
            else:
                custom.delete_namespaced_custom_object(
                    SPARK_GROUP,
                    SPARK_VERSION,
                    namespace,
                    SPARK_PLURAL,
                    name,
                    _request_timeout=API_TIMEOUT,
                )
            out.deleted.append(ref)
        except ApiException as e:
            if e.status == 404:
                out.not_running.append(ref)
            else:
                out.failures.append(f"deleting {ref}: {api_message(e)}")
        except Urllib3HTTPError as e:
            out.failures.append(f"deleting {ref}: cluster unreachable: {e}")


def pre_stop(cfg: Any, k8s: Any) -> None:
    """Hook that runs before `stop` deletes anything; a no-op for now.

    For a financial continuous deployment it is to call
    ``cli._aml_post.request_drain(cfg, k8s, 300)``, so gold-refresh finishes
    its tick before it is deleted. The caller turns an exception into one warning and still
    deletes.
    """
    return None


# -- status --------------------------------------------------------------------

# The `logs` component for each object `status` reads, for its drift hint.
STATUS_LOG_COMPONENT: dict[str, str] = {
    "lakebench-postgres": "postgres",
    "lakebench-hive-metastore-default": "hive",
    "lakebench-polaris": "polaris",
    "lakebench-trino-coordinator": "trino",
    "lakebench-trino-worker": "trino-worker",
    "lakebench-spark-thrift": "thrift",
    "lakebench-duckdb": "duckdb",
}


@dataclass(frozen=True)
class ComponentState:
    """One `status` row."""

    name: str
    kind: str
    state: str  # "ok", "unready", "missing", "error"
    detail: str


def read_component(apps: Any, namespace: str, name: str, kind: str) -> ComponentState:
    """Read one StatefulSet or Deployment. A transport failure raises
    ClusterReadError; any other API error is an ``error`` row.
    """
    try:
        if kind == "StatefulSet":
            obj = apps.read_namespaced_stateful_set(name, namespace, _request_timeout=API_TIMEOUT)
        else:
            obj = apps.read_namespaced_deployment(name, namespace, _request_timeout=API_TIMEOUT)
    except ApiException as e:
        if e.status == 404:
            return ComponentState(name, kind, "missing", "Not found")
        return ComponentState(name, kind, "error", f"Error: {api_message(e)}")
    except Urllib3HTTPError as e:
        raise ClusterReadError(f"cluster unreachable: {e}") from e
    ready = (obj.status.ready_replicas if obj.status else None) or 0
    desired = obj.spec.replicas if obj.spec and obj.spec.replicas is not None else 1
    if desired == 0:
        # Lakebench never scales a component to zero; someone did.
        return ComponentState(name, kind, "unready", "scaled to 0 replicas")
    state = "ok" if ready >= desired else "unready"
    return ComponentState(name, kind, state, f"{ready}/{desired} replicas")


def status_exit(rows: list[ComponentState], *, config_known: bool) -> tuple[str, list[str]]:
    """``("ok" | "drift" | "unverified", names)`` for the rows.

    With a config every listed component is expected, so a missing one is
    drift. Without one (``--namespace`` only) the list is every component
    lakebench can deploy: a missing one is not drift, an unready one is, and
    a namespace with none of them is drift. Drift wins over a read error;
    a read error with no drift is "unverified" (exit 4).
    """
    drift = [r.name for r in rows if r.state == "unready"]
    if config_known:
        drift += [r.name for r in rows if r.state == "missing"]
    elif rows and all(r.state == "missing" for r in rows):
        drift = ["no lakebench component found"]
    if drift:
        return "drift", drift
    errors = [r.name for r in rows if r.state == "error"]
    if errors:
        return "unverified", errors
    return "ok", []
