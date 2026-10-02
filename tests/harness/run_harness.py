"""Characterisation harness for ``lakebench run`` (QA-9, DESIGN ch05 section 1).

Drives the real ``lakebench run`` command (``typer.testing.CliRunner`` on
``lakebench.cli.app``) with every cluster, object-store and engine seam
replaced, and returns a *trace* of what the run did: the calls it made to the
cluster and the object store (with the arguments that matter: timeouts,
credentials, SQL), the Spark jobs it submitted and with what environment, the
journal events it wrote, the shape of the ``metrics.json`` it saved, the
published values in it that do not depend on the wall clock, and its verdict.
No product code is changed; every seam is replaced with
``monkeypatch.setattr`` on the module attribute the code looks up at call
time (``run()`` imports most of its collaborators inside the function, so the
defining module is patched, not ``lakebench.cli._run``).

Seams (each records its calls into one ordered list):

- ``lakebench.k8s.get_k8s_client`` and ``lakebench.cli.get_k8s_client``:
  :class:`RecordingK8s`, a ``K8sClient`` stand-in. ``exec_in_pod`` answers
  Trino SQL from :class:`FakeTrino`.
- ``kubernetes.client`` ``CoreV1Api``, ``AppsV1Api``, ``CustomObjectsApi``,
  ``BatchV1Api`` and ``VersionApi``: recording fakes that script reads, and
  the uid-precondition deletes of an interrupted run's SparkApplications and
  datagen Job. Every other API class reaches ``ApiClient.call_api``, which
  refuses, so any other create, patch or delete is unscripted and fails the
  trace (SD-9's ``recording_k8s`` fixture is the general SAF-4 oracle, with
  the namespace rule).
- ``lakebench.spark.SparkOperatorManager``: ready, watching the namespace.
- ``lakebench.engine.get_engine``: :class:`FakeJobManager`.
- ``lakebench.spark.SparkJobMonitor``: :class:`FakeMonitor`, whose driver logs
  are the scrubbed fixture logs under ``tests/fixtures/run_char/<scenario>/``;
  a scenario can make a stage fail.
- ``lakebench.s3.S3Client``: :class:`FakeS3`.
- ``lakebench.benchmark.BenchmarkRunner``: :class:`FakeBenchmark`.
- ``lakebench.benchmark.executor.get_executor``: :class:`FakeExecutor` (the
  Trino worker readiness probe after maintenance).
- ``lakebench.cli._prerequisites.run_prerequisites``: a passing report; its
  arguments are recorded, because ``run`` decides them.
- ``subprocess.run`` and ``subprocess.Popen``: ``git`` (code provenance) runs
  for real; ``kubectl get statefulset lakebench-trino-worker`` is answered;
  anything else is unscripted.
- ``kubernetes.config`` loaders are unscripted: no real cluster is reached.
- The storage settle wait's clock and sleep (``wait_for_settle``'s keyword
  defaults) advance a fake clock, so the wait costs no wall time.

The batch AML scenario adds the score stage's recall.json sidecar, served by
the fake boto client from the record, and the AML query set's row counts.

The continuous scenarios (``lakebench.cli._sustained._run_sustained``) add:

- :class:`FakeClock` for ``time`` and ``datetime`` in ``cli._sustained`` and
  ``utc_now`` there and in the collector, started where the record's window
  opened; sleeping advances it, and so do the fakes for slow cluster work
  (each in-stream round and maintenance statement takes the record's time).
- The ownership check before the continuous reset: the namespace carries the
  deployment's identity stamp and created-buckets record, the kubeconfig's
  cluster fingerprint (``deploy.ownership.api_server_fingerprint``) matches
  it, and the backend has no bucket tagging (FlashBlade).
- Streams: submitted streams are RUNNING on their first driver until deleted;
  their driver logs are the record's, cut at the fake cluster clock
  (:func:`log_until`), so the window and the settle wait see them grow.
- ``lakebench.deploy.DatagenDeployer`` (the Job starts), the datagen Job's
  status (finished) and fleet (``metrics.datagen_aggregator.collect_from_k8s``,
  the record's row and file counts).

Every fake method is called through :func:`_checked`, which binds the call
against the real method's signature: a call the real seam would reject with a
``TypeError`` is unscripted here too. A call no fake scripts raises
``Unscripted`` (a ``NotImplementedError``) and is recorded in the trace's
``unscripted`` list even when the code under test catches it, so a new seam is
never silent; every golden expects that list empty.

Failure scenarios (V16-5 interrupt, V16-6 namespace loss) use:

- ``Scenario.interrupt``: a batch stage whose wait is interrupted, by a
  raised ``KeyboardInterrupt`` or a real ``SIGINT``/``SIGTERM`` sent to this
  process; ``Scenario.submit_interrupt``: a stage whose submission is
  interrupted after the API server created the application.
- ``Scenario.events``: the fake-time event hook, ``FakeClock.at``; at a
  window second it sends a signal or makes the namespace disappear.
- ``Recorder.namespace_present``: the scenario namespace exists (separate
  from ``Recorder.namespace``, its name).
- ``Scenario.lease_signal``: the fake ``ensure_namespace_watched`` takes the
  real ``cluster_lock`` against an in-memory lease and sends the signal while
  it holds it, then finishes a fake helm upgrade.
- SparkApplications and the datagen Job carry uids; their deletes record the
  uid precondition, answer 404 when gone and 409 when the uid differs or the
  object is listed in ``Recorder.foreign`` (recreated by someone else).

There is no golden update flag (SPEC section 6.4): a change that moves a
trace on purpose ships a new golden written by a second agent from the record.
"""

from __future__ import annotations

import email.utils
import inspect
import json
import os
import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "run_char"

#: Deployment name and namespace every scenario config uses.
NAME = "runchar"
#: The placeholder S3 host the docs and examples use.
PLACEHOLDER_HOST = "10.0.1.50"

_TRINO_POD = "lakebench-trino-coordinator-0"
_TRINO_SELECTOR = "app=lakebench-trino,component=coordinator"


class Unscripted(NotImplementedError):
    """A seam was called in a way no fake scripts."""


# ---------------------------------------------------------------------------
# Fixture logs
# ---------------------------------------------------------------------------

_IPV4 = re.compile(r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3})(?![\d.])")
_ALLOWED_IPS = {PLACEHOLDER_HOST, "127.0.0.1", "0.0.0.0"}
_CREDENTIAL = re.compile(
    r"(?i)(access[._-]?key(?:[._-]?id)?|secret(?:[._-]?access)?[._-]?key|password|passwd"
    r"|session[._-]?token|api[._-]?key)\s*[=:]\s*(\S+)"
)
_ENDPOINT = re.compile(r"(?i)endpoint\S*\s*[=:]\s*(\S+)")
_BUCKET_URL = re.compile(r"\bs3a?://([A-Za-z0-9.\-]+)")
_HTTP_HOST = re.compile(r"(?i)\bhttps?://([A-Za-z0-9.\-]+)")
_IPV6 = re.compile(r"(?<![\w:])(?:[0-9a-fA-F]{1,4}:){4,7}[0-9a-fA-F]{1,4}(?![\w:])")


def scrub_driver_log(text: str, source_name: str, target_name: str = NAME) -> str:
    """The fixture form of a Spark driver log (SPEC section 6 rule 5).

    Keeps only the lines Lakebench's scripts print (``[lb] `` prefix), which
    hold every fact line the collector parses (``=== JOB METRICS``,
    ``[c360-check]``, ``[c360-bronze]``, ``[detection]``). Spark and log4j
    lines, which carry pod IPs, node names and Spark's S3A configuration, are
    dropped. The source deployment name (and with it its bucket names)
    becomes *target_name*, and any IPv4 address left becomes the placeholder
    host. What it cannot rewrite, :func:`fixture_problems` refuses.
    Idempotent: ``scrub_driver_log(fixture, target_name, target_name) ==
    fixture``.
    """
    out: list[str] = []
    for line in text.splitlines():
        if "[lb] " not in line:
            continue
        line = line.replace(source_name, target_name)
        line = _IPV4.sub(
            lambda m: m.group(1) if m.group(1) in _ALLOWED_IPS else PLACEHOLDER_HOST, line
        )
        out.append(line.rstrip())
    return "\n".join(out) + "\n"


def fixture_problems(text: str, name: str = NAME) -> list[str]:
    """Why *text* may not be a committed fixture: an address or URL host
    other than the placeholder, an endpoint, a bucket not of deployment
    *name*, or a credential-looking value (anything but a ``${VAR}``
    placeholder)."""
    problems = [
        f"address {ip}" for ip in sorted(set(_IPV4.findall(text))) if ip not in _ALLOWED_IPS
    ]
    problems += [f"IPv6 address {ip}" for ip in sorted(set(_IPV6.findall(text)))]
    for m in _CREDENTIAL.finditer(text):
        if not m.group(2).startswith("${"):
            problems.append(f"credential {m.group(1)}")
    for m in _ENDPOINT.finditer(text):
        if PLACEHOLDER_HOST not in m.group(1):
            problems.append(f"endpoint {m.group(1)}")
    for host in sorted(set(_HTTP_HOST.findall(text))):
        if host not in _ALLOWED_IPS and host != "localhost":
            problems.append(f"host {host}")
    for bucket in sorted(set(_BUCKET_URL.findall(text))):
        if not bucket.startswith(f"{name}-"):
            problems.append(f"bucket {bucket}")
    return problems


# ---------------------------------------------------------------------------
# Recording fakes
# ---------------------------------------------------------------------------


#: Seconds the cluster's clock is ahead of the host's in the continuous
#: record (run-20261001-090400-db3ffe: cluster_clock_offset_seconds 22.9).
CLUSTER_OFFSET_S = 22.9


class FakeClock:
    """The continuous scenario's clock: ``time.time``, ``time.monotonic``,
    ``time.sleep`` and ``datetime.now`` in ``lakebench.cli._sustained`` read
    it, and sleeping advances it, so a 30 min window runs in milliseconds.
    Fakes that stand for slow cluster work (benchmark rounds, maintenance
    statements) advance it by the time that work took in the record."""

    def __init__(self, start: float) -> None:
        self.t = float(start)
        self._origin = float(start)
        #: (seconds after the origin, action) not yet fired, in time order.
        self._events: list[tuple[float, Callable[[], None]]] = []

    def time(self) -> float:
        return self.t

    def monotonic(self) -> float:
        return 10_000.0 + self.t - self._origin

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def advance(self, seconds: float) -> None:
        self.t += max(0.0, float(seconds))
        # Fire every event now due, oldest first. An action may raise (a
        # signal whose handler raises KeyboardInterrupt does so here, inside
        # the sleep or the slow fake that advanced the clock, as it would live).
        while self._events and self._events[0][0] <= self.t - self._origin:
            _, action = self._events.pop(0)
            action()

    def at(self, seconds: float, action: Callable[[], None]) -> None:
        """Run *action* when the clock reaches *seconds* after its origin."""
        self._events.append((float(seconds), action))
        self._events.sort(key=lambda e: e[0])

    def cluster_now(self) -> datetime:
        """The cluster's clock now, naive UTC (pod log timestamps)."""
        return datetime.fromtimestamp(self.t + CLUSTER_OFFSET_S, timezone.utc).replace(tzinfo=None)

    def time_module(self) -> SimpleNamespace:
        return SimpleNamespace(time=self.time, monotonic=self.monotonic, sleep=self.sleep)

    def datetime_class(self) -> type:
        clock = self

        class ClockDatetime(datetime):
            @classmethod
            def now(cls, tz=None):  # type: ignore[override]
                when = datetime.fromtimestamp(clock.t, timezone.utc)
                return (
                    when.astimezone(tz)
                    if tz is not None
                    else when.astimezone().replace(tzinfo=None)
                )

        return ClockDatetime


@dataclass
class Recorder:
    """Every seam call, in order."""

    calls: list[list[Any]] = field(default_factory=list)
    submits: list[list[Any]] = field(default_factory=list)
    #: Every unscripted call, even one the code under test caught and
    #: swallowed: the trace carries the list, and the golden expects none.
    unscripted: list[str] = field(default_factory=list)
    namespace: str = NAME
    #: The fake clock of a continuous scenario (None: batch, real clock).
    clock: FakeClock | None = None
    #: Stream SparkApplications submitted and not yet deleted.
    live_streams: set[str] = field(default_factory=set)
    #: Bucket bytes per layer for FakeS3 (None: the batch C360 record's).
    sizes: dict[str, int] | None = None
    #: S3 objects the scenario's fake boto client serves, by (bucket, key).
    objects: dict[tuple[str, str], bytes] = field(default_factory=dict)
    #: The scenario namespace exists (False: deleted mid-run).
    namespace_present: bool = True
    #: Its uid (changes when it is destroyed and deployed again), its phase,
    #: and reads that fail with a 503 before it answers again (-1: all).
    namespace_uid: str = "ns-uid-runchar-1"
    namespace_phase: str = "Active"
    namespace_errors: int = 0
    #: SparkApplications that exist in the fake cluster: name -> uid. A
    #: submit creates one; a delete removes it; a completed stage keeps it.
    apps: dict[str, str] = field(default_factory=dict)
    #: Each application's LB_RUN_ID values (the driver env of its submit).
    app_run_ids: dict[str, list[str]] = field(default_factory=dict)
    #: The datagen Job's uid while it exists (None: no Job).
    datagen_uid: str | None = None
    #: Objects recreated by someone else since the run created them: their
    #: deletes with the run's uid answer 409.
    foreign: set[str] = field(default_factory=set)
    #: Stage whose wait is interrupted, and how ("raise", "SIGINT", "SIGTERM").
    interrupt: tuple[str, str] | None = None
    #: Stage whose submit is interrupted after the application was created.
    submit_interrupt: str | None = None
    #: Signal sent while the fake operator heal holds the cluster lease.
    lease_signal: str | None = None
    #: Number of deletes left before one is interrupted (None: never).
    interrupt_delete_after: int | None = None
    #: The bronze datagen prefix is empty (a batch --generate scenario).
    fresh_bronze: bool = False
    #: The datagen Job this run deployed has not finished.
    datagen_running: bool = False
    #: With ``interrupt``: the state the monitor reports before the signal
    #: ("completed", "failed"): the application ended, its log is being read.
    interrupt_after_state: str | None = None

    def add(self, *entry: Any) -> None:
        self.calls.append(list(entry))

    def refuse(self, message: str) -> Unscripted:
        """Record an unscripted call; the caller raises the result."""
        self.unscripted.append(message)
        return Unscripted(message)


def send_interrupt(how: str) -> None:
    """Interrupt this process as an operator would: ``"raise"`` raises
    ``KeyboardInterrupt`` (Python's own SIGINT handler), ``"SIGINT"`` and
    ``"SIGTERM"`` send the real signal to this process, so whatever handler
    the code under test installed runs."""
    import signal as _signal

    if how == "raise":
        raise KeyboardInterrupt
    os.kill(os.getpid(), getattr(_signal, how))


def _unbound(func: Callable[..., Any]) -> Callable[..., Any]:
    """A module-level function as :func:`_checked` binds it (a leading
    ``self`` slot), so free functions and methods are checked alike."""

    def shim(self, *args, **kwargs):  # pragma: no cover - never called
        raise AssertionError("signature shim")

    params = [inspect.Parameter("self", inspect.Parameter.POSITIONAL_ONLY)]
    shim.__signature__ = inspect.signature(func).replace(  # type: ignore[attr-defined]
        parameters=params + list(inspect.signature(func).parameters.values())
    )
    shim.__qualname__ = func.__qualname__
    return shim


def _checked(rec: Recorder, real: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """Bind a fake's call against the real seam's signature (``self`` given
    as None): a call the real method would refuse is unscripted here too."""
    try:
        inspect.signature(real).bind(None, *args, **kwargs)
    except TypeError as e:
        raise rec.refuse(f"call the real {real.__qualname__} would refuse: {e}") from None


class FakeTrino:
    """Answers the SQL Lakebench sends to the Trino coordinator.

    The data file counts are those of run-20260927-084902-fc1eb5 (silver 366,
    one per interaction date; gold 1), before and after its compaction,
    which changed none (a no-op, recorded with a note). The silver table's
    partitions are those dates: every day of the 2024 corpus window, in the
    Trino CLI's quoted CSV, as the compaction plan's partition read gets
    them.
    """

    def __init__(
        self,
        rec: Recorder,
        silver_files: int | tuple[int, ...] = 366,
        gold_files: int | tuple[int, ...] = 1,
        snapshots: tuple[tuple[int, int], ...] = (),
    ) -> None:
        self._rec = rec
        #: A count, or one count per probe of that table in order (the last
        #: repeats): the continuous record's table health per round.
        self.silver_files = silver_files
        self.gold_files = gold_files
        #: (silver, gold) snapshot counts per probe; empty: 3 for both.
        self.snapshots = snapshots
        self._probes = {"silver files": 0, "gold files": 0, "silver snaps": 0, "gold snaps": 0}

    def _next(self, key: str, counts: int | tuple[int, ...]) -> int:
        if isinstance(counts, int):
            return counts
        n = self._probes[key]
        self._probes[key] += 1
        return counts[min(n, len(counts) - 1)]

    def _advance(self, low: str) -> None:
        """Statement time in the continuous record: an optimize about 28 s
        (169 s for six), an expire or orphan removal about 4 s (24 s for
        six). Batch scenarios have no clock."""
        clock = self._rec.clock
        if clock is None:
            return
        if "execute optimize" in low:
            clock.advance(28.0)
        elif "execute expire_snapshots" in low or "execute remove_orphan_files" in low:
            clock.advance(4.0)

    @staticmethod
    def silver_partitions() -> list[str]:
        day = datetime(2024, 1, 1)
        out = []
        while day.year == 2024:
            out.append(day.strftime("%Y-%m-%d"))
            day += timedelta(days=1)
        return out

    def answer(self, sql: str) -> tuple[int, str, str]:
        low = sql.lower()
        if (
            low.startswith("select distinct partition.interaction_date from")
            and '.silver."customer_interactions_enriched$partitions"' in low
        ):
            return 0, "".join(f'"{d}"\n' for d in self.silver_partitions()), ""
        if "execute optimize" in low:
            self._advance(low)
            return 0, "", ""
        if "execute expire_snapshots" in low or "execute remove_orphan_files" in low:
            self._advance(low)
            return 0, "", ""
        if low.startswith("select count(*) from") and '$files"' in low:
            if ".silver." in low:
                files = self._next("silver files", self.silver_files)
            else:
                files = self._next("gold files", self.gold_files)
            return 0, f'"{files}"\n', ""
        if low.startswith("select count(*) from") and '$snapshots"' in low:
            if not self.snapshots:
                return 0, '"3"\n', ""
            layer = "silver snaps" if ".silver." in low else "gold snaps"
            pair = self.snapshots[min(self._probes[layer], len(self.snapshots) - 1)]
            self._probes[layer] += 1
            return 0, f'"{pair[0] if layer == "silver snaps" else pair[1]}"\n', ""
        raise self._rec.refuse(f"unscripted Trino SQL: {sql}")


class RecordingK8s:
    """``K8sClient`` stand-in. Unscripted methods raise."""

    def __init__(self, rec: Recorder, trino: FakeTrino, context: str = "", namespace: str = ""):
        from lakebench.k8s.client import K8sClient

        self._real = K8sClient
        self._rec = rec
        self._trino = trino
        self.namespace = namespace or rec.namespace
        self.context_name = context
        rec.add("k8s", "get_k8s_client", namespace)

    def get_cluster_capacity(self, *args, **kwargs):
        _checked(self._rec, self._real.get_cluster_capacity, *args, **kwargs)
        self._rec.add("k8s", "get_cluster_capacity")
        return None

    def namespace_exists(self, *args, **kwargs) -> bool:
        _checked(self._rec, self._real.namespace_exists, *args, **kwargs)
        name = args[0] if args else kwargs["name"]
        self._rec.add("k8s", "namespace_exists", name)
        return name == self._rec.namespace and self._rec.namespace_present

    def exec_in_pod(self, name, command, namespace=None, container=None, timeout=30):
        _checked(self._rec, self._real.exec_in_pod, name, command, namespace, container, timeout)
        ns = namespace or self.namespace
        self._rec.add("exec", name, ns, container, timeout, " ".join(command[:2]), command[-1])
        if name != _TRINO_POD or command[:2] != ["trino", "--execute"]:
            raise self._rec.refuse(f"unscripted exec in {name}: {command}")
        return self._trino.answer(command[-1])

    def delete_custom_resource(self, *args, **kwargs):
        _checked(self._rec, self._real.delete_custom_resource, *args, **kwargs)
        bound = inspect.signature(self._real.delete_custom_resource).bind(None, *args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        self._rec.add("k8s", "delete_custom_resource", a["plural"], a["name"], a["namespace"])
        if a["namespace"] != self._rec.namespace:
            raise self._rec.refuse(f"delete outside the namespace: {a['namespace']}")
        self._rec.live_streams.discard(a["name"])
        self._rec.apps.pop(a["name"], None)
        return True

    # The raw API handles metrics/system_identity reads through (the system
    # identity and load samples at run start and end, ER-8 and ER-10b).
    @property
    def _core_v1(self) -> FakeCoreV1Api:
        return FakeCoreV1Api(self._rec)

    @property
    def _custom(self) -> FakeCustomObjectsApi:
        return FakeCustomObjectsApi(self._rec)

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted K8sClient.{attr}")


#: The CA bundle the fake API client verifies with: the system identity
#: hashes it (metrics/system_identity._api_server_ca).
HARNESS_CA = FIXTURES / "harness-ca.pem"

#: The fake cluster: two workers of one class, one cordoned worker and a
#: control-plane node (system identity counts all four by class; load sums
#: allocatable over the two schedulable workers).
_NODES = (
    ("worker-0", False, False),
    ("worker-1", False, False),
    ("worker-2", False, True),
    ("master-0", True, False),
)


def _fake_node(name: str, control_plane: bool, cordoned: bool):
    labels = {"node.kubernetes.io/instance-type": "vsphere-vm.cpu-32.mem-256gb.os-linux"}
    if control_plane:
        labels["node-role.kubernetes.io/control-plane"] = ""
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, labels=labels),
        spec=SimpleNamespace(unschedulable=cordoned),
        status=SimpleNamespace(
            capacity={"cpu": "32", "memory": "263882936Ki"},
            allocatable={"cpu": "31500m", "memory": "256Gi"},
            node_info=SimpleNamespace(architecture="amd64"),
        ),
    )


def _fake_pod(namespace: str, node: str, cpu: str, memory: str):
    container = SimpleNamespace(resources=SimpleNamespace(requests={"cpu": cpu, "memory": memory}))
    return SimpleNamespace(
        metadata=SimpleNamespace(namespace=namespace),
        spec=SimpleNamespace(
            node_name=node, containers=[container], init_containers=None, overhead=None
        ),
    )


def _api_exception(status: int):
    from kubernetes.client.rest import ApiException

    reason = {404: "Not Found", 409: "Conflict"}.get(status, "Error")
    return ApiException(status=status, reason=reason)


def _precondition_uid(body: Any) -> str | None:
    pre = getattr(body, "preconditions", None)
    return getattr(pre, "uid", None)


def _delete_with_uid(rec: Recorder, kind: str, name: str, namespace: str, body: Any) -> None:
    """A delete as the API server answers it: 404 when the object is gone,
    409 when the uid precondition names another object, else it is gone."""
    if namespace != rec.namespace:
        raise rec.refuse(f"delete outside the namespace: {kind} {name} in {namespace}")
    if rec.interrupt_delete_after is not None:
        if rec.interrupt_delete_after == 0:
            rec.interrupt_delete_after = None
            send_interrupt("SIGINT")
        else:
            rec.interrupt_delete_after -= 1
    uid = _precondition_uid(body)
    current = rec.apps.get(name) if kind == "SparkApplication" else rec.datagen_uid
    if current is None:
        raise _api_exception(404)
    if uid is None:
        raise rec.refuse(f"delete of {kind} {name} without a uid precondition")
    if uid != current or f"{kind}/{name}" in rec.foreign:
        raise _api_exception(409)
    if kind == "SparkApplication":
        rec.apps.pop(name, None)
        rec.live_streams.discard(name)
    else:
        rec.datagen_uid = None


class _FakeApi:
    """Base for the kubernetes.client API fakes: records, refuses the unknown."""

    def __init__(self, rec: Recorder, *args, **kwargs) -> None:
        self._rec = rec

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted {type(self).__name__}.{attr}")


#: The cluster fingerprint the fake kubeconfig resolves to, stamped on the
#: scenario namespace by its deploy.
API_SERVER_FP = "harnessfp001"


def _namespace_object(name: str):
    """The scenario namespace as ``lakebench deploy`` leaves it: stamped
    with the deployment's name and cluster, and recording the three buckets
    it created (the backend has no bucket tagging, as FlashBlade)."""
    from lakebench.deploy import ownership

    buckets = ",".join(f"{NAME}-{layer}" for layer in ("bronze", "gold", "silver"))
    annotations = {
        ownership.ANNOTATION_DEPLOYMENT_NAME: NAME,
        ownership.ANNOTATION_API_SERVER: API_SERVER_FP,
        ownership.ANNOTATION_CREATED_BUCKETS: buckets,
    }
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            annotations=annotations,
            labels={},
            deletion_timestamp=None,
            uid=NAMESPACE_UID,
        ),
        status=SimpleNamespace(phase="Active"),
    )


#: The scenario namespace's uid.
NAMESPACE_UID = "ns-uid-runchar-1"


class FakeCoreV1Api(_FakeApi):
    def read_namespace(self, name, **kw):
        self._rec.add("CoreV1Api", "read_namespace", name)
        if name != self._rec.namespace or not self._rec.namespace_present:
            raise _api_exception(404)
        if self._rec.namespace_errors:
            if self._rec.namespace_errors > 0:
                self._rec.namespace_errors -= 1
            raise _api_exception(503)
        ns = _namespace_object(name)
        ns.metadata.uid = self._rec.namespace_uid
        ns.status.phase = self._rec.namespace_phase
        return ns

    def list_namespace(self, **kw):
        self._rec.add("CoreV1Api", "list_namespace")
        return SimpleNamespace(items=[_namespace_object(self._rec.namespace)])

    def read_namespaced_pod(self, name, namespace, **kw):
        # Driver pods of an earlier run's streams: none are left.
        self._rec.add("CoreV1Api", "read_namespaced_pod", name, namespace)
        raise _api_exception(404)

    api_client = SimpleNamespace(
        configuration=SimpleNamespace(ssl_ca_cert=str(HARNESS_CA), verify_ssl=True)
    )

    def list_node(self, **kw):
        self._rec.add("CoreV1Api", "list_node", kw.get("_request_timeout"))
        return SimpleNamespace(items=[_fake_node(*n) for n in _NODES])

    def list_pod_for_all_namespaces(self, field_selector="", **kw):
        self._rec.add(
            "CoreV1Api", "list_pod_for_all_namespaces", field_selector, kw.get("_request_timeout")
        )
        return SimpleNamespace(
            items=[
                _fake_pod(self._rec.namespace, "worker-0", "8", "64Gi"),  # this deployment
                _fake_pod("other-tenant", "worker-1", "4500m", "16Gi"),  # co-tenant
                _fake_pod("openshift-dns", "master-0", "1", "1Gi"),  # control plane
            ]
        )

    def list_namespaced_pod(self, namespace, label_selector="", **kw):
        self._rec.add("CoreV1Api", "list_namespaced_pod", namespace, label_selector)
        if label_selector == _TRINO_SELECTOR:
            pod = SimpleNamespace(metadata=SimpleNamespace(name=_TRINO_POD))
            return SimpleNamespace(items=[pod])
        return SimpleNamespace(items=[])


class FakeAppsV1Api(_FakeApi):
    def _ready(self, kind: str, name: str, namespace: str):
        self._rec.add("AppsV1Api", f"read_namespaced_{kind}", name, namespace)
        replicas = 2 if name == "lakebench-trino-worker" else 1
        return SimpleNamespace(
            status=SimpleNamespace(ready_replicas=replicas), spec=SimpleNamespace(replicas=replicas)
        )

    def read_namespaced_stateful_set(self, name, namespace, **kw):
        return self._ready("stateful_set", name, namespace)

    def read_namespaced_deployment(self, name, namespace, **kw):
        return self._ready("deployment", name, namespace)


class FakeCustomObjectsApi(_FakeApi):
    def get_cluster_custom_object(self, group, version, plural, name, **kw):
        self._rec.add("CustomObjectsApi", "get_cluster", plural, name)
        # Vanilla Kubernetes: no ClusterVersion.
        raise _api_exception(404)

    def get_namespaced_custom_object(self, group, version, namespace, plural, name, **kw):
        self._rec.add("CustomObjectsApi", "get", plural, name, namespace)
        if name in self._rec.live_streams:
            return {"status": {"applicationState": {"state": "RUNNING"}}}
        if name in self._rec.apps:
            env = [{"name": "LB_RUN_ID", "value": v} for v in self._rec.app_run_ids.get(name, [])]
            return {
                "metadata": {"name": name, "uid": self._rec.apps[name]},
                "spec": {"driver": {"env": env}},
                "status": {"applicationState": {"state": "RUNNING"}},
            }
        # No SparkApplication is left over from an earlier run.
        raise _api_exception(404)

    def delete_namespaced_custom_object(
        self, group, version, namespace, plural, name, body=None, **kw
    ):
        self._rec.add(
            "CustomObjectsApi",
            "delete",
            plural,
            name,
            namespace,
            _precondition_uid(body),
            getattr(body, "propagation_policy", None),
        )
        if (group, version, plural) != ("sparkoperator.k8s.io", "v1beta2", "sparkapplications"):
            raise self._rec.refuse(f"delete of {group}/{version} {plural}/{name}")
        _delete_with_uid(self._rec, "SparkApplication", name, namespace, body)
        return {"status": "Success"}


class FakeBatchV1Api(_FakeApi):
    def read_namespaced_job(self, name, namespace, **kw):
        """The datagen Job of a continuous run: finished (its corpus is
        written within minutes, before the streams start)."""
        self._rec.add("BatchV1Api", "read_namespaced_job", name, namespace)
        if name != "lakebench-datagen" or namespace != self._rec.namespace:
            raise _api_exception(404)
        cond = SimpleNamespace(type="Complete", status="True")
        running = self._rec.datagen_running and self._rec.datagen_uid is not None
        return SimpleNamespace(
            metadata=SimpleNamespace(name=name, uid=self._rec.datagen_uid or "uid-datagen-pre"),
            status=SimpleNamespace(conditions=[] if running else [cond]),
        )

    def delete_namespaced_job(self, name, namespace, body=None, **kw):
        self._rec.add(
            "BatchV1Api",
            "delete_namespaced_job",
            name,
            namespace,
            _precondition_uid(body),
            getattr(body, "propagation_policy", None),
        )
        if name != "lakebench-datagen":
            raise self._rec.refuse(f"delete of Job {name}")
        _delete_with_uid(self._rec, "Job", name, namespace, body)


class FakeApisApi(_FakeApi):
    def get_api_versions(self, **kw):
        """The API groups: an OpenShift cluster, as the records' is."""
        self._rec.add("ApisApi", "get_api_versions")
        group = SimpleNamespace(name="security.openshift.io")
        return SimpleNamespace(groups=[SimpleNamespace(name="apps"), group])


class FakeVersionApi(_FakeApi):
    def get_code(self, **kw):
        self._rec.add("VersionApi", "get_code_version", kw.get("_request_timeout"))
        return SimpleNamespace(git_version="v1.31.6")

    def get_code_with_http_info(self, **kw):
        """The API server's HTTP Date header: the cluster clock is the host's."""
        self._rec.add("VersionApi", "get_code")
        if self._rec.clock is not None:
            now = datetime.fromtimestamp(self._rec.clock.t + CLUSTER_OFFSET_S, timezone.utc)
        else:
            now = datetime.now(timezone.utc)
        date = email.utils.format_datetime(now, usegmt=True)
        return None, 200, {"Date": date}


def _refuse_call_api(rec: Recorder):
    def call_api(self, resource_path, method, *args, **kwargs):
        raise rec.refuse(f"unscripted Kubernetes API call: {method} {resource_path}")

    return call_api


class FakeLeaseApi:
    """The cluster lease ConfigMap in ``lakebench-system`` as the API server
    keeps it, for ``cluster_lock`` (the calls ``acquire_cluster_lock`` and
    ``release_cluster_lock`` make). Its create and delete are traced."""

    def __init__(self, rec: Recorder) -> None:
        self._rec = rec
        self.cm: Any = None
        self._rv = 0

    def _stamp(self, body: Any) -> Any:
        self._rv += 1
        body.metadata.resource_version = str(self._rv)
        body.metadata.uid = "uid-lease-1"
        self.cm = body
        return body

    def read_namespace(self, name, **kw):
        return SimpleNamespace(metadata=SimpleNamespace(name=name))

    def create_namespaced_config_map(self, namespace, body, **kw):
        self._rec.add("Lease", "create", namespace, body.metadata.name)
        if self.cm is not None:
            raise _api_exception(409)
        return self._stamp(body)

    def replace_namespaced_config_map(self, name, namespace, body, **kw):
        if self.cm is None:
            raise _api_exception(404)
        if body.metadata.resource_version != self.cm.metadata.resource_version:
            raise _api_exception(409)
        return self._stamp(body)

    def read_namespaced_config_map(self, name, namespace, **kw):
        if self.cm is None:
            raise _api_exception(404)
        return self.cm

    def delete_namespaced_config_map(self, name, namespace, body=None, **kw):
        self._rec.add("Lease", "delete", namespace, name)
        if self.cm is None:
            raise _api_exception(404)
        self.cm = None

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted lease API call {attr}")


class FakeOperator:
    """``SparkOperatorManager`` stand-in: installed, ready, watching."""

    def __init__(self, rec: Recorder, *args, **kwargs):
        from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

        self._real = SparkOperatorManager
        self._rec = rec
        _checked(rec, SparkOperatorManager.__init__, *args, **kwargs)
        bound = inspect.signature(SparkOperatorManager.__init__).bind(None, *args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        self.job_namespace = a.get("job_namespace")
        rec.add("SparkOperator", "init", a.get("namespace"), a.get("version"), self.job_namespace)

    def _status(self, **kw):
        from lakebench.modules.pipeline_engines.spark.operator import OperatorStatus

        return OperatorStatus(
            installed=True,
            version="2.5.1",
            namespace="spark-operator",
            ready=True,
            message="ready",
            **kw,
        )

    def check_status(self, *args, **kwargs):
        _checked(self._rec, self._real.check_status, *args, **kwargs)
        self._rec.add("SparkOperator", "check_status")
        return self._status()

    def ensure_namespace_watched(self, *args, **kwargs):
        _checked(self._rec, self._real.ensure_namespace_watched, *args, **kwargs)
        self._rec.add("SparkOperator", "ensure_namespace_watched", kwargs.get("can_heal", False))
        if self._rec.lease_signal is not None:
            # The heal path: the watch list changes under the real cluster
            # lease (operator.py takes it the same way), and the signal
            # arrives while the shared helm upgrade is running.
            from lakebench.deploy import cluster_lock

            lease = FakeLeaseApi(self._rec)
            with cluster_lock.cluster_lock(lease, timeout=0):
                self._rec.add("SparkOperator", "helm upgrade", "start")
                send_interrupt(self._rec.lease_signal)
                self._rec.add("SparkOperator", "helm upgrade", "done")
        return self._status(watching_namespace=True, watched_namespaces=[self.job_namespace])

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted SparkOperatorManager.{attr}")


#: Environment keys a stage submission is traced with the value of.
TRACED_ENV_VALUES = (
    "LB_RUN_ID",
    "LB_BRONZE_CYCLE",
    "LB_SILVER_INCREMENTAL",
    "LB_GOLD_INCREMENTAL",
    "LB_FORCE_REBUILD",
)
#: Environment keys traced with their value only when a submission sets
#: them (continuous preflight and streams), so batch traces keep their shape.
TRACED_ENV_IF_SET = ("LB_CONTINUOUS_RESET", "LB_CONTINUOUS_WINDOW_S", "LB_REGISTER_TABLE")

#: Executors each stream requested in the continuous record
#: (run-20261001-090400-db3ffe, streaming[].requested_executors).
STREAM_EXECUTORS = {"bronze-ingest": 2, "silver-stream": 4, "gold-refresh": 2}


class FakeJobManager:
    """``SparkJobManager`` stand-in: scripts deploy, submissions succeed."""

    def __init__(self, rec: Recorder) -> None:
        from lakebench.modules.pipeline_engines.spark.job import SparkJobManager

        self._real = SparkJobManager
        self._rec = rec
        #: Read (and cleared) by the continuous submit loop.
        self.budget_warnings: list[str] = []

    def engine_name(self) -> str:
        return "spark"

    def get_job_status(self, *args, **kwargs):
        """A stream submitted and not deleted is RUNNING on its first driver
        and submission for the whole run; anything else is not found."""
        from lakebench.spark.job import JobState, JobStatus

        _checked(self._rec, self._real.get_job_status, *args, **kwargs)
        name = args[0] if args else kwargs["job_name"]
        self._rec.add("JobManager", "get_job_status", name)
        if name not in self._rec.live_streams:
            # The real manager's text for a 404 (job.py get_job_status).
            return JobStatus(name=name, state=JobState.UNKNOWN, message="Job not found")
        stage = name.removeprefix("lakebench-")
        return JobStatus(
            name=name,
            state=JobState.RUNNING,
            message="",
            driver_pod=f"{name}-driver",
            start_time="2026-10-01T15:05:10Z",
            executor_count=STREAM_EXECUTORS.get(stage, 0),
            submission_attempts=1,
        )

    def _delete_job(self, *args, **kwargs) -> None:
        _checked(self._rec, self._real._delete_job, *args, **kwargs)
        name = args[0] if args else kwargs["job_name"]
        self._rec.add("JobManager", "_delete_job", name)
        self._rec.live_streams.discard(name)

    def deploy_scripts_configmap(self, *args, **kwargs) -> bool:
        _checked(self._rec, self._real.deploy_scripts_configmap, *args, **kwargs)
        self._rec.add("JobManager", "deploy_scripts_configmap")
        return True

    def submit_job(self, *args, **kwargs):
        from lakebench.spark.job import JobState, JobStatus

        _checked(self._rec, self._real.submit_job, *args, **kwargs)
        bound = inspect.signature(self._real.submit_job).bind(None, *args, **kwargs)
        job_type = bound.arguments["job_type"]
        env = dict(bound.arguments.get("cycle_env") or {})
        self._rec.add("JobManager", "submit_job", job_type.value)
        values = {k: env.get(k) for k in TRACED_ENV_VALUES}
        values.update({k: env[k] for k in TRACED_ENV_IF_SET if k in env})
        self._rec.submits.append([job_type.value, sorted(env), values])
        name = f"lakebench-{job_type.value}"
        # The API server's object: a fresh uid per create, and the run id
        # its driver env carries.
        uid = f"uid-{job_type.value}-{len(self._rec.submits)}"
        self._rec.apps[name] = uid
        self._rec.app_run_ids[name] = [env["LB_RUN_ID"]] if "LB_RUN_ID" in env else []
        if self._rec.submit_interrupt == job_type.value:
            # The create landed; the interrupt arrived before its reply.
            self._rec.submit_interrupt = None
            send_interrupt("SIGINT")
        if job_type.value in STREAM_EXECUTORS:
            self._rec.live_streams.add(name)
            return JobStatus(
                name=name,
                state=JobState.SUBMITTED,
                message="submitted",
                executor_count=STREAM_EXECUTORS[job_type.value],
                uid=uid,
            )
        return JobStatus(name=name, state=JobState.SUBMITTED, message="submitted", uid=uid)

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted SparkJobManager.{attr}")


#: Seconds a fake stage runs: its last RUNNING poll and the driver's end.
_STAGE_SECONDS = 60.0
_LAST_RUNNING_SECONDS = 50.0
_DRIVER_END_SECONDS = 55.0


class FakeMonitor:
    """``SparkJobMonitor`` stand-in: each stage completes (or, when the
    scenario lists it in *failing*, fails) with its fixture driver log, and
    the driver container's end is readable, so ``run`` times the stage from
    the cluster's clock as it does live."""

    def __init__(self, rec: Recorder, scenario_dir: Path, failing: tuple[str, ...], *a, **kw):
        from lakebench.modules.pipeline_engines.spark.monitor import SparkJobMonitor

        self._real = SparkJobMonitor
        _checked(rec, SparkJobMonitor.__init__, *a, **kw)
        self._rec = rec
        self._dir = scenario_dir
        self._failing = failing
        self._waited: dict[str, datetime] = {}

    def wait_for_completion(self, *args, **kwargs):
        from lakebench.modules.pipeline_engines.spark.monitor import JobResult
        from lakebench.spark.job import JobState, JobStatus

        _checked(self._rec, self._real.wait_for_completion, *args, **kwargs)
        bound = inspect.signature(self._real.wait_for_completion).bind(None, *args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        job_name = a["job_name"]
        self._rec.add(
            "SparkJobMonitor",
            "wait_for_completion",
            job_name,
            a["timeout_seconds"],
            a["poll_interval"],
        )
        self._waited[job_name] = datetime.now(timezone.utc)
        stage = job_name.removeprefix("lakebench-")
        log_path = self._dir / f"{stage}.log"
        if not log_path.exists():
            raise self._rec.refuse(f"no fixture driver log for {job_name}: {log_path}")
        running = JobStatus(name=job_name, state=JobState.RUNNING, message="", executor_count=4)
        if a["progress_callback"] is not None:
            a["progress_callback"](running)
        if self._rec.interrupt is not None and self._rec.interrupt[0] == stage:
            # Ctrl-C (or SIGTERM) while the stage's application runs, or, with
            # interrupt_after_state, once it has ended and its log is read.
            ended = self._rec.interrupt_after_state
            if ended is not None and a["progress_callback"] is not None:
                a["progress_callback"](JobStatus(name=job_name, state=JobState(ended), message=""))
            send_interrupt(self._rec.interrupt[1])
        failed = stage in self._failing
        final = JobStatus(
            name=job_name,
            state=JobState.FAILED if failed else JobState.COMPLETED,
            message="driver exited with code 1" if failed else "completed",
        )
        return JobResult(
            job_name=job_name,
            success=not failed,
            message=final.message,
            elapsed_seconds=_STAGE_SECONDS,
            driver_logs=log_path.read_text(),
            final_status=final,
            last_running_elapsed=_LAST_RUNNING_SECONDS,
        )

    def wait_until_running(self, *args, **kwargs):
        """A stream's driver is running at once (no submission failure)."""
        from lakebench.modules.pipeline_engines.spark.monitor import JobResult
        from lakebench.spark.job import JobState, JobStatus

        _checked(self._rec, self._real.wait_until_running, *args, **kwargs)
        bound = inspect.signature(self._real.wait_until_running).bind(None, *args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        job_name = a["job_name"]
        self._rec.add("SparkJobMonitor", "wait_until_running", job_name, a["poll_interval"])
        if job_name not in self._rec.live_streams:
            raise self._rec.refuse(f"wait_until_running on a stream not submitted: {job_name}")
        running = JobStatus(name=job_name, state=JobState.RUNNING, message="", executor_count=0)
        if a["on_status"] is not None:
            a["on_status"](running, 0.0)
        return JobResult(
            job_name=job_name,
            success=True,
            message="running",
            elapsed_seconds=0.0,
            final_status=running,
        )

    def _get_driver_logs(self, *args, **kwargs):
        """A stream driver's log as the cluster holds it now: the fixture
        log (captured just before the record's streams stopped) up to the
        last line stamped at or before the fake cluster clock, so the window
        and the settle wait see the log grow as they did live."""
        _checked(self._rec, self._real._get_driver_logs, *args, **kwargs)
        bound = inspect.signature(self._real._get_driver_logs).bind(None, *args, **kwargs)
        bound.apply_defaults()
        job_name = bound.arguments["job_name"]
        tail = bound.arguments["tail_lines"]
        self._rec.add("SparkJobMonitor", "_get_driver_logs", job_name)
        if job_name not in self._rec.live_streams:
            raise self._rec.refuse(f"driver logs of a stream not running: {job_name}")
        stage = job_name.removeprefix("lakebench-")
        log_path = self._dir / f"{stage}.log"
        if not log_path.exists():
            raise self._rec.refuse(f"no fixture driver log for {job_name}: {log_path}")
        clock = self._rec.clock
        text = log_path.read_text()
        if clock is not None:
            text = log_until(text, clock.cluster_now())
        if tail is not None:
            # The real read's tail (100 lines by default): a caller that
            # drops tail_lines=None loses the window's first lines here too.
            text = "\n".join(text.splitlines()[-tail:]) + "\n"
        return text

    def application_end(self, *args, **kwargs):
        from lakebench.modules.pipeline_engines.spark.monitor import TIMING_DRIVER

        _checked(self._rec, self._real.application_end, *args, **kwargs)
        job_name = args[0] if args else kwargs["job_name"]
        self._rec.add("SparkJobMonitor", "application_end", job_name)
        waited = self._waited.get(job_name)
        if waited is None:
            return None, ""
        return waited + timedelta(seconds=_DRIVER_END_SECONDS), TIMING_DRIVER

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted SparkJobMonitor.{attr}")


_LOG_TS = re.compile(r"^\[lb\] (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?) - ")


def log_until(text: str, until: datetime) -> str:
    """The lines of a fixture stream log written at or before *until*
    (naive UTC): every line up to the first one stamped later."""
    out: list[str] = []
    for line in text.splitlines():
        m = _LOG_TS.match(line)
        if m and datetime.fromisoformat(m.group(1)) > until:
            break
        out.append(line)
    return "\n".join(out) + ("\n" if out else "")


class _FakeBoto:
    """The boto3 client under ``S3Client.raw_client``: the backend has no
    bucket tagging (FlashBlade answers NotImplemented), and the scenario
    deployment's buckets are empty before its first run."""

    def __init__(self, rec: Recorder) -> None:
        self._rec = rec

    def get_bucket_tagging(self, Bucket):  # noqa: N803 -- boto3 keyword
        from botocore.exceptions import ClientError

        self._rec.add("S3", "get_bucket_tagging", Bucket)
        raise ClientError(
            {"Error": {"Code": "NotImplemented", "Message": "Not implemented"}}, "GetBucketTagging"
        )

    def get_object(self, Bucket, Key):  # noqa: N803 -- boto3 keywords
        """An object the scenario serves (the AML recall.json sidecar)."""
        import io

        self._rec.add("S3", "get_object", Bucket, Key)
        if not Bucket.startswith(f"{NAME}-"):
            raise self._rec.refuse(f"read from a bucket not of the deployment: {Bucket}")
        # The key must name this run: run() exports LB_RUN_ID before the
        # score stage, so a read of another run's sidecar is unscripted.
        run_id = os.environ.get("LB_RUN_ID") or "<no run id>"
        for (bucket, key), body in self._rec.objects.items():
            if bucket == Bucket and key.replace("<run_id>", run_id) == Key:
                return {"Body": io.BytesIO(body)}
        raise self._rec.refuse(f"unscripted S3 object {Bucket}/{Key}")

    def list_objects_v2(self, Bucket, Prefix="", MaxKeys=1000, **kw):  # noqa: N803
        self._rec.add("S3", "list_objects_v2", Bucket, Prefix, MaxKeys)
        return {"KeyCount": 0, "Contents": []}

    def get_paginator(self, operation):
        """The corpus observation's one listing of the datagen scope
        (corpus_digest.list_scope): the scenario's markers are not served,
        so the corpus reads as written by an image without markers."""
        if operation != "list_objects_v2":
            raise self._rec.refuse(f"unscripted paginator {operation}")
        rec = self._rec

        class _Paginator:
            def paginate(self, Bucket, Prefix=""):  # noqa: N803 -- boto3 keywords
                rec.add("S3", "paginate list_objects_v2", Bucket, Prefix)
                if not Bucket.startswith(f"{NAME}-"):
                    raise rec.refuse(f"listing of a bucket not of the deployment: {Bucket}")
                return iter([{"KeyCount": 0, "Contents": []}])

        return _Paginator()

    def head_bucket(self, Bucket):  # noqa: N803 -- boto3 keyword
        """The HEAD the system identity sends on the bronze bucket (its
        Server header)."""
        self._rec.add("S3", "head_bucket", Bucket)
        return {"ResponseMetadata": {"HTTPHeaders": {"server": "FakeS3"}}}

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted boto3 S3 client.{attr}")


class FakeS3:
    """``S3Client`` stand-in with fixed bucket sizes; its constructor
    arguments are recorded (endpoint and keys come from the config)."""

    #: Bytes per bucket, after run-20260927-084902-fc1eb5 (the run the batch
    #: C360 fixture logs come from): bronze 9.917 GiB, silver 9.762 GiB,
    #: gold 64.5 KiB. Bronze and silver are rounded to 10 kB, so the traced
    #: sizes differ from the record's in the fourth decimal; gold is exact.
    SIZES = {"bronze": 10_648_840_000, "silver": 10_482_310_000, "gold": 69_274}
    #: Bytes per bucket at the end of the continuous record
    #: run-20261001-090400-db3ffe (bronze_size_gb 17.3319..., exact).
    CONTINUOUS_SIZES = {"bronze": 18_609_986_568, "silver": 13_972_495_805, "gold": 423_926}
    #: The client constructed (``S3Client`` sets it to the error otherwise).
    _init_error = None

    def __init__(self, rec: Recorder, *args, **kwargs) -> None:
        from lakebench.s3.client import S3Client

        self._real = S3Client
        self._rec = rec
        _checked(rec, S3Client.__init__, *args, **kwargs)
        bound = inspect.signature(S3Client.__init__).bind(None, *args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        rec.add(
            "S3",
            "init",
            a.get("endpoint"),
            a.get("access_key"),
            a.get("secret_key"),
            a.get("region"),
            a.get("path_style"),
            a.get("verify_ssl"),
        )

    def get_bucket_size(self, *args, **kwargs):
        from lakebench.s3.client import BucketInfo

        _checked(self._rec, self._real.get_bucket_size, *args, **kwargs)
        bound = inspect.signature(self._real.get_bucket_size).bind(None, *args, **kwargs)
        bound.apply_defaults()
        bucket_name = bound.arguments["bucket_name"]
        self._rec.add("S3", "get_bucket_size", bucket_name, bound.arguments["prefix"])
        layer = bucket_name.rsplit("-", 1)[-1]
        sizes = self._rec.sizes or self.SIZES
        if layer not in sizes:
            raise self._rec.refuse(f"unscripted bucket {bucket_name}")
        if bound.arguments["prefix"] and self._rec.fresh_bronze and layer == "bronze":
            # The datagen prefix of a fresh deployment, before it generates.
            return BucketInfo(name=bucket_name, exists=True, object_count=0, size_bytes=0)
        return BucketInfo(name=bucket_name, exists=True, object_count=100, size_bytes=sizes[layer])

    @property
    def raw_client(self) -> _FakeBoto:
        self._rec.add("S3", "raw_client")
        return _FakeBoto(self._rec)

    def delete_prefix(self, *args, **kwargs) -> int:
        _checked(self._rec, self._real.delete_prefix, *args, **kwargs)
        bound = inspect.signature(self._real.delete_prefix).bind(None, *args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        self._rec.add("S3", "delete_prefix", a["bucket_name"], a["prefix"])
        if not a["bucket_name"].startswith(f"{NAME}-"):
            raise self._rec.refuse(f"delete in a bucket not of the deployment: {a['bucket_name']}")
        return 0

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted S3Client.{attr}")


#: Row counts per Customer 360 query, from run-20260927-084902-fc1eb5.
C360_ROWS = {
    "Q1": 1,
    "Q2": 455,
    "Q3": 12,
    "Q4": 6,
    "Q5": 90,
    "Q6": 6,
    "Q7": 5,
    "Q8": 1,
    "Q9": 30,
}
#: Row counts per AML query (FQ and the investigator IQ set), from
#: run-20261001-114528-37810a.
AML_ROWS = {
    "FQ1": 1,
    "FQ2": 100,
    "FQ6": 500,
    "FQ3": 200,
    "FQ7": 100,
    "FQ4": 878356,
    "FQ5": 17,
    "FQ8": 100,
    "IQ1": 1,
    "IQ2": 12,
    "IQ3": 500,
    "IQ4": 21,
}
#: Seconds every fake query takes.
_QUERY_SECONDS = 2.0

#: The continuous record's in-stream rounds (run-20261001-090400-db3ffe):
#: per round, the seconds each query took (in query-set order), and the
#: round's wall time from its start to the next health line, which the fake
#: clock advances by (table probes and the event-age probe cost no time).
CONTINUOUS_ROUNDS = (
    ((21.866, 13.098, 14.632, 14.371, 11.923, 13.197, 16.277, 4.108), 136.5),
    ((18.887, 10.917, 15.388, 16.001, 13.418, 14.386, 18.199, 3.501), 131.6),
    ((19.091, 12.026, 16.866, 20.708, 18.193, 21.101, 25.827, 3.959), 158.0),
)
#: Gold event age the record's second round read (seconds).
CONTINUOUS_EVENT_AGE = 55_264_713


class FakeBenchmark:
    """``BenchmarkRunner`` stand-in: every query of the schema's real query
    set succeeds in 2 s with the row count the recorded run returned."""

    def __init__(self, rec: Recorder, *args, **kwargs) -> None:
        from lakebench.benchmark.runner import BenchmarkRunner

        self._real = BenchmarkRunner
        self._rec = rec
        _checked(rec, BenchmarkRunner.__init__, *args, **kwargs)
        bound = inspect.signature(BenchmarkRunner.__init__).bind(None, *args, **kwargs)
        bound.apply_defaults()
        self.config = bound.arguments["config"]
        self.tm_run_id = bound.arguments.get("tm_run_id")
        rec.add("Benchmark", "init", self.tm_run_id is not None)
        # What the in-stream round reads from the real runner.
        t = self.config.architecture.tables
        self.executor = FakeRoundExecutor(rec)
        self.catalog = self.config.architecture.query_engine.trino.catalog_name
        self.gold_table = t.gold
        self._extra_tables = {"gold_alerts": t.gold_alerts}
        self._rounds = 0

    def _queries(self):
        from lakebench.benchmark.queries import get_benchmark_queries

        queries = get_benchmark_queries(self.config.architecture.workload.schema_type)
        if not self.config.architecture.workload.tm_operations.enabled or not self.tm_run_id:
            queries = [q for q in queries if q.query_class != "investigator"]
        return queries

    def _result(self, query, iterations: int = 1, seconds: float = _QUERY_SECONDS, fp=None):
        from lakebench.benchmark.runner import QueryResult

        prefix = query.name.split("_", 1)[0]
        return QueryResult(
            query=query,
            elapsed_seconds=seconds,
            rows_returned={**C360_ROWS, **AML_ROWS}.get(prefix, 1),
            success=True,
            samples=[seconds] * iterations,
            result_fingerprint=fp,
        )

    def _round_seconds(self, n_queries: int) -> tuple[list[float], float]:
        """Per-query seconds and wall seconds of the next in-stream round
        (the record's rounds, the last repeating), or the batch constant."""
        if self._rec.clock is None:
            return [_QUERY_SECONDS] * n_queries, 0.0
        times, wall = CONTINUOUS_ROUNDS[min(self._rounds, len(CONTINUOUS_ROUNDS) - 1)]
        self._rounds += 1
        return list(times[:n_queries]), wall

    def run_power(self, *args, **kwargs):
        from lakebench.benchmark.runner import BenchmarkResult

        _checked(self._rec, self._real.run_power, *args, **kwargs)
        bound = inspect.signature(self._real.run_power).bind(None, *args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        self._rec.add(
            "Benchmark",
            "run_power",
            a["cache"],
            a["iterations"],
            a["query_timeout"],
            a["fingerprint"],
        )
        progress = a["progress_callback"]
        if self._rec.interrupt is not None and self._rec.interrupt[0] == "benchmark":
            send_interrupt(self._rec.interrupt[1])
        queries = self._queries()
        if a["fingerprint"]:
            # The batch benchmark and the continuous result check: fixed
            # times; the continuous check carries the record's fingerprints.
            seconds, wall = [_QUERY_SECONDS] * len(queries), 0.0
        else:
            seconds, wall = self._round_seconds(len(queries))
        fps = CONTINUOUS_FINGERPRINTS if self._rec.clock is not None else {}
        results = []
        for n, q in enumerate(queries, 1):
            if progress is not None:
                progress(n, len(queries), q.name, "start")
            fp = fps.get(q.name) if a["fingerprint"] else None
            results.append(self._result(q, a["iterations"], seconds[n - 1], fp))
            if progress is not None:
                progress(n, len(queries), q.name, "done", elapsed=seconds[n - 1], success=True)
        if self._rec.clock is not None:
            self._rec.clock.advance(wall)
        total = sum(r.elapsed_seconds for r in results)
        return BenchmarkResult(
            mode="power",
            cache=a["cache"],
            scale=self.config.architecture.workload.datagen.get_effective_scale(),
            queries=results,
            total_seconds=total,
            qph=len(results) / total * 3600 if total else 0.0,
            iterations=a["iterations"],
            engine=self.config.architecture.query_engine.type.value,
        )

    def probe_query(self, *args, **kwargs):
        _checked(self._rec, self._real.probe_query, *args, **kwargs)
        name = args[0] if args else kwargs.get("name")
        self._rec.add("Benchmark", "probe_query", name)
        queries = self._queries()
        if name:
            for q in queries:
                if q.name == name:
                    return q
            raise ValueError(f"probe query {name!r} is not in the query set")
        return next(q for q in queries if q.query_class == "scan")

    def time_query(self, *args, **kwargs):
        _checked(self._rec, self._real.time_query, *args, **kwargs)
        bound = inspect.signature(self._real.time_query).bind(None, *args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        self._rec.add("Benchmark", "time_query", a["query"].name, a["iterations"])
        return self._result(a["query"], a["iterations"])

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted BenchmarkRunner.{attr}")


class FakeRoundExecutor:
    """The benchmark runner's executor as an in-stream round uses it:
    cache flush and the gold event-age probe."""

    def __init__(self, rec: Recorder) -> None:
        self._rec = rec

    def engine_name(self) -> str:
        return "trino"

    def flush_cache(self) -> None:
        self._rec.add("RoundExecutor", "flush_cache")

    def adapt_query(self, sql: str) -> str:
        return sql

    def execute_query(self, sql: str, timeout: int = 300):
        from lakebench.benchmark.result import QueryExecutorResult

        self._rec.add("RoundExecutor", "execute_query", sql, timeout)
        if not sql.startswith("SELECT date_diff('second'"):
            raise self._rec.refuse(f"unscripted round executor SQL: {sql}")
        return QueryExecutorResult(
            sql=sql,
            engine="trino",
            duration_seconds=0.1,
            rows_returned=1,
            raw_output=f'"{CONTINUOUS_EVENT_AGE}"\n',
        )

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted round QueryExecutor.{attr}")


class FakeExecutor:
    """The Trino executor the post-maintenance worker-readiness probe uses."""

    def __init__(self, rec: Recorder) -> None:
        self._rec = rec

    def execute_query(self, sql: str, timeout: int = 300):
        from lakebench.benchmark.result import QueryExecutorResult

        self._rec.add("Executor", "execute_query", sql, timeout)
        if "system.runtime.nodes" not in sql:
            raise self._rec.refuse(f"unscripted executor SQL: {sql}")
        return QueryExecutorResult(
            sql=sql, engine="trino", duration_seconds=0.1, rows_returned=1, raw_output='"2"\n'
        )

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted QueryExecutor.{attr}")


class FakeSubprocess:
    """``subprocess.run``/``Popen``: git runs for real, the Trino worker
    replica read is answered, anything else is unscripted."""

    def __init__(
        self, rec: Recorder, real_run: Callable[..., Any], real_popen: Callable[..., Any]
    ) -> None:
        self._rec = rec
        self._real_run = real_run
        self._real_popen = real_popen

    def run(self, argv, *args, **kwargs):
        argv = [str(a) for a in argv]
        if argv and Path(argv[0]).name == "git":
            return self._real_run(argv, *args, **kwargs)
        self._rec.add("subprocess", *argv)
        if (
            Path(argv[0]).name == "kubectl"
            and "statefulset" in argv
            and "lakebench-trino-worker" in argv
        ):
            return subprocess.CompletedProcess(argv, 0, stdout="2", stderr="")
        raise self._rec.refuse(f"unscripted subprocess: {argv}")

    def popen(self, argv, *args, **kwargs):
        # subprocess.run itself starts a Popen: git passes through.
        if argv and Path(str(argv[0])).name == "git":
            return self._real_popen(argv, *args, **kwargs)
        self._rec.add("subprocess-popen", *[str(a) for a in argv])
        raise self._rec.refuse(f"unscripted subprocess.Popen: {argv}")


class FakeDatagenDeployer:
    """``DatagenDeployer``: the continuous run's datagen Job starts."""

    def __init__(self, rec: Recorder, *args, **kwargs) -> None:
        from lakebench.deploy.datagen import DatagenDeployer

        self._real = DatagenDeployer
        self._rec = rec
        _checked(rec, DatagenDeployer.__init__, *args, **kwargs)

    def deploy(self, *args, **kwargs):
        from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

        _checked(self._rec, self._real.deploy, *args, **kwargs)
        self._rec.add("Datagen", "deploy")
        self._rec.datagen_uid = "uid-datagen-1"
        return DeploymentResult(
            component="datagen", status=DeploymentStatus.SUCCESS, message="datagen started"
        )

    def get_progress(self, *args, **kwargs) -> dict[str, Any]:
        """Batch --generate's progress poll: both pods have finished."""
        _checked(self._rec, self._real.get_progress, *args, **kwargs)
        self._rec.add("Datagen", "get_progress")
        if self._rec.interrupt is not None and self._rec.interrupt[0] == "datagen":
            send_interrupt(self._rec.interrupt[1])
        return {"running": False, "completions": 2, "succeeded": 2}

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted DatagenDeployer.{attr}")


#: The datagen fleet of the continuous record: both pods reported
#: (pipeline_benchmark.config_snapshot.datagen_output_rows and _files).
CONTINUOUS_FLEET = {"rows": 2_478_560, "files": 160}


def _fake_collect_from_k8s(rec: Recorder):
    from lakebench.metrics import datagen_aggregator

    original = datagen_aggregator.collect_from_k8s
    real = _unbound(original)

    def collect_from_k8s(*args, **kwargs):
        _checked(rec, real, *args, **kwargs)
        bound = inspect.signature(original).bind(*args, **kwargs)
        bound.apply_defaults()
        rec.add("Datagen", "collect_from_k8s", bound.arguments["namespace"])
        return SimpleNamespace(
            data_quality="complete",
            total_rows_written=CONTINUOUS_FLEET["rows"],
            total_files_written=CONTINUOUS_FLEET["files"],
        )

    return collect_from_k8s


def _passing_prerequisites(rec: Recorder):
    from lakebench.cli._prerequisites import PrereqReport, PrereqResult

    def run_prerequisites(cfg, *, sustained=None, datagen_runs=True):
        rec.add("prerequisites", "run", {"sustained": sustained, "datagen_runs": datagen_runs})
        return PrereqReport(checks=[PrereqResult(name="harness", passed=True, message="faked")])

    return run_prerequisites


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def base_config(**overrides: Any) -> dict[str, Any]:
    """The scenario config: hive-iceberg-spark-trino Customer 360 at scale 1,
    the corpus window of the fixture logs (2024)."""
    cfg: dict[str, Any] = {
        "name": NAME,
        "recipe": "hive-iceberg-spark-trino",
        "platform": {
            "kubernetes": {"namespace": NAME},
            "storage": {
                "s3": {
                    "endpoint": f"http://{PLACEHOLDER_HOST}:80",
                    "access_key": "${LAKEBENCH_S3_ACCESS_KEY}",
                    "secret_key": "${LAKEBENCH_S3_SECRET_KEY}",
                },
                "scratch": {"enabled": True, "storage_class": "px-csi-scratch"},
            },
        },
        "architecture": {"pipeline": {"mode": "batch"}},
        "workload": {
            "schema": "customer360",
            "datagen": {
                "scale": 1,
                "file_size": "64mb",
                "timestamp_start": "2024-01-01",
                "timestamp_end": "2025-01-01",
            },
        },
    }
    _deep_update(cfg, overrides)
    return cfg


def _deep_update(base: dict, extra: dict) -> None:
    for k, v in extra.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v


@dataclass
class Scenario:
    name: str
    argv: list[str]
    config: dict[str, Any]
    #: Fixture log directory under tests/fixtures/run_char/.
    logs: str
    #: Stages whose Spark application fails.
    failing: tuple[str, ...] = ()
    #: Continuous: the fake clock's start (host epoch seconds), which places
    #: the window where the record's was so the fixture logs line up with it.
    clock_start: float | None = None
    #: FakeTrino's table health answers.
    trino: dict[str, Any] = field(default_factory=dict)
    #: Failure scenarios: see the module docstring.
    interrupt: tuple[str, str] | None = None
    submit_interrupt: str | None = None
    lease_signal: str | None = None
    #: (window second, "SIGINT" | "SIGTERM" | "namespace_gone" |
    #: "namespace_redeployed" | "namespace_terminating" |
    #: "namespace_unreadable" | "namespace_blip" (two failed reads)).
    events: tuple[tuple[float, str], ...] = ()
    #: The continuous datagen Job is still running when the streams start.
    datagen_running: bool = False
    interrupt_after_state: str | None = None
    foreign: tuple[str, ...] = ()
    interrupt_delete_after: int | None = None


#: The continuous record's window opened at 15:05:56.504 cluster time
#: (2026-10-01, UTC), 22.9 s ahead of the host: the host clock read
#: 15:05:33.604. Nothing before the window advances the fake clock.
_CONTINUOUS_START = datetime(2026, 10, 1, 15, 5, 33, 604000, tzinfo=timezone.utc).timestamp()


def _continuous_fingerprints() -> dict[str, Any]:
    path = FIXTURES / "continuous_c360" / "fingerprints.json"
    return json.loads(path.read_text()) if path.exists() else {}


#: The result check's fingerprints in the continuous record (its
#: continuous.result_check.fingerprints), keyed by query name.
CONTINUOUS_FINGERPRINTS = _continuous_fingerprints()


SCENARIOS = {
    "batch_c360": Scenario(
        name="batch_c360",
        argv=["--skip-generate", "--yes"],
        config=base_config(),
        logs="batch_c360",
    ),
    # A failed stage: run stops before the next stage, saves the record,
    # and exits non-zero.
    "batch_c360_silver_fails": Scenario(
        name="batch_c360_silver_fails",
        argv=["--skip-generate", "--yes"],
        config=base_config(),
        logs="batch_c360",
        failing=("silver-build",),
    ),
    # The continuous record run-20261001-090400-db3ffe (SD-19 live, lane
    # tree 189fa3d's code): a fresh deployment, datagen started by the run,
    # a 1800 s window, the default maintenance schedule (retention every
    # 600 s, compaction every 1200 s). Its stream driver logs are the
    # fixtures; the clock replays the record's round and statement times.
    "continuous_c360": Scenario(
        name="continuous_c360",
        argv=["--timeout", "1200", "--yes"],
        config=base_config(
            architecture={"pipeline": {"mode": "continuous", "continuous": {"run_duration": 1800}}}
        ),
        logs="continuous_c360",
        clock_start=_CONTINUOUS_START,
        # Table health per round in the record (round_meta.table_health).
        trino={
            "silver_files": (1830, 4758, 7320),
            "gold_files": 1,
            "snapshots": ((5, 1), (13, 2), (20, 4)),
        },
    ),
}
#: AML batch on hive-iceberg-spark-trino, scale 1, seed 43, as the record
#: run-20261001-114528-37810a ran it (its config, without images and
#: storage classes, which no fake reads).
_AML_TABLES = {
    "bronze": "default.pacs008_raw",
    "silver": "silver.transactions",
    "silver_entities": "silver.entities",
    "silver_accounts": "silver.accounts",
    "silver_counterparty_edges": "silver.counterparty_edges",
    "gold": "gold.daily_dashboards",
    "gold_alerts": "gold.alerts",
    "gold_risk_scores": "gold.risk_scores",
    "gold_entity_clusters": "gold.entity_clusters",
    "gold_daily_dashboards": "gold.daily_dashboards",
}
SCENARIOS["batch_aml"] = Scenario(
    name="batch_aml",
    argv=["--skip-generate", "--timeout", "1200", "--yes"],
    config=base_config(
        architecture={
            "pipeline": {
                "mode": "batch",
                "medallion": {"bronze": {"format": "parquet", "path_template": "pacs008"}},
            },
            "tables": _AML_TABLES,
        },
        workload={
            "schema": "financial",
            "retention_workload": True,
            "retention_months": 60,
            "datagen": {"scale": 1, "seed": 43},
        },
    ),
    logs="batch_aml",
    # The record's silver and gold data files before and after compaction
    # total 65 and 61 (scores.pre/post_compaction_file_count); the split is
    # the harness's: one gold file, the rest silver.
    trino={"silver_files": (64, 60), "gold_files": 1},
)

# The same run with --skip-generate: datagen is not deployed and the raw
# corpus is neither listed nor cleared; everything after is the record's.
SCENARIOS["continuous_c360_skip_generate"] = Scenario(
    name="continuous_c360_skip_generate",
    argv=["--skip-generate", "--timeout", "1200", "--yes"],
    config=SCENARIOS["continuous_c360"].config,
    logs="continuous_c360",
    clock_start=_CONTINUOUS_START,
    trino=SCENARIOS["continuous_c360"].trino,
)


#: Bucket bytes of each scenario's record, where it is not batch C360.
_SCENARIO_SIZES = {
    "continuous_c360": FakeS3.CONTINUOUS_SIZES,
    "continuous_c360_skip_generate": FakeS3.CONTINUOUS_SIZES,
    # run-20261001-114528-37810a: 8.4901, 9.0947 and 0.6576 GiB.
    "batch_aml": {"bronze": 9_116_144_816, "silver": 9_765_373_653, "gold": 706_137_988},
}


def install_fakes(monkeypatch, rec: Recorder, scenario: Scenario) -> None:
    """Replace every seam ``lakebench run`` reaches (module docstring)."""
    rec.sizes = _SCENARIO_SIZES.get(scenario.name)
    rec.interrupt = scenario.interrupt
    rec.submit_interrupt = scenario.submit_interrupt
    rec.lease_signal = scenario.lease_signal
    rec.foreign = set(scenario.foreign)
    rec.interrupt_delete_after = scenario.interrupt_delete_after
    rec.fresh_bronze = "--generate" in scenario.argv
    rec.datagen_running = scenario.datagen_running
    rec.interrupt_after_state = scenario.interrupt_after_state
    recall = FIXTURES / scenario.logs / "recall.json"
    if recall.exists():
        # The score stage's sidecar, at the key the run reads.
        rec.objects[(f"{NAME}-gold", "scoring/<run_id>/recall.json")] = recall.read_bytes()
    import kubernetes.client
    import kubernetes.config

    import lakebench.benchmark
    import lakebench.benchmark.executor
    import lakebench.benchmark.settle
    import lakebench.cli
    import lakebench.cli._helpers
    import lakebench.cli._prerequisites
    import lakebench.cli._sustained
    import lakebench.engine
    import lakebench.k8s
    import lakebench.s3
    import lakebench.spark

    trino = FakeTrino(rec, **scenario.trino)

    def k8s_factory(context: str = "", namespace: str = "") -> RecordingK8s:
        return RecordingK8s(rec, trino, context=context, namespace=namespace)

    monkeypatch.setattr(lakebench.k8s, "get_k8s_client", k8s_factory)
    monkeypatch.setattr(lakebench.cli, "get_k8s_client", k8s_factory)
    # _sustained imports it by name at module level.
    monkeypatch.setattr(lakebench.cli._sustained, "get_k8s_client", k8s_factory)
    for api, fake in (
        ("CoreV1Api", FakeCoreV1Api),
        ("AppsV1Api", FakeAppsV1Api),
        ("CustomObjectsApi", FakeCustomObjectsApi),
        ("BatchV1Api", FakeBatchV1Api),
        ("ApisApi", FakeApisApi),
        ("VersionApi", FakeVersionApi),
    ):
        monkeypatch.setattr(kubernetes.client, api, lambda *a, _f=fake, **k: _f(rec, *a, **k))
    # Every other API class goes through ApiClient.call_api.
    monkeypatch.setattr(kubernetes.client.ApiClient, "call_api", _refuse_call_api(rec))

    def no_kubeconfig(*a, **k):
        raise rec.refuse("a real kubeconfig load")

    for loader in ("load_kube_config", "load_incluster_config"):
        monkeypatch.setattr(kubernetes.config, loader, no_kubeconfig)

    log_dir = FIXTURES / scenario.logs
    monkeypatch.setattr(
        lakebench.spark, "SparkOperatorManager", lambda *a, **k: FakeOperator(rec, *a, **k)
    )
    monkeypatch.setattr(lakebench.engine, "get_engine", lambda cfg, k8s: FakeJobManager(rec))
    monkeypatch.setattr(
        lakebench.spark,
        "SparkJobMonitor",
        lambda *a, **k: FakeMonitor(rec, log_dir, scenario.failing, *a, **k),
    )
    monkeypatch.setattr(lakebench.s3, "S3Client", lambda *a, **k: FakeS3(rec, *a, **k))
    monkeypatch.setattr(
        lakebench.benchmark, "BenchmarkRunner", lambda *a, **k: FakeBenchmark(rec, *a, **k)
    )

    def get_executor(*args, **kwargs) -> FakeExecutor:
        _checked(rec, _unbound(lakebench.benchmark.executor.get_executor), *args, **kwargs)
        return FakeExecutor(rec)

    monkeypatch.setattr(lakebench.benchmark.executor, "get_executor", get_executor)
    monkeypatch.setattr(
        lakebench.cli._prerequisites, "run_prerequisites", _passing_prerequisites(rec)
    )

    fake_sub = FakeSubprocess(rec, subprocess.run, subprocess.Popen)
    monkeypatch.setattr(subprocess, "run", fake_sub.run)
    monkeypatch.setattr(subprocess, "Popen", fake_sub.popen)

    # The settle wait sleeps between probes: advance a fake clock instead.
    advanced = [0.0]

    def clock() -> float:
        return time.monotonic() + advanced[0]

    def sleep(seconds: float) -> None:
        advanced[0] += max(0.0, float(seconds))

    kwdefaults = lakebench.benchmark.settle.wait_for_settle.__kwdefaults__
    monkeypatch.setitem(kwdefaults, "clock", clock)
    monkeypatch.setitem(kwdefaults, "sleep", sleep)

    # A fresh journal in the scenario's working directory.
    monkeypatch.setattr(lakebench.cli._helpers, "_journal", None)

    import lakebench.deploy

    monkeypatch.setattr(
        lakebench.deploy, "DatagenDeployer", lambda *a, **k: FakeDatagenDeployer(rec, *a, **k)
    )

    if scenario.clock_start is not None:
        _install_continuous(monkeypatch, rec, scenario)


def _install_continuous(monkeypatch, rec: Recorder, scenario: Scenario) -> None:
    """The continuous runner's extra seams: its clock, the datagen Job and
    fleet, and the kubeconfig's cluster fingerprint (read by the ownership
    check before a continuous reset)."""
    import lakebench.cli._sustained
    import lakebench.deploy
    import lakebench.deploy.ownership
    import lakebench.metrics.datagen_aggregator

    assert scenario.clock_start is not None
    clock = FakeClock(scenario.clock_start)
    rec.clock = clock
    namespace_events: dict[str, Callable[[], None]] = {
        "namespace_gone": lambda: setattr(rec, "namespace_present", False),
        "namespace_redeployed": lambda: setattr(rec, "namespace_uid", "ns-uid-runchar-2"),
        "namespace_terminating": lambda: setattr(rec, "namespace_phase", "Terminating"),
        "namespace_unreadable": lambda: setattr(rec, "namespace_errors", -1),
        "namespace_blip": lambda: setattr(rec, "namespace_errors", 2),
    }
    for when, what in scenario.events:
        if what in namespace_events:
            clock.at(when, namespace_events[what])
        else:
            clock.at(when, lambda how=what: send_interrupt(how))
    monkeypatch.setattr(lakebench.cli._sustained, "time", clock.time_module())
    monkeypatch.setattr(lakebench.cli._sustained, "datetime", clock.datetime_class())
    # The run record's start and end (and with them total_elapsed_seconds,
    # which the freshness verdict divides by) are stamped from utc_now.
    import lakebench.metrics.collector

    def utc_now() -> datetime:
        return datetime.fromtimestamp(clock.t, timezone.utc)

    monkeypatch.setattr(lakebench.cli._sustained, "utc_now", utc_now)
    monkeypatch.setattr(lakebench.metrics.collector, "utc_now", utc_now)
    # MaintenanceBudget imports time inside __init__ (the module patch above
    # misses it): its deadline and per-statement timeouts run on the fake
    # clock too, so they shrink as the fake statements take their time and
    # never depend on how fast this host is.
    budget_cls = lakebench.cli._sustained.MaintenanceBudget
    budget_init = budget_cls.__init__

    def clocked_init(self, *args, **kwargs):
        import time as real_time

        before = real_time.monotonic()
        budget_init(self, *args, **kwargs)
        # Keep whatever deadline the real __init__ set, moved onto the fake
        # clock: a later change to its formula still shows in the trace.
        offset = self.deadline - before
        self._clock = clock.monotonic
        self.deadline = self._clock() + offset

    monkeypatch.setattr(budget_cls, "__init__", clocked_init)
    monkeypatch.setattr(
        lakebench.metrics.datagen_aggregator, "collect_from_k8s", _fake_collect_from_k8s(rec)
    )
    real_fp = _unbound(lakebench.deploy.ownership.api_server_fingerprint)

    def api_server_fingerprint(*args, **kwargs):
        _checked(rec, real_fp, *args, **kwargs)
        rec.add("ownership", "api_server_fingerprint")
        return API_SERVER_FP

    monkeypatch.setattr(
        lakebench.deploy.ownership, "api_server_fingerprint", api_server_fingerprint
    )


def run_scenario(name: str, tmp_path: Path, monkeypatch) -> dict[str, Any]:
    """Run ``lakebench run`` for scenario *name* in *tmp_path*; return its trace."""
    return run_scenario_full(SCENARIOS[name], tmp_path, monkeypatch)[0]


def saved_record(workdir: Path) -> dict[str, Any]:
    """The metrics.json the run saved under *workdir* ({} when none)."""
    runs = sorted((workdir / "lakebench-output" / "runs").glob("run-*/metrics.json"))
    return json.loads(runs[-1].read_text()) if runs else {}


def run_scenario_full(
    scenario: Scenario, tmp_path: Path, monkeypatch
) -> tuple[dict[str, Any], Recorder]:
    """Run ``lakebench run`` for *scenario*; return its trace and the
    recorder (the fake cluster's state after the run)."""
    result, rec = invoke_scenario(scenario, tmp_path, monkeypatch)
    if result.exception is not None and not isinstance(result.exception, SystemExit):
        raise result.exception
    return build_trace(result.exit_code, rec, tmp_path, os.environ.get("LB_RUN_ID")), rec


def invoke_scenario(scenario: Scenario, tmp_path: Path, monkeypatch) -> tuple[Any, Recorder]:
    """Run ``lakebench run`` for *scenario*; return the ``CliRunner`` result
    and the recorder."""
    from typer.testing import CliRunner

    from lakebench.cli import app

    rec = Recorder()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "harness-access")
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", "harness-secret")
    # run() exports LB_RUN_ID for its child processes, and the trace records
    # it. Unset at the start (so an export run() stops making shows), and
    # set then deleted through monkeypatch so its undo removes run()'s value
    # and nothing leaks into the rest of the test session.
    monkeypatch.setenv("LB_RUN_ID", "unset")
    monkeypatch.delenv("LB_RUN_ID")
    install_fakes(monkeypatch, rec, scenario)

    cfg_path = tmp_path / f"{NAME}.yaml"
    cfg_path.write_text(yaml.safe_dump(scenario.config, sort_keys=False))
    return CliRunner().invoke(app, ["run", str(cfg_path), *scenario.argv]), rec


#: Maintenance outcome fields the trace keeps (counts and skips, not timings).
_OUTCOME_KEYS = (
    "kind",
    "total",
    "succeeded",
    "failed",
    "timed_out",
    "not_attempted",
    "skipped",
    "user_skip",
    "files_before",
    "files_after",
    "note",
)

#: Published job fields that do not depend on the wall clock.
_JOB_VALUES = (
    "job_type",
    "success",
    "error_message",
    "timing_source",
    "executor_count",
    "executor_cores",
    "executor_memory_gb",
    "memory_gb_requested",
    "input_rows",
    "output_rows",
    "input_size_gb",
    "output_size_gb",
)


#: Pipeline scores that do not depend on the wall clock (stage and
#: maintenance durations do, so time to value and the elapsed totals are left
#: out).
_DETERMINISTIC_SCORES = (
    "composite_qph",
    "scale_ratio",
    "total_data_processed_gb",
    "benchmark_samples_per_query",
    "qph_spread",
    "pre_compaction_file_count",
    "post_compaction_file_count",
    "compaction_ratio",
    "pre_compaction_qph",
    "post_compaction_qph",
    "maintenance_value_pct",
    "maintenance_paired_queries",
    "maintenance_value_reason",
    "maintenance_settled",
    "maintenance_settle_capped",
    "maintenance_settle_verified",
)


def _rounded(value: Any) -> Any:
    return round(value, 6) if isinstance(value, float) else value


def build_trace(
    exit_code: int, rec: Recorder, workdir: Path, exported_run_id: str | None
) -> dict[str, Any]:
    out_dir = workdir / "lakebench-output"
    runs = sorted((out_dir / "runs").glob("run-*/metrics.json"))
    metrics: dict[str, Any] = json.loads(runs[-1].read_text()) if runs else {}
    events: list[list[Any]] = []
    for path in sorted((out_dir / "journal").glob("session-*.jsonl")):
        for line in path.read_text().splitlines():
            ev = json.loads(line)
            details = ev.get("details") or {}
            events.append([ev.get("event_type"), ev.get("success"), details.get("stage")])
    pb = metrics.get("pipeline_benchmark") or {}
    experiment = metrics.get("experiment") or {}
    benchmark = metrics.get("benchmark") or {}
    effective = experiment.get("effective_maintenance") or {}
    trace = {
        "exit_code": exit_code,
        "unscripted": rec.unscripted,
        "runs_saved": len(runs),
        "exported_run_id": exported_run_id,
        "calls": rec.calls,
        "submits": rec.submits,
        "journal_events": events,
        "metrics_keys": {
            "top": sorted(metrics),
            "pipeline_benchmark": sorted(pb),
            "jobs[0]": sorted((metrics.get("jobs") or [{}])[0]),
            "experiment": sorted(experiment),
        },
        "jobs": [{k: _rounded(j.get(k)) for k in _JOB_VALUES} for j in metrics.get("jobs") or []],
        "maintenance_outcomes": [
            {k: o[k] for k in sorted(o) if k in _OUTCOME_KEYS}
            for o in metrics.get("maintenance_outcomes") or []
        ],
        "values": {
            "maintenance_policy_id": metrics.get("maintenance_policy_id"),
            "effective_maintenance_id": effective.get("id"),
            "effective_maintenance_detail_id": effective.get("detail_id"),
            "bucket_gb": [
                _rounded(metrics.get(k))
                for k in ("bronze_size_gb", "silver_size_gb", "gold_size_gb")
            ],
            "benchmark_qph": _rounded(benchmark.get("qph")),
            "benchmark_queries": [
                [q.get("name"), q.get("rows_returned"), q.get("success")]
                for q in benchmark.get("queries") or []
            ],
            "scores": {k: _rounded((pb.get("scores") or {}).get(k)) for k in _DETERMINISTIC_SCORES},
        },
        "c360_correctness": (metrics.get("c360_correctness") or {}).get("status"),
        "success": metrics.get("success"),
        "verdict": (metrics.get("verdict") or {}).get("status"),
        "verdict_reasons": (metrics.get("verdict") or {}).get("reasons"),
    }
    if metrics.get("continuous") is not None:
        trace.update(continuous_sections(metrics))
    if metrics.get("financial_scoring") is not None or metrics.get("tm_operations") is not None:
        trace["aml"] = aml_section(metrics)
    return normalise(trace, workdir)


def aml_section(metrics: dict[str, Any]) -> dict[str, Any]:
    """What an AML run publishes about detection: per-rule alerts, skips and
    errors from gold-finalize, the whole folded-in scoring record (recall
    per typology, false-positive rates, precision, chance and the control
    floor), and the whole TM operations record (verdict, invariants, ops)."""
    gold = [j for j in metrics.get("jobs") or [] if j.get("job_type") == "gold-finalize"]
    return {
        "gold_alerts_by_rule": [j.get("alerts_by_rule") for j in gold],
        "gold_rules_skipped": [j.get("rules_skipped") for j in gold],
        "gold_rule_errors": [j.get("rule_errors") for j in gold],
        "financial_scoring": metrics.get("financial_scoring"),
        "tm_operations": metrics.get("tm_operations"),
    }


#: Per-stream published fields that do not depend on the wall clock.
_STREAM_VALUES = (
    "job_type",
    "success",
    "requested_executors",
    "total_batches",
    "total_rows_processed",
    "window_input_rows",
    "pre_window_input_rows",
    "window_commits",
    "window_new_data_cycles",
    "micro_batch_duration_ms",
    "freshness_seconds",
    "throughput_rps",
    "submission_failures",
)

#: Continuous pipeline scores that come from the logs, the fake clock and
#: the fixed fleet, not from this host's wall clock.
_CONTINUOUS_SCORES = (
    "data_freshness_seconds",
    "sustained_throughput_rps",
    "stage_latency_profile",
    "composite_qph",
    "composite_qph_rounds",
    "pipeline_saturated",
    "corpus_drained",
    "intake_limit",
    "corpus_ingest_ratio",
    "released_rows",
    "window_seconds",
    "arrival_seconds",
    "window_arrival_fraction",
    "pre_window_rows",
    "total_s3_objects",
    "benchmark_rounds_count",
    "in_stream_composite_qph",
)


def continuous_sections(metrics: dict[str, Any]) -> dict[str, Any]:
    """The trace sections only a continuous run has: what its streams did
    in the window, its gate, settle and result check, and its rounds."""
    cont = metrics.get("continuous") or {}
    window = cont.get("window") or {}
    check = cont.get("result_check") or {}
    pb = metrics.get("pipeline_benchmark") or {}
    return {
        "continuous": {
            "window_seconds": _rounded(window.get("seconds")),
            "cluster_clock_offset_seconds": _rounded(window.get("cluster_clock_offset_seconds")),
            "gate_problems": cont.get("gate_problems"),
            "trickle": cont.get("trickle"),
            "retention": cont.get("retention"),
            "settle": cont.get("settle"),
            "result_check": {
                "query_set_id": check.get("query_set_id"),
                "failed": check.get("failed"),
                "not_checked": check.get("not_checked"),
            },
            "streams": {
                name: {k: v for k, v in sorted(st.items()) if k != "running_at"}
                for name, st in sorted((cont.get("streams") or {}).items())
            },
        },
        "streaming": [
            {k: _rounded(st.get(k)) for k in _STREAM_VALUES}
            for st in metrics.get("streaming") or []
        ],
        "benchmark_rounds": [
            [
                _rounded(r.get("qph")),
                [[q.get("name"), q.get("rows_returned"), q.get("success")] for q in r["queries"]],
                (r.get("round_meta") or {}).get("table_health"),
                (r.get("round_meta") or {}).get("gold_event_age_seconds"),
            ]
            for r in metrics.get("benchmark_rounds") or []
        ],
        "continuous_scores": {
            k: _rounded((pb.get("scores") or {}).get(k)) for k in _CONTINUOUS_SCORES
        },
    }


_RUN_ID = re.compile(r"\d{8}-\d{6}-[0-9a-f]{6}")
_ISO_TS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2}|Z)?")


def normalise(value: Any, workdir: Path) -> Any:
    """Run ids, timestamps and paths replaced, so two runs compare equal."""
    if isinstance(value, dict):
        return {k: normalise(v, workdir) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalise(v, workdir) for v in value]
    if isinstance(value, str):
        value = value.replace(str(workdir), "<tmp>")
        value = _RUN_ID.sub("<run_id>", value)
        return _ISO_TS.sub("<ts>", value)
    return value


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


class TraceMismatch(AssertionError):
    pass


def load_golden(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{name}.json").read_text())


def assert_trace_equal(actual: dict[str, Any], golden: dict[str, Any]) -> None:
    """Fail on the first section that differs, naming its first differing entry."""
    for section in golden:
        if section.startswith("_"):
            continue  # provenance notes, not trace
        if section not in actual:
            raise TraceMismatch(f"trace has no section {section!r}")
        got, want = actual[section], golden[section]
        if got == want:
            continue
        if isinstance(want, list) and isinstance(got, list):
            for n, (g, w) in enumerate(zip(got, want, strict=False)):
                if g != w:
                    raise TraceMismatch(f"{section}[{n}]: got {g!r}, golden {w!r}")
            raise TraceMismatch(
                f"{section}: got {len(got)} entries, golden {len(want)}; first extra: "
                f"{(got[len(want)] if len(got) > len(want) else want[len(got)])!r}"
            )
        if isinstance(want, dict) and isinstance(got, dict):
            for key in sorted(set(want) | set(got)):
                if got.get(key) != want.get(key):
                    raise TraceMismatch(
                        f"{section}.{key}: got {got.get(key)!r}, golden {want.get(key)!r}"
                    )
        raise TraceMismatch(f"{section}: got {got!r}, golden {want!r}")
    extra = sorted(set(actual) - {s for s in golden if not s.startswith("_")})
    if extra:
        raise TraceMismatch(f"trace sections not in the golden: {extra}")


def main(argv: list[str] | None = None) -> int:
    """``python -m tests.harness.run_harness scrub SRC DEST --source-name NAME``
    writes a fixture driver log through :func:`scrub_driver_log`."""
    import argparse

    p = argparse.ArgumentParser(prog="run_harness")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scrub")
    s.add_argument("src", type=Path)
    s.add_argument("dest", type=Path)
    s.add_argument("--source-name", required=True)
    args = p.parse_args(argv)
    text = scrub_driver_log(args.src.read_text(errors="replace"), args.source_name)
    problems = fixture_problems(text)
    if problems:
        print(f"refused: {problems}")
        return 1
    args.dest.write_text(text)
    print(f"{args.dest}: {len(text.splitlines())} lines")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
