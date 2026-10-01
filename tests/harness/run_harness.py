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
  ``BatchV1Api`` and ``VersionApi``: recording fakes that script reads only.
  Every other API class reaches ``ApiClient.call_api``, which refuses. Batch
  ``run`` mutates nothing through the API today, so any create, patch or
  delete is unscripted and fails the trace (SD-9's ``recording_k8s`` fixture
  is the general SAF-4 oracle, with the namespace rule).
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

Every fake method is called through :func:`_checked`, which binds the call
against the real method's signature: a call the real seam would reject with a
``TypeError`` is unscripted here too. A call no fake scripts raises
``Unscripted`` (a ``NotImplementedError``) and is recorded in the trace's
``unscripted`` list even when the code under test catches it, so a new seam is
never silent; every golden expects that list empty.

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
#: The documented placeholder S3 host (CLAUDE.md section 8).
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
    """Why *text* may not be a committed fixture: an address other than the
    placeholder, an endpoint, a bucket not of deployment *name*, or a
    credential-looking value (anything but a ``${VAR}`` placeholder)."""
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
    for bucket in sorted(set(_BUCKET_URL.findall(text))):
        if not bucket.startswith(f"{name}-"):
            problems.append(f"bucket {bucket}")
    return problems


# ---------------------------------------------------------------------------
# Recording fakes
# ---------------------------------------------------------------------------


@dataclass
class Recorder:
    """Every seam call, in order."""

    calls: list[list[Any]] = field(default_factory=list)
    submits: list[list[Any]] = field(default_factory=list)
    #: Every unscripted call, even one the code under test caught and
    #: swallowed: the trace carries the list, and the golden expects none.
    unscripted: list[str] = field(default_factory=list)
    namespace: str = NAME

    def add(self, *entry: Any) -> None:
        self.calls.append(list(entry))

    def refuse(self, message: str) -> Unscripted:
        """Record an unscripted call; the caller raises the result."""
        self.unscripted.append(message)
        return Unscripted(message)


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
    which changed none (a no-op, recorded with a note).
    """

    def __init__(self, rec: Recorder, silver_files: int = 366, gold_files: int = 1) -> None:
        self._rec = rec
        self.silver_files = silver_files
        self.gold_files = gold_files

    def answer(self, sql: str) -> tuple[int, str, str]:
        low = sql.lower()
        if "execute optimize" in low:
            return 0, "", ""
        if "execute expire_snapshots" in low or "execute remove_orphan_files" in low:
            return 0, "", ""
        if low.startswith("select count(*) from") and '$files"' in low:
            files = self.silver_files if ".silver." in low else self.gold_files
            return 0, f'"{files}"\n', ""
        if low.startswith("select count(*) from") and '$snapshots"' in low:
            return 0, '"3"\n', ""
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
        return name == self._rec.namespace

    def exec_in_pod(self, name, command, namespace=None, container=None, timeout=30):
        _checked(self._rec, self._real.exec_in_pod, name, command, namespace, container, timeout)
        ns = namespace or self.namespace
        self._rec.add("exec", name, ns, container, timeout, " ".join(command[:2]), command[-1])
        if name != _TRINO_POD or command[:2] != ["trino", "--execute"]:
            raise self._rec.refuse(f"unscripted exec in {name}: {command}")
        return self._trino.answer(command[-1])

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted K8sClient.{attr}")


def _api_exception(status: int):
    from kubernetes.client.rest import ApiException

    return ApiException(status=status, reason="Not Found" if status == 404 else "Error")


class _FakeApi:
    """Base for the kubernetes.client API fakes: records, refuses the unknown."""

    def __init__(self, rec: Recorder, *args, **kwargs) -> None:
        self._rec = rec

    def __getattr__(self, attr: str):
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise self._rec.refuse(f"unscripted {type(self).__name__}.{attr}")


class FakeCoreV1Api(_FakeApi):
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
    def get_namespaced_custom_object(self, group, version, namespace, plural, name, **kw):
        self._rec.add("CustomObjectsApi", "get", plural, name, namespace)
        # No SparkApplication is left over from an earlier run.
        raise _api_exception(404)


class FakeBatchV1Api(_FakeApi):
    pass


class FakeVersionApi(_FakeApi):
    def get_code_with_http_info(self, **kw):
        """The API server's HTTP Date header: the cluster clock is the host's."""
        self._rec.add("VersionApi", "get_code")
        date = email.utils.format_datetime(datetime.now(timezone.utc), usegmt=True)
        return None, 200, {"Date": date}


def _refuse_call_api(rec: Recorder):
    def call_api(self, resource_path, method, *args, **kwargs):
        raise rec.refuse(f"unscripted Kubernetes API call: {method} {resource_path}")

    return call_api


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


class FakeJobManager:
    """``SparkJobManager`` stand-in: scripts deploy, submissions succeed."""

    def __init__(self, rec: Recorder) -> None:
        from lakebench.modules.pipeline_engines.spark.job import SparkJobManager

        self._real = SparkJobManager
        self._rec = rec

    def engine_name(self) -> str:
        return "spark"

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
        self._rec.submits.append(
            [job_type.value, sorted(env), {k: env.get(k) for k in TRACED_ENV_VALUES}]
        )
        return JobStatus(
            name=f"lakebench-{job_type.value}", state=JobState.SUBMITTED, message="submitted"
        )

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


class FakeS3:
    """``S3Client`` stand-in with fixed bucket sizes; its constructor
    arguments are recorded (endpoint and keys come from the config)."""

    #: Bytes per bucket, from run-20260927-084902-fc1eb5 (the run the batch
    #: C360 fixture logs come from): bronze 9.917 GiB, silver 9.762 GiB,
    #: gold 64.5 KiB.
    SIZES = {"bronze": 10_648_840_000, "silver": 10_482_310_000, "gold": 69_274}

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
        if layer not in self.SIZES:
            raise self._rec.refuse(f"unscripted bucket {bucket_name}")
        return BucketInfo(
            name=bucket_name, exists=True, object_count=100, size_bytes=self.SIZES[layer]
        )

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
#: Seconds every fake query takes.
_QUERY_SECONDS = 2.0


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

    def _queries(self):
        from lakebench.benchmark.queries import get_benchmark_queries

        queries = get_benchmark_queries(self.config.architecture.workload.schema_type)
        if not self.config.architecture.workload.tm_operations.enabled or not self.tm_run_id:
            queries = [q for q in queries if q.query_class != "investigator"]
        return queries

    def _result(self, query, iterations: int = 1):
        from lakebench.benchmark.runner import QueryResult

        prefix = query.name.split("_", 1)[0]
        return QueryResult(
            query=query,
            elapsed_seconds=_QUERY_SECONDS,
            rows_returned=C360_ROWS.get(prefix, 1),
            success=True,
            samples=[_QUERY_SECONDS] * iterations,
        )

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
        queries = self._queries()
        results = []
        for n, q in enumerate(queries, 1):
            if progress is not None:
                progress(n, len(queries), q.name, "start")
            results.append(self._result(q, a["iterations"]))
            if progress is not None:
                progress(n, len(queries), q.name, "done", elapsed=_QUERY_SECONDS, success=True)
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
}


def install_fakes(monkeypatch, rec: Recorder, scenario: Scenario) -> None:
    """Replace every seam ``lakebench run`` reaches (module docstring)."""
    import kubernetes.client
    import kubernetes.config

    import lakebench.benchmark
    import lakebench.benchmark.executor
    import lakebench.benchmark.settle
    import lakebench.cli
    import lakebench.cli._helpers
    import lakebench.cli._prerequisites
    import lakebench.engine
    import lakebench.k8s
    import lakebench.s3
    import lakebench.spark

    trino = FakeTrino(rec)

    def k8s_factory(context: str = "", namespace: str = "") -> RecordingK8s:
        return RecordingK8s(rec, trino, context=context, namespace=namespace)

    monkeypatch.setattr(lakebench.k8s, "get_k8s_client", k8s_factory)
    monkeypatch.setattr(lakebench.cli, "get_k8s_client", k8s_factory)
    for api, fake in (
        ("CoreV1Api", FakeCoreV1Api),
        ("AppsV1Api", FakeAppsV1Api),
        ("CustomObjectsApi", FakeCustomObjectsApi),
        ("BatchV1Api", FakeBatchV1Api),
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
    monkeypatch.setattr(
        lakebench.benchmark.executor, "get_executor", lambda cfg, ns=None: FakeExecutor(rec)
    )
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


def run_scenario(name: str, tmp_path: Path, monkeypatch) -> dict[str, Any]:
    """Run ``lakebench run`` for scenario *name* in *tmp_path*; return its trace."""
    from typer.testing import CliRunner

    from lakebench.cli import app

    scenario = SCENARIOS[name]
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
    result = CliRunner().invoke(app, ["run", str(cfg_path), *scenario.argv])
    if result.exception is not None and not isinstance(result.exception, SystemExit):
        raise result.exception
    return build_trace(result.exit_code, rec, tmp_path, os.environ.get("LB_RUN_ID"))


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
    return normalise(trace, workdir)


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
