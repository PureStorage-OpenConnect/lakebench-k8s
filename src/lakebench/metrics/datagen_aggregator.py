"""Datagen per-pod metrics aggregation.

The Rust datagen driver emits one JSON line per pod at completion, prefixed
`LB_METRICS_JSON `. This module reads each pod's logs after the job completes,
extracts and parses those lines, and rolls them up into a single fleet-level
summary that lakebench persists next to the run's metrics.json.

Design notes:

- Each pod runs one Kubernetes container; the metrics line is on stderr but
  goes to the merged container log stream (kubectl logs is stream-agnostic).
- Multiple emit lines per pod are not expected but not fatal: we take the
  last one, matching the "last write wins" semantic that a repeated main()
  would produce.
- Fleet aggregates are computed over the pods that actually emitted a line.
  If a pod finished successfully but somehow did not emit (e.g. an OOM kill
  mid-print), we count it as `missing` but do not fail the aggregation:
  the pipeline still gets partial numbers.
- We do NOT reach into the S3 bucket or measure files here. That is done
  by the existing pipeline size-measurement path. The aggregator is a pure
  log parser.
"""

from __future__ import annotations

import json
import logging
import statistics
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

METRICS_PREFIX = "LB_METRICS_JSON "


@dataclass
class PodMetrics:
    """One pod's LB_METRICS_JSON record, deserialized."""

    node_id: int
    node_count: int
    schema: str
    cores_used: int
    bucket: str
    prefix: str
    file_size_mb: int
    rows_per_file: int
    total_files: int
    files_written: int
    bytes_written: int
    rows_written: int
    elapsed_s: float
    setup_s: float
    gen_s: float
    build_batch_s: float
    encode_parquet_s: float
    s3_put_s: float
    throughput_mbps: float
    cpu_seconds: float
    cpu_hr_per_tb: float | None = None
    # k8s CPU request in millicores, from LB_POD_CPU_REQUEST_MILLI env var
    # set by the K8s Job template. When present, use this over
    # `cores_used` (rayon pool) for CPU-seconds accounting.
    cpu_request_millicores: int | None = None
    world_s: float | None = None
    typology_s: float | None = None
    reference_s: float | None = None
    scale: float | None = None
    corpus_months: int | None = None
    population: int | None = None
    # Corpus-wide constant on the financial path (SAME on every pod).
    # DO NOT sum across pods. Use `rows_written` instead.
    total_txns: int | None = None
    typology_instances: int | None = None
    target_tb: float | None = None
    customer_id_max: int | None = None
    dirty_ratio: float | None = None
    pod_name: str = ""

    def effective_cores(self) -> float:
        """Effective cores this pod occupied k8s for, in cost accounting.

        Prefers `cpu_request_millicores` (what k8s actually reserved) over
        `cores_used` (rayon pool). The two can silently disagree because
        rayon's default reflects `available_parallelism()`, which does
        not read the cgroup CPU quota on Linux.
        """
        if self.cpu_request_millicores and self.cpu_request_millicores > 0:
            return self.cpu_request_millicores / 1000.0
        return float(self.cores_used)

    def recompute_cpu_seconds(self) -> float:
        return self.effective_cores() * self.elapsed_s

    @classmethod
    def from_json_obj(cls, obj: dict[str, Any], pod_name: str = "") -> PodMetrics:
        # Only project known fields; unknown fields are ignored so a future
        # datagen release can add fields without breaking the aggregator.
        allowed = {
            "node_id",
            "node_count",
            "schema",
            "cores_used",
            "cpu_request_millicores",
            "bucket",
            "prefix",
            "file_size_mb",
            "rows_per_file",
            "total_files",
            "files_written",
            "bytes_written",
            "rows_written",
            "elapsed_s",
            "setup_s",
            "gen_s",
            "build_batch_s",
            "encode_parquet_s",
            "s3_put_s",
            "throughput_mbps",
            "cpu_seconds",
            "cpu_hr_per_tb",
            "world_s",
            "typology_s",
            "reference_s",
            "scale",
            "corpus_months",
            "population",
            "total_txns",
            "typology_instances",
            "target_tb",
            "customer_id_max",
            "dirty_ratio",
        }
        kwargs = {k: obj[k] for k in obj if k in allowed}
        # rows_written was added later; older logs won't have it. Fall back
        # to files_written * rows_per_file, which is exact for c360 (every
        # file targets rows_per_file and no short tail) and an approximation
        # for financial (the last file per pod may be short by up to
        # rows_per_file-1 rows).
        if "rows_written" not in kwargs:
            fw = int(kwargs.get("files_written", 0) or 0)
            rpf = int(kwargs.get("rows_per_file", 0) or 0)
            kwargs["rows_written"] = fw * rpf
        # Cast numeric fields defensively; Rust emits with fixed precision but
        # a hand-edited log or a future encoding tweak could hand us strings.
        for f in (
            "node_id",
            "node_count",
            "cores_used",
            "cpu_request_millicores",
            "file_size_mb",
            "rows_per_file",
            "total_files",
            "files_written",
            "bytes_written",
            "rows_written",
            "corpus_months",
            "population",
            "total_txns",
            "typology_instances",
            "customer_id_max",
        ):
            if f in kwargs and kwargs[f] is not None:
                kwargs[f] = int(kwargs[f])
        for f in (
            "elapsed_s",
            "setup_s",
            "gen_s",
            "build_batch_s",
            "encode_parquet_s",
            "s3_put_s",
            "throughput_mbps",
            "cpu_seconds",
            "cpu_hr_per_tb",
            "world_s",
            "typology_s",
            "reference_s",
            "scale",
            "target_tb",
            "dirty_ratio",
        ):
            if f in kwargs and kwargs[f] is not None:
                kwargs[f] = float(kwargs[f])
        return cls(pod_name=pod_name, **kwargs)


@dataclass
class FleetSummary:
    """Fleet-level rollup across all pods that emitted a metrics line."""

    schema: str
    pods_expected: int
    pods_reported: int
    pods_missing: int
    # "complete" -- every expected pod reported; aggregates are trustworthy
    # "partial" -- some pods missing; aggregates computed over what we have
    # "empty" -- no pods reported; all aggregates zero and cpu_hr_per_tb is None
    # "mixed" -- pods disagree on a corpus-defining parameter (see
    #   mixed_params): the pods did not write one corpus
    data_quality: str
    total_bytes_written: int
    total_files_written: int
    # Sum of per-pod rows_written. NEVER derived from total_txns (which is
    # a corpus-wide constant on the financial path).
    total_rows_written: int
    aggregate_mbps: float  # bytes/elapsed_wall_max
    wall_elapsed_max_s: float
    wall_elapsed_min_s: float
    # Sum of per-pod effective_cores() (k8s CPU request when set, else
    # rayon pool size). Not the raw sum of rayon `cores_used`.
    cores_total: float
    cpu_seconds_total: float
    cpu_hr_per_tb: float | None
    phase_pct: dict[str, float]  # build_batch / encode_parquet / s3_put share of CPU
    phase_p50_s: dict[str, float]
    phase_p95_s: dict[str, float]
    worst_pod_elapsed_s: float
    best_pod_elapsed_s: float
    per_pod: list[PodMetrics] = field(default_factory=list)
    # c360 customer id space shared by every pod; None when absent or when
    # the pods disagree (then it is listed in mixed_params).
    customer_id_max: int | None = None
    mixed_params: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "pods_expected": self.pods_expected,
            "pods_reported": self.pods_reported,
            "pods_missing": self.pods_missing,
            "data_quality": self.data_quality,
            "total_bytes_written": self.total_bytes_written,
            "total_files_written": self.total_files_written,
            "total_rows_written": self.total_rows_written,
            "aggregate_mbps": round(self.aggregate_mbps, 3),
            "wall_elapsed_max_s": round(self.wall_elapsed_max_s, 3),
            "wall_elapsed_min_s": round(self.wall_elapsed_min_s, 3),
            "cores_total": round(self.cores_total, 3),
            "cpu_seconds_total": round(self.cpu_seconds_total, 3),
            "cpu_hr_per_tb": (
                round(self.cpu_hr_per_tb, 3) if self.cpu_hr_per_tb is not None else None
            ),
            "phase_pct": {k: round(v, 2) for k, v in self.phase_pct.items()},
            "phase_p50_s": {k: round(v, 3) for k, v in self.phase_p50_s.items()},
            "phase_p95_s": {k: round(v, 3) for k, v in self.phase_p95_s.items()},
            "worst_pod_elapsed_s": round(self.worst_pod_elapsed_s, 3),
            "best_pod_elapsed_s": round(self.best_pod_elapsed_s, 3),
            "customer_id_max": self.customer_id_max,
            "mixed_params": list(self.mixed_params),
            "per_pod": [_pod_to_dict(p) for p in self.per_pod],
        }


def _pod_to_dict(p: PodMetrics) -> dict[str, Any]:
    d = p.__dict__.copy()
    for k, v in list(d.items()):
        if isinstance(v, float):
            d[k] = round(v, 6)
    return d


def parse_metrics_line(log_text: str) -> dict[str, Any] | None:
    """Return the last LB_METRICS_JSON object in `log_text`, or None if absent.

    Tolerant of prefix lines from prior runs of the same container (K8s
    restarts) -- we take the last one, matching last-write-wins.
    """
    last = None
    for line in log_text.splitlines():
        if line.startswith(METRICS_PREFIX):
            payload = line[len(METRICS_PREFIX) :]
            try:
                last = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning("malformed LB_METRICS_JSON line: %s", payload[:120])
    return last


def collect_from_pod_logs(
    pod_logs: dict[str, str],
    expected_pods: int | None = None,
) -> FleetSummary:
    """Parse a mapping of pod_name -> log text into a FleetSummary.

    Pure function so the caller (which owns the K8s client) can also test
    without mocking kubernetes.
    """
    pods: list[PodMetrics] = []
    for pod_name, log_text in pod_logs.items():
        obj = parse_metrics_line(log_text)
        if obj is None:
            logger.info("pod %s emitted no metrics line", pod_name)
            continue
        try:
            pods.append(PodMetrics.from_json_obj(obj, pod_name=pod_name))
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("pod %s metrics line unusable: %s", pod_name, e)

    return _summarize(pods, expected_pods or len(pod_logs))


_CORPUS_PARAMS = ("schema", "customer_id_max", "scale", "target_tb", "dirty_ratio")


def _summarize(pods: list[PodMetrics], pods_expected: int) -> FleetSummary:
    if not pods:
        return FleetSummary(
            schema="",
            pods_expected=pods_expected,
            pods_reported=0,
            pods_missing=pods_expected,
            data_quality="empty",
            total_bytes_written=0,
            total_files_written=0,
            total_rows_written=0,
            aggregate_mbps=0.0,
            wall_elapsed_max_s=0.0,
            wall_elapsed_min_s=0.0,
            cores_total=0.0,
            cpu_seconds_total=0.0,
            cpu_hr_per_tb=None,
            phase_pct={"build_batch": 0.0, "encode_parquet": 0.0, "s3_put": 0.0},
            phase_p50_s={"build_batch": 0.0, "encode_parquet": 0.0, "s3_put": 0.0},
            phase_p95_s={"build_batch": 0.0, "encode_parquet": 0.0, "s3_put": 0.0},
            worst_pod_elapsed_s=0.0,
            best_pod_elapsed_s=0.0,
            per_pod=[],
        )

    total_bytes = sum(p.bytes_written for p in pods)
    total_files = sum(p.files_written for p in pods)
    # rows_written is authoritative per-pod (populated by the datagen
    # atomics from the actual batch.num_rows() written). NEVER use
    # `total_txns`: on the financial path that is a corpus-wide constant
    # that every pod reports identically, so summing it multiplies by N.
    total_rows = sum(p.rows_written for p in pods)

    wall_max = max(p.elapsed_s for p in pods)
    wall_min = min(p.elapsed_s for p in pods)
    # Aggregate MB/s: total bytes / max wall across pods. This is the fleet's
    # true throughput because pods run in parallel; the slowest one gates
    # completion. Sum-of-per-pod-throughputs would overstate.
    # Caveat: if the slowest pod's metrics line went missing, wall_max
    # collapses to the next-slowest and aggregate_mbps overstates. That
    # is why `data_quality` is set to "partial" below; consumers must not
    # publish `aggregate_mbps` from a partial fleet without disclosure.
    aggregate_mbps = total_bytes / wall_max / 1e6 if wall_max > 0 else 0.0

    # Cores + CPU-seconds use each pod's effective_cores() -- k8s request
    # when set, rayon pool otherwise -- rather than the raw `cores_used`
    # or the pre-emitted `cpu_seconds`. This makes the aggregate stay
    # correct even if the emit predates the cpu_request_millicores field
    # and downstream still gets what k8s reserved.
    cores_total = sum(p.effective_cores() for p in pods)
    cpu_seconds_total = sum(p.recompute_cpu_seconds() for p in pods)
    tb = total_bytes / 1e12
    cpu_hr_per_tb = (cpu_seconds_total / 3600.0) / tb if tb > 0 else None

    build = [p.build_batch_s for p in pods]
    encode = [p.encode_parquet_s for p in pods]
    s3put = [p.s3_put_s for p in pods]
    cpu_totals = [b + e for b, e in zip(build, encode, strict=True)]
    cpu_grand = sum(cpu_totals) or 1e-9
    phase_pct = {
        "build_batch": 100.0 * sum(build) / cpu_grand,
        "encode_parquet": 100.0 * sum(encode) / cpu_grand,
        "s3_put": 100.0 * sum(s3put) / cpu_grand,
    }

    def pct(values: list[float], p: float) -> float:
        if not values:
            return 0.0
        sorted_v = sorted(values)
        k = max(0, min(len(sorted_v) - 1, int(round(p * (len(sorted_v) - 1)))))
        return sorted_v[k]

    phase_p50_s = {
        "build_batch": statistics.median(build) if build else 0.0,
        "encode_parquet": statistics.median(encode) if encode else 0.0,
        "s3_put": statistics.median(s3put) if s3put else 0.0,
    }
    phase_p95_s = {
        "build_batch": pct(build, 0.95),
        "encode_parquet": pct(encode, 0.95),
        "s3_put": pct(s3put, 0.95),
    }

    # Parameters that define the corpus must be identical on every pod. A pod
    # that disagrees (a stale image, a hand-run Job) wrote rows from a
    # different id space or size, and the union is not one corpus.
    mixed_params = []
    shared: dict[str, Any] = {}
    for name in _CORPUS_PARAMS:
        values = {getattr(p, name) for p in pods if getattr(p, name) is not None}
        if len(values) > 1:
            mixed_params.append(name)
            logger.warning("datagen pods disagree on %s: %s", name, sorted(values))
        shared[name] = values.pop() if len(values) == 1 else None

    pods_missing = max(0, pods_expected - len(pods))
    if mixed_params:
        data_quality = "mixed"
    else:
        data_quality = "complete" if pods_missing == 0 else "partial"
    return FleetSummary(
        schema=pods[0].schema,
        pods_expected=pods_expected,
        pods_reported=len(pods),
        pods_missing=pods_missing,
        data_quality=data_quality,
        total_bytes_written=total_bytes,
        total_files_written=total_files,
        total_rows_written=total_rows,
        aggregate_mbps=aggregate_mbps,
        wall_elapsed_max_s=wall_max,
        wall_elapsed_min_s=wall_min,
        cores_total=cores_total,
        cpu_seconds_total=cpu_seconds_total,
        cpu_hr_per_tb=cpu_hr_per_tb,
        phase_pct=phase_pct,
        phase_p50_s=phase_p50_s,
        phase_p95_s=phase_p95_s,
        worst_pod_elapsed_s=wall_max,
        best_pod_elapsed_s=wall_min,
        per_pod=sorted(pods, key=lambda x: x.node_id),
        customer_id_max=shared["customer_id_max"],
        mixed_params=mixed_params,
    )


def collect_from_k8s(
    namespace: str,
    label_selector: str = "app=lakebench-datagen",
    job_completions: int | None = None,
) -> FleetSummary:
    """Read pod logs from Kubernetes and return the fleet summary.

    Wraps `collect_from_pod_logs`. Kept separate so unit tests can exercise
    the parser without a live cluster.
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    core_v1 = k8s_client.CoreV1Api()
    try:
        pods = core_v1.list_namespaced_pod(namespace, label_selector=label_selector)
    except ApiException as e:
        logger.warning("could not list datagen pods: %s", e)
        return _summarize([], job_completions or 0)

    pod_logs: dict[str, str] = {}
    for pod in pods.items:
        name = pod.metadata.name
        try:
            text = core_v1.read_namespaced_pod_log(
                name=name,
                namespace=namespace,
                # LB_METRICS_JSON is emitted last, so we only need the tail.
                # 200 was too tight for financial pods under RUST_LOG=debug
                # or a jittery AWS SDK: the metrics line got scrolled off
                # and was reported as "no metrics line" (aggregator
                # undercounts silently). 5000 is generous but a datagen
                # pod's total log is well under this even with warnings.
                tail_lines=5000,
                _preload_content=True,
            )
            pod_logs[name] = text or ""
        except ApiException as e:
            logger.info("could not read log for pod %s: %s", name, e)
            pod_logs[name] = ""

    return collect_from_pod_logs(pod_logs, expected_pods=job_completions or len(pods.items))
