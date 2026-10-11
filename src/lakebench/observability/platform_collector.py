"""Platform metrics collector for Lakebench.

Queries Prometheus ``/api/v1/query_range`` at the end of a benchmark run
to capture infrastructure metrics (CPU, memory, S3 I/O per pod).

These metrics are attached to the benchmark report's platform tab.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PodMetrics:
    """Metrics for a single pod over the benchmark window."""

    pod_name: str
    component: str
    # None: not collected (the query failed or returned no series for this
    # pod), never a measured zero.
    cpu_avg_cores: float | None = None
    cpu_max_cores: float | None = None
    memory_avg_bytes: float | None = None
    memory_max_bytes: float | None = None


class PrometheusQueryError(Exception):
    """Prometheus answered a query with a non-200 status."""


@dataclass
class EngineMetrics:
    """Engine-level Tier 2 metrics from Spark and Trino.

    Populated when Prometheus scrapes engine-native metrics (Spark
    PrometheusServlet sink or Trino JMX exporter). All fields are
    best-effort -- None means the metric was not available.
    """

    # Spark
    spark_gc_seconds_total: float | None = None
    spark_shuffle_read_bytes: float | None = None
    spark_shuffle_write_bytes: float | None = None
    spark_task_count: int | None = None
    # Trino
    trino_running_queries: int | None = None
    trino_completed_queries: int | None = None
    trino_failed_queries: int | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if v is not None:
                d[k] = v
        return d

    @property
    def has_data(self) -> bool:
        return any(v is not None for v in self.__dict__.values())


POD_QUERY_VERSION = 2


@dataclass
class PlatformMetrics:
    """Platform-level metrics collected from Prometheus."""

    start_time: datetime
    end_time: datetime
    pods: list[PodMetrics] = field(default_factory=list)
    # No source emits S3 request metrics yet: None is "not collected", never
    # a measured zero.
    s3_requests_total: int | None = None
    s3_errors_total: int | None = None
    s3_avg_latency_ms: float | None = None
    engine: EngineMetrics = field(default_factory=EngineMetrics)
    collection_error: str | None = None
    # How the per-pod CPU and memory were queried. 2: containers only, each
    # (pod, container) once. Records without it summed the pod-level total,
    # the pause container and any duplicate scrape into each pod.
    query_version: int = POD_QUERY_VERSION

    @property
    def duration_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dict."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "collection_window_seconds": self.duration_seconds,
            "pods": [
                {
                    "pod_name": p.pod_name,
                    "component": p.component,
                    "cpu_avg_cores": _rounded(p.cpu_avg_cores, 3),
                    "cpu_max_cores": _rounded(p.cpu_max_cores, 3),
                    "memory_avg_bytes": _as_int(p.memory_avg_bytes),
                    "memory_max_bytes": _as_int(p.memory_max_bytes),
                }
                for p in self.pods
            ],
            "s3_requests_total": self.s3_requests_total,
            "s3_errors_total": self.s3_errors_total,
            "s3_avg_latency_ms": self.s3_avg_latency_ms,
            "engine": self.engine.to_dict() if self.engine.has_data else None,
            "collection_error": self.collection_error,
            "query_version": self.query_version,
        }


def _rounded(value: float | None, digits: int) -> float | None:
    return None if value is None else round(value, digits)


def _as_int(value: float | None) -> int | None:
    return None if value is None else int(value)


class PlatformCollector:
    """Collects platform metrics from Prometheus at the end of a benchmark run.

    Args:
        prometheus_url: Base URL of the Prometheus service
            (e.g. ``http://lakebench-prometheus:9090``).
        namespace: Kubernetes namespace to filter pods.
    """

    def __init__(self, prometheus_url: str, namespace: str):
        self.prometheus_url = prometheus_url.rstrip("/")
        self.namespace = namespace

    def collect(self, start_time: datetime, end_time: datetime) -> PlatformMetrics:
        """Query Prometheus for platform metrics over the benchmark window.

        Returns a PlatformMetrics with best-effort data. If Prometheus is
        unreachable, returns a PlatformMetrics with ``collection_error`` set.
        """
        try:
            import httpx
        except ImportError:
            return PlatformMetrics(
                start_time=start_time,
                end_time=end_time,
                collection_error="httpx not installed; cannot query Prometheus",
            )

        metrics = PlatformMetrics(start_time=start_time, end_time=end_time)

        try:
            client = httpx.Client(timeout=30)

            # A failed query names itself in collection_error and leaves its
            # fields None, so the other family is still reported.
            failed: list[str] = []

            # Collect CPU usage per pod
            cpu_pods = self._query_family(
                failed,
                "CPU",
                client,
                "sum by (pod) (max by (pod, container) "
                f"(rate(container_cpu_usage_seconds_total{{{self._container_selector()}}}[1m])))",
                start_time,
                end_time,
            )
            for pod_data in cpu_pods:
                pod_name = pod_data.get("metric", {}).get("pod", "unknown")
                values = [float(v[1]) for v in pod_data.get("values", [])]
                if values:
                    component = self._infer_component(pod_name)
                    metrics.pods.append(
                        PodMetrics(
                            pod_name=pod_name,
                            component=component,
                            cpu_avg_cores=sum(values) / len(values),
                            cpu_max_cores=max(values),
                        )
                    )

            # Collect memory usage per pod
            mem_pods = self._query_family(
                failed,
                "memory",
                client,
                "sum by (pod) (max by (pod, container) "
                f"(container_memory_working_set_bytes{{{self._container_selector()}}}))",
                start_time,
                end_time,
            )
            for mem_data in mem_pods:
                pod_name = mem_data.get("metric", {}).get("pod", "unknown")
                values = [float(v[1]) for v in mem_data.get("values", [])]
                if values:
                    # Update existing pod or create new
                    existing = next((p for p in metrics.pods if p.pod_name == pod_name), None)
                    if existing:
                        existing.memory_avg_bytes = sum(values) / len(values)
                        existing.memory_max_bytes = max(values)
                    else:
                        metrics.pods.append(
                            PodMetrics(
                                pod_name=pod_name,
                                component=self._infer_component(pod_name),
                                memory_avg_bytes=sum(values) / len(values),
                                memory_max_bytes=max(values),
                            )
                        )

            # Engine-level Tier 2 metrics (best-effort)
            self._collect_engine_metrics(client, metrics, start_time, end_time)

            # A pod with a series in one query but not the other (one that
            # lived under the 1m rate window has memory but no CPU rate) has
            # that value not collected; say how many.
            for label, attr in (("CPU", "cpu_max_cores"), ("memory", "memory_max_bytes")):
                missing = sum(1 for p in metrics.pods if getattr(p, attr) is None)
                if missing and not any(f.startswith(label) for f in failed):
                    failed.append(
                        f"{missing} of {len(metrics.pods)} pods returned no {label} series"
                    )

            if failed:
                metrics.collection_error = "; ".join(failed)

            client.close()

        except Exception as e:
            metrics.collection_error = str(e)[:200]
            logger.warning("Failed to collect platform metrics: %s", e)

        return metrics

    def _container_selector(self) -> str:
        """Label selector for per-container cAdvisor series in the namespace.

        cAdvisor also exports a pod-level series (``container=""``, the pod
        cgroup total, which already holds its containers) and the pause
        container (``container="POD"``); both are left out. The queries then
        take ``max by (pod, container)`` before summing by pod, so a
        container scraped more than once (two kubelet ServiceMonitors, as
        when the cluster's own monitoring and kube-prometheus-stack both
        scrape the kubelet) is counted once. The cost: for up to the 1m rate
        window after a container restarts, its old and new series overlap
        and max keeps only the larger, so that minute's CPU is undercounted.
        Spark pods do not restart (restartPolicy Never).
        """
        return f'namespace="{self.namespace}", container!="", container!="POD"'

    def _query_family(self, failed: list[str], label: str, *args: Any) -> list[dict]:
        """Run one range query; on a non-200 answer record why in *failed*."""
        try:
            return self._query_range(*args)
        except PrometheusQueryError as e:
            failed.append(f"{label} query failed: {e}")
            logger.warning("Prometheus %s query failed: %s", label, e)
            return []

    def _query_range(
        self,
        client: Any,
        query: str,
        start: datetime,
        end: datetime,
        step: str = "15s",
    ) -> list[dict]:
        """Execute a Prometheus range query."""
        resp = client.get(
            f"{self.prometheus_url}/api/v1/query_range",
            params={
                "query": query,
                "start": str(start.timestamp()),
                "end": str(end.timestamp()),
                "step": step,
            },
        )
        if resp.status_code != 200:
            raise PrometheusQueryError(f"HTTP {resp.status_code}: {resp.text[:160]}")
        data = resp.json()
        return data.get("data", {}).get("result", [])

    def _query_instant(self, client: Any, query: str, time_point: datetime) -> float | None:
        """Execute a Prometheus instant query."""
        resp = client.get(
            f"{self.prometheus_url}/api/v1/query",
            params={
                "query": query,
                "time": str(time_point.timestamp()),
            },
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if results and results[0].get("value"):
            return float(results[0]["value"][1])
        return None

    def _collect_engine_metrics(
        self,
        client: Any,
        metrics: PlatformMetrics,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        """Collect Trino engine-level metrics (best-effort) from its JMX
        exporter. Spark engine metrics are not collected: pipeline jobs run
        with the Spark UI off, which serves Spark's Prometheus endpoint, so
        the Spark fields stay None. Fields stay None when a series is absent.
        """
        ns = self.namespace

        # Trino's query counters are lifetime totals since the coordinator
        # started; increase() over the run window counts this run's queries.
        window = max(60, int((end_time - start_time).total_seconds()))
        trino_completed = self._query_instant(
            client,
            f'sum(increase(trino_execution_querymanager_completedqueries_totalcount{{namespace="{ns}"}}[{window}s]))',
            end_time,
        )
        if trino_completed is not None:
            metrics.engine.trino_completed_queries = int(trino_completed)

        # Trino failed queries
        trino_failed = self._query_instant(
            client,
            f'sum(increase(trino_execution_querymanager_failedqueries_totalcount{{namespace="{ns}"}}[{window}s]))',
            end_time,
        )
        if trino_failed is not None:
            metrics.engine.trino_failed_queries = int(trino_failed)

    @staticmethod
    def _infer_component(pod_name: str) -> str:
        """Infer the lakebench component from a pod name."""
        for component in [
            "trino-coordinator",
            "trino-worker",
            "spark-thrift",
            "duckdb",
            "hive",
            "postgres",
            "polaris",
            "prometheus",
            "grafana",
            "alertmanager",
            "kube-state-metrics",
        ]:
            if component in pod_name:
                return component
        if "datagen" in pod_name:
            return "datagen"
        if "-driver" in pod_name:
            return "spark-driver"
        if "-exec-" in pod_name:
            return "spark-executor"
        if "spark" in pod_name:
            return "spark-job"
        return "unknown"
