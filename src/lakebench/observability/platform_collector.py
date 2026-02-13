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
    cpu_avg_cores: float = 0.0
    cpu_max_cores: float = 0.0
    memory_avg_bytes: float = 0.0
    memory_max_bytes: float = 0.0


@dataclass
class PlatformMetrics:
    """Platform-level metrics collected from Prometheus."""

    start_time: datetime
    end_time: datetime
    pods: list[PodMetrics] = field(default_factory=list)
    s3_requests_total: int = 0
    s3_errors_total: int = 0
    s3_avg_latency_ms: float = 0.0
    collection_error: str | None = None

    @property
    def duration_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dict."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "pods": [
                {
                    "pod_name": p.pod_name,
                    "component": p.component,
                    "cpu_avg_cores": round(p.cpu_avg_cores, 3),
                    "cpu_max_cores": round(p.cpu_max_cores, 3),
                    "memory_avg_bytes": int(p.memory_avg_bytes),
                    "memory_max_bytes": int(p.memory_max_bytes),
                }
                for p in self.pods
            ],
            "s3_requests_total": self.s3_requests_total,
            "s3_errors_total": self.s3_errors_total,
            "s3_avg_latency_ms": round(self.s3_avg_latency_ms, 2),
            "collection_error": self.collection_error,
        }


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

            # Collect CPU usage per pod
            cpu_pods = self._query_range(
                client,
                f'sum by (pod) (rate(container_cpu_usage_seconds_total{{namespace="{self.namespace}"}}[1m]))',
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
            mem_pods = self._query_range(
                client,
                f'sum by (pod) (container_memory_working_set_bytes{{namespace="{self.namespace}"}})',
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

            # Collect S3 metrics (lakebench CLI-side)
            s3_total = self._query_instant(
                client,
                "sum(lakebench_s3_requests_total)",
                end_time,
            )
            if s3_total is not None:
                metrics.s3_requests_total = int(s3_total)

            s3_errors = self._query_instant(
                client,
                "sum(lakebench_s3_errors_total)",
                end_time,
            )
            if s3_errors is not None:
                metrics.s3_errors_total = int(s3_errors)

            client.close()

        except Exception as e:
            metrics.collection_error = str(e)[:200]
            logger.warning("Failed to collect platform metrics: %s", e)

        return metrics

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
                "start": start.isoformat(),
                "end": end.isoformat(),
                "step": step,
            },
        )
        if resp.status_code != 200:
            logger.warning("Prometheus query failed: %s", resp.text[:200])
            return []
        data = resp.json()
        return data.get("data", {}).get("result", [])

    def _query_instant(self, client: Any, query: str, time_point: datetime) -> float | None:
        """Execute a Prometheus instant query."""
        resp = client.get(
            f"{self.prometheus_url}/api/v1/query",
            params={
                "query": query,
                "time": time_point.isoformat(),
            },
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if results and results[0].get("value"):
            return float(results[0]["value"][1])
        return None

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
        ]:
            if component in pod_name:
                return component
        if "spark" in pod_name:
            return "spark-job"
        return "unknown"
