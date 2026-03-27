"""Pipeline engine protocol -- interface for Spark, Beam, Flink, etc.

Spark is the v1.x implementation. Future engines implement this same
interface and are selected via the ``pipeline.engine`` config field.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s import K8sClient


@dataclass
class JobResult:
    """Result of a completed pipeline job."""

    job_id: str
    job_type: str
    success: bool
    elapsed_seconds: float
    error_message: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class PipelineEngine(Protocol):
    """Interface for pipeline execution engines."""

    def engine_name(self) -> str:
        """Return engine identifier (e.g. 'spark', 'beam')."""
        ...

    def submit_job(self, job_type: str, config: Any, **kwargs: Any) -> str:
        """Submit a pipeline job. Returns job ID."""
        ...

    def wait_for_completion(self, job_id: str, timeout: int = 3600) -> JobResult:
        """Block until job completes or times out."""
        ...

    def get_logs(self, job_id: str) -> str:
        """Get job logs."""
        ...

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job."""
        ...


def get_engine(cfg: LakebenchConfig, k8s: K8sClient) -> PipelineEngine:
    """Factory that returns the configured pipeline engine.

    Reads ``cfg.architecture.pipeline_engine`` and returns the matching
    engine instance.  Today only ``spark`` is implemented; future engines
    (Beam, Flink) plug in here.
    """
    engine_type = cfg.architecture.pipeline_engine.value

    if engine_type == "spark":
        from lakebench.spark.job import SparkJobManager

        return SparkJobManager(cfg, k8s)

    raise ValueError(f"Unsupported pipeline engine: {engine_type!r}. Currently supported: spark.")
