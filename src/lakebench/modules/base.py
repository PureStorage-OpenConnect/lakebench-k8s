"""Protocol definitions for Lakebench component modules.

Four module types cover the lakebench architecture axes:

- CatalogModule: Hive, Polaris, Unity
- QueryEngineModule: Trino, Spark Thrift, DuckDB
- PipelineEngineModule: Spark (extensible to Beam, Flink)
- TableFormatModule: Iceberg, Delta

Modules are self-contained: each owns its config model, deployer, templates,
and any engine-specific integration (Spark catalog conf, query adaptation).
The core merges contributions from each module without understanding them.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pydantic import BaseModel

    from lakebench.benchmark.executor import QueryExecutor
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s import K8sClient


# -- Deployer result (shared across all module types) -----------------------


class DeployResult:
    """Minimal deploy/destroy outcome."""

    __slots__ = ("name", "success", "message")

    def __init__(self, name: str, success: bool, message: str = "") -> None:
        self.name = name
        self.success = success
        self.message = message


class Deployer(Protocol):
    """Deploys or destroys a single component on Kubernetes."""

    def deploy(self) -> DeployResult: ...
    def destroy(self) -> DeployResult: ...


# -- Catalog module ----------------------------------------------------------


@runtime_checkable
class CatalogModule(Protocol):
    """Interface for catalog backends (Hive, Polaris, Unity)."""

    @property
    def name(self) -> str: ...

    @property
    def config_model(self) -> type[BaseModel]: ...

    @property
    def defaults(self) -> dict[str, Any]: ...

    @property
    def templates_dir(self) -> Path: ...

    @property
    def requires_operators(self) -> list[str]: ...

    def create_deployer(self, config: LakebenchConfig, k8s: K8sClient) -> Deployer: ...

    def get_spark_catalog_conf(self, config: LakebenchConfig) -> dict[str, str]:
        """Spark properties for this catalog (e.g. catalog type, URI, auth)."""
        ...

    def get_context_vars(self, config: LakebenchConfig) -> dict[str, Any]:
        """Template variables contributed by this catalog."""
        ...

    def check_prerequisites(self, k8s: K8sClient) -> list[str]:
        """Return list of unmet prerequisites (empty = ready)."""
        ...


# -- Query engine module -----------------------------------------------------


@runtime_checkable
class QueryEngineModule(Protocol):
    """Interface for query engines (Trino, Spark Thrift, DuckDB)."""

    @property
    def name(self) -> str: ...

    @property
    def config_model(self) -> type[BaseModel]: ...

    @property
    def defaults(self) -> dict[str, Any]: ...

    @property
    def templates_dir(self) -> Path: ...

    def create_deployer(self, config: LakebenchConfig, k8s: K8sClient) -> Deployer: ...

    def create_executor(self, config: LakebenchConfig, k8s: K8sClient) -> QueryExecutor:
        """Create a query executor for benchmarking."""
        ...

    def get_context_vars(self, config: LakebenchConfig) -> dict[str, Any]:
        """Template variables contributed by this engine."""
        ...

    def supports_maintenance(self) -> bool:
        """Whether this engine can run Iceberg/Delta maintenance SQL."""
        ...


# -- Pipeline engine module --------------------------------------------------


@runtime_checkable
class PipelineEngineModule(Protocol):
    """Interface for pipeline engines (Spark, future: Beam, Flink)."""

    @property
    def name(self) -> str: ...

    @property
    def config_model(self) -> type[BaseModel]: ...

    @property
    def defaults(self) -> dict[str, Any]: ...

    @property
    def templates_dir(self) -> Path: ...

    def create_deployer(self, config: LakebenchConfig, k8s: K8sClient) -> Deployer: ...

    def create_job_manager(self, config: LakebenchConfig, k8s: K8sClient) -> Any:
        """Create a job manager (e.g. SparkJobManager)."""
        ...

    def get_context_vars(self, config: LakebenchConfig) -> dict[str, Any]:
        """Template variables contributed by this engine."""
        ...

    def get_scripts_dir(self) -> Path:
        """Path to pipeline scripts (PySpark, Beam, etc.)."""
        ...

    def get_resource_requirements(self, scale: int) -> dict[str, Any]:
        """Resource profile for this engine at the given scale."""
        ...


# -- Table format module -----------------------------------------------------


@runtime_checkable
class TableFormatModule(Protocol):
    """Interface for table formats (Iceberg, Delta)."""

    @property
    def name(self) -> str: ...

    @property
    def config_model(self) -> type[BaseModel]: ...

    @property
    def defaults(self) -> dict[str, Any]: ...

    @property
    def version_compat(self) -> dict[tuple[int, int], list[str]]:
        """Map of (spark_major, spark_minor) -> compatible format versions."""
        ...

    def get_spark_packages(self, spark_version: tuple[int, int], format_version: str) -> list[str]:
        """Maven coordinates for this format at the given Spark version."""
        ...

    def get_spark_conf(self, config: LakebenchConfig) -> dict[str, str]:
        """Spark properties for this table format."""
        ...

    def get_pipeline_scripts(self) -> dict[str, Path]:
        """Map of job_type -> script path for pipeline stages."""
        ...

    def create_maintenance_helper(self) -> Any:
        """Create a helper for maintenance SQL (expire, compact, vacuum)."""
        ...
