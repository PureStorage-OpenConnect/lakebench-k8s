"""Deployment module for Lakebench.

Deployer classes are lazy-imported to avoid circular imports with the
module system (modules/catalogs/*/deployer.py imports deploy.engine,
which would trigger this __init__.py to re-import the shim files).
"""

from .engine import DeploymentEngine, DeploymentResult, DeploymentStatus


def __getattr__(name: str):
    """Lazy import deployers on first access."""
    _lazy = {
        "DatagenDeployer": ".datagen",
        "DuckDBDeployer": ".duckdb",
        "HiveDeployer": ".hive",
        "ObservabilityDeployer": ".observability",
        "PolarisDeployer": ".polaris",
        "PostgresDeployer": ".postgres",
        "RBACDeployer": ".rbac",
        "TrinoDeployer": ".trino",
    }
    if name in _lazy:
        import importlib

        mod = importlib.import_module(_lazy[name], __package__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DeploymentEngine",
    "DeploymentResult",
    "DeploymentStatus",
    "PostgresDeployer",
    "HiveDeployer",
    "PolarisDeployer",
    "TrinoDeployer",
    "RBACDeployer",
    "DatagenDeployer",
    "DuckDBDeployer",
    "ObservabilityDeployer",
]
