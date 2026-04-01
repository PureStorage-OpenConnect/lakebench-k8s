"""RBAC deployment for Lakebench.

Backward-compat re-export. Implementation moved to
``lakebench.modules.pipeline_engines.spark.rbac``.
"""

from lakebench.modules.pipeline_engines.spark.rbac import RBACDeployer  # noqa: F401

__all__ = ["RBACDeployer"]
