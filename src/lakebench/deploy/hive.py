"""Hive Metastore deployment for Lakebench.

Backward-compat re-export. Implementation moved to
``lakebench.modules.catalogs.hive.deployer``.
"""

from lakebench.modules.catalogs.hive.deployer import HiveDeployer

__all__ = ["HiveDeployer"]
