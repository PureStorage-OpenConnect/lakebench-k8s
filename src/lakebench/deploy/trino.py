"""Trino deployment for Lakebench.

Backward-compat re-export. Implementation moved to
``lakebench.modules.query_engines.trino.deployer``.
"""

from lakebench.modules.query_engines.trino.deployer import TrinoDeployer

__all__ = ["TrinoDeployer"]
