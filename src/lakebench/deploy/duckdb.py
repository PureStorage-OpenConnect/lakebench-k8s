"""DuckDB query engine deployment for Lakebench.

Backward-compat re-export. Implementation moved to
``lakebench.modules.query_engines.duckdb.deployer``.
"""

from lakebench.modules.query_engines.duckdb.deployer import DuckDBDeployer

__all__ = ["DuckDBDeployer"]
