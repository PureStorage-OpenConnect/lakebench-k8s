"""Unity Catalog deployment for Lakebench.

Backward-compat re-export. Implementation moved to
``lakebench.modules.catalogs.unity.deployer``.
"""

from lakebench.modules.catalogs.unity.deployer import UnityDeployer

__all__ = ["UnityDeployer"]
