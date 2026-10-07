"""Shared test helpers moved from tests/test_deploy.py (imported by several test files)."""

from unittest.mock import MagicMock

from lakebench.config import LakebenchConfig


def _make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig with sensible defaults for testing.

    Auto-fills the Polaris client_secret for tests whose
    architecture selects the Polaris catalog, mirroring the top-level
    conftest helper. Production configs must supply their own.
    """
    from lakebench.config.schema import CatalogType

    base = {
        "name": "test-deploy",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                    # Explicit: the default is now <name>-<layer>.
                    "buckets": {
                        "bronze": "lakebench-bronze",
                        "silver": "lakebench-silver",
                        "gold": "lakebench-gold",
                    },
                }
            }
        },
    }
    base.update(overrides)
    cfg = LakebenchConfig(**base)
    if (
        cfg.architecture.catalog.type == CatalogType.POLARIS
        and not cfg.architecture.catalog.polaris.client_secret
    ):
        cfg.architecture.catalog.polaris.client_secret = "test-only-secret"
    return cfg


def _mock_k8s():
    """Create a mock K8sClient."""
    k8s = MagicMock()
    k8s.namespace_exists.return_value = True
    k8s.apply_manifest.return_value = True
    k8s.get_cluster_capacity.return_value = None
    return k8s
