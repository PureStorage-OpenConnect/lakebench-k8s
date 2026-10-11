"""Config and K8s-client builders for the deploy tests."""

from unittest.mock import MagicMock

from lakebench.config import LakebenchConfig
from tests.conftest import make_config


def _make_config(**overrides) -> LakebenchConfig:
    """``make_config`` named test-deploy, with explicit bucket names (the default
    is <name>-<layer>)."""
    base: dict = {
        "name": "test-deploy",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                    "buckets": {
                        "bronze": "lakebench-bronze",
                        "silver": "lakebench-silver",
                        "gold": "lakebench-gold",
                    },
                }
            }
        },
    }
    return make_config(**{**base, **overrides})


def _mock_k8s():
    """Create a mock K8sClient."""
    k8s = MagicMock()
    k8s.namespace_exists.return_value = True
    k8s.apply_manifest.return_value = True
    k8s.get_cluster_capacity.return_value = None
    return k8s
