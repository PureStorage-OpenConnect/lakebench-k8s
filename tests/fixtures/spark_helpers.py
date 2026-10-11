"""Shared test helpers moved from tests/test_spark.py (imported by several test files)."""

from unittest.mock import MagicMock

from lakebench.config import LakebenchConfig


def _make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig for testing Spark configuration."""
    base = {
        "name": "test-spark",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                    "buckets": {
                        "bronze": "test-bronze",
                        "silver": "test-silver",
                        "gold": "test-gold",
                    },
                }
            }
        },
    }
    base.update(overrides)
    return LakebenchConfig(**base)


def _mock_k8s(**overrides):
    k8s = MagicMock()
    # Default: no cluster capacity (prevents concurrent budget capping)
    k8s.get_cluster_capacity.return_value = None
    for key, val in overrides.items():
        setattr(k8s, key, val)
    return k8s
