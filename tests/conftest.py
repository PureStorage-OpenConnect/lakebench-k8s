"""Shared fixtures for Lakebench test suite."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lakebench.config import LakebenchConfig


def make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig with sensible defaults for testing.

    This is the canonical config factory for tests. Prefer this over
    hand-building dicts so that new required fields are handled in one place.

    LB-090: when the resulting config selects Polaris and no explicit
    ``client_secret`` was supplied, fill in a test-only value so the
    deploy-time ``require_polaris_client_secret`` gate does not fire
    inside unrelated tests. Real production configs must set the secret
    themselves; the loader's ${VAR} substitution is the recommended
    channel.
    """
    from lakebench.config.schema import CatalogType

    base: dict = {
        "name": "test-fixture",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
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


@pytest.fixture
def default_config() -> LakebenchConfig:
    """A default LakebenchConfig for tests that don't care about specifics."""
    return make_config()


@pytest.fixture
def duckdb_config() -> LakebenchConfig:
    """Config with DuckDB engine selected."""
    return make_config(recipe="hive-iceberg-spark-duckdb")


@pytest.fixture
def trino_config() -> LakebenchConfig:
    """Config with Trino engine selected."""
    return make_config(recipe="hive-iceberg-spark-trino")


@pytest.fixture
def mock_subprocess():
    """Mock subprocess.run for tests that exercise kubectl/helm calls."""
    with patch("subprocess.run") as m:
        m.return_value = MagicMock(
            returncode=0,
            stdout="",
            stderr="",
        )
        yield m


@pytest.fixture
def mock_k8s_client():
    """Pre-configured mock K8sClient for unit tests."""
    client = MagicMock()
    client.namespace = "test-ns"
    client.namespace_exists.return_value = True
    client.apply_manifest.return_value = True
    client.test_connectivity.return_value = (True, "Connected")
    return client
