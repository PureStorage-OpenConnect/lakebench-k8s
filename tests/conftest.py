"""Shared fixtures for Lakebench test suite."""

from __future__ import annotations

import os
import tempfile

# Hermetic kube config. Code under test loads kube config before making
# (mocked) API calls; on a developer machine that silently used the real
# ~/.kube/config while CI has none, so tests passed locally and failed in CI.
# The kubernetes client reads KUBECONFIG when it is imported, so this must run
# at conftest import time, before anything imports kubernetes. Nothing
# listens on the fake server.
_FAKE_KUBECONFIG = os.path.join(tempfile.mkdtemp(prefix="lb-test-kube-"), "config")
with open(_FAKE_KUBECONFIG, "w") as _f:
    _f.write(
        "apiVersion: v1\n"
        "kind: Config\n"
        "clusters:\n"
        "- name: test\n"
        "  cluster: {server: 'https://127.0.0.1:1', insecure-skip-tls-verify: true}\n"
        "users:\n"
        "- name: test\n"
        "  user: {token: test}\n"
        "contexts:\n"
        "- name: test\n"
        "  context: {cluster: test, user: test, namespace: default}\n"
        "- name: stale-ctx\n"
        "  context: {cluster: test, user: test, namespace: default}\n"
        "current-context: test\n"
    )
os.environ["KUBECONFIG"] = _FAKE_KUBECONFIG

# Unit tests must never run real cluster CLIs. Before this guard, destroy
# tests ran `helm get values` (and could reach `helm upgrade`) against the
# developer's live cluster. These stubs shadow helm/kubectl/oc on PATH and
# fail loudly; tests that exercise those calls mock subprocess.run.
_CLI_GUARD_DIR = tempfile.mkdtemp(prefix="lb-test-cli-guard-")
for _tool in ("helm", "kubectl", "oc"):
    _path = os.path.join(_CLI_GUARD_DIR, _tool)
    with open(_path, "w") as _f:
        _f.write(
            "#!/bin/sh\n"
            f'echo "{_tool} is blocked in unit tests (mock subprocess.run)" >&2\n'
            "exit 97\n"
        )
    os.chmod(_path, 0o755)
os.environ["PATH"] = _CLI_GUARD_DIR + os.pathsep + os.environ.get("PATH", "")

from unittest.mock import MagicMock, patch  # noqa: E402

import pytest  # noqa: E402

from lakebench.config import LakebenchConfig  # noqa: E402


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
