"""LB-185: a fresh generate clears the owned bronze prefix so a re-generate into
a reused bucket does not inherit stale part-* files from a larger earlier run.

Covers _clear_bronze_prefix_if_fresh: clears at cycle 0 for a bucket lakebench
owns, skips append cycles (n > 0) and operator-managed buckets (create_buckets
false), and swallows S3 errors (best-effort, never blocks generation).
"""

from __future__ import annotations

import pytest

from lakebench.deploy.datagen import DatagenDeployer
from lakebench.deploy.engine import DeploymentEngine
from tests.conftest import make_config


def _deployer(schema="customer360", create_buckets=True):
    cfg = make_config(
        architecture={"workload": {"schema": schema, "datagen": {"seed": 43}}},
    )
    cfg.platform.storage.s3.create_buckets = create_buckets
    return DatagenDeployer(DeploymentEngine(cfg, dry_run=True))


class _FakeS3:
    instances: list = []

    def __init__(self, **kw):
        self.kw = kw
        self._init_error = None
        self.deleted: list[tuple[str, str]] = []
        _FakeS3.instances.append(self)

    def delete_prefix(self, bucket, prefix):
        self.deleted.append((bucket, prefix))
        return 3


@pytest.fixture(autouse=True)
def _reset_instances():
    _FakeS3.instances = []
    yield
    _FakeS3.instances = []


def test_clears_prefix_at_cycle_zero_for_owned_bucket(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360")
    bucket = d.config.platform.storage.s3.buckets.bronze
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert len(_FakeS3.instances) == 1
    assert _FakeS3.instances[0].deleted == [(bucket, "customer/interactions")]


def test_financial_prefix_is_cleared(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="financial")
    bucket = d.config.platform.storage.s3.buckets.bronze
    d._clear_bronze_prefix_if_fresh(0, "pacs008")
    assert _FakeS3.instances[0].deleted == [(bucket, "pacs008")]


def test_append_cycle_never_clears(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360")
    d._clear_bronze_prefix_if_fresh(1, "customer/interactions")
    assert _FakeS3.instances == []  # no S3 client even constructed


def test_operator_managed_bucket_is_not_cleared(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360", create_buckets=False)
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert _FakeS3.instances == []


def test_empty_prefix_is_never_cleared(monkeypatch):
    # A root/empty prefix would be too broad; never clear it.
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360")
    d._clear_bronze_prefix_if_fresh(0, "/")
    assert _FakeS3.instances == []


def test_s3_init_error_is_swallowed(monkeypatch):
    class _InitErr(_FakeS3):
        def __init__(self, **kw):
            super().__init__(**kw)
            self._init_error = "endpoint unreachable"

    monkeypatch.setattr("lakebench.s3.S3Client", _InitErr)
    d = _deployer(schema="customer360")
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")  # must not raise
    assert _FakeS3.instances[0].deleted == []


def test_delete_prefix_failure_is_swallowed(monkeypatch):
    class _Boom(_FakeS3):
        def delete_prefix(self, bucket, prefix):
            raise RuntimeError("S3 down")

    monkeypatch.setattr("lakebench.s3.S3Client", _Boom)
    d = _deployer(schema="customer360")
    # Best-effort: a clearing failure must not block generation.
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
