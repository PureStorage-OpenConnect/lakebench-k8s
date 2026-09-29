"""LB-185: a fresh generate clears the owned bronze prefix so a re-generate into
a reused bucket does not inherit stale part-* files from a larger earlier run.

Covers _clear_bronze_prefix_if_fresh and its ownership gate:
- clears at cycle 0 only for a bucket this deployment recorded creating;
- skips append cycles (n > 0), operator-managed buckets (create_buckets false),
  and buckets not proven owned (invariant 4);
- FAILS the generate (raises) when clearing an owned bucket cannot complete,
  rather than proceeding over a half-cleared prefix (invariant 3);
- the dry-run deploy path never clears.
"""

from __future__ import annotations

import pytest

from lakebench.deploy.datagen import DatagenDeployer
from lakebench.deploy.engine import DeploymentEngine
from tests.conftest import make_config


def _deployer(schema="customer360", create_buckets=True, dry_run=True):
    cfg = make_config(
        architecture={"workload": {"schema": schema, "datagen": {"seed": 43}}},
    )
    cfg.platform.storage.s3.create_buckets = create_buckets
    return DatagenDeployer(DeploymentEngine(cfg, dry_run=dry_run))


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


def _own(deployer, owned: bool):
    """Force the ownership verdict so the clear-path logic can be tested alone."""
    deployer._bronze_bucket_is_owned = lambda bucket: owned  # type: ignore[method-assign]


# --- clear path (ownership forced) ------------------------------------------


def test_clears_prefix_at_cycle_zero_for_owned_bucket(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360")
    _own(d, True)
    bucket = d.config.platform.storage.s3.buckets.bronze
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert len(_FakeS3.instances) == 1
    assert _FakeS3.instances[0].deleted == [(bucket, "customer/interactions")]


def test_financial_prefix_is_cleared(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="financial")
    _own(d, True)
    bucket = d.config.platform.storage.s3.buckets.bronze
    d._clear_bronze_prefix_if_fresh(0, "pacs008")
    assert _FakeS3.instances[0].deleted == [(bucket, "pacs008")]


def test_append_cycle_never_clears(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360")
    _own(d, True)
    d._clear_bronze_prefix_if_fresh(1, "customer/interactions")
    assert _FakeS3.instances == []  # no S3 client even constructed


def test_operator_managed_bucket_is_not_cleared(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360", create_buckets=False)
    _own(d, True)
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert _FakeS3.instances == []


def test_empty_prefix_is_never_cleared(monkeypatch):
    # A root/empty prefix would be too broad; never clear it.
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360")
    _own(d, True)
    d._clear_bronze_prefix_if_fresh(0, "/")
    assert _FakeS3.instances == []


def test_unowned_bucket_is_not_cleared(monkeypatch):
    # Invariant 4: never delete data in a bucket this deployment did not create.
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    d = _deployer(schema="customer360")
    _own(d, False)
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert _FakeS3.instances == []  # ownership not proven -> no S3 touch


def test_s3_init_error_fails_the_clear(monkeypatch):
    class _InitErr(_FakeS3):
        def __init__(self, **kw):
            super().__init__(**kw)
            self._init_error = "endpoint unreachable"

    monkeypatch.setattr("lakebench.s3.S3Client", _InitErr)
    d = _deployer(schema="customer360")
    _own(d, True)
    # Owned bucket: a clear that cannot even connect must fail, not proceed.
    with pytest.raises(RuntimeError, match="cannot clear the bronze prefix"):
        d._clear_bronze_prefix_if_fresh(0, "customer/interactions")


def test_delete_prefix_failure_propagates(monkeypatch):
    class _Boom(_FakeS3):
        def delete_prefix(self, bucket, prefix):
            raise RuntimeError("S3 down mid-delete")

    monkeypatch.setattr("lakebench.s3.S3Client", _Boom)
    d = _deployer(schema="customer360")
    _own(d, True)
    # A partial clear must fail the generate, not be swallowed (invariant 3).
    with pytest.raises(RuntimeError, match="S3 down mid-delete"):
        d._clear_bronze_prefix_if_fresh(0, "customer/interactions")


# --- the real ownership gate ------------------------------------------------


def test_owned_true_when_bucket_in_created_record(monkeypatch):
    d = _deployer(schema="customer360")
    bucket = d.config.platform.storage.s3.buckets.bronze
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda: object())
    monkeypatch.setattr(
        "lakebench.deploy.ownership.read_created_buckets", lambda core_v1, ns: {bucket}
    )
    assert d._bronze_bucket_is_owned(bucket) is True


def test_owned_false_when_bucket_not_in_record(monkeypatch):
    d = _deployer(schema="customer360")
    bucket = d.config.platform.storage.s3.buckets.bronze
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda: object())
    monkeypatch.setattr(
        "lakebench.deploy.ownership.read_created_buckets",
        lambda core_v1, ns: {"someone-else-bronze"},
    )
    assert d._bronze_bucket_is_owned(bucket) is False


def test_owned_false_on_read_error(monkeypatch):
    # Fail-safe: any error confirming ownership means "do not clear".
    d = _deployer(schema="customer360")
    bucket = d.config.platform.storage.s3.buckets.bronze
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda: object())

    def _boom(core_v1, ns):
        raise RuntimeError("cannot list namespace")

    monkeypatch.setattr("lakebench.deploy.ownership.read_created_buckets", _boom)
    assert d._bronze_bucket_is_owned(bucket) is False


# --- dry-run bypass ---------------------------------------------------------


def test_dry_run_deploy_never_clears():
    d = _deployer(schema="customer360", dry_run=True)
    called = {"clear": False}
    d._clear_bronze_prefix_if_fresh = lambda *a, **k: called.__setitem__("clear", True)  # type: ignore[method-assign]
    result = d.deploy()
    assert result.status.value == "success"
    assert called["clear"] is False
