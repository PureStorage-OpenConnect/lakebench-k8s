"""Before a fresh generate the deployer clears only the owned bronze prefix.

Covers ``DatagenDeployer._clear_bronze_prefix_if_fresh``:
- clears at cycle 0 only for a bucket this deployment proved its own, scoped
  to the datagen prefix with incomplete uploads aborted;
- never clears on append cycles (n > 0);
- refuses over a non-empty prefix it may not clear, unless built with
  ``allow_stale_bronze``;
- never clears a whole bucket (empty prefix);
- fails the generate when clearing an owned bucket cannot complete;
- the dry-run deploy path never clears.
"""

from __future__ import annotations

import pytest

from lakebench.deploy.datagen import DatagenDeployer, StaleBronzeRefused
from lakebench.deploy.engine import DeploymentEngine
from tests.conftest import make_config


def _deployer(
    schema="customer360", create_buckets=True, dry_run=True, allow=False, continuous=False
):
    cfg = make_config(
        architecture={"workload": {"schema": schema, "datagen": {"seed": 43}}},
    )
    cfg.platform.storage.s3.create_buckets = create_buckets
    return DatagenDeployer(
        DeploymentEngine(cfg, dry_run=dry_run), allow_stale_bronze=allow, continuous=continuous
    )


class _FakeS3:
    instances: list = []
    holds = True

    def __init__(self, **kw):
        self.kw = kw
        self._init_error = None
        self.deleted: list[tuple[str, str]] = []
        self.aborts: list[bool] = []
        _FakeS3.instances.append(self)

    def bucket_exists(self, bucket):
        return True

    @property
    def raw_client(self):
        """The corpus series marker the clear writes first (kept by it)."""
        from tests.fixtures.memory_s3 import MemoryBoto

        return MemoryBoto({})

    def has_user_objects(self, bucket, prefix=""):
        return _FakeS3.holds

    def delete_prefix(self, bucket, prefix, *, abort_multipart=False, keep_keys=frozenset()):
        self.deleted.append((bucket, prefix))
        self.aborts.append(abort_multipart)
        self.kept = keep_keys
        return 3


@pytest.fixture(autouse=True)
def _reset_instances():
    _FakeS3.instances = []
    _FakeS3.holds = True
    yield
    _FakeS3.instances = []


def _own(monkeypatch, owned: bool):
    """Force the ownership verdict so the clear-path logic can be tested alone."""
    monkeypatch.setattr("lakebench.deploy.datagen.deployment_may_empty", lambda *a, **k: owned)


# --- clear path (ownership forced) ------------------------------------------


@pytest.mark.parametrize(
    "schema, prefix",
    [("customer360", "customer/interactions"), ("financial", "pacs008")],
)
def test_clears_prefix_at_cycle_zero_for_owned_bucket(monkeypatch, schema, prefix):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, True)
    d = _deployer(schema=schema)
    bucket = d.config.platform.storage.s3.buckets.bronze
    d._clear_bronze_prefix_if_fresh(0, prefix)
    assert len(_FakeS3.instances) == 1
    assert _FakeS3.instances[0].deleted == [(bucket, prefix)]
    assert _FakeS3.instances[0].aborts == [True]  # multipart ghosts are aborted


@pytest.mark.parametrize("owned", [True, False])
def test_append_cycle_never_clears(monkeypatch, owned):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, owned)
    d = _deployer(schema="customer360")
    d._clear_bronze_prefix_if_fresh(1, "customer/interactions")
    assert all(i.deleted == [] for i in _FakeS3.instances)


def test_allow_stale_bronze_writes_over_unowned_prefix_without_clearing(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, False)
    monkeypatch.setattr("lakebench.deploy.datagen._holds_corpus", lambda *a, **k: True)
    d = _deployer(schema="customer360", allow=True)
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert all(i.deleted == [] for i in _FakeS3.instances)
    with pytest.raises(StaleBronzeRefused):
        _deployer(schema="customer360", allow=False)._clear_bronze_prefix_if_fresh(
            0, "customer/interactions"
        )
    assert all(i.deleted == [] for i in _FakeS3.instances)


def test_operator_managed_bucket_follows_ownership(monkeypatch):
    """create_buckets false: a pre-provisioned bucket is cleared only when it is
    this deployment's (adopted with --force-legacy); otherwise refused."""
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, False)
    d = _deployer(schema="customer360", create_buckets=False)
    with pytest.raises(StaleBronzeRefused):
        d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert _FakeS3.instances[0].deleted == []


def test_empty_prefix_is_never_cleared(monkeypatch):
    # A root/empty prefix would be the whole bucket; never clear it.
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, True)
    d = _deployer(schema="customer360")
    with pytest.raises(StaleBronzeRefused, match="prefix is empty"):
        d._clear_bronze_prefix_if_fresh(0, "/")
    assert _FakeS3.instances[0].deleted == []
    _FakeS3.holds = False
    d._clear_bronze_prefix_if_fresh(0, "/")  # an empty bucket: nothing to refuse


def test_s3_init_error_fails_the_clear(monkeypatch):
    class _InitErr(_FakeS3):
        def __init__(self, **kw):
            super().__init__(**kw)
            self._init_error = "endpoint unreachable"

    monkeypatch.setattr("lakebench.s3.S3Client", _InitErr)
    _own(monkeypatch, True)
    d = _deployer(schema="customer360")
    # A clear that cannot even connect must fail, not proceed.
    with pytest.raises(RuntimeError, match="cannot check the bronze prefix"):
        d._clear_bronze_prefix_if_fresh(0, "customer/interactions")


def test_delete_prefix_failure_propagates(monkeypatch):
    class _Boom(_FakeS3):
        def delete_prefix(self, bucket, prefix, **kw):
            raise RuntimeError("S3 down mid-delete")

    monkeypatch.setattr("lakebench.s3.S3Client", _Boom)
    _own(monkeypatch, True)
    d = _deployer(schema="customer360")
    # A partial clear must fail the generate, not be swallowed (invariant 3).
    with pytest.raises(RuntimeError, match="S3 down mid-delete"):
        d._clear_bronze_prefix_if_fresh(0, "customer/interactions")


# --- the ownership rule fails safe ------------------------------------------


def test_may_empty_false_on_read_error(monkeypatch):
    from lakebench.deploy.datagen import deployment_may_empty

    d = _deployer(schema="customer360")
    bucket = d.config.platform.storage.s3.buckets.bronze

    def _boom(core_v1, ns):
        raise RuntimeError("cannot list namespace")

    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda: object())
    monkeypatch.setattr("lakebench.deploy.ownership.read_created_buckets", _boom)
    assert deployment_may_empty(d.config, bucket, _FakeS3()) is False


# --- dry-run bypass ---------------------------------------------------------


def test_dry_run_deploy_never_clears(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, True)
    d = _deployer(schema="customer360", dry_run=True)
    result = d.deploy()
    assert result.status.value == "success"
    assert all(i.deleted == [] for i in _FakeS3.instances)
