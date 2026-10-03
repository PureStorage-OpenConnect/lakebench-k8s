"""LB-185 and SAF-9: before a fresh generate, the deployer clears the owned bronze
prefix, or refuses to write over objects in a prefix it may not clear.

Covers ``DatagenDeployer._clear_bronze_prefix_if_fresh``:
- clears at cycle 0 only for a bucket ``deployment_may_empty`` proves this
  deployment's (the SAF-10 verdicts; tests/test_bronze_gate.py covers the
  verdicts themselves on the S3 fake), scoped to the datagen prefix with its
  incomplete uploads aborted;
- skips append cycles (n > 0);
- refuses (``StaleBronzeRefused``) over a non-empty prefix it may not clear,
  unless the deployer was built with ``allow_stale_bronze`` (SAF-9; before
  v1.7 it skipped the clear with an INFO line and silver over-counted);
- never clears a whole bucket (empty prefix);
- FAILS the generate (raises) when clearing an owned bucket cannot complete,
  rather than proceeding over a half-cleared prefix (invariant 3);
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


def test_clears_prefix_at_cycle_zero_for_owned_bucket(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, True)
    d = _deployer(schema="customer360")
    bucket = d.config.platform.storage.s3.buckets.bronze
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert len(_FakeS3.instances) == 1
    assert _FakeS3.instances[0].deleted == [(bucket, "customer/interactions")]
    assert _FakeS3.instances[0].aborts == [True]  # GOTCHAS 2: multipart ghosts


def test_financial_prefix_is_cleared(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, True)
    d = _deployer(schema="financial")
    bucket = d.config.platform.storage.s3.buckets.bronze
    d._clear_bronze_prefix_if_fresh(0, "pacs008")
    assert _FakeS3.instances[0].deleted == [(bucket, "pacs008")]


def test_append_cycle_never_clears(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, True)
    d = _deployer(schema="customer360")
    d._clear_bronze_prefix_if_fresh(1, "customer/interactions")
    assert _FakeS3.instances == []  # no S3 client even constructed


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


def test_unowned_bucket_is_not_cleared(monkeypatch):
    # Invariant 4: never delete data in a bucket this deployment did not create.
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, False)
    d = _deployer(schema="customer360")
    with pytest.raises(StaleBronzeRefused, match="--allow-stale-bronze"):
        d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert _FakeS3.instances[0].deleted == []


@pytest.mark.parametrize(
    "schema,prefix", [("customer360", "customer/interactions"), ("financial", "pacs008")]
)
def test_continuous_refusal_names_the_continuous_remedy(monkeypatch, schema, prefix):
    """Continuous datagen never takes --allow-stale-bronze (run refuses the flag
    there, exit 2), and its reset already cleared the prefix: the refusal says
    to re-run once no datagen pod is left, not to pass the flag."""
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, False)
    d = _deployer(schema=schema, continuous=True)
    with pytest.raises(StaleBronzeRefused) as e:
        d._clear_bronze_prefix_if_fresh(0, prefix)
    msg = str(e.value)
    assert f"/{prefix} holds objects" in msg
    assert "continuous reset cleared the prefix" in msg
    ns = d.config.get_namespace()
    assert f"kubectl get pods -n {ns} -l app=lakebench-datagen` lists none" in msg
    assert "cannot prove it may empty" in msg
    assert "clean bronze" not in msg  # empties the whole bucket; the re-run suffices
    assert "own datagen does not take --allow-stale-bronze" in msg
    assert "--force-reset does not change this check" in msg
    assert "Pass --allow-stale-bronze" not in msg
    assert _FakeS3.instances[0].deleted == []


def test_continuous_empty_prefix_refusal_does_not_suggest_the_flag(monkeypatch):
    """Unreachable from a continuous run today (the datagen prefix is fixed
    and non-empty); pinned so the branch cannot start suggesting the flag."""
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, True)
    d = _deployer(continuous=True)
    with pytest.raises(StaleBronzeRefused, match="prefix is empty") as e:
        d._clear_bronze_prefix_if_fresh(0, "/")
    assert "--allow-stale-bronze" not in str(e.value)


def test_continuous_run_builds_its_deployer_as_continuous():
    """The continuous path is the only caller that must pass continuous=True;
    every batch caller passes the operator's --allow-stale-bronze instead."""
    import ast
    import inspect

    from lakebench.cli import _generate, _run, _sustained

    def calls(mod):
        tree = ast.parse(inspect.getsource(mod))
        return [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "DatagenDeployer"
        ]

    sustained = calls(_sustained)
    assert len(sustained) == 1
    kw = {k.arg: k.value for k in sustained[0].keywords}
    assert isinstance(kw.get("continuous"), ast.Constant) and kw["continuous"].value is True
    for mod in (_run, _generate):
        for c in calls(mod):
            names = {k.arg for k in c.keywords}
            assert "continuous" not in names and "allow_stale_bronze" in names


def test_unowned_bucket_with_the_flag_is_written_over(monkeypatch):
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _own(monkeypatch, False)
    d = _deployer(schema="customer360", allow=True)
    d._clear_bronze_prefix_if_fresh(0, "customer/interactions")
    assert _FakeS3.instances[0].deleted == []


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


def test_dry_run_deploy_never_clears():
    d = _deployer(schema="customer360", dry_run=True)
    called = {"clear": False}
    d._clear_bronze_prefix_if_fresh = lambda *a, **k: called.__setitem__("clear", True)  # type: ignore[method-assign]
    result = d.deploy()
    assert result.status.value == "success"
    assert called["clear"] is False
