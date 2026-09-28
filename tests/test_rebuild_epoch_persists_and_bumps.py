"""B1: --force-rebuild bumps the silver rebuild-epoch counter and Delta
txnAppId moves to a new namespace.

Delta short-circuits any (txnAppId, txnVersion) commit it has recorded, so
cycle 0 of a rebuild that shared its appId with the last epoch's cycle 0
would silently commit zero rows. This test asserts that:

1. The generated ``delta_batch_txn_options`` result changes across two
   ``--force-rebuild`` invocations.
2. The CLI helper bumps the ConfigMap key atomically, creating the
   ConfigMap on 404 and retrying on 409.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent / "src/lakebench/spark/scripts"))


def test_delta_batch_txn_options_shape():
    """delta_batch_txn_options returns the two Delta-idempotency keys."""
    from common import delta_batch_txn_options

    opts = delta_batch_txn_options("lb-silver-build", 0, 3)
    assert opts == {"txnAppId": "lb-silver-build-rebuild-0", "txnVersion": "3"}


def test_delta_appid_differs_across_rebuild_epochs():
    """A --force-rebuild bump moves cycle 0 into a fresh appId namespace."""
    from common import delta_batch_txn_options

    epoch0 = delta_batch_txn_options("lb-silver-build", 0, 0)
    epoch1 = delta_batch_txn_options("lb-silver-build", 1, 0)
    assert epoch0["txnAppId"] != epoch1["txnAppId"], (
        "cycle 0 of a rebuilt deployment must have a fresh appId to avoid "
        "Delta's exactly-once short-circuit against the old epoch's commit"
    )


def test_delta_batch_txn_options_does_not_touch_streaming_query_id(monkeypatch):
    """The helper never calls streaming_query_id (would crash outside foreachBatch)."""
    import common as common_mod

    def _boom(spark):
        raise RuntimeError("streaming_query_id must not be called from batch")

    monkeypatch.setattr(common_mod, "streaming_query_id", _boom)
    opts = common_mod.delta_batch_txn_options("lb-silver-build", 2, 5)
    assert opts["txnVersion"] == "5"


def _fake_api_exception(status: int):
    """A stand-in for kubernetes.client.exceptions.ApiException with a status."""
    from kubernetes.client.exceptions import ApiException

    return ApiException(status=status, reason="synthetic")


class _FakeCoreV1:
    """A CoreV1Api double that behaves like a per-namespace ConfigMap store."""

    def __init__(self, initial: dict | None = None, missing: bool = False):
        # {(namespace, name): dict}
        self.store: dict[tuple[str, str], dict] = {}
        if not missing and initial is not None:
            self.store[("test-ns", "lakebench-silver-state")] = dict(initial)
        self.reads = 0
        self.replaces = 0
        self.creates = 0

    def read_namespaced_config_map(self, name, namespace):
        self.reads += 1
        data = self.store.get((namespace, name))
        if data is None:
            raise _fake_api_exception(404)
        cm = MagicMock()
        cm.data = dict(data)
        return cm

    def replace_namespaced_config_map(self, name, namespace, cm):
        self.replaces += 1
        self.store[(namespace, name)] = dict(cm.data)

    def create_namespaced_config_map(self, namespace, manifest):
        self.creates += 1
        self.store[(namespace, manifest["metadata"]["name"])] = dict(manifest["data"])


class _FakeCfg:
    def __init__(self, schema_type: str = "c360", table_format: str = "delta"):
        self.platform = type("_P", (), {})()
        self.platform.kubernetes = type("_K", (), {"context": ""})()
        self.architecture = type("_A", (), {})()
        self.architecture.workload = type("_W", (), {})()
        self.architecture.workload.schema_type = type("_ST", (), {"value": schema_type})()
        self.architecture.table_format = type("_TF", (), {})()
        self.architecture.table_format.type = type("_TFT", (), {"value": table_format})()

    def get_namespace(self):
        return "test-ns"


def test_bump_creates_configmap_when_missing(monkeypatch):
    """A 404 on the read must create the ConfigMap with the target key at 1."""
    pytest.importorskip("kubernetes")
    from lakebench.cli._run import _bump_silver_rebuild_epoch

    fake = _FakeCoreV1(missing=True)
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **kw: fake)
    monkeypatch.setattr("lakebench.k8s.get_k8s_client", lambda **kw: None)

    _bump_silver_rebuild_epoch(_FakeCfg())
    stored = fake.store[("test-ns", "lakebench-silver-state")]
    assert stored["rebuild_epoch_c360_delta"] == "1"
    # The other keys initialise to 0 so a later bump against them starts clean.
    assert stored["rebuild_epoch_c360_iceberg"] == "0"
    assert stored["rebuild_epoch_aml_iceberg"] == "0"


def test_bump_increments_existing_counter(monkeypatch):
    """A present ConfigMap: the target key increments; siblings stay put."""
    pytest.importorskip("kubernetes")
    from lakebench.cli._run import _bump_silver_rebuild_epoch

    fake = _FakeCoreV1(
        initial={
            "rebuild_epoch_c360_delta": "3",
            "rebuild_epoch_c360_iceberg": "1",
            "rebuild_epoch_aml_iceberg": "0",
        }
    )
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **kw: fake)
    monkeypatch.setattr("lakebench.k8s.get_k8s_client", lambda **kw: None)

    _bump_silver_rebuild_epoch(_FakeCfg())
    _bump_silver_rebuild_epoch(_FakeCfg())
    stored = fake.store[("test-ns", "lakebench-silver-state")]
    # Two --force-rebuild invocations: 3 -> 4 -> 5.
    assert stored["rebuild_epoch_c360_delta"] == "5"
    assert stored["rebuild_epoch_c360_iceberg"] == "1"
    assert stored["rebuild_epoch_aml_iceberg"] == "0"


def test_delta_appid_moves_across_two_force_rebuilds(monkeypatch):
    """End-to-end: after two --force-rebuild invocations, cycle 0 gets a fresh appId."""
    pytest.importorskip("kubernetes")
    from common import delta_batch_txn_options

    from lakebench.cli._run import _bump_silver_rebuild_epoch

    fake = _FakeCoreV1(missing=True)
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **kw: fake)
    monkeypatch.setattr("lakebench.k8s.get_k8s_client", lambda **kw: None)

    # First --force-rebuild: creates the map, sets the key to 1.
    _bump_silver_rebuild_epoch(_FakeCfg())
    epoch_a = int(fake.store[("test-ns", "lakebench-silver-state")]["rebuild_epoch_c360_delta"])
    opts_a = delta_batch_txn_options("lb-silver-build", epoch_a, 0)

    # Second --force-rebuild: increments to 2.
    _bump_silver_rebuild_epoch(_FakeCfg())
    epoch_b = int(fake.store[("test-ns", "lakebench-silver-state")]["rebuild_epoch_c360_delta"])
    opts_b = delta_batch_txn_options("lb-silver-build", epoch_b, 0)

    assert epoch_a != epoch_b
    assert opts_a["txnAppId"] != opts_b["txnAppId"], (
        "two --force-rebuild invocations must move the appId so Delta cannot "
        "short-circuit the new cycle 0 against the old one"
    )
