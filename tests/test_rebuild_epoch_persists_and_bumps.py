"""--force-rebuild bumps the silver rebuild-epoch counter and Delta txnAppId
moves to a new namespace; the CLI helper bumps the ConfigMap key atomically."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

_HERE = Path(__file__).resolve().parent
pytestmark = pytest.mark.usefixtures("load_script")


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


def _cfg():
    from tests.conftest import make_config

    return make_config(
        name="test-ns",
        workload={"schema": "customer360"},
        architecture={"table_format": {"type": "delta"}},
    )


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

    _bump_silver_rebuild_epoch(_cfg())
    _bump_silver_rebuild_epoch(_cfg())
    stored = fake.store[("test-ns", "lakebench-silver-state")]
    # Two --force-rebuild invocations: 3 -> 4 -> 5.
    assert stored["rebuild_epoch_c360_delta"] == "5"
    assert stored["rebuild_epoch_c360_iceberg"] == "1"
    assert stored["rebuild_epoch_aml_iceberg"] == "0"


def test_delta_appid_moves_across_two_force_rebuilds(monkeypatch):
    """End-to-end: after two --force-rebuild invocations, cycle 0 gets a fresh appId."""
    pytest.importorskip("kubernetes")
    import common as common_mod
    from common import delta_batch_txn_options

    from lakebench.cli._run import _bump_silver_rebuild_epoch

    def _boom(spark):
        raise RuntimeError("streaming_query_id must not be called from batch")

    monkeypatch.setattr(common_mod, "streaming_query_id", _boom)

    fake = _FakeCoreV1(missing=True)
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **kw: fake)
    monkeypatch.setattr("lakebench.k8s.get_k8s_client", lambda **kw: None)

    # First --force-rebuild: creates the map, sets the key to 1.
    _bump_silver_rebuild_epoch(_cfg())
    epoch_a = int(fake.store[("test-ns", "lakebench-silver-state")]["rebuild_epoch_c360_delta"])
    opts_a = delta_batch_txn_options("lb-silver-build", epoch_a, 0)

    # Second --force-rebuild: increments to 2.
    _bump_silver_rebuild_epoch(_cfg())
    epoch_b = int(fake.store[("test-ns", "lakebench-silver-state")]["rebuild_epoch_c360_delta"])
    opts_b = delta_batch_txn_options("lb-silver-build", epoch_b, 0)

    assert epoch_a != epoch_b
    assert opts_a["txnAppId"] != opts_b["txnAppId"], (
        "two --force-rebuild invocations must move the appId so Delta cannot "
        "short-circuit the new cycle 0 against the old one"
    )


def test_read_silver_state_epoch_404_still_returns_zero(monkeypatch):
    """job.py: a 404 on the ConfigMap read is the greenfield / pre-B1 case."""
    pytest.importorskip("kubernetes")
    from lakebench.modules.pipeline_engines.spark.job import _read_silver_state_epoch

    fake = _FakeCoreV1(missing=True)
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **kw: fake)
    assert _read_silver_state_epoch("test-ns", "rebuild_epoch_c360_delta") == 0
