"""The run's capacity preflight counts free capacity and fails closed.

It used to compare the request with total allocatable, pass when the node
list could not be read or anything went wrong, drop every control-plane
node (so a single-node cluster read as "unknown" and passed), and never
look at scratch. These cases drive ``K8sClient.get_free_capacity`` and
``get_scratch_capacity`` on fake node, pod and CSIStorageCapacity lists,
and ``_check_cluster_capacity`` on top of them.
"""

from __future__ import annotations

from types import SimpleNamespace as NS
from unittest import mock

import pytest
from kubernetes.client.rest import ApiException

from lakebench.cli._prerequisites import PREFLIGHT_SKIPPED, _check_cluster_capacity
from lakebench.config.schema import LakebenchConfig
from lakebench.k8s.client import CapacityUnknown, FreeCapacity, K8sClient

GIB = 1024**3


def _node(name, cpu="64", mem="512Gi", *, ready=True, taints=(), cordoned=False, labels=None):
    return NS(
        metadata=NS(name=name, labels=labels or {}),
        spec=NS(unschedulable=cordoned, taints=[NS(effect=e, key="k") for e in taints]),
        status=NS(
            allocatable={"cpu": cpu, "memory": mem},
            conditions=[NS(type="Ready", status="True" if ready else "False")],
        ),
    )


def _pod(name, node, cpu="0", mem="0", *, ns="other", init=None):
    def c(cpu_, mem_):
        return NS(resources=NS(requests={"cpu": cpu_, "memory": mem_}))

    return NS(
        metadata=NS(name=name, namespace=ns),
        spec=NS(
            node_name=node,
            containers=[c(cpu, mem)],
            init_containers=[c(*init)] if init else None,
            overhead=None,
        ),
    )


def _client(nodes=(), pods=(), *, node_error=None, pod_error=None, csi=None, csi_error=None):
    core = mock.MagicMock()
    if node_error:
        core.list_node.side_effect = node_error
    else:
        core.list_node.return_value = NS(items=list(nodes))
    if pod_error:
        core.list_pod_for_all_namespaces.side_effect = pod_error
    else:
        core.list_pod_for_all_namespaces.return_value = NS(items=list(pods))
    storage = mock.MagicMock()
    if csi_error:
        storage.list_csi_storage_capacity_for_all_namespaces.side_effect = csi_error
    else:
        storage.list_csi_storage_capacity_for_all_namespaces.return_value = NS(
            items=list(csi or [])
        )
    k = object.__new__(K8sClient)
    k._core_v1 = core
    k._storage_v1 = storage
    return k


def _cfg(scratch=False, sc="px-csi-scratch"):
    data = {"name": "pf-t", "recipe": "hive-iceberg-spark-trino"}
    if scratch:
        data["platform"] = {"storage": {"scratch": {"enabled": True, "storage_class": sc}}}
    return LakebenchConfig.model_validate(data)


def _check(k8s, cfg=None):
    with mock.patch("lakebench.k8s.get_k8s_client", return_value=k8s):
        return _check_cluster_capacity(cfg or _cfg())


# -- reading free capacity ----------------------------------------------------


def test_free_is_allocatable_minus_other_namespaces_requests():
    k = _client(
        [_node("a"), _node("b")],
        [
            _pod("x", "a", "10", "100Gi"),
            _pod("init-heavy", "b", "1", "1Gi", init=("20", "10Gi")),
            _pod("mine", "a", "30", "300Gi", ns="pf-t"),  # this deployment's own
            _pod("pending", None, "50", "500Gi"),  # not on a node
        ],
    )
    got = k.get_free_capacity(exclude_namespace="pf-t")
    assert isinstance(got, FreeCapacity)
    assert got.allocatable.total_cpu_millicores == 128_000
    assert got.free.total_cpu_millicores == 128_000 - 10_000 - 20_000
    assert got.free.total_memory_bytes == (1024 - 100 - 10) * GIB
    # The largest-pod check uses one node: the one with the most free memory.
    assert got.free.largest_node_memory_bytes == 502 * GIB
    assert got.free.largest_node_cpu_millicores == 44_000


def test_hidden_node_refuses():
    k = _client([_node("a")], [_pod("x", "ghost", "1", "1Gi")])
    got = k.get_free_capacity()
    assert isinstance(got, CapacityUnknown)
    assert "ghost" in got.reason and "does not show" in got.reason
    res = _check(k)
    assert not res.passed and res.message.startswith("capacity could not be read:")


@pytest.mark.parametrize("which", ["nodes", "pods"])
def test_forbidden_nodes_refuses(which):
    err = ApiException(status=403, reason="Forbidden")
    k = _client([_node("a")], node_error=err if which == "nodes" else None, pod_error=err)
    got = k.get_free_capacity()
    assert isinstance(got, CapacityUnknown) and "403 Forbidden" in got.reason
    res = _check(k)
    assert not res.passed
    assert "--skip-preflight" in res.hint


def test_no_schedulable_node_refuses():
    k = _client(
        [
            _node("cordoned", cordoned=True),
            _node("tainted", taints=("NoSchedule",)),
            _node("notready", ready=False),
        ]
    )
    got = k.get_free_capacity()
    assert isinstance(got, CapacityUnknown) and "no schedulable node" in got.reason


def test_free_not_total():
    """The cluster fits by total allocatable but not by what is free."""
    nodes = [_node(f"n{i}", "16", "128Gi") for i in range(5)]  # 80 cores / 640 GiB
    k_idle = _client(nodes)
    assert _check(k_idle).passed
    busy = [_pod(f"p{i}", f"n{i}", "8", "64Gi") for i in range(5)]  # half of it taken
    res = _check(_client(nodes, busy))
    assert not res.passed
    assert "free of" in res.hint and "allocatable" in res.hint
    assert res.record["capacity"] == "checked"


def test_single_node_control_plane_counted():
    """A single untainted control-plane node is the cluster, not "unknown"."""
    cp = _node("only", "96", "1024Gi", labels={"node-role.kubernetes.io/control-plane": ""})
    got = _client([cp]).get_free_capacity()
    assert isinstance(got, FreeCapacity) and got.free.node_count == 1
    assert _check(_client([cp])).passed
    # Tainted the usual way, it holds no pods: nothing schedulable is left.
    tainted = _node(
        "only", "96", "1024Gi", labels={"node-role.kubernetes.io/control-plane": ""},
        taints=("NoSchedule",),
    )  # fmt: skip
    assert isinstance(_client([tainted]).get_free_capacity(), CapacityUnknown)


# -- scratch ---------------------------------------------------------------------


def _csi(sc, cap):
    return NS(storage_class_name=sc, capacity=cap)


def test_scratch_csi_capacity_short():
    nodes = [_node(f"n{i}") for i in range(4)]
    cfg = _cfg(scratch=True)
    short = _check(
        _client(nodes, csi=[_csi("px-csi-scratch", "1000Gi"), _csi("other", "9Ti")]), cfg
    )
    assert not short.passed
    assert "Scratch: need 2,400 Gi of StorageClass px-csi-scratch" in short.hint
    assert "totals 1,000 Gi" in short.hint
    assert short.record["scratch"] == "checked"
    ok = _check(_client(nodes, csi=[_csi("px-csi-scratch", "2000Gi")] * 2), cfg)
    assert ok.passed and ok.record["scratch"] == "checked"


def test_scratch_unmeasurable_recorded_warning():
    """No CSIStorageCapacity: admitted, with a named warning and the record."""
    nodes = [_node(f"n{i}") for i in range(4)]
    res = _check(_client(nodes, csi=[]), _cfg(scratch=True))
    assert res.passed
    assert "scratch capacity not measurable for StorageClass px-csi-scratch" in res.message
    assert res.record == {
        "capacity": "checked",
        "scratch": "not_measurable",
        "scratch_reason": "no CSIStorageCapacity published for it",
        "storage_class": "px-csi-scratch",
    }
    # The record reaches the run's provenance and the verdict says so.
    from lakebench.metrics.collector import MetricsCollector
    from lakebench.metrics.verdict import compute_verdict

    col = MetricsCollector()
    col.start_run("r1", "pf-t", {})
    col.record_preflight(res.record)
    assert col.current_run.provenance["preflight"]["scratch"] == "not_measurable"
    q = compute_verdict(col.current_run).qualifiers
    assert q["scratch_capacity"] == "scratch capacity not checked"


def test_scratch_disabled_is_recorded_disabled():
    res = _check(_client([_node(f"n{i}") for i in range(4)]))
    assert res.passed and res.record["scratch"] == "disabled"


# -- --skip-preflight -------------------------------------------------------------


def test_skip_preflight_recorded(tmp_path, monkeypatch):
    """--skip-preflight records capacity "skipped" and the verdict says
    "capacity not checked"."""
    import json

    from tests.harness import run_harness as h

    base = h.SCENARIOS["batch_c360"]
    scenario = h.Scenario(
        name="batch_c360_skip_preflight",
        argv=[*base.argv, "--skip-preflight"],
        config=base.config,
        logs=base.logs,
    )
    monkeypatch.setitem(h.SCENARIOS, scenario.name, scenario)
    h.run_scenario(scenario.name, tmp_path, monkeypatch)
    (record,) = list(tmp_path.glob("lakebench-output/runs/*/metrics.json"))
    data = json.loads(record.read_text())
    assert data["provenance"]["preflight"] == PREFLIGHT_SKIPPED
    assert data["verdict"]["qualifiers"]["capacity"] == "capacity not checked"
