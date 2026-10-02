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


# -- review fixes ----------------------------------------------------------------


def test_largest_pod_needs_one_node_with_both_free():
    """A pod fits when one node has its cores and memory free together,
    whichever node has the most free memory."""
    nodes = [_node("mem", "40", "402Gi"), _node("cpu", "40", "402Gi")] + [
        _node(f"n{i}", "40", "402Gi") for i in range(4)
    ]
    pods = [
        _pod("hog-cpu", "mem", "38", "10Gi"),  # "mem": 2 cores, 392 GiB free
        _pod("hog-mem", "cpu", "1", "300Gi"),  # "cpu": 39 cores, 102 GiB free
    ] + [_pod(f"h{i}", f"n{i}", "36", "380Gi") for i in range(4)]
    res = _check(_client(nodes, pods))
    # The 8-core / 60 GB executor fits "cpu", though "mem" has more memory.
    assert "Largest pod" not in (res.hint or ""), res.hint


def test_largest_pod_refused_when_no_node_has_both():
    nodes = [_node("a", "40", "402Gi"), _node("b", "40", "402Gi")] + [
        _node(f"n{i}", "40", "402Gi") for i in range(6)
    ]
    pods = [_pod("x", "a", "38", "1Gi"), _pod("y", "b", "1", "390Gi")] + [
        _pod(f"h{i}", f"n{i}", "36", "380Gi") for i in range(6)
    ]
    res = _check(_client(nodes, pods))
    assert not res.passed
    assert "on one node; no schedulable node has both free" in res.hint


def test_own_namespace_spark_pods_and_running_datagen_are_counted():
    """Leftover Spark pods in the deployment's namespace hold capacity the
    plan does not count; its Trino and catalog pods are counted by the plan."""

    def own(name, cpu, mem, labels):
        p = _pod(name, "a", cpu, mem, ns="pf-t")
        p.metadata.labels = labels
        return p

    k = _client(
        [_node("a")],
        [
            own("trino-worker", "8", "48Gi", {"app": "trino"}),
            own("stream-exec", "4", "40Gi", {"spark-role": "executor"}),
            own("datagen-0", "8", "4Gi", {"job-name": "lakebench-datagen"}),
        ],
    )
    from lakebench.cli import _prerequisites as pre

    with mock.patch("lakebench.k8s.get_k8s_client", return_value=k):
        seen = {}
        real = k.get_free_capacity

        def spy(**kw):
            got = real(**kw)
            seen["free_cpu"] = got.free.total_cpu_millicores
            return got

        k.get_free_capacity = spy
        pre._check_cluster_capacity(_cfg(), datagen_runs=False)
        assert seen["free_cpu"] == 64_000 - 4_000 - 8_000  # Trino not subtracted
        pre._check_cluster_capacity(_cfg(), datagen_runs=True)
        assert seen["free_cpu"] == 64_000 - 4_000  # datagen counted by the plan instead


def test_native_sidecar_counts_with_the_main_containers():
    from lakebench.k8s.client import _pod_requests

    sidecar = NS(resources=NS(requests={"cpu": "2", "memory": "2Gi"}), restart_policy="Always")
    plain_init = NS(resources=NS(requests={"cpu": "3", "memory": "1Gi"}), restart_policy=None)
    main = NS(resources=NS(requests={"cpu": "1", "memory": "1Gi"}))
    pod = NS(spec=NS(containers=[main], init_containers=[sidecar, plain_init], overhead=None))
    cpu, mem = _pod_requests(pod)
    assert cpu == 3.0  # max(1 + 2, 3)
    assert mem == 3 * GIB  # max(1 + 2, 1) GiB


def test_continuous_degraded_caps_against_the_allocatable_base():
    """Review finding (HIGH): the capped request was computed on free
    capacity, while the run caps against allocatable, so a busy cluster was
    admitted as "degraded" at a size the run never deploys."""
    nodes = [_node(f"n{i}", "40", "402Gi") for i in range(11)]  # 440 cores allocatable
    # 120 cores free on four nodes: enough for the streams capped to a
    # 120-core budget, not for the 138 cores the run deploys uncapped.
    busy = [_pod(f"h{i}", f"n{i}", "40", "300Gi") for i in range(4, 11)]
    busy += [_pod(f"p{i}", f"n{i}", "10", "100Gi") for i in range(4)]
    cfg = LakebenchConfig.model_validate(
        {
            "name": "pf-t",
            "recipe": "hive-iceberg-spark-trino",
            "workload": {"schema": "financial", "datagen": {"scale": 1}},
            "architecture": {"pipeline": {"mode": "continuous"}},
        }
    )
    res = _check(_client(nodes, busy), cfg)
    assert not res.passed, res.message


def test_checked_record_reaches_the_run_record(tmp_path, monkeypatch):
    """The record of a passing preflight lands in provenance.preflight."""
    import json

    from lakebench.cli._prerequisites import PrereqReport, PrereqResult
    from tests.harness import run_harness as h

    record = {
        "capacity": "checked",
        "scratch": "not_measurable",
        "scratch_reason": "no CSIStorageCapacity published for it",
        "storage_class": "px-csi-scratch",
    }

    def passing(rec):
        def run_prerequisites(cfg, **kw):
            rec.add("prerequisites", "run", {})
            return PrereqReport(
                checks=[PrereqResult("cluster-capacity", True, "ok", record=dict(record))]
            )

        return run_prerequisites

    monkeypatch.setattr(h, "_passing_prerequisites", passing)
    h.run_scenario("batch_c360", tmp_path, monkeypatch)
    (path,) = list(tmp_path.glob("lakebench-output/runs/*/metrics.json"))
    data = json.loads(path.read_text())
    assert data["provenance"]["preflight"] == record
    assert data["verdict"]["qualifiers"]["scratch_capacity"] == "scratch capacity not checked"


def test_continuous_run_does_not_count_its_own_leftover_streams():
    """A continuous run stops its leftover streams before starting, so they
    are not subtracted (counting them refused reruns after an interrupt)."""
    stream = _pod("old-stream-exec", "a", "30", "300Gi", ns="pf-t")
    stream.metadata.labels = {"spark-role": "executor"}
    k = _client([_node("a")], [stream])
    seen = {}
    real = k.get_free_capacity

    def spy(**kw):
        got = real(**kw)
        seen["free_cpu"] = got.free.total_cpu_millicores
        return got

    k.get_free_capacity = spy
    with mock.patch("lakebench.k8s.get_k8s_client", return_value=k):
        _check_cluster_capacity(_cfg(), sustained=True)
        assert seen["free_cpu"] == 64_000
        _check_cluster_capacity(_cfg(), sustained=False)
        assert seen["free_cpu"] == 34_000
