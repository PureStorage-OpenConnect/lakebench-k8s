"""One Kubernetes quantity parser for capacity arithmetic, and a
capacity preflight that fails, not skips, when it cannot read a value."""

from __future__ import annotations

from decimal import Decimal
from unittest import mock

import pytest

from lakebench.config import LakebenchConfig
from lakebench.k8s.client import (
    CapacityUnknown,
    ClusterCapacity,
    FreeCapacity,
    K8sClient,
    ScratchCapacity,
)
from lakebench.quantity import QuantityError, parse, to_bytes, to_gib, to_millicores

GIB = 1024**3


def test_parse_every_kubernetes_form():
    for value, want in [
        ("16Gi", 16 * GIB),
        ("2000000Ki", 2_048_000_000),
        ("1Ti", 1024 * GIB),
        ("1Pi", 1024**5),
        ("2Ei", 2 * 1024**6),
        ("512Mi", 512 * 1024**2),
        ("16G", 16 * 10**9),
        ("4k", 4000),
        ("3M", 3 * 10**6),
        ("2T", 2 * 10**12),
        ("1P", 10**15),
        ("1E", 10**18),
        ("1e3", 1000),
        ("5E-2", Decimal("0.05")),
        ("1.5Gi", Decimal("1.5") * GIB),
        ("500m", Decimal("0.5")),
        ("100n", Decimal("1E-7")),
        ("7u", Decimal("7E-6")),
        ("17179869184", 16 * GIB),
        (" 8Gi ", 8 * GIB),
        ("+4", 4),
        (".5", Decimal("0.5")),
        (4, 4),
        (1.5, Decimal("1.5")),
    ]:
        assert parse(value) == Decimal(want)


def test_rejects_what_kubernetes_rejects():
    for value in ["16g", "1.5gb", "16GB", "-1Gi", "", "12e6Ki", "1K", "Gi", "1 Gi x", True, -2]:
        with pytest.raises(QuantityError):
            parse(value)


def test_rounding_and_units():
    assert to_bytes("500m") == 1
    assert to_millicores("1.5") == 1500
    assert to_millicores("250m") == 250
    assert to_millicores("0.0001") == 1
    assert to_millicores(2) == 2000
    assert to_gib("2000000Ki") == pytest.approx(1.9073486328125)


def test_node_allocatable_reads_real_node_values():
    assert K8sClient._parse_memory_to_bytes("421547872Ki") == 421547872 * 1024
    assert K8sClient._parse_cpu_to_millicores("39500m") == 39500


def test_node_allocatable_in_other_units_now_reads():
    assert K8sClient._parse_memory_to_bytes("1Pi") == 1024**5
    assert K8sClient._parse_memory_to_bytes("4e9") == 4 * 10**9
    assert K8sClient._parse_cpu_to_millicores("1e2") == 100_000


# -- the capacity preflight ---------------------------------------------------


def _config(query_engine=None, mode="batch"):
    return LakebenchConfig(
        name="t",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "a",
                    "secret_key": "b",
                    "buckets": {"bronze": "b", "silver": "s", "gold": "g"},
                }
            }
        },
        architecture={
            "workload": {"schema": "customer360", "datagen": {"scale": 1}},
            "pipeline": {"mode": mode},
            **({"query_engine": query_engine} if query_engine else {}),
        },
    )


def _engine_gb(cfg) -> float:
    """Query-engine pod memory as the capacity plan counts it."""
    from lakebench.config.sizing import _engine_pods

    return sum(mem for _, _, mem in _engine_pods(cfg))


@pytest.mark.parametrize(
    "engine,workers_gib",
    [
        ({"worker": {"replicas": 1, "memory": "1Ti"}}, 1024),
        ({"worker": {"replicas": 2, "memory": "2000000Ki"}}, 2 * to_gib("2000000Ki")),
    ],
    ids=["ti", "ki"],
)
def test_trino_memory_is_counted(engine, workers_gib):
    cfg = _config({"type": "trino", "trino": engine})
    coordinator = to_gib(cfg.architecture.query_engine.trino.coordinator.memory)
    assert _engine_gb(cfg) == pytest.approx(coordinator + workers_gib)


def test_unreachable_cluster_refuses_run_and_skips_for_deploy():
    # run's preflight fails closed (CC-24); deploy, which has no
    # --skip-preflight, warns and leaves the refusal to run.
    from lakebench.cli._prerequisites import _check_cluster_capacity, deploy_capacity_check

    with mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("no cluster")):
        run_check = _check_cluster_capacity(_config())
        deploy_check = deploy_capacity_check(_config())
    assert not run_check.passed and "capacity could not be read" in run_check.message
    assert deploy_check.passed and "skipped" in deploy_check.message


def test_thrift_counts_the_pod_not_the_heap():
    from lakebench.deploy.engine import thrift_pod_memory_limit

    qe = {"type": "spark-thrift", "spark_thrift": {"memory": "16g"}}
    gb = _engine_gb(_config(qe))
    assert gb == to_gib(thrift_pod_memory_limit("16g"))
    assert gb > 16


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_non_finite_numbers_are_quantity_errors(value):
    with pytest.raises(QuantityError):
        parse(value)


def test_fingerprint_quantities_match_the_kubernetes_client():
    for value in ["421547872Ki", "39500m", "40", "17179869184", "500M", "256Gi", "7800m", "1e3"]:
        from kubernetes.utils import parse_quantity as k8s_parse

        from lakebench.metrics.system_identity import _quantities

        assert parse(value) == k8s_parse(value)
        assert _quantities({"cpu": value, "memory": value}) == (
            float(k8s_parse(value)),
            float(k8s_parse(value)),
        )


def test_bad_node_allocatable_fails_the_real_capacity_read():
    from lakebench.cli._prerequisites import _check_cluster_capacity

    node = mock.MagicMock()
    node.metadata.name = "w1"
    node.metadata.labels = {}
    node.spec.unschedulable = False
    node.spec.taints = []
    node.status.conditions = [mock.MagicMock(type="Ready", status="True")]
    node.status.allocatable = {"cpu": "40", "memory": "402 gigs"}
    client = K8sClient.__new__(K8sClient)
    client._core_v1 = mock.MagicMock()
    client._core_v1.list_node.return_value.items = [node]
    client._core_v1.list_pod_for_all_namespaces.return_value.items = []
    with mock.patch("lakebench.k8s.get_k8s_client", return_value=client):
        result = _check_cluster_capacity(_config())
    assert not result.passed
    assert "402 gigs" in result.message


def test_duckdb_counts_its_pod():
    # A Spark-style size ("6g"), which deploy renders as 6Gi.
    qe = {"type": "duckdb", "duckdb": {"memory": "6g"}}
    assert _engine_gb(_config(qe)) == 6


@pytest.mark.parametrize("bad", ["16g", "16GB", "lots"])
@pytest.mark.parametrize("cluster", ["unreachable", "nodes unlisted", "healthy"])
def test_unreadable_config_fails_whatever_the_cluster(cluster, bad):
    from lakebench.cli._prerequisites import _check_cluster_capacity

    trino = {"type": "trino", "trino": {"coordinator": {"memory": bad}}}
    if cluster == "unreachable":
        patch = mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("down"))
    elif cluster == "nodes unlisted":
        k8s = mock.MagicMock()
        k8s.get_free_capacity.return_value = CapacityUnknown("listing nodes failed (403)")
        patch = mock.patch("lakebench.k8s.get_k8s_client", return_value=k8s)
    else:
        cap = ClusterCapacity(434_000, 4349 * GIB, 8, 40_000, 402 * GIB)
        k8s = mock.MagicMock()
        k8s.get_free_capacity.return_value = FreeCapacity(
            free=cap, allocatable=cap, free_by_node=((40_000, 402 * GIB),)
        )
        k8s.get_scratch_capacity.return_value = ScratchCapacity(None, "none published (test)")
        patch = mock.patch("lakebench.k8s.get_k8s_client", return_value=k8s)
    with patch:
        result = _check_cluster_capacity(_config(trino))
    assert not result.passed
    assert bad in result.message


def test_a_context_conflict_is_not_a_skip():
    from lakebench.cli._prerequisites import _check_cluster_capacity
    from lakebench.k8s.target import ContextConflictError

    with (
        mock.patch("lakebench.k8s.get_k8s_client", side_effect=ContextConflictError("moved")),
        pytest.raises(ContextConflictError),
    ):
        _check_cluster_capacity(_config())
