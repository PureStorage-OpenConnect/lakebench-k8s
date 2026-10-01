"""ER-8 (EVD-7, SP-1): the system fingerprint (metrics/system_identity.py)."""

from __future__ import annotations

from types import SimpleNamespace as NS
from typing import Any

import pytest

from lakebench.metrics import system_identity as si


class _ApiError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"({status}) Reason: refused")
        self.status = status


def _node(cpu="40", mem="415236812Ki", arch="amd64", labels=None, alloc_cpu="39500m", name="w"):
    return NS(
        metadata=NS(name=name, labels=dict(labels or {})),
        status=NS(
            capacity={"cpu": cpu, "memory": mem},
            allocatable={"cpu": alloc_cpu, "memory": "400000000Ki"},
            node_info=NS(architecture=arch),
            conditions=[NS(type="Ready", status="True")],
        ),
        spec=NS(unschedulable=False),
    )


def _cluster(nodes=None, openshift="4.19.45", node_error=None, cv_error=None):
    workers = [
        _node(name=f"w{i}", labels={"node.kubernetes.io/instance-type": "vm-40"}) for i in range(3)
    ]
    cp = [
        _node(
            cpu="8",
            mem="32780000Ki",
            name=f"m{i}",
            labels={
                "node-role.kubernetes.io/control-plane": "",
                "node.kubernetes.io/instance-type": "vm-8",
            },
        )
        for i in range(3)
    ]
    items = workers + cp if nodes is None else nodes

    class Core:
        api_client = object()

        def list_node(self):
            if node_error:
                raise node_error
            return NS(items=items)

    class Custom:
        def get_cluster_custom_object(self, **kw):
            assert kw["plural"] == "clusterversions"
            if cv_error:
                raise cv_error
            return {"status": {"history": [{"version": openshift, "state": "Completed"}]}}

    return NS(context_name="lab", _core_v1=Core(), _custom=Custom())


@pytest.fixture
def patched(monkeypatch):
    """Patch the CA and version readers; returns a setter for both."""
    state: dict[str, Any] = {"ca": "fc35b751e6c1", "version": "v1.32.13"}
    monkeypatch.setattr(
        "lakebench.deploy.ownership.api_server_fingerprint", lambda context=None: state["ca"]
    )

    class VersionApi:
        def __init__(self, api_client):
            pass

        def get_code(self):
            return NS(git_version=state["version"])

    monkeypatch.setattr("kubernetes.client.VersionApi", VersionApi)
    return state


def _cfg(endpoint="http://10.0.1.50:80", scratch=True):
    return NS(
        platform=NS(
            storage=NS(
                s3=NS(endpoint=endpoint, buckets=NS(bronze="b-bronze")),
                scratch=NS(enabled=scratch, storage_class="px-csi-scratch"),
            )
        )
    )


class _S3:
    def __init__(self, server="PureStorageFlashBlade", error=None):
        self.server, self.error, self.calls = server, error, []

    @property
    def raw_client(self):
        return self

    def head_bucket(self, Bucket):  # noqa: N803 -- boto3 keyword
        self.calls.append(Bucket)
        if self.error:
            raise self.error
        headers = {"Server": self.server} if self.server else {}
        return {"ResponseMetadata": {"HTTPHeaders": headers}}


def _observe(k8s, cfg=None, s3=None, **kw):
    return si.observe_system(k8s, cfg or _cfg(), s3_client=s3 if s3 is not None else _S3(), **kw)


def test_full_observation(patched) -> None:
    k8s = _cluster()
    s3 = _S3()
    out = _observe(k8s, s3=s3)
    assert out["type"] == "cluster"
    assert out["partial"] is False
    parts = out["parts"]
    assert parts["api_server_ca"] == {"sha256_12": "fc35b751e6c1"}
    assert parts["platform"] == {"kubernetes": "v1.32.13", "openshift": "4.19.45"}
    assert sorted(parts["nodes"], key=lambda n: n["role"]) == [
        {
            "role": "control-plane",
            "architecture": "amd64",
            "cpu": 8,
            "memory_gib": 31,
            "model": "vm-8",
            "count": 3,
        },
        {
            "role": "worker",
            "architecture": "amd64",
            "cpu": 40,
            "memory_gib": 396,
            "model": "vm-40",
            "count": 3,
        },
    ]
    assert parts["storage"] == {
        "endpoint_host": "10.0.1.50",
        "backend": "unknown",
        "server": "PureStorageFlashBlade",
    }
    assert parts["scratch"] == {"enabled": True, "storage_class": "px-csi-scratch"}
    assert s3.calls == ["b-bronze"]
    assert len(out["fingerprint"]) == 16
    assert out["fingerprint"] == si.fingerprint_of(parts)


def test_fingerprint_ignores_load_names_and_order(patched) -> None:
    """Allocatable, node names, conditions and list order move with load
    and maintenance, not hardware: the fingerprint must not."""
    k8s = _cluster()
    base = _observe(k8s)["fingerprint"]
    nodes = list(reversed(k8s._core_v1.list_node().items))
    for i, n in enumerate(nodes):
        n.metadata.name = f"renamed-{i}"
        n.status.allocatable = {"cpu": "1", "memory": "1Ki"}
        n.status.conditions = [NS(type="Ready", status="False")]
        n.spec.unschedulable = True
    k8s2 = _cluster(nodes=nodes)
    assert _observe(k8s2)["fingerprint"] == base


@pytest.mark.parametrize(
    "change",
    [
        "ca",
        "k8s",
        "openshift",
        "node_cpu",
        "node_count",
        "node_model",
        "endpoint",
        "server",
        "scratch",
    ],
)
def test_fingerprint_moves_with_the_system(patched, change) -> None:
    k8s = _cluster()
    base = _observe(k8s)["fingerprint"]
    cfg, s3 = _cfg(), _S3()
    if change == "ca":
        patched["ca"] = "000000000000"
    elif change == "k8s":
        patched["version"] = "v1.33.0"
    elif change == "openshift":
        k8s = _cluster(openshift="4.20.1")
    elif change in ("node_cpu", "node_count", "node_model"):
        nodes = k8s._core_v1.list_node().items
        if change == "node_cpu":
            nodes[0].status.capacity["cpu"] = "48"
        elif change == "node_count":
            nodes = nodes[1:]
        else:
            nodes[0].metadata.labels["node.kubernetes.io/instance-type"] = "vm-40-v2"
        k8s = _cluster(nodes=nodes)
    elif change == "endpoint":
        cfg = _cfg(endpoint="http://10.0.1.51:80")
    elif change == "server":
        s3 = _S3(server="MinIO")
    elif change == "scratch":
        cfg = _cfg(scratch=False)
    assert _observe(k8s, cfg=cfg, s3=s3)["fingerprint"] != base


def test_forbidden_node_list_is_partial_and_left_out(patched) -> None:
    full = _observe(_cluster())
    k8s = _cluster(node_error=_ApiError(403))
    out = _observe(k8s)
    assert out["partial"] is True
    assert out["parts"]["nodes"] == {"not_observed": "node list: forbidden (403)"}
    # Left out of the hash: equal to the full observation hashed without nodes.
    others = [p for p in si.PARTS if p != "nodes"]
    assert out["fingerprint"] == si.fingerprint_of(full["parts"], others)
    assert out["fingerprint"] != full["fingerprint"]


def test_reason_text_never_enters_the_hash(patched) -> None:
    """Two gaps with different reasons hash alike: only observed values count."""
    k8s = _cluster()
    a = _observe(k8s, s3=_S3(error=_ApiError(403)))
    b = si.observe_system(k8s, _cfg(), s3_client=None)
    assert a["parts"]["storage"]["server"] != b["parts"]["storage"]["server"]
    assert a["partial"] and b["partial"]
    assert a["fingerprint"] == b["fingerprint"]


def test_refused_head_still_reads_server_header(patched) -> None:
    err = _ApiError(403)
    err.response = {"ResponseMetadata": {"HTTPHeaders": {"server": "PureStorageFlashBlade"}}}
    out = _observe(_cluster(), s3=_S3(error=err))
    assert out["parts"]["storage"]["server"] == "PureStorageFlashBlade"
    assert out["partial"] is False


def test_vanilla_kubernetes_is_observed_not_partial(patched) -> None:
    k8s = _cluster(cv_error=_ApiError(404))
    out = _observe(k8s)
    assert out["parts"]["platform"] == {"kubernetes": "v1.32.13", "openshift": None}
    assert out["partial"] is False


def test_forbidden_clusterversion_is_not_observed(patched) -> None:
    k8s = _cluster(cv_error=_ApiError(403))
    out = _observe(k8s)
    assert out["parts"]["platform"] == {"not_observed": "openshift clusterversion: forbidden (403)"}
    assert out["partial"] is True


def test_nfd_cpu_model_when_no_instance_type(patched) -> None:
    labels = {
        "feature.node.kubernetes.io/cpu-model.vendor_id": "Intel",
        "feature.node.kubernetes.io/cpu-model.family": "6",
        "feature.node.kubernetes.io/cpu-model.id": "106",
    }
    k8s = _cluster(nodes=[_node(labels=labels), _node(labels={})])
    nodes = _observe(k8s)["parts"]["nodes"]
    assert sorted(str(n["model"]) for n in nodes) == ["Intel/6/106", "None"]


def test_memory_rounded_to_gib(patched) -> None:
    """Capacity a few MiB apart (a kernel update) stays one class."""
    k8s = _cluster(nodes=[_node(mem="415236812Ki"), _node(mem="415240000Ki")])
    nodes = _observe(k8s)["parts"]["nodes"]
    assert nodes == [
        {
            "role": "worker",
            "architecture": "amd64",
            "cpu": 40,
            "memory_gib": 396,
            "model": None,
            "count": 2,
        }
    ]


def test_local_run(patched) -> None:
    out = si.observe_system(None, _cfg(), s3_client=_S3(), local=True)
    assert out["type"] == "local"
    assert out["partial"] is True
    for part in ("api_server_ca", "platform", "nodes"):
        assert out["parts"][part] == {"not_observed": "local run"}


def test_reader_bug_is_a_gap_not_a_crash(patched) -> None:
    k8s = _cluster()
    k8s._core_v1.list_node = None  # TypeError inside the reader
    out = _observe(k8s)
    assert out["partial"] is True
    assert "not_observed" in out["parts"]["nodes"]


def test_observation_round_trips_through_json(patched) -> None:
    """metrics.json stores it; a stored copy re-hashes to the same value."""
    import json

    out = _observe(_cluster())
    stored = json.loads(json.dumps(out))
    assert stored == out
    assert si.fingerprint_of(stored["parts"]) == out["fingerprint"]
