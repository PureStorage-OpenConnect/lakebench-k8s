"""ER-8 (EVD-7, SP-1): the system fingerprint (metrics/system_identity.py)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Any

import pytest

from lakebench.metrics import system_identity as si


class _ApiError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"({status}) Reason: refused")
        self.status = status


def _node(cpu="40", mem="415236812Ki", arch="amd64", labels=None, name="w"):
    return NS(
        metadata=NS(name=name, labels=dict(labels or {})),
        status=NS(
            capacity={"cpu": cpu, "memory": mem},
            allocatable={"cpu": "39500m", "memory": "400000000Ki"},
            node_info=NS(architecture=arch),
            conditions=[NS(type="Ready", status="True")],
        ),
        spec=NS(unschedulable=False),
    )


def _default_nodes():
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
    return workers + cp


@pytest.fixture
def ca_file(tmp_path: Path) -> Path:
    p = tmp_path / "ca.crt"
    p.write_bytes(b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")
    return p


@pytest.fixture
def cluster(ca_file, monkeypatch):
    """A factory for fake K8sClients; also patches VersionApi."""
    state: dict[str, Any] = {"version": "v1.32.13", "timeouts": []}

    class VersionApi:
        def __init__(self, api_client):
            self.api_client = api_client

        def get_code(self, **kw):
            state["timeouts"].append(kw.get("_request_timeout"))
            return NS(git_version=state["version"])

    monkeypatch.setattr("kubernetes.client.VersionApi", VersionApi)

    def make(
        nodes=None,
        openshift=("4.19.45", "Completed"),
        node_error=None,
        cv_error=None,
        ca=ca_file,
        verify_ssl=True,
    ):
        items = _default_nodes() if nodes is None else nodes
        configuration = NS(ssl_ca_cert=str(ca) if ca else None, verify_ssl=verify_ssl)

        class Core:
            api_client = NS(configuration=configuration)

            def list_node(self, **kw):
                state["timeouts"].append(kw.get("_request_timeout"))
                if node_error:
                    raise node_error
                return NS(items=items)

        class Custom:
            def get_cluster_custom_object(self, **kw):
                assert kw["plural"] == "clusterversions"
                state["timeouts"].append(kw.get("_request_timeout"))
                if cv_error:
                    raise cv_error
                version, st = openshift
                return {"status": {"history": [{"version": version, "state": st}]}}

        return NS(context_name="", _core_v1=Core(), _custom=Custom())

    make.state = state  # type: ignore[attr-defined]
    return make


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


def _ca_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def test_full_observation(cluster, ca_file) -> None:
    s3 = _S3()
    out = _observe(cluster(), s3=s3)
    assert out["type"] == "cluster"
    assert out["partial"] is False
    parts = out["parts"]
    assert parts["api_server_ca"] == _ca_hash(ca_file)
    assert parts["kubernetes"] == "v1.32.13"
    assert parts["openshift"] == {"version": "4.19.45", "state": "Completed"}
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
    assert parts["storage_endpoint"] == "10.0.1.50:80"
    assert parts["storage_backend"] == "unknown"
    assert parts["storage_server"] == "PureStorageFlashBlade"
    assert parts["scratch"] == {"enabled": True, "storage_class": "px-csi-scratch"}
    assert s3.calls == ["b-bronze"]
    assert len(out["fingerprint"]) == 16
    assert out["fingerprint"] == si.fingerprint_of(parts)


def test_ca_follows_the_client_not_current_context(cluster, tmp_path, monkeypatch) -> None:
    """The CA part hashes the bundle the client verifies with, the same
    bytes deploy/ownership hashes, never the kubeconfig's current-context
    (another lane may run `oc login` mid-run)."""
    monkeypatch.setattr(
        "lakebench.deploy.ownership.api_server_fingerprint", lambda context=None: "current-ctx"
    )
    other = tmp_path / "other.crt"
    other.write_bytes(b"other CA")
    assert _observe(cluster(ca=other))["parts"]["api_server_ca"] == _ca_hash(other)


def test_ca_not_observed_without_verification(cluster) -> None:
    out = _observe(cluster(ca=None, verify_ssl=False))
    assert out["parts"]["api_server_ca"] == {"not_observed": "client skips TLS verification"}
    assert out["partial"] is True


def test_fingerprint_ignores_load_names_and_order(cluster) -> None:
    """Allocatable, node names, conditions and list order move with load
    and maintenance, not hardware: the fingerprint must not."""
    base = _observe(cluster())["fingerprint"]
    nodes = list(reversed(_default_nodes()))
    for i, n in enumerate(nodes):
        n.metadata.name = f"renamed-{i}"
        n.status.allocatable = {"cpu": "1", "memory": "1Ki"}
        n.status.conditions = [NS(type="Ready", status="False")]
        n.spec.unschedulable = True
    assert _observe(cluster(nodes=nodes))["fingerprint"] == base


@pytest.mark.parametrize(
    "change",
    [
        "ca",
        "k8s",
        "openshift",
        "upgrade_state",
        "node_cpu",
        "node_count",
        "node_model",
        "endpoint",
        "endpoint_port",
        "server",
        "scratch",
    ],
)
def test_fingerprint_moves_with_the_system(cluster, tmp_path, change) -> None:
    base = _observe(cluster())["fingerprint"]
    cfg, s3, make_kw = _cfg(), _S3(), {}
    if change == "ca":
        other = tmp_path / "other.crt"
        other.write_bytes(b"another CA")
        make_kw["ca"] = other
    elif change == "k8s":
        cluster.state["version"] = "v1.33.0"
    elif change == "openshift":
        make_kw["openshift"] = ("4.20.1", "Completed")
    elif change == "upgrade_state":
        make_kw["openshift"] = ("4.19.45", "Partial")
    elif change in ("node_cpu", "node_count", "node_model"):
        nodes = _default_nodes()
        if change == "node_cpu":
            nodes[0].status.capacity["cpu"] = "48"
        elif change == "node_count":
            nodes = nodes[1:]
        else:
            nodes[0].metadata.labels["node.kubernetes.io/instance-type"] = "vm-40-v2"
        make_kw["nodes"] = nodes
    elif change == "endpoint":
        cfg = _cfg(endpoint="http://10.0.1.51:80")
    elif change == "endpoint_port":
        cfg = _cfg(endpoint="http://10.0.1.50:9000")
    elif change == "server":
        s3 = _S3(server="MinIO")
    elif change == "scratch":
        cfg = _cfg(scratch=False)
    assert _observe(cluster(**make_kw), cfg=cfg, s3=s3)["fingerprint"] != base


def test_type_enters_the_hash(cluster) -> None:
    parts = _observe(cluster())["parts"]
    assert si.fingerprint_of(parts, system_type="local") != si.fingerprint_of(parts)


def test_endpoint_spelling_normalised(cluster) -> None:
    def ep(endpoint: str) -> Any:
        return _observe(cluster(), cfg=_cfg(endpoint=endpoint))["parts"]["storage_endpoint"]

    assert ep("http://FB.Example:80") == ep("fb.example") == "fb.example:80"
    assert ep("https://fb.example") == "fb.example:443"


def test_storage_backend_is_the_conformance_guess(cluster) -> None:
    """The conformance summary part is ``detect_backend`` of the endpoint
    (main-lane decision 2026-10-01); no endpoint is a gap, not ``unknown``."""
    aws = _observe(cluster(), cfg=_cfg(endpoint="https://s3.us-east-1.amazonaws.com"))
    assert aws["parts"]["storage_backend"] == "aws"
    none = _observe(cluster(), cfg=_cfg(endpoint=""))
    assert none["parts"]["storage_backend"] == {"not_observed": "no S3 endpoint configured"}
    assert none["partial"] is True


def test_forbidden_node_list_is_partial_and_left_out(cluster) -> None:
    full = _observe(cluster())
    out = _observe(cluster(node_error=_ApiError(403)))
    assert out["partial"] is True
    assert out["parts"]["nodes"] == {"not_observed": "node list: forbidden (403)"}
    others = [p for p in si.PARTS if p != "nodes"]
    assert out["fingerprint"] == si.fingerprint_of(full["parts"], others)
    assert out["fingerprint"] != full["fingerprint"]


def test_common_fingerprints_compare_on_parts_both_observed(cluster) -> None:
    """The run-start HEAD answers and the run-end one times out: the two
    samples are one system on every part both observed (ER-10b's case)."""
    start = _observe(cluster())
    end = _observe(cluster(), s3=_S3(error=TimeoutError("read timed out")))
    assert end["parts"]["storage_server"] == {
        "not_observed": "head bucket: TimeoutError: read timed out"
    }
    assert start["fingerprint"] != end["fingerprint"]
    fa, fb, keys = si.common_fingerprints(start, end)
    assert fa == fb
    assert "storage_server" not in keys and "nodes" in keys
    # A real difference on a shared part still shows.
    smaller = _observe(cluster(nodes=_default_nodes()[1:]), s3=_S3(error=TimeoutError("x")))
    fa, fb, _ = si.common_fingerprints(start, smaller)
    assert fa != fb


def test_absent_server_header_is_observed(cluster) -> None:
    """A backend that answers without a Server header is observed as None,
    not a gap, so the system is not partial on every run."""
    out = _observe(cluster(), s3=_S3(server=None))
    assert out["parts"]["storage_server"] is None
    assert out["partial"] is False


def test_refused_head_still_reads_server_header(cluster) -> None:
    err = _ApiError(403)
    err.response = {"ResponseMetadata": {"HTTPHeaders": {"server": "PureStorageFlashBlade"}}}
    out = _observe(cluster(), s3=_S3(error=err))
    assert out["parts"]["storage_server"] == "PureStorageFlashBlade"
    assert out["partial"] is False


def test_vanilla_kubernetes_is_observed_not_partial(cluster) -> None:
    out = _observe(cluster(cv_error=_ApiError(404)))
    assert out["parts"]["openshift"] is None
    assert out["partial"] is False


def test_forbidden_clusterversion_keeps_the_kubernetes_version(cluster) -> None:
    """A 403 on ClusterVersion loses only that part: a Kubernetes upgrade
    between two runs still shows."""
    out = _observe(cluster(cv_error=_ApiError(403)))
    assert out["parts"]["openshift"] == {
        "not_observed": "openshift clusterversion: forbidden (403)"
    }
    assert out["parts"]["kubernetes"] == "v1.32.13"
    assert out["partial"] is True


def test_nfd_cpu_model_when_no_instance_type(cluster) -> None:
    labels = {
        "feature.node.kubernetes.io/cpu-model.vendor_id": "Intel",
        "feature.node.kubernetes.io/cpu-model.family": "6",
        "feature.node.kubernetes.io/cpu-model.id": "106",
    }
    nodes = _observe(cluster(nodes=[_node(labels=labels), _node(labels={})]))["parts"]["nodes"]
    assert sorted(str(n["model"]) for n in nodes) == ["Intel/6/106", "None"]


@pytest.mark.parametrize(
    "mem,gib",
    [("415236812Ki", 396), ("415240000Ki", 396), ("416284000Ki", 397), ("415761000Ki", 397)],
)
def test_memory_rounded_to_nearest_gib(cluster, mem, gib) -> None:
    """Nearest, not floor: 396.5 GiB and above reads 397."""
    nodes = _observe(cluster(nodes=[_node(mem=mem)]))["parts"]["nodes"]
    assert nodes[0]["memory_gib"] == gib


def test_unparseable_node_makes_the_inventory_a_gap(cluster) -> None:
    """One bad quantity must not silently shrink the fleet."""
    nodes = _default_nodes()
    nodes[0].status.capacity["memory"] = "lots"
    out = _observe(cluster(nodes=nodes))
    assert "not_observed" in out["parts"]["nodes"]
    assert out["partial"] is True


def test_local_run() -> None:
    out = si.observe_system(None, _cfg(), s3_client=_S3(), local=True)
    assert out["type"] == "local"
    assert out["partial"] is True
    for part in si.CLUSTER_PARTS:
        assert out["parts"][part] == {"not_observed": "local run"}


def test_reader_bug_is_a_gap_not_a_crash(cluster) -> None:
    k8s = cluster()
    k8s._core_v1.list_node = None  # TypeError inside the reader
    out = si.observe_system(k8s, NS(platform=NS(storage=None)), s3_client=_S3())
    assert out["partial"] is True
    assert "not_observed" in out["parts"]["nodes"]
    assert "not_observed" in out["parts"]["scratch"]


def test_observation_round_trips_through_json(cluster) -> None:
    """metrics.json stores it; a stored copy re-hashes to the same value."""
    out = _observe(cluster())
    stored = json.loads(json.dumps(out))
    assert stored == out
    assert si.fingerprint_of(stored["parts"]) == out["fingerprint"]


def test_common_fingerprints_keep_each_version(cluster) -> None:
    a = _observe(cluster())
    b = json.loads(json.dumps(a))
    b["version"] = a["version"] + 1
    fa, fb, _ = si.common_fingerprints(a, b)
    assert fa != fb


def test_versionless_observation_never_matches(cluster) -> None:
    a = _observe(cluster())
    b = {k: v for k, v in a.items() if k != "version"}
    c = {**a, "version": "v1"}
    assert si.common_fingerprints(a, b)[0] != si.common_fingerprints(a, b)[1]
    assert si.common_fingerprints(a, c)[0] != si.common_fingerprints(a, c)[1]


def test_every_cluster_read_has_a_timeout(cluster) -> None:
    """An API server that stops answering must not hang the run."""
    _observe(cluster())
    assert cluster.state["timeouts"] == [si._REQUEST_TIMEOUT] * 3


def test_sibling_clusters_are_not_one_system(cluster, tmp_path) -> None:
    """Two clusters on one VM template, OpenShift release and storage share
    every shape part; with one side's CA not observed they hash equal over
    the common parts, but they are not known to be one system (review)."""
    a = _observe(cluster())
    b = _observe(cluster(ca=None, verify_ssl=False))
    fa, fb, keys = si.common_fingerprints(a, b)
    assert fa == fb and "api_server_ca" not in keys
    assert si.same_system(a, b) is False
    assert si.same_system(a, _observe(cluster())) is True
    other_ca = tmp_path / "other.crt"
    other_ca.write_bytes(b"other")
    assert si.same_system(a, _observe(cluster(ca=other_ca))) is False


def test_node_without_capacity_is_a_gap(cluster) -> None:
    joining = _node(name="new")
    joining.status.capacity = {}
    out = _observe(cluster(nodes=[*_default_nodes(), joining]))
    assert out["parts"]["nodes"] == {
        "not_observed": "node new: ValueError: capacity not reported yet"
    }


def test_detect_backend_answers_are_pinned() -> None:
    """storage_backend hashes detect_backend's answer. If this fails because
    detect_backend learned a new backend, bump SYSTEM_IDENTITY_VERSION with
    it, or stored v2 observations of an unchanged system stop matching."""
    from lakebench.s3.conformance import detect_backend

    assert si.SYSTEM_IDENTITY_VERSION == 2
    assert detect_backend("http://10.0.1.50:80") == "unknown"
    assert detect_backend("https://minio.example:9000") == "unknown"
    assert detect_backend("https://s3.us-east-1.amazonaws.com") == "aws"


# ---------------------------------------------------------------------------
# Load (K31, ER-10b)
# ---------------------------------------------------------------------------


def _pod(namespace, node, cpu=None, memory=None, init=None, overhead=None):
    def c(cpu, mem):
        req = {}
        if cpu:
            req["cpu"] = cpu
        if mem:
            req["memory"] = mem
        return NS(resources=NS(requests=req or None))

    return NS(
        metadata=NS(namespace=namespace),
        spec=NS(
            node_name=node,
            containers=[c(cpu, memory)],
            init_containers=[c(*i) for i in init] if init else None,
            overhead=overhead,
        ),
    )


def _load_cluster(nodes=None, pods=(), pod_error=None, node_error=None):
    calls: list[Any] = []
    items = _default_nodes() if nodes is None else nodes

    class Core:
        def list_node(self, **kw):
            calls.append(("list_node", kw.get("_request_timeout")))
            if node_error:
                raise node_error
            return NS(items=items)

        def list_pod_for_all_namespaces(self, **kw):
            calls.append(("pods", kw.get("field_selector"), kw.get("_request_timeout")))
            if pod_error:
                raise pod_error
            return NS(items=list(pods))

    return NS(_core_v1=Core()), calls


def test_load_sums_schedulable_workers_and_other_namespaces() -> None:
    nodes = _default_nodes()
    nodes[2].spec.unschedulable = True  # a cordoned worker
    pods = [
        _pod("mine", "w0", "8", "64Gi"),  # this deployment: left out
        _pod("other", "w1", "4500m", "16Gi"),
        _pod("other", "w2", "2", "2Gi"),  # on the cordoned worker: left out
        _pod("openshift-dns", "m0", "1", "1Gi"),  # control plane: left out
        _pod("other", "w0", "500m", None, init=[("2", "1Gi")]),  # init dominates cpu
        _pod("pending", None, "16", "16Gi"),  # not scheduled: left out
    ]
    k8s, calls = _load_cluster(nodes, pods)
    out = si.observe_load(k8s, "mine")
    assert out["allocatable"] == {
        "cpu": 79.0,
        "memory_gib": round(2 * 400000000 / 1024**2, 1),
        "nodes": 2,
    }
    assert out["cotenant_requested"] == {"cpu": 6.5, "memory_gib": 17.0, "pods": 2}
    assert out["cotenant_pending"] == {"cpu": 16.0, "memory_gib": 16.0, "pods": 1}
    assert calls[1][:3] == ("pods", "status.phase!=Succeeded,status.phase!=Failed", (5, 60))
    assert out["at"]


def test_load_pod_overhead_counts() -> None:
    k8s, _ = _load_cluster(pods=[_pod("o", "w0", "1", "1Gi", overhead={"cpu": "250m"})])
    assert si.observe_load(k8s, "mine")["cotenant_requested"]["cpu"] == 1.25


def test_endless_continue_token_is_bounded() -> None:
    class Core:
        def list_node(self, **kw):
            return NS(items=_default_nodes())

        def list_pod_for_all_namespaces(self, **kw):
            return NS(items=[], metadata=NS(_continue="again"))  # never ends

    out = si.observe_load(NS(_core_v1=Core()), "mine")
    assert "did not end" in out["cotenant_requested"]["not_observed"]


def test_mock_continue_token_ends_the_list() -> None:
    """A test double's continue token (not a string) ends the list."""
    from unittest import mock

    class Core:
        def list_node(self, **kw):
            return NS(items=_default_nodes())

        def list_pod_for_all_namespaces(self, **kw):
            return mock.MagicMock()

    assert si.observe_load(NS(_core_v1=Core()), "mine")["cotenant_requested"]["pods"] == 0


def test_refused_pod_list_is_not_observed() -> None:
    k8s, _ = _load_cluster(pod_error=_ApiError(403))
    out = si.observe_load(k8s, "mine")
    assert si.is_observed(out["allocatable"])
    assert out["cotenant_requested"] == {"not_observed": "cluster-wide pod list: forbidden (403)"}


def test_refused_node_list_leaves_both_halves_not_observed() -> None:
    k8s, calls = _load_cluster(node_error=_ApiError(403))
    out = si.observe_load(k8s, "mine")
    assert not si.is_observed(out["allocatable"]) and not si.is_observed(out["cotenant_requested"])
    assert [c[0] for c in calls] == ["list_node"]


def test_local_load_is_not_observed() -> None:
    out = si.observe_load(None, "mine", local=True)
    assert out["allocatable"] == {"not_observed": "local run"}


def test_run_samples_land_in_the_inputs_and_the_block(cluster) -> None:
    """sample_run_start writes system_identity and the start sample;
    sample_run_end adds the end sample; build_experiment copies both."""
    from tests.test_experiment import _cfg as exp_cfg
    from tests.test_experiment import _metrics

    cfg = exp_cfg()
    run = _metrics(cfg)
    k8s = cluster()
    k8s._core_v1.list_pod_for_all_namespaces = lambda **kw: NS(
        items=[_pod("other", "w0", "2", "4Gi")]
    )
    si.sample_run_start(run, cfg, k8s=k8s)
    si.sample_run_end(run, cfg, k8s=k8s)
    inputs = run.config_snapshot["experiment_inputs"]
    assert inputs["system_identity"]["fingerprint"]
    observed = inputs["observed"]
    assert set(observed) == {"allocatable", "cotenant_requested", "cotenant_pending"}
    for half in observed.values():
        assert set(half) == {"start", "end"} and half["start"]["at"]
    assert observed["cotenant_requested"]["end"]["cpu"] == 2.0
    e = run.to_dict()["experiment"]
    assert e["observed"] == observed
    assert e["system_identity"] == inputs["system_identity"]


def test_run_sampling_never_raises() -> None:
    class Boom:
        def __getattr__(self, name):
            raise RuntimeError("boom")

    run = NS(config_snapshot={"experiment_inputs": {}})
    si.sample_run_start(run, Boom(), k8s=Boom())
    si.sample_run_end(run, Boom(), k8s=Boom())
    si.sample_run_start(NS(config_snapshot=None), Boom())


def test_load_quantities_in_any_canonical_form() -> None:
    pods = [
        _pod("o", "w0", "250m", "107374182400m"),  # 0.1 GiB in milli-bytes
        _pod("o", "w0", "1", "1Pi"),
    ]
    k8s, _ = _load_cluster(pods=pods)
    out = si.observe_load(k8s, "mine")["cotenant_requested"]
    assert out["cpu"] == 1.25 and out["pods"] == 2
    assert out["memory_gib"] == round((0.1 * 1024**3 + 1024**5) / 1024**3, 1)


def test_one_unreadable_pod_is_counted_not_fatal() -> None:
    k8s, _ = _load_cluster(pods=[_pod("o", "w0", "1", "1Gi"), _pod("o", "w0", "bogus", "1Gi")])
    out = si.observe_load(k8s, "mine")["cotenant_requested"]
    assert (out["cpu"], out["pods"], out["unreadable_pods"]) == (1.0, 1, 1)


def test_sidecars_and_pod_level_requests() -> None:
    side = _pod("o", "w0", "1", "1Gi", init=[("2", "1Gi")])
    side.spec.init_containers[0].restart_policy = "Always"  # a native sidecar
    pod_level = _pod("o", "w0", "1", "8Gi")
    pod_level.spec.resources = NS(requests={"cpu": "4"})  # memory not set: containers' 8Gi
    k8s, _ = _load_cluster(pods=[side, pod_level])
    out = si.observe_load(k8s, "mine")["cotenant_requested"]
    assert out["cpu"] == 3.0 + 4.0
    assert out["memory_gib"] == 2.0 + 8.0


def test_unsampled_start_marks_every_half() -> None:
    from tests.test_experiment import _cfg as exp_cfg
    from tests.test_experiment import _metrics

    class Core:
        def list_node(self, **kw):
            raise RuntimeError("boom")

    cfg = exp_cfg()
    run = _metrics(cfg)
    import lakebench.metrics.system_identity as mod

    orig = mod.observe_system
    mod.observe_system = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x"))
    try:
        si.sample_run_start(run, cfg, k8s=NS(_core_v1=Core(), _custom=None))
    finally:
        mod.observe_system = orig
    observed = run.config_snapshot["experiment_inputs"]["observed"]
    for half in ("allocatable", "cotenant_requested", "cotenant_pending"):
        assert observed[half]["start"]["not_observed"].startswith("sampling failed")


def test_pod_list_is_paged() -> None:
    pages = [
        NS(items=[_pod("o", "w0", "1", "1Gi")], metadata=NS(_continue="t1")),
        NS(items=[_pod("o", "w1", "2", "1Gi")], metadata=NS(_continue=None)),
    ]
    seen: list[Any] = []

    class Core:
        def list_node(self, **kw):
            return NS(items=_default_nodes())

        def list_pod_for_all_namespaces(self, **kw):
            seen.append((kw.get("limit"), kw.get("_continue")))
            return pages[len(seen) - 1]

    out = si.observe_load(NS(_core_v1=Core()), "mine")
    assert out["cotenant_requested"]["cpu"] == 3.0
    assert seen == [(500, None), (500, "t1")]


def test_reasons_carry_no_address() -> None:
    exc = ConnectionError(
        "HTTPSConnectionPool(host='api.lab.example', port=6443): Max retries "
        "(Caused by ... http://192.0.2.15:80/bucket)"
    )
    reason = si._reason(exc)
    assert "api.lab.example" not in reason and "192.0.2.15" not in reason


def test_start_sample_is_bounded_by_its_deadline(monkeypatch) -> None:
    import threading

    gate = threading.Event()

    class Core:
        def list_node(self, **kw):
            gate.wait(10)
            return NS(items=[])

    monkeypatch.setattr(si, "START_DEADLINE_S", 0.2)
    from tests.test_experiment import _cfg as exp_cfg
    from tests.test_experiment import _metrics

    cfg = exp_cfg()
    run = _metrics(cfg)
    si.sample_run_start(run, cfg, k8s=NS(_core_v1=Core(), _custom=None))
    gate.set()
    inputs = run.config_snapshot["experiment_inputs"]
    assert "system_identity" not in inputs
    assert "not sampled within" in inputs["observed"]["allocatable"]["start"]["not_observed"]


def test_local_identity_names_nothing() -> None:
    ident = si._local_identity(None)
    assert ident["type"] == "local" and ident["partial"] is True
    assert not si.observed_parts(ident["parts"])
