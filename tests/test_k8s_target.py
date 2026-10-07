"""SAF-7 behaviour: one pinned cluster context per process (CC-7).

Every test writes a real kubeconfig with two contexts, A and B, on
different (unreachable) API servers, and records the server each Kubernetes
API call would have gone to by patching ``ApiClient.call_api``. Nothing
reaches a network.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from kubernetes.client import V1Namespace, V1NamespaceList, V1ObjectMeta
from kubernetes.client.rest import ApiException
from kubernetes.config import ConfigException
from typer.testing import CliRunner

from lakebench.k8s import target as target_mod
from lakebench.k8s.target import ClusterTarget, ContextConflictError, cli_args
from tests.conftest import point_kubeconfig_at, write_kubeconfig

# Loopback, nothing listening: an unmocked call is refused at once.
pytestmark = pytest.mark.tool_pin

SERVER_A = "https://127.0.0.1:1"
SERVER_B = "https://127.0.0.1:2"


@pytest.fixture
def kubeconfig(tmp_path, monkeypatch) -> Path:
    """Contexts A (the config's) and B (the kubeconfig's current)."""
    path = tmp_path / "kubeconfig"
    write_kubeconfig(path, {"A": SERVER_A, "B": SERVER_B}, current="B")
    point_kubeconfig_at(monkeypatch, path)
    return path


def _use_context(path: Path, name: str) -> None:
    """``kubectl config use-context NAME`` on the fake kubeconfig."""
    doc = yaml.safe_load(path.read_text())
    doc["current-context"] = name
    path.write_text(yaml.safe_dump(doc))


@pytest.fixture
def hosts():
    """Record the API server of every Kubernetes API call; answer 404,
    except namespace lists, which answer empty."""
    seen: list[str] = []

    def fake_call_api(self, resource_path, method, *args, **kwargs):
        seen.append(self.configuration.host)
        if method == "GET" and resource_path == "/api/v1/namespaces":
            return V1NamespaceList(items=[])
        raise ApiException(status=404, reason="Not Found")

    with patch("kubernetes.client.api_client.ApiClient.call_api", fake_call_api):
        yield seen


def _config_file(tmp_path: Path, context: str = "A") -> Path:
    p = tmp_path / "lakebench.yaml"
    data: dict = {"name": "ctxpin", "recipe": "hive-iceberg-spark-trino"}
    if context:
        data["platform"] = {"kubernetes": {"context": context}}
    p.write_text(yaml.safe_dump(data))
    return p


# ---------------------------------------------------------------------------
# ClusterTarget
# ---------------------------------------------------------------------------


def test_resolve_names_the_current_context(kubeconfig) -> None:
    t = ClusterTarget.resolve(None)
    assert t.context == "B" and not t.in_cluster


def test_resolve_prefers_the_config_context(kubeconfig) -> None:
    cfg = MagicMock()
    cfg.platform.kubernetes.context = "A"
    assert ClusterTarget.resolve(cfg).context == "A"


def test_activate_second_context_raises(kubeconfig) -> None:
    from kubernetes import client

    ClusterTarget.resolve(context="A").activate()
    with pytest.raises(ContextConflictError, match="one cluster context per process"):
        ClusterTarget.resolve(context="B").activate()
    # The refused activation changed nothing.
    assert client.Configuration.get_default_copy().host == SERVER_A


def test_current_context_change_mid_run_keeps_the_pinned_one(kubeconfig) -> None:
    """``kubectl config use-context A`` after the first resolution must not
    move a nameless config's clients from B."""
    from kubernetes import client

    ClusterTarget.resolve(None).activate()
    _use_context(kubeconfig, "A")
    t = ClusterTarget.resolve(None).activate()
    assert t.context == "B"
    assert client.Configuration.get_default_copy().host == SERVER_B


def test_unknown_context_refused_not_replaced_by_in_cluster(kubeconfig, monkeypatch) -> None:
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    with pytest.raises(ConfigException, match="'nope'"):
        ClusterTarget.resolve(context="nope")


def test_in_cluster_only_without_a_kubeconfig(tmp_path, monkeypatch) -> None:
    point_kubeconfig_at(monkeypatch, tmp_path / "missing")
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    with pytest.raises(ConfigException):
        ClusterTarget.resolve(None)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    t = ClusterTarget.resolve(None)
    assert t.in_cluster and t.context is None
    assert t.cli_args("kubectl") == []
    # A named context never falls back to in-cluster credentials.
    with pytest.raises(ConfigException):
        ClusterTarget.resolve(context="A")


def test_kubeconfig_wins_over_in_cluster(kubeconfig, monkeypatch) -> None:
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    assert ClusterTarget.resolve(None).context == "B"


def test_reactivation_refuses_a_context_that_moved_to_another_server(kubeconfig) -> None:
    """The same context name rewritten to point at another cluster mid-run
    (every OpenShift installer kubeconfig calls its context ``admin``)."""
    from kubernetes import client

    ClusterTarget.resolve(context="A").activate()
    doc = yaml.safe_load(kubeconfig.read_text())
    for cl in doc["clusters"]:
        if cl["name"] == "cl-A":
            cl["cluster"]["server"] = "https://127.0.0.1:3"
    kubeconfig.write_text(yaml.safe_dump(doc))
    with pytest.raises(ContextConflictError, match="127.0.0.1:3"):
        ClusterTarget.resolve(context="A").activate()
    assert client.Configuration.get_default_copy().host == SERVER_A
    assert target_mod.active_target().api_server == SERVER_A


def test_cli_args_follow_the_active_target(kubeconfig) -> None:
    assert cli_args("oc", "A") == ["--context", "A"]
    assert target_mod.active_target() is None
    ClusterTarget.resolve(None).activate()
    assert cli_args("kubectl", None) == ["--context", "B"]
    assert cli_args("helm", "") == ["--kube-context", "B"]
    assert cli_args("oc", "B") == ["--context", "B"]


def test_cli_args_pins_the_current_context_on_a_first_tool_call(kubeconfig) -> None:
    """A tool call before any API client pins the context it names, so a
    later use-context cannot split the process across two contexts."""
    assert cli_args("kubectl", None) == ["--context", "B"]
    assert target_mod.active_target().context == "B"
    _use_context(kubeconfig, "A")
    assert cli_args("helm", None) == ["--kube-context", "B"]
    with pytest.raises(ContextConflictError):
        ClusterTarget.resolve(context="A").activate()


def test_current_context_follows_kubectl_first_file_rule(tmp_path, monkeypatch) -> None:
    """kubectl takes current-context from the first file that sets it; the
    Python client's merger takes the last. Resolution follows kubectl."""
    first = tmp_path / "first"
    second = tmp_path / "second"
    write_kubeconfig(first, {"A": SERVER_A}, current="A")
    write_kubeconfig(second, {"admin": SERVER_B}, current="admin")
    point_kubeconfig_at(monkeypatch, f"{first}{os.pathsep}{second}")
    assert ClusterTarget.resolve(None).context == "A"
    t = ClusterTarget.resolve(None).activate()
    assert t.api_server == SERVER_A


def test_activate_freezes_the_ca_fingerprint(tmp_path, monkeypatch) -> None:
    """The ownership fingerprint is the CA loaded at activation; a kubeconfig
    rewritten afterwards changes neither it nor what a reload accepts."""
    import base64

    from lakebench.deploy.ownership import api_server_fingerprint

    path = tmp_path / "kubeconfig"
    write_kubeconfig(path, {"A": SERVER_A}, current="A")
    doc = yaml.safe_load(path.read_text())
    cluster = doc["clusters"][0]["cluster"]
    cluster.pop("insecure-skip-tls-verify")
    cluster["certificate-authority-data"] = base64.b64encode(b"CA-ONE").decode()
    path.write_text(yaml.safe_dump(doc))
    point_kubeconfig_at(monkeypatch, path)

    before = api_server_fingerprint("A")
    t = ClusterTarget.resolve(None).activate()
    assert t.ca_fingerprint == before is not None

    cluster["certificate-authority-data"] = base64.b64encode(b"CA-TWO").decode()
    path.write_text(yaml.safe_dump(doc))
    assert api_server_fingerprint(None) == before
    assert api_server_fingerprint("A") == before
    with pytest.raises(ContextConflictError, match="different cluster CA"):
        ClusterTarget.resolve(None).activate()


def _with_ca(path: Path, ca: bytes, *, as_file: Path | None = None) -> dict:
    """Give every cluster in the kubeconfig at ``path`` the CA ``ca``."""
    import base64

    doc = yaml.safe_load(path.read_text())
    for cl in doc["clusters"]:
        cl["cluster"].pop("insecure-skip-tls-verify", None)
        cl["cluster"].pop("certificate-authority-data", None)
        cl["cluster"].pop("certificate-authority", None)
        if as_file is not None:
            as_file.write_bytes(ca)
            cl["cluster"]["certificate-authority"] = str(as_file)
        else:
            cl["cluster"]["certificate-authority-data"] = base64.b64encode(ca).decode()
    path.write_text(yaml.safe_dump(doc))
    return doc


def test_fallback_fingerprint_refuses_an_entry_rewritten_to_another_server(
    tmp_path, monkeypatch
) -> None:
    """With the CA unread at activation, a later read must not hash the CA of
    a kubeconfig that now points the pinned name at another cluster."""
    from lakebench.deploy.ownership import api_server_fingerprint

    path = tmp_path / "kubeconfig"
    write_kubeconfig(path, {"A": SERVER_A}, current="A")
    _with_ca(path, b"CA-ONE")
    point_kubeconfig_at(monkeypatch, path)
    with patch.object(target_mod, "_kubeconfig_cluster_block", return_value=None):
        t = ClusterTarget.resolve(None).activate()
    assert not t.ca_fp_known
    write_kubeconfig(path, {"A": SERVER_B}, current="A")
    _with_ca(path, b"CA-TWO")
    assert api_server_fingerprint("A") is None
    assert api_server_fingerprint(None) is None


def test_activate_refuses_a_server_that_changed_during_the_load(kubeconfig) -> None:
    moved = {"server": "https://127.0.0.1:9"}
    with patch.object(target_mod, "_kubeconfig_cluster_block", return_value=moved):
        with pytest.raises(ContextConflictError, match="changed while it was being loaded"):
            ClusterTarget.resolve(context="A").activate()
    assert target_mod.active_target() is None


def test_cli_args_refuses_when_the_pinned_ca_changed(tmp_path, monkeypatch) -> None:
    path = tmp_path / "kubeconfig"
    write_kubeconfig(path, {"A": SERVER_A}, current="A")
    _with_ca(path, b"CA-ONE")
    point_kubeconfig_at(monkeypatch, path)
    ClusterTarget.resolve(None).activate()
    assert cli_args("kubectl", None) == ["--context", "A"]
    _with_ca(path, b"CA-TWO")
    with pytest.raises(ContextConflictError, match="different cluster CA"):
        cli_args("kubectl", None)


@pytest.mark.parametrize("later", ["", "nowhere"])
def test_first_file_current_context_wins_over_a_later_empty_or_dangling_one(
    tmp_path, monkeypatch, later
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    write_kubeconfig(first, {"A": SERVER_A}, current="A")
    write_kubeconfig(second, {"B": SERVER_B}, current="B")
    doc = yaml.safe_load(second.read_text())
    doc["current-context"] = later
    second.write_text(yaml.safe_dump(doc))
    point_kubeconfig_at(monkeypatch, f"{first}{os.pathsep}{second}")
    assert ClusterTarget.resolve(None).context == "A"


def test_cli_args_refuses_a_second_context(kubeconfig) -> None:
    ClusterTarget.resolve(None).activate()  # B
    with pytest.raises(ContextConflictError, match="--context A"):
        cli_args("helm", "A")


def test_cli_args_refuses_when_the_pinned_context_moved_server(kubeconfig) -> None:
    """kubectl re-reads the kubeconfig per call: a rewritten server for the
    pinned name must stop the subprocess, not send it to the new cluster."""
    ClusterTarget.resolve(context="A").activate()
    assert cli_args("kubectl", None) == ["--context", "A"]
    doc = yaml.safe_load(kubeconfig.read_text())
    for cl in doc["clusters"]:
        if cl["name"] == "cl-A":
            cl["cluster"]["server"] = "https://127.0.0.1:3/"
    kubeconfig.write_text(yaml.safe_dump(doc))
    with pytest.raises(ContextConflictError, match="127.0.0.1:3"):
        cli_args("helm", None)


def test_pin_command_raises_on_a_broken_kubeconfig(tmp_path, monkeypatch) -> None:
    """An existing kubeconfig that cannot be resolved stops the command;
    it never runs unpinned."""
    from lakebench.benchmark.executor import get_executor
    from lakebench.k8s.target import pin_command
    from tests.conftest import make_config

    path = tmp_path / "kubeconfig"
    path.write_text("apiVersion: v1\nkind: Config\ncontexts: []\nclusters: []\nusers: []\n")
    point_kubeconfig_at(monkeypatch, path)
    with pytest.raises(ConfigException):
        pin_command(None)
    with pytest.raises(ValueError, match="cannot pin the cluster context"):
        get_executor(make_config())


def test_existing_but_broken_kubeconfig_never_falls_back_to_in_cluster(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "kubeconfig"
    path.write_text("apiVersion: v1\nkind: Config\ncontexts: []\nclusters: []\nusers: []\n")
    point_kubeconfig_at(monkeypatch, path)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    with pytest.raises(ConfigException):
        ClusterTarget.resolve(None)


def test_pinned_helpers_use_the_active_target(kubeconfig) -> None:
    from lakebench.k8s._pinned import _pinned_argv

    ClusterTarget.resolve(None).activate()
    assert _pinned_argv("helm", None, ["list"]) == ["helm", "--kube-context", "B", "list"]
    assert _pinned_argv("kubectl", "", ["get", "pods"]) == [
        "kubectl",
        "--context",
        "B",
        "get",
        "pods",
    ]


def test_query_executor_prefix_uses_the_active_target(kubeconfig) -> None:
    for module_cls in [
        ("lakebench.modules.query_engines.trino.executor", "TrinoExecutor"),
        ("lakebench.modules.query_engines.duckdb.executor", "DuckDBExecutor"),
        ("lakebench.modules.query_engines.spark_thrift.executor", "SparkThriftExecutor"),
    ]:
        import importlib

        cls = getattr(importlib.import_module(module_cls[0]), module_cls[1])
        ClusterTarget.resolve(None).activate()
        ex = cls.__new__(cls)
        ex.kube_context = None
        assert ex._kubectl_prefix() == ["kubectl", "--context", "B"]


def test_get_executor_pins_before_the_first_query(kubeconfig) -> None:
    """benchmark and query run only ``kubectl exec`` subprocesses; a
    use-context during the run must not move the remaining queries."""
    from lakebench.benchmark.executor import get_executor
    from tests.conftest import make_config

    ex = get_executor(make_config())
    _use_context(kubeconfig, "A")
    assert ex._kubectl_prefix() == ["kubectl", "--context", "B"]


# ---------------------------------------------------------------------------
# K8sClient and get_k8s_client
# ---------------------------------------------------------------------------


def test_k8s_client_pins_the_resolved_name(kubeconfig, hosts) -> None:
    from lakebench.k8s.client import get_k8s_client

    k = get_k8s_client(context="", namespace="ns")
    assert k.context_name == "B"
    _use_context(kubeconfig, "A")
    assert k.get_current_context().name == "B"
    k.namespace_exists("ns")
    k2 = get_k8s_client(context="", namespace="ns")
    k2.namespace_exists("ns")
    assert hosts == [SERVER_B, SERVER_B]


def test_ownership_fingerprint_uses_the_pinned_context(tmp_path, monkeypatch) -> None:
    """With no configured context the fingerprint follows the pinned target,
    not a current context that changed after it was resolved."""
    import base64

    from lakebench.deploy import ownership

    path = tmp_path / "kubeconfig"
    write_kubeconfig(path, {"A": SERVER_A, "B": SERVER_B}, current="B")
    doc = yaml.safe_load(path.read_text())
    for cl in doc["clusters"]:
        cl["cluster"]["certificate-authority-data"] = base64.b64encode(
            f"ca-of-{cl['name']}".encode()
        ).decode()
    path.write_text(yaml.safe_dump(doc))
    point_kubeconfig_at(monkeypatch, path)

    fp_a = ownership.api_server_fingerprint("A")
    fp_b = ownership.api_server_fingerprint("B")
    assert fp_a and fp_b and fp_a != fp_b
    ClusterTarget.resolve(None).activate()  # pins B
    _use_context(path, "A")
    assert ownership.api_server_fingerprint(None) == fp_b
    assert target_mod.active_target().context == "B"


# ---------------------------------------------------------------------------
# Commands: every API call goes to the config's context (A), never the
# kubeconfig's current one (B)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("argv", "with_config", "context", "server", "codes"),
    [
        (["status"], True, None, SERVER_A, (1,)),  # 404: the namespace is missing
        (["status", "--namespace", "some-ns"], False, None, SERVER_B, (1,)),
        (["stop"], True, None, SERVER_A, (0,)),
        (["logs", "<cfg>", "hive"], True, "A", SERVER_A, (4,)),  # 404 on the pod list
        (["logs", "<cfg>", "hive"], True, "", SERVER_B, (4,)),
        (["info"], True, None, SERVER_A, None),
        (["config", "recommend"], True, None, SERVER_A, None),
        (["admin", "status"], False, None, SERVER_B, (0,)),
        (["admin", "release-lock"], False, None, SERVER_B, (0,)),
        (["admin", "doctor"], True, None, SERVER_A, (0, 1)),  # registry checks 404
    ],
)
def test_commands_use_the_config_context(
    kubeconfig, hosts, tmp_path, monkeypatch, argv, with_config, context, server, codes
) -> None:
    """Every API call a command makes goes to the config's cluster context,
    or to the current context when no config names one."""
    from lakebench.cli import app

    monkeypatch.chdir(tmp_path)
    if with_config:
        cfg = str(
            _config_file(tmp_path, context) if context is not None else _config_file(tmp_path)
        )
        argv = [cfg if a == "<cfg>" else a for a in argv] if "<cfg>" in argv else [*argv, cfg]
    res = CliRunner().invoke(app, argv)
    if codes is not None:
        assert res.exit_code in codes, res.output
    assert hosts and set(hosts) == {server}, res.output


def test_sustained_prometheus_lookup_reuses_the_active_target(kubeconfig, hosts) -> None:
    from lakebench.cli._sustained import _find_prometheus_svc

    ClusterTarget.resolve(context="A").activate()
    with patch("lakebench.cli._sustained.pinned_kubectl") as kubectl:
        kubectl.return_value = MagicMock(returncode=1, stdout="", stderr="")
        _find_prometheus_svc("obs", context=None)
    assert hosts == [SERVER_A]


def test_sustained_prometheus_lookup_refuses_a_second_context(kubeconfig, hosts) -> None:
    from lakebench.cli._sustained import _find_prometheus_svc

    ClusterTarget.resolve(context="A").activate()
    with pytest.raises(ContextConflictError):
        _find_prometheus_svc("obs", context="B")
    assert hosts == []


def test_observability_lookup_reuses_the_active_target(kubeconfig, hosts) -> None:
    from lakebench.deploy import observability

    ClusterTarget.resolve(context="A").activate()
    with patch.object(observability, "pinned_kubectl") as kubectl:
        kubectl.return_value = MagicMock(returncode=1, stdout="", stderr="")
        observability._find_helm_service("obs", "grafana", context=None)
    assert hosts == [SERVER_A]


def test_config_recommend_refuses_a_context_not_in_the_kubeconfig(
    kubeconfig, hosts, tmp_path
) -> None:
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["config", "recommend", str(_config_file(tmp_path, "C"))])
    assert res.exit_code == 4, res.output  # k8s.unreachable: the context does not load
    assert "not in the kubeconfig" in res.output
    assert hosts == []


@pytest.mark.parametrize("argv", [["admin", "status"], ["status", "--namespace", "x"]])
def test_configless_commands_stay_pinned_when_current_context_changes_mid_command(
    kubeconfig, tmp_path, monkeypatch, argv
) -> None:
    """The context resolved at the first call holds for the whole command,
    even when `kubectl config use-context` runs between two API calls."""
    from lakebench.cli import app

    monkeypatch.chdir(tmp_path)
    seen: list[str] = []

    def switching_call_api(self, resource_path, method, *args, **kwargs):
        seen.append(self.configuration.host)
        if len(seen) == 1:
            _use_context(kubeconfig, "A")
        if method == "GET" and resource_path == "/api/v1/namespaces":
            return V1NamespaceList(items=[])
        if method == "GET" and resource_path == "/api/v1/namespaces/{name}":
            return V1Namespace(metadata=V1ObjectMeta(name="x"))
        raise ApiException(status=404, reason="Not Found")

    with patch("kubernetes.client.api_client.ApiClient.call_api", switching_call_api):
        res = CliRunner().invoke(app, argv)
    assert len(seen) >= 2, res.output
    assert set(seen) == {SERVER_B}, res.output
