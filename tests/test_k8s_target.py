"""SAF-7 behaviour: one pinned cluster context per process (CC-7).

Every test writes a real kubeconfig with two contexts, A and B, on
different (unreachable) API servers, and records the server each Kubernetes
API call would have gone to by patching ``ApiClient.call_api``. Nothing
reaches a network.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from kubernetes.client import V1NamespaceList
from kubernetes.client.rest import ApiException
from kubernetes.config import ConfigException
from typer.testing import CliRunner

from lakebench.k8s import target as target_mod
from lakebench.k8s.target import ClusterTarget, ContextConflictError, cli_args
from tests.conftest import point_kubeconfig_at, write_kubeconfig

# Loopback, nothing listening: an unmocked call is refused at once.
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


def test_activate_sets_the_default_host(kubeconfig) -> None:
    from kubernetes import client

    t = ClusterTarget.resolve(context="A").activate()
    assert t.api_server == SERVER_A
    assert client.Configuration.get_default_copy().host == SERVER_A


def test_activate_second_context_raises(kubeconfig) -> None:
    from kubernetes import client

    ClusterTarget.resolve(context="A").activate()
    with pytest.raises(ContextConflictError, match="one cluster context per process"):
        ClusterTarget.resolve(context="B").activate()
    # The refused activation changed nothing.
    assert client.Configuration.get_default_copy().host == SERVER_A


def test_repeat_activation_of_the_same_target_is_allowed(kubeconfig) -> None:
    first = ClusterTarget.resolve(context="A").activate()
    again = ClusterTarget.resolve(context="A").activate()
    assert first == again


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
    assert cli_args("kubectl", None) == []
    assert cli_args("oc", "A") == ["--context", "A"]
    ClusterTarget.resolve(None).activate()
    assert cli_args("kubectl", None) == ["--context", "B"]
    assert cli_args("helm", "") == ["--kube-context", "B"]
    assert cli_args("oc", "B") == ["--context", "B"]


def test_cli_args_refuses_a_second_context(kubeconfig) -> None:
    ClusterTarget.resolve(None).activate()  # B
    with pytest.raises(ContextConflictError, match="--context A"):
        cli_args("helm", "A")


def test_pin_command_without_any_credentials_pins_nothing(tmp_path, monkeypatch) -> None:
    from lakebench.k8s.target import pin_command

    point_kubeconfig_at(monkeypatch, tmp_path / "missing")
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    assert pin_command(None) is None
    assert target_mod.active_target() is None


def test_existing_but_broken_kubeconfig_never_falls_back_to_in_cluster(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "kubeconfig"
    path.write_text("apiVersion: v1\nkind: Config\ncontexts: []\nclusters: []\nusers: []\n")
    point_kubeconfig_at(monkeypatch, path)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    with pytest.raises(ConfigException):
        ClusterTarget.resolve(None)


def test_in_cluster_target_activates_with_service_account(tmp_path, monkeypatch) -> None:
    point_kubeconfig_at(monkeypatch, tmp_path / "missing")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    with patch.object(target_mod._k8s_config, "load_incluster_config") as load:
        load.side_effect = lambda client_configuration: setattr(
            client_configuration, "host", "https://10.0.0.1:443"
        )
        t = ClusterTarget.current()
    assert t.in_cluster and t.api_server == "https://10.0.0.1:443"
    assert cli_args("kubectl", None) == []


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


@pytest.mark.parametrize(
    "module_cls",
    [
        ("lakebench.modules.query_engines.trino.executor", "TrinoExecutor"),
        ("lakebench.modules.query_engines.duckdb.executor", "DuckDBExecutor"),
        ("lakebench.modules.query_engines.spark_thrift.executor", "SparkThriftExecutor"),
    ],
)
def test_query_executor_prefix_uses_the_active_target(kubeconfig, module_cls) -> None:
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


def test_get_k8s_client_requires_context_or_target(kubeconfig) -> None:
    from lakebench.k8s.client import get_k8s_client

    with pytest.raises(TypeError, match="context= or target="):
        get_k8s_client(namespace="x")


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


def test_k8s_client_conflict_is_not_a_connection_error(kubeconfig) -> None:
    from lakebench.k8s.client import get_k8s_client

    get_k8s_client(context="A")
    with pytest.raises(ContextConflictError):
        get_k8s_client(context="B")


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


def test_status_uses_config_context(kubeconfig, hosts, tmp_path) -> None:
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["status", str(_config_file(tmp_path))])
    assert res.exit_code == 0, res.output
    assert hosts and set(hosts) == {SERVER_A}


def test_status_namespace_without_config_names_the_context(
    kubeconfig, hosts, tmp_path, monkeypatch
) -> None:
    from lakebench.cli import app

    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["status", "--namespace", "some-ns"])
    assert res.exit_code == 0, res.output
    assert "Cluster context: B" in res.output
    assert hosts and set(hosts) == {SERVER_B}


def test_stop_uses_config_context(kubeconfig, hosts, tmp_path) -> None:
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["stop", str(_config_file(tmp_path))])
    assert res.exit_code == 0, res.output
    assert hosts and set(hosts) == {SERVER_A}, res.output


@pytest.mark.parametrize(("context", "expected"), [("A", "A"), ("", "B")])
def test_logs_uses_config_context(kubeconfig, tmp_path, context, expected) -> None:
    from lakebench.cli import app

    with patch("lakebench.k8s._pinned.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        res = CliRunner().invoke(app, ["logs", "hive", str(_config_file(tmp_path, context))])
    assert res.exit_code == 0, res.output
    argv = run.call_args[0][0]
    assert argv[:3] == ["kubectl", "--context", expected]


def test_admin_status_names_and_uses_the_resolved_context(kubeconfig, hosts) -> None:
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["admin", "status"])
    assert res.exit_code == 0, res.output
    assert "Cluster context: B" in res.output
    assert hosts and set(hosts) == {SERVER_B}


def test_release_lock_names_and_uses_the_resolved_context(kubeconfig, hosts) -> None:
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["admin", "release-lock"])
    assert res.exit_code == 0, res.output
    assert "Cluster context: B" in res.output
    assert hosts and set(hosts) == {SERVER_B}


def test_admin_with_config_uses_config_context(kubeconfig, hosts, tmp_path) -> None:
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["admin", "doctor", str(_config_file(tmp_path))])
    assert res.exit_code == 0, res.output
    assert hosts and set(hosts) == {SERVER_A}


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
