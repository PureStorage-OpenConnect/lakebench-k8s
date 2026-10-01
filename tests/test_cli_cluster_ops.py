"""CLI-6 (CC-27): `logs`, `stop` and `status` through the Kubernetes API.

Each command is driven through the real Typer app with fake API objects in
place of ``kubernetes.client.CoreV1Api`` and friends, so the tests see the
selectors, deletions and exit codes the commands produce.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from kubernetes.client.rest import ApiException
from typer.testing import CliRunner
from urllib3.exceptions import MaxRetryError

from lakebench.cli import _cluster_ops as ops
from lakebench.cli import app
from lakebench.exit_codes import ExitCode


def _stderr(result) -> str:
    try:
        return result.stderr
    except ValueError:  # Click < 8.2 without mix_stderr=False
        return result.output


def _stdout(result) -> str:
    try:
        return result.stdout
    except ValueError:
        return result.output


# -- fakes ---------------------------------------------------------------------


def _pod(name: str, minute: int, containers=("main",), annotations=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            creation_timestamp=datetime(2026, 10, 1, 12, minute, tzinfo=timezone.utc),
            annotations=annotations or {},
        ),
        spec=SimpleNamespace(containers=[SimpleNamespace(name=c) for c in containers]),
    )


class _Resp:
    def __init__(self, data: bytes):
        self.data = data
        self.released = False

    def stream(self, amt):
        for i in range(0, len(self.data), 3):  # small chunks: split lines and code points
            yield self.data[i : i + 3]

    def release_conn(self):
        self.released = True


class FakeCore:
    def __init__(self, pods=(), logs=None, list_error=None, read_errors=None):
        self.pods = list(pods)
        self.logs = logs or {}
        self.list_error = list_error
        self.read_errors = read_errors or {}
        self.selectors: list[str] = []
        self.reads: list[tuple[str, dict]] = []

    def list_namespaced_pod(self, namespace, label_selector=""):
        self.selectors.append(label_selector)
        if self.list_error is not None:
            raise self.list_error
        return SimpleNamespace(items=list(self.pods))

    def read_namespaced_pod_log(self, name, namespace, **kwargs):
        self.reads.append((name, kwargs))
        if name in self.read_errors:
            raise self.read_errors[name]
        return _Resp(self.logs.get(name, b""))


class FakeApps:
    def __init__(self, objects=None, errors=None):
        self.objects = objects or {}
        self.errors = errors or {}

    def _read(self, name):
        if name in self.errors:
            raise self.errors[name]
        if name not in self.objects:
            raise ApiException(status=404, reason="Not Found")
        ready, desired = self.objects[name]
        return SimpleNamespace(
            status=SimpleNamespace(ready_replicas=ready),
            spec=SimpleNamespace(replicas=desired),
        )

    def read_namespaced_stateful_set(self, name, namespace):
        return self._read(name)

    def read_namespaced_deployment(self, name, namespace):
        return self._read(name)


class FakeBatch:
    def __init__(self, job=False, read_error=None, delete_error=None):
        self.job = job
        self.read_error = read_error
        self.delete_error = delete_error
        self.deleted: list[tuple[str, Any]] = []

    def read_namespaced_job(self, name, namespace):
        if self.read_error is not None:
            raise self.read_error
        if not self.job:
            raise ApiException(status=404, reason="Not Found")
        return SimpleNamespace(
            status=SimpleNamespace(active=0, succeeded=1), spec=SimpleNamespace(completions=1)
        )

    def delete_namespaced_job(self, name, namespace, body=None):
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append((name, body))


class FakeCustom:
    def __init__(self, apps=(), list_error=None, delete_errors=None):
        self.apps = list(apps)
        self.list_error = list_error
        self.delete_errors = delete_errors or {}
        self.deleted: list[str] = []

    def list_namespaced_custom_object(self, group, version, namespace, plural):
        assert (group, version, plural) == ("sparkoperator.k8s.io", "v1beta2", "sparkapplications")
        if self.list_error is not None:
            raise self.list_error
        return {"items": [{"metadata": {"name": n}} for n in self.apps]}

    def delete_namespaced_custom_object(self, group, version, namespace, plural, name):
        if name in self.delete_errors:
            raise self.delete_errors[name]
        self.deleted.append(name)


class FakeK8s:
    def __init__(self, exists=True, exists_error=None):
        self.exists = exists
        self.exists_error = exists_error

    def namespace_exists(self, name):
        if self.exists_error is not None:
            raise self.exists_error
        return self.exists


@pytest.fixture
def cluster(monkeypatch, tmp_path):
    """Install fakes; return a namespace whose attributes the test sets."""
    import lakebench.cli as cli

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    state = SimpleNamespace(
        k8s=FakeK8s(), core=FakeCore(), apps=FakeApps(), batch=FakeBatch(), custom=FakeCustom()
    )
    monkeypatch.setattr(cli, "get_k8s_client", lambda **_k: state.k8s)
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda: state.core)
    monkeypatch.setattr("kubernetes.client.AppsV1Api", lambda: state.apps)
    monkeypatch.setattr("kubernetes.client.BatchV1Api", lambda: state.batch)
    monkeypatch.setattr("kubernetes.client.CustomObjectsApi", lambda: state.custom)
    state.config = _config(tmp_path)
    return state


def _config(tmp_path: Path, recipe: str = "hive-iceberg-spark-trino") -> Path:
    p = tmp_path / "lakebench.yaml"
    p.write_text(yaml.safe_dump({"name": "ops", "recipe": recipe}))
    return p


def _invoke(*argv):
    return CliRunner().invoke(app, [str(a) for a in argv])


# -- logs: registry ------------------------------------------------------------


def test_stage_list_matches_job_types():
    """Every SparkApplication lakebench submits has a `logs` component."""
    from lakebench.modules.pipeline_engines.spark.job import JobType

    assert set(ops.STAGES) == {j.value for j in JobType}


def test_logs_datagen_selector(cluster):
    cluster.core.pods = [_pod("lakebench-datagen-abc", 1, containers=("datagen",))]
    cluster.core.logs = {"lakebench-datagen-abc": b"generated 10 files\n"}
    r = _invoke("logs", cluster.config, "datagen")
    assert r.exit_code == 0, r.output
    assert cluster.core.selectors == ["job-name=lakebench-datagen"]
    assert _stdout(r) == "generated 10 files\n"
    assert cluster.core.reads[0][1]["container"] == "datagen"


def test_logs_stage_selector(cluster):
    driver = _pod("lakebench-silver-build-driver", 1, containers=("jmx", "spark-kubernetes-driver"))
    cluster.core.pods = [driver]
    cluster.core.logs = {"lakebench-silver-build-driver": b"stage done\n"}
    r = _invoke("logs", cluster.config, "silver-build", "--lines", "7")
    assert r.exit_code == 0, r.output
    assert cluster.core.selectors == [
        "spark-role=driver,sparkoperator.k8s.io/app-name=lakebench-silver-build"
    ]
    _name, kwargs = cluster.core.reads[0]
    # The driver container by name, not the first one listed.
    assert kwargs["container"] == "spark-kubernetes-driver"
    assert kwargs["tail_lines"] == 7 and kwargs["previous"] is False
    assert "follow" not in kwargs


def test_logs_previous_is_passed(cluster):
    cluster.core.pods = [_pod("p", 1)]
    r = _invoke("logs", cluster.config, "trino", "--previous")
    assert r.exit_code == 0, r.output
    assert cluster.core.reads[0][1]["previous"] is True


def test_logs_no_pod_exits_1(cluster):
    r = _invoke("logs", cluster.config, "gold-finalize")
    assert r.exit_code == ExitCode.FAILED, r.output
    assert "no pod for gold-finalize" in _stderr(r)
    assert "lakebench status" in _stderr(r)


def test_logs_api_error_exit4(cluster):
    cluster.core.list_error = ApiException(status=403, reason="Forbidden")
    r = _invoke("logs", cluster.config, "hive")
    assert r.exit_code == ExitCode.PREREQUISITE, r.output
    assert "403 Forbidden" in _stderr(r)


def test_logs_unreachable_exit4(cluster):
    cluster.core.list_error = MaxRetryError(None, "/api/v1/pods", "connection refused")
    r = _invoke("logs", cluster.config, "hive")
    assert r.exit_code == ExitCode.PREREQUISITE, r.output
    assert "Traceback" not in r.output


def test_logs_read_error_on_one_pod_still_reads_the_rest(cluster):
    cluster.core.pods = [_pod("a", 1), _pod("b", 2)]
    cluster.core.logs = {"b": b"from b\n"}
    cluster.core.read_errors = {"a": ApiException(status=400, reason="Bad Request")}
    r = _invoke("logs", cluster.config, "trino-worker")
    assert r.exit_code == ExitCode.PREREQUISITE, r.output
    assert "from b" in _stdout(r)
    assert "pod a: 400 Bad Request" in _stderr(r)


def test_logs_unknown_component_exits_2_with_the_list(cluster):
    r = _invoke("logs", cluster.config, "zookeeper")
    assert r.exit_code == ExitCode.USAGE, r.output
    err = _stderr(r)
    assert "Unknown component: zookeeper" in err
    assert "datagen" in err and "silver-build" in err and "postgres" in err
    assert cluster.core.selectors == []


def test_logs_missing_component_exits_2(cluster):
    r = _invoke("logs", "--file", cluster.config)
    assert r.exit_code == ExitCode.USAGE, r.output
    assert "Name a component" in _stderr(r)


def test_logs_old_argument_order_works_and_says_so(cluster):
    cluster.core.pods = [_pod("p", 1)]
    r = _invoke("logs", "postgres", cluster.config)
    assert r.exit_code == 0, r.output
    assert "1.6 argument order" in _stderr(r)
    assert cluster.core.selectors == ["app.kubernetes.io/component=postgres"]


def test_logs_component_alone_uses_the_default_config(cluster):
    cluster.core.pods = [_pod("p", 1)]
    r = _invoke("logs", "polaris")  # ./lakebench.yaml in the cwd
    assert r.exit_code == 0, r.output
    assert "1.6 argument order" not in _stderr(r)
    assert cluster.core.selectors == ["app.kubernetes.io/component=polaris"]


def test_logs_file_option_and_two_positionals_is_refused(cluster):
    r = _invoke("logs", "--file", cluster.config, cluster.config, "trino")
    assert r.exit_code == ExitCode.USAGE, r.output


def test_logs_several_pods_get_headers_and_clean_stdout(cluster):
    cluster.core.pods = [_pod("new", 5), _pod("old", 1)]
    cluster.core.logs = {"old": b"one\n", "new": b"two\n"}
    r = _invoke("logs", cluster.config, "datagen")
    assert r.exit_code == 0, r.output
    assert _stdout(r) == "one\ntwo\n"  # oldest first, no headers on stdout
    assert "pod old" in _stderr(r) and "pod new" in _stderr(r)


def test_logs_follow_reads_the_newest_pod_only(cluster):
    cluster.core.pods = [_pod("old", 1), _pod("new", 5)]
    cluster.core.logs = {"new": "a\nbé\n".encode()}
    r = _invoke("logs", cluster.config, "datagen", "--follow")
    assert r.exit_code == 0, r.output
    assert [name for name, _ in cluster.core.reads] == ["new"]
    assert cluster.core.reads[0][1]["follow"] is True
    assert _stdout(r) == "a\nbé\n"  # a code point split across chunks survives
    assert "newest of 2 pods" in _stderr(r)


def test_logs_output_is_not_markup_and_bad_bytes_do_not_raise(cluster):
    cluster.core.pods = [_pod("p", 1)]
    cluster.core.logs = {"p": b"[bold]x[/tmp] \xff\x1b[31mred\x1b[0m\n"}
    r = _invoke("logs", cluster.config, "trino")
    assert r.exit_code == 0, r.output
    assert _stdout(r) == "[bold]x[/tmp] �red\n"


def test_logs_container_from_the_default_annotation():
    pod = _pod("p", 1, containers=("vector", "hive"))
    pod.metadata.annotations = {"kubectl.kubernetes.io/default-container": "hive"}
    assert ops.pod_container(pod, None) == "hive"
    assert ops.pod_container(_pod("q", 1, containers=("a", "b")), None) == "a"


def test_logs_uses_no_kubectl(cluster, monkeypatch):
    def boom(*_a, **_k):
        raise AssertionError("logs ran a subprocess")

    monkeypatch.setattr("subprocess.run", boom)
    monkeypatch.setattr("subprocess.Popen", boom)
    cluster.core.pods = [_pod("p", 1)]
    r = _invoke("logs", cluster.config, "trino", "--follow")
    assert r.exit_code == 0, r.output


# -- stop ------------------------------------------------------------------------


def test_stop_deletes_every_lakebench_app_and_the_datagen_job(cluster):
    cluster.custom.apps = ["lakebench-silver-build", "lakebench-gold-refresh", "other-app"]
    cluster.batch.job = True
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    assert sorted(cluster.custom.deleted) == ["lakebench-gold-refresh", "lakebench-silver-build"]
    assert "other-app" not in r.output


def test_stop_datagen_job_deleted(cluster):
    cluster.batch.job = True
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    (name, body), *_ = cluster.batch.deleted
    assert name == "lakebench-datagen"
    assert body.propagation_policy == "Foreground"


def test_stop_403_exits_1(cluster):
    """A refused delete is recorded, the other deletions still run, exit 1."""
    cluster.custom.apps = ["lakebench-bronze-ingest", "lakebench-silver-stream"]
    cluster.custom.delete_errors = {
        "lakebench-bronze-ingest": ApiException(status=403, reason="Forbidden")
    }
    cluster.batch.job = True
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    assert cluster.custom.deleted == ["lakebench-silver-stream"]
    assert [n for n, _ in cluster.batch.deleted] == ["lakebench-datagen"]
    assert "deleting SparkApplication/lakebench-bronze-ingest: 403 Forbidden" in _stderr(r)


def test_stop_404_on_delete_is_not_running(cluster):
    cluster.custom.apps = ["lakebench-gold-refresh"]
    cluster.custom.delete_errors = {
        "lakebench-gold-refresh": ApiException(status=404, reason="Not Found")
    }
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    assert "not running" in _stderr(r)


def test_stop_list_error_still_deletes_the_job_and_exits_1(cluster):
    cluster.custom.list_error = ApiException(status=500, reason="Internal Server Error")
    cluster.batch.job = True
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    assert [n for n, _ in cluster.batch.deleted] == ["lakebench-datagen"]


def test_stop_without_the_spark_crd_is_not_an_error(cluster):
    cluster.custom.list_error = ApiException(status=404, reason="Not Found")
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    assert "Nothing was running" in _stderr(r)


def test_stop_dry_run_deletes_nothing(cluster, monkeypatch):
    called = []
    monkeypatch.setattr(ops, "pre_stop", lambda cfg, k8s: called.append(1))
    cluster.custom.apps = ["lakebench-bronze-ingest"]
    cluster.batch.job = True
    r = _invoke("stop", cluster.config, "--dry-run")
    assert r.exit_code == 0, r.output
    assert cluster.custom.deleted == [] and cluster.batch.deleted == []
    assert called == []
    assert "Would delete SparkApplication/lakebench-bronze-ingest" in _stderr(r)
    assert "Would delete Job/lakebench-datagen" in _stderr(r)


def test_stop_pre_stop_runs_first_and_a_failure_still_deletes(cluster, monkeypatch):
    order = []

    def failing(cfg, k8s):
        order.append(("pre_stop", list(cluster.custom.deleted)))
        raise RuntimeError("drain timed out")

    monkeypatch.setattr(ops, "pre_stop", failing)
    cluster.custom.apps = ["lakebench-gold-refresh"]
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    assert order == [("pre_stop", [])]  # before any deletion
    assert cluster.custom.deleted == ["lakebench-gold-refresh"]
    assert "drain timed out" in _stderr(r)


def test_stop_missing_namespace_is_nothing_to_stop(cluster):
    cluster.k8s.exists = False
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    assert "nothing to stop" in _stderr(r)


def test_stop_unreachable_exits_4(cluster):
    cluster.custom.list_error = MaxRetryError(None, "/apis", "connection refused")
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output


# -- status ------------------------------------------------------------------------

_TRINO_HIVE = {
    "lakebench-postgres": (1, 1),
    "lakebench-hive-metastore-default": (1, 1),
    "lakebench-trino-coordinator": (1, 1),
    "lakebench-trino-worker": (2, 2),
}


def test_status_ok_exits_0(cluster):
    cluster.apps.objects = dict(_TRINO_HIVE)
    r = _invoke("status", cluster.config)
    assert r.exit_code == 0, r.output
    assert "Every listed component is ready" in _stderr(r)


def test_status_missing_ns_exit1(cluster):
    cluster.k8s.exists = False
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    err = _stderr(r)
    assert "namespace ops does not exist" in err
    assert "lakebench deploy" in err


def test_status_unready_exit1(cluster):
    cluster.apps.objects = dict(_TRINO_HIVE, **{"lakebench-trino-worker": (1, 2)})
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    assert "Drift: lakebench-trino-worker" in _stderr(r)


def test_status_missing_component_exit1(cluster):
    objects = dict(_TRINO_HIVE)
    del objects["lakebench-hive-metastore-default"]
    cluster.apps.objects = objects
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    assert "lakebench-hive-metastore-default" in _stderr(r)


def test_status_read_error_exits_4(cluster):
    cluster.apps.objects = dict(_TRINO_HIVE)
    cluster.apps.errors = {"lakebench-postgres": ApiException(status=403, reason="Forbidden")}
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output


def test_status_drift_wins_over_a_read_error(cluster):
    cluster.apps.objects = dict(_TRINO_HIVE, **{"lakebench-trino-worker": (0, 2)})
    cluster.apps.errors = {"lakebench-postgres": ApiException(status=500, reason="Error")}
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output


def test_status_unreachable_exits_4(cluster):
    cluster.apps.errors = {"lakebench-postgres": MaxRetryError(None, "/apis", "connection refused")}
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output
    assert "Traceback" not in r.output


def test_status_namespace_read_forbidden_exits_4(cluster):
    from lakebench.k8s import K8sResourceError

    cluster.k8s.exists_error = K8sResourceError("Error checking namespace: (403)")
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output


def test_status_namespace_only_absent_components_are_not_drift(cluster, monkeypatch):
    from lakebench.k8s import target as target_mod

    monkeypatch.setattr(
        target_mod.ClusterTarget, "current", classmethod(lambda cls: target_mod.ClusterTarget("B"))
    )
    cluster.apps.objects = {"lakebench-postgres": (1, 1), "lakebench-polaris": (1, 1)}
    r = _invoke("status", "--namespace", "some-ns")
    assert r.exit_code == 0, r.output


def test_status_namespace_only_with_nothing_found_is_drift(cluster, monkeypatch):
    from lakebench.k8s import target as target_mod

    monkeypatch.setattr(
        target_mod.ClusterTarget, "current", classmethod(lambda cls: target_mod.ClusterTarget("B"))
    )
    r = _invoke("status", "--namespace", "some-ns")
    assert r.exit_code == ExitCode.FAILED, r.output
    assert "no lakebench component found" in _stderr(r)


def test_status_namespace_only_unready_is_drift(cluster, monkeypatch):
    from lakebench.k8s import target as target_mod

    monkeypatch.setattr(
        target_mod.ClusterTarget, "current", classmethod(lambda cls: target_mod.ClusterTarget("B"))
    )
    cluster.apps.objects = {"lakebench-postgres": (0, 1)}
    r = _invoke("status", "--namespace", "some-ns")
    assert r.exit_code == ExitCode.FAILED, r.output


def test_status_scaled_to_zero_is_drift():
    apps = FakeApps({"x": (0, 0)})
    assert ops.read_component(apps, "ns", "x", "Deployment").state == "unready"
