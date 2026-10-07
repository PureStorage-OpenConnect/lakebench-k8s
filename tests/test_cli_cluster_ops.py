"""CLI-6 (CC-27): `logs`, `stop` and `status` through the Kubernetes API.

Each command is driven through the real Typer app with fake API objects in
place of ``kubernetes.client.CoreV1Api`` and friends, so the tests see the
selectors, deletions and exit codes the commands produce.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException
from typer.testing import CliRunner
from urllib3.exceptions import MaxRetryError

from lakebench.cli import _cluster_ops as ops
from lakebench.cli import app
from lakebench.exit_codes import ExitCode
from tests.fixtures.cli_cluster_ops_helpers import _TRINO_HIVE as _TRINO_HIVE
from tests.fixtures.cli_cluster_ops_helpers import FakeApps as FakeApps
from tests.fixtures.cli_cluster_ops_helpers import FakeBatch as FakeBatch
from tests.fixtures.cli_cluster_ops_helpers import FakeCore as FakeCore
from tests.fixtures.cli_cluster_ops_helpers import FakeCustom as FakeCustom
from tests.fixtures.cli_cluster_ops_helpers import FakeK8s as FakeK8s
from tests.fixtures.cli_cluster_ops_helpers import _config as _config
from tests.fixtures.cli_cluster_ops_helpers import _Resp as _Resp


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


def _invoke(*argv):
    return CliRunner().invoke(app, [str(a) for a in argv])


# -- logs: registry ------------------------------------------------------------


def test_logs_datagen_selector(cluster):
    cluster.core.pods = [_pod("lakebench-datagen-abc", 1, containers=("datagen",))]
    cluster.core.logs = {"lakebench-datagen-abc": b"generated 10 files\n"}
    r = _invoke("logs", cluster.config, "datagen")
    assert r.exit_code == 0, r.output
    assert cluster.core.selectors == ["job-name=lakebench-datagen"]
    assert _stdout(r) == "generated 10 files\n"
    assert cluster.core.reads[0][1]["container"] == "datagen"


def test_logs_releases_each_connection(cluster):
    cluster.core.pods = [_pod("a", 1), _pod("b", 2)]
    r = _invoke("logs", cluster.config, "trino-worker")
    assert r.exit_code == 0, r.output
    assert len(cluster.core.responses) == 2
    assert all(resp.released for resp in cluster.core.responses)


def test_logs_several_pods_get_headers_and_clean_stdout(cluster):
    cluster.core.pods = [_pod("new", 5), _pod("old", 1)]
    cluster.core.logs = {"old": b"one\n", "new": b"two\n"}
    r = _invoke("logs", cluster.config, "datagen")
    assert r.exit_code == 0, r.output
    assert _stdout(r) == "one\ntwo\n"  # oldest first, no headers on stdout
    assert "pod old" in _stderr(r) and "pod new" in _stderr(r)


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


def test_stop_list_error_still_deletes_the_job_and_exits_1(cluster):
    cluster.custom.list_error = ApiException(status=500, reason="Internal Server Error")
    cluster.batch.job = True
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    assert [n for n, _ in cluster.batch.deleted] == ["lakebench-datagen"]


def test_stop_leaves_finished_jobs_and_their_logs(cluster):
    """A finished stage's driver pod holds its failure logs; stop keeps it."""
    cluster.custom.apps = ["lakebench-silver-build", "lakebench-gold-refresh"]
    cluster.custom.states = {
        "lakebench-silver-build": "FAILED",
        "lakebench-gold-refresh": "RUNNING",
    }
    cluster.batch.job = True
    cluster.batch.finished = "Complete"
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    assert cluster.custom.deleted == ["lakebench-gold-refresh"]
    assert cluster.batch.deleted == []
    err = _stderr(r)
    assert "left in place: SparkApplication/lakebench-silver-build (FAILED)" in err
    assert "left in place: Job/lakebench-datagen (Complete)" in err


@pytest.mark.parametrize(
    ("state", "deleted"),
    [
        ("COMPLETED", False),
        ("FAILED", False),
        # The operator resubmits from these (restartPolicy Always / OnFailure
        # with submission retries), or the app still runs.
        ("SUBMISSION_FAILED", True),
        ("PENDING_RERUN", True),
        ("FAILING", True),
        ("SUCCEEDING", True),
        ("RUNNING", True),
        ("UNKNOWN", True),
        ("", True),  # no status yet
    ],
)
def test_stop_deletes_every_app_state_but_completed_and_failed(cluster, state, deleted):
    cluster.custom.apps = ["lakebench-silver-stream"]
    if state:
        cluster.custom.states = {"lakebench-silver-stream": state}
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    assert (cluster.custom.deleted == ["lakebench-silver-stream"]) is deleted


def test_stop_dry_run_with_a_list_failure_exits_1(cluster):
    cluster.custom.list_error = ApiException(status=403, reason="Forbidden")
    r = _invoke("stop", cluster.config, "--dry-run")
    assert r.exit_code == ExitCode.FAILED, r.output


def test_stop_unreachable_at_delete_exits_1(cluster):
    cluster.custom.apps = ["lakebench-bronze-ingest", "lakebench-silver-stream"]
    cluster.custom.delete_errors = {
        "lakebench-bronze-ingest": MaxRetryError(None, "/apis", "connection reset")
    }
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    assert cluster.custom.deleted == ["lakebench-silver-stream"]


def test_stop_journal_records_failure(cluster, monkeypatch):
    import lakebench.cli as cli

    ends = []

    class _J:
        def begin_command(self, *a, **k):
            pass

        def record(self, *a, **k):
            pass

        def end_command(self, success, message=""):
            ends.append(success)

    monkeypatch.setattr(cli, "journal_open", lambda *a, **k: _J())
    cluster.custom.apps = ["lakebench-gold-refresh"]
    cluster.custom.delete_errors = {"lakebench-gold-refresh": ApiException(status=500, reason="x")}
    _invoke("stop", cluster.config)
    cluster.custom.delete_errors = {}
    _invoke("stop", cluster.config)
    assert ends == [False, True]


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


def test_stop_unreachable_exits_4(cluster):
    cluster.custom.list_error = MaxRetryError(None, "/apis", "connection refused")
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output


# -- status ------------------------------------------------------------------------


def test_status_ok_exits_0(cluster):
    cluster.apps.objects = dict(_TRINO_HIVE)
    r = _invoke("status", cluster.config)
    assert r.exit_code == 0, r.output
    assert "Every listed component is ready" in _stderr(r)


def test_status_missing_ns_exit1(cluster):
    cluster.core.ns_exists = False
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
    # The hint names a component `logs` accepts, not the object name.
    # The exact line: no --name suffix for a config run without --name.
    hint = (
        f"Next: lakebench logs {cluster.config} trino-worker, or lakebench deploy {cluster.config}"
    )
    assert hint in " ".join(_stderr(r).split())


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
    cluster.core.ns_error = ApiException(status=403, reason="Forbidden")
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output
    assert "Kubernetes API error: cannot read namespace ops: 403 Forbidden" in _stderr(r)


def test_stop_namespace_read_forbidden_exits_4(cluster):
    cluster.core.ns_error = ApiException(status=403, reason="Forbidden")
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output
    assert "Kubernetes API error" in _stderr(r)
    assert cluster.custom.deleted == []


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
