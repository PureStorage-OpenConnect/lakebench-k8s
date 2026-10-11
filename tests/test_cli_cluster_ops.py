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
from tests.fixtures import cli_cluster_ops_helpers as co

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
        k8s=co.FakeK8s(),
        core=co.FakeCore(),
        apps=co.FakeApps(),
        batch=co.FakeBatch(),
        custom=co.FakeCustom(),
    )
    monkeypatch.setattr(cli, "get_k8s_client", lambda **_k: state.k8s)
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda: state.core)
    monkeypatch.setattr("kubernetes.client.AppsV1Api", lambda: state.apps)
    monkeypatch.setattr("kubernetes.client.BatchV1Api", lambda: state.batch)
    monkeypatch.setattr("kubernetes.client.CustomObjectsApi", lambda: state.custom)
    state.config = co._config(tmp_path)
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
    assert co.stdout(r) == "generated 10 files\n"
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
    assert co.stdout(r) == "one\ntwo\n"  # oldest first, no headers on stdout
    assert "pod old" in co.stderr(r) and "pod new" in co.stderr(r)


# -- stop ------------------------------------------------------------------------


def test_stop_deletes_every_lakebench_app_and_the_datagen_job(cluster):
    cluster.custom.apps = ["lakebench-silver-build", "lakebench-gold-refresh", "other-app"]
    cluster.batch.job = True
    r = _invoke("stop", cluster.config)
    assert r.exit_code == 0, r.output
    assert sorted(cluster.custom.deleted) == ["lakebench-gold-refresh", "lakebench-silver-build"]
    (name, body), *rest = cluster.batch.deleted
    assert name == "lakebench-datagen" and not rest
    assert body.propagation_policy == "Foreground"


_APPS = ["lakebench-bronze-ingest", "lakebench-silver-stream"]


@pytest.mark.parametrize(
    ("faults", "argv", "apps_deleted", "jobs_deleted"),
    [
        # a refused delete is recorded, the other deletions still run
        (
            {"delete_errors": {_APPS[0]: ApiException(status=403, reason="Forbidden")}},
            [],
            [_APPS[1]],
            ["lakebench-datagen"],
        ),
        (
            {"delete_errors": {_APPS[0]: MaxRetryError(None, "/apis", "connection reset")}},
            [],
            [_APPS[1]],
            None,
        ),
        # a failed listing does not stop the datagen job being deleted
        (
            {"list_error": ApiException(status=500, reason="Internal Server Error")},
            [],
            [],
            ["lakebench-datagen"],
        ),
        # a dry run that cannot list still fails, and deletes nothing
        (
            {"list_error": ApiException(status=403, reason="Forbidden")},
            ["--dry-run"],
            [],
            [],
        ),
    ],
)
def test_stop_with_an_api_fault_exits_1_after_deleting_what_it_can(
    cluster, faults, argv, apps_deleted, jobs_deleted
):
    cluster.custom.apps = list(_APPS)
    cluster.batch.job = True
    for attr, value in faults.items():
        setattr(cluster.custom, attr, value)
    r = _invoke("stop", cluster.config, *argv)
    assert r.exit_code == ExitCode.FAILED, r.output
    assert cluster.custom.deleted == apps_deleted
    if jobs_deleted is not None:
        assert [n for n, _ in cluster.batch.deleted] == jobs_deleted


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
    err = co.stderr(r)
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
    assert "Would delete SparkApplication/lakebench-bronze-ingest" in co.stderr(r)
    assert "Would delete Job/lakebench-datagen" in co.stderr(r)


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
    assert "drain timed out" in co.stderr(r)


def test_stop_unreachable_exits_4(cluster):
    cluster.custom.list_error = MaxRetryError(None, "/apis", "connection refused")
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output


# -- status ------------------------------------------------------------------------


def test_status_ok_exits_0(cluster):
    cluster.apps.objects = dict(co._TRINO_HIVE)
    r = _invoke("status", cluster.config)
    assert r.exit_code == 0, r.output


def test_status_missing_ns_exit1(cluster):
    cluster.core.ns_exists = False
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output


def test_status_unready_exit1(cluster):
    cluster.apps.objects = dict(co._TRINO_HIVE, **{"lakebench-trino-worker": (1, 2)})
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    # The hint names a component `logs` accepts, not the object name.
    # The exact line: no --name suffix for a config run without --name.
    hint = (
        f"Next: lakebench logs {cluster.config} trino-worker, or lakebench deploy {cluster.config}"
    )
    assert hint in " ".join(co.stderr(r).split())


def test_status_missing_component_exit1(cluster):
    objects = dict(co._TRINO_HIVE)
    del objects["lakebench-hive-metastore-default"]
    cluster.apps.objects = objects
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.FAILED, r.output
    assert "lakebench-hive-metastore-default" in co.stderr(r)


def test_status_read_error_exits_4(cluster):
    cluster.apps.objects = dict(co._TRINO_HIVE)
    cluster.apps.errors = {"lakebench-postgres": ApiException(status=403, reason="Forbidden")}
    r = _invoke("status", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output


def test_status_drift_wins_over_a_read_error(cluster):
    cluster.apps.objects = dict(co._TRINO_HIVE, **{"lakebench-trino-worker": (0, 2)})
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
    assert "Kubernetes API error: cannot read namespace ops: 403 Forbidden" in co.stderr(r)


def test_stop_namespace_read_forbidden_exits_4(cluster):
    cluster.core.ns_error = ApiException(status=403, reason="Forbidden")
    r = _invoke("stop", cluster.config)
    assert r.exit_code == ExitCode.PREREQUISITE, r.output
    assert "Kubernetes API error" in co.stderr(r)
    assert cluster.custom.deleted == []


@pytest.fixture
def namespace_only(monkeypatch):
    """`status --namespace` with no config: the cluster target is current."""
    from lakebench.k8s import target as target_mod

    monkeypatch.setattr(
        target_mod.ClusterTarget, "current", classmethod(lambda cls: target_mod.ClusterTarget("B"))
    )


@pytest.mark.parametrize(
    ("objects", "exit_code"),
    [
        # components the recipe does not use are absent, not drift
        ({"lakebench-postgres": (1, 1), "lakebench-polaris": (1, 1)}, 0),
        ({}, ExitCode.FAILED),  # nothing found
        ({"lakebench-postgres": (0, 1)}, ExitCode.FAILED),  # unready
    ],
)
def test_status_namespace_only_drift(cluster, namespace_only, objects, exit_code):
    cluster.apps.objects = objects
    r = _invoke("status", "--namespace", "some-ns")
    assert r.exit_code == exit_code, r.output


def test_status_scaled_to_zero_is_drift():
    apps = co.FakeApps({"x": (0, 0)})
    assert ops.read_component(apps, "ns", "x", "Deployment").state == "unready"
