"""``deploy`` refuses a config the cluster cannot hold before it creates
anything, and ``run --skip-deploy`` still runs the read-only prerequisite
checks (LB-247)."""

from __future__ import annotations

from unittest import mock

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.cli._prerequisites import PrereqReport, PrereqResult
from lakebench.exit_codes import ExitCode

GIB = 1024**3

CONFIG = """\
name: lb247-cap
recipe: polaris-iceberg-spark-trino
platform:
  storage:
    s3:
      endpoint: http://10.0.1.50:80
      access_key: a
      secret_key: b
architecture:
  pipeline:
    mode: batch
workload:
  schema: customer360
  datagen:
    scale: 1
"""


def _node(name, cpu, memory, control_plane=False):
    labels = {"node-role.kubernetes.io/control-plane": ""} if control_plane else {}
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": name, "labels": labels},
        "spec": {},
        "status": {
            "allocatable": {"cpu": cpu, "memory": memory},
            # Ready: the capacity check counts schedulable nodes only (CC-24).
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


@pytest.fixture
def cfg_file(tmp_path):
    path = tmp_path / "lb247-cap.yaml"
    path.write_text(CONFIG)
    return path


def _seed(recording_k8s, cfg_file, nodes):
    from lakebench.config._load_context import LoadPurpose
    from lakebench.config.loader import load_config

    cfg = load_config(cfg_file, purpose=LoadPurpose.MUTATE, print_notes=False)
    recording_k8s.for_config(cfg)
    for n in nodes:
        recording_k8s.add("nodes", n)
    return cfg


def test_deploy_refuses_a_cluster_too_small_and_creates_nothing(recording_k8s, cfg_file):
    # Two 16-core / 64 GiB workers: the scale-1 peak (36 cores, 525 GB) does not fit.
    _seed(
        recording_k8s,
        cfg_file,
        [_node("w1", "16", "64Gi"), _node("w2", "16", "64Gi"), _node("m", "8", "32Gi", True)],
    )
    res = CliRunner().invoke(app, ["deploy", str(cfg_file), "-y"])
    assert res.exit_code == ExitCode.PREREQUISITE, res.output
    assert "cluster-capacity" in res.output and "Nothing was created" in res.output
    # The check read the nodes, and nothing was created, patched or deleted.
    recording_k8s.assert_recorded(kind="nodes", verb="list")
    assert recording_k8s.mutations() == []
    assert not (cfg_file.parent / ".lakebench").exists()


def test_deploy_capacity_check_sizes_a_copy(recording_k8s, cfg_file):
    from lakebench.cli._prerequisites import deploy_capacity_check

    cfg = _seed(recording_k8s, cfg_file, [_node(f"w{i}", "40", "402Gi") for i in range(8)])
    before = cfg.model_dump()
    deploy_capacity_check(cfg)
    assert cfg.model_dump() == before


def test_unreachable_cluster_does_not_block_deploy_preflight(cfg_file):
    from lakebench.cli._prerequisites import deploy_capacity_check
    from lakebench.config._load_context import LoadPurpose
    from lakebench.config.loader import load_config

    cfg = load_config(cfg_file, purpose=LoadPurpose.MUTATE, print_notes=False)
    with mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("no cluster")):
        result = deploy_capacity_check(cfg)
    assert result.passed and "skipped" in result.message


# -- run --skip-deploy ---------------------------------------------------------


def _failing_report():
    report = PrereqReport()
    report.checks.append(
        PrereqResult(name="cluster-capacity", passed=False, message="too small", hint="")
    )
    return report


@pytest.mark.parametrize(
    ("flags", "checked"), [(["--skip-deploy"], True), (["--skip-preflight"], False)]
)
def test_run_skip_deploy_still_runs_the_prerequisites(monkeypatch, cfg_file, flags, checked):
    calls = []

    def fake(cfg, **kw):
        calls.append(kw)
        return _failing_report()

    monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", fake)
    # The corpus check reads S3 (tests/test_c360_series_run.py covers it).
    monkeypatch.setattr("lakebench.cli._run._check_series_reuse", lambda *a, **k: None)
    # Past the prerequisites the run would need a cluster; stop it there.
    monkeypatch.setattr(
        "lakebench.cli._run.no_query_engine_skip",
        mock.Mock(side_effect=SystemExit(99)),
    )
    with mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("no cluster")):
        res = CliRunner().invoke(app, ["run", str(cfg_file), "--skip-benchmark", *flags])
    assert bool(calls) is checked, res.output
    if checked:
        assert res.exit_code == ExitCode.PREREQUISITE, res.output
        assert "Skipping the infrastructure readiness check" not in res.output
    else:
        assert res.exit_code != ExitCode.PREREQUISITE, res.output
        assert "Skipping prerequisites (--skip-preflight)" in res.output


def _client(nodes, *, pod_error=None):
    """A real K8sClient over fake node and pod lists (no scratch published)."""
    from types import SimpleNamespace as NS

    from lakebench.k8s.client import K8sClient

    core = mock.MagicMock()
    core.list_node.return_value = NS(items=list(nodes))
    if pod_error is not None:
        core.list_pod_for_all_namespaces.side_effect = pod_error
    else:
        core.list_pod_for_all_namespaces.return_value = NS(items=[])
    storage = mock.MagicMock()
    storage.list_csi_storage_capacity_for_all_namespaces.return_value = NS(items=[])
    k = object.__new__(K8sClient)
    k._core_v1 = core
    k._storage_v1 = storage
    return k


def _worker(name, cpu, mem):
    from types import SimpleNamespace as NS

    return NS(
        metadata=NS(name=name, labels={}),
        spec=NS(unschedulable=False, taints=[]),
        status=NS(
            allocatable={"cpu": cpu, "memory": mem},
            conditions=[NS(type="Ready", status="True")],
        ),
    )


def _deploy_check(cfg_file, client):
    from lakebench.cli._prerequisites import deploy_capacity_check
    from lakebench.config._load_context import LoadPurpose
    from lakebench.config.loader import load_config

    cfg = load_config(cfg_file, purpose=LoadPurpose.MUTATE, print_notes=False)
    with mock.patch("lakebench.k8s.get_k8s_client", return_value=client):
        return deploy_capacity_check(cfg)


def test_deploy_refuses_an_unreadable_node_quantity(cfg_file):
    result = _deploy_check(cfg_file, _client([_worker("w1", "40", "402 gigs")]))
    assert not result.passed, result.message
    assert "402 gigs" in result.message


def test_deploy_sizes_against_the_worker_allocatable(cfg_file):
    # Sized as the deploy engine and run size it: get_cluster_capacity, not
    # the free-capacity node set.
    from lakebench.config import sizing

    client = _client([_worker(f"w{i}", "40", "402Gi") for i in range(8)])
    workers = client.get_cluster_capacity()
    with mock.patch.object(sizing, "check_capacity", wraps=sizing.check_capacity) as check:
        assert _deploy_check(cfg_file, client).passed
    assert check.call_args.kwargs["sizing_capacity"] == workers
