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


def test_deploy_capacity_check_passes_on_a_large_cluster(recording_k8s, cfg_file):
    from lakebench.cli._prerequisites import deploy_capacity_check

    cfg = _seed(
        recording_k8s,
        cfg_file,
        [_node(f"w{i}", "40", "402Gi") for i in range(8)],
    )
    result = deploy_capacity_check(cfg)
    assert result.passed, result.message
    assert "Cluster capacity OK" in result.message
    recording_k8s.assert_recorded(kind="nodes", verb="list")
    assert recording_k8s.mutations() == []


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


def test_peak_error_names_its_exception_type(cfg_file):
    from lakebench.cli._prerequisites import _check_cluster_capacity
    from lakebench.config._load_context import LoadPurpose
    from lakebench.config.loader import load_config
    from lakebench.k8s.client import ClusterCapacity

    cfg = load_config(cfg_file, purpose=LoadPurpose.MUTATE, print_notes=False)
    k8s = mock.MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        434_000, 4349 * GIB, 8, 40_000, 402 * GIB
    )
    with (
        mock.patch("lakebench.k8s.get_k8s_client", return_value=k8s),
        mock.patch(
            "lakebench.modules.pipeline_engines.spark.job.compute_peak_requirements",
            side_effect=KeyError("silver-build"),
        ),
    ):
        result = _check_cluster_capacity(cfg)
    assert not result.passed
    assert "KeyError: 'silver-build'" in result.message


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


def test_run_skip_deploy_skips_only_the_infra_check(monkeypatch, cfg_file):
    passing = PrereqReport()
    passing.checks.append(PrereqResult(name="cluster-capacity", passed=True, message="OK"))
    monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", lambda cfg, **kw: passing)
    infra = mock.Mock()
    monkeypatch.setattr("lakebench.cli._run._run_preflight_infra_check", infra)
    monkeypatch.setattr(
        "lakebench.cli._run.no_query_engine_skip",
        mock.Mock(side_effect=SystemExit(99)),
    )
    with mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("no cluster")):
        res = CliRunner().invoke(app, ["run", str(cfg_file), "--skip-benchmark", "--skip-deploy"])
    assert "All prerequisites passed" in res.output, res.output
    assert "Skipping the infrastructure readiness check (--skip-deploy)" in res.output
    infra.assert_not_called()


def test_dry_run_previews_the_refusal(recording_k8s, cfg_file):
    _seed(recording_k8s, cfg_file, [_node("w1", "16", "64Gi")])
    res = CliRunner().invoke(app, ["deploy", str(cfg_file), "--dry-run"])
    assert "deploy would refuse it, exit 4" in res.output, res.output
    assert recording_k8s.mutations() == []


def test_deploy_check_leaves_datagen_out(cfg_file):
    # Deploy does not generate: it checks the pipeline and always-on pods.
    from lakebench.cli import _prerequisites
    from lakebench.config._load_context import LoadPurpose
    from lakebench.config.loader import load_config

    cfg = load_config(cfg_file, purpose=LoadPurpose.MUTATE, print_notes=False)
    with mock.patch.object(_prerequisites, "_check_cluster_capacity") as check:
        _prerequisites.deploy_capacity_check(cfg)
    check.assert_called_once_with(cfg, datagen_runs=False, fail_closed=False)


def test_deploy_refuses_a_cluster_whose_capacity_is_taken(recording_k8s, cfg_file):
    # Eight 40-core / 402 GiB workers hold the scale-1 peak by total, but
    # another namespace's pods request most of every node: deploy reads free
    # capacity, as run's preflight does (CC-24), and refuses.
    from lakebench.cli._prerequisites import deploy_capacity_check

    cfg = _seed(recording_k8s, cfg_file, [_node(f"w{i}", "40", "402Gi") for i in range(8)])
    for i in range(8):
        recording_k8s.add(
            "pods",
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": f"busy-{i}", "namespace": "someone-else"},
                "spec": {
                    "nodeName": f"w{i}",
                    "containers": [
                        {"name": "c", "resources": {"requests": {"cpu": "38", "memory": "390Gi"}}}
                    ],
                },
                "status": {"phase": "Running"},
            },
            namespace="someone-else",
        )
    result = deploy_capacity_check(cfg)
    assert not result.passed, result.message
    assert "Insufficient free cluster capacity" in result.message
    recording_k8s.assert_recorded(kind="pods", verb="list")
    assert recording_k8s.mutations() == []
