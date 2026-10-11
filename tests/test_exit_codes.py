"""CLI-1 (CC-8): one exit-code enum, a top-level handler, a generated table.

The expected code values below are copied from the target UX design table
(TUD 4.3), not from ``lakebench.exit_codes``, so a renumbering in the module
fails here.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer
from typer.testing import CliRunner

from lakebench import exit_codes
from lakebench.cli import _exit as cli_exit
from lakebench.cli import app
from lakebench.deploy.datagen import stop_previous_datagen as _real_stop_previous_datagen
from lakebench.exit_codes import (
    ExitCode,
    Incomplete,
    LakebenchError,
    NotConfirmed,
    PrerequisiteError,
    SafetyRefusal,
    UsageError,
)

ROOT = Path(__file__).resolve().parents[1]

# TUD 4.3, by hand.
TUD_CODES = {
    "OK": 0,
    "FAILED": 1,
    "USAGE": 2,
    "REFUSED": 3,
    "PREREQUISITE": 4,
    "NOT_CONFIRMED": 5,
    "INCOMPLETE": 6,
    "REQUIREMENT_UNMET": 14,
    "INTERRUPTED": 130,
}


def _runner() -> CliRunner:
    # init writes the S3 keys as ${VAR} references; the scenarios that load
    # its output need them set, as a user who followed init's "next" would.
    return CliRunner(
        env={"LAKEBENCH_S3_ACCESS_KEY": "placeholder", "LAKEBENCH_S3_SECRET_KEY": "placeholder"}
    )


def _stderr(result) -> str:
    try:
        return result.stderr
    except ValueError:  # Click < 8.2 without mix_stderr=False
        return result.output


# -- the enum and the module ---------------------------------------------------


def test_exit_code_values_match_tud():
    assert {m.name: int(m) for m in ExitCode} == TUD_CODES


def test_error_class_codes():
    for cls, code in [
        (LakebenchError, 1),
        (UsageError, 2),
        (SafetyRefusal, 3),
        (PrerequisiteError, 4),
        (NotConfirmed, 5),
        (Incomplete, 6),
    ]:
        assert int(cls("x").code) == code


# -- refusals reported as failed step results ---------------------------------
# The producers' own tests (test_destroy_namespace_wait, test_destroy_bucket_delete,
# test_destroy_legacy_refuse, test_destroy_unwatches_namespace, test_deploy) check
# that each real refusal carries the flag; these check the rule.


def _result(component: str, status: str, message: str = "", **details):
    from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

    return DeploymentResult(component, DeploymentStatus(status), message, details=details)


_REFUSED = {exit_codes.REFUSAL_DETAIL: "deploy.identity_foreign"}
_FOLLOWS = {exit_codes.FOLLOWS_REFUSAL_DETAIL: True}


def test_refusal_results_exit_refused():
    results = [_result("postgres", "success"), _result("s3-buckets", "failed", **_REFUSED)]
    assert cli_exit.refused_result_code(results) == ExitCode.REFUSED


def test_a_real_failure_is_never_hidden_by_a_refusal():
    refused = _result("s3-buckets", "failed", **_REFUSED)
    kept = _result("namespace", "failed", **_FOLLOWS)
    broken = _result("trino", "failed", "helm uninstall failed")
    assert cli_exit.refused_result_code([refused, kept]) == ExitCode.REFUSED
    assert cli_exit.refused_result_code([refused, broken]) is None
    assert cli_exit.refused_result_code([kept]) is None  # a consequence alone is a failure


def test_refusal_text_without_the_flag_is_a_failure():
    """Classification reads the flag, never the wording (CLI-2 rewrites text)."""
    text_only = _result("namespace", "failed", "Destroy NOT completed: a redeploy")
    assert cli_exit.refused_result_code([text_only]) is None
    ok = _result("namespace", "success", **_REFUSED)  # a flag on a step that did not fail
    assert cli_exit.refused_result_code([ok]) is None


def test_cluster_lock_held_maps_to_refused():
    from lakebench.deploy.cluster_lock import ClusterLockHeld

    exc = ClusterLockHeld("h@u@s", "2026-10-01T00:00:00Z", 300, "2026-10-01T00:05:00Z")
    err = cli_exit.error_for(exc)
    assert err is not None and err.code == ExitCode.REFUSED and err.path == "lease.held"


# -- the handler, on a throwaway app that uses the same group class ----------


def _probe_app(exc_factory: Callable[[], BaseException]) -> typer.Typer:
    probe = typer.Typer(cls=cli_exit.LakebenchGroup, pretty_exceptions_enable=False)

    @probe.command()
    def boom() -> None:
        raise exc_factory()

    @probe.command()
    def other() -> None:  # a second command keeps `boom` a subcommand
        pass

    return probe


def _config_validation_error():
    from lakebench.config import ConfigValidationError

    return ConfigValidationError(
        "2 errors", errors=[{"loc": ("name",), "msg": "required"}, {"loc": ("a", 0), "msg": "bad"}]
    )


def _config_error():
    from lakebench.config import ConfigError

    return ConfigError("cannot parse c.yaml")


def _k8s_error():
    from lakebench.k8s import K8sConnectionError

    return K8sConnectionError("connection refused")


def _kubeconfig_error():
    from kubernetes.config import ConfigException

    return ConfigException("Invalid kube-config file. No configuration found.")


def test_handler_maps_exception():
    for factory, code in [
        (lambda: SafetyRefusal("refused"), 3),
        (lambda: PrerequisiteError("missing"), 4),
        (lambda: Incomplete("still going"), 6),
        (lambda: LakebenchError("custom", code=ExitCode.REQUIREMENT_UNMET), 14),
        (_config_validation_error, 2),
        (_config_error, 2),
        (_k8s_error, 4),
        (_kubeconfig_error, 4),
        (lambda: typer.Abort(), 5),
        (lambda: EOFError(), 1),
        (lambda: KeyboardInterrupt(), 130),
        (lambda: RuntimeError("unexpected"), 1),
        (lambda: typer.Exit(7), 7),
        (lambda: SystemExit(9), 9),
    ]:
        result = _runner().invoke(_probe_app(factory), ["boom"])
        assert result.exit_code == code, result.output


# -- named paths on the real CLI -----------------------------------------------


def _scenario_version_ok(monkeypatch, tmp_path):
    return _runner().invoke(app, ["version"])


def _scenario_click_usage(monkeypatch, tmp_path):
    return _runner().invoke(app, ["deploy", "--no-such-flag"])


def _scenario_unhandled_exception(monkeypatch, tmp_path):
    import lakebench.cli as cli

    def explode(*_a, **_k):
        raise RuntimeError("unexpected [/tmp] failure")

    monkeypatch.setattr(cli, "load_config", explode)
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["status", str(tmp_path / "c.yaml")])


def _scenario_confirm_non_tty(monkeypatch, tmp_path):
    import lakebench.cli._deploy as deploy_mod

    runner = _runner()
    init = runner.invoke(app, ["init", "--output", str(tmp_path / "c.yaml")])
    assert init.exit_code == 0, init.output
    # Stop before any cluster call: the prompt is the first thing left.
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)
    return runner.invoke(app, ["deploy", str(tmp_path / "c.yaml")], input="")


def _scenario_financial_k8s_unreachable(monkeypatch, tmp_path):
    from types import SimpleNamespace

    import lakebench.cli._financial as fin
    import lakebench.k8s as k8s_mod
    from lakebench.k8s import K8sConnectionError

    cfg = SimpleNamespace(
        platform=SimpleNamespace(kubernetes=SimpleNamespace(context=None)),
        get_namespace=lambda: "ns-x",
    )
    monkeypatch.setattr(fin, "_load_config", lambda path, verb="financial": cfg)

    def unreachable(**_k):
        raise K8sConnectionError("connection refused")

    monkeypatch.setattr(k8s_mod, "get_k8s_client", unreachable)
    (tmp_path / "c.yaml").write_text("name: x\n")
    argv = ["financial", "reference-score", str(tmp_path / "c.yaml"), "--manifest", "s3://m"]
    return _runner().invoke(app, [*argv, "--output-prefix", "s3://o"])


def _scenario_sigint(monkeypatch, tmp_path):
    import lakebench.cli as cli

    def interrupt(*_a, **_k):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "load_config", interrupt)
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["status", str(tmp_path / "c.yaml")])


def _scenario_config_upgrade_refused(monkeypatch, tmp_path):
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["config", "upgrade", str(tmp_path / "c.yaml")])


def _init_config(tmp_path) -> Path:
    init = _runner().invoke(app, ["init", "--output", str(tmp_path / "c.yaml")])
    assert init.exit_code == 0, init.output
    return tmp_path / "c.yaml"


def _scenario_config_validation(monkeypatch, tmp_path):
    (tmp_path / "c.yaml").write_text("name: x\nplatform: 5\n")
    return _runner().invoke(app, ["info", str(tmp_path / "c.yaml")])


def _scenario_config_name_required(monkeypatch, tmp_path):
    (tmp_path / "c.yaml").write_text("recipe: hive-iceberg-spark-trino\n")
    return _runner().invoke(app, ["deploy", str(tmp_path / "c.yaml"), "--yes"])


def _scenario_config_unsupported(monkeypatch, tmp_path):
    monkeypatch.setattr("lakebench.cli._run._run_local_mode", lambda *a, **k: None)
    cfg = _init_config(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), "--local", "--continuous"])


def _scenario_cli_bad_argument(monkeypatch, tmp_path):
    return _runner().invoke(app, ["config", "recipes", "no-such-recipe"])


def _scenario_k8s_unreachable(monkeypatch, tmp_path):
    import lakebench.cli as cli
    from lakebench.k8s import K8sConnectionError

    def unreachable(**_k):
        raise K8sConnectionError("connection refused")

    monkeypatch.setattr(cli, "get_k8s_client", unreachable)
    return _runner().invoke(app, ["status", str(_init_config(tmp_path))])


def _destroy_with(monkeypatch, results):
    from unittest.mock import MagicMock

    engine = MagicMock()
    engine.destroy_all.return_value = results
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda *a, **k: engine)
    fixture = ROOT / "tests" / "fixtures" / "v14user.yaml"
    return _runner().invoke(app, ["destroy", str(fixture), "--force"])


def _scenario_destroy_redeployed(monkeypatch, tmp_path):
    msg = (
        "Destroy NOT completed: namespace v14user is now a newer deployment with "
        "the same name (a redeploy); it was left alone"
    )
    flag = {exit_codes.REFUSAL_DETAIL: "destroy.redeployed"}
    return _destroy_with(monkeypatch, [_result("namespace", "failed", msg, **flag)])


def _scenario_destroy_unverified_cluster(monkeypatch, tmp_path):
    msg = (
        "Destroy NOT completed: this cluster has no fingerprint, so buckets with a "
        "cluster stamp are kept: v14user-bronze"
    )
    flag = {exit_codes.REFUSAL_DETAIL: "destroy.unverified_cluster"}
    return _destroy_with(monkeypatch, [_result("s3-buckets", "failed", msg, **flag)])


def _scenario_lease_held(monkeypatch, tmp_path):
    msg = (
        "another lakebench process holds the cluster lock (h@u@sha); wait for it, or "
        "run `lakebench admin release-lock` once its lease has expired."
    )
    flag = {exit_codes.REFUSAL_DETAIL: "lease.held"}
    return _destroy_with(monkeypatch, [_result("spark-operator-watch", "failed", msg, **flag)])


def _scenario_context_changed(monkeypatch, tmp_path):
    from lakebench.k8s.target import ContextConflictError

    def conflict(*a, **k):
        raise ContextConflictError("one cluster context per process: A is active, refusing B")

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", conflict)
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["destroy", str(tmp_path / "c.yaml"), "--yes"])


def _scenario_destroy_namespace_terminating(monkeypatch, tmp_path):
    from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

    pending = DeploymentResult(
        "namespace",
        DeploymentStatus.SKIPPED,
        "Namespace v14user is still terminating after 600s",
        details={"still_terminating": True},
    )
    return _destroy_with(monkeypatch, [_result("postgres", "success", "removed"), pending])


def _scenario_deploy_identity_foreign(monkeypatch, tmp_path):
    from unittest.mock import MagicMock

    import lakebench.cli._deploy as deploy_mod
    from tests.fixtures import saf2_deploy_state_helpers as t

    cfg = _init_config(tmp_path)
    # deploy reads the namespace to reconcile its recorded nonces first.
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: t.FakeCore())
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)
    engine = MagicMock()
    engine.deploy_all.return_value = [
        _result(
            "namespace",
            "failed",
            "Namespace ownership refused: namespace 'x' is owned by another lakebench deployment",
            **_REFUSED,
        )
    ]
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda *a, **k: engine)
    return _runner().invoke(app, ["deploy", str(cfg), "--yes"])


def _fake_s3(monkeypatch, *, info=None, init_error=None):
    from tests.fixtures import datagen_timeout_helpers as dg

    monkeypatch.setattr(dg._FakeS3, "instances", [])
    monkeypatch.setattr(dg._FakeS3, "store", {})
    monkeypatch.setattr(dg._FakeS3, "_next_info", info)
    monkeypatch.setattr(dg._FakeS3, "_next_init_error", init_error)
    monkeypatch.setattr("lakebench.s3.S3Client", dg._FakeS3)
    return dg


_RUN_GENERATE = ["--generate", "--skip-preflight", "--skip-benchmark", "--skip-maintenance"]


def _scenario_run_bronze_nonempty(monkeypatch, tmp_path):
    from lakebench.s3.client import BucketInfo

    dg = _fake_s3(monkeypatch, info=BucketInfo(name="b", exists=True, object_count=9, size_bytes=9))
    dg._stub_full_run(monkeypatch)
    # A bucket this deployment cannot prove it owns: the gate's unowned row.
    monkeypatch.setattr("lakebench.deploy.datagen.deployment_may_empty", lambda *a, **k: False)
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), *_RUN_GENERATE, "--yes"])


def _scenario_generate_multi_cycle(monkeypatch, tmp_path):
    dg = _fake_s3(monkeypatch)
    cfg = dg._write_cfg(tmp_path, architecture="{pipeline: {mode: batch, cycles: 3}}")
    return _runner().invoke(app, ["generate", str(cfg), "--yes"])


def _scenario_run_series_mismatch(monkeypatch, tmp_path):

    dg = _fake_s3(monkeypatch)
    dg._stub_full_run(monkeypatch)
    # The marker of a generate that never finished (no cycle complete).
    dg._FakeS3.store[("a4-datagen-bronze", "customer/interactions/_corpus/series.json")] = (
        json.dumps(
            {"format": 1, "schema": "customer360", "cycles_total": 1, "cycles_complete": []}
        ).encode()
    )
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), "--skip-generate", "--skip-preflight", "--yes"])


def _scenario_run_no_corpus(monkeypatch, tmp_path):
    """A run that reuses bronze on a deployment where nothing was generated
    (live: it went on to a bronze-verify crash and a "crashed" verdict)."""
    import lakebench.deploy.corpus as corpus

    real = corpus.bronze_holds_data
    dg = _fake_s3(monkeypatch)  # an empty bronze
    dg._stub_full_run(monkeypatch)
    monkeypatch.setattr(corpus, "bronze_holds_data", real)
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), "--skip-preflight", "--yes"])


def _scenario_run_s3_unreadable(monkeypatch, tmp_path):
    """A run that reuses bronze cannot read its series marker: exit 4 before
    anything is deployed or submitted."""
    dg = _fake_s3(monkeypatch, init_error="endpoint unreachable")
    stubs = dg._stub_full_run(monkeypatch)
    res = _runner().invoke(app, ["run", str(dg._write_cfg(tmp_path)), "--skip-preflight", "--yes"])
    stubs["job_manager"].submit_job.assert_not_called()
    return res


def _scenario_datagen_pods_live(monkeypatch, tmp_path):
    dg = _fake_s3(monkeypatch)  # an empty bronze prefix: the gate proceeds
    dg._stub_run_deps(monkeypatch)
    # The deployer deletes the earlier Job, but one of its pods never stops.
    from lakebench.deploy import datagen as _datagen

    monkeypatch.setattr(
        _datagen.DatagenDeployer,
        "_build_datagen_context",
        lambda self: {"datagen_path_prefix": "customer/interactions"},
    )
    from unittest.mock import MagicMock

    import kubernetes.client as _kc

    monkeypatch.setattr(_datagen, "stop_previous_datagen", _real_stop_previous_datagen)
    monkeypatch.setattr(_kc, "BatchV1Api", MagicMock)  # the Job delete succeeds
    monkeypatch.setattr(_datagen, "live_datagen_pods", lambda ns: ["lakebench-datagen-0-old"])
    monkeypatch.setattr(_datagen, "DATAGEN_POD_STOP_WAIT_S", 0.0)
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["generate", str(cfg), "--yes"])


def _scenario_s3_unreachable(monkeypatch, tmp_path):
    dg = _fake_s3(monkeypatch, init_error="endpoint unreachable")
    dg._stub_run_deps(monkeypatch)
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["generate", str(cfg), "--yes"])


def _scenario_run_datagen_timeout(monkeypatch, tmp_path):
    import time as _time

    dg = _fake_s3(monkeypatch)
    dg._stub_full_run(monkeypatch)
    monkeypatch.setattr("lakebench.deploy.DatagenDeployer", dg._FakeDatagenDeployer)
    clock = {"t": 1_000.0}
    monkeypatch.setattr(_time, "time", lambda: clock["t"])

    def _sleep(seconds: float) -> None:
        clock["t"] += max(float(seconds), 0.001)

    monkeypatch.setattr(_time, "sleep", _sleep)
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), *_RUN_GENERATE, "--timeout", "60", "--yes"])


def _scenario_run_prereq_failed(monkeypatch, tmp_path):
    from types import SimpleNamespace

    report = SimpleNamespace(checks=[], all_passed=False)
    monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", lambda *a, **k: report)
    return _runner().invoke(app, ["run", str(_init_config(tmp_path))])


def _capacity_run(monkeypatch, tmp_path, found):
    """``run`` whose preflight is the real capacity check on a fake reading."""
    from unittest import mock

    from lakebench.cli import _prerequisites as pre

    k8s = mock.MagicMock()
    k8s.get_free_capacity.return_value = found

    def run_prerequisites(cfg, **kw):
        with mock.patch("lakebench.k8s.get_k8s_client", return_value=k8s):
            return pre.PrereqReport(checks=[pre._check_cluster_capacity(cfg, **kw)])

    monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", run_prerequisites)
    return _runner().invoke(app, ["run", str(_init_config(tmp_path))])


def _scenario_capacity_unknown(monkeypatch, tmp_path):
    from lakebench.k8s.client import CapacityUnknown

    return _capacity_run(
        monkeypatch, tmp_path, CapacityUnknown("listing nodes failed (403 Forbidden)")
    )


def _scenario_capacity_shortfall(monkeypatch, tmp_path):
    from lakebench.k8s.client import ClusterCapacity, FreeCapacity

    gib = 1024**3
    small = ClusterCapacity(8_000, 32 * gib, 1, 8_000, 32 * gib)
    return _capacity_run(monkeypatch, tmp_path, FreeCapacity(small, small, ((8_000, 32 * gib),)))


def _plan_online(monkeypatch, tmp_path, **status_by_id):
    """``plan`` online against a fake cluster whose prerequisites report
    *status_by_id* (others ok) and whose capacity check passes."""
    from unittest import mock

    from lakebench.cli import _prerequisites as pre
    from lakebench.deploy.prereqs import PREREQS, PrereqOutcome, PrereqResult, PrereqStatus

    k8s = mock.MagicMock()
    k8s.test_connectivity.return_value = (True, "ok")
    k8s.get_cluster_capacity.return_value = None
    monkeypatch.setattr("lakebench.k8s.get_k8s_client", lambda **kw: k8s)
    outcomes = [
        PrereqOutcome(p, PrereqResult(PrereqStatus(status_by_id.get(p.id, "ok")), p.id))
        for p in PREREQS
    ]
    monkeypatch.setattr("lakebench.deploy.prereqs.run_prereqs", lambda cfg: outcomes)
    monkeypatch.setattr(
        pre, "_check_cluster_capacity", lambda cfg, **kw: pre.PrereqResult("cc", True, "ok")
    )
    return _runner().invoke(app, ["plan", str(_init_config(tmp_path))])


def _scenario_plan_ok(monkeypatch, tmp_path):
    return _plan_online(monkeypatch, tmp_path)


def _scenario_plan_missing_storage_class(monkeypatch, tmp_path):
    return _plan_online(monkeypatch, tmp_path, **{"scratch-storage-class": "fail"})


def _scenario_run_namespace_missing_no_yes(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from tests.fixtures import datagen_timeout_helpers as dg

    stubs = dg._stub_full_run(monkeypatch)
    stubs["k8s"].namespace_exists.return_value = False
    report = SimpleNamespace(checks=[], all_passed=True)
    monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", lambda *a, **k: report)
    return _runner().invoke(app, ["run", str(dg._write_cfg(tmp_path))])


def _local_run(monkeypatch, tmp_path, success: bool):
    from types import SimpleNamespace

    import lakebench.cli._local as local
    import lakebench.cli._run as run_mod

    stack = SimpleNamespace()
    monkeypatch.setattr(local, "check_local_supported", lambda *a, **k: None)
    monkeypatch.setattr(local, "deploy_local", lambda *a, **k: stack)
    monkeypatch.setattr(
        local,
        "run_local",
        lambda *a, **k: SimpleNamespace(success=success, stages=[], elapsed_seconds=1.0),
    )
    monkeypatch.setattr(local, "print_local_summary", lambda *a, **k: None)
    # Recording needs a real stack; the pass path stubs it, since a run with
    # no jobs recorded fails the verdict.
    monkeypatch.setattr(run_mod, "_record_local_jobs", lambda *a, **k: None)
    monkeypatch.setattr(run_mod, "_save_local_metrics", lambda *a, **k: None)
    return _runner().invoke(
        app, ["run", str(_init_config(tmp_path)), "--local", "--skip-benchmark", "--yes"]
    )


def _scenario_run_pass(monkeypatch, tmp_path):
    return _local_run(monkeypatch, tmp_path, success=True)


def _scenario_run_verdict_failed(monkeypatch, tmp_path):
    return _local_run(monkeypatch, tmp_path, success=False)


def _scenario_run_interrupted(monkeypatch, tmp_path):
    """The QA-9 harness's batch run, with Ctrl-C while silver-build runs."""
    import dataclasses

    from tests.harness.run_harness import SCENARIOS as RUN_SCENARIOS
    from tests.harness.run_harness import invoke_scenario

    scenario = dataclasses.replace(
        RUN_SCENARIOS["batch_c360"], interrupt=("silver-build", "SIGINT")
    )
    return invoke_scenario(scenario, tmp_path, monkeypatch)[0]


def _scenario_run_namespace_gone(monkeypatch, tmp_path):
    """The QA-9 harness's continuous run, its namespace deleted at second 95."""
    import dataclasses

    from tests.harness.run_harness import SCENARIOS as RUN_SCENARIOS
    from tests.harness.run_harness import invoke_scenario

    scenario = dataclasses.replace(
        RUN_SCENARIOS["continuous_c360"], events=((95.0, "namespace_gone"),)
    )
    return invoke_scenario(scenario, tmp_path, monkeypatch)[0]


def _scenario_run_args(monkeypatch, tmp_path):
    """--force-reset on a batch run: refused before any cluster call."""
    cfg = _init_config(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), "--force-reset", "--yes"])


def _series_scenario(monkeypatch, tmp_path, after_first=None, **changes):
    """The QA-9 harness's batch run as a three-repetition series over a
    two-object bronze; *after_first* changes the fake cluster after
    repetition 1."""
    import dataclasses

    import lakebench.cli._series as series_mod
    from tests.harness import run_harness

    box = []
    real_install = run_harness.install_fakes

    def install(mp, rec, scenario):
        rec.bronze_objects = {
            "customer/interactions/part-0.parquet": (10, "a"),
            "customer/interactions/part-1.parquet": (10, "b"),
        }
        box.append(rec)
        return real_install(mp, rec, scenario)

    monkeypatch.setattr(run_harness, "install_fakes", install)
    real_call = series_mod._call
    n = [0]

    def call(*a, **k):
        code = real_call(*a, **k)
        n[0] += 1
        if n[0] == 1 and after_first:
            after_first(box[0])
        return code

    monkeypatch.setattr(series_mod, "_call", call)
    scenario = dataclasses.replace(
        run_harness.SCENARIOS["batch_c360"],
        argv=["--skip-generate", "--yes", "--repeat", "3"],
        **changes,
    )
    return run_harness.invoke_scenario(scenario, tmp_path, monkeypatch)[0]


def _scenario_repeat_no_verified_corpus(monkeypatch, tmp_path):
    return _series_scenario(monkeypatch, tmp_path, failing=("bronze-verify",))


def _scenario_series_corpus_changed(monkeypatch, tmp_path):
    def rewrite(rec):
        rec.bronze_objects["customer/interactions/part-1.parquet"] = (10, "b2")

    return _series_scenario(monkeypatch, tmp_path, after_first=rewrite)


def _scenario_confirm_declined(monkeypatch, tmp_path):
    cfg = _init_config(tmp_path)
    return _runner().invoke(app, ["clean", "silver", str(cfg)], input="n\n")


def _scenario_alias_refused(monkeypatch, tmp_path):
    return _runner().invoke(app, ["clean", "bronze", str(tmp_path / "secret-name.yaml")])


def _scenario_run_protected_corpus(monkeypatch, tmp_path):
    """A config naming the (test) evaluation seed and role, with looks open
    so it loads: run refuses it before any cluster call."""
    from tests.fixtures import protected_corpus as pc

    pc.use_heldout(monkeypatch)
    cfg = pc.financial_config(tmp_path / "c.yaml", seed=pc.EV, role="evaluation")
    return _runner().invoke(app, ["run", str(cfg), "--yes"])


def _scenario_deploy_existing_namespace(monkeypatch, tmp_path):
    """`deploy --require-new` reports the engine's refusal of an existing
    namespace as exit 3, naming the path."""
    import lakebench.cli._deploy as deploy_mod
    from tests.fixtures import saf2_deploy_state_helpers as t

    cfg = _init_config(tmp_path)
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: t.FakeCore())
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)

    class Engine:
        def __init__(self, cfg, require_new=False, **kw):
            self.require_new = require_new
            self.deploy_nonce = None

        def deploy_all(self, **kw):
            assert self.require_new
            # The real refusal the namespace step returns under require_new.
            from lakebench.deploy.engine import DeploymentEngine

            return [DeploymentEngine._existing_refusal(self, "namespace", "namespace x", 0.0)]

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    return _runner().invoke(app, ["deploy", str(cfg), "--yes", "--require-new"])


def _ops_cluster(monkeypatch, tmp_path):
    """The CC-27 fakes (tests/test_cli_cluster_ops.py) on the real commands."""
    import lakebench.cli as cli
    from tests.fixtures import cli_cluster_ops_helpers as co

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
    state.co = co
    return state


def _scenario_status_ok(monkeypatch, tmp_path):
    st = _ops_cluster(monkeypatch, tmp_path)
    st.apps.objects = dict(st.co._TRINO_HIVE)
    return _runner().invoke(app, ["status", str(st.config)])


def _scenario_status_drift(monkeypatch, tmp_path):
    st = _ops_cluster(monkeypatch, tmp_path)
    st.apps.objects = dict(st.co._TRINO_HIVE, **{"lakebench-trino-worker": (1, 2)})
    return _runner().invoke(app, ["status", str(st.config)])


def _scenario_status_namespace_missing(monkeypatch, tmp_path):
    st = _ops_cluster(monkeypatch, tmp_path)
    st.core.ns_exists = False
    return _runner().invoke(app, ["status", str(st.config)])


def _scenario_stop_api_error(monkeypatch, tmp_path):
    from kubernetes.client.rest import ApiException

    st = _ops_cluster(monkeypatch, tmp_path)
    st.custom.apps = ["lakebench-gold-refresh"]
    st.custom.delete_errors = {
        "lakebench-gold-refresh": ApiException(status=403, reason="Forbidden")
    }
    return _runner().invoke(app, ["stop", str(st.config)])


def _scenario_logs_no_pod(monkeypatch, tmp_path):
    st = _ops_cluster(monkeypatch, tmp_path)
    return _runner().invoke(app, ["logs", str(st.config), "silver-build"])


def _scenario_k8s_api_error(monkeypatch, tmp_path):
    from kubernetes.client.rest import ApiException

    st = _ops_cluster(monkeypatch, tmp_path)
    st.core.list_error = ApiException(status=403, reason="Forbidden")
    return _runner().invoke(app, ["logs", str(st.config), "trino"])


from lakebench.deps.runtime import load_handle as _real_load_handle  # noqa: E402


def _deps_cluster(monkeypatch, *, annotation: str | None, manifest: dict | None):
    """`run` up to its dependency-set check, against a namespace whose
    annotation and lb-deps-manifest ConfigMap are given; the real check."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from kubernetes.client.rest import ApiException

    from lakebench.deps import runtime
    from tests.fixtures import datagen_timeout_helpers as dg

    dg._stub_full_run(monkeypatch)
    monkeypatch.setattr(runtime, "load_handle", _real_load_handle)
    core = MagicMock()
    anns = {"lakebench.deployment/deps-set": annotation} if annotation else {}
    core.read_namespace.return_value = SimpleNamespace(metadata=SimpleNamespace(annotations=anns))

    def read_cm(name, ns):
        if name == "lb-deps-manifest" and manifest is not None:
            import json

            return SimpleNamespace(data={"manifest.json": json.dumps(manifest)})
        raise ApiException(status=404)

    core.read_namespaced_config_map.side_effect = read_cm
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    return dg


def _scenario_run_deps_missing(monkeypatch, tmp_path):
    dg = _deps_cluster(monkeypatch, annotation=None, manifest=None)
    return _runner().invoke(app, ["run", str(dg._write_cfg(tmp_path)), "--skip-preflight"])


def _scenario_run_deps_stale(monkeypatch, tmp_path):
    dg = _deps_cluster(monkeypatch, annotation="p" * 64, manifest={"request_sha256": "x" * 64})
    return _runner().invoke(app, ["run", str(dg._write_cfg(tmp_path)), "--skip-preflight"])


def _scenario_run_deps_mismatch(monkeypatch, tmp_path):
    from lakebench.config import load_config
    from lakebench.deps.request import select_request
    from tests.fixtures.deps_manifest_helpers import fake_shown

    dg = _deps_cluster(monkeypatch, annotation=None, manifest=None)
    cfg_path = dg._write_cfg(tmp_path)
    shown = fake_shown(select_request(load_config(cfg_path)))
    _deps_cluster(monkeypatch, annotation="0" * 64, manifest=shown)  # another pinset
    return _runner().invoke(app, ["run", str(cfg_path), "--skip-preflight"])


def _nameless_destroy(monkeypatch, tmp_path, setup, argv_extra=()):
    """`destroy --force` on a nameless config against a fake namespace (SAF-2)."""
    import lakebench.cli._nameless as nameless
    from tests.fixtures import saf2_deploy_state_helpers as t

    core = t.FakeCore()
    monkeypatch.setattr(nameless, "_core_v1_factory", lambda cfg: lambda: core)
    monkeypatch.setattr(nameless, "_bucket_owned_factory", lambda cfg: lambda b: False)
    cfg = t._nameless(tmp_path)
    setup(t, core, cfg)
    return _runner().invoke(app, ["destroy", str(cfg), "--force", *argv_extra])


def _nameless_status(monkeypatch, tmp_path, setup):
    """`status` on a nameless config with no v1.6 state (a suggested name)."""
    import lakebench.cli._nameless as nameless
    from lakebench.config import deploy_state as ds
    from tests.fixtures import saf2_deploy_state_helpers as t

    core = t.FakeCore()
    monkeypatch.setattr(nameless, "_core_v1_factory", lambda cfg: lambda: core)
    monkeypatch.setattr(ds, "suggested_name", lambda *a, **k: t.NAME)
    cfg = t._nameless(tmp_path)
    setup(t, core, cfg)
    return _runner().invoke(app, ["status", str(cfg)])


def _scenario_nameless_ambiguous(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._nameless(tmp_path, "b.yaml")

    return _nameless_status(monkeypatch, tmp_path, setup)


def _scenario_nameless_nonce_mismatch(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._v16_namespace(core, nonce="foreign")
        t._v17_state(cfg, [("mine", "confirmed")])

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_copied_dir(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._v16_namespace(core, nonce="n1")
        t._v17_state(cfg, [("n1", "confirmed")], config_dir="/elsewhere")

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_moved(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._v16_namespace(core, nonce="n1")
        t._v17_state(cfg, [("n1", "confirmed")], moved_to="/new/home")

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_name_required(monkeypatch, tmp_path):
    # The suggested name happens to name a live namespace: no v1.7 state
    # proves it, so status refuses without --name.
    def setup(t, core, cfg):
        t._v16_namespace(core)

    return _nameless_status(monkeypatch, tmp_path, setup)


def _scenario_nameless_stamp_mismatch(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)
        core.add("lb-x", **{t.ANNOTATION_DEPLOYMENT_NAME: "lb-other"})

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_v17_state_elsewhere(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)
        t._v16_namespace(core, **{t.ANNOTATION_STATE_SCHEMA: "lb-state/1"})

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_namespace_missing(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_namespace_unreadable(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)
        core.fail_reads = True

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_deploy_state_unrecordable(monkeypatch, tmp_path):
    import lakebench.cli._deploy as deploy_mod
    from lakebench.config import deploy_state as ds
    from tests.fixtures import saf2_deploy_state_helpers as t

    core = t.FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)

    class Engine:
        def __init__(self, cfg, dry_run=False, **kw):
            self.results = []

        def deploy_all(self, **kw):  # pragma: no cover -- must not be reached
            raise AssertionError("deploy_all after a failed state write")

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)

    def no_write(path, state):
        raise OSError("read-only file system")

    monkeypatch.setattr(ds, "write_state", no_write)
    cfg = t._named(tmp_path)
    return _runner().invoke(app, ["deploy", str(cfg), "--yes"])


def _scenario_deploy_state_copied(monkeypatch, tmp_path):
    import lakebench.cli._deploy as deploy_mod
    from tests.fixtures import saf2_deploy_state_helpers as t

    core = t.FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)

    class Engine:
        def __init__(self, cfg, dry_run=False, **kw):
            self.results = []

        def deploy_all(self, **kw):  # pragma: no cover -- must not be reached
            raise AssertionError("deploy_all from a copied directory")

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    cfg = t._named(tmp_path)
    t._v17_state(cfg, [("n1", "confirmed")], config_dir="/elsewhere")
    return _runner().invoke(app, ["deploy", str(cfg), "--yes"])


def _scenario_destroy_incarnation_mismatch(monkeypatch, tmp_path):
    from lakebench.deploy import DeploymentResult, DeploymentStatus

    class Engine:
        def __init__(self, cfg, **kw):
            pass

        def destroy_all(self, **kw):
            assert kw["expected_incarnation"] == "u1#n1"
            return [
                DeploymentResult(
                    component="ownership-check",
                    status=DeploymentStatus.FAILED,
                    message="Destroy NOT started: namespace lb-x is not the deployment "
                    "this command checked. Nothing was changed.",
                    details={"incarnation_mismatch": True},
                )
            ]

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)

    def setup(t, core, cfg):
        t._v16_namespace(core, nonce="n1")
        t._v17_state(cfg, [("n1", "confirmed")])

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


_CLUSTER_RUN_FLAGS = ["--skip-preflight", "--skip-generate", "--skip-benchmark", "--yes"]


def _scenario_run_operator_not_ready(monkeypatch, tmp_path):
    """The finally block re-raises the run's code; it must keep 4, not 1."""
    from unittest.mock import MagicMock

    dg = _fake_s3(monkeypatch)  # the finally block measures bucket sizes
    stubs = dg._stub_full_run(monkeypatch)
    stubs["op"].check_status.return_value = MagicMock(
        ready=False, installed=True, version="2.5.1", message="controller down"
    )
    return _runner().invoke(app, ["run", str(dg._write_cfg(tmp_path)), *_CLUSTER_RUN_FLAGS])


def _scenario_run_step_failed(monkeypatch, tmp_path):
    dg = _fake_s3(monkeypatch)
    stubs = dg._stub_full_run(monkeypatch)
    stubs["job_manager"].deploy_scripts_configmap.return_value = False
    return _runner().invoke(app, ["run", str(dg._write_cfg(tmp_path)), *_CLUSTER_RUN_FLAGS])


def _path_name(name: str) -> str:
    """A scenario name is a PATHS name, then "#" and a variant when one path
    has several producers."""
    return name.split("#", 1)[0]


SCENARIOS = {
    "config.upgrade_refused": _scenario_config_upgrade_refused,
    "alias.refused": _scenario_alias_refused,
    "deploy.state_copied": _scenario_deploy_state_copied,
    "destroy.incarnation_mismatch": _scenario_destroy_incarnation_mismatch,
    "deploy.state_unrecordable": _scenario_deploy_state_unrecordable,
    "nameless.ambiguous": _scenario_nameless_ambiguous,
    "nameless.nonce_mismatch": _scenario_nameless_nonce_mismatch,
    "nameless.copied_dir": _scenario_nameless_copied_dir,
    "nameless.moved": _scenario_nameless_moved,
    "nameless.name_required": _scenario_nameless_name_required,
    "nameless.stamp_mismatch": _scenario_nameless_stamp_mismatch,
    "nameless.v17_state_elsewhere": _scenario_nameless_v17_state_elsewhere,
    "nameless.namespace_missing": _scenario_nameless_namespace_missing,
    "nameless.namespace_unreadable": _scenario_nameless_namespace_unreadable,
    "version.ok": _scenario_version_ok,
    "click.usage": _scenario_click_usage,
    "unhandled_exception": _scenario_unhandled_exception,
    "confirm.non_tty": _scenario_confirm_non_tty,
    "sigint": _scenario_sigint,
    "financial.k8s_unreachable": _scenario_financial_k8s_unreachable,
    "config.validation": _scenario_config_validation,
    "config.unsupported": _scenario_config_unsupported,
    "config.name_required": _scenario_config_name_required,
    "cli.bad_argument": _scenario_cli_bad_argument,
    "k8s.unreachable": _scenario_k8s_unreachable,
    "s3.unreachable": _scenario_s3_unreachable,
    "s3.unreachable#run_reuse": _scenario_run_s3_unreadable,
    "destroy.redeployed": _scenario_destroy_redeployed,
    "destroy.unverified_cluster": _scenario_destroy_unverified_cluster,
    "lease.held": _scenario_lease_held,
    "context.changed": _scenario_context_changed,
    "destroy.namespace_terminating": _scenario_destroy_namespace_terminating,
    "deploy.identity_foreign": _scenario_deploy_identity_foreign,
    "run.bronze_nonempty": _scenario_run_bronze_nonempty,
    "datagen.pods_live": _scenario_datagen_pods_live,
    "generate.multi_cycle": _scenario_generate_multi_cycle,
    "run.series_mismatch": _scenario_run_series_mismatch,
    "run.no_corpus": _scenario_run_no_corpus,
    "run.datagen_timeout": _scenario_run_datagen_timeout,
    "run.prereq_failed": _scenario_run_prereq_failed,
    "run.prereq_failed#operator_not_ready": _scenario_run_operator_not_ready,
    "run.deps_missing": _scenario_run_deps_missing,
    "run.deps_stale": _scenario_run_deps_stale,
    "run.deps_mismatch": _scenario_run_deps_mismatch,
    "capacity.unknown": _scenario_capacity_unknown,
    "plan.ok": _scenario_plan_ok,
    "plan.missing_storage_class": _scenario_plan_missing_storage_class,
    "capacity.shortfall": _scenario_capacity_shortfall,
    "run.namespace_missing_no_yes": _scenario_run_namespace_missing_no_yes,
    "run.pass": _scenario_run_pass,
    "run.verdict_failed": _scenario_run_verdict_failed,
    "run.verdict_failed#step_failed": _scenario_run_step_failed,
    "run.interrupted": _scenario_run_interrupted,
    "repeat.no_verified_corpus": _scenario_repeat_no_verified_corpus,
    "series.corpus_changed": _scenario_series_corpus_changed,
    "run.args": _scenario_run_args,
    "run.namespace_gone": _scenario_run_namespace_gone,
    "confirm.declined": _scenario_confirm_declined,
    "run.protected_corpus": _scenario_run_protected_corpus,
    "deploy.existing_namespace": _scenario_deploy_existing_namespace,
    "status.ok": _scenario_status_ok,
    "status.drift": _scenario_status_drift,
    "status.namespace_missing": _scenario_status_namespace_missing,
    "stop.api_error": _scenario_stop_api_error,
    "logs.no_pod": _scenario_logs_no_pod,
    "k8s.api_error": _scenario_k8s_api_error,
}

# The line each path must print on stderr, where it prints one.
EXPECTED_STDERR = {
    "config.upgrade_refused": "ERROR  `lakebench config upgrade` is removed",
    "alias.refused": "ERROR  `lakebench clean bronze` is removed",
    "unhandled_exception": "ERROR  RuntimeError: unexpected [/tmp] failure",
    "confirm.non_tty": "ERROR  Not confirmed",
    "sigint": "ERROR  Interrupted.",
    "financial.k8s_unreachable": "ERROR  Cannot reach the Kubernetes cluster: connection refused",
    "k8s.unreachable": "ERROR Kubernetes connection failed: connection refused",
    "s3.unreachable": "refusing to generate",
    "run.prereq_failed": "ERROR Prerequisites not met",
    "run.deps_missing": "has no dependency server",
    "run.deps_stale": "resolved for another request",
    "run.namespace_missing_no_yes": "does not exist",
    "cli.bad_argument": "ERROR Unknown recipe: no-such-recipe",
    "config.unsupported": "Unsupported combination, refused",
    "status.drift": "ERROR Drift: lakebench-trino-worker",
    "status.namespace_missing": "ERROR  namespace ops does not exist",
    "stop.api_error": "ERROR deleting SparkApplication/lakebench-gold-refresh: 403",
    "logs.no_pod": "ERROR  no pod for silver-build",
    "k8s.api_error": "ERROR Kubernetes API error: listing pods",
}


# Text in the combined output that shows the scenario took its named path,
# where the code alone has more than one producer. Refusals (exit 3) are
# identified by the exit-path file instead.
EXPECTED_OUTPUT = {
    "plan.missing_storage_class": "Next: (cluster admin) lakebench admin install --component",
    "plan.ok": "ok: Kubeflow Spark Operator 2.x",
    "capacity.unknown": "capacity could not be read: listing nodes failed",
    "capacity.shortfall": "Insufficient free cluster capacity",
    "run.datagen_timeout": "wait budget",
    "s3.unreachable#run_reuse": "Cannot check the corpus in bronze before reusing it",
    "run.prereq_failed#operator_not_ready": "Spark Operator not ready",
    "run.verdict_failed#step_failed": "Failed to deploy Spark scripts ConfigMap",
    "destroy.namespace_terminating": "still terminating",
    "run.interrupted": "Interrupted by SIGINT during silver-build",
    "repeat.no_verified_corpus": "no verified corpus to reuse",
    "run.args": "--force-reset only applies to a continuous run",
    "generate.multi_cycle": "does not apply to a multi-cycle config",
    "run.protected_corpus": "never runs on a protected AML corpus",
    "run.namespace_gone": "was deleted mid-run; stopping",
    "config.name_required": "config has no name, so it cannot change data",
    "config.validation": "Config error",
}
# Text that must not appear: a declined prompt is not an unanswered one.
UNEXPECTED_OUTPUT = {"confirm.declined": "Not confirmed", "run.verdict_failed": "ERROR"}


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_exit_code_paths(name, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # journal and state files land here
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    result = SCENARIOS[name](monkeypatch, tmp_path)
    assert result.exit_code == exit_codes.path_code(_path_name(name)), result.output
    assert "Traceback" not in result.output
    if name in EXPECTED_STDERR:
        assert EXPECTED_STDERR[name] in _stderr(result), result.output
    if name in EXPECTED_OUTPUT:
        assert EXPECTED_OUTPUT[name] in result.output, result.output
    if name in UNEXPECTED_OUTPUT:
        assert UNEXPECTED_OUTPUT[name] not in result.output, result.output


# Every refusal (exit 3) names its path in LB_EXIT_PATH_FILE, so the release
# harness tells refusals apart (incarnation mismatch, unverified cluster,
# lease held) without reading message text. Deploy and destroy step
# refusals exit through typer.Exit and note their paths first.
_REFUSALS = sorted(n for n in SCENARIOS if exit_codes.path_code(_path_name(n)) == ExitCode.REFUSED)
# Exit 6 has one producer the harness and S-P4 check by path.
_PATH_NAMED = [*_REFUSALS, "destroy.namespace_terminating"]


@pytest.mark.parametrize("name", _PATH_NAMED)
def test_refusal_names_its_path_in_the_exit_path_file(name, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    target = tmp_path / "exit-path"
    monkeypatch.setenv(cli_exit.EXIT_PATH_FILE_ENV, str(target))
    result = SCENARIOS[name](monkeypatch, tmp_path)
    assert result.exit_code == exit_codes.path_code(_path_name(name)), result.output
    code, *paths = target.read_text().splitlines()[-1].split()
    assert code == str(int(exit_codes.path_code(_path_name(name))))
    assert name in paths, (name, paths)


# -- generated table -----------------------------------------------------------


def _foreign_class(module: str, name: str, base: type[BaseException]) -> type[BaseException]:
    return type(name, (base,), {"__module__": module})


def test_click_family_passes_through_by_package():
    """Click prints its own usage error and exits 2; the handler must not wrap it."""
    for module, name in [
        ("click.exceptions", "UsageError"),  # stock click from a dependency
        ("click.exceptions", "Exit"),
        ("typer._click.exceptions", "NoSuchOption"),
        ("typer._click.exceptions", "RenamedClickException"),  # a later rename
    ]:
        exc = _foreign_class(module, name, RuntimeError)("x")
        assert cli_exit.error_for(exc) is None


@pytest.mark.parametrize("module", ["click.exceptions", "typer._click.exceptions"])
def test_any_click_abort_is_not_confirmed(module):
    exc = _foreign_class(module, "Abort", RuntimeError)()
    assert cli_exit.exit_code_for(exc) == ExitCode.NOT_CONFIRMED


def test_non_click_runtime_error_is_failed():
    exc = _foreign_class("somelib.errors", "UsageError", RuntimeError)("x")
    assert cli_exit.exit_code_for(exc) == ExitCode.FAILED
