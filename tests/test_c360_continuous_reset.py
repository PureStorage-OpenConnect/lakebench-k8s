"""c360 continuous runs reset their state before any stream starts (LB-142).

After a batch run, silver and gold are full and the stream checkpoints are
gone or stale; silver-stream refuses a fresh checkpoint over a full table,
and nothing cleared c360 state. The continuous entry path now clears the
checkpoints (and, when it generates data, the raw landing zone) and runs a
bronze-verify preflight that drops the continuous tables, all behind the
same ownership gate AML uses.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import typer

from lakebench.cli import _sustained
from lakebench.spark.job import JobState, JobType
from tests.conftest import make_config


def _c360_cfg():
    return make_config(
        name="c360-reset",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://127.0.0.1:1",
                    "access_key": "x",
                    "secret_key": "y",
                    "buckets": {"bronze": "c-b", "silver": "c-s", "gold": "c-g"},
                }
            }
        },
    )


class _Result:
    def __init__(self, success, message="", elapsed=12.0):
        self.success = success
        self.message = message
        self.elapsed_seconds = elapsed


def test_reset_preflight_submits_bronze_verify_in_reset_mode():
    jm = MagicMock()
    jm.submit_job.return_value = MagicMock(state=JobState.RUNNING)
    mon = MagicMock()
    mon.wait_for_completion.return_value = _Result(True)
    assert _sustained._run_c360_continuous_reset(jm, mon, MagicMock(), timeout_seconds=5400)
    jm.submit_job.assert_called_once_with(
        JobType.BRONZE_VERIFY, cycle_env={"LB_CONTINUOUS_RESET": "1"}
    )
    assert mon.wait_for_completion.call_args.args[0] == "lakebench-bronze-verify"
    assert mon.wait_for_completion.call_args.kwargs["timeout_seconds"] == 5400


@pytest.mark.parametrize(
    "submit_state,wait_ok", [(JobState.FAILED, True), (JobState.RUNNING, False)]
)
def test_reset_preflight_failure_is_reported(submit_state, wait_ok):
    jm = MagicMock()
    jm.submit_job.return_value = MagicMock(state=submit_state, message="boom")
    mon = MagicMock()
    mon.wait_for_completion.return_value = _Result(wait_ok, "timeout")
    assert _sustained._run_c360_continuous_reset(jm, mon, MagicMock(), timeout_seconds=60) is False


def test_c360_state_reset_clears_checkpoints_and_c360_landing_zone(monkeypatch):
    """c360 clears customer/interactions, never the AML pacs008 prefix."""
    cfg = _c360_cfg()
    monkeypatch.setattr(_sustained, "_require_reset_ownership", lambda c: None)
    client = MagicMock()
    client.delete_prefix.return_value = 0
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: client)
    _sustained._reset_continuous_state(cfg, clear_raw=True)
    deleted = [c.args for c in client.delete_prefix.call_args_list]
    assert deleted == [
        ("c-b", "checkpoints/bronze-ingest"),
        ("c-s", "checkpoints/silver-stream"),
        ("c-g", "checkpoints/gold-refresh"),
        ("c-b", "customer/interactions"),
    ]


def test_c360_state_reset_keeps_raw_with_skip_generate(monkeypatch):
    cfg = _c360_cfg()
    monkeypatch.setattr(_sustained, "_require_reset_ownership", lambda c: None)
    client = MagicMock()
    client.delete_prefix.return_value = 0
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: client)
    _sustained._reset_continuous_state(cfg, clear_raw=False)
    prefixes = [c.args[1] for c in client.delete_prefix.call_args_list]
    assert "customer/interactions" not in prefixes and len(prefixes) == 3


def test_c360_state_reset_refuses_without_ownership(monkeypatch):
    cfg = _c360_cfg()
    monkeypatch.setattr(_sustained, "_reset_ownership_problem", lambda c: "bucket c-b: FOREIGN")
    client = MagicMock()
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: client)
    with pytest.raises(typer.Exit):
        _sustained._reset_continuous_state(cfg, clear_raw=True)
    client.delete_prefix.assert_not_called()


class _StopAfterFirstStream(Exception):
    pass


events_ref: dict = {}


def _drive_sustained(
    monkeypatch, tmp_path, cfg, *, reset_ok=True, owned=True, existing=(), force_reset=False
):
    """Run _run_sustained with every cluster and S3 edge mocked; return the
    ordered list of side effects it performed."""
    monkeypatch.chdir(tmp_path)
    events: list[str] = []

    op = MagicMock()
    op.check_status.return_value = MagicMock(ready=True, version="2.5.1")
    op.ensure_namespace_watched.return_value = MagicMock(watching_namespace=True)
    monkeypatch.setattr("lakebench.spark.SparkOperatorManager", lambda **kw: op)
    monkeypatch.setattr(_sustained, "get_k8s_client", lambda **kw: MagicMock())

    jm = MagicMock()
    jm.deploy_scripts_configmap.return_value = True

    def submit(job_type, **kw):
        events.append(f"submit:{job_type.value}:{kw.get('cycle_env')}")
        if job_type == JobType.BRONZE_INGEST:
            raise _StopAfterFirstStream
        return MagicMock(state=JobState.RUNNING)

    jm.submit_job.side_effect = submit
    monkeypatch.setattr("lakebench.engine.get_engine", lambda c, k: jm)
    mon = MagicMock()
    mon.wait_for_completion.return_value = _Result(reset_ok)
    monkeypatch.setattr("lakebench.spark.SparkJobMonitor", lambda *a, **kw: mon)
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda c: MagicMock())
    dg = MagicMock()

    def dg_deploy():
        events.append("datagen")
        return MagicMock(status=_sustained_status_success())

    dg.deploy.side_effect = dg_deploy
    monkeypatch.setattr("lakebench.deploy.DatagenDeployer", lambda e: dg)

    def ownership(c):
        events.append("ownership")
        if not owned:
            print("refused")
            raise typer.Exit(1)

    monkeypatch.setattr(_sustained, "_require_reset_ownership", ownership)
    monkeypatch.setattr(
        _sustained, "_stop_leftover_streams", lambda jm_, ns: events.append("stop-streams")
    )
    monkeypatch.setattr(
        _sustained,
        "_reset_continuous_state",
        lambda c, clear_raw: events.append(f"reset-s3:clear_raw={clear_raw}"),
    )
    monkeypatch.setattr(_sustained, "_c360_existing_state", lambda c, clear_raw: list(existing))
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: MagicMock())
    monkeypatch.setattr(_sustained, "_collect_platform_metrics", lambda *a, **kw: None)
    events_ref["mon"] = mon

    # The run ends at the first stream submit (or an Exit); the finally
    # block may then fail on mocked metrics, which is irrelevant here.
    with pytest.raises(Exception):  # noqa: B017
        _sustained._run_sustained(cfg, tmp_path / "cfg.yaml", 60, True, 60, force_reset=force_reset)
    return events


def _sustained_status_success():
    from lakebench.deploy import DeploymentStatus

    return DeploymentStatus.SUCCESS


def test_c360_continuous_entry_resets_before_any_stream(monkeypatch, tmp_path):
    events = _drive_sustained(monkeypatch, tmp_path, _c360_cfg())
    reset_submit = "submit:bronze-verify:{'LB_CONTINUOUS_RESET': '1'}"
    assert events[:4] == ["ownership", "stop-streams", "reset-s3:clear_raw=True", "datagen"]
    assert reset_submit in events
    first_stream = next(i for i, e in enumerate(events) if e.startswith("submit:bronze-ingest"))
    assert events.index(reset_submit) < first_stream


def test_c360_failed_reset_starts_no_stream(monkeypatch, tmp_path):
    events = _drive_sustained(monkeypatch, tmp_path, _c360_cfg(), reset_ok=False)
    assert not any(e.startswith("submit:bronze-ingest") for e in events)


def test_c360_foreign_deployment_is_not_touched(monkeypatch, tmp_path):
    events = _drive_sustained(monkeypatch, tmp_path, _c360_cfg(), owned=False)
    assert events == ["ownership"]


def test_existing_state_refuses_without_force_reset(monkeypatch, tmp_path, capsys):
    events = _drive_sustained(
        monkeypatch, tmp_path, _c360_cfg(), existing=["c-s/", "c-b/customer/interactions/"]
    )
    assert events == ["ownership"]  # nothing stopped, deleted or submitted
    out = capsys.readouterr().out
    assert "--force-reset" in out and "silver.customer_interactions_enriched" in out
    assert "c-b/customer/interactions/" in out


def test_force_reset_proceeds_over_existing_state(monkeypatch, tmp_path):
    events = _drive_sustained(
        monkeypatch, tmp_path, _c360_cfg(), existing=["c-s/"], force_reset=True
    )
    assert "submit:bronze-verify:{'LB_CONTINUOUS_RESET': '1'}" in events
    assert any(e.startswith("submit:bronze-ingest") for e in events)


def test_reset_timeout_scales_like_the_aml_budget(monkeypatch, tmp_path):
    from lakebench.spark.job import aml_bronze_verify_timeout_budget

    cfg = _c360_cfg()
    _drive_sustained(monkeypatch, tmp_path, cfg)
    scale = cfg.architecture.workload.datagen.get_effective_scale()
    got = events_ref["mon"].wait_for_completion.call_args.kwargs["timeout_seconds"]
    assert got == aml_bronze_verify_timeout_budget(scale)
    assert aml_bronze_verify_timeout_budget(1000) > aml_bronze_verify_timeout_budget(10)


def test_existing_state_lists_only_non_empty_prefixes(monkeypatch):
    cfg = _c360_cfg()
    raw = MagicMock()

    def list_objects_v2(Bucket, Prefix, MaxKeys):
        if Bucket == "c-g":
            raise RuntimeError("NoSuchBucket")
        if (Bucket, Prefix) == ("c-s", ""):
            return {"KeyCount": 1}
        if (Bucket, Prefix) == ("c-b", "checkpoints/bronze-ingest/"):
            raise RuntimeError("AccessDenied")
        return {"KeyCount": 0}

    raw.list_objects_v2.side_effect = list_objects_v2
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: MagicMock(raw_client=raw))
    found = _sustained._c360_existing_state(cfg, clear_raw=True)
    assert found[0] == "c-s/"
    assert found[1].startswith("c-b/checkpoints/bronze-ingest/ (could not list")
    assert len(found) == 2
    listed = [c.kwargs["Prefix"] for c in raw.list_objects_v2.call_args_list]
    assert "customer/interactions/" in listed
    raw.list_objects_v2.reset_mock()
    _sustained._c360_existing_state(cfg, clear_raw=False)
    assert "customer/interactions/" not in [
        c.kwargs["Prefix"] for c in raw.list_objects_v2.call_args_list
    ]


def test_run_command_passes_force_reset(monkeypatch, tmp_path):
    from typer.testing import CliRunner

    from lakebench.cli import app

    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(
        "name: c360-flag\n"
        "platform:\n  storage:\n    s3:\n      endpoint: http://127.0.0.1:1\n"
        "      access_key: x\n      secret_key: y\n"
    )
    seen = {}
    monkeypatch.setattr(
        "lakebench.cli._sustained._run_sustained",
        lambda *a, **kw: seen.update(kw),
    )
    res = CliRunner().invoke(
        app, ["run", str(cfg_file), "--sustained", "--skip-deploy", "--force-reset"]
    )
    assert seen.get("force_reset") is True, res.output
    res = CliRunner().invoke(app, ["run", str(cfg_file), "--sustained", "--skip-deploy"])
    assert seen.get("force_reset") is False, res.output
