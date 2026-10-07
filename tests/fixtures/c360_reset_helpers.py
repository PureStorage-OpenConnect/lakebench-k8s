"""Shared test helpers moved from tests/test_c360_continuous_reset.py (imported by several test files)."""

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


class _StopAfterFirstStream(Exception):
    pass


events_ref: dict = {}


def _drive_sustained(
    monkeypatch,
    tmp_path,
    cfg,
    *,
    reset_ok=True,
    owned=True,
    existing=(),
    force_reset=False,
    raw_problem=None,
    dg_state="unfinished",
    skip_generate=False,
    stop_raises=None,
    deploy_result=None,
):
    """Run _run_sustained with every cluster and S3 edge mocked; return the
    ordered list of side effects it performed. The run is stopped at the
    first stream submit, unless *deploy_result* is given: datagen starts
    after the streams, so those runs go on to its deploy."""
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
            events.append(f"dg_running={jm.datagen_running}")
            if deploy_result is None:
                raise _StopAfterFirstStream
        return MagicMock(state=JobState.RUNNING)

    jm.submit_job.side_effect = submit
    monkeypatch.setattr("lakebench.engine.get_engine", lambda c, k: jm)
    mon = MagicMock()
    mon.wait_for_completion.return_value = _Result(reset_ok)
    monkeypatch.setattr("lakebench.spark.SparkJobMonitor", lambda *a, **kw: mon)
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda c, **kw: MagicMock())
    dg = MagicMock()

    def dg_deploy():
        events.append("datagen")
        if deploy_result is not None:
            return deploy_result
        return MagicMock(status=_sustained_status_success())

    def dg_stop():
        events.append("stop-datagen")
        if stop_raises is not None:
            raise stop_raises

    dg.deploy.side_effect = dg_deploy
    monkeypatch.setattr("lakebench.deploy.datagen.stop_previous_datagen", lambda c: dg_stop())
    monkeypatch.setattr("lakebench.deploy.datagen.end_continuous_datagen", lambda c: True)
    # The window opens at datagen's first file in bronze.
    monkeypatch.setattr(_sustained, "_wait_for_bronze_data", lambda *a, **kw: True)
    monkeypatch.setattr("lakebench.deploy.DatagenDeployer", lambda e, **kw: dg)

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
    monkeypatch.setattr(_sustained, "_c360_raw_replace_problem", lambda c: raw_problem)
    monkeypatch.setattr(_sustained, "_datagen_job_state", lambda ns: (dg_state, ""))
    monkeypatch.setattr(_sustained, "_DATAGEN_RELEASE_WAIT_S", 0)
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: MagicMock())
    monkeypatch.setattr(_sustained, "_collect_platform_metrics", lambda *a, **kw: None)
    events_ref["mon"] = mon

    # The run ends at the first stream submit (or an Exit); the finally
    # block may then fail on mocked metrics, which is irrelevant here.
    with pytest.raises(Exception) as ei:  # noqa: B017
        _sustained._run_sustained(
            cfg,
            tmp_path / "cfg.yaml",
            60,
            True,
            60,
            skip_generate=skip_generate,
            force_reset=force_reset,
        )
    events_ref["exc"] = _exit_in_chain(ei.value)
    return events


def _exit_in_chain(exc):
    """The typer/click Exit that ended the run: the finally block may raise
    over it on mocked metrics, which keeps it as ``__context__``."""
    seen = exc
    while seen is not None:
        if hasattr(seen, "exit_code"):
            return seen
        seen = seen.__context__
    return exc


def _sustained_status_success():
    from lakebench.deploy import DeploymentStatus

    return DeploymentStatus.SUCCESS
