"""The interrupt record, its verdict, and the cleanup's rules (V16-5).

Unit tier under tests/test_run_interrupt.py, which drives the whole run:
- the verdict of an interrupted run is INTERRUPTED or FAILED, never PASSED;
- ``prior_failure`` (or a record that does not say) keeps FAILED;
- the stage the interrupt stopped is not a failed job;
- ``interrupted`` survives a save and load;
- ``RunInterrupt``: signal names, handlers saved and restored, an ignored
  signal left ignored, the second and third signal, the cleanup deadline,
  ``LeaseAbort``;
- ``submit_job`` returns the uid of the object the API server created.
"""

from __future__ import annotations

import signal
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from lakebench.cli import _interrupt
from lakebench.cli._interrupt import RunInterrupt
from lakebench.metrics.collector import JobMetrics, PipelineMetrics
from lakebench.metrics.verdict import compute_verdict

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _job(stage: str, success: bool = True, error: str | None = None) -> JobMetrics:
    return JobMetrics(
        job_name=f"lakebench-{stage}",
        job_type=stage,
        start_time=T0,
        end_time=T0,
        elapsed_seconds=10.0,
        success=success,
        error_message=error,
    )


def _metrics(interrupted: dict | None, jobs: list[JobMetrics], success: bool = False):
    m = PipelineMetrics(run_id="r1", deployment_name="d", start_time=T0, success=success)
    m.jobs = jobs
    m.interrupted = interrupted
    return m


def _intr(**kw) -> dict:
    rec = {
        "signal": "SIGINT",
        "at_stage": "silver-build",
        "at_utc": T0.isoformat(),
        "prior_failure": False,
        "stopped": [],
        "left": [],
        "skipped": [],
    }
    rec.update(kw)
    return rec


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------


_RAN = ["bronze-verify", "silver-build", "gold-finalize"]


def _verdict_case(case):
    if case == "in-flight":
        return _metrics(
            _intr(), [_job("bronze-verify"), _job("silver-build", False, "interrupted")]
        )
    if case == "prior-failure":
        return _metrics(_intr(prior_failure=True), [_job("bronze-verify")])
    if case.startswith("prior="):
        prior = {"None": None, "no": "no", "0": 0, "1": 1}[case.split("=", 1)[1]]
        return _metrics(_intr(prior_failure=prior), [_job("bronze-verify")])
    if case == "another-failed-gate":
        return _metrics(
            _intr(at_stage="gold-finalize"),
            [_job("bronze-verify", False, "driver exited with code 1"), _job("silver-build")],
        )
    if case == "interrupted-elsewhere":
        # only the stage the record names is excused, not any job reading "interrupted"
        return _metrics(
            _intr(at_stage="gold-finalize"), [_job("silver-build", False, "interrupted")]
        )
    if case == "benchmark-failure":
        m = _metrics(_intr(at_stage="benchmark"), [_job("bronze-verify")])
        m.benchmark_error = "query engine gone"
        return m
    if case.startswith("success="):
        return _metrics(_intr(), [_job("bronze-verify")], success=case.endswith("True"))
    if case == "not-interrupted":
        jobs = [_job(s) for s in _RAN]
        for j in jobs:
            j.output_rows = 100  # rows in every layer: the layer_rows gate passes
        return _metrics(None, jobs, success=True)
    if case == "not-interrupted-failed":
        return _metrics(None, [_job("silver-build", False, "interrupted")], success=False)
    raise AssertionError(case)


@pytest.mark.parametrize(
    ("case", "status", "gates"),
    [
        ("in-flight", "INTERRUPTED", {"pipeline": "PASS", "interrupt": "INTERRUPTED"}),
        # something had already failed: the interrupt does not soften it
        ("prior-failure", "FAILED", {"interrupt": "INTERRUPTED"}),
        *[(f"prior={p}", "FAILED", None) for p in ("None", "no", "0", "1")],
        ("another-failed-gate", "FAILED", {"pipeline": "FAIL"}),
        ("interrupted-elsewhere", "FAILED", None),
        ("benchmark-failure", "FAILED", None),
        ("not-interrupted", "PASSED", None),
        ("not-interrupted-failed", "FAILED", None),
    ],
)
def test_interrupt_verdict(case, status, gates):
    v = compute_verdict(_verdict_case(case))
    assert v.status == status
    for k, want in (gates or {}).items():
        assert v.gates[k] == want


@pytest.mark.parametrize("success", [True, False])
def test_interrupted_is_never_passed(success):
    """Even a record whose success flag were True."""
    assert compute_verdict(_verdict_case(f"success={success}")).status != "PASSED"


def test_interrupted_round_trips_through_storage(tmp_path):
    from lakebench.metrics.storage import MetricsStorage

    m = _metrics(_intr(stopped=["SparkApplication/lakebench-silver-build"]), [_job("x")])
    m.end_time = T0
    data = m.to_dict()
    assert data["interrupted"]["stopped"] == ["SparkApplication/lakebench-silver-build"]
    loaded = MetricsStorage(tmp_path)._dict_to_metrics(data)
    assert loaded.interrupted == data["interrupted"]
    assert loaded.to_dict()["verdict"] == data["verdict"]
    assert "interrupted" not in _metrics(None, []).to_dict()


# ---------------------------------------------------------------------------
# RunInterrupt
# ---------------------------------------------------------------------------


@pytest.fixture
def handlers():
    saved = {s: signal.getsignal(s) for s in _interrupt.INTERRUPT_SIGNALS}
    yield
    for s, h in saved.items():
        signal.signal(s, h)


def test_install_and_restore(handlers):
    before = signal.getsignal(signal.SIGTERM)
    ri = RunInterrupt("ns")
    ri.install()
    assert signal.getsignal(signal.SIGTERM) == ri._on_signal
    ri.restore()
    assert signal.getsignal(signal.SIGTERM) == before


def test_ignored_signal_stays_ignored(handlers):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    ri = RunInterrupt("ns")
    ri.install()
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN
    ri.restore()
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN


def test_a_leaked_instance_is_replaced_not_chained(handlers):
    before = signal.getsignal(signal.SIGTERM)
    first = RunInterrupt("ns")
    first.install()  # never restored: an error in its run's finally
    second = RunInterrupt("ns")
    second.install()
    assert second._saved[signal.SIGTERM] == before
    second.restore()
    assert signal.getsignal(signal.SIGTERM) == before


def test_first_raises_second_skips_third_stops(handlers, capsys):
    ri = RunInterrupt("ns")
    ri.install()
    with pytest.raises(KeyboardInterrupt):
        ri._on_signal(signal.SIGTERM, None)
    assert ri.sealing and ri.received == ["SIGTERM"]
    ri._on_signal(signal.SIGINT, None)  # does not raise
    assert ri.skip
    assert "the run record is still being written" in capsys.readouterr().err
    with pytest.raises(KeyboardInterrupt):
        ri._on_signal(signal.SIGINT, None)
    assert signal.getsignal(signal.SIGTERM) != ri._on_signal  # restored
    assert ri.interrupt_signal(KeyboardInterrupt()) == "SIGTERM"


def test_lease_abort_names_its_signal():
    from lakebench.deploy.cluster_lock import LeaseAbort

    ri = RunInterrupt("ns")
    rec = ri.seal(at_stage="operator-check", prior_failure=False, exc=LeaseAbort(signal.SIGHUP))
    assert rec["signal"] == "SIGHUP"
    # cluster_lock itself names the recovery when it aborts inside the lease.
    assert rec["left"] == []


def test_no_handler_means_sigint():
    assert RunInterrupt("ns").interrupt_signal(KeyboardInterrupt()) == "SIGINT"


def test_registry_rules():
    ri = RunInterrupt("ns")
    ri.creating("SparkApplication", "lakebench-a")
    ri.created("SparkApplication", "lakebench-a", "u1")
    ri.finished("SparkApplication", "lakebench-a")
    ri.creating("SparkApplication", "lakebench-b")
    failed = MagicMock(name="status")
    from lakebench.spark.job import JobState

    failed.name, failed.state = "lakebench-c", JobState.FAILED
    ri.creating("SparkApplication", "lakebench-c")
    ri.submitted(failed)
    rec = ri.seal(at_stage="x", prior_failure=False)
    # a finished: kept; b: being created; c: its submit failed, nothing to stop.
    assert rec["skipped"] == ["SparkApplication/lakebench-b"]


def test_cleanup_deadline_leaves_the_rest(monkeypatch):
    ri = RunInterrupt("ns")
    for n in ("a", "b"):
        ri.created("SparkApplication", f"lakebench-{n}", f"u-{n}")
    from types import SimpleNamespace

    clock = iter([0.0, 0.0, 1000.0, 1000.0])
    monkeypatch.setattr(_interrupt, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    monkeypatch.setattr(RunInterrupt, "_stop_one", lambda self, e, api, timeout: ("stopped", ""))
    rec = ri.seal(at_stage="x", prior_failure=False)
    ri.stop_owned(rec)
    assert rec["stopped"] == ["SparkApplication/lakebench-a"]
    assert rec["left"] == [
        {
            "object": "SparkApplication/lakebench-b",
            "reason": "not reached: the cleanup deadline passed",
        }
    ]
    assert rec["skipped"] == []


def test_delete_carries_the_uid_precondition():
    ri = RunInterrupt("ns")
    ri.created("Job", "lakebench-datagen", "u-dg")
    batch = MagicMock()
    with patch("kubernetes.client.BatchV1Api", return_value=batch):
        rec = ri.seal(at_stage="datagen", prior_failure=False)
        ri.stop_owned(rec)
    kwargs = batch.delete_namespaced_job.call_args.kwargs
    assert kwargs["name"] == "lakebench-datagen" and kwargs["namespace"] == "ns"
    assert kwargs["body"].preconditions.uid == "u-dg"
    assert kwargs["body"].propagation_policy == "Background"
    connect, read = kwargs["_request_timeout"]
    assert connect == _interrupt.CLEANUP_CONNECT_TIMEOUT_S
    assert 0 < read <= _interrupt.CLEANUP_READ_TIMEOUT_S
    assert rec["stopped"] == ["Job/lakebench-datagen"]


def test_delete_answers():
    for status, outcome in [(404, "stopped"), (409, "left"), (500, "left")]:
        from kubernetes.client.rest import ApiException

        ri = RunInterrupt("ns")
        ri.created("SparkApplication", "lakebench-x", "u-x")
        api = MagicMock()
        api.delete_namespaced_custom_object.side_effect = ApiException(status=status, reason="r")
        with patch("kubernetes.client.CustomObjectsApi", return_value=api):
            rec = ri.seal(at_stage="x", prior_failure=False)
            ri.stop_owned(rec)
        if outcome == "stopped":
            assert rec["stopped"] == ["SparkApplication/lakebench-x"]
        else:
            assert [x["object"] for x in rec["left"]] == ["SparkApplication/lakebench-x"]


def test_unknown_uid_job_without_this_runs_id_is_left():
    ri = RunInterrupt("ns", run_id="r1")
    ri.creating("Job", "lakebench-datagen")
    batch = MagicMock()
    with patch("kubernetes.client.BatchV1Api", return_value=batch):
        rec = ri.seal(at_stage="datagen", prior_failure=False)
        ri.stop_owned(rec)
    batch.delete_namespaced_job.assert_not_called()
    assert rec["left"][0]["reason"].startswith("exists, but")


def test_unknown_uid_absent_object_is_not_recorded():
    """The interrupt landed before the create: nothing to stop, nothing left."""
    from kubernetes.client.rest import ApiException

    ri = RunInterrupt("ns", run_id="r1")
    ri.creating("Job", "lakebench-datagen")
    batch = MagicMock()
    batch.read_namespaced_job.side_effect = ApiException(status=404, reason="Not Found")
    with patch("kubernetes.client.BatchV1Api", return_value=batch):
        rec = ri.seal(at_stage="datagen", prior_failure=False)
        ri.stop_owned(rec)
    batch.delete_namespaced_job.assert_not_called()
    assert rec["stopped"] == rec["left"] == rec["skipped"] == []


def test_cleanup_client_does_not_retry():
    api_client = _interrupt.no_retry_api_client()
    assert api_client.configuration.retries is False
    api_client.close()


# ---------------------------------------------------------------------------
# submit_job
# ---------------------------------------------------------------------------


def test_submit_job_returns_the_created_uid():
    from lakebench.spark.job import JobState, JobType, SparkJobManager
    from tests.fixtures.scripts_maps_helpers import FakeK8s, _cfg

    k8s = FakeK8s()
    mgr = SparkJobManager(_cfg(), k8s)
    assert mgr.deploy_scripts_configmap() is True
    api = MagicMock()
    api.create_namespaced_custom_object.return_value = {"metadata": {"uid": "abc-123"}}
    with patch("kubernetes.client.CustomObjectsApi", return_value=api):
        status = mgr.submit_job(JobType.SILVER_BUILD)
    assert status.state == JobState.SUBMITTED and status.uid == "abc-123"
    api.create_namespaced_custom_object.return_value = {}
    with patch("kubernetes.client.CustomObjectsApi", return_value=api):
        assert mgr.submit_job(JobType.SILVER_BUILD).uid is None
