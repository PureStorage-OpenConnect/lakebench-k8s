"""Ctrl-C and SIGTERM seal ``lakebench run`` INTERRUPTED and stop its jobs (V16-5).

Each scenario drives the real ``run`` through the QA-9 harness
(tests/harness/run_harness.py) and interrupts it the way an operator would:
a real SIGINT or SIGTERM sent to this process while a stage runs, inside the
cluster lease, or at a window second of a continuous run. Asserted from the
saved metrics.json and the fake cluster: the verdict is INTERRUPTED (never
PASSED), the exit code is 130, every unfinished SparkApplication and datagen
Job this run created was deleted with its uid as a precondition, and nothing
it did not create was deleted.

A sentinel SIGTERM handler is installed around every test, so a change that
stops handling SIGTERM fails the test instead of killing the test process.
"""

from __future__ import annotations

import dataclasses
import signal

import pytest

from tests.harness.run_harness import (
    SCENARIOS,
    run_scenario_full,
    saved_record,
)


class SentinelSigterm(Exception):
    """SIGTERM reached the handler that was installed before the run."""


@pytest.fixture(autouse=True)
def sentinel_sigterm():
    def handler(signum, frame):
        raise SentinelSigterm

    previous = signal.signal(signal.SIGTERM, handler)
    previous_int = signal.getsignal(signal.SIGINT)
    try:
        yield handler
    finally:
        signal.signal(signal.SIGTERM, previous)
        signal.signal(signal.SIGINT, previous_int)


def _run(base: str, tmp_path, monkeypatch, **changes):
    scenario = dataclasses.replace(SCENARIOS[base], **changes)
    trace, rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    return trace, rec, saved_record(tmp_path)


def _deletes(rec, kind: str) -> list[list]:
    if kind == "SparkApplication":
        return [c for c in rec.calls if c[:2] == ["CustomObjectsApi", "delete"]]
    return [c for c in rec.calls if c[:2] == ["BatchV1Api", "delete_namespaced_job"]]


def _assert_handlers_restored(sentinel) -> None:
    assert signal.getsignal(signal.SIGTERM) is sentinel
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("how", ["SIGINT", "SIGTERM", "raise"])
def test_interrupt_batch_seals_interrupted(tmp_path, monkeypatch, sentinel_sigterm, how):
    """A signal while silver-build runs: INTERRUPTED, 130, its app deleted by uid.

    With the change reverted the record reads success true and PASSED, with
    no interrupted block, and the silver-build application stays in the
    fake cluster (a SIGTERM reaches the sentinel instead).
    """
    trace, rec, record = _run("batch_c360", tmp_path, monkeypatch, interrupt=("silver-build", how))
    assert trace["unscripted"] == []
    assert trace["exit_code"] == 130
    assert record["success"] is False
    assert record["verdict"]["status"] == "INTERRUPTED"
    assert record["verdict"]["gates"]["interrupt"] == "INTERRUPTED"
    # bronze-verify completed and passed; silver-build is not a failed job.
    assert record["verdict"]["gates"]["pipeline"] == "PASS"
    intr = record["interrupted"]
    assert intr["signal"] == ("SIGINT" if how == "raise" else how)
    assert intr["at_stage"] == "silver-build"
    assert intr["prior_failure"] is False
    assert intr["stopped"] == ["SparkApplication/lakebench-silver-build"]
    assert intr["left"] == [] and intr["skipped"] == []
    # Deleted with the uid of the object this run created, in the background.
    [delete] = _deletes(rec, "SparkApplication")
    assert delete[3] == "lakebench-silver-build"
    assert delete[5] == "uid-silver-build-2" and delete[6] == "Background"
    # The completed stage keeps its application (and its driver logs).
    assert set(rec.apps) == {"lakebench-bronze-verify"}
    # Gold never ran; the in-flight stage is recorded as interrupted.
    assert [s[0] for s in rec.submits] == ["bronze-verify", "silver-build"]
    jobs = record["jobs"]
    assert jobs[-1]["job_type"] == "silver-build"
    assert jobs[-1]["error_message"] == "interrupted"
    assert jobs[-1]["success"] is False
    # The corpus observation still runs after an interrupt (it never raises):
    # one listing of the datagen prefix, and the record carries it.
    assert ["S3", "paginate list_objects_v2", "runchar-bronze", "customer/interactions/"] in [
        c[:4] for c in rec.calls
    ]
    assert "corpus_observation" in record["config_snapshot"]["experiment_inputs"]
    _assert_handlers_restored(sentinel_sigterm)


def test_interrupt_batch_generate_keeps_the_finished_datagen_job(
    tmp_path, monkeypatch, sentinel_sigterm
):
    """--generate: datagen finished before bronze-verify, so on an interrupt in
    bronze-verify its Job is kept (with its pods' logs); only the running
    stage is stopped."""
    trace, rec, record = _run(
        "batch_c360",
        tmp_path,
        monkeypatch,
        argv=["--generate", "--yes"],
        interrupt=("bronze-verify", "SIGINT"),
    )
    assert trace["unscripted"] == []
    assert trace["exit_code"] == 130
    assert record["verdict"]["status"] == "INTERRUPTED"
    assert record["interrupted"]["stopped"] == ["SparkApplication/lakebench-bronze-verify"]
    assert not _deletes(rec, "Job")
    assert rec.datagen_uid == "uid-datagen-1"


def test_interrupt_during_batch_datagen_deletes_its_job(tmp_path, monkeypatch, sentinel_sigterm):
    """Ctrl-C while the datagen Job runs: it is deleted with its uid."""
    trace, rec, record = _run(
        "batch_c360",
        tmp_path,
        monkeypatch,
        argv=["--generate", "--yes"],
        interrupt=("datagen", "SIGINT"),
    )
    assert trace["unscripted"] == []
    assert trace["exit_code"] == 130
    intr = record["interrupted"]
    assert intr["at_stage"] == "datagen"
    assert intr["stopped"] == ["Job/lakebench-datagen"]
    [delete] = _deletes(rec, "Job")
    assert delete[2:] == ["lakebench-datagen", "runchar", "uid-datagen-1", "Background"]
    assert rec.datagen_uid is None and rec.submits == []


def test_uid_precondition_leaves_foreign_object(tmp_path, monkeypatch, sentinel_sigterm):
    """An application of the same name recreated by another invocation: the
    API server answers 409 to the uid precondition and it is left running."""
    trace, rec, record = _run(
        "batch_c360",
        tmp_path,
        monkeypatch,
        interrupt=("silver-build", "SIGINT"),
        foreign=("SparkApplication/lakebench-silver-build",),
    )
    assert trace["exit_code"] == 130
    assert record["verdict"]["status"] == "INTERRUPTED"
    intr = record["interrupted"]
    assert intr["stopped"] == []
    assert intr["left"] == [
        {
            "object": "SparkApplication/lakebench-silver-build",
            "reason": "not ours: recreated since this run created it",
        }
    ]
    assert "lakebench-silver-build" in rec.apps
    # Never deleted by name: the by-name path is not used on an interrupt.
    assert not [c for c in rec.calls if c[:2] == ["k8s", "delete_custom_resource"]]


def test_interrupt_during_submit_finds_its_own_application(tmp_path, monkeypatch, sentinel_sigterm):
    """The create landed but the interrupt came before its reply: the run reads
    the application back and deletes it only because it carries this run's
    LB_RUN_ID."""
    trace, rec, record = _run("batch_c360", tmp_path, monkeypatch, submit_interrupt="silver-build")
    assert trace["exit_code"] == 130
    intr = record["interrupted"]
    assert intr["at_stage"] == "silver-build"
    assert intr["stopped"] == ["SparkApplication/lakebench-silver-build"]
    assert "lakebench-silver-build" not in rec.apps
    # Not a job record: the stage never started from the run's side.
    assert [j["job_type"] for j in record["jobs"]] == ["bronze-verify"]


def test_interrupt_during_submit_of_unknown_owner_is_left(tmp_path, monkeypatch, sentinel_sigterm):
    """An application read back that does not carry this run's id is left."""
    from tests.harness import run_harness

    real_submit = run_harness.FakeJobManager.submit_job

    def foreign_run_id(self, *args, **kwargs):
        # The object the create left carries another run's id on its driver
        # (whatever created it, it cannot be shown to be this run's).
        try:
            return real_submit(self, *args, **kwargs)
        finally:
            for name in self._rec.app_run_ids:
                self._rec.app_run_ids[name] = ["20990101-000000-ffffff-c1"]

    monkeypatch.setattr(run_harness.FakeJobManager, "submit_job", foreign_run_id)
    trace, rec, record = _run("batch_c360", tmp_path, monkeypatch, submit_interrupt="silver-build")
    assert trace["exit_code"] == 130
    intr = record["interrupted"]
    assert intr["stopped"] == []
    assert [x["object"] for x in intr["left"]] == ["SparkApplication/lakebench-silver-build"]
    assert intr["left"][0]["reason"].startswith("exists, but this run cannot show it created it")
    assert "lakebench-silver-build" in rec.apps
    assert not _deletes(rec, "SparkApplication")


def test_second_interrupt_stops_the_cleanup(tmp_path, monkeypatch, sentinel_sigterm):
    """A second Ctrl-C during the cleanup cuts the delete in flight and skips
    the rest; the record is still written and still INTERRUPTED."""
    trace, rec, record = _run(
        "continuous_c360",
        tmp_path,
        monkeypatch,
        datagen_running=True,
        events=((700.0, "SIGINT"),),
        interrupt_delete_after=1,
    )
    assert trace["exit_code"] == 130
    assert record["verdict"]["status"] == "INTERRUPTED"
    intr = record["interrupted"]
    # The first delete ran to its reply; the second was cut, the rest not sent.
    assert intr["stopped"] == ["Job/lakebench-datagen"]
    assert intr["skipped"] == [
        "SparkApplication/lakebench-bronze-ingest",
        "SparkApplication/lakebench-silver-stream",
        "SparkApplication/lakebench-gold-refresh",
    ]
    assert len(_deletes(rec, "Job")) == 1
    assert len(_deletes(rec, "SparkApplication")) == 1  # sent, then cut
    assert rec.live_streams == {
        "lakebench-bronze-ingest",
        "lakebench-silver-stream",
        "lakebench-gold-refresh",
    }
    # Skipped on request: not stopped by name afterwards either.
    assert not [c for c in rec.calls if c[:2] == ["k8s", "delete_custom_resource"]]
    _assert_handlers_restored(sentinel_sigterm)


def test_sigterm_inside_lease_is_deferred(tmp_path, monkeypatch, sentinel_sigterm):
    """SIGTERM during the watch-list heal's helm upgrade (cluster-safety 2):
    the upgrade finishes, the lease is released, and only then does the
    interrupt land and seal the record."""
    trace, rec, record = _run("batch_c360", tmp_path, monkeypatch, lease_signal="SIGTERM")
    assert trace["unscripted"] == []
    assert trace["exit_code"] == 130
    marks = [c for c in rec.calls if c[0] in ("Lease", "SparkOperator") and c[1] != "init"]
    assert [m[:3] for m in marks] == [
        ["SparkOperator", "check_status"],
        ["SparkOperator", "ensure_namespace_watched", True],
        ["Lease", "create", "lakebench-system"],
        ["SparkOperator", "helm upgrade", "start"],
        ["SparkOperator", "helm upgrade", "done"],
        ["Lease", "delete", "lakebench-system"],
    ]
    assert record["verdict"]["status"] == "INTERRUPTED"
    intr = record["interrupted"]
    assert intr["signal"] == "SIGTERM" and intr["at_stage"] == "operator-check"
    assert intr["stopped"] == [] and intr["left"] == []
    assert rec.submits == []
    _assert_handlers_restored(sentinel_sigterm)


def test_sigterm_inside_lease_without_the_deferral_lands_inside_helm(
    tmp_path, monkeypatch, sentinel_sigterm
):
    """The same scenario with the lease's deferral bypassed: the interrupt
    lands inside the helm upgrade, which never finishes. This is what the
    test above rules out."""
    from lakebench.deploy import cluster_lock

    monkeypatch.setattr(cluster_lock._SignalDeferral, "install", lambda self, held: False)
    trace, rec, record = _run("batch_c360", tmp_path, monkeypatch, lease_signal="SIGTERM")
    assert ["SparkOperator", "helm upgrade", "done"] not in rec.calls
    assert record["interrupted"]["at_stage"] == "operator-check"


def test_interrupt_after_a_gate_failed_reads_failed(tmp_path, monkeypatch, sentinel_sigterm):
    """A gate that fails the run without stopping it, then Ctrl-C in the
    benchmark: prior_failure is true and the record reads FAILED, not
    INTERRUPTED."""
    import lakebench.metrics.c360_correctness as c360

    monkeypatch.setattr(c360, "gating_problems", lambda rec: ["forced gate problem"])
    trace, rec, record = _run(
        "batch_c360", tmp_path, monkeypatch, interrupt=("benchmark", "SIGINT")
    )
    assert trace["exit_code"] == 130
    intr = record["interrupted"]
    # The first benchmark is the pre-compaction one, part of Phase 5.
    assert intr["at_stage"] == "maintenance" and intr["prior_failure"] is True
    assert record["verdict"]["status"] == "FAILED"
    # Every stage had completed: nothing to stop.
    assert intr["stopped"] == [] and not _deletes(rec, "SparkApplication")


def test_interrupt_in_the_benchmark_of_a_good_run_reads_interrupted(
    tmp_path, monkeypatch, sentinel_sigterm
):
    trace, rec, record = _run(
        "batch_c360", tmp_path, monkeypatch, interrupt=("benchmark", "SIGTERM")
    )
    assert trace["exit_code"] == 130
    assert record["interrupted"]["prior_failure"] is False
    assert record["verdict"]["status"] == "INTERRUPTED"
    assert set(rec.apps) == {
        "lakebench-bronze-verify",
        "lakebench-silver-build",
        "lakebench-gold-finalize",
    }


@pytest.mark.parametrize(
    "ended, status, job_error",
    [
        ("COMPLETED", "INTERRUPTED", "interrupted"),
        ("FAILED", "FAILED", "FAILED (interrupted before its result was read)"),
        # The operator retries a failed submission; the stage has not failed.
        ("SUBMISSION_FAILED", "INTERRUPTED", "interrupted"),
    ],
)
def test_interrupt_after_the_stage_ended(
    tmp_path, monkeypatch, sentinel_sigterm, ended, status, job_error
):
    """The monitor saw silver-build end and was reading its log: a completed
    stage keeps its application; a failed one makes the run FAILED."""
    trace, rec, record = _run(
        "batch_c360",
        tmp_path,
        monkeypatch,
        interrupt=("silver-build", "SIGINT"),
        interrupt_after_state=ended,
    )
    assert trace["exit_code"] == 130
    assert record["verdict"]["status"] == status
    assert record["jobs"][-1]["error_message"] == job_error
    assert record["interrupted"]["prior_failure"] is (ended == "FAILED")
    if ended == "COMPLETED":
        assert "lakebench-silver-build" in rec.apps
        assert record["interrupted"]["stopped"] == []
    else:
        assert "lakebench-silver-build" not in rec.apps


def test_interrupt_while_a_failed_stage_is_parsed_reads_failed(
    tmp_path, monkeypatch, sentinel_sigterm
):
    """silver-build failed; Ctrl-C lands while its driver log is parsed,
    before its job is recorded: the run still reads FAILED."""
    from lakebench.metrics.collector import MetricsCollector
    from tests.harness.run_harness import send_interrupt

    real = MetricsCollector.parse_driver_logs

    def parse(self, logs, stage, *a, **k):
        if stage == "silver-build":
            send_interrupt("SIGINT")
        return real(self, logs, stage, *a, **k)

    monkeypatch.setattr(MetricsCollector, "parse_driver_logs", parse)
    trace, rec, record = _run("batch_c360_silver_fails", tmp_path, monkeypatch)
    assert trace["exit_code"] == 130
    assert record["interrupted"]["prior_failure"] is True
    assert record["verdict"]["status"] == "FAILED"


def test_failed_datagen_deploy_registers_no_job(tmp_path, monkeypatch, sentinel_sigterm):
    """deploy() reported failure: whatever Job holds the name is not this
    run's, and an interrupt does not touch it."""
    from lakebench.deploy.engine import DeploymentResult, DeploymentStatus
    from tests.harness import run_harness

    def failed_deploy(self, *args, **kwargs):
        self._rec.add("Datagen", "deploy")
        return DeploymentResult(
            component="datagen", status=DeploymentStatus.FAILED, message="apply failed"
        )

    monkeypatch.setattr(run_harness.FakeDatagenDeployer, "deploy", failed_deploy)
    trace, rec, record = _run(
        "batch_c360",
        tmp_path,
        monkeypatch,
        argv=["--generate", "--yes"],
        interrupt=("datagen", "SIGINT"),
    )
    assert trace["exit_code"] == 130
    assert record["interrupted"]["stopped"] == []
    assert record["interrupted"]["left"] == []
    assert not _deletes(rec, "Job")


def test_interrupt_in_the_score_stage_stops_the_score_app(tmp_path, monkeypatch, sentinel_sigterm):
    trace, rec, record = _run(
        "batch_aml", tmp_path, monkeypatch, interrupt=("score-financial", "SIGINT")
    )
    assert trace["exit_code"] == 130
    intr = record["interrupted"]
    assert intr["at_stage"] == "score-financial"
    assert intr["stopped"] == ["SparkApplication/lakebench-score-financial"]
    assert record["verdict"]["status"] == "INTERRUPTED"


@pytest.mark.parametrize("how", ["SIGINT", "SIGTERM"])
def test_a_signal_while_results_are_gathered_keeps_the_record(
    tmp_path, monkeypatch, sentinel_sigterm, how
):
    """Ctrl-C during Phase 7 (the bucket sizing, slow at scale) of a run whose
    stages all completed: the record is still written, sealed INTERRUPTED
    at "results". Without the seal at the top of the finally the signal
    escaped it and no metrics.json was written."""
    from lakebench.metrics.collector import MetricsCollector
    from tests.harness.run_harness import send_interrupt

    real = MetricsCollector.record_actual_sizes

    def sizes(self, *args, **kwargs):
        send_interrupt(how)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(MetricsCollector, "record_actual_sizes", sizes)
    trace, rec, record = _run("batch_c360", tmp_path, monkeypatch)
    assert trace["runs_saved"] == 1
    assert trace["exit_code"] == 130
    assert record["verdict"]["status"] == "INTERRUPTED"
    assert record["interrupted"]["at_stage"] == "results"
    assert record["interrupted"]["signal"] == how
    # The sizing it was in finished; nothing was stopped.
    assert record["bronze_size_gb"] > 0 and record["interrupted"]["stopped"] == []
    _assert_handlers_restored(sentinel_sigterm)


# ---------------------------------------------------------------------------
# Continuous
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("how", ["SIGINT", "SIGTERM"])
def test_interrupt_continuous_deletes_datagen_job(tmp_path, monkeypatch, sentinel_sigterm, how):
    """A signal in the window while datagen still runs: the three streams and
    the datagen Job this run created are deleted by uid, the record reads
    INTERRUPTED at the window, and nothing is deleted by name afterwards.

    With the change reverted the datagen Job is never deleted and the record
    reads FAILED.
    """
    trace, rec, record = _run(
        "continuous_c360",
        tmp_path,
        monkeypatch,
        datagen_running=True,
        events=((700.0, how),),
    )
    assert trace["unscripted"] == []
    assert trace["exit_code"] == 130
    assert record["success"] is False
    assert record["verdict"]["status"] == "INTERRUPTED"
    intr = record["interrupted"]
    assert intr["signal"] == how and intr["at_stage"] == "window"
    assert intr["prior_failure"] is False
    assert intr["stopped"] == [
        "Job/lakebench-datagen",
        "SparkApplication/lakebench-bronze-ingest",
        "SparkApplication/lakebench-silver-stream",
        "SparkApplication/lakebench-gold-refresh",
    ]
    assert intr["left"] == [] and intr["skipped"] == []
    [job_delete] = _deletes(rec, "Job")
    assert job_delete[4:] == ["uid-datagen-1", "Background"]
    assert {c[3] for c in _deletes(rec, "SparkApplication")} == {
        "lakebench-bronze-ingest",
        "lakebench-silver-stream",
        "lakebench-gold-refresh",
    }
    assert rec.live_streams == set() and rec.datagen_uid is None
    # The finished reset preflight keeps its application.
    assert set(rec.apps) == {"lakebench-bronze-verify"}
    assert not [c for c in rec.calls if c[:2] == ["k8s", "delete_custom_resource"]]
    _assert_handlers_restored(sentinel_sigterm)


def test_interrupt_continuous_keeps_a_finished_datagen_job(tmp_path, monkeypatch, sentinel_sigterm):
    """Datagen finished before the streams started (the record's case): only
    the streams are stopped."""
    trace, rec, record = _run("continuous_c360", tmp_path, monkeypatch, events=((300.0, "SIGINT"),))
    assert trace["unscripted"] == []
    assert trace["exit_code"] == 130
    assert [s.split("/")[0] for s in record["interrupted"]["stopped"]] == ["SparkApplication"] * 3
    assert not _deletes(rec, "Job") and rec.datagen_uid == "uid-datagen-1"


def test_continuous_interrupt_after_a_gate_failed_reads_failed(
    tmp_path, monkeypatch, sentinel_sigterm
):
    """The window gate failed the run; Ctrl-C lands while the streams are
    being stopped: FAILED, and the streams are stopped by uid."""
    import lakebench.cli._sustained as sustained
    import lakebench.metrics.continuous_window as window

    monkeypatch.setattr(window, "window_gate_problems", lambda *a, **k: ["forced gate problem"])

    def ctrl_c(*a, **k):
        raise KeyboardInterrupt

    monkeypatch.setattr(sustained, "_stop_streams", ctrl_c)
    trace, rec, record = _run("continuous_c360", tmp_path, monkeypatch)
    assert trace["exit_code"] == 130
    intr = record["interrupted"]
    assert intr["prior_failure"] is True and intr["at_stage"] == "stop-streams"
    assert record["verdict"]["status"] == "FAILED"
    assert len(intr["stopped"]) == 3 and rec.live_streams == set()


def test_continuous_signal_while_streams_stop_in_the_finally_keeps_the_record(
    tmp_path, monkeypatch, sentinel_sigterm
):
    """A run that failed after the window (an error, not an interrupt) is
    stopping its streams by name in the finally when SIGTERM arrives: the
    streams are all stopped, the record is still written, sealed at
    "results" and FAILED, and the run's own error still ends it (a failed
    run keeps its exit)."""
    import lakebench.cli._sustained as sustained
    import lakebench.metrics.continuous_window as window
    from tests.harness.run_harness import send_interrupt

    def boom(*a, **k):
        raise RuntimeError("window error")

    monkeypatch.setattr(window, "window_gate_problems", boom)
    real_stop = sustained._stop_streams

    def stop(k8s, ns, submitted):
        send_interrupt("SIGTERM")
        return real_stop(k8s, ns, submitted)

    monkeypatch.setattr(sustained, "_stop_streams", stop)
    # The CLI reports the error as one line and exits 1 (unhandled_exception).
    trace, rec, record = _run("continuous_c360", tmp_path, monkeypatch)
    assert trace["exit_code"] == 1
    assert record["interrupted"]["at_stage"] == "results"
    assert record["interrupted"]["signal"] == "SIGTERM"
    assert record["interrupted"]["prior_failure"] is True
    assert record["verdict"]["status"] == "FAILED"
    _assert_handlers_restored(sentinel_sigterm)


def test_interrupt_continuous_skip_generate_stops_only_streams(
    tmp_path, monkeypatch, sentinel_sigterm
):
    """--skip-generate: the datagen Job (not this run's) is never touched."""
    trace, rec, record = _run(
        "continuous_c360_skip_generate", tmp_path, monkeypatch, events=((300.0, "SIGINT"),)
    )
    assert trace["exit_code"] == 130
    intr = record["interrupted"]
    assert [s for s in intr["stopped"] if s.startswith("Job/")] == []
    assert not _deletes(rec, "Job")
    assert len(intr["stopped"]) == 3


def test_a_refusal_exit_in_continuous_is_never_a_success(tmp_path, monkeypatch, sentinel_sigterm):
    """A continuous run that stops on a scripts-map failure exits 1, and its
    record says failed (the flag used to stay set, so it saved PASSED)."""
    from tests.harness import run_harness

    monkeypatch.setattr(
        run_harness.FakeJobManager, "deploy_scripts_configmap", lambda self, *a, **k: False
    )
    trace, rec, record = _run("continuous_c360", tmp_path, monkeypatch)
    assert trace["exit_code"] == 1
    assert record["success"] is False
    assert record["verdict"]["status"] == "FAILED"
