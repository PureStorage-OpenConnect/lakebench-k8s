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

    With the change reverted the record reads FAILED with no interrupted
    block and the silver-build application stays in the fake cluster (a
    SIGTERM reaches the sentinel instead).
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
    _assert_handlers_restored(sentinel_sigterm)


def test_interrupt_batch_generate_deletes_datagen_job(tmp_path, monkeypatch, sentinel_sigterm):
    """With --generate the datagen Job this run created is deleted by uid too."""
    argv = ["--generate", "--yes"]
    trace, rec, record = _run(
        "batch_c360", tmp_path, monkeypatch, argv=argv, interrupt=("bronze-verify", "SIGINT")
    )
    assert trace["unscripted"] == []
    assert trace["exit_code"] == 130
    assert record["verdict"]["status"] == "INTERRUPTED"
    intr = record["interrupted"]
    assert intr["stopped"] == ["Job/lakebench-datagen", "SparkApplication/lakebench-bronze-verify"]
    [delete] = _deletes(rec, "Job")
    assert delete[2:] == ["lakebench-datagen", "runchar", "uid-datagen-1", "Background"]
    assert rec.datagen_uid is None and rec.apps == {}


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
    assert intr["left"][0]["reason"].startswith("uid unknown")
    assert "lakebench-silver-build" in rec.apps
    assert not _deletes(rec, "SparkApplication")


def test_second_interrupt_skips_remaining_deletes(tmp_path, monkeypatch, sentinel_sigterm):
    """A second Ctrl-C during the cleanup skips what is left of it; the
    record is still written and still INTERRUPTED."""
    trace, rec, record = _run(
        "batch_c360",
        tmp_path,
        monkeypatch,
        argv=["--generate", "--yes"],
        interrupt=("bronze-verify", "SIGINT"),
        interrupt_delete_after=0,
    )
    assert trace["exit_code"] == 130
    assert record["verdict"]["status"] == "INTERRUPTED"
    intr = record["interrupted"]
    # The first delete ran to its reply; the second was never sent.
    assert intr["stopped"] == ["Job/lakebench-datagen"]
    assert intr["skipped"] == ["SparkApplication/lakebench-bronze-verify"]
    assert len(_deletes(rec, "Job")) == 1 and not _deletes(rec, "SparkApplication")
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


# ---------------------------------------------------------------------------
# Continuous
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("how", ["SIGINT", "SIGTERM"])
def test_interrupt_continuous_deletes_datagen_job(tmp_path, monkeypatch, sentinel_sigterm, how):
    """A signal at window second 300: the three streams and the datagen Job
    this run created are deleted by uid, the record reads INTERRUPTED at
    the window, and nothing is deleted by name afterwards.

    With the change reverted the datagen Job is never deleted and the record
    reads FAILED.
    """
    trace, rec, record = _run("continuous_c360", tmp_path, monkeypatch, events=((300.0, how),))
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
