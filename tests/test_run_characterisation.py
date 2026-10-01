"""Characterisation test of ``lakebench run`` (QA-9, DESIGN ch05 section 1).

Each scenario drives the real ``run`` command through the harness in
``tests/harness/run_harness.py`` and compares its trace (cluster and object
store calls, Spark submissions and their environment, journal events, the
saved metrics.json's shape and verdict) with a golden file under
``tests/fixtures/run_char/``. Every later change to ``cli/_run.py`` or
``cli/_sustained.py`` runs behind these tests; a change that moves a trace on
purpose ships a new golden written by a second agent from the record (SPEC
section 6.4). There is no update flag.
"""

from __future__ import annotations

import enum

import pytest

from tests.harness.run_harness import (
    FIXTURES,
    TraceMismatch,
    assert_trace_equal,
    fixture_problems,
    load_golden,
    run_scenario,
    scrub_driver_log,
)


def test_batch_c360(tmp_path, monkeypatch):
    trace = run_scenario("batch_c360", tmp_path, monkeypatch)
    assert_trace_equal(trace, load_golden("batch_c360"))


def test_seeded_stage_order_change_fails(tmp_path, monkeypatch):
    """The spec's named case, through the real run(): JobType's
    BRONZE_VERIFY and SILVER_BUILD values are swapped, so run() submits
    silver-build where it used to submit bronze-verify (its stage labels
    and the monitor's job names are separate strings and do not move), and
    the characterisation fails on that first submission."""
    import lakebench.spark.job as job_mod

    class SwappedJobType(enum.Enum):
        BRONZE_VERIFY = "silver-build"
        SILVER_BUILD = "bronze-verify"
        GOLD_FINALIZE = "gold-finalize"

    monkeypatch.setattr(job_mod, "JobType", SwappedJobType)
    trace = run_scenario("batch_c360", tmp_path, monkeypatch)
    # The first difference is the first submission: silver-build where the
    # golden has bronze-verify.
    with pytest.raises(TraceMismatch, match=r"submit_job', 'silver-build'"):
        assert_trace_equal(trace, load_golden("batch_c360"))


def test_batch_c360_silver_fails(tmp_path, monkeypatch):
    trace = run_scenario("batch_c360_silver_fails", tmp_path, monkeypatch)
    assert_trace_equal(trace, load_golden("batch_c360_silver_fails"))


def test_trace_mismatch_names_the_first_difference():
    golden = {"exit_code": 0, "submits": [["bronze-verify"], ["silver-build"]]}
    with pytest.raises(TraceMismatch, match=r"submits\[0\]"):
        assert_trace_equal(
            {"exit_code": 0, "submits": [["silver-build"], ["bronze-verify"]]}, golden
        )
    with pytest.raises(TraceMismatch, match="not in the golden"):
        assert_trace_equal({**golden, "extra": 1}, golden)


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*/*.log")), ids=lambda p: p.name)
def test_fixture_logs_are_scrubbed(path):
    """Fixture driver logs entered through the scrubber (SPEC section 6 rule
    5): they are its fixed point and carry no address or credential."""
    text = path.read_text()
    assert scrub_driver_log(text, "runchar") == text
    assert fixture_problems(text) == []


def test_fixture_check_refuses_hosts_and_foreign_buckets():
    """The scrub check refuses a URL host (named or numeric) other than the
    placeholder and a bucket not of the harness deployment."""
    assert fixture_problems("[lb] wrote http://10.0.1.50:80/x") == []
    assert fixture_problems("[lb] wrote http://s3.lab.example:80/x") == ["host s3.lab.example"]
    assert fixture_problems("[lb] read s3a://lab-bronze/x") == ["bucket lab-bronze"]


def test_seam_fakes_refuse_calls_the_real_signature_would():
    """A call the real ``get_executor`` would reject (a wrong keyword) is
    unscripted in the harness too, never silently accepted."""
    import lakebench.benchmark.executor as executor_mod
    from tests.harness.run_harness import Recorder, Unscripted, _checked, _unbound

    rec = Recorder()
    real = _unbound(executor_mod.get_executor)
    _checked(rec, real, object(), namespace="ns")
    with pytest.raises(Unscripted):
        _checked(rec, real, object(), ns="ns")
    assert len(rec.unscripted) == 1


def test_continuous_c360(tmp_path, monkeypatch):
    trace = run_scenario("continuous_c360", tmp_path, monkeypatch)
    assert_trace_equal(trace, load_golden("continuous_c360"))


def test_seeded_continuous_window_change_fails(tmp_path, monkeypatch):
    """A seeded change to the continuous runner, through the real
    _run_sustained(): the streams no longer receive their window length,
    and the characterisation fails on the first stream submission."""
    import lakebench.cli._sustained as sustained

    real = sustained._streaming_job_env

    def without_window(run_id, run_duration):
        env = real(run_id, run_duration)
        env.pop("LB_CONTINUOUS_WINDOW_S")
        return env

    monkeypatch.setattr(sustained, "_streaming_job_env", without_window)
    trace = run_scenario("continuous_c360", tmp_path, monkeypatch)
    with pytest.raises(TraceMismatch, match=r"submits\[1\]"):
        assert_trace_equal(trace, load_golden("continuous_c360"))


def test_stream_logs_are_cut_at_the_cluster_clock():
    from datetime import datetime

    from tests.harness.run_harness import log_until

    text = (
        "[lb] 2026-10-01T15:05:58.280383 - start\n"
        "untimed line\n"
        "[lb] 2026-10-01T15:06:30.000000 - batch 1\n"
        "[lb] 2026-10-01T15:07:00.000000 - batch 2\n"
    )
    cut = log_until(text, datetime(2026, 10, 1, 15, 6, 30))
    assert cut.splitlines() == [
        "[lb] 2026-10-01T15:05:58.280383 - start",
        "untimed line",
        "[lb] 2026-10-01T15:06:30.000000 - batch 1",
    ]
    assert log_until(text, datetime(2026, 10, 1, 15, 0)) == ""


def test_batch_aml(tmp_path, monkeypatch):
    trace = run_scenario("batch_aml", tmp_path, monkeypatch)
    assert_trace_equal(trace, load_golden("batch_aml"))


def test_continuous_c360_skip_generate(tmp_path, monkeypatch):
    trace = run_scenario("continuous_c360_skip_generate", tmp_path, monkeypatch)
    assert_trace_equal(trace, load_golden("continuous_c360_skip_generate"))
