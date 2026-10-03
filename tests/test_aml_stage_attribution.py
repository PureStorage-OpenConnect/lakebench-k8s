"""AML-3 fallback: scripts/aml_stage_attribution.py builds the per-rule
stage profile from a Spark event log (event names and keys as Spark 4.0.1
and 4.1.1 write them, checked against real local logs on 2026-10-02) and
attaches it to a record."""

from __future__ import annotations

import json
from pathlib import Path

from tests.conftest import exec_repo_script

ROOT = Path(__file__).resolve().parents[1]


def _script():
    return exec_repo_script(ROOT / "scripts" / "aml_stage_attribution.py", "aml_stage_attribution")


def _events():
    def job(jid, group, stages):
        return {
            "Event": "SparkListenerJobStart",
            "Job ID": jid,
            "Stage IDs": stages,
            "Properties": (
                {"spark.jobGroup.id": group, "spark.job.description": f"{group[8:10]} run r1"}
                if group
                else {}
            ),
        }

    def task(sid, run_ms, read=0):
        return {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": sid,
            "Stage Attempt ID": 0,
            "Task Metrics": {
                "Executor Run Time": run_ms,
                "Shuffle Read Metrics": {"Remote Bytes Read": read, "Local Bytes Read": 0},
            },
        }

    def done(sid, name, tasks, sub, comp):
        return {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {
                "Stage ID": sid,
                "Stage Attempt ID": 0,
                "Stage Name": name,
                "Number of Tasks": tasks,
                "Submission Time": sub,
                "Completion Time": comp,
            },
        }

    return [
        job(0, "lb-rule-W5_sanctions_match-0a1b2c3d", [1, 2]),
        task(1, 400_000, read=1048576),
        task(1, 300_000),
        task(2, 5_000),
        done(1, "count at\nNativeMethodAccessorImpl.java:0", 2, 1000, 481_000),
        done(2, "collect at x.py:1", 1, 481_000, 486_000),
        job(1, "lb-rule-W2_structuring-11112222", [3]),
        task(3, 2_000),
        done(3, "count at y", 1, 0, 2_000),
        job(2, None, [4]),  # outside any rule group: TM, setup
        task(4, 999_000),
        done(4, "tm", 1, 0, 999_000),
    ]


def test_profile_per_rule_from_events():
    mod = _script()
    prof = mod.profile_from_events([json.dumps(e) for e in _events()], top=3)
    assert set(prof) == {"W5_sanctions_match", "W2_structuring"}
    top = prof["W5_sanctions_match"][0]
    assert top == {
        "stage": 1,
        "attempt": 0,
        "status": "COMPLETE",
        "tasks": 2,
        "wall_s": 480.0,
        "exec_s": 700.0,
        "shuffle_read_mb": 1.0,
        "max_task_s": 400.0,
        "name": "count at NativeMethodAccessorImpl.java:0",
        "stages": 2,
        "truncated": False,
        "complete": True,
        "lossy": None,
        "source": "eventlog",
    }
    assert [s["stage"] for s in prof["W5_sanctions_match"]] == [1, 2]
    assert mod.profile_from_events([json.dumps(e) for e in _events()], top=1)[
        "W5_sanctions_match"
    ] == [top]


def test_a_retried_stage_counts_its_last_attempt_only():
    mod = _script()
    events = _events()
    retry = [
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 2,
            "Stage Attempt ID": 1,
            "Task Metrics": {"Executor Run Time": 9_000},
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {
                "Stage ID": 2,
                "Stage Attempt ID": 1,
                "Stage Name": "collect at x.py:1",
                "Number of Tasks": 1,
                "Submission Time": 0,
                "Completion Time": 9_000,
            },
        },
    ]
    events[5]["Stage Info"]["Failure Reason"] = "FetchFailed"
    prof = mod.profile_from_events([json.dumps(e) for e in events + retry])
    st2 = [s for s in prof["W5_sanctions_match"] if s["stage"] == 2]
    assert len(st2) == 1 and st2[0]["attempt"] == 1 and st2[0]["exec_s"] == 9.0
    assert st2[0]["status"] == "COMPLETE"


def test_rolling_log_directory_is_read_in_order(tmp_path):
    mod = _script()
    d = tmp_path / "eventlog_v2_local-1"
    d.mkdir()
    lines = [json.dumps(e) for e in _events()]
    (d / "events_2_local-1").write_text("\n".join(lines[6:]) + "\n")
    (d / "events_1_local-1").write_text("\n".join(lines[:6]) + "\n")
    (d / "appstatus_local-1").write_text("")
    prof = mod.profile_from_events(mod._lines(d))
    assert set(prof) == {"W5_sanctions_match", "W2_structuring"}


def test_attach_rewrites_the_gold_job_and_attribution(tmp_path):
    mod = _script()
    rec = tmp_path / "metrics.json"
    rec.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "job_name": "g",
                        "job_type": "gold-finalize",
                        "elapsed_seconds": 1000.0,
                        "rule_elapsed_s": {"W5_sanctions_match": 600.0, "W2_structuring": 5.0},
                        "stage_profile_unavailable": {"W5_sanctions_match": "Py4JError"},
                    }
                ],
                "run_id": "r1",
                "experiment": {"attribution": {"profile": "unavailable: Py4JError"}},
            }
        )
    )
    prof, run_ids = mod.read_events([json.dumps(e) for e in _events()])
    assert run_ids == {"r1"}
    block = mod.attach(rec, prof, run_ids)
    data = json.loads(rec.read_text())
    job = data["jobs"][0]
    assert job["stage_profile_unavailable"] == {}
    assert job["stage_profile"]["W5_sanctions_match"][0]["stage"] == 1
    assert block["dominant_rule"] == "W5_sanctions_match"
    assert block["dominant_stage"]["stage"] == 1 and block["profile"] == "read"
    assert data["experiment"]["attribution"]["profile_source"] == "eventlog"


def test_attach_refuses_another_runs_log(tmp_path):
    import pytest

    mod = _script()
    rec = tmp_path / "metrics.json"
    original = json.dumps(
        {"run_id": "r2", "jobs": [{"job_type": "gold-finalize", "rule_elapsed_s": {"W5": 1.0}}]}
    )
    rec.write_text(original)
    prof, run_ids = mod.read_events([json.dumps(e) for e in _events()])
    with pytest.raises(SystemExit, match="r1"):
        mod.attach(rec, prof, run_ids)
    assert rec.read_text() == original
