"""AML-1: common.rule_stage_profile reads a job group's stages from the
driver's status store, with the UI off, and marks the profile truncated when
the store has already dropped some of the group's jobs."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

# A store that keeps only 3 jobs and 3 stages, so eviction is reachable.
pytestmark = pytest.mark.spark_static_conf(
    {
        "spark.ui.enabled": "false",
        "spark.ui.retainedJobs": "3",
        "spark.ui.retainedStages": "3",
    }
)


def _profile(spark, common, group, work):
    sc = spark.sparkContext
    sc.setJobGroup(group, "test", interruptOnCancel=False)
    try:
        first = common.spark_jobs_submitted(spark)
        work()
        return first, common.rule_stage_profile(spark, group, "WX", first_job=first)
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)
        sc.setLocalProperty("spark.job.description", None)
        sc.setLocalProperty("spark.job.interruptOnCancel", None)


def test_profile_of_one_job(spark_session, load_script, capsys):
    common = load_script("common")
    first, rows = _profile(
        spark_session, common, "g-one", lambda: spark_session.range(1000).collect()
    )
    assert isinstance(first, int)
    assert rows and rows[0]["tasks"] >= 1 and rows[0]["max_task_s"] is not None, rows
    out = capsys.readouterr().out
    assert "[stage-profile] rule=WX group=g-one stage=" in out
    assert "truncated=false" in out


def test_evicted_jobs_mark_the_profile_truncated(spark_session, load_script, capsys):
    common = load_script("common")

    def many_jobs():
        for _ in range(6):
            spark_session.range(10).collect()

    _, rows = _profile(spark_session, common, "g-many", many_jobs)
    assert rows, rows
    out = capsys.readouterr().out
    assert "truncated=true" in out and "truncated=false" not in out, out
