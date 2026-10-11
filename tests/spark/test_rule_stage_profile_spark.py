"""AML-1: common.rule_stage_profile reads a job group's stages from the
driver's status store, with the UI off, and marks the profile truncated when
the store has already dropped some of the group's jobs."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from lakebench.metrics.stage_profile import parse_stage_profile

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
    # A job that completes before the mark: the store then shows nothing of
    # the group can have been evicted yet.
    spark.range(1).collect()
    sc.setJobGroup(group, "test", interruptOnCancel=False)
    try:
        mark = common.rule_profile_mark(spark)
        work()
        return mark, common.rule_stage_profile(spark, group, "WX", mark=mark)
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)
        sc.setLocalProperty("spark.job.description", None)
        sc.setLocalProperty("spark.job.interruptOnCancel", None)


def test_profile_of_one_job(spark_session, load_script, capsys):
    common = load_script("common")
    mark, rows = _profile(
        spark_session, common, "g-one", lambda: spark_session.range(1000).collect()
    )
    assert isinstance(mark["jobs"], int) and mark["dropped"] == 0, mark
    assert rows and rows[0]["tasks"] >= 1 and rows[0]["max_task_s"] is not None, rows
    parsed, unavailable, _cost = parse_stage_profile(capsys.readouterr().out)
    assert not unavailable, unavailable
    stages = parsed["WX"]
    assert stages and stages[0]["tasks"] >= 1, stages
    assert all(not r["truncated"] and r["complete"] and not r["lossy"] for r in stages), stages


def test_evicted_jobs_mark_the_profile_truncated(spark_session, load_script, capsys):
    common = load_script("common")

    def many_jobs():
        for _ in range(6):
            spark_session.range(10).collect()

    _, rows = _profile(spark_session, common, "g-many", many_jobs)
    assert rows, rows
    parsed, unavailable, _cost = parse_stage_profile(capsys.readouterr().out)
    stages = parsed.get("WX") or []
    assert stages and all(r["truncated"] for r in stages), (parsed, unavailable)
