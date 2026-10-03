"""AML-1: common.rule_stage_profile never raises; a status store it cannot
read gives one ``unavailable`` line and None."""

from __future__ import annotations


class _NoStore:
    @property
    def sparkContext(self):  # noqa: N802
        raise RuntimeError("no py4j\nsecond line")


def test_unreadable_store_logs_unavailable(load_script, capsys):
    common = load_script("common")
    assert (
        common.rule_stage_profile(
            _NoStore(), "g", "W2_structuring", mark={"jobs": 3, "dropped": 0, "ms": 10}
        )
        is None
    )
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1, out
    assert out[0].endswith(
        "[stage-profile] rule=W2_structuring group=g unavailable "
        "reason=RuntimeError: no py4j second line"
    ), out
    assert common.rule_profile_mark(_NoStore()) is None


class _Obj:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _fake_spark(*, wait=None, last_stage=None, jobs_now=1, dropped=0):
    """A SparkContext stand-in: one job (id 0) in the group with stage 7."""
    counter = _Obj(getCount=lambda: dropped)
    registry = _Obj(counter=lambda name: counter)
    bus = _Obj(
        waitUntilEmpty=wait or (lambda ms: None),
        metrics=lambda: _Obj(metricRegistry=lambda: registry),
    )
    none = _Obj(isDefined=lambda: False)
    done = _Obj(isDefined=lambda: True, get=lambda: _Obj(getTime=lambda: 0))
    retained = _Obj(
        size=lambda: 1,
        apply=lambda i: _Obj(
            jobId=lambda: 0,
            status=lambda: _Obj(toString=lambda: "SUCCEEDED"),
            completionTime=lambda: done,
            jobGroup=lambda: none,
        ),
    )
    jsc = _Obj(
        listenerBus=lambda: bus,
        statusStore=lambda: _Obj(lastStageAttempt=last_stage, jobsList=lambda statuses: retained),
        dagScheduler=lambda: _Obj(numTotalJobs=lambda: jobs_now),
    )
    tracker = _Obj(
        getJobIdsForGroup=lambda g: [0],
        getJobInfo=lambda j: _Obj(stageIds=[7]),
    )
    sc = _Obj(_jsc=_Obj(sc=lambda: jsc), statusTracker=lambda: tracker)
    return _Obj(sparkContext=sc)


def _evicted(sid):
    raise RuntimeError("An error occurred: java.util.NoSuchElementException: No stage with id 7")


def test_evicted_stage_is_truncation_not_failure(load_script, capsys):
    common = load_script("common")
    rows = common.rule_stage_profile(
        _fake_spark(last_stage=_evicted), "g", "W2", mark={"jobs": 0, "dropped": 0, "ms": 10}
    )
    assert rows == []
    out = capsys.readouterr().out
    assert "rule=W2 group=g stages=0 truncated=true complete=true lossy=false profile_s=" in out


def test_other_store_error_is_unavailable_not_truncation(load_script, capsys):
    """Only an evicted stage counts as truncation: any other store failure
    (an API break on a new Spark) gives the unavailable line."""
    common = load_script("common")

    def broken(sid):
        raise RuntimeError(
            "py4j.Py4JException: Method lastStageAttempt([class Integer]) does not exist"
        )

    rows = common.rule_stage_profile(
        _fake_spark(last_stage=broken), "g", "W2", mark={"jobs": 0, "dropped": 0, "ms": 10}
    )
    assert rows is None
    out = capsys.readouterr().out
    assert "rule=W2 group=g unavailable reason=RuntimeError: py4j.Py4JException" in out


def test_listener_that_does_not_drain_is_incomplete(load_script, capsys):
    common = load_script("common")

    def timeout(ms):
        raise RuntimeError("java.util.concurrent.TimeoutException")

    common.rule_stage_profile(
        _fake_spark(wait=timeout, last_stage=_evicted, dropped=2),
        "g",
        "W2",
        mark={"jobs": 0, "dropped": 1, "ms": 10},
    )
    out = capsys.readouterr().out
    assert "truncated=true complete=false lossy=true" in out


def test_without_a_mark_the_flags_are_not_claimed_clean(load_script, capsys):
    common = load_script("common")
    common.rule_stage_profile(_fake_spark(last_stage=_evicted), "g", "W2", mark=None)
    out = capsys.readouterr().out
    assert "truncated=true complete=true lossy=true" in out


def test_truncation_is_read_from_completion_order(load_script):
    """The store evicts completed jobs oldest completion first: a held job
    that completed before the rule's mark proves nothing of the rule was
    evicted; otherwise the profile is flagged."""
    common = load_script("common")
    mark = 1000
    held_before = [(1, "SUCCEEDED", 900, None), (7, "SUCCEEDED", 1500, "g")]
    assert common._jobs_evicted_since(held_before, mark) is False
    # A low-id job that completed late does not vouch for anything.
    late_low = [(1, "SUCCEEDED", 1600, None), (9, "SUCCEEDED", 1500, "g")]
    assert common._jobs_evicted_since(late_low, mark) is True
    # A running job is never evicted, so it does not vouch either.
    running = [(1, "RUNNING", None, None), (9, "SUCCEEDED", 1500, "g")]
    assert common._jobs_evicted_since(running, mark) is True
    assert common._jobs_evicted_since([], mark) is True
