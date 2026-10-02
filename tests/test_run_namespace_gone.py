"""The continuous runner stops within one loop interval when its namespace
is gone (V16-6).

Driven through the QA-9 harness (tests/harness/run_harness.py) on the
continuous C360 record: at a window second the fake namespace is deleted,
deleted and created again, or starts terminating. The loop reads the
namespace after every 30 s sleep, so the run must stop at the next read,
exit 1, and save a record that names the reason (``abort_reason``). A read
that fails twice and then answers is not a reason; three in a row is.
"""

from __future__ import annotations

import dataclasses
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from tests.harness.run_harness import SCENARIOS, run_scenario_full, saved_record

#: The loop's sleep between namespace reads (cli/_sustained.py check_interval).
INTERVAL_S = 30.0


def _run(tmp_path, monkeypatch, *events):
    scenario = dataclasses.replace(SCENARIOS["continuous_c360"], events=events)
    trace, rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    return trace, rec, saved_record(tmp_path)


@pytest.mark.parametrize(
    "event, reason",
    [
        ("namespace_gone", "namespace runchar was deleted"),
        ("namespace_redeployed", "namespace runchar was deleted and created again"),
        ("namespace_terminating", "namespace runchar is being deleted"),
    ],
)
def test_namespace_gone_exits_within_one_interval(tmp_path, monkeypatch, event, reason):
    """Gone from second 95: the run stops at its next read (120), within
    one interval, instead of running its 1800 s window.

    With the change reverted the run reads nothing and runs to the end of
    the window with every round (exit 0)."""
    trace, rec, record = _run(tmp_path, monkeypatch, (95.0, event))
    assert trace["unscripted"] == []
    assert trace["exit_code"] == 1
    abort = record["abort_reason"]
    assert abort["reason"] == reason
    assert 95.0 <= abort["at_elapsed"] <= 95.0 + INTERVAL_S
    assert record["success"] is False
    assert record["verdict"]["status"] == "FAILED"
    assert f"Run stopped: {reason}" in record["verdict"]["reasons"]
    # No in-window round ran (the first is due at 300 s).
    assert record.get("benchmark_rounds", []) == []
    # The streams went with the namespace: nothing is deleted, by uid or by name.
    assert not [c for c in rec.calls if c[1] in ("delete", "delete_custom_resource")]
    assert "interrupted" not in record
    # Not observed: the bucket may be a redeployment's. Said so, not absent.
    assert not [c for c in rec.calls if c[:2] == ["S3", "paginate list_objects_v2"]]
    obs = record["config_snapshot"]["experiment_inputs"]["corpus_observation"]
    assert obs["markers"]["error"] == f"corpus not observed: {reason}"
    # The end load sample reads the cluster, not this namespace: still taken.
    observed = record["config_snapshot"]["experiment_inputs"]["observed"]
    assert "end" in observed["allocatable"]


def test_a_namespace_read_that_fails_twice_is_not_a_reason(tmp_path, monkeypatch):
    """Two 503s in a row, then the namespace answers: the run goes on to
    the end of its window."""
    trace, rec, record = _run(tmp_path, monkeypatch, (95.0, "namespace_blip"))
    assert "abort_reason" not in record
    assert trace["exit_code"] == 0
    assert len(record["benchmark_rounds"]) == 3


def test_three_failed_reads_stop_the_run(tmp_path, monkeypatch):
    trace, rec, record = _run(tmp_path, monkeypatch, (95.0, "namespace_unreadable"))
    assert trace["exit_code"] == 1
    abort = record["abort_reason"]
    assert abort["reason"].startswith("namespace runchar unreadable (503")
    # Reads at 120, 150, 180: the third failure stops it.
    assert abort["at_elapsed"] == pytest.approx(180.0)


def test_the_namespace_is_read_at_every_interval(tmp_path, monkeypatch):
    """A whole window: one read at the start, one per loop sleep, one before
    each round, maintenance and compaction round (the goldens pin where)."""
    trace, rec, record = _run(tmp_path, monkeypatch)
    assert trace["exit_code"] == 0
    i_window = next(i for i, c in enumerate(rec.calls) if c[:2] == ["VersionApi", "get_code"])
    in_window = [c for c in rec.calls[i_window:] if c[:2] == ["CoreV1Api", "read_namespace"]]
    # Start, 42 sleeps of the 1800 s window, before and after each of the 3
    # rounds, 2 maintenance rounds and 1 compaction round; then 29 settle
    # sleeps and one before the streams are stopped by name.
    assert len(in_window) == 1 + 42 + 3 + 3 + 2 + 1 + 29 + 1


def test_namespace_gone_inside_a_round_is_seen_when_it_ends(tmp_path, monkeypatch):
    """Gone at 350, inside round 1 (300 to 436.5): seen as the round ends,
    not after the next sleep."""
    trace, rec, record = _run(tmp_path, monkeypatch, (350.0, "namespace_gone"))
    assert trace["exit_code"] == 1
    assert record["abort_reason"]["at_elapsed"] == pytest.approx(436.5)


@pytest.mark.parametrize("event", ["namespace_gone", "namespace_redeployed"])
def test_namespace_gone_during_settle_stops_the_settle(tmp_path, monkeypatch, event):
    """After the window, the run waits up to 30 min for the corpus to settle
    and then stops its streams by name. Gone (or deployed again, with new
    streams of the same names) during the settle: the run stops at the next
    settle poll and deletes nothing by name."""
    trace, rec, record = _run(tmp_path, monkeypatch, (1810.0, event))
    assert trace["exit_code"] == 1
    abort = record["abort_reason"]
    assert 1810.0 <= abort["at_elapsed"] <= 1810.0 + INTERVAL_S
    assert not [c for c in rec.calls if c[:2] == ["k8s", "delete_custom_resource"]]
    assert record["verdict"]["status"] == "FAILED"


# ---------------------------------------------------------------------------
# NamespaceWatch
# ---------------------------------------------------------------------------


def _ns(uid="u1", phase="Active", deleting=False):
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid, deletion_timestamp="t" if deleting else None),
        status=SimpleNamespace(phase=phase),
    )


@pytest.fixture
def watch(monkeypatch):
    """A NamespaceWatch over a scripted CoreV1Api, on a clock the test moves."""
    import lakebench.cli._sustained as sustained

    clock = SimpleNamespace(t=0.0)
    monkeypatch.setattr(sustained, "time", SimpleNamespace(time=lambda: clock.t))

    def make(*answers):
        api = MagicMock()
        api.read_namespace.side_effect = list(answers)
        patcher = patch("kubernetes.client.CoreV1Api", return_value=api)
        patcher.start()
        made.append(patcher)
        return sustained.NamespaceWatch("ns1"), api

    made: list = []
    yield make, clock
    for p in reversed(made):  # the last patch saved the one before it
        p.stop()


def _e503():
    from kubernetes.client.rest import ApiException

    return ApiException(status=503, reason="Unavailable")


def test_watch_deleting_and_timeouts(watch):
    from lakebench.cli._sustained import NamespaceGone

    make, _ = watch
    w, api = make(_ns(), _ns(), _ns(deleting=True))
    w.start()
    assert w.uid == "u1"
    w.check(1.0)
    with pytest.raises(NamespaceGone, match="is being deleted") as info:
        w.check(2.0)
    assert info.value.at_elapsed == 2.0
    assert api.read_namespace.call_args.kwargs["_request_timeout"] == (5, 10)


def test_strikes_need_three_reads_over_a_minute(watch):
    from lakebench.cli._sustained import NamespaceGone

    make, clock = watch
    w, _ = make(_ns(), *[_e503() for _ in range(3)], _ns(), *[_e503() for _ in range(4)])
    w.start()
    for t in (10.0, 20.0, 30.0):  # three quick failures in one pass: not yet
        clock.t = t
        w.check(t)
    clock.t = 40.0
    w.check(40.0)  # answered: the strikes reset
    for t in (100.0, 130.0):
        clock.t = t
        w.check(t)
    clock.t = 160.0  # third failure 60 s after the first
    with pytest.raises(NamespaceGone, match="unreadable"):
        w.check(160.0)


def test_redeployed_namespace_is_seen(watch):
    from lakebench.cli._sustained import NamespaceGone

    make, _ = watch
    w, _ = make(_ns("u1"), _ns("u2"))
    w.start()
    with pytest.raises(NamespaceGone, match="created again"):
        w.check(1.0)


def test_start_retries_then_the_first_answer_sets_the_uid(watch):
    """Unread at the window start (three tries): the first read that answers
    becomes the baseline, so a later redeploy is still seen."""
    from kubernetes.client.rest import ApiException

    from lakebench.cli._sustained import NamespaceGone

    make, _ = watch
    w, _ = make(
        RuntimeError("down"),
        RuntimeError("down"),
        RuntimeError("down"),
        _ns("u9"),
        _ns("u9"),
        _ns("u10"),
    )
    w.start()
    assert w.uid is None
    w.check(1.0)
    assert w.uid == "u9"
    w.check(2.0)
    with pytest.raises(NamespaceGone, match="created again"):
        w.check(3.0)
    w2, _ = make(_ns("u1"), ApiException(status=404, reason="NF"))
    w2.start()
    with pytest.raises(NamespaceGone, match="was deleted"):
        w2.check(4.0)
