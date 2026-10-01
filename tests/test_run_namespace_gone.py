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
    # Start, 42 sleeps of the 1800 s window, 3 rounds, 2 maintenance rounds
    # and 1 compaction round.
    assert len(in_window) == 1 + 42 + 3 + 2 + 1


# ---------------------------------------------------------------------------
# NamespaceWatch
# ---------------------------------------------------------------------------


def _ns(uid="u1", phase="Active", deleting=False):
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid, deletion_timestamp="t" if deleting else None),
        status=SimpleNamespace(phase=phase),
    )


def _watch(*answers):
    from lakebench.cli._sustained import NamespaceWatch

    api = MagicMock()
    api.read_namespace.side_effect = list(answers)
    patcher = patch("kubernetes.client.CoreV1Api", return_value=api)
    patcher.start()
    w = NamespaceWatch("ns1")
    return w, api, patcher


def test_watch_rules():
    from kubernetes.client.rest import ApiException

    from lakebench.cli._sustained import NamespaceGone

    e503 = ApiException(status=503, reason="Unavailable")
    w, api, p = _watch(_ns(), _ns(), _ns(deleting=True))
    try:
        w.start()
        assert w.uid == "u1"
        w.check(1.0)
        with pytest.raises(NamespaceGone, match="is being deleted") as info:
            w.check(2.0)
        assert info.value.at_elapsed == 2.0
        assert api.read_namespace.call_args.kwargs["_request_timeout"] == (5, 10)
    finally:
        p.stop()
    w, api, p = _watch(_ns(), e503, e503, _ns(), e503, e503, e503)
    try:
        w.start()
        w.check(1.0)
        w.check(2.0)
        w.check(3.0)  # answered: the strikes reset
        w.check(4.0)
        w.check(5.0)
        with pytest.raises(NamespaceGone, match="unreadable"):
            w.check(6.0)
    finally:
        p.stop()
    w, api, p = _watch(_ns("u1"), _ns("u2"))
    try:
        w.start()
        with pytest.raises(NamespaceGone, match="created again"):
            w.check(1.0)
    finally:
        p.stop()


def test_watch_without_a_start_uid_still_sees_a_404():
    from kubernetes.client.rest import ApiException

    from lakebench.cli._sustained import NamespaceGone

    w, api, p = _watch(RuntimeError("down"), _ns("u9"), ApiException(status=404, reason="NF"))
    try:
        w.start()
        assert w.uid is None
        w.check(1.0)  # no start uid: a uid is not compared
        with pytest.raises(NamespaceGone, match="was deleted"):
            w.check(2.0)
    finally:
        p.stop()
