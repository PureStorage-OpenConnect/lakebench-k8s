"""Driver log capture across pod rotation (LB-279 Part B).

Tests the ``DriverLogCapturer`` lifecycle in isolation: mocks ``pinned_kubectl``
(the pod lookup) and ``pinned_kubectl_popen`` (the ``kubectl logs -f``
subprocess). The real cluster integration is covered by live runs.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lakebench.cli import _driver_log_capture as cap_mod
from lakebench.cli._driver_log_capture import DriverLogCapturer


class _FakeProc:
    """Just enough of subprocess.Popen to satisfy terminate/wait/poll."""

    def __init__(self, name: str, alive: bool = True):
        self.name = name
        self._alive = alive
        self.terminate_called = False
        self.kill_called = False

    def poll(self):
        return None if self._alive else 0

    def terminate(self):
        self.terminate_called = True
        self._alive = False

    def kill(self):
        self.kill_called = True
        self._alive = False

    def wait(self, timeout=None):
        if not self._alive:
            return 0
        return 0


def _kubectl_result(stdout: str, returncode: int = 0):
    return SimpleNamespace(stdout=stdout, stderr="", returncode=returncode)


@pytest.fixture
def tmp_run(tmp_path):
    d = tmp_path / "run-xyz"
    d.mkdir()
    return d


def test_watch_starts_and_close_terminates(tmp_run):
    cap = DriverLogCapturer("ns1", tmp_run, context=None)
    # No apps watched -> poller runs but captures nothing.
    with patch.object(cap_mod, "pinned_kubectl", MagicMock()):
        cap.watch([])
        time.sleep(0.1)
        cap.close()
    assert not (tmp_run / "drivers").exists() or list((tmp_run / "drivers").iterdir()) == []


def test_captures_first_driver_pod(tmp_run):
    procs: list[_FakeProc] = []

    def _popen(ctx, args, **kwargs):
        p = _FakeProc(args[-1])
        procs.append(p)
        return p

    with (
        patch.object(
            cap_mod,
            "pinned_kubectl",
            MagicMock(return_value=_kubectl_result("silver-stream-driver|uid-1|Running\n")),
        ),
        patch.object(cap_mod, "pinned_kubectl_popen", side_effect=_popen),
    ):
        cap = DriverLogCapturer("ns1", tmp_run, context=None)
        cap.watch(["lakebench-silver-stream"])
        time.sleep(2.5)  # let one poll loop fire
        cap.close()

    assert (tmp_run / "drivers" / "lakebench-silver-stream.driver.log").exists()
    assert len(procs) == 1
    assert procs[0].terminate_called


def test_rotates_file_on_pod_uid_change(tmp_run):
    """A resubmit creates a new pod with a new uid; we open a .r1.log file."""
    calls = {"n": 0}

    def _kubectl(ctx, args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return _kubectl_result("silver-stream-driver|uid-1|Running\n")
        return _kubectl_result("silver-stream-driver|uid-2|Running\n")

    procs: list[_FakeProc] = []

    def _popen(ctx, args, **kwargs):
        p = _FakeProc(args[-1])
        procs.append(p)
        return p

    with (
        patch.object(cap_mod, "pinned_kubectl", side_effect=_kubectl),
        patch.object(cap_mod, "pinned_kubectl_popen", side_effect=_popen),
        patch.object(cap_mod, "_POLL_INTERVAL_S", 0.1),
    ):
        cap = DriverLogCapturer("ns1", tmp_run, context=None)
        cap.watch(["lakebench-silver-stream"])
        time.sleep(0.6)
        cap.close()

    drivers = tmp_run / "drivers"
    assert (drivers / "lakebench-silver-stream.driver.log").exists()
    assert (drivers / "lakebench-silver-stream.driver.r1.log").exists()
    assert len(procs) >= 2  # one per pod uid
    for p in procs:
        assert p.terminate_called


def test_skips_non_running_pod(tmp_run):
    def _kubectl(ctx, args, **kwargs):
        return _kubectl_result("silver-stream-driver|uid-1|Pending\n")

    popen_calls: list[list[str]] = []

    def _popen(ctx, args, **kwargs):
        popen_calls.append(args)
        return _FakeProc("x")

    with (
        patch.object(cap_mod, "pinned_kubectl", side_effect=_kubectl),
        patch.object(cap_mod, "pinned_kubectl_popen", side_effect=_popen),
        patch.object(cap_mod, "_POLL_INTERVAL_S", 0.1),
    ):
        cap = DriverLogCapturer("ns1", tmp_run, context=None)
        cap.watch(["lakebench-silver-stream"])
        time.sleep(0.3)
        cap.close()

    assert popen_calls == []  # no capture started while Pending


def test_close_is_idempotent(tmp_run):
    cap = DriverLogCapturer("ns1", tmp_run, context=None)
    with patch.object(cap_mod, "pinned_kubectl", MagicMock()):
        cap.watch(["lakebench-silver-stream"])
        cap.close()
        cap.close()  # must not raise


def test_kubectl_error_does_not_fail_pipeline(tmp_run):
    """A missing kubectl / RBAC error only means less evidence, not run failure."""

    def _raise(*a, **kw):
        raise FileNotFoundError("kubectl")

    with patch.object(cap_mod, "pinned_kubectl", side_effect=_raise):
        cap = DriverLogCapturer("ns1", tmp_run, context=None)
        cap.watch(["lakebench-silver-stream"])
        time.sleep(0.3)
        cap.close()  # must not raise


def test_popen_failure_closes_filehandle(tmp_run):
    """If kubectl logs -f cannot be started, the opened log file is closed."""

    def _popen(ctx, args, **kwargs):
        raise OSError("popen boom")

    with (
        patch.object(
            cap_mod,
            "pinned_kubectl",
            MagicMock(return_value=_kubectl_result("silver-stream-driver|uid-1|Running\n")),
        ),
        patch.object(cap_mod, "pinned_kubectl_popen", side_effect=_popen),
        patch.object(cap_mod, "_POLL_INTERVAL_S", 0.1),
    ):
        cap = DriverLogCapturer("ns1", tmp_run, context=None)
        cap.watch(["lakebench-silver-stream"])
        time.sleep(0.3)
        cap.close()

    # File created (we open before popen), but no running proc attached.
    drivers = tmp_run / "drivers"
    assert (drivers / "lakebench-silver-stream.driver.log").exists()
