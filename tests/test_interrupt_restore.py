"""A run whose save raises leaves the SIGINT and SIGTERM handlers as they were.

``RunInterrupt.restore()`` runs at the end of the run's finally, after the
save, the report and the journal. When one of those raised, the run's
handler stayed installed in the process (LB-250): a later cluster-lock
acquire, whose guard installs only over ``SIG_DFL``, then never armed. The
``run`` command's ``interrupt_scope`` restores a leaked handler on its way
out; these tests switch it off, so they see what the run functions
themselves leave behind (as a caller of ``_run_sustained`` or ``_run_once``
that is not the CLI would).
"""

from __future__ import annotations

import contextlib
import signal

import pytest

from lakebench.cli import _interrupt
from lakebench.cli._interrupt import RunInterrupt, restores_handlers
from tests.harness.run_harness import SCENARIOS, invoke_scenario


def _sentinel(signum, frame):
    raise AssertionError("a signal reached the test")


@pytest.fixture
def sentinels():
    """Known handlers to compare against, put back afterwards."""
    saved = {s: signal.getsignal(s) for s in _interrupt.INTERRUPT_SIGNALS}
    for s in _interrupt.INTERRUPT_SIGNALS:
        signal.signal(s, _sentinel)
    try:
        yield
    finally:
        for s, h in saved.items():
            signal.signal(s, h)


@pytest.mark.parametrize("scenario", ["batch_c360", "continuous_c360"])
def test_a_run_whose_save_raises_restores_the_handlers(tmp_path, monkeypatch, sentinels, scenario):
    import lakebench.metrics

    # Without the command's own safety net.
    monkeypatch.setattr(_interrupt, "interrupt_scope", contextlib.nullcontext)

    def boom(self, *a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(lakebench.metrics.MetricsStorage, "save_run", boom)
    result, rec = invoke_scenario(SCENARIOS[scenario], tmp_path, monkeypatch)
    # The CLI reports the error and exits 1; the save never happened.
    assert result.exit_code == 1 and "disk full" in result.output, result.output
    for s in _interrupt.INTERRUPT_SIGNALS:
        assert signal.getsignal(s) is _sentinel, signal.Signals(s).name


def test_restores_handlers_puts_back_what_a_raising_call_left(sentinels):
    @restores_handlers
    def run():
        RunInterrupt("ns", "r1").install()
        raise RuntimeError("the save failed")

    with pytest.raises(RuntimeError):
        run()
    for s in _interrupt.INTERRUPT_SIGNALS:
        assert signal.getsignal(s) is _sentinel
