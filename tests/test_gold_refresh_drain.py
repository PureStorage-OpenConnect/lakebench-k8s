"""AML-6 drain inside the continuous gold-refresh driver, with fakes.

``gold_refresh_financial`` imports pyspark and its sibling scripts at module
level; the unit tier has no pyspark, so those are stubbed (any attribute is
a MagicMock) and ``common`` is the real module. The loop, the drain marker
check and the ``_pin_silver`` fallbacks are then exercised directly.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"
_PYSPARK = (
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.window",
    "pyspark.storagelevel",
)


class _Permissive(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        value = mock.MagicMock(name=f"{self.__name__}.{name}")
        setattr(self, name, value)
        return value


@pytest.fixture
def gr(load_script, monkeypatch):
    """gold_refresh_financial with run id run-1 and a checkpoint, pyspark
    stubbed (the unit tier has none) and the other scripts real."""
    monkeypatch.setenv("LB_RUN_ID", "run-1")
    monkeypatch.setenv("CHECKPOINT_LOCATION", "s3a://gold/cp/gold-refresh/")
    monkeypatch.setenv("LB_FINANCIAL_GOLD_REFRESH_S", "3")
    for name in _PYSPARK:
        monkeypatch.setitem(sys.modules, name, _Permissive(name))
    mod, common = load_script("gold_refresh_financial", extra=("common",))
    mod.common = common
    return mod


class _Loop:
    """Fakes for main(): ticks, the marker, the clock and the log."""

    def __init__(self, gr, monkeypatch, *, marker_at_start=False):
        self.gr = gr
        self.marker = marker_at_start
        self.ticks: list[int] = []
        self.started: list[int] = []
        self.lines: list[str] = []
        self.bootstrapped = False
        self.on_tick = None  # cycle -> None, runs mid-tick
        self.spark = mock.MagicMock(name="spark")
        builder = mock.MagicMock()
        builder.appName.return_value.getOrCreate.return_value = self.spark
        monkeypatch.setattr(gr, "SparkSession", mock.MagicMock(builder=builder))
        monkeypatch.setattr(gr, "_install_signal_handlers", lambda: None)
        monkeypatch.setattr(gr, "_bootstrap_gold_tables", self._bootstrap)
        monkeypatch.setattr(gr, "TickState", mock.MagicMock())
        monkeypatch.setattr(gr, "run_tick", self._tick)
        monkeypatch.setattr(gr, "stop_requested", lambda _s: self.marker)
        monkeypatch.setattr(gr, "log", self.lines.append)
        self.now = 1000.0
        monkeypatch.setattr(gr, "time", SimpleNamespace(time=lambda: self.now, sleep=self._sleep))
        self.sleeps = 0
        self.idle_sleeps = 0
        self.sleeps_before_drain = 0

    def _bootstrap(self, _spark):
        self.bootstrapped = True

    def _tick(self, _spark, _state, cycle):
        self.started.append(cycle)
        if self.on_tick:
            self.on_tick(cycle)
        self.ticks.append(cycle)
        return {}

    def _sleep(self, s):
        self.now += s
        self.sleeps += 1
        if not any(ln.startswith("Drain complete") for ln in self.lines):
            self.sleeps_before_drain += 1
            if self.sleeps > 1000:  # never drained: end the test's driver
                self.gr._SHUTDOWN = True
            return
        self.idle_sleeps += 1
        if self.idle_sleeps >= 130:  # the idle wait: end the test's driver
            self.gr._SHUTDOWN = True


def test_marker_mid_tick_completes_the_tick_and_starts_no_other(gr, monkeypatch):
    loop = _Loop(gr, monkeypatch)

    def mid(cycle):
        if cycle == 2:
            loop.marker = True

    loop.on_tick = mid
    gr.main()
    assert loop.started == [1, 2] and loop.ticks == [1, 2]
    drained = [ln for ln in loop.lines if ln.startswith("Drain complete")]
    # Logged before and after spark.stop, then once a minute while idle, so
    # it stays at the tail of the log.
    assert set(drained) == {"Drain complete: last completed cycle 2 run=run-1"}
    assert len(drained) == 4, drained
    loop.spark.stop.assert_called()
    # Idles after the drain until a signal, so the pod and its log stay.
    assert loop.idle_sleeps == 130


def test_marker_between_ticks_is_seen_in_the_sleep(gr, monkeypatch):
    loop = _Loop(gr, monkeypatch)
    calls = {"n": 0}

    def requested(_s):
        calls["n"] += 1
        return bool(loop.ticks)  # set once tick 1 has completed

    monkeypatch.setattr(gr, "stop_requested", requested)
    gr.main()
    assert loop.ticks == [1]
    assert "Drain complete: last completed cycle 1 run=run-1" in loop.lines
    # Seen within a second, not after the rest of the refresh interval.
    assert loop.sleeps_before_drain == 0


def test_sigterm_mid_tick_completes_the_tick_without_a_drain_line(gr, monkeypatch):
    loop = _Loop(gr, monkeypatch)

    def mid(cycle):
        gr._SHUTDOWN = True

    loop.on_tick = mid
    gr.main()
    assert loop.ticks == [1]
    assert not [ln for ln in loop.lines if ln.startswith("Drain complete")]
    loop.spark.stop.assert_called_once()


def test_marker_at_start_does_no_work(gr, monkeypatch):
    loop = _Loop(gr, monkeypatch, marker_at_start=True)
    gr.main()
    assert loop.ticks == [] and not loop.bootstrapped
    assert (
        "Drain complete: stop marker present at start; last completed cycle 0 run=run-1"
        in loop.lines
    )


def test_failed_tick_is_not_the_last_completed(gr, monkeypatch):
    loop = _Loop(gr, monkeypatch)

    def mid(cycle):
        if cycle == 2:
            loop.marker = True
            raise RuntimeError("rule read failed")

    loop.on_tick = mid
    gr.main()
    assert loop.started == [1, 2] and loop.ticks == [1]
    assert "Drain complete: last completed cycle 1 run=run-1" in loop.lines


# --- the marker read --------------------------------------------------------


def _jvm_spark(body=None, exists=True, error=None):
    spark = mock.MagicMock()
    path = spark._jvm.org.apache.hadoop.fs.Path.return_value
    fs = path.getFileSystem.return_value
    if error is not None:
        fs.exists.side_effect = error
    fs.exists.return_value = exists
    spark._jvm.org.apache.commons.io.IOUtils.toString.return_value = body
    return spark


def test_stop_requested_needs_this_runs_id(gr, monkeypatch):
    monkeypatch.setattr(gr, "log", lambda _m: None)
    assert gr.stop_requested(_jvm_spark("run-1\n")) is True
    assert gr.stop_requested(_jvm_spark("run-0")) is False
    assert gr.stop_requested(_jvm_spark(exists=False)) is False
    spark = _jvm_spark("run-1")
    gr.stop_requested(spark)
    spark._jvm.org.apache.hadoop.fs.Path.assert_called_with("s3a://gold/cp/gold-refresh/_lb_stop")


def test_stop_requested_read_error_is_absent_and_logged(gr, monkeypatch):
    lines: list[str] = []
    monkeypatch.setattr(gr, "log", lines.append)
    assert gr.stop_requested(_jvm_spark(error=RuntimeError("503 slow down"))) is False
    assert any("stop marker unreadable" in ln for ln in lines), lines


# --- _pin_silver: the logged versions token is the one used -----------------


class _Pin:
    def __init__(self, gr, monkeypatch, sid, vsid, filter_at_error=None):
        self.calls: list[str] = []
        snaps = {
            f"{gr.CATALOG}.{gr.SILVER_TXNS}": sid,
            f"{gr.CATALOG}.{gr.SILVER_BATCH_VERSIONS}": vsid,
        }
        monkeypatch.setattr(gr, "_current_snapshot", lambda _s, fq: snaps[fq])
        monkeypatch.setattr(gr, "read_at_snapshot", lambda _s, fq, s: f"pinned@{s}")
        monkeypatch.setattr(gr, "iceberg_table_stats", lambda _s, _fq: (5, None))
        monkeypatch.setattr(gr, "_newest_ingest_epoch_s", lambda _s, _fq: 1.0)
        monkeypatch.setattr(gr, "log", lambda _m: None)

        def at(_s, frame, _c, _t, v):
            self.calls.append(f"at:{frame}:{v}")
            if filter_at_error is not None:
                raise filter_at_error
            return mock.MagicMock(name="sealed_at")

        def current(_s, frame, _c, _t):
            self.calls.append(f"current:{frame}")
            return mock.MagicMock(name="sealed_current")

        monkeypatch.setattr(gr, "sealed_txns_filter_at", at)
        monkeypatch.setattr(gr, "sealed_txns_filter", current)


def test_pin_uses_the_versions_snapshot_and_names_it(gr, monkeypatch):
    p = _Pin(gr, monkeypatch, 11, 22)
    _txns, sid, used, _rows, _newest = gr._pin_silver(mock.MagicMock())
    assert (sid, used) == (11, "22")
    assert p.calls == ["at:pinned@11:22"]


@pytest.mark.parametrize(
    ("vsid", "error", "want"),
    [
        ("unknown", None, "unknown"),
        (None, None, "none"),
        (22, "sealed", "unknown"),
        (22, "type", "unknown"),
    ],
)
def test_pin_fallback_never_names_an_unused_snapshot(gr, monkeypatch, vsid, error, want):
    err = {
        None: None,
        "sealed": gr.common.SealedFilterError("snapshot expired"),
        "type": TypeError("bad id"),
    }[error]
    p = _Pin(gr, monkeypatch, 11, vsid, filter_at_error=err)
    _txns, sid, used, _rows, _newest = gr._pin_silver(mock.MagicMock())
    assert sid == 11 and used == want
    assert p.calls[-1] == "current:pinned@11"


@pytest.mark.parametrize(("sid", "want"), [(None, "none"), ("unknown", "unknown")])
def test_pin_without_a_txns_snapshot_reads_live(gr, monkeypatch, sid, want):
    p = _Pin(gr, monkeypatch, sid, 22)
    _txns, got, used, _rows, _newest = gr._pin_silver(mock.MagicMock())
    assert got == sid and used == want
    assert all(c.startswith("current:") for c in p.calls), p.calls


def test_tick_line_round_trips_through_the_parser(gr):
    from lakebench.metrics.tick_records import parse_tick_records

    line = gr.tick_pinned_line(3, 11, None, "unknown", "22", 1700000000.5)
    t = parse_tick_records(line, "run-1")["ticks"][0]
    assert t["cycle"] == 3 and t["pinned_txns"] == 11 and t["pinned_versions"] == 22
    assert t["pinned_entities"] == "none" and t["pinned_accounts"] == "unknown"
    assert t["pinned_at"] == 1700000000.5


def test_marker_name_matches_the_cli(gr):
    from lakebench.modules.pipeline_engines.spark.job import GOLD_REFRESH_STOP_MARKER

    assert gr.STOP_MARKER == GOLD_REFRESH_STOP_MARKER


def test_run_tick_logs_pinned_committed_completed_in_order():
    """Static: the pinned line precedes detection, the committed line
    follows it, and the completed line follows ``Tick complete``."""
    src = (SCRIPTS / "gold_refresh_financial.py").read_text()
    body = src[src.index("def run_tick(") : src.index("def _stop_marker_path(")]
    marks = [
        "tick_pinned_line(",
        "detection = run_detection_rules(",
        'f"Cycle {cycle}: committed alerts=',
        'log(f"Tick complete in',
        'log(f"Cycle {cycle}: completed run={RUN_ID}")',
    ]
    pos = [body.index(m) for m in marks]
    assert pos == sorted(pos), list(zip(marks, pos, strict=True))


def test_marker_reads_do_not_stretch_the_refresh_interval(gr, monkeypatch):
    """The sleep runs to a deadline: a slow marker read is part of the
    interval, not added to it."""
    loop = _Loop(gr, monkeypatch)
    starts: list[float] = []

    def slow_read(_s):
        loop.now += 0.5  # a marker read that takes half a second
        return len(starts) >= 3

    def tick(_spark, _state, cycle):
        starts.append(loop.now)
        loop.ticks.append(cycle)
        return {}

    monkeypatch.setattr(gr, "stop_requested", slow_read)
    monkeypatch.setattr(gr, "run_tick", tick)
    gr.main()
    gaps = [b - a for a, b in zip(starts, starts[1:], strict=False)]
    assert gaps and all(g <= 3.0 + 0.5 + 1e-9 for g in gaps), gaps
