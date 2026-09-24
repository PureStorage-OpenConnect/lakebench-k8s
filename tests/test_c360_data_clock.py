"""c360 data clock wiring and replay log lines (E3 round 2, E4)."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

from lakebench.metrics.collector import MetricsCollector
from lakebench.spark.job import JobType, SparkJobManager
from tests.test_spark import _make_config, _mock_k8s

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"))


def _env(config, job_type):
    manifest = SparkJobManager(config, _mock_k8s())._build_manifest(job_type)
    return {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}


@pytest.mark.parametrize(
    "job_type", [JobType.SILVER_BUILD, JobType.SILVER_STREAM, JobType.BRONZE_VERIFY]
)
def test_data_clock_env_follows_datagen_end(job_type):
    cfg = _make_config(architecture={"workload": {"datagen": {"timestamp_end": "2025-07-01"}}})
    assert _env(cfg, job_type)["LB_DATA_CLOCK"] == "2025-07-01"


def test_data_clock_env_absent_without_configured_end():
    assert "LB_DATA_CLOCK" not in _env(_make_config(), JobType.SILVER_BUILD)


def test_configured_clock_is_the_day_before_the_exclusive_end(monkeypatch):
    from common import configured_data_clock

    assert configured_data_clock("2025-01-01") == date(2024, 12, 31)
    assert configured_data_clock("2025-07-01T00:00:00") == date(2025, 6, 30)
    assert configured_data_clock("") is None
    monkeypatch.setenv("LB_DATA_CLOCK", "2024-03-01")
    assert configured_data_clock() == date(2024, 2, 29)
    monkeypatch.delenv("LB_DATA_CLOCK")
    assert configured_data_clock() is None


def _lines(*msgs):
    return "\n".join(f"[lb] 2026-09-24T00:00:00 - {m}" for m in msgs)


def test_skipped_replay_lines_are_not_counted_as_rows():
    """A replay Delta or the snapshot tag skipped committed nothing; counting
    it would let a run that wrote no data pass the c360 continuous gate."""
    c = MetricsCollector()
    bronze = c.parse_streaming_logs(
        _lines(
            "Batch 4: skipped (txn already applied)",
            "Batch 4: skipped (already committed to lakehouse.default.bronze_raw)",
            "Batch 5: writing 1,000 rows to lakehouse.default.bronze_raw",
            "Batch 5: committed in 1.0s",
        ),
        "bronze-ingest",
    )
    assert bronze.total_rows_processed == 1000
    silver = c.parse_streaming_logs(
        _lines(
            "Batch 7: read 2,000 bronze rows",
            "Batch 7: skipped (txn already applied)",
        ),
        "silver-stream",
    )
    assert silver.total_rows_processed == 0


def test_await_stream_stops_from_the_loop_not_the_signal_handler():
    """The handler only sets a flag: query.stop() runs in the loop, so no
    py4j call happens inside a signal handler (reentrancy)."""
    import os
    import signal
    import threading

    from common import await_stream

    class FakeQuery:
        def __init__(self):
            self.active = True
            self.stop_frames = None

        @property
        def isActive(self):  # noqa: N802 -- mirrors StreamingQuery
            return self.active

        def stop(self):
            import inspect

            self.stop_frames = [f.function for f in inspect.stack()]
            self.active = False

        def exception(self):
            return None

    saved = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    q = FakeQuery()
    try:
        threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGTERM)).start()
        await_stream(object(), q)
    finally:
        for s, h in saved.items():
            signal.signal(s, h)
    assert q.active is False
    assert "await_stream" in q.stop_frames
    assert "_shutdown_handler" not in q.stop_frames


def test_replay_check_runs_once_per_query_run():
    """Only a run's first micro-batch pays the replay check; a restart (new
    run id) checks again; an unreadable run id checks every batch."""
    from common import replay_possible

    class Ctx:
        def __init__(self, run):
            self.run = run

        def getLocalProperty(self, key):  # noqa: N802 -- mirrors SparkContext
            return self.run if key == "spark.jobGroup.id" else None

    class Spark:
        def __init__(self, run):
            self.sparkContext = Ctx(run)

    first, later = Spark("run-a"), Spark("run-a")
    assert replay_possible(first) is True
    assert replay_possible(later) is False
    assert replay_possible(later) is False
    assert replay_possible(Spark("run-b")) is True
    assert replay_possible(Spark(None)) is True
    assert replay_possible(Spark(None)) is True
