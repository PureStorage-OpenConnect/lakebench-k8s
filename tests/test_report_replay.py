"""The Workload replay tab: every value in its data block is a recorded field.

``build_replay`` reads the run record only. These tests recompute each figure
from the record by hand, check that a run with no replay profile gets no tab,
and read the data block back out of the rendered goldens.
"""

from __future__ import annotations

import json
import re
from datetime import datetime

import pytest

from lakebench.reports.replay import build_replay, render_replay
from tests.fixtures.report_goldens import GOLDEN_RUNS, golden_path
from tests.fixtures.stored_records import load_record

_DATA = re.compile(r'<script type="application/json" id="lb-replay-data">(.*?)</script>', re.S)


def _ts(stamp: str) -> float:
    return datetime.fromisoformat(stamp).timestamp()


def _record(mode: str = "batch", schema: str = "customer360") -> dict:
    return {
        "run_id": "r1",
        "start_time": "2026-01-01T00:00:00+00:00",
        "end_time": "2026-01-01T00:10:00+00:00",
        "config_snapshot": {"workload_schema": schema},
        "pipeline_benchmark": {
            "pipeline_mode": mode,
            "stages": [
                {"stage_name": "datagen", "output_rows": 1000},
                {
                    "stage_name": "bronze",
                    "start_time": "2026-01-01T00:00:20+00:00",
                    "end_time": "2026-01-01T00:01:20+00:00",
                    "output_rows": 1000,
                },
                {
                    "stage_name": "silver",
                    "start_time": "2026-01-01T00:01:30+00:00",
                    "end_time": "2026-01-01T00:02:30+00:00",
                    "output_rows": 900,
                },
                {
                    "stage_name": "gold",
                    "start_time": "2026-01-01T00:02:40+00:00",
                    "end_time": "2026-01-01T00:03:40+00:00",
                    "output_rows": 12,
                },
            ],
        },
        "jobs": [
            {
                "job_name": "j-bronze",
                "job_type": "bronze-verify",
                "start_time": "2026-01-01T00:00:20+00:00",
                "end_time": "2026-01-01T00:01:20+00:00",
                "elapsed_seconds": 60.0,
                "success": True,
            }
        ],
        "benchmark": {
            "queries": [{"name": "Q1", "success": True}, {"name": "Q2", "success": False}]
        },
        "io": {
            "intervals": [
                {
                    "source": "trino",
                    "label": "benchmark",
                    "start": "2026-01-01T00:05:00+00:00",
                    "end": "2026-01-01T00:05:10+00:00",
                    "read_bytes": 100_000_000,
                    "write_bytes": 0,
                },
                {"source": "datagen", "label": "datagen", "start": None, "end": None},
            ],
            "not_measured": [
                {"source": "spark", "stage": "bronze", "direction": "read"},
                {"source": "spark", "stage": "gold", "direction": "write"},
            ],
        },
    }


def test_stage_windows_and_counts_are_the_records() -> None:
    d = build_replay(_record())
    assert d is not None
    by = {s["key"]: s for s in d["stages"]}
    assert by["bronze"]["start"] == 20 and by["bronze"]["end"] == 80
    assert by["silver"]["value"] == 900
    assert by["datagen"]["start"] is None and by["datagen"]["value"] == 1000
    assert by["gold"]["path"] == "pipeline_benchmark.stages[gold].output_rows"
    assert d["duration"] == 600


def test_query_stage_is_the_benchmark_window_and_successes() -> None:
    d = build_replay(_record())
    q = {s["key"]: s for s in d["stages"]}["query"]
    assert q["value"] == 1
    assert (q["start"], q["end"]) == (300, 310)
    assert q["ends"] == [310]


def test_storage_lane_places_only_windowed_intervals() -> None:
    d = build_replay(_record())
    st = d["storage"]
    assert sum(st["read"]) * st["step"] * 1e6 == pytest.approx(100_000_000, rel=1e-2)
    assert sum(st["write"]) == 0
    assert d["storage_unplaced"] == ["datagen"]
    assert d["storage_gaps"] == ["spark bronze", "spark gold"]


def test_no_io_means_no_storage_lane() -> None:
    rec = _record()
    del rec["io"]
    d = build_replay(rec)
    assert d["storage"] is None
    assert "Not measured: Storage" in render_replay(d)


@pytest.mark.parametrize(
    ("mode", "schema"),
    [("continuous", "customer360"), ("sustained", "financial"), ("batch", "unknown")],
)
def test_no_profile_no_replay(mode: str, schema: str) -> None:
    assert build_replay(_record(mode, schema)) is None


def test_no_timeline_no_replay() -> None:
    rec = _record()
    del rec["end_time"]
    assert build_replay(rec) is None


def test_data_block_cannot_close_its_script() -> None:
    d = build_replay(_record())
    d["jobs"][0]["name"] = "</script><b>x"
    html = render_replay(d)
    assert "</script><b>" not in html
    body = _DATA.search(html).group(1)
    assert json.loads(body)["jobs"][0]["name"] == "</script><b>x"


@pytest.mark.parametrize("run_id", GOLDEN_RUNS)
def test_golden_replay_block_matches_its_record(run_id: str) -> None:
    html = golden_path(run_id).read_text()
    m = _DATA.search(html)
    record = load_record(run_id)
    if m is None:
        assert build_replay(record) is None
        return
    data = json.loads(m.group(1))
    t0 = _ts(record["start_time"])
    stages = {s["stage_name"]: s for s in record["pipeline_benchmark"]["stages"]}
    for s in data["stages"]:
        if s["key"] == "query":
            continue
        rec = stages[s["key"]]
        assert s["value"] == rec["output_rows"]
        if rec.get("start_time"):
            assert s["start"] == pytest.approx(_ts(rec["start_time"]) - t0)
            assert s["end"] == pytest.approx(_ts(rec["end_time"]) - t0)
        else:
            assert s["start"] is None
    assert [j["name"] for j in data["jobs"]] == [j["job_name"] for j in record["jobs"]]
    assert data["duration"] == pytest.approx(_ts(record["end_time"]) - t0)


def test_partial_spark_storage_is_named_on_the_replay() -> None:
    rec = _record()
    rec["io"]["problems"] = [
        "spark silver: write counters cover 70% of the 9.76 GiB that landed",
        "trino: 1 query sample(s) had no I/O report",
    ]
    d = build_replay(rec)
    assert d["storage_problems"] == [
        "spark silver: write counters cover 70% of the 9.76 GiB that landed"
    ]
    assert "Partial: spark silver: write counters cover 70%" in render_replay(d)
