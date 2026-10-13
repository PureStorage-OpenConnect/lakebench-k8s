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
    # Only a read with no counter is a gap; a write entry and datagen are not.
    assert d["storage_read_gap"] == ["bronze"]
    assert "Storage read bytes omit bronze" in render_replay(d)


def test_no_io_means_no_storage_lane() -> None:
    rec = _record()
    del rec["io"]
    d = build_replay(rec)
    assert d["storage"] is None
    assert "Not in the record: Storage" in render_replay(d)


@pytest.mark.parametrize(
    ("mode", "schema"),
    [("continuous", "customer360"), ("sustained", "unknown"), ("batch", "unknown")],
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
    if data["kind"] == "sustained":
        assert data["stages"][1]["start"] is not None
        return
    t0 = _ts(record["start_time"])
    stages = {s["stage_name"]: s for s in record["pipeline_benchmark"]["stages"]}
    for s in data["stages"]:
        if s["key"] == "query":
            continue
        rec = stages[s["key"]]
        assert s["value"] == rec[s["path"].rsplit(".", 1)[1]]
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


def _with_series(rec: dict) -> dict:
    steps = 41  # 600 s at 15 s
    flat = lambda v: [[k * 15.0, v] for k in range(steps)]  # noqa: E731
    rec["platform_metrics"] = {
        "series": {
            "step_seconds": 15,
            "start": rec["start_time"],
            "cpu_cores": {"silver": flat(2.0), "engine": flat(1.0)},
            "memory_bytes": {"silver": flat(2 * 1024**3)},
            "network_rx_bytes_per_s": {"silver": flat(4_000_000)},
            "network_tx_bytes_per_s": {"silver": flat(1_000_000)},
            "pods": {"silver": flat(3), "engine": flat(2)},
        }
    }
    return rec


def test_platform_lanes_are_totals_over_the_pod_groups() -> None:
    d = build_replay(_with_series(_record()))
    lanes = d["lanes"]
    assert lanes["compute"]["cpu"][0] == 3.0 and lanes["compute"]["mem"][0] == 2.0
    assert lanes["network"]["rx"][0] == 4.0 and lanes["network"]["tx"][0] == 1.0
    assert lanes["k8s"]["pods"][0] == 5
    assert lanes["compute"]["shift"] == 0.0 and lanes["compute"]["step"] == 15.0
    assert d["not_measured"] == []
    html = render_replay(d)
    for lane in ("compute", "network", "k8s"):
        assert f'data-lane="{lane}"' in html
    assert "not in the run record" not in html


def test_without_a_series_the_platform_lanes_say_not_measured() -> None:
    d = build_replay(_record())
    assert d["lanes"] == {}
    assert d["not_measured"] == ["Compute", "Network", "Kubernetes"]
    assert "Not in the record: Compute, Network, Kubernetes" in render_replay(d)


def test_a_series_starting_after_the_run_is_shifted_onto_its_clock() -> None:
    rec = _with_series(_record())
    rec["platform_metrics"]["series"]["start"] = "2026-01-01T00:00:30+00:00"
    assert build_replay(rec)["lanes"]["compute"]["shift"] == 30.0


def _sustained_record() -> dict:
    rec = _record("sustained", "financial")
    rec["continuous"] = {
        "window": {
            "start": "2026-01-01T00:01:00Z",
            "end": "2026-01-01T00:09:00Z",
            "seconds": 480.0,
        },
        "streams": {
            "bronze-ingest": {"running_at": "2026-01-01T00:00:50+00:00"},
            "silver-stream": {"running_at": "2026-01-01T00:00:55+00:00"},
            "gold-refresh": {"running_at": "2026-01-01T00:01:00+00:00"},
        },
        "ticks": [
            {"cycle": 1, "completed_at": "2026-01-01T00:03:00Z"},
            {"cycle": 2, "completed_at": "2026-01-01T00:06:00Z"},
        ],
        "time_travel": {
            "ticks": [
                {"cycle": 1, "table": "silver.transactions", "total_records": 100},
                {"cycle": 2, "table": "silver.transactions", "total_records": 250},
                {"cycle": 2, "table": "silver.accounts", "total_records": 9},
            ]
        },
        "balance": {
            "handoffs": {
                "datagen->bronze": {"samples": [[30.0, 4.5], [60.0, 9.0]], "keeps_up": True},
                "silver->gold": {"samples": [[30.0, 1.0]], "keeps_up": False},
            }
        },
    }
    rec["pipeline_benchmark"]["scores"] = {"released_rows": 300, "time_to_detect_alerts": 7}
    return rec


def test_sustained_replay_reads_the_window_ticks_and_lags() -> None:
    data = build_replay(_sustained_record())
    assert data is not None and data["kind"] == "sustained"
    gold = data["stages"][3]
    # Payments screened per cycle: the pinned transactions snapshot, at the
    # cycle's recorded completion, not the other tables.
    assert gold["steps"] == [[180.0, 100.0], [360.0, 250.0]] and gold["value"] == 250.0
    assert gold["start"] == 60.0 and gold["end"] == 540.0
    assert data["stages"][0]["value"] == 300 and data["stages"][4]["value"] == 7
    assert [h["label"] for h in data["handoffs"]] == ["Generate → Ingest", "Prepare → Monitor"]
    assert data["lag"]["shift"] == 60.0 and len(data["lag"]["series"]) == 2
    assert 'data-lane="lag"' in render_replay(data)


def test_sustained_pace_is_payments_screened_over_the_window() -> None:
    from lakebench.reports.tiles import build_tiles

    tile = next(
        t
        for t in build_tiles(_sustained_record(), mode="sustained", gates=None)
        if t.label == "Pace"
    )
    assert "250 payments screened in 480 s" in tile.sub and "payments/s" in tile.value
    rec = _sustained_record()
    rec["continuous"]["time_travel"] = {}
    nr = next(t for t in build_tiles(rec, mode="sustained", gates=None) if t.label == "Pace")
    assert nr.evidence == "nr"


def test_sustained_customer360_counts_cleaned_interactions_and_queries() -> None:
    rec = _record("sustained", "customer360")
    rec["pipeline_benchmark"]["stages"] = [
        {"stage_name": "bronze", "output_rows": 500},
        {"stage_name": "silver", "input_rows": 500, "output_rows": 480},
    ]
    rec["pipeline_benchmark"]["scores"] = {"released_rows": 510}
    rec["continuous"] = {
        "window": {"start": "2026-01-01T00:01:00Z", "end": "2026-01-01T00:09:00Z", "seconds": 480},
        "streams": {"silver-stream": {"running_at": "2026-01-01T00:00:55+00:00"}},
        "freshness": {"cycles": 12},
    }
    rec["benchmark_rounds"] = [
        {"ended_at": "2026-01-01T00:04:00Z", "queries": [{"success": True}, {"success": False}]},
        {"ended_at": "2026-01-01T00:07:00Z", "queries": [{"success": True}, {"success": True}]},
    ]
    data = build_replay(rec)
    assert data is not None and data["title"] == "C360 / Continuous"
    by = {s["key"]: s for s in data["stages"]}
    assert by["silver"]["value"] == 480 and by["silver"]["path"].endswith("silver].output_rows")
    assert by["gold"]["value"] == 12 and by["gold"]["path"] == "continuous.freshness.cycles"
    assert by["query"]["steps"] == [[240.0, 1.0], [420.0, 3.0]] and by["query"]["value"] == 3.0
    from lakebench.reports.tiles import build_tiles

    pace = next(t for t in build_tiles(rec, mode="sustained", gates=None) if t.label == "Pace")
    assert "interactions/s" in pace.value and "480 interactions cleaned in 480 s" in pace.sub
