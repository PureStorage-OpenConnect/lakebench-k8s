"""The scorecard tile grid: each tile cites a recorded field or says NR.

``build_tiles`` reads the run record and the loaded run's stored scores. These
tests check each tile against the record by hand, that a missing field is NR
and never zero, and the rendered goldens.
"""

from __future__ import annotations

import re

import pytest

from lakebench.reports.tiles import (
    EVIDENCE_DERIVED,
    EVIDENCE_NR,
    EVIDENCE_RECORDED,
    build_tiles,
    render_tiles,
)
from tests.fixtures.report_goldens import GOLDEN_RUNS, golden_path, page_text
from tests.fixtures.stored_records import load_record

_GIB = 1024**3


def _record() -> dict:
    return {
        "pipeline_benchmark": {
            "stages": [
                {"stage_name": "bronze", "output_rows": 2478560},
                {"stage_name": "silver", "output_rows": 2428561, "input_size_gb": 9.5},
                {"stage_name": "gold", "input_rows": 2428561, "input_size_gb": 9.0},
            ],
            "scores": {
                "time_to_value_seconds": 152.13,
                "pipeline_saturated": False,
                "pipeline_throughput_gb_per_second": 0.1946,
                "composite_qph": 936.2,
                "benchmark_samples_per_query": 3,
                "total_core_hours": 0.86,
                "compute_efficiency_gb_per_core_hour": 34.5642,
                "sustained_throughput_rps": 1032.7,
                "data_freshness_seconds": 101.0,
                "in_stream_composite_qph": 260.0,
            },
        },
        "experiment": {"support": {"state": "unverified"}},
        "io": {
            "totals": {"trino": {"read_bytes": 3 * _GIB, "write_bytes": 0}},
            "intervals": [],
            "not_measured": [{"source": "spark", "stage": "bronze", "direction": "read"}],
        },
    }


def _by_label(tiles):
    return {t.label: t for t in tiles}


def test_batch_tiles_cite_the_records_fields() -> None:
    t = _by_label(build_tiles(_record(), mode="batch", gates={"a": "PASS", "b": "PASS"}))
    assert list(t) == [
        "Pace",
        "Time to value",
        "Query rate",
        "Compute",
        "Efficiency",
        "Storage",
        "Gates",
        "Support",
    ]
    assert "152 s" in t["Time to value"].value
    assert "15,964 interactions/s" in t["Pace"].value
    assert t["Pace"].evidence == EVIDENCE_DERIVED
    assert "2,428,561 interactions cleaned in 152 s" in t["Pace"].sub
    assert "MiB/s" in t["Pace"].alt_value  # 9.5 GiB over 152 s is 64 MiB/s
    assert t["Time to value"].source == "pipeline_benchmark.scores.time_to_value_seconds"
    assert "936.2" in t["Query rate"].value and "3 samples per query" in t["Query rate"].sub
    assert t["Time to value"].evidence == EVIDENCE_RECORDED
    assert t["Storage"].evidence == EVIDENCE_DERIVED
    assert "3.00 GiB" in t["Storage"].value
    assert "spark not measured" in t["Storage"].sub


def test_continuous_tiles_swap_the_lead_and_the_query_rate_field() -> None:
    t = _by_label(build_tiles(_record(), mode="continuous", gates={"a": "PASS"}))
    assert list(t)[:3] == ["Pace", "Freshness", "Query rate"]
    assert "1,033 interactions/s" in t["Pace"].value and "kept pace" in t["Pace"].sub
    assert "260.0" in t["Query rate"].value
    assert t["Query rate"].source.endswith("in_stream_composite_qph")


def test_missing_fields_are_nr_never_zero() -> None:
    rec = {"pipeline_benchmark": {"scores": {}}}
    tiles = build_tiles(rec, mode="batch", gates=None)
    assert all(x.evidence == EVIDENCE_NR and x.value == "NR" for x in tiles)
    assert all(x.sub.startswith("not measured") for x in tiles)


def test_zero_is_measured_zero() -> None:
    rec = _record()
    rec["pipeline_benchmark"]["scores"]["total_core_hours"] = 0.0
    t = _by_label(build_tiles(rec, mode="batch", gates={"a": "PASS"}))
    assert t["Compute"].evidence == EVIDENCE_RECORDED
    assert "0.00" in t["Compute"].value


def test_gates_count_and_name_what_did_not_pass() -> None:
    t = _by_label(
        build_tiles(_record(), mode="batch", gates={"a": "PASS", "b": "FAIL", "c": "PASS"})
    )
    assert t["Gates"].value == "2 / 3"
    assert "b" in t["Gates"].sub


def test_stored_scores_win_over_the_recomputed_ones() -> None:
    class Run:
        stored_scores = {"composite_qph": 260.0, "time_to_value_seconds": 1.0}

    rec = _record()
    rec["pipeline_benchmark"]["scores"]["composite_qph"] = 260.1
    t = _by_label(build_tiles(rec, mode="batch", gates=None, metrics=Run()))
    assert "260.0" in t["Query rate"].value


def test_rendered_html_escapes_and_names_the_field() -> None:
    rec = _record()
    rec["experiment"]["support"]["state"] = "<b>x</b>"
    html = render_tiles(build_tiles(rec, mode="batch", gates=None))
    assert "<b>x</b>" not in html
    assert 'title="pipeline_benchmark.scores.time_to_value_seconds"' in html


@pytest.mark.parametrize("run_id", GOLDEN_RUNS)
def test_golden_tiles_match_the_stored_record(run_id: str) -> None:
    text = re.sub(r"\|+", "|", re.sub(r"<[^>]+>", "|", page_text(golden_path(run_id).read_text())))
    sc = load_record(run_id)["pipeline_benchmark"]["scores"]
    grid = text[text.index("lb-grid") :]
    if "failed-headline" in golden_path(run_id).read_text():
        # A run that did not pass shows no score figure, only the reason.
        assert grid.count("not shown: the run did not pass") == 5
        assert f"{sc['total_core_hours']:.2f}" not in grid
        return
    assert f"{sc['total_core_hours']:.2f}" in grid
    assert f"{sc['compute_efficiency_gb_per_core_hour']:.2f}" in grid
    key = "composite_qph" if "time_to_value_seconds" in sc else "in_stream_composite_qph"
    if sc.get(key) is None:
        # Not recorded: the tile says so rather than showing a number.
        assert "Query rate|NR|" in grid
    else:
        assert f"{sc[key]:,.1f}" in grid


def test_aml_pace_counts_transactions_screened_by_gold() -> None:
    rec = _record()
    rec["config_snapshot"] = {"workload_schema": "financial"}
    t = _by_label(build_tiles(rec, mode="batch", gates={"a": "PASS"}))["Pace"]
    assert "15,964 payments/s" in t.value
    assert "payments screened" in t.sub
    assert t.source.startswith("pipeline_benchmark.stages[2].input_rows")
    assert "gold input bytes" in t.alt_sub


def test_pace_without_its_stage_is_nr_not_zero() -> None:
    rec = _record()
    rec["pipeline_benchmark"]["stages"] = []
    t = _by_label(build_tiles(rec, mode="batch", gates=None))["Pace"]
    assert t.evidence == EVIDENCE_NR and t.value == "NR"


def test_the_bytes_view_steps_its_unit_with_the_size() -> None:
    from lakebench.reports import derived as dv

    assert "TiB/s" in dv._fmt(2048.0, dv.AUTO_GIB, "/s")
    assert "GiB/s" in dv._fmt(2.0, dv.AUTO_GIB, "/s")
    assert "MiB/s" in dv._fmt(0.5, dv.AUTO_GIB, "/s")
