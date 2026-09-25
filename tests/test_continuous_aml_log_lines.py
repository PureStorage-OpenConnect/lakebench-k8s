"""LB-136: the AML continuous scripts log the lines the collector parses.

A healthy scale-10 continuous AML run reported zero rows ingested, no
freshness and pipeline_saturated=True, because bronze_ingest_financial,
silver_stream_financial and gold_refresh_financial logged in formats
parse_streaming_logs never matched.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from lakebench.metrics.collector import MetricsCollector

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _common():
    spec = importlib.util.spec_from_file_location("lb_common_stream_lines", SCRIPTS / "common.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _prefixed(lines):
    return "\n".join(f"[lb] 2026-09-24T09:00:00.000000 - {ln}" for ln in lines)


def test_bronze_batch_lines_are_counted():
    c = _common()
    lines = []
    lines += c.stream_batch_lines(0, 1_200_000, 12.5, "lakehouse.default.pacs008_raw")
    lines += c.stream_batch_lines(1, 0, 0.2, "lakehouse.default.pacs008_raw")  # idle trigger
    lines += c.stream_batch_lines(2, 800_000, 7.5, "lakehouse.default.pacs008_raw")
    m = MetricsCollector().parse_streaming_logs(_prefixed(lines), "bronze-ingest")
    assert m.total_rows_processed == 2_000_000
    assert m.total_batches == 2
    assert m.unique_rows_processed == 2_000_000
    assert m.micro_batch_duration_ms == pytest.approx(10_000)


def test_silver_and_gold_formats_in_the_scripts_parse():
    """The literal f-string shapes in the scripts, rendered, must parse."""
    silver = [
        "Batch 4: transforming 1,000 rows",
        "Batch 4: committed to silver.transactions in 3.0s",
        "Batch 5: empty, skipping",
    ]
    m = MetricsCollector().parse_streaming_logs(_prefixed(silver), "silver-stream")
    assert m.total_rows_processed == 1000
    assert m.total_batches == 2
    gold = [
        "Cycle 1: aggregating 26,666,639 Silver records",
        "Cycle 1: refreshed gold.alerts in 41.0s",
        "Cycle 1: data freshness 95s",
        "Cycle 2: aggregating 26,666,639 Silver records",
        "Cycle 2: refreshed gold.alerts in 39.0s",
        "Cycle 2: data freshness 120s",
    ]
    g = MetricsCollector().parse_streaming_logs(_prefixed(gold), "gold-refresh")
    assert g.freshness_seconds == pytest.approx(120)
    assert g.total_batches == 2


@pytest.mark.parametrize(
    ("script", "fragments"),
    [
        (
            "silver_stream_financial.py",
            ['"Batch {batch_id}: transforming', "committed to {SILVER_TXNS} in"],
        ),
        (
            "gold_refresh_financial.py",
            ["aggregating {silver_rows:,} Silver records", "data freshness {"],
        ),
        ("bronze_ingest_financial.py", ["stream_batch_lines("]),
    ],
)
def test_scripts_emit_the_parsed_shapes(script, fragments):
    src = (SCRIPTS / script).read_text()
    for frag in fragments:
        assert frag in src, f"{script} lost {frag!r}"


def test_gold_tick_timing_lines_are_kept_per_cycle():
    """gold_refresh_financial.tick_timing_line: one entry per tick, phases in
    the order the tick ran them (rule ids are phase names)."""
    gold = [
        "Cycle 1: aggregating 26,666,639 Silver records",
        "Cycle 1: tick timing silver_rows=26666639 probe=2.1s detect_setup=1.0s "
        "W4_risk_propagation=40.2s W2_structuring=12.0s W17_layering_chain=150.5s "
        "W3_round_tripping=120.0s detect_finish=3.1s baseline=15.0s count=0.4s "
        "ttd=9.9s tm=55.3s total=409.5s",
        "Cycle 2: aggregating 30,000,000 Silver records",
        "Cycle 2: tick timing silver_rows=30000000 probe=1.0s tm=0.0s total=300.0s",
    ]
    g = MetricsCollector().parse_streaming_logs(_prefixed(gold), "gold-refresh")
    assert [t["cycle"] for t in g.tick_timings] == [1, 2]
    first = g.tick_timings[0]
    assert first["silver_rows"] == 26_666_639
    assert list(first["phases"])[:4] == [
        "probe",
        "detect_setup",
        "W4_risk_propagation",
        "W2_structuring",
    ]
    assert first["phases"]["W17_layering_chain"] == pytest.approx(150.5)
    assert first["phases"]["total"] == pytest.approx(409.5)
    # The line is not mistaken for a refresh, freshness or TTD line.
    assert g.total_batches == 2
    assert g.freshness_seconds == 0.0
    assert g.ttd_alerts is None
    # Round trip through metrics.json.
    assert g.to_dict()["tick_timings"] == g.tick_timings


def test_tick_timings_only_for_gold():
    line = "Cycle 1: tick timing silver_rows=5 probe=1.0s total=1.0s"
    m = MetricsCollector().parse_streaming_logs(_prefixed([line]), "silver-stream")
    assert m.tick_timings == []


def test_gold_refresh_emits_the_tick_timing_shape():
    src = (SCRIPTS / "gold_refresh_financial.py").read_text()
    assert 'f"Cycle {cycle}: tick timing silver_rows={int(silver_rows or 0)} {parts}"' in src
    assert "log(tick_timing_line(cycle, silver_rows, phases))" in src


def test_tick_timings_survive_a_metrics_json_reload():
    from lakebench.metrics.storage import MetricsStorage

    tt = [{"cycle": 1, "silver_rows": 5, "phases": {"probe": 1.0, "total": 2.0}}]
    data = {
        "run_id": "r",
        "start_time": "2026-09-25T00:00:00",
        "streaming": [{"job_name": "lakebench-gold-refresh", "job_type": "gold-refresh"}],
    }
    data["streaming"][0]["tick_timings"] = tt
    by_rule = {"W4_risk_propagation": {"alerts": 3, "p50_seconds": 10.0}}
    data["streaming"][0].update(
        ttd_pass_end_p50_seconds=70.0, ttd_pass_end_p95_seconds=95.0, ttd_by_rule=by_rule
    )
    m = MetricsStorage.__new__(MetricsStorage)._dict_to_metrics(data)
    assert m.streaming[0].tick_timings == tt
    assert m.streaming[0].ttd_pass_end_p50_seconds == 70.0
    assert m.streaming[0].ttd_pass_end_p95_seconds == 95.0
    assert m.streaming[0].ttd_by_rule == by_rule


def test_ttd_pass_end_and_per_rule_lines_are_merged():
    """gold_refresh_financial.ttd_detail_lines: the pass-end histogram (the
    definition before per-rule commit times) and one per rule, merged over
    cycles; neither is counted as the main time-to-detect line."""
    gold = [
        "Cycle 1: time to detect alerts=3 late=0 unmatched=0 max=35.0s bin=10s bins=0:2,3:1",
        "Cycle 1: time to detect at pass end alerts=3 max=95.0s bin=10s bins=6:2,9:1",
        "Cycle 1: time to detect rule=W4_risk_propagation alerts=2 max=5.0s bin=10s bins=0:2",
        "Cycle 1: time to detect rule=W3_round_tripping alerts=1 max=35.0s bin=10s bins=3:1",
        "Cycle 2: time to detect alerts=1 late=0 unmatched=0 max=8.0s bin=10s bins=0:1",
        "Cycle 2: time to detect at pass end alerts=1 max=70.0s bin=10s bins=7:1",
        "Cycle 2: time to detect rule=W4_risk_propagation alerts=1 max=8.0s bin=10s bins=0:1",
    ]
    g = MetricsCollector().parse_streaming_logs(_prefixed(gold), "gold-refresh")
    assert g.ttd_alerts == 4
    assert g.ttd_pass_end_p50_seconds == pytest.approx(70.0)
    assert g.ttd_pass_end_p95_seconds == pytest.approx(95.0)
    assert g.ttd_by_rule["W4_risk_propagation"]["alerts"] == 3
    assert g.ttd_by_rule["W4_risk_propagation"]["max_seconds"] == pytest.approx(8.0)
    assert g.ttd_by_rule["W3_round_tripping"]["p50_seconds"] == pytest.approx(35.0)
    assert g.to_dict()["ttd_by_rule"] == g.ttd_by_rule


def test_gold_refresh_emits_the_ttd_detail_shapes():
    src = (SCRIPTS / "gold_refresh_financial.py").read_text()
    assert "f\"Cycle {cycle}: time to detect at pass end {_fmt(stats['pass_end'])}\"" in src
    assert 'f"Cycle {cycle}: time to detect rule={rid} {_fmt(h)}"' in src
