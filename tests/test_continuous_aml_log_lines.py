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
