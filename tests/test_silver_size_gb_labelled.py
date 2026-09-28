"""G2: LB_SILVER_SIZE_GB override is labelled in emitted metrics.

The metrics collector's `_apply_metric` allowlist stashes any
unrecognised key on `JobMetrics.extra_metrics`. The silver mains write
`input_size_gb_source=filesystem` when they measured the bronze size
via the filesystem and `input_size_gb_source=operator_override` when
the LB_SILVER_SIZE_GB env var is set. Without the label, a downstream
reader cannot tell an operator-asserted size from a measured one
(invariant 5).
"""

from __future__ import annotations

from lakebench.metrics.collector import MetricsCollector


def _driver_log(source_label: str) -> str:
    """Fake driver log carrying a `input_size_gb_source` line."""
    return "\n".join(
        [
            "=== JOB METRICS: silver-build ===",
            "input_size_gb: 42.000",
            f"input_size_gb_source: {source_label}",
            "input_rows: 10000000",
            "output_rows: 9000000",
            "elapsed_seconds: 200.0",
            "=" * 60,
        ]
    )


def test_collector_records_filesystem_label():
    """Fresh runs without LB_SILVER_SIZE_GB emit `filesystem`."""
    collector = MetricsCollector()
    metrics = collector.parse_driver_logs(_driver_log("filesystem"), "silver-build")
    assert metrics.extra_metrics.get("input_size_gb_source") == "filesystem"


def test_collector_records_operator_override_label():
    """A run with the size override emits `operator_override`.

    The label must survive to metrics.json so downstream reports can
    refuse to publish the value as measured infrastructure performance.
    """
    collector = MetricsCollector()
    metrics = collector.parse_driver_logs(_driver_log("operator_override"), "silver-build")
    assert metrics.extra_metrics.get("input_size_gb_source") == "operator_override"


def test_silver_build_emits_operator_override():
    """The silver_build.py main path picks the operator_override label.

    Import the module fresh and drive its size-override branch: the
    `input_size_gb_source` local must equal `operator_override` when the
    env var is set to a positive float, and `filesystem` otherwise. The
    module executes top-level code that needs a SparkSession, so we
    exercise the branch by reading the source lines that set the label
    -- lightweight, but enough to fail if the label were removed.
    """
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/silver_build.py"
    text = src.read_text()
    # G2 emits both labels; if either disappears, the fix reverts.
    assert 'input_size_gb_source = "operator_override"' in text
    assert 'input_size_gb_source = "filesystem"' in text
    # And the JOB METRICS block prints the label.
    assert "input_size_gb_source: {input_size_gb_source}" in text


def test_silver_build_delta_emits_operator_override():
    """Same label discipline in the Delta variant."""
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/silver_build_delta.py"
    text = src.read_text()
    assert 'input_size_gb_source = "operator_override"' in text
    assert 'input_size_gb_source = "filesystem"' in text
    assert "input_size_gb_source: {input_size_gb_source}" in text
