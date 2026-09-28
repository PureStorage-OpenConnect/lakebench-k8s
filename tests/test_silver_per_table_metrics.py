"""A2 (LB-044 gate metrics): per-silver-table row counts flow from silver
mains through the collector into JobMetrics.

Without this wire-up A1's diagnostic per-table counts are silently
dropped by _apply_metric's allowlist and never appear in metrics.json --
the same silent-no-op class the LB-044 gate exists to prevent.
"""

from __future__ import annotations

from lakebench.metrics.collector import JobMetrics, MetricsCollector


def _driver_log(**metrics: object) -> str:
    """Fake driver log with a JOB METRICS block carrying `metrics`."""
    lines = [
        "some earlier log line",
        "=== JOB METRICS: silver-build ===",
        "input_size_gb: 1.234",
        "input_rows: 5000",
        "output_rows: 5000",
        "elapsed_seconds: 12.5",
    ]
    for key, value in metrics.items():
        lines.append(f"{key}: {value}")
    lines.append("=" * 60)
    lines.append("later unrelated line")
    return "\n".join(lines)


def test_collector_accepts_silver_per_table_metrics():
    """AML silver-build emits six silver_*_rows keys; each lands on JobMetrics."""
    logs = _driver_log(
        silver_transactions_rows=5000,
        silver_entities_rows=1200,
        silver_accounts_rows=1300,
        silver_statements_rows=6100,
        silver_edges_rows=980,
        silver_profiles_rows=1150,
    )
    collector = MetricsCollector()
    metrics = collector.parse_driver_logs(logs, "silver-build")
    assert metrics.silver_tables == {
        "silver_transactions_rows": 5000,
        "silver_entities_rows": 1200,
        "silver_accounts_rows": 1300,
        "silver_statements_rows": 6100,
        "silver_edges_rows": 980,
        "silver_profiles_rows": 1150,
    }


def test_collector_captures_unknown_keys_in_extra_metrics():
    """Unknown metric keys stash on extra_metrics rather than silently drop.

    A5 emits `bronze_rows` in STREAMING mode, G2 emits
    `input_size_gb_source=filesystem|operator_override`, G5 emits
    `silver_stream_scale_admission`. Each must survive to metrics.json.
    """
    logs = _driver_log(
        bronze_rows=1_000_000,
        input_size_gb_source="operator_override",
        silver_stream_scale_admission="ok",
    )
    collector = MetricsCollector()
    metrics = collector.parse_driver_logs(logs, "silver-build")
    # bronze_rows starts with `silver_` prefix guard: it does NOT, so it
    # lands in extra_metrics with the raw string value.
    assert metrics.extra_metrics["bronze_rows"] == "1000000"
    assert metrics.extra_metrics["input_size_gb_source"] == "operator_override"
    assert metrics.extra_metrics["silver_stream_scale_admission"] == "ok"


def test_collector_ignores_garbled_silver_row_count():
    """A garbled silver_*_rows value is logged and skipped, not stored as 0.

    Coercing a bad value to zero would look like a legitimate empty table
    and mask the emit bug.
    """
    logs = _driver_log(
        silver_transactions_rows="not-a-number",
    )
    collector = MetricsCollector()
    metrics = collector.parse_driver_logs(logs, "silver-build")
    assert "silver_transactions_rows" not in metrics.silver_tables


def test_collector_silver_tables_empty_by_default():
    """A silver-build with no per-table emit has an empty dict, not None.

    Downstream readers can `metrics.silver_tables.get("silver_transactions_rows", 0)`
    safely without a None check.
    """
    logs = _driver_log()  # no extras
    collector = MetricsCollector()
    metrics = collector.parse_driver_logs(logs, "silver-build")
    assert metrics.silver_tables == {}
    assert isinstance(metrics.silver_tables, dict)


def test_jobmetrics_to_dict_includes_new_fields():
    """to_dict must carry silver_tables and extra_metrics to metrics.json."""
    m = JobMetrics(job_name="lb-silver-build", job_type="silver-build")
    m.silver_tables["silver_transactions_rows"] = 42
    m.extra_metrics["bronze_rows"] = "100"
    d = m.to_dict()
    assert d["silver_tables"] == {"silver_transactions_rows": 42}
    assert d["extra_metrics"] == {"bronze_rows": "100"}
