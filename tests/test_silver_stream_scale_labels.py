"""G5: silver streams emit `silver_stream_scale_cap` and
`silver_stream_scale_admission` labels at startup.

Without these labels, downstream reports have no way to distinguish a
Lakebench-imposed scale ceiling from measured infrastructure headroom
(invariant 6). The three silver_stream mains log both keys before
`query.start()` and the collector's `parse_streaming_logs` stashes
them on `StreamingJobMetrics.extra_metrics`.
"""

from __future__ import annotations

from pathlib import Path

from lakebench.metrics.collector import MetricsCollector, StreamingJobMetrics

_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _driver_log(scale_cap: str = "measured_up_to_scale_10", admission: str = "ok") -> str:
    """Fake stream driver log with the two G5 labels."""
    return "\n".join(
        [
            "[lb] 2026-09-27T00:00:00 - Silver Stream (Structured Streaming)",
            f"silver_stream_scale_cap: {scale_cap}",
            f"silver_stream_scale_admission: {admission}",
            "[lb] 2026-09-27T00:00:01 - Streaming query started, awaiting termination...",
            "Batch 0: transforming 1000 rows",
            "Batch 0: committed to ice.silver.customer_interactions_enriched in 5.0s",
        ]
    )


def test_streaming_metrics_have_extra_metrics_field():
    """StreamingJobMetrics gains the extra_metrics dict for G5 (and future keys)."""
    m = StreamingJobMetrics(job_name="lb-silver-stream", job_type="silver-stream")
    assert hasattr(m, "extra_metrics")
    assert m.extra_metrics == {}
    m.extra_metrics["silver_stream_scale_cap"] = "measured_up_to_scale_10"
    assert m.to_dict()["extra_metrics"] == {"silver_stream_scale_cap": "measured_up_to_scale_10"}


def test_collector_captures_scale_labels_from_prefixed_line():
    """Labels emitted through common.log (`[lb] <ts> - key: value`) parse."""
    collector = MetricsCollector()
    m = collector.parse_streaming_logs(_driver_log(), "silver-stream")
    assert m.extra_metrics.get("silver_stream_scale_cap") == "measured_up_to_scale_10"
    assert m.extra_metrics.get("silver_stream_scale_admission") == "ok"


def test_collector_captures_scale_labels_from_bare_line():
    """A stream that logs without the `[lb]` prefix still parses (defensive)."""
    logs = "\n".join(
        [
            "silver_stream_scale_cap: measured_up_to_scale_10",
            "silver_stream_scale_admission: labelled",
        ]
    )
    m = MetricsCollector().parse_streaming_logs(logs, "silver-stream")
    assert m.extra_metrics == {
        "silver_stream_scale_cap": "measured_up_to_scale_10",
        "silver_stream_scale_admission": "labelled",
    }


def test_collector_does_not_confuse_scale_labels_with_batch_rows():
    """The scale-label parse skips the batch-processing branches.

    The log's `Batch 0: transforming 1000 rows` line must still count as
    a batch; the label parse continues to the next line rather than
    swallowing it.
    """
    collector = MetricsCollector()
    m = collector.parse_streaming_logs(_driver_log(), "silver-stream")
    assert m.total_rows_processed == 1000
    assert m.total_batches == 1


def test_all_three_silver_streams_call_emit_helper_before_start():
    """The three stream mains delegate G5 to the shared emit_stream_scale_admission
    helper before `query.start()`.

    Static-string emit was replaced by a decision helper so admission
    reflects the real deployment scale (not a hardcoded "ok"). Each
    stream calls the helper; the helper does the emit.
    """
    for script in (
        "silver_stream.py",
        "silver_stream_delta.py",
        "silver_stream_financial.py",
    ):
        text = (_SCRIPTS_DIR / script).read_text()
        assert "emit_stream_scale_admission(" in text, (
            f"{script}: missing emit_stream_scale_admission(...) call"
        )
        helper_idx = text.index("emit_stream_scale_admission(")
        start_idx = text.index(".start()")
        assert helper_idx < start_idx, f"{script}: helper called after .start()"


def _driver_env(scale=None):
    """Env dict for the helper test; None means LB_SCALE unset."""
    if scale is None:
        return {}
    return {"LB_SCALE": str(scale)}


def _call_helper(monkeypatch, scale=None, envelope=10):
    """Import common and call emit_stream_scale_admission with LB_SCALE set."""
    import sys

    sys.path.insert(0, str(_SCRIPTS_DIR))
    monkeypatch.delenv("LB_SCALE", raising=False)
    if scale is not None:
        monkeypatch.setenv("LB_SCALE", str(scale))
    from common import emit_stream_scale_admission

    captured: list[str] = []
    monkeypatch.setattr("common.log", lambda msg, _sink=captured: _sink.append(msg) or None)
    emit_stream_scale_admission(measured_envelope_scale=envelope)
    return captured


def test_helper_admission_ok_at_or_below_envelope(monkeypatch):
    """LB_SCALE within envelope -> admission ok."""
    lines = _call_helper(monkeypatch, scale=10.0, envelope=10)
    assert any("silver_stream_scale_cap: measured_up_to_scale_10" in ln for ln in lines)
    assert any("silver_stream_scale_admission: ok" in ln for ln in lines)


def test_helper_admission_labelled_beyond_envelope(monkeypatch):
    """LB_SCALE above envelope -> labelled_beyond_measured_envelope.

    The stream still runs -- refusal is D-safe territory -- but the
    label must not lie by claiming "ok" (invariant 6).
    """
    lines = _call_helper(monkeypatch, scale=100.0, envelope=10)
    assert any(
        "silver_stream_scale_admission: labelled_beyond_measured_envelope" in ln for ln in lines
    )
    assert not any("silver_stream_scale_admission: ok" in ln for ln in lines)


def test_helper_admission_labelled_when_scale_unknown(monkeypatch):
    """LB_SCALE unset -> labelled_scale_unknown, never ok.

    An unset LB_SCALE means job.py did not thread it; the safe default
    is to label the admission unknown, not silently claim in-envelope.
    """
    lines = _call_helper(monkeypatch, scale=None, envelope=10)
    assert any("silver_stream_scale_admission: labelled_scale_unknown" in ln for ln in lines)


def test_helper_admission_labelled_when_scale_unparseable(monkeypatch):
    lines = _call_helper(monkeypatch, scale="not-a-float", envelope=10)
    assert any("silver_stream_scale_admission: labelled_scale_unknown" in ln for ln in lines)
