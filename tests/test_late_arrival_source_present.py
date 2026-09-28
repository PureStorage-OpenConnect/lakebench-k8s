"""D-full-simple invariant-6 label: static-analysis fix-reverted gate.

The Spark subprocess tests
(``tests/spark/test_late_arrival_labelled.py``,
``tests/spark/test_late_arrival_strict_refuses.py``) exercise the
label + refusal live but skip when no Iceberg jar is on disk (local
dev / lint CI). This module asserts the load-bearing lines are present
in the tree so a regression that deletes them fails fast even in the
lint tier -- the same fix-reverted-must-fail contract without needing
Iceberg.

Keep the checks tight to the shape of the fix rather than a generic
`grep any of the words`, so a rename or refactor still trips the gate
unless the meaning is preserved. Each check names the invariant it
protects.
"""

from __future__ import annotations

from pathlib import Path

_STREAM = (
    Path(__file__).resolve().parent.parent
    / "src/lakebench/spark/scripts/silver_stream_financial.py"
)


def _src() -> str:
    return _STREAM.read_text()


def test_late_iban_count_is_computed_per_batch():
    """The late-arrival detection compares this-batch min book_ts against
    prior max book_ts per account_id. Removing this comparison silently
    lets the arrival-order running_balance ship as if it matched batch mode.
    """
    src = _src()
    assert "_this_min_book_ts" in src, (
        "silver_stream_financial no longer computes this-batch min book_ts; "
        "the D-full-simple invariant-6 late-arrival detection is broken"
    )
    assert "_prev_max_book_ts" in src, (
        "silver_stream_financial no longer reads prior max book_ts from "
        "silver.account_statements; late arrivals silently pass"
    )
    assert "late_iban_count" in src, (
        "silver_stream_financial no longer reports late_iban_count; "
        "downstream label emission is a wire-not-connected no-op"
    )


def test_strict_parity_env_gate_present():
    """LB_SILVER_STATEMENTS_STRICT_PARITY=1 must raise SilverAbort on any
    late arrival. This is the invariant-2 gate for batch/stream comparison
    validity; removing it lets a live gate publish divergent numbers.
    """
    src = _src()
    assert "LB_SILVER_STATEMENTS_STRICT_PARITY" in src, (
        "the strict-parity opt-in env var was removed; late arrivals no "
        "longer refuse the run in strict mode"
    )
    assert "STATEMENTS_STRICT_PARITY_ENV" in src, (
        "the module-level env-var constant was removed; the strict gate "
        "check has no anchor"
    )
    # The gate composes: strict env var AND non-zero late count.
    assert "late_iban_count > 0" in src and "STATEMENTS_STRICT_PARITY_ENV" in src, (
        "the strict-parity gate no longer conditions on late_iban_count > 0 "
        "and the env var together; a partial gate would either silently "
        "skip late arrivals or fail on strict-monotone runs"
    )


def test_parity_mode_label_emission_present():
    """Per-batch labels (silver_statements_parity_mode +
    silver_statements_late_arrivals_this_batch) must be emitted through
    common.log so parse_streaming_logs can lift them into metrics.json.
    """
    src = _src()
    assert "silver_statements_parity_mode" in src, (
        "the parity-mode label is no longer emitted; downstream reports "
        "cannot distinguish strict-monotone runs from arrival-order fallbacks"
    )
    assert "silver_statements_late_arrivals_this_batch" in src, (
        "the per-batch late-arrival counter label is no longer emitted"
    )
    assert "silver_statements_total_late_arrivals" in src, (
        "the run-total late-arrival counter is no longer emitted at stream stop"
    )
    assert "strict_monotone" in src and "arrival_order_running_balance" in src, (
        "the two parity-mode values were removed; the label carries no "
        "meaning without them"
    )


def test_shared_opening_balance_helper_is_used():
    """The deterministic opening-balance formula is shared between the
    batch build (silver_build_financial.build_statements) and the stream
    (silver_stream_financial._maintain_statements) through
    common.aml_opening_balance so a formula change cannot drift the two
    write paths silently.
    """
    stream_src = _src()
    assert "aml_opening_balance" in stream_src, (
        "silver_stream_financial no longer imports the shared helper; "
        "the stream's opening_balance formula can drift from batch"
    )
    build = (
        Path(__file__).resolve().parent.parent
        / "src/lakebench/spark/scripts/silver_build_financial.py"
    ).read_text()
    assert "aml_opening_balance" in build, (
        "silver_build_financial no longer uses the shared helper; "
        "the batch's opening_balance formula can drift from stream"
    )
    common = (
        Path(__file__).resolve().parent.parent
        / "src/lakebench/spark/scripts/common.py"
    ).read_text()
    assert "def aml_opening_balance" in common, (
        "common.aml_opening_balance was removed"
    )
