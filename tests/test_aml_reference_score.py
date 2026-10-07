"""Tests for the AML band leakage gate.

- The gate correctly names a leaking band and correctly clears a
  non-leaking one, per the AML audit's 10 % threshold.
- Bands with no typology transactions produce ``NO_TYPOLOGY`` and
  do not sway the overall verdict.
"""

from __future__ import annotations

import pytest

from lakebench.aml.reference_score import (
    DEFAULT_LEAKAGE_RATIO,
    LeakageVerdict,
    compute_leakage_gate,
)


class TestLeakageGate:
    def test_leaking_band_flagged(self):
        """AML audit's structuring-band example: baseline count is
        very small relative to typology in the USD 9500..9999 band."""
        report = compute_leakage_gate(
            [
                {
                    "currency": "USD",
                    "band_lo": 9500.0,
                    "band_hi": 9999.0,
                    "baseline_count": 3,
                    "typology_count": 1000,
                },
            ]
        )
        assert not report.overall_pass
        row = report.rows[0]
        assert row.verdict is LeakageVerdict.LEAKING
        assert row.ratio_baseline_over_typology == pytest.approx(0.003)
        # Hint must be actionable (call out band + fix directions).
        assert "USD" in row.hint
        assert "9500" in row.hint or "9,500" in row.hint
        assert "baseline" in row.hint.lower()

    def test_passing_band_flagged(self):
        """A band with enough baseline density is not the sole label."""
        report = compute_leakage_gate(
            [
                {
                    "currency": "USD",
                    "band_lo": 9500.0,
                    "band_hi": 9999.0,
                    "baseline_count": 500,
                    "typology_count": 1000,
                },
            ]
        )
        assert report.overall_pass
        row = report.rows[0]
        assert row.verdict is LeakageVerdict.PASS
        assert row.ratio_baseline_over_typology == pytest.approx(0.5)

    def test_no_typology_does_not_affect_overall_when_others_pass(self):
        """A currency with zero typology rows carries no leakage claim
        and must NOT flip overall_pass to False *as long as at least
        one other band was scored*."""
        report = compute_leakage_gate(
            [
                {
                    "currency": "KRW",
                    "band_lo": 9_900_000.0,
                    "band_hi": 9_999_999.0,
                    "baseline_count": 42,
                    "typology_count": 0,
                },
                {
                    "currency": "USD",
                    "band_lo": 9500.0,
                    "band_hi": 9999.0,
                    "baseline_count": 500,
                    "typology_count": 1000,
                },
            ]
        )
        assert report.overall_pass
        verdicts = {r.currency: r.verdict for r in report.rows}
        assert verdicts["KRW"] is LeakageVerdict.NO_TYPOLOGY
        assert verdicts["USD"] is LeakageVerdict.PASS

    def test_all_no_typology_is_not_a_pass(self):
        """ADR-P2 from PR-A adversarial review: if every band has zero
        typology rows, that's a datagen regression, not a pass.
        overall_pass must be False so a "nothing planted" run does
        not masquerade as a clean gate."""
        report = compute_leakage_gate(
            [
                {
                    "currency": "USD",
                    "band_lo": 9500.0,
                    "band_hi": 9999.0,
                    "baseline_count": 100,
                    "typology_count": 0,
                },
                {
                    "currency": "GBP",
                    "band_lo": 14700.0,
                    "band_hi": 14995.0,
                    "baseline_count": 100,
                    "typology_count": 0,
                },
            ]
        )
        assert not report.overall_pass

    def test_empty_report_is_not_a_pass(self):
        """No rows scored means nothing was measured -- do not pass."""
        report = compute_leakage_gate([])
        assert not report.overall_pass

    def test_boundary_ratio_is_pass(self):
        """Ratio exactly at the threshold passes (>=, not >)."""
        report = compute_leakage_gate(
            [
                {
                    "currency": "USD",
                    "band_lo": 9500.0,
                    "band_hi": 9999.0,
                    "baseline_count": 100,
                    "typology_count": 1000,
                },
            ],
            threshold_ratio=DEFAULT_LEAKAGE_RATIO,
        )
        assert report.rows[0].verdict is LeakageVerdict.PASS

    def test_overall_pass_requires_every_band(self):
        """A single LEAKING band flips overall_pass to False even if
        others pass."""
        report = compute_leakage_gate(
            [
                {
                    "currency": "USD",
                    "band_lo": 9500.0,
                    "band_hi": 9999.0,
                    "baseline_count": 5,
                    "typology_count": 1000,
                },
                {
                    "currency": "GBP",
                    "band_lo": 14700.0,
                    "band_hi": 14995.0,
                    "baseline_count": 500,
                    "typology_count": 1000,
                },
            ]
        )
        assert not report.overall_pass

    def test_serialisable_rows(self):
        """Report can be turned into a list of plain dicts for parquet."""
        report = compute_leakage_gate(
            [
                {
                    "currency": "USD",
                    "band_lo": 9500.0,
                    "band_hi": 9999.0,
                    "baseline_count": 3,
                    "typology_count": 1000,
                },
            ]
        )
        rows = report.as_dicts()
        assert len(rows) == 1
        assert rows[0]["verdict"] == "leaking"
        assert rows[0]["currency"] == "USD"
        assert rows[0]["baseline_count"] == 3

    def test_threshold_must_be_positive(self):
        with pytest.raises(ValueError):
            compute_leakage_gate([], threshold_ratio=0.0)
