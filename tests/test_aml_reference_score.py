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
    @pytest.mark.parametrize(
        ("label", "baseline", "threshold", "verdict", "ratio", "overall"),
        [
            # AML audit's structuring-band example: baseline tiny next to
            # typology leaks (USD 9500..9999 band, typology 1000).
            ("leaking", 3, None, LeakageVerdict.LEAKING, 0.003, False),
            # enough baseline density passes.
            ("passing", 500, None, LeakageVerdict.PASS, 0.5, True),
            # a ratio exactly at the threshold passes (>=, not >).
            ("boundary", 100, DEFAULT_LEAKAGE_RATIO, LeakageVerdict.PASS, None, None),
        ],
    )
    def test_single_band_verdict(self, label, baseline, threshold, verdict, ratio, overall):
        kw = {"threshold_ratio": threshold} if threshold is not None else {}
        report = compute_leakage_gate(
            [
                {
                    "currency": "USD",
                    "band_lo": 9500.0,
                    "band_hi": 9999.0,
                    "baseline_count": baseline,
                    "typology_count": 1000,
                },
            ],
            **kw,
        )
        row = report.rows[0]
        assert row.verdict is verdict, label
        if ratio is not None:
            assert row.ratio_baseline_over_typology == pytest.approx(ratio), label
        if overall is not None:
            assert report.overall_pass is overall, label

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

    @pytest.mark.parametrize(
        "rows",
        [
            [],
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
            ],
        ],
        ids=["no_rows", "every_band_without_typology"],
    )
    def test_nothing_measured_is_not_a_pass(self, rows):
        """Nothing scored, or no typology planted in any band, must not
        masquerade as a clean gate."""
        assert not compute_leakage_gate(rows).overall_pass

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

    def test_threshold_must_be_positive(self):
        with pytest.raises(ValueError):
            compute_leakage_gate([], threshold_ratio=0.0)
