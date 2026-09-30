"""Tests for the AML leakage gate and reference-detector library.

These are pure-Python; no Spark, no live model training beyond
scikit-learn's smallest usable case. The point is to lock the
contract the standing rule requires:

- The gate correctly names a leaking band and correctly clears a
  non-leaking one, per the AML audit's 10 % threshold.
- Bands with no typology transactions produce ``NO_TYPOLOGY`` and
  do not sway the overall verdict.
- The reference model REFUSES to train on features known to encode
  the label; the refusal is explicit and names the offending columns.
- When scikit-learn is available, the model trains and returns
  per-typology recall on a synthetic dataset. When it isn't, the
  report says so cleanly rather than raising.
"""

from __future__ import annotations

import importlib.util

import numpy as np
import pandas as pd
import pytest

from lakebench.aml.reference_score import (
    DEFAULT_LEAKAGE_RATIO,
    LEAKY_FEATURES,
    LeakageVerdict,
    ReferenceModelVerdict,
    compute_leakage_gate,
    train_reference_gbt,
)

_HAS_SKLEARN = importlib.util.find_spec("sklearn") is not None


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


class TestReferenceModelLeakyRefuse:
    """The library refuses to train on features that ARE the label.
    This is the single most important safety property of the module."""

    @pytest.mark.parametrize("leaky", sorted(LEAKY_FEATURES))
    def test_refuses_any_leaky_feature(self, leaky):
        features = pd.DataFrame(
            {
                "log_amount": np.random.default_rng(0).normal(size=20),
                leaky: np.zeros(20),
            }
        )
        labels = pd.Series(["baseline"] * 20)
        with pytest.raises(ValueError) as ei:
            train_reference_gbt(features, labels)
        assert leaky in str(ei.value)
        assert "tautology" in str(ei.value) or "leaky" in str(ei.value).lower()

    def test_refusal_lists_all_leaks(self):
        features = pd.DataFrame(
            {
                "log_amount": [0.0] * 5,
                "amount_in_structuring_band": [1] * 5,
                "typology_type": ["x"] * 5,
            }
        )
        labels = pd.Series(["baseline"] * 5)
        with pytest.raises(ValueError) as ei:
            train_reference_gbt(features, labels)
        msg = str(ei.value)
        assert "amount_in_structuring_band" in msg
        assert "typology_type" in msg


@pytest.mark.skipif(not _HAS_SKLEARN, reason="scikit-learn not installed")
class TestReferenceModelTrain:
    """When scikit-learn is available, the GBT trains and produces
    per-typology metrics. Uses a synthetic separable dataset so the
    test is deterministic."""

    def _synthetic(self, n_baseline=200, n_typ_a=60, n_typ_b=60, seed=0):
        rng = np.random.default_rng(seed)
        # Baseline: mean ~= 5, spread of 2.
        base_amt = rng.normal(loc=5.0, scale=2.0, size=n_baseline)
        base_cp = rng.integers(2, 15, size=n_baseline)
        # Typology A: high mean amount, low counterparty count.
        a_amt = rng.normal(loc=15.0, scale=1.0, size=n_typ_a)
        a_cp = rng.integers(1, 3, size=n_typ_a)
        # Typology B: low mean amount, high counterparty count.
        b_amt = rng.normal(loc=2.0, scale=0.5, size=n_typ_b)
        b_cp = rng.integers(20, 40, size=n_typ_b)

        features = pd.DataFrame(
            {
                "log_amount": np.concatenate([base_amt, a_amt, b_amt]),
                "counterparty_count": np.concatenate([base_cp, a_cp, b_cp]),
            }
        )
        labels = pd.Series(["baseline"] * n_baseline + ["typ_a"] * n_typ_a + ["typ_b"] * n_typ_b)
        return features, labels

    def test_recovers_signal_on_separable_data(self):
        features, labels = self._synthetic()
        report = train_reference_gbt(features, labels, random_state=0)
        assert report.verdict is ReferenceModelVerdict.OK
        # Model should get respectable recall on both synthetic
        # typologies. The bar is deliberately low (~0.70) so a small
        # random split does not flake this test.
        by_name = {r.typology_type: r for r in report.per_typology}
        assert by_name["typ_a"].recall >= 0.70
        assert by_name["typ_b"].recall >= 0.70
        assert 0.0 < report.overall_f1 <= 1.0
        assert report.n_train + report.n_test == 320

    def test_insufficient_labels_verdict(self):
        """A too-small synthetic set produces INSUFFICIENT_LABELS but
        still returns partial per-typology rows for inspection."""
        features, labels = self._synthetic(n_baseline=20, n_typ_a=6, n_typ_b=6)
        report = train_reference_gbt(features, labels, min_positive_per_class=100)
        assert report.verdict is ReferenceModelVerdict.INSUFFICIENT_LABELS
        assert len(report.per_typology) == 2
        assert "min_positive_per_class" in report.note

    def test_feature_names_excluded_features_recorded(self):
        features, labels = self._synthetic()
        report = train_reference_gbt(features, labels)
        assert set(report.feature_names) == {"log_amount", "counterparty_count"}
        assert set(report.excluded_features) == LEAKY_FEATURES

    def test_report_serialises_cleanly(self):
        features, labels = self._synthetic()
        report = train_reference_gbt(features, labels)
        d = report.as_dict()
        assert d["verdict"] == "ok"
        assert "per_typology" in d
        assert d["overall_f1"] >= 0.0


@pytest.mark.skipif(_HAS_SKLEARN, reason="only runs when sklearn is absent")
class TestReferenceModelNoSklearn:
    def test_no_sklearn_verdict(self):
        features = pd.DataFrame({"log_amount": [0.0, 1.0, 2.0]})
        labels = pd.Series(["baseline", "typ_a", "baseline"])
        report = train_reference_gbt(features, labels)
        assert report.verdict is ReferenceModelVerdict.NO_SKLEARN
        assert "scikit-learn" in report.note or "sklearn" in report.note
