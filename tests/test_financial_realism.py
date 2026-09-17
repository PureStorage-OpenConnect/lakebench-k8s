"""Tests for the datagen realism distributions.

Asserts distribution SHAPE, not fidelity. Real bank distributions are
proprietary; ours are order-of-magnitude approximations chosen to make
the first-30-seconds practitioner queries return plausible numbers.
"""

from __future__ import annotations

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

DATAGEN_DIR = Path(__file__).resolve().parents[1] / "datagen"
if str(DATAGEN_DIR) not in sys.path:
    sys.path.insert(0, str(DATAGEN_DIR))

np = pytest.importorskip("numpy")


class TestLogNormalAmount:
    def test_median_in_expected_range(self):
        from realism import log_normal_amount

        rng = np.random.default_rng(seed=42)
        amounts = [float(log_normal_amount(rng)) for _ in range(5_000)]
        p50 = float(np.percentile(amounts, 50))
        # Median configured at $5,000; sampling variance keeps it in [3K, 8K]
        assert 3_000 <= p50 <= 8_000

    def test_p95_in_expected_range(self):
        from realism import log_normal_amount

        rng = np.random.default_rng(seed=42)
        amounts = [float(log_normal_amount(rng)) for _ in range(5_000)]
        p95 = float(np.percentile(amounts, 95))
        assert 30_000 <= p95 <= 100_000, f"p95={p95}"

    def test_amount_ceiling_enforced(self):
        from realism import log_normal_amount

        # Try a lot of samples; ceiling is 50M. Nothing should exceed it.
        rng = np.random.default_rng(seed=42)
        amounts = [log_normal_amount(rng) for _ in range(10_000)]
        assert max(amounts) <= Decimal("50000000.00")

    def test_deterministic_given_seed(self):
        from realism import log_normal_amount

        a = np.random.default_rng(seed=99)
        b = np.random.default_rng(seed=99)
        for _ in range(50):
            assert log_normal_amount(a) == log_normal_amount(b)


class TestStructuringAmount:
    def test_tight_cluster_under_ctr_threshold(self):
        from realism import structuring_amount

        rng = np.random.default_rng(seed=42)
        amounts = [float(structuring_amount(rng)) for _ in range(1_000)]
        assert min(amounts) >= 9_500
        assert max(amounts) < 10_000  # never breaches CTR


class TestPartySelector:
    def test_hot_corporates_get_share_of_traffic(self):
        from realism import PartySelector

        sel = PartySelector(customer_id_max=100_000, hot_corp_count=500, hot_corp_share=0.4)
        rng = np.random.default_rng(seed=42)
        samples = [sel.sample(rng) for _ in range(20_000)]
        top500 = sum(1 for s in samples if s <= 500)
        # Configured 40% hit rate for hot corporates; allow +-15% wiggle
        assert 0.25 <= top500 / len(samples) <= 0.55, f"top500 share = {top500/len(samples):.2f}"

    def test_ids_never_exceed_customer_id_max(self):
        from realism import PartySelector

        sel = PartySelector(customer_id_max=1_000, hot_corp_count=50)
        rng = np.random.default_rng(seed=42)
        for _ in range(1_000):
            s = sel.sample(rng)
            assert 1 <= s <= 1_000


class TestCorridorMix:
    def test_all_weights_positive_and_normalisable(self):
        from realism import CORRIDORS

        total = sum(w for _, w in CORRIDORS)
        # Sampler renormalises to 1.0 internally; table just needs to be
        # a positive, sensibly-scoped weight distribution.
        assert total > 0
        assert 0.5 <= total <= 1.5
        assert all(w > 0 for _, w in CORRIDORS)

    def test_cross_border_share_realistic(self):
        from realism import sample_corridor

        rng = np.random.default_rng(seed=42)
        pairs = [sample_corridor(rng) for _ in range(10_000)]
        cross_border = sum(1 for a, b in pairs if a != b)
        # Corridor table has ~55% cross-border weight; expect 45-70%
        assert 0.40 <= cross_border / len(pairs) <= 0.75


class TestSanctionsAndPep:
    def test_sanctions_rate_near_configured(self):
        from realism import sanctions_flag

        flagged = sum(1 for eid in range(1, 20_001) if sanctions_flag(eid))
        # 0.5% target; hash-bucket variance keeps to [0.3%, 0.8%]
        rate = flagged / 20_000
        assert 0.003 <= rate <= 0.008, f"sanctions rate = {rate:.4f}"

    def test_pep_rate_near_configured(self):
        from realism import pep_flag

        flagged = sum(1 for eid in range(1, 20_001) if pep_flag(eid))
        rate = flagged / 20_000
        assert 0.015 <= rate <= 0.026, f"pep rate = {rate:.4f}"

    def test_flags_deterministic_across_calls(self):
        from realism import pep_flag, sanctions_flag

        assert sanctions_flag(12345) == sanctions_flag(12345)
        assert pep_flag(12345) == pep_flag(12345)


class TestCorrespondentChain:
    def test_cross_border_chain_populated_frequently(self):
        from realism import build_bic_pool, sample_correspondent_chain

        bics = build_bic_pool(40)
        rng = np.random.default_rng(seed=42)
        populated = 0
        n = 5_000
        for _ in range(n):
            a, b, c = sample_correspondent_chain(rng, True, bics)
            if a is not None:
                populated += 1
        rate = populated / n
        # Cross-border rate is 35%; expect 25-45%
        assert 0.25 <= rate <= 0.45, f"cross-border chain rate = {rate:.3f}"

    def test_domestic_chain_rarely_populated(self):
        from realism import build_bic_pool, sample_correspondent_chain

        bics = build_bic_pool(40)
        rng = np.random.default_rng(seed=42)
        populated = 0
        n = 5_000
        for _ in range(n):
            a, b, c = sample_correspondent_chain(rng, False, bics)
            if a is not None:
                populated += 1
        rate = populated / n
        # Domestic rate is 2%; expect below 5%
        assert rate <= 0.05, f"domestic chain rate = {rate:.3f}"


class TestRegulatoryReporting:
    def test_cross_border_over_threshold_reports(self):
        from realism import sample_regulatory_reporting

        entries = sample_regulatory_reporting("US", "GB", Decimal("15000.00"))
        assert entries is not None
        assert len(entries) == 2
        countries = {e["authrty_ctry"] for e in entries}
        assert countries == {"US", "GB"}

    def test_cross_border_under_threshold_does_not_report(self):
        from realism import sample_regulatory_reporting

        entries = sample_regulatory_reporting("US", "GB", Decimal("5000.00"))
        assert entries is None

    def test_domestic_never_reports(self):
        from realism import sample_regulatory_reporting

        entries = sample_regulatory_reporting("US", "US", Decimal("500000.00"))
        assert entries is None


class TestTimestampShape:
    def test_intraday_shape_peaks_at_business_hours(self):
        from realism import sample_timestamp_shaped

        rng = np.random.default_rng(seed=42)
        start = datetime(2026, 3, 2)  # a Monday
        end = datetime(2026, 3, 6)     # a Friday
        samples = [sample_timestamp_shaped(rng, start, end) for _ in range(3_000)]
        # Business hours 08:00-17:59 should hold >45% of volume
        business_hours = sum(1 for t in samples if 8 <= t.hour <= 17)
        assert business_hours / len(samples) >= 0.45

    def test_ordered_window_preserved(self):
        from realism import sample_timestamp_shaped

        rng = np.random.default_rng(seed=42)
        start = datetime(2026, 3, 2, 12, 0)
        end = datetime(2026, 3, 2, 12, 30)
        for _ in range(100):
            t = sample_timestamp_shaped(rng, start, end)
            assert start <= t <= end
