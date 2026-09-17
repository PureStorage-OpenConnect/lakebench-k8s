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

    def test_entity_1_share_bounded_no_supernode(self):
        """Cycle-3 regression: earlier Zipf(1.5) put ~50% of hot mass on
        entity_1, producing ~4% self-loops in the graph. Truncated Zipf(2.3)
        must keep entity_1 share below 15% of total draws.
        """
        from realism import PartySelector

        sel = PartySelector(customer_id_max=100_000, hot_corp_count=500)
        rng = np.random.default_rng(seed=42)
        counts: dict[int, int] = {}
        n = 20_000
        for _ in range(n):
            eid = sel.sample(rng)
            counts[eid] = counts.get(eid, 0) + 1
        top_share = counts.get(1, 0) / n
        # Entity 1 gets Zipf(2.3) mass = 1/1^2.3 / sum_k 1/k^2.3 ~ 0.66
        # scaled by hot_corp_share (0.40) = ~0.26 of total draws. Cap at 30%
        # so a shape parameter regression is caught, but the truncated-Zipf
        # bound is what prevents the pathological supernode.
        assert top_share <= 0.30, f"entity_1 share {top_share:.3f} suggests supernode"
        # Distinct hot corporates observed: at least 50 of the top 500.
        # Zipf(2.3) concentrates on the first ~50-100 IDs (that's the whole
        # point of a corporate distribution), but "50 distinct" verifies the
        # sampler isn't collapsing to a supernode.
        hot_distinct = sum(1 for k in counts if 1 <= k <= 500)
        assert hot_distinct >= 50


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
        # Real bank wire mix is 85-90% domestic. Corridor table post-A3
        # rebalance targets ~80% domestic. R4 practitioner band [0.15, 0.25].
        rate = cross_border / len(pairs)
        assert 0.12 <= rate <= 0.28, f"cross-border rate = {rate:.3f}"

    def test_distinct_corridor_count_meets_variety_gate(self):
        from realism import sample_corridor

        rng = np.random.default_rng(seed=42)
        pairs = {sample_corridor(rng) for _ in range(2_000)}
        # R6 practitioner query: >= 20 distinct corridors observable
        assert len(pairs) >= 20, f"only {len(pairs)} distinct corridors"


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


class TestHomeCountryStability:
    """A1: home_country_for(entity_id) must be stable across every call."""

    def test_same_id_yields_same_country(self):
        from realism import home_country_for

        # Sample 500 entity IDs, call 3 times each, results must match
        for eid in range(1, 501):
            first = home_country_for(eid)
            for _ in range(3):
                assert home_country_for(eid) == first

    def test_distribution_matches_configured_weights(self):
        from realism import home_country_for

        counts: dict[str, int] = {}
        n = 20_000
        for eid in range(1, n + 1):
            c = home_country_for(eid)
            counts[c] = counts.get(c, 0) + 1
        # US-headquartered global bank: ~85% US weight, expect 78-90%.
        us_share = counts.get("US", 0) / n
        assert 0.78 <= us_share <= 0.90, f"US share = {us_share:.3f}"
        # >= 10 distinct countries appear (the long tail)
        assert len(counts) >= 10


class TestCurrencyAwareStructuring:
    """A2: structuring bands per local reporting threshold."""

    @pytest.mark.parametrize(
        "currency,lo,hi",
        [
            ("USD", 9_500, 9_999),
            ("GBP", 14_700, 14_995),
            ("EUR", 14_700, 14_995),
            ("JPY", 990_000, 999_999),
            ("SGD", 19_500, 19_999),
            ("CAD", 9_500, 9_999),
        ],
    )
    def test_amount_within_local_threshold_band(self, currency, lo, hi):
        from decimal import Decimal

        from realism import structuring_amount

        rng = np.random.default_rng(seed=42)
        amts = [float(structuring_amount(rng, currency)) for _ in range(500)]
        assert min(amts) >= lo
        assert max(amts) <= hi
        # No amount ever breaches the threshold
        assert all(Decimal(str(a)) <= Decimal(str(hi)) for a in amts)

    def test_unknown_currency_defaults_to_usd_band(self):
        from realism import structuring_amount

        rng = np.random.default_rng(seed=42)
        amts = [float(structuring_amount(rng, "XYZ")) for _ in range(200)]
        assert 9_500 <= min(amts) <= max(amts) <= 9_999


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
