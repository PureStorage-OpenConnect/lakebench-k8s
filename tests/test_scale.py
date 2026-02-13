"""Tests for scale factor module."""

import pytest

from lakebench.config.scale import (
    compute_guidance,
    customer360_dimensions,
    financial_dimensions,
    get_dimensions,
    iot_dimensions,
)


class TestCustomer360Dimensions:
    """Tests for Customer360 scale-to-dimension mapping."""

    def test_scale_1(self):
        dims = customer360_dimensions(1)
        assert dims.scale == 1
        assert dims.customers == 100_000
        assert dims.events_per_customer == 24
        assert dims.date_range_days == 365
        assert dims.approx_rows == 2_400_000
        assert dims.approx_bronze_gb == 10.0

    def test_scale_10(self):
        dims = customer360_dimensions(10)
        assert dims.customers == 1_000_000
        assert dims.approx_rows == 24_000_000
        assert dims.approx_bronze_gb == 100.0

    def test_scale_100(self):
        dims = customer360_dimensions(100)
        assert dims.customers == 10_000_000
        assert dims.approx_rows == 240_000_000
        assert dims.approx_bronze_gb == 1000.0
        assert dims.approx_bronze_tb == pytest.approx(1000.0 / 1024)

    def test_scale_1000(self):
        dims = customer360_dimensions(1000)
        assert dims.customers == 100_000_000
        assert dims.approx_rows == 2_400_000_000
        assert dims.approx_bronze_gb == 10_000.0

    def test_linearity(self):
        """Customers and rows scale linearly."""
        d1 = customer360_dimensions(1)
        d10 = customer360_dimensions(10)
        assert d10.customers == d1.customers * 10
        assert d10.approx_rows == d1.approx_rows * 10
        assert d10.approx_bronze_gb == d1.approx_bronze_gb * 10

    def test_events_per_customer_fixed(self):
        """Events per customer is the same at any scale."""
        d1 = customer360_dimensions(1)
        d100 = customer360_dimensions(100)
        assert d1.events_per_customer == d100.events_per_customer == 24

    def test_frozen(self):
        """ScaleDimensions is immutable."""
        dims = customer360_dimensions(1)
        with pytest.raises(AttributeError):
            dims.scale = 2  # type: ignore[misc]


class TestIoTDimensions:
    """Tests for IoT scale-to-dimension mapping."""

    def test_scale_1(self):
        dims = iot_dimensions(1)
        assert dims.customers == 1_000  # sensors
        assert dims.date_range_days == 30
        assert dims.approx_bronze_gb == 10.0

    def test_linearity(self):
        d1 = iot_dimensions(1)
        d5 = iot_dimensions(5)
        assert d5.customers == d1.customers * 5


class TestFinancialDimensions:
    """Tests for Financial scale-to-dimension mapping."""

    def test_scale_1(self):
        dims = financial_dimensions(1)
        assert dims.customers == 500_000  # accounts
        assert dims.date_range_days == 365
        assert dims.approx_bronze_gb == 10.0


class TestGetDimensions:
    """Tests for the get_dimensions registry lookup."""

    def test_customer360(self):
        dims = get_dimensions("customer360", 10)
        assert dims.customers == 1_000_000

    def test_iot(self):
        dims = get_dimensions("iot", 1)
        assert dims.customers == 1_000

    def test_financial(self):
        dims = get_dimensions("financial", 1)
        assert dims.customers == 500_000

    def test_unknown_schema_raises(self):
        with pytest.raises(ValueError, match="Unknown schema type"):
            get_dimensions("unknown_schema", 1)

    def test_scale_zero_raises(self):
        with pytest.raises(ValueError, match="Scale must be >= 1"):
            get_dimensions("customer360", 0)

    def test_scale_negative_raises(self):
        with pytest.raises(ValueError, match="Scale must be >= 1"):
            get_dimensions("customer360", -5)


class TestComputeGuidance:
    """Tests for compute guidance tiers."""

    def test_minimal_tier(self):
        """Scale 1-5 -> minimal tier."""
        g = compute_guidance(1)
        assert g.tier_name == "minimal"
        assert g.min_executors == 2
        assert g.recommended_executors == 2
        assert g.recommended_memory == "4g"
        assert g.recommended_cores == 2
        assert g.warning is None

    def test_minimal_boundary(self):
        """Scale 5 is still minimal."""
        g = compute_guidance(5)
        assert g.tier_name == "minimal"

    def test_balanced_tier(self):
        """Scale 6-50 -> balanced tier."""
        g = compute_guidance(6)
        assert g.tier_name == "balanced"
        assert g.min_executors == 4
        assert g.recommended_memory == "16g"
        assert g.warning is None

    def test_balanced_boundary(self):
        """Scale 50 is still balanced."""
        g = compute_guidance(50)
        assert g.tier_name == "balanced"

    def test_performance_tier(self):
        """Scale 51-500 -> performance tier."""
        g = compute_guidance(51)
        assert g.tier_name == "performance"
        assert g.min_executors == 8
        assert g.recommended_memory == "32g"
        assert g.warning is None

    def test_performance_boundary(self):
        """Scale 500 is still performance."""
        g = compute_guidance(500)
        assert g.tier_name == "performance"

    def test_extreme_tier(self):
        """Scale 501+ -> extreme tier."""
        g = compute_guidance(501)
        assert g.tier_name == "extreme"
        assert g.min_executors == 16
        assert g.recommended_memory == "48g"
        assert g.warning is not None  # extreme always has a warning

    def test_extreme_large_scale(self):
        """Large scale gets extreme guidance with warning."""
        g = compute_guidance(5000)
        assert g.tier_name == "extreme"
        assert "5000" in g.warning

    def test_frozen(self):
        """ComputeGuidance is immutable."""
        g = compute_guidance(1)
        with pytest.raises(AttributeError):
            g.tier_name = "other"  # type: ignore[misc]
