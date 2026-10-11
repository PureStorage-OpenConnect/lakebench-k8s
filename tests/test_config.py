"""Tests for configuration loading and validation."""

import pytest
from pydantic import ValidationError

from lakebench.config import LakebenchConfig


class TestComponentValidation:
    """Tests for component combination validation."""

    @pytest.mark.parametrize(
        ("catalog", "fmt", "loc"),
        [
            ("unity", "iceberg", ("architecture",)),
            ("polaris", "delta", ("architecture",)),
            ("hive", "hudi", ("architecture", "table_format", "type")),
        ],
    )
    def test_unsupported_combination_rejected(self, catalog, fmt, loc):
        with pytest.raises(ValidationError) as exc:
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": catalog},
                    "table_format": {"type": fmt},
                    "query_engine": {"type": "trino"},
                },
            )
        assert [e["loc"] for e in exc.value.errors()] == [loc]


class TestSustainedBenchmarkConfig:
    """Tests for benchmark_interval and benchmark_warmup on SustainedConfig."""

    @pytest.mark.parametrize(
        ("gold_refresh", "settings", "field", "expected"),
        [
            ("10 minutes", {"benchmark_warmup": 300}, "benchmark_warmup", 600),
            ("5 minutes", {"benchmark_warmup": 300}, "benchmark_warmup", 300),
            ("10 minutes", {"benchmark_interval": 300}, "benchmark_interval", 600),
            (
                "5 minutes",
                {"benchmark_interval": 300, "benchmark_warmup": 300},
                "benchmark_interval",
                300,
            ),
        ],
    )
    def test_warmup_and_interval_never_below_gold_refresh(
        self, gold_refresh, settings, field, expected
    ):
        """The measurement window must not start before gold refreshes."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {"sustained": {"gold_refresh_interval": gold_refresh, **settings}}
            },
        )
        assert getattr(config.architecture.pipeline.sustained, field) == expected
