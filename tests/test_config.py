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


class TestSilverWriteMode:
    """Tests for silver_write_mode on SustainedConfig."""

    def test_default_is_merge_on_read(self):
        """Silver's per-batch write tracks the batch, not the table, by default."""
        config = LakebenchConfig(name="test", architecture={"processing": {"sustained": {}}})
        sustained = config.architecture.pipeline.sustained
        assert sustained.silver_write_mode.value == "merge_on_read"


class TestSustainedBenchmarkConfig:
    """Tests for benchmark_interval and benchmark_warmup on SustainedConfig."""

    def test_short_warmup_and_interval_accepted(self):
        """Short rounds are an override, not a refusal, while gold runs back to back."""
        warmup, interval = 60, 60
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {
                    "sustained": {"benchmark_warmup": warmup, "benchmark_interval": interval}
                }
            },
        )
        sustained = config.architecture.pipeline.sustained
        assert (sustained.benchmark_warmup, sustained.benchmark_interval) == (warmup, interval)

    @pytest.mark.parametrize("field", ["benchmark_warmup", "benchmark_interval"])
    def test_below_sixty_seconds_refused(self, field):
        with pytest.raises(ValidationError):
            LakebenchConfig(
                name="test",
                architecture={"processing": {"sustained": {field: 59}}},
            )

    def test_gold_interval_still_raises_short_values(self):
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {
                    "sustained": {
                        "gold_refresh_interval": "5 minutes",
                        "benchmark_warmup": 120,
                        "benchmark_interval": 120,
                    }
                }
            },
        )
        sustained = config.architecture.pipeline.sustained
        assert (sustained.benchmark_warmup, sustained.benchmark_interval) == (300, 300)

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
