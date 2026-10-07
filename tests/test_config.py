"""Tests for configuration loading and validation."""

import pytest

from lakebench.config import (
    LakebenchConfig,
)


class TestScaleConfig:
    """Tests for scale factor configuration."""

    def test_target_size_backward_compat(self):
        """Legacy target_size is converted to scale."""
        with pytest.warns(DeprecationWarning, match="target_size is deprecated"):
            config = LakebenchConfig(
                name="test", architecture={"workload": {"datagen": {"target_size": "100gb"}}}
            )
        assert config.architecture.workload.datagen.scale == 10


class TestComponentValidation:
    """Tests for component combination validation."""

    @pytest.mark.parametrize(
        ("catalog", "fmt"),
        [("unity", "iceberg"), ("polaris", "delta"), ("hive", "hudi")],
    )
    def test_unsupported_combination_rejected(self, catalog, fmt):
        with pytest.raises(ValueError):
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": catalog},
                    "table_format": {"type": fmt},
                    "query_engine": {"type": "trino"},
                },
            )


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


# ---------------------------------------------------------------------------
# Batch Cycles Config (v1.1.0)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Sustained Compaction Config (v1.1.0)
# ---------------------------------------------------------------------------


def test_job_injects_w1_max_vertices_env():
    """job.py must inject LB_FINANCIAL_W1_MAX_VERTICES for financial so
    gold_finalize can thread the configured cap into W1."""
    from pathlib import Path

    p = Path(__file__).resolve().parents[1] / (
        "src/lakebench/modules/pipeline_engines/spark/job.py"
    )
    body = p.read_text()
    assert "LB_FINANCIAL_W1_MAX_VERTICES" in body
    assert "cfg.architecture.workload.w1_max_vertices" in body
