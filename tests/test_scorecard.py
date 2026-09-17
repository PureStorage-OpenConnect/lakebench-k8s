"""Tests for the ScorecardBlock domain hook (ENG-2C.5)."""

import pytest

from lakebench.reports.scorecard import (
    Customer360ScorecardBlock,
    FinancialScorecardBlock,
    ScorecardBlock,
    get_scorecard_block,
    register_scorecard_block,
)


class TestScorecardBlockRegistry:
    def test_customer360_lookup(self):
        block = get_scorecard_block("customer360")
        assert isinstance(block, Customer360ScorecardBlock)
        assert block.domain_label == "Customer360"

    def test_financial_lookup(self):
        block = get_scorecard_block("financial")
        assert isinstance(block, FinancialScorecardBlock)
        assert "Financial" in block.domain_label

    def test_missing_schema_defaults_to_customer360(self):
        block = get_scorecard_block(None)
        assert block.domain_label == "Customer360"

    def test_unknown_schema_defaults_to_customer360(self):
        block = get_scorecard_block("no-such-schema")
        assert block.domain_label == "Customer360"

    def test_blocks_implement_the_protocol(self):
        assert isinstance(Customer360ScorecardBlock(), ScorecardBlock)
        assert isinstance(FinancialScorecardBlock(), ScorecardBlock)

    def test_render_detail_html_is_empty_in_v1(self):
        for schema in ("customer360", "financial"):
            assert get_scorecard_block(schema).render_detail_html(None) == ""

    def test_register_allows_third_party_blocks(self):
        class _StubBlock:
            schema_name = "stub"
            domain_label = "Stub"

            def render_detail_html(self, metrics):
                return "<p>stub</p>"

        register_scorecard_block(_StubBlock())
        try:
            assert get_scorecard_block("stub").domain_label == "Stub"
        finally:
            # Clean up so we don't leak into other tests.
            from lakebench.reports.scorecard import _REGISTRY

            _REGISTRY.pop("stub", None)


class TestScorecardBlockInGeneratedReport:
    """The run-context banner substitutes the block's domain_label."""

    def _minimal_metrics(self, workload_schema=None):
        from unittest.mock import MagicMock

        cs = {
            "scale": 1.0,
            "catalog": "hive",
            "table_format": "iceberg",
            "pipeline_engine": "spark",
            "query_engine": "trino",
        }
        if workload_schema is not None:
            cs["workload_schema"] = workload_schema

        metrics = MagicMock()
        metrics.config_snapshot = cs
        metrics.total_elapsed_seconds = 90
        metrics.pipeline_benchmark = None
        metrics.streaming = None
        return metrics

    def _generator(self):
        from lakebench.reports.generator import ReportGenerator

        return ReportGenerator()

    def test_customer360_banner(self):
        gen = self._generator()
        html = gen._generate_run_context(self._minimal_metrics("customer360"))
        assert "Customer360 at scale" in html

    def test_financial_banner(self):
        gen = self._generator()
        html = gen._generate_run_context(self._minimal_metrics("financial"))
        assert "Financial" in html
        assert "Customer360 at scale" not in html

    def test_missing_schema_falls_back_to_customer360(self):
        gen = self._generator()
        html = gen._generate_run_context(self._minimal_metrics(workload_schema=None))
        assert "Customer360 at scale" in html


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
