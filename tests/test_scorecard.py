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

    def test_render_detail_html_none_is_empty(self):
        # None metrics short-circuits to "" for every block. (The financial
        # block DOES render a table for real metrics -- see
        # TestFinancialDetailScorecard.)
        for schema in ("customer360", "financial"):
            assert get_scorecard_block(schema).render_detail_html(None) == ""

    def test_customer360_detail_is_empty_even_with_metrics(self):
        from types import SimpleNamespace

        m = SimpleNamespace(jobs=[], financial_scoring=None, config_snapshot={})
        assert Customer360ScorecardBlock().render_detail_html(m) == ""

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


class TestFinancialDetailScorecard:
    """FinancialScorecardBlock.render_detail_html renders the per-rule
    detection table (LB-123), and a skipped rule reads "not run", never 0%."""

    def _job(self, alerts_by_rule=None, rules_skipped=None, job_type="gold-finalize"):
        from types import SimpleNamespace

        return SimpleNamespace(
            job_type=job_type,
            alerts_by_rule=alerts_by_rule or {},
            rules_skipped=rules_skipped or {},
        )

    def _metrics(self, jobs, financial_scoring=None):
        from types import SimpleNamespace

        return SimpleNamespace(jobs=jobs, financial_scoring=financial_scoring, config_snapshot={})

    def _row_for(self, html, rule):
        """Return the <tr>...</tr> substring whose first cell is `rule`, so
        assertions bind to that rule's row rather than the whole document."""
        import re

        for m in re.finditer(r"<tr>.*?</tr>", html, re.DOTALL):
            row = m.group(0)
            if f"<td>{rule}</td>" in row:
                return row
        return ""

    def test_none_renders_nothing(self):
        assert FinancialScorecardBlock().render_detail_html(None) == ""

    def test_empty_metrics_renders_nothing(self):
        block = FinancialScorecardBlock()
        assert block.render_detail_html(self._metrics(jobs=[self._job()])) == ""

    def test_renders_recall_and_alert_counts(self):
        block = FinancialScorecardBlock()
        job = self._job(alerts_by_rule={"W2_structuring": 12, "W3_round_tripping": 5})
        scoring = {
            "typologies": [
                {
                    "typology_type": "micro_structuring",
                    "recall": 0.83,
                    "instance_count": 6,
                    "detection_status": "scored",
                }
            ],
            "total_alerts": 17,
            "fp_alerts": 2,
            "fp_rate": 0.1176,
            "run_id": "r1",
        }
        html = block.render_detail_html(self._metrics(jobs=[job], financial_scoring=scoring))
        assert "Detection Scorecard" in html
        # Recall + alert count must bind to W2's row, not just appear somewhere.
        w2_row = self._row_for(html, "W2_structuring")
        assert "micro_structuring" in w2_row
        assert "83.0%" in w2_row
        assert "<td>12</td>" in w2_row
        assert "Overall off-target rate" in html

    def test_recall_column_labelled_uncalibrated(self):
        # v1.6 publishes no held-out Level-2 result: the recall a run shows is
        # in-sample, and the column says so.
        block = FinancialScorecardBlock()
        job = self._job(alerts_by_rule={"W2_structuring": 1})
        scoring = {"typologies": [], "total_alerts": 1, "run_id": "r1"}
        html = block.render_detail_html(self._metrics(jobs=[job], financial_scoring=scoring))
        assert ">Recall (uncalibrated, in-sample)</th>" in html

    def test_multi_cycle_alert_counts_last_cycle_wins(self):
        # gold_finalize re-detects over the whole cumulative silver each cycle
        # (DELETE-then-INSERT), so each cycle's count is CUMULATIVE and the
        # final gold.alerts holds the last cycle's total. The table must show
        # the LAST gold-finalize job's count, not the sum (LB-123 re-review
        # F2: summing would triple-count and contradict the footer).
        block = FinancialScorecardBlock()
        jobs = [
            self._job(alerts_by_rule={"W2_structuring": 40}),
            self._job(alerts_by_rule={"W2_structuring": 80}),
            self._job(alerts_by_rule={"W2_structuring": 120}),
        ]
        html = block.render_detail_html(self._metrics(jobs=jobs))
        w2_row = self._row_for(html, "W2_structuring")
        assert "<td>120</td>" in w2_row  # last cycle's cumulative total
        assert "<td>240</td>" not in w2_row  # never the sum

    def test_non_gold_jobs_ignored_for_detection(self):
        # A bronze/silver job carries no detection dicts; only gold-finalize
        # feeds the scorecard.
        block = FinancialScorecardBlock()
        jobs = [
            self._job(job_type="silver-build"),
            self._job(job_type="gold-finalize", alerts_by_rule={"W2_structuring": 7}),
        ]
        html = block.render_detail_html(self._metrics(jobs=jobs))
        assert "<td>7</td>" in self._row_for(html, "W2_structuring")

    def test_unknown_rule_not_dropped(self):
        # A rule that emitted alerts but is absent from RULE_TARGETS must still
        # appear (LB-123 review F3).
        block = FinancialScorecardBlock()
        job = self._job(alerts_by_rule={"W99_experimental": 7})
        html = block.render_detail_html(self._metrics(jobs=[job]))
        assert "W99_experimental" in html

    def test_malformed_scoring_does_not_crash(self):
        # A hand-edited / partially-written recall.json never takes down the
        # whole report, and the section says it could not render (not "").
        block = FinancialScorecardBlock()
        job = self._job(alerts_by_rule={"W2_structuring": 3})
        bad = {"typologies": [{"typology_type": "micro_structuring", "recall": "oops"}]}
        # float("oops") raises inside the body; the outer guard shows a notice.
        html = block.render_detail_html(self._metrics(jobs=[job], financial_scoring=bad))
        assert "AML results could not be rendered: ValueError:" in html

    def test_skipped_rule_reads_not_run_not_zero(self):
        block = FinancialScorecardBlock()
        job = self._job(rules_skipped={"W1_connected_components": "vertex-cap"})
        html = block.render_detail_html(self._metrics(jobs=[job]))
        w1_row = self._row_for(html, "W1_connected_components")
        assert w1_row  # the skipped rule still appears
        assert "not run" in w1_row
        assert "vertex-cap" in w1_row
        # A skip must never be rendered as any % recall (LB-119 invariant).
        assert "%" not in w1_row

    def test_rule_skipped_in_scoring_not_zero(self):
        block = FinancialScorecardBlock()
        job = self._job(alerts_by_rule={"W2_structuring": 4})
        scoring = {
            "typologies": [
                {
                    "typology_type": "gather_scatter",
                    "recall": None,
                    "instance_count": 3,
                    "detection_status": "rule_skipped",
                }
            ],
            "total_alerts": 4,
            "fp_alerts": 1,
            "fp_rate": 0.25,
            "run_id": "r1",
        }
        html = block.render_detail_html(self._metrics(jobs=[job], financial_scoring=scoring))
        # gather_scatter is W1's typology; a rule_skipped status reads "not run"
        # in W1's row, with no numeric recall there (even though the footer's
        # fp_rate legitimately renders a percentage elsewhere).
        w1_row = self._row_for(html, "W1_connected_components")
        assert "not run" in w1_row
        assert "%" not in w1_row

    def test_screening_rules_have_planted_targets(self):
        block = FinancialScorecardBlock()
        job = self._job(alerts_by_rule={"W5_sanctions_match": 3})
        html = block.render_detail_html(self._metrics(jobs=[job]))
        assert "W5_sanctions_match" in html and "sanctions_match" in html
        assert "pep_match" in html
        assert "attribute" not in html


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestFinancialScorecardEdgeRows:
    """Rows the coverage floor found unexercised: continuous alert counts from
    gold-refresh time-to-detect, and a crashed rule in batch."""

    def test_continuous_counts_come_from_gold_refresh(self):
        from types import SimpleNamespace

        sm = SimpleNamespace(
            job_type="gold-refresh",
            ttd_by_rule={"W2_structuring": {"alerts": 12}, "W3_round_tripping": {"alerts": "x"}},
        )
        m = SimpleNamespace(jobs=[], streaming=[sm], financial_scoring=None, config_snapshot={})
        html = FinancialScorecardBlock().render_detail_html(m)
        assert "W2_structuring" in html and "12" in html

    def test_crashed_rule_reads_error_not_zero(self):
        from types import SimpleNamespace

        job = SimpleNamespace(
            job_type="gold-finalize",
            alerts_by_rule={"W2_structuring": 0},
            rules_skipped={},
            rule_errors={"W2_structuring": "boom"},
        )
        m = SimpleNamespace(jobs=[job], streaming=[], financial_scoring=None, config_snapshot={})
        html = FinancialScorecardBlock().render_detail_html(m)
        assert "error" in html
