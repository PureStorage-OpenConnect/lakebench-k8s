"""A3 + A6: shared measurement formatter, WCAG delta tokens, confidence chip,
BOUNDED BY tooltip, "Read this first" panel.

Tests here verify the qualifiers a score card must carry (invariants 5, 6, 7),
the WCAG-safe pass/fail encoding in the compare table (no colour-only cues),
and the header panel that names what a reader should know before reading a
single card. Every test is silent-corruption-shaped: a card that renders a
capped figure as bare headline evidence, or a compare table that says only
"[red]" without a text token, is exactly the class of defect these guard.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from lakebench.metrics import MetricsStorage, PipelineMetrics
from lakebench.reports.formatter import (
    ALL_DELTA_TOKENS,
    DELTA_TOKEN_A_FASTER,
    DELTA_TOKEN_B_FASTER,
    DELTA_TOKEN_CAPPED,
    DELTA_TOKEN_OVERLAP,
    DELTA_TOKEN_WITHHELD,
    caps_bound_from,
    confidence_chip,
    confidence_chip_html,
    delta_token,
    format_measurement,
)
from lakebench.reports.generator import ReportGenerator

# ---------------------------------------------------------------------------
# format_measurement: qualifier tags line up with (caps_bound, n_runs,
# support_state), and a plain call renders just the value.
# ---------------------------------------------------------------------------


class TestFormatMeasurement:
    def test_plain_value_no_tags(self):
        out = format_measurement("12,345", "rows/s")
        assert "BOUNDED BY" not in out
        assert "n=" not in out
        assert "12,345" in out
        assert "rows/s" in out

    def test_capped_value_carries_bounded_by_and_cap_name(self):
        out = format_measurement(
            "1,200",
            "rows/s",
            caps_bound=["bronze-verify: executor cap 28 (scale asks for 40)"],
        )
        assert "BOUNDED BY" in out
        # The cap name (invariant 6): reader sees the constant, not just
        # a prose reason.
        assert "_MAX_EXECUTORS_SAFE=28" in out
        # The full reason is in the tooltip.
        assert "scale asks for 40" in out

    def test_n_runs_one_labels_single_sample(self):
        out = format_measurement("42.0", "QpH", n_runs=1)
        assert "n=1" in out
        assert "n=2" not in out

    def test_n_runs_many_labels_repeated(self):
        out = format_measurement("42.0", "QpH", n_runs=5)
        assert "n=5" in out

    def test_n_runs_none_or_zero_adds_no_tag(self):
        assert "n=" not in format_measurement("42.0", "QpH", n_runs=None)
        assert "n=" not in format_measurement("42.0", "QpH", n_runs=0)

    def test_support_state_renders_pill(self):
        for state, expect in (
            ("supported", "supported"),
            ("unverified", "unverified"),
            ("unsupported", "unsupported"),
        ):
            out = format_measurement("42", "QpH", support_state=state)
            assert expect in out

    def test_support_state_unknown_omitted_by_caller(self):
        # None ("unknown") does not add a pill; the card stays clean.
        out = format_measurement("42", "QpH", support_state=None)
        assert "qual-support" not in out

    def test_html_escape_applied_to_value_and_cap_reason(self):
        out = format_measurement("<script>&", "QpH")
        assert "<script>" not in out
        assert "&lt;script&gt;" in out or "&lt;script&gt;&amp;" in out

    def test_cap_names_cover_known_bound_shapes(self):
        # Every known bound-line shape resolves to a cap name; the reader
        # gets the constant next to the number for each.
        cases = [
            ("bronze-verify: executor cap 28 (scale asks for 40)", "_MAX_EXECUTORS_SAFE=28"),
            (
                "bronze-ingest: concurrent executor budget granted 6 of 12",
                "concurrent executor budget",
            ),
            (
                "TM max_alerts_per_customer (50): 1200 alerts over capacity",
                "tm_max_alerts_per_customer",
            ),
            ("auto-sizing: silver dropped 4 executors", "auto-sizing cut"),
            (
                "pre-benchmark maintenance stopped on its time budget",
                "pre-benchmark maintenance budget",
            ),
            (
                "rule R7 skipped: over cap",
                "rule R7 cap",
            ),
        ]
        for reason, name in cases:
            out = format_measurement("1", "", caps_bound=[reason])
            assert name in out, f"{reason} -> {name} missing from {out}"


# ---------------------------------------------------------------------------
# confidence_chip: one label per state (single_run, replicated_n=N, high).
# ---------------------------------------------------------------------------


class TestConfidenceChip:
    def test_single_run_when_n_is_one_or_missing(self):
        assert confidence_chip(1) == "single_run"
        assert confidence_chip(None) == "single_run"
        assert confidence_chip(0) == "single_run"

    def test_replicated_when_three_or_more(self):
        assert confidence_chip(3) == "replicated_n=3"
        assert confidence_chip(4) == "replicated_n=4"

    def test_high_when_five_and_low_spread(self):
        assert confidence_chip(5, spread=0.05) == "high"
        assert confidence_chip(6, spread=0.09) == "high"

    def test_high_needs_spread_below_10pct(self):
        # A high-confidence claim needs measured spread; without it, no
        # such claim is made.
        assert confidence_chip(5, spread=None) == "replicated_n=5"
        assert confidence_chip(5, spread=0.15) == "replicated_n=5"

    def test_chip_html_carries_label(self):
        assert "single_run" in confidence_chip_html(1)
        assert "replicated_n=3" in confidence_chip_html(3)
        assert "high" in confidence_chip_html(5, spread=0.05)


# ---------------------------------------------------------------------------
# delta_token: WCAG-safe compare tokens. Every state has a distinct token,
# and A_faster / B_faster respect direction sensitivity of the score.
# ---------------------------------------------------------------------------


class TestDeltaToken:
    def test_higher_is_better_positive_pct_is_b_faster(self):
        assert (
            delta_token(higher_is_better=True, pct=10.0, within_noise=False) == DELTA_TOKEN_B_FASTER
        )

    def test_higher_is_better_negative_pct_is_a_faster(self):
        assert (
            delta_token(higher_is_better=True, pct=-10.0, within_noise=False)
            == DELTA_TOKEN_A_FASTER
        )

    def test_lower_is_better_positive_pct_is_a_faster(self):
        # B took longer; A is the faster one.
        assert (
            delta_token(higher_is_better=False, pct=10.0, within_noise=False)
            == DELTA_TOKEN_A_FASTER
        )

    def test_within_noise_is_overlap(self):
        assert delta_token(higher_is_better=True, pct=1.0, within_noise=True) == DELTA_TOKEN_OVERLAP

    def test_capped_takes_precedence(self):
        assert (
            delta_token(higher_is_better=True, pct=50.0, within_noise=False, capped=True)
            == DELTA_TOKEN_CAPPED
        )

    def test_withheld_takes_precedence_over_capped(self):
        assert (
            delta_token(
                higher_is_better=True,
                pct=50.0,
                within_noise=False,
                capped=True,
                withheld=True,
            )
            == DELTA_TOKEN_WITHHELD
        )

    def test_all_tokens_are_distinct_strings(self):
        assert len(set(ALL_DELTA_TOKENS)) == 5


# ---------------------------------------------------------------------------
# HTML report end-to-end: a capped run's report carries BOUNDED BY on the
# right card; the "Read this first" panel is present with all five fields;
# the provenance label is at the top; the confidence chip sits next to the
# badge.
# ---------------------------------------------------------------------------


def _metrics_with_experiment(
    tmp_path: Path,
    *,
    bound: list[str] | None = None,
    qph: float = 47.5,
    n_iterations: int = 3,
    n_runs: int = 1,
    corpus_role: str = "evaluation",
) -> tuple[MetricsStorage, PipelineMetrics]:
    from lakebench.metrics.collector import BenchmarkMetrics

    metrics_dir = tmp_path / "runs"
    metrics_dir.mkdir(parents=True)
    storage = MetricsStorage(metrics_dir)
    m = PipelineMetrics(
        run_id="20260928-a3a6",
        deployment_name="lane-a3a6",
        start_time=datetime(2026, 9, 28, 12, 0, 0),
        end_time=datetime(2026, 9, 28, 12, 30, 0),
        success=True,
    )
    m.benchmark = BenchmarkMetrics(
        mode="power",
        cache="cold",
        scale=1.0,
        qph=qph,
        total_seconds=60.0,
        queries=[{"name": "q1", "success": True}],
        iterations=n_iterations,
        streams=1,
    )
    # An experiment block with a corpus, an identity and (optionally) a
    # bound-limit entry, so the panel and the cards read live data.
    m.experiment = {
        "schema": "exp1",
        "workload": {"name": "financial", "version": "aml-1"},
        "corpus": {
            "id": "abc123abc123abc1",
            "seed": 42,
            "corpus_role": corpus_role,
            "scale": 1.0,
        },
        "architecture": {"query_access_path": "catalog"},
        "limits": {
            "bound": list(bound or []),
            "bound_kinds": [],
            "benchmark_iterations": n_iterations,
        },
        "mode": "batch",
        "effective_maintenance": {"id": "m-none"},
        "maintenance_settings": {},
        "system": "cluster",
        "support": {"state": "unverified", "basis": "test-fixture"},
        "results": {"fingerprints": {"q1": "aa" * 16}},
        # repetitions.runs counts INDEPENDENT runs behind the record; the
        # confidence chip must never claim replication from in-stream
        # rounds (benchmark_iterations), which are within one run.
        "repetitions": {"runs": n_runs},
    }
    m.config_snapshot = {
        "scale": 1.0,
        "catalog": "hive",
        "table_format": "iceberg",
        "pipeline_engine": "spark",
        "query_engine": "trino",
    }
    return storage, m


class TestReportContainsQualifiers:
    def test_capped_run_has_bounded_by_on_qph_card(self, tmp_path):
        storage, m = _metrics_with_experiment(
            tmp_path,
            bound=["bronze-verify: executor cap 28 (scale asks for 40)"],
        )
        # Bypass experiment_block()'s live rebuild by feeding the stored
        # block through caps_bound_from directly, and rendering the QpH
        # card path (batch, single benchmark).
        html = ReportGenerator(storage.metrics_dir)._generate_qph_card(m)
        assert "BOUNDED BY" in html
        assert "_MAX_EXECUTORS_SAFE=28" in html

    def test_uncapped_run_has_no_bounded_tag(self, tmp_path):
        storage, m = _metrics_with_experiment(tmp_path, bound=[])
        html = ReportGenerator(storage.metrics_dir)._generate_qph_card(m)
        assert "BOUNDED BY" not in html

    def test_qph_card_carries_n_from_iterations(self, tmp_path):
        storage, m = _metrics_with_experiment(tmp_path, n_iterations=5)
        html = ReportGenerator(storage.metrics_dir)._generate_qph_card(m)
        assert "n=5" in html


class TestReadFirstPanel:
    def test_panel_has_all_five_fields(self, tmp_path):
        storage, m = _metrics_with_experiment(tmp_path)
        html = ReportGenerator(storage.metrics_dir)._generate_read_first_panel(
            m,
            passed=True,
            warnings=[],
            fail_reasons=[],
            n_runs=3,
        )
        assert "Read this first" in html
        # Verdict, headline, corpus label, n, limits present, record digest.
        assert "Verdict:" in html
        assert "Headline:" in html
        assert "Corpus:" in html
        assert ">n:</span>" in html or "n:</span>" in html
        assert "Limits present:" in html
        assert "Record digest:" in html

    def test_corpus_label_includes_role_and_id_prefix(self, tmp_path):
        storage, m = _metrics_with_experiment(tmp_path, corpus_role="evaluation")
        html = ReportGenerator(storage.metrics_dir)._generate_read_first_panel(
            m,
            passed=True,
            warnings=[],
            fail_reasons=[],
            n_runs=1,
        )
        assert "evaluation" in html
        assert "abc123abc123" in html  # first 12 chars of the corpus id

    def test_provenance_label_at_top(self, tmp_path):
        storage, m = _metrics_with_experiment(tmp_path)
        gen = ReportGenerator(storage.metrics_dir)
        html = gen._generate_html(m, platform_metrics=None)
        # The provenance label sits above the header, so it appears before
        # the <h1> tag in the document flow.
        h1 = html.index("<h1>")
        prov = html.index("internal benchmark, single-owner recorded")
        assert prov < h1

    def test_read_first_panel_present_in_full_html(self, tmp_path):
        storage, m = _metrics_with_experiment(tmp_path)
        gen = ReportGenerator(storage.metrics_dir)
        html = gen._generate_html(m, platform_metrics=None)
        assert "Read this first" in html
        assert "Record digest:" in html


class TestConfidenceChipOnBadge:
    def test_single_run_chip_when_n_is_one(self, tmp_path):
        storage, m = _metrics_with_experiment(tmp_path, n_runs=1)
        html = ReportGenerator(storage.metrics_dir)._generate_html(m, None)
        assert "single_run" in html

    def test_replicated_chip_when_n_is_three(self, tmp_path):
        storage, m = _metrics_with_experiment(tmp_path, n_runs=3)
        html = ReportGenerator(storage.metrics_dir)._generate_html(m, None)
        assert "replicated_n=3" in html

    def test_in_stream_rounds_do_not_count_as_replication(self, tmp_path):
        # Invariant 7: n_iterations is in-stream benchmark rounds within one
        # continuous run and must NOT be counted as replication. A sustained
        # run with 5 rounds gets single_run on the badge until independent
        # runs are recorded.
        storage, m = _metrics_with_experiment(tmp_path, n_iterations=5, n_runs=1)
        html = ReportGenerator(storage.metrics_dir)._generate_html(m, None)
        assert "single_run" in html
        assert "replicated_n=5" not in html

    def test_two_runs_still_reads_single_run(self, tmp_path):
        # Invariant 7: never claim replication from fewer than 3 independent
        # runs. n=2 renders as single_run, not replicated_n=2.
        storage, m = _metrics_with_experiment(tmp_path, n_runs=2)
        html = ReportGenerator(storage.metrics_dir)._generate_html(m, None)
        assert "single_run" in html
        assert "replicated_n=2" not in html

    def test_high_chip_needs_five_and_low_spread(self):
        # Direct helper test (the full HTML path does not have live spread
        # today, so the chip's "high" state is verified at the API).
        assert "high" in confidence_chip_html(5, spread=0.05)


# ---------------------------------------------------------------------------
# Compare CLI: WCAG 1.4.1 -- nothing is carried by colour alone. compare
# names no winner, so it prints no winner token; what a row may be read as
# is its assessment, in text.
# ---------------------------------------------------------------------------


def _strip_rich_markup(text: str) -> str:
    """Remove Rich colour tags: WCAG check reads the plain text stream."""
    return re.sub(r"\[/?[a-zA-Z0-9_ ]+\]", "", text)


def _capture_compare_print(comparison: dict) -> str:
    from io import StringIO

    from rich.console import Console

    from lakebench.cli import _compare as compare_mod

    buf = StringIO()
    fake_console = Console(file=buf, force_terminal=False, width=250)
    with mock.patch.object(compare_mod, "console", fake_console):
        compare_mod._print_table(comparison)
    return buf.getvalue()


class TestCompareTextNotColour:
    def _pair(self, qph_a: float, qph_b: float, *, results_differ: bool = False) -> dict:
        import copy

        from lakebench.metrics.compare import compare_records
        from tests.fixtures import stored_records as sr

        a = sr.load_record("5105a0")
        b = copy.deepcopy(a)
        b["run_id"] = "20261001-000000-c0c0c0"
        a["pipeline_benchmark"]["scores"] = {"composite_qph": qph_a}
        b["pipeline_benchmark"]["scores"] = {"composite_qph": qph_b}
        if results_differ:
            fps = b["experiment"]["results"]["fingerprints"]
            first = next(iter(fps))
            fps[first] = "different"
        return compare_records([a], [b])

    def test_no_winner_token_on_a_like_for_like_pair(self):
        out = _strip_rich_markup(_capture_compare_print(self._pair(100.0, 120.0)))
        assert DELTA_TOKEN_A_FASTER not in out and DELTA_TOKEN_B_FASTER not in out
        assert "not_assessed" in out and "+20.00%" in out

    def test_withheld_in_text_when_not_comparable(self):
        out = _strip_rich_markup(
            _capture_compare_print(self._pair(100.0, 120.0, results_differ=True))
        )
        assert "NOT COMPARABLE" in out and "withheld" in out
        assert "+20.00%" not in out


# ---------------------------------------------------------------------------
# caps_bound_from + n_runs_of: pull-through helpers from PipelineMetrics.
# ---------------------------------------------------------------------------


class TestExperimentPullThrough:
    def test_caps_bound_from_experiment_block(self, tmp_path):
        _, m = _metrics_with_experiment(
            tmp_path,
            bound=[
                "bronze-verify: executor cap 28 (scale asks for 40)",
                "auto-sizing: silver dropped 2 executors",
            ],
        )
        bounds = caps_bound_from(m)
        assert len(bounds) == 2
        assert any("executor cap" in b for b in bounds)

    def test_caps_bound_empty_when_no_block(self):
        assert caps_bound_from(object()) == []


# ---------------------------------------------------------------------------
# Lint smoke: the lint script itself catches a regression.
# ---------------------------------------------------------------------------


def _load_lint_module():
    """Load the standalone lint script under a stable module name."""
    import importlib.util

    script_path = Path(__file__).resolve().parent.parent / "scripts" / "lint_capped_bare_numbers.py"
    spec = importlib.util.spec_from_file_location("lint_capped_bare_numbers", script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_lint_catches_bare_headline(tmp_path):
    target = tmp_path / "bad.py"
    target.write_text('html = f"""<div class="card-value">{qph:,.1f}</div>"""')
    lint_mod = _load_lint_module()
    assert lint_mod.scan(target), "lint should flag a bare {qph:...} in card-value"


def test_lint_passes_on_current_generator():
    lint_mod = _load_lint_module()
    target = (
        Path(__file__).resolve().parent.parent / "src" / "lakebench" / "reports" / "generator.py"
    )
    assert lint_mod.scan(target) == [], "generator.py must not carry bare headlines"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
