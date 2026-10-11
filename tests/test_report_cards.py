"""Shared measurement formatter, confidence chip and the qualifiers a score
card must carry beside a number."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from lakebench.metrics import MetricsStorage, PipelineMetrics
from lakebench.reports.formatter import (
    confidence_chip,
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

    @pytest.mark.parametrize(
        ("n_runs", "tag"),
        [(None, None), (0, None), (1, "n=1"), (5, "n=5")],
    )
    def test_n_runs_tag(self, n_runs, tag):
        out = format_measurement("42.0", "QpH", n_runs=n_runs)
        if tag is None:
            assert "n=" not in out
        else:
            assert tag in out

    @pytest.mark.parametrize(
        ("reason", "name"),
        [
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
            ("rule R7 skipped: over cap", "rule R7 cap"),
        ],
    )
    def test_capped_value_carries_bounded_by_and_cap_name(self, reason, name):
        # The reader gets the cap constant next to the number, with the full
        # reason in the tooltip.
        out = format_measurement("1,200", "rows/s", caps_bound=[reason])
        assert "BOUNDED BY" in out
        assert name in out
        assert reason in out


# ---------------------------------------------------------------------------
# confidence_chip: one label per state (single_run, replicated_n=N, high).
# ---------------------------------------------------------------------------


class TestConfidenceChip:
    @pytest.mark.parametrize(
        ("n", "spread", "label"),
        [
            (1, None, "single_run"),
            (None, None, "single_run"),
            (0, None, "single_run"),
            (3, None, "replicated_n=3"),
            (4, None, "replicated_n=4"),
            (5, 0.05, "high"),
            (6, 0.09, "high"),
            # a high-confidence claim needs measured spread below 10%
            (5, None, "replicated_n=5"),
            (5, 0.15, "replicated_n=5"),
        ],
    )
    def test_chip_label(self, n, spread, label):
        assert confidence_chip(n, spread=spread) == label


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

    def test_qph_card_labels_iterations_as_samples_not_runs(self, tmp_path):
        # Five iterations inside one run are samples per query, not five runs.
        storage, m = _metrics_with_experiment(tmp_path, n_iterations=5)
        html = ReportGenerator(storage.metrics_dir)._generate_qph_card(m)
        assert "5 samples/query" in html
        assert "n=5" not in html


class TestConfidenceChipOnBadge:
    @pytest.mark.parametrize(
        ("n_runs", "n_iterations", "present", "absent"),
        [
            (1, None, "single_run", None),
            # invariant 7: fewer than 3 independent runs never claim replication
            (2, None, "single_run", "replicated_n=2"),
            (3, None, "replicated_n=3", None),
            # in-stream rounds within one continuous run are not replication
            (1, 5, "single_run", "replicated_n=5"),
        ],
    )
    def test_badge_chip(self, tmp_path, n_runs, n_iterations, present, absent):
        kw = {"n_iterations": n_iterations} if n_iterations is not None else {}
        storage, m = _metrics_with_experiment(tmp_path, n_runs=n_runs, **kw)
        html = ReportGenerator(storage.metrics_dir)._generate_html(m, None)
        assert present in html
        if absent:
            assert absent not in html
