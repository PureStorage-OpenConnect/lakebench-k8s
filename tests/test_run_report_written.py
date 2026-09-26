"""P1.4: every run writes report.html, whatever its schema, mode or exit code.

`run` saved metrics.json and printed "Full report: lakebench report"; the
report existed only when a runner called `lakebench report` afterwards, so
AML batch s10/s100 and c360 continuous runs had metrics and no report.
AML continuous reports also carried no per-rule table; they now show the
per-rule counts gold-refresh measured, and recall whenever the run carries
scoring data.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pytest

from lakebench.cli._helpers import write_run_report
from lakebench.metrics import MetricsStorage, PipelineMetrics
from lakebench.metrics.collector import StreamingJobMetrics


def _metrics(run_id="20260925-000000-abcdef", **kw):
    return PipelineMetrics(
        run_id=run_id,
        deployment_name="p14",
        start_time=datetime(2026, 9, 25, 0, 0, 0),
        end_time=datetime(2026, 9, 25, 0, 30, 0),
        **kw,
    )


@pytest.mark.parametrize("success", [True, False])
def test_write_run_report_writes_next_to_metrics(tmp_path, success):
    storage = MetricsStorage(tmp_path)
    m = _metrics(success=success)
    storage.save_run(m)
    path = write_run_report(storage, m.run_id)
    assert path == tmp_path / f"run-{m.run_id}" / "report.html"
    assert path.exists() and "p14" in path.read_text()


def test_write_run_report_never_raises(tmp_path, capsys):
    storage = MetricsStorage(tmp_path)
    assert write_run_report(storage, "no-such-run") is None
    assert "Could not write report.html" in " ".join(capsys.readouterr().out.split())


_SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "cli"


@pytest.mark.parametrize(
    ("module", "anchor"),
    [
        ("_run.py", "metrics_path = metrics_storage.save_run(run_metrics)"),
        ("_run.py", "metrics_path = _save_local_metrics("),
        ("_sustained.py", "metrics_path = metrics_storage.save_run(run_metrics)"),
    ],
)
def test_every_metrics_save_is_followed_by_a_report(module, anchor):
    """Batch (cluster and local) and continuous save paths run in the
    finally block, so a report follows every saved run, rc=1 included."""
    lines = (_SRC / module).read_text().splitlines()
    hits = [i for i, line in enumerate(lines) if anchor in line]
    assert hits, anchor
    for i in hits:
        window = "\n".join(lines[i : i + 14])
        assert "write_run_report(metrics_storage, run_id)" in window, (module, i)


def _aml_continuous(scoring=None):
    gold = StreamingJobMetrics(
        job_name="lakebench-gold-refresh",
        job_type="gold-refresh",
        ttd_by_rule={
            "W2_structuring": {"alerts": 17993, "p50_seconds": 200.0},
            "W17_layering_chain": {"alerts": 215296, "p50_seconds": 470.0},
        },
        success=True,
    )
    return _metrics(
        success=True,
        streaming=[gold],
        config_snapshot={"workload": {"schema": "financial"}, "pipeline_mode": "sustained"},
        financial_scoring=scoring,
    )


def _scorecard_text(m):
    from lakebench.reports.scorecard import get_scorecard_block

    html = get_scorecard_block("financial").render_detail_html(m)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def test_aml_continuous_renders_per_rule_alerts_without_scoring():
    text = _scorecard_text(_aml_continuous())
    assert "Detection Scorecard" in text
    assert "W2_structuring micro_structuring 17,993" in text
    assert "W17_layering_chain stack 215,296" in text
    # A rule missing from the histogram is unknown, not zero.
    assert re.search(r"W7_cross_border_high_risk corridor_high_risk - .* no data", text)
    assert "New alert versions" in text and "not gold.alerts rows" in text
    assert "Recall is not scored in continuous mode" in text


def test_aml_continuous_renders_per_rule_recall_from_scoring():
    scoring = {
        "total_alerts": 233289,
        "fp_rate": 0.5,
        "typologies": [
            {
                "typology_type": "micro_structuring",
                "recall": 0.98,
                "incidental_recall": 0.99,
                "detection_status": "scored",
            },
            {
                "typology_type": "stack",
                "recall": 0.75,
                "incidental_recall": 0.8,
                "detection_status": "scored",
            },
        ],
    }
    text = _scorecard_text(_aml_continuous(scoring))
    assert "W2_structuring micro_structuring 17,993 98.0%" in text
    assert "W17_layering_chain stack 215,296 75.0%" in text
    assert "Recall is not scored" not in text


def test_batch_rendering_is_unchanged_by_the_continuous_fallback():
    """A batch run without per-rule counts keeps its '0' cells."""
    from lakebench.metrics import JobMetrics

    job = JobMetrics(
        job_name="gold",
        job_type="gold-finalize",
        start_time=datetime(2026, 9, 25),
        end_time=datetime(2026, 9, 25),
        elapsed_seconds=1.0,
        success=True,
    )
    job.alerts_by_rule = {"W2_structuring": 5}
    text = _scorecard_text(_metrics(success=True, jobs=[job]))
    assert "W2_structuring micro_structuring 5" in text
    assert "Continuous run" not in text


def test_failed_continuous_run_writes_its_report(monkeypatch, tmp_path):
    """A continuous run that exits 1 (here: its reset preflight fails)
    saves metrics in the finally block, and then writes its report."""
    from lakebench.cli import _sustained
    from tests.test_c360_continuous_reset import _c360_cfg, _drive_sustained

    saved, reported = [], []
    monkeypatch.setattr(
        "lakebench.metrics.MetricsStorage.save_run",
        lambda self, m: saved.append(m.run_id) or tmp_path / "metrics.json",
    )
    monkeypatch.setattr(
        _sustained, "write_run_report", lambda storage, run_id: reported.append(run_id)
    )
    _drive_sustained(monkeypatch, tmp_path, _c360_cfg(), reset_ok=False)
    assert saved and reported == saved
