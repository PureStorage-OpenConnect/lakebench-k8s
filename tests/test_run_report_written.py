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

from lakebench.metrics import PipelineMetrics
from lakebench.metrics.collector import StreamingJobMetrics


def _metrics(run_id="20260925-000000-abcdef", **kw):
    return PipelineMetrics(
        run_id=run_id,
        deployment_name="p14",
        start_time=datetime(2026, 9, 25, 0, 0, 0),
        end_time=datetime(2026, 9, 25, 0, 30, 0),
        **kw,
    )


_SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "cli"


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
