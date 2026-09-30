"""Both c360 gold adapters (Iceberg + Delta) must push stage metrics via
log_job_metrics, so gold reaches the live Pushgateway on either format (Gate 3
observability). A raw hand-rolled '=== JOB METRICS ===' block would write
metrics.json but skip the push, silently flat-lining gold on the dashboard for
one format -- the asymmetry the gold-lane review caught.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"


@pytest.mark.parametrize("script", ["gold_finalize.py", "gold_finalize_delta.py"])
def test_c360_gold_adapter_pushes_via_log_job_metrics(script):
    src = (_SCRIPTS / script).read_text()
    assert "log_job_metrics(" in src, (
        f"{script} does not call log_job_metrics (no Pushgateway push)"
    )
    # The raw block emitted estimated_rows directly; log_job_metrics replaces it.
    assert 'log(f"estimated_rows:' not in src, (
        f"{script} still emits a raw JOB METRICS block instead of log_job_metrics"
    )
