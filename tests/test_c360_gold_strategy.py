"""Customer 360 gold strategy: never silently incremental (V16-3).

The run-time half: ``config/c360_run.gold_override_problem`` refuses
``spark.lb.gold.strategy=incremental`` (and a value that names no strategy)
when a data-changing command loads the config, so ``run`` makes no cluster
call and ``deploy`` builds nothing; the gold scripts lose the auto
switch to incremental and the dead ``LB_GOLD_STRATEGY`` fallback, and the
strategy that ran is recorded per job. The Spark-tier half, a repeat over
changed silver, is ``tests/spark/test_gold_repeat_reaggregates_spark.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lakebench.config import c360_run
from tests.fixtures.run_args_helpers import (
    CONFIG,
    no_cluster,  # noqa: F401 (fixture
)

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "src/lakebench/spark/scripts"


def test_incremental_override_is_refused():
    for value in ["incremental", "INCREMENTAL", " incremental "]:
        problem = c360_run.gold_override_problem({"spark.lb.gold.strategy": value})
        assert problem and "multi-cycle cycles 2+ only" in problem


def test_unknown_override_is_refused():
    problem = c360_run.gold_override_problem({"spark.lb.gold.strategy": "fastest"})
    assert problem and "names no gold strategy" in problem


def test_full_rebuild_overrides_are_accepted():
    for value in [None, "auto", "simple_agg", "TWO_PHASE_AGG", ""]:
        conf = {} if value is None else {"spark.lb.gold.strategy": value}
        assert c360_run.gold_override_problem(conf) is None


def _config(tmp_path, workload="customer360", value="incremental"):
    text = CONFIG.replace("schema: customer360", f"schema: {workload}")
    cfg = tmp_path / "gold.yaml"
    cfg.write_text(text + f"spark:\n  conf:\n    spark.lb.gold.strategy: {value}\n")
    return cfg


def test_load_refuses_for_data_changing_commands_only(tmp_path):
    from lakebench.config import ConfigValidationError
    from lakebench.config.loader import LoadPurpose, load_config

    cfg = _config(tmp_path)
    for purpose in (LoadPurpose.RUN, LoadPurpose.MUTATE):
        with pytest.raises(ConfigValidationError) as exc:
            load_config(cfg, purpose=purpose)
        assert "multi-cycle cycles 2+ only" in str(exc.value.errors), purpose
    # Teardown and read commands still load it, so the deployment can go.
    load_config(cfg, purpose=LoadPurpose.TEARDOWN)
    load_config(cfg, purpose=LoadPurpose.READ)
    # No Customer 360 gold reads the key on another workload.
    load_config(_config(tmp_path, workload="financial"), purpose=LoadPurpose.RUN)
    load_config(_config(tmp_path, value="two_phase_agg"), purpose=LoadPurpose.RUN)


def test_collector_keeps_the_recorded_strategy():
    from lakebench.metrics.collector import MetricsCollector

    logs = (
        "=== JOB METRICS: gold-finalize ===\nelapsed_seconds: 3.0\n"
        "gold_strategy: two_phase_agg\ngold_strategy_source: auto\n" + "=" * 40
    )
    jm = MetricsCollector().parse_driver_logs(logs, "gold-finalize")
    assert jm.extra_metrics["gold_strategy"] == "two_phase_agg"
    assert jm.extra_metrics["gold_strategy_source"] == "auto"
