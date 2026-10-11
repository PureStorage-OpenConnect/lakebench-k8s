"""Customer 360 gold strategy: an incremental or unknown override is refused on data-changing commands."""

from __future__ import annotations

import pytest

from lakebench.config import c360_run
from tests.fixtures.run_args_helpers import CONFIG


@pytest.mark.parametrize(
    ("value", "refused"),
    [
        ("incremental", True),
        ("INCREMENTAL", True),
        (" incremental ", True),
        ("fastest", True),
        (None, False),
        ("auto", False),
        ("simple_agg", False),
        ("TWO_PHASE_AGG", False),
        ("", False),
    ],
)
def test_gold_override_refusal(value, refused):
    conf = {} if value is None else {"spark.lb.gold.strategy": value}
    problem = c360_run.gold_override_problem(conf)
    assert (problem is not None) == refused


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
        assert "spark.lb.gold.strategy" in str(exc.value.errors), purpose
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
