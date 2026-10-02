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

import ast
from pathlib import Path

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.config import c360_run
from lakebench.exit_codes import ExitCode
from tests.test_run_args import CONFIG, no_cluster  # noqa: F401 (fixture)

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "src/lakebench/spark/scripts"
GOLD_SCRIPTS = ("gold_finalize.py", "gold_finalize_delta.py")


@pytest.mark.parametrize("value", ["incremental", "INCREMENTAL", " incremental "])
def test_incremental_override_is_refused(value):
    problem = c360_run.gold_override_problem({"spark.lb.gold.strategy": value})
    assert problem and "multi-cycle cycles 2+ only" in problem


def test_unknown_override_is_refused():
    problem = c360_run.gold_override_problem({"spark.lb.gold.strategy": "fastest"})
    assert problem and "names no gold strategy" in problem


@pytest.mark.parametrize("value", [None, "auto", "simple_agg", "TWO_PHASE_AGG", ""])
def test_full_rebuild_overrides_are_accepted(value):
    conf = {} if value is None else {"spark.lb.gold.strategy": value}
    assert c360_run.gold_override_problem(conf) is None


def test_incremental_override_refused_before_phase1(tmp_path, monkeypatch, no_cluster):  # noqa: F811
    """The refusal exits 2 with no Kubernetes, S3 or subprocess call and no
    record: nothing is created before it."""
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["run", str(_config(tmp_path)), "--yes"])
    assert result.exit_code == ExitCode.USAGE, result.output
    assert "spark.lb.gold.strategy=incremental is refused" in result.output, result.output
    assert no_cluster == []
    assert not list(tmp_path.glob("lakebench-output/runs/*/metrics.json"))


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


def _functions(name: str) -> dict[str, ast.FunctionDef]:
    tree = ast.parse((SCRIPTS / name).read_text())
    return {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}


@pytest.mark.parametrize("name", GOLD_SCRIPTS)
def test_gold_scripts_have_no_silent_incremental(name):
    src = (SCRIPTS / name).read_text()
    fns = _functions(name)
    # The auto choice depends on silver's size alone, not on gold having rows.
    assert [a.arg for a in fns["select_gold_strategy"].args.args] == ["silver_size_gb"]
    assert "check_gold_exists" not in fns
    assert "LB_GOLD_STRATEGY" not in src
    # The script's main runs only as __main__ (the Spark Operator runs it so).
    tree = ast.parse(src)
    assert isinstance(tree.body[-1], ast.If) and "__main__" in ast.unparse(tree.body[-1].test)
    # The strategy and its source are recorded with the job metrics.
    assert "gold_strategy=strategy.value" in src
    assert "gold_strategy_source=strategy_source" in src


@pytest.mark.parametrize("name", GOLD_SCRIPTS)
def test_script_and_config_accept_the_same_values(name):
    """The scripts' OVERRIDABLE_STRATEGIES names the same values as
    c360_run.GOLD_STRATEGY_VALUES (plus auto)."""
    tree = ast.parse((SCRIPTS / name).read_text())
    enum = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "GoldStrategy")
    values = {
        t.targets[0].id: t.value.value
        for t in enum.body
        if isinstance(t, ast.Assign) and isinstance(t.value, ast.Constant)
    }
    allowed = next(
        n
        for n in tree.body
        if isinstance(n, ast.Assign)
        and isinstance(n.targets[0], ast.Name)
        and n.targets[0].id == "OVERRIDABLE_STRATEGIES"
    )
    names = [e.attr for e in allowed.value.elts]
    assert {"auto", *(values[n] for n in names)} == set(c360_run.GOLD_STRATEGY_VALUES)


def test_collector_keeps_the_recorded_strategy():
    from lakebench.metrics.collector import MetricsCollector

    logs = (
        "=== JOB METRICS: gold-finalize ===\nelapsed_seconds: 3.0\n"
        "gold_strategy: two_phase_agg\ngold_strategy_source: auto\n" + "=" * 40
    )
    jm = MetricsCollector().parse_driver_logs(logs, "gold-finalize")
    assert jm.extra_metrics["gold_strategy"] == "two_phase_agg"
    assert jm.extra_metrics["gold_strategy_source"] == "auto"


def test_c360_workload_version_is_c360_2():
    """V16-3 changes what C360 gold computes on a repeat run, so it starts
    c360-2 (K17)."""
    from lakebench.metrics.experiment import WORKLOAD_VERSIONS

    assert WORKLOAD_VERSIONS["customer360"] == "c360-2"
