"""No-query-engine recipes and the recorded benchmark engine (lb16 sweep).

- A plain `lakebench run` of a ``*-none`` recipe skipped nothing and failed
  on "Cannot run queries without a query engine", exiting 1 on a correct
  pipeline.
- A run that ran no benchmark published composite_qph 0.0.
- benchmark_type was "trino_query" on DuckDB and Spark Thrift runs.
"""

from __future__ import annotations

import ast
import inspect
from unittest.mock import MagicMock, patch

import pytest

from lakebench.metrics import BenchmarkMetrics, MetricsCollector, build_config_snapshot
from lakebench.metrics.collector import build_pipeline_benchmark
from tests.conftest import make_config


def _cfg(engine="trino", mode="batch", recipe=None):
    arch = {"workload": {"schema": "customer360", "datagen": {"scale": 1}}}
    arch["pipeline"] = {"mode": mode}
    if recipe is None:
        arch["query_engine"] = {"type": engine}
        return make_config(architecture=arch)
    return make_config(recipe=recipe, architecture=arch)


def _run(cfg, bench: BenchmarkMetrics | None):
    run = MetricsCollector().start_run(
        "20260927-120000-aaaaaa", cfg.name, build_config_snapshot(cfg)
    )
    run.benchmark = bench
    return run


def _bench(engine: str | None, qph: float = 100.0) -> BenchmarkMetrics:
    return BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1,
        qph=qph,
        total_seconds=10.0,
        queries=[{"name": "Q1_full_aggregation_scan", "elapsed_seconds": 1.0, "success": True}],
        engine=engine,
    )


# -- 3. no query engine -----------------------------------------------------


@pytest.mark.parametrize(
    "recipe",
    ["hive-iceberg-spark-none", "polaris-iceberg-spark-none", "hive-delta-spark-none"],
)
def test_no_engine_recipe_skips_the_benchmark_without_a_flag(recipe, capsys):
    from lakebench.cli._run import no_query_engine_skip

    cfg = _cfg(recipe=recipe)
    none, skip = no_query_engine_skip(cfg, skip_benchmark=False)
    assert none is True and skip is True
    assert "no query engine" in "".join(capsys.readouterr()).lower()


def test_an_engine_recipe_is_not_skipped():
    from lakebench.cli._run import no_query_engine_skip

    assert no_query_engine_skip(_cfg("trino"), skip_benchmark=False) == (False, False)
    assert no_query_engine_skip(_cfg("trino"), skip_benchmark=True) == (False, True)


def test_batch_run_applies_the_skip_before_the_benchmark_and_maintenance():
    """run() decides the skip before maintenance and the benchmark read
    skip_benchmark, and the maintenance record names the reason."""
    import lakebench.cli._run as run_mod

    src = inspect.getsource(run_mod.run)
    call = src.index("no_query_engine_skip(cfg, skip_benchmark)")
    assert call < src.index("do_maintenance = (")
    assert call < src.index("Phase 6/7: Benchmark")
    tree = ast.parse(inspect.getsource(run_mod))
    strings = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)}
    assert "no query engine" in strings


def test_no_engine_run_records_the_benchmark_as_skipped_not_failed():
    from lakebench.metrics.experiment import build_experiment

    cfg = _cfg(recipe="hive-iceberg-spark-none")
    run = _run(cfg, None)
    run.maintenance_outcomes = [
        {"kind": k, "user_skip": "no query engine"} for k in ("expire", "compaction")
    ]
    exp = build_experiment(run)
    assert "benchmark (skipped: no query engine)" in exp["stages"]["skipped"]
    assert not any("failed" in s for s in exp["stages"]["skipped"])
    # No engine to run maintenance through: not supported, not failed.
    assert set(exp["effective_maintenance"]["operations"].values()) == {"not_supported"}


def test_an_engine_run_without_a_benchmark_keeps_the_generic_label():
    from lakebench.metrics.experiment import build_experiment

    exp = build_experiment(_run(_cfg("trino"), None))
    assert "benchmark (not run)" in exp["stages"]["skipped"]


def test_batch_scores_have_no_qph_when_no_benchmark_ran():
    cfg = _cfg(recipe="hive-iceberg-spark-none")
    scores = build_pipeline_benchmark(_run(cfg, None)).to_dict()["scores"]
    assert "composite_qph" not in scores
    ran = build_pipeline_benchmark(_run(_cfg("trino"), _bench("trino"))).to_dict()["scores"]
    assert ran["composite_qph"] == 100.0


def test_continuous_scores_have_no_qph_when_no_benchmark_ran():
    cfg = _cfg(mode="continuous", recipe="hive-iceberg-spark-none")
    run = _run(cfg, None)
    pb = build_pipeline_benchmark(run)
    pb.pipeline_mode = "sustained"
    assert "composite_qph" not in pb.to_dict()["scores"]
    # A benchmark that ran and measured nothing stays visible as None.
    run2 = _run(_cfg("trino", mode="continuous"), _bench("trino", qph=0.0))
    pb2 = build_pipeline_benchmark(run2)
    pb2.pipeline_mode = "sustained"
    scores = pb2.to_dict()["scores"]
    assert "composite_qph" in scores and scores["composite_qph"] is None


# -- 4. the recorded benchmark engine ---------------------------------------


@pytest.mark.parametrize(
    "engine,expected",
    [
        ("trino", "trino_query"),
        ("spark-thrift", "spark_thrift_query"),
        ("duckdb", "duckdb_query"),
        (None, "unknown_query"),
    ],
)
def test_benchmark_type_names_the_engine(engine, expected):
    d = _bench(engine).to_dict()
    assert d["benchmark_type"] == expected and d["engine"] == engine


def test_runner_result_carries_the_executor_engine():
    from lakebench.benchmark.runner import BenchmarkRunner

    executor = MagicMock()
    executor.engine_name.return_value = "duckdb"
    cfg = _cfg("duckdb")
    with patch("lakebench.benchmark.executor.get_executor", return_value=executor):
        runner = BenchmarkRunner(cfg)
    with patch.object(BenchmarkRunner, "_queries", return_value=[]):
        result = runner.run_power()
    assert result.engine == "duckdb"
    assert result.to_dict()["benchmark_type"] == "duckdb_query"


def test_aggregated_rounds_keep_the_engine():
    from lakebench.metrics import aggregate_benchmark_rounds

    agg = aggregate_benchmark_rounds([_bench("spark-thrift"), _bench("spark-thrift")])
    assert agg.to_dict()["benchmark_type"] == "spark_thrift_query"


def test_old_records_take_the_engine_from_the_fingerprints():
    """Records before the field stamped trino_query on every engine; the
    per-query fingerprints carried the real engine."""
    from lakebench.metrics.storage import recorded_engine

    old = {
        "benchmark_type": "trino_query",
        "queries": [{"name": "Q1", "result_fingerprint": {"engine": "duckdb"}}],
    }
    assert recorded_engine(old) == "duckdb"
    assert recorded_engine({"benchmark_type": "trino_query", "queries": []}) is None
    assert recorded_engine({"engine": "spark-thrift"}) == "spark-thrift"
    assert recorded_engine({"queries": ["not a dict"]}) is None


def test_engine_survives_a_save_and_load(tmp_path):
    from lakebench.metrics import MetricsStorage

    run = _run(_cfg("spark-thrift", recipe="hive-iceberg-spark-thrift"), _bench("spark-thrift"))
    run.pipeline_benchmark = build_pipeline_benchmark(run)
    storage = MetricsStorage(tmp_path)
    storage.save_run(run)
    loaded = storage.load_run(run.run_id)
    assert loaded.benchmark.engine == "spark-thrift"
    assert loaded.pipeline_benchmark.query_benchmark.engine == "spark-thrift"
