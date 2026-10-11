"""QpH is queries per hour over one query set. Runs over different sets (the
AML set grew from 8 to 12 queries) carry different query-set ids."""

from __future__ import annotations

from datetime import datetime

from lakebench.benchmark.queries import (
    INVESTIGATOR_QUERIES,
    get_benchmark_queries,
    query_set_id,
)
from lakebench.config.schema import WorkloadSchema
from lakebench.metrics import BenchmarkMetrics, MetricsStorage, PipelineMetrics

FIN = [q.name for q in get_benchmark_queries(WorkloadSchema.FINANCIAL)]
FIN8 = [n for n in FIN if n not in {q.name for q in INVESTIGATOR_QUERIES}]


def _bench(names):
    return BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1,
        qph=100.0,
        total_seconds=10.0,
        queries=[{"name": n, "elapsed_seconds": 1.0, "success": True} for n in names],
    )


def test_ids_differ_by_set_and_not_by_order():
    assert query_set_id(FIN) == query_set_id(list(reversed(FIN)))
    assert query_set_id(FIN) != query_set_id(FIN8)


def test_benchmark_metrics_carry_the_id_through_metrics_json(tmp_path):
    b = _bench(FIN)
    assert b.query_set_id == query_set_id(FIN) and b.to_dict()["query_set_id"] == b.query_set_id
    st = MetricsStorage(tmp_path)
    st.save_run(
        PipelineMetrics(run_id="r1", deployment_name="d", start_time=datetime.now(), benchmark=b)
    )
    assert st.load_run("r1").benchmark.query_set_id == b.query_set_id


def test_legacy_run_gets_the_id_of_the_set_it_ran(tmp_path):
    """A run recorded before query-set ids keeps comparing with runs over
    the same set: its id is derived from its recorded query names."""
    import json

    st = MetricsStorage(tmp_path)
    st.save_run(
        PipelineMetrics(
            run_id="old", deployment_name="d", start_time=datetime.now(), benchmark=_bench(FIN8)
        )
    )
    path = st.run_dir("old") / "metrics.json"
    raw = json.loads(path.read_text())
    raw["benchmark"].pop("query_set_id")
    path.write_text(json.dumps(raw))
    loaded = st.load_run("old").benchmark
    # frozen id of the pre-tiebreaker FQ1-FQ8 SQL
    assert loaded.query_set_id == "qs8-1c2902f0b26a"
    assert query_set_id(FIN8) != loaded.query_set_id
    assert query_set_id(FIN) != loaded.query_set_id


def test_investigator_queries_only_for_a_run_whose_tm_layer_ran():
    from unittest.mock import MagicMock

    from lakebench.benchmark.runner import BenchmarkRunner
    from tests.conftest import make_config

    def runner(enabled, tm_run_id):
        cfg = make_config(
            architecture={
                "workload": {
                    "schema": "financial",
                    "datagen": {"scale": 1},
                    "tm_operations": {"enabled": enabled},
                }
            }
        )
        r = BenchmarkRunner.__new__(BenchmarkRunner)
        r.config = cfg
        r.tm_run_id = tm_run_id
        r.executor = MagicMock()
        return r

    assert [q.name for q in runner(True, "run-1")._queries()] == FIN
    assert [q.name for q in runner(True, None)._queries()] == FIN8  # layer did not run
    assert [q.name for q in runner(False, "run-1")._queries()] == FIN8


def test_investigator_sql_is_scoped_to_the_run():
    from unittest.mock import MagicMock

    from lakebench.benchmark.runner import BenchmarkRunner
    from tests.conftest import make_config

    cfg = make_config(architecture={"workload": {"schema": "financial", "datagen": {"scale": 1}}})
    r = BenchmarkRunner.__new__(BenchmarkRunner)
    r.config, r.tm_run_id, r.catalog = cfg, "run-9", "lakehouse"
    r.silver_table, r.gold_table = "silver.transactions", "gold.daily_dashboards"
    r._extra_tables = {
        "gold_cases": "gold.cases",
        "gold_alert_dispositions": "gold.alert_dispositions",
        "silver_entities": "silver.entities",
        "silver_accounts": "silver.accounts",
        "silver_counterparty_edges": "silver.counterparty_edges",
    }
    seen = []
    r.executor = MagicMock()
    r.executor.adapt_query.side_effect = lambda s: seen.append(s) or s
    r.executor.execute_query.return_value = MagicMock(
        duration_seconds=1.0, rows_returned=1, success=True, error=None
    )
    for q in INVESTIGATOR_QUERIES:
        r._execute_single_query(q)
    assert len(seen) == 4 and all("base_run_id = 'run-9'" in s for s in seen)
    assert not any("{" in s for s in seen)
