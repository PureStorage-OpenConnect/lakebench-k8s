"""QpH is queries per hour over one query set. Runs over different sets (the
AML set grew from 8 to 12 queries) are refused by compare and reproduce."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from lakebench.benchmark.queries import (
    INVESTIGATOR_QUERIES,
    get_benchmark_queries,
    qph_comparable,
    query_set_id,
)
from lakebench.config.schema import WorkloadSchema
from lakebench.metrics import BenchmarkMetrics, MetricsStorage, PipelineMetrics
from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID

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
    assert len(FIN) == 12 and len(FIN8) == 8
    assert query_set_id(FIN) == query_set_id(list(reversed(FIN)))
    assert query_set_id(FIN) != query_set_id(FIN8)
    assert query_set_id(FIN).startswith("qs12-") and query_set_id(FIN8).startswith("qs8-")


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
    assert loaded.query_set_id == query_set_id(FIN8)
    assert qph_comparable(loaded.query_set_id, query_set_id(FIN8))[0] is True
    assert qph_comparable(loaded.query_set_id, query_set_id(FIN))[0] is False


def test_compare_keeps_legacy_c360_runs_comparable():
    from lakebench.cli._compare import _build_comparison

    c360 = [q.name for q in get_benchmark_queries(WorkloadSchema.CUSTOMER360)]
    old = _bench(c360).to_dict()
    old.pop("query_set_id")
    new = _bench(c360).to_dict()

    def m(b, qph, start="2026-09-24T20:00:00-06:00"):
        return {
            "run_id": "x",
            "start_time": start,
            "benchmark": b,
            "pipeline_benchmark": {"scores": {"composite_qph": qph}},
        }

    c = _build_comparison("a", m(old, 500), "b", m(new, 480))
    assert c["qph_comparable"] is True
    assert [r["metric"] for r in c["metrics"]] == ["composite_qph"]
    # Recorded before the last c360 SQL change (Q6 recency): not the same SQL.
    c = _build_comparison("a", m(old, 500, "2026-09-20T10:00:00-06:00"), "b", m(new, 480))
    assert c["qph_comparable"] is False


def test_compare_refuses_qph_across_query_sets():
    from lakebench.cli._compare import _build_comparison

    def m(names, qph):
        return {
            "run_id": "x",
            "benchmark": _bench(names).to_dict(),
            "pipeline_benchmark": {"scores": {"composite_qph": qph, "time_to_value_seconds": 9}},
        }

    c = _build_comparison("a", m(FIN8, 500), "b", m(FIN, 300))
    assert c["qph_comparable"] is False
    assert [r["metric"] for r in c["metrics"]] == ["time_to_value_seconds"]
    assert c["qph_refused"]["metrics"] == ["composite_qph"]
    same = _build_comparison("a", m(FIN, 500), "b", m(FIN, 300))
    assert same["qph_comparable"] is True
    assert "composite_qph" in [r["metric"] for r in same["metrics"]]


def test_reproduce_refuses_qph_across_query_sets():
    from lakebench.cli._reproduce import _build_package, _compare

    exp = {"composite_qph": 100.0, "scale_ratio": 1.0}
    rows, code = _compare(exp, dict(exp), {}, (query_set_id(FIN8), query_set_id(FIN)))
    st = {r["metric"]: r["status"] for r in rows}
    assert st == {"composite_qph": "incomparable", "scale_ratio": "pass"} and code == 1
    rows, code = _compare(exp, dict(exp), {}, (None, query_set_id(FIN)))
    # A package that predates query-set ids: QpH not compared, not failed.
    assert code == 0 and rows[0]["status"] == "incomparable"
    rows, code = _compare(exp, dict(exp), {}, (query_set_id(FIN), query_set_id(FIN)))
    assert code == 0
    pb = SimpleNamespace(
        pipeline_mode="batch",
        scale_ratio=1.0,
        post_compaction_qph=0.0,
        query_benchmark=_bench(FIN),
        stages=[],
    )
    pkg = _build_package(
        SimpleNamespace(
            pipeline_benchmark=pb,
            config_snapshot={},
            run_id="r",
            benchmark=None,
            maintenance_policy_id=MAINTENANCE_POLICY_ID,
        ),
        config_reference=None,
        commit_sha="abc",
    )
    assert pkg["reproduction_metadata"]["query_set_id"] == query_set_id(FIN)


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
    # Every FROM of a TM table carries the run filter.
    for q in INVESTIGATOR_QUERIES:
        reads = q.sql.count("{gold_cases}") + q.sql.count("{gold_alert_dispositions}")
        assert q.sql.count("{tm_run_id}") == reads, q.name
