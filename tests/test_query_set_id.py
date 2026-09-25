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


def test_legacy_run_without_an_id_is_unknown_and_not_comparable(tmp_path):
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
    assert loaded.query_set_id == "unknown"
    assert qph_comparable(loaded.query_set_id, query_set_id(FIN8))[0] is False


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
        SimpleNamespace(pipeline_benchmark=pb, config_snapshot={}, run_id="r", benchmark=None),
        config_reference=None,
        commit_sha="abc",
    )
    assert pkg["reproduction_metadata"]["query_set_id"] == query_set_id(FIN)


def test_disabled_tm_drops_the_investigator_queries():
    from unittest.mock import MagicMock

    from lakebench.benchmark.runner import BenchmarkRunner
    from tests.conftest import make_config

    def names(enabled):
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
        r.executor = MagicMock()
        return [q.name for q in r._queries()]

    assert names(True) == FIN and names(False) == FIN8
