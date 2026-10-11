"""Compaction by engine and blended query sets (EVD-13).

Trino optimize with a 128 MB threshold and Iceberg rewrite_data_files with
its defaults are different maintenance, recorded by name. An in-stream
composite QpH over rounds that executed different query sets is labelled
blended and not assessed in compare.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lakebench.metrics.collector import (
    BLENDED_QUERY_SET,
    BenchmarkMetrics,
    BenchmarkRoundMeta,
    MetricsCollector,
    aggregate_benchmark_rounds,
    composite_qph_basis,
)
from lakebench.metrics.maintenance_policy import (
    compaction_label,
    effective_maintenance,
    with_compaction_operation,
)
from lakebench.modules.table_formats.iceberg.maintenance import (
    build_compaction_sql,
    compaction_operation,
)
from tests.fixtures import stored_records as sr


def _compaction_outcome(engine: str, threshold: str = "128MB") -> dict:
    op = compaction_operation(engine, threshold)
    return {
        "kind": "compaction",
        "unit": "tables",
        "engine": engine,
        **op,
        "total": 2,
        "succeeded": 2,
        "statements_total": 2,
        "statements_succeeded": 2,
        "statements": [],
        "failures": [],
    }


def _effective(engine: str, outcomes: list[dict]) -> dict:
    expire = {"kind": "expire", "total": 2, "succeeded": 2}
    return effective_maintenance(
        "m2-2026-09-26",
        table_format="iceberg",
        query_engine=engine,
        mode="batch",
        outcomes=[expire, *outcomes],
    )


# --- compaction operation -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("engine", "threshold"),
    [("trino", "256MB"), ("spark-thrift", None), ("duckdb", None)],
)
def test_operation_matches_the_statement_the_builder_writes(engine, threshold):
    """The operation recorded for an engine is the one its compaction
    statement runs; an engine with no statement records none."""
    args = (threshold,) if threshold else ()
    op = compaction_operation(engine, *args)
    statements = build_compaction_sql(engine, "lakehouse", "silver.t", *args)
    if op is None:
        assert statements == []
        return
    verb = op["operation"].removeprefix("trino_").removeprefix("iceberg_")
    assert verb in statements[0]
    for value in op["params"].values():
        assert value in statements[0]


def test_failed_compaction_names_no_operation():
    out = _compaction_outcome("trino")
    out["succeeded"] = out["statements_succeeded"] = 0
    out["failed"] = 2
    em = _effective("trino", [out])
    assert em["operations"]["compaction"] == "failed"
    assert "operation" not in (em["detail"].get("operations") or {}).get("compaction", {})
    assert with_compaction_operation(em)["id"] == em["id"]


def _constructed_pair():
    """Two exp2-shaped blocks that differ only in the compaction their
    engines ran. No architecture in the blocks, so the condition difference
    comes from the recorded operation alone."""
    from lakebench.metrics import comparability as cmp

    trino = with_compaction_operation(_effective("trino", [_compaction_outcome("trino")]))
    thrift = with_compaction_operation(
        _effective("spark-thrift", [_compaction_outcome("spark-thrift")])
    )
    assert trino["id"] != thrift["id"]
    return (
        cmp.classify({"schema": "exp2", "identity_version": 2, "effective_maintenance": trino}),
        cmp.classify({"schema": "exp2", "identity_version": 2, "effective_maintenance": thrift}),
    )


def _stored_pair():
    """Two stored exp1 records, one polaris Thrift and one hive Trino, whose
    operation is derived from the composition."""
    from lakebench.metrics import comparability as cmp

    a, b = sr.load_record("103055-de1772"), sr.load_record("130953-f8a2cf")
    return cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)


@pytest.mark.parametrize(
    ("pair", "want"),
    [
        pytest.param(
            _constructed_pair, {"compaction operation", "effective maintenance"}, id="constructed"
        ),
        pytest.param(_stored_pair, {"compaction operation"}, id="stored"),
    ],
)
def test_trino_thrift_not_like_for_like(pair, want):
    from lakebench.metrics import comparability as cmp

    ca, cb = pair()
    keys = {d.key for d in cmp.diff_group(ca, cb, cmp.CONDITIONS)}
    assert want <= keys


# --- query set per round -------------------------------------------------------------------


def _round(names: list[str], failed: tuple[str, ...] = (), qph: float = 100.0, index: int = 0):
    queries = [{"name": n, "success": n not in failed, "elapsed_seconds": 1.0} for n in names]
    return BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1.0,
        qph=qph,
        total_seconds=8.0,
        queries=queries,
        round_meta=BenchmarkRoundMeta(round_index=index),
    )


EIGHT = [f"Q{i}" for i in range(1, 9)]
TWELVE = [*EIGHT, "IQ1", "IQ2", "IQ3", "IQ4"]


def _collector():
    c = MetricsCollector()
    c.start_run("20261002-000000-aaaaaa", "d", {})
    return c


def test_record_round_writes_the_round_record():
    c = _collector()
    t0 = datetime(2026, 10, 2, 1, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 10, 2, 1, 1, tzinfo=timezone.utc)
    c.record_round(_round(EIGHT, index=3), started_at=t0, ended_at=t1)
    (r,) = c.current_run.benchmark_rounds
    d = r.to_dict()
    from lakebench.benchmark.queries import query_set_id

    assert d["index"] == 3
    assert d["started_at"] == t0.isoformat() and d["ended_at"] == t1.isoformat()
    assert d["executed_queries"] == EIGHT
    assert d["executed_query_set_id"] == query_set_id(EIGHT)


def test_a_failed_query_is_not_executed():
    c = _collector()
    c.record_round(_round(EIGHT, failed=("Q8",)))
    from lakebench.benchmark.queries import query_set_id

    d = c.current_run.benchmark_rounds[0].to_dict()
    assert d["executed_queries"] == EIGHT[:7]
    assert d["executed_query_set_id"] == query_set_id(EIGHT[:7])


def test_the_round_record_survives_a_save():
    from lakebench.metrics.storage import _deserialize_benchmark_rounds

    c = _collector()
    c.record_round(_round(EIGHT), started_at=datetime(2026, 10, 2, tzinfo=timezone.utc))
    raw = [r.to_dict() for r in c.current_run.benchmark_rounds]
    (loaded,) = _deserialize_benchmark_rounds(raw)
    assert loaded.to_dict() == raw[0]


def test_blended_rounds_labelled():
    """Rounds of 8 and of 12 executed queries: composite QpH is blended,
    the per-set medians are recorded, and the aggregate's query set says
    blended, so a QpH over one set is not compared with it."""
    c = _collector()
    for i, (names, qph) in enumerate([(EIGHT, 100.0), (EIGHT, 110.0), (TWELVE, 60.0)]):
        c.record_round(_round(names, qph=qph, index=i))
    rounds = c.current_run.benchmark_rounds
    basis, by_set = composite_qph_basis(rounds)
    from lakebench.benchmark.queries import query_set_id

    q8, q12 = query_set_id(EIGHT), query_set_id(TWELVE)
    assert basis == {"blended": True, "sets": dict(sorted({q8: 2, q12: 1}.items()))}
    assert by_set == dict(sorted({q8: 105.0, q12: 60.0}.items()))
    assert aggregate_benchmark_rounds(rounds).query_set_id == BLENDED_QUERY_SET


def test_one_query_set_is_not_blended():
    c = _collector()
    for i in range(3):
        c.record_round(_round(EIGHT, qph=100.0 + i, index=i))
    rounds = c.current_run.benchmark_rounds
    basis, by_set = composite_qph_basis(rounds)
    from lakebench.benchmark.queries import query_set_id

    assert basis == {"blended": False, "sets": {query_set_id(EIGHT): 3}}
    assert aggregate_benchmark_rounds(rounds).query_set_id == query_set_id(EIGHT)


def test_stored_rounds_get_their_sets_from_the_queries():
    """Rounds recorded before the round record: their queries' success flags
    say what they executed."""
    from lakebench.benchmark.queries import query_set_id

    rounds = [_round(EIGHT, index=i) for i in range(3)]
    assert all(r.round_record is None for r in rounds)
    basis, by_set = composite_qph_basis(rounds)
    assert basis == {"blended": False, "sets": {query_set_id(EIGHT): 3}}
    assert list(by_set) == [query_set_id(EIGHT)]


def test_a_stored_round_with_a_failed_query_is_blended():
    rounds = [_round(EIGHT, index=0), _round(EIGHT, failed=("Q1",), index=1)]
    assert composite_qph_basis(rounds)[0]["blended"] is True


def test_by_set_is_the_median_and_failed_rounds_are_left_out():
    c = _collector()
    for i, qph in enumerate([100.0, 110.0, 200.0]):
        c.record_round(_round(EIGHT, qph=qph, index=i))
    c.record_round(_round(EIGHT, failed=tuple(EIGHT), qph=0.0, index=3))
    basis, by_set = composite_qph_basis(c.current_run.benchmark_rounds)
    from lakebench.benchmark.queries import query_set_id

    assert basis == {"blended": False, "sets": {query_set_id(EIGHT): 3}}
    assert by_set == {query_set_id(EIGHT): 110.0}


def test_one_set_keeps_the_declared_query_set():
    """Every round missed Q8 the same way: one executed set, so the
    aggregate keeps the query set of every name it ran, as before."""
    from lakebench.benchmark.queries import query_set_id

    c = _collector()
    for i in range(3):
        c.record_round(_round(EIGHT, failed=("Q8",), index=i))
    assert aggregate_benchmark_rounds(c.current_run.benchmark_rounds).query_set_id == (
        query_set_id(EIGHT)
    )


def test_mixed_compaction_operations_are_named_mixed():
    trino, thrift = _compaction_outcome("trino"), _compaction_outcome("spark-thrift")
    em = _effective("trino", [trino, thrift])
    comp = em["detail"]["operations"]["compaction"]
    assert comp["operation"] == "mixed"
    assert [o["operation"] for o in comp["operations"]] == [
        "iceberg_rewrite_data_files",
        "trino_optimize",
    ]
    assert compaction_label(em) == "mixed(iceberg_rewrite_data_files+trino_optimize:128MB)"
    other = _effective("trino", [_compaction_outcome("trino", "256MB"), thrift])
    assert compaction_label(other) != compaction_label(em)
    from lakebench.metrics.comparability import compaction_operation as read_op

    assert read_op({"effective_maintenance": em}) == compaction_label(em)
    assert (
        "compaction=ran(mixed(iceberg_rewrite_data_files+trino_optimize:128MB))"
        in (with_compaction_operation(em)["id"])
    )
    empty = dict(thrift, statements_succeeded=0, succeeded=0)
    em = _effective("trino", [trino, empty])
    assert em["detail"]["operations"]["compaction"]["operation"] == "trino_optimize"


def test_the_compaction_call_records_its_operation(monkeypatch):
    """cli/_sustained._run_iceberg_compaction (batch and continuous) writes
    the operation of the statements it ran into its outcome."""
    from unittest.mock import MagicMock

    import lakebench.deploy.iceberg as iceberg
    from lakebench.cli import _sustained
    from lakebench.config import LakebenchConfig

    def fake_run(plan, **kw):
        n = len(plan)
        return {
            "succeeded": n,
            "failures": [],
            "failure_records": [],
            "timed_out": [],
            "not_attempted": [],
            "status": ["ok"] * n,
        }

    monkeypatch.setattr(_sustained, "_run_statements", fake_run)
    monkeypatch.setattr(_sustained, "_compaction_partitions", lambda *a, **k: None)
    monkeypatch.setattr(iceberg, "find_maintenance_engine", lambda cfg, ns: ("trino", "p", "lh"))
    cfg = LakebenchConfig(
        name="t",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "a",
                    "secret_key": "b",
                    "buckets": {"bronze": "b", "silver": "s", "gold": "g"},
                }
            }
        },
    )
    outcomes: list = []
    _sustained._run_iceberg_compaction(
        cfg, MagicMock(), MagicMock(), MagicMock(), file_size_threshold="256MB", outcomes=outcomes
    )
    (out,) = [o for o in outcomes if o.get("kind") == "compaction"]
    assert out["operation"] == "trino_optimize"
    assert out["params"] == {"file_size_threshold": "256MB"}


def test_the_experiment_block_records_and_names_the_compaction_operation():
    """The block the experiment builds records the operation in its detail;
    the exp1 id never names it and the exp2 id does."""
    from lakebench.metrics import experiment as ex

    m = sr.load_metrics("231711-6dd3bc")
    m.experiment = None
    m.maintenance_outcomes = [
        {"kind": "expire", "total": 2, "succeeded": 2},
        _compaction_outcome("trino"),
    ]
    em = ex.build_experiment(m)["effective_maintenance"]
    op = compaction_operation("trino")
    recorded = em["detail"]["operations"]["compaction"]
    assert {k: recorded[k] for k in op} == op
    label = compaction_label(em)
    assert label and label not in em["id"]
    named = with_compaction_operation(em)
    assert label in named["id"]
    assert named["detail"] == em["detail"]


# --- QpH degradation over rounds of different query sets (owner, 10-03) -------


def _sustained_with(rounds):
    from lakebench.metrics.collector import PipelineBenchmark

    pb = PipelineBenchmark(
        run_id="20261002-000000-aaaaaa",
        deployment_name="d",
        pipeline_mode="sustained",
        start_time=datetime(2026, 10, 2, 1, 0, tzinfo=timezone.utc),
        benchmark_rounds=rounds,
    )
    pb.compute_aggregates()
    return pb


def test_degradation_is_withheld_when_rounds_ran_different_sets():
    """An AML continuous run: 8-query rounds before its first case, 12 after.
    The halves time different work, so no degradation figure is recorded."""
    from lakebench.metrics.collector import QPH_DEGRADATION_BLENDED

    rounds = [_round(EIGHT, qph=400.0), _round(EIGHT, qph=400.0)]
    rounds += [_round(TWELVE, qph=100.0), _round(TWELVE, qph=100.0)]
    run = _sustained_with(rounds)
    assert run.qph_degradation_pct is None
    assert run.qph_degradation_withheld == QPH_DEGRADATION_BLENDED
    scores = run.to_dict()["scores"]
    assert "qph_degradation_pct" not in scores
    assert scores["qph_degradation_withheld"] == QPH_DEGRADATION_BLENDED


def test_degradation_is_computed_over_one_set():
    rounds = [_round(TWELVE, qph=q) for q in (100.0, 100.0, 80.0, 80.0)]
    run = _sustained_with(rounds)
    assert run.qph_degradation_pct == 20.0 and run.qph_degradation_withheld is None
