"""Compaction by engine and blended query sets (EVD-13, DESIGN ch03 section 12).

Trino optimize with a 128 MB threshold and Iceberg rewrite_data_files with
its defaults are different maintenance, recorded by name. An in-stream
composite QpH over rounds that executed different query sets is labelled
blended and not assessed in compare.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest import mock

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


# --- LB-212 -------------------------------------------------------------------


def test_operation_matches_the_statement_the_builder_writes():
    sql = build_compaction_sql("trino", "lakehouse", "silver.t", "256MB")[0]
    op = compaction_operation("trino", "256MB")
    assert op == {"operation": "trino_optimize", "params": {"file_size_threshold": "256MB"}}
    assert "optimize" in sql and "256MB" in sql
    thrift = build_compaction_sql("spark-thrift", "lakehouse", "silver.t")[0]
    assert "rewrite_data_files" in thrift
    assert compaction_operation("spark-thrift") == {
        "operation": "iceberg_rewrite_data_files",
        "params": {},
    }
    assert compaction_operation("duckdb") is None


def test_effective_maintenance_records_the_operation():
    em = _effective("trino", [_compaction_outcome("trino")])
    assert em["detail"]["operations"]["compaction"]["operation"] == "trino_optimize"
    assert em["detail"]["operations"]["compaction"]["params"] == {"file_size_threshold": "128MB"}
    # The exp1 id never names it; the exp2 id does.
    assert "compaction=ran," in em["id"] + "," and "(" not in em["id"]
    named = with_compaction_operation(em)
    assert "compaction=ran(trino_optimize:128MB)" in named["id"]
    assert named["detail"] == em["detail"]


def test_failed_compaction_names_no_operation():
    out = _compaction_outcome("trino")
    out["succeeded"] = out["statements_succeeded"] = 0
    out["failed"] = 2
    em = _effective("trino", [out])
    assert em["operations"]["compaction"] == "failed"
    assert "operation" not in (em["detail"].get("operations") or {}).get("compaction", {})
    assert with_compaction_operation(em)["id"] == em["id"]


def test_trino_thrift_not_like_for_like():
    """Two exp2-shaped blocks that differ only in the compaction their
    engines ran: not like-for-like on the compaction operation (and the
    effective-maintenance id names it). With the operation unrecorded the
    ids read the same."""
    from lakebench.metrics import comparability as cmp

    trino = with_compaction_operation(_effective("trino", [_compaction_outcome("trino")]))
    thrift = with_compaction_operation(
        _effective("spark-thrift", [_compaction_outcome("spark-thrift")])
    )
    assert trino["id"] != thrift["id"]
    # No architecture in the blocks, so nothing can be derived: the
    # condition difference comes from the recorded operation alone.
    ca = cmp.classify({"schema": "exp2", "identity_version": 2, "effective_maintenance": trino})
    cb = cmp.classify({"schema": "exp2", "identity_version": 2, "effective_maintenance": thrift})
    keys = {d.key for d in cmp.diff_group(ca, cb, cmp.CONDITIONS)}
    assert {"compaction operation", "effective maintenance"} <= keys
    assert cmp.compaction_operation({"effective_maintenance": trino}) == "trino_optimize:128MB"
    assert (
        cmp.compaction_operation({"effective_maintenance": thrift}) == "iceberg_rewrite_data_files"
    )


def test_stored_pair_p5_reads_not_like_for_like():
    """P5 (AML batch, polaris Thrift vs hive Trino), exp1: the operation is
    derived from the composition (the read-time derivation that predates
    this change), so the pair stays not like-for-like with recorded
    operations in the code path."""
    from lakebench.metrics import comparability as cmp

    a, b = sr.load_record("103055-de1772"), sr.load_record("130953-f8a2cf")
    ca, cb = cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)
    diffs = cmp.diff_group(ca, cb, cmp.CONDITIONS)
    assert any(d.key == "compaction operation" for d in diffs)


# --- LB-211 -------------------------------------------------------------------


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
    c.record_round(_round(EIGHT, failed=("Q9",), index=3), started_at=t0, ended_at=t1)
    (r,) = c.current_run.benchmark_rounds
    d = r.to_dict()
    from lakebench.benchmark.queries import query_set_id

    assert d["index"] == 3
    assert d["started_at"] == t0.isoformat() and d["ended_at"] == t1.isoformat()
    assert d["executed_queries"] == EIGHT
    assert d["executed_query_set_id"] == query_set_id(EIGHT)
    assert d["investigator_queries"] is None
    assert "round_meta" in d and "investigator" not in str(d["round_meta"])


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
    say what they executed. P2's rounds all ran 8 of 8."""
    from lakebench.benchmark.queries import query_set_id

    m = sr.load_metrics("204941-1d17f4")
    rounds = m.pipeline_benchmark.benchmark_rounds
    names = [q["name"] for q in rounds[0].queries]
    basis, by_set = composite_qph_basis(rounds)
    assert basis == {"blended": False, "sets": {query_set_id(names): len(rounds)}}
    assert list(by_set) == [query_set_id(names)]


def test_a_stored_round_with_a_failed_query_is_blended():
    m = sr.load_metrics("204941-1d17f4")
    rounds = m.pipeline_benchmark.benchmark_rounds
    rounds[1].queries[0]["success"] = False
    assert composite_qph_basis(rounds)[0]["blended"] is True


ROUND_MEDIANS = ("composite_qph", "in_stream_composite_qph", "qph_degradation_pct")


def test_a_mode_less_round_median_is_still_blended_by_rounds():
    """lookup(key, None) merges a mode-split key's entries, so a pair whose
    records name no mode still applies the rounds rule to composite_qph."""
    from lakebench.metrics.metric_registry import lookup

    for key in ROUND_MEDIANS:
        assert lookup(key, None).blended_by_rounds is True
    assert lookup("composite_qph", "batch").blended_by_rounds is False


def test_two_blends_are_not_one_query_set():
    from lakebench.benchmark.queries import qph_comparable

    ok, why = qph_comparable(BLENDED_QUERY_SET, BLENDED_QUERY_SET)
    assert ok is False and "different query sets" in why


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


def test_blended_qph_is_not_gated_or_reproduced():
    """The perf gate and reproduce leave out a median over blended rounds."""
    from lakebench.cli._reproduce import _extract_expected_numbers

    m = sr.load_metrics("204941-1d17f4")
    assert "composite_qph" in _extract_expected_numbers(m)
    m.pipeline_benchmark.benchmark_rounds[1].queries[0]["success"] = False
    assert "composite_qph" not in _extract_expected_numbers(m)


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


def test_exp2_block_names_the_compaction_operation(monkeypatch):
    """A block the experiment stamps exp2 carries the operation in its
    effective-maintenance id; an exp1 block does not."""
    from lakebench.metrics import experiment as ex

    m = sr.load_metrics("231711-6dd3bc")
    m.experiment = None
    m.maintenance_outcomes = [
        {"kind": "expire", "total": 2, "succeeded": 2},
        _compaction_outcome("trino"),
    ]
    exp1 = ex.build_experiment(m)
    assert "(" not in exp1["effective_maintenance"]["id"]
    assert (
        exp1["effective_maintenance"]["detail"]["operations"]["compaction"]["operation"]
        == "trino_optimize"
    )
    named = with_compaction_operation(exp1["effective_maintenance"])
    assert "compaction=ran(trino_optimize:128MB)" in named["id"]


def test_recorded_rounds_that_all_missed_a_query_read_the_smaller_set():
    """Rounds written through record_round that all missed Q8 executed the
    7-query set: compare and reproduce both read that set, not the declared
    8-query one, and a full set reads as the aggregate's id."""
    from lakebench.benchmark.queries import query_set_id
    from lakebench.cli._reproduce import _run_query_set
    from lakebench.metrics.collector import executed_subset_query_set

    c = _collector()
    for i in range(3):
        c.record_round(_round(EIGHT, failed=("Q8",), index=i))
    rounds = c.current_run.benchmark_rounds
    assert executed_subset_query_set(rounds) == query_set_id(EIGHT[:7])
    run = mock.Mock()
    run.pipeline_benchmark.benchmark_rounds = rounds
    run.pipeline_benchmark.query_benchmark.query_set_id = query_set_id(EIGHT)
    assert _run_query_set(run) == query_set_id(EIGHT[:7])

    c = _collector()
    for i in range(3):
        c.record_round(_round(EIGHT, index=i))
    assert executed_subset_query_set(c.current_run.benchmark_rounds) is None


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
