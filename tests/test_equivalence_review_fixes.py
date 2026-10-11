"""Evidence: comparability, stamps and maintenance records."""

from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from lakebench.metrics import experiment as ex
from lakebench.metrics.collector import (
    BenchmarkMetrics,
    JobMetrics,
    MetricsCollector,
    StreamingJobMetrics,
    build_config_snapshot,
)
from tests.conftest import make_config


def _cfg(**arch):
    base = {"workload": {"schema": "customer360", "datagen": {"scale": 1}}}
    base.update(arch)
    return make_config(architecture=base)


def _run(cfg=None, fleet=None):
    cfg = cfg or _cfg()
    run = MetricsCollector().start_run(
        "20260926-120000-aaaaaa", cfg.name, build_config_snapshot(cfg)
    )
    run.benchmark = BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1,
        qph=100.0,
        total_seconds=10.0,
        queries=[{"name": "Q1", "elapsed_seconds": 1.0, "success": True}],
    )
    run.datagen_fleet = fleet
    # Compare and perf_gate refuse a FAILED run. Mark the synthetic run
    # successful with rows in every layer so its verdict is PASSED.
    run.success = True
    run.jobs = [
        JobMetrics(job_name=f"lakebench-{s}", job_type=s, success=True, output_rows=100)
        for s in ("bronze-verify", "silver-build", "gold-finalize")
    ]
    return run


# ---------------------------------------------------------------------------
# Benchmark gate and freshness probe
# ---------------------------------------------------------------------------


class TestEmptyGateInRounds:
    def test_early_rounds_do_not_fail_on_empty_the_final_round_does(self):
        from lakebench.cli._run import _benchmark_gate_problems, empty_benchmark_queries

        qs = [{"name": "Q1_full_aggregation_scan", "success": True, "rows_returned": 0}]
        assert _benchmark_gate_problems(_cfg(), qs, check_empty=False) == []
        assert _benchmark_gate_problems(_cfg(), qs, check_empty=True)
        assert empty_benchmark_queries(qs) == ["Q1_full_aggregation_scan"]

    def test_empty_q9_is_gated_in_the_final_round_only(self):
        from lakebench.cli._sustained import tolerated_q9_results

        empty = {"name": "Q9_gold_dashboard", "success": True, "rows_returned": 0}
        failed = {"name": "Q9_gold_dashboard", "success": False, "rows_returned": 0}
        assert tolerated_q9_results([empty], final=False) == [empty]
        assert tolerated_q9_results([empty], final=True) == []
        assert tolerated_q9_results([failed], final=True) == [failed]


class TestFreshnessProbe:
    @pytest.mark.parametrize(
        "engine,output,value",
        [
            ("trino", '"3600"\n', 3600.0),
            ("spark-thrift", "_c0\n3600\n", 3600.0),
            ("duckdb", '\r100% bar\n{"rows": 1, "data": ["(3600,)"]}', 3600.0),
            ("spark-thrift", "3600\n", None),  # no header: not the value line
            ("duckdb", '{"rows": 0, "data": []}', None),
            ("trino", "", None),
        ],
    )
    def test_scalar_parsing(self, engine, output, value):
        from lakebench.cli._sustained import scalar_from_output

        assert scalar_from_output(engine, output) == value


# ---------------------------------------------------------------------------
# Observed corpus
# ---------------------------------------------------------------------------


class TestObservedCorpus:
    def test_mixed_fleet_is_a_problem(self):
        e = _run(fleet={"data_quality": "mixed", "mixed_params": ["scale"]}).to_dict()["experiment"]
        assert any("mixed" in p for p in e["corpus"]["problems"])

    def test_without_a_fleet_record_it_is_declared_not_observed(self):
        c = _run().to_dict()["experiment"]["corpus"]
        assert c["observed"] is False

    def test_pod_args_reach_the_fleet_record(self):
        from lakebench.metrics.datagen_aggregator import _datagen_args, collect_from_pod_logs

        pod = SimpleNamespace(
            spec=SimpleNamespace(
                containers=[SimpleNamespace(args=["--seed", "7", "--scale", "1.000000"])]
            )
        )
        assert _datagen_args(pod) == {"seed": 7, "scale": 1.0}
        s = collect_from_pod_logs({"a": "", "b": ""}, pod_args={"a": {"seed": 7}, "b": {"seed": 8}})
        assert s.seed is None and s.data_quality == "mixed"


# ---------------------------------------------------------------------------
# Stamps
# ---------------------------------------------------------------------------


class TestStamps:
    def test_streaming_budget_cap_and_tm_cap_are_bound(self):
        run = _run(
            _cfg(
                workload={"schema": "financial", "datagen": {"scale": 1}},
                pipeline={"mode": "sustained"},
            )
        )
        run.streaming.append(
            StreamingJobMetrics(job_name="s", job_type="silver-stream", requested_executors=1)
        )
        run.jobs.append(
            JobMetrics(
                job_name="g",
                job_type="gold-finalize",
                success=True,
                tm_ops={"alerts_over_capacity": 3},
            )
        )
        lim = run.to_dict()["experiment"]["limits"]
        silver = next(x for x in lim["executors"] if x["job_type"] == "silver-stream")
        assert silver["observed"] == 1 and silver["budget_cap"]["granted"] == 1
        assert lim["bound_kinds"] == [
            "TM max_alerts_per_customer",
            "silver-stream: concurrent executor budget",
        ]

    def test_iterations_and_bound_limits_are_conditions(self):
        ra, rb = _run(_cfg(benchmark={"iterations": 1})), _run(_cfg(benchmark={"iterations": 3}))
        ra.benchmark.iterations, rb.benchmark.iterations = 1, 3
        a, b = ra.to_dict(), rb.to_dict()
        assert ex.identity_differences(a["experiment"], b["experiment"]) == []
        assert any(
            d.startswith("benchmark iterations")
            for d in ex.condition_differences(a["experiment"], b["experiment"])
        )


# ---------------------------------------------------------------------------
# Stale block, package usability, maintenance never reached
# ---------------------------------------------------------------------------


class TestBlockFollowsTheRecord:
    def test_benchmark_rewrite_updates_the_block(self, tmp_path):
        from lakebench.metrics.storage import MetricsStorage

        run = _run()
        run.benchmark = None
        storage = MetricsStorage(tmp_path)
        storage.save_run(run)
        loaded = storage.load_run(run.run_id)
        assert loaded.to_dict()["experiment"]["results"]["query_set_id"] is None
        loaded.benchmark = _run().benchmark  # what `lakebench benchmark` does
        stored = copy.deepcopy(loaded.to_dict()["experiment"])
        assert stored["results"]["query_set_id"] is None, "a stored block is never rebuilt"
        ex.refresh_benchmark(loaded)  # and then this (cli/_query.py)
        # ... under its own run id: the run's record is written once.
        loaded.run_id = "bench-1"
        storage.save_run(loaded)
        e = storage.load_run("bench-1").to_dict()["experiment"]
        assert e["results"]["query_set_id"] == loaded.benchmark.query_set_id
        assert e["benchmark_source"].startswith("lakebench benchmark")
        moved = ("results", "limits", "repetitions", "stages", "benchmark_source")
        assert {k: v for k, v in e.items() if k not in moved} == {
            k: v for k, v in stored.items() if k not in moved
        }
        assert "benchmark (not run)" in stored["stages"]["skipped"]
        assert not any(x.startswith("benchmark (") for x in e["stages"]["skipped"])

    def test_run_ending_before_maintenance_is_not_run(self):
        run = _run()
        run.maintenance_outcomes = []
        assert "not_run" in run.to_dict()["experiment"]["effective_maintenance"]["id"]


# ---------------------------------------------------------------------------
# Recorded values and maintenance stamps
# ---------------------------------------------------------------------------


class TestRecordedValues:
    def test_bound_condition_carries_no_counts(self):
        def run(granted):
            r = _run(_cfg(pipeline={"mode": "sustained"}))
            r.streaming.append(
                StreamingJobMetrics(
                    job_name="s", job_type="silver-stream", requested_executors=granted
                )
            )
            return r.to_dict()

        a, b = run(1), run(2)
        ea, eb = ex.experiment_of(a), ex.experiment_of(b)
        assert ea["limits"]["bound"] != eb["limits"]["bound"]  # evidence keeps the counts
        diffs = ex.condition_differences(ea, eb, a, b)
        assert not [d for d in diffs if d.startswith("Lakebench limits")]

    def test_iterations_come_from_the_recorded_benchmark(self):
        r = _run(_cfg(benchmark={"iterations": 3}))
        r.benchmark.iterations = 5
        assert r.to_dict()["experiment"]["limits"]["benchmark_iterations"] == 5

    def test_scale_rendered_to_six_places_is_not_a_disagreement(self):
        corpus, problems = ex._observed_corpus({"scale": 0.1234567}, {"scale": 0.123457})
        assert problems == []

    def test_error_before_any_statement_is_not_run(self):
        from lakebench.metrics.maintenance_policy import (
            MAINTENANCE_POLICY_ID,
            effective_maintenance,
        )

        e = effective_maintenance(
            MAINTENANCE_POLICY_ID,
            table_format="iceberg",
            query_engine="trino",
            mode="batch",
            outcomes=[{"kind": "maintenance", "error": "x", "before_statements": True}],
        )
        assert e["id"].endswith(
            "expire_snapshots=not_run,remove_orphan_files=not_run,compaction=not_run"
        )


class TestMaintenanceStamps:
    def test_batch_maintenance_skips_the_continuous_only_tables(self):
        from lakebench.cli._sustained import maintained_tables

        batch = maintained_tables(_cfg())
        assert not any("bronze" in t for t in batch)
        assert any("bronze" in t for t in maintained_tables(_cfg(pipeline={"mode": "sustained"})))
        aml = {"schema": "financial", "datagen": {"scale": 1}}
        fin = maintained_tables(_cfg(workload=aml))
        assert any(t.startswith("bronze") for t in fin)
        # silver.counterparty_pairs exists only after a continuous run.
        pairs = _cfg(workload=aml).architecture.tables.silver_counterparty_pairs
        assert pairs not in fin
        assert pairs in maintained_tables(_cfg(workload=aml, pipeline={"mode": "continuous"}))

    def test_applied_retention_and_its_source_are_stamped(self):
        r = _run()
        r.maintenance_outcomes = [
            {"kind": "expire", "total": 4, "succeeded": 4, "retention": "0s"},
            {"kind": "compaction", "total": 2, "succeeded": 2},
        ]
        e = r.to_dict()["experiment"]
        assert e["effective_maintenance"]["detail"]["applied_retention"] == ["0s"]
        assert e["maintenance_settings"]["retention_threshold"] == "0s"
        assert "batch pre-benchmark" in e["maintenance_settings"]["retention_source"]

    def test_failed_operations_are_recorded_as_failed(self):
        r = _run()
        r.maintenance_outcomes = [
            {"kind": "expire", "total": 6, "succeeded": 0},
            {"kind": "compaction", "total": 2, "succeeded": 2},
        ]
        assert "expire_snapshots=failed" in r.to_dict()["experiment"]["effective_maintenance"]["id"]

    def test_compaction_no_op_is_detail_not_identity(self):
        from lakebench.metrics.maintenance_policy import (
            MAINTENANCE_POLICY_ID,
            effective_maintenance,
        )

        base = [
            {"kind": "expire", "total": 4, "succeeded": 4},
            {"kind": "compaction", "total": 2, "succeeded": 2},
        ]
        noop = [
            *base,
            {
                "kind": "compaction",
                "files_before": 367,
                "files_after": 367,
                "note": "no-op: data files 367 -> 367",
            },
        ]
        kw = {"table_format": "iceberg", "query_engine": "trino", "mode": "batch"}
        a = effective_maintenance(MAINTENANCE_POLICY_ID, outcomes=base, **kw)
        b = effective_maintenance(MAINTENANCE_POLICY_ID, outcomes=noop, **kw)
        assert a["id"] == b["id"] and any("no-op" in r for r in b["reasons"])


class TestQ1AverageKpi:
    def test_q1_averages_transactions_not_interactions(self):
        """Non-purchase rows carry transaction_amount 0.0 (datagen_rs
        customer360.rs); averaging them in read about 5.5x low."""
        duckdb = pytest.importorskip("duckdb")

        from lakebench.benchmark.queries import get_benchmark_queries
        from lakebench.config.schema import WorkloadSchema

        q1 = next(
            q for q in get_benchmark_queries(WorkloadSchema.CUSTOMER360) if q.name.startswith("Q1")
        )
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE t(customer_id BIGINT, session_id VARCHAR, transaction_amount DOUBLE)"
        )
        con.execute(
            "INSERT INTO t VALUES (1,'a',100.0),(2,'b',0.0),(3,'c',0.0),(4,'d',50.0),(5,'e',0.0)"
        )
        row = con.execute(q1.sql.format(catalog="main", silver_table="t")).fetchone()
        assert row[3] == 150.0 and row[4] == 75.0
