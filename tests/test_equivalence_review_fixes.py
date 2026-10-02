"""Fixes from the adversarial reviews of the result-equivalence and evidence
work (lane compare-equiv). Each test fails with its fix reverted."""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from unittest import mock

import pytest

from lakebench.benchmark.fingerprint import (
    Unsupported,
    canonical_cell,
    fingerprint_rows,
    mismatch,
    rows_from_beeline_tsv2,
    rows_from_trino_json,
)
from lakebench.metrics import experiment as ex
from lakebench.metrics.collector import (
    BenchmarkMetrics,
    JobMetrics,
    MetricsCollector,
    StreamingJobMetrics,
    build_config_snapshot,
)
from tests.conftest import make_config, stub_experiment


def _cfg(**arch):
    base = {"workload": {"schema": "customer360", "datagen": {"scale": 1}}}
    base.update(arch)
    return make_config(architecture=base)


def _run(cfg=None, fps=None, fleet=None):
    cfg = cfg or _cfg()
    run = MetricsCollector().start_run(
        "20260926-120000-aaaaaa", cfg.name, build_config_snapshot(cfg)
    )
    fps = fps if fps is not None else {"Q1": fingerprint_rows([(1, "x")])}
    run.benchmark = BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1,
        qph=100.0,
        total_seconds=10.0,
        queries=[
            {"name": n, "elapsed_seconds": 1.0, "success": True, "result_fingerprint": f}
            for n, f in fps.items()
        ],
    )
    run.datagen_fleet = fleet
    # A2b wiring: compare and perf_gate now refuse a run whose verdict is
    # FAILED. Mark the synthetic collector as successful so its computed
    # verdict is PASSED; these tests exercise the comparability ladder,
    # not the failed-run refusal path.
    run.success = True
    return run


# ---------------------------------------------------------------------------
# S1-S3: fingerprint rules
# ---------------------------------------------------------------------------


class TestApproxRowAssociation:
    def test_swapped_values_between_groups_differ(self):
        a = fingerprint_rows([("web", 1000.00), ("mobile", 5.00)], {1: 0.01})
        b = fingerprint_rows([("web", 5.00), ("mobile", 1000.00)], {1: 0.01})
        assert a["approx"] == b["approx"]  # the plain sum cannot see it
        assert mismatch(a, b) and "row-weighted" in mismatch(a, b)

    def test_one_row_far_off_is_not_absorbed_by_row_count(self):
        rows = [(f"d{i}", 100.0) for i in range(90)]
        off = [*rows[:-1], ("d89", 189.0)]
        assert mismatch(fingerprint_rows(rows, {1: 1.0}), fingerprint_rows(off, {1: 1.0}))

    def test_summation_noise_still_matches(self):
        rows = [(f"d{i}", 100.25) for i in range(455)]
        noisy = [(k, v + (0.01 if i % 50 == 0 else 0.0)) for i, (k, v) in enumerate(rows)]
        assert (
            mismatch(fingerprint_rows(rows, {1: 0.01}), fingerprint_rows(noisy, {1: 0.01})) is None
        )


class TestApproxSpecials:
    def test_nan_matches_only_nan(self):
        nan = fingerprint_rows([("a", float("nan"))], {1: 0.01})
        num = fingerprint_rows([("a", 1.0)], {1: 0.01})
        assert mismatch(nan, num)
        assert mismatch(nan, fingerprint_rows([("a", "NaN")], {1: 0.01})) is None
        assert mismatch(
            fingerprint_rows([("a", float("inf"))], {1: 0.01}),
            fingerprint_rows([("a", float("-inf"))], {1: 0.01}),
        )


class TestExactDigits:
    def test_bigint_ids_as_text_keep_every_digit(self):
        """Beeline sends a 19-digit xxhash64 id as text; Trino and DuckDB as an int."""
        assert canonical_cell("-1234567890123456789") == canonical_cell(-1234567890123456789)
        assert canonical_cell("1234567890123456789") != canonical_cell("1234567890123456788")

    def test_decimal_sums_keep_their_cents(self):
        assert canonical_cell("12345678901234.57") != canonical_cell("12345678901234.56")
        assert canonical_cell("12345678901234.570") == canonical_cell("12345678901234.57")

    def test_trino_json_numbers_are_read_as_text(self):
        rows = rows_from_trino_json('{"id":1234567890123456789,"v":12345678901234.57}\n')
        tsv = rows_from_beeline_tsv2("id\tv\n1234567890123456789\t12345678901234.57\n")
        assert fingerprint_rows(rows)["exact"] == fingerprint_rows(tsv)["exact"]


# ---------------------------------------------------------------------------
# S4, S6: empty output and empty rows
# ---------------------------------------------------------------------------


class TestEmptyOutput:
    def test_no_tsv2_header_is_unsupported_not_zero_rows(self):
        with pytest.raises(Unsupported):
            rows_from_beeline_tsv2("")

    def test_empty_trino_output_is_unsupported(self):
        with pytest.raises(Unsupported):
            rows_from_trino_json("")

    def test_a_row_of_empty_cells_is_kept(self):
        assert rows_from_beeline_tsv2("name\n\n") == [[""]]

    def test_thrift_count_keeps_a_row_of_empty_cells(self):
        from lakebench.benchmark.executor import SparkThriftExecutor

        ex_ = SparkThriftExecutor(namespace="t", catalog_name="c")
        ex_._pod = "p"
        with mock.patch("subprocess.run") as run:
            run.return_value = mock.MagicMock(returncode=0, stdout="name\n\n", stderr="")
            assert ex_.execute_query("SELECT ''").rows_returned == 1


class TestRunnerCrossCheck:
    def _runner(self, timed_rows, fp):
        from lakebench.benchmark.result import QueryExecutorResult
        from lakebench.benchmark.runner import BenchmarkRunner

        class Ex:
            catalog_name = "lakehouse"

            def engine_name(self):
                return "trino"

            def adapt_query(self, sql):
                return sql

            def flush_cache(self):
                pass

            def execute_query(self, sql, timeout=300):
                return QueryExecutorResult(sql, "trino", 1.0, timed_rows, "x")

            def fingerprint_query(self, sql, timeout=300, approx_columns=None):
                self.timeout = timeout
                return QueryExecutorResult(sql, "trino", 1.0, 0, "", fingerprint=fp)

        executor = Ex()
        with mock.patch("lakebench.benchmark.executor.get_executor", return_value=executor):
            return BenchmarkRunner(make_config()), executor

    def test_row_count_disagreeing_with_the_timed_run_is_unusable(self):
        runner, _ = self._runner(5, fingerprint_rows([(1,)]))
        result = runner.run_power(iterations=1)
        fp = result.queries[0].result_fingerprint
        assert "error" in fp and "timed run" in fp["error"]

    def test_empty_trino_result_agreeing_with_the_timed_run_is_zero_rows(self):
        from lakebench.benchmark.fingerprint import unusable

        fp = unusable("unsupported", "Trino printed no rows (an empty result ...)", "trino")
        runner, _ = self._runner(0, fp)
        result = runner.run_power(iterations=1)
        got = result.queries[0].result_fingerprint
        assert got["rows"] == 0 and "exact" in got

    def test_throughput_fingerprints_use_the_query_timeout(self):
        runner, executor = self._runner(1, fingerprint_rows([(1,)]))
        runner.run_throughput(streams=1, query_timeout=1800)
        assert executor.timeout == 1800


# ---------------------------------------------------------------------------
# S5, S7: benchmark gate and freshness probe
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
# S8, E7: failed queries are one failure, not also a result mismatch
# ---------------------------------------------------------------------------


class TestFailedQueries:
    def test_reference_side_failed_query_is_not_a_mismatch(self):
        exp = stub_experiment(["Q1", "Q2"])
        refs = ex.stored_identity_refusals(
            ex.identity(exp),
            {"Q1": exp["results"]["fingerprints"]["Q1"], "Q2": None},
            exp,
            "baseline",
        )
        assert refs == []

    def test_reproduce_skips_a_failed_query(self):
        from lakebench.cli._reproduce import _experiment_refusal

        exp = stub_experiment(["Q1", "Q2"])
        meta = {
            "experiment_identity": ex.identity(exp),
            "result_fingerprints": ex.result_fingerprints(exp),
        }
        run_exp = stub_experiment(["Q1", "Q2"], failed=("Q2",))
        bench = SimpleNamespace(
            queries=[{"name": "Q1", "success": True}, {"name": "Q2", "success": False}]
        )
        metrics = SimpleNamespace(experiment=run_exp, benchmark=bench, pipeline_benchmark=None)
        assert _experiment_refusal(meta, metrics) is None


# ---------------------------------------------------------------------------
# E1: comparability not established
# ---------------------------------------------------------------------------


class TestNotEstablished:
    def _pair(self, a, b):
        b = dict(b)
        b["run_id"] = "20260926-120000-bbbbbb"
        for m in (a, b):
            m.setdefault("pipeline_benchmark", {})["scores"] = {"time_to_value_seconds": 100.0}
        b["pipeline_benchmark"]["scores"]["time_to_value_seconds"] = 50.0
        return a, b

    def _comparison(self, a, b):
        from lakebench.metrics.compare import compare_records

        return compare_records(*[[m] for m in self._pair(a, b)])

    def test_runs_without_results_are_not_established(self):
        c = self._comparison(_run(fps={}).to_dict(), _run(fps={}).to_dict())
        assert c["verdict"] == "NOT ESTABLISHED" and c["exit_code"] == 11
        assert all(r["assessment"] == "withheld" for r in c["metrics"])
        assert all(r["delta_pct"] is None for r in c["metrics"])
        assert c["missing"]["condition"] == "checked results"

    def test_compare_exits_11_and_shows_no_winner_when_not_established(self, tmp_path):
        from typer.testing import CliRunner

        from lakebench.cli import app

        a, b = self._pair(_run(fps={}).to_dict(), _run(fps={}).to_dict())
        runs = tmp_path / "runs"
        for m in (a, b):
            d = runs / f"run-{m['run_id']}"
            d.mkdir(parents=True)
            (d / "metrics.json").write_text(json.dumps(m))
        result = CliRunner().invoke(
            app, ["compare", a["run_id"], b["run_id"], "--runs-dir", str(runs)]
        )
        assert result.exit_code == 11, result.output
        assert "NOT ESTABLISHED" in result.output and "-50.00%" not in result.output

    def test_stored_references_refuse_a_batch_run_without_results(self):
        exp = stub_experiment(["Q1"])
        empty = stub_experiment([])
        refs = ex.stored_identity_refusals(
            ex.identity(exp), ex.result_fingerprints(exp), empty, "baseline"
        )
        assert any("comparability not established" in r for r in refs)
        refs = ex.stored_identity_refusals(ex.identity(exp), {}, exp, "package")
        assert any("package has no result fingerprints" in r for r in refs)


# ---------------------------------------------------------------------------
# E2: corpus as generated
# ---------------------------------------------------------------------------


class TestObservedCorpus:
    def test_fleet_values_are_stamped_and_disagreement_refuses(self):
        cfg = _cfg()
        fleet = {"seed": 999, "scale": 1.0, "image": cfg.images.datagen, "image_ids": []}
        e = _run(cfg, fleet=fleet).to_dict()["experiment"]
        assert e["corpus"]["seed"] == 999 and e["corpus"]["observed"] is True
        assert any("seed" in p for p in e["corpus"]["problems"])
        good = _run(cfg).to_dict()
        prov, _, _ = ex.refusals(good, _run(cfg, fleet=fleet).to_dict())
        assert any("datagen pods ran 999" in p for p in prov), prov

    def test_mixed_fleet_is_a_problem(self):
        e = _run(fleet={"data_quality": "mixed", "mixed_params": ["scale"]}).to_dict()["experiment"]
        assert any("mixed" in p for p in e["corpus"]["problems"])

    def test_without_a_fleet_record_it_is_declared_not_observed(self):
        c = _run().to_dict()["experiment"]["corpus"]
        assert c["observed"] is False and "declared" in c["observed_note"]

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
# E3, E4, E5, E8: stamps
# ---------------------------------------------------------------------------


class TestStamps:
    def test_local_run_stamps_what_ran(self):
        run = _run(_cfg(query_engine={"type": "trino"}))
        run.config_snapshot["local"] = True
        e = run.to_dict()["experiment"]
        assert e["system"] == "local"
        assert e["architecture"]["query_engine"]["type"] == "duckdb"
        assert e["architecture"]["query_access_path"] == "direct_storage"
        assert "not_supported" in e["effective_maintenance"]["id"]
        # The system is its own OD-2 group, not an execution condition.
        cluster = _run(_cfg(query_engine={"type": "duckdb"})).to_dict()
        assert not any(d.startswith("system") for d in ex.like_for_like(run.to_dict(), cluster))

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
        assert any("concurrent executor budget" in b for b in lim["bound"])
        assert any("over capacity" in b for b in lim["bound"])

    def test_iterations_and_bound_limits_are_conditions(self):
        ra, rb = _run(_cfg(benchmark={"iterations": 1})), _run(_cfg(benchmark={"iterations": 3}))
        ra.benchmark.iterations, rb.benchmark.iterations = 1, 3
        a, b = ra.to_dict(), rb.to_dict()
        assert ex.refusals(a, b)[0] == []
        assert any(d.startswith("benchmark iterations") for d in ex.like_for_like(a, b))

    def test_hive_version_is_the_stackable_image(self):
        v = ex.experiment_inputs(_cfg())["architecture"]["catalog"]["version"]
        assert v.startswith("oci.stackable.tech/sdp/hive:3.1.3-stackable25.7.0")


# ---------------------------------------------------------------------------
# Reviewer extras: stale block, package usability, maintenance never reached
# ---------------------------------------------------------------------------


class TestBlockFollowsTheRecord:
    def test_benchmark_rewrite_updates_the_block(self, tmp_path):
        from lakebench.metrics.storage import MetricsStorage

        run = _run()
        run.benchmark = None
        storage = MetricsStorage(tmp_path)
        storage.save_run(run)
        loaded = storage.load_run(run.run_id)
        assert loaded.to_dict()["experiment"]["results"]["not_checked"]
        loaded.benchmark = _run().benchmark  # what `lakebench benchmark` does
        stored = copy.deepcopy(loaded.to_dict()["experiment"])
        assert stored["results"]["not_checked"], "a stored block is never rebuilt"
        ex.refresh_benchmark(loaded)  # and then this (cli/_query.py)
        storage.save_run(loaded)
        e = storage.load_run(run.run_id).to_dict()["experiment"]
        assert "not_checked" not in e["results"] and e["results"]["fingerprints"]
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


class TestPackageUsability:
    def test_package_refuses_an_unusable_fingerprint(self):
        from lakebench.cli._reproduce import ReproduceError, _build_package
        from tests.test_reproduce import _metrics

        exp = stub_experiment(["Q1"])
        exp["results"]["fingerprints"]["Q1"] = {"spec": "rf2", "error": "timed out"}
        with pytest.raises(ReproduceError, match="usable result fingerprint"):
            _build_package(_metrics(experiment=exp), config_reference="c.yaml", commit_sha="abc")


class TestRunLocalIds:
    def test_fq8_alert_id_is_volatile_and_its_pick_is_deterministic(self):
        """alert_id is uuid() per pipeline run: two runs on one corpus must
        still match, and the LIMIT must not pick among tied alerts by it."""
        from lakebench.benchmark.queries import get_benchmark_queries
        from lakebench.config.schema import WorkloadSchema

        fq8 = next(
            q for q in get_benchmark_queries(WorkloadSchema.FINANCIAL) if q.name.startswith("FQ8")
        )
        assert "ORDER BY alert_ts DESC, entity_id, rule_id, alert_id" in fq8.sql
        cols = fq8.fingerprint_columns()
        run1 = fingerprint_rows([("uuid-a", "2026-01-01 00:00:00", "acme")], cols)
        run2 = fingerprint_rows([("uuid-b", "2026-01-01 00:00:00", "acme")], cols)
        assert mismatch(run1, run2) is None
        other = fingerprint_rows([("uuid-b", "2026-01-01 00:00:00", "other")], cols)
        assert mismatch(run1, other)
        assert mismatch(run1, fingerprint_rows([(None, "2026-01-01 00:00:00", "acme")], cols))


# ---------------------------------------------------------------------------
# Fix-pass review and live run 61489ab (M1-M3)
# ---------------------------------------------------------------------------


class TestFixPass:
    def test_continuous_run_without_usable_fingerprints_is_not_a_baseline_or_package(self):
        # The continuous deviation is removed: rounds read tables still being
        # written, so a continuous run is comparable only through its
        # end-of-run result check, never on aggregated round results.
        from lakebench.cli._reproduce import ReproduceError, _build_package
        from lakebench.metrics import perf_gate as pg
        from tests.test_reproduce import _metrics

        exp = stub_experiment(["Q1_full_aggregation_scan"], mode="sustained")
        exp["results"]["fingerprints"] = {"Q1_full_aggregation_scan": None}  # aggregated rounds
        exp["results"]["by_design"] = True  # the old deviation's marker no longer excuses it
        run = SimpleNamespace(
            raw={"experiment": exp, "provenance": {"deps": {"pinset_sha256": "a" * 64}}},
            run_id="r",
            mode="sustained",
            scores={},
        )
        with (
            mock.patch.object(pg, "run_refusals", return_value=[]),
            mock.patch.object(pg.BaselineStore, "pinned", return_value=SimpleNamespace()),
            pytest.raises(pg.PerfGateError, match="usable result fingerprint"),
        ):
            store = pg.BaselineStore(path=None, baselines={"x": SimpleNamespace(accepted=False)})
            pg.record_baseline(store, "x", run, "abc")
        pb = SimpleNamespace(
            pipeline_mode="sustained",
            ingest_ratio=1.0,
            sustained_throughput_rps=1.0,
            data_freshness_seconds=1.0,
            compute_efficiency_gb_per_core_hour=1.0,
            post_compaction_qph=0.0,
            query_benchmark=None,
            stages=[],
        )
        with pytest.raises(ReproduceError, match="usable result fingerprint"):
            _build_package(
                _metrics(experiment=exp, pipeline_benchmark=pb),
                config_reference="c",
                commit_sha="a",
            )

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
        assert not [d for d in ex.like_for_like(a, b) if d.startswith("Lakebench limits")]

    def test_iterations_come_from_the_recorded_benchmark(self):
        r = _run(_cfg(benchmark={"iterations": 3}))
        r.benchmark.iterations = 5
        assert r.to_dict()["experiment"]["limits"]["benchmark_iterations"] == 5

    def test_one_row_approx_tolerance_is_a_few_quanta(self):
        a = fingerprint_rows([("x", 10.0)], {1: 1.0})
        assert mismatch(a, fingerprint_rows([("x", 21.0)], {1: 1.0}))
        assert mismatch(a, fingerprint_rows([("x", 12.0)], {1: 1.0})) is None

    def test_old_stored_identity_gets_one_clear_message(self):
        exp = stub_experiment(["Q1"])
        old = {k: v for k, v in ex.identity(exp).items() if k != "system"}
        refs = ex.stored_identity_refusals(old, ex.result_fingerprints(exp), exp, "baseline")
        assert len(refs) == 1 and "record it again" in refs[0]

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


class TestLiveRun61489ab:
    def test_batch_c360_maintenance_skips_the_continuous_bronze_table(self):
        from lakebench.cli._sustained import maintained_tables

        batch = maintained_tables(_cfg())
        assert not any("bronze" in t for t in batch)
        assert any("bronze" in t for t in maintained_tables(_cfg(pipeline={"mode": "sustained"})))
        fin = maintained_tables(_cfg(workload={"schema": "financial", "datagen": {"scale": 1}}))
        assert any(t.startswith("bronze") for t in fin)

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
