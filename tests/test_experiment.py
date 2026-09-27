"""The metrics.json experiment block and the comparisons that read it.

Mission outcome 3 / invariant 5: every result names what produced it.
Invariant 2: runs are compared on performance only when they are the same
experiment and returned the same results. Old records without the block are
"not comparable: no provenance", never a crash and never a silent pass.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from lakebench.benchmark.fingerprint import fingerprint_rows
from lakebench.metrics import experiment as ex
from lakebench.metrics.collector import (
    BenchmarkMetrics,
    JobMetrics,
    MetricsCollector,
    build_config_snapshot,
)
from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance
from tests.conftest import make_config

ROOT = Path(__file__).resolve().parents[1]


def _cfg(schema="customer360", mode="batch", engine="trino", fmt="iceberg", **datagen):
    return make_config(
        architecture={
            "workload": {"schema": schema, "datagen": {"scale": 1, **datagen}},
            "pipeline": {"mode": mode},
            "query_engine": {"type": engine},
            "table_format": {"type": fmt},
        }
    )


def _fp(value: int = 1) -> dict:
    return fingerprint_rows([(value, "x")], engine="trino", adapted_sql="SELECT 1")


def _metrics(cfg, fingerprints: dict | None = None, fleet: dict | None = None):
    run = MetricsCollector().start_run(
        "20260926-120000-aaaaaa", cfg.name, build_config_snapshot(cfg)
    )
    fps = fingerprints if fingerprints is not None else {"Q1_full_aggregation_scan": _fp()}
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
    return run


class TestStamping:
    @pytest.mark.parametrize("schema", ["customer360", "financial"])
    @pytest.mark.parametrize("mode", ["batch", "sustained"])
    def test_every_record_carries_the_block(self, schema, mode):
        cfg = _cfg(schema, mode)
        d = _metrics(cfg).to_dict()
        e = d["experiment"]
        assert e["schema"] == ex.EXPERIMENT_SCHEMA
        assert e["workload"]["name"] == schema
        assert e["workload"]["version"] == ex.WORKLOAD_VERSIONS[schema]
        assert e["corpus"]["seed"] is not None
        assert e["corpus"]["generator_image"] == cfg.images.datagen
        assert e["corpus"]["scale"] == 1
        assert e["mode"] == mode
        assert e["architecture"]["recipe"] == "hive-iceberg-spark-trino"
        assert e["architecture"]["query_access_path"] == "catalog"
        assert e["architecture"]["table_format"]["version"]
        assert e["maintenance_policy_id"] == MAINTENANCE_POLICY_ID
        assert e["effective_maintenance"]["id"].startswith(MAINTENANCE_POLICY_ID)
        assert e["lakebench"]["lakebench_version"]
        assert "executors" in e["limits"]
        if schema == "financial":
            assert e["workload"]["generator_model_version"] == ex.DATAGEN_MODEL_VERSIONS["financial"]
            assert "corpus_role" in e["corpus"]
            assert e["limits"]["w1_max_vertices"] == cfg.architecture.workload.w1_max_vertices
        if mode == "sustained":
            assert e["limits"]["max_files_per_trigger"] is not None
            assert e["results"]["not_checked"]
        else:
            assert e["results"]["fingerprints"]["Q1_full_aggregation_scan"]["rows"] == 1

    def test_block_survives_a_save_and_load(self, tmp_path):
        from lakebench.metrics.storage import MetricsStorage

        run = _metrics(_cfg())
        storage = MetricsStorage(tmp_path)
        storage.save_run(run)
        assert storage.load_run(run.run_id).to_dict()["experiment"] == run.to_dict()["experiment"]

    def test_python_model_version_matches_the_generator_source(self):
        src = (ROOT / "datagen_rs" / "src" / "model.rs").read_text()
        m = re.search(r'pub const MODEL_VERSION: &str = "([^"]+)";', src)
        assert m and ex.DATAGEN_MODEL_VERSIONS["financial"] == m.group(1)

    def test_duckdb_reads_storage_directly(self):
        e = _metrics(_cfg(engine="duckdb")).to_dict()["experiment"]
        assert e["architecture"]["query_access_path"] == "direct_storage"

    def test_datagen_digest_from_pod_status(self):
        fleet = {
            "image": "docker.io/sillidata/lb-datagen:14c4eee",
            "image_ids": ["docker.io/sillidata/lb-datagen@sha256:abc"],
        }
        dg = _metrics(_cfg(), fleet=fleet).to_dict()["experiment"]["corpus"]["datagen"]
        assert dg["digest"] == "sha256:abc" and "digest_reason" not in dg

    def test_missing_or_mixed_digest_is_null_with_a_reason(self):
        dg = _metrics(_cfg()).to_dict()["experiment"]["corpus"]["datagen"]
        assert dg["digest"] is None and "no datagen fleet" in dg["digest_reason"]
        mixed = {"image_ids": ["r@sha256:a", "r@sha256:b"]}
        dg = _metrics(_cfg(), fleet=mixed).to_dict()["experiment"]["corpus"]["datagen"]
        assert dg["digest"] is None and "different images" in dg["digest_reason"]

    def test_fleet_records_pod_image_ids(self):
        from lakebench.metrics.datagen_aggregator import collect_from_pod_logs

        s = collect_from_pod_logs(
            {"p0": "", "p1": ""},
            pod_images={"p0": ("img:1", "img@sha256:x"), "p1": ("img:1", "img@sha256:x")},
        )
        assert s.to_dict()["image"] == "img:1"
        assert s.to_dict()["image_ids"] == ["img@sha256:x"]

    def test_rules_and_executor_caps_are_recorded(self):
        cfg = _cfg("financial", scale=1000)
        run = _metrics(cfg)
        run.jobs.append(
            JobMetrics(
                job_name="g",
                job_type="gold-finalize",
                success=True,
                alerts_by_rule={"W2": 5, "W3": 0},
                rules_skipped={"W1": "vertex-cap"},
            )
        )
        e = run.to_dict()["experiment"]
        assert e["rules"]["executed"] == ["W2", "W3"]
        assert e["rules"]["skipped"] == {"W1": "vertex-cap"}
        gold = next(x for x in e["limits"]["executors"] if x["job_type"] == "gold-finalize")
        assert gold["cap_hit"] and gold["scale_derived"] > gold["cap"]
        # Invariant 4: the bound limits are named, not left to be inferred.
        bound = e["limits"]["bound"]
        assert any(b.startswith("gold-finalize: executor cap") for b in bound), bound
        assert any("W1" in b and "vertex-cap" in b for b in bound), bound

    def test_support_state_and_repetitions_are_stamped(self):
        e = _metrics(_cfg()).to_dict()["experiment"]
        assert e["support"]["state"] == "unverified"
        assert e["repetitions"]["runs"] == 1
        assert e["repetitions"]["benchmark_samples_per_query"] == 1
        key = ("customer360", "hive-iceberg-spark-trino", "batch")
        with mock.patch.object(ex, "RELEASE_VALIDATED", frozenset({key})):
            assert _metrics(_cfg()).to_dict()["experiment"]["support"]["state"] == "supported"

    def test_autosize_cuts_are_a_recorded_limit(self):
        run = _metrics(_cfg())
        run.autosize_cuts = ["trino workers 4 -> 2 to fit the cluster"]
        assert run.to_dict()["experiment"]["limits"]["autosize_cuts"] == run.autosize_cuts

    def test_report_shows_the_block(self, tmp_path):
        from lakebench.reports.generator import ReportGenerator

        html = ReportGenerator(output_dir=tmp_path)._generate_experiment_section(_metrics(_cfg()))
        assert "Experiment" in html and "c360-1" in html and "Result fingerprints" in html
        for label in (
            "Query access path",
            "Support state",
            "Maintenance (effective",
            "Repetitions",
        ):
            assert label in html

    def test_report_says_when_a_record_has_no_provenance(self, tmp_path):
        from lakebench.reports.generator import ReportGenerator

        run = MetricsCollector().start_run("r", "d", {})
        html = ReportGenerator(output_dir=tmp_path)._generate_experiment_section(run)
        assert "No provenance" in html


class TestLegacy:
    def test_record_without_the_block_never_gets_one(self, tmp_path):
        from lakebench.metrics.storage import MetricsStorage

        storage = MetricsStorage(tmp_path)
        path = storage.save_run(_metrics(_cfg()))
        raw = json.loads(path.read_text())
        del raw["experiment"]
        del raw["config_snapshot"]["experiment_inputs"]
        path.write_text(json.dumps(raw))
        loaded = storage.load_run(raw["run_id"])
        assert "experiment" not in loaded.to_dict()

    def test_compare_reports_no_provenance(self):
        new = _metrics(_cfg()).to_dict()
        old = {"run_id": "old-1", "pipeline_benchmark": {}}
        prov, results, _ = ex.refusals(old, new)
        assert prov and prov[0].startswith(ex.NO_PROVENANCE) and "old-1" in prov[0]
        assert results == []


class TestRefusals:
    def test_the_same_experiment_with_the_same_results_compares(self):
        a, b = _metrics(_cfg()).to_dict(), _metrics(_cfg()).to_dict()
        assert ex.refusals(a, b) == ([], [], [])

    @pytest.mark.parametrize(
        "other,field",
        [
            ({"seed": 7}, "seed"),
            ({"scale": 2}, "scale"),
            ({"schema": "financial"}, "workload"),
            ({"mode": "sustained"}, "mode"),
        ],
    )
    def test_a_different_experiment_is_refused(self, other, field):
        schema = other.pop("schema", "customer360")
        mode = other.pop("mode", "batch")
        a = _metrics(_cfg()).to_dict()
        b = _metrics(_cfg(schema, mode, **other)).to_dict()
        prov, _, _ = ex.refusals(a, b)
        assert any(p.startswith(field) for p in prov), prov

    def test_different_results_name_the_query_and_both_fingerprints(self):
        a = _metrics(_cfg(), {"Q2_filtered_aggregation": _fp(455)}).to_dict()
        b = _metrics(_cfg(), {"Q2_filtered_aggregation": _fp(470)}).to_dict()
        prov, results, _ = ex.refusals(a, b)
        assert prov == []
        assert len(results) == 1
        line = results[0]
        assert "Q2_filtered_aggregation" in line
        assert _fp(455)["exact"] in line and _fp(470)["exact"] in line

    def test_a_query_without_a_fingerprint_is_not_shown_equal(self):
        a = _metrics(_cfg(), {"Q1": _fp()}).to_dict()
        b = _metrics(_cfg(), {"Q1": None}).to_dict()
        _, results, _ = ex.refusals(a, b)
        assert results and "Q1" in results[0]

    def test_trino_vs_duckdb_is_comparable_but_not_like_for_like(self):
        """DESIGN 6.5: matching results make the pair comparable; the
        different effective maintenance and access path make it not
        like-for-like. Neither is a refusal."""
        a = _metrics(_cfg(engine="trino")).to_dict()
        b = _metrics(_cfg(engine="duckdb")).to_dict()
        prov, results, _ = ex.refusals(a, b)
        assert prov == [] and results == []
        conditions = ex.like_for_like(a, b)
        assert any(c.startswith("effective maintenance") for c in conditions), conditions
        assert any(c.startswith("query access path") for c in conditions), conditions

    def test_stored_references_refuse_on_conditions(self):
        """The perf gate and reproduce need the same experiment under the same
        conditions: a condition difference refuses there."""
        a = ex.experiment_of(_metrics(_cfg(engine="trino")).to_dict())
        b = ex.experiment_of(_metrics(_cfg(engine="duckdb")).to_dict())
        reasons = ex.stored_identity_refusals(
            ex.identity(a), ex.result_fingerprints(a), b, "baseline"
        )
        assert any(r.startswith("effective maintenance") for r in reasons), reasons

    def test_continuous_results_are_noted_not_refused(self):
        a = _metrics(_cfg(mode="sustained")).to_dict()
        b = _metrics(_cfg(mode="sustained")).to_dict()
        prov, results, notes = ex.refusals(a, b)
        assert prov == [] and results == [] and notes

    def test_query_set_change_is_refused(self):
        """Tiebreakers moved the query-set id: the old id never matches the new."""
        from lakebench.benchmark.queries import (
            LEGACY_QUERY_SET_IDS,
            get_benchmark_queries,
            qph_comparable,
            query_set_id,
        )
        from lakebench.config.schema import WorkloadSchema

        for schema in (WorkloadSchema.CUSTOMER360, WorkloadSchema.FINANCIAL):
            names = [q.name for q in get_benchmark_queries(schema)]
            current = query_set_id(names)
            for old, _ in LEGACY_QUERY_SET_IDS.values():
                ok, _why = qph_comparable(old, current)
                assert not ok


class TestEffectiveMaintenance:
    @pytest.mark.parametrize(
        "fmt,engine,mode,expire,compaction",
        [
            ("iceberg", "trino", "batch", True, True),
            ("iceberg", "duckdb", "batch", False, False),
            ("delta", "trino", "batch", True, False),
            ("delta", "spark-thrift", "batch", False, False),
            ("delta", "trino", "sustained", False, False),
            ("iceberg", "spark-thrift", "sustained", True, True),
        ],
    )
    def test_matches_what_the_run_paths_skip(self, fmt, engine, mode, expire, compaction):
        e = effective_maintenance(
            MAINTENANCE_POLICY_ID, table_format=fmt, query_engine=engine, mode=mode
        )
        # Coarse classes: what the composition can run, not whether it ran.
        ok = {True: "ran", False: "not_supported"}
        assert (e["expire"], e["compaction"]) == (ok[expire], ok[compaction])

    def test_recorded_outcomes_decide_what_ran(self):
        """Effective means what ran: a policy that asked for maintenance whose
        statements failed is not stamped as having had it. Partial success
        and stops are detail, not identity (one timed-out round in a long
        run is the same experiment)."""

        def eff(outcomes, **kw):
            return effective_maintenance(
                MAINTENANCE_POLICY_ID,
                table_format=kw.get("fmt", "iceberg"),
                query_engine="trino",
                mode=kw.get("mode", "batch"),
                outcomes=outcomes,
                stopped=kw.get("stopped", False),
            )

        ok = {"kind": "expire", "total": 4, "succeeded": 4}
        comp = {"kind": "compaction", "total": 2, "succeeded": 2}
        full = eff([ok, comp])
        assert full["id"].endswith("expire=ran,compaction=ran")
        failed = eff([{**ok, "succeeded": 0}, comp])
        assert "expire=failed" in failed["id"] and "0 of 4" in " ".join(failed["reasons"])
        partial = eff([{**ok, "succeeded": 3}, comp], stopped=True)
        assert partial["id"] == full["id"]
        assert "expire=partial" in partial["detail_id"] and partial["detail_id"].endswith("stopped")
        # The run ended before the maintenance phase.
        assert eff([])["id"].endswith("expire=not_run,compaction=not_run")
        # The user turned it off (--skip-benchmark, pre_benchmark_maintenance off).
        user = [{"kind": k, "user_skip": "--skip-benchmark"} for k in ("expire", "compaction")]
        assert eff(user)["id"].endswith("expire=skipped_by_user,compaction=skipped_by_user")
        # A crash in the maintenance phase is recorded, not assumed away.
        crashed = eff([{"kind": "maintenance", "error": "boom"}])
        assert "expire=failed" in crashed["id"] and any("boom" in r for r in crashed["reasons"])
        # Continuous: rounds that raised count as failed attempts.
        rounds = [{"kind": "expire", "error": "x"}] * 4 + [ok, comp]
        assert "expire=partial" in eff(rounds, mode="sustained")["detail_id"]
        # Outcomes never turn an operation up past the rules (Delta OPTIMIZE).
        assert "compaction=not_supported" in eff([ok, comp], fmt="delta")["id"]
        assert eff(None)["basis"].startswith("policy rules")

    def test_run_outcomes_reach_the_experiment_block(self):
        run = _metrics(_cfg())
        run.maintenance_outcomes = [{"kind": "expire", "total": 2, "succeeded": 0}]
        e = run.to_dict()["experiment"]["effective_maintenance"]
        assert "expire=failed" in e["id"] and e["basis"] == "recorded outcomes"

    def test_skip_flag_and_stop_show_in_the_id(self):
        skipped = effective_maintenance(
            MAINTENANCE_POLICY_ID + "+skipped", table_format="iceberg", query_engine="trino",
            mode="batch",
        )  # fmt: skip
        assert "expire=skipped_by_user" in skipped["id"]
        stopped = effective_maintenance(
            MAINTENANCE_POLICY_ID, table_format="iceberg", query_engine="trino", mode="batch",
            stopped=True,
        )  # fmt: skip
        assert stopped["detail_id"].endswith(",stopped") and "stopped" not in stopped["id"]


class TestCompareCommand:
    def _comparison(self, a, b):
        from lakebench.cli._compare import _build_comparison

        for m in (a, b):
            m.setdefault("pipeline_benchmark", {})["scores"] = {"composite_qph": 100.0}
        return _build_comparison("A", a, "B", b)

    def test_comparable_pair(self):
        c = self._comparison(_metrics(_cfg()).to_dict(), _metrics(_cfg()).to_dict())
        assert c["comparable"] is True and c["like_for_like"] is True
        assert not any(r.get("not_comparable") for r in c["metrics"])
        assert c["support"] == {"config_a": "unverified", "config_b": "unverified"}

    def test_matching_results_under_other_conditions_are_labelled(self, capsys):
        from lakebench.cli import _compare

        a = _metrics(_cfg(engine="trino")).to_dict()
        b = _metrics(_cfg(engine="duckdb")).to_dict()
        c = self._comparison(a, b)
        assert c["comparable"] is True and c["like_for_like"] is False
        assert c["condition_differences"]
        import io

        from rich.console import Console

        buf = io.StringIO()
        with mock.patch.object(_compare, "console", Console(file=buf, width=200)):
            _compare._print_comparison_table(c)
        text = buf.getvalue()
        assert "NOT LIKE-FOR-LIKE" in text and "NOT COMPARABLE" not in text
        assert "unverified" in text

    def test_a_failed_run_is_not_comparable(self):
        c = self._comparison(_metrics(_cfg()).to_dict(), {"error": "Run failed with exit code 1"})
        assert c["comparable"] is False
        assert "did not complete" in c["refusals"]["provenance"][0]

    def test_legacy_records_are_not_comparable_and_do_not_crash(self):
        a = _metrics(_cfg()).to_dict()
        b = dict(a)
        b.pop("experiment")
        c = self._comparison(a, b)
        assert c["comparable"] is False
        assert ex.NO_PROVENANCE in c["refusals"]["provenance"][0]

    def test_non_comparable_pair_keeps_the_numbers_labelled(self):
        a = _metrics(_cfg(), {"Q1": _fp(1)}).to_dict()
        b = _metrics(_cfg(), {"Q1": _fp(2)}).to_dict()
        c = self._comparison(a, b)
        assert c["comparable"] is False
        assert c["refusals"]["results"]
        assert c["metrics"] and all(r["not_comparable"] for r in c["metrics"])

    def test_table_withholds_deltas(self, capsys):
        from lakebench.cli import _compare

        a = _metrics(_cfg(), {"Q1": _fp(1)}).to_dict()
        b = _metrics(_cfg(), {"Q1": _fp(2)}).to_dict()
        a["pipeline_benchmark"] = {"scores": {"composite_qph": 100.0}}
        b["pipeline_benchmark"] = {"scores": {"composite_qph": 200.0}}
        import io

        from rich.console import Console

        buf = io.StringIO()
        with mock.patch.object(_compare, "console", Console(file=buf, width=200)):
            _compare._print_comparison_table(_compare._build_comparison("A", a, "B", b))
        text = buf.getvalue()
        assert "NOT COMPARABLE" in text and "Q1" in text
        assert "not comparable" in text
        assert "+100.0%" not in text

    def _invoke(self, tmp_path, metrics_a, metrics_b, cfg_b=None):
        from lakebench.cli import app

        cfg_a = _cfg()
        cfg_b = cfg_b or _cfg()
        for p in ("a.yaml", "b.yaml"):
            (tmp_path / p).write_text("name: x\n")
        with (
            mock.patch("lakebench.cli._compare.load_config", side_effect=[cfg_a, cfg_b]),
            mock.patch("lakebench.cli._compare._run_single", side_effect=[metrics_a, metrics_b]),
            mock.patch("lakebench.cli._compare.DEFAULT_OUTPUT_DIR", str(tmp_path / "out")),
        ):
            return CliRunner().invoke(
                app, ["compare", str(tmp_path / "a.yaml"), str(tmp_path / "b.yaml"), "--yes"]
            )

    def test_exit_code_is_non_zero_when_not_comparable(self, tmp_path):
        a = _metrics(_cfg(), {"Q1": _fp(1)}).to_dict()
        b = _metrics(_cfg(), {"Q1": _fp(2)}).to_dict()
        result = self._invoke(tmp_path, a, b)
        assert result.exit_code == 1, result.output
        saved = next((tmp_path / "out" / "comparisons").glob("*/comparison.json"))
        assert json.loads(saved.read_text())["comparable"] is False

    def test_exit_code_is_zero_when_comparable(self, tmp_path):
        a, b = _metrics(_cfg()).to_dict(), _metrics(_cfg()).to_dict()
        result = self._invoke(tmp_path, a, b)
        assert result.exit_code == 0, result.output

    def test_different_configs_are_flagged_before_running(self, tmp_path):
        a, b = _metrics(_cfg()).to_dict(), _metrics(_cfg(seed=7)).to_dict()
        result = self._invoke(tmp_path, a, b, cfg_b=_cfg(seed=7))
        assert "NOT COMPARABLE" in result.output
        assert result.exit_code == 1


class TestBenchmarkGateEmptyResults:
    def test_a_query_that_returned_no_rows_fails_the_gate(self):
        from lakebench.cli._run import _benchmark_gate_problems

        cfg = _cfg()
        qs = [
            {"name": "Q1_full_aggregation_scan", "success": True, "rows_returned": 0},
            {"name": "Q2_filtered_aggregation", "success": True, "rows_returned": 5},
        ]
        problems = _benchmark_gate_problems(cfg, qs)
        assert problems and "Q1_full_aggregation_scan" in problems[0]
        assert "no rows" in problems[0]

    def test_an_allowed_empty_query_passes(self):
        from lakebench.benchmark.queries import BENCHMARK_QUERIES_BY_DOMAIN, BenchmarkQuery
        from lakebench.cli._run import _benchmark_gate_problems
        from lakebench.config.schema import WorkloadSchema

        q = BenchmarkQuery("QX_may_be_empty", "x", "scan", "SELECT 1", allow_empty=True)
        qs = [{"name": q.name, "success": True, "rows_returned": 0}]
        fin = BENCHMARK_QUERIES_BY_DOMAIN[WorkloadSchema.FINANCIAL]
        with mock.patch.dict(BENCHMARK_QUERIES_BY_DOMAIN, {WorkloadSchema.FINANCIAL: [*fin, q]}):
            assert _benchmark_gate_problems(_cfg("financial"), qs) == []
        # Without the declaration the same result fails the gate.
        assert _benchmark_gate_problems(_cfg("financial"), qs)

    def test_only_the_case_queries_are_declared_allowed_empty(self):
        """IQ2 and IQ4 need TM cases a small or short run may not have."""
        from lakebench.benchmark.queries import BENCHMARK_QUERIES_BY_DOMAIN

        declared = {
            q.name for qs in BENCHMARK_QUERIES_BY_DOMAIN.values() for q in qs if q.allow_empty
        }
        assert declared == {"IQ2_case_activity_12m", "IQ4_open_cases_over_60_days"}
