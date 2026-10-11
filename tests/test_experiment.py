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

import lakebench
from lakebench.config.datagen_seed import config_seed
from lakebench.config.recipes import RECIPES
from lakebench.config.support import resolved_format_version
from lakebench.metrics import experiment as ex
from lakebench.metrics.collector import (
    JobMetrics,
    MetricsCollector,
    build_config_snapshot,
)
from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance
from lakebench.metrics.seed_record import recorded_seed
from tests.conftest import make_config
from tests.fixtures.experiment_helpers import _cfg as _cfg
from tests.fixtures.experiment_helpers import _metrics as _metrics

ROOT = Path(__file__).resolve().parents[1]


def _cfg_spark41():
    """The default C360 batch config on the Spark 4.1 image, the release
    matrix's version for hive-iceberg-spark-trino."""
    return make_config(
        images={"spark": "apache/spark:4.1.1-python3"},
        architecture={"workload": {"schema": "customer360", "datagen": {"scale": 1}}},
    )


class TestStamping:
    @pytest.mark.parametrize("schema", ["customer360", "financial"])
    @pytest.mark.parametrize("mode", ["batch", "sustained"])
    def test_every_record_carries_the_block(self, schema, mode):
        cfg = _cfg(schema, mode)
        run = _metrics(cfg)
        d = run.to_dict()
        e = d["experiment"]
        # No corpus observation and no system identity in this synthetic
        # run: identity v1, naming what v2 lacked (ER-10a stamping rule).
        assert e["schema"] == ex.EXPERIMENT_SCHEMA_V1
        assert e["v2_unavailable"] == ["corpus id v2", "system identity"]
        assert e["corpus"]["id_v2"] is None and e["corpus"]["id_v2_unavailable"]
        assert e["architecture"]["access_paths"] == {"pipeline": "catalog", "query": "catalog"}
        assert e["workload"]["name"] == schema
        assert e["workload"]["version"] == ex.WORKLOAD_VERSIONS[schema]
        assert e["corpus"]["seed"] == recorded_seed(config_seed(cfg))
        assert e["corpus"]["generator_image"] == cfg.images.datagen
        assert e["corpus"]["scale"] == 1
        assert e["mode"] == mode
        assert e["architecture"]["recipe"] == "hive-iceberg-spark-trino"
        assert e["architecture"]["query_access_path"] == "catalog"
        assert e["architecture"]["table_format"]["version"] == resolved_format_version(cfg)
        assert e["maintenance_policy_id"] == MAINTENANCE_POLICY_ID
        assert e["effective_maintenance"]["id"].startswith(MAINTENANCE_POLICY_ID)
        assert e["lakebench"]["lakebench_version"] == lakebench.__version__
        if schema == "financial":
            assert (
                e["workload"]["generator_model_version"] == ex.DATAGEN_MODEL_VERSIONS["financial"]
            )
            assert "corpus_role" in e["corpus"]
            assert e["limits"]["w1_max_vertices"] == cfg.architecture.workload.w1_max_vertices
        if mode == "sustained":
            # The cap label: the config's value, None when no per-trigger limit applies.
            assert (
                e["limits"]["max_files_per_trigger"]
                == cfg.architecture.pipeline.sustained.max_files_per_trigger
            )
        else:
            assert e["results"]["query_set_id"] == run.benchmark.query_set_id

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

    def test_rules_are_recorded(self):
        run = _metrics(_cfg("financial", scale=1000))
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
        # Invariant 4: the bound limits are named, not left to be inferred.
        assert any("W1" in b and "vertex-cap" in b for b in e["limits"]["bound"])

    @pytest.mark.parametrize(
        ("mode", "scale", "job"),
        [("batch", 1000, "gold-finalize"), ("continuous", 100, "gold-refresh")],
    )
    def test_a_stage_the_executor_cap_cut_is_labelled(self, mode, scale, job):
        """Invariant 6: a stage whose scale asks for more executors than its
        cap (AML gold-refresh at scale 100 asks for more than 28) is labelled
        capped, beside its numbers."""
        from lakebench.metrics.collector import StreamingJobMetrics

        run = _metrics(_cfg("financial", mode, scale=scale))
        if mode == "batch":
            run.jobs.append(JobMetrics(job_name="g", job_type=job, success=True))
        else:
            run.streaming.append(StreamingJobMetrics(job_name="g", job_type=job))
        e = run.to_dict()["experiment"]
        row = next(x for x in e["limits"]["executors"] if x["job_type"] == job)
        assert row["cap_hit"] and row["scale_derived"] > row["cap"]
        assert any(b.startswith(f"{job}: executor cap") for b in e["limits"]["bound"])

    def test_support_state_and_repetitions_are_stamped(self):
        e = _metrics(_cfg()).to_dict()["experiment"]
        assert e["support"]["state"] == "unverified"
        assert e["repetitions"]["runs"] == 1
        assert e["repetitions"]["benchmark_samples_per_query"] == 1

    def test_support_is_supported_only_when_the_record_lists_it(self, tmp_path):
        from lakebench.config import support
        from lakebench.metrics import provenance

        clean = {"lakebench_version": "x", "git_sha": "abc1234", "git_dirty": False}
        rec = tmp_path / "validated_combinations.yaml"
        rec.write_text(
            "validated:\n"
            "  - {workload: customer360, recipe: hive-iceberg-spark-trino, mode: batch,\n"
            "     spark: '4.1', table_format_version: 1.11.0,\n"
            "     runs: [run-1]}\n"
        )
        with (
            mock.patch.object(support, "VALIDATION_RECORD", rec),
            mock.patch.object(provenance, "run_provenance", lambda: clean),
        ):
            run = _metrics(_cfg_spark41())
            s = run.to_dict()["experiment"]["support"]
            assert s["state"] == "supported"
            assert s["validation_runs"] == ["run-1"] and "this release" in s["basis"]
            # A modified tree is not the validated code.
            dirty = dict(clean, git_dirty=True)
            with mock.patch.object(provenance, "run_provenance", lambda: dirty):
                s = _metrics(_cfg_spark41()).to_dict()["experiment"]["support"]
            assert s["state"] == "unverified" and "modified tree" in s["basis"]
            unknown = dict(clean, git_dirty=None)
            with mock.patch.object(provenance, "run_provenance", lambda: unknown):
                s = _metrics(_cfg_spark41()).to_dict()["experiment"]["support"]
            assert s["state"] == "unverified"
            wheel = dict(clean, git_sha=None, git_dirty=None)
            with mock.patch.object(provenance, "run_provenance", lambda: wheel):
                s = _metrics(_cfg_spark41()).to_dict()["experiment"]["support"]
            assert s["state"] == "supported"
        # Frozen at run start: re-rendering after the record changes keeps it.
        assert run.to_dict()["experiment"]["support"]["state"] == "supported"
        # Listed for another mode only: still unverified.
        rec.write_text(rec.read_text().replace("mode: batch", "mode: continuous"))
        with (
            mock.patch.object(support, "VALIDATION_RECORD", rec),
            mock.patch.object(provenance, "run_provenance", lambda: clean),
        ):
            s = _metrics(_cfg_spark41()).to_dict()["experiment"]["support"]
            assert s["state"] == "unverified"
            # A record from before the state was frozen is never re-stamped
            # supported, whatever the installed record now says.
            rec.write_text(rec.read_text().replace("mode: continuous", "mode: batch"))
            old = _metrics(_cfg_spark41())
            old.config_snapshot["experiment_inputs"].pop("support")
            s = old.to_dict()["experiment"]["support"]
            assert s["state"] == "unverified" and "not recorded at run start" in s["basis"]

    def test_run_mode_from_the_flag_is_stamped(self):
        """run --continuous does not write the mode back; a continuous run
        that failed before any stream was recorded is still continuous."""

        cfg = _cfg()
        run = MetricsCollector().start_run(
            "20260926-120000-bbbbbb", cfg.name, build_config_snapshot(cfg, run_mode="continuous")
        )
        e = run.to_dict()["experiment"]
        assert e["mode"] == "sustained"
        assert e["support"]["mode"] == "continuous"
        # run sets continuous on its copy of the config so sizing sizes for
        # it: the record is then the continuous config's, same identity, and
        # says the mode came from the command line.
        from lakebench.cli._run import apply_run_mode

        def block(cfg):
            snap = build_config_snapshot(cfg, run_mode="continuous")
            return MetricsCollector().start_run("r", cfg.name, snap).to_dict()["experiment"]

        moved = _cfg()
        apply_run_mode(moved, "continuous")
        flag, configured = block(moved), block(_cfg(mode="continuous"))
        assert ex.identity_hash(flag) == ex.identity_hash(configured)
        assert flag["requested_effective"]["pipeline_mode"]["source"] == "command line"

    @pytest.mark.parametrize("recipe", sorted(n for n in RECIPES if n != "default"))
    def test_stamped_recipe_is_a_recipe_name(self, recipe):
        cfg = make_config(
            recipe=recipe,
            architecture={"workload": {"schema": "customer360", "datagen": {"scale": 1}}},
        )
        assert ex.experiment_inputs(cfg)["architecture"]["recipe"] == recipe

    def test_autosize_cuts_are_a_recorded_limit(self):
        run = _metrics(_cfg())
        run.autosize_cuts = ["trino workers 4 -> 2 to fit the cluster"]
        assert run.to_dict()["experiment"]["limits"]["autosize_cuts"] == run.autosize_cuts


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


class TestQuerySetId:
    def test_query_set_change_moves_the_id(self):
        """Tiebreakers moved the query-set id: the old id never matches the new."""
        from lakebench.benchmark.queries import (
            LEGACY_QUERY_SET_IDS,
            get_benchmark_queries,
            query_set_id,
        )
        from lakebench.config.schema import WorkloadSchema

        for schema in (WorkloadSchema.CUSTOMER360, WorkloadSchema.FINANCIAL):
            current = query_set_id([q.name for q in get_benchmark_queries(schema)])
            for old, _ in LEGACY_QUERY_SET_IDS.values():
                assert old != current


class TestEffectiveMaintenance:
    @pytest.mark.parametrize(
        "fmt,engine,mode,expire,compaction",
        [
            ("iceberg", "trino", "batch", True, True),
            ("iceberg", "duckdb", "batch", False, False),
            ("delta", "trino", "batch", True, False),
            ("delta", "spark-thrift", "batch", False, False),
            # Continuous Delta VACUUM executes on Trino but at the 7 d
            # default, so nothing written in the window is eligible.
            ("delta", "trino", "sustained", "no_effect", False),
            ("delta", "spark-thrift", "sustained", False, False),
            ("iceberg", "spark-thrift", "sustained", True, True),
        ],
    )
    def test_matches_what_the_run_paths_skip(self, fmt, engine, mode, expire, compaction):
        e = effective_maintenance(
            MAINTENANCE_POLICY_ID, table_format=fmt, query_engine=engine, mode=mode
        )
        # Coarse classes: what the composition can run, not whether it ran.
        ok = {True: "ran", False: "not_supported", "no_effect": "ran_no_effect"}
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
        assert full["id"].endswith("expire_snapshots=ran,remove_orphan_files=ran,compaction=ran")
        failed = eff([{**ok, "succeeded": 0}, comp])
        assert "expire_snapshots=failed,remove_orphan_files=failed" in failed["id"]
        assert "0 of 4" in " ".join(failed["reasons"])
        partial = eff([{**ok, "succeeded": 3}, comp], stopped=True)
        assert partial["id"] == full["id"]
        assert "expire_snapshots=partial" in partial["detail_id"]
        assert partial["detail_id"].endswith("stopped")
        # The run ended before the maintenance phase.
        assert eff([])["id"].endswith(
            "expire_snapshots=not_run,remove_orphan_files=not_run,compaction=not_run"
        )
        # The user turned it off (--skip-benchmark, pre_benchmark_maintenance off).
        user = [{"kind": k, "user_skip": "--skip-benchmark"} for k in ("expire", "compaction")]
        assert eff(user)["id"].endswith(
            "remove_orphan_files=skipped_by_user,compaction=skipped_by_user"
        )
        # A crash in the maintenance phase is recorded, not assumed away.
        crashed = eff([{"kind": "maintenance", "error": "boom"}])
        assert "expire_snapshots=failed" in crashed["id"] and any(
            "boom" in r for r in crashed["reasons"]
        )
        # Continuous: rounds that raised count as failed attempts.
        rounds = [{"kind": "expire", "error": "x"}] * 4 + [ok, comp]
        assert "remove_orphan_files=partial" in eff(rounds, mode="sustained")["detail_id"]
        # Outcomes never turn an operation up past the rules (Delta OPTIMIZE).
        assert "compaction=not_supported" in eff([ok, comp], fmt="delta")["id"]
        assert eff(None)["basis"].startswith("policy rules")

    def test_run_outcomes_reach_the_experiment_block(self):
        run = _metrics(_cfg())
        run.maintenance_outcomes = [{"kind": "expire", "total": 2, "succeeded": 0}]
        e = run.to_dict()["experiment"]["effective_maintenance"]
        assert "expire_snapshots=failed" in e["id"] and e["basis"] == "recorded outcomes"

    def test_skip_flag_and_stop_show_in_the_id(self):
        skipped = effective_maintenance(
            MAINTENANCE_POLICY_ID + "+skipped", table_format="iceberg", query_engine="trino",
            mode="batch",
        )  # fmt: skip
        assert "expire_snapshots=skipped_by_user" in skipped["id"]
        stopped = effective_maintenance(
            MAINTENANCE_POLICY_ID, table_format="iceberg", query_engine="trino", mode="batch",
            stopped=True,
        )  # fmt: skip
        assert stopped["detail_id"].endswith(",stopped") and "stopped" not in stopped["id"]


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


class TestReportCarriesWhatAReaderCompares:
    """compare is removed (owner, 10-03): the report itself must show what a
    reader checks before comparing two runs (invariant 2)."""

    def test_identity_digest_and_query_set_are_shown(self, tmp_path):
        from lakebench.reports.generator import ReportGenerator

        run = _metrics(_cfg())
        html = ReportGenerator(output_dir=tmp_path)._generate_experiment_section(run)
        exp = run.to_dict()["experiment"]
        assert ex.identity_hash(exp) in html

    def test_aml_alert_set_is_shown_per_rule(self):
        from lakebench.reports.generator import ReportGenerator

        exp = {
            "results": {
                "alert_set": {
                    "spec": "as1",
                    "columns": ["rule_id", "entity_id", "alert_ts"],
                    "cols_sha": "0123456789abcdef",
                    "rows": 7,
                    "h": "30",
                    "by_rule": {"W2": {"rows": 3, "h": "10"}, "W5": {"rows": 4, "h": "20"}},
                }
            }
        }
        html = ReportGenerator._alert_set_html(exp)
        for rule, rows in (("W2", "3"), ("W5", "4"), ("total", "7")):
            assert re.search(rf"<td>{rule}</td>\s*<td>{rows}</td>", html)
