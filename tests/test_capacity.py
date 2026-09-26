"""Tests for peak resource computation and the cluster capacity preflight check.

Covers ``compute_peak_requirements()`` (the single source of truth for
documented minimums) and ``_check_cluster_capacity()`` (prerequisite check 9).
"""

import re
from pathlib import Path
from unittest import mock

import pytest

from lakebench.cli._prerequisites import _check_cluster_capacity
from lakebench.k8s.client import ClusterCapacity
from lakebench.modules.pipeline_engines.spark.job import (
    BATCH_JOB_TYPES,
    STREAMING_JOB_TYPES,
    compute_peak_requirements,
)

GIB = 1024**3


class TestComputePeakRequirements:
    """Peak resource derivation from _JOB_PROFILES."""

    def test_scale_1_matches_documented_minimums(self):
        """The published docs table must match this exactly.

        Scratch bumped from 150Gi to 300Gi per silver-build executor
        during the AML/silver hardening work (fixes for typology
        window OOMs). 8 executors x 300Gi = 2400 GiB peak.
        """
        peak = compute_peak_requirements(1)
        assert peak.cpu_cores == 36
        assert peak.memory_gb == 512
        assert peak.scratch_gb == 2400

    def test_silver_build_drives_the_peak(self):
        """silver-build is the largest batch job at every scale."""
        for scale in (1, 10, 50, 100, 500):
            assert compute_peak_requirements(scale).driving_job == "silver-build"

    def test_scale_1_and_10_are_identical(self):
        """Executor counts are fixed at or below scale 10.

        This is the counter-intuitive property that made the old docs wrong,
        so it is asserted explicitly rather than left implicit.
        """
        one, ten = compute_peak_requirements(1), compute_peak_requirements(10)
        assert (one.cpu_cores, one.memory_gb, one.scratch_gb) == (
            ten.cpu_cores,
            ten.memory_gb,
            ten.scratch_gb,
        )

    def test_requirements_grow_above_scale_10(self):
        small = compute_peak_requirements(10)
        large = compute_peak_requirements(100)
        assert large.cpu_cores > small.cpu_cores
        assert large.memory_gb > small.memory_gb
        assert large.scratch_gb > small.scratch_gb

    def test_batch_peak_is_max_not_sum(self):
        """Batch jobs run sequentially, so the peak is the largest job."""
        peak = compute_peak_requirements(100)
        assert peak.memory_gb == max(r.memory_gb for r in peak.per_job)
        assert peak.memory_gb < sum(r.memory_gb for r in peak.per_job)

    def test_sustained_peak_is_sum_not_max(self):
        """Streaming jobs run concurrently, so their needs add up."""
        peak = compute_peak_requirements(10, "sustained")
        assert peak.memory_gb == sum(r.memory_gb for r in peak.per_job)

    def test_batch_and_streaming_cover_expected_jobs(self):
        batch = compute_peak_requirements(1)
        assert {r.job_type for r in batch.per_job} == set(BATCH_JOB_TYPES)
        streaming = compute_peak_requirements(1, "sustained")
        assert {r.job_type for r in streaming.per_job} == set(STREAMING_JOB_TYPES)

    def test_max_pod_fits_single_executor(self):
        """Largest pod is one silver executor: 4 cores, 48g + 12g overhead."""
        peak = compute_peak_requirements(1)
        assert peak.max_pod_cpu_cores == 4
        assert peak.max_pod_memory_gb == 60

    def test_unknown_mode_falls_back_to_batch(self):
        unknown, batch = (
            compute_peak_requirements(1, "nonsense"),
            compute_peak_requirements(1, "batch"),
        )
        assert unknown.per_job == batch.per_job
        assert (unknown.cpu_cores, unknown.memory_gb) == (batch.cpu_cores, batch.memory_gb)

    def test_mode_is_case_insensitive(self):
        assert compute_peak_requirements(5, "SUSTAINED").per_job[0].job_type in STREAMING_JOB_TYPES


def _cfg(scale=1, mode="batch"):
    """Minimal config stub with the paths the capacity check reads."""
    cfg = mock.MagicMock()
    cfg.architecture.workload.datagen.scale = scale
    cfg.architecture.pipeline.mode = mode
    cfg.platform.kubernetes.context = None
    cfg.get_namespace.return_value = "lakebench"
    # Co-resident pods (LB-155): no query engine, so only catalog/Postgres
    # (1 core) and, in continuous mode, datagen are added to the peak.
    cfg.architecture.query_engine.type.value = "none"
    cfg.architecture.workload.datagen.parallelism = 4
    cfg.architecture.workload.datagen.cpu = "8"
    cfg.architecture.workload.datagen.memory = "12Gi"
    return cfg


@pytest.fixture
def patched_capacity():
    """Patch get_k8s_client so the check sees a controlled ClusterCapacity."""

    def _run(capacity, cfg=None):
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = capacity
            return _check_cluster_capacity(cfg or _cfg())

    return _run


class TestClusterCapacityCheck:
    """Prerequisite check 9."""

    def test_ample_cluster_passes(self, patched_capacity):
        result = patched_capacity(
            ClusterCapacity(652_000, 4000 * GIB, 20, 64_000, 256 * GIB),
        )
        assert result.passed
        assert result.name == "cluster-capacity"

    def test_undersized_cluster_fails_with_shortfall(self, patched_capacity):
        """The old README claim (8 CPU / 32 GB) must be rejected."""
        result = patched_capacity(ClusterCapacity(8_000, 32 * GIB, 2, 4_000, 16 * GIB))
        assert not result.passed
        assert "36 cores" in result.hint
        assert "512 GB" in result.hint

    def test_enough_total_but_node_too_small_fails(self, patched_capacity):
        """512 GB spread across 16 GB nodes cannot schedule a 60 GB executor."""
        result = patched_capacity(ClusterCapacity(200_000, 600 * GIB, 20, 8_000, 16 * GIB))
        assert not result.passed
        assert "Largest pod" in result.hint

    def test_message_names_the_driving_job(self, patched_capacity):
        result = patched_capacity(ClusterCapacity(8_000, 32 * GIB, 2, 4_000, 16 * GIB))
        assert "silver-build" in result.message

    def test_unknown_capacity_does_not_block(self, patched_capacity):
        """No permission to list nodes must not fail the deploy."""
        result = patched_capacity(None)
        assert result.passed
        assert "skipping" in result.message.lower()

    def test_check_never_raises(self):
        """A broken capacity estimate must not block a deploy."""
        with mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("boom")):
            result = _check_cluster_capacity(_cfg())
        assert result.passed
        assert "skipped" in result.message.lower()

    def test_sustained_mode_uses_streaming_profiles(self, patched_capacity):
        result = patched_capacity(
            ClusterCapacity(652_000, 4000 * GIB, 20, 64_000, 256 * GIB),
            cfg=_cfg(mode="sustained"),
        )
        assert result.passed
        assert "sustained" in result.message

    def test_capacity_check_plumbs_schema_to_compute_peak(self):
        """LB-118 review finding: the fix is only operator-visible if
        _check_cluster_capacity actually passes the workload schema down
        to compute_peak_requirements. A refactor that drops the schema
        arg would leave every unit test green while silently reverting
        AML sizing to c360."""
        cfg = _cfg()
        cfg.architecture.workload.schema_type.value = "financial"
        with (
            mock.patch("lakebench.k8s.get_k8s_client") as get_client,
            mock.patch(
                "lakebench.modules.pipeline_engines.spark.job.compute_peak_requirements"
            ) as peak,
        ):
            get_client.return_value.get_cluster_capacity.return_value = ClusterCapacity(
                652_000, 4000 * GIB, 20, 64_000, 256 * GIB
            )
            _check_cluster_capacity(cfg)
        assert peak.called
        # positional-arg or kwarg both fine; the third value is the schema.
        args, kwargs = peak.call_args
        schema_arg = kwargs.get("schema_type", args[2] if len(args) > 2 else None)
        assert schema_arg == "financial"


class TestCoResidentPodsAreCounted:
    """LB-155: the check skipped Trino, catalog/Postgres and datagen whenever
    the uncapped pipeline request fit, so c360 continuous s10 passed on a
    40-core cluster that needs 49."""

    @staticmethod
    def _real_cfg(mode, scale=10):
        from tests.conftest import make_config

        return make_config(
            architecture={
                "workload": {"datagen": {"scale": scale}},
                "pipeline": {"mode": mode},
            }
        )

    def _check(self, cfg, cores, memory_gb=4000):
        cap = ClusterCapacity(cores * 1000, memory_gb * GIB, 8, 64_000, 256 * GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            return _check_cluster_capacity(cfg)

    def test_c360_continuous_s10_needs_more_than_its_pipeline(self):
        cfg = self._real_cfg("sustained")
        peak = compute_peak_requirements(10, "sustained", "customer360")
        # Fits the Spark streams alone, not the streams plus co-residents.
        result = self._check(cfg, peak.cpu_cores)
        assert not result.passed
        assert "Trino" in result.hint and "datagen" in result.hint

    def test_40_cores_fails_for_c360_continuous_s10(self):
        assert not self._check(self._real_cfg("sustained"), 40).passed

    def test_batch_counts_engine_but_not_datagen(self):
        from lakebench.cli._prerequisites import _co_resident_request

        cfg = self._real_cfg("batch")
        batch = _co_resident_request(cfg, sustained=False)
        cont = _co_resident_request(cfg, sustained=True)
        assert "datagen" not in batch[2] and "datagen" in cont[2]
        assert cont[0] > batch[0] > 1
        peak = compute_peak_requirements(10, "batch", "customer360")
        assert not self._check(cfg, peak.cpu_cores).passed
        assert self._check(cfg, peak.cpu_cores + batch[0]).passed

    def test_memory_counts_co_residents(self):
        from lakebench.cli._prerequisites import _co_resident_request

        cfg = self._real_cfg("batch")
        peak = compute_peak_requirements(10, "batch", "customer360")
        _, co_gb, _ = _co_resident_request(cfg, sustained=False)
        assert co_gb > 0
        assert not self._check(cfg, 1000, memory_gb=peak.memory_gb).passed
        assert self._check(cfg, 1000, memory_gb=peak.memory_gb + co_gb).passed

    def test_sustained_flag_overrides_a_batch_config(self):
        """`run --sustained` does not write the mode back to the config; the
        check must size the continuous run it will actually start."""
        cfg = self._real_cfg("batch")
        cap = ClusterCapacity(40_000, 4000 * GIB, 8, 64_000, 256 * GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            res = _check_cluster_capacity(cfg, sustained=True)
        assert not res.passed and "sustained" in res.message

    def test_skip_generate_leaves_datagen_out(self):
        from lakebench.cli._prerequisites import _co_resident_request

        cfg = self._real_cfg("sustained")
        with_dg = _co_resident_request(cfg, True)
        without = _co_resident_request(cfg, True, datagen_runs=False)
        assert "datagen" not in without[2] and without[0] < with_dg[0]
        peak = compute_peak_requirements(10, "sustained", "customer360")
        cores = peak.cpu_cores + without[0]
        cap = ClusterCapacity(cores * 1000, 4000 * GIB, 8, 64_000, 256 * GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            assert _check_cluster_capacity(cfg, datagen_runs=False).passed
            assert not _check_cluster_capacity(cfg).passed

    def test_run_passes_the_resolved_mode_to_the_check(self, tmp_path, monkeypatch):
        from typer.testing import CliRunner

        from lakebench.cli import app

        cfg_file = tmp_path / "c.yaml"
        cfg_file.write_text(
            "name: cap-flag\n"
            "platform:\n  storage:\n    s3:\n      endpoint: http://127.0.0.1:1\n"
            "      access_key: x\n      secret_key: y\n"
        )
        seen: dict = {}

        def fake_prereqs(cfg, **kw):
            seen.update(kw)
            raise SystemExit(3)

        monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", fake_prereqs)
        dg_state = {"v": "finished"}
        monkeypatch.setattr(
            "lakebench.cli._sustained._datagen_job_state", lambda ns: (dg_state["v"], "")
        )
        # No cluster: auto-sizing runs without capacity.
        monkeypatch.setattr(
            "lakebench.k8s.get_k8s_client", mock.MagicMock(side_effect=RuntimeError("no cluster"))
        )
        CliRunner().invoke(app, ["run", str(cfg_file), "--sustained", "--skip-generate", "--yes"])
        assert seen == {"sustained": True, "datagen_runs": False}
        # The run keeps datagen reserved unless the Job finished; so does
        # the preflight (LB-155 re-review).
        for state in ("absent", "unfinished", "unknown"):
            dg_state["v"] = state
            seen.clear()
            CliRunner().invoke(
                app, ["run", str(cfg_file), "--sustained", "--skip-generate", "--yes"]
            )
            assert seen == {"sustained": True, "datagen_runs": True}, state
        seen.clear()
        CliRunner().invoke(app, ["run", str(cfg_file), "--yes"])
        assert seen == {"sustained": False, "datagen_runs": True}


class TestDocumentedMinimumsMatchCode:
    """The published docs table must stay in sync with _JOB_PROFILES.

    LB-050: the README claimed 8 CPU / 32 GB for scale 1 when the code
    requested 36 cores / 512 GB. This test makes that drift a test failure
    rather than something a user discovers with Pending pods.
    """

    DOC = Path(__file__).resolve().parents[1] / "docs" / "getting-started.md"
    ROW = re.compile(
        r"^\| (\d+) \| [^|]+ \| ([\d,]+) cores \| ([\d,]+) GB \| ([\d,]+) Gi \|",
        re.MULTILINE,
    )

    @pytest.mark.skipif(not DOC.exists(), reason="docs not present in this checkout")
    def test_getting_started_table_matches_profiles(self):
        rows = self.ROW.findall(self.DOC.read_text())
        assert rows, "minimums table not found in docs/getting-started.md"

        for scale, cores, memory, scratch in rows:
            peak = compute_peak_requirements(int(scale))
            documented = (
                int(cores.replace(",", "")),
                int(memory.replace(",", "")),
                int(scratch.replace(",", "")),
            )
            assert (peak.cpu_cores, peak.memory_gb, peak.scratch_gb) == documented, (
                f"docs/getting-started.md scale {scale} is stale. "
                f"Regenerate from compute_peak_requirements()."
            )


class TestContinuousDocsTable:
    """The continuous-mode minimums in getting-started.md come from
    compute_peak_requirements(scale, "sustained", schema)."""

    DOC = Path(__file__).resolve().parents[1] / "docs" / "getting-started.md"
    ROW = re.compile(
        r"^\| (Customer360|AML) \| ([\d-]+) \| ([\d,]+) cores \| ([\d,]+) GB \| ([\d,]+) Gi \|",
        re.MULTILINE,
    )

    @pytest.mark.skipif(not DOC.exists(), reason="docs not present in this checkout")
    def test_continuous_table_matches_profiles(self):
        rows = self.ROW.findall(self.DOC.read_text())
        assert len(rows) == 6, "continuous minimums table not found in docs/getting-started.md"
        schema = {"Customer360": "customer360", "AML": "financial"}
        for workload, scales, cores, memory, scratch in rows:
            documented = tuple(int(v.replace(",", "")) for v in (cores, memory, scratch))
            for scale in {int(x) for x in scales.split("-")}:
                peak = compute_peak_requirements(scale, "sustained", schema[workload])
                assert (peak.cpu_cores, peak.memory_gb, peak.scratch_gb) == documented, (
                    f"docs/getting-started.md continuous {workload} scale {scale} is stale."
                )


class TestAmlBatchDocsTable:
    """The AML batch minimums in getting-started.md come from
    compute_peak_requirements(scale, "batch", "financial")."""

    DOC = Path(__file__).resolve().parents[1] / "docs" / "getting-started.md"
    ROW = re.compile(
        r"^\| AML batch \| ([\d-]+) \| ([\d,]+) cores \| ([\d,]+) GB \| ([\d,]+) Gi \|",
        re.MULTILINE,
    )

    @pytest.mark.skipif(not DOC.exists(), reason="docs not present in this checkout")
    def test_aml_batch_table_matches_profiles(self):
        rows = self.ROW.findall(self.DOC.read_text())
        assert len(rows) == 3, "AML batch minimums table not found in docs/getting-started.md"
        for scales, cores, memory, scratch in rows:
            documented = tuple(int(v.replace(",", "")) for v in (cores, memory, scratch))
            for scale in {int(x) for x in scales.split("-")}:
                peak = compute_peak_requirements(scale, "batch", "financial")
                assert (peak.cpu_cores, peak.memory_gb, peak.scratch_gb) == documented, (
                    f"docs/getting-started.md AML batch scale {scale} is stale."
                )


class TestPrerequisiteWiring:
    """The check must actually be registered in run_prerequisites()."""

    def test_capacity_check_is_registered(self):
        from lakebench.cli._prerequisites import run_prerequisites

        cfg = _cfg()
        cfg.architecture.catalog.type.value = "polaris"
        cfg.platform.storage.s3.endpoint = ""
        cfg.platform.storage.s3.access_key = ""
        cfg.platform.kubernetes.create_namespace = True

        with mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("no cluster")):
            report = run_prerequisites(cfg)

        assert "cluster-capacity" in {c.name for c in report.checks}
