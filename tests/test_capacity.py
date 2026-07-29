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
        """The published docs table must match this exactly."""
        peak = compute_peak_requirements(1)
        assert peak.cpu_cores == 36
        assert peak.memory_gb == 512
        assert peak.scratch_gb == 1200

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
