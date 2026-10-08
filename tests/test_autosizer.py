"""Tests for auto-sizing of compute resources."""

import pytest

from lakebench.config import LakebenchConfig
from lakebench.config.autosizer import (
    _parse_cpu_millicores,
    _resolve_datagen_mode,
    resolve_auto_sizing,
)
from lakebench.config.scale import full_compute_guidance
from lakebench.k8s.client import ClusterCapacity


class TestParseCpuMillicores:
    """The parser must accept every Kubernetes-idiomatic CPU form."""

    def test_accepts_kubernetes_forms(self):
        for value, expected in [
            ("500m", 500),
            ("1500m", 1500),
            ("1", 1000),
            ("2", 2000),
            ("1.5", 1500),
            ("0.5", 500),
            (1, 1000),
            (2, 2000),
            (1.5, 1500),
        ]:
            assert _parse_cpu_millicores(value) == expected


# ---------------------------------------------------------------------------
# full_compute_guidance()
# ---------------------------------------------------------------------------


class TestFullComputeGuidance:
    """Tests for the full_compute_guidance function."""

    def test_datagen_mode_batch_for_small_scale(self):
        """scale <= 10 should get batch mode."""
        g = full_compute_guidance(1)
        assert g.datagen.mode == "batch"
        assert g.datagen.generators == 1

    def test_datagen_mode_batch_at_boundary(self):
        """scale=10 should still get batch mode."""
        g = full_compute_guidance(10)
        assert g.datagen.mode == "batch"
        assert g.datagen.generators == 1

    def test_datagen_mode_continuous_above_boundary(self):
        """scale > 10 should get continuous mode."""
        g = full_compute_guidance(20)
        assert g.datagen.mode == "continuous"
        assert g.datagen.generators == 8

    def test_datagen_fixed_cpu_batch(self):
        """Batch mode: fixed 4 CPU per pod."""
        g = full_compute_guidance(1)
        assert g.datagen.cpu == "4"
        assert g.datagen.memory == "4Gi"

    def test_datagen_fixed_cpu_continuous(self):
        """Continuous mode: fixed 8 CPU, 8Gi per pod (Rust image)."""
        g = full_compute_guidance(100)
        assert g.datagen.cpu == "8"
        assert g.datagen.memory == "8Gi"


# ---------------------------------------------------------------------------
# _resolve_datagen_mode()
# ---------------------------------------------------------------------------


class TestDatagenModeResolution:
    """Tests for mode resolution logic."""

    def test_auto_mode_resolves_continuous_at_every_scale(self):
        """AUTO resolves to CONTINUOUS unconditionally (Wave 2 D-wave adv-review
        fix, 2026-09-28). Pre-fix rule was scale<=10 -> batch, scale>10 ->
        continuous, which contradicted the delivery-mode docstring and the
        template default. The choice is now delivery pattern, not resource
        profile; resource sizing is scale-based elsewhere in this module."""
        for s in (1, 5, 10, 11, 50, 1000):
            config = LakebenchConfig(
                name="test",
                architecture={"workload": {"datagen": {"scale": s}}},
            )
            assert _resolve_datagen_mode(config) == "continuous", (
                f"AUTO at scale={s} did not resolve to continuous"
            )


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- scale only (no cluster cap)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- user overrides preserved
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- cluster capacity capping and boosting
# ---------------------------------------------------------------------------


class TestAutoSizingClusterCap:
    """Tests for cluster capacity capping."""

    def test_cluster_cap_memory_per_pod(self):
        """Per-pod memory is capped to 85% of largest node."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1000}}},
        )

        # Nodes with 16Gi each
        cap = ClusterCapacity(
            total_cpu_millicores=64000,
            total_memory_bytes=64 * 1024**3,
            node_count=4,
            largest_node_cpu_millicores=16000,
            largest_node_memory_bytes=16 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        # 85% of 16Gi ≈ 13Gi: the Trino worker tier memory is capped to it.
        assert config.architecture.query_engine.trino.worker.memory == "13Gi"


class TestAutosizerLeavesSparkToTheProfiles:
    """Spark sizing is the job profiles'; the autosizer neither sets nor cuts it."""

    def test_no_spark_executor_change_or_cut(self):
        # A small cluster used to "cap" executor memory and instances on
        # fields nothing read; those cuts were recorded as Lakebench caps.
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1000}}},
        )
        cap = ClusterCapacity(
            total_cpu_millicores=32000,
            total_memory_bytes=128 * 1024**3,
            node_count=4,
            largest_node_cpu_millicores=8000,
            largest_node_memory_bytes=16 * 1024**3,
        )
        cuts = resolve_auto_sizing(config, cap)
        assert not [c for c in cuts if c.startswith("spark.")], cuts


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- cluster-aware scaling
# ---------------------------------------------------------------------------


class TestAutoSizingClusterAware:
    """Tests for cluster-aware scaling: cap small scales, scale up large."""

    def test_large_cluster_does_not_boost_datagen(self):
        """Big cluster should NOT boost datagen parallelism beyond tier."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1}}},
        )

        cap = ClusterCapacity(
            total_cpu_millicores=256000,
            total_memory_bytes=512 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=32000,
            largest_node_memory_bytes=64 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        # Tier guidance for scale=1: parallelism=2 -- should NOT be boosted
        assert config.architecture.workload.datagen.parallelism == 2

    def test_small_cluster_caps_datagen(self):
        """Small cluster caps datagen parallelism to fit."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 10}}},
        )

        # Small cluster: 4 nodes, 8 cores each = 32 cores total
        cap = ClusterCapacity(
            total_cpu_millicores=32000,
            total_memory_bytes=128 * 1024**3,
            node_count=4,
            largest_node_cpu_millicores=8000,
            largest_node_memory_bytes=32 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        datagen = config.architecture.workload.datagen
        coord = config.architecture.query_engine.trino.coordinator
        worker = config.architecture.query_engine.trino.worker

        # Datagen + co-resident must fit in 32 cores
        co_resident = int(coord.cpu) + worker.replicas * int(worker.cpu) + 1
        total = datagen.parallelism * int(datagen.cpu) + co_resident
        assert total <= 32, f"datagen phase: {total} > 32"

    def test_no_overprovisioning(self):
        """Total CPU demand per phase must not exceed cluster capacity.

        This is the core constraint: we should never schedule more pods
        than the cluster can actually run in any single phase.
        """
        for scale in (1, 10, 50, 100, 500, 1000):
            config = LakebenchConfig(
                name="test",
                architecture={"workload": {"datagen": {"scale": scale}}},
            )

            # Cluster similar to real test environment:
            # 8 worker nodes × 40 cores = 320 cores
            cap = ClusterCapacity(
                total_cpu_millicores=320000,
                total_memory_bytes=8 * 432 * 1024**3,
                node_count=8,
                largest_node_cpu_millicores=40000,
                largest_node_memory_bytes=432 * 1024**3,
            )

            resolve_auto_sizing(config, cap)

            coord = config.architecture.query_engine.trino.coordinator
            worker = config.architecture.query_engine.trino.worker
            datagen = config.architecture.workload.datagen

            co_resident = int(coord.cpu) + worker.replicas * int(worker.cpu) + 1
            cluster_cores = cap.total_cpu_millicores // 1000

            # Datagen phase
            datagen_demand = datagen.parallelism * int(datagen.cpu) + co_resident
            assert datagen_demand <= cluster_cores, (
                f"scale={scale}: datagen phase demand {datagen_demand} CPU "
                f"exceeds cluster capacity {cluster_cores} CPU"
            )

    def test_large_scale_scales_up_datagen(self):
        """Scale=102 on a big cluster should scale datagen beyond tier guidance."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 102}}},
        )

        cap = ClusterCapacity(
            total_cpu_millicores=320000,
            total_memory_bytes=8 * 432 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=40000,
            largest_node_memory_bytes=432 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        # Tier guidance for scale=102: parallelism=10
        # With phase budget ≈ 254, at 8 CPU/pod → can fit ~31
        assert config.architecture.workload.datagen.parallelism > 10

    def test_streaming_mode_no_overprovisioning(self):
        """In STREAMING mode, datagen stays inside its share of the phase budget."""

        for scale in (10, 50, 100, 500):
            config = LakebenchConfig(
                name="test",
                architecture={
                    "workload": {"datagen": {"scale": scale}},
                    "processing": {"pattern": "streaming"},
                },
            )

            cap = ClusterCapacity(
                total_cpu_millicores=320000,
                total_memory_bytes=8 * 432 * 1024**3,
                node_count=8,
                largest_node_cpu_millicores=40000,
                largest_node_memory_bytes=432 * 1024**3,
            )

            resolve_auto_sizing(config, cap)

            coord = config.architecture.query_engine.trino.coordinator
            worker = config.architecture.query_engine.trino.worker
            datagen = config.architecture.workload.datagen

            co_resident = int(coord.cpu) + worker.replicas * int(worker.cpu) + 1
            cluster_cores = cap.total_cpu_millicores // 1000
            datagen_demand = datagen.parallelism * int(datagen.cpu)
            share = (cluster_cores - co_resident) * 0.9 * 0.4
            assert datagen_demand <= share, (
                f"scale={scale}: streaming datagen demand {datagen_demand} CPU "
                f"exceeds its share {share:.0f} CPU"
            )


class TestDatagenSetInConfigIsKept:
    """A datagen pod count set in the config is the run's pressure: the
    autosizer never cuts it to fit the cluster, it warns that the rest will
    wait Pending (outcome 6, exact override)."""

    @staticmethod
    def _cfg(scale):
        return LakebenchConfig(
            name="dg-cut",
            architecture={
                "workload": {
                    "datagen": {"scale": scale, "parallelism": 43, "cpu": "8", "memory": "12Gi"}
                },
            },
        )

    @staticmethod
    def _cap():
        return ClusterCapacity(434_000, 8 * 432 * 1024**3, 8, 54_000, 432 * 1024**3)

    @pytest.mark.parametrize("scale", [250, 500])
    def test_a_count_over_the_cluster_is_kept_and_warned(self, scale):
        cfg = self._cfg(scale)
        cuts = resolve_auto_sizing(cfg, self._cap())
        assert cfg.architecture.workload.datagen.parallelism == 43
        assert [c for c in cuts if c.startswith("datagen.parallelism")]

    def test_no_cut_no_warning(self):
        cfg = self._cfg(100)
        cuts = resolve_auto_sizing(cfg, self._cap())
        assert cfg.architecture.workload.datagen.parallelism == 43
        assert not [c for c in cuts if c.startswith("datagen")]


class TestScratchAutoenable:
    """Scale 50 silver-build spilled past node ephemeral storage without a
    scratch PVC per executor (R.5.2 2026-10-05, ExitCode 137 "node was low
    on resource: ephemeral-storage"). The autosizer enables scratch for
    batch at scale 50 and above.
    """

    def _cfg(
        self, scale: float, scratch_override: dict | None = None, mode: str = "batch"
    ) -> LakebenchConfig:
        from lakebench.config import LakebenchConfig

        return LakebenchConfig.model_validate(
            {
                "name": "autoscratch",
                "architecture": {
                    "workload": {"datagen": {"scale": scale}},
                    "pipeline": {"mode": mode},
                },
                "platform": {
                    "storage": {
                        "s3": {
                            "endpoint": "http://minio:9000",
                            "access_key": "a",
                            "secret_key": "b",
                        },
                        **({"scratch": scratch_override} if scratch_override else {}),
                    }
                },
            }
        )

    def test_small_scale_leaves_scratch_disabled(self):
        cfg = self._cfg(scale=1)
        resolve_auto_sizing(cfg)
        assert cfg.platform.storage.scratch.enabled is False

    def test_scale_50_enables_scratch(self):
        """R.5.2 regression: scale 50 batch must enable scratch so silver
        shuffle does not spill into pod ephemeral and get evicted."""
        cfg = self._cfg(scale=50)
        resolve_auto_sizing(cfg)
        assert cfg.platform.storage.scratch.enabled is True

    def test_explicit_user_override_wins(self):
        """A user who sets scratch.enabled=False explicitly keeps that
        value even above the threshold."""
        cfg = self._cfg(scale=50, scratch_override={"enabled": False})
        resolve_auto_sizing(cfg)
        assert cfg.platform.storage.scratch.enabled is False


_BIG_CLUSTER = ClusterCapacity(
    total_cpu_millicores=256000,
    total_memory_bytes=512 * 1024**3,
    node_count=8,
    largest_node_cpu_millicores=32000,
    largest_node_memory_bytes=64 * 1024**3,
)


@pytest.mark.parametrize(
    ("architecture", "capacity", "expected"),
    [
        (
            {
                "workload": {"datagen": {"scale": 1}},
                "query_engine": {"trino": {"worker": {"replicas": 8}}},
            },
            None,
            {"query_engine.trino.worker.replicas": 8},
        ),
        (
            {"workload": {"datagen": {"scale": 1, "parallelism": 32}}},
            None,
            {"workload.datagen.parallelism": 32},
        ),
        (
            {"workload": {"datagen": {"scale": 1, "parallelism": 3}}},
            _BIG_CLUSTER,
            {"workload.datagen.parallelism": 3},
        ),
        (
            {"workload": {"datagen": {"scale": 100, "memory": "16Gi"}}},
            None,
            {"workload.datagen.memory": "16Gi", "workload.datagen.cpu": "8"},
        ),
        (
            {"workload": {"datagen": {"scale": 5, "mode": "batch", "memory": "16Gi"}}},
            None,
            {"workload.datagen.memory": "16Gi", "workload.datagen.cpu": "8"},
        ),
        # batch at a large scale keeps batch resources: 8 CPU, the RSS-model
        # memory floor, generators 0 (threads follow the pod CPU request)
        (
            {"workload": {"datagen": {"scale": 100, "mode": "batch"}}},
            None,
            {
                "workload.datagen.cpu": "8",
                "workload.datagen.memory": "4Gi",
                "workload.datagen.generators": 0,
            },
        ),
    ],
)
def test_user_set_values_survive_auto_sizing(architecture, capacity, expected):
    """Outcome 6: a value the user set is the value deployed."""
    config = LakebenchConfig(name="test", architecture=architecture)
    if capacity is None:
        resolve_auto_sizing(config)
    else:
        resolve_auto_sizing(config, capacity)
    for path, want in expected.items():
        obj = config.architecture
        for part in path.split("."):
            obj = getattr(obj, part)
        assert obj == want, path


@pytest.mark.parametrize(("scale", "mode"), [(1000, "batch"), (1, "continuous")])
def test_explicit_datagen_mode_is_kept_at_any_scale(scale, mode):
    config = LakebenchConfig(
        name="test", architecture={"workload": {"datagen": {"scale": scale, "mode": mode}}}
    )
    assert _resolve_datagen_mode(config) == mode
