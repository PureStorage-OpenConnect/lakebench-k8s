"""Tests for auto-sizing of compute resources."""

from lakebench.config import LakebenchConfig
from lakebench.config.autosizer import _resolve_datagen_mode, resolve_auto_sizing
from lakebench.config.scale import full_compute_guidance
from lakebench.k8s.client import ClusterCapacity

# ---------------------------------------------------------------------------
# full_compute_guidance()
# ---------------------------------------------------------------------------


class TestFullComputeGuidance:
    """Tests for the full_compute_guidance function."""

    def test_minimal_tier(self):
        g = full_compute_guidance(1)
        assert g.spark.tier_name == "minimal"
        assert g.trino.worker_replicas == 1
        assert g.datagen.parallelism == 2

    def test_balanced_tier(self):
        g = full_compute_guidance(10)
        assert g.spark.tier_name == "balanced"
        assert g.trino.worker_replicas == 2
        assert g.datagen.parallelism == 4

    def test_performance_tier(self):
        g = full_compute_guidance(100)
        assert g.spark.tier_name == "performance"
        assert g.trino.worker_replicas == 4
        assert g.datagen.parallelism >= 8

    def test_extreme_tier(self):
        g = full_compute_guidance(1000)
        assert g.spark.tier_name == "extreme"
        assert g.trino.worker_replicas >= 8
        assert g.datagen.parallelism >= 16

    def test_datagen_mode_batch_for_small_scale(self):
        """scale <= 10 should get batch mode."""
        g = full_compute_guidance(1)
        assert g.datagen.mode == "batch"
        assert g.datagen.generators == 1
        assert g.datagen.uploaders == 1

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
        assert g.datagen.uploaders == 2

    def test_datagen_fixed_cpu_batch(self):
        """Batch mode: fixed 4 CPU per pod."""
        g = full_compute_guidance(1)
        assert g.datagen.cpu == "4"
        assert g.datagen.memory == "4Gi"

    def test_datagen_fixed_cpu_continuous(self):
        """Continuous mode: fixed 8 CPU, 24Gi per pod (MVP sizing)."""
        g = full_compute_guidance(100)
        assert g.datagen.cpu == "8"
        assert g.datagen.memory == "24Gi"


# ---------------------------------------------------------------------------
# _resolve_datagen_mode()
# ---------------------------------------------------------------------------


class TestDatagenModeResolution:
    """Tests for mode resolution logic."""

    def test_auto_mode_small_scale_resolves_batch(self):
        """Auto mode with scale <= 10 -> batch."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 5}}},
        )
        assert _resolve_datagen_mode(config) == "batch"

    def test_auto_mode_large_scale_resolves_continuous(self):
        """Auto mode with scale > 10 -> continuous."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 50}}},
        )
        assert _resolve_datagen_mode(config) == "continuous"

    def test_auto_mode_boundary_scale_10(self):
        """Auto mode with scale=10 -> batch (boundary)."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 10}}},
        )
        assert _resolve_datagen_mode(config) == "batch"

    def test_auto_mode_boundary_scale_11(self):
        """Auto mode with scale=11 -> continuous (just above boundary)."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 11}}},
        )
        assert _resolve_datagen_mode(config) == "continuous"

    def test_explicit_batch_mode(self):
        """Explicit batch mode preserved regardless of scale."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1000, "mode": "batch"}}},
        )
        assert _resolve_datagen_mode(config) == "batch"

    def test_explicit_continuous_mode(self):
        """Explicit continuous mode preserved regardless of scale."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1, "mode": "continuous"}}},
        )
        assert _resolve_datagen_mode(config) == "continuous"


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- scale only (no cluster cap)
# ---------------------------------------------------------------------------


class TestAutoSizingScaleOnly:
    """Tests for auto-sizing when no cluster capacity is available."""

    def test_minimal_scale_sizing(self):
        """Scale=1 should get minimal tier resources with batch mode."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1}}},
        )
        resolve_auto_sizing(config)

        assert config.platform.compute.spark.executor.instances == 2
        assert config.platform.compute.spark.executor.memory == "4g"
        assert config.platform.compute.spark.executor.cores == 2
        assert config.architecture.query_engine.trino.worker.replicas == 1
        assert config.architecture.query_engine.trino.worker.memory == "8Gi"
        assert config.architecture.workload.datagen.parallelism == 2
        # Batch mode: fixed 4 CPU, 4Gi
        assert config.architecture.workload.datagen.cpu == "4"
        assert config.architecture.workload.datagen.memory == "4Gi"
        assert config.architecture.workload.datagen.generators == 1
        assert config.architecture.workload.datagen.uploaders == 1

    def test_balanced_scale_sizing(self):
        """Scale=10 should get balanced tier resources, still batch mode."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 10}}},
        )
        resolve_auto_sizing(config)

        assert config.platform.compute.spark.executor.instances >= 4
        assert config.platform.compute.spark.executor.memory == "16g"
        assert config.architecture.query_engine.trino.worker.replicas == 2
        assert config.architecture.workload.datagen.parallelism == 4
        # Batch mode at scale=10
        assert config.architecture.workload.datagen.cpu == "4"
        assert config.architecture.workload.datagen.memory == "4Gi"
        assert config.architecture.workload.datagen.generators == 1

    def test_performance_scale_sizing(self):
        """Scale=100 should get performance tier resources, continuous mode."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 100}}},
        )
        resolve_auto_sizing(config)

        assert config.platform.compute.spark.executor.instances >= 8
        assert config.platform.compute.spark.executor.memory == "32g"
        assert config.architecture.query_engine.trino.worker.replicas == 4
        assert config.architecture.query_engine.trino.worker.memory == "32Gi"
        assert config.architecture.workload.datagen.parallelism >= 8
        # Continuous mode: fixed 8 CPU, 24Gi, 8 generators (MVP sizing)
        assert config.architecture.workload.datagen.cpu == "8"
        assert config.architecture.workload.datagen.memory == "24Gi"
        assert config.architecture.workload.datagen.generators == 8
        assert config.architecture.workload.datagen.uploaders == 2

    def test_extreme_scale_sizing(self):
        """Scale=1000 should get extreme tier resources, continuous mode."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1000}}},
        )
        resolve_auto_sizing(config)

        assert config.platform.compute.spark.executor.instances >= 16
        assert config.platform.compute.spark.executor.memory == "48g"
        assert config.architecture.query_engine.trino.worker.replicas >= 8
        # Continuous mode (MVP sizing)
        assert config.architecture.workload.datagen.cpu == "8"
        assert config.architecture.workload.datagen.memory == "24Gi"
        assert config.architecture.workload.datagen.generators == 8

    def test_memory_overhead_derived(self):
        """Memory overhead should be ~25% of executor memory."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 10}}},
        )
        resolve_auto_sizing(config)
        # 16g * 0.25 = 4g
        assert config.platform.compute.spark.executor.memory_overhead == "4g"


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- user overrides preserved
# ---------------------------------------------------------------------------


class TestAutoSizingUserOverride:
    """Tests that user-specified values are preserved."""

    def test_user_executor_instances_preserved(self):
        """Explicitly set executor.instances should not be overwritten."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1}}},
            platform={"compute": {"spark": {"executor": {"instances": 16}}}},
        )
        resolve_auto_sizing(config)

        # User set instances=16, should not be overridden to 2
        assert config.platform.compute.spark.executor.instances == 16

    def test_user_trino_replicas_preserved(self):
        """Explicitly set trino worker replicas should not be overwritten."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "workload": {"datagen": {"scale": 1}},
                "query_engine": {"trino": {"worker": {"replicas": 8}}},
            },
        )
        resolve_auto_sizing(config)

        assert config.architecture.query_engine.trino.worker.replicas == 8

    def test_user_datagen_parallelism_preserved(self):
        """Explicitly set datagen parallelism should not be overwritten."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1, "parallelism": 32}}},
        )
        resolve_auto_sizing(config)

        assert config.architecture.workload.datagen.parallelism == 32

    def test_mixed_user_and_auto(self):
        """User sets some fields, auto-sizer fills the rest."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 100}}},
            platform={"compute": {"spark": {"executor": {"instances": 4}}}},
        )
        resolve_auto_sizing(config)

        # instances preserved (user set)
        assert config.platform.compute.spark.executor.instances == 4
        # memory auto-sized (user didn't set)
        assert config.platform.compute.spark.executor.memory == "32g"

    def test_user_explicit_mode_preserved(self):
        """User setting mode=batch on large scale should keep batch resources."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 100, "mode": "batch"}}},
        )
        resolve_auto_sizing(config)

        # Batch mode forced: 4 CPU, 4Gi, 1 generator
        assert config.architecture.workload.datagen.cpu == "4"
        assert config.architecture.workload.datagen.memory == "4Gi"
        assert config.architecture.workload.datagen.generators == 1

    def test_datagen_memory_hardlocked_continuous(self):
        """User-set memory is overridden to mode-correct value (continuous)."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "workload": {"datagen": {"scale": 100, "memory": "8Gi"}},
            },
        )
        resolve_auto_sizing(config)

        # Even though user set 8Gi, continuous mode hard-locks to 24Gi
        assert config.architecture.workload.datagen.memory == "24Gi"
        assert config.architecture.workload.datagen.cpu == "8"

    def test_datagen_memory_hardlocked_batch(self):
        """User-set memory is overridden to mode-correct value (batch)."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "workload": {
                    "datagen": {"scale": 5, "mode": "batch", "memory": "16Gi"},
                },
            },
        )
        resolve_auto_sizing(config)

        # Even though user set 16Gi, batch mode hard-locks to 4Gi
        assert config.architecture.workload.datagen.memory == "4Gi"
        assert config.architecture.workload.datagen.cpu == "4"


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- cluster capacity capping and boosting
# ---------------------------------------------------------------------------


class TestAutoSizingClusterCap:
    """Tests for cluster capacity capping."""

    def test_cluster_cap_reduces_executors(self):
        """When cluster is small, executor count is capped."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1000}}},
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

        # Co-resident: Trino coord (4 CPU) + 8 workers*8 CPU (64) + infra (1) = 69 CPU
        # But cluster only has 32 cores, so available for Spark ≈ 0 after co-resident
        # Executor count is capped to at least 1
        assert config.platform.compute.spark.executor.instances >= 1
        # But certainly not the full guidance of 16-32 executors
        assert config.platform.compute.spark.executor.instances <= 3

    def test_cluster_cap_reduces_trino_workers(self):
        """When cluster is small, Trino worker count is capped."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1000}}},
        )

        # Small cluster: 2 nodes, 4 cores each
        cap = ClusterCapacity(
            total_cpu_millicores=8000,
            total_memory_bytes=64 * 1024**3,
            node_count=2,
            largest_node_cpu_millicores=4000,
            largest_node_memory_bytes=32 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        # 30% of 8 cores = 2.4 cores budget
        # Each worker uses 8 cores → max 0 ... but min is 1
        assert config.architecture.query_engine.trino.worker.replicas >= 1

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

        # 85% of 16Gi ≈ 13Gi
        # Executor memory (48g) should be capped
        exec_mem = config.platform.compute.spark.executor.memory
        # Should be capped to ~13g
        assert exec_mem != "48g"

    def test_no_cluster_cap_still_works(self):
        """Auto-sizing works fine without cluster capacity."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 10}}},
        )
        resolve_auto_sizing(config, None)

        # Should still have auto-sized values from scale
        assert config.platform.compute.spark.executor.memory == "16g"


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- cluster-aware scaling
# ---------------------------------------------------------------------------


class TestAutoSizingClusterAware:
    """Tests for cluster-aware scaling: cap small scales, scale up large."""

    def test_large_cluster_does_not_boost_executors(self):
        """Big cluster with scale=1: executors stay at tier guidance (2)."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1}}},
        )

        # Large cluster: 8 nodes, 32 cores each = 256 cores total
        cap = ClusterCapacity(
            total_cpu_millicores=256000,
            total_memory_bytes=512 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=32000,
            largest_node_memory_bytes=64 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        # Tier guidance for scale=1: 2 executors -- should NOT be boosted
        assert config.platform.compute.spark.executor.instances == 2

    def test_large_cluster_does_not_boost_trino_workers(self):
        """Big cluster should NOT boost Trino worker count beyond tier."""
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

        # Tier guidance for scale=1: 1 Trino worker -- should NOT be boosted
        assert config.architecture.query_engine.trino.worker.replicas == 1

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

    def test_user_set_parallelism_preserved(self):
        """User-set parallelism should be preserved even with cluster capacity."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 1, "parallelism": 3}}},
        )

        cap = ClusterCapacity(
            total_cpu_millicores=256000,
            total_memory_bytes=512 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=32000,
            largest_node_memory_bytes=64 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        # User explicitly set 3 -- should stay at 3
        assert config.architecture.workload.datagen.parallelism == 3

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

    def test_phase_aware_budgets(self):
        """Datagen and Spark each get the full phase budget (they're sequential)."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 100}}},
        )

        # Medium cluster: 4 nodes, 16 cores each = 64 cores
        cap = ClusterCapacity(
            total_cpu_millicores=64000,
            total_memory_bytes=256 * 1024**3,
            node_count=4,
            largest_node_cpu_millicores=16000,
            largest_node_memory_bytes=64 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        executor = config.platform.compute.spark.executor
        datagen = config.architecture.workload.datagen
        coord = config.architecture.query_engine.trino.coordinator
        worker = config.architecture.query_engine.trino.worker

        # Co-resident: Trino coord + workers + infra
        co_resident = int(coord.cpu) + worker.replicas * int(worker.cpu) + 1

        # Datagen phase: datagen + co-resident must fit
        datagen_total = datagen.parallelism * int(datagen.cpu) + co_resident
        assert datagen_total <= 64, f"datagen phase: {datagen_total} > 64"

        # Spark phase: executors + co-resident must fit
        spark_total = executor.instances * executor.cores + co_resident
        assert spark_total <= 64, f"spark phase: {spark_total} > 64"

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
            executor = config.platform.compute.spark.executor

            co_resident = int(coord.cpu) + worker.replicas * int(worker.cpu) + 1
            cluster_cores = cap.total_cpu_millicores // 1000

            # Datagen phase
            datagen_demand = datagen.parallelism * int(datagen.cpu) + co_resident
            assert datagen_demand <= cluster_cores, (
                f"scale={scale}: datagen phase demand {datagen_demand} CPU "
                f"exceeds cluster capacity {cluster_cores} CPU"
            )

            # Spark phase
            spark_demand = executor.instances * executor.cores + co_resident
            assert spark_demand <= cluster_cores, (
                f"scale={scale}: spark phase demand {spark_demand} CPU "
                f"exceeds cluster capacity {cluster_cores} CPU"
            )

    def test_large_scale_scales_up_executors(self):
        """Scale=102 on a big cluster should scale executors beyond tier guidance."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 102}}},
        )

        # 8 nodes × 40 cores = 320 cores
        cap = ClusterCapacity(
            total_cpu_millicores=320000,
            total_memory_bytes=8 * 432 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=40000,
            largest_node_memory_bytes=432 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        # Tier guidance for scale=102 (performance): 8 executors
        # With 320 cores and ~37 co-resident, phase budget ≈ 254
        # Each executor uses 8 cores → can fit ~31
        assert config.platform.compute.spark.executor.instances > 8

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

    def test_large_scale_trino_still_not_boosted(self):
        """Even at large scale, Trino workers stay at tier guidance."""
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

        # Tier guidance: 4 workers -- should NOT be boosted
        assert config.architecture.query_engine.trino.worker.replicas == 4

    def test_medium_scale_not_scaled_up(self):
        """Scale=10 on a big cluster: executors stay at tier guidance."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 10}}},
        )

        cap = ClusterCapacity(
            total_cpu_millicores=320000,
            total_memory_bytes=8 * 432 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=40000,
            largest_node_memory_bytes=432 * 1024**3,
        )

        resolve_auto_sizing(config, cap)

        # Tier guidance for scale=10 (balanced): 4 executors -- no scaling up
        assert config.platform.compute.spark.executor.instances == 4
        assert config.architecture.workload.datagen.parallelism == 4

    def test_streaming_mode_splits_phase_budget(self):
        """STREAMING mode should give datagen less budget than MEDALLION mode."""
        cap = ClusterCapacity(
            total_cpu_millicores=320000,
            total_memory_bytes=8 * 432 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=40000,
            largest_node_memory_bytes=432 * 1024**3,
        )

        # Batch (MEDALLION) config
        batch_config = LakebenchConfig(
            name="test-batch",
            architecture={
                "workload": {"datagen": {"scale": 100}},
                "processing": {"pattern": "medallion"},
            },
        )
        resolve_auto_sizing(batch_config, cap)
        batch_datagen = batch_config.architecture.workload.datagen.parallelism

        # Streaming config (same scale, same cluster)
        stream_config = LakebenchConfig(
            name="test-stream",
            architecture={
                "workload": {"datagen": {"scale": 100}},
                "processing": {"pattern": "streaming"},
            },
        )
        resolve_auto_sizing(stream_config, cap)
        stream_datagen = stream_config.architecture.workload.datagen.parallelism

        # Streaming mode should cap datagen lower (40% budget vs 100%)
        assert stream_datagen < batch_datagen, (
            f"Streaming datagen ({stream_datagen}) should be less than "
            f"batch datagen ({batch_datagen})"
        )

    def test_streaming_mode_no_overprovisioning(self):
        """In STREAMING mode, datagen + Spark executor demand should fit the cluster."""

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
            executor = config.platform.compute.spark.executor

            co_resident = int(coord.cpu) + worker.replicas * int(worker.cpu) + 1
            cluster_cores = cap.total_cpu_millicores // 1000

            # Datagen demand
            datagen_demand = datagen.parallelism * int(datagen.cpu)

            # Spark executor demand (global -- autosizer value)
            spark_demand = executor.instances * executor.cores

            # In streaming mode both run at the same time
            total = datagen_demand + spark_demand + co_resident
            assert total <= cluster_cores, (
                f"scale={scale}: streaming total demand {total} CPU "
                f"exceeds cluster capacity {cluster_cores} CPU "
                f"(datagen={datagen_demand}, spark={spark_demand}, "
                f"co_resident={co_resident})"
            )

    def test_batch_mode_unchanged(self):
        """MEDALLION mode should still give each phase the full budget."""
        cap = ClusterCapacity(
            total_cpu_millicores=320000,
            total_memory_bytes=8 * 432 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=40000,
            largest_node_memory_bytes=432 * 1024**3,
        )

        config = LakebenchConfig(
            name="test",
            architecture={
                "workload": {"datagen": {"scale": 102}},
                "processing": {"pattern": "medallion"},
            },
        )

        resolve_auto_sizing(config, cap)

        # Should still scale up (sequential phases -- each gets full budget)
        assert config.platform.compute.spark.executor.instances > 8
        assert config.architecture.workload.datagen.parallelism > 10
