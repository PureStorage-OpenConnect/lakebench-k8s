"""Tests for auto-sizing of compute resources."""

import pytest

from lakebench.config import LakebenchConfig
from lakebench.config.autosizer import (
    _parse_cpu_millicores,
    _resolve_datagen_mode,
    resolve_auto_sizing,
)
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

        cuts = resolve_auto_sizing(config, cap)

        # 85% of 16Gi ≈ 13Gi: the Trino worker tier memory is capped to it,
        # and the cap is reported as a cut.
        assert config.architecture.query_engine.trino.worker.memory == "13Gi"
        assert [c for c in cuts if c.startswith("trino.worker.memory capped to 13Gi")], cuts


class TestAutosizerLeavesSparkToTheProfiles:
    """Spark sizing is the job profiles'; the autosizer neither sets nor cuts it."""

    def test_no_spark_executor_change_or_cut(self):
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
        before = config.platform.compute.spark.model_dump()
        resolve_auto_sizing(config, cap)
        assert config.platform.compute.spark.model_dump() == before


# ---------------------------------------------------------------------------
# resolve_auto_sizing -- cluster-aware scaling
# ---------------------------------------------------------------------------


class TestAutoSizingClusterAware:
    """Tests for cluster-aware scaling: cap small scales, scale up large."""

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

    @pytest.mark.parametrize("pattern", ["medallion", "streaming"])
    @pytest.mark.parametrize("scale", [1, 10, 50, 100, 500, 1000])
    def test_no_overprovisioning(self, scale, pattern):
        """Datagen demand plus the co-resident pods must not exceed cluster
        capacity: never schedule more pods than the cluster can run."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "workload": {"datagen": {"scale": scale}},
                "processing": {"pattern": pattern},
            },
        )

        # 8 worker nodes x 40 cores = 320 cores
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
    """Batch at scale 50 and above enables scratch unless the user set it."""

    @pytest.mark.parametrize(
        ("scale", "scratch_override", "expected"),
        [(1, None, False), (50, None, True), (50, {"enabled": False}, False)],
        ids=["small_scale", "scale_50", "user_override_wins"],
    )
    def test_scratch_autoenable(self, scale, scratch_override, expected):
        cfg = LakebenchConfig.model_validate(
            {
                "name": "autoscratch",
                "architecture": {
                    "workload": {"datagen": {"scale": scale}},
                    "pipeline": {"mode": "batch"},
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
        resolve_auto_sizing(cfg)
        assert cfg.platform.storage.scratch.enabled is expected


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


@pytest.mark.parametrize(
    ("scale", "mode", "expected"),
    [(1000, "batch", "batch"), (1, "continuous", "continuous")]
    + [(s, "auto", "continuous") for s in (1, 5, 10, 11, 50, 1000)],
)
def test_datagen_mode_resolution(scale, mode, expected):
    """An explicit mode is kept at any scale; auto resolves to continuous."""
    config = LakebenchConfig(
        name="test", architecture={"workload": {"datagen": {"scale": scale, "mode": mode}}}
    )
    assert _resolve_datagen_mode(config) == expected
