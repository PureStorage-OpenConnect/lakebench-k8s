"""Tests for Spark module (job submission, monitoring)."""

from unittest.mock import MagicMock, patch

from lakebench.config import LakebenchConfig
from lakebench.spark.job import JobState, JobStatus, JobType, SparkJobManager
from lakebench.spark.monitor import JobResult, SparkJobMonitor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig for testing Spark configuration."""
    base = {
        "name": "test-spark",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                    "buckets": {
                        "bronze": "test-bronze",
                        "silver": "test-silver",
                        "gold": "test-gold",
                    },
                }
            }
        },
    }
    base.update(overrides)
    return LakebenchConfig(**base)


def _mock_k8s(**overrides):
    k8s = MagicMock()
    # Default: no cluster capacity (prevents concurrent budget capping)
    k8s.get_cluster_capacity.return_value = None
    for key, val in overrides.items():
        setattr(k8s, key, val)
    return k8s


# ---------------------------------------------------------------------------
# JobType / JobState enums
# ---------------------------------------------------------------------------


class TestJobType:
    """Tests for JobType enum values."""

    def test_job_types(self):
        assert JobType.BRONZE_VERIFY.value == "bronze-verify"
        assert JobType.SILVER_BUILD.value == "silver-build"
        assert JobType.GOLD_FINALIZE.value == "gold-finalize"

    def test_all_types_iterable(self):
        types = list(JobType)
        assert len(types) == 6


class TestJobState:
    """Tests for JobState enum values."""

    def test_states(self):
        assert JobState.PENDING.value == "PENDING"
        assert JobState.SUBMITTED.value == "SUBMITTED"
        assert JobState.RUNNING.value == "RUNNING"
        assert JobState.COMPLETED.value == "COMPLETED"
        assert JobState.FAILED.value == "FAILED"
        assert JobState.UNKNOWN.value == "UNKNOWN"


# ---------------------------------------------------------------------------
# JobStatus dataclass
# ---------------------------------------------------------------------------


class TestJobStatus:
    """Tests for JobStatus dataclass."""

    def test_basic_status(self):
        status = JobStatus(
            name="lakebench-bronze-verify",
            state=JobState.RUNNING,
            message="Running",
        )
        assert status.name == "lakebench-bronze-verify"
        assert status.state == JobState.RUNNING
        assert status.driver_pod is None
        assert status.executor_count == 0

    def test_status_with_details(self):
        status = JobStatus(
            name="lakebench-silver-build",
            state=JobState.COMPLETED,
            message="Done",
            driver_pod="silver-driver-abc",
            executor_count=8,
        )
        assert status.driver_pod == "silver-driver-abc"
        assert status.executor_count == 8


# ---------------------------------------------------------------------------
# SparkJobManager - manifest building
# ---------------------------------------------------------------------------


class TestSparkJobManager:
    """Tests for SparkJobManager manifest building."""

    def test_build_manifest_bronze(self):
        """Bronze manifest should reference bronze_verify.py."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)

        assert manifest["apiVersion"] == "sparkoperator.k8s.io/v1beta2"
        assert manifest["kind"] == "SparkApplication"
        assert manifest["metadata"]["name"] == "lakebench-bronze-verify"
        assert "bronze_verify.py" in manifest["spec"]["mainApplicationFile"]

    def test_build_manifest_silver(self):
        """Silver manifest should reference silver_build.py."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        assert "silver_build.py" in manifest["spec"]["mainApplicationFile"]
        assert manifest["metadata"]["name"] == "lakebench-silver-build"

    def test_build_manifest_gold(self):
        """Gold manifest should reference gold_finalize.py."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.GOLD_FINALIZE)
        assert "gold_finalize.py" in manifest["spec"]["mainApplicationFile"]

    def test_warehouse_bucket_per_stage(self):
        """Silver jobs use silver bucket, gold jobs use gold bucket."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        silver_manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        silver_wh = silver_manifest["spec"]["sparkConf"]["spark.sql.catalog.lakehouse.warehouse"]
        assert "test-silver" in silver_wh

        gold_manifest = mgr._build_manifest(JobType.GOLD_FINALIZE)
        gold_wh = gold_manifest["spec"]["sparkConf"]["spark.sql.catalog.lakehouse.warehouse"]
        assert "test-gold" in gold_wh

        bronze_manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        bronze_wh = bronze_manifest["spec"]["sparkConf"]["spark.sql.catalog.lakehouse.warehouse"]
        assert "test-silver" in bronze_wh

    def test_manifest_scratch_storage_class(self):
        """Scratch PVC should use the configured storage class."""
        config = _make_config()
        config.platform.storage.scratch.enabled = True
        config.platform.storage.scratch.storage_class = "px-csi-scratch"
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)

        # Scratch volumes should appear in executor spec
        executor = manifest["spec"]["executor"]
        # Look for scratch storage class in spark conf or volume spec
        spark_conf = manifest["spec"]["sparkConf"]
        # The storage class flows through spark.kubernetes.executor.volumes config
        found_scratch = False
        for _key, val in spark_conf.items():
            if "px-csi-scratch" in str(val):
                found_scratch = True
                break
        # Also check executor volume mounts or volumes
        if not found_scratch:
            for vol in manifest["spec"].get("volumes", []):
                if vol.get("name", "").startswith("scratch"):
                    found_scratch = True
                    break
        # Check dynamicAllocation or executor volumes
        if not found_scratch:
            vol_mounts = executor.get("volumeMounts", [])
            for vm in vol_mounts:
                if "scratch" in vm.get("name", ""):
                    found_scratch = True
                    break
        assert found_scratch, "px-csi-scratch storage class not found in manifest"

    def test_manifest_has_iceberg_packages(self):
        """Spark conf should include Iceberg JARs."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        spark_conf = manifest["spec"]["sparkConf"]

        packages = spark_conf["spark.jars.packages"]
        assert "iceberg-spark-runtime" in packages
        assert "hadoop-aws" in packages

    def test_manifest_has_s3_config(self):
        """Spark conf should include S3A endpoint."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        spark_conf = manifest["spec"]["sparkConf"]

        assert spark_conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
        assert spark_conf["spark.hadoop.fs.s3a.path.style.access"] == "true"

    def test_manifest_has_catalog_config(self):
        """Spark conf should include Iceberg catalog via Hive."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        spark_conf = manifest["spec"]["sparkConf"]

        # Default catalog name is "lakehouse"
        assert "spark.sql.catalog.lakehouse" in spark_conf
        assert spark_conf["spark.sql.catalog.lakehouse.type"] == "hive"
        assert "thrift://" in spark_conf["spark.sql.catalog.lakehouse.uri"]

    def test_manifest_has_polaris_catalog_config(self):
        """Spark conf should use REST catalog when Polaris is configured."""
        config = _make_config(
            architecture={
                "catalog": {"type": "polaris"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            }
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        spark_conf = manifest["spec"]["sparkConf"]

        # Should use RESTCatalog, not Hive
        assert "spark.sql.catalog.lakehouse.catalog-impl" in spark_conf
        assert "RESTCatalog" in spark_conf["spark.sql.catalog.lakehouse.catalog-impl"]
        assert "spark.sql.catalog.lakehouse.credential" in spark_conf
        # Should NOT have Hive-specific config
        assert "spark.sql.catalog.lakehouse.type" not in spark_conf
        assert "spark.hadoop.hive.metastore.client.socket.timeout" not in spark_conf

    def test_manifest_volumes(self):
        """Manifest should include script, work-dir, and ivy volumes."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        volumes = manifest["spec"]["volumes"]
        volume_names = [v["name"] for v in volumes]

        assert "spark-scripts" in volume_names
        assert "spark-work-dir" in volume_names
        assert "spark-ivy-cache" in volume_names

    def test_manifest_env_vars(self):
        """Driver/executor should have S3 and bucket env vars."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        env = manifest["spec"]["driver"]["env"]
        env_names = [e["name"] for e in env]

        assert "BRONZE_BUCKET" in env_names
        assert "SILVER_BUCKET" in env_names
        assert "GOLD_BUCKET" in env_names
        assert "S3_ENDPOINT" in env_names

    def test_manifest_extra_conf(self):
        """Extra Spark conf should be merged into the manifest."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        extra = {"spark.custom.key": "custom-value"}
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY, extra_conf=extra)
        spark_conf = manifest["spec"]["sparkConf"]

        assert spark_conf["spark.custom.key"] == "custom-value"

    def test_manifest_security_context(self):
        """Spark pods should run as UID 185."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        driver_sc = manifest["spec"]["driver"]["securityContext"]

        assert driver_sc["runAsUser"] == 185
        assert driver_sc["runAsGroup"] == 185

    def test_manifest_labels(self):
        """Manifest should have lakebench labels."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        labels = manifest["metadata"]["labels"]

        assert labels["app.kubernetes.io/name"] == "lakebench"
        assert labels["app.kubernetes.io/managed-by"] == "lakebench"


# ---------------------------------------------------------------------------
# JobResult dataclass
# ---------------------------------------------------------------------------


class TestPerJobExecutorOverridesInManifest:
    """Tests for per-job executor count overrides in SparkJobManager manifests."""

    def test_no_override_uses_auto_scale(self):
        """Without overrides, executor count comes from _scale_executor_count."""
        from lakebench.spark.job import _JOB_PROFILES, _scale_executor_count

        config = _make_config(
            architecture={"workload": {"datagen": {"scale": 100}}},
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        expected = _scale_executor_count(_JOB_PROFILES["silver-build"], 100)
        assert manifest["spec"]["executor"]["instances"] == expected

    def test_override_silver_executors(self):
        """silver_executors override changes executor count in manifest."""
        config = _make_config(
            architecture={"workload": {"datagen": {"scale": 100}}},
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    },
                },
                "compute": {"spark": {"silver_executors": 25}},
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        assert manifest["spec"]["executor"]["instances"] == 25

    def test_override_does_not_affect_other_jobs(self):
        """Overriding silver_executors doesn't change bronze or gold."""
        from lakebench.spark.job import _JOB_PROFILES, _scale_executor_count

        config = _make_config(
            architecture={"workload": {"datagen": {"scale": 100}}},
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    },
                },
                "compute": {"spark": {"silver_executors": 25}},
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        bronze_manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        gold_manifest = mgr._build_manifest(JobType.GOLD_FINALIZE)

        expected_bronze = _scale_executor_count(_JOB_PROFILES["bronze-verify"], 100)
        expected_gold = _scale_executor_count(_JOB_PROFILES["gold-finalize"], 100)

        assert bronze_manifest["spec"]["executor"]["instances"] == expected_bronze
        assert gold_manifest["spec"]["executor"]["instances"] == expected_gold

    def test_per_executor_sizing_unchanged_by_override(self):
        """Overriding executor count does NOT change per-executor sizing."""
        config = _make_config(
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    },
                },
                "compute": {"spark": {"silver_executors": 30}},
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        executor = manifest["spec"]["executor"]

        # Per-executor sizing stays from _JOB_PROFILES, not config
        assert executor["cores"] == 4
        assert executor["memory"] == "48g"
        assert executor["memoryOverhead"] == "12g"
        assert executor["instances"] == 30


class TestDriverResourceOverrides:
    """Tests for driver memory/cores overrides in SparkJobManager manifests."""

    def test_no_override_uses_profile_default(self):
        """Without overrides, driver resources come from _JOB_PROFILES."""
        from lakebench.spark.job import _JOB_PROFILES

        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        driver = manifest["spec"]["driver"]

        assert driver["cores"] == _JOB_PROFILES["silver-build"]["driver_cores"]
        assert driver["memory"] == _JOB_PROFILES["silver-build"]["driver_memory"]

    def test_driver_memory_override(self):
        """driver_memory override changes driver memory in manifest."""
        config = _make_config(
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    },
                },
                "compute": {"spark": {"driver_memory": "32g"}},
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        assert manifest["spec"]["driver"]["memory"] == "32g"

    def test_driver_cores_override(self):
        """driver_cores override changes driver cores in manifest."""
        config = _make_config(
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    },
                },
                "compute": {"spark": {"driver_cores": 8}},
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        assert manifest["spec"]["driver"]["cores"] == 8
        assert manifest["spec"]["driver"]["coreLimit"] == "8"

    def test_driver_overrides_apply_to_all_jobs(self):
        """Driver overrides are global -- they apply to all job types."""
        config = _make_config(
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    },
                },
                "compute": {"spark": {"driver_memory": "24g", "driver_cores": 6}},
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        for job_type in [JobType.BRONZE_VERIFY, JobType.SILVER_BUILD, JobType.GOLD_FINALIZE]:
            manifest = mgr._build_manifest(job_type)
            assert manifest["spec"]["driver"]["memory"] == "24g"
            assert manifest["spec"]["driver"]["cores"] == 6


class TestJobResult:
    """Tests for JobResult dataclass."""

    def test_success_result(self):
        result = JobResult(
            job_name="lakebench-bronze-verify",
            success=True,
            message="Completed",
            elapsed_seconds=53.2,
        )
        assert result.success is True
        assert result.driver_logs is None

    def test_failure_result_with_logs(self):
        result = JobResult(
            job_name="lakebench-silver-build",
            success=False,
            message="OOM killed",
            elapsed_seconds=120.0,
            driver_logs="ERROR: java.lang.OutOfMemoryError",
        )
        assert result.success is False
        assert "OutOfMemoryError" in result.driver_logs


# ---------------------------------------------------------------------------
# SparkJobMonitor - basic init
# ---------------------------------------------------------------------------


class TestSparkJobMonitor:
    """Tests for SparkJobMonitor initialisation."""

    def test_monitor_init(self):
        config = _make_config()
        k8s = _mock_k8s()
        monitor = SparkJobMonitor(config, k8s)

        assert monitor.namespace == "test-spark"
        assert monitor.job_manager is not None

    def test_monitor_timeout_result(self):
        """Verify the timeout path returns a failed JobResult."""
        config = _make_config()
        k8s = _mock_k8s()
        monitor = SparkJobMonitor(config, k8s)

        # Mock job_manager to always return RUNNING
        monitor.job_manager = MagicMock()
        monitor.job_manager.get_job_status.return_value = JobStatus(
            name="test-job",
            state=JobState.RUNNING,
            message="Still running",
        )

        # Use very short timeout so test completes quickly
        with patch.object(monitor, "_get_driver_logs", return_value="timeout logs"):
            result = monitor.wait_for_completion(
                "test-job",
                timeout_seconds=0,  # Immediate timeout
                poll_interval=0,
            )

        assert result.success is False
        assert "timed out" in result.message


# ---------------------------------------------------------------------------
# Streaming concurrent budget
# ---------------------------------------------------------------------------


class TestStreamingConcurrentBudget:
    """Tests for streaming concurrent resource budgeting."""

    def _make_k8s_with_capacity(self, total_cpu_m):
        """Create a mock K8s client with cluster capacity."""
        from lakebench.k8s.client import ClusterCapacity

        k8s = _mock_k8s()
        k8s.get_cluster_capacity.return_value = ClusterCapacity(
            total_cpu_millicores=total_cpu_m,
            total_memory_bytes=8 * 432 * 1024**3,
            node_count=8,
            largest_node_cpu_millicores=total_cpu_m // 8,
            largest_node_memory_bytes=432 * 1024**3,
        )
        return k8s

    def test_streaming_budget_caps_to_cluster(self):
        """On a small cluster, streaming executor counts should be reduced."""
        from lakebench.spark.job import _JOB_PROFILES, _scale_executor_count

        config = _make_config(
            architecture={
                "workload": {"datagen": {"scale": 100}},
            },
        )

        # Small cluster: 64 cores -- streaming jobs should get capped
        k8s = self._make_k8s_with_capacity(64000)
        mgr = SparkJobManager(config, k8s)

        # Get uncapped executor count for silver-stream
        profile = _JOB_PROFILES["silver-stream"]
        uncapped = _scale_executor_count(profile, 100)

        manifest = mgr._build_manifest(JobType.SILVER_STREAM)
        actual = manifest["spec"]["executor"]["instances"]

        # Should be capped below uncapped
        assert actual < uncapped, (
            f"silver-stream should be capped: actual={actual}, uncapped={uncapped}"
        )
        # But at least 2 (minimum)
        assert actual >= 2

    def test_batch_jobs_unaffected_by_budget(self):
        """Batch manifest executor counts should be unchanged by concurrent budget."""

        config = _make_config(
            architecture={
                "workload": {"datagen": {"scale": 100}},
            },
        )

        # With and without cluster capacity -- batch should be same
        k8s_no_cap = _mock_k8s()
        mgr_no_cap = SparkJobManager(config, k8s_no_cap)
        manifest_no_cap = mgr_no_cap._build_manifest(JobType.SILVER_BUILD)

        k8s_with_cap = self._make_k8s_with_capacity(64000)
        mgr_with_cap = SparkJobManager(config, k8s_with_cap)
        manifest_with_cap = mgr_with_cap._build_manifest(JobType.SILVER_BUILD)

        # Batch jobs use profile-based scaling, not concurrent budget
        assert (
            manifest_no_cap["spec"]["executor"]["instances"]
            == manifest_with_cap["spec"]["executor"]["instances"]
        )

    def test_streaming_budget_without_cluster_cap(self):
        """Without cluster capacity, streaming jobs use profile-derived counts."""
        from lakebench.spark.job import _JOB_PROFILES, _scale_executor_count

        config = _make_config(
            architecture={
                "workload": {"datagen": {"scale": 100}},
            },
        )

        k8s = _mock_k8s()  # No cluster capacity
        mgr = SparkJobManager(config, k8s)

        profile = _JOB_PROFILES["silver-stream"]
        expected = _scale_executor_count(profile, 100)

        manifest = mgr._build_manifest(JobType.SILVER_STREAM)
        actual = manifest["spec"]["executor"]["instances"]

        assert actual == expected

    def test_user_override_bypasses_budget(self):
        """Per-job executor override should bypass concurrent budget cap."""
        config = _make_config(
            architecture={
                "workload": {"datagen": {"scale": 100}},
            },
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    },
                },
                "compute": {
                    "spark": {
                        "silver_stream_executors": 15,
                    },
                },
            },
        )

        # Even on a small cluster, override wins
        k8s = self._make_k8s_with_capacity(64000)
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_STREAM)
        assert manifest["spec"]["executor"]["instances"] == 15

    def test_proportional_allocation(self):
        """Silver-stream (highest demand) should get the largest share."""
        from lakebench.spark.job import _streaming_concurrent_budget

        config = _make_config(
            architecture={
                "workload": {"datagen": {"scale": 100}},
            },
        )

        budget = _streaming_concurrent_budget(config, 320000)

        # silver-stream uses 4 cores/executor, more executors → highest demand
        # gold-refresh has same cores but fewer executors
        # bronze-ingest uses 2 cores/executor, lowest demand
        assert budget[JobType.SILVER_STREAM] >= budget[JobType.GOLD_REFRESH]
        assert budget[JobType.GOLD_REFRESH] >= budget[JobType.BRONZE_INGEST]


# ---------------------------------------------------------------------------
# Phase 1: Monitor returns driver_logs on success
# ---------------------------------------------------------------------------


class TestMonitorDriverLogsOnSuccess:
    """Tests that wait_for_completion returns driver_logs for successful jobs."""

    def test_completed_job_has_driver_logs(self):
        """COMPLETED path should include driver_logs (not None)."""
        config = _make_config()
        k8s = _mock_k8s()
        monitor = SparkJobMonitor(config, k8s)

        # Mock job status to return COMPLETED immediately
        monitor.job_manager = MagicMock()
        monitor.job_manager.get_job_status.return_value = JobStatus(
            name="test-job",
            state=JobState.COMPLETED,
            message="Job completed successfully",
        )

        expected_logs = """\
[lb] 2026-02-01T10:00:45.000000 - === JOB METRICS: bronze-verify ===
[lb] 2026-02-01T10:00:45.000000 - input_size_gb: 9.523
[lb] 2026-02-01T10:00:45.000000 - elapsed_seconds: 45.2
[lb] 2026-02-01T10:00:45.000000 - ========================================
"""

        with patch.object(monitor, "_get_driver_logs", return_value=expected_logs):
            result = monitor.wait_for_completion(
                "test-job",
                timeout_seconds=60,
                poll_interval=0,
            )

        assert result.success is True
        assert result.driver_logs is not None
        assert "JOB METRICS" in result.driver_logs

    def test_failed_job_also_has_driver_logs(self):
        """FAILED path should also include driver_logs."""
        config = _make_config()
        k8s = _mock_k8s()
        monitor = SparkJobMonitor(config, k8s)

        monitor.job_manager = MagicMock()
        monitor.job_manager.get_job_status.return_value = JobStatus(
            name="test-job",
            state=JobState.FAILED,
            message="OOM killed",
        )

        with patch.object(monitor, "_get_driver_logs", return_value="ERROR: OOM"):
            result = monitor.wait_for_completion(
                "test-job",
                timeout_seconds=60,
                poll_interval=0,
            )

        assert result.success is False
        assert result.driver_logs is not None
        assert "OOM" in result.driver_logs


# ---------------------------------------------------------------------------
# Streaming env vars: throughput tuning fields
# ---------------------------------------------------------------------------


class TestStreamingThroughputEnvVars:
    """Tests that throughput tuning config fields are injected as env vars."""

    def _get_env_dict(self, manifest):
        """Extract driver env vars as a dict from a manifest (skip secretKeyRef entries)."""
        env_list = manifest["spec"]["driver"]["env"]
        return {e["name"]: e["value"] for e in env_list if "value" in e}

    def test_bronze_ingest_gets_max_files_and_target_size(self):
        config = _make_config(
            architecture={
                "processing": {
                    "continuous": {
                        "max_files_per_trigger": 100,
                        "bronze_target_file_size_mb": 256,
                    },
                },
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.BRONZE_INGEST)
        env = self._get_env_dict(manifest)

        assert env["MAX_FILES_PER_TRIGGER"] == "100"
        assert env["TARGET_FILE_SIZE_BYTES"] == str(256 * 1024 * 1024)

    def test_silver_stream_gets_target_size(self):
        config = _make_config(
            architecture={
                "processing": {
                    "continuous": {
                        "silver_target_file_size_mb": 1024,
                    },
                },
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.SILVER_STREAM)
        env = self._get_env_dict(manifest)

        assert env["TARGET_FILE_SIZE_BYTES"] == str(1024 * 1024 * 1024)

    def test_gold_refresh_gets_target_size(self):
        config = _make_config(
            architecture={
                "processing": {
                    "continuous": {
                        "gold_target_file_size_mb": 64,
                    },
                },
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.GOLD_REFRESH)
        env = self._get_env_dict(manifest)

        assert env["TARGET_FILE_SIZE_BYTES"] == str(64 * 1024 * 1024)

    def test_defaults_match_schema(self):
        """Default env var values match the ContinuousConfig defaults."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        bronze_env = self._get_env_dict(mgr._build_manifest(JobType.BRONZE_INGEST))
        silver_env = self._get_env_dict(mgr._build_manifest(JobType.SILVER_STREAM))
        gold_env = self._get_env_dict(mgr._build_manifest(JobType.GOLD_REFRESH))

        assert bronze_env["MAX_FILES_PER_TRIGGER"] == "50"
        assert bronze_env["TARGET_FILE_SIZE_BYTES"] == str(512 * 1024 * 1024)
        assert silver_env["TARGET_FILE_SIZE_BYTES"] == str(512 * 1024 * 1024)
        assert gold_env["TARGET_FILE_SIZE_BYTES"] == str(128 * 1024 * 1024)


# ---------------------------------------------------------------------------
# Spark Operator Namespace Watching
# ---------------------------------------------------------------------------


class TestSparkOperatorNamespaceWatching:
    """Tests for Spark Operator namespace watching detection and self-healing."""

    def test_operator_status_backward_compatible(self):
        """OperatorStatus can be constructed without the new fields."""
        from lakebench.spark.operator import OperatorStatus

        status = OperatorStatus(
            installed=True,
            version="2.4.0",
            namespace="spark-operator",
            ready=True,
            message="OK",
        )
        assert status.watching_namespace is None
        assert status.watched_namespaces is None

    @patch("lakebench.spark.operator.subprocess.run")
    def test_get_watched_namespaces_returns_list(self, mock_run):
        """When helm returns explicit namespaces, returns them as a list."""
        from lakebench.spark.operator import SparkOperatorManager

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"spark":{"jobNamespaces":["default","lakebench-test"]}}',
        )
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        result = mgr._get_watched_namespaces()
        assert result == ["default", "lakebench-test"]

    @patch("lakebench.spark.operator.subprocess.run")
    def test_get_watched_namespaces_empty_means_all(self, mock_run):
        """When jobNamespaces is empty list, returns None (watches all)."""
        from lakebench.spark.operator import SparkOperatorManager

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"spark":{"jobNamespaces":[]}}',
        )
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        result = mgr._get_watched_namespaces()
        assert result is None

    @patch("lakebench.spark.operator.subprocess.run")
    def test_get_watched_namespaces_not_set_means_all(self, mock_run):
        """When spark.jobNamespaces key is missing, returns None (watches all)."""
        from lakebench.spark.operator import SparkOperatorManager

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"webhook":{"enable":true}}',
        )
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        result = mgr._get_watched_namespaces()
        assert result is None

    @patch("lakebench.spark.operator.subprocess.run")
    def test_get_watched_namespaces_empty_string_means_all(self, mock_run):
        """When jobNamespaces is empty string, returns None (watches all)."""
        from lakebench.spark.operator import SparkOperatorManager

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"spark":{"jobNamespaces":""}}',
        )
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        result = mgr._get_watched_namespaces()
        assert result is None

    @patch("lakebench.spark.operator.subprocess.run")
    def test_get_watched_namespaces_helm_failure(self, mock_run):
        """When helm fails, returns empty list (unknown)."""
        from lakebench.spark.operator import SparkOperatorManager

        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Error: release not found",
        )
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        result = mgr._get_watched_namespaces()
        assert result == []

    @patch("lakebench.spark.operator.subprocess.run")
    def test_check_status_watching_namespace_true(self, mock_run):
        """check_status sets watching_namespace=True when namespace is in list."""
        from lakebench.spark.operator import SparkOperatorManager

        mock_run.side_effect = [
            # CRD check
            MagicMock(returncode=0, stdout="sparkapplications"),
            # Deployment list
            MagicMock(
                returncode=0,
                stdout="NAMESPACE       NAME\nspark-operator  spark-op-ctrl",
            ),
            # Ready replicas
            MagicMock(returncode=0, stdout="1"),
            # Helm version (helm list)
            MagicMock(
                returncode=0,
                stdout='[{"chart":"spark-operator-2.4.0"}]',
            ),
            # Helm get values (for _get_watched_namespaces)
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["lakebench-test"]}}',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.check_status()
        assert status.ready is True
        assert status.watching_namespace is True

    @patch("lakebench.spark.operator.subprocess.run")
    def test_check_status_watching_namespace_false(self, mock_run):
        """check_status sets watching_namespace=False when namespace is NOT in list."""
        from lakebench.spark.operator import SparkOperatorManager

        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="sparkapplications"),
            MagicMock(
                returncode=0,
                stdout="NAMESPACE       NAME\nspark-operator  spark-op-ctrl",
            ),
            MagicMock(returncode=0, stdout="1"),
            MagicMock(
                returncode=0,
                stdout='[{"chart":"spark-operator-2.4.0"}]',
            ),
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["default"]}}',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.check_status()
        assert status.ready is True
        assert status.watching_namespace is False
        assert "does NOT watch" in status.message

    @patch("lakebench.spark.operator.subprocess.run")
    def test_check_status_watches_all_namespaces(self, mock_run):
        """check_status sets watching_namespace=True when operator watches all."""
        from lakebench.spark.operator import SparkOperatorManager

        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="sparkapplications"),
            MagicMock(
                returncode=0,
                stdout="NAMESPACE       NAME\nspark-operator  spark-op-ctrl",
            ),
            MagicMock(returncode=0, stdout="1"),
            MagicMock(
                returncode=0,
                stdout='[{"chart":"spark-operator-2.4.0"}]',
            ),
            # Empty jobNamespaces = watches all
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":[]}}',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.check_status()
        assert status.ready is True
        assert status.watching_namespace is True

    @patch("lakebench.spark.operator.subprocess.run")
    def test_ensure_namespace_watched_provides_fix_command(self, mock_run):
        """When can_heal=False, message contains the exact helm fix command."""
        from lakebench.spark.operator import SparkOperatorManager

        # check_status() returns watching_namespace=False
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="sparkapplications"),
            MagicMock(
                returncode=0,
                stdout="NAMESPACE       NAME\nspark-operator  spark-op-ctrl",
            ),
            MagicMock(returncode=0, stdout="1"),
            MagicMock(
                returncode=0,
                stdout='[{"chart":"spark-operator-2.4.0"}]',
            ),
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["lakebench"]}}',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.ensure_namespace_watched(can_heal=False)
        assert status.watching_namespace is False
        assert "helm upgrade" in status.message
        assert "lakebench-test" in status.message
        assert "lakebench,lakebench-test" in status.message

    @patch("lakebench.spark.operator.subprocess.run")
    def test_ensure_namespace_watched_self_heals(self, mock_run):
        """When can_heal=True, adds namespace via helm upgrade."""
        from lakebench.spark.operator import SparkOperatorManager

        # First batch: check_status() returns watching_namespace=False
        # Then: _add_namespace_to_watch() calls _get_watched_namespaces + helm upgrade
        # Then: re-check_status() returns watching_namespace=True
        mock_run.side_effect = [
            # check_status() -- CRD, deployment, ready, helm version, helm values
            MagicMock(returncode=0, stdout="sparkapplications"),
            MagicMock(
                returncode=0,
                stdout="NAMESPACE       NAME\nspark-operator  spark-op-ctrl",
            ),
            MagicMock(returncode=0, stdout="1"),
            MagicMock(
                returncode=0,
                stdout='[{"chart":"spark-operator-2.4.0"}]',
            ),
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["lakebench"]}}',
            ),
            # _add_namespace_to_watch: _get_watched_namespaces
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["lakebench"]}}',
            ),
            # _add_namespace_to_watch: helm upgrade
            MagicMock(returncode=0, stdout="Release updated"),
            # _is_openshift check (returncode=1 -> not OpenShift)
            MagicMock(returncode=1, stdout=""),
            # _restart_operator: rollout restart + rollout status
            MagicMock(returncode=0, stdout="deployment restarted"),
            MagicMock(returncode=0, stdout="successfully rolled out"),
            # re-check_status() -- CRD, deployment, ready, helm version, helm values
            MagicMock(returncode=0, stdout="sparkapplications"),
            MagicMock(
                returncode=0,
                stdout="NAMESPACE       NAME\nspark-operator  spark-op-ctrl",
            ),
            MagicMock(returncode=0, stdout="1"),
            MagicMock(
                returncode=0,
                stdout='[{"chart":"spark-operator-2.4.0"}]',
            ),
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["lakebench","lakebench-test"]}}',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.ensure_namespace_watched(can_heal=True)
        assert status.watching_namespace is True

    @patch("lakebench.spark.operator.subprocess.run")
    def test_install_handles_helm_not_found(self, mock_run):
        """install() returns False when helm binary is not on PATH."""
        from lakebench.spark.operator import SparkOperatorManager

        def _side_effect(cmd, **kwargs):
            if cmd[0] == "helm":
                raise FileNotFoundError("helm not found")
            # kubectl calls (e.g. _is_openshift) succeed
            return MagicMock(returncode=1, stdout="", stderr="")

        mock_run.side_effect = _side_effect
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        assert mgr.install() is False

    @patch("lakebench.spark.operator.subprocess.run")
    def test_add_namespace_handles_helm_not_found(self, mock_run):
        """_add_namespace_to_watch returns False when helm is not on PATH."""
        from lakebench.spark.operator import SparkOperatorManager

        # First call: _get_watched_namespaces returns a list
        # Second call: helm upgrade raises FileNotFoundError
        mock_run.side_effect = [
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["default"]}}',
            ),
            FileNotFoundError("helm not found"),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        assert mgr._add_namespace_to_watch("lakebench-test") is False
