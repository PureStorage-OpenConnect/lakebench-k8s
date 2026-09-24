"""Tests for Spark module (job submission, monitoring)."""

from unittest.mock import MagicMock, patch

import pytest

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
        # 6 medallion + 3 Financial-only operator actions (ENG-2C.3g/h/i)
        # + 1 reference detector / leakage gate (SCORE_FINANCIAL_REFERENCE, LB-130 gate)
        assert len(types) == 10
        assert JobType.SCORE_FINANCIAL_REFERENCE in types


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

    def test_manifest_has_maven_mirror_repositories(self):
        """Spark conf must set ``spark.jars.repositories`` to a Central
        mirror so Ivy falls to it when the cluster's egress hits an
        HTTP 429 rate-limit on repo1.maven.org. Live-verified 2026-09-22
        on faml-baseline-s1 where a fresh Central 429 blocked
        bronze-verify; adding this fallback let the same run finish.
        """
        from lakebench.spark.job import _MAVEN_MIRROR_REPOS

        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        spark_conf = manifest["spec"]["sparkConf"]

        assert "spark.jars.repositories" in spark_conf, (
            "spark.jars.repositories missing -- Ivy has no Central mirror "
            "fallback when repo1.maven.org 429s the cluster's egress IP"
        )
        assert spark_conf["spark.jars.repositories"] == _MAVEN_MIRROR_REPOS
        assert "maven-central.storage-download.googleapis.com" in _MAVEN_MIRROR_REPOS

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
                "catalog": {
                    "type": "polaris",
                    "polaris": {"client_secret": "test-only-secret"},
                },
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
        """Volumes should be in pod templates (not top-level spec)."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)

        # Volumes must NOT be in the top-level spec (avoids duplicates
        # with the Spark Operator webhook which also injects .spec.volumes).
        assert "volumes" not in manifest["spec"]

        # Volumes should be in driver and executor pod templates
        driver_tpl = manifest["spec"]["driver"]["template"]
        driver_vols = [v["name"] for v in driver_tpl["spec"]["volumes"]]
        assert "spark-scripts" in driver_vols
        assert "spark-work-dir" in driver_vols
        assert "spark-ivy-cache" in driver_vols

        executor_tpl = manifest["spec"]["executor"]["template"]
        executor_vols = [v["name"] for v in executor_tpl["spec"]["volumes"]]
        assert "spark-scripts" in executor_vols
        assert "spark-work-dir" in executor_vols
        assert "spark-ivy-cache" in executor_vols

    def test_manifest_truststore_when_ca_cert(self):
        """When ca_cert is set, truststore volumes and init container are added."""
        config = _make_config(
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "https://flashblade:443",
                        "access_key": "key",
                        "secret_key": "secret",
                        "ca_cert": "/tmp/ca.pem",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    }
                }
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)

        driver_tpl = manifest["spec"]["driver"]["template"]
        driver_vols = [v["name"] for v in driver_tpl["spec"]["volumes"]]
        assert "ca-cert" in driver_vols
        assert "truststore" in driver_vols

        # Check keytool init container exists
        init_names = [ic["name"] for ic in driver_tpl["spec"]["initContainers"]]
        assert "import-ca-cert" in init_names

        # Check JVM truststore args
        spark_conf = manifest["spec"]["sparkConf"]
        assert "trustStore" in spark_conf.get("spark.driver.extraJavaOptions", "")
        assert "trustStore" in spark_conf.get("spark.executor.extraJavaOptions", "")

    def test_manifest_no_truststore_without_ca_cert(self):
        """Without ca_cert, no truststore volumes or init container."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)

        driver_tpl = manifest["spec"]["driver"]["template"]
        driver_vols = [v["name"] for v in driver_tpl["spec"]["volumes"]]
        assert "ca-cert" not in driver_vols
        assert "truststore" not in driver_vols
        assert "trustStore" not in manifest["spec"]["sparkConf"].get(
            "spark.driver.extraJavaOptions", ""
        )

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
        """Without overrides, driver resources come from _JOB_PROFILES.

        The default image is Spark 4.0.x, which uses the profile's 32g
        driver memory (Spark 4's SDK v2 jar payload needs more heap).
        """
        from lakebench.spark.job import _JOB_PROFILES

        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        driver = manifest["spec"]["driver"]

        assert driver["cores"] == _JOB_PROFILES["silver-build"]["driver_cores"]
        # Spark 4 uses profile default: 32g
        assert driver["memory"] == "32g"

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


class TestMaxResultSizeScaling:
    """Tests for dynamic spark.driver.maxResultSize based on executor count."""

    def test_low_executor_count_gets_floor(self):
        """At default scale (10), few executors -> maxResultSize = 8g (Spark 4 floor)."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        spark_conf = manifest["spec"]["sparkConf"]
        # Default image is Spark 4: min(16, max(8, 4//2)) = 8g
        assert spark_conf["spark.driver.maxResultSize"] == "8g"

    def test_high_executor_override_scales_max_result(self):
        """24 executors -> maxResultSize = 12g (Spark 4: min(16, max(8, 24//2)))."""
        config = _make_config(
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "ak",
                        "secret_key": "sk",
                        "buckets": {
                            "bronze": "b",
                            "silver": "s",
                            "gold": "g",
                        },
                    }
                },
                "compute": {"spark": {"silver_executors": 24}},
            }
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        spark_conf = manifest["spec"]["sparkConf"]
        assert spark_conf["spark.driver.maxResultSize"] == "12g"

    def test_max_result_size_capped_at_16g(self):
        """Even at extreme executor counts, cap at 16g."""
        config = _make_config(
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "ak",
                        "secret_key": "sk",
                        "buckets": {
                            "bronze": "b",
                            "silver": "s",
                            "gold": "g",
                        },
                    }
                },
                "compute": {"spark": {"silver_executors": 60}},
            }
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        spark_conf = manifest["spec"]["sparkConf"]
        # min(16, max(4, 60//3)) = min(16, 20) = 16
        assert spark_conf["spark.driver.maxResultSize"] == "16g"

    def test_user_spark_conf_override_takes_precedence(self):
        """If user sets maxResultSize in spark.conf, it wins."""
        config = _make_config(spark={"conf": {"spark.driver.maxResultSize": "2g"}})
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        spark_conf = manifest["spec"]["sparkConf"]
        assert spark_conf["spark.driver.maxResultSize"] == "2g"


class TestMaxExecutorsCaps:
    """Tests for max_executors limits in _JOB_PROFILES."""

    def test_silver_max_executors_28(self):
        """Silver auto-scale caps at 28."""
        from lakebench.spark.job import _JOB_PROFILES, _scale_executor_count

        count = _scale_executor_count(_JOB_PROFILES["silver-build"], 10000)
        assert count == 28

    def test_gold_max_executors_28(self):
        """Gold auto-scale caps at 28."""
        from lakebench.spark.job import _JOB_PROFILES, _scale_executor_count

        count = _scale_executor_count(_JOB_PROFILES["gold-finalize"], 10000)
        assert count == 28

    def test_bronze_max_executors_20(self):
        """Bronze auto-scale caps at 20 (unchanged)."""
        from lakebench.spark.job import _JOB_PROFILES, _scale_executor_count

        count = _scale_executor_count(_JOB_PROFILES["bronze-verify"], 10000)
        assert count == 20


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
                    "sustained": {
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
                    "sustained": {
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
                    "sustained": {
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

    def test_financial_bronze_ingest_gets_lb_financial_env_aliases(self):
        """LB-090 bronze half: bronze_ingest_financial reads
        ``LB_FINANCIAL_BRONZE_CHECKPOINT``, ``LB_FINANCIAL_BRONZE_MAX_FILES``,
        ``LB_FINANCIAL_BRONZE_TRIGGER_S``. Without matching aliases the
        script falls back to a hard-coded checkpoint under
        ``s3a://lb-bronze/`` (a bucket that is NOT part of the current
        deployment on any non-default config), and two parallel
        sustained runs stomp each other's checkpoint state -- silent
        corruption of the very parallel-safety invariant the S-P
        scenarios exist to prove.
        """
        config = _make_config(
            architecture={
                "workload": {"schema": "financial", "datagen": {"scale": 1}},
                "processing": {
                    "sustained": {
                        "max_files_per_trigger": 30,
                        "bronze_trigger_interval": "20 seconds",
                    },
                },
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.BRONZE_INGEST)
        env = self._get_env_dict(manifest)

        assert "CHECKPOINT_LOCATION" in env
        assert env["MAX_FILES_PER_TRIGGER"] == "30"
        assert env["TRIGGER_INTERVAL"] == "20 seconds"

        assert env.get("LB_FINANCIAL_BRONZE_CHECKPOINT") == env["CHECKPOINT_LOCATION"]
        assert env.get("LB_FINANCIAL_BRONZE_MAX_FILES") == "30"
        # bronze_ingest_financial expects an int-seconds string.
        assert env.get("LB_FINANCIAL_BRONZE_TRIGGER_S") == "20"

    def test_financial_silver_stream_gets_lb_financial_env_aliases(self):
        """LB-090 silver half: silver_stream_financial reads
        ``LB_FINANCIAL_SILVER_CHECKPOINT`` and
        ``LB_FINANCIAL_SILVER_TRIGGER_S``. Same failure shape as bronze
        -- silent checkpoint at a wrong bucket on any non-default
        config, and cross-run stomping on parallel deploys."""
        config = _make_config(
            architecture={
                "workload": {"schema": "financial", "datagen": {"scale": 1}},
                "processing": {
                    "sustained": {"silver_trigger_interval": "45 seconds"},
                },
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.SILVER_STREAM)
        env = self._get_env_dict(manifest)

        assert env.get("LB_FINANCIAL_SILVER_CHECKPOINT") == env["CHECKPOINT_LOCATION"]
        assert env.get("LB_FINANCIAL_SILVER_TRIGGER_S") == "45"

    def test_financial_gold_refresh_gets_lb_financial_env_alias(self):
        """LB-090 gold half: gold_refresh_financial reads
        ``LB_FINANCIAL_GOLD_REFRESH_S`` (an int seconds trigger). Gold
        refresh does not have a checkpoint like the streaming stages,
        so only the refresh-interval alias is required."""
        config = _make_config(
            architecture={
                "workload": {"schema": "financial", "datagen": {"scale": 1}},
                "processing": {
                    "sustained": {"gold_refresh_interval": "2 minutes"},
                },
            },
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.GOLD_REFRESH)
        env = self._get_env_dict(manifest)

        assert env.get("LB_FINANCIAL_GOLD_REFRESH_S") == "120"

    def test_c360_streaming_does_not_get_lb_financial_env(self):
        """The LB_FINANCIAL_* aliases must only appear under
        ``workload.schema=financial``. Belt and braces against a
        future generic script that greps for the LB_FINANCIAL_ prefix."""
        config = _make_config()  # default schema = customer360
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        for jt in (JobType.BRONZE_INGEST, JobType.SILVER_STREAM, JobType.GOLD_REFRESH):
            manifest = mgr._build_manifest(jt)
            env = self._get_env_dict(manifest)
            for k in env:
                assert not k.startswith("LB_FINANCIAL_"), (
                    f"{jt.value} unexpectedly carries {k!r} under a C360 schema"
                )


# ---------------------------------------------------------------------------
# Spark Operator Namespace Watching
# ---------------------------------------------------------------------------


class TestSparkOperatorNamespaceWatching:
    """Tests for Spark Operator namespace watching detection and self-healing."""

    @pytest.fixture(autouse=True)
    def _bypass_cluster_lock(self):
        """ADR-F5 wraps _add_namespace_to_watch in a cluster lease
        acquisition. In this class the tests mock subprocess.run to
        drive helm exchange; the lease attempt would introduce extra
        subprocess.run calls (git rev-parse in build_holder_id) and
        upset side_effect counts. Bypass the lease here -- the
        concurrency safety it provides is covered by
        test_watch_list_strict.py."""
        from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

        SparkOperatorManager._bypass_cluster_lock = True
        try:
            yield
        finally:
            try:
                del SparkOperatorManager._bypass_cluster_lock
            except AttributeError:
                pass

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

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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
            # _get_active_namespaces: deployment spec args
            MagicMock(
                returncode=0,
                stdout='["controller","start","--namespaces=default,lakebench-test"]',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.check_status()
        assert status.ready is True
        assert status.watching_namespace is True

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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
            # _get_active_namespaces: deployment spec args (no lakebench-test)
            MagicMock(
                returncode=0,
                stdout='["controller","start","--namespaces=default"]',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.check_status()
        assert status.ready is True
        assert status.watching_namespace is False
        assert "does NOT watch" in status.message

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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
            # _get_active_namespaces: no --namespaces arg = watches all
            MagicMock(
                returncode=0,
                stdout='["controller","start","--zap-log-level=info"]',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.check_status()
        assert status.ready is True
        assert status.watching_namespace is True

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
    def test_check_status_falls_back_to_helm_values(self, mock_run):
        """check_status falls back to Helm values when deployment spec unreadable."""
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
            # _get_active_namespaces: kubectl fails -> raises _DeploymentReadError
            MagicMock(returncode=1, stdout="", stderr="not found"),
            # _get_watched_namespaces: helm values fallback
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["default","lakebench-test"]}}',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.check_status()
        assert status.ready is True
        assert status.watching_namespace is True

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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
            # _get_active_namespaces: only has lakebench, not lakebench-test
            MagicMock(
                returncode=0,
                stdout='["controller","start","--namespaces=lakebench"]',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.ensure_namespace_watched(can_heal=False)
        assert status.watching_namespace is False
        assert "helm upgrade" in status.message
        assert "lakebench-test" in status.message
        assert "lakebench,lakebench-test" in status.message

    @patch(
        "lakebench.spark.operator.SparkOperatorManager._filter_existing_namespaces",
        side_effect=lambda ns: ns,
    )
    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
    def test_ensure_namespace_watched_self_heals(self, mock_run, _mock_filter):
        """When can_heal=True, adds namespace via helm upgrade + restart + verify."""
        from lakebench.spark.operator import SparkOperatorManager

        # First batch: check_status() returns watching_namespace=False
        # Then: _add_namespace_to_watch() calls _get_watched_namespaces + helm upgrade
        #       + restart (controller+webhook) + verify + re-check_status()
        mock_run.side_effect = [
            # check_status() -- CRD, deployment, ready, helm version, deploy args
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
            # _get_active_namespaces: only lakebench, not lakebench-test
            MagicMock(
                returncode=0,
                stdout='["controller","start","--namespaces=lakebench"]',
            ),
            # _add_namespace_to_watch: _get_watched_namespaces (uses helm values)
            MagicMock(
                returncode=0,
                stdout='{"spark":{"jobNamespaces":["lakebench"]}}',
            ),
            # _add_namespace_to_watch: helm upgrade
            MagicMock(returncode=0, stdout="Release updated"),
            # _is_openshift check (returncode=1 -> not OpenShift)
            MagicMock(returncode=1, stdout=""),
            # _restart_operator: rollout restart (controller + webhook)
            MagicMock(returncode=0, stdout="deployment restarted"),
            MagicMock(returncode=0, stdout="deployment restarted"),
            # _restart_operator: rollout status (controller + webhook)
            MagicMock(returncode=0, stdout="successfully rolled out"),
            MagicMock(returncode=0, stdout="successfully rolled out"),
            # _verify_namespace_watched: kubectl get pods
            MagicMock(
                returncode=0,
                stdout='["controller","start","--namespaces=lakebench,lakebench-test"]',
            ),
            # re-check_status() -- CRD, deployment, ready, helm version, deploy args
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
            # _get_active_namespaces: now includes lakebench-test
            MagicMock(
                returncode=0,
                stdout='["controller","start","--namespaces=lakebench,lakebench-test"]',
            ),
        ]
        mgr = SparkOperatorManager(job_namespace="lakebench-test")
        status = mgr.ensure_namespace_watched(can_heal=True)
        assert status.watching_namespace is True

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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

    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
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


# ---------------------------------------------------------------------------
# Polaris Spark Manifest Tests
# ---------------------------------------------------------------------------


_POLARIS_ARCH = {
    "catalog": {"type": "polaris", "polaris": {"client_secret": "test-only-secret"}},
    "table_format": {"type": "iceberg"},
    "query_engine": {"type": "trino"},
}


class TestPolarisSparkManifest:
    """Tests that Polaris catalog config is correctly injected into Spark manifests."""

    def test_polaris_streaming_manifest(self):
        """Streaming manifest uses RESTCatalog URI, not Hive type key."""
        config = _make_config(architecture=_POLARIS_ARCH)
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.BRONZE_INGEST)
        spark_conf = manifest["spec"]["sparkConf"]

        assert "RESTCatalog" in spark_conf["spark.sql.catalog.lakehouse.catalog-impl"]
        assert "polaris" in spark_conf["spark.sql.catalog.lakehouse.uri"]
        assert "spark.sql.catalog.lakehouse.type" not in spark_conf

    def test_polaris_spark4_packages(self):
        """Spark 4 + Polaris uses iceberg-aws-bundle, drops aws-java-sdk-bundle."""
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
                    }
                },
                "compute": {"spark": {"image": "apache/spark:4.0.0-python3"}},
            },
            architecture=_POLARIS_ARCH,
            images={"spark": "apache/spark:4.0.0-python3"},
        )
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        spark_conf = manifest["spec"]["sparkConf"]
        packages = spark_conf["spark.jars.packages"]

        assert "iceberg-spark-runtime-4" in packages
        assert "iceberg-aws-bundle" in packages
        assert "aws-java-sdk-bundle" not in packages
        assert spark_conf["spark.sql.catalog.lakehouse.rest.http-client.type"] == "apache"

    def test_polaris_oauth2_scope(self):
        """Polaris manifest includes OAuth2 scope and credential."""
        config = _make_config(architecture=_POLARIS_ARCH)
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        spark_conf = manifest["spec"]["sparkConf"]

        assert spark_conf["spark.sql.catalog.lakehouse.scope"] == "PRINCIPAL_ROLE:ALL"
        assert spark_conf.get("spark.sql.catalog.lakehouse.credential")

    def test_polaris_s3_credentials_in_manifest(self):
        """Polaris manifest includes static S3 credentials (no STS vending)."""
        config = _make_config(architecture=_POLARIS_ARCH)
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)
        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        spark_conf = manifest["spec"]["sparkConf"]

        assert "spark.sql.catalog.lakehouse.s3.access-key-id" in spark_conf
        assert "spark.sql.catalog.lakehouse.s3.secret-access-key" in spark_conf
        assert "spark.sql.catalog.lakehouse.s3.endpoint" in spark_conf


class TestCycleEnv:
    """Tests for cycle_env parameter in _build_manifest (v1.1.0)."""

    def test_cycle_env_adds_env_vars(self):
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        cycle_env = {"LB_SILVER_INCREMENTAL": "true", "LB_GOLD_INCREMENTAL": "true"}
        manifest = mgr._build_manifest(JobType.SILVER_BUILD, cycle_env=cycle_env)
        env = manifest["spec"]["driver"]["env"]
        env_dict = {e["name"]: e.get("value") for e in env}

        assert env_dict.get("LB_SILVER_INCREMENTAL") == "true"
        assert env_dict.get("LB_GOLD_INCREMENTAL") == "true"

    def test_no_cycle_env_no_extra_vars(self):
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        env = manifest["spec"]["driver"]["env"]
        env_names = [e["name"] for e in env]

        assert "LB_SILVER_INCREMENTAL" not in env_names
        assert "LB_GOLD_INCREMENTAL" not in env_names


# ---------------------------------------------------------------------------
# ConfigMap includes Delta scripts (v1.2)
# ---------------------------------------------------------------------------


class TestScriptsConfigMapDeltaScripts:
    """Verify deploy_scripts_configmap includes Delta script files."""

    def test_script_files_list_includes_delta_variants(self):
        """The script_files list in deploy_scripts_configmap should include Delta scripts."""
        config = _make_config()
        k8s = _mock_k8s()
        mgr = SparkJobManager(config, k8s)

        # Inspect the method source to verify the list, or call and check.
        # We mock k8s.apply_manifest to capture the ConfigMap data.
        k8s.apply_manifest.return_value = True

        with patch("lakebench._resources.get_scripts_dir") as mock_dir:
            import tempfile
            from pathlib import Path

            with tempfile.TemporaryDirectory() as tmpdir:
                tmp = Path(tmpdir)
                # Create all expected script files
                expected_delta_scripts = [
                    "silver_build_delta.py",
                    "gold_finalize_delta.py",
                    "gold_refresh_delta.py",
                    "bronze_ingest_delta.py",
                    "silver_stream_delta.py",
                ]
                expected_iceberg_scripts = [
                    "common.py",
                    "bronze_verify.py",
                    "silver_build.py",
                    "gold_finalize.py",
                    "bronze_ingest.py",
                    "silver_stream.py",
                    "gold_refresh.py",
                ]
                all_scripts = expected_iceberg_scripts + expected_delta_scripts
                for script in all_scripts:
                    (tmp / script).write_text(f"# {script}\nprint('hello')\n")
                mock_dir.return_value = tmp

                result = mgr.deploy_scripts_configmap()
                assert result is True

                # Verify the ConfigMap data includes Delta scripts
                call_args = k8s.apply_manifest.call_args[0][0]
                configmap_data = call_args["data"]
                for delta_script in expected_delta_scripts:
                    assert delta_script in configmap_data, (
                        f"Delta script {delta_script} missing from ConfigMap"
                    )


class TestFinancialScriptDispatch:
    """schema=financial routes JobType -> *_financial.py scripts (ENG-2C.3c+)."""

    def _make_financial_config(self):
        cfg = _make_config()
        # Rebuild via schema to pick up the FINANCIAL enum path cleanly.
        from lakebench.config import LakebenchConfig

        blob = cfg.model_dump(by_alias=True)
        blob["architecture"]["workload"]["schema"] = "financial"
        return LakebenchConfig(**blob)

    def test_bronze_verify_dispatches_to_financial_script(self):
        from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

        cfg = self._make_financial_config()
        mgr = SparkJobManager(cfg, _mock_k8s())
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        assert "bronze_verify_financial.py" in manifest["spec"]["mainApplicationFile"]

    def test_customer360_bronze_verify_unchanged(self):
        from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

        cfg = _make_config()  # default schema is customer360
        mgr = SparkJobManager(cfg, _mock_k8s())
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        assert manifest["spec"]["mainApplicationFile"].endswith("bronze_verify.py")
        assert "financial" not in manifest["spec"]["mainApplicationFile"]

    def test_reference_score_dispatches_to_reference_script(self):
        """SCORE_FINANCIAL_REFERENCE routes to score_financial_reference.py so
        the leakage gate + reference detector (the LB-130 gate) is runnable."""
        from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

        cfg = self._make_financial_config()
        mgr = SparkJobManager(cfg, _mock_k8s())
        manifest = mgr._build_manifest(JobType.SCORE_FINANCIAL_REFERENCE)
        assert "score_financial_reference.py" in manifest["spec"]["mainApplicationFile"]
        assert manifest["metadata"]["name"] == "lakebench-score-financial-reference"


class TestReferenceScoreWiring:
    """The reference detector (LB-130 gate) must be packaged and importable on a
    Spark driver that has no lakebench install."""

    def test_configmap_includes_reference_script_and_module(self):
        """deploy_scripts_configmap must ship BOTH score_financial_reference.py
        and reference_score.py (the self-contained module it imports) flat, so
        the bare `from reference_score import` resolves on the driver."""
        config = _make_config()
        k8s = _mock_k8s()
        k8s.apply_manifest.return_value = True
        mgr = SparkJobManager(config, k8s)

        result = mgr.deploy_scripts_configmap()
        assert result is True
        data = k8s.apply_manifest.call_args[0][0]["data"]
        assert "score_financial_reference.py" in data, "reference Spark entry point not packaged"
        assert "reference_score.py" in data, (
            "reference_score.py module not packaged -- the driver has no lakebench "
            "install, so the bare import would fail at runtime"
        )
        # The packaged module must be the real thing, not an empty stub.
        assert "def compute_leakage_gate" in data["reference_score.py"]
        assert "def train_reference_gbt" in data["reference_score.py"]

    def test_reference_spark_script_uses_bare_import(self):
        """score_financial_reference.py must import the module by its flat name,
        never `from lakebench.faml...` -- the lakebench package is not on the
        apache/spark driver image."""
        from lakebench._resources import get_scripts_dir

        src = (get_scripts_dir() / "score_financial_reference.py").read_text()
        assert "from lakebench.faml" not in src, (
            "reference script imports from lakebench.faml, which is absent on the driver"
        )
        assert "from reference_score import" in src, (
            "reference script no longer imports the flat-packaged reference_score module"
        )

    def test_reference_score_module_is_self_contained(self):
        """reference_score.py must not import from lakebench (it ships flat with
        no package around it)."""
        from lakebench._resources import _package_dir

        src = (_package_dir() / "faml" / "reference_score.py").read_text()
        assert "from lakebench" not in src and "import lakebench" not in src, (
            "reference_score.py imports lakebench; it cannot ship as a flat driver module"
        )

    def test_reference_script_uses_real_silver_column(self):
        """The reference feature build must read silver's real timestamp column
        (txn_timestamp), not the txn_ts that never existed in the DDL -- a
        column-not-found at runtime is exactly the never-run-script bug class."""
        from lakebench._resources import get_scripts_dir

        src = (get_scripts_dir() / "score_financial_reference.py").read_text()
        import re

        assert not re.search(r'"txn_ts"|\btxn_ts\b', src), (
            "reference script still references the nonexistent txn_ts column"
        )
        assert "txn_timestamp" in src, "reference script no longer reads txn_timestamp"


class TestSchemaProfileOverrides:
    """LB-118: FAML bronze-verify needs a bigger scratch PVC than c360's
    50Gi baseline because the CTAS fallback path rewrites the full pacs.008
    dataset through Iceberg and its per-executor spill overwhelms 50Gi at
    scale >= 5. Live at scale 10 this hit ``No space left on device`` after
    78 min."""

    def _find_pvc_size_limit(self, manifest):
        conf = manifest["spec"]["sparkConf"]
        key = (
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-local-dir-1.options.sizeLimit"
        )
        return conf.get(key)

    def _make_config(self, schema):
        from lakebench.config import LakebenchConfig

        cfg = _make_config()
        blob = cfg.model_dump(by_alias=True)
        blob["architecture"]["workload"]["schema"] = schema
        # Portworx scratch must be enabled for sizeLimit to appear in the manifest.
        blob["platform"]["storage"]["scratch"]["enabled"] = True
        blob["platform"]["storage"]["scratch"]["storage_class"] = "px-csi-scratch"
        return LakebenchConfig(**blob)

    def test_faml_bronze_verify_gets_500gi_scratch(self):
        from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

        cfg = self._make_config("financial")
        mgr = SparkJobManager(cfg, _mock_k8s())
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        assert self._find_pvc_size_limit(manifest) == "500Gi"

    def test_c360_bronze_verify_stays_50gi_scratch(self):
        from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

        cfg = self._make_config("customer360")
        mgr = SparkJobManager(cfg, _mock_k8s())
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        assert self._find_pvc_size_limit(manifest) == "50Gi"

    def test_faml_silver_build_scratch_unchanged(self):
        """FAML overrides scoped to bronze-verify only; silver-build stays at c360."""
        from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

        cfg = self._make_config("financial")
        mgr = SparkJobManager(cfg, _mock_k8s())
        manifest = mgr._build_manifest(JobType.SILVER_BUILD)
        assert self._find_pvc_size_limit(manifest) == "300Gi"

    def test_resolve_job_profile_returns_copy(self):
        """Callers must be able to mutate the resolved profile without
        corrupting module-level state (regression guard: _JOB_PROFILES.get()
        returns a reference)."""
        from lakebench.modules.pipeline_engines.spark.job import (
            _JOB_PROFILES,
            _resolve_job_profile,
        )

        base_before = _JOB_PROFILES["bronze-verify"]["scratch_size"]
        merged = _resolve_job_profile("bronze-verify", "financial")
        assert merged is not None
        merged["scratch_size"] = "999Gi"
        assert _JOB_PROFILES["bronze-verify"]["scratch_size"] == base_before

    def test_score_financial_has_dedicated_small_profile(self):
        """LB-123 review: score-financial must NOT inherit the silver-build
        fallback (36 cores / 512 GB at scale 1) just to score recall. It has
        its own small profile."""
        from lakebench.modules.pipeline_engines.spark.job import (
            _JOB_PROFILES,
            _resolve_job_profile,
        )

        prof = _resolve_job_profile("score-financial", "financial")
        assert prof is not None
        silver = _JOB_PROFILES["silver-build"]
        # Must be genuinely smaller than the silver-build fallback it replaces.
        assert prof["executor_memory"] != silver["executor_memory"]
        assert prof["scratch_size"] == "50Gi"
        assert prof["max_executors"] <= 10

    def test_compute_peak_requirements_faml_bumps_bronze_scratch(self):
        """compute_peak_requirements is the docs source of truth; FAML
        peaks must reflect the bronze-verify override."""
        from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

        c360 = compute_peak_requirements(1, "batch", "customer360")
        faml = compute_peak_requirements(1, "batch", "financial")
        c360_bronze = next(r for r in c360.per_job if r.job_type == "bronze-verify")
        faml_bronze = next(r for r in faml.per_job if r.job_type == "bronze-verify")
        assert faml_bronze.scratch_gb == 10 * c360_bronze.scratch_gb  # 500 / 50

    def test_faml_bronze_verify_scales_executors_at_scale_100(self):
        """LB-118 review finding: at scale 100 the base bronze-verify
        profile gives 7 executors (~143 GB input/executor for FAML),
        which projects to CTAS spill above 200 Gi. The FAML override
        bumps ``executors_per_100_scale`` 4 -> 8 and ``max_executors``
        20 -> 28 so per-executor load at scale 100 stays under 100 GB."""
        from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

        c360 = compute_peak_requirements(100, "batch", "customer360")
        faml = compute_peak_requirements(100, "batch", "financial")
        c360_bronze = next(r for r in c360.per_job if r.job_type == "bronze-verify")
        faml_bronze = next(r for r in faml.per_job if r.job_type == "bronze-verify")
        # FAML must have more executors than c360 at s100+.
        assert faml_bronze.executors > c360_bronze.executors
        # And scale toward the fabric8 ceiling by scale 500.
        faml_500 = compute_peak_requirements(500, "batch", "financial")
        faml_500_bronze = next(r for r in faml_500.per_job if r.job_type == "bronze-verify")
        assert faml_500_bronze.executors == 28  # matches silver/gold ceiling

    def test_compute_peak_requirements_defaults_to_c360(self):
        """Backward compat: no schema arg == c360 baseline."""
        from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

        default = compute_peak_requirements(1, "batch")
        c360 = compute_peak_requirements(1, "batch", "customer360")
        assert default.scratch_gb == c360.scratch_gb

    def test_get_job_profile_is_schema_aware(self):
        """LB-135 review Finding 2: the metrics/scorecard path must be able to
        get schema-resolved profiles, else FAML bronze-verify is reported at the
        c360 base (6Gi) instead of the deployed 20Gi -- an honest-scorecard bug."""
        from lakebench.modules.pipeline_engines.spark.job import (
            get_executor_count,
            get_job_profile,
        )

        # No schema == c360 base (backward compat).
        base = get_job_profile("bronze-verify")
        assert base["executor_memory"] == "4g"
        # Schema-aware == deployed FAML profile.
        faml = get_job_profile("bronze-verify", "financial")
        assert faml["executor_memory"] == "8g"
        assert faml["executor_memory_overhead"] == "12g"
        # Executor count also schema-aware at scale > 10 (FAML 8-per-100 vs base 4).
        assert get_executor_count("bronze-verify", 100, "financial") > get_executor_count(
            "bronze-verify", 100
        )

    def test_faml_bronze_verify_has_memory_headroom_over_c360(self):
        """LB-135: c360's 4g+2g bronze-verify (a thin add_files register) is too
        small for FAML's full-corpus CTAS DISTINCT/ORDER BY -- executors
        OOMKilled on the 6Gi container limit at scale 10. The FAML override must
        give real per-executor memory headroom, in both modes (OOMKilled is a
        container-limit hit, not node contention)."""
        from lakebench.modules.pipeline_engines.spark.job import (
            _JOB_PROFILES,
            _resolve_job_profile,
        )

        base = _JOB_PROFILES["bronze-verify"]
        faml = _resolve_job_profile("bronze-verify", "financial")
        assert faml is not None
        # The OOM is OFF-HEAP (partitioned Iceberg write shuffle + S3A bytebuffer
        # uploads), so the bump goes into OVERHEAD, not heap. Total 20Gi.
        assert faml["executor_memory"] == "8g"
        assert faml["executor_memory_overhead"] == "12g"
        heap = int(faml["executor_memory"].rstrip("g"))
        overhead = int(faml["executor_memory_overhead"].rstrip("g"))
        assert heap + overhead == 20  # total container
        # Overhead must exceed heap -- the pressure is off-heap, not heap. A
        # regression that pours the bump back into heap (the original mistake)
        # would flip this.
        assert overhead > heap, "bronze-verify memory bump must favour overhead, not heap"
        assert overhead > int(base["executor_memory_overhead"].rstrip("g"))
        # Still bounded well under the heaviest batch job (silver-build 48g heap).
        assert heap + overhead < int(
            _JOB_PROFILES["silver-build"]["executor_memory"].rstrip("g")
        ) + int(_JOB_PROFILES["silver-build"]["executor_memory_overhead"].rstrip("g"))
        # c360 bronze-verify must stay register-sized (no FAML cost leak).
        c360 = _resolve_job_profile("bronze-verify", "customer360")
        assert c360["executor_memory"] == base["executor_memory"]
        assert c360["executor_memory_overhead"] == base["executor_memory_overhead"]


# ---------------------------------------------------------------------------
# PipelineEngine protocol conformance
# ---------------------------------------------------------------------------


def test_pipeline_engine_protocol_exists():
    """PipelineEngine protocol is importable and defines required methods."""
    from lakebench.engine.protocol import PipelineEngine

    required = ["engine_name", "submit_job", "wait_for_completion", "get_logs", "cancel_job"]
    for method in required:
        assert method in dir(PipelineEngine), f"PipelineEngine missing {method}"


def test_spark_job_manager_has_core_protocol_methods():
    """SparkJobManager has the core methods needed for the PipelineEngine protocol.

    submit_job and engine_name are implemented. wait_for_completion, get_logs,
    and cancel_job are in cli.py today -- they will be migrated to
    SparkJobManager when the engine abstraction is fully wired.
    """
    from lakebench.spark.job import SparkJobManager

    # These are implemented today
    assert hasattr(SparkJobManager, "engine_name")
    assert hasattr(SparkJobManager, "submit_job")
    # These exist as get_job_status (will be renamed/wrapped)
    assert hasattr(SparkJobManager, "get_job_status")


def test_get_engine_returns_spark_job_manager():
    """get_engine() returns a SparkJobManager for the default config."""
    from unittest.mock import MagicMock

    from lakebench.config import LakebenchConfig
    from lakebench.engine import get_engine
    from lakebench.spark.job import SparkJobManager

    cfg = LakebenchConfig(
        name="test-engine",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "key",
                    "secret_key": "secret",
                }
            }
        },
    )
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    engine = get_engine(cfg, k8s)
    assert isinstance(engine, SparkJobManager)
    assert engine.engine_name() == "spark"


def test_get_engine_rejects_unknown_engine():
    """get_engine() raises ValueError for unsupported engine types."""
    from unittest.mock import MagicMock, patch

    from lakebench.config import LakebenchConfig
    from lakebench.engine import get_engine

    cfg = LakebenchConfig(
        name="test-engine",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "key",
                    "secret_key": "secret",
                }
            }
        },
    )
    k8s = MagicMock()
    # Patch the engine type to something unsupported
    with patch.object(cfg.architecture, "pipeline_engine") as mock_engine:
        mock_engine.value = "flink"
        with pytest.raises(ValueError, match="Unsupported pipeline engine"):
            get_engine(cfg, k8s)
