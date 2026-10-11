"""Tests for Spark module (job submission, monitoring)."""

import pytest

from lakebench.modules.pipeline_engines.spark.job import (
    JobType,
    SparkJobManager,
    _resolve_job_profile,
    compute_peak_requirements,
    get_executor_count,
    get_job_profile,
)
from tests.conftest import make_config
from tests.fixtures.spark_helpers import _make_config, _mock_k8s


class TestSparkJobManager:
    """Tests for SparkJobManager manifest building."""

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
        """Concurrent streams share the cluster budget: their executor cores sum
        to at most the budget fraction of the cluster, and every stream keeps one."""
        config = _make_config(
            architecture={
                "workload": {"datagen": {"scale": 100}},
            },
        )
        total_cores = 64
        mgr = SparkJobManager(config, self._make_k8s_with_capacity(total_cores * 1000))

        manifests = [
            mgr._build_manifest(jt)["spec"]["executor"]
            for jt in (JobType.BRONZE_INGEST, JobType.SILVER_STREAM, JobType.GOLD_REFRESH)
        ]
        assert all(e["instances"] >= 1 for e in manifests)
        assert sum(e["instances"] * e["cores"] for e in manifests) <= total_cores


class TestStreamingThroughputEnvVars:
    """Tests that throughput tuning config fields are injected as env vars."""

    def _get_env_dict(self, manifest):
        """Extract driver env vars as a dict from a manifest (skip secretKeyRef entries)."""
        env_list = manifest["spec"]["driver"]["env"]
        return {e["name"]: e["value"] for e in env_list if "value" in e}

    @pytest.mark.parametrize(
        ("job", "sustained", "expected", "same_checkpoint"),
        [
            (
                JobType.BRONZE_INGEST,
                {"max_files_per_trigger": 30, "bronze_trigger_interval": "20 seconds"},
                {
                    "MAX_FILES_PER_TRIGGER": "30",
                    "TRIGGER_INTERVAL": "20 seconds",
                    "LB_FINANCIAL_BRONZE_MAX_FILES": "30",
                    "LB_FINANCIAL_BRONZE_TRIGGER_S": "20",
                },
                "LB_FINANCIAL_BRONZE_CHECKPOINT",
            ),
            (
                JobType.SILVER_STREAM,
                {"silver_trigger_interval": "45 seconds"},
                {"LB_FINANCIAL_SILVER_TRIGGER_S": "45"},
                "LB_FINANCIAL_SILVER_CHECKPOINT",
            ),
            (
                JobType.GOLD_REFRESH,
                {"gold_refresh_interval": "2 minutes"},
                {"LB_FINANCIAL_GOLD_REFRESH_S": "120"},
                None,
            ),
        ],
    )
    def test_financial_env_aliases_carry_the_deployments_values(
        self, job, sustained, expected, same_checkpoint
    ):
        """The financial scripts read LB_FINANCIAL_*; without the aliases they
        fall back to a hard-coded checkpoint under s3a://lb-bronze/ and two
        parallel deployments share checkpoint state (silent corruption)."""
        config = _make_config(
            architecture={
                "workload": {"schema": "financial", "datagen": {"scale": 1}},
                "processing": {"sustained": sustained},
            },
        )
        env = self._get_env_dict(SparkJobManager(config, _mock_k8s())._build_manifest(job))
        for k, v in expected.items():
            assert env.get(k) == v, k
        if same_checkpoint:
            assert "CHECKPOINT_LOCATION" in env
            assert env.get(same_checkpoint) == env["CHECKPOINT_LOCATION"]


@pytest.mark.parametrize(
    ("job", "script"),
    [
        (JobType.BRONZE_VERIFY, "bronze_verify_financial.py"),
        (JobType.SCORE_FINANCIAL_REFERENCE, "score_financial_reference.py"),
    ],
)
def test_financial_schema_dispatches_job_to_financial_script(job, script):
    cfg = make_config(architecture={"workload": {"schema": "financial"}})
    manifest = SparkJobManager(cfg, _mock_k8s())._build_manifest(job)
    assert manifest["spec"]["mainApplicationFile"].endswith(script)


class TestSchemaProfileOverrides:
    """The scorecard path resolves the same profile and executor count that
    deploy puts in the manifest, per schema."""

    @pytest.mark.parametrize("schema", ["customer360", "financial"])
    @pytest.mark.parametrize("job", [JobType.BRONZE_VERIFY, JobType.SILVER_BUILD])
    @pytest.mark.parametrize("scale", [10, 100])
    def test_get_job_profile_is_schema_aware(self, job, schema, scale):
        cfg = make_config(
            architecture={"workload": {"schema": schema, "datagen": {"scale": scale}}}
        )
        manifest = SparkJobManager(cfg, _mock_k8s())._build_manifest(job)
        executor = manifest["spec"]["executor"]
        profile = get_job_profile(job.value, schema)

        assert profile == _resolve_job_profile(job.value, schema)
        assert get_executor_count(job.value, scale, schema) == executor["instances"]
        assert profile["executor_memory"] == executor["memory"]

    def test_aml_bronze_verify_gets_more_memory_than_c360(self):
        def total_mem(profile):
            return sum(
                int(profile[k].rstrip("g")) for k in ("executor_memory", "executor_memory_overhead")
            )

        aml = get_job_profile("bronze-verify", "financial")
        c360 = get_job_profile("bronze-verify", "customer360")
        assert total_mem(aml) > total_mem(c360)
        assert c360 == get_job_profile("bronze-verify")

    def test_aml_bronze_verify_scales_executors_at_scale_100(self):
        """AML bronze-verify gets more executors than c360 at scale 100 and stays
        within the executor cap at scale 500."""
        c360 = compute_peak_requirements(100, "batch", "customer360")
        aml = compute_peak_requirements(100, "batch", "financial")
        c360_bronze = next(r for r in c360.per_job if r.job_type == "bronze-verify")
        aml_bronze = next(r for r in aml.per_job if r.job_type == "bronze-verify")
        assert aml_bronze.executors > c360_bronze.executors

        aml_500 = compute_peak_requirements(500, "batch", "financial")
        aml_500_bronze = next(r for r in aml_500.per_job if r.job_type == "bronze-verify")
        assert aml_500_bronze.executors <= 28


class TestReferencePyDeps:
    """D9: the reference-detector job installs pinned scikit-learn/pandas into a
    driver-only emptyDir; no other job pays for it."""

    def _mgr(self):
        config = _make_config(
            architecture={"workload": {"schema": "financial", "datagen": {"scale": 1}}}
        )
        return SparkJobManager(config, _mock_k8s())

    def test_reference_job_installs_pinned_deps_on_driver_only(self):
        from lakebench.deps import manifest as dm
        from lakebench.modules.pipeline_engines.spark.job import REFERENCE_PY_DEPS_DIR

        mgr = self._mgr()
        m = mgr._build_manifest(JobType.SCORE_FINANCIAL_REFERENCE)
        drv = m["spec"]["driver"]["template"]["spec"]
        init = {c["name"]: c for c in drv["initContainers"]}
        assert "install-pydeps" not in init
        cmd = init["lb-deps-py-reference"]["command"][-1]
        # From the deployment's set only, hash-checked against the manifest.
        for flag in ("--no-index", "--require-hashes"):
            assert flag in cmd
        mounts = drv["containers"][0]["volumeMounts"]
        assert any(v["mountPath"] == REFERENCE_PY_DEPS_DIR for v in mounts)
        vols = {v["name"]: v for v in drv["volumes"]}
        assert vols["lb-deps-manifest"]["configMap"]["name"] == dm.MANIFEST_CONFIGMAP
        exe = m["spec"]["executor"]["template"]["spec"]
        assert not any(v["name"] in ("lb-pydeps", "lb-deps-manifest") for v in exe["volumes"])
        assert not any(
            v["mountPath"] == REFERENCE_PY_DEPS_DIR for v in exe["containers"][0]["volumeMounts"]
        )
