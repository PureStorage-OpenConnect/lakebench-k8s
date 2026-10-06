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

class TestJobState:
    """Tests for JobState enum values."""

# ---------------------------------------------------------------------------
# JobStatus dataclass
# ---------------------------------------------------------------------------


class TestJobStatus:
    """Tests for JobStatus dataclass."""

# ---------------------------------------------------------------------------
# SparkJobManager - manifest building
# ---------------------------------------------------------------------------


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

# ---------------------------------------------------------------------------
# JobResult dataclass
# ---------------------------------------------------------------------------


class TestPerJobExecutorOverridesInManifest:
    """Tests for per-job executor count overrides in SparkJobManager manifests."""

class TestDriverResourceOverrides:
    """Tests for driver memory/cores overrides in SparkJobManager manifests."""

class TestMaxResultSizeScaling:
    """Tests for dynamic spark.driver.maxResultSize based on executor count."""

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

# ---------------------------------------------------------------------------
# SparkJobMonitor - basic init
# ---------------------------------------------------------------------------


class TestSparkJobMonitor:
    """Tests for SparkJobMonitor initialisation."""

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

# ---------------------------------------------------------------------------
# Phase 1: Monitor returns driver_logs on success
# ---------------------------------------------------------------------------


class TestMonitorDriverLogsOnSuccess:
    """Tests that wait_for_completion returns driver_logs for successful jobs."""

# ---------------------------------------------------------------------------
# Streaming env vars: throughput tuning fields
# ---------------------------------------------------------------------------


class TestStreamingThroughputEnvVars:
    """Tests that throughput tuning config fields are injected as env vars."""

    def _get_env_dict(self, manifest):
        """Extract driver env vars as a dict from a manifest (skip secretKeyRef entries)."""
        env_list = manifest["spec"]["driver"]["env"]
        return {e["name"]: e["value"] for e in env_list if "value" in e}

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
    def test_ensure_namespace_watched_provides_fix_command(self, mock_run):
        """When can_heal=False, the remedy routes through lakebench deploy.

        A raw ``helm upgrade --reuse-values`` bypasses the cluster lease
        and can overwrite other deployments' watch entries, so it must
        never be suggested."""
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
        assert "helm upgrade" not in status.message
        assert "--reuse-values" not in status.message
        assert "lakebench deploy" in status.message
        assert "lakebench-test" in status.message

    @patch(
        "lakebench.spark.operator.SparkOperatorManager._namespace_is_terminating",
        return_value=False,
    )
    @patch(
        "lakebench.spark.operator.SparkOperatorManager._filter_existing_namespaces",
        side_effect=lambda ns: ns,
    )
    @patch("lakebench.modules.pipeline_engines.spark.operator.subprocess.run")
    def test_ensure_namespace_watched_self_heals(self, mock_run, _mock_filter, _mock_live):
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
            # _watch_list_pin: the installed chart, read again inside the lease
            MagicMock(
                returncode=0,
                stdout='[{"name":"spark-operator","chart":"spark-operator-2.4.0"}]',
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

class TestCycleEnv:
    """Tests for cycle_env parameter in _build_manifest (v1.1.0)."""

# ---------------------------------------------------------------------------
# ConfigMap includes Delta scripts (v1.2)
# ---------------------------------------------------------------------------


class TestScriptsConfigMapDeltaScripts:
    """The scripts ConfigMaps ship the Delta script files (v1.2)."""

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

    def test_reference_script_uses_real_silver_column(self):
        """The reference feature build must read silver's real timestamp column
        (txn_timestamp), not the txn_ts that never existed in the DDL -- a
        column-not-found at runtime is exactly the never-run-script bug class."""
        from lakebench._resources import get_scripts_dir

        src = (get_scripts_dir() / "score_financial_reference.py").read_text()
        src += (get_scripts_dir() / "aml_features.py").read_text()
        import re

        assert not re.search(r'"txn_ts"|\btxn_ts\b', src), (
            "reference script still references the nonexistent txn_ts column"
        )
        assert "txn_timestamp" in src, "reference script no longer reads txn_timestamp"


class TestSchemaProfileOverrides:
    """LB-118: AML bronze-verify needs a bigger scratch PVC than c360's
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

    def test_aml_bronze_verify_gets_500gi_scratch(self):
        from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

        cfg = self._make_config("financial")
        mgr = SparkJobManager(cfg, _mock_k8s())
        manifest = mgr._build_manifest(JobType.BRONZE_VERIFY)
        assert self._find_pvc_size_limit(manifest) == "500Gi"

    def test_compute_peak_requirements_aml_bumps_bronze_scratch(self):
        """compute_peak_requirements is the docs source of truth; AML
        peaks must reflect the bronze-verify override."""
        from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

        c360 = compute_peak_requirements(1, "batch", "customer360")
        aml = compute_peak_requirements(1, "batch", "financial")
        c360_bronze = next(r for r in c360.per_job if r.job_type == "bronze-verify")
        aml_bronze = next(r for r in aml.per_job if r.job_type == "bronze-verify")
        assert aml_bronze.scratch_gb == 10 * c360_bronze.scratch_gb  # 500 / 50

    def test_aml_bronze_verify_scales_executors_at_scale_100(self):
        """LB-118 review finding: at scale 100 the base bronze-verify
        profile gives 7 executors (~143 GB input/executor for AML),
        which projects to CTAS spill above 200 Gi. The AML override
        bumps ``executors_per_100_scale`` 4 -> 8 and ``max_executors``
        20 -> 28 so per-executor load at scale 100 stays under 100 GB."""
        from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

        c360 = compute_peak_requirements(100, "batch", "customer360")
        aml = compute_peak_requirements(100, "batch", "financial")
        c360_bronze = next(r for r in c360.per_job if r.job_type == "bronze-verify")
        aml_bronze = next(r for r in aml.per_job if r.job_type == "bronze-verify")
        # AML must have more executors than c360 at s100+.
        assert aml_bronze.executors > c360_bronze.executors
        # And scale toward the fabric8 ceiling by scale 500.
        aml_500 = compute_peak_requirements(500, "batch", "financial")
        aml_500_bronze = next(r for r in aml_500.per_job if r.job_type == "bronze-verify")
        assert aml_500_bronze.executors == 28  # matches silver/gold ceiling

    def test_compute_peak_requirements_defaults_to_c360(self):
        """Backward compat: no schema arg == c360 baseline."""
        from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

        default = compute_peak_requirements(1, "batch")
        c360 = compute_peak_requirements(1, "batch", "customer360")
        assert default.scratch_gb == c360.scratch_gb

    def test_get_job_profile_is_schema_aware(self):
        """LB-135 review Finding 2: the metrics/scorecard path must be able to
        get schema-resolved profiles, else AML bronze-verify is reported at the
        c360 base (6Gi) instead of the deployed 20Gi -- an honest-scorecard bug."""
        from lakebench.modules.pipeline_engines.spark.job import (
            get_executor_count,
            get_job_profile,
        )

        # No schema == c360 base (backward compat).
        base = get_job_profile("bronze-verify")
        assert base["executor_memory"] == "4g"
        # Schema-aware == deployed AML profile.
        aml = get_job_profile("bronze-verify", "financial")
        assert aml["executor_memory"] == "8g"
        assert aml["executor_memory_overhead"] == "12g"
        # Executor count also schema-aware at scale > 10 (AML 8-per-100 vs base 4).
        assert get_executor_count("bronze-verify", 100, "financial") > get_executor_count(
            "bronze-verify", 100
        )

    def test_aml_bronze_verify_has_memory_headroom_over_c360(self):
        """LB-135: c360's 4g+2g bronze-verify (a thin add_files register) is too
        small for AML's full-corpus CTAS DISTINCT/ORDER BY -- executors
        OOMKilled on the 6Gi container limit at scale 10. The AML override must
        give real per-executor memory headroom, in both modes (OOMKilled is a
        container-limit hit, not node contention)."""
        from lakebench.modules.pipeline_engines.spark.job import (
            _JOB_PROFILES,
            _resolve_job_profile,
        )

        base = _JOB_PROFILES["bronze-verify"]
        aml = _resolve_job_profile("bronze-verify", "financial")
        assert aml is not None
        # The OOM is OFF-HEAP (partitioned Iceberg write shuffle + S3A bytebuffer
        # uploads), so the bump goes into OVERHEAD, not heap. Total 20Gi.
        assert aml["executor_memory"] == "8g"
        assert aml["executor_memory_overhead"] == "12g"
        heap = int(aml["executor_memory"].rstrip("g"))
        overhead = int(aml["executor_memory_overhead"].rstrip("g"))
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
        # c360 bronze-verify must stay register-sized (no AML cost leak).
        c360 = _resolve_job_profile("bronze-verify", "customer360")
        assert c360["executor_memory"] == base["executor_memory"]
        assert c360["executor_memory_overhead"] == base["executor_memory_overhead"]


# ---------------------------------------------------------------------------
# PipelineEngine protocol conformance
# ---------------------------------------------------------------------------


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
        for flag in ("--no-index", "--require-hashes", "--only-binary=:all:", "--no-deps"):
            assert flag in cmd
        assert f"--find-links {mgr.deps.base_url}/py-reference/" in cmd
        assert f"-r {dm.MANIFEST_MOUNT}/requirements-py-reference.txt" in cmd
        assert f"--target {REFERENCE_PY_DEPS_DIR}" in cmd
        assert "pypi.org" not in cmd
        mounts = drv["containers"][0]["volumeMounts"]
        assert any(v["mountPath"] == REFERENCE_PY_DEPS_DIR for v in mounts)
        vols = {v["name"]: v for v in drv["volumes"]}
        assert vols["lb-deps-manifest"]["configMap"]["name"] == dm.MANIFEST_CONFIGMAP
        exe = m["spec"]["executor"]["template"]["spec"]
        assert not any(v["name"] in ("lb-pydeps", "lb-deps-manifest") for v in exe["volumes"])
        assert not any(
            v["mountPath"] == REFERENCE_PY_DEPS_DIR for v in exe["containers"][0]["volumeMounts"]
        )

class TestSparkDriverPushgatewayEnv:
    """Gate 2: the Spark driver gets LB_PUSHGATEWAY_URL + LB_RUN_ID only when
    observability + the pushgateway are enabled (drives common.py stage push)."""

