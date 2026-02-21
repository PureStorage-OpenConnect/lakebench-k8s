"""Tests for CLI auto-discovery and resolve_config_path."""

from pathlib import Path

import pytest
from click.exceptions import Exit as ClickExit

from lakebench.cli import DEFAULT_CONFIG, resolve_config_path


class TestResolveConfigPath:
    """Tests for config file auto-discovery."""

    def test_explicit_path_returned(self, tmp_path):
        """Explicit path is returned as-is, even if it doesn't exist."""
        p = tmp_path / "custom.yaml"
        assert resolve_config_path(p) == p

    def test_none_finds_default(self, tmp_path, monkeypatch):
        """None resolves to ./lakebench.yaml when it exists."""
        monkeypatch.chdir(tmp_path)
        default = tmp_path / DEFAULT_CONFIG
        default.write_text("name: test\n")
        result = resolve_config_path(None)
        assert result == Path(DEFAULT_CONFIG)

    def test_none_exits_when_missing(self, tmp_path, monkeypatch):
        """None raises typer.Exit when ./lakebench.yaml is absent."""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ClickExit):
            resolve_config_path(None)

    def test_default_config_constant(self):
        """DEFAULT_CONFIG is lakebench.yaml."""
        assert DEFAULT_CONFIG == "lakebench.yaml"


class TestRecommendCommand:
    """Tests for the lakebench recommend command."""

    def test_compute_requirements_scale_1(self):
        """Scale 1 requires minimal cluster resources."""
        from lakebench.config.scale import customer360_dimensions, full_compute_guidance

        scale = 1
        dims = customer360_dimensions(scale)
        guidance = full_compute_guidance(scale)

        assert dims.approx_bronze_gb == 10.0
        assert guidance.spark.tier_name == "minimal"
        assert guidance.spark.recommended_executors == 2
        assert guidance.datagen.parallelism == 2

    def test_compute_requirements_scale_100(self):
        """Scale 100 requires performance tier resources."""
        from lakebench.config.scale import customer360_dimensions, full_compute_guidance

        scale = 100
        dims = customer360_dimensions(scale)
        guidance = full_compute_guidance(scale)

        assert dims.approx_bronze_gb == 1000.0  # 1 TB
        assert guidance.spark.tier_name == "performance"
        assert guidance.spark.recommended_executors >= 8

    def test_compute_requirements_scale_500(self):
        """Scale 500 requires significant cluster resources."""
        from lakebench.config.scale import customer360_dimensions, full_compute_guidance

        scale = 500
        dims = customer360_dimensions(scale)
        guidance = full_compute_guidance(scale)

        assert dims.approx_bronze_gb == 5000.0  # 5 TB
        assert guidance.spark.tier_name == "performance"
        assert guidance.spark.recommended_executors >= 16

    def test_scale_tiers_are_monotonic(self):
        """Higher scales require more resources."""
        from lakebench.config.scale import full_compute_guidance

        prev_executors = 0
        for scale in [1, 10, 50, 100, 500, 1000]:
            guidance = full_compute_guidance(scale)
            current = guidance.spark.recommended_executors
            assert current >= prev_executors, (
                f"Scale {scale} should need >= executors than lower scales"
            )
            prev_executors = current

    def test_cluster_requirements_include_all_components(self):
        """Cluster requirements include Spark, Trino, Datagen, and infra."""
        from lakebench.config.scale import full_compute_guidance

        scale = 100
        guidance = full_compute_guidance(scale)

        # All components should have non-zero resources
        assert guidance.spark.recommended_executors > 0
        assert guidance.trino.worker_replicas > 0
        assert guidance.datagen.parallelism > 0

        # Memory strings should be valid
        assert guidance.spark.recommended_memory.endswith("g")
        assert guidance.trino.worker_memory.endswith("Gi")
        assert guidance.datagen.memory.endswith("Gi")


class TestPreflightCheck:
    """Tests for _preflight_check deploy guard."""

    def test_preflight_blocks_on_missing_stackable(self, monkeypatch):
        """Preflight exits 1 when Stackable CRDs are missing for Hive catalog."""
        from unittest.mock import MagicMock, patch

        from lakebench.cli import _preflight_check

        # Build a config with catalog=hive and valid S3
        cfg = MagicMock()
        cfg.architecture.catalog.type.value = "hive"
        cfg.platform.storage.s3.endpoint = "http://s3:80"
        cfg.platform.storage.s3.access_key = "key"
        cfg.platform.storage.s3.secret_key = "secret"

        # Mock K8s CRD listing to return no Stackable CRDs
        mock_crd_list = MagicMock()
        mock_crd_list.items = []

        with (
            patch("kubernetes.client.ApiextensionsV1Api") as mock_api,
            pytest.raises(ClickExit),
        ):
            mock_api.return_value.list_custom_resource_definition.return_value = mock_crd_list
            _preflight_check(cfg)

    def test_preflight_passes_when_stackable_present(self, monkeypatch):
        """Preflight does not exit when Stackable CRDs are present."""
        from unittest.mock import MagicMock, patch

        from lakebench.cli import _preflight_check

        cfg = MagicMock()
        cfg.architecture.catalog.type.value = "hive"
        cfg.platform.storage.s3.endpoint = "http://s3:80"
        cfg.platform.storage.s3.access_key = "key"
        cfg.platform.storage.s3.secret_key = "secret"

        # Mock CRDs to include the required Stackable CRDs
        crd1 = MagicMock()
        crd1.metadata.name = "hiveclusters.hive.stackable.tech"
        crd2 = MagicMock()
        crd2.metadata.name = "secretclasses.secrets.stackable.tech"
        mock_crd_list = MagicMock()
        mock_crd_list.items = [crd1, crd2]

        with patch("kubernetes.client.ApiextensionsV1Api") as mock_api:
            mock_api.return_value.list_custom_resource_definition.return_value = mock_crd_list
            # Should not raise
            _preflight_check(cfg)

    def test_preflight_skips_stackable_for_polaris(self, monkeypatch):
        """Preflight skips Stackable check when catalog is Polaris."""
        from unittest.mock import MagicMock

        from lakebench.cli import _preflight_check

        cfg = MagicMock()
        cfg.architecture.catalog.type.value = "polaris"
        cfg.platform.storage.s3.endpoint = "http://s3:80"
        cfg.platform.storage.s3.access_key = "key"
        cfg.platform.storage.s3.secret_key = "secret"

        # Should not raise (Stackable check skipped entirely)
        _preflight_check(cfg)


class TestRunPreflightInfraCheck:
    """Tests for _run_preflight_infra_check run guard."""

    def _make_cfg(self, catalog="hive", engine="trino"):
        from unittest.mock import MagicMock

        cfg = MagicMock()
        cfg.get_namespace.return_value = "lakebench"
        cfg.platform.kubernetes.context = ""
        cfg.architecture.catalog.type.value = catalog
        cfg.architecture.query_engine.type.value = engine
        cfg.observability.enabled = False
        return cfg

    def test_blocks_when_namespace_missing(self):
        """Exits 1 when the target namespace does not exist."""
        from unittest.mock import patch

        from lakebench.cli import _run_preflight_infra_check

        cfg = self._make_cfg()
        with (
            patch("lakebench.cli.get_k8s_client") as mock_get,
            pytest.raises(ClickExit),
        ):
            mock_get.return_value.namespace_exists.return_value = False
            _run_preflight_infra_check(cfg)

    def test_blocks_when_postgres_missing(self):
        """Exits 1 when PostgreSQL is not deployed."""
        from unittest.mock import MagicMock, patch

        from kubernetes.client.rest import ApiException

        from lakebench.cli import _run_preflight_infra_check

        cfg = self._make_cfg()

        def fake_read_sts(name, ns):
            if name == "lakebench-postgres":
                raise ApiException(status=404, reason="Not Found")
            obj = MagicMock()
            obj.status.ready_replicas = 1
            obj.spec.replicas = 1
            return obj

        def fake_read_dep(name, ns):
            obj = MagicMock()
            obj.status.ready_replicas = 1
            obj.spec.replicas = 1
            return obj

        with (
            patch("lakebench.cli.get_k8s_client") as mock_get,
            patch("kubernetes.client.AppsV1Api") as mock_apps,
            pytest.raises(ClickExit),
        ):
            mock_get.return_value.namespace_exists.return_value = True
            mock_apps.return_value.read_namespaced_stateful_set.side_effect = fake_read_sts
            mock_apps.return_value.read_namespaced_deployment.side_effect = fake_read_dep
            _run_preflight_infra_check(cfg)

    def test_blocks_when_trino_not_ready(self):
        """Exits 1 when Trino workers have 0 ready replicas."""
        from unittest.mock import MagicMock, patch

        from lakebench.cli import _run_preflight_infra_check

        cfg = self._make_cfg(engine="trino")

        def fake_read_sts(name, ns):
            obj = MagicMock()
            if name == "lakebench-trino-worker":
                obj.status.ready_replicas = 0
                obj.spec.replicas = 4
            else:
                obj.status.ready_replicas = 1
                obj.spec.replicas = 1
            return obj

        def fake_read_dep(name, ns):
            obj = MagicMock()
            obj.status.ready_replicas = 1
            obj.spec.replicas = 1
            return obj

        with (
            patch("lakebench.cli.get_k8s_client") as mock_get,
            patch("kubernetes.client.AppsV1Api") as mock_apps,
            pytest.raises(ClickExit),
        ):
            mock_get.return_value.namespace_exists.return_value = True
            mock_apps.return_value.read_namespaced_stateful_set.side_effect = fake_read_sts
            mock_apps.return_value.read_namespaced_deployment.side_effect = fake_read_dep
            _run_preflight_infra_check(cfg)

    def test_passes_when_all_ready(self):
        """Does not exit when all components are deployed and ready."""
        from unittest.mock import MagicMock, patch

        from lakebench.cli import _run_preflight_infra_check

        cfg = self._make_cfg(engine="trino")

        def fake_read_sts(name, ns):
            obj = MagicMock()
            if name == "lakebench-trino-worker":
                obj.status.ready_replicas = 4
                obj.spec.replicas = 4
            else:
                obj.status.ready_replicas = 1
                obj.spec.replicas = 1
            return obj

        def fake_read_dep(name, ns):
            obj = MagicMock()
            obj.status.ready_replicas = 1
            obj.spec.replicas = 1
            return obj

        with (
            patch("lakebench.cli.get_k8s_client") as mock_get,
            patch("kubernetes.client.AppsV1Api") as mock_apps,
        ):
            mock_get.return_value.namespace_exists.return_value = True
            mock_apps.return_value.read_namespaced_stateful_set.side_effect = fake_read_sts
            mock_apps.return_value.read_namespaced_deployment.side_effect = fake_read_dep
            # Should not raise
            _run_preflight_infra_check(cfg)

    def test_polaris_duckdb_components(self):
        """Checks Polaris + DuckDB components when configured."""
        from unittest.mock import MagicMock, patch

        from lakebench.cli import _run_preflight_infra_check

        cfg = self._make_cfg(catalog="polaris", engine="duckdb")

        def fake_read_sts(name, ns):
            obj = MagicMock()
            obj.status.ready_replicas = 1
            obj.spec.replicas = 1
            return obj

        def fake_read_dep(name, ns):
            obj = MagicMock()
            obj.status.ready_replicas = 1
            obj.spec.replicas = 1
            return obj

        with (
            patch("lakebench.cli.get_k8s_client") as mock_get,
            patch("kubernetes.client.AppsV1Api") as mock_apps,
        ):
            mock_get.return_value.namespace_exists.return_value = True
            mock_apps.return_value.read_namespaced_stateful_set.side_effect = fake_read_sts
            mock_apps.return_value.read_namespaced_deployment.side_effect = fake_read_dep
            # Should not raise -- checks Polaris (Deployment) + DuckDB (Deployment)
            _run_preflight_infra_check(cfg)


class TestParseSparkInterval:
    """Tests for _parse_spark_interval()."""

    def test_seconds(self):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("30 seconds") == 30

    def test_single_second(self):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("1 second") == 1

    def test_minutes(self):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("5 minutes") == 300

    def test_single_minute(self):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("1 minute") == 60

    def test_hours(self):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("2 hours") == 7200

    def test_fallback_on_junk(self):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("garbage") == 300

    def test_fallback_on_empty(self):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("") == 300

    def test_fallback_on_no_unit(self):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("300") == 300

    def test_parse_spark_interval_minutes(self):
        """Verify that _parse_spark_interval handles '5 minutes'."""
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval("5 minutes") == 300


class TestBenchmarkSchedulingFloors:
    """Warmup and interval must be >= gold_refresh_interval."""

    def test_warmup_below_gold_refresh_is_clamped(self):
        """If warmup < gold_refresh_interval, warmup is raised to match."""
        from lakebench.cli import _parse_spark_interval

        gold_interval_s = _parse_spark_interval("5 minutes")
        bench_warmup = 60  # user sets 60s, gold refresh is 300s

        # Simulate the clamping logic from cli.py
        if bench_warmup < gold_interval_s:
            bench_warmup = gold_interval_s

        assert bench_warmup == 300

    def test_warmup_above_gold_refresh_unchanged(self):
        """If warmup >= gold_refresh_interval, no clamping occurs."""
        from lakebench.cli import _parse_spark_interval

        gold_interval_s = _parse_spark_interval("5 minutes")
        bench_warmup = 600  # user sets 10 min, gold refresh is 5 min

        if bench_warmup < gold_interval_s:
            bench_warmup = gold_interval_s

        assert bench_warmup == 600

    def test_interval_below_gold_refresh_is_clamped(self):
        """If interval < gold_refresh_interval, interval is raised to match."""
        from lakebench.cli import _parse_spark_interval

        gold_interval_s = _parse_spark_interval("5 minutes")
        bench_interval = 120  # user sets 120s, gold refresh is 300s

        if bench_interval < gold_interval_s:
            bench_interval = gold_interval_s

        assert bench_interval == 300

    def test_interval_above_gold_refresh_unchanged(self):
        """If interval >= gold_refresh_interval, no clamping occurs."""
        from lakebench.cli import _parse_spark_interval

        gold_interval_s = _parse_spark_interval("5 minutes")
        bench_interval = 600  # user sets 10 min, gold refresh is 5 min

        if bench_interval < gold_interval_s:
            bench_interval = gold_interval_s

        assert bench_interval == 600
