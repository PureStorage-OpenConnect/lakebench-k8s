"""Tests for CLI auto-discovery and resolve_config_path."""

import pytest
import typer


class TestPreflightCheck:
    """Tests for _preflight_check deploy guard."""

    @pytest.fixture(autouse=True)
    def _capacity_fits(self, monkeypatch):
        # These tests are about the Stackable check; the capacity check has
        # its own tests (tests/test_deploy_capacity.py).
        from lakebench.cli._prerequisites import PrereqResult

        monkeypatch.setattr(
            "lakebench.cli._prerequisites.deploy_capacity_check",
            lambda cfg: PrereqResult(name="cluster-capacity", passed=True, message="OK"),
        )

    def test_preflight_blocks_on_missing_stackable(self, monkeypatch):
        """Preflight exits 1 when Stackable CRDs are missing and install is false."""
        from unittest.mock import MagicMock, patch

        from lakebench.cli import _preflight_check

        # Build a config with catalog=hive, install=false, and valid S3
        cfg = MagicMock()
        cfg.architecture.catalog.type.value = "hive"
        cfg.architecture.catalog.hive.operator.install = False
        cfg.architecture.catalog.hive.operator.version = "25.7.0"
        cfg.architecture.catalog.hive.operator.namespace = "stackable"
        cfg.platform.storage.s3.endpoint = "http://s3:80"
        cfg.platform.storage.s3.access_key = "key"
        cfg.platform.storage.s3.secret_key = "secret"

        # Mock K8s CRD listing to return no Stackable CRDs
        mock_crd_list = MagicMock()
        mock_crd_list.items = []

        with (
            patch("kubernetes.client.ApiextensionsV1Api") as mock_api,
            pytest.raises(typer.Exit),
        ):
            mock_api.return_value.list_custom_resource_definition.return_value = mock_crd_list
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
            pytest.raises(typer.Exit),
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
            pytest.raises(typer.Exit),
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
            pytest.raises(typer.Exit),
        ):
            mock_get.return_value.namespace_exists.return_value = True
            mock_apps.return_value.read_namespaced_stateful_set.side_effect = fake_read_sts
            mock_apps.return_value.read_namespaced_deployment.side_effect = fake_read_dep
            _run_preflight_infra_check(cfg)


class TestParseSparkInterval:
    """Tests for _parse_spark_interval()."""

    @pytest.mark.parametrize(
        ("text", "seconds"),
        [
            ("30 seconds", 30),
            ("1 second", 1),
            ("5 minutes", 300),
            ("1 minute", 60),
            ("2 hours", 7200),
            ("garbage", 300),
            ("", 300),
            ("300", 300),
        ],
    )
    def test_parse(self, text, seconds):
        from lakebench.cli import _parse_spark_interval

        assert _parse_spark_interval(text) == seconds


class TestResolveMaintenanceRetention:
    """Tests for the schema-aware retention resolver (ENG-R-05)."""

    def _cfg(self, retention_workload=False, retention_months=60):
        from unittest.mock import MagicMock

        cfg = MagicMock()
        cfg.architecture.workload.retention_workload = retention_workload
        cfg.architecture.workload.retention_months = retention_months
        return cfg

    @pytest.mark.parametrize(
        ("kw", "threshold"),
        [
            ({}, "0s"),
            # retention workloads keep the window plus 6 months headroom at 30.5 days
            ({"retention_workload": True, "retention_months": 60}, "2013d"),
            ({"retention_workload": True, "retention_months": 12}, "549d"),
        ],
    )
    def test_threshold(self, kw, threshold):
        from lakebench.cli._sustained import resolve_maintenance_retention
        from lakebench.modules.table_formats.iceberg.maintenance import _parse_threshold_seconds

        got = resolve_maintenance_retention(self._cfg(**kw))
        assert got == threshold
        # the day-string round-trips through the maintenance parser
        if threshold.endswith("d"):
            assert _parse_threshold_seconds(got) == int(threshold[:-1]) * 86400


class TestRunIcebergMaintenance:
    """Tests for _run_iceberg_maintenance() engine-aware helper."""

    def _make_cfg(self, engine_type="trino"):
        from unittest.mock import MagicMock

        cfg = MagicMock()
        cfg.get_namespace.return_value = "lakebench-sustained"
        cfg.architecture.query_engine.type.value = engine_type
        cfg.architecture.query_engine.trino.catalog_name = "lakehouse"
        cfg.architecture.query_engine.spark_thrift.catalog_name = "lakehouse"
        from lakebench.config.schema import TableNamesConfig

        cfg.architecture.tables = TableNamesConfig()
        cfg.architecture.workload.schema_type.value = "customer360"
        return cfg

    def test_runs_maintenance_on_all_tables_trino(self):
        """Runs expire_snapshots + remove_orphan_files for each table via Trino."""
        from unittest.mock import MagicMock, patch

        from rich.console import Console

        from lakebench.cli import _run_iceberg_maintenance

        cfg = self._make_cfg("trino")
        k8s = MagicMock()
        console = Console(quiet=True)
        j = MagicMock()

        mock_pod = MagicMock()
        mock_pod.metadata.name = "trino-coordinator-0"
        mock_pod_list = MagicMock()
        mock_pod_list.items = [mock_pod]

        with patch("kubernetes.client") as mock_core:
            mock_core.CoreV1Api.return_value.list_namespaced_pod.return_value = mock_pod_list
            _run_iceberg_maintenance(cfg, k8s, console, j, "30m")

        # Batch c360: silver + gold (no bronze table exists) x 2 operations.
        assert k8s.exec_in_pod.call_count == 4
        assert not any("bronze" in str(c[0][1]) for c in k8s.exec_in_pod.call_args_list)
        # Expire at the threshold; orphan removal never below 24 h + 10 min.
        for call in k8s.exec_in_pod.call_args_list:
            cmd = call[0][1]
            assert cmd[0] == "trino"
            if "expire_snapshots" in cmd[2]:
                assert "retention_threshold => '30m'" in cmd[2]
            else:
                assert "retention_threshold => '1450m'" in cmd[2]
