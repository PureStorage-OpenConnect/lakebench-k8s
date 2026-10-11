"""CLI preflight guards, Spark interval parsing, maintenance retention and the
engine-aware Iceberg maintenance helper."""

import pytest
import typer


class TestPreflightCheck:
    """_preflight_check refuses a deploy whose Stackable operators are absent."""

    @pytest.fixture(autouse=True)
    def _capacity_fits(self, monkeypatch):
        # These tests are about the Stackable check; the capacity check has
        # its own tests (tests/test_deploy_capacity.py).
        from lakebench.cli._prerequisites import PrereqResult

        monkeypatch.setattr(
            "lakebench.cli._prerequisites.deploy_capacity_check",
            lambda cfg: PrereqResult(name="cluster-capacity", passed=True, message="OK"),
        )

    @pytest.mark.parametrize(
        ("present", "missing"),
        [
            ([], ["hive-operator", "secret-operator"]),
            (["hiveclusters.hive.stackable.tech"], ["secret-operator"]),
            (["secretclasses.secrets.stackable.tech"], ["hive-operator"]),
            (["hiveclusters.hive.stackable.tech", "secretclasses.secrets.stackable.tech"], []),
        ],
    )
    def test_blocks_on_missing_stackable_operators(self, present, missing, capsys):
        from unittest.mock import MagicMock, patch

        from lakebench.cli import _preflight_check
        from lakebench.exit_codes import ExitCode
        from tests.conftest import make_config

        cfg = make_config()
        assert cfg.architecture.catalog.type.value == "hive"
        crds = MagicMock()
        crds.items = []
        for name in present:
            crd = MagicMock()
            crd.metadata.name = name
            crds.items.append(crd)

        with patch("kubernetes.client.ApiextensionsV1Api") as mock_api:
            mock_api.return_value.list_custom_resource_definition.return_value = crds
            if not missing:
                _preflight_check(cfg)
                return
            with pytest.raises(typer.Exit) as exc:
                _preflight_check(cfg)
        assert exc.value.exit_code == ExitCode.PREREQUISITE
        out = capsys.readouterr()
        text = out.out + out.err
        for op in ("hive-operator", "secret-operator"):
            assert (op in text) is (op in missing)


class TestRunPreflightInfraCheck:
    """_run_preflight_infra_check refuses a run when a component is absent or unready."""

    @pytest.mark.parametrize(
        ("fault", "named"),
        [
            ("namespace", "lakebench-test"),
            ("postgres", "PostgreSQL"),
            ("trino-workers", "Trino workers"),
            (None, None),
        ],
    )
    def test_blocks_on_a_missing_or_unready_component(self, fault, named, capsys):
        from unittest.mock import MagicMock, patch

        from kubernetes.client.rest import ApiException

        from lakebench.cli import _run_preflight_infra_check
        from lakebench.exit_codes import ExitCode
        from tests.conftest import make_config

        cfg = make_config(name="lakebench-test")
        assert cfg.architecture.query_engine.type.value == "trino"
        assert cfg.get_namespace() == "lakebench-test"

        def read(name, ns):
            if fault == "postgres" and name == "lakebench-postgres":
                raise ApiException(status=404, reason="Not Found")
            obj = MagicMock()
            workers = name == "lakebench-trino-worker"
            obj.spec.replicas = 4 if workers else 1
            obj.status.ready_replicas = (
                0 if workers and fault == "trino-workers" else obj.spec.replicas
            )
            return obj

        with (
            patch("lakebench.cli.get_k8s_client") as mock_get,
            patch("kubernetes.client.AppsV1Api") as mock_apps,
        ):
            mock_get.return_value.namespace_exists.return_value = fault != "namespace"
            mock_apps.return_value.read_namespaced_stateful_set.side_effect = read
            mock_apps.return_value.read_namespaced_deployment.side_effect = read
            if fault is None:
                _run_preflight_infra_check(cfg)
                return
            with pytest.raises(typer.Exit) as exc:
                _run_preflight_infra_check(cfg)
        assert exc.value.exit_code == ExitCode.PREREQUISITE
        out = capsys.readouterr()
        assert named in out.out + out.err


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
