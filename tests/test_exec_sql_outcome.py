"""exec_sql must report the real outcome of a statement.

``K8sClient.exec_in_pod`` never raises: a failed Trino CLI or beeline call,
a kubectl error and a timeout all come back as ``rc != 0``. ``exec_sql``
used to drop that tuple, so every failed maintenance, compaction or DROP read
as success. These tests pin the fixed contract, the destroy classifier that
decides which failures mean "table was never there", and what each caller
outside destroy does with a real failure.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from lakebench.deploy import destroy as destroy_mod
from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout, exec_sql

# -- exec_sql -----------------------------------------------------------------


class TestExecSql:
    @pytest.mark.parametrize(
        ("engine", "rc", "stdout", "stderr", "exc", "carried", "container"),
        [
            (
                "trino",
                1,
                "",
                "Query q1 failed: line 1:13: Table 'lakehouse.gold.t' does not exist",
                RuntimeError,
                "does not exist",
                None,
            ),
            (
                "spark-thrift",
                2,
                "Error: [TABLE_OR_VIEW_NOT_FOUND] ...",
                "",
                RuntimeError,
                "TABLE_OR_VIEW_NOT_FOUND",
                "spark-thrift",
            ),
            ("trino", 1, "", "Command timed out", ExecSqlTimeout, None, None),
        ],
        ids=["trino-failure", "beeline-failure", "timeout"],
    )
    def test_failure_raises_the_right_class(
        self, engine, rc, stdout, stderr, exc, carried, container
    ):
        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (rc, stdout, stderr)
        with pytest.raises(RuntimeError) as ei:
            exec_sql(engine, k8s, "pod-0", "ns", "ALTER TABLE t EXECUTE x", timeout=600)
        assert type(ei.value) is exc
        if carried:
            assert carried in str(ei.value)
        assert k8s.exec_in_pod.call_args.kwargs["timeout"] == 600
        assert k8s.exec_in_pod.call_args.kwargs.get("container") == container


# -- destroy's classifier ------------------------------------------------------

TRINO_METASTORE_DOWN = (
    "Query q8 failed: Failed connecting to Hive metastore: "
    "[lakebench-hive-metastore.ns.svc.cluster.local:9083]"
)


def _err(text: str) -> RuntimeError:
    return RuntimeError(f"exec_sql failed (rc=1): {text}")


_BEELINE = "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "


@pytest.mark.parametrize(
    ("output", "table_missing", "schema_missing"),
    [
        pytest.param(
            "Query q1 failed: line 1:13: Table 'lakehouse.gold.t' does not exist",
            True,
            False,
            id="trino-table-missing",
        ),
        pytest.param(
            "Query q2 failed: line 1:13: Schema 'gold' does not exist",
            True,
            True,
            id="trino-schema-missing",
        ),
        pytest.param(
            _BEELINE + "[TABLE_OR_VIEW_NOT_FOUND] The table or view `lakehouse`.`gold`.`t` "
            "cannot be found. (state=42P01,code=0)",
            True,
            False,
            id="beeline-table-missing",
        ),
        pytest.param(
            _BEELINE + "[SCHEMA_NOT_FOUND] The schema `lakehouse`.`gold` cannot be found. "
            "(state=42704,code=0)",
            True,
            True,
            id="beeline-schema-missing",
        ),
        pytest.param(
            "Query q3 failed: line 1:13: Catalog 'lakehouse' not found",
            False,
            False,
            id="trino-catalog-not-found",
        ),
        pytest.param(
            "Query q4 failed: line 1:13: Catalog 'lakehouse' does not exist",
            False,
            False,
            id="trino-catalog-does-not-exist",
        ),
        # The echoed statement names a table and "does not exist" belongs to a
        # different error: free-text matching would call this "missing".
        pytest.param(
            "ALTER TABLE lakehouse.gold.t EXECUTE remove_orphan_files -- table cleanup\n"
            "Query q5 failed: Catalog 'lakehouse' does not exist",
            False,
            False,
            id="echoed-sql-then-catalog",
        ),
        pytest.param(
            "Query q6 failed: ALTER TABLE lakehouse.gold.t EXECUTE expire_snapshots: "
            "Catalog 'lakehouse' does not exist",
            False,
            False,
            id="same-line-sql-then-catalog",
        ),
        pytest.param(
            "Query q7 failed: line 1:1: Table procedure not registered: remove_orphan_files",
            False,
            False,
            id="trino-procedure-missing",
        ),
        pytest.param(TRINO_METASTORE_DOWN, False, False, id="trino-metastore-down"),
        pytest.param(
            _BEELINE + "[CATALOG_NOT_FOUND] The catalog `lakehouse` not found. "
            "(state=42P08,code=0)",
            False,
            False,
            id="beeline-catalog-missing",
        ),
        pytest.param(
            _BEELINE + "java.lang.IllegalArgumentException: Cannot find procedure: "
            "system.remove_orphan_filez",
            False,
            False,
            id="beeline-procedure-missing",
        ),
        pytest.param(
            _BEELINE + "org.apache.thrift.transport.TTransportException: "
            "java.net.ConnectException: Connection refused (state=08S01,code=0)",
            False,
            False,
            id="beeline-metastore-down",
        ),
        pytest.param("Command timed out", False, False, id="timeout"),
        pytest.param(
            _BEELINE + "java.lang.IllegalArgumentException: Couldn't load table 'exp.nope' "
            "in catalog 'lakehouse'",
            True,
            False,
            id="spark-procedure-table-missing",
        ),
        pytest.param(
            _BEELINE + "[TABLE_OR_VIEW_NOT_FOUND] The table or view `lakehouse`.`exp`.`nope` "
            "cannot be found.",
            True,
            False,
            id="spark-dml-table-missing",
        ),
        pytest.param(
            "Query q9 failed: Retention specified (30.00m) is shorter than the minimum "
            "retention configured in the system (7.00d). Minimum retention can be changed "
            "with iceberg.expire_snapshots_min_retention configuration property or "
            "iceberg.expire_snapshots_min_retention session property",
            False,
            False,
            id="trino-min-retention",
        ),
        pytest.param(
            _BEELINE + "java.lang.IllegalArgumentException: Cannot remove orphan files with "
            "an interval less than 24 hours. Executing this procedure with a short interval "
            "may corrupt the table if other operations are happening at the same time.",
            False,
            False,
            id="spark-orphan-interval",
        ),
        # Trino table procedures raise TableNotFoundException, whose message
        # differs from the analyzer's.
        pytest.param(
            "Query q10 failed: Table 'gold.customer_executive_dashboard' not found",
            True,
            False,
            id="trino-procedure-table-missing",
        ),
        pytest.param(
            _BEELINE + "java.lang.IllegalArgumentException: Couldn't load table 'gold.t' in "
            "catalog 'lakehouse' (state=,code=0)",
            True,
            False,
            id="beeline-iceberg-procedure-table-missing",
        ),
    ],
)
def test_classifier(output, table_missing, schema_missing):
    e = _err(output)
    assert destroy_mod._is_table_missing(e) is table_missing
    assert destroy_mod._is_schema_missing(e) is schema_missing


# -- callers outside destroy: a failure is recorded, never fatal ---------------


def _cfg(engine_type="trino"):
    from lakebench.config.schema import TableNamesConfig

    cfg = MagicMock()
    cfg.get_namespace.return_value = "lakebench-test"
    cfg.architecture.query_engine.type.value = engine_type
    cfg.architecture.query_engine.trino.catalog_name = "lakehouse"
    cfg.architecture.table_format.type.value = "iceberg"
    cfg.architecture.tables = TableNamesConfig()
    cfg.architecture.workload.schema_type.value = "customer360"
    return cfg


def _journal_details(j, message):
    for call in j.record.call_args_list:
        if call.kwargs.get("message") == message:
            return call.kwargs["details"]
    raise AssertionError(f"no journal record {message!r}")


@pytest.fixture
def _engine_pod():
    with patch(
        "lakebench.deploy.iceberg.find_maintenance_engine",
        return_value=("trino", "trino-coordinator-0", "lakehouse"),
    ):
        yield


@pytest.mark.usefixtures("_engine_pod")
class TestSustainedCallers:
    @pytest.mark.parametrize(
        ("step", "rc", "stderr", "outcome"),
        [
            ("maintenance", 1, TRINO_METASTORE_DOWN, "failed"),
            ("maintenance", 0, "", "succeeded"),
            ("maintenance", 1, "Command timed out", "timed_out"),
            ("compaction", 1, TRINO_METASTORE_DOWN, "failed"),
        ],
    )
    def test_journal_counts_each_outcome_apart(self, step, rc, stderr, outcome):
        from lakebench.cli._sustained import _run_iceberg_compaction, _run_iceberg_maintenance

        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (rc, "", stderr)
        j = MagicMock()
        if step == "maintenance":
            _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m")
            d = _journal_details(j, "Iceberg maintenance")
        else:
            _run_iceberg_compaction(_cfg(), k8s, Console(quiet=True), j)
            d = _journal_details(j, "Iceberg compaction")
            assert d["operations_total"] == 2
        assert d["operations_total"] > 0
        assert d[f"operations_{outcome}"] == d["operations_total"]
        for other in {"succeeded", "failed", "timed_out"} - {outcome}:
            assert d.get(f"operations_{other}", 0) == 0


@pytest.mark.usefixtures("_engine_pod")
def test_pre_benchmark_budget_stops_at_first_timeout_across_both_steps():
    """Financial: 51 statements x 1800 s must not become a 25 h wait."""
    from lakebench.cli._sustained import (
        MaintenanceBudget,
        _run_iceberg_compaction,
        _run_iceberg_maintenance,
    )

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (1, "", "Command timed out")
    j = MagicMock()
    budget = MaintenanceBudget(1800)
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m", budget=budget)
    _run_iceberg_compaction(_cfg(), k8s, Console(quiet=True), j, budget=budget)
    assert k8s.exec_in_pod.call_count == 1, "the first timeout stops everything"
    assert "timed out" in budget.stopped
    m = _journal_details(j, "Iceberg maintenance")
    c = _journal_details(j, "Iceberg compaction")
    assert m["operations_timed_out"] == 1
    assert m["operations_not_attempted"] == m["operations_total"] - 1
    assert c["operations_not_attempted"] == c["operations_total"] == 2
    assert c["not_attempted"] and c["stopped"]


@pytest.mark.usefixtures("_engine_pod")
def test_pre_benchmark_budget_has_an_overall_cap(monkeypatch):
    from lakebench.cli._sustained import MaintenanceBudget, _run_iceberg_maintenance

    clock = {"t": 0.0}
    budget = MaintenanceBudget(1800)
    budget._clock = lambda: clock["t"]
    budget.deadline = 1800

    k8s = MagicMock()

    def slow(*_a, **_kw):
        clock["t"] += 1000  # each statement takes 1000 s
        return (0, "", "")

    k8s.exec_in_pod.side_effect = slow
    j = MagicMock()
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m", budget=budget)
    d = _journal_details(j, "Iceberg maintenance")
    assert k8s.exec_in_pod.call_count == 2
    assert d["operations_not_attempted"] == d["operations_total"] - 2
    assert "cap" in budget.stopped


def _delta_cfg():
    cfg = _cfg()
    cfg.architecture.table_format.type.value = "delta"
    return cfg


@pytest.mark.usefixtures("_engine_pod")
def test_continuous_delta_vacuum_keeps_default_retention():
    """Policy: no retention override while streams are live."""
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    _run_iceberg_maintenance(_delta_cfg(), k8s, Console(quiet=True), j, "30m", live_streams=True)
    sent = [c.args[1][2] for c in k8s.exec_in_pod.call_args_list]
    assert sent and all("vacuum_min_retention" not in q for q in sent)
    assert all("168.0h" in q for q in sent)
    _journal_details(j, "Delta VACUUM at default 7d retention (live streams)")


@pytest.mark.usefixtures("_engine_pod")
def test_batch_delta_vacuum_still_honours_short_retention():
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    _run_iceberg_maintenance(_delta_cfg(), k8s, Console(quiet=True), MagicMock(), "30m")
    sent = [c.args[1][2] for c in k8s.exec_in_pod.call_args_list]
    assert sent and all("vacuum_min_retention" in q for q in sent)


# -- maintenance SQL that actually runs ---------------------------------------


def test_trino_maintenance_sets_the_session_minimum_in_the_same_submission():
    from lakebench.deploy.iceberg import build_maintenance_sql

    exp, orph = build_maintenance_sql("trino", "lakehouse", "lakehouse.silver.t", "30m")
    assert exp == (
        "SET SESSION lakehouse.expire_snapshots_min_retention = '30m'; "
        "ALTER TABLE lakehouse.silver.t EXECUTE expire_snapshots(retention_threshold => '30m')"
    )
    # Orphans never below 24 h + 10 min, even when asked for less.
    assert orph == (
        "SET SESSION lakehouse.remove_orphan_files_min_retention = '1450m'; "
        "ALTER TABLE lakehouse.silver.t EXECUTE remove_orphan_files(retention_threshold => '1450m')"
    )
    # The catalog comes from config, not a hard-coded name.
    exp2, orph2 = build_maintenance_sql("trino", "iceberg_cat", "iceberg_cat.s.t", "1h", "48h")
    assert exp2.startswith("SET SESSION iceberg_cat.expire_snapshots_min_retention = '1h'; ")
    assert "remove_orphan_files_min_retention = '48h'" in orph2


def _iceberg_trino_statements():
    from lakebench.deploy.iceberg import build_maintenance_sql

    return build_maintenance_sql("trino", "lakehouse", "lakehouse.silver.t", "30m")


def _delta_trino_statements():
    from lakebench.deploy.delta_maintenance import build_delta_maintenance_sql

    return build_delta_maintenance_sql("trino", "lakehouse", "lakehouse.gold.t", 0.0)


@pytest.mark.parametrize(
    ("statements", "main_statement"),
    [
        (_iceberg_trino_statements, "ALTER TABLE"),
        (_delta_trino_statements, "CALL lakehouse.system.vacuum"),
    ],
    ids=["iceberg", "delta"],
)
def test_trino_maintenance_reaches_one_execute_each(statements, main_statement):
    """SET SESSION and the statement it governs travel in one `trino --execute`."""
    sqls = list(statements())
    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    for sql in sqls:
        exec_sql("trino", k8s, "trino-0", "ns", sql)
    assert k8s.exec_in_pod.call_count == len(sqls)
    for call in k8s.exec_in_pod.call_args_list:
        cmd = call.args[1]
        assert cmd[:2] == ["trino", "--execute"]
        assert cmd[2].index("SET SESSION") < cmd[2].index(main_statement)


@pytest.mark.parametrize(
    "now",
    [
        datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 26, 14, 0, 0, tzinfo=timezone(timedelta(hours=2))),
    ],
    ids=["utc", "cest"],
)
def test_spark_maintenance_uses_a_timestamp_literal(now):
    from lakebench.deploy.iceberg import build_maintenance_sql

    exp, orph = build_maintenance_sql(
        "spark-thrift", "lakehouse", "lakehouse.silver.t", "30m", "24h", now=now
    )
    # An explicit UTC offset whatever the input zone, so the Thrift session
    # time zone cannot shift it.
    assert exp == (
        "CALL lakehouse.system.expire_snapshots(table => 'lakehouse.silver.t', "
        "older_than => TIMESTAMP '2026-09-26 11:30:00+00:00')"
    )
    # 24 h asked, 24 h 10 min enforced.
    assert "older_than => TIMESTAMP '2026-09-25 11:50:00+00:00'" in orph
    assert "UNIX_TIMESTAMP" not in exp + orph


def _sent(k8s):
    return [
        c.args[1][-2] if c.args[1][0] != "trino" else c.args[1][2]
        for c in k8s.exec_in_pod.call_args_list
    ]


@pytest.mark.usefixtures("_engine_pod")
def test_continuous_floors_expire_at_1h_and_orphans_at_24h10m():
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m", live_streams=True)
    sent = _sent(k8s)
    assert any("expire_snapshots(retention_threshold => '1h')" in q for q in sent)
    assert any("remove_orphan_files(retention_threshold => '1450m')" in q for q in sent)
    d = _journal_details(j, "Iceberg maintenance")
    assert d["expire_retention"] == "1h" and d["orphan_retention"] == "1450m"


@pytest.mark.usefixtures("_engine_pod")
def test_batch_trino_orphans_are_floored_too():
    """Stream apps may be writing even in batch (restartPolicy Always)."""
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "0s")
    sent = _sent(k8s)
    assert any("expire_snapshots(retention_threshold => '0s')" in q for q in sent)
    assert all("'0s'" not in q for q in sent if "remove_orphan_files" in q)
    d = _journal_details(j, "Iceberg maintenance")
    assert d["expire_retention"] == "0s" and d["orphan_retention"] == "1450m"


def test_batch_spark_orphans_never_below_24h10m():
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    with patch(
        "lakebench.deploy.iceberg.find_maintenance_engine",
        return_value=("spark-thrift", "thrift-0", "lakehouse"),
    ):
        _run_iceberg_maintenance(_cfg("spark-thrift"), k8s, Console(quiet=True), j, "30m")
    d = _journal_details(j, "Iceberg maintenance")
    assert d["expire_retention"] == "30m" and d["orphan_retention"] == "1450m"


# -- parser, live-stream detection, loop resilience ---------------------------


@pytest.mark.parametrize(
    ("given", "stored"), [("30m", "30m"), (" 7D ", "7d"), ("1 H", "1h"), ("0s", "0s")]
)
def test_config_normalises_valid_thresholds(given, stored):
    from lakebench.config.schema import SustainedConfig

    assert SustainedConfig(retention_threshold=given).retention_threshold == stored


def test_live_stream_apps_detects_present_apps_and_fails_safe():
    from kubernetes.client.rest import ApiException

    from lakebench.cli._sustained import _STREAM_APPS, _live_stream_apps

    timeouts = []

    def get(_g, _v, _ns, _p, name, _request_timeout=None):
        timeouts.append(_request_timeout)
        if name == "lakebench-silver-stream":
            return {"metadata": {"name": name}}
        if name == "lakebench-gold-refresh":
            raise ApiException(status=503)
        raise ApiException(status=404)

    with patch("kubernetes.client.CustomObjectsApi") as api:
        api.return_value.get_namespaced_custom_object.side_effect = get
        live, errors = _live_stream_apps("ns")
    assert live == ["lakebench-silver-stream", "lakebench-gold-refresh"]
    assert errors == ["lakebench-gold-refresh: HTTP 503"]
    assert set(live) <= set(_STREAM_APPS)
    assert timeouts and all(t == 10 for t in timeouts), "every read is bounded"


def test_live_stream_probe_timeout_counts_as_live():
    from urllib3.exceptions import ReadTimeoutError

    from lakebench.cli._sustained import _STREAM_APPS, _live_stream_apps

    def slow(*_a, **_kw):
        raise ReadTimeoutError(None, "/apis", "Read timed out. (read timeout=10)")

    with patch("kubernetes.client.CustomObjectsApi") as api:
        api.return_value.get_namespaced_custom_object.side_effect = slow
        live, errors = _live_stream_apps("ns")
    assert live == list(_STREAM_APPS)
    assert all("ReadTimeoutError" in e for e in errors)


def test_live_streams_nulls_the_maintenance_value():
    from lakebench.cli._run import _maintenance_value

    value, n, reason = _maintenance_value(
        [], [], 66, 61, 180.0, None, live_streams_reason="stream apps present: x"
    )
    assert value is None and "streams were live" in reason


def test_finished_leftover_stream_apps_are_not_live():
    from lakebench.cli._sustained import _live_stream_apps

    states = {
        "lakebench-bronze-ingest": "COMPLETED",
        "lakebench-silver-stream": "FAILED",
        "lakebench-gold-refresh": "RUNNING",
    }

    def get(_g, _v, _ns, _p, name, _request_timeout=None):
        return {"status": {"applicationState": {"state": states[name]}}}

    with patch("kubernetes.client.CustomObjectsApi") as api:
        api.return_value.get_namespaced_custom_object.side_effect = get
        live, errors = _live_stream_apps("ns")
    assert live == ["lakebench-gold-refresh"] and errors == []
