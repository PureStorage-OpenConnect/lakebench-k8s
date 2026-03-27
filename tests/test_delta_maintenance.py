"""Tests for Delta Lake maintenance module (v1.2).

Covers:
- parse_retention_to_hours: duration string parsing
- build_delta_maintenance_sql: VACUUM SQL generation per engine
- build_delta_compaction_sql: OPTIMIZE SQL generation per engine
- build_delta_table_health_sql: health probe SQL per engine
- build_delta_drop_table_sql: DROP TABLE generation
"""

import pytest

from lakebench.deploy.delta_maintenance import (
    build_delta_compaction_sql,
    build_delta_drop_table_sql,
    build_delta_maintenance_sql,
    build_delta_table_health_sql,
    parse_retention_to_hours,
)

# ---------------------------------------------------------------------------
# parse_retention_to_hours
# ---------------------------------------------------------------------------


class TestParseRetentionToHours:
    """Tests for duration string to hours conversion."""

    def test_minutes(self):
        assert parse_retention_to_hours("30m") == 0.5

    def test_hours(self):
        assert parse_retention_to_hours("1h") == 1.0

    def test_days(self):
        assert parse_retention_to_hours("7d") == 168.0

    def test_zero_seconds(self):
        assert parse_retention_to_hours("0s") == 0.0

    def test_seconds_to_hours(self):
        assert parse_retention_to_hours("3600s") == 1.0

    def test_invalid_input_raises(self):
        with pytest.raises(ValueError):
            parse_retention_to_hours("abc")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            parse_retention_to_hours("")

    def test_no_unit_raises(self):
        with pytest.raises(ValueError):
            parse_retention_to_hours("30")

    def test_whitespace_trimmed(self):
        assert parse_retention_to_hours("  30m  ") == 0.5


# ---------------------------------------------------------------------------
# build_delta_maintenance_sql (VACUUM)
# ---------------------------------------------------------------------------


class TestBuildDeltaMaintenanceSql:
    """Tests for VACUUM SQL generation per engine."""

    def test_trino_vacuum(self):
        stmts = build_delta_maintenance_sql(
            engine="trino",
            catalog="lakehouse",
            table="lakehouse.bronze.events",
            retention_hours=168.0,
        )
        # At 168h (7d) -- no SET SESSION prepended, only the CALL
        assert len(stmts) == 1
        assert "CALL lakehouse.system.vacuum" in stmts[0]
        assert "schema_name => 'bronze'" in stmts[0]
        assert "table_name => 'events'" in stmts[0]
        assert "168.0h" in stmts[0]

    def test_trino_vacuum_short_retention_prepends_session_property(self):
        stmts = build_delta_maintenance_sql(
            engine="trino",
            catalog="lakehouse",
            table="lakehouse.bronze.events",
            retention_hours=0.0,
        )
        # Short retention: SET SESSION first, then catalog-qualified CALL
        assert len(stmts) == 2
        assert "SET SESSION lakehouse.vacuum_min_retention = '0s'" in stmts[0]
        assert "CALL lakehouse.system.vacuum" in stmts[1]
        assert "retention => '0.0h'" in stmts[1]

    def test_spark_thrift_vacuum(self):
        stmts = build_delta_maintenance_sql(
            engine="spark-thrift",
            catalog="lakehouse",
            table="lakehouse.bronze.events",
            retention_hours=168.0,
        )
        assert len(stmts) == 1
        assert "VACUUM" in stmts[0]
        assert "lakehouse.bronze.events" in stmts[0]
        assert "168.0 HOURS" in stmts[0]

    def test_duckdb_returns_empty(self):
        stmts = build_delta_maintenance_sql(
            engine="duckdb",
            catalog="lakehouse",
            table="lakehouse.bronze.events",
        )
        assert stmts == []

    def test_unknown_engine_returns_empty(self):
        stmts = build_delta_maintenance_sql(
            engine="unknown",
            catalog="lakehouse",
            table="lakehouse.bronze.events",
        )
        assert stmts == []


# ---------------------------------------------------------------------------
# build_delta_compaction_sql (OPTIMIZE)
# ---------------------------------------------------------------------------


class TestBuildDeltaCompactionSql:
    """Tests for OPTIMIZE SQL generation per engine."""

    def test_trino_optimize(self):
        stmts = build_delta_compaction_sql(
            engine="trino",
            catalog="lakehouse",
            table="lakehouse.silver.enriched",
        )
        assert len(stmts) == 1
        assert "ALTER TABLE" in stmts[0]
        assert "EXECUTE optimize" in stmts[0]
        assert "lakehouse.silver.enriched" in stmts[0]

    def test_spark_thrift_optimize(self):
        stmts = build_delta_compaction_sql(
            engine="spark-thrift",
            catalog="lakehouse",
            table="lakehouse.silver.enriched",
        )
        assert len(stmts) == 1
        assert "OPTIMIZE" in stmts[0]
        assert "lakehouse.silver.enriched" in stmts[0]

    def test_duckdb_returns_empty(self):
        stmts = build_delta_compaction_sql(
            engine="duckdb",
            catalog="lakehouse",
            table="lakehouse.silver.enriched",
        )
        assert stmts == []


# ---------------------------------------------------------------------------
# build_delta_table_health_sql
# ---------------------------------------------------------------------------


class TestBuildDeltaTableHealthSql:
    """Tests for Delta table health probe SQL per engine."""

    def test_trino_properties(self):
        result = build_delta_table_health_sql(
            engine="trino",
            catalog="lakehouse",
            table="lakehouse.silver.enriched",
        )
        assert isinstance(result, dict)
        assert "table_properties" in result
        assert "$properties" in result["table_properties"]

    def test_spark_thrift_describe_detail(self):
        result = build_delta_table_health_sql(
            engine="spark-thrift",
            catalog="lakehouse",
            table="lakehouse.silver.enriched",
        )
        assert isinstance(result, dict)
        assert "table_detail" in result
        assert "DESCRIBE DETAIL" in result["table_detail"]

    def test_duckdb_returns_empty(self):
        result = build_delta_table_health_sql(
            engine="duckdb",
            catalog="lakehouse",
            table="lakehouse.silver.enriched",
        )
        assert result == {}


# ---------------------------------------------------------------------------
# build_delta_drop_table_sql
# ---------------------------------------------------------------------------


class TestBuildDeltaDropTableSql:
    """Tests for DROP TABLE SQL generation."""

    def test_drop_table(self):
        sql = build_delta_drop_table_sql(
            catalog="lakehouse",
            table="lakehouse.silver.enriched",
        )
        assert sql == "DROP TABLE IF EXISTS lakehouse.silver.enriched"

    def test_drop_table_simple_name(self):
        sql = build_delta_drop_table_sql(catalog="cat", table="my_table")
        assert sql == "DROP TABLE IF EXISTS my_table"
