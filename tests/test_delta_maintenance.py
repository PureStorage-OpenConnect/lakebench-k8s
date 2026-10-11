"""Tests for the Delta Lake maintenance module.

Covers:
- parse_retention_to_hours: duration string parsing
- build_delta_maintenance_sql: VACUUM SQL generation per engine
"""

import pytest

from lakebench.deploy.delta_maintenance import (
    build_delta_maintenance_sql,
    parse_retention_to_hours,
)

# ---------------------------------------------------------------------------
# parse_retention_to_hours
# ---------------------------------------------------------------------------


class TestParseRetentionToHours:
    """Tests for duration string to hours conversion."""

    @pytest.mark.parametrize(
        ("text", "hours"),
        [("30m", 0.5), ("1h", 1.0), ("7d", 168.0), ("0s", 0.0), ("3600s", 1.0), ("  30m  ", 0.5)],
    )
    def test_parse(self, text, hours):
        assert parse_retention_to_hours(text) == hours


# ---------------------------------------------------------------------------
# build_delta_maintenance_sql (VACUUM)
# ---------------------------------------------------------------------------


class TestBuildDeltaMaintenanceSql:
    """VACUUM SQL generation per engine."""

    @pytest.mark.parametrize(
        ("engine", "retention_hours", "expected"),
        [
            # At the 7-day default no retention-check override is needed.
            (
                "trino",
                168.0,
                [
                    "CALL lakehouse.system.vacuum(schema_name => 'bronze', "
                    "table_name => 'events', retention => '168.0h')"
                ],
            ),
            # Short retention: the override and the VACUUM go in ONE
            # submission. exec_sql runs each element as its own process, so a
            # separate SET would be lost and VACUUM would hit the minimum.
            (
                "trino",
                0.0,
                [
                    "SET SESSION lakehouse.vacuum_min_retention = '0s'; "
                    "CALL lakehouse.system.vacuum(schema_name => 'bronze', "
                    "table_name => 'events', retention => '0.0h')"
                ],
            ),
            ("spark-thrift", 168.0, ["VACUUM lakehouse.bronze.events RETAIN 168.0 HOURS"]),
            # retention_hours=0 is the destroy path: without the override
            # VACUUM fails and DROP TABLE would leave orphan S3 files.
            (
                "spark-thrift",
                0.0,
                [
                    "SET spark.databricks.delta.retentionDurationCheck.enabled=false; "
                    "VACUUM lakehouse.bronze.events RETAIN 0.0 HOURS"
                ],
            ),
        ],
    )
    def test_vacuum(self, engine, retention_hours, expected):
        stmts = build_delta_maintenance_sql(
            engine=engine,
            catalog="lakehouse",
            table="lakehouse.bronze.events",
            retention_hours=retention_hours,
        )
        assert stmts == expected
