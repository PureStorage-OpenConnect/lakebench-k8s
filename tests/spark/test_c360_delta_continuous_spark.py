"""Executed: hive-delta continuous names every c360 table in spark_catalog
(bronze, silver and gold), and the reset and a fresh stream agree on them.

Runs ``c360_delta_continuous_scenarios.py`` in a fresh JVM with the Delta
jars from ``LB_SPARK_TEST_JARS`` on the classpath, and the Iceberg jar too:
the scenarios' session registers an Iceberg catalog.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("c360-delta-continuous")
    script = Path(__file__).with_name("c360_delta_continuous_scenarios.py")
    proc = spark_subprocess(script, spark_jars.classpath, work, timeout=900)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_bronze_names_and_lands_in_spark_catalog(result):
    # bronze-ingest names bronze in spark_catalog, with the expected rows.
    assert result["bronze_name"] == "spark_catalog.default.bronze_raw"
    assert result["bronze_rows"] == 20
    # default exists in HMS with a pod-local warehouse; bronze must not go
    # there: it lands at its bucket path.
    assert result["bronze_location"] == result["bronze_expected_location"]
    assert result["bronze_location"].endswith("/bronze/warehouse/default.db/bronze_raw/")


def test_reset_drops_all_three_tables_and_clears_orphan_logs(result):
    # The reset drops all three tables in spark_catalog and clears their files.
    assert result["targets"] == [
        "spark_catalog.default.bronze_raw",
        "spark_catalog.silver.customer_interactions_enriched",
        "spark_catalog.gold.customer_executive_dashboard",
    ]
    assert result["exists_after"] == dict.fromkeys(result["targets"], False)
    assert result["bronze_dir_files_after"] == 0
    assert result["raw_files_after"] == 1
    # A fresh stream after the reset recreates bronze.
    assert result["bronze_rows_after_restart"] == 10
    # The reset clears the _delta_log an unregistered EXTERNAL bronze leaves
    # behind, so a fresh bronze-ingest accepts the location.
    assert result["orphan_log_left"] is True
    assert result["orphan_dir_files_after_reset"] == 0
    assert result["bronze_rows_after_orphan_reset"] == 10
    # A failed batch build can leave a Delta log at silver's managed path with
    # no catalog entry; the continuous reset clears it.
    assert result["silver_orphan_unregistered"] is True
    assert result["silver_orphan_files_after_reset"] == 0
