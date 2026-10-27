"""Executed: the c360 continuous reset drops only this deployment's stream
tables and their files, and afterwards silver-stream no longer refuses a
fresh checkpoint (GOALS P6.1).

Runs ``c360_reset_scenarios.py`` in a fresh JVM with the Iceberg and Delta
jars from ``LB_SPARK_TEST_JARS`` on the classpath.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("c360-reset")
    script = Path(__file__).with_name("c360_reset_scenarios.py")
    proc = spark_subprocess(script, spark_jars.classpath, work, timeout=900)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_refusal_before_and_start_allowed_after(result):
    """The state before a reset, then the state the reset leaves."""
    assert result["refused_before"] is True
    assert result["refused_after"] is False


def test_reset_drops_the_three_tables_and_their_files(result):
    # The three continuous tables and their files (including the Delta table
    # under the owned root) are dropped; the raw landing zone survives.
    assert result["targets"] == [
        "ice.default.bronze_raw",
        "ice.silver.customer_interactions_enriched",
        "ice.gold.customer_executive_dashboard",
    ]
    assert result["exists_after"] == dict.fromkeys(result["targets"], False)
    assert result["silver_dir_files_after"] == 0
    assert result["raw_files_after"] == 1
    assert result["delta_files_before"] > 0
    assert result["delta_exists_after"] is False
    assert result["delta_files_after"] == 0


def test_reset_keeps_foreign_overlapping_and_rooted_paths_and_is_idempotent(result):
    # Files outside the deployment are kept.
    assert result["foreign_files_before"] > 0
    assert result["foreign_exists_after"] is False
    assert result["foreign_files_after"] == result["foreign_files_before"]
    # A directory overlapping a kept path is kept.
    assert result["dropped_wide"] == ["spark_catalog.other.wide_t"]
    assert result["wide_files_after"] == result["wide_files_before"] > 0
    # A table resolving to a directory not named after it keeps that directory.
    assert result["dropped_rooted"] == ["spark_catalog.other.rooted_t"]
    assert result["sibling_survives"] is True
    # Missing tables are skipped and a second reset drops nothing.
    assert result["dropped_direct"] == [
        "spark_catalog.silver.delta_t",
        "spark_catalog.other.foreign_t",
    ]
    assert result["second_dropped"] == []
