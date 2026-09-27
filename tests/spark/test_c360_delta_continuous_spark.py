"""Executed: hive-delta continuous names every c360 table in spark_catalog
(lb16-cs, 2026-09-27). The reset used CATALOG_NAME ("lakehouse") for bronze
and failed with REQUIRES_SINGLE_PART_NAMESPACE on every attempt, before any
stream ran; bronze-ingest built the same name and would have failed next.

Runs ``c360_delta_continuous_scenarios.py`` in a fresh JVM with the Delta
jars on the classpath. Set ``LB_SPARK_TEST_JARS`` to a directory holding
delta-spark_2.13 and delta-storage jars (Iceberg too: the shared session
registers an Iceberg catalog); the test is skipped without it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

_JARS = os.environ.get("LB_SPARK_TEST_JARS", "")
_NEEDED = ("iceberg-spark-runtime", "delta-spark", "delta-storage")


def _have_jars() -> bool:
    if not _JARS or not Path(_JARS).is_dir():
        return False
    names = [p.name for p in Path(_JARS).glob("*.jar")]
    return all(any(n.startswith(k) for n in names) for k in _NEEDED)


pytestmark = pytest.mark.skipif(
    not _have_jars(), reason="LB_SPARK_TEST_JARS with Iceberg and Delta jars not set"
)


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    work = tmp_path_factory.mktemp("c360-delta-continuous")
    env = dict(os.environ)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    script = Path(__file__).with_name("c360_delta_continuous_scenarios.py")
    proc = subprocess.run(
        [sys.executable, str(script), _JARS, str(work)],
        capture_output=True,
        text=True,
        env=env,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_bronze_ingest_names_bronze_in_spark_catalog(result):
    assert result["bronze_name"] == "spark_catalog.default.bronze_raw"
    assert result["bronze_rows"] == 20


def test_bronze_lands_at_its_bucket_path_not_the_metastore_warehouse(result):
    """default exists in HMS with a pod-local warehouse; bronze must not go there."""
    assert result["bronze_location"] == result["bronze_expected_location"]
    assert result["bronze_location"].endswith("/bronze/warehouse/default.db/bronze_raw/")


def test_reset_drops_all_three_tables_in_spark_catalog(result):
    assert result["targets"] == [
        "spark_catalog.default.bronze_raw",
        "spark_catalog.silver.customer_interactions_enriched",
        "spark_catalog.gold.customer_executive_dashboard",
    ]
    assert result["exists_after"] == dict.fromkeys(result["targets"], False)
    assert result["bronze_dir_files_after"] == 0
    assert result["raw_files_after"] == 1


def test_a_fresh_stream_after_the_reset_recreates_bronze(result):
    assert result["bronze_rows_after_restart"] == 10
