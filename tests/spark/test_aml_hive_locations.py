"""AML on the Hive catalog: bronze gets an explicit S3 location, namespaces exist.

Hive's default database lives on the metastore pod's local disk
(file:/stackable/warehouse), so the AML bronze table must carry an S3 location
there, and the silver/gold namespaces must be created before their tables
(Polaris creates them at bootstrap; Hive does not).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


class _RecordingSpark:
    def __init__(self):
        self.sql_calls = []

    def sql(self, text):
        self.sql_calls.append(text)


def test_ensure_namespaces_creates_each_namespace_once():
    from common import ensure_namespaces

    spark = _RecordingSpark()
    ensure_namespaces(spark, "lakehouse", ("silver.transactions", "silver.edges", "gold.alerts"))
    assert spark.sql_calls == [
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold",
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver",
    ]


def _bvf(monkeypatch, catalog_type):
    monkeypatch.setenv("LB_CATALOG_TYPE", catalog_type)
    monkeypatch.setenv("LB_BRONZE_URI", "s3a://ns-bronze/")
    monkeypatch.setenv("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
    import bronze_verify_financial

    return importlib.reload(bronze_verify_financial)


def test_hive_bronze_table_gets_an_s3_location(monkeypatch):
    mod = _bvf(monkeypatch, "hive")
    assert mod._bronze_location() == "s3a://ns-bronze/warehouse/default.db/pacs008_raw"
    assert mod._location_clause() == "LOCATION 's3a://ns-bronze/warehouse/default.db/pacs008_raw'"


def test_polaris_bronze_table_keeps_the_catalog_default(monkeypatch):
    mod = _bvf(monkeypatch, "polaris")
    assert mod._bronze_location() is None
    assert mod._location_clause() == ""


def test_namespaces_come_from_the_ddl_itself():
    from common import ensure_namespaces_for_ddl

    spark = _RecordingSpark()
    ddls = (
        "CREATE TABLE IF NOT EXISTS lakehouse.gold.alerts (a INT)",
        "\nCREATE TABLE IF NOT EXISTS lakehouse.ops.risk_scores (a INT)",
        "not a create statement",
    )
    ensure_namespaces_for_ddl(spark, "lakehouse", ddls)
    assert spark.sql_calls == [
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold",
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.ops",
    ]


def test_single_part_bronze_name_uses_default_namespace(monkeypatch):
    monkeypatch.setenv("LB_CATALOG_TYPE", "hive")
    monkeypatch.setenv("LB_BRONZE_URI", "s3a://ns-bronze/")
    monkeypatch.setenv("LB_FINANCIAL_BRONZE_TABLE", "bronze_raw")
    import bronze_verify_financial

    mod = importlib.reload(bronze_verify_financial)
    assert mod._bronze_location() == "s3a://ns-bronze/warehouse/default.db/bronze_raw"
