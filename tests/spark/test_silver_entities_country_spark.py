"""Executed test: silver.entities carries each party's country (2026-09-24
audit: it was a NULL literal, so W7's high-risk-corridor join never matched
and W7 reported "ran, 0 alerts" on every run)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


PARTY = (
    "struct<nm:string, ctry_of_res:string, pstl_adr:struct<twn_nm:string>, id:struct<lei:string>>"
)


def test_entities_get_the_country_they_were_keyed_with(spark):
    from silver_build_financial import _entity_id_from, build_entities

    bronze = spark.createDataFrame(
        [
            (("ACME LTD", "AE", ("DUBAI",), (None,)), ("JO BLOGGS", "US", ("BOSTON",), (None,))),
            (("JO BLOGGS", "US", ("BOSTON",), (None,)), ("ACME LTD", "AE", ("DUBAI",), (None,))),
        ],
        f"dbtr {PARTY}, cdtr {PARTY}",
    )
    txns = bronze.select(
        _entity_id_from(
            bronze.dbtr.nm, bronze.dbtr.ctry_of_res, bronze.dbtr.pstl_adr.twn_nm, bronze.dbtr.id.lei
        ).alias("originator_id"),
        _entity_id_from(
            bronze.cdtr.nm, bronze.cdtr.ctry_of_res, bronze.cdtr.pstl_adr.twn_nm, bronze.cdtr.id.lei
        ).alias("beneficiary_id"),
        bronze.dbtr.nm.alias("rptd_originator_name"),
        bronze.cdtr.nm.alias("rptd_beneficiary_name"),
    )
    ents = {r["name"]: r["country"] for r in build_entities(txns, bronze).collect()}
    assert ents == {"ACME LTD": "AE", "JO BLOGGS": "US"}
