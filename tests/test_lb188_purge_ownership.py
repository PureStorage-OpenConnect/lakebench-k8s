"""LB-188: DROP ... PURGE must run only for a table whose directory this
deployment owns.

PURGE deletes every file the table metadata references, wherever it sits, so a
table whose location is unreadable, shared, or overlapping the raw datagen
landing zone must be dropped catalog-only (files kept). Covers both reset
paths: c360 (common.reset_stream_tables) and financial
(bronze_verify_financial._drop_owned_table). Pure-Python with a fake spark
that records the SQL issued; the existing live scenario only exercised Delta
not-owned tables, where PURGE never ran regardless.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_SCRIPTS = str(Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts")


class _FakeFS:
    def exists(self, _path):
        return False

    def delete(self, _path, _recursive):  # pragma: no cover - never reached here
        raise AssertionError("_hadoop_fs delete should not run when the dir is absent")


class _FakeSpark:
    """Records every SQL statement; PURGE can be made to raise (Polaris 403)."""

    def __init__(self, purge_raises=False):
        self.sql_calls: list[str] = []
        self.purge_raises = purge_raises

    def sql(self, q):
        self.sql_calls.append(q)
        if self.purge_raises and "PURGE" in q:
            raise RuntimeError("PURGE refused (403)")
        return None

    def purged(self, fq):
        return any("PURGE" in q and fq in q for q in self.sql_calls)

    def plain_dropped(self, fq):
        return any(q.strip() == f"DROP TABLE IF EXISTS {fq}" for q in self.sql_calls)


@pytest.fixture
def common(load_script):
    return load_script("common")


@pytest.fixture
def financial(monkeypatch, load_script):
    # bronze_verify_financial reads these into module constants (CATALOG,
    # BRONZE_URI, PACS_PREFIX) at import time. Clear anything a prior test in
    # the full suite leaked so the re-import uses the declared defaults these
    # assertions expect (a leaked LB_ICEBERG_CATALOG once turned every
    # lakehouse.* assertion into ice.*).
    for var in (
        "LB_ICEBERG_CATALOG",
        "LB_CATALOG_TYPE",
        "LB_BRONZE_URI",
        "LB_FINANCIAL_BRONZE_PREFIX",
        "LB_FINANCIAL_PACS_PATH",
        "LB_FINANCIAL_SILVER_TRANSACTIONS",
        "LB_FINANCIAL_BRONZE_TABLE",
    ):
        monkeypatch.delenv(var, raising=False)
    return load_script("bronze_verify_financial")


# --- c360: common.reset_stream_tables ---------------------------------------


def _wire_reset(common, monkeypatch, *, location, provider):
    monkeypatch.setattr(common, "table_exists", lambda spark, fq: True)
    monkeypatch.setattr(common, "_describe_table", lambda spark, fq: (location, provider))
    monkeypatch.setattr(common, "_delete_children", lambda spark, loc, keep=(): 0)
    monkeypatch.setattr(common, "_hadoop_fs", lambda spark, uri: (_FakeFS(), object()))


def test_reset_purges_an_owned_iceberg_table(common, monkeypatch):
    _wire_reset(
        common,
        monkeypatch,
        location="s3://lb-c360/silver/customer_interactions_enriched",
        provider="iceberg",
    )
    spark = _FakeSpark()
    fq = "ice.silver.customer_interactions_enriched"
    common.reset_stream_tables(
        spark, [fq], owned_uris=["s3://lb-c360/"], keep_uris=["s3://lb-c360/raw/"]
    )
    assert spark.purged(fq)


def test_reset_never_purges_a_not_owned_iceberg_table(common, monkeypatch):
    # Location outside the deployment's owned roots. PURGE would delete the
    # foreign files (LB-188); the reset must drop catalog-only.
    _wire_reset(
        common,
        monkeypatch,
        location="s3://someone-else/silver/customer_interactions_enriched",
        provider="iceberg",
    )
    spark = _FakeSpark()
    fq = "ice.silver.customer_interactions_enriched"
    common.reset_stream_tables(
        spark, [fq], owned_uris=["s3://lb-c360/"], keep_uris=["s3://lb-c360/raw/"]
    )
    assert not spark.purged(fq), "PURGE ran on a table outside this deployment"
    assert spark.plain_dropped(fq)


def test_reset_never_purges_when_location_overlaps_the_datagen_zone(common, monkeypatch):
    # keep_uris (raw landing zone) inside the table's own directory: owned_table_dir
    # refuses it, so no PURGE.
    _wire_reset(
        common,
        monkeypatch,
        location="s3://lb-c360/raw",
        provider="iceberg",
    )
    spark = _FakeSpark()
    fq = "ice.silver.raw"
    common.reset_stream_tables(
        spark, [fq], owned_uris=["s3://lb-c360/"], keep_uris=["s3://lb-c360/raw/"]
    )
    assert not spark.purged(fq)
    assert spark.plain_dropped(fq)


# --- financial: bronze_verify_financial._drop_owned_table -------------------


def _wire_financial(financial, monkeypatch, *, location):
    monkeypatch.setattr(financial, "_table_location", lambda spark, fq: location)
    calls: list[str] = []
    monkeypatch.setattr(
        financial, "_delete_dir_if_disjoint", lambda spark, loc, raw: calls.append(loc)
    )
    return calls


def test_financial_purges_an_owned_table(financial, monkeypatch):
    _wire_financial(
        financial, monkeypatch, location="s3://lb-bronze/warehouse/silver.db/transactions"
    )
    spark = _FakeSpark()
    financial._drop_owned_table(spark, "silver.transactions")
    assert spark.purged("lakehouse.silver.transactions")


def test_financial_purges_iceberg_unique_location_form(financial, monkeypatch):
    _wire_financial(
        financial,
        monkeypatch,
        location="s3://lb-bronze/warehouse/silver.db/transactions-9c1f2a",
    )
    spark = _FakeSpark()
    financial._drop_owned_table(spark, "silver.transactions")
    assert spark.purged("lakehouse.silver.transactions")


def test_financial_never_purges_when_location_overlaps_datagen(financial, monkeypatch):
    # Under the raw datagen path (BRONZE_URI + PACS_PREFIX): PURGE would delete
    # the corpus. Catalog-only DROP.
    _wire_financial(
        financial,
        monkeypatch,
        location=financial.BRONZE_URI + financial.PACS_PREFIX + "sub/transactions",
    )
    spark = _FakeSpark()
    fq = "lakehouse.silver.transactions"
    financial._drop_owned_table(spark, "silver.transactions")
    assert not spark.purged(fq)
    assert spark.plain_dropped(fq)


def test_financial_never_purges_a_namespace_root(financial, monkeypatch):
    # Location is a namespace/warehouse root, not named after the table.
    _wire_financial(financial, monkeypatch, location="s3://lb-bronze/warehouse/silver.db")
    spark = _FakeSpark()
    fq = "lakehouse.silver.transactions"
    financial._drop_owned_table(spark, "silver.transactions")
    assert not spark.purged(fq)
    assert spark.plain_dropped(fq)


def test_financial_no_location_is_catalog_only(financial, monkeypatch):
    _wire_financial(financial, monkeypatch, location=None)
    spark = _FakeSpark()
    fq = "lakehouse.silver.transactions"
    financial._drop_owned_table(spark, "silver.transactions")
    assert not spark.purged(fq)
    assert spark.plain_dropped(fq)


def test_financial_purge_refused_falls_back_to_drop_and_dir_delete(financial, monkeypatch):
    calls = _wire_financial(
        financial, monkeypatch, location="s3://lb-bronze/warehouse/silver.db/transactions"
    )
    spark = _FakeSpark(purge_raises=True)
    fq = "lakehouse.silver.transactions"
    financial._drop_owned_table(spark, "silver.transactions")
    # PURGE attempted, refused, then plain DROP and the owned dir is deleted.
    assert any("PURGE" in q for q in spark.sql_calls)
    assert spark.plain_dropped(fq)
    assert calls == ["s3://lb-bronze/warehouse/silver.db/transactions"]
