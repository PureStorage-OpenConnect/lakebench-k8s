"""Destroy's table step never deletes data outside proven ownership (LB-186).

Trino's DROP TABLE deletes files: on Iceberg with a Hive metastore it deletes
every file the table references (add_files-registered datagen files
included) and the table directory; on Delta it deletes a managed table's
directory. The table step runs before the bucket step's ownership checks, so a
bucket destroy refuses to empty (another deployment's, adopted with data,
untagged pre-provisioned) still lost the files its tables pointed at.

The fake engine below applies those semantics: a file-deleting DROP of a
``bronze.*`` table wipes the bronze bucket, as Trino would wipe an
add_files-registered corpus. Each scenario checks the corpus survives.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from lakebench.deploy import destroy as destroy_mod
from lakebench.deploy.engine import DeploymentStatus

from . import test_destroy_bucket_delete as _tdb

FakeBoto = _tdb.FakeBoto

TRINO = ("trino", "trino-coordinator-0", "lakehouse")
THRIFT = ("spark-thrift", "thrift-0", "lakehouse")
THRIFT_DELTA = ("spark-thrift", "thrift-0", "spark_catalog")
CORPUS = ["raw/pacs008/part-0.parquet", "raw/pacs008/part-1.parquet"]


class _Harness(_tdb.TestDestroyAllBuckets):
    __test__ = False
    _tables = ("bronze.pacs008_raw", "silver.t", "gold.t")

    def run(self, verdicts, maint, *, table_format="iceberg", **kw):
        engine_name = maint[0]
        boto = FakeBoto({"b-bronze": list(CORPUS), "a-silver": ["s"], "a-gold": ["g"]})
        self.ran: list[str] = []

        def on_sql(sql):
            self.ran.append(sql)
            deletes = sql.startswith("DROP") and not (
                engine_name == "spark-thrift" and table_format == "iceberg"
            )
            if deletes:
                for layer, bucket in (
                    ("bronze", "b-bronze"),
                    ("silver", "a-silver"),
                    ("gold", "a-gold"),
                ):
                    if f".{layer}." in sql:
                        boto.buckets[bucket] = []

        self._layers = ("b-bronze", "a-silver", "a-gold")
        self._on_sql = on_sql
        try:
            buckets = self._run(boto, verdicts, maint=maint, table_format=table_format, **kw)
        finally:
            self._on_sql = None
            self._layers = None
        tables = [x for x in self._results if x.component == "table-cleanup"][-1]
        return boto, tables, buckets

    def drops(self):
        return [q for q in self.ran if q.startswith("DROP")]


@pytest.fixture(autouse=True)
def _no_real_sleep():
    with patch("time.sleep"):
        yield


@pytest.fixture
def h():
    return _Harness()


OWNED = {"a-silver": "MATCH", "a-gold": "MATCH"}


class TestTrinoNeverDeletesFiles:
    def test_bronze_owned_by_another_deployment_keeps_its_corpus(self, h):
        """S-P6 shape: a shared bronze tagged by deployment B."""
        boto, tables, buckets = h.run({"b-bronze": "MISMATCH", **OWNED}, TRINO)
        assert h.drops() == []
        assert boto.buckets["b-bronze"] == CORPUS
        assert "system.unregister_table(schema_name => 'bronze'" in h.ran[0]
        assert tables.status is DeploymentStatus.SUCCESS, tables.message
        assert buckets.status is DeploymentStatus.FAILED  # the refusal is still reported

    def test_keep_buckets_with_an_adopted_bucket_holding_data(self, h):
        """Tagless backend, bronze adopted with data (not on the adopted-empty
        record): the bucket step leaves it alone, so must the table step."""
        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "UNSUPPORTED")
        boto, tables, _b = h.run(
            verdicts,
            TRINO,
            delete_buckets=False,
            created={"a-silver", "a-gold"},
            other=[],
        )
        assert h.drops() == []
        assert boto.buckets["b-bronze"] == CORPUS
        assert tables.status is DeploymentStatus.SUCCESS, tables.message

    def test_create_buckets_false_with_an_untagged_pre_provisioned_bucket(self, h):
        boto, _t, _b = h.run(
            {"b-bronze": "ABSENT", **OWNED}, TRINO, create_buckets=False, created=set()
        )
        assert h.drops() == []
        assert boto.buckets["b-bronze"] == CORPUS

    def test_delta_on_trino_unregisters_too(self, h):
        boto, _t, _b = h.run({"b-bronze": "MISMATCH", **OWNED}, TRINO, table_format="delta")
        assert h.drops() == []
        assert boto.buckets["b-bronze"] == CORPUS

    def test_polaris_kept_namespace_unregisters_instead_of_purging(self, h):
        boto, _t, _b = h.run({"b-bronze": "MISMATCH", **OWNED}, TRINO, catalog_type="polaris")
        assert h.drops() == []
        assert len(h.ran) == 3 and all("unregister_table" in q for q in h.ran)
        assert boto.buckets["b-bronze"] == CORPUS

    def test_missing_schema_on_unregister_is_a_clean_teardown(self, h):
        """unregister_table raises SchemaNotFoundException, whose message is
        "Schema <name> not found" (no quotes), not the DROP analyzer's form."""

        def missing(sql):
            if "'gold'" in sql:
                raise RuntimeError(
                    "exec_sql failed (rc=1): Query 20260927_101010_00002_abcde failed: "
                    "Schema gold not found"
                )
            if "'silver'" in sql:
                raise RuntimeError(
                    "exec_sql failed (rc=1): Query 20260927_101010_00003_abcde failed: "
                    "Table 'silver.t' not found"
                )

        tables = h._run_tables(missing)
        assert tables.status is DeploymentStatus.SUCCESS, tables.message


class TestSparkThriftDelta:
    """Spark Thrift has no metadata-only drop for a managed Delta table."""

    def test_refused_bucket_skips_every_drop_and_says_so(self, h):
        boto, tables, _b = h.run(
            {"b-bronze": "MISMATCH", **OWNED}, THRIFT_DELTA, table_format="delta"
        )
        assert h.ran == []
        assert boto.buckets["b-bronze"] == CORPUS
        assert tables.status is DeploymentStatus.SKIPPED
        assert "left registered" in tables.message and "b-bronze" in tables.message

    def test_refused_bucket_with_the_namespace_deleted_is_not_a_failure(self, h):
        _boto, tables, _b = h.run(
            {"b-bronze": "ABSENT", **OWNED},
            THRIFT_DELTA,
            table_format="delta",
            create_namespace=True,
            created=set(),
        )
        assert h.ran == []
        assert tables.status is DeploymentStatus.SUCCESS
        assert "goes with the namespace" in tables.message

    def test_all_buckets_owned_drops_even_with_keep_buckets(self, h):
        """--keep-buckets still empties the buckets, so the DROP deletes
        nothing destroy was not about to delete."""
        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        _boto, tables, _b = h.run(
            verdicts, THRIFT_DELTA, table_format="delta", delete_buckets=False
        )
        assert len(h.drops()) == 3
        assert tables.status is DeploymentStatus.SUCCESS, tables.message

    def test_unreadable_ownership_keeps_the_drops_back(self, h, monkeypatch):
        def boom(*_a, **_k):
            raise RuntimeError("apiserver 503")

        monkeypatch.setattr(destroy_mod, "_classify_buckets", boom)
        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        _boto, tables, _b = h.run(verdicts, THRIFT_DELTA, table_format="delta")
        assert h.ran == []
        assert "could not be checked" in tables.message


class TestSparkThriftIceberg:
    def test_plain_drop_is_catalog_only_and_still_runs(self, h):
        boto, tables, _b = h.run({"b-bronze": "MISMATCH", **OWNED}, THRIFT)
        assert len(h.drops()) == 3
        assert boto.buckets["b-bronze"] == CORPUS
        assert tables.status is DeploymentStatus.SUCCESS


def test_unregister_sql_quotes_its_arguments():
    sql = destroy_mod._trino_unregister_sql("lakehouse.bronze.o'brien")
    assert sql == (
        "CALL lakehouse.system.unregister_table(schema_name => 'bronze', table_name => 'o''brien')"
    )


@pytest.mark.parametrize(
    ("engine", "fmt", "deletes"),
    [
        ("trino", "iceberg", True),
        ("trino", "delta", True),
        ("spark-thrift", "iceberg", False),
        ("spark-thrift", "delta", True),
        ("duckdb", "iceberg", True),
    ],
)
def test_drop_file_semantics_table(engine, fmt, deletes):
    assert destroy_mod._drop_deletes_files(engine, fmt) is deletes
