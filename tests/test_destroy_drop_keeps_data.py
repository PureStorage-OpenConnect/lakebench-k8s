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

    # Where each table's files live (DESCRIBE DETAIL location). Tests move a
    # table elsewhere (a renamed bucket keeps the old schema LOCATION).
    locations: dict[str, str] = {}

    def run(self, verdicts, maint, *, table_format="iceberg", describe=None, **kw):
        engine_name = maint[0]
        boto = FakeBoto(
            {"b-bronze": list(CORPUS), "a-silver": ["s"], "a-gold": ["g"], "old-silver": ["o"]}
        )
        self.ran: list[str] = []
        where = {
            "bronze": "s3a://b-bronze/warehouse/bronze.db/pacs008_raw",
            "silver": "s3a://a-silver/warehouse/silver.db/t",
            "gold": "s3a://a-gold/warehouse/gold.db/t",
            **self.locations,
        }

        def bucket_of(sql):
            for layer, loc in where.items():
                if f".{layer}." in sql:
                    return loc.split("/")[2]
            return None

        def fake_query(_engine, _k8s, _pod, _ns, sql):
            self.ran.append(sql)
            if describe is not None:
                return describe(sql)
            b = bucket_of(sql)
            layer = next(x for x in where if f".{x}." in sql)
            return (
                "+--------+--------------------------+-----------------------+\n"
                "| format | name                     | location              |\n"
                f"| delta  | spark_catalog.{layer}.t  | {where[layer]} |\n"
                if b
                else ""
            )

        def on_sql(sql):
            self.ran.append(sql)
            deletes = sql.startswith("DROP") and not (
                engine_name == "spark-thrift" and table_format == "iceberg"
            )
            if deletes:
                boto.buckets[bucket_of(sql)] = []

        self._layers = ("b-bronze", "a-silver", "a-gold")
        self._on_sql = on_sql
        try:
            with patch("lakebench.deploy.iceberg.query_sql", side_effect=fake_query):
                buckets = self._run(boto, verdicts, maint=maint, table_format=table_format, **kw)
        finally:
            self._on_sql = None
            self._layers = None
        tables = [x for x in self._results if x.component == "table-cleanup"][-1]
        return boto, tables, buckets

    def drops(self):
        return [q for q in self.ran if q.startswith("DROP")]

    def left(self, tables):
        return [e["table"] for e in tables.details.get("tables_left_registered", [])]


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
    """Spark Thrift has no metadata-only drop for a managed Delta table: each
    table is dropped only when its own location is in a bucket destroy
    empties."""

    def test_refused_bronze_keeps_bronze_and_still_drops_silver_and_gold(self, h):
        """Refusing one bucket must not leave silver and gold registered over
        paths the bucket step then empties (dangling entries)."""
        boto, tables, _b = h.run(
            {"b-bronze": "MISMATCH", **OWNED}, THRIFT_DELTA, table_format="delta"
        )
        assert h.drops() == ["DROP spark_catalog.silver.t", "DROP spark_catalog.gold.t"]
        assert boto.buckets["b-bronze"] == CORPUS
        assert tables.status is DeploymentStatus.SKIPPED
        assert h.left(tables) == ["spark_catalog.bronze.pacs008_raw"]
        assert "b-bronze, which destroy does not empty" in tables.message

    def test_renamed_bucket_keeps_the_table_whose_location_is_elsewhere(self, h):
        """The config's buckets are all owned, but silver's schema LOCATION
        still points at the bucket used before a rename."""
        h.locations = {"silver": "s3a://old-silver/warehouse/silver.db/t"}
        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        boto, tables, _b = h.run(verdicts, THRIFT_DELTA, table_format="delta")
        assert "DROP spark_catalog.silver.t" not in h.drops()
        assert boto.buckets["old-silver"] == ["o"]
        assert h.left(tables) == ["spark_catalog.silver.t"]

    def test_unreadable_location_is_kept_and_fails_the_step(self, h):
        def describe(sql):
            raise RuntimeError("query_sql failed (rc=1): Command timed out")

        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        boto, tables, _b = h.run(verdicts, THRIFT_DELTA, table_format="delta", describe=describe)
        assert h.drops() == []
        assert tables.status is DeploymentStatus.FAILED
        assert "location unreadable" in tables.message

    def test_missing_table_is_a_clean_teardown(self, h):
        def describe(sql):
            raise RuntimeError(
                "query_sql failed (rc=1): Error: [TABLE_OR_VIEW_NOT_FOUND] The table or "
                "view `spark_catalog`.`gold`.`t` cannot be found."
            )

        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        _boto, tables, _b = h.run(verdicts, THRIFT_DELTA, table_format="delta", describe=describe)
        assert h.drops() == []
        assert tables.status is DeploymentStatus.SUCCESS, tables.message

    def test_registered_table_whose_directory_is_gone_is_dropped(self, h):
        """A re-run after the bucket step emptied the buckets: DESCRIBE DETAIL
        says the path is gone, a DROP deletes nothing, so it must not fail
        forever."""

        def describe(sql):
            raise RuntimeError(
                "query_sql failed (rc=1): Error: [DELTA_PATH_DOES_NOT_EXIST] "
                "s3a://a-silver/warehouse/silver.db/t doesn't exist"
            )

        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        _boto, tables, _b = h.run(verdicts, THRIFT_DELTA, table_format="delta", describe=describe)
        assert len(h.drops()) == 3
        assert tables.status is DeploymentStatus.SUCCESS, tables.message

    def test_refused_bucket_with_the_namespace_deleted_is_not_a_failure(self, h):
        _boto, tables, _b = h.run(
            {"b-bronze": "ABSENT", **OWNED},
            THRIFT_DELTA,
            table_format="delta",
            create_namespace=True,
            created=set(),
        )
        assert "DROP spark_catalog.bronze.pacs008_raw" not in h.drops()
        assert tables.status is DeploymentStatus.SUCCESS
        assert "goes with the namespace" in tables.message
        assert h.left(tables) == []

    def test_all_buckets_owned_drops_even_with_keep_buckets(self, h):
        """--keep-buckets still empties the buckets, so the DROP deletes
        nothing destroy was not about to delete."""
        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        _boto, tables, _b = h.run(
            verdicts, THRIFT_DELTA, table_format="delta", delete_buckets=False
        )
        assert len(h.drops()) == 3
        assert tables.status is DeploymentStatus.SUCCESS, tables.message

    def test_unreadable_ownership_keeps_the_drops_back_and_fails(self, h, monkeypatch):
        def boom(*_a, **_k):
            raise RuntimeError("apiserver 503")

        monkeypatch.setattr(destroy_mod, "_classify_buckets", boom)
        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        _boto, tables, _b = h.run(verdicts, THRIFT_DELTA, table_format="delta")
        assert h.drops() == []
        assert tables.status is DeploymentStatus.FAILED
        assert "could not be checked" in tables.message

    def test_bucket_cleanup_off_keeps_every_table_and_reports_it(self, h):
        verdicts = dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH")
        boto, tables, _b = h.run(verdicts, THRIFT_DELTA, table_format="delta", clean_buckets=False)
        assert h.drops() == []
        assert boto.buckets["a-silver"] == ["s"]
        assert tables.status is DeploymentStatus.SKIPPED
        assert len(h.left(tables)) == 3


class TestSparkThriftIceberg:
    def test_plain_drop_is_catalog_only_and_still_runs(self, h):
        boto, tables, _b = h.run({"b-bronze": "MISMATCH", **OWNED}, THRIFT)
        assert len(h.drops()) == 3
        assert boto.buckets["b-bronze"] == CORPUS
        assert tables.status is DeploymentStatus.SUCCESS


class TestReviewFixes:
    def test_unparsable_table_name_fails_the_step(self, h):
        """A name without catalog.schema.table must not silently keep every
        table (it raised ValueError into the generic handler, SKIPPED)."""
        h._tables = ("silver.t", "gold")
        _boto, tables, _b = h.run(dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH"), TRINO)
        assert tables.status is DeploymentStatus.FAILED
        assert "not <catalog>.<schema>.<table>" in tables.message
        assert any("'silver'" in q for q in h.ran), "the parsable table is still unregistered"

    def test_message_is_true_when_bucket_cleanup_is_off(self, h):
        _boto, tables, _b = h.run(
            dict.fromkeys(["b-bronze", "a-silver", "a-gold"], "MATCH"), TRINO, clean_buckets=False
        )
        assert "no files are removed (bucket cleanup is off)" in tables.message


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


class TestCliReportsTablesLeftRegistered:
    """A SKIPPED table step used to print nothing, and the panel said
    "Destroy Complete" with the tables still registered."""

    def test_warning_per_table_and_summary_title(self, monkeypatch, tmp_path):
        from pathlib import Path
        from unittest.mock import MagicMock

        from typer.testing import CliRunner

        from lakebench.cli import app
        from lakebench.deploy.engine import DeploymentResult

        results = [
            DeploymentResult(
                "table-cleanup",
                DeploymentStatus.SKIPPED,
                "1 Delta table(s) left registered",
                details={
                    "tables_left_registered": [
                        {
                            "table": "spark_catalog.bronze.pacs008_raw",
                            "reason": "its data is in b-bronze, which destroy does not empty",
                        }
                    ]
                },
            ),
            DeploymentResult("namespace", DeploymentStatus.SUCCESS, "Namespace x deleted"),
        ]
        monkeypatch.chdir(tmp_path)
        fixture = Path(__file__).parent / "fixtures" / "v14user.yaml"
        engine = MagicMock()
        engine.destroy_all.side_effect = lambda progress_callback=None, **_kw: (
            [progress_callback(r.component, r.status, r.message) for r in results],
            results,
        )[1]
        with patch("lakebench.deploy.DeploymentEngine", return_value=engine):
            out = CliRunner().invoke(app, ["destroy", str(fixture), "--force"])
        assert out.exit_code == 0, out.output
        assert "1 Delta table(s) left registered" in out.output
        assert "table left registered: spark_catalog.bronze.pacs008_raw" in out.output
        assert "1 tables left registered" in out.output


class TestOrphanDeltaLogGuard:
    """A later run must not append to or adopt a _delta_log that destroy
    left in a bucket it did not own."""

    def _spark(self, *, registered, location="s3a://b-bronze/warehouse/bronze.db"):
        from unittest.mock import MagicMock

        spark = MagicMock()
        if not registered:
            spark.table.side_effect = Exception("[TABLE_OR_VIEW_NOT_FOUND] not found")
        row = MagicMock()
        row.asDict.return_value = {"info_name": "Location", "info_value": location}
        spark.sql.return_value.collect.return_value = [row]
        return spark

    def _fs(self, monkeypatch, exists):
        from pathlib import Path
        from unittest.mock import MagicMock

        monkeypatch.syspath_prepend(
            str(Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts")
        )
        import common

        fs = MagicMock()
        fs.exists.return_value = exists
        seen = []

        def hfs(_spark, uri):
            seen.append(uri)
            return fs, uri

        monkeypatch.setattr(common, "_hadoop_fs", hfs)
        common._REGISTERED_DELTA_TABLES.clear()
        return common, seen

    def test_unregistered_table_over_an_existing_log_is_refused(self, monkeypatch):
        common, seen = self._fs(monkeypatch, exists=True)
        spark = self._spark(registered=False)
        with pytest.raises(RuntimeError, match="already holds a Delta log"):
            common.write_delta_table(spark, MagicMockDF(), "spark_catalog.bronze.PACS", "s3a://b/")
        assert seen == ["s3a://b-bronze/warehouse/bronze.db/pacs/_delta_log"]

    def test_fresh_location_writes(self, monkeypatch):
        common, _seen = self._fs(monkeypatch, exists=False)
        spark = self._spark(registered=False)
        df = MagicMockDF()
        common.write_delta_table(spark, df, "spark_catalog.bronze.t", "s3a://b/")
        assert df.saved == "spark_catalog.bronze.t"

    def test_registered_table_is_not_checked(self, monkeypatch):
        common, seen = self._fs(monkeypatch, exists=True)
        spark = self._spark(registered=True)
        df = MagicMockDF()
        common.write_delta_table(spark, df, "spark_catalog.silver.t", "s3a://b/")
        assert seen == [] and df.saved == "spark_catalog.silver.t"


class MagicMockDF:
    """Enough of DataFrame.write for write_delta_table's managed path."""

    def __init__(self):
        self.saved = None
        outer = self

        class _W:
            def format(self, _f):
                return self

            def mode(self, _m):
                return self

            def option(self, *_a):
                return self

            def partitionBy(self, *_a):
                return self

            def saveAsTable(self, name):
                outer.saved = name

        self.write = _W()
