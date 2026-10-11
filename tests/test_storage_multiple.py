"""RPT-5 storage multiple.

A fake listing and fake metadata answers with known bytes give the expected
multiple per table, layer and total; raw bronze stays outside the total;
excluded prefixes are taken out before table attribution.
"""

from __future__ import annotations

import pytest

from lakebench.metrics import storage_multiple as sm

GB = 1024**3

BUCKETS = {"bronze": "lb-bronze", "silver": "lb-silver", "gold": "lb-gold"}
TABLES = {"bronze": ["bronze.raw"], "silver": ["silver.txn"], "gold": ["gold.daily"]}
LOCATIONS = {
    "lakehouse.bronze.raw": "s3a://lb-bronze/warehouse/bronze/raw",
    "lakehouse.silver.txn": "s3a://lb-silver/warehouse/silver/txn",
    "lakehouse.gold.daily": "s3a://lb-gold/warehouse/gold/daily",
}


def _objects(extra_gold=()):
    return {
        "lb-bronze": [
            # Raw corpus files: physical only, outside the total.
            {"Key": "customer/interactions/part-0.parquet", "Size": 10 * GB},
            {"Key": "customer/interactions/_corpus/c000-node-0000.json", "Size": 1000},
            {"Key": "customer/interactions/manifest/manifest.parquet", "Size": 2000},
            {"Key": "checkpoints/bronze-ingest/offsets/0", "Size": 300},
            {"Key": "warehouse/bronze/raw/data/a.parquet", "Size": 4 * GB},
            {"Key": "warehouse/bronze/raw/metadata/v1.metadata.json", "Size": 1 * GB},
        ],
        "lb-silver": [
            {"Key": "warehouse/silver/txn/data/a.parquet", "Size": 6 * GB},
            {"Key": "warehouse/silver/txn/data/old.parquet", "Size": 2 * GB},
            {"Key": "warehouse/silver/txn/data/orphan.parquet", "Size": 1 * GB},
            {"Key": "warehouse/silver/txn/metadata/snap-1.avro", "Size": 1 * GB},
        ],
        "lb-gold": [
            {"Key": "warehouse/gold/daily/data/a.parquet", "Size": 2 * GB},
            {"Key": "scoring/run-1/recall.parquet", "Size": 500},
            *extra_gold,
        ],
    }


class _Sql:
    """Trino answers: SHOW CREATE TABLE, $files sums, $all_entries sums."""

    current = {"bronze.raw": 4 * GB, "silver.txn": 6 * GB, "gold.daily": 2 * GB}
    retained = {"bronze.raw": 0, "silver.txn": 2 * GB, "gold.daily": 0}
    outside = {"bronze.raw": 0, "silver.txn": 0, "gold.daily": 0}
    snapshots = 5

    def __init__(self):
        self.calls: list[str] = []

    def __call__(self, sql: str) -> str:
        self.calls.append(sql)
        if "NOT LIKE" in sql:
            for fq in LOCATIONS:
                short = fq.split(".", 1)[1]
                schema, tbl = short.split(".")
                if f'"{tbl}$files"' in sql and schema in sql:
                    return f'"{self.outside[short]}"' if self.outside[short] else '"NULL"'
        if '$snapshots"' in sql:
            return f'"{self.snapshots}"'
        for fq, loc in LOCATIONS.items():
            short = fq.split(".", 1)[1]
            if sql == f"SHOW CREATE TABLE {fq}":
                return f"CREATE TABLE {fq} (\n x int\n)\nWITH (\n location = '{loc}'\n)"
            schema, tbl = short.split(".")
            if "$all_entries" in sql and f'"{tbl}$all_entries"' in sql and schema in sql:
                return f'"{self.retained[short]}"'
            if sql.endswith(f'lakehouse.{schema}."{tbl}$files"'):
                return f'"{self.current[short]}"'
        raise RuntimeError(f"unexpected SQL {sql}")


def _measure(objects=None, sql=None, **kw):
    objects = objects or _objects()
    listed: list[str] = []

    def list_objects(bucket):
        listed.append(bucket)
        return objects[bucket]

    out = sm.measure(
        buckets=BUCKETS,
        tables_by_layer=TABLES,
        catalog="lakehouse",
        engine=kw.pop("engine", "trino"),
        table_format=kw.pop("table_format", "iceberg"),
        datagen_prefix="customer/interactions",
        list_objects=list_objects,
        run_sql=sql if sql is not None else _Sql(),
        **kw,
    )
    return out, listed


def _row(out, table):
    return next(t for t in out["tables"] if t["table"] == table)


def _bucket(out, name):
    return next(b for b in out["buckets"] if b["bucket"] == name)


def _unattributed(out):
    return {b["bucket"]: b["unattributed_bytes"] for b in out["buckets"]}


def _listing_errors(out):
    return {b["bucket"]: b["listing_error"] for b in out["buckets"] if b["listing_error"]}


def test_known_multiple_fixture():
    out, _ = _measure(maintenance_id="m2", orphan_removal_ran=True)
    silver = _row(out, "silver.txn")
    assert silver["physical_bytes"] == 10 * GB
    assert silver["current_bytes"] == 6 * GB
    assert silver["retained_bytes"] == 2 * GB
    assert silver["metadata_bytes"] == 1 * GB
    assert silver["other_bytes"] == 1 * GB
    assert silver["other_label"]
    # Without orphan removal the unreferenced bytes carry no floor label.
    plain = _row(_measure(maintenance_id="m2")[0], "silver.txn")
    assert plain["other_bytes"] == silver["other_bytes"]
    assert "other_label" not in plain
    assert silver["multiple"] == pytest.approx(10 / 6, abs=1e-4)
    assert _row(out, "gold.daily")["multiple"] == 1.0
    assert _row(out, "bronze.raw")["multiple"] == pytest.approx(5 / 4)
    assert out["layers"]["silver"]["multiple"] == pytest.approx(10 / 6, abs=1e-4)
    assert out["total"]["physical_bytes"] == 17 * GB
    assert out["total"]["current_bytes"] == 12 * GB
    assert out["total"]["multiple"] == pytest.approx(17 / 12, abs=1e-4)
    # Raw corpus files are physical only and outside the total.
    assert out["raw_files"] == {"bronze": 10 * GB}
    assert out["excluded"]["datagen markers (_corpus/)"] == 1000
    assert out["excluded"]["datagen manifest (manifest/)"] == 2000
    assert out["excluded"]["stream checkpoints"] == 300
    assert out["excluded"]["scoring outputs (scoring/)"] == 500
    assert out["policy"] == "m2"


@pytest.mark.parametrize(
    ("extra_key", "size", "checkpoint_base", "base_checkpoint_base", "label"),
    [
        (
            "_ml_loop/gold/customer_features/data/x.parquet",
            5 * GB,
            None,
            None,
            "ML loop (<gold>/_ml_loop/)",
        ),
        # A moved checkpoint_base keeps the stream state and the drain marker excluded.
        ("streams/gold-refresh/_lb_stop", 3 * GB, "streams", "streams", "stream checkpoints"),
        # A base over the table locations excludes only its stream directories.
        ("warehouse/gold-refresh/offsets/0", 1 * GB, "warehouse", None, "stream checkpoints"),
    ],
    ids=["ml-loop", "moved-checkpoint-base", "checkpoint-base-over-table-location"],
)
def test_extra_object_is_excluded_not_attributed(
    extra_key, size, checkpoint_base, base_checkpoint_base, label
):
    extra = [{"Key": extra_key, "Size": size}]
    base_kw = {"checkpoint_base": base_checkpoint_base} if base_checkpoint_base else {}
    kw = {"checkpoint_base": checkpoint_base} if checkpoint_base else {}
    base, _ = _measure(**base_kw)
    out, _ = _measure(objects=_objects(extra_gold=extra), **kw)
    assert out["excluded"][label] == (base["excluded"].get(label) or 0) + size
    assert out["total"] == base["total"]
    assert out["layers"] == base["layers"]
    assert _unattributed(out) == _unattributed(base)


def test_pvcs_named_not_listed():
    out, listed = _measure()
    assert out["excluded"]["executor scratch PVCs"] is None
    assert out["excluded"]["dependency server PVC (lb-deps)"] is None
    assert listed == ["lb-bronze", "lb-silver", "lb-gold"]


def test_trino_without_all_entries_is_not_separable():
    class _NoEntries(_Sql):
        def __call__(self, sql):
            if "$all_entries" in sql:
                raise RuntimeError("Table does not exist")
            return super().__call__(sql)

    out, _ = _measure(sql=_NoEntries())
    silver = _row(out, "silver.txn")
    assert silver["separable"] is False and silver["retained_bytes"] is None
    assert silver["other_bytes"] == 3 * GB  # retained and unreferenced together
    assert "not separable" in silver["other_label"]


def test_table_the_catalog_does_not_know():
    class _NoBronze(_Sql):
        def __call__(self, sql):
            if "bronze.raw" in sql:
                raise RuntimeError("Table not found")
            return super().__call__(sql)

    out, _ = _measure(sql=_NoBronze())
    assert _row(out, "bronze.raw")["not_measured"] == "not in the catalog (RuntimeError)"
    assert "bronze" not in out["layers"]
    # Its objects are not attributed to any table.
    assert _bucket(out, "lb-bronze")["unattributed_bytes"] == 5 * GB


def test_foreign_location_is_never_listed():
    class _Foreign(_Sql):
        def __call__(self, sql):
            if sql == "SHOW CREATE TABLE lakehouse.gold.daily":
                return "WITH ( location = 's3a://someone-else/gold/daily' )"
            return super().__call__(sql)

    out, listed = _measure(sql=_Foreign())
    assert _row(out, "gold.daily")["not_measured"] == "location outside this deployment's buckets"
    assert "someone-else" not in listed


def test_no_sql_engine_records_physical_only():
    objects = _objects()
    out = sm.measure(
        buckets=BUCKETS,
        tables_by_layer=TABLES,
        catalog=None,
        engine=None,
        table_format="iceberg",
        datagen_prefix="customer/interactions",
        list_objects=lambda b: objects[b],
        run_sql=None,
        not_measured="no SQL engine in this recipe can read table metadata",
    )
    assert out["not_measured"] == "no SQL engine in this recipe can read table metadata"
    assert out["tables"] == [] and out["total"] == {}
    assert _bucket(out, "lb-silver")["physical_bytes"] == 10 * GB
    assert [b["bucket"] for b in out["buckets"]] == ["lb-bronze", "lb-silver", "lb-gold"]


def _thrift_sql(table_format):
    """Spark Thrift answers (beeline tables) for the three fixture tables."""
    current = {"raw": 4 * GB, "txn": 6 * GB, "daily": 2 * GB}
    retained = {"raw": 0, "txn": 2 * GB, "daily": 0}

    def table(header, value):
        return f"+------------+\n| {header} |\n+------------+\n| {value} |\n"

    def run(sql):
        name = next((t for t in current if f".{t}" in sql), None)
        if sql.startswith("DESCRIBE TABLE EXTENDED"):
            loc = LOCATIONS[f"lakehouse.{sql.rsplit('.', 2)[1]}.{name}"]
            return f"| col_name | data_type |\n| Location | {loc} |\n"
        if sql.startswith("DESCRIBE DETAIL"):
            return f"| format | sizeInBytes | numFiles |\n| delta | {current[name]} | 3 |\n"
        if "NOT LIKE" in sql:
            return table("sum(file_size_in_bytes)", "NULL")
        if "LEFT ANTI JOIN" in sql:
            return table("sum(file_size_in_bytes)", retained[name])
        if sql.startswith("SELECT sum(file_size_in_bytes)") and sql.endswith(f"{name}.files"):
            return table("sum(file_size_in_bytes)", current[name])
        raise RuntimeError(f"unexpected SQL {sql}")

    return run


def test_spark_thrift_iceberg_measures_bytes():
    out, _ = _measure(sql=_thrift_sql("iceberg"), engine="spark-thrift")
    silver = _row(out, "silver.txn")
    assert silver["current_bytes"] == 6 * GB
    assert silver["retained_bytes"] == 2 * GB
    assert silver["other_bytes"] == 1 * GB
    assert silver["multiple"] == pytest.approx(10 / 6, abs=1e-4)
    assert out["total"]["multiple"] == pytest.approx(17 / 12, abs=1e-4)


def test_spark_thrift_delta_reads_the_size_but_cannot_separate_retained():
    out, _ = _measure(sql=_thrift_sql("delta"), engine="spark-thrift", table_format="delta")
    silver = _row(out, "silver.txn")
    assert silver["current_bytes"] == 6 * GB
    assert silver["separable"] is False and silver["retained_bytes"] is None
    assert silver["multiple"] == pytest.approx(10 / 6, abs=1e-4)


def test_listing_error_is_recorded_not_raised():
    def boom(bucket):
        raise OSError("connection reset")

    out = sm.measure(
        buckets=BUCKETS,
        tables_by_layer=TABLES,
        catalog="lakehouse",
        engine="trino",
        table_format="iceberg",
        datagen_prefix="customer/interactions",
        list_objects=boom,
        run_sql=_Sql(),
    )
    assert _listing_errors(out) == dict.fromkeys(BUCKETS.values(), "OSError")
    assert all(b["physical_bytes"] is None for b in out["buckets"])


def test_orphan_removal_ran_from_outcomes():
    outcomes = [
        {
            "kind": "expire",
            "operations": [{"operation": "remove_orphan_files", "succeeded": 2}],
        }
    ]
    assert sm.orphan_removal_ran(outcomes) is True
    assert sm.orphan_removal_ran([]) is False
    assert sm.orphan_removal_ran(None) is False


def test_shared_bucket_is_counted_once():
    one = {"bronze": "lb-all", "silver": "lb-all", "gold": "lb-all"}
    objects = [
        {"Key": "warehouse/silver/txn/data/a.parquet", "Size": 6 * GB},
        {"Key": "scoring/run-1/recall.parquet", "Size": 500},
        {"Key": "customer/interactions/_corpus/c000.json", "Size": 100},
    ]

    class _OneBucket(_Sql):
        def __call__(self, sql):
            if sql.startswith("SHOW CREATE TABLE"):
                return (
                    super()
                    .__call__(sql)
                    .replace("lb-silver", "lb-all")
                    .replace("lb-bronze", "lb-all")
                    .replace("lb-gold", "lb-all")
                )
            return super().__call__(sql)

    listed = []

    def list_objects(bucket):
        listed.append(bucket)
        return objects

    out = sm.measure(
        buckets=one,
        tables_by_layer={"silver": ["silver.txn"]},
        catalog="lakehouse",
        engine="trino",
        table_format="iceberg",
        datagen_prefix="customer/interactions",
        list_objects=list_objects,
        run_sql=_OneBucket(),
    )
    assert listed == ["lb-all"]
    assert _row(out, "silver.txn")["physical_bytes"] == 6 * GB
    # Exclusions of every layer the bucket serves still apply.
    assert out["excluded"]["scoring outputs (scoring/)"] == 500
    assert out["excluded"]["datagen markers (_corpus/)"] == 100
    # One bucket entry for the three layers: its bytes are counted once.
    assert out["buckets"] == [
        {
            "bucket": "lb-all",
            "layers": ["bronze", "silver", "gold"],
            "physical_bytes": 6 * GB + 600,
            "unattributed_bytes": 0.0,
            "listing_error": None,
        }
    ]


def test_files_registered_outside_the_location_are_not_measured():
    """AML batch bronze registers its raw files in place (add_files): a
    multiple over a location that holds none of them would be meaningless."""

    class _InPlace(_Sql):
        outside = {"bronze.raw": 8 * GB, "silver.txn": 0, "gold.daily": 0}

    out, _ = _measure(sql=_InPlace())
    bronze = _row(out, "bronze.raw")
    assert bronze["not_measured"].startswith("data files registered in place outside")
    assert "bronze" not in out["layers"]
    assert out["total"]["physical_bytes"] == 12 * GB


def test_negative_other_is_not_measured():
    class _Overcount(_Sql):
        current = {"bronze.raw": 4 * GB, "silver.txn": 20 * GB, "gold.daily": 2 * GB}

    out, _ = _measure(sql=_Overcount())
    assert "below the bytes the table references" in _row(out, "silver.txn")["not_measured"]
    assert "silver" not in out["layers"]


def test_delta_on_trino_says_why():
    out, _ = _measure(table_format="delta")
    assert _row(out, "silver.txn")["not_measured"] == (
        "current data size is not readable for delta on trino"
    )
    assert out["total"] == {}


def test_c360_batch_bronze_is_raw_files():
    out, _ = _measure(raw_layers=("bronze",))
    assert _row(out, "bronze.raw")["not_measured"].startswith("raw files")
    assert "bronze" not in out["layers"]


def test_too_many_snapshots_skip_the_retained_read():
    class _Many(_Sql):
        snapshots = sm.TRINO_ALL_ENTRIES_MAX_SNAPSHOTS + 1

    sql = _Many()
    out, _ = _measure(sql=sql)
    assert not any("$all_entries" in c for c in sql.calls)
    assert _row(out, "silver.txn")["separable"] is False


def test_time_budget_stops_further_statements():
    ticks = iter(range(0, 10_000, 100))
    out, _ = _measure(clock=lambda: float(next(ticks)), budget_seconds=250)
    assert out["budget_spent"].startswith("the 250 s budget ran out")
    assert any("time budget" in (t.get("not_measured") or "") for t in out["tables"])


def test_partial_listing_yields_no_multiple():
    """A listing that fails partway leaves the bucket's tables not measured
    and keeps none of its partial bytes."""
    objects = _objects()

    def flaky(bucket):
        for n, obj in enumerate(objects[bucket]):
            if bucket == "lb-silver" and n == 2:
                raise OSError("connection reset")
            yield obj

    out = sm.measure(
        buckets=BUCKETS,
        tables_by_layer=TABLES,
        catalog="lakehouse",
        engine="trino",
        table_format="iceberg",
        datagen_prefix="customer/interactions",
        list_objects=flaky,
        run_sql=_Sql(),
    )
    silver = _row(out, "silver.txn")
    assert silver["not_measured"] == "the listing of lb-silver failed (OSError)"
    assert "multiple" not in silver
    assert _listing_errors(out) == {"lb-silver": "OSError"}
    assert _bucket(out, "lb-silver")["physical_bytes"] is None
    assert out["total"]["tables_measured"] == 2 and out["total"]["tables"] == 3


def test_budget_bounds_the_listing():
    ticks = iter([0.0] + [1000.0] * 100)
    out, _ = _measure(clock=lambda: next(ticks), budget_seconds=600)
    assert _listing_errors(out)["lb-bronze"] == "TimeoutError"


def test_stage_only_run_is_not_measured():
    """run --stage ran one layer: the other layers' tables are an earlier
    run's, so nothing is listed or queried and the record says why."""

    class Metrics:
        stage_only = "silver-build"

    out = sm.measure_run(object(), None, object(), Metrics())
    assert out["not_measured"] == "a run --stage run: the other layers are not this run's"


def _partial_block():
    """A block with every per-bucket figure set: physical bytes, unattributed
    bytes in silver (a stray key under no table) and a failed gold listing."""
    objects = _objects()
    objects["lb-silver"] = [*objects["lb-silver"], {"Key": "tmp/stray.parquet", "Size": 1 * GB}]

    def list_objects(bucket):
        if bucket == "lb-gold":
            raise OSError("connection reset")
        return objects[bucket]

    return sm.measure(
        buckets=BUCKETS,
        tables_by_layer=TABLES,
        catalog="lakehouse",
        engine="trino",
        table_format="iceberg",
        datagen_prefix="customer/interactions",
        list_objects=list_objects,
        run_sql=_Sql(),
        maintenance_id="m2-2026-09-26",
    )


def _keys_of(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from _keys_of(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _keys_of(v)


def test_no_bucket_name_is_a_key_in_the_block():
    block = _partial_block()
    assert not set(BUCKETS.values()) & set(_keys_of(block))
    assert block["buckets"] == [
        {
            "bucket": "lb-bronze",
            "layers": ["bronze"],
            "physical_bytes": 10 * GB + 1000 + 2000 + 300 + 5 * GB,
            "unattributed_bytes": 0.0,
            "listing_error": None,
        },
        {
            "bucket": "lb-silver",
            "layers": ["silver"],
            "physical_bytes": 11 * GB,
            "unattributed_bytes": 1 * GB,
            "listing_error": None,
        },
        {
            "bucket": "lb-gold",
            "layers": ["gold"],
            "physical_bytes": None,
            "unattributed_bytes": None,
            "listing_error": "OSError",
        },
    ]


class _InPlace(_Sql):
    outside = {"bronze.raw": 8 * GB, "silver.txn": 0, "gold.daily": 0}


def _known_block():
    return _measure(maintenance_id="m2-2026-09-26", orphan_removal_ran=True)[0]


def _in_place_block():
    block, _ = _measure(sql=_InPlace())
    block["budget_spent"] = "the 600 s budget ran out; later tables not measured"
    return block


def _not_measured_block():
    return {"not_measured": "the run did not pass"}


@pytest.mark.parametrize(
    ("block", "present", "absent"),
    [
        (
            _known_block,
            [
                "1.67x",
                "1.42x",
                "Raw datagen files in bronze: 10.00 GiB, physical only, outside the total.",
            ],
            [],
        ),
        (
            _in_place_block,
            ["Total (2 of 3 tables)", "Time budget: the 600 s budget ran out"],
            [],
        ),
        (_not_measured_block, ["Not measured: the run did not pass."], []),
        (
            _partial_block,
            [
                "Unattributed in lb-silver: 1.00 GiB.",
                "Listing of lb-gold failed (OSError); its tables are not measured.",
            ],
            ["Unattributed in lb-bronze"],
        ),
    ],
    ids=["known-multiple", "in-place-and-budget", "failed-run", "unattributed-and-failed-listing"],
)
def test_report_section_agrees_with_the_record(block, present, absent):
    """The record block renders under its heading, every multiple and size on
    the page agrees with the record, and the labels a reader needs are there."""
    from tests.fixtures.report_consistency_helpers import _plain_text, _render_dict, mismatches
    from tests.fixtures.stored_records import load_record

    record = load_record("5105a0")
    record["storage_multiple"] = block()
    html = _render_dict(record)
    text = _plain_text(html)
    assert "<h2>Storage multiple</h2>" in html
    for label in present:
        assert label in text
    for label in absent:
        assert label not in text
    assert mismatches(record, html) == []
