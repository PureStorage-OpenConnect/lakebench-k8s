"""Executed: the time-travel job reads recorded snapshots back on Iceberg.

A transactions-like table takes three commits (two appends, then a
row-level DELETE, which Iceberg's copy-on-write default rewrites), each
recorded the way a gold-refresh tick records the snapshot it read: the
snapshot id and the summary's ``total-records`` and delete totals, from
metadata only. ``expire_snapshots`` then expires snapshots older than the
second commit, and ``time_travel_financial.time_travel`` runs both passes
over real files: the first snapshot reads ``expired``, the others
``verified`` (scan rows equal the recorded count, scan fingerprint equals
the hash pass). An altered hashes file gives ``mismatch``, and a Delta
table reads ``not_supported``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, "lakehouse", tmp_path_factory.mktemp("time-travel-wh"))
    spark_session.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    return spark_session


def _record(spark, fq, cycle):
    """The tick's time-travel record of the table's current snapshot, from
    the snapshot metadata (as gold_refresh_financial.snapshot_record)."""
    row = spark.sql(
        "SELECT snapshot_id, unix_micros(committed_at) AS us, summary['total-records'] AS n, "
        "summary['total-position-deletes'] AS pos, summary['total-equality-deletes'] AS eq "
        f"FROM {fq}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]
    # UTC from the epoch micros: a collected timestamp is converted to the
    # Python process's local time.
    stamp = datetime.fromtimestamp(row["us"] / 1e6, timezone.utc)
    return {
        "start": 0,
        "cycle": cycle,
        "table": fq.split(".", 1)[1],
        "snapshot": int(row["snapshot_id"]),
        "committed_at": stamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "total_records": int(row["n"]),
        # Iceberg writes the delete totals into every summary; a missing one
        # would turn the record into verified_hash_only, so it must be there.
        "pos_deletes": int(row["pos"]),
        "eq_deletes": int(row["eq"]),
        "count_source": "summary",
        "_committed": stamp,
    }


@pytest.fixture
def three_commits(spark):
    """(table, [record per commit]) after three commits and an expiry of
    every snapshot older than the second."""
    import time

    table = f"silver.tt_{datetime.now().strftime('%H%M%S%f')}"
    fq = f"lakehouse.{table}"
    spark.sql(
        f"CREATE TABLE {fq} (txn_id STRING, amount DECIMAL(18,2), _stream_id STRING, "
        "_batch_id BIGINT, ingest_ts TIMESTAMP, tags ARRAY<STRING>) USING iceberg"
    )
    records = []
    spark.sql(
        f"INSERT INTO {fq} VALUES ('a', 1.00, 's', 1, TIMESTAMP '2026-01-01 00:00:00', array('x')), "
        "('b', 2.00, 's', 1, TIMESTAMP '2026-01-01 00:00:01', NULL)"
    )
    records.append(_record(spark, fq, 1))
    time.sleep(1.1)
    spark.sql(
        f"INSERT INTO {fq} VALUES ('c', 3.00, 's', 2, TIMESTAMP '2026-01-01 00:00:02', array()), "
        "('d', 4.00, 's', 2, TIMESTAMP '2026-01-01 00:00:03', array('y', NULL))"
    )
    records.append(_record(spark, fq, 2))
    time.sleep(1.1)
    spark.sql(f"DELETE FROM {fq} WHERE txn_id = 'a'")
    records.append(_record(spark, fq, 3))
    cutoff = records[1]["_committed"].strftime("%Y-%m-%d %H:%M:%S.%f")
    spark.sql(
        f"CALL lakehouse.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{cutoff}', retain_last => 1)"
    )
    for r in records:
        r.pop("_committed")
    return table, records


@pytest.fixture
def mod(load_script, monkeypatch):
    m = load_script("time_travel_financial")
    monkeypatch.setattr(m, "CATALOG", "lakehouse")
    return m


def _inputs(records):
    return {"run_id": "run-tt", "nonce": "n-1", "records": records}


def test_expired_and_verified_on_a_real_table(spark, mod, three_commits, tmp_path):
    _table, records = three_commits
    assert records[2]["total_records"] == 3  # copy-on-write: live rows after the DELETE
    out = mod.time_travel(
        spark, _inputs(records), f"file://{tmp_path}/tt_hashes.json", mod.Budget(None)
    )
    states = [t["state"] for t in out["ticks"]]
    assert states == ["expired", "verified", "verified"], out
    assert [t["rows"] for t in out["ticks"]] == [None, 4, 3]
    assert all(t["fp_match"] and t["count_match"] for t in out["ticks"][1:])
    assert out["current"]["snapshot"] == records[2]["snapshot"]
    assert out["current"]["rows"] == 3 and out["current"]["read_s"] is not None
    assert out["incomplete"] is False
    doc = json.loads((tmp_path / "tt_hashes.json").read_text())
    assert doc["nonce"] == "n-1"
    assert sorted(h["snapshot"] for h in doc["hashes"]) == sorted(
        r["snapshot"] for r in records[1:]
    )


def test_an_altered_hashes_file_is_a_mismatch(spark, mod, three_commits, tmp_path, monkeypatch):
    _table, records = three_commits
    uri = f"file://{tmp_path}/tt_hashes.json"
    real = mod.hash_pass

    def alter(spark_, recs, uri_, nonce, budget):
        done = real(spark_, recs, uri_, nonce, budget)
        path = tmp_path / "tt_hashes.json"
        doc = json.loads(path.read_text())
        for h in doc["hashes"]:
            if h["snapshot"] == records[2]["snapshot"]:
                h["fp"] = str(int(h["fp"]) + 1)
        path.write_text(json.dumps(doc))
        # The local file system checks a .crc sidecar; an edit outside
        # Hadoop drops it, as an edit to the S3 object would carry none.
        (tmp_path / ".tt_hashes.json.crc").unlink(missing_ok=True)
        return done

    monkeypatch.setattr(mod, "hash_pass", alter)
    out = mod.time_travel(spark, _inputs(records), uri, mod.Budget(None))
    assert [t["state"] for t in out["ticks"]] == ["expired", "verified", "mismatch"]
    assert out["ticks"][2]["fp_match"] is False and out["ticks"][2]["count_match"] is True


def test_a_recorded_count_one_off_is_a_mismatch(spark, mod, three_commits, tmp_path):
    _table, records = three_commits
    records[1]["total_records"] += 1
    out = mod.time_travel(
        spark, _inputs(records), f"file://{tmp_path}/tt_hashes.json", mod.Budget(None)
    )
    assert out["ticks"][1]["state"] == "mismatch" and out["ticks"][1]["count_match"] is False


def test_a_delta_table_is_not_supported(spark, mod, tmp_path, monkeypatch):
    path = tmp_path / "delta_txns"
    spark.sql(f"CREATE TABLE default.tt_delta (txn_id STRING) USING delta LOCATION '{path}'")
    try:
        spark.sql("INSERT INTO default.tt_delta VALUES ('a')")
        monkeypatch.setattr(mod, "CATALOG", "spark_catalog")
        rec = {"start": 0, "cycle": 1, "table": "default.tt_delta", "snapshot": 0}
        out = mod.time_travel(
            spark, _inputs([rec]), f"file://{tmp_path}/tt_hashes.json", mod.Budget(None)
        )
        assert out["status"] == "not_supported"
        assert [t["state"] for t in out["ticks"]] == ["not_supported"]
        assert not (tmp_path / "tt_hashes.json").exists()
    finally:
        spark.sql("DROP TABLE IF EXISTS default.tt_delta")
