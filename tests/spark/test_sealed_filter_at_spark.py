"""AM-15a (AML-6, AML-10 helper): ``common.sealed_txns_filter_at`` reads the
versions table at one recorded snapshot and fails closed.

Each test builds its own tables: three batches of five transactions, of
which only the first two are sealed in the versions table, so batch 2 is a
partial batch (transactions with no versions row) that no path may return.
"""

from __future__ import annotations

import itertools
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]

CATALOG = "ice"
# The same warehouse layout with Iceberg's table cache off: each scan then
# reloads the table, so a current-state read sees a later commit on the next
# action, and only a snapshot-pinned read stays put.
NO_CACHE = "ice_nocache"
TXN_SCHEMA = "txn_id string, _stream_id string, _batch_id bigint, ingest_ts timestamp"
PARTIAL = ("S1", 2)
_N = itertools.count()


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    # The product leaves Iceberg's cache-enabled at its default.
    iceberg_catalog(spark_session, CATALOG, tmp_path_factory.mktemp("sealed-at-wh"))
    iceberg_catalog(
        spark_session, NO_CACHE, tmp_path_factory.mktemp("sealed-at-nc-wh"), cache_enabled=False
    )
    for cat in (CATALOG, NO_CACHE):
        spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.silver")
    return spark_session


def _fixture(spark, cat=CATALOG):
    """(txns frame, versions table name without catalog, versions snapshot
    id) with batches 0 and 1 sealed and batch 2 partial."""
    n = next(_N)
    txns_t, versions_t = f"silver.txns_{n}", f"silver.versions_{n}"
    spark.sql(
        f"CREATE TABLE {cat}.{txns_t} (txn_id STRING, _stream_id STRING, "
        "_batch_id BIGINT, ingest_ts TIMESTAMP) USING iceberg"
    )
    spark.sql(
        f"CREATE TABLE {cat}.{versions_t} (stream_id STRING NOT NULL, "
        "batch_id BIGINT NOT NULL, committed_at TIMESTAMP NOT NULL) USING iceberg"
    )
    t0 = datetime(2024, 6, 1, tzinfo=timezone.utc)
    rows = [(f"T{b}-{i}", "S1", b, t0) for b in range(3) for i in range(5)]
    spark.createDataFrame(rows, TXN_SCHEMA).writeTo(f"{cat}.{txns_t}").append()
    spark.sql(
        f"INSERT INTO {cat}.{versions_t} VALUES "
        "('S1', 0, current_timestamp()), ('S1', 1, current_timestamp())"
    )
    return spark.table(f"{cat}.{txns_t}"), versions_t, _snapshot(spark, versions_t, cat)


def _snapshot(spark, table, cat=CATALOG):
    return int(
        spark.sql(
            f"SELECT snapshot_id FROM {cat}.{table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()[0]
    )


def _seal_partial(spark, versions_t, cat=CATALOG):
    spark.sql(
        f"INSERT INTO {cat}.{versions_t} VALUES ('{PARTIAL[0]}', {PARTIAL[1]}, current_timestamp())"
    )


def _batches(df):
    """The (stream, batch) pairs in *df*, through a new derived frame each
    call, as each rule in a tick derives its own: a second action on the
    same Dataset would reuse its first physical plan and scan."""
    rows = df.select("_stream_id", "_batch_id").distinct().collect()
    return sorted((r["_stream_id"], r["_batch_id"]) for r in rows)


def test_same_rows_as_current_filter_without_later_commit(spark):
    from common import frame_fingerprint, sealed_txns_filter, sealed_txns_filter_at

    txns, versions_t, vsid = _fixture(spark)
    at = sealed_txns_filter_at(spark, txns, CATALOG, versions_t, vsid)
    current = sealed_txns_filter(spark, txns, CATALOG, versions_t)
    cols = txns.columns
    assert frame_fingerprint(at, cols) == frame_fingerprint(current, cols)
    assert _batches(at) == [("S1", 0), ("S1", 1)]


@pytest.mark.parametrize("cat", [CATALOG, NO_CACHE], ids=["cache-default", "cache-off"])
def test_seal_committed_between_actions_is_invisible(spark, cat, record_property):
    """A versions row committed after the recorded snapshot, between two
    actions on one frame, is seen by neither action (silent-corruption L2).
    Also records whether today's current-state filter, built before the
    commit, sees it on its second action (the d2 item 1 assumption)."""
    from common import sealed_txns_filter, sealed_txns_filter_at

    txns, versions_t, vsid = _fixture(spark, cat)
    at = sealed_txns_filter_at(spark, txns, cat, versions_t, vsid)
    current = sealed_txns_filter(spark, txns, cat, versions_t)
    first_at, first_current = _batches(at), _batches(current)
    _seal_partial(spark, versions_t, cat)
    second_at, second_current = _batches(at), _batches(current)

    assert first_at == second_at == [("S1", 0), ("S1", 1)]
    assert first_current == [("S1", 0), ("S1", 1)]
    sees = PARTIAL in second_current
    record_property("current_filter_sees_later_seal_on_second_action", sees)
    print(f"{cat}: current_filter_sees_later_seal_on_second_action={sees}")
    # A fresh call at the new snapshot does see it: the pin is the snapshot.
    newer = _snapshot(spark, versions_t, cat)
    assert PARTIAL in _batches(sealed_txns_filter_at(spark, txns, cat, versions_t, newer))


def test_filter_at_fails_closed(spark):
    """An int id that is not a snapshot of the versions table, and a dropped
    versions table, raise SealedFilterError inside the call, before any
    action, and no path returns the partial batch."""
    from common import SealedFilterError, sealed_txns_filter_at

    txns, versions_t, vsid = _fixture(spark)
    returned = {}

    def attempt(case, snapshot):
        try:
            returned[case] = sealed_txns_filter_at(spark, txns, CATALOG, versions_t, snapshot)
        except SealedFilterError:
            pass

    attempt("unknown id", vsid + 1)
    attempt("id 1", 1)
    spark.sql(f"DROP TABLE {CATALOG}.{versions_t}")
    attempt("dropped table", vsid)
    leaked = [case for case, df in returned.items() if PARTIAL in _batches(df)]
    assert not leaked, f"returned the partial batch: {leaked}"
    assert not returned, f"did not raise: {sorted(returned)}"
