"""Executed: financial replay reads silver at its resolved snapshot on the
default Iceberg of Spark 4.x.

Replay read with ``spark.read.option("snapshot-id", ...)``, which Iceberg 1.11
removed, so on Spark 4.1.1 + Iceberg 1.11.0 it failed before any rule ran.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg")


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, "lakehouse", tmp_path_factory.mktemp("replay-snap-wh"))
    spark_session.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    return spark_session


def test_replay_main_runs_the_rule_on_the_resolved_older_snapshot(spark, load_script, monkeypatch):
    """AM-27: ``main()`` resolves the snapshot for ``--depth-months`` and
    hands the rule silver as of that snapshot, through the real read path.

    Two snapshots: rows 1 and 2, then row 3. The clock is set between the
    two commits, so depth 0 resolves to the first snapshot and the rule
    must see rows 1 and 2 only. With the removed ``snapshot-id`` read
    option in ``main()`` this raises on Iceberg 1.11 before the rule runs.
    """
    import sys
    import time
    from datetime import datetime, timedelta, timezone

    replay, rules = load_script("replay_financial", extra=("detection_rules",))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    txns = f"{replay.CATALOG}.{replay.SILVER_TXNS}"
    versions = f"{replay.CATALOG}.{replay.SILVER_BATCH_VERSIONS}"
    out = "lakehouse.gold.alerts_replay_am27"
    for t in (txns, versions, out):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    schema = "n bigint, _stream_id string, _batch_id bigint"
    spark.createDataFrame(
        [("batch", 0), ("batch", 1)], "stream_id string, batch_id bigint"
    ).writeTo(versions).create()
    spark.createDataFrame([(1, "batch", 0), (2, "batch", 0)], schema).writeTo(txns).create()
    # Epoch millis, not the collected TIMESTAMP: PySpark converts that to a
    # naive datetime in the host's local zone, not the session's UTC.
    first_id, first_ms = spark.sql(
        f"SELECT snapshot_id, unix_millis(committed_at) FROM {txns}.snapshots"
    ).collect()[0]
    # resolve_snapshot_id formats its target to whole seconds: put the
    # clock on the next whole second after the first commit and commit the
    # second snapshot after it.
    first_at = datetime.fromtimestamp(first_ms / 1000, timezone.utc)
    pivot = first_at.replace(microsecond=0) + timedelta(seconds=1)
    while datetime.now(timezone.utc) <= pivot + timedelta(milliseconds=200):
        time.sleep(0.1)
    spark.createDataFrame([(3, "batch", 1)], schema).writeTo(txns).append()
    snaps = spark.sql(f"SELECT snapshot_id FROM {txns}.snapshots").collect()
    assert len(snaps) == 2

    class _Clock(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001, ANN206
            return pivot.astimezone(tz) if tz else pivot.replace(tzinfo=None)

    monkeypatch.setattr(replay, "datetime", _Clock)

    seen: list[list[int]] = []

    def _capture_rule(txns_df, run_id):  # noqa: ANN001, ANN202
        seen.append(sorted(r.n for r in txns_df.select("n").collect()))
        return replay._empty_alerts_df(spark)

    monkeypatch.setattr(rules, "get_rule", lambda _rule: _capture_rule)
    monkeypatch.setattr(rules, "cleanup_w1_checkpoints", lambda _s: None)

    class _Builder:
        def appName(self, *_a, **_kw):  # noqa: N802, ANN202
            return self

        def getOrCreate(self):  # noqa: N802, ANN202
            return spark

    monkeypatch.setattr(replay.SparkSession, "builder", _Builder())
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["replay_financial.py", "--rule", "W2_structuring", "--depth-months", "0"]
        + ["--output-alerts", out],
    )

    replay.main()

    assert seen == [[1, 2]], f"rule saw {seen}, expected silver as of snapshot {first_id}"
    assert spark.table(out).count() == 0
