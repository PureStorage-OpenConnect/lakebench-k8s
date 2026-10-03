"""Executed: financial reproduce reads what gold read and reruns one rule.

Each case builds silver transactions, entities and the versions table on a
local Iceberg catalog, records the snapshots and their fingerprints as the
batch scorer does (``score_financial.read_snapshot_fingerprints``), writes the
alert the rule raised into gold.alerts, then changes the tables and calls
``reproduce_financial.reproduce``. The rule is a small test rule (one alert
per originator with two or more sealed transactions), patched in through
``detection_rules.get_rule``: the cases test the reads, the sealed filter and
the match, not a production rule's logic.
"""

from __future__ import annotations

import itertools
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg")

RUN = "20261003-120000-aaaaaa"
RULE = "W2_structuring"
_ids = itertools.count()


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, "lakehouse", tmp_path_factory.mktemp("reproduce-wh"))
    for ns in ("silver", "gold"):
        spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")
    return spark_session


@pytest.fixture
def case(spark, load_script, monkeypatch):
    """Fresh tables for one case, the loaded scripts, and helpers."""
    repro, rules, score, gold = load_script(
        "reproduce_financial",
        extra=("detection_rules", "score_financial", "gold_finalize_financial"),
    )
    n = next(_ids)
    names = {
        "SILVER_TXNS": f"silver.txns_{n}",
        "SILVER_ENTITIES": f"silver.entities_{n}",
        "SILVER_BATCH_VERSIONS": f"silver.versions_{n}",
        "GOLD_ALERTS": f"gold.alerts_{n}",
    }
    for attr, table in names.items():
        monkeypatch.setattr(repro, attr, table)
    monkeypatch.setattr(score, "CATALOG", "lakehouse")

    from pyspark.sql import functions as F

    def rule(txns, run_id, silver_entities=None):
        assert silver_entities is not None
        return (
            txns.groupBy("originator_id")
            .agg(
                F.count("*").alias("n"),
                F.max("txn_timestamp").alias("alert_ts"),
                F.collect_set("txn_id").alias("related_txn_ids"),
            )
            .where("n >= 2")
            .select(
                F.lit(RULE).alias("rule_id"),
                F.col("originator_id").alias("entity_id"),
                "alert_ts",
                "related_txn_ids",
                F.lit(run_id).alias("run_id"),
            )
        )

    monkeypatch.setattr(rules, "get_rule", lambda rule_id: rule if rule_id == RULE else None)

    t0 = datetime(2026, 1, 1, 12, 0, 0)

    def fq(key):
        return f"lakehouse.{names[key]}"

    def txn(i, originator, batch, minutes=0):
        return (f"t{i}", originator, t0 + timedelta(minutes=minutes), "batch", batch)

    def write_txns(rows, mode="append"):
        df = spark.createDataFrame(
            rows,
            "txn_id string, originator_id bigint, txn_timestamp timestamp, "
            "_stream_id string, _batch_id bigint",
        )
        if mode == "create":
            df.writeTo(fq("SILVER_TXNS")).create()
        else:
            df.writeTo(fq("SILVER_TXNS")).append()

    def seal(batches, mode="append"):
        df = spark.createDataFrame(
            [("batch", b) for b in batches], "stream_id string, batch_id bigint"
        )
        if mode == "create":
            df.writeTo(fq("SILVER_BATCH_VERSIONS")).create()
        else:
            df.writeTo(fq("SILVER_BATCH_VERSIONS")).append()

    def current(key):
        return spark.sql(
            f"SELECT snapshot_id FROM {fq(key)}.history WHERE is_current_ancestor "
            "ORDER BY made_current_at DESC LIMIT 1"
        ).collect()[0][0]

    def record():
        """What gold-finalize logs and the scorer fingerprints, as the run
        record holds it."""
        args = [
            f"{names[k]}={current(k)}:0"
            for k in ("SILVER_TXNS", "SILVER_ENTITIES", "SILVER_BATCH_VERSIONS")
        ]
        return {
            "run_id": RUN,
            "nonce": "n-1",
            "read_snapshots": score.read_snapshot_fingerprints(spark, args),
        }

    def raise_alert(alert_id, txns_ids):
        spark.createDataFrame(
            [(alert_id, RULE, 7, t0 + timedelta(minutes=1), txns_ids, RUN)],
            "alert_id string, rule_id string, entity_id bigint, alert_ts timestamp, "
            "related_txn_ids array<string>, run_id string",
        ).writeTo(fq("GOLD_ALERTS")).createOrReplace()

    def expire(key):
        spark.sql(
            f"CALL lakehouse.system.expire_snapshots(table => '{names[key]}', "
            f"older_than => TIMESTAMP '{(datetime.now(timezone.utc) + timedelta(seconds=5)):%Y-%m-%d %H:%M:%S}', "
            "retain_last => 1)"
        )

    def setup():
        spark.createDataFrame(
            [(7, True), (8, True)], "entity_id bigint, is_customer boolean"
        ).writeTo(fq("SILVER_ENTITIES")).create()
        write_txns([txn(1, 7, 0), txn(2, 7, 0, 1), txn(3, 8, 0)], mode="create")
        seal([0], mode="create")

    from types import SimpleNamespace

    for attr in ("SILVER_TXNS", "SILVER_ENTITIES", "SILVER_BATCH_VERSIONS"):
        monkeypatch.setattr(gold, attr, names[attr])
    monkeypatch.setattr(gold, "CATALOG", "lakehouse")

    return SimpleNamespace(
        gold=gold,
        rules=rules,
        rule=rule,
        repro=repro,
        names=names,
        fq=fq,
        txn=txn,
        write_txns=write_txns,
        seal=seal,
        record=record,
        raise_alert=raise_alert,
        expire=expire,
        setup=setup,
        current=current,
    )


def test_reproduced_from_the_recorded_snapshots(spark, case):
    case.setup()
    inputs = case.record()
    assert all(e["fp"] for e in inputs["read_snapshots"]), inputs
    case.raise_alert("a-1", ["t1", "t2"])
    # Later commits gold never read do not change the reproduction.
    case.write_txns([case.txn(9, 7, 1, 2)])
    case.seal([1])
    cleaned = []
    real_cleanup = case.rules.cleanup_w1_checkpoints
    case.rules.cleanup_w1_checkpoints = lambda sp: cleaned.append(1) or real_cleanup(sp)
    try:
        out = case.repro.reproduce(spark, "a-1", inputs)
    finally:
        case.rules.cleanup_w1_checkpoints = real_cleanup
    assert (out["outcome"], out["basis"], out["matched"], out["diff_size"]) == (
        "reproduced",
        "recorded",
        1,
        0,
    ), out
    assert out["nonce"] == "n-1" and out["not_pinned"]
    assert cleaned == [1]  # W1 checkpoints and path spill removed after the rule


def test_a_batch_sealed_after_the_recorded_versions_snapshot_stays_hidden(spark, case):
    """Batch 1's rows are in the recorded transactions snapshot, but the
    versions table sealed it only after gold read: the reproduction must not
    see them (the current versions table would)."""
    case.setup()
    case.write_txns([case.txn(4, 7, 1, 1)])  # committed, not sealed when gold read
    inputs = case.record()
    case.raise_alert("a-2", ["t1", "t2"])
    case.seal([1])
    out = case.repro.reproduce(spark, "a-2", inputs)
    assert out["outcome"] == "reproduced", out


def test_equivalent_after_a_rewrite_and_expiry(spark, case):
    case.setup()
    inputs = case.record()
    case.raise_alert("a-3", ["t1", "t2"])
    txns = case.fq("SILVER_TXNS")
    # A content-preserving rewrite (as compaction does), then expiry.
    spark.sql(f"INSERT OVERWRITE {txns} SELECT * FROM {txns}")
    case.expire("SILVER_TXNS")
    snaps = [r[0] for r in spark.sql(f"SELECT snapshot_id FROM {txns}.snapshots").collect()]
    assert inputs["read_snapshots"][0]["snapshot"] not in snaps
    out = case.repro.reproduce(spark, "a-3", inputs)
    assert (out["outcome"], out["basis"]) == ("reproduced", "equivalent"), out


def test_snapshot_gone_when_content_changed_and_expired(spark, case):
    case.setup()
    inputs = case.record()
    case.raise_alert("a-4", ["t1", "t2"])
    case.write_txns([case.txn(5, 8, 0, 3)])
    case.expire("SILVER_TXNS")
    out = case.repro.reproduce(spark, "a-4", inputs)
    assert out["outcome"] == "snapshot_gone" and "content differs" in out["reason"], out


def test_snapshot_gone_when_only_batch_stamping_changed(spark, case):
    """Business columns equal, _batch_id different: the stamping decides
    sealed visibility, so the content is not the one gold read."""
    case.setup()
    inputs = case.record()
    case.raise_alert("a-5", ["t1", "t2"])
    txns = case.fq("SILVER_TXNS")
    spark.sql(
        f"INSERT OVERWRITE {txns} SELECT txn_id, originator_id, txn_timestamp, _stream_id, "
        f"_batch_id + 100 FROM {txns}"
    )
    case.expire("SILVER_TXNS")
    out = case.repro.reproduce(spark, "a-5", inputs)
    assert out["outcome"] == "snapshot_gone", out


def test_mismatch_on_different_related_transactions(spark, case):
    case.setup()
    inputs = case.record()
    case.raise_alert("a-6", ["t1", "t2", "t3"])
    out = case.repro.reproduce(spark, "a-6", inputs)
    assert (out["outcome"], out["matched"], out["diff_size"]) == ("mismatch", 1, 1), out


def test_not_found_and_another_runs_alert(spark, case):
    case.setup()
    inputs = case.record()
    case.raise_alert("a-7", ["t1", "t2"])
    assert case.repro.reproduce(spark, "nope-1", inputs)["outcome"] == "not_found"
    other = case.repro.reproduce(spark, "a-7", {**inputs, "run_id": "20261003-000000-bbbbbb"})
    assert other["outcome"] == "not_found" and f"pass --run {RUN}" in other["reason"], other


def test_the_scorer_fingerprints_what_gold_read(spark, case):
    """score_financial.read_snapshot_fingerprints: one entry per snapshot
    with rows, fp and cols_sha; an unknown snapshot records an error."""
    case.setup()
    entries = case.record()["read_snapshots"]
    assert [e["table"] for e in entries] == [
        case.names["SILVER_TXNS"],
        case.names["SILVER_ENTITIES"],
        case.names["SILVER_BATCH_VERSIONS"],
    ]
    assert entries[0]["rows"] == 3 and entries[0]["cols_sha"]
    import score_financial

    (bad,) = score_financial.read_snapshot_fingerprints(spark, ["silver.x=unknown:null"])
    assert bad["fp"] is None and "unknown" in bad["error"]


def test_gold_logs_the_snapshots_it_reads(spark, case, monkeypatch):
    """gold_finalize_financial.log_read_snapshots on real Iceberg metadata:
    the current snapshot of each table and its summary record count, in the
    form metrics/read_snapshots.py parses."""
    from lakebench.metrics.read_snapshots import parse_read_snapshots

    case.setup()
    lines: list[str] = []
    monkeypatch.setattr(case.gold, "log", lines.append)
    case.gold.log_read_snapshots(spark)
    got = parse_read_snapshots("\n".join(lines))
    assert [g["table"] for g in got] == [
        case.names["SILVER_TXNS"],
        case.names["SILVER_ENTITIES"],
        case.names["SILVER_BATCH_VERSIONS"],
    ]
    assert got[0] == {
        "table": case.names["SILVER_TXNS"],
        "snapshot": case.current("SILVER_TXNS"),
        "total_records": 3,
    }
    assert got[1]["total_records"] == 2 and got[2]["total_records"] == 1


def test_reproduces_an_alert_gold_wrote(spark, case, monkeypatch):
    """The alert row comes from gold-finalize's own writer
    (run_detection_rules: the positional INSERT of ALERT_COLUMNS, a uuid
    alert_id, the Iceberg timestamp and array round trip), not a hand-made
    row, and reproduce finds and matches it. A rule version other than the
    running code's is a mismatch."""
    from pyspark.sql import functions as F

    gf, rules = case.gold, case.rules
    for ddl in (gf.DDL_ALERTS, gf.DDL_STATUS):
        spark.sql(ddl)
    case.setup()
    inputs = case.record()
    template = rules._empty_alerts_df(spark, "x")

    def toy(txns, run_id, silver_entities=None):
        grouped = case.rule(txns, run_id, silver_entities)
        cols = {
            "alert_id": F.expr("uuid()"),
            "rule_id": F.lit("WX_toy"),
            "rule_version": F.lit(rules.RULE_VERSION),
            "model_id": F.lit("m"),
            "model_version": F.lit("1"),
            "entity_id": F.col("entity_id"),
            "related_txn_ids": F.col("related_txn_ids"),
            "related_entity_ids": F.array(F.col("entity_id")),
            "alert_ts": F.col("alert_ts"),
            "alert_score": F.lit(0.5),
            "priority": F.lit("LOW"),
            "status": F.lit("OPEN"),
            "disposition": F.lit(None).cast("string"),
            "alert_type": F.lit("test"),
            "run_id": F.lit(run_id),
            "narrative": F.lit("n"),
            "evidence": F.lit(None).cast("map<string,string>"),
            "detected_ts": F.current_timestamp(),
            "reason_codes": F.array(F.lit("X_CODE")),
        }
        return grouped.select(*[cols[f.name].alias(f.name) for f in template.schema.fields])

    monkeypatch.setattr(rules, "get_rule", lambda rid: toy if rid == "WX_toy" else None)
    txns = gf._sealed_txns(spark, case.names["SILVER_TXNS"])
    gf.run_detection_rules(spark, txns, RUN, rules=("WX_toy",))
    written = spark.table("lakehouse.gold.alerts").where("rule_id = 'WX_toy'").collect()
    assert len(written) == 1, written
    monkeypatch.setattr(case.repro, "GOLD_ALERTS", "gold.alerts")
    out = case.repro.reproduce(spark, written[0]["alert_id"], inputs)
    assert (out["outcome"], out["basis"]) == ("reproduced", "recorded"), out

    monkeypatch.setattr(rules, "RULE_VERSION", "9.9.9")
    out = case.repro.reproduce(spark, written[0]["alert_id"], inputs)
    assert out["outcome"] == "mismatch" and "rule version" in out["reason"], out
