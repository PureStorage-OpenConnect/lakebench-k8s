"""Reproduce (Financial) -- reproduce one batch alert from what gold read.

Input (``--input``, written by ``lakebench financial reproduce``): the run
id and the snapshots its gold-finalize read, with their fingerprints
(``financial_scoring.read_snapshots`` of the run record): silver
transactions, silver entities and the versions table that decides which
micro-batches are sealed.

1. Look up the alert in gold.alerts. Missing, or written by another run:
   ``not_found``.
2. Read each table at its recorded snapshot (``basis: recorded``). When a
   snapshot is gone (expired), read the current table if its fingerprint
   over every column (``common.frame_fingerprint``) equals the recorded one
   (``basis: equivalent``: compaction and expiry keep content, and the
   batch stamping columns are hashed too); otherwise ``snapshot_gone``.
3. Filter the transactions to the batches sealed in the versions table as
   gold saw it (``sealed_txns_filter_at`` at the recorded versions snapshot,
   or the current table when that is equivalent).
4. Run the alert's rule with the parameters gold used
   (``detection_rules.rule_params``). A rule that declines to run:
   ``rule_skipped``.
5. Match the reproduced alerts on (rule_id, entity_id, alert_ts): exactly
   one match with the same set of related transactions is ``reproduced``;
   none, several, or a different set is ``mismatch`` with the size of the
   symmetric difference.

Writes ``--output`` (``scoring/reproduce/<alert_id>/result.json``) and exits
0 for every determined outcome; a non-zero exit is a crash. Writes no
table. Never feeds a temp view over a table into a MERGE (LB-226: Iceberg
1.11 on Spark 4.1 fails that plan).
"""

from __future__ import annotations

import argparse
import json
import sys

from common import env, frame_fingerprint, log, sealed_txns_filter, sealed_txns_filter_at
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_distinct, array_sort, col, lit, unix_micros

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_BATCH_VERSIONS = env("LB_FINANCIAL_SILVER_BATCH_VERSIONS", "silver.silver_batch_versions")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")

#: Outcomes the job determines (each exits 0).
OUTCOMES = ("reproduced", "not_found", "snapshot_gone", "mismatch", "rule_skipped")

#: What a rule reads that the run record does not pin: W5/W6 read the bronze
#: watchlist as it is now, and W1 its vertex cap from the current config.
NOT_PINNED = ("bronze watchlist (W5, W6)", "W1 vertex cap (current config)")

# Whitelist of alert_id characters. UUIDs and short prefixes with digits,
# dashes, and lowercase letters cover every alert we produce. Anything else
# is either a bug or an injection attempt; refuse rather than interpolate.
_ALERT_ID_ALLOWED = set("0123456789abcdefABCDEF-_")


def _validate_alert_id(alert_id: str) -> str:
    if not alert_id or len(alert_id) > 128:
        raise SystemExit(f"alert_id must be 1..128 chars; got {len(alert_id) if alert_id else 0}")
    bad = [c for c in alert_id if c not in _ALERT_ID_ALLOWED]
    if bad:
        raise SystemExit(
            f"alert_id contains disallowed characters {sorted(set(bad))}; "
            "expected hex + dash + underscore only."
        )
    return alert_id


def _read_text(spark, uri: str) -> str:
    rows = spark.read.text(uri, wholetext=True).collect()
    if not rows:
        raise SystemExit(f"{uri} is empty")
    return rows[0][0]


def _write_text(spark, uri: str, text: str) -> None:
    """One object through the configured Hadoop file system (S3A)."""
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(uri), hconf)
    stream = fs.create(jvm.org.apache.hadoop.fs.Path(uri), True)
    stream.write(bytearray(text, "utf-8"))
    stream.close()


def _tables() -> tuple[str, str, str]:
    return (SILVER_TXNS, SILVER_ENTITIES, SILVER_BATCH_VERSIONS)


def _snapshot_present(spark, fq: str, snapshot: int) -> bool:
    rows = spark.sql(f"SELECT 1 FROM {fq}.snapshots WHERE snapshot_id = {int(snapshot)}").collect()
    return bool(rows)


def read_recorded(spark, table: str, entry: dict | None) -> tuple:
    """``(frame, basis, reason)`` for *table* as gold read it: the recorded
    snapshot when it is still there, else the current table when its
    fingerprint over every column equals the recorded one. ``frame`` is None
    with the reason when neither holds."""
    fq = f"{CATALOG}.{table}"
    if not entry:
        return None, None, f"{table}: no recorded snapshot"
    snapshot = entry.get("snapshot")
    if isinstance(snapshot, bool) or not isinstance(snapshot, int):
        return None, None, f"{table}: gold read no known snapshot ({snapshot})"
    if _snapshot_present(spark, fq, snapshot):
        return spark.sql(f"SELECT * FROM {fq} VERSION AS OF {snapshot}"), "recorded", None
    want = (entry.get("rows"), entry.get("fp"), entry.get("cols_sha"))
    if None in want:
        return None, None, f"{table}: snapshot {snapshot} expired and no fingerprint was recorded"
    # The current snapshot, pinned: the frame fingerprinted is the frame read.
    now = spark.sql(
        f"SELECT snapshot_id FROM {fq}.history WHERE is_current_ancestor "
        "ORDER BY made_current_at DESC LIMIT 1"
    ).collect()
    if not now:
        return None, None, f"{table}: snapshot {snapshot} expired and the table has no snapshot"
    current = spark.sql(f"SELECT * FROM {fq} VERSION AS OF {int(now[0][0])}")
    rows, fp, cols_sha = frame_fingerprint(current, current.columns)
    if (int(rows), str(fp), str(cols_sha)) == (int(want[0]), str(want[1]), str(want[2])):
        return current, "equivalent", None
    return (
        None,
        None,
        f"{table}: snapshot {snapshot} expired and the current table's content differs "
        f"(rows {rows} against {want[0]})",
    )


def match_alert(original_txns, reproduced_rows) -> tuple[str, int, int]:
    """``(outcome, matched, diff_size)``: *reproduced_rows* are the reproduced
    alerts with the original's (rule_id, entity_id, alert_ts), each a list
    of related transaction ids. One match with an equal set reproduces it;
    ``diff_size`` is the smallest symmetric difference over the matches (the
    original's size when nothing matched)."""
    want = set(original_txns or [])
    sets = [set(r or []) for r in reproduced_rows]
    if not sets:
        return "mismatch", 0, len(want)
    diff = min(len(want ^ s) for s in sets)
    if len(sets) == 1 and diff == 0:
        return "reproduced", 1, 0
    return "mismatch", len(sets), diff


def reproduce(spark, alert_id: str, inputs: dict) -> dict:
    """The result record for *alert_id* (``outcome`` one of OUTCOMES)."""
    run_id = str(inputs.get("run_id") or "")
    nonce = inputs.get("nonce")
    by_table = {
        e.get("table"): e for e in inputs.get("read_snapshots") or [] if isinstance(e, dict)
    }
    result: dict = {
        "alert_id": alert_id,
        "run_id": run_id,
        "outcome": None,
        "basis": None,
        "rule_id": None,
        "matched": None,
        "diff_size": None,
        "snapshot_ids": {t: (by_table.get(t) or {}).get("snapshot") for t in _tables()},
        "reason": None,
        # The CLI's token for this reproduction: a result without it is not
        # this one's.
        "nonce": nonce,
        # Inputs the rule reads that the run did not record: the basis
        # covers the three silver tables only.
        "not_pinned": list(NOT_PINNED),
    }

    # alert_ts compared as epoch microseconds: a timestamp collected to
    # Python and sent back can move by the driver's zone rules.
    rows = spark.sql(
        f"SELECT *, unix_micros(alert_ts) AS _alert_us FROM {CATALOG}.{GOLD_ALERTS} "
        f"WHERE alert_id = '{alert_id}' LIMIT 2"
    ).collect()
    if not rows:
        return {**result, "outcome": "not_found", "reason": "no such alert in gold.alerts"}
    alert = rows[0]
    result["rule_id"] = alert["rule_id"]
    if str(alert["run_id"]) != run_id:
        return {
            **result,
            "outcome": "not_found",
            "reason": f"alert belongs to run {alert['run_id']}; pass --run {alert['run_id']}",
        }

    frames: dict = {}
    bases: dict = {}
    for table in _tables():
        frame, basis, why = read_recorded(spark, table, by_table.get(table))
        if frame is None:
            return {**result, "outcome": "snapshot_gone", "reason": why}
        frames[table] = frame
        bases[table] = basis
    result["basis"] = "recorded" if all(b == "recorded" for b in bases.values()) else "equivalent"

    txns_raw = frames[SILVER_TXNS]
    if bases[SILVER_BATCH_VERSIONS] == "recorded":
        versions_snapshot = int(by_table[SILVER_BATCH_VERSIONS]["snapshot"])
        txns = sealed_txns_filter_at(
            spark, txns_raw, CATALOG, SILVER_BATCH_VERSIONS, versions_snapshot
        )
    else:
        # The current versions table holds what gold saw (equal content).
        txns = sealed_txns_filter(spark, txns_raw, CATALOG, SILVER_BATCH_VERSIONS)

    from detection_rules import (
        RULE_VERSION,
        RuleSkipped,
        cleanup_w1_checkpoints,
        get_rule,
        rule_params,
    )

    fn = get_rule(alert["rule_id"])
    if fn is None:
        return {**result, "outcome": "mismatch", "reason": f"unknown rule {alert['rule_id']}"}
    recorded_version = _field(alert, "rule_version")
    if recorded_version is not None and str(recorded_version) != RULE_VERSION:
        return {
            **result,
            "outcome": "mismatch",
            "reason": f"rule version {recorded_version} raised it; this code is {RULE_VERSION}",
        }
    try:
        reproduced = fn(txns, **rule_params(fn, run_id, frames[SILVER_ENTITIES]))
        same = reproduced.where(
            (col("rule_id") == alert["rule_id"])
            & (col("entity_id") == alert["entity_id"])
            & (unix_micros(col("alert_ts")) == lit(alert["_alert_us"]))
        ).select(array_sort(array_distinct(col("related_txn_ids"))).alias("txns"))
        matches = [r["txns"] for r in same.limit(10).collect()]
    except RuleSkipped as skip:
        return {**result, "outcome": "rule_skipped", "reason": f"{skip.reason}: {skip.detail}"}
    finally:
        # W1 checkpoints and W3/W17 path spill under the gold bucket would be
        # counted in the next run's measured gold size.
        cleanup_w1_checkpoints(spark)
    outcome, matched, diff = match_alert(alert["related_txn_ids"], matches)
    return {**result, "outcome": outcome, "matched": matched, "diff_size": diff}


def _field(row, name):
    """A Row field, or None when the table has no such column."""
    try:
        return row[name]
    except (KeyError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Reproduce one AML batch alert")
    parser.add_argument("--alert-id", required=True, help="alert_id to reproduce")
    parser.add_argument("--input", required=True, help="S3 URI of input.json")
    parser.add_argument("--output", required=True, help="S3 URI for result.json")
    args = parser.parse_args()
    alert_id = _validate_alert_id(args.alert_id)

    spark = SparkSession.builder.appName(f"lb-reproduce-financial-{alert_id[:12]}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    log("=" * 60)
    log(f"Reproducing alert {alert_id}")
    log("=" * 60)
    inputs = json.loads(_read_text(spark, args.input))
    result = reproduce(spark, alert_id, inputs)
    log(
        f"[reproduce] outcome={result['outcome']} basis={result['basis']} "
        f"rule={result['rule_id']} matched={result['matched']} diff={result['diff_size']}"
        + (f" reason={result['reason']}" if result.get("reason") else "")
    )
    _write_text(spark, args.output, json.dumps(result, default=str))
    log(f"Wrote {args.output}")
    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
