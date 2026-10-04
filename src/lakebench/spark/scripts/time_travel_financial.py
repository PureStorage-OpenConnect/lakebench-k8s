"""Time travel (Financial) -- re-read the snapshots a continuous run's ticks read.

Each continuous AML gold-refresh tick records the silver transactions
snapshot it read and that snapshot's metadata counts (the ``tt-record`` line,
``continuous.time_travel.ticks``). After the window, with the streams
stopped and no measured interval open, ``lakebench run --continuous``
submits this job with those records (``--input``, written by
``cli/_aml_post.run_time_travel``) and reads its result.

1. Hash pass: for each recorded snapshot still in the table's snapshots
   metadata, fingerprint the raw snapshot over every column of its schema
   (``common.frame_fingerprint``; no sealed filter) and write
   ``tt_hashes.json``. This is the order-independent hash computed for that
   snapshot after the window.
2. Read pass, the published measurement: re-read ``tt_hashes.json`` from
   storage (not from memory, so an altered file is seen), then for each
   record either report the snapshot ``expired`` (gone from the snapshots
   metadata) or time a full scan ``VERSION AS OF`` it with the same
   fingerprint and compare it with what the tick recorded (the summary
   count, when it is a live-row count) and with the hash pass. Equal on
   both is ``verified``; with no comparable count, ``verified_hash_only``;
   either differing is ``mismatch``. A scan that fails on a snapshot still
   listed is ``error``.
3. The same scan of the current snapshot is timed, for comparison only.

Writes ``time_travel.json`` beside the hashes and exits 0 for every
determined outcome; a non-zero exit is a crash. A budget (``--budget-s``)
stops starting new scans once spent: the records left read ``not_read``
and the result says ``incomplete``. A Delta table is refused: every record
reads ``not_supported`` (AML on Delta is refused by the config today).
Writes no table and creates no tag, branch or other snapshot reference.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from common import env, frame_fingerprint, log, one_line

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")

#: Record states this job writes. The CLI turns ``expired`` into an explained
#: expiry or ``missing_unexplained`` (it holds the maintenance rounds).
VERIFIED = "verified"
VERIFIED_HASH_ONLY = "verified_hash_only"
EXPIRED = "expired"
MISMATCH = "mismatch"
ERROR = "error"
NOT_READ = "not_read"
NOT_SUPPORTED = "not_supported"


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
    try:
        stream.write(bytearray(text, "utf-8"))
    finally:
        stream.close()


def is_snapshot(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def table_provider(spark, fq: str) -> str | None:
    """The table's provider (``iceberg``, ``delta``) from ``DESCRIBE TABLE
    EXTENDED``, lower case; None when it does not say."""
    for row in spark.sql(f"DESCRIBE TABLE EXTENDED {fq}").collect():
        if str(row["col_name"]).strip().lower() == "provider":
            return str(row["data_type"]).strip().lower() or None
    return None


def live_snapshots(spark, fq: str) -> set[int]:
    """Snapshot ids in the table's snapshots metadata (metadata only)."""
    return {
        int(r["snapshot_id"])
        for r in spark.sql(f"SELECT snapshot_id FROM {fq}.snapshots").collect()
    }


def current_snapshot(spark, fq: str) -> int | None:
    """The table's current snapshot id, or None (no snapshot)."""
    rows = spark.sql(
        f"SELECT snapshot_id FROM {fq}.history WHERE is_current_ancestor "
        "ORDER BY made_current_at DESC LIMIT 1"
    ).collect()
    return int(rows[0]["snapshot_id"]) if rows else None


def fingerprint_at(spark, fq: str, sid: int | None) -> dict:
    """``{rows, fp, cols_sha}`` of the raw table at snapshot *sid* (the
    current table for None), over every column of that snapshot's schema,
    including the batch stamping columns. One full scan."""
    df = (
        spark.table(fq)
        if sid is None
        else spark.sql(f"SELECT * FROM {fq} VERSION AS OF {int(sid)}")
    )
    rows, fp, cols_sha = frame_fingerprint(df, df.columns)
    return {"rows": int(rows), "fp": str(fp), "cols_sha": str(cols_sha)}


def count_comparable(record: dict) -> bool:
    """Whether the tick's recorded ``total_records`` is the snapshot's live
    row count: it came from the snapshot summary, and the snapshot carries
    no delete files (copy-on-write, as Lakebench creates its tables)."""
    return (
        record.get("count_source") == "summary"
        and is_snapshot(record.get("total_records"))
        and record.get("pos_deletes") == 0
        and record.get("eq_deletes") == 0
    )


def classify(record: dict, scan: dict, hashed: dict | None) -> dict:
    """The state of one live record from its read-pass scan and its
    hash-pass entry (None when the hashes file has none for it).

    ``fp_match``: the scan's rows, fp and column spec equal the hash pass's.
    ``count_match``: the scan's rows equal the tick's ``total_records``, or
    None when that count is not a live-row count (``count_comparable``)."""
    fp_match = hashed is not None and all(
        str(scan.get(k)) == str(hashed.get(k)) for k in ("rows", "fp", "cols_sha")
    )
    count_match = scan["rows"] == record["total_records"] if count_comparable(record) else None
    if not fp_match or count_match is False:
        state = MISMATCH
    elif count_match:
        state = VERIFIED
    else:
        state = VERIFIED_HASH_ONLY
    out = {"state": state, "fp_match": fp_match, "count_match": count_match}
    if hashed is None:
        out["reason"] = "no hash-pass entry for this snapshot"
    return out


class Budget:
    """Seconds left before no new scan starts."""

    def __init__(self, seconds: float | None, clock=time.monotonic) -> None:
        self.clock = clock
        self.end = None if seconds is None else clock() + float(seconds)

    def spent(self) -> bool:
        return self.end is not None and self.clock() >= self.end


def _fq(record: dict) -> str:
    return f"{CATALOG}.{record['table']}"


def hash_pass(spark, records: list[dict], hashes_uri: str, nonce: str, budget: Budget) -> bool:
    """Fingerprint every recorded snapshot still listed, once per snapshot,
    and write ``{nonce, hashes: [{table, snapshot, rows, fp, cols_sha}]}`` to
    *hashes_uri*. Returns False when the budget stopped it early."""
    hashes: list[dict] = []
    done: set[tuple[str, int]] = set()
    complete = True
    live: dict[str, set[int]] = {}
    for rec in records:
        sid = rec.get("snapshot")
        if not is_snapshot(sid):
            continue
        fq = _fq(rec)
        if fq not in live:
            live[fq] = live_snapshots(spark, fq)
        key = (rec["table"], sid)
        if key in done or sid not in live[fq]:
            continue
        if budget.spent():
            complete = False
            break
        try:
            hashes.append(
                {"table": rec["table"], "snapshot": sid, **fingerprint_at(spark, fq, sid)}
            )
        except Exception as e:  # noqa: BLE001 -- the read pass reports it
            log(f"[time-travel] hash pass: {fq} at {sid} unreadable: {one_line(e)}")
        done.add(key)
        log(f"[time-travel] hash pass: {fq} snapshot={sid} hashed")
    _write_text(spark, hashes_uri, json.dumps({"nonce": nonce, "hashes": hashes}))
    log(f"Wrote {hashes_uri} ({len(hashes)} snapshot(s))")
    return complete


def read_hashes(spark, hashes_uri: str, nonce: str) -> dict[tuple[str, int], dict]:
    """The hash pass's entries by (table, snapshot), read back from storage.
    A file from another submission (another nonce) reads as empty, so every
    record is a mismatch."""
    doc = json.loads(_read_text(spark, hashes_uri))
    if not isinstance(doc, dict) or doc.get("nonce") != nonce:
        log(f"[time-travel] {hashes_uri} is not this submission's; no hash is trusted")
        return {}
    out = {}
    for h in doc.get("hashes") or []:
        try:
            out[(str(h["table"]), int(h["snapshot"]))] = h
        except (KeyError, TypeError, ValueError):
            continue
    return out


def read_pass(
    spark, records: list[dict], hashed: dict, budget: Budget, clock=time.monotonic
) -> list[dict]:
    """One result per record, in record order: ``{start, cycle, table,
    snapshot, state, read_s, rows, total_records, fp_match, count_match}``.
    A snapshot read by several ticks is scanned once and its result shared."""
    out: list[dict] = []
    scans: dict[tuple[str, int], dict] = {}
    live: dict[str, set[int]] = {}
    stopped = False
    for rec in records:
        base = {
            "start": rec.get("start"),
            "cycle": rec.get("cycle"),
            "table": rec.get("table"),
            "snapshot": rec.get("snapshot"),
            "total_records": rec.get("total_records"),
            "read_s": None,
            "rows": None,
            "fp_match": None,
            "count_match": None,
        }
        sid = rec.get("snapshot")
        if not is_snapshot(sid):
            out.append({**base, "state": MISMATCH, "reason": "the tick recorded no snapshot id"})
            continue
        fq = _fq(rec)
        if fq not in live:
            live[fq] = live_snapshots(spark, fq)
        if sid not in live[fq]:
            out.append({**base, "state": EXPIRED})
            continue
        key = (rec["table"], sid)
        if key not in scans:
            if stopped or budget.spent():
                stopped = True
                out.append({**base, "state": NOT_READ, "reason": "the time-travel budget ran out"})
                continue
            t0 = clock()
            try:
                scan = fingerprint_at(spark, fq, sid)
                scans[key] = {**scan, "read_s": round(clock() - t0, 3)}
            except Exception as e:  # noqa: BLE001 -- reported per record
                scans[key] = {"error": f"{type(e).__name__}: {one_line(e)}"}
            log(f"[time-travel] read pass: {fq} snapshot={sid} {scans[key]}")
        scan = scans[key]
        if "error" in scan:
            out.append({**base, "state": ERROR, "reason": scan["error"]})
            continue
        out.append(
            {
                **base,
                "read_s": scan["read_s"],
                "rows": scan["rows"],
                **classify(rec, scan, hashed.get(key)),
            }
        )
    return out


def time_travel(spark, inputs: dict, hashes_uri: str, budget: Budget, clock=time.monotonic) -> dict:
    """Both passes and the current-snapshot scan: the ``time_travel.json``
    body (without the nonce)."""
    records = [r for r in inputs.get("records") or [] if isinstance(r, dict)]
    tables = sorted({str(r.get("table")) for r in records if r.get("table")})
    for table in tables:
        provider = table_provider(spark, f"{CATALOG}.{table}")
        if provider == "delta":
            return {
                "status": NOT_SUPPORTED,
                "reason": f"{table} is a Delta table; time-travel reads support Iceberg only",
                "ticks": [
                    {
                        "start": r.get("start"),
                        "cycle": r.get("cycle"),
                        "table": r.get("table"),
                        "snapshot": r.get("snapshot"),
                        "state": NOT_SUPPORTED,
                    }
                    for r in records
                ],
                "current": None,
                "incomplete": False,
            }
    nonce = str(inputs.get("nonce") or "")
    complete = hash_pass(spark, records, hashes_uri, nonce, budget)
    hashed = read_hashes(spark, hashes_uri, nonce)
    ticks = read_pass(spark, records, hashed, budget, clock)
    current = None
    if tables and not budget.spent():
        fq = f"{CATALOG}.{tables[0]}"
        try:
            sid = current_snapshot(spark, fq)
            t0 = clock()
            scan = fingerprint_at(spark, fq, sid)
            current = {
                "table": tables[0],
                "snapshot": sid,
                "rows": scan["rows"],
                "read_s": round(clock() - t0, 3),
            }
        except Exception as e:  # noqa: BLE001 -- for comparison only
            current = {"table": tables[0], "error": f"{type(e).__name__}: {one_line(e)}"}
    return {
        "status": "read",
        "ticks": ticks,
        "current": current,
        "incomplete": (not complete) or any(t["state"] == NOT_READ for t in ticks),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-read the snapshots continuous ticks read")
    parser.add_argument("--input", required=True, help="S3 URI of tt_input.json")
    parser.add_argument("--hashes", required=True, help="S3 URI for tt_hashes.json")
    parser.add_argument("--output", required=True, help="S3 URI for time_travel.json")
    parser.add_argument(
        "--budget-s", type=float, default=None, help="seconds after which no new scan starts"
    )
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("lb-time-travel-financial").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    log("=" * 60)
    log("Time-travel reads of the recorded tick snapshots")
    log("=" * 60)
    inputs = json.loads(_read_text(spark, args.input))
    result = time_travel(spark, inputs, args.hashes, Budget(args.budget_s))
    result["nonce"] = inputs.get("nonce")
    result["run_id"] = inputs.get("run_id")
    states: dict[str, int] = {}
    for t in result["ticks"]:
        states[t["state"]] = states.get(t["state"], 0) + 1
    log(
        f"[time-travel] status={result['status']} states={states} incomplete={result['incomplete']}"
    )
    _write_text(spark, args.output, json.dumps(result, default=str))
    log(f"Wrote {args.output}")
    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
