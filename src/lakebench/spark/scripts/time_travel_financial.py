"""Time travel (Financial) -- re-read the snapshots a continuous run's ticks read.

Each continuous AML gold-refresh tick records the silver transactions
snapshot it read and that snapshot's metadata counts (the ``tt-record`` line,
``continuous.time_travel.ticks``). After the window, with the streams
stopped and no measured interval open, ``lakebench run --continuous``
submits this job with those records (``--input``, written by
``cli/_aml_post.run_time_travel``) and reads its result.

1. Hash pass: for each recorded snapshot still in the table's snapshots
   metadata, newest first, fingerprint the raw snapshot over its business
   columns (``common.frame_fingerprint``; no sealed filter): every column of
   the snapshot's schema except the batch-version sentinels the input names
   (``exclude_columns``, the CLI's ``metrics.time_travel.SENTINEL_COLUMNS``,
   which is release/silver_parity.py's definition), and write
   ``tt_hashes.json``, with the snapshots it could not read. This is the
   order-independent hash computed for that snapshot after the window.
2. Read pass, the published measurement: re-read ``tt_hashes.json`` from
   storage (not from memory, so an altered file is seen), then for each
   record either report the snapshot ``expired`` (gone from the snapshots
   metadata) or time a full scan ``VERSION AS OF`` it with the same
   fingerprint (scan plus fingerprint: ``read_s``) and compare it with what
   the tick recorded (the summary count, when it is a live-row count) and
   with the hash pass. Equal on both is ``verified``; with no comparable
   count, ``verified_hash_only``; either differing is ``mismatch``. A
   snapshot still listed that either pass could not read is ``error``.
3. The same scan of the current snapshot is timed, for comparison only.

What ``verified`` proves: the snapshot id the tick read still holds as many
rows as that snapshot's summary said when the tick read it, and reads the
same way twice. Iceberg snapshots are immutable, so the hash comparison
shows read determinism and an unaltered hashes file, not tick-time content.

Writes ``time_travel.json`` beside the hashes and exits 0 for every
determined outcome; a non-zero exit is a crash. A deadline
(``--deadline-epoch``, on this driver's clock, a Lakebench-imposed bound)
stops starting a scan that would not finish before it (the time left must
exceed 1.5 times the longest scan so far, so the first scan of a pass
always starts); the hash pass gets half of the
time: the records left read ``not_read`` and the result says
``incomplete``. A Delta table is refused: every record reads
``not_supported`` (AML on Delta is refused by the config today). Writes no
table and creates no tag, branch or other snapshot reference.
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


#: Columns left out of the hash: the batch-version sentinels, from the input
#: (``exclude_columns``); set by ``time_travel`` before any scan.
EXCLUDED_COLUMNS: frozenset[str] = frozenset()


def business_columns(columns: list[str]) -> list[str]:
    """The snapshot's columns, in schema order, less ``EXCLUDED_COLUMNS``."""
    return [c for c in columns if c not in EXCLUDED_COLUMNS]


def fingerprint_at(spark, fq: str, sid: int | None) -> dict:
    """``{rows, fp, cols_sha}`` of the raw table at snapshot *sid* (the
    current table for None), over its business columns (the batch-version
    sentinels left out). One full scan; ``rows`` counts every row."""
    df = (
        spark.table(fq)
        if sid is None
        else spark.sql(f"SELECT * FROM {fq} VERSION AS OF {int(sid)}")
    )
    rows, fp, cols_sha = frame_fingerprint(df, business_columns(df.columns))
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
    """Time left before *deadline* (epoch seconds on this driver's clock;
    None: no limit). A scan starts only when the time left exceeds 1.5
    times the longest scan so far, so the job ends near the deadline, not
    one scan after it."""

    def __init__(self, deadline: float | None, clock=time.time) -> None:
        self.clock = clock
        self.deadline = deadline
        self.longest = 0.0

    def left(self) -> float | None:
        return None if self.deadline is None else self.deadline - self.clock()

    def can_start(self) -> bool:
        left = self.left()
        return left is None or (left > 0 and left > 1.5 * self.longest)

    def took(self, seconds: float) -> None:
        self.longest = max(self.longest, seconds)

    def split(self) -> Budget:
        """A budget ending halfway to this one's deadline (the hash pass's
        share), sharing the longest-scan estimate."""
        left = self.left()
        half = Budget(None if left is None else self.clock() + left / 2, self.clock)
        half.longest = self.longest
        return half


def _fq(record: dict) -> str:
    return f"{CATALOG}.{record['table']}"


def _key(record: dict) -> tuple[str, int]:
    return (str(record["table"]), int(record["snapshot"]))


def newest_first(records: list[dict]) -> list[tuple[str, int]]:
    """The recorded (table, snapshot) keys, once each, from the latest tick
    back: the snapshots most likely to be live, and the last tick's (the
    one recall was scored on), are read first when the time runs short."""
    keys: list[tuple[str, int]] = []
    for rec in reversed(records):
        if is_snapshot(rec.get("snapshot")) and _key(rec) not in keys:
            keys.append(_key(rec))
    return keys


def table_state(spark, tables: list[str]) -> dict[str, dict]:
    """Per table: ``{provider, live}`` (live snapshot ids), or ``{error}``
    when the table cannot be described or its snapshots listed."""
    out: dict[str, dict] = {}
    for table in tables:
        fq = f"{CATALOG}.{table}"
        try:
            provider = table_provider(spark, fq)
            live = live_snapshots(spark, fq) if provider != "delta" else set()
            out[table] = {"provider": provider, "live": live}
        except Exception as e:  # noqa: BLE001 -- every record of it reads error
            out[table] = {"error": f"{type(e).__name__}: {one_line(e)}"}
    return out


def hash_pass(
    spark, keys: list[tuple[str, int]], hashes_uri: str, nonce: str, budget: Budget
) -> set[tuple[str, int]]:
    """Fingerprint each live (table, snapshot) in *keys* order and write
    ``{nonce, hashes: [{table, snapshot, rows, fp, cols_sha}], errors:
    [{table, snapshot, error}]}`` to *hashes_uri*. Returns the keys it
    reached (hashed or failed); the budget may stop it before the rest.
    Only which keys it reached is kept in memory: their hashes are read back
    from the file."""
    hashes: list[dict] = []
    errors: list[dict] = []
    reached: set[tuple[str, int]] = set()
    for table, sid in keys:
        if not budget.can_start():
            break
        reached.add((table, sid))
        t0 = budget.clock()
        try:
            hashes.append(
                {
                    "table": table,
                    "snapshot": sid,
                    **fingerprint_at(spark, f"{CATALOG}.{table}", sid),
                }
            )
            log(f"[time-travel] hash pass: {table} snapshot={sid} hashed")
        except Exception as e:  # noqa: BLE001 -- the read pass reports it as error
            errors.append(
                {"table": table, "snapshot": sid, "error": f"{type(e).__name__}: {one_line(e)}"}
            )
            log(f"[time-travel] hash pass: {table} at {sid} unreadable: {one_line(e)}")
        budget.took(budget.clock() - t0)
    _write_text(spark, hashes_uri, json.dumps({"nonce": nonce, "hashes": hashes, "errors": errors}))
    log(f"Wrote {hashes_uri} ({len(hashes)} hashed, {len(errors)} unreadable)")
    return reached


def read_hashes(spark, hashes_uri: str, nonce: str) -> tuple[dict, dict]:
    """``(hashes, errors)`` of the hash pass by (table, snapshot), read back
    from storage. A file from another submission (another nonce) reads as
    empty, so every live record is a mismatch."""
    doc = json.loads(_read_text(spark, hashes_uri))
    if not isinstance(doc, dict) or doc.get("nonce") != nonce:
        log(f"[time-travel] {hashes_uri} is not this submission's; no hash is trusted")
        return {}, {}
    out: dict[tuple[str, int], dict] = {}
    errors: dict[tuple[str, int], str] = {}
    for name, target in (("hashes", out), ("errors", errors)):
        for h in doc.get(name) or []:
            try:
                key = (str(h["table"]), int(h["snapshot"]))
            except (KeyError, TypeError, ValueError):
                continue
            target[key] = h if name == "hashes" else str(h.get("error"))
    return out, errors


def read_pass(
    spark,
    records: list[dict],
    tables: dict[str, dict],
    hashed: dict,
    hash_errors: dict,
    budget: Budget,
    reached: set | None = None,
) -> list[dict]:
    """One result per record, in record order: ``{start, cycle, table,
    snapshot, state, read_s, rows, total_records, fp_match, count_match}``.
    Snapshots are scanned newest first; one several ticks read is scanned
    once and its result shared. *reached*: the keys the hash pass reached
    (None: all); a live key it did not reach is ``not_read``, one it reached
    with no entry in the file read back is a ``mismatch``."""
    scans: dict[tuple[str, int], dict] = {}
    for key in newest_first(records):
        st = tables.get(key[0]) or {}
        if "error" in st or key[1] not in st.get("live", ()) or key in hash_errors:
            continue
        if reached is not None and key not in reached:
            scans[key] = {"not_read": "the hash pass's share of the time-travel budget ran out"}
            continue
        if not budget.can_start():
            scans[key] = {"not_read": "the time-travel budget ran out"}
            continue
        t0 = budget.clock()
        try:
            scan = fingerprint_at(spark, f"{CATALOG}.{key[0]}", key[1])
            scans[key] = {**scan, "read_s": round(budget.clock() - t0, 3)}
        except Exception as e:  # noqa: BLE001 -- reported per record
            scans[key] = {"error": f"{type(e).__name__}: {one_line(e)}"}
        budget.took(budget.clock() - t0)
        log(f"[time-travel] read pass: {key[0]} snapshot={key[1]} {scans[key]}")
    out: list[dict] = []
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
        if not is_snapshot(rec.get("snapshot")):
            out.append({**base, "state": MISMATCH, "reason": "the tick recorded no snapshot id"})
            continue
        key = _key(rec)
        st = tables.get(key[0]) or {}
        if "error" in st:
            out.append(
                {**base, "state": ERROR, "reason": f"the table could not be read: {st['error']}"}
            )
            continue
        if key[1] not in st.get("live", ()):
            out.append({**base, "state": EXPIRED})
            continue
        if key in hash_errors:
            out.append(
                {
                    **base,
                    "state": ERROR,
                    "reason": f"the hash pass could not read it: {hash_errors[key]}",
                }
            )
            continue
        scan = scans[key]
        if "not_read" in scan:
            out.append({**base, "state": NOT_READ, "reason": scan["not_read"]})
        elif "error" in scan:
            out.append({**base, "state": ERROR, "reason": scan["error"]})
        else:
            out.append(
                {
                    **base,
                    "read_s": scan["read_s"],
                    "rows": scan["rows"],
                    **classify(rec, scan, hashed.get(key)),
                }
            )
    return out


def time_travel(spark, inputs: dict, hashes_uri: str, budget: Budget) -> dict:
    """Both passes and the current-snapshot scan: the ``time_travel.json``
    body (without the nonce)."""
    global EXCLUDED_COLUMNS
    excluded = inputs.get("exclude_columns")
    if (
        not isinstance(excluded, list)
        or not excluded
        or not all(isinstance(c, str) for c in excluded)
    ):
        # No business-column definition: hashing every column would publish
        # another measurement than the one defined. A crash: the CLI reads
        # not_run.
        raise SystemExit("tt_input.json names no exclude_columns (the business-column definition)")
    EXCLUDED_COLUMNS = frozenset(excluded)
    records = [r for r in inputs.get("records") or [] if isinstance(r, dict)]
    names = sorted({str(r.get("table")) for r in records if r.get("table")})
    tables = table_state(spark, names)
    delta = [t for t, st in tables.items() if st.get("provider") == "delta"]
    if delta:
        return {
            "status": NOT_SUPPORTED,
            "reason": f"{', '.join(delta)} is a Delta table; time-travel reads support Iceberg only",
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
    live_keys = [
        k
        for k in newest_first(records)
        if "error" not in tables.get(k[0], {}) and k[1] in tables.get(k[0], {}).get("live", ())
    ]
    share = budget.split()
    reached = hash_pass(spark, live_keys, hashes_uri, nonce, share)
    complete = len(reached) == len(live_keys)
    budget.took(share.longest)
    hashed, hash_errors = read_hashes(spark, hashes_uri, nonce)
    ticks = read_pass(spark, records, tables, hashed, hash_errors, budget, reached)
    current = None
    readable = [t for t in names if "error" not in tables[t]]
    if readable and budget.can_start():
        fq = f"{CATALOG}.{readable[0]}"
        try:
            sid = current_snapshot(spark, fq)
            t0 = budget.clock()
            scan = fingerprint_at(spark, fq, sid)
            current = {
                "table": readable[0],
                "snapshot": sid,
                "rows": scan["rows"],
                "read_s": round(budget.clock() - t0, 3),
            }
        except Exception as e:  # noqa: BLE001 -- for comparison only
            current = {"table": readable[0], "error": f"{type(e).__name__}: {one_line(e)}"}
    elif readable:
        current = {"table": readable[0], "not_read": "the time-travel budget ran out"}
    return {
        "status": "read",
        "excluded_columns": sorted(EXCLUDED_COLUMNS),
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
        "--deadline-epoch",
        type=float,
        default=None,
        help="epoch seconds (this driver's clock) after which no scan starts",
    )
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("lb-time-travel-financial").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    log("=" * 60)
    log("Time-travel reads of the recorded tick snapshots")
    log("=" * 60)
    inputs = json.loads(_read_text(spark, args.input))
    budget = Budget(args.deadline_epoch)
    log(f"[time-travel] seconds left before the deadline: {budget.left()}")
    result = time_travel(spark, inputs, args.hashes, budget)
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
