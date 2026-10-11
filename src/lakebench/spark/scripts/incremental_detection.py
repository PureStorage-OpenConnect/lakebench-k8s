"""Continuous AML gold: re-detect only what the new silver rows can change.

A continuous run delivers its corpus in event-time order (each silver
micro-batch reaches at most a few days behind the newest row before it), and
every continuous rule is local in event time:

- W2 originator: a tumbling ``window_hours`` window holds only its own rows.
- W2 beneficiary: a window anchored at a credit at ``t`` reads the credits in
  [t - window_hours, t + window_hours); an entity's bursts read all its
  qualifying windows, which are few.
- W3: a cycle lies within ``total_window_days``, and hub status is per
  hop-window bucket (week), so it reads only the weeks of its own transfers.
- W17: a chain of at most ``max_hops`` transfers spans at most
  ``(max_hops - 1)`` hop windows, and whether it is complete reads the hop
  window after its last transfer.
- W4 (continuous: one alert per entity per ``alert_window_hours`` window): a
  matched pair lies within ``velocity_hours``, and a window's alert reads
  only that window's pairs.
- W5 and W6 (continuous: the transaction screen only, no rescreen): one
  alert per payment, screened against a watchlist fixed for the run, so a
  payment's alert reads only that payment and its parties' entity rows.
  silver_stream commits a batch's entities after its transactions, so the
  days whose payments had a party with no entity row yet are screened again
  on the next pass (at most MAX_CARRY_PASSES passes in a row). A later change to an entity's country does
  not rescreen earlier payments (a payment is screened with what was known
  when it was screened, as a real-time screen is), and nor would a customer
  flag that changed after its entity row was written (silver sets it at
  first insert).

So with ``cut`` the event-time spans that hold the rows new since the rule's
last committed pass (whole days, see ``gold_refresh_financial.tick_position``),
every alert whose evidence lies outside rule-specific spans around them is
unchanged, and the rest are recomputed from the silver rows of those spans
widened by the rule's lookback. Datagen pods drift apart in event time, so
the new rows of a tick lie in several separate spans; a single bound below
the earliest of them would re-read everything faster pods delivered since.
The state a rule keeps between passes (its alerts, W2's window rows and
qualifying windows) is written as Parquet under the gold bucket. The output
each pass is the rule's full alert set, equal to a full recompute over the
same snapshot while no path cap binds (W3 and W17's edge and path caps apply
to the rows a pass reads, so a region pass can run where a full pass over all
of silver would stop at the cap); ``write_spans`` names the alert_ts spans outside which that
set is unchanged, so the driver rewrites only those rows.

A rule's first pass in a driver, and any pass after a silver position that
could not be read, is a full recompute. A rule that skips or fails keeps its
earlier state, and its next pass covers every row since that state.
"""

from __future__ import annotations

import inspect
import time
import uuid

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, timestamp_micros, unix_micros

HOUR_US = 3_600_000_000

# The rules this module re-detects incrementally.
INCREMENTAL_RULES = (
    "W5_sanctions_match",
    "W6_pep_counterparty",
    "W2_structuring",
    "W3_round_tripping",
    "W4_risk_propagation",
    "W17_layering_chain",
)

# A cut that forces a full recompute (the silver position was unreadable).
FULL = float("-inf")

DAY_US = 24 * HOUR_US
# State is stored in parts of this much key time, so a pass rewrites only the
# parts its spans reach, not all the state the run has built up.
PART_US = 30 * DAY_US
# More spans than this are merged across their smallest gaps: a wider read,
# never a missed row.
MAX_SPANS = 32


def _floor(x: int, step: int) -> int:
    return (x // step) * step


def _ceil(x: int, step: int) -> int:
    return -((-x) // step) * step


def merge_spans(spans) -> tuple:
    """Sorted, disjoint ``(lo, hi)`` epoch-microsecond spans (``hi``
    exclusive) covering ``spans``, at most MAX_SPANS of them."""
    out: list[list[int]] = []
    for lo, hi in sorted((int(a), int(b)) for a, b in spans):
        if out and lo <= out[-1][1]:
            out[-1][1] = max(out[-1][1], hi)
        else:
            out.append([lo, hi])
    while len(out) > MAX_SPANS:
        i = min(range(len(out) - 1), key=lambda k: out[k + 1][0] - out[k][1])
        out[i : i + 2] = [[out[i][0], out[i + 1][1]]]
    return tuple((lo, hi) for lo, hi in out)


def widen(spans, back: int, fwd: int, step: int = 1) -> tuple:
    """Each span from ``back`` before its start to ``fwd`` after its end,
    aligned outward to ``step``, merged."""
    return merge_spans((_floor(lo - back, step), _ceil(hi + fwd, step)) for lo, hi in spans)


def _in(us, spans):
    """Whether the epoch-microsecond column ``us`` lies in ``spans``."""
    out = lit(False)
    for lo, hi in spans:
        out = out | ((us >= lit(int(lo))) & (us < lit(int(hi))))
    return out


def _within(txns: DataFrame, spans) -> DataFrame:
    """Rows with event time in ``spans``, as literal timestamp comparisons so
    Iceberg prunes the month partitions outside them."""
    out = lit(False)
    for lo, hi in spans:
        out = out | (
            (col("txn_timestamp") >= timestamp_micros(lit(int(lo))))
            & (col("txn_timestamp") < timestamp_micros(lit(int(hi))))
        )
    return txns.filter(out)


def _us(c):
    return unix_micros(c)


def _reach(spans) -> list[int]:
    """The state parts (PART_US of key time) ``spans`` reach."""
    return sorted({k for lo, hi in spans for k in range(lo // PART_US, (hi - 1) // PART_US + 1)})


def _customers_key(silver_entities):
    """(count, xor of hashes) of the customer entity ids in
    ``silver_entities``; None without a frame or an is_customer column. An
    xor, not a sum: a sum of 64-bit hashes overflows under ANSI mode."""
    from pyspark.sql.functions import expr

    if silver_entities is None or "is_customer" not in silver_entities.columns:
        return None
    r = (
        silver_entities.filter(col("is_customer") == lit(True))
        .agg(expr("count(1)").alias("n"), expr("bit_xor(xxhash64(entity_id))").alias("h"))
        .collect()[0]
    )
    return (int(r["n"]), int(r["h"] or 0))


def _defaults(fn, params: dict) -> dict:
    """The rule's keyword defaults, overridden by ``params``."""
    out = {
        name: p.default
        for name, p in inspect.signature(fn).parameters.items()
        if p.default is not inspect.Parameter.empty
    }
    out.update(params)
    return out


class IncrementalDetection:
    """Per-rule state and cuts for one gold-refresh driver.

    ``begin_tick(cut, rules)`` records each tick's cut for every continuous
    rule, whether or not it runs this tick. ``rule(rule_id, fn)`` returns the
    function the detection driver calls in place of ``fn``; its alerts are
    the rule's full set. ``committed(rule_id)`` makes that pass's state
    current once its alerts are written; ``failed(rule_id)`` drops it.
    ``last`` describes each rule's latest pass (mode, bounds), for the log.
    """

    def __init__(self, spark, root: str) -> None:
        self.spark = spark
        self.root = root.rstrip("/")
        self.state: dict[str, dict[str, DataFrame]] = {}
        self.cut: dict[str, float] = {}
        self.staged: dict[str, dict[str, DataFrame]] = {}
        # {rule_id: {frame name: {part: path}}}, committed and staged; part
        # None holds a frame written whole.
        self.parts: dict[str, dict[str, dict]] = {}
        self.staged_parts: dict[str, dict[str, dict]] = {}
        # The directories a pass wrote, deleted if it does not commit.
        self.staged_paths: dict[str, list[str]] = {}
        self.last: dict[str, dict] = {}
        # {rule_id: customers key} of each W2 pass, committed and staged.
        self.customers: dict[str, tuple | None] = {}
        self.staged_customers: dict[str, tuple | None] = {}
        # Rules whose last pass did not commit: the driver dropped their
        # alerts, so the next pass writes them again even with nothing new.
        self.dirty: set[str] = set()
        # {rule_id: {day: passes}} the days a screening pass carries into
        # its next cut (payments a party of which had no entity row yet),
        # with how many passes in a row each has been carried; staged, then
        # current once the pass commits. A day is carried at most
        # MAX_CARRY_PASSES times.
        self.staged_pending: dict[str, dict[int, int]] = {}
        self.carried: dict[str, dict[int, int]] = {}

    # -- tick bookkeeping ---------------------------------------------------

    def begin_tick(self, cut, rules) -> None:
        """``cut``: the event-time spans of the rows new since the previous
        tick (``merge_spans`` form), None when there are none, FULL when the
        previous position is unknown. A rule that does not pass this tick
        keeps the union for its next pass."""
        if cut is None:
            return
        for rule_id in rules:
            prior = self.cut.get(rule_id)
            if prior is None:
                self.cut[rule_id] = cut
            elif prior == FULL or cut == FULL:
                self.cut[rule_id] = FULL
            else:
                self.cut[rule_id] = merge_spans(prior + cut)

    def write_spans(self, rule_id: str):
        """The alert_ts spans outside which the rule's latest pass left its
        alerts as the previous committed pass wrote them; None when the whole
        set must be written (a full pass, a rule without spans, or a rule
        whose earlier alerts the driver dropped)."""
        return self.last.get(rule_id, {}).get("write")

    def unchanged(self, rule_id: str, silver_entities=None) -> bool:
        """No row is new since the rule's last committed pass, so the alerts
        that pass wrote stand as they are. For W2 the customers must also be
        the ones its last pass read (``silver_entities``): a customer row
        that lands with no new transactions still turns windows into alerts."""
        if rule_id not in self.state or self.cut.get(rule_id) is not None:
            return False
        if rule_id in self.dirty:
            return False
        if rule_id == "W2_structuring":
            return self.customers.get(rule_id) == _customers_key(silver_entities)
        return True

    def committed(self, rule_id: str) -> None:
        staged = self.staged.pop(rule_id, None)
        self.dirty.discard(rule_id)
        if rule_id in self.staged_customers:
            self.customers[rule_id] = self.staged_customers.pop(rule_id)
        if staged is None:
            # An unchanged pass: the state stands, and its alerts are written.
            return
        new = self.staged_parts.pop(rule_id, {})
        keep = {p for parts in new.values() for p in parts.values()}
        old = {p for parts in self.parts.get(rule_id, {}).values() for p in parts.values()}
        self.state[rule_id] = staged
        self.parts[rule_id] = new
        self.staged_paths.pop(rule_id, None)
        self.cut.pop(rule_id, None)
        pending = self.staged_pending.pop(rule_id, None)
        if pending is not None:
            if pending:
                self.cut[rule_id] = _day_spans(pending)
            self.carried[rule_id] = pending
        for path in sorted(old - keep):
            self._delete(path)

    def failed(self, rule_id: str, drop_state: bool = False) -> None:
        """The pass did not commit: its staged files are deleted. With
        ``drop_state`` (the rule raised, so its state may be unreadable) the
        rule's next pass is a full recompute; a skip keeps the state."""
        self.staged.pop(rule_id, None)
        self.staged_parts.pop(rule_id, None)
        self.staged_pending.pop(rule_id, None)
        self.dirty.add(rule_id)
        for path in self.staged_paths.pop(rule_id, []):
            self._delete(path)
        if drop_state:
            self.state.pop(rule_id, None)
            self.cut.pop(rule_id, None)
            for parts in self.parts.pop(rule_id, {}).values():
                for path in parts.values():
                    self._delete(path)

    # -- storage ------------------------------------------------------------

    def _write(self, rule_id: str, name: str, df: DataFrame, key=None, spans=None) -> DataFrame:
        """Write the frame ``df`` of the rule's new state and return it read
        back. With ``key`` (epoch microseconds) it is stored in PART_US parts
        of key time, and with ``spans`` (a region pass) only the parts the
        spans reach are written: ``df`` is unchanged outside them, so the
        other parts of the frame's last committed state stand."""
        path = f"{self.root}/{rule_id}/{name}-{uuid.uuid4().hex[:12]}"
        schema = df.schema
        self.staged_paths.setdefault(rule_id, []).append(path)
        if key is None:
            df.write.mode("overwrite").parquet(path)
            parts = {None: path}
        else:
            part = (key / lit(PART_US)).cast("long")
            parts = {}
            if spans is not None:
                reach = _reach(spans)
                df = df.filter(part.isin(reach))
                prior = self.parts.get(rule_id, {}).get(name, {})
                parts = {k: v for k, v in prior.items() if k not in set(reach)}
            df.withColumn("_part", part).write.mode("overwrite").partitionBy("_part").parquet(path)
            for k in self._listed_parts(path):
                parts[k] = f"{path}/_part={k}"
        self.staged_parts.setdefault(rule_id, {})[name] = parts
        if not parts:
            return self.spark.createDataFrame([], schema)
        return self.spark.read.schema(schema).parquet(*sorted(parts.values()))

    def prior(self, rule_id: str, name: str, spans) -> DataFrame:
        """The rule's committed frame ``name`` read from only the parts
        ``spans`` reach: a region pass rewrites just those parts, and the
        rows of the others stand without being read. The whole frame when it
        is not stored in parts."""
        frame = self.state[rule_id][name]
        parts = self.parts.get(rule_id, {}).get(name, {})
        if not parts or None in parts:
            return frame
        reach = set(_reach(spans))
        paths = sorted(v for k, v in parts.items() if k in reach)
        if not paths:
            return self.spark.createDataFrame([], frame.schema)
        return self.spark.read.schema(frame.schema).parquet(*paths)

    def _listed_parts(self, path: str) -> list[int]:
        """The ``_part=K`` directories Spark wrote under ``path``."""
        jvm = self.spark._jvm  # type: ignore[attr-defined]
        hconf = self.spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = p.getFileSystem(hconf)
        if not fs.exists(p):
            return []
        out = []
        for st in fs.listStatus(p):
            name = st.getPath().getName()
            if st.isDirectory() and name.startswith("_part="):
                out.append(int(name.split("=", 1)[1]))
        return out

    def _delete(self, path: str) -> None:
        from detection_rules import _delete_uri

        _delete_uri(self.spark, path, "incremental")

    def discard_all(self) -> None:
        """Delete this driver's state (at the drain)."""
        self._delete(self.root)

    # -- the wrapped rule ---------------------------------------------------

    def rule(self, rule_id: str, fn):
        def run(txns: DataFrame, **params) -> DataFrame:
            started = time.time()
            p = _defaults(fn, params)
            if rule_id == "W2_structuring":
                self.staged_customers[rule_id] = _customers_key(p.get("silver_entities"))
            prior = self.state.get(rule_id)
            cut = self.cut.get(rule_id)
            if prior is None or cut == FULL:
                mode, cut = "full", None
            elif cut is None:
                mode = "unchanged"
            else:
                mode = "region"
            alerts_of, steps, bounds = _RULES[rule_id](self, txns, p, prior, mode, cut)
            if mode == "unchanged":
                state = prior
            else:
                # Each step builds one frame from the frames written before
                # it, so a later frame never recomputes an earlier one.
                state = {}
                try:
                    for step in steps:
                        name, build = step[0], step[1]
                        key = step[2] if len(step) > 2 else None
                        spans = step[3] if len(step) > 3 and mode == "region" else None
                        state[name] = self._write(rule_id, name, build(state), key, spans)
                except BaseException:
                    self.failed(rule_id)
                    raise
                self.staged[rule_id] = state
            write = (
                bounds.pop("write", None)
                if mode == "region" and rule_id not in self.dirty
                else None
            )
            self.last[rule_id] = {
                "mode": mode,
                **bounds,
                "write": write,
                "s": round(time.time() - started, 1),
            }
            # In gold.alerts column order: the driver's INSERT is positional,
            # and a join on entity_id moves that column first.
            return _alert_columns(alerts_of(state))

        return run


def incremental_line(last: dict | None) -> str:
    """One rule's latest pass for the driver log: its mode (full, region or
    unchanged), the event time from which its alerts were recomputed and
    from which silver was read (UTC), and its seconds."""
    import datetime as dt

    if not last:
        return "mode=unknown"

    def iso(us):
        return dt.datetime.fromtimestamp(us / 1e6, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    parts = [f"mode={last.get('mode')}"]
    for key, name in (("from_us", "from"), ("read_us", "read")):
        if last.get(key) is not None:
            parts.append(f"{name}={iso(last[key])}")
    if last.get("read_days") is not None:
        parts.append(f"spans={last.get('spans')} read_days={last['read_days']:.1f}")
    parts.append(f"s={last.get('s')}")
    return " ".join(parts)


def _alert_columns(df: DataFrame) -> DataFrame:
    from detection_rules import ALERT_COLUMNS

    return df.select(*[name for name, _, _ in ALERT_COLUMNS])


# -- per rule --------------------------------------------------------------
# Each returns (alerts_of, steps, bounds): ``alerts_of`` builds the rule's
# alerts from its state as written; ``steps`` is the new state as
# (name, build) in order, ``build`` taking the frames written so far (empty
# for "unchanged"). In a region pass ``cut`` is the spans of the new rows;
# ``affected`` the spans of the key time of every alert they can change;
# ``read`` the silver spans that hold all those alerts' evidence.


def _bounds(cut, affected, read, write=None) -> dict:
    return {
        "from_us": affected[0][0],
        "read_us": read[0][0],
        "spans": len(cut),
        "read_days": sum(hi - lo for lo, hi in read) / DAY_US,
        "read": read,
        "write": write,
    }


def _w3(inc, txns, p, prior, mode, cut):
    from detection_rules import w3_round_tripping

    params = {k: v for k, v in p.items() if k != "silver_entities"}
    bounds: dict = {}
    steps: list = []
    if mode == "full":
        steps = [("alerts", lambda w: w3_round_tripping(txns, **params), _us(col("alert_ts")))]
    elif mode == "region":
        hop = int(p["hop_window_hours"]) * HOUR_US
        total = int(p["total_window_days"]) * 24 * HOUR_US
        # Hub status changes only in the weeks (hop buckets) of new rows, and
        # a cycle through them ends (alert_ts) at most total_window_days
        # after; it starts at most total_window_days before it ends. Whole
        # buckets are read, so hub status is judged on all their rows.
        affected = widen(widen(cut, 0, 0, hop), 0, total, hop)
        read = widen(affected, total, 0, hop)

        def alerts(w):
            new = w3_round_tripping(_within(txns, read), **params)
            return (
                inc.prior("W3_round_tripping", "alerts", affected)
                .filter(~_in(_us(col("alert_ts")), affected))
                .unionByName(new.filter(_in(_us(col("alert_ts")), affected)))
            )

        steps = [("alerts", alerts, _us(col("alert_ts")), affected)]
        bounds = _bounds(cut, affected, read, write=affected)
    return (lambda s: s["alerts"]), steps, bounds


def _w17(inc, txns, p, prior, mode, cut):
    from detection_rules import _empty_alerts_df, w17_alert_frame, w17_chains

    def alerts_of(frame):
        if frame is None:
            return _empty_alerts_df(inc.spark, p["run_id"]).withColumn(
                "ts_last", lit(None).cast("timestamp")
            )
        return w17_alert_frame(
            frame,
            p["hop_window_hours"],
            p["min_forward_ratio"],
            p["max_forward_ratio"],
            p["max_out_degree"],
            p["run_id"],
            keep=("ts_last",),
        )

    chain_params = {
        k: p[k]
        for k in (
            "min_hops",
            "max_hops",
            "hop_window_hours",
            "min_forward_ratio",
            "max_forward_ratio",
            "max_out_degree",
            "max_edges",
            "max_paths",
        )
    }
    bounds: dict = {}
    steps: list = []
    if mode == "full":
        steps = [
            ("alerts", lambda w: alerts_of(w17_chains(txns, **chain_params)), _us(col("ts_last")))
        ]
    elif mode == "region":
        hop = int(p["hop_window_hours"]) * HOUR_US
        span = (max(2, int(p["min_hops"]), int(p["max_hops"])) - 1) * hop
        # A chain is complete or returns to its start by the transfers in
        # the hop window after its last one, so a chain ending (ts_last) up
        # to a hop window before a new row can change, and one through a new
        # row or its bucket's hub status ends at most ``span`` after it. A
        # chain starts at most ``span`` before it ends and is judged on the
        # hop window after.
        affected = widen(widen(cut, 0, 0, hop), hop, span, hop)
        read = widen(affected, span, hop, hop)

        def alerts(w):
            new = alerts_of(w17_chains(_within(txns, read), **chain_params))
            return (
                inc.prior("W17_layering_chain", "alerts", affected)
                .filter(~_in(_us(col("ts_last")), affected))
                .unionByName(new.filter(_in(_us(col("ts_last")), affected)))
            )

        steps = [("alerts", alerts, _us(col("ts_last")), affected)]
        # alert_ts is the chain's first transfer, at most ``span`` before
        # ts_last.
        bounds = _bounds(cut, affected, read, write=widen(affected, span, 0))
    return (lambda s: s["alerts"]), steps, bounds


def _w2(inc, txns, p, prior, mode, cut):
    from detection_rules import (
        _customer_ids,
        _customers_only,
        w2_alert_frame,
        w2_band,
        w2_bursts,
        w2_originator_windows,
        w2_qualifying_windows,
    )

    n, hours, cap = int(p["threshold_count"]), int(p["window_hours"]), int(p["max_txns_per_alert"])
    per_bene = bool(p["per_beneficiary"])
    # Customers are read each pass: a customer row that lands after its
    # transactions turns their earlier windows into alerts. So W2's alerts
    # can change anywhere in time, and the driver writes them whole (no
    # write spans).
    customers = _customer_ids(inc.spark, p.get("silver_entities"), "W2")

    def alerts_of(s):
        rows = s["originator"]
        if per_bene:
            rows = rows.unionByName(s["beneficiary"])
        return _customers_only(w2_alert_frame(rows, n, hours, cap, p["run_id"]), customers)

    bounds: dict = {}
    steps: list = []
    if mode == "full":
        band = w2_band(txns)
        steps = [
            ("originator", lambda w: w2_originator_windows(band, n, hours), _us(col("last_ts")))
        ]
        if per_bene:
            steps += [
                ("qualifying", lambda w: w2_qualifying_windows(band, n, hours), col("_t")),
                ("beneficiary", lambda w: w2_bursts(w["qualifying"], hours, cap)),
            ]
    elif mode == "region":
        window = hours * HOUR_US
        # Tumbling windows from the epoch: only the windows of new rows change.
        windows = widen(cut, 0, 0, window)
        affected = read = windows

        def originator(w):
            new = w2_originator_windows(w2_band(_within(txns, windows)), n, hours)
            return (
                inc.prior("W2_structuring", "originator", windows)
                .filter(~_in(_us(col("last_ts")), windows))
                .unionByName(new.filter(_in(_us(col("last_ts")), windows)))
            )

        steps = [("originator", originator, _us(col("last_ts")), windows)]
        if per_bene:
            # A window anchored at t holds the credits in [t, t + window), and
            # whether it is contained in the previous credit's window reads
            # the credits up to a window before t: a new credit at x changes
            # the windows anchored within a window of x.
            anchors = widen(cut, window, window)
            read = merge_spans(windows + widen(anchors, window, window))
            # Only the qualifying windows anchored in ``anchors`` are read
            # from the old state: the rest stand.
            old_qual = inc.prior("W2_structuring", "qualifying", anchors)

            def qualifying(w):
                new = w2_qualifying_windows(
                    w2_band(_within(txns, widen(anchors, window, window))), n, hours
                )
                return old_qual.filter(~_in(col("_t"), anchors)).unionByName(
                    new.filter(_in(col("_t"), anchors))
                )

            def beneficiary(w):
                # An entity whose qualifying windows changed has its bursts
                # rebuilt from all of them; every other entity's stand.
                touched = (
                    old_qual.filter(_in(col("_t"), anchors))
                    .select("entity_id")
                    .unionByName(
                        w["qualifying"].filter(_in(col("_t"), anchors)).select("entity_id")
                    )
                    .distinct()
                )
                rebuilt = w2_bursts(
                    w["qualifying"].join(touched, "entity_id", "left_semi"), hours, cap
                )
                return (
                    prior["beneficiary"]
                    .join(touched, "entity_id", "left_anti")
                    .unionByName(rebuilt)
                )

            steps += [
                ("qualifying", qualifying, col("_t"), anchors),
                ("beneficiary", beneficiary),
            ]
            affected = merge_spans(windows + anchors)
        bounds = _bounds(cut, affected, read)
    return alerts_of, steps, bounds


def _w4(inc, txns, p, prior, mode, cut):
    from detection_rules import w4_alert_frame, w4_pairs

    hours, ratio = int(p["velocity_hours"]), float(p["forward_ratio"])
    per = p.get("alert_window_hours")

    def alerts_of_pairs(pairs):
        return w4_alert_frame(
            pairs, hours, ratio, p["run_id"], int(p["max_txns_per_alert"]), alert_window_hours=per
        )

    bounds: dict = {}
    steps: list = []
    if mode == "full" or not per:
        # Without alert windows an entity's alert reads all its pairs ever:
        # every pass is a full recompute.
        steps = [
            (
                "alerts",
                lambda w: alerts_of_pairs(w4_pairs(txns, hours, ratio)),
                _us(col("alert_ts")),
            )
        ]
    elif mode == "region":
        # A new row is a pair's credit or its payment, so the pair's payment
        # (ts_out) lies within velocity_hours after it (the extra second
        # covers the rule's whole-second arithmetic); an alert is one entity
        # in one window of payments, so only the windows of those payments
        # change. Their pairs' credits lie up to velocity_hours earlier.
        reach = hours * HOUR_US + 1_000_000
        affected = widen(cut, 0, reach, int(per) * HOUR_US)
        read = widen(affected, reach, 0)

        def alerts(w):
            pairs = w4_pairs(_within(txns, read), hours, ratio)
            new = alerts_of_pairs(pairs.filter(_in(_us(col("ts_out")), affected)))
            return (
                inc.prior("W4_risk_propagation", "alerts", affected)
                .filter(~_in(_us(col("alert_ts")), affected))
                .unionByName(new)
            )

        steps = [("alerts", alerts, _us(col("alert_ts")), affected)]
        # alert_ts is the window's last payment, inside its window.
        bounds = _bounds(cut, affected, read, write=affected)
    return (lambda s: s["alerts"]), steps, bounds


#: Passes in a row a screening pass carries a day whose payments' parties
#: still have no entity row: silver merges a batch's entities just after
#: its transactions, so one pass is the norm; the cap keeps a party that
#: never gets a row from re-reading its days every tick.
MAX_CARRY_PASSES = 3


def _days_with_unknown_party(txns: DataFrame, entities) -> set[int]:
    """Days (since the epoch) of the payments in ``txns`` whose originator
    or beneficiary has no row in ``entities`` yet: the screen needs the
    originator's customer flag and the beneficiary's country."""
    from pyspark.sql.functions import expr

    if entities is None:
        return set()
    ids = entities.select(col("entity_id").alias("_id"))
    day = expr(f"floor(unix_micros(txn_timestamp) / {DAY_US})").alias("d")
    missing = None
    for side in ("originator_id", "beneficiary_id"):
        m = txns.join(ids, col(side) == col("_id"), "left_anti").select(day)
        missing = m if missing is None else missing.unionByName(m)
    return {int(r["d"]) for r in missing.distinct().collect()}


def _day_spans(days) -> tuple:
    return merge_spans((d * DAY_US, (d + 1) * DAY_US) for d in days)


def _carry(inc, rule_id: str, txns: DataFrame, entities) -> None:
    """Stage the days this pass carries: each day with a payment whose
    party has no entity row yet, until it has been carried
    MAX_CARRY_PASSES passes in a row."""
    before = inc.carried.get(rule_id, {})
    inc.staged_pending[rule_id] = {
        d: before.get(d, 0) + 1
        for d in _days_with_unknown_party(txns, entities)
        if before.get(d, 0) < MAX_CARRY_PASSES
    }


def _screen(rule_id: str):
    """W5 or W6, one alert per payment (alert_ts its timestamp): a pass
    screens only the payments in the cut."""

    def run(inc, txns, p, prior, mode, cut):
        from detection_rules import get_rule

        fn = get_rule(rule_id)
        params = {k: v for k, v in p.items() if k != "screen_base"}
        bounds: dict = {}
        steps: list = []
        if mode == "full":
            _carry(inc, rule_id, txns, p.get("silver_entities"))
            steps = [("alerts", lambda w: fn(txns, **params), _us(col("alert_ts")))]
        elif mode == "region":
            new_txns = _within(txns, cut)
            # Days whose payments' parties are not all known yet: screened
            # again next pass.
            _carry(inc, rule_id, new_txns, p.get("silver_entities"))

            def alerts(w):
                new = fn(new_txns, **params)
                return (
                    inc.prior(rule_id, "alerts", cut)
                    .filter(~_in(_us(col("alert_ts")), cut))
                    .unionByName(new)
                )

            steps = [("alerts", alerts, _us(col("alert_ts")), cut)]
            bounds = _bounds(cut, cut, cut, write=cut)
        return (lambda s: s["alerts"]), steps, bounds

    return run


_RULES = {
    "W5_sanctions_match": _screen("W5_sanctions_match"),
    "W6_pep_counterparty": _screen("W6_pep_counterparty"),
    "W2_structuring": _w2,
    "W3_round_tripping": _w3,
    "W4_risk_propagation": _w4,
    "W17_layering_chain": _w17,
}
