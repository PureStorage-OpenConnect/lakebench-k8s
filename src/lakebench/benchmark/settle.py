"""Wait for storage to settle after batch maintenance.

expire_snapshots and remove_orphan_files at 0 s, then compaction, delete and
rewrite a large share of the tables' objects in a few minutes. The object
store keeps working on that burst after the SQL returns. On FlashBlade at
c360 scale 10 the same compacted files read QpH 546 about 2 minutes after
maintenance, 569 at +15 minutes and 841 at +35 minutes, against 828 before
maintenance (the pre round repeated within 0.3% across three runs). A post
round taken straight away measured the settling, not the compacted layout.

``wait_for_settle`` times one small, storage-bound probe query at a fixed
interval until two consecutive probes agree within a tolerance and, when a
pre-maintenance time for the same query is known, neither is slower than
that time by more than the tolerance. The second condition matters: at
+2 and +15 minutes the probes above agreed within 4% while still 33% slow,
so consecutive agreement alone would have accepted the slow plateau.

The bound against the pre-maintenance median widens to the probe query's
own noise (twice the median absolute deviation of the pre samples, so one
outlier cannot set it) when the pre round timed it three or more times; agreement between consecutive probes stays at the
configured tolerance: a query whose pre-maintenance samples spread 15%
cannot be held to 10% (DuckDB Q1 at scale 1, run 20260927-001340-6ab705,
waited 783 s on probes 2.8-3.1 s against a 2.6 s median). The widening is
capped at ``MAX_NOISE_TOLERANCE_PCT``, under the 27-34% slowdown of the one
measured unsettled store, and it never drops below the configured value.
Every probe in the settling pair must still be within it of the
pre-maintenance median.

The wait is kept out of every pipeline score: it runs after maintenance
has been timed and before the post round starts, and neither span is a
pipeline stage.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

# A probe that fails this many times in a row ends the wait: the query is
# broken or the engine is down, and waiting out the cap would not change it.
MAX_CONSECUTIVE_PROBE_FAILURES = 3
# Agreeing probes needed when there is no pre-maintenance time. More than
# two, because a slow plateau also agrees with itself; still no proof, so the
# result is recorded as unverified.
UNVERIFIED_STABLE_PROBES = 3
# Ceiling on the noise-derived tolerance. The unsettled FlashBlade rounds in
# in a live incident were 27-34% slower than before maintenance; 20% keeps them out
# whatever the probe query's own spread.
MAX_NOISE_TOLERANCE_PCT = 20.0


def _median(xs: list[float]) -> float:
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


# Samples needed before the bound widens: with two, one outlier is half the
# data and sets the spread on its own.
MIN_SPREAD_SAMPLES = 3


def reference_spread_pct(samples: list[float] | None) -> float | None:
    """Robust spread of the pre-maintenance samples: twice their median
    absolute deviation, as a percent of the median. None with fewer than
    ``MIN_SPREAD_SAMPLES`` positive samples.

    The median absolute deviation ignores a single outlier on either side:
    one slow sample (a GC pause, a cold read) or one fast one cannot open
    the bound; only noise that most samples share can.
    """
    xs = [float(x) for x in samples or [] if x is not None and x > 0]
    if len(xs) < MIN_SPREAD_SAMPLES:
        return None
    med = _median(xs)
    mad = _median([abs(x - med) for x in xs])
    return 2.0 * mad / med * 100.0


def effective_tolerance_pct(tolerance_pct: float, samples: list[float] | None) -> float:
    """The configured tolerance, widened to the reference samples' spread and
    capped at ``MAX_NOISE_TOLERANCE_PCT`` (never below the configured value)."""
    spread = reference_spread_pct(samples)
    if spread is None:
        return float(tolerance_pct)
    return max(float(tolerance_pct), min(spread, MAX_NOISE_TOLERANCE_PCT))


@dataclass
class SettleProbe:
    """One timed probe."""

    offset_seconds: float  # from maintenance end to the probe's start
    seconds: float | None  # probe time; None when the probe failed
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "offset_seconds": round(self.offset_seconds, 1),
            "seconds": None if self.seconds is None else round(self.seconds, 3),
        }
        if self.error:
            d["error"] = self.error
        return d


@dataclass
class SettleResult:
    """Outcome of the settle wait."""

    probe_query: str
    settled: bool
    # Maintenance end to the end of the probe that settled; when not
    # settled, maintenance end to when the wait gave up.
    settle_seconds: float
    capped: bool
    max_seconds: float
    tolerance_pct: float
    reference_seconds: float | None
    probes: list[SettleProbe] = field(default_factory=list)
    reason: str = ""  # why it did not settle; "" when settled
    # False when there was no pre-maintenance time to check against: the
    # probes agreed with each other, which a slow plateau also does.
    verified: bool = True
    # The pre-maintenance samples of the probe query, and the tolerance the
    # wait applied (tolerance_pct widened to their spread, capped).
    reference_samples: list[float] = field(default_factory=list)
    effective_tolerance_pct: float | None = None
    # Why the wait ran (for example how many maintenance statements ran).
    trigger: str = ""

    def value_reason(self) -> str:
        """Why the maintenance value cannot be reported, or "" when it can."""
        if self.settled:
            return ""
        if self.capped:
            msg = f"storage did not settle within {self.max_seconds:.0f} s"
            if self.reason and "slower than" in self.reason:
                msg += f" ({self.reason})"
            return msg
        return f"settle wait ended without settling: {self.reason}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "probe_query": self.probe_query,
            "settled": self.settled,
            "settle_seconds": round(self.settle_seconds, 1),
            "capped": self.capped,
            "max_seconds": self.max_seconds,
            "tolerance_pct": self.tolerance_pct,
            "reference_seconds": (
                None if self.reference_seconds is None else round(self.reference_seconds, 3)
            ),
            "probes": [p.to_dict() for p in self.probes],
            "reason": self.reason,
            "verified": self.verified,
            "reference_samples": [round(x, 3) for x in self.reference_samples],
            "reference_spread_pct": (
                None
                if (sp := reference_spread_pct(self.reference_samples)) is None
                else round(sp, 1)
            ),
            "effective_tolerance_pct": (
                self.tolerance_pct
                if self.effective_tolerance_pct is None
                else round(self.effective_tolerance_pct, 1)
            ),
            **({"trigger": self.trigger} if self.trigger else {}),
        }


def _within(a: float, b: float, tolerance_pct: float) -> bool:
    """True when a and b differ by at most tolerance_pct of the smaller."""
    return abs(a - b) <= min(a, b) * tolerance_pct / 100.0


def wait_for_settle(
    probe: Callable[[float], float],
    *,
    probe_query: str,
    started_at: float,
    max_seconds: float,
    interval_seconds: float,
    tolerance_pct: float,
    reference_seconds: float | None = None,
    reference_samples: list[float] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    on_probe: Callable[[SettleProbe], None] | None = None,
) -> SettleResult:
    """Probe until two consecutive probes agree, or the cap is reached.

    Args:
        probe: Runs the probe query once and returns its seconds; called
            with the seconds left before the cap, which it should use to
            bound its own timeout. Raises on failure (timeout, engine error).
        probe_query: Name recorded with the result.
        started_at: ``clock()`` value at maintenance end. Offsets and
            ``settle_seconds`` are measured from here.
        max_seconds: Cap on the wait, from ``started_at``. A probe is not
            started once the cap has passed.
        interval_seconds: Gap between the start of consecutive probes.
        tolerance_pct: Allowed difference between consecutive probes, and
            between a probe and ``reference_seconds``.
        reference_seconds: The same query's pre-maintenance time, or None
            when there was no pre round.
        reference_samples: The pre-maintenance samples behind
            ``reference_seconds``. With two or more, ``tolerance_pct`` widens
            to their spread, capped at ``MAX_NOISE_TOLERANCE_PCT``.
    """
    probes: list[SettleProbe] = []
    failures = 0
    has_reference = reference_seconds is not None and reference_seconds > 0
    ref_samples = list(reference_samples or []) if has_reference else []
    tol = effective_tolerance_pct(tolerance_pct, ref_samples) if has_reference else tolerance_pct
    need = 2 if has_reference else UNVERIFIED_STABLE_PROBES

    def _result(settled: bool, capped: bool, reason: str) -> SettleResult:
        return SettleResult(
            probe_query=probe_query,
            settled=settled,
            settle_seconds=max(0.0, clock() - started_at),
            capped=capped,
            max_seconds=float(max_seconds),
            tolerance_pct=float(tolerance_pct),
            reference_seconds=reference_seconds,
            probes=probes,
            reason=reason,
            verified=has_reference,
            reference_samples=ref_samples,
            effective_tolerance_pct=float(tol),
        )

    last_reason = ""

    def _near_reference(t: float) -> bool:
        if reference_seconds is None or reference_seconds <= 0:
            return True
        return t <= reference_seconds * (1 + tol / 100.0)

    while True:
        began = clock()
        offset = began - started_at
        if offset > max_seconds:
            return _result(False, True, last_reason or f"cap of {max_seconds:.0f} s reached")
        try:
            t = float(probe(max(0.0, max_seconds - offset)))
            p = SettleProbe(offset_seconds=offset, seconds=t)
            failures = 0
        except Exception as e:  # noqa: BLE001
            p = SettleProbe(offset_seconds=offset, seconds=None, error=str(e)[:200])
            failures += 1
        probes.append(p)
        if on_probe is not None:
            on_probe(p)

        if failures >= MAX_CONSECUTIVE_PROBE_FAILURES:
            return _result(
                False,
                False,
                f"probe {probe_query} failed {failures} times in a row ({p.error})",
            )
        recent = probes[-need:]
        window = [q.seconds for q in recent if q.seconds is not None]
        if len(recent) == need and len(window) == need:
            # Pair agreement stays at the configured tolerance; only the
            # bound against the pre-maintenance median widens.
            stable = all(_within(window[0], t, tolerance_pct) for t in window[1:]) and all(
                _within(a, b, tolerance_pct) for a, b in zip(window, window[1:], strict=False)
            )
            if stable and all(_near_reference(t) for t in window):
                return _result(True, False, "")
            last_reason = ""
            if stable:
                last_reason = (
                    f"probe stable at {window[-1]:.1f} s but slower than the "
                    f"pre-maintenance {reference_seconds:.1f} s by more than "
                    f"{tol:.3g}%; settling and a maintenance regression "
                    "are not separable"
                )

        # Next probe one interval after this one started, never before now.
        wait = began + interval_seconds - clock()
        if clock() + max(wait, 0.0) - started_at > max_seconds:
            # The next probe would start past the cap.
            return _result(False, True, last_reason or f"cap of {max_seconds:.0f} s reached")
        if wait > 0:
            sleep(wait)
