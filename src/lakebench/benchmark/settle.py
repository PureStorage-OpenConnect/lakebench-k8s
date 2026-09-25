"""Wait for storage to settle after batch maintenance (LB-150).

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

    def value_reason(self) -> str:
        """Why the maintenance value cannot be reported, or "" when it can."""
        if self.settled:
            return ""
        if self.capped:
            return f"storage did not settle within {self.max_seconds:.0f} s"
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
        }


def _within(a: float, b: float, tolerance_pct: float) -> bool:
    """True when a and b differ by at most tolerance_pct of the smaller."""
    return abs(a - b) <= min(a, b) * tolerance_pct / 100.0


def wait_for_settle(
    probe: Callable[[], float],
    *,
    probe_query: str,
    started_at: float,
    max_seconds: float,
    interval_seconds: float,
    tolerance_pct: float,
    reference_seconds: float | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    on_probe: Callable[[SettleProbe], None] | None = None,
) -> SettleResult:
    """Probe until two consecutive probes agree, or the cap is reached.

    Args:
        probe: Runs the probe query once and returns its seconds. Raises on
            failure (timeout, engine error).
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
    """
    probes: list[SettleProbe] = []
    failures = 0

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
        )

    def _near_reference(t: float) -> bool:
        if reference_seconds is None or reference_seconds <= 0:
            return True
        return t <= reference_seconds * (1 + tolerance_pct / 100.0)

    while True:
        began = clock()
        offset = began - started_at
        if offset > max_seconds:
            return _result(False, True, f"cap of {max_seconds:.0f} s reached")
        try:
            t = float(probe())
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
        if len(probes) >= 2:
            a, b = probes[-2].seconds, probes[-1].seconds
            if (
                a is not None
                and b is not None
                and _within(a, b, tolerance_pct)
                and _near_reference(a)
                and _near_reference(b)
            ):
                return _result(True, False, "")

        # Next probe one interval after this one started, never before now.
        wait = began + interval_seconds - clock()
        if clock() + max(wait, 0.0) - started_at > max_seconds:
            # The next probe would start past the cap.
            return _result(False, True, f"cap of {max_seconds:.0f} s reached")
        if wait > 0:
            sleep(wait)
