"""Customer 360 run rules read from the config.

Standard library only, so the config schema imports it without a cycle.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: The config key the gold-finalize scripts read for a strategy override.
GOLD_STRATEGY_KEY = "spark.lb.gold.strategy"

#: Values ``spark.lb.gold.strategy`` may take. ``incremental`` is not one:
#: lakebench chooses it for multi-cycle cycles 2+ only, so a run never
#: aggregates less of silver than its identity says.
GOLD_STRATEGY_VALUES: tuple[str, ...] = ("auto", "simple_agg", "two_phase_agg")


def gold_override_problem(conf: Mapping[str, Any]) -> str | None:
    """The refusal for a ``spark.lb.gold.strategy`` in *conf* (the config's
    ``spark.conf``) the gold scripts will not run, or None. The scripts
    refuse the same values (exit 1 before any write); the config load
    (``schema.LakebenchConfig``) refuses them before anything deploys."""
    value = (conf or {}).get(GOLD_STRATEGY_KEY)
    if value is None:
        return None
    norm = str(value).strip().lower()
    if norm == "incremental":
        return (
            f"{GOLD_STRATEGY_KEY}=incremental is refused: incremental gold is chosen "
            "by lakebench for multi-cycle cycles 2+ only"
        )
    if norm not in GOLD_STRATEGY_VALUES and norm != "":
        return f"{GOLD_STRATEGY_KEY}={value!r} names no gold strategy"
    return None


#: Event-time window defaults (config ``datagen.timestamp_start`` and
#: ``timestamp_end`` unset). A single-cycle generate uses the generator's own
#: default end (``datagen_rs`` ``generate.rs``: 2025-01-01); a multi-cycle run
#: splits the wider window below across its cycles.
SERIES_START = "2024-01-01"
SINGLE_END = "2025-01-01"
MULTI_END = "2025-12-31"


def _day(value: Any) -> str | None:
    """A configured window bound as ``YYYY-MM-DD`` (its first ten
    characters, as the correctness check has always read it), or None."""
    if value is None:
        return None
    text = str(value).strip()[:10]
    return text or None


def cycle_windows(total: int, ts_start: Any = None, ts_end: Any = None) -> list[tuple[str, str]]:
    """The ``(start, end)`` event-time window of every cycle, in order: the
    configured range (defaults above) split into ``total`` non-overlapping,
    chronological windows, the last taking the remainder. The one copy of
    the window rule: the datagen deployer, the series marker and the
    correctness check all read it.

    The chronological, non-overlapping property is load-bearing for the c360
    gold non-degeneracy gate (gold_finalize*.py gold_date_coverage_problem):
    the incremental strategy recomputes silver dates on or after the gold
    watermark and keeps older gold rows, so it covers every date only while
    each cycle's dates are at or after the prior cycle's. If this ever admits
    overlapping or backfilled dates, update that gate with it.
    """
    from datetime import datetime, timedelta

    if int(total) < 1:
        raise ValueError(f"a run has at least one cycle, not {total}")
    total = int(total)
    start = datetime.strptime(_day(ts_start) or SERIES_START, "%Y-%m-%d")
    default_end = SINGLE_END if total == 1 else MULTI_END
    end = datetime.strptime(_day(ts_end) or default_end, "%Y-%m-%d")
    per = (end - start).days // total
    out: list[tuple[str, str]] = []
    for i in range(total):
        lo = start + timedelta(days=per * i)
        hi = end if i == total - 1 else start + timedelta(days=per * (i + 1))
        out.append((lo.strftime("%Y-%m-%d"), hi.strftime("%Y-%m-%d")))
    return out


def run_cycles(cfg: Any) -> int:
    """The config's cycle count (``architecture.pipeline.cycles``), at least 1."""
    try:
        return max(1, int(getattr(cfg.architecture.pipeline, "cycles", 1) or 1))
    except (TypeError, ValueError):
        return 1


def config_windows(cfg: Any) -> list[tuple[str, str]]:
    """``cycle_windows`` for *cfg*'s cycle count and datagen window."""
    dg = cfg.architecture.workload.datagen
    return cycle_windows(run_cycles(cfg), dg.timestamp_start, dg.timestamp_end)


def series_clock(cfg: Any) -> tuple[str, str]:
    """The event-time range the run's cycles cover: the first window's start
    and the last window's (exclusive) end."""
    windows = config_windows(cfg)
    return windows[0][0], windows[-1][1]
