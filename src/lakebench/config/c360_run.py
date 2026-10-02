"""Customer 360 run rules read from the config, checked before Phase 1.

Standard library only, so the CLI can call it before any cluster call.
"""

from __future__ import annotations

from typing import Any

#: The config key the gold-finalize scripts read for a strategy override.
GOLD_STRATEGY_KEY = "spark.lb.gold.strategy"

#: Values ``spark.lb.gold.strategy`` may take. ``incremental`` is not one:
#: lakebench chooses it for multi-cycle cycles 2+ only, so a run never
#: aggregates less of silver than its identity says.
GOLD_STRATEGY_VALUES: tuple[str, ...] = ("auto", "simple_agg", "two_phase_agg")


def gold_override_problem(cfg: Any) -> str | None:
    """The refusal for a ``spark.lb.gold.strategy`` the gold scripts will
    not run, or None. The scripts refuse the same values (exit 1 before any
    write); this catches them before anything is deployed or submitted."""
    conf = getattr(getattr(cfg, "spark", None), "conf", None) or {}
    value = conf.get(GOLD_STRATEGY_KEY)
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
