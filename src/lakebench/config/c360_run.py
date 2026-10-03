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
