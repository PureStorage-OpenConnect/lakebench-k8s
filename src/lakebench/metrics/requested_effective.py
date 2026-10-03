"""Requested and effective values of a run, and their mismatches.

A run can ask for one thing and do another: a gold strategy chosen by size
when the config asked for none, a trickle resolved from the corpus, an
executor override the cluster did not grant, a batch config run in
continuous mode. Each decision is recorded as an entry ``{requested,
effective, source, stage}`` read from fields the record already carries:

- ``gold_strategy`` (Customer 360): the strategy the config names in
  ``spark.lb.gold.strategy`` (``config_snapshot.requested.gold_strategy``,
  "auto" when unset), and the strategy and its source each gold-finalize
  job wrote in its JOB METRICS block (``jobs[].extra_metrics.gold_strategy``
  and ``gold_strategy_source``: ``auto``, ``override`` or ``cycle``). One
  entry per gold job, keyed ``gold_strategy[cycle=N]`` when there are
  several.
- ``pipeline_mode``: the mode the command asked for
  (``experiment_inputs.run_mode``: the config's, or ``run --continuous``)
  against the pipeline that ran (``pipeline_benchmark.pipeline_mode``).
- ``executors[<job type>]``: the override, else the profile's count at this
  scale under its cap, against the count the job ran with
  (``experiment.limits.executors``). The executor cap and a concurrent
  budget show in the entry's source; the limits they impose are labelled by
  ``limits.bound``.
- ``trickle`` (continuous): ``max_files_per_trigger`` as configured, or
  "auto", against the value the run resolved (``continuous.trickle``).

The other Lakebench caps (W1 vertices, TM alerts per customer, benchmark
iterations) are recorded as configured in ``experiment.limits`` and, when
one bounds the run, in ``limits.bound``; no run resolves them to another
value, so they have no entry here.

A mismatch is an entry whose request was explicit and not met, or an
"auto" request that resolved to a value labelled even when chosen
automatically (``LABEL_WHEN_AUTO``: incremental gold, which aggregates part
of silver). Incremental gold chosen by Lakebench for cycles 2+ of a
multi-cycle run (source ``cycle``) is by design and not a mismatch. A
mismatch is a verdict warning and qualifier; it never fails a run and never
enters identity.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

NOT_RECORDED = "not recorded"

#: Values an "auto" request may resolve to that are still labelled.
LABEL_WHEN_AUTO: dict[str, frozenset[str]] = {"gold_strategy": frozenset({"incremental"})}

#: Sources whose outcome is by design for the key (never a mismatch).
EXEMPT_SOURCES: dict[str, frozenset[str]] = {"gold_strategy": frozenset({"cycle"})}

#: Requests that leave the choice to Lakebench.
_AUTO = frozenset({None, "auto", "profile"})

#: Every function whose name says it chooses a strategy, trickle, executor
#: count, cap or mode (``tests/test_requested_effective_sites.py`` walks the
#: tree for them), and where its decision reaches the record. A new site
#: must be listed here with its carrier, or the test fails.
KNOWN_SITES: dict[tuple[str, str], str] = {
    ("cli/_sustained.py", "resolve_trickle"): "record: continuous.trickle {value, source}",
    ("spark/scripts/gold_finalize.py", "determine_gold_strategy"): (
        "record: JOB METRICS gold_strategy, gold_strategy_source"
    ),
    ("spark/scripts/gold_finalize.py", "select_gold_strategy"): (
        "returns into: determine_gold_strategy"
    ),
    ("spark/scripts/gold_finalize_delta.py", "determine_gold_strategy"): (
        "record: JOB METRICS gold_strategy, gold_strategy_source"
    ),
    ("spark/scripts/gold_finalize_delta.py", "select_gold_strategy"): (
        "returns into: determine_gold_strategy"
    ),
    ("spark/scripts/silver_build.py", "determine_silver_strategy"): (
        "exempt: both silver strategies write the same rows; the choice "
        "changes only how silver is computed and is logged on the Strategy line"
    ),
    ("spark/scripts/silver_build.py", "select_silver_strategy"): (
        "returns into: determine_silver_strategy"
    ),
    ("spark/scripts/silver_build_delta.py", "determine_silver_strategy"): (
        "exempt: both silver strategies write the same rows; the choice "
        "changes only how silver is computed and is logged on the Strategy line"
    ),
    ("spark/scripts/silver_build_delta.py", "select_silver_strategy"): (
        "returns into: determine_silver_strategy"
    ),
}


def _entry(requested: Any, effective: Any, source: Any, stage: str | None) -> dict[str, Any]:
    return {"requested": requested, "effective": effective, "source": source, "stage": stage}


def _norm_mode(mode: Any) -> Any:
    return "continuous" if mode in ("continuous", "sustained") else mode


def _gold_entries(metrics: Any) -> dict[str, dict[str, Any]]:
    snapshot = getattr(metrics, "config_snapshot", None) or {}
    if snapshot.get("workload_schema") not in (None, "customer360"):
        return {}
    configured = (snapshot.get("requested") or {}).get("gold_strategy")
    gold = [j for j in getattr(metrics, "jobs", None) or [] if j.job_type == "gold-finalize"]
    out: dict[str, dict[str, Any]] = {}
    for idx, job in enumerate(gold, start=1):
        extra = getattr(job, "extra_metrics", None) or {}
        effective = extra.get("gold_strategy") or NOT_RECORDED
        source = extra.get("gold_strategy_source")
        if configured is not None:
            requested = configured
        elif source == "override":
            # The script saw an override; it is the strategy it ran.
            requested = effective
        elif source == "auto":
            requested = "auto"
        else:
            requested = NOT_RECORDED
        key = "gold_strategy" if len(gold) == 1 else f"gold_strategy[cycle={idx}]"
        out[key] = _entry(requested, effective, source, "gold-finalize")
    return out


def _mode_entry(metrics: Any) -> dict[str, dict[str, Any]]:
    snapshot = getattr(metrics, "config_snapshot", None) or {}
    inputs = snapshot.get("experiment_inputs") or {}
    configured = _norm_mode(inputs.get("mode"))
    requested = _norm_mode(inputs.get("run_mode")) or configured
    if requested is None:
        return {}
    pb = getattr(metrics, "pipeline_benchmark", None)
    effective = _norm_mode(getattr(pb, "pipeline_mode", None))
    if effective is None:
        if getattr(metrics, "streaming", None):
            effective = "continuous"
        elif getattr(metrics, "jobs", None):
            effective = "batch"
        else:
            effective = NOT_RECORDED
    source = "config" if requested == configured else "command line"
    return {"pipeline_mode": _entry(requested, effective, source, None)}


def _executor_entries(limits: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for e in (limits or {}).get("executors") or []:
        if not isinstance(e, Mapping) or not e.get("job_type"):
            continue
        override = e.get("override")
        if override is not None:
            requested: Any = int(override)
            source = "override"
        else:
            requested = "profile"
            source = "profile"
            if e.get("cap_hit"):
                source = f"profile, capped at {e.get('cap')}"
        if isinstance(e.get("budget_cap"), Mapping):
            source += f", concurrent budget granted {e['budget_cap'].get('granted')}"
        observed = e.get("observed")
        out[f"executors[{e['job_type']}]"] = _entry(
            requested,
            observed if observed is not None else NOT_RECORDED,
            source,
            str(e["job_type"]),
        )
    return out


def _trickle_entry(metrics: Any) -> dict[str, dict[str, Any]]:
    trickle = (getattr(metrics, "continuous", None) or {}).get("trickle")
    if not isinstance(trickle, Mapping) or "value" not in trickle:
        return {}
    source = trickle.get("source")
    requested = trickle.get("value") if source == "config" else "auto"
    return {"trickle": _entry(requested, trickle.get("value"), source, "bronze-ingest")}


def derive(metrics: Any, limits: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    """The requested and effective entries of *metrics* (a PipelineMetrics,
    fresh or loaded from a stored record). *limits* is the experiment
    block's ``limits`` (the stored block's when omitted)."""
    if limits is None:
        limits = (getattr(metrics, "experiment", None) or {}).get("limits")
    entries: dict[str, dict[str, Any]] = {}
    entries.update(_gold_entries(metrics))
    entries.update(_mode_entry(metrics))
    entries.update(_executor_entries(limits))
    entries.update(_trickle_entry(metrics))
    return entries


def _base_key(key: str) -> str:
    return key.split("[", 1)[0]


def is_mismatch(key: str, entry: Mapping[str, Any]) -> bool:
    """Whether *entry* is a request not met (see the module docstring)."""
    base = _base_key(key)
    requested, effective = entry.get("requested"), entry.get("effective")
    if NOT_RECORDED in (requested, effective):
        return False
    if entry.get("source") in EXEMPT_SOURCES.get(base, frozenset()):
        return False
    if requested in _AUTO:
        return effective in LABEL_WHEN_AUTO.get(base, frozenset())
    return str(requested) != str(effective)


def mismatches(entries: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """The keys of *entries* that are mismatches, in entry order."""
    return [k for k, e in entries.items() if isinstance(e, Mapping) and is_mismatch(k, e)]


def stored_or_derived(metrics: Any) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """The entries and mismatch keys of a record: the stored block's when it
    carries them (a record written by this version), else derived from the
    record's fields (a record from before them)."""
    exp = getattr(metrics, "experiment", None) or {}
    stored = exp.get("requested_effective")
    if isinstance(stored, Mapping):
        keys = exp.get("requested_effective_mismatches")
        entries = {str(k): dict(v) for k, v in stored.items() if isinstance(v, Mapping)}
        if isinstance(keys, list):
            return entries, [str(k) for k in keys]
        return entries, mismatches(entries)
    entries = derive(metrics)
    return entries, mismatches(entries)


def warning_line(key: str, entry: Mapping[str, Any]) -> str:
    """One line for the badge and the report: what was asked, what ran."""
    source = entry.get("source")
    return f"{key}: requested {entry.get('requested')}, ran {entry.get('effective')}" + (
        f" ({source})" if source else ""
    )
