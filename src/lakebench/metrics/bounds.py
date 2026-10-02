"""Lakebench limits that bound a run, and the trickle.

``BOUND_KINDS`` is the one registry of the kinds that may enter an
experiment's ``limits.bound_kinds`` (the identity key "Lakebench limits that
bound"); ``bound_entries`` is the one place both that list and the display
list ``limits.bound`` are derived from. A kind enters a record only when it
bound the run.

The trickle (``max_files_per_trigger``) is not a bound kind: it is set on
every continuous run, so putting it in ``bound_kinds`` would move every
continuous identity. ``trickle_bound`` says whether it held intake (SPEC
section 8: a trickle was set and the pipeline kept pace); the experiment
block records the answer in ``limits.trickle_bound`` and adds one line to
``limits.bound``. Readers of a stored record that lacks it call
``record_trickle_bound``.

Standard library only: the metric registry, which the collector imports at
class-definition time, imports these names.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# Kind patterns as experiment limits write them; ``*`` matches any job type.
BOUND_EXECUTOR_CAP = "*: executor cap"
BOUND_EXECUTOR_BUDGET = "*: concurrent executor budget"
BOUND_EXECUTOR_OVERRIDE = "*: executor override"
BOUND_AUTOSIZE = "auto-sizing cuts"
BOUND_RULE_CAP = "rule * cap"
BOUND_TM_ALERTS = "TM max_alerts_per_customer"
BOUND_MAINTENANCE = "pre-benchmark maintenance budget"
#: Not a bound kind (see the module docstring); readers pass it to
#: ``metric_registry.capped_by`` as ``extra`` when ``trickle_bound`` holds.
BOUND_TRICKLE = "trickle"
#: The ML loop's executor cap; it sits in the loop's own limits, never in the
#: pipeline's ``bound_kinds``.
BOUND_ML_LOOP_EXECUTOR_CAP = "ML loop executor cap"

#: ``limits.bound`` lines that come from the trickle start with this.
TRICKLE_LINE_PREFIX = "trickle:"


@dataclass(frozen=True)
class BoundKind:
    #: fnmatch pattern of the kind string.
    name: str
    #: A sizing limit (how much compute the run was given), as against a
    #: limit on what the workload did.
    sizing: bool
    description: str


BOUND_KINDS: tuple[BoundKind, ...] = (
    BoundKind(
        BOUND_EXECUTOR_CAP,
        False,
        "a job's executors held at its profile's cap below what the scale asks for",
    ),
    BoundKind(
        BOUND_EXECUTOR_BUDGET,
        True,
        "a continuous stream given fewer executors than requested by the concurrent budget",
    ),
    BoundKind(
        BOUND_EXECUTOR_OVERRIDE,
        True,
        "a job's executor count set by a config override instead of the scale",
    ),
    BoundKind(BOUND_AUTOSIZE, True, "Lakebench cut resources to fit the cluster"),
    BoundKind(BOUND_TM_ALERTS, False, "transaction monitoring dropped alerts over capacity"),
    BoundKind(BOUND_MAINTENANCE, False, "pre-benchmark maintenance stopped on its time budget"),
    BoundKind(BOUND_RULE_CAP, False, "an AML rule skipped on a Lakebench cap"),
)


def is_registered(kind: str) -> bool:
    """Whether *kind* matches a ``BOUND_KINDS`` entry."""
    return any(fnmatch.fnmatchcase(kind, k.name) for k in BOUND_KINDS)


def bound_entries(limits: Mapping[str, Any], rules: Mapping[str, Any]) -> list[tuple[str, str]]:
    """``(kind, display line)`` for each Lakebench limit that bound the run,
    in display order. ``limits.bound`` is the lines; ``limits.bound_kinds``
    the sorted distinct kinds (the counts in a line, such as a budget
    granted from live cluster capacity, vary between runs of one config).
    Raises ValueError for a kind ``BOUND_KINDS`` does not list."""
    out: list[tuple[str, str]] = []
    executors = list(limits.get("executors") or [])
    for x in executors:
        if x.get("cap_hit"):
            out.append(
                (
                    f"{x['job_type']}: executor cap",
                    f"{x['job_type']}: executor cap {x.get('cap')} "
                    f"(scale asks for {x.get('scale_derived')})",
                )
            )
    for x in executors:
        if x.get("budget_cap"):
            out.append(
                (
                    f"{x['job_type']}: concurrent executor budget",
                    f"{x['job_type']}: concurrent executor budget granted "
                    f"{x['budget_cap'].get('granted')} of {x['budget_cap'].get('requested')}",
                )
            )
    if limits.get("tm_alerts_over_capacity"):
        out.append(
            (
                BOUND_TM_ALERTS,
                f"TM max_alerts_per_customer ({limits.get('tm_max_alerts_per_customer')}): "
                f"{limits['tm_alerts_over_capacity']} alerts over capacity",
            )
        )
    out += [(BOUND_AUTOSIZE, f"auto-sizing: {c}") for c in limits.get("autosize_cuts") or []]
    if limits.get("maintenance_stopped"):
        out.append((BOUND_MAINTENANCE, "pre-benchmark maintenance stopped on its time budget"))
    for rule, why in (rules.get("skipped") or {}).items():
        if "cap" in str(why):
            out.append((f"rule {rule} cap", f"rule {rule} skipped: {why}"))
    for kind, _line in out:
        if not is_registered(kind):
            raise ValueError(f"bound kind {kind!r} is not in BOUND_KINDS")
    return out


# --- the trickle ---------------------------------------------------------------


def _num(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _interval_seconds(interval: Any) -> float | None:
    from lakebench.metrics.collector import _interval_seconds as parse

    return parse(interval)


def _from_record(rec: Mapping[str, Any]) -> dict[str, Any] | None:
    """The trickle inputs from a metrics.json dict, or None for a batch run."""
    pb = rec.get("pipeline_benchmark") or {}
    if pb.get("pipeline_mode") not in ("sustained", "continuous"):
        return None
    scores = pb.get("scores") or {}
    snapshot = rec.get("config_snapshot") or {}
    sustained = snapshot.get("sustained") or {}
    trickle = (rec.get("continuous") or {}).get("trickle") or {}
    inputs = snapshot.get("experiment_inputs") or {}
    value = next(
        (
            v
            for v in (
                trickle.get("value"),
                sustained.get("max_files_per_trigger"),
                (inputs.get("config_limits") or {}).get("max_files_per_trigger"),
            )
            if v is not None
        ),
        None,
    )
    bronze: Mapping[str, Any] = next(
        (s for s in rec.get("streaming") or [] if s.get("job_type") == "bronze-ingest"), {}
    )
    pb_snap = pb.get("config_snapshot") or {}
    return {
        "value": value,
        "source": trickle.get("source"),
        "ingest_ratio": _num(scores.get("ingest_ratio")),
        "released_rows": _num(scores.get("released_rows")),
        "corpus_ingest_ratio": _num(scores.get("corpus_ingest_ratio")),
        "window_s": _num(scores.get("window_seconds")),
        "last_write_offset_s": _num(bronze.get("last_write_offset_seconds")),
        "trigger_s": _interval_seconds(sustained.get("bronze_trigger_interval")),
        "corpus_rows": _num(pb_snap.get("datagen_output_rows")),
        "corpus_files": _num(pb_snap.get("datagen_output_files")),
    }


def _from_metrics(metrics: Any) -> dict[str, Any] | None:
    """The same inputs from a PipelineMetrics (a run being saved)."""
    pb = getattr(metrics, "pipeline_benchmark", None)
    if pb is None or getattr(pb, "pipeline_mode", None) not in ("sustained", "continuous"):
        return None
    snapshot = getattr(metrics, "config_snapshot", None) or {}
    sustained = snapshot.get("sustained") or {}
    trickle = (getattr(metrics, "continuous", None) or {}).get("trickle") or {}
    inputs = snapshot.get("experiment_inputs") or {}
    value = next(
        (
            v
            for v in (
                trickle.get("value"),
                sustained.get("max_files_per_trigger"),
                (inputs.get("config_limits") or {}).get("max_files_per_trigger"),
            )
            if v is not None
        ),
        None,
    )
    bronze = next(
        (s for s in getattr(metrics, "streaming", None) or [] if s.job_type == "bronze-ingest"),
        None,
    )
    pb_snap = getattr(pb, "config_snapshot", None) or {}
    window = _num(getattr(pb, "window_seconds", None))
    return {
        "value": value,
        "source": trickle.get("source"),
        "ingest_ratio": _num(getattr(pb, "ingest_ratio", None)),
        "released_rows": _num(getattr(pb, "released_rows", None)),
        "corpus_ingest_ratio": _num(getattr(pb, "corpus_ingest_ratio", None)),
        # As metrics.json records it, so a run and its saved record agree.
        "window_s": round(window, 1) if window is not None else None,
        "last_write_offset_s": _num(getattr(bronze, "last_write_offset_seconds", None)),
        "trigger_s": _interval_seconds(sustained.get("bronze_trigger_interval")),
        "corpus_rows": _num(pb_snap.get("datagen_output_rows")),
        "corpus_files": _num(pb_snap.get("datagen_output_files")),
    }


def trickle_bound(source: Any) -> dict[str, Any] | None:
    """Whether the trickle bounded a continuous run's intake, from a
    metrics.json dict or a PipelineMetrics.

    None for a batch run, a run with no trickle, and a run whose bronze fell
    behind what the trickle offered (``continuous_window.trickle_kept_pace``
    False). Otherwise ``{"kind": "trickle", "value", "source", "kept_pace",
    ...}``: ``kept_pace`` True, or None when an input to the test was not
    recorded (the trickle was set and the run cannot be shown to have fallen
    behind, so the number it holds is still not a capacity)."""
    from lakebench.metrics.continuous_window import trickle_kept_pace

    v = _from_record(source) if isinstance(source, Mapping) else _from_metrics(source)
    if v is None or v["value"] is None:
        return None
    ratio, released = v["ingest_ratio"], v["released_rows"]
    per_trigger = None
    if v["corpus_rows"] and v["corpus_files"]:
        per_trigger = float(v["value"]) * v["corpus_rows"] / v["corpus_files"]
    pace = trickle_kept_pace(
        # ingest_ratio is ingested over released only when released is known
        # (otherwise the collector falls back to the corpus share).
        ingested_rows=ratio * released if (ratio is not None and released) else None,
        released_rows=released,
        rows_per_trigger=per_trigger,
        corpus_taken=bool(v["corpus_ingest_ratio"] is not None and v["corpus_ingest_ratio"] >= 1),
        window_s=v["window_s"],
        last_write_offset_s=v["last_write_offset_s"],
        trigger_s=v["trigger_s"],
    )
    if pace["kept_pace"] is False:
        return None
    return {"kind": BOUND_TRICKLE, "value": v["value"], "source": v["source"], **pace}


def trickle_line(bound: Mapping[str, Any]) -> str:
    """The ``limits.bound`` line for a trickle bound."""
    source = f" ({bound['source']})" if bound.get("source") else ""
    head = f"{TRICKLE_LINE_PREFIX} max_files_per_trigger {bound.get('value')}{source}"
    if bound.get("kept_pace"):
        return f"{head}, the pipeline kept pace"
    return f"{head}; whether the pipeline kept pace was not measured"


def trickle_label(bound: Mapping[str, Any]) -> str:
    """What a card shows on a number the trickle bounds."""
    head = f"trickle {bound.get('value')} files per trigger"
    if bound.get("kept_pace"):
        return f"{head}; this is the offered load, not infrastructure capacity"
    return f"{head} set; whether the pipeline kept pace was not measured"


def record_trickle_bound(record: Any) -> dict[str, Any] | None:
    """The trickle bound of a stored record (a metrics.json dict or a loaded
    PipelineMetrics): ``limits.trickle_bound`` when its experiment block
    has the key, else computed from the record (a block written before it,
    which is never rebuilt)."""
    if isinstance(record, Mapping):
        exp = record.get("experiment")
    else:
        exp = getattr(record, "experiment", None)
    limits = (exp or {}).get("limits") if isinstance(exp, Mapping) else None
    if isinstance(limits, Mapping) and "trickle_bound" in limits:
        stored = limits["trickle_bound"]
        return dict(stored) if isinstance(stored, Mapping) else None
    return trickle_bound(record)
