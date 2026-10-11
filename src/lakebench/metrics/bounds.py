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
#: The binding-caps line for a continuous run whose intake datagen's
#: offered load set (``datagen_line``) starts with this. Like the trickle it
#: is not a bound kind: it labels the throughputs, not the run's identity.
DATAGEN_LINE_PREFIX = "datagen offered load:"
#: ``limits.bound`` lines for a stage's trigger interval start with this.
TRIGGER_LINE_PREFIX = "trigger interval:"
#: Each stream and its interval key in the config snapshot's ``sustained``.
_TRIGGER_KEYS = (
    ("bronze-ingest", "bronze_trigger_interval"),
    ("silver-stream", "silver_trigger_interval"),
    ("gold-refresh", "gold_refresh_interval"),
)


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


def bound_entries(
    limits: Mapping[str, Any], rules: Mapping[str, Any], *, strict: bool = False
) -> list[tuple[str, str]]:
    """``(kind, display line)`` for each Lakebench limit that bound the run,
    in display order. ``limits.bound`` is the lines; ``limits.bound_kinds``
    the sorted distinct kinds (the counts in a line, such as a budget
    granted from live cluster capacity, vary between runs of one config).
    With *strict* (the tests), raises ValueError for a kind ``BOUND_KINDS``
    does not list; a run saving its record never fails on one."""
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
        if x.get("override_bound"):
            asks = min(int(x.get("scale_derived") or 0), int(x.get("cap") or 0))
            out.append(
                (
                    f"{x['job_type']}: executor override",
                    f"{x['job_type']}: executor override {x.get('override')} (profile asks {asks})",
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
    if strict:
        for kind, _line in out:
            if not is_registered(kind):
                raise ValueError(f"bound kind {kind!r} is not in BOUND_KINDS")
    return out


def evidence_cap_lines(capped: Any) -> list[str]:
    """One line per AML typology whose recall the evidence cap bounds, from
    ``financial_scoring.recall_bounded_by_evidence_cap`` (typology -> its
    designated rules with an alert cut at max_txns_per_alert). A binding-caps
    line, not a bound kind: it bounds a workload result (recall), which no
    registry metric carries."""
    if not isinstance(capped, Mapping):
        return []
    return [
        f"evidence cap: {typ} recall bounded ({', '.join(str(r) for r in rules)} "
        "alerts cut at max_txns_per_alert)"
        for typ, rules in sorted(capped.items())
        if rules
    ]


def datagen_line(source: Any) -> str | None:
    """The binding-caps line for a continuous run whose intake was set by
    the run's own datagen, from a metrics.json dict or a PipelineMetrics;
    None otherwise. Datagen sets it when bronze took about every row datagen
    wrote (``datagen_ahead`` False: no backlog, so rows/s and the paces are
    datagen's rate, not the pipeline's capacity), or when datagen offered
    less than it was sized for (``stage_capacity.datagen.fell_short``)."""
    if isinstance(source, Mapping):
        pb = source.get("pipeline_benchmark") or {}
        if not isinstance(pb, Mapping):
            return None
        mode = pb.get("pipeline_mode")
        ahead = (pb.get("scores") or {}).get("datagen_ahead")
        capacity = pb.get("stage_capacity") or {}
    else:
        pb = getattr(source, "pipeline_benchmark", None)
        if pb is None:
            return None
        mode = getattr(pb, "pipeline_mode", None)
        ahead = getattr(pb, "datagen_ahead", None)
        capacity = getattr(pb, "stage_capacity", None) or {}
    if mode not in ("sustained", "continuous"):
        return None
    dg = capacity.get("datagen") if isinstance(capacity, Mapping) else None
    dg = dg if isinstance(dg, Mapping) else {}
    if dg.get("fell_short"):
        return (
            f"{DATAGEN_LINE_PREFIX} datagen wrote {dg.get('measured_mb_s')} MB/s of the "
            f"{dg.get('sized_mb_s')} MB/s it was sized for; rows/s and the paces are its "
            "rate, not the pipeline's capacity"
        )
    if ahead is False:
        return (
            f"{DATAGEN_LINE_PREFIX} bronze took about every row datagen wrote (no backlog); "
            "rows/s and the paces are datagen's rate, not the pipeline's capacity"
        )
    return None


def trigger_lines(snapshot: Mapping[str, Any], *, trickle: bool) -> list[str]:
    """One line per continuous stream on a timer (``limits.trigger_bound``):
    a Lakebench setting (default or config), not the stack, sets how stale
    its output can get, so the line labels the freshness numbers. It is not a
    bound kind (the interval is in the config already). Bronze is left out
    under a *trickle*, whose line names its cadence."""
    sustained = snapshot.get("sustained") or {}
    out = []
    for stage, key in _TRIGGER_KEYS:
        seconds = _interval_seconds(sustained.get(key))
        if seconds and not (trickle and stage == "bronze-ingest"):
            out.append(
                f"{TRIGGER_LINE_PREFIX} {stage} runs every {seconds:g} s ({key}): between "
                "runs its output is up to that much older than the freshness measured at each run"
            )
    return out


# --- the trickle ---------------------------------------------------------------


def _num(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _interval_seconds(interval: Any) -> float | None:
    from lakebench.metrics.collector import _interval_seconds as parse

    return parse(interval)


def _trickle_value(snapshot: Mapping[str, Any], trickle: Mapping[str, Any]) -> Any:
    """max_files_per_trigger as the run resolved it (``continuous.trickle``),
    else as its config snapshot recorded it."""
    inputs = snapshot.get("experiment_inputs") or {}
    for v in (
        trickle.get("value"),
        (snapshot.get("sustained") or {}).get("max_files_per_trigger"),
        (inputs.get("config_limits") or {}).get("max_files_per_trigger"),
    ):
        if v is not None:
            return v
    return None


def _round4(value: Any) -> float | None:
    """As the record's scores keep ratios (4 places), so a run being saved
    and its saved record give one answer."""
    v = _num(value)
    return round(v, 4) if v is not None else None


def _from_record(rec: Mapping[str, Any]) -> dict[str, Any] | None:
    """The trickle inputs from a metrics.json dict, or None for a batch run."""
    pb = rec.get("pipeline_benchmark") or {}
    if pb.get("pipeline_mode") not in ("sustained", "continuous"):
        return None
    scores = pb.get("scores") or {}
    snapshot = rec.get("config_snapshot") or {}
    trickle = (rec.get("continuous") or {}).get("trickle") or {}
    bronze: Mapping[str, Any] = next(
        (s for s in rec.get("streaming") or [] if s.get("job_type") == "bronze-ingest"), {}
    )
    pb_snap = pb.get("config_snapshot") or {}
    return {
        "value": _trickle_value(snapshot, trickle),
        "source": trickle.get("source"),
        "ingest_ratio": _round4(scores.get("ingest_ratio")),
        "released_rows": _num(scores.get("released_rows")),
        "intake_limit": scores.get("intake_limit"),
        "window_s": _num(scores.get("window_seconds")),
        "last_write_offset_s": _num(bronze.get("last_write_offset_seconds")),
        "trigger_s": _interval_seconds(
            (snapshot.get("sustained") or {}).get("bronze_trigger_interval")
        ),
        "corpus_rows": _num(pb_snap.get("datagen_output_rows")),
        "corpus_files": _num(pb_snap.get("datagen_output_files")),
    }


def _from_metrics(metrics: Any) -> dict[str, Any] | None:
    """The same inputs from a PipelineMetrics (a run being saved)."""
    pb = getattr(metrics, "pipeline_benchmark", None)
    if pb is None or getattr(pb, "pipeline_mode", None) not in ("sustained", "continuous"):
        return None
    snapshot = getattr(metrics, "config_snapshot", None) or {}
    trickle = (getattr(metrics, "continuous", None) or {}).get("trickle") or {}
    bronze = next(
        (s for s in getattr(metrics, "streaming", None) or [] if s.job_type == "bronze-ingest"),
        None,
    )
    pb_snap = getattr(pb, "config_snapshot", None) or {}
    window = _num(getattr(pb, "window_seconds", None))
    return {
        "value": _trickle_value(snapshot, trickle),
        "source": trickle.get("source"),
        "ingest_ratio": _round4(getattr(pb, "ingest_ratio", None)),
        "released_rows": _num(getattr(pb, "released_rows", None)),
        "intake_limit": getattr(pb, "intake_limit", None),
        # As metrics.json records it (0.1 s).
        "window_s": round(window, 1) if window is not None else None,
        "last_write_offset_s": _num(getattr(bronze, "last_write_offset_seconds", None)),
        "trigger_s": _interval_seconds(
            (snapshot.get("sustained") or {}).get("bronze_trigger_interval")
        ),
        "corpus_rows": _num(pb_snap.get("datagen_output_rows")),
        "corpus_files": _num(pb_snap.get("datagen_output_files")),
    }


def trickle_bound(source: Any) -> dict[str, Any] | None:
    """Whether the trickle bounded a continuous run's intake, from a
    metrics.json dict or a PipelineMetrics.

    None for a batch run, a run with no trickle, and a run whose bronze fell
    behind what the trickle offered (``continuous_window.trickle_kept_pace``
    False), unless the collector's own ``intake_limit`` says the trickle held
    intake. Otherwise ``{"kind": "trickle", "value", "source", "kept_pace",
    ...}``: ``kept_pace`` True, or None when it was not shown either way (an
    input missing, or ``intake_limit`` and the 0.99 test disagree). The
    number the trickle holds is then still not a capacity."""
    from lakebench.metrics.continuous_window import trickle_kept_pace

    v = _from_record(source) if isinstance(source, Mapping) else _from_metrics(source)
    if v is None or v["value"] is None:
        return None
    ratio, released = v["ingest_ratio"], v["released_rows"]
    # ingest_ratio is ingested over released only when released is known
    # (otherwise the collector falls back to the corpus share).
    ingested = ratio * released if (ratio is not None and released) else None
    pace = trickle_kept_pace(
        ingested_rows=round(ingested) if ingested is not None else None,
        released_rows=released,
        corpus_taken=bool(
            ingested is not None and v["corpus_rows"] and ingested >= v["corpus_rows"]
        ),
        window_s=v["window_s"],
        last_write_offset_s=v["last_write_offset_s"],
        trigger_s=v["trigger_s"],
    )
    if pace["kept_pace"] is False:
        per_trigger = (
            float(v["value"]) * v["corpus_rows"] / v["corpus_files"]
            if v["corpus_rows"] and v["corpus_files"]
            else None
        )
        within_one_batch = bool(
            ingested is not None
            and per_trigger
            and released - ingested <= per_trigger
            and pace["lag_s"] is not None
            and v["trigger_s"]
            and pace["lag_s"] <= v["trigger_s"] + 0.05
        )
        if v["intake_limit"] == "trickle_rate":
            why = (
                "ingested rows were under 0.99 of the offered rows, but intake_limit says the "
                "trickle held intake"
            )
        elif within_one_batch:
            # released_rows counts the trigger at the window's edge: its batch
            # may be in flight. Not shown to keep pace, not shown capacity.
            why = (
                "ingested rows are within one trigger's batch of the offered rows and the lag "
                "is within one trigger: the last batch may have been in flight"
            )
        else:
            return None
        pace["kept_pace"] = None
        pace["not_measured"] = why
    return {"kind": BOUND_TRICKLE, "value": v["value"], "source": v["source"], **pace}


def _value_text(bound: Mapping[str, Any]) -> str:
    value = bound.get("value")
    return "unknown" if value is None else str(value)


def trickle_line(bound: Mapping[str, Any]) -> str:
    """The ``limits.bound`` line for a trickle bound."""
    source = f" ({bound['source']})" if bound.get("source") else ""
    head = f"{TRICKLE_LINE_PREFIX} max_files_per_trigger {_value_text(bound)}{source}"
    if bound.get("kept_pace"):
        return f"{head}, the pipeline kept pace"
    return f"{head}; the pipeline was not shown to keep pace"


def trickle_label(bound: Mapping[str, Any]) -> str:
    """What a card shows on a number the trickle bounds."""
    head = f"trickle {_value_text(bound)} files per trigger"
    if bound.get("kept_pace"):
        return f"{head}; this is the offered load, not infrastructure capacity"
    return f"{head} set; the pipeline was not shown to keep pace, so this is not a capacity"


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
    try:
        return trickle_bound(record)
    except Exception:  # noqa: BLE001 -- unreadable: a continuous run is not shown as capacity
        mode = (
            (record.get("pipeline_benchmark") or {}).get("pipeline_mode")
            if isinstance(record, Mapping)
            else getattr(getattr(record, "pipeline_benchmark", None), "pipeline_mode", None)
        )
        if mode in ("sustained", "continuous"):
            return {
                "kind": BOUND_TRICKLE,
                "value": None,
                "source": None,
                "kept_pace": None,
                "ingested_rows": None,
                "offered_rows": None,
                "ratio": None,
                "lag_s": None,
                "trigger_s": None,
                "not_measured": "the trickle inputs could not be read",
            }
        return None


def binding_caps(record: Any) -> list[str]:
    """Every Lakebench limit that bound the run, one line each: the stored
    ``limits.bound`` lines and the trickle line, whether or not the stored
    block has it (a record from before it gets it computed). For lists and
    counts; a figure takes only the caps that bound it."""
    if isinstance(record, Mapping):
        exp = record.get("experiment")
    else:
        block = getattr(record, "experiment_block", None)
        exp = block() if callable(block) else getattr(record, "experiment", None)
    limits = (exp or {}).get("limits") if isinstance(exp, Mapping) else None
    lines = [
        str(x)
        for x in ((limits or {}).get("bound") or [])
        if x and not str(x).startswith(TRICKLE_LINE_PREFIX)
    ]
    tb = record_trickle_bound(record)
    if tb:
        lines.append(trickle_line(tb))
    dg = datagen_line(record)
    if dg:
        lines.append(dg)
    # The evidence cap on AML recall, from the scoring record.
    scoring = (
        record.get("financial_scoring")
        if isinstance(record, Mapping)
        else getattr(record, "financial_scoring", None)
    )
    if isinstance(scoring, Mapping):
        lines += [
            line
            for line in evidence_cap_lines(scoring.get("recall_bounded_by_evidence_cap"))
            if line not in lines
        ]
    return lines


def trickle_note(record: Any) -> str:
    """A plain-text suffix for a rows/s figure the trickle held (CLI
    output), or "" when it did not."""
    tb = record_trickle_bound(record)
    if not tb:
        return ""
    if tb.get("kept_pace"):
        return " (BOUNDED BY trickle: offered load, not capacity)"
    return " (BOUNDED BY trickle: not shown to keep pace, not a capacity)"
