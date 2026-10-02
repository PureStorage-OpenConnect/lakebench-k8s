"""Shared formatter for a headline measurement.

Every score card that prints a number pulled from a run's evidence goes
through :func:`format_measurement`. It attaches, next to the number:

- ``BOUNDED BY: <cap>`` when a Lakebench cap held the run, so the reader
  cannot mistake the capped figure for what the system can do (invariant
  6); the cap name (``_MAX_EXECUTORS_SAFE=28``, ``w1_max_vertices=8M``,
  the concurrent executor budget, etc.) is next to the number.
- ``n=<N>`` when the score was measured over more than one sample, so a
  repeated measurement is labelled and an ``n=1`` is not silently sold as
  repeated (invariant 7).
- A support-state pill (``supported`` / ``unverified`` / ``unsupported``)
  when set, so a figure from an unsupported combination reads as such.

The lint at ``scripts/lint_capped_bare_numbers.py`` scans the report
generator for bare ``rows_per_s`` / ``qph`` prints that would bypass this
formatter and paint a capped or single-run figure as headline evidence.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from html import escape as _html_escape

_SUPPORT_STATE_LABEL = {
    "supported": "supported",
    "unverified": "unverified",
    "unsupported": "unsupported",
}


def _cap_short_name(cap_line: str) -> str:
    """Short name for a ``limits.bound`` line.

    The bound lines carry the reason (``bronze-verify: executor cap 28
    (scale asks for 40)``); the cap name we surface is the label the
    reader can grep for in the code (``_MAX_EXECUTORS_SAFE=28``,
    ``concurrent executor budget``, ``w1_max_vertices``, etc.).

    Job profiles cap executors at different values (10, 20, 28), so the
    surfaced number must come from the line, not a hardcoded value; a
    wrong cap number would mislabel which cap held the run and breach
    invariant 6.
    """
    import re as _re

    text = str(cap_line)
    if "executor cap" in text:
        m = _re.search(r"executor cap\s+(\d+)", text)
        cap = m.group(1) if m else "?"
        return f"_MAX_EXECUTORS_SAFE={cap}"
    if "concurrent executor budget" in text:
        return "concurrent executor budget"
    if "TM max_alerts_per_customer" in text:
        return "tm_max_alerts_per_customer"
    if "w1_max_vertices" in text.lower():
        return "w1_max_vertices"
    if text.startswith("auto-sizing:"):
        return "auto-sizing cut"
    if text.startswith("pre-benchmark maintenance"):
        return "pre-benchmark maintenance budget"
    if text.startswith("rule "):
        # "rule <name> skipped: ..." -> "rule <name> cap"
        parts = text.split()
        if len(parts) >= 2:
            return f"rule {parts[1]} cap"
    return text


def format_measurement(
    value: str,
    unit: str = "",
    *,
    caps_bound: Sequence[str] | None = None,
    n_runs: int | None = None,
    spread: float | None = None,
    support_state: str | None = None,
) -> str:
    """Render a headline value with qualifiers.

    Args:
        value: The number, already stringified (``"12,345"`` or ``"3.5"``).
            The formatter does not know the axis; the caller shapes the number.
        unit: Optional unit suffix (``"rows/s"``, ``"GB/s"``, ``"QpH"``).
        caps_bound: The ``limits.bound`` entries from ``experiment_block``;
            when non-empty a ``BOUNDED BY: <cap>`` tag is appended.
        n_runs: The number of independent samples behind the value. ``1``
            renders as ``n=1``; ``>=2`` renders as ``n=N``; ``None`` and
            ``0`` add no tag.
        spread: Optional coefficient of variation, as a decimal fraction
            (``0.008`` for 0.8%). Ignored today; kept in the signature so
            the confidence chip can grow into it without a fan-out change.
        support_state: When set, one of ``supported``, ``unverified``,
            ``unsupported``. Any other value is passed through as-is.

    Returns:
        An HTML fragment with the value, unit and qualifier tags. The
        qualifiers render as small muted spans so they sit under the
        headline number without competing with it.
    """
    _ = spread  # signature-only; see docstring
    parts: list[str] = [f"{_html_escape(str(value))}"]
    if unit:
        parts.append(f" {_html_escape(unit)}")
    tags: list[str] = []

    bounds = [c for c in (caps_bound or ()) if c]
    if bounds:
        names = ", ".join(_html_escape(_cap_short_name(c)) for c in bounds)
        titles = _html_escape("; ".join(str(c) for c in bounds))
        tags.append(
            f'<span class="qual qual-bounded" title="{titles}" '
            f'style="color: var(--danger); font-size: 0.55em; margin-left: 0.4em;">'
            f"BOUNDED BY: {names}</span>"
        )

    if n_runs and n_runs > 0:
        tags.append(
            f'<span class="qual qual-n" style="color: var(--text-muted); '
            f'font-size: 0.55em; margin-left: 0.4em;">n={int(n_runs)}</span>'
        )

    if support_state:
        label = _SUPPORT_STATE_LABEL.get(support_state, support_state)
        state_color = {
            "supported": "var(--success)",
            "unverified": "var(--warning)",
            "unsupported": "var(--danger)",
        }.get(support_state, "var(--text-muted)")
        tags.append(
            f'<span class="qual qual-support" style="color: {state_color}; '
            f'font-size: 0.55em; margin-left: 0.4em;">{_html_escape(label)}</span>'
        )

    parts.extend(tags)
    return "".join(parts)


def confidence_chip(n_runs: int | None, spread: float | None = None) -> str:
    """Confidence chip label from run-count and spread.

    - ``high``: ``n >= 5`` and spread known and <10%.
    - ``replicated_n=<N>``: ``n >= 3``.
    - ``single_run``: ``n == 1`` (or unknown, treated as one).

    ``spread`` is a coefficient of variation as a decimal (``0.08`` for
    8%). None means unknown; a high-confidence claim needs it below 10%.
    """
    n = int(n_runs or 1)
    if n < 3:
        # Invariant 7: never claim replication from fewer than 3
        # independent runs. n=1 and n=2 both render as single_run.
        return "single_run"
    if n >= 5 and spread is not None and spread < 0.10:
        return "high"
    return f"replicated_n={n}"


def confidence_chip_html(n_runs: int | None, spread: float | None = None) -> str:
    """The chip as an HTML span, coloured by strength."""
    label = confidence_chip(n_runs, spread)
    color = {
        "single_run": "var(--warning)",
        "high": "var(--success)",
    }.get(label, "var(--text-muted)")
    return (
        f'<span class="confidence-chip" title="run-count qualifier" '
        f'style="display: inline-block; margin-left: 0.5em; padding: 0.1em 0.5em; '
        f"border-radius: 9999px; background: rgba(148,163,184,0.15); "
        f"color: {color}; font-size: 0.65em; font-weight: 600; "
        f'text-transform: none;">{_html_escape(label)}</span>'
    )


def caps_bound_from(metrics: object, *, include_trickle: bool = False) -> list[str]:
    """Extract ``limits.bound`` from a ``PipelineMetrics`` if present.

    The trickle line is left out unless *include_trickle*: the trickle bounds
    intake only (the throughput and efficiency cards add it with
    ``trickle_caps_from``), not every number of the run."""
    exp_block = getattr(metrics, "experiment_block", None)
    if not callable(exp_block):
        return []
    try:
        exp = exp_block()
    except Exception:  # noqa: BLE001 -- formatting must not raise on a bad record
        return []
    if not isinstance(exp, dict):
        return []
    limits = exp.get("limits") or {}
    bound = limits.get("bound") or []
    from lakebench.metrics.bounds import TRICKLE_LINE_PREFIX

    return [
        str(x)
        for x in bound
        if x and (include_trickle or not str(x).startswith(TRICKLE_LINE_PREFIX))
    ]


def trickle_caps_from(metrics: object) -> list[str]:
    """The card label for the trickle when it bounded the run's intake
    (``bounds.record_trickle_bound``: the stored ``limits.trickle_bound``,
    or computed for a record from before it); [] otherwise."""
    from lakebench.metrics.bounds import record_trickle_bound, trickle_label

    try:
        bound = record_trickle_bound(metrics)
    except Exception:  # noqa: BLE001 -- formatting must not raise on a bad record
        return []
    return [trickle_label(bound)] if bound else []


def n_runs_of(metrics: object) -> int | None:
    """Runs behind this record (repetitions.runs when present)."""
    exp_block = getattr(metrics, "experiment_block", None)
    if callable(exp_block):
        try:
            exp = exp_block()
        except Exception:  # noqa: BLE001
            exp = None
        if isinstance(exp, dict):
            rep = exp.get("repetitions") or {}
            runs = rep.get("runs")
            if isinstance(runs, int) and runs > 0:
                return runs
    return None


def qph_samples_of(metrics: object) -> int | None:
    """The number of samples per query behind a QpH (benchmark or rounds)."""
    bench = getattr(metrics, "benchmark", None)
    if bench is not None:
        iters = getattr(bench, "iterations", None)
        if isinstance(iters, int) and iters > 0:
            return iters
    rounds = getattr(metrics, "benchmark_rounds", None)
    if rounds:
        try:
            return len([r for r in rounds if getattr(r, "qph", 0) > 0])
        except TypeError:
            return None
    return None


def support_state_of(metrics: object) -> str | None:
    """Support state from the experiment block, when set."""
    exp_block = getattr(metrics, "experiment_block", None)
    if not callable(exp_block):
        return None
    try:
        exp = exp_block()
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(exp, dict):
        return None
    sup = exp.get("support") or {}
    state = sup.get("state")
    if isinstance(state, str) and state and state != "unknown":
        return state
    return None


# ---------------------------------------------------------------------------
# WCAG 1.4.1: text tokens for compare deltas, so pass/fail and winner/loser
# are not encoded by colour alone. A screen-reader or a copy-paste of the
# text carries the token; the colour is redundant.
# ---------------------------------------------------------------------------

DELTA_TOKEN_A_FASTER = "A_faster"
DELTA_TOKEN_B_FASTER = "B_faster"
DELTA_TOKEN_OVERLAP = "overlap"
DELTA_TOKEN_CAPPED = "capped"
DELTA_TOKEN_WITHHELD = "withheld"


def delta_token(
    *,
    higher_is_better: bool,
    pct: float | None,
    within_noise: bool,
    capped: bool = False,
    withheld: bool = False,
) -> str:
    """The WCAG-safe text token for a delta between two runs.

    ``A_faster`` / ``B_faster`` for a decisive win (B relative to A);
    ``overlap`` when the difference is inside the noise floor; ``capped``
    when a Lakebench cap held one side, so the comparison is not clean;
    ``withheld`` when the pair is not comparable and no delta is shown.
    """
    if withheld:
        return DELTA_TOKEN_WITHHELD
    if capped:
        return DELTA_TOKEN_CAPPED
    if pct is None or within_noise:
        return DELTA_TOKEN_OVERLAP
    if (pct > 0) == higher_is_better:
        return DELTA_TOKEN_B_FASTER
    return DELTA_TOKEN_A_FASTER


ALL_DELTA_TOKENS: Iterable[str] = (
    DELTA_TOKEN_A_FASTER,
    DELTA_TOKEN_B_FASTER,
    DELTA_TOKEN_OVERLAP,
    DELTA_TOKEN_CAPPED,
    DELTA_TOKEN_WITHHELD,
)
