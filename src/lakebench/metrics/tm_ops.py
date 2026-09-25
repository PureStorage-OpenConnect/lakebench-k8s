"""Parse and gate the P10 transaction-monitoring lines of the AML gold driver.

tm_operations.py (in the Spark driver) logs, per cycle or operations tick:

- ``[tm-status] status=<ran|not_run|disabled|waiting> cycle=<n> reason=<text>``:
  whether the layer ran, and why not;
- ``[tm-invariant] <name>: status=<pass|fail> cycle=<n> detail=<text>``, one
  line per workflow invariant (P10.2) when it ran;
- ``[tm-ops] <json>``, the operations summary the scorecard renders.

The P10 verdict (:func:`tm_verdict`) is separate from detection scoring:

- ``fail``: the layer ran and an invariant is violated. The tables users query
  are inconsistent, so the run fails.
- ``not_run``: the layer could not run (no manifest, a layer error, a
  continuous window that ended before the manifest was ready, or a driver log
  with no TM lines). The P10 gate is not met and the report says why, but
  detection results stand and the run is not failed for it.
- ``disabled``: ``workload.tm_operations.enabled`` is false; nothing is gated.
- ``unknown``: no driver log was captured; a warning, in batch and continuous
  alike.
- ``pass``: every cycle that reported ran, and every invariant passed.
"""

from __future__ import annotations

import json
import re
from typing import Any

_INVARIANT_RE = re.compile(
    r"\[tm-invariant\]\s+(?P<name>[A-Za-z0-9_]+):\s+status=(?P<status>[a-z_]+)"
    r"\s+cycle=(?P<cycle>\d+)\s+detail=(?P<detail>.*?)\s*$",
    re.MULTILINE,
)
_STATUS_RE = re.compile(
    r"\[tm-status\]\s+status=(?P<status>[a-z_]+)\s+cycle=(?P<cycle>\d+)"
    r"\s+reason=(?P<reason>.*?)\s*$",
    re.MULTILINE,
)
_OPS_RE = re.compile(r"\[tm-ops\]\s+(?P<json>\{.*\})\s*$", re.MULTILINE)


def parse_tm_invariants(logs: str | None) -> dict[int, dict[str, dict[str, str]]]:
    """``{cycle: {invariant: {"status", "detail"}}}``; a later line for the
    same cycle and invariant wins (a rerun of the cycle)."""
    out: dict[int, dict[str, dict[str, str]]] = {}
    for m in _INVARIANT_RE.finditer(logs or ""):
        out.setdefault(int(m.group("cycle")), {})[m.group("name")] = {
            "status": m.group("status"),
            "detail": m.group("detail"),
        }
    return out


def parse_tm_status(logs: str | None) -> dict[int, dict[str, str]]:
    """``{cycle: {"status", "reason"}}`` from the ``[tm-status]`` lines; the
    last line per cycle wins."""
    out: dict[int, dict[str, str]] = {}
    for m in _STATUS_RE.finditer(logs or ""):
        out[int(m.group("cycle"))] = {"status": m.group("status"), "reason": m.group("reason")}
    return out


def parse_tm_ops(logs: str | None) -> dict[str, Any] | None:
    """The last ``[tm-ops]`` summary in the log, or None."""
    last = None
    for m in _OPS_RE.finditer(logs or ""):
        try:
            last = json.loads(m.group("json"))
        except ValueError:
            continue
    return last


def tm_gate_problems(
    invariants_by_cycle: dict[int, dict[str, dict[str, str]]], *, label: str = ""
) -> list[str]:
    """One problem per failed invariant (the layer ran and its tables are
    inconsistent). A cycle with no invariants is not a problem here; the
    verdict reports it as not run."""
    prefix = f"{label}: " if label else ""
    problems = []
    for cycle in sorted(invariants_by_cycle):
        for name, r in sorted(invariants_by_cycle[cycle].items()):
            if r.get("status") != "pass":
                problems.append(
                    f"{prefix}cycle {cycle}: workflow invariant {name} "
                    f"{r.get('status')}: {r.get('detail')}"
                )
    return problems


def tm_verdict(
    invariants_by_cycle: dict | None,
    status_by_cycle: dict | None,
    *,
    enabled: bool = True,
    logs_captured: bool = True,
    continuous: bool = False,
    label: str = "",
) -> dict[str, Any]:
    """The P10 verdict for one run (see the module docstring).

    Returns ``{"status", "reason", "problems", "cycles_ran", "cycles_not_run"}``.
    Only ``problems`` (non-empty when status is ``fail``) fail the run.
    """
    inv = {int(c): v for c, v in (invariants_by_cycle or {}).items()}
    sts = {int(c): v for c, v in (status_by_cycle or {}).items()}
    out: dict[str, Any] = {
        "status": "pass",
        "reason": "",
        "problems": [],
        "cycles_ran": sorted(inv),
        "cycles_not_run": [],
    }
    if not enabled:
        out.update(status="disabled", reason="workload.tm_operations.enabled is false")
        return out
    if not logs_captured:
        out.update(
            status="unknown",
            reason="no gold driver log was captured; the TM workflow invariants are unchecked",
        )
        return out
    problems = tm_gate_problems(inv, label=label)
    # A cycle that says it did not run (and has no invariants). Continuous
    # 'waiting' ticks before the first pass are expected, not a miss.
    not_run = {
        c: s for c, s in sts.items() if c not in inv and s.get("status") in ("not_run", "disabled")
    }
    waiting = [s for _, s in sorted(sts.items()) if s.get("status") == "waiting"]
    out["cycles_not_run"] = sorted(not_run)
    if problems:
        out.update(status="fail", reason=problems[0], problems=problems)
        return out
    if inv and not not_run:
        return out
    if not_run:
        c, s = sorted(not_run.items())[-1]
        reason = f"cycle {c}: {s.get('reason')}"
    elif continuous and waiting:
        reason = f"the window ended before the layer could run ({waiting[-1].get('reason')})"
    else:
        reason = "the gold driver log has no TM lines; the layer did not run"
    if inv:
        reason = f"ran on cycles {sorted(inv)}; {reason}"
    out.update(status="not_run", reason=reason)
    return out
