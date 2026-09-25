"""Parse and gate the P10 transaction-monitoring lines of the AML gold driver.

tm_operations.py (in the Spark driver) logs, per cycle:

- ``[tm-invariant] <name>: status=<pass|fail|error> cycle=<n> detail=<text>``,
  one line per workflow invariant (P10.2), or a single ``workflow`` line with
  status ``error`` when the operations layer could not run;
- ``[tm-ops] <json>``, the operations summary the scorecard renders.

The gate is honest in both directions: a failed or errored invariant fails
the run, and a gold driver whose log was read but carries no invariant lines
fails it too (the layer did not run, so nothing was checked).
"""

from __future__ import annotations

import json
import re
from typing import Any

_INVARIANT_RE = re.compile(
    r"\[tm-invariant\]\s+(?P<name>[A-Za-z0-9_]+):\s+status=(?P<status>[a-z]+)"
    r"\s+cycle=(?P<cycle>\d+)\s+detail=(?P<detail>.*?)\s*$",
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
    """One problem per failed or errored invariant, or one for no invariants."""
    prefix = f"{label}: " if label else ""
    if not invariants_by_cycle:
        return [
            f"{prefix}TM operations reported no workflow invariants; the P10 "
            "layer did not run, so cases, SARs and completeness are unchecked."
        ]
    problems = []
    for cycle in sorted(invariants_by_cycle):
        for name, r in sorted(invariants_by_cycle[cycle].items()):
            if r.get("status") != "pass":
                problems.append(
                    f"{prefix}cycle {cycle}: workflow invariant {name} "
                    f"{r.get('status')}: {r.get('detail')}"
                )
    return problems
