"""Parse the AML gold driver's per-rule ``[stage-profile]`` lines.

``common.rule_stage_profile`` (in the Spark driver) runs after each
detection rule and logs, for the rule's own Spark job group, its heaviest
stages by executor run time::

    [stage-profile] rule=<id> group=<g> stage=<n> attempt=<a> tasks=<t>
        wall_s=<s> exec_s=<s> shuffle_read_mb=<m> max_task_s=<s>
        stages=<k> truncated=<true|false> name=<stage name>
    [stage-profile] rule=<id> group=<g> stages=0 truncated=<true|false>
    [stage-profile] rule=<id> group=<g> unavailable reason=<text>

The group is unique per rule invocation. When one log holds several
invocations of a rule (a rerun driver), the last group's lines win.
"""

from __future__ import annotations

import re
from typing import Any

_PREFIX = r"\[stage-profile\]\s+rule=(?P<rule>[A-Za-z0-9_]+)\s+group=(?P<group>\S+)\s+"
_STAGE_RE = re.compile(
    _PREFIX + r"stage=(?P<stage>\d+)\s+attempt=(?P<attempt>\d+)\s+tasks=(?P<tasks>\d+)"
    r"\s+wall_s=(?P<wall_s>\S+)\s+exec_s=(?P<exec_s>\S+)"
    r"\s+shuffle_read_mb=(?P<shuffle_read_mb>\S+)\s+max_task_s=(?P<max_task_s>\S+)"
    r"\s+stages=(?P<stages>\d+)\s+truncated=(?P<truncated>true|false)"
    r"\s+name=(?P<name>.*?)\s*$"
)
_EMPTY_RE = re.compile(_PREFIX + r"stages=0\s+truncated=(?P<truncated>true|false)\s*$")
_UNAVAILABLE_RE = re.compile(_PREFIX + r"unavailable\s+reason=(?P<reason>.*?)\s*$")


def _num(v: str) -> float | None:
    try:
        return float(v)
    except ValueError:
        return None  # "None": the stage had no completion time or task summary


def parse_stage_profile(
    logs: str | None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
    """``(stage_profile, unavailable)``: per rule, the stages of its last
    group, heaviest first as logged (an empty list when the group ran no
    stage); and per rule whose last group could not be read, the reason."""
    last_group: dict[str, str] = {}
    profile: dict[str, list[dict[str, Any]]] = {}
    unavailable: dict[str, str] = {}
    for line in (logs or "").splitlines():
        if "[stage-profile]" not in line:
            continue
        stage = _STAGE_RE.search(line)
        empty = None if stage else _EMPTY_RE.search(line)
        gone = None if stage or empty else _UNAVAILABLE_RE.search(line)
        m = stage or empty or gone
        if m is None:
            continue
        rule, group = m.group("rule"), m.group("group")
        if last_group.get(rule) != group:
            last_group[rule] = group
            profile[rule] = []
            unavailable.pop(rule, None)
        if gone:
            profile.pop(rule, None)
            unavailable[rule] = gone.group("reason")
        elif stage:
            profile[rule].append(
                {
                    "stage": int(stage.group("stage")),
                    "attempt": int(stage.group("attempt")),
                    "tasks": int(stage.group("tasks")),
                    "wall_s": _num(stage.group("wall_s")),
                    "exec_s": _num(stage.group("exec_s")),
                    "shuffle_read_mb": _num(stage.group("shuffle_read_mb")),
                    "max_task_s": _num(stage.group("max_task_s")),
                    "stages": int(stage.group("stages")),
                    "truncated": stage.group("truncated") == "true",
                    "name": stage.group("name"),
                }
            )
    return profile, unavailable
