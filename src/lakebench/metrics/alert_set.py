"""The alert-set fingerprint, CLI side (EVD-10, OD-3).

AML gold-finalize prints one ``LB_ALERT_SET {json}`` line after its last
write to gold.alerts (``spark/scripts/gold_finalize_financial.alert_set_line``,
through ``common.alert_set_fingerprint``). The collector parses it into the
gold-finalize job (``alert_set``, ``alert_set_seconds``,
``alert_set_unavailable``) and ``experiment.build_experiment`` copies the
fingerprint to ``experiment.results.alert_set``.

An alert is ``(rule_id, entity_id, alert_ts)``: rule, subject and the event
time the rule derived from the data. Generated ids and wall-clock times are
not part of it, so two runs that raised the same alerts read equal. The
fingerprint is per rule (``by_rule``: rows and an order-independent hash sum)
with the totals beside it.

Comparison (``diff_alert_sets``) is a results check, ladder step 5 between
the sides and step 2 inside one: any difference in a rule's rows or hash is
"different results". A record that should carry one and does not
(``alert_set_missing``: an AML batch record written by 1.7, exp1 or exp2)
has results not established, step 4, so a failed or skipped fingerprint
never lets a pair through unchecked. A 1.6 record never had one; absent on a
1.6 side is a note, not a refusal (and such a pair is normally refused
earlier, on its workload version).

The continuous alert set is a different, diagnostic value
(``results.alert_set_continuous``, from the covered score after the drain)
and is not compared here.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

#: Must equal ``gold_finalize_financial.ALERT_SET_TAG`` and
#: ``common.ALERT_SET_SPEC`` on the Spark side (tests/test_alert_set.py).
ALERT_SET_TAG = "LB_ALERT_SET"
ALERT_SET_SPEC = "as1"

_LINE_RE = re.compile(r"LB_ALERT_SET (?P<json>\{.*\})\s*$", re.MULTILINE)
# ASCII digits, at most 40: a Spark sum of xxhash64 values has about 21, and an
# unbounded string would make int() raise on a hostile line.
_INT_RE = re.compile(r"^-?[0-9]{1,40}$")
_KEYS = ("spec", "columns", "cols_sha", "rows", "h", "by_rule")


def _count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _hash(value: Any) -> bool:
    return isinstance(value, str) and bool(_INT_RE.match(value))


def shape_problem(body: Any) -> str | None:
    """Why *body* is not a well-formed alert set, or None.

    Checks the keys and their types, and that ``rows`` and ``h`` are the
    sums of ``by_rule``'s (the Spark side writes them that way, so a
    mismatch is a garbled line, not a result)."""
    if not isinstance(body, Mapping):
        return "not an object"
    missing = [k for k in _KEYS if k not in body]
    if missing:
        return f"missing {', '.join(missing)}"
    if not isinstance(body["spec"], str) or not body["spec"]:
        return "spec is not a name"
    if not isinstance(body["columns"], list) or not all(
        isinstance(c, str) for c in body["columns"]
    ):
        return "columns is not a list of names"
    if not isinstance(body["cols_sha"], str) or not body["cols_sha"]:
        return "cols_sha is not a string"
    if not _count(body["rows"]) or not _hash(body["h"]):
        return "rows or h is not a count and an integer string"
    by_rule = body["by_rule"]
    if not isinstance(by_rule, Mapping):
        return "by_rule is not an object"
    for rule, part in by_rule.items():
        if (
            not isinstance(part, Mapping)
            or not _count(part.get("rows"))
            or not _hash(part.get("h"))
        ):
            return f"by_rule[{rule}] is not {{rows, h}}"
    if sum(p["rows"] for p in by_rule.values()) != body["rows"]:
        return "rows is not the sum of by_rule rows"
    if sum(int(p["h"]) for p in by_rule.values()) != int(body["h"]):
        return "h is not the sum of by_rule hashes"
    return None


def parse_alert_set(logs: str | None) -> tuple[dict[str, Any] | None, float | None, str | None]:
    """``(alert_set, seconds, unavailable)`` from a gold-finalize driver log.

    The last ``LB_ALERT_SET`` line wins. ``alert_set`` is the fingerprint
    without ``seconds``; ``unavailable`` is the reason when the line says
    the fingerprint could not be computed or the line is not well formed.
    All three are None when the log has no such line (a C360 run, or a
    script from before EVD-10)."""
    last = None
    for m in _LINE_RE.finditer(logs or ""):
        last = m.group("json")
    if last is None:
        return None, None, None
    try:
        body = json.loads(last)
    except ValueError:
        return None, None, "the LB_ALERT_SET line is not valid JSON"
    if not isinstance(body, dict):
        return None, None, "the LB_ALERT_SET line is not an object"
    seconds = body.pop("seconds", None)
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or seconds < 0:
        seconds = None
    if "unavailable" in body:
        return None, seconds, str(body["unavailable"]) or "no reason given"
    problem = shape_problem(body)
    if problem:
        return None, seconds, f"the LB_ALERT_SET line is malformed ({problem})"
    return {k: body[k] for k in _KEYS}, seconds, None


def alert_set_of(exp: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """The batch alert set an experiment block recorded, or None."""
    value = ((exp or {}).get("results") or {}).get("alert_set")
    return value if isinstance(value, Mapping) else None


def alert_set_expected(exp: Mapping[str, Any] | None) -> bool:
    """True for an AML batch block written by 1.7 or later, whose
    gold-finalize records the alert set. Decided by
    ``comparability.written_by_v17``, never by the schema string: 1.7 also
    writes exp1 blocks (identity incomplete, ``v2_unavailable``), and a
    failed fingerprint there must not pass as a 1.6 record's absence."""
    from lakebench.metrics.comparability import written_by_v17

    # The block alone suffices: build_experiment stamps exp2 or
    # v2_unavailable on every block whose run-start inputs carry identity
    # version 2, so the record's snapshot adds nothing here.

    exp = exp or {}
    return (
        (exp.get("workload") or {}).get("name") == "financial"
        and exp.get("mode") == "batch"
        and written_by_v17(exp)[0]
    )


def alert_set_missing(exp: Mapping[str, Any] | None) -> str | None:
    """Why the results of *exp* are not established for want of an alert
    set (ladder step 4), or None. Only an AML batch block written by 1.7
    must carry one; a malformed one counts as missing."""
    if not alert_set_expected(exp):
        return None
    value = alert_set_of(exp)
    if value is not None:
        problem = shape_problem(value)
        if problem is None:
            return None
        return f"the alert-set fingerprint is malformed ({problem})"
    why = ((exp or {}).get("results") or {}).get("alert_set_unavailable")
    return "the alert-set fingerprint was not recorded" + (f" ({why})" if why else "")


def diff_alert_sets(
    a: Mapping[str, Any] | None,
    b: Mapping[str, Any] | None,
    label_a: str = "A",
    label_b: str = "B",
) -> list[str]:
    """One line per difference between two recorded alert sets: a refusal
    each ("different results"). Empty when they are equal, and when either
    is absent (absence is ``alert_set_missing``'s and ``alert_set_notes``'
    business, not a difference)."""
    if a is None or b is None:
        return []
    out: list[str] = []
    if a.get("spec") != b.get("spec"):
        return [
            f"alert-set fingerprints have different definitions "
            f"({label_a} {a.get('spec')}, {label_b} {b.get('spec')})"
        ]
    if a.get("cols_sha") != b.get("cols_sha"):
        return [
            f"alert-set fingerprints cover different columns "
            f"({label_a} {a.get('columns')}, {label_b} {b.get('columns')})"
        ]
    ra, rb = a.get("by_rule") or {}, b.get("by_rule") or {}
    for rule in sorted(set(ra) | set(rb)):
        pa, pb = ra.get(rule), rb.get(rule)
        if pa is None or pb is None:
            have, lack, part = (label_a, label_b, pa) if pb is None else (label_b, label_a, pb)
            out.append(
                f"alert set differs: rule {rule} raised {(part or {}).get('rows')} alert(s) in "
                f"{have} and none in {lack}"
            )
        elif pa.get("rows") != pb.get("rows") or str(pa.get("h")) != str(pb.get("h")):
            same = " (same count, different alerts)" if pa.get("rows") == pb.get("rows") else ""
            out.append(
                f"alert set differs: rule {rule} raised {pa.get('rows')} alert(s) in "
                f"{label_a} and {pb.get('rows')} in {label_b}{same}"
            )
    if not out and (a.get("rows") != b.get("rows") or str(a.get("h")) != str(b.get("h"))):
        out.append(
            f"alert set differs: {a.get('rows')} alert(s) in {label_a} and "
            f"{b.get('rows')} in {label_b}"
        )
    return out


def alert_set_notes(
    ea: Mapping[str, Any] | None,
    eb: Mapping[str, Any] | None,
    label_a: str = "A",
    label_b: str = "B",
) -> list[str]:
    """The caveat for a pair where one side recorded an alert set and the
    other, a 1.6 record, did not: the results were compared on the
    benchmark queries only. Empty otherwise (a 1.7 side without one is not
    established, step 4, and never reaches this)."""
    sa, sb = alert_set_of(ea), alert_set_of(eb)
    if (sa is None) == (sb is None):
        return []
    lacking = label_b if sb is None else label_a
    return [
        f"{lacking} recorded no alert-set fingerprint (a record from before 1.7); "
        "results compared on the benchmark queries only"
    ]
