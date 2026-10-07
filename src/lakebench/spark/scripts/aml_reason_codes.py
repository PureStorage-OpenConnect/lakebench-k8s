"""Per-alert reason codes for the AML detection rules.

Every alert carries ``reason_codes`` (array<string>, the last gold.alerts
column): first its rule's base code (``BASE_CODE``), then each conditional
code whose condition holds on the alert. The base code makes the codes of a
rule cover all of its alerts, so the per-code hit sets of a rule union to the
rule's hit set (score_financial's per-code recall relies on it).

Conditional codes read only columns the rule's own projection already holds,
and use only cut points the rule already has: its HIGH priority threshold
(detection_rules.HIGH_PRIORITY_CUTOFFS), the screen's exact/fuzzy split
(similarity 1.0), the rescreen pass, and the corridor list's risk tier. No
code introduces a threshold of its own, so no code is a tuned constant. A
code never changes which alerts a rule raises.

This module has no pyspark import at its top: detection_rules imports it
inside ``_alert_frame`` only, and the scripts that import
detection_rules never reach it.
"""

from __future__ import annotations

#: Every rule's base code, carried by every alert of the rule.
BASE_CODE = {
    "W1_connected_components": "W1_COMPONENT",
    "W2_structuring": "W2_SUB_THRESHOLD_BURST",
    "W3_round_tripping": "W3_CYCLE",
    "W4_risk_propagation": "W4_FAST_PASS_THROUGH",
    "W5_sanctions_match": "W5_SANCTIONS_HIT",
    "W6_pep_counterparty": "W6_PEP_HIT",
    "W7_cross_border_high_risk": "W7_HIGH_RISK_CORRIDOR",
    "W8_dormant_reactivation": "W8_DORMANCY_GAP",
    "W17_layering_chain": "W17_CHAIN",
}

#: The conditional codes of each projection, in the order they are listed on
#: an alert: (code, what it says). Keys are rule ids, plus
#: "W5_sanctions_match:rescreen" for W5's rescreen projection.
CONDITIONAL_CODES = {
    "W1_connected_components": (("W1_LARGE_COMPONENT", "component at the HIGH priority size"),),
    "W2_structuring": (
        ("W2_BENEFICIARY_FAN_IN", "several senders into one beneficiary"),
        ("W2_HIGH_COUNT", "in-band payment count at the HIGH priority count"),
    ),
    "W3_round_tripping": (("W3_LONG_CYCLE", "cycle at the HIGH priority hop count"),),
    "W4_risk_propagation": (("W4_MULTI_CHAIN", "pass-through chains at the HIGH priority count"),),
    "W5_sanctions_match": (
        ("W5_EXACT", "exact name match"),
        ("W5_FUZZY", "fuzzy name match"),
    ),
    "W5_sanctions_match:rescreen": (
        ("W5_EXACT", "exact name match"),
        ("W5_FUZZY", "fuzzy name match"),
        ("W5_RESCREEN", "raised when a list version listed a prior counterparty"),
    ),
    "W6_pep_counterparty": (
        ("W6_EXACT", "exact name match"),
        ("W6_FUZZY", "fuzzy name match"),
    ),
    "W7_cross_border_high_risk": (
        ("W7_FATF_BLACK", "FATF black-list jurisdiction"),
        ("W7_FATF_GREY", "FATF grey-list jurisdiction"),
        ("W7_SYNTHETIC_CORRIDOR", "the generator's synthetic high-risk corridor"),
    ),
    "W8_dormant_reactivation": (),
    "W17_layering_chain": (("W17_LONG_CHAIN", "chain at the HIGH priority hop count"),),
}


def _all_codes(rule_id: str) -> tuple[str, ...]:
    projections = [p for p in CONDITIONAL_CODES if p.split(":")[0] == rule_id]
    seen = [BASE_CODE[rule_id]]
    for p in projections:
        for code, _ in CONDITIONAL_CODES[p]:
            if code not in seen:
                seen.append(code)
    return tuple(seen)


#: Every code each rule can write, base first.
REASON_CODES = {rule: _all_codes(rule) for rule in BASE_CODE}


def vocabulary_digest(cutoffs: dict | None = None) -> str:
    """sha256 (16 hex) of the code vocabulary and the cut points its codes
    read (detection_rules.HIGH_PRIORITY_CUTOFFS, passed by the caller: this
    module does not import pyspark at its top), recorded beside per-code
    scores so a reader knows which codes, meaning what, they are in."""
    import hashlib
    import json

    text = json.dumps(
        {"base": BASE_CODE, "conditional": CONDITIONAL_CODES, "cutoffs": cutoffs or {}},
        sort_keys=True,
    )
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _conditions(projection: str):
    """(code, Column condition) for *projection*."""
    from detection_rules import HIGH_PRIORITY_CUTOFFS as cut
    from pyspark.sql.functions import col, lit

    exact = col("similarity") >= lit(1.0)
    if projection == "W1_connected_components":
        return [("W1_LARGE_COMPONENT", col("component_size") >= lit(cut[projection]))]
    if projection == "W2_structuring":
        return [
            ("W2_BENEFICIARY_FAN_IN", col("_aggregation") == lit("beneficiary")),
            ("W2_HIGH_COUNT", col("suspicious_count") >= lit(cut[projection])),
        ]
    if projection == "W3_round_tripping":
        return [("W3_LONG_CYCLE", col("hops") >= lit(cut[projection]))]
    if projection == "W17_layering_chain":
        return [("W17_LONG_CHAIN", col("hops") >= lit(cut[projection]))]
    if projection == "W4_risk_propagation":
        return [("W4_MULTI_CHAIN", col("chain_count") >= lit(cut[projection]))]
    if projection in ("W5_sanctions_match", "W6_pep_counterparty"):
        p = projection[:2]
        return [(f"{p}_EXACT", exact), (f"{p}_FUZZY", ~exact)]
    if projection == "W5_sanctions_match:rescreen":
        return [("W5_EXACT", exact), ("W5_FUZZY", ~exact), ("W5_RESCREEN", lit(True))]
    if projection == "W7_cross_border_high_risk":
        tier = col("risk_tier")
        return [
            ("W7_FATF_BLACK", tier == lit("black")),
            ("W7_FATF_GREY", tier == lit("grey")),
            ("W7_SYNTHETIC_CORRIDOR", tier == lit("synthetic_corridor")),
        ]
    if projection == "W8_dormant_reactivation":
        return []
    raise KeyError(f"no reason codes for {projection!r}")


def reason_expr(projection: str):
    """Column array<string>: the conditional codes of *projection* (a rule id,
    or "W5_sanctions_match:rescreen") that hold on the row, in
    CONDITIONAL_CODES order. A NULL condition is false."""
    from pyspark.sql.functions import array, coalesce, expr, lit, when
    from pyspark.sql.functions import filter as filter_

    conds = _conditions(projection)
    declared = [code for code, _ in CONDITIONAL_CODES[projection]]
    if [code for code, _ in conds] != declared:
        raise AssertionError(f"{projection}: conditions do not match CONDITIONAL_CODES")
    if not conds:
        return expr("cast(array() as array<string>)")
    items = [when(coalesce(c, lit(False)), lit(code)) for code, c in conds]
    return filter_(array(*items), lambda x: x.isNotNull())
