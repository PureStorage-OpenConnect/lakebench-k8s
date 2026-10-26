"""Pure AML rule metadata, importable without pyspark.

These three mappings describe the detection rules: their high-priority
cutoffs, the planted typology each targets, and which rules are
customer-scoped. ``detection_rules`` (a Spark script that imports pyspark at
its top) defines the rule logic and re-exports these; the CLI-side scorer
(``aml.scoring_queries``) needs them without a Spark import, so they live
here and ship flat to the Spark driver alongside the scripts.
"""

from __future__ import annotations

#: The count or size at which a rule's alert is HIGH priority.
HIGH_PRIORITY_CUTOFFS = {
    "W1_connected_components": 8,
    "W2_structuring": 6,
    "W3_round_tripping": 4,
    "W4_risk_propagation": 3,
    "W17_layering_chain": 5,
}

#: Rule -> planted-typology-target map. Stays in lock-step with RULE_TARGETS
#: in benchmark/aml_queries.py (a consistency test asserts they match). A None
#: target means the rule has no planted typology.
RULE_TARGET_TYPOLOGY = {
    "W1_connected_components": "gather_scatter",
    "W2_structuring": "micro_structuring",
    "W3_round_tripping": "cycle",
    "W4_risk_propagation": "rapid_layering",
    "W5_sanctions_match": "sanctions_match",
    "W6_pep_counterparty": "pep_match",
    "W7_cross_border_high_risk": "corridor_high_risk",
    "W8_dormant_reactivation": "dormant_reactivation",
    "W17_layering_chain": "stack",
}

#: Customer-scoped rules (GOALS P10 stage 0): a customer-scoped rule raises an
#: alert only when its subject (entity_id) is a customer in silver.entities.
CUSTOMER_SCOPED_RULES = frozenset(
    {
        "W2_structuring",
        "W5_sanctions_match",
        "W6_pep_counterparty",
        "W7_cross_border_high_risk",
        "W8_dormant_reactivation",
    }
)
