"""The AML rule-to-typology map.

Recall and precision need to know which planted typology TYPE a rule
targets. The map is ``aml.rule_meta.RULE_TARGET_TYPOLOGY``, which the Spark
driver reads too; changing it changes what the score attributes to what
typology. What the scores mean, and their caveats, is in
`docs/aml-scoring.md`.
"""

from __future__ import annotations

from lakebench.aml.rule_meta import RULE_TARGET_TYPOLOGY

# Map each W-rule to the typology_type in `bronze.manifest` that the
# rule is supposed to detect. Several rules may target the same type;
# each rule targets exactly one. A rule with no planted-typology target reports N/A for
# precision/recall.
#
# **Semantic contract for downstream consumers:** RULE_TARGETS is the
# join key for computing rule precision/recall. `bronze.manifest` also
# carries a `workload` column (populated from `datagen_rs/src/typology.rs::SPECS`);
# that column is a coarser AML *category* grouping ("W2_structuring",
# "W3_round_tripping", "W1_synthetic_id") and is NOT the rule that
# targets the row. Joining `alerts.rule_id` on `manifest.workload`
# produces different numbers than joining via RULE_TARGETS -- for
# example `gather_scatter` has category `W2_structuring` (it's a
# scatter of small amounts) but is scored under W1_connected_components
# because that's the rule that can actually detect its graph shape.
# Consumers computing precision/recall MUST use RULE_TARGETS as the
# authoritative mapping; `manifest.workload` is category-level context.
RULE_TARGETS: dict[str, str | None] = dict(RULE_TARGET_TYPOLOGY)


# Planted typologies that no current W-rule targets. Explicitly listed
# so a downstream consumer can distinguish "we plant this but no
# detector scores it (documented gap)" from "we forgot to hook this
# typology up (a bug)." Every planted typology (see
# `datagen_rs/src/typology.rs::SPECS`) must appear in either
# `RULE_TARGETS.values()` or here; the test suite enforces this.
#
# Adding a new W-rule that targets one of these should remove it from
# this set and add the mapping in RULE_TARGETS. Adding a new planted
# typology without a rule should add it here with a one-line reason.
UNMAPPED_TYPOLOGIES: dict[str, str] = {
    # Multi-participant fan-out with recycled participant identities;
    # no dedicated rule -- W1_connected_components will catch some
    # instances via graph closure but that co-detection is a happy
    # accident, not a scored target.
    "bipartite": (
        "No dedicated synthetic-identity detector shipped yet; "
        "W1_connected_components co-detects some instances via graph "
        "closure but that is not a scored precision/recall target."
    ),
    "cross_border_cycle": (
        "Cross-border variant of `cycle`; same gap -- W7 targets "
        "`corridor_high_risk`, not this multi-hop cross-border form."
    ),
    "fan_in": (
        "Category W2_structuring in the manifest but no dedicated fan-in "
        "detector. W2's beneficiary kind (structuring_beneficiary) co-detects "
        "it, since fan_in plants structuring-band credits from many senders "
        "into one account, and W1 co-detects some instances; neither is a "
        "scored target, so those alerts count as W2 false positives."
    ),
    "fan_out": (
        "Category W2_structuring in the manifest but no dedicated fan-out "
        "detector; W1_connected_components co-detects some instances."
    ),
    "random": (
        "Smoke-test typology (rows_per_instance=1); intentionally not "
        "targeted -- its recall would be meaningless."
    ),
    "scatter_gather": (
        "Category W3_round_tripping in the manifest, but W3 targets "
        "`cycle` and scatter_gather does not return funds to its start. "
        "scatter_gather recall is untestable today."
    ),
    "synthetic_identity": (
        "No dedicated synthetic-identity detector shipped yet; the "
        "`W5_splink_resolution` rule is future work (see the "
        "`detection_rules` module docstring)."
    ),
    "tbml_repeated_invoice": (
        "Trade-Based ML repeated-invoice pattern is planted but no "
        "W-rule targets it. Adding a TBML detector is future work; "
        "recall/precision for TBML remains untestable until then."
    ),
}
