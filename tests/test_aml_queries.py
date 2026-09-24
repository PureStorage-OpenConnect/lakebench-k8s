"""Tests for the AML benchmark query loader.

Exercises template loading and expansion without a live Spark session.
The SQL text itself is inspected for the expected placeholders and
`{catalog}` substitution.
"""

from __future__ import annotations

import pytest

from lakebench.benchmark.aml_queries import (
    RULE_TARGETS,
    UNMAPPED_TYPOLOGIES,
    load_aml_queries,
    query_count,
)

# Authoritative typology names, mirrored from
# `datagen_rs/src/typology.rs::SPECS`. Tests below assert that
# RULE_TARGETS and UNMAPPED_TYPOLOGIES together cover every one.
_ALL_PLANTED_TYPOLOGIES = frozenset(
    {
        "bipartite",
        "cycle",
        "fan_in",
        "fan_out",
        "gather_scatter",
        "random",
        "scatter_gather",
        "stack",
        "synthetic_identity",
        "corridor_high_risk",
        "cross_border_cycle",
        "dormant_reactivation",
        "micro_structuring",
        "rapid_layering",
        "tbml_repeated_invoice",
    }
)


def test_all_w_rules_have_target_entry():
    """Every shipped rule (W1-W8, W17) must have a target (or explicit None)
    entry. W9-W16 are workload ids the spec reserves (writeback, reproduce,
    ingest, ML), not detection rules."""
    for i in (*range(1, 9), 17):
        matches = [k for k in RULE_TARGETS if k.startswith(f"W{i}_")]
        assert len(matches) == 1, f"W{i} has {len(matches)} entries: {matches}"


def test_query_count_matches_documented():
    """7 rules with typology targets * 4 kinds (detect + precision +
    recall + pattern_span) + 2 rules with no target * 1 kind (detect only) +
    4 aggregate queries = 34. W5/W6 have `None` target because
    sanctions and PEP are party attributes in the datagen, not
    typology_type rows, so precision/recall/pattern_span would silently report
    0/0 -- omitted rather than misleading. See RULE_TARGETS docstring.
    The 4th aggregate is `aggregate_reference_vs_rule` from PR-A
    (reference-detector + leakage-gate wiring)."""
    assert query_count() == 34


def test_load_aml_queries_returns_expected_count():
    qs = load_aml_queries("iceberg")
    assert len(qs) == 34


def test_reference_vs_rule_aggregate_present():
    """PR-A wired an aggregate that compares rule recall to the
    sklearn-GBT reference model. Regression against dropping it."""
    qs = load_aml_queries("iceberg")
    aggs = [q.query_id for q in qs if q.kind == "aggregate"]
    assert "aggregate_reference_vs_rule" in aggs
    for q in qs:
        if q.query_id == "aggregate_reference_vs_rule":
            assert "reference_metrics" in q.sql
            assert "recall_gap" in q.sql


def test_catalog_placeholder_expanded_in_every_query():
    qs = load_aml_queries("iceberg")
    for q in qs:
        # No unexpanded placeholders should survive.
        assert "{catalog}" not in q.sql, f"{q.query_id} left {{catalog}} unexpanded"
        assert "iceberg." in q.sql, f"{q.query_id} did not substitute catalog"


def test_rule_id_placeholder_expanded_where_used():
    qs = load_aml_queries("iceberg")
    for q in qs:
        assert "{rule_id}" not in q.sql, f"{q.query_id} left {{rule_id}}"
        assert "{typology_type}" not in q.sql, f"{q.query_id} left {{typology_type}}"


def test_precision_recall_pattern_span_all_reference_the_rule():
    qs = load_aml_queries("iceberg")
    for q in qs:
        if q.kind in ("precision", "recall", "pattern_span"):
            assert q.rule_id in q.sql, f"{q.query_id} does not reference its rule_id in SQL"


def test_detect_queries_exist_for_all_rules():
    qs = load_aml_queries("iceberg")
    detect_rules = {q.rule_id for q in qs if q.kind == "detect"}
    assert detect_rules == set(RULE_TARGETS.keys())


def test_catalog_name_rejects_injection():
    """Allowlist: only plain SQL identifiers pass."""
    with pytest.raises(ValueError):
        load_aml_queries("iceberg'; DROP TABLE x; --")
    with pytest.raises(ValueError):
        load_aml_queries("has space")
    with pytest.raises(ValueError):
        load_aml_queries("")
    with pytest.raises(ValueError):
        load_aml_queries("has\nnewline")
    with pytest.raises(ValueError):
        load_aml_queries("has\ttab")
    with pytest.raises(ValueError):
        load_aml_queries("has-hyphen")
    with pytest.raises(ValueError):
        load_aml_queries("has.dot")
    with pytest.raises(ValueError):
        load_aml_queries("has“quote”")  # unicode smart quotes
    # Plain identifiers pass:
    load_aml_queries("iceberg")
    load_aml_queries("spark_catalog")
    load_aml_queries("_underscore_leading")


def test_rule_targets_reference_real_typology_names():
    """Regression against P0 in the AML query adversarial review: three
    RULE_TARGETS mappings pointed at typology_type strings that the
    datagen never emits (sanctions_hit, pep_hit, high_risk_corridor),
    which silently reported 0/0 precision/recall for W5/W6/W7.

    The authoritative typology enum is `datagen_rs/src/typology.rs`
    `RAW_TYPOLOGIES`. Anything RULE_TARGETS points at (that isn't None)
    must match one of those names exactly.
    """
    valid_typology_names = {
        "bipartite",
        "cycle",
        "fan_in",
        "fan_out",
        "gather_scatter",
        "random",
        "scatter_gather",
        "stack",
        "synthetic_identity",
        "corridor_high_risk",
        "cross_border_cycle",
        "dormant_reactivation",
        "micro_structuring",
        "rapid_layering",
        "tbml_repeated_invoice",
    }
    for rule_id, typology in RULE_TARGETS.items():
        if typology is None:
            continue
        assert typology in valid_typology_names, (
            f"{rule_id} -> {typology!r} is not a valid typology name from "
            f"datagen_rs/src/typology.rs::RAW_TYPOLOGIES. This would cause "
            f"the precision/recall/pattern_span queries to silently return 0/0."
        )


def test_aggregate_queries_do_not_reference_rule():
    qs = load_aml_queries("iceberg")
    aggs = [q for q in qs if q.kind == "aggregate"]
    assert len(aggs) == 4  # + aggregate_reference_vs_rule from PR-A
    for q in aggs:
        assert q.rule_id is None


def test_query_ids_are_unique():
    qs = load_aml_queries("iceberg")
    ids = [q.query_id for q in qs]
    assert len(ids) == len(set(ids)), "duplicate query_id"


def test_typology_type_appears_in_precision_when_target_present():
    qs = load_aml_queries("iceberg")
    for q in qs:
        if q.kind == "precision":
            typ = RULE_TARGETS[q.rule_id]
            assert typ is not None
            assert typ in q.sql, f"{q.query_id} does not filter by typology_type"


class TestTypologyCoverage:
    """Every planted typology in `datagen_rs/src/typology.rs::SPECS` is
    either targeted by a W-rule (RULE_TARGETS) or explicitly documented
    as unmapped (UNMAPPED_TYPOLOGIES). No overlap between the two.
    Regression against AML audit P1 #2 (tbml_repeated_invoice was
    planted but silently untargeted -- any TBML metric would have
    reported recall=0 with no signal to the reader that the gap was
    documented vs a bug)."""

    def test_every_planted_typology_is_covered(self):
        targeted = {t for t in RULE_TARGETS.values() if t is not None}
        unmapped = set(UNMAPPED_TYPOLOGIES.keys())
        covered = targeted | unmapped
        missing = _ALL_PLANTED_TYPOLOGIES - covered
        assert not missing, (
            f"planted typologies with neither a W-rule nor an "
            f"UNMAPPED_TYPOLOGIES entry: {sorted(missing)}. Either add "
            f"a rule to RULE_TARGETS or document the gap in "
            f"UNMAPPED_TYPOLOGIES with a one-line reason."
        )

    def test_no_overlap_between_targeted_and_unmapped(self):
        targeted = {t for t in RULE_TARGETS.values() if t is not None}
        unmapped = set(UNMAPPED_TYPOLOGIES.keys())
        overlap = targeted & unmapped
        assert not overlap, (
            f"typologies in both RULE_TARGETS.values() and "
            f"UNMAPPED_TYPOLOGIES: {sorted(overlap)}. A typology cannot "
            f"be both scored and documented-as-gap."
        )

    def test_unmapped_reasons_are_nonempty(self):
        for name, reason in UNMAPPED_TYPOLOGIES.items():
            assert reason and reason.strip(), (
                f"UNMAPPED_TYPOLOGIES[{name!r}] has empty reason. Every "
                f"documented gap must explain why it is a gap so a "
                f"reader can tell 'documented' from 'forgotten'."
            )

    def test_unmapped_typologies_are_real_typology_names(self):
        """A typo in UNMAPPED_TYPOLOGIES would silently mask a real
        coverage gap: the test above passes as long as the string
        matches something, even if it names no real typology."""
        for name in UNMAPPED_TYPOLOGIES:
            assert name in _ALL_PLANTED_TYPOLOGIES, (
                f"UNMAPPED_TYPOLOGIES key {name!r} is not a real planted "
                f"typology in datagen_rs/src/typology.rs::SPECS. Fix the "
                f"typo or remove the entry."
            )

    def test_tbml_repeated_invoice_is_documented_not_forgotten(self):
        """AML audit P1 #2: tbml_repeated_invoice is planted (tid=14)
        but has no W-rule. Not a bug -- documented as unmapped so a
        future TBML detector can pick it up cleanly."""
        assert "tbml_repeated_invoice" in UNMAPPED_TYPOLOGIES
        assert "TBML" in UNMAPPED_TYPOLOGIES["tbml_repeated_invoice"]
