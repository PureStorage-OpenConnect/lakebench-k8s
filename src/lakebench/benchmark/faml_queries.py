"""FAML benchmark query set.

Ships four SQL templates (detect / precision / recall / pattern_span)
that are
instantiated once per W-rule, plus three aggregate queries that run
as-is. This lands 30 executable queries with 8 source files -- a
readable version of the design in
`dev-artifacts/FAML-SCORING-QUERIES.md`.

Precision, recall, and pattern-span all need to know which planted
typology TYPE a rule targets. That mapping lives here as the single
source of truth; changing it changes what the score attributes to
what typology.

Callers pass a catalog name and get back a list of
(query_id, sql_text) tuples ready to run against Trino or Spark SQL.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Map each W-rule to the typology_type in `bronze.manifest` that the
# rule is supposed to detect. Multiple rules can target the same type
# (W1 connected_components and W3 round_tripping both hit round-trip
# chains). A rule with no planted-typology target reports N/A for
# precision/recall and only contributes a detect + volume query.
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
RULE_TARGETS: dict[str, str | None] = {
    "W1_connected_components": "gather_scatter",  # multi-entity graph clusters
    "W2_structuring": "micro_structuring",
    "W3_round_tripping": "rapid_layering",  # round-trip signal
    "W4_risk_propagation": "stack",  # high-velocity chain
    # W5/W6 target no planted typology today. Sanctions and PEP hits
    # live as party attributes in the datagen (see
    # `datagen_rs/src/party.rs::party_flags`), not as typology_type
    # rows in `bronze.manifest`. Extending the datagen to plant
    # sanctions/PEP typologies is a follow-up; until then these rules
    # only emit the `detect` query and skip precision/recall/pattern_span
    # (downstream reports N/A rather than a bogus 0/0).
    "W5_sanctions_match": None,
    "W6_pep_counterparty": None,
    # W7 targets `corridor_high_risk` (typology.rs enum name, not
    # `high_risk_corridor` -- the latter is the transposed variant an
    # earlier draft used and it silently matched zero manifest rows).
    "W7_cross_border_high_risk": "corridor_high_risk",
    "W8_dormant_reactivation": "dormant_reactivation",
}


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
    "cycle": (
        "2-hop and 3-hop transaction cycles; targeted approximately by "
        "W3_round_tripping via self-join but scored against rapid_layering "
        "as the primary target. cycle recall is untestable today."
    ),
    "cross_border_cycle": (
        "Cross-border variant of `cycle`; same gap -- W7 targets "
        "`corridor_high_risk`, not this multi-hop cross-border form."
    ),
    "fan_in": (
        "Category W2_structuring in the manifest but no dedicated fan-in "
        "detector; W1_connected_components co-detects some instances."
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
        "Category W3_round_tripping but W3_round_tripping targets "
        "`rapid_layering`. scatter_gather recall is untestable today."
    ),
    "synthetic_identity": (
        "No dedicated synthetic-identity detector shipped yet; the "
        "`W5_splink_resolution` rule is spec-level future work per "
        "`detection_rules.py:26-28`."
    ),
    "tbml_repeated_invoice": (
        "Trade-Based ML repeated-invoice pattern is planted but no "
        "W-rule targets it. Adding a TBML detector is future work; "
        "recall/precision for TBML remains untestable until then."
    ),
}


_TEMPLATE_DIR = Path(__file__).resolve().parent / "queries" / "faml" / "trino"


@dataclass(frozen=True)
class FamlQuery:
    """One expanded FAML query, ready to execute."""

    query_id: str  # e.g. "W2_structuring_precision"
    sql: str
    rule_id: str | None  # rule the query attributes to, None for aggregates
    kind: str  # "detect" | "precision" | "recall" | "pattern_span" | "aggregate"


def _read_template(name: str) -> str:
    path = _TEMPLATE_DIR / name
    return path.read_text()


def load_faml_queries(catalog: str) -> list[FamlQuery]:
    """Return the full FAML query set instantiated for `catalog`.

    26 rule queries (8 detect + 6 targeted rules x 3 kinds) + 4 aggregate
    queries = 30 total.
    Query text uses `{catalog}` as a placeholder; the caller has
    already picked the catalog name (varies per config: iceberg,
    spark_catalog, polaris, etc.).
    """
    # Allowlist rather than blocklist: a blocklist misses newline, tab,
    # non-ASCII whitespace, comment sequences, and unicode "smart quotes",
    # any of which could smuggle SQL through a config-driven catalog name.
    # Real Trino / Spark catalog names are plain SQL identifiers.
    import re

    if not catalog or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError(f"catalog name {catalog!r} is not a plain identifier")

    detect_tmpl = _read_template("rule_detect.sql.tmpl")
    precision_tmpl = _read_template("rule_precision.sql.tmpl")
    recall_tmpl = _read_template("rule_recall.sql.tmpl")
    pattern_span_tmpl = _read_template("rule_pattern_span.sql.tmpl")

    queries: list[FamlQuery] = []
    for rule_id, typology_type in RULE_TARGETS.items():
        # Detect always emits; volume rule is inspectable regardless of
        # whether there's a planted typology.
        queries.append(
            FamlQuery(
                query_id=f"{rule_id}_detect",
                sql=detect_tmpl.format(catalog=catalog, rule_id=rule_id),
                rule_id=rule_id,
                kind="detect",
            )
        )
        # Precision / recall / pattern_span need a typology target. Rules that
        # have none produce no query in these kinds; downstream
        # scoring reports them as N/A.
        if typology_type is None:
            continue
        for kind, tmpl in (
            ("precision", precision_tmpl),
            ("recall", recall_tmpl),
            ("pattern_span", pattern_span_tmpl),
        ):
            queries.append(
                FamlQuery(
                    query_id=f"{rule_id}_{kind}",
                    sql=tmpl.format(
                        catalog=catalog,
                        rule_id=rule_id,
                        typology_type=typology_type,
                    ),
                    rule_id=rule_id,
                    kind=kind,
                )
            )

    # Aggregate queries. Same directory, no template expansion beyond
    # the catalog name.
    for filename, qid in (
        ("aggregate_alert_volume.sql", "aggregate_alert_volume"),
        ("aggregate_top_entities.sql", "aggregate_top_entities"),
        ("aggregate_typology_coverage.sql", "aggregate_typology_coverage"),
        ("aggregate_reference_vs_rule.sql", "aggregate_reference_vs_rule"),
    ):
        sql = _read_template(filename).format(catalog=catalog)
        queries.append(
            FamlQuery(
                query_id=qid,
                sql=sql,
                rule_id=None,
                kind="aggregate",
            )
        )

    return queries


def query_count() -> int:
    """Static count of queries produced. Useful for tests to detect
    accidental removal of a template."""
    n = 0
    for typ in RULE_TARGETS.values():
        n += 1  # detect
        if typ is not None:
            n += 3  # precision + recall + pattern_span
    n += 4  # aggregates: volume, top entities, typology coverage, reference-vs-rule
    return n


# Sanity: at import time, the file layout should be intact. The
# benchmark's CLI plumbing loads this module during config load, so
# a missing template file fails fast rather than at query run time.
def _self_check() -> None:
    for name in (
        "rule_detect.sql.tmpl",
        "rule_precision.sql.tmpl",
        "rule_recall.sql.tmpl",
        "rule_pattern_span.sql.tmpl",
        "aggregate_alert_volume.sql",
        "aggregate_top_entities.sql",
        "aggregate_typology_coverage.sql",
        "aggregate_reference_vs_rule.sql",
    ):
        if not (_TEMPLATE_DIR / name).exists():
            raise ImportError(
                f"FAML query template missing: {_TEMPLATE_DIR / name}. "
                "Reinstall the package or check the wheel build."
            )


if os.environ.get("LB_FAML_SKIP_SELF_CHECK") != "1":
    _self_check()
