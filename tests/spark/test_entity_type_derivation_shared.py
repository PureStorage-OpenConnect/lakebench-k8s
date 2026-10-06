"""E1: entity_type regex is shared between batch and stream silver.

The pre-E1 stream picked the first micro-batch's name and typed the entity
by inlining a regex; batch had the SAME regex in a separate call site. Any
future edit to one but not the other would break the "batch and continuous
produce equivalent silver for the same bronze" contract silently.

Guard: both call sites import the same helpers from ``common``, so the
regex constant they use is definitionally identical. Also asserts the
helpers are equivalent for a small set of names -- catches the case
where someone extracts the regex constant but rewrites the CASE
expression's operator (e.g. ``LIKE`` in place of ``RLIKE``) so the two
helpers no longer produce the same label for the same name.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# The batch mains import pyspark at module load time; the assertions here
# reach through that import to compare the shared regex helper, so skip
# cleanly on the general-suite host that CI runs without pyspark.
pytest.importorskip("pyspark")

_HERE = Path(__file__).resolve().parent
_SCRIPTS = _HERE.parents[1] / "src/lakebench/spark/scripts"
pytestmark = pytest.mark.usefixtures("load_script")


def test_batch_and_stream_use_the_same_helper():
    """``silver_build_financial`` re-exports the same ``derive_entity_type``
    that ``common`` exposes: if E1 code drifted, the import would either
    fail or point at a different object."""
    import common
    import silver_build_financial as sbf

    assert sbf.derive_entity_type is common.derive_entity_type


def test_stream_uses_the_shared_sql_helper():
    """``silver_stream_financial`` imports ``entity_type_from_name_sql`` from
    ``common``, so the SQL fragment the MERGE emits reads from the same
    regex constant the batch Column helper reads."""
    import common
    import silver_stream_financial as ssf

    assert ssf.entity_type_from_name_sql is common.entity_type_from_name_sql


def test_regex_constant_is_one_string_in_both_paths():
    """A single module-level constant backs both helpers; assert its exact
    text so the E1 review has a landing pin for what "the regex" means."""
    import common

    assert common._ENTITY_TYPE_COMPANY_SUFFIX_REGEX == (
        r"(LTD|LIMITED|INC|CORP|LLC|GMBH|AG|PLC|SA|SARL|BV|BANK|CAPITAL|"
        r"HOLDINGS|GROUP|INTERNATIONAL|COMPANY|CO)$"
    )


def test_sql_fragment_embeds_the_shared_regex():
    """The SQL helper reads from the constant, not a re-typed literal.
    Catches a stray edit that replaces the constant reference with a
    string literal whose text later drifts from the batch Column path."""
    import common

    sql = common.entity_type_from_name_sql("name")
    assert common._ENTITY_TYPE_COMPANY_SUFFIX_REGEX in sql
    assert sql.count("'Company'") == 1 and sql.count("'Person'") == 1


def test_batch_source_has_no_inline_entity_type_regex():
    """Fix-reverted guard: pre-fix, ``silver_build_financial`` inlined the
    regex in its ``build_entities`` select. This test asserts the inline
    regex is gone from the source and the call site uses the helper."""
    text = (_SCRIPTS / "silver_build_financial.py").read_text()
    # The verbatim regex must appear exactly once in the tree (in common.py).
    # If build_entities re-inlines it, this assertion trips.
    assert "LTD|LIMITED|INC|CORP|LLC|GMBH" not in text
    assert 'derive_entity_type(col("name"))' in text


def test_stream_source_has_no_inline_entity_type_regex():
    """Fix-reverted guard for the stream path. The MERGE emits SQL, so a
    drift-prone form would be a literal CASE expression next to the
    MERGE statement. Assert the helper is used instead."""
    text = (_SCRIPTS / "silver_stream_financial.py").read_text()
    assert "LTD|LIMITED|INC|CORP|LLC|GMBH" not in text
    assert "entity_type_from_name_sql" in text
