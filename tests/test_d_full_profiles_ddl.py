"""D-full-profiles: `silver.entity_profiles` gains internal columns needed by
the continuous-mode Welford + additive MERGE update.

Additions across three DDL sites:

- ``_m2`` DOUBLE: Welford sum-of-squared-deviations accumulator so the
  streaming update can combine (n, mean, M2) blocks in the parallel
  algorithm rather than needing to re-scan all txns.
- ``_first_out_ts`` / ``_last_out_ts`` TIMESTAMP: originator-side first and
  last transaction timestamps. ``avg_gap_days`` is derived from the
  originator-side span (last send - first send) / (sends - 1); the public
  ``first_seen_ts`` / ``last_seen_ts`` are cross-side profile bounds and
  cannot be used for the originator-only gap. Kept as internal columns so
  the MERGE can apply LEAST / GREATEST without re-scanning.
- ``_stream_id`` STRING: idempotency-scope column mirroring the same B2
  pattern silver.transactions / silver.counterparty_edges use. Batch build
  stamps the sentinel ``'batch'``; stream writes carry the streaming query
  id.

The three DDL sites are:
1. ``src/lakebench/deploy/financial_ddl.py::SILVER_ENTITY_PROFILES_DDL`` --
   the deployer's authoritative DDL.
2. ``src/lakebench/spark/scripts/silver_build_financial.py::DDL_PROFILES``
   -- inline bootstrap in the batch main.
3. ``src/lakebench/spark/scripts/silver_stream_financial.py`` startup
   ``ensure_column`` calls -- brings a reused catalog forward.

Any drift between the three surfaces as either a runtime
`AnalysisException` on the MERGE or a silent NULL column, so all three
are gated here.
"""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_DDL_PATH = _REPO / "src/lakebench/deploy/financial_ddl.py"
_BUILD_PATH = _REPO / "src/lakebench/spark/scripts/silver_build_financial.py"
_STREAM_PATH = _REPO / "src/lakebench/spark/scripts/silver_stream_financial.py"

_NEW_COLS: tuple[str, ...] = ("_m2", "_first_out_ts", "_last_out_ts", "_stream_id")


def _deployer_ddl_block() -> str:
    text = _DDL_PATH.read_text(encoding="utf-8")
    marker = "SILVER_ENTITY_PROFILES_DDL"
    body = text[text.index(marker) :]
    # The DDL is bounded by the closing triple-quote of the raw string literal.
    return body.split('"""')[1]


def _inline_ddl_block() -> str:
    text = _BUILD_PATH.read_text(encoding="utf-8")
    marker = "DDL_PROFILES"
    body = text[text.index(marker) :]
    return body.split('"""')[1]


def test_deployer_ddl_declares_new_internal_columns():
    ddl = _deployer_ddl_block()
    for col in _NEW_COLS:
        assert col in ddl, (
            f"deployer SILVER_ENTITY_PROFILES_DDL missing {col!r} -- "
            "D-full-profiles Welford + streaming MERGE needs the internal "
            "aggregate accumulators"
        )
    assert "_m2" in ddl and "DOUBLE" in ddl, (
        "_m2 must be typed DOUBLE (Welford sum-of-squared-deviations)"
    )


def test_inline_ddl_declares_new_internal_columns():
    ddl = _inline_ddl_block()
    for col in _NEW_COLS:
        assert col in ddl, (
            f"silver_build_financial.DDL_PROFILES missing {col!r} -- "
            "batch-mode CREATE TABLE and stream ensure_column must agree"
        )


def test_inline_ddl_columns_match_deployer_ddl():
    """Column sets across the two DDL files must be identical (H1)."""
    import re

    def _cols(ddl: str) -> set[str]:
        # Extract column names from the CREATE TABLE column list. The DDL
        # is bounded by the first '(' after CREATE TABLE and its matching
        # ')'. The lower-case-first regex naturally excludes USING /
        # PARTITIONED / TBLPROPERTIES clauses without hunting for the
        # right ')' among nested parentheses.
        depth = 0
        start = ddl.index("(")
        end = start
        for i, ch in enumerate(ddl[start:], start=start):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        body = ddl[start + 1 : end]
        out: set[str] = set()
        for line in body.splitlines():
            stripped = line.strip().rstrip(",")
            # Column names in Iceberg-flavour DDL are lower_snake_case or
            # start with an underscore for internal accumulators; the
            # match excludes uppercase constraint clauses.
            m = re.match(r"^([a-z_][a-z0-9_]*)\s", stripped)
            if m:
                out.add(m.group(1))
        return out

    deployer = _cols(_deployer_ddl_block())
    inline = _cols(_inline_ddl_block())
    assert deployer == inline, (
        f"entity_profiles DDL drift: deployer-only={deployer - inline}, "
        f"inline-only={inline - deployer}"
    )


def test_stream_ensures_new_columns_on_startup():
    """A reused catalog whose entity_profiles predates D-full-profiles is
    forward-migrated at stream startup via ``ensure_column``. ``_batch_id``
    is included (BLOCKER 6) because the MERGE writes it and a legacy
    catalog whose entity_profiles predates the column would otherwise fail
    the MERGE with column-not-found."""
    src = _STREAM_PATH.read_text(encoding="utf-8")
    # Each new column needs an ensure_column call against SILVER_PROFILES (or
    # the resolved fq name) that names the column and its SQL type.
    expected_types = {
        "_m2": "DOUBLE",
        "_first_out_ts": "TIMESTAMP",
        "_last_out_ts": "TIMESTAMP",
        "_stream_id": "STRING",
        "_batch_id": "BIGINT",
    }
    for col, sql_type in expected_types.items():
        needle = f'"{col}", "{sql_type}"'
        assert needle in src, (
            f"silver_stream_financial startup does not ensure_column({needle}) on entity_profiles"
        )


def test_stream_import_pulls_profiles_symbols():
    """The stream side of the merge imports SILVER_PROFILES so the MERGE
    can name the target table without a bare literal."""
    src = _STREAM_PATH.read_text(encoding="utf-8")
    assert "SILVER_PROFILES" in src, (
        "silver_stream_financial must reference SILVER_PROFILES to MERGE into it"
    )


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
