"""DDL sync across silver definitions.

The silver-table DDLs are declared twice: once for the deployer (creates the
table on deploy, ``lakebench.deploy.financial_ddl``) and once inline in the
batch build script (creates the table on first run when nothing deployed it,
``silver_build_financial``). A column that lands in one and not the other
leaves a reused catalog out of schema between deploy-created and
script-created tables. The two are compared column by column, not against a
hardcoded list.
"""

from __future__ import annotations

import re
import sys
from unittest.mock import MagicMock

import pytest

from lakebench.deploy import financial_ddl

# Table pairs to compare: (deployer-constant-name, batch-script-constant-name).
_PAIRS = (
    ("SILVER_TRANSACTIONS_DDL", "DDL_TXNS"),
    ("SILVER_ENTITIES_DDL", "DDL_ENTITIES"),
    ("SILVER_ACCOUNTS_DDL", "DDL_ACCOUNTS"),
    ("SILVER_ACCOUNT_STATEMENTS_DDL", "DDL_STATEMENTS"),
    ("SILVER_COUNTERPARTY_EDGES_DDL", "DDL_EDGES"),
    ("SILVER_ENTITY_PROFILES_DDL", "DDL_PROFILES"),
    ("SILVER_BATCH_VERSIONS_DDL", "DDL_BATCH_VERSIONS"),
)


def _extract_columns(ddl_body: str) -> list[tuple[str, str]]:
    """Parse a ``CREATE TABLE ... ( ... )`` body into ordered (name, type) pairs.

    The parser walks lines inside the outermost parentheses, strips SQL
    comments (``--``), skips blank/comment-only lines, and pulls the leading
    identifier and its SQL type. Nested ``<>`` and ``()`` in STRUCT / DECIMAL
    types are preserved so ``STRUCT<street: STRING, ...>`` reads as one type.
    """
    # Slice to inside the outermost parentheses of the CREATE TABLE body.
    open_paren = ddl_body.find("(")
    if open_paren < 0:
        return []
    # Match parens to find the closing one for the column list.
    depth = 0
    end = -1
    for i in range(open_paren, len(ddl_body)):
        ch = ddl_body[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        return []
    inner = ddl_body[open_paren + 1 : end]

    # Strip SQL "--" comments line-by-line before splitting on commas. If a
    # column line ended with a trailing comma AND a comment (``STRING, -- ...``),
    # the naive comma-split would keep the comment inside the next entry and
    # then the leading-comment strip erases the following column name entirely.
    cleaned_lines = []
    for ln in inner.splitlines():
        idx = ln.find("--")
        if idx >= 0:
            ln = ln[:idx]
        cleaned_lines.append(ln)
    inner = "\n".join(cleaned_lines)

    # Split on commas outside <>, () to get one raw entry per column.
    entries: list[str] = []
    buf: list[str] = []
    angle_depth = 0
    paren_depth = 0
    for ch in inner:
        if ch == "<":
            angle_depth += 1
        elif ch == ">":
            angle_depth -= 1
        elif ch == "(":
            paren_depth += 1
        elif ch == ")":
            paren_depth -= 1
        if ch == "," and angle_depth == 0 and paren_depth == 0:
            entries.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        entries.append("".join(buf))

    columns: list[tuple[str, str]] = []
    for raw in entries:
        # Strip inline SQL comment.
        stripped = raw.split("--", 1)[0].strip()
        if not stripped:
            continue
        # Split off the first token as the column name.
        parts = stripped.split(None, 1)
        if len(parts) != 2:
            continue
        name, sql_type = parts[0], parts[1]
        # Normalise NOT NULL / defaults out of the type so the compare is
        # about the column set, not per-file phrasing.
        sql_type = re.sub(r"\s+NOT\s+NULL\b", "", sql_type, flags=re.IGNORECASE).strip()
        sql_type = re.sub(r"\s+", " ", sql_type)
        # ``STRUCT< street: STRING, ... >`` vs ``STRUCT<street: STRING, ... >``:
        # collapse padding immediately inside angle-bracket type parameters so
        # the two spellings are read as the same type.
        sql_type = re.sub(r"<\s+", "<", sql_type)
        sql_type = re.sub(r"\s+>", ">", sql_type)
        columns.append((name, sql_type))
    return columns


@pytest.mark.parametrize(("deployer_name", "build_name"), _PAIRS)
def test_silver_ddl_columns_match_across_files(deployer_name, build_name, monkeypatch, load_script):
    for mod in (
        "pyspark",
        "pyspark.sql",
        "pyspark.sql.functions",
        "pyspark.sql.types",
        "pyspark.sql.window",
    ):
        monkeypatch.setitem(sys.modules, mod, MagicMock())
    build = load_script("silver_build_financial")

    deployer_cols = _extract_columns(getattr(financial_ddl, deployer_name))
    build_cols = _extract_columns(getattr(build, build_name))
    assert deployer_cols, f"{deployer_name} parsed no columns"
    assert build_cols, f"{build_name} parsed no columns"
    assert [c[0] for c in deployer_cols] == [c[0] for c in build_cols]
    # Types compared case-insensitively; whitespace already normalised.
    assert [(n, t.lower()) for n, t in deployer_cols] == [(n, t.lower()) for n, t in build_cols]
