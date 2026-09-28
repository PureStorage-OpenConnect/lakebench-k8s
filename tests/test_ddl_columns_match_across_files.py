"""H1: DDL sync across silver definitions.

The silver-table DDLs are declared twice: once for the deployer (creates the
table on deploy) and once inline in the batch build script (creates the table
on first run when nothing deployed it). Two copies is a maintenance hazard --
any new column that lands in one must land in the other or a reused catalog
falls out of schema between deploy-created and script-created tables. This
test greps both definitions and asserts column-by-column equality.

Scope: the AML silver tables that both files own --
transactions, entities, accounts, account_statements, counterparty_edges,
entity_profiles. Deployer DDL constants live in
``src/lakebench/deploy/financial_ddl.py``; the batch script's inline DDLs
live in ``src/lakebench/spark/scripts/silver_build_financial.py``.

Notes for the next diff:

* When B2 lands, ``_stream_id STRING`` is added to ``SILVER_TRANSACTIONS_DDL``
  and ``SILVER_COUNTERPARTY_EDGES_DDL`` in the deployer and to ``DDL_TXNS``
  and ``DDL_EDGES`` in the batch script. This test then already passes -- it
  compares the two files symmetrically, not against a hardcoded list -- so
  the guard survives B2 without editing.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FINANCIAL_DDL = REPO_ROOT / "src/lakebench/deploy/financial_ddl.py"
SILVER_BUILD = REPO_ROOT / "src/lakebench/spark/scripts/silver_build_financial.py"


# Table pairs to compare: (deployer-constant-name, batch-script-constant-name).
_PAIRS = (
    ("SILVER_TRANSACTIONS_DDL", "DDL_TXNS"),
    ("SILVER_ENTITIES_DDL", "DDL_ENTITIES"),
    ("SILVER_ACCOUNTS_DDL", "DDL_ACCOUNTS"),
    ("SILVER_ACCOUNT_STATEMENTS_DDL", "DDL_STATEMENTS"),
    ("SILVER_COUNTERPARTY_EDGES_DDL", "DDL_EDGES"),
    ("SILVER_ENTITY_PROFILES_DDL", "DDL_PROFILES"),
)


_ASSIGN_RE = re.compile(
    r'^(?P<name>[A-Z_][A-Z0-9_]*)\s*=\s*f?"""(?P<body>.*?)"""',
    re.MULTILINE | re.DOTALL,
)


def _load_ddls(path: Path) -> dict[str, str]:
    """Extract triple-quoted DDL string assignments keyed by constant name."""
    text = path.read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for m in _ASSIGN_RE.finditer(text):
        out[m.group("name")] = m.group("body")
    return out


_COL_LINE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+([A-Z][A-Z0-9<>()\s,]*?)\s*(?:,|$)")


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


def test_silver_ddl_columns_match_across_files():
    deployer_ddls = _load_ddls(FINANCIAL_DDL)
    build_ddls = _load_ddls(SILVER_BUILD)

    mismatches: list[str] = []
    for deployer_name, build_name in _PAIRS:
        assert deployer_name in deployer_ddls, (
            f"{deployer_name} not found in {FINANCIAL_DDL}; test needs updating."
        )
        assert build_name in build_ddls, (
            f"{build_name} not found in {SILVER_BUILD}; test needs updating."
        )
        deployer_cols = _extract_columns(deployer_ddls[deployer_name])
        build_cols = _extract_columns(build_ddls[build_name])
        assert deployer_cols, f"{deployer_name} parsed no columns"
        assert build_cols, f"{build_name} parsed no columns"

        deployer_names = [c[0] for c in deployer_cols]
        build_names = [c[0] for c in build_cols]
        if deployer_names != build_names:
            mismatches.append(
                f"{deployer_name} vs {build_name}: column set differs.\n"
                f"  deployer: {deployer_names}\n"
                f"  build:    {build_names}"
            )
            continue

        for (d_name, d_type), (b_name, b_type) in zip(deployer_cols, build_cols, strict=True):
            # Types compared case-insensitively; whitespace already normalised.
            if d_type.lower() != b_type.lower():
                mismatches.append(
                    f"{deployer_name}.{d_name} type '{d_type}' != "
                    f"{build_name}.{b_name} type '{b_type}'"
                )

    assert not mismatches, "DDL column drift:\n" + "\n".join(mismatches)


def test_extract_columns_helper_sane():
    """The parser must recognise nested STRUCT types and inline SQL comments."""
    body = """
    CREATE TABLE IF NOT EXISTS x.y (
        a BIGINT NOT NULL,
        b STRUCT<c: STRING, d: STRING>,   -- inline comment
        e DECIMAL(38, 2)
    )
    USING iceberg
    """
    cols = _extract_columns(body)
    assert cols == [
        ("a", "BIGINT"),
        ("b", "STRUCT<c: STRING, d: STRING>"),
        ("e", "DECIMAL(38, 2)"),
    ]
