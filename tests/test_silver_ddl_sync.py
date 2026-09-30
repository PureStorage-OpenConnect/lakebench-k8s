"""H1: silver CREATE TABLE column lists in `financial_ddl.py` and in the
inline `DDL_*` constants in `silver_build_financial.py` must match per
table. The stream and batch bootstrap paths both call CREATE TABLE IF NOT
EXISTS, and drift between the two locations silently ships a table with
one schema in one deploy path and another schema in the other.

Approach: parse each CREATE TABLE body (between the opening `(` and its
matching `)`), strip line comments and whitespace, tokenise into
``(column_name, column_type)`` pairs, and set-compare per table. Types are
canonicalised (lowercased, whitespace collapsed, trailing NOT NULL retained)
so a real semantic mismatch fails the test but a purely cosmetic difference
(alignment spaces, comments) does not.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_DEPLOY_DDL = _REPO / "src/lakebench/deploy/financial_ddl.py"
_SCRIPT = _REPO / "src/lakebench/spark/scripts/silver_build_financial.py"


def _strip_line_comments(text: str) -> str:
    """Remove SQL ``--`` line comments, preserving inline content up to them."""
    out = []
    for line in text.splitlines():
        idx = line.find("--")
        if idx >= 0:
            line = line[:idx]
        out.append(line)
    return "\n".join(out)


def _extract_table_body(ddl: str) -> str:
    """Return the parenthesised column-list body of a CREATE TABLE.

    Finds ``CREATE TABLE`` then the first ``(`` after the table name, then
    walks characters tracking paren depth to find the matching ``)``.
    """
    m = re.search(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^\(]*\(", ddl, re.IGNORECASE)
    if not m:
        raise AssertionError(f"CREATE TABLE opening paren not found in DDL:\n{ddl[:200]}")
    start = m.end() - 1  # position of the opening '('
    depth = 0
    for i in range(start, len(ddl)):
        ch = ddl[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return ddl[start + 1 : i]
    raise AssertionError("Unbalanced parentheses in DDL body")


def _split_top_level_commas(body: str) -> list[str]:
    """Split by comma at paren-depth 0, so STRUCT<a: INT, b: STRING> stays whole."""
    parts, buf, depth = [], [], 0
    for ch in body:
        if ch in "(<":
            depth += 1
            buf.append(ch)
        elif ch in ")>":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def _canonicalise_type(type_text: str) -> str:
    """Lowercase, collapse whitespace, drop spaces around structural
    punctuation, so ``STRUCT< a: int, b: STRING >`` and
    ``struct<a: int, b: string>`` compare equal."""
    t = re.sub(r"\s+", " ", type_text.strip()).lower()
    # DECIMAL(18, 2) vs DECIMAL(18,2) -> canonical `decimal(18,2)`.
    t = re.sub(r"\s*,\s*", ",", t)
    # STRUCT< foo > -> STRUCT<foo>, ARRAY< bar > -> ARRAY<bar>.
    t = re.sub(r"\s*<\s*", "<", t)
    t = re.sub(r"\s*>\s*", ">", t)
    # DECIMAL ( 18,2 ) -> DECIMAL(18,2).
    t = re.sub(r"\s*\(\s*", "(", t)
    t = re.sub(r"\s*\)\s*", ")", t)
    return t


def _parse_columns(ddl: str) -> dict[str, str]:
    """Return {column_name: canonical type} for a CREATE TABLE DDL string."""
    body = _strip_line_comments(_extract_table_body(ddl))
    out: dict[str, str] = {}
    for part in _split_top_level_commas(body):
        tokens = part.split(None, 1)
        if len(tokens) != 2:
            raise AssertionError(f"Cannot parse column entry: {part!r}")
        name, rest = tokens
        out[name.lower()] = _canonicalise_type(rest)
    return out


def _load_str_assignments(pyfile: Path) -> dict[str, str]:
    """Return {name: string_value} for every top-level ``NAME = "..."``
    assignment where the RHS is a plain (or f-string) string constant we
    can resolve without executing Spark. F-strings' ``{CATALOG}`` /
    ``{SILVER_*}`` / ``{ICEBERG_V2_SNAPPY_PROPS_SQL}`` placeholders are
    substituted with harmless stand-ins so parsing still works.
    """
    tree = ast.parse(pyfile.read_text())
    out: dict[str, str] = {}
    placeholders = {
        "CATALOG": "lakehouse",
        "SILVER_TRANSACTIONS": "silver.transactions",
        "SILVER_ENTITIES": "silver.entities",
        "SILVER_ACCOUNTS": "silver.accounts",
        "SILVER_STATEMENTS": "silver.account_statements",
        "SILVER_EDGES": "silver.counterparty_edges",
        "SILVER_PROFILES": "silver.entity_profiles",
        "SILVER_BATCH_VERSIONS": "silver.silver_batch_versions",
        "ICEBERG_V2_SNAPPY_PROPS_SQL": "'format-version' = '2'",
    }
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        value = node.value
        text = _extract_string(value, placeholders)
        if text is not None:
            out[target.id] = text
    return out


def _extract_string(node, placeholders):
    """Resolve a small subset of expressions that appear on the RHS of the
    DDL string constants: plain strings, f-strings composed of names in
    ``placeholders``, and ``.strip()`` calls on those.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                parts.append(v.value)
            elif isinstance(v, ast.FormattedValue) and isinstance(v.value, ast.Name):
                parts.append(placeholders.get(v.value.id, f"{{{v.value.id}}}"))
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        # `"""..."""` .strip() or .lstrip()/.rstrip(): recurse on the base
        # and apply the trailing whitespace method.
        base = _extract_string(node.func.value, placeholders)
        if base is None:
            return None
        method = node.func.attr
        if method == "strip":
            return base.strip()
        if method == "lstrip":
            return base.lstrip()
        if method == "rstrip":
            return base.rstrip()
    return None


# Tables to cross-check: (deploy DDL constant, script DDL constant).
_TABLES = [
    ("silver_transactions", "SILVER_TRANSACTIONS_DDL", "DDL_TXNS"),
    ("silver_entities", "SILVER_ENTITIES_DDL", "DDL_ENTITIES"),
    ("silver_accounts", "SILVER_ACCOUNTS_DDL", "DDL_ACCOUNTS"),
    ("silver_statements", "SILVER_ACCOUNT_STATEMENTS_DDL", "DDL_STATEMENTS"),
    ("silver_edges", "SILVER_COUNTERPARTY_EDGES_DDL", "DDL_EDGES"),
    ("silver_profiles", "SILVER_ENTITY_PROFILES_DDL", "DDL_PROFILES"),
    ("silver_batch_versions", "SILVER_BATCH_VERSIONS_DDL", "DDL_BATCH_VERSIONS"),
]


@pytest.fixture(scope="module")
def deploy_ddls():
    return _load_str_assignments(_DEPLOY_DDL)


@pytest.fixture(scope="module")
def script_ddls():
    return _load_str_assignments(_SCRIPT)


@pytest.mark.parametrize("label,deploy_name,script_name", _TABLES)
def test_column_names_match(label, deploy_name, script_name, deploy_ddls, script_ddls):
    deploy = _parse_columns(deploy_ddls[deploy_name])
    script = _parse_columns(script_ddls[script_name])
    missing_in_script = set(deploy) - set(script)
    missing_in_deploy = set(script) - set(deploy)
    assert not missing_in_script and not missing_in_deploy, (
        f"{label}: column-name drift. "
        f"deploy only={sorted(missing_in_script)}, script only={sorted(missing_in_deploy)}"
    )


@pytest.mark.parametrize("label,deploy_name,script_name", _TABLES)
def test_column_types_match(label, deploy_name, script_name, deploy_ddls, script_ddls):
    deploy = _parse_columns(deploy_ddls[deploy_name])
    script = _parse_columns(script_ddls[script_name])
    diffs = {
        name: (deploy[name], script[name])
        for name in deploy
        if name in script and deploy[name] != script[name]
    }
    assert not diffs, f"{label}: type drift per column: {diffs}"
