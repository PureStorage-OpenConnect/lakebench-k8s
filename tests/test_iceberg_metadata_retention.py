"""Every Iceberg table lakebench creates deletes old metadata.json files.

Without write.metadata.delete-after-commit.enabled, Iceberg keeps every
metadata.json it ever wrote (expire_snapshots does not remove them), so a
continuous run's metadata objects grow with every micro-batch commit. These
tests scan the Spark scripts for every table-creating statement and fail when
one does not carry the retention properties.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
sys.path.insert(0, str(SCRIPTS))

ICEBERG_SCRIPTS = sorted(p for p in SCRIPTS.glob("*.py") if not p.stem.endswith("_delta"))

# A DDL statement always interpolates its table name, which keeps prose in
# docstrings and comments out of the scan.
_CREATE_DDL = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\{", re.IGNORECASE
)
_PROPS_PLACEHOLDER = re.compile(r"\{ICEBERG_(?:V2_SNAPPY|METADATA)_PROPS_SQL\}")


def _functions(tree: ast.AST) -> list[ast.AST]:
    return [tree] + [
        n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _sql_create_statements(text: str) -> list[str]:
    """Each f-string that issues CREATE [OR REPLACE] TABLE, except Delta DDL."""
    out = []
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.JoinedStr):
            seg = ast.get_source_segment(text, node) or ""
            if _CREATE_DDL.search(seg) and not re.search(r"USING\s+DELTA", seg, re.I):
                out.append(seg)
    return out


def _writer_create_chains(text: str) -> list[str]:
    """Source from the nearest writeTo( to each .create()/.createOrReplace() call.

    Parsed with ast, so comments and docstrings never match, and the search
    for writeTo( stays inside the innermost function holding the call.
    """
    tree = ast.parse(text)
    lines = text.splitlines(keepends=True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))

    def pos(lineno: int, col: int) -> int:
        return offsets[lineno - 1] + col

    funcs = _functions(tree)
    out = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("create", "createOrReplace")
            and not node.args
        ):
            continue
        owner = max(
            (f for f in funcs if f is tree or f.lineno <= node.lineno <= f.end_lineno),
            key=lambda f: 0 if f is tree else f.lineno,
        )
        lo = 0 if owner is tree else pos(owner.lineno, owner.col_offset)
        call_end = pos(node.end_lineno, node.end_col_offset)
        start = text.rfind("writeTo(", lo, call_end)
        assert start != -1, f"create() without a writeTo chain at line {node.lineno}"
        out.append(text[start:call_end])
    return out


def test_constants_render_the_retention_properties():
    import common

    assert common.METADATA_DELETE_AFTER_COMMIT == (
        "write.metadata.delete-after-commit.enabled",
        "true",
    )
    assert common.METADATA_PREVIOUS_VERSIONS_MAX == ("write.metadata.previous-versions-max", "50")
    assert "'write.metadata.delete-after-commit.enabled' = 'true'" in (
        common.ICEBERG_METADATA_PROPS_SQL
    )
    assert "'write.metadata.previous-versions-max' = '50'" in common.ICEBERG_METADATA_PROPS_SQL
    assert common.ICEBERG_V2_SNAPPY_PROPS_SQL.startswith(
        "'format-version' = '2', 'write.parquet.compression-codec' = 'snappy', "
    )
    assert common.ICEBERG_V2_SNAPPY_PROPS_SQL.endswith(common.ICEBERG_METADATA_PROPS_SQL)


def test_scan_finds_the_known_creation_sites():
    """Guards the scanner itself: a regex that matched nothing would pass."""
    sql = sum(len(_sql_create_statements(p.read_text())) for p in ICEBERG_SCRIPTS)
    chains = sum(len(_writer_create_chains(p.read_text())) for p in ICEBERG_SCRIPTS)
    assert sql >= 19
    assert chains >= 12


@pytest.mark.parametrize("path", ICEBERG_SCRIPTS, ids=lambda p: p.stem)
def test_every_sql_create_sets_metadata_retention(path):
    for stmt in _sql_create_statements(path.read_text()):
        assert _PROPS_PLACEHOLDER.search(stmt), (
            f"{path.name}: Iceberg CREATE without metadata retention:\n{stmt[:200]}"
        )


@pytest.mark.parametrize("path", ICEBERG_SCRIPTS, ids=lambda p: p.stem)
def test_every_writer_create_sets_metadata_retention(path):
    for chain in _writer_create_chains(path.read_text()):
        assert "METADATA_DELETE_AFTER_COMMIT" in chain, f"{path.name}:\n{chain[:300]}"
        assert "METADATA_PREVIOUS_VERSIONS_MAX" in chain, f"{path.name}:\n{chain[:300]}"


@pytest.mark.parametrize("path", ICEBERG_SCRIPTS, ids=lambda p: p.stem)
def test_no_v1_or_streaming_table_creation(path):
    """saveAsTable/toTable create a missing table without our properties.

    Allowed: common.write_delta_table (Delta only) and bronze_ingest_financial,
    whose toTable targets a table bronze_verify_financial already created and
    refuses to start when it is missing.
    """
    text = path.read_text()
    allowed = {"common": "saveAsTable", "bronze_ingest_financial": "toTable"}
    for name in ("saveAsTable", "toTable"):
        if allowed.get(path.stem) == name:
            continue
        assert f".{name}(" not in text, f"{path.name}: .{name}( creates tables unprotected"


def test_financial_ddl_mirror_declares_retention():
    from lakebench.deploy.financial_ddl import FINANCIAL_TABLE_DDLS

    ddls = FINANCIAL_TABLE_DDLS.values() if isinstance(FINANCIAL_TABLE_DDLS, dict) else None
    assert ddls is not None
    for ddl in ddls:
        assert "'write.metadata.delete-after-commit.enabled' = 'true'" in ddl
        assert "'write.metadata.previous-versions-max' = '50'" in ddl
