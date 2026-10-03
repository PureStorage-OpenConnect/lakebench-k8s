"""AM-27 (LB-226): no Spark script time-travels with the ``snapshot-id``
or ``as-of-timestamp`` DataFrameReader options.

The Iceberg 1.11 runtime for Spark 4.1 refuses both ("Time travel option
`snapshot-id` is no longer supported, use Spark built-in `versionAsOf`").
The 4.0 runtime still accepts them, so the 4.0 Spark-tier leg alone would
not catch a reintroduction; this static check covers every script on both
lines. Reads at a snapshot use SQL ``VERSION AS OF`` (replay's
``read_at_snapshot_id``, tm_operations' ``read_at_snapshot``).
"""

from __future__ import annotations

import ast
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"
_REMOVED = {"snapshot-id", "as-of-timestamp"}


def _removed_option_calls(tree: ast.AST) -> list[int]:
    lines = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr == "option" and node.args:
            key = node.args[0]
            if isinstance(key, ast.Constant) and key.value in _REMOVED:
                lines.append(node.lineno)
        elif node.func.attr == "options":
            # options({"snapshot-id": ...}) or options(**{"snapshot-id": ...})
            dicts = [a for a in node.args if isinstance(a, ast.Dict)]
            dicts += [kw.value for kw in node.keywords if isinstance(kw.value, ast.Dict)]
            for d in dicts:
                if any(isinstance(k, ast.Constant) and k.value in _REMOVED for k in d.keys):
                    lines.append(node.lineno)
    return lines


def test_no_script_uses_a_removed_time_travel_option():
    found = []
    for path in sorted(SCRIPTS.glob("*.py")):
        for line in _removed_option_calls(ast.parse(path.read_text(), filename=str(path))):
            found.append(f"{path.name}:{line}")
    assert not found, f"removed Iceberg time-travel read option used at {found}"


def test_the_check_sees_the_lb226_form():
    old = 'df = spark.read.option("snapshot-id", snap).table(t)\n'
    assert _removed_option_calls(ast.parse(old)) == [1]
    assert _removed_option_calls(ast.parse('spark.read.options(**{"as-of-timestamp": 1})\n')) == [1]
    assert _removed_option_calls(ast.parse('spark.read.option("versionAsOf", snap)\n')) == []
