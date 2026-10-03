"""AML-2: every detection rule projects its alerts through one helper.

gold_finalize writes each rule's alerts with a positional
``INSERT INTO gold.alerts SELECT * FROM <view>``, so a rule that picked its
own column order would write same-typed columns into each other's slots
(priority, status, disposition, alert_type and narrative are all STRING).
Since AML-2 every rule returns through ``detection_rules._alert_frame``,
which selects in ``ALERT_COLUMNS`` order, and nothing else in the module
builds an alert projection.

Also here, because AML-2 is the first change to these modules after the
freeze rule (design 04 "Frozen-symbol recheck (d3)", Open risk 13): the
frozen ``aml_features.py`` imports ``detection_rules`` and the frozen
``bronze_verify_financial.py`` imports ``tm_operations``, so their module top
level runs inside frozen scripts. Their module-level statements other than
defs, classes and literal assignments, and the pinned symbols, stay
byte-identical to integrate dfc8afa7. Interim: decorators, default
arguments and class bodies are not hashed; QR-10's frozen guard replaces
this check.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"
DETECTION_RULES = SCRIPTS / "detection_rules.py"

# The gold.alerts columns as v1.6 shipped them. Columns may be appended
# (AML-5 adds reason_codes), never inserted or reordered.
V16_ALERT_COLUMNS = (
    ("alert_id", "STRING", False),
    ("rule_id", "STRING", False),
    ("rule_version", "STRING", False),
    ("model_id", "STRING", False),
    ("model_version", "STRING", False),
    ("entity_id", "BIGINT", False),
    ("related_txn_ids", "ARRAY<STRING>", True),
    ("related_entity_ids", "ARRAY<BIGINT>", True),
    ("alert_ts", "TIMESTAMP", False),
    ("alert_score", "DOUBLE", True),
    ("priority", "STRING", True),
    ("status", "STRING", True),
    ("disposition", "STRING", True),
    ("alert_type", "STRING", True),
    ("run_id", "STRING", False),
    ("narrative", "STRING", True),
    ("evidence", "MAP<STRING, STRING>", True),
    ("detected_ts", "TIMESTAMP", True),
)

# Producers of an alert frame: a call to one of these, a name bound to one in
# the same function, or a unionByName / _customers_only over producers.
_PRODUCERS = {"_alert_frame", "_screen_alerts", "_empty_alerts_df"}


def _own_nodes(fn: ast.FunctionDef):
    """Nodes of ``fn`` outside nested functions and lambdas."""
    nested = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
    stack = [n for n in fn.body if not isinstance(n, nested)]
    while stack:
        node = stack.pop()
        yield node
        stack.extend(c for c in ast.iter_child_nodes(node) if not isinstance(c, nested))


def _is_producer(node: ast.AST, bound: set[str]) -> bool:
    if isinstance(node, ast.Name):
        return node.id in bound
    if not isinstance(node, ast.Call):
        return False
    f = node.func
    if isinstance(f, ast.Name) and f.id in _PRODUCERS:
        return True
    if isinstance(f, ast.Name) and f.id == "_customers_only":
        return bool(node.args) and _is_producer(node.args[0], bound)
    if isinstance(f, ast.Attribute) and f.attr == "unionByName":
        return _is_producer(f.value, bound) and all(_is_producer(a, bound) for a in node.args)
    return False


def _alias_names(call: ast.Call) -> list[str]:
    out = []
    for arg in call.args:
        if (
            isinstance(arg, ast.Call)
            and isinstance(arg.func, ast.Attribute)
            and arg.func.attr == "alias"
            and arg.args
            and isinstance(arg.args[0], ast.Constant)
        ):
            out.append(arg.args[0].value)
    return out


def violations(source: str) -> list[str]:
    """Why ``source`` (detection_rules.py) breaks the one-helper rule."""
    tree = ast.parse(source)
    fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    dispatch = next(
        n
        for n in tree.body
        if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", None) == "_RULE_DISPATCH"
    )
    rules = [v.id for v in dispatch.value.values]  # type: ignore[attr-defined]
    found = []
    for name in [*rules, "_screen_alerts"]:
        fn = fns[name]
        nodes = list(_own_nodes(fn))
        bound: set[str] = set()
        # Names bound to a producer, in source order (a rebinding to a
        # non-producer drops the name).
        for node in sorted(
            (n for n in nodes if isinstance(n, ast.Assign)), key=lambda n: (n.lineno, n.col_offset)
        ):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    if _is_producer(node.value, bound):
                        bound.add(t.id)
                    else:
                        bound.discard(t.id)
        returns = [n for n in nodes if isinstance(n, ast.Return)]
        if not returns:
            found.append(f"{name}: no return")
        for r in returns:
            if r.value is None or not _is_producer(r.value, bound):
                found.append(f"{name}:{r.lineno} returns something other than an alert frame")
    for fn in fns.values():
        if fn.name == "_alert_frame":
            continue
        for node in ast.walk(fn):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "select"
                and "alert_id" in _alias_names(node)
            ):
                found.append(f"{fn.name}:{node.lineno} builds its own alert projection")
    return found


def test_every_rule_returns_through_the_helper():
    assert violations(DETECTION_RULES.read_text()) == []


def test_a_rule_with_its_own_projection_fails():
    """The LB-125 shape: a rule that selects its own columns, here W8 with
    two STRING columns swapped, is refused."""
    src = DETECTION_RULES.read_text()
    fn = next(
        n
        for n in ast.parse(src).body
        if isinstance(n, ast.FunctionDef) and n.name == "w8_dormant_reactivation"
    )
    lines = src.splitlines(keepends=True)
    body = "".join(lines[fn.lineno - 1 : fn.end_lineno])
    mutated_fn = body.replace(
        "    return _customers_only(alerts, customers)",
        "    return hits.select(\n"
        '        expr("uuid()").alias("alert_id"),\n'
        '        lit("W8_dormant_reactivation").alias("rule_id"),\n'
        '        lit("MED").alias("status"),\n'
        '        lit("OPEN").alias("priority"),\n'
        "    )",
    )
    assert mutated_fn != body
    found = violations(src.replace(body, mutated_fn))
    assert any("w8_dormant_reactivation" in f and "own alert projection" in f for f in found)
    assert any("w8_dormant_reactivation" in f and "returns something other" in f for f in found)


def _alert_columns() -> tuple:
    node = next(
        n
        for n in ast.parse(DETECTION_RULES.read_text()).body
        if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", None) == "ALERT_COLUMNS"
    )
    return ast.literal_eval(node.value)


def test_alert_columns_only_append_to_the_v16_table():
    cols = _alert_columns()
    assert cols[: len(V16_ALERT_COLUMNS)] == V16_ALERT_COLUMNS
    names = [c[0] for c in cols]
    assert len(names) == len(set(names))


def test_gold_alerts_ddl_renders_the_v16_column_block():
    """gold_finalize renders DDL_ALERTS from ALERT_COLUMNS; with the v1.6
    columns the rendered block is the v1.6 literal, character for character."""
    v16_block = """\
    alert_id           STRING NOT NULL,
    rule_id            STRING NOT NULL,
    rule_version       STRING NOT NULL,
    model_id           STRING NOT NULL,
    model_version      STRING NOT NULL,
    entity_id          BIGINT NOT NULL,
    related_txn_ids    ARRAY<STRING>,
    related_entity_ids ARRAY<BIGINT>,
    alert_ts           TIMESTAMP NOT NULL,
    alert_score        DOUBLE,
    priority           STRING,
    status             STRING,
    disposition        STRING,
    alert_type         STRING,
    run_id             STRING NOT NULL,
    narrative          STRING,
    evidence           MAP<STRING, STRING>,
    detected_ts        TIMESTAMP"""
    gf = (SCRIPTS / "gold_finalize_financial.py").read_text()
    tree = ast.parse(gf)
    render = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_alerts_ddl_columns"
    )
    ns: dict = {"ALERT_COLUMNS": V16_ALERT_COLUMNS}
    exec(compile(ast.Module(body=[render], type_ignores=[]), "gf", "exec"), ns)  # noqa: S102
    assert ns["_alerts_ddl_columns"]() == v16_block
    assert "{_alerts_ddl_columns()}" in gf


# Source of the module-level statements a frozen importer runs (all but the
# docstring, defs, classes and literal assignments), and of each pinned
# symbol, at integrate dfc8afa7 (sha256, first 16 hex). Moving one needs a
# Freeze-cost trailer and must land before the Level-2 predictions lock.
_FROZEN_BASELINE = {
    "detection_rules.py": (
        "4fa6b8481121ad61",
        {
            "_STRUCTURING_THRESHOLDS": "e18c54a588b8bba4",
            "_suspicious_amount_expr": "da2b4aacf24873ac",
        },
    ),
    "tm_operations.py": (
        "19f4b629ec8d28db",
        {
            "DDL_CASES": "9af3891f8d999a59",
            "DDL_COVERAGE": "09dfc38fe7854f4c",
            "DDL_DISPOSITIONS": "c2f294f224981d3e",
            "DDL_RECON": "e3bebf957c456a0b",
            "GOLD_CASES": "71c3174e9ebcb2b8",
            "GOLD_COVERAGE": "434b038f36b2e909",
            "GOLD_DISPOSITIONS": "39b385791185c3ad",
            "GOLD_RECON": "8e3ed088c38c162a",
            "TM_TABLES": "76f9c99710750433",
        },
    ),
}


def _h(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _is_literal_assign(n: ast.stmt) -> bool:
    if not isinstance(n, (ast.Assign, ast.AnnAssign)) or n.value is None:
        return False
    targets = n.targets if isinstance(n, ast.Assign) else [n.target]
    if not all(isinstance(t, ast.Name) for t in targets):
        return False  # x[k] = 1 or x.a = 1 mutates an existing object
    try:
        ast.literal_eval(n.value)
    except (ValueError, TypeError, SyntaxError):
        return False
    return True


def _frozen_signature(path: Path, pinned: set[str]) -> tuple[str, dict[str, str]]:
    """Hash of every module-level statement other than the docstring, a def,
    a class or a literal assignment (imports, expressions, computed or
    augmented assignments: what a frozen importer runs), and per pinned name
    the hash of the statement that defines it."""
    src = path.read_text()
    tree = ast.parse(src)
    top = []
    found: dict[str, str] = {}
    for i, n in enumerate(tree.body):
        docstring = (
            i == 0
            and isinstance(n, ast.Expr)
            and isinstance(n.value, ast.Constant)
            and isinstance(n.value.value, str)
        )
        if not (
            docstring
            or isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            or _is_literal_assign(n)
        ):
            top.append(ast.get_source_segment(src, n) or "")
        names: list[str] = []
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)):
            names = [n.name]
        elif isinstance(n, ast.Assign):
            names = [t.id for t in n.targets if isinstance(t, ast.Name)]
        elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name):
            names = [n.target.id]
        for name in names:
            if name in pinned:
                found[name] = _h(ast.get_source_segment(src, n) or "")
    return _h("\n".join(top)), dict(sorted(found.items()))


def test_frozen_import_targets_top_level_unchanged():
    for name, (top, pinned) in _FROZEN_BASELINE.items():
        got_top, got_pinned = _frozen_signature(SCRIPTS / name, set(pinned))
        assert got_top == top, (
            f"{name}: a module-level statement other than a def, class or literal "
            "assignment changed; frozen scripts run it"
        )
        assert got_pinned == pinned, f"{name}: a pinned symbol changed"


def _ddl_column_names(text: str) -> list[str]:
    """(name, type) of each column of the first CREATE TABLE ( ... ) block
    in *text*, the type without NOT NULL."""
    body = text.split("(", 1)[1]
    cols = []
    for line in body.splitlines():
        line = line.split("--", 1)[0].strip().rstrip(",")
        if not line:
            continue
        if line.startswith(")"):
            break
        name, rest = line.split(None, 1)
        cols.append((name, rest.replace(" NOT NULL", "").strip()))
    return cols


def test_other_alert_ddl_copies_match_alert_columns():
    """Two gold.alerts column lists are not built from ALERT_COLUMNS yet
    (replay's target table and empty schema, deploy's DDL). A column
    appended to ALERT_COLUMNS, or a type changed, without them fails here.
    (A reused replay target table still needs its own upgrade for a new
    column; replay adds only detected_ts today.)"""
    cols = [(n, t) for n, t, _ in _alert_columns()]
    names = [n for n, _ in cols]
    rp = (SCRIPTS / "replay_financial.py").read_text()
    replay_ddl = rp[rp.index("CREATE TABLE IF NOT EXISTS {args.output_alerts}") :]
    assert _ddl_column_names(replay_ddl) == cols
    empty = rp[rp.index("def _empty_alerts_df") :]
    empty = empty[: empty.index("return spark.createDataFrame")]
    import re

    assert re.findall(r'"(\w+) [A-Z]', empty) == names
    fd = (SCRIPTS.parent.parent / "deploy" / "financial_ddl.py").read_text()
    i = fd.index("related_txn_ids    ARRAY<STRING>")
    start = fd.rindex("CREATE TABLE", 0, i)
    assert [n for n, _ in _ddl_column_names(fd[start:])] == names
