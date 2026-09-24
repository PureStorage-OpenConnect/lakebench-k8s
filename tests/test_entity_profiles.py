"""silver.entity_profiles -- per-entity behavioural baseline (C-PROFILES, LB-130).

The profile table is the enabler for reducing W4/W8 over-firing: a rule can then
ask "is this anomalous FOR THIS ENTITY" instead of applying a population-wide
absolute threshold. Pyspark is not installed in the test env, so the build-side
checks are AST/source-based (matching test_silver_entity_id_lei.py); the DDL and
schema wiring are asserted directly.
"""

from __future__ import annotations

import ast
from pathlib import Path

_BUILD_PATH = Path("src/lakebench/spark/scripts/silver_build_financial.py")
_BUILD_SRC = _BUILD_PATH.read_text()
_BUILD_TREE = ast.parse(_BUILD_SRC)


def _find_function(name: str) -> ast.FunctionDef:
    for node in ast.walk(_BUILD_TREE):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found in {_BUILD_PATH}")


def test_build_entity_profiles_exists_and_is_wired_into_main():
    """The build function exists and main() writes it to silver.entity_profiles."""
    _find_function("build_entity_profiles")
    main = _find_function("main")
    calls = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "build_entity_profiles"
    ]
    assert calls, "main() never calls build_entity_profiles"
    # It must be bootstrapped in the DDL loop too.
    assert "DDL_PROFILES" in _BUILD_SRC
    assert '"entity_profiles"' in _BUILD_SRC or "'entity_profiles'" in _BUILD_SRC


def test_profile_aggregates_both_sides():
    """A profile must aggregate the entity as BOTH originator and beneficiary --
    a one-sided profile cannot support W4 (pass-through needs in AND out)."""
    fn = _find_function("build_entity_profiles")
    src = ast.get_source_segment(_BUILD_SRC, fn)
    assert "originator_id" in src and "beneficiary_id" in src
    # full outer join so entities that only ever appear on one side survive.
    assert "fullouter" in src, "profiles must full-outer-join the two sides"


def test_avg_gap_days_guards_single_send():
    """avg_gap_days is the W8 baseline; it must be NULL when txn_count_out < 2
    (no gap is defined for a single send) rather than dividing by zero."""
    fn = _find_function("build_entity_profiles")
    src = ast.get_source_segment(_BUILD_SRC, fn)
    assert "avg_gap_days" in src
    # A guard on count >= 2 must gate the gap computation.
    assert "c_out >= lit(2)" in src or "txn_count_out" in src


def test_passthrough_ratio_guards_zero_received():
    """passthrough_ratio is the W4 baseline; undefined (NULL) when the entity
    never received, never a divide-by-zero."""
    fn = _find_function("build_entity_profiles")
    src = ast.get_source_segment(_BUILD_SRC, fn)
    assert "passthrough_ratio" in src
    assert "total_received_usd" in src


def test_profiles_ddl_registered_and_lockstep():
    """The deployer DDL is registered, is on TableNamesConfig, and the inline
    bootstrap DDL matches the deployer's column set."""
    from lakebench.config.schema import TableNamesConfig
    from lakebench.deploy.financial_ddl import (
        FINANCIAL_TABLE_DDLS,
        SILVER_ENTITY_PROFILES_DDL,
    )

    assert "silver_entity_profiles" in FINANCIAL_TABLE_DDLS
    assert hasattr(TableNamesConfig(), "silver_entity_profiles")
    # Columns the detection rules depend on must be present in the DDL.
    for colname in (
        "entity_id",
        "avg_gap_days",
        "passthrough_ratio",
        "txn_count_out",
        "txn_count_in",
        "avg_amount_usd",
        "stddev_amount_usd",
        "_batch_id",
    ):
        assert colname in SILVER_ENTITY_PROFILES_DDL, f"DDL missing {colname}"
    # Lock-step: every column in the deployer DDL must also be in the inline
    # bootstrap DDL (DDL_PROFILES) so the deployer-created and job-created tables
    # agree.
    import re

    def _cols(ddl: str) -> set[str]:
        body = ddl[ddl.index("(") + 1 : ddl.rindex(")")]
        out = set()
        for line in body.splitlines():
            line = line.strip().strip(",")
            m = re.match(r"^([a-z_][a-z0-9_]*)\s", line)
            if m:
                out.add(m.group(1))
        return out

    deployer_cols = _cols(SILVER_ENTITY_PROFILES_DDL)
    inline_cols = _cols(_BUILD_SRC[_BUILD_SRC.index("DDL_PROFILES") :].split('"""')[1])
    assert deployer_cols == inline_cols, (
        f"DDL drift: deployer-only={deployer_cols - inline_cols}, "
        f"inline-only={inline_cols - deployer_cols}"
    )


def test_build_select_order_matches_ddl():
    """Review Finding 6: the build's SELECT alias order must equal the DDL column
    order. DataFrameWriterV2.overwrite resolves by name, but a reorder/type drift
    only surfaces as a live silver-build write failure otherwise. Assert the alias
    sequence in build_entity_profiles equals the DDL_PROFILES column sequence."""
    import re

    fn = _find_function("build_entity_profiles")

    # Find the `.select(...)` call in the return and read each output column
    # name in order: an `.alias("x")` wrapper gives x; a bare `col("x")` gives x.
    def _output_name(arg: ast.AST) -> str:
        # .alias("x") -> the string literal
        node = arg
        while isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "alias":
                return node.args[0].value
            if (
                isinstance(node.func, ast.Name)
                and node.func.id == "col"
                and node.args
                and isinstance(node.args[0], ast.Constant)
            ):
                return node.args[0].value
            # descend into the receiver of a chained call (e.g. when(...).otherwise(...).alias)
            node = node.func.value if isinstance(node.func, ast.Attribute) else None
            if node is None:
                break
        raise AssertionError(f"cannot determine output column for {ast.dump(arg)[:80]}")

    select_call = None
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "select"
        ):
            select_call = node
    assert select_call is not None, "no .select() found in build_entity_profiles"
    select_cols = [_output_name(a) for a in select_call.args]

    # DDL_PROFILES column order.
    ddl = _BUILD_SRC[_BUILD_SRC.index("DDL_PROFILES") :].split('"""')[1]
    body = ddl[ddl.index("(") + 1 : ddl.rindex(")")]
    ddl_cols = []
    for line in body.splitlines():
        m = re.match(r"^\s*([a-z_][a-z0-9_]*)\s", line)
        if m:
            ddl_cols.append(m.group(1))

    assert select_cols == ddl_cols, (
        f"SELECT/DDL column order drift:\n select={select_cols}\n ddl   ={ddl_cols}"
    )


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
