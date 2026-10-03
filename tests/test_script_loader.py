"""The load_script fixtures give each test its own ``common`` (QA-2).

Two tests with conflicting expectations of ``common`` must pass in both
orders; with the old loader (scripts directory on sys.path, plain import)
the second one sees the first one's change.
"""

from __future__ import annotations

import os
import sys
import textwrap
from pathlib import Path

import pytest

from tests.conftest import ScriptNamespace, shipped_script_modules

pytest_plugins = ["pytester"]

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "src/lakebench/spark/scripts"

_TEST_A = """
def test_a_changes_common(load_script):
    mod, common = load_script("silver_build_financial", extra=("common",))
    common.SOME_CONST = "A"
    assert mod.SilverAbort is common.SilverAbort
"""
_TEST_B = """
def test_b_sees_a_clean_common(load_script):
    mod, common = load_script("bronze_verify_financial", extra=("common",))
    assert not hasattr(common, "SOME_CONST")
"""

# The loader as it was before QA-2: the scripts directory on sys.path and a
# plain import, so every test shares one ``common``.
_OLD_LOADER = f"""
import importlib, sys
import pytest

@pytest.fixture
def load_script():
    if {str(SCRIPTS)!r} not in sys.path:
        sys.path.insert(0, {str(SCRIPTS)!r})

    def load(name, extra=()):
        return (importlib.import_module(name), *map(importlib.import_module, extra))

    return load
"""


def _session(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, conftest: str):
    # Both scripts import pyspark at top level; the unit tier has no pyspark,
    # so the child stubs whatever is missing.
    stub = "import sys\nfrom unittest.mock import MagicMock\n" + "".join(
        f"sys.modules.setdefault({m!r}, MagicMock())\n"
        for m in (
            "pyspark",
            "pyspark.sql",
            "pyspark.sql.functions",
            "pyspark.sql.types",
            "pyspark.sql.window",
        )
    )
    pytester.makeconftest(stub + conftest)
    pytester.makepyfile(
        test_1_a_then_b=_TEST_A + _TEST_B,
        test_2_b_then_a=_TEST_B + _TEST_A,
    )
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(ROOT), str(ROOT / "src")]))
    return pytester.runpytest_subprocess("-p", "no:cacheprovider", "-q")


def test_conflicting_common_expectations_both_orders(pytester, monkeypatch):
    res = _session(pytester, monkeypatch, "from tests.conftest import load_script  # noqa: F401\n")
    res.assert_outcomes(passed=4)


def test_old_loader_leaks_common_between_tests(pytester, monkeypatch):
    """The check above can see the defect: with the old loader, B fails
    after A in the same process."""
    res = _session(pytester, monkeypatch, _OLD_LOADER)
    out = res.stdout.str()
    # A passes; B, run after A in the same process, fails on the leaked
    # attribute, not on some import error.
    assert "FAILED test_1_a_then_b.py::test_b_sees_a_clean_common" in out, out
    assert "FAILED test_1_a_then_b.py::test_a_changes_common" not in out, out
    assert 'assert not hasattr(common, "SOME_CONST")' in out, out


def test_namespace_restores_sys_modules_and_meta_path():
    sentinel = object()
    sys.modules["common"] = sentinel  # type: ignore[assignment]
    meta = list(sys.meta_path)
    try:
        ns = ScriptNamespace("function", "t")
        ns.activate()
        try:
            mod = ns.load("common")
            assert mod is not sentinel and sys.modules["common"] is mod
        finally:
            ns.deactivate()
        assert sys.modules["common"] is sentinel
        assert sys.meta_path == meta
    finally:
        sys.modules.pop("common", None)


def test_function_level_import_inside_a_script_gets_the_private_common():
    """tm_operations.bootstrap_tm_tables imports common inside the function
    (``from common import ensure_column, ...``); that import must resolve to
    the copy the test loaded and patched."""
    from unittest.mock import MagicMock

    class _Reached(Exception):
        pass

    def _raise(*_a, **_k):
        raise _Reached

    ns = ScriptNamespace("function", "t")
    ns.activate()
    try:
        tm, common = ns.load("tm_operations", extra=("common",))
        common.ensure_namespaces_for_ddl = _raise
        with pytest.raises(_Reached):
            tm.bootstrap_tm_tables(MagicMock())
    finally:
        ns.deactivate()
    assert "tm_operations" not in sys.modules


def test_two_namespaces_get_different_commons():
    first = ScriptNamespace("function", "a")
    first.activate()
    try:
        c1 = first.load("common")
    finally:
        first.deactivate()
    second = ScriptNamespace("function", "b")
    second.activate()
    try:
        c2 = second.load("common")
    finally:
        second.deactivate()
    assert c1 is not c2


def test_unknown_name_is_refused():
    ns = ScriptNamespace("function", "t")
    ns.activate()
    try:
        with pytest.raises(ValueError, match="not a Spark script"):
            ns.load("lakebench")
    finally:
        ns.deactivate()


def test_shipped_names_match_the_scripts_configmaps():
    """The loader resolves what the driver pod can import: every .py file the
    scripts ConfigMaps ship flat (scripts_maps.SCRIPT_MAPS), which must be
    every script in spark/scripts, plus the modules the maps ship from
    outside it."""
    from lakebench.modules.pipeline_engines.spark.scripts_maps import SCRIPT_MAPS

    shipped = {
        src.key[: -len(".py")]: ROOT / "src/lakebench" / src.path
        for sources in SCRIPT_MAPS.values()
        for src in sources
        if src.key.endswith(".py")
    }
    in_dir = {p.stem for p in SCRIPTS.glob("*.py")}
    assert in_dir <= set(shipped), sorted(in_dir - set(shipped))
    assert {"reference_score", "fidelity_gate", "datagen_seed"} <= set(shipped)
    names = shipped_script_modules()
    assert names == shipped
    assert all(p.is_file() for p in names.values())


def test_leaked_scripts_fail_the_test_that_leaked_them(pytester, monkeypatch):
    pytester.makeconftest("from tests.conftest import pytest_runtest_teardown  # noqa: F401\n")
    pytester.makepyfile(
        f"""
        import sys

        def test_leaks():
            sys.path.insert(0, {str(SCRIPTS)!r})

        def test_after_is_clean():
            assert {str(SCRIPTS)!r} not in sys.path
        """
    )
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(ROOT), str(ROOT / "src")]))
    res = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-q")
    res.assert_outcomes(passed=2, errors=1)
    assert "Spark script state leaked" in res.stdout.str()


def test_path_leak_inside_a_module_namespace_fails_that_test(pytester, monkeypatch):
    """A module namespace does not hide a sys.path edit: the test that made
    it fails, not the first test of the next module."""
    # The whole tests/conftest.py, so fixture order is the suite's.
    pytester.makeconftest('pytest_plugins = ["tests.conftest"]\n')
    pytester.makepyfile(
        test_a_leaks=f"""
        import sys
        import pytest

        @pytest.fixture(scope="module", autouse=True)
        def _mod(load_script_module):
            return load_script_module("common")

        def test_leaks():
            sys.path.insert(0, {str(SCRIPTS)!r})
        """,
        test_b_innocent="""
        def test_innocent():
            pass
        """,
    )
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(ROOT), str(ROOT / "src")]))
    res = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-q", "-rE")
    res.assert_outcomes(passed=2, errors=1)
    assert "ERROR test_a_leaks.py::test_leaks" in res.stdout.str()


def test_leak_restored_by_monkeypatch_undo_fails_that_test(pytester, monkeypatch):
    """monkeypatch undoes its changes after the test's other fixtures; a
    script module it puts back into sys.modules is blamed on the test that
    set it, not on the next test."""
    # The whole tests/conftest.py: its autouse _journal_in_tmp requests
    # monkeypatch before any test fixture, as in the suite.
    pytester.makeconftest('pytest_plugins = ["tests.conftest"]\n')
    pytester.makepyfile(
        test_a_leaks="""
        import sys

        def test_leaks(load_script, monkeypatch):
            common = load_script("common")
            name = "common"
            monkeypatch.delitem(sys.modules, name)
            sys.modules[name] = common
        """,
        test_b_innocent="""
        import sys

        def test_innocent():
            assert "common" not in sys.modules
        """,
    )
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(ROOT), str(ROOT / "src")]))
    res = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-q", "-rE")
    res.assert_outcomes(passed=2, errors=1)
    assert "ERROR test_a_leaks.py::test_leaks" in res.stdout.str()


def test_function_loader_refused_inside_a_module_namespace(pytester, monkeypatch):
    pytester.makeconftest(
        "from tests.conftest import load_script, load_script_module  # noqa: F401\n"
    )
    pytester.makepyfile(
        textwrap.dedent(
            """
            import pytest

            @pytest.fixture(scope="module", autouse=True)
            def _mod(load_script_module):
                return load_script_module("common")

            def test_mixed(load_script):
                pass
            """
        )
    )
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(ROOT), str(ROOT / "src")]))
    res = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-q")
    res.assert_outcomes(errors=1)
    assert "load_script and load_script_module in one test module" in res.stdout.str()
