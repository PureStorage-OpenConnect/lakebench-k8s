"""Tests for Spark scripts validation (P6).

Validates that PySpark scripts under spark/scripts/ are syntactically
valid Python. These scripts run inside Spark pods so they can't be
tested with real PySpark, but we can verify they parse correctly.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "src" / "lakebench" / "spark" / "scripts"


def _get_script_files():
    """Return all .py files under the scripts directory."""
    return sorted(SCRIPTS_DIR.glob("*.py"))


class TestSparkScriptsSyntax:
    """Validate that all Spark scripts are syntactically valid Python."""

    @pytest.fixture(params=_get_script_files(), ids=lambda p: p.stem)
    def script_path(self, request):
        return request.param

    def test_script_parses(self, script_path):
        """Each script should parse without SyntaxError."""
        source = script_path.read_text()
        try:
            ast.parse(source, filename=str(script_path))
        except SyntaxError as e:
            pytest.fail(f"{script_path.name} has syntax error: {e}")

    def test_script_not_empty(self, script_path):
        """Each script should have meaningful content."""
        source = script_path.read_text().strip()
        assert len(source) > 50, f"{script_path.name} is suspiciously short"


class TestCommonModule:
    """Tests for the common.py module used by all scripts."""

    def test_common_has_log(self):
        source = (SCRIPTS_DIR / "common.py").read_text()
        tree = ast.parse(source)
        func_names = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
        assert "log" in func_names

    def test_common_has_env(self):
        source = (SCRIPTS_DIR / "common.py").read_text()
        tree = ast.parse(source)
        func_names = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
        assert "env" in func_names

    def test_common_has_parse_size_gb(self):
        source = (SCRIPTS_DIR / "common.py").read_text()
        tree = ast.parse(source)
        func_names = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
        assert "parse_size_gb" in func_names


class TestScriptImports:
    """Verify each script imports from common module."""

    @pytest.fixture(
        params=[p for p in _get_script_files() if p.name != "common.py"],
        ids=lambda p: p.stem,
    )
    def non_common_script(self, request):
        return request.param

    def test_imports_common(self, non_common_script):
        """Non-common scripts should import from common."""
        source = non_common_script.read_text()
        assert "from common import" in source or "import common" in source, (
            f"{non_common_script.name} should import from common module"
        )


class TestScriptContents:
    """Validate specific script contents."""

    def test_bronze_ingest_uses_streaming(self):
        source = (SCRIPTS_DIR / "bronze_ingest.py").read_text()
        assert "readStream" in source or "writeStream" in source or "streaming" in source.lower()

    def test_bronze_verify_batch(self):
        source = (SCRIPTS_DIR / "bronze_verify.py").read_text()
        assert "SparkSession" in source

    def test_silver_build_exists(self):
        assert (SCRIPTS_DIR / "silver_build.py").exists()

    def test_gold_finalize_exists(self):
        assert (SCRIPTS_DIR / "gold_finalize.py").exists()

    def test_gold_refresh_exists(self):
        assert (SCRIPTS_DIR / "gold_refresh.py").exists()

    def test_silver_stream_exists(self):
        assert (SCRIPTS_DIR / "silver_stream.py").exists()
