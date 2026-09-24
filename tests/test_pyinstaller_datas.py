"""Every data file the package ships must be in the PyInstaller spec, or the
binary silently loses it (W5/W6/W7 had no reference JSON in the binary build).

The list is derived from the tracked tree rather than written out, so a new
data directory fails this test until the spec picks it up."""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _spec_datas() -> list[tuple[str, str]]:
    """(src, dest) string pairs from the literal list in Analysis(datas=...).

    Parsed with ast, so a commented-out tuple or one elsewhere in the spec
    does not count."""
    tree = ast.parse((ROOT / "lakebench.spec").read_text())
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "Analysis"
        ):
            continue
        for kw in node.keywords:
            if kw.arg != "datas":
                continue
            value = kw.value
            # datas=[...] + pydantic_datas + ...: the literal list is the
            # leftmost operand.
            while isinstance(value, ast.BinOp):
                value = value.left
            assert isinstance(value, ast.List), "datas= does not start with a literal list"
            pairs = []
            for elt in value.elts:
                assert isinstance(elt, ast.Tuple) and len(elt.elts) == 2, ast.dump(elt)
                src, dest = (ast.literal_eval(e) for e in elt.elts)
                pairs.append((src, dest))
            return pairs
    raise AssertionError("no Analysis(datas=...) in lakebench.spec")


def _package_data_files() -> list[str]:
    try:
        out = subprocess.run(
            ["git", "ls-files", "src/lakebench"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        out = [str(p.relative_to(ROOT)) for p in (ROOT / "src" / "lakebench").rglob("*")]
    return [
        f
        for f in out
        if (ROOT / f).is_file() and not f.endswith((".py", ".pyc")) and "__pycache__" not in f
    ]


def test_spec_entries_exist_and_map_to_package_paths():
    datas = _spec_datas()
    assert datas, "no datas entries parsed from lakebench.spec"
    for src, dest in datas:
        assert (ROOT / src).exists(), src
        # A file entry lands in its parent directory; a directory maps 1:1.
        expected = src.removeprefix("src/")
        if (ROOT / src).is_file():
            expected = str(Path(expected).parent)
        assert dest == expected, f"{src} -> {dest}, expected {expected}"


def test_every_package_data_file_is_shipped():
    roots = [src for src, _ in _spec_datas()]
    missing = [
        f for f in _package_data_files() if not any(f == r or f.startswith(r + "/") for r in roots)
    ]
    assert not missing, f"not in lakebench.spec datas: {missing}"


def test_source_shipped_python_is_in_spec():
    # reference_score.py is read as text into the driver ConfigMap, so it
    # must exist as a file in the binary, not only as bytecode.
    shipped = [s for s, _ in _spec_datas()]
    assert "src/lakebench/aml/reference_score.py" in shipped
    assert "src/lakebench/aml/fidelity_gate.py" in shipped
