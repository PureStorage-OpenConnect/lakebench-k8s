"""Every data directory the code resolves via _package_dir() must be in the
PyInstaller spec, or the binary silently loses it (W5/W6/W7 had no reference
JSON in the binary build)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_spec_ships_runtime_data():
    spec = (ROOT / "lakebench.spec").read_text()
    for src in (
        "src/lakebench/templates",
        "src/lakebench/spark/scripts",
        "src/lakebench/spark/data/aml",
        "src/lakebench/benchmark/queries",
        "src/lakebench/aml/reference_score.py",
    ):
        assert f'"{src}"' in spec, src
        assert (ROOT / src).exists(), src
