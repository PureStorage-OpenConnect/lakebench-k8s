"""Every tracked docs/*.md file must be non-empty and under 1 MB.

A scripted edit once replaced an empty match in docs/deployment.md, growing
it from 214 lines to 17 MB and deleting its content; no other test noticed.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAX_BYTES = 1_000_000


def _tracked_docs() -> list[Path]:
    try:
        out = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files", "docs/*.md", "docs/**/*.md"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return sorted(ROOT.glob("docs/**/*.md"))
    return sorted({ROOT / line for line in out.splitlines() if line})


DOCS = _tracked_docs()


@pytest.mark.skipif(not DOCS, reason="docs not present in this checkout")
def test_docs_found():
    assert len(DOCS) > 10


@pytest.mark.parametrize("path", DOCS, ids=lambda p: str(p.relative_to(ROOT)))
def test_doc_not_empty_or_oversized(path: Path):
    if not path.exists():
        pytest.skip("deleted in the working tree")
    size = path.stat().st_size
    assert size > 0, f"{path.relative_to(ROOT)} is empty"
    assert size < MAX_BYTES, f"{path.relative_to(ROOT)} is {size:,} bytes (limit {MAX_BYTES:,})"
