"""DEP-4: docs/prerequisites.md is generated from the prerequisite registry.

A hand edit of the page, or a registry change without
``python scripts/gen_prereq_docs.py``, fails here.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from lakebench.deploy.prereqs import PREREQS, render_markdown

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "docs" / "prerequisites.md"


def test_prereq_docs_drift():
    assert PAGE.read_text(encoding="utf-8") == render_markdown(), (
        "docs/prerequisites.md is stale or hand-edited; run python scripts/gen_prereq_docs.py"
    )


def test_page_names_every_check_and_its_fix():
    text = PAGE.read_text(encoding="utf-8")
    for p in PREREQS:
        assert f'<a id="{p.id}"></a>' in text
        assert p.fix in text


def test_generator_check_mode(tmp_path):
    script = ROOT / "scripts" / "gen_prereq_docs.py"
    ok = subprocess.run([sys.executable, str(script), "--check"], capture_output=True, text=True)
    assert ok.returncode == 0, ok.stdout + ok.stderr
