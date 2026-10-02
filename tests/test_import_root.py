"""pytest tests the checkout it runs in.

Several worktrees of this repository can share one Python whose editable
install points at a single checkout. Without ``pythonpath = ["src"]`` in
``pyproject.toml``, a bare ``pytest`` in any other worktree imports that
checkout's lakebench and silently tests the wrong code. In CI the editable
install is this tree, so the test only fails in a second worktree; check it
there when the option changes.
"""

from __future__ import annotations

from pathlib import Path

import lakebench

ROOT = Path(__file__).resolve().parents[1]


def test_lakebench_imported_from_this_tree():
    imported = Path(lakebench.__file__).resolve()
    src = (ROOT / "src").resolve()
    assert imported.is_relative_to(src), (
        f"lakebench imported from {imported}, not from this checkout's {src}; "
        'pytest needs pythonpath = ["src"] in pyproject.toml'
    )
