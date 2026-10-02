"""Hatch build hook: write the build's commit into the package.

A wheel has no git checkout around it, so ``metrics/provenance.py`` reads
the commit from ``lakebench/_build_info.py``, which this hook generates:

- Built from a git checkout that tracks the package: ``GIT_SHA`` is
  ``git rev-parse HEAD`` and ``GIT_DIRTY`` is whether ``git status
  --porcelain -- src/lakebench`` lists anything (the same test the runtime
  applies to a checkout).
- Built from an unpacked sdist (``python -m build`` builds the wheel this
  way; the sdist has ``PKG-INFO`` at its root): the sdist already carries
  the file this hook wrote into it, and it is kept as it is.
- Neither: ``GIT_SHA = None`` and ``GIT_DIRTY = None``; the run record then
  says the commit is unknown.

The file is never written into the source tree: it is generated in a
temporary directory and added to the archive with ``force_include``.
Editable installs are skipped (a checkout's provenance comes from git).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

PACKAGE = "src/lakebench"
BUILD_INFO = "_build_info.py"

_GIT_ENV_OVERRIDES = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR")


def _git(args: list[str], cwd: str) -> str | None:
    env = {k: v for k, v in os.environ.items() if k not in _GIT_ENV_OVERRIDES}
    try:
        r = subprocess.run(
            ["git", "--no-optional-locks", *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def build_info_source(root: str) -> str | None:
    """The ``_build_info.py`` text for a build of *root*, or None when *root*
    is not a git checkout that tracks the package."""
    if _git(["ls-files", "--error-unmatch", f"{PACKAGE}/__init__.py"], root) is None:
        return None
    sha = _git(["rev-parse", "HEAD"], root)
    if not sha:
        return None
    status = _git(["status", "--porcelain", "--", PACKAGE], root)
    dirty = None if status is None else bool(status)
    return render(sha, dirty)


def render(sha: str | None, dirty: bool | None) -> str:
    return (
        '"""Written by hatch_build.py when this package was built; not tracked."""\n'
        "\n"
        f"GIT_SHA = {sha!r}\n"
        f"GIT_DIRTY = {dirty!r}\n"
    )


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if version == "editable":
            return
        root = self.root
        existing = Path(root, PACKAGE, BUILD_INFO)
        if existing.is_file() and Path(root, "PKG-INFO").is_file():
            # An unpacked sdist carries the file this hook wrote into it. It
            # is included explicitly: the sdist's .gitignore lists it, and
            # hatch applies that to the wheel.
            if self.target_name == "wheel":
                build_data.setdefault("force_include", {})[str(existing)] = (
                    f"lakebench/{BUILD_INFO}"
                )
            return
        if existing.is_file():
            raise RuntimeError(
                f"{existing} is in the source tree; it is generated at build time and must "
                "not be there (delete it and build again)"
            )
        text = build_info_source(root) or render(None, None)
        self._tmp = tempfile.mkdtemp(prefix="lakebench-build-info-")
        out = Path(self._tmp) / BUILD_INFO
        out.write_text(text, encoding="utf-8")
        target = (
            f"{PACKAGE}/{BUILD_INFO}" if self.target_name == "sdist" else f"lakebench/{BUILD_INFO}"
        )
        build_data.setdefault("force_include", {})[str(out)] = target

    def finalize(self, version: str, build_data: dict[str, Any], artifact_path: str) -> None:
        tmp = getattr(self, "_tmp", None)
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)
