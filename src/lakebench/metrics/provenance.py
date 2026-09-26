"""Code provenance stamped into every run's metrics.json (GOALS P9.1).

Records which lakebench produced the numbers: the package version and, when
lakebench runs from a git checkout (an editable install), the commit and
whether the working tree had uncommitted changes to tracked files. Image
versions are already in ``config_snapshot["images"]``.
"""

from __future__ import annotations

import os
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Any

# Inherited from a git hook or `rebase --exec`, these would point git at
# another repository whatever the working directory.
_GIT_ENV_OVERRIDES = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR")


def _git(args: list[str], cwd: Path) -> str | None:
    env = {k: v for k, v in os.environ.items() if k not in _GIT_ENV_OVERRIDES}
    try:
        r = subprocess.run(
            # --no-optional-locks: never take index.lock, so a commit in the
            # same checkout at the moment a run starts cannot fail on it.
            ["git", "--no-optional-locks", *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.SubprocessError, OSError):
        return None
    return r.stdout.strip() if r.returncode == 0 else None


@lru_cache(maxsize=1)
def run_provenance() -> dict[str, Any]:
    """{lakebench_version, git_sha, git_dirty} for the running lakebench.

    git_sha and git_dirty are None when the package is not in a git checkout
    (an installed wheel), the checkout does not track the package, or git is
    unavailable. The checkout is found from the package's own location, not
    the working directory.
    """
    from lakebench import __version__

    pkg_dir = Path(__file__).resolve().parent.parent
    sha: str | None = None
    dirty: bool | None = None
    # The package must be tracked by the repository git finds: a wheel in a
    # virtualenv inside some other checkout would otherwise record that
    # checkout's commit.
    if _git(["ls-files", "--error-unmatch", str(pkg_dir / "__init__.py")], pkg_dir) is not None:
        sha = _git(["rev-parse", "HEAD"], pkg_dir)
    if sha:
        # Dirty means any change under the package, untracked files included
        # (an untracked module the run imports is at no commit).
        status = _git(["status", "--porcelain", "--", str(pkg_dir)], pkg_dir)
        dirty = None if status is None else bool(status)
    return {"lakebench_version": __version__, "git_sha": sha, "git_dirty": dirty}
