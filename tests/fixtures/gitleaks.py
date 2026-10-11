"""Shared helpers for the gitleaks scan tests."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest


def _gitleaks() -> str:
    """The gitleaks binary; skip without it unless LB_REQUIRE_GITLEAKS=1, then fail."""
    exe = shutil.which("gitleaks")
    if exe is None:
        if os.environ.get("LB_REQUIRE_GITLEAKS") == "1":
            pytest.fail("gitleaks is not on PATH and LB_REQUIRE_GITLEAKS=1")
        pytest.skip("requires gitleaks on PATH")
    return exe


def _env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        check=True,
        capture_output=True,
        text=True,
        env=_env(),
    ).stdout.strip()


def _key(fill: str) -> str:
    # Built at run time so the test files never match the FlashBlade rule themselves.
    return "PSFB" + fill * 38
