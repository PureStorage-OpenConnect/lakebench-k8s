"""Resource path resolution for lakebench.

Handles correct path resolution whether running from:
- Source tree (development)
- pip install (site-packages)
- PyInstaller bundle (frozen binary)
"""

from __future__ import annotations

import sys
from pathlib import Path


def _package_dir() -> Path:
    """Return the lakebench package directory.

    Works in all execution contexts:
    - Development: src/lakebench/
    - Installed: site-packages/lakebench/
    - PyInstaller: sys._MEIPASS/lakebench/
    """
    if getattr(sys, "frozen", False):
        # PyInstaller bundle -- data files extracted under _MEIPASS
        return Path(sys._MEIPASS) / "lakebench"  # type: ignore[attr-defined]
    return Path(__file__).parent


def get_templates_dir() -> Path:
    """Return path to Jinja2 templates directory."""
    return _package_dir() / "templates"


def get_scripts_dir() -> Path:
    """Return path to Spark pipeline scripts directory."""
    return _package_dir() / "spark" / "scripts"
