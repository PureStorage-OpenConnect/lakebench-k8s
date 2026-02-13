#!/usr/bin/env python3
"""Lakebench CLI entrypoint -- run without pip install.

Usage:
    python lbrun.py deploy
    python lbrun.py --help

Works on Linux, macOS, and Windows without requiring pip install.
"""

import sys
from pathlib import Path

# Add src/ to import path so the lakebench package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from lakebench.cli import app

if __name__ == "__main__":
    app()
