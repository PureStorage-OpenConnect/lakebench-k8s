"""Shared test helpers moved from tests/test_deps_request.py (imported by several test files)."""

from __future__ import annotations

from tests.conftest import make_config

SPARK40 = "apache/spark:4.0.2-python3"


def _cfg(image=SPARK40, fmt="iceberg", version=None, **over):
    tf: dict = {"type": fmt}
    if version:
        tf[fmt] = {"version": version}
    arch = over.pop("architecture", {})
    arch.setdefault("table_format", tf)
    return make_config(images={"spark": image}, architecture=arch, **over)
