"""Shared test helpers moved from tests/test_deps_manifest.py (imported by several test files)."""

from __future__ import annotations

import hashlib

from lakebench.deps import manifest as m
from lakebench.deps import request as req


def _h(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def fake_shown(request: req.DepsRequest, **over) -> dict:
    """What ``lb_deps.py show`` prints for a set that serves ``request``."""
    jars = [m.ivy_jar_name(c) for c in request.jar_coordinates]
    jars.append("software.amazon.awssdk_bundle-2.24.6.jar")
    groups: dict[str, list[dict]] = {
        "jars": [{"file": f, "sha256": _h(f), "size": 10} for f in jars]
    }
    if req.GROUP_PY_REFERENCE in request.groups:
        groups["py-reference"] = []
        for pin in request.py_reference:
            n, v = pin.split("==")
            f = f"{n.replace('-', '_')}-{v}-cp310-cp310-manylinux_2_17_x86_64.whl"
            groups["py-reference"].append({"file": f, "sha256": _h(f), "size": 5})
    if req.GROUP_DUCKDB in request.groups:
        v = request.duckdb_version
        w = f"duckdb-{v}-cp311-cp311-manylinux_2_27_x86_64.whl"
        groups["duckdb-wheels"] = [{"file": w, "sha256": _h(w), "size": 7}]
        groups["duckdb-ext"] = [
            {"file": f"v{v}/linux_amd64/{n}.duckdb_extension", "sha256": _h(n), "size": 3}
            for n in sorted(request.duckdb_extensions)
        ]
    order = list(jars)
    shown = {
        "request_sha256": request.request_sha256,
        "tools_sha256": request.tools_sha256,
        "groups": groups,
        "jar_order": order,
        "overlaps": [],
        "repositories": list(request.repositories),
        "python": {"spark": "3.10.12"},
        "resolved_at": "2026-10-01T00:00:00Z",
    }
    shown["pinset_sha256"] = req.pinset_sha256(groups, order)
    shown.update(over)
    return shown
