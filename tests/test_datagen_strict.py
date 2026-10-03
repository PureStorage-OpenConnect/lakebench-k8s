"""The datagen entrypoint refuses an unknown flag (DAT-3 strict parsing):
a typo or a flag from a newer Lakebench exits 2 instead of being dropped."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def _run(monkeypatch, argv):
    spec = importlib.util.spec_from_file_location(
        "datagen_entrypoint_strict", REPO / "datagen_rs" / "entrypoint.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    seen = {}

    def execvp(prog, cmd):
        seen["cmd"] = cmd
        raise SystemExit(0)

    monkeypatch.setattr(mod.os, "execvp", execvp)
    monkeypatch.setattr(mod, "detect_memory_limit_bytes", lambda: None)
    monkeypatch.delenv("LB_DATAGEN_SEED", raising=False)
    monkeypatch.setattr(sys, "argv", ["entrypoint", *argv])
    try:
        rc = mod.main()
    except SystemExit as e:
        rc = e.code
    return rc, seen.get("cmd")


BASE = ["--schema", "financial", "--bucket", "b", "--seed", "43", "--scale", "1"]


@pytest.mark.parametrize("extra", [["--nope", "1"], ["--sede", "43"], ["stray"]])
def test_entrypoint_unknown_flag_exits_2(monkeypatch, extra):
    rc, cmd = _run(monkeypatch, [*BASE, *extra])
    assert rc == 2 and cmd is None


def test_entrypoint_known_flags_still_run(monkeypatch):
    # --payload-kb stays accepted (a v1.6 template may pass it) and is not
    # forwarded; every flag the current template renders is known.
    rc, cmd = _run(monkeypatch, [*BASE, "--payload-kb", "2", "--workers", "0", "--mode", "all"])
    assert rc == 0 and "--payload-kb" not in cmd
