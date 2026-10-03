"""EVD-3 static test: every function that chooses a strategy, trickle,
executor count, cap or mode is listed in
``requested_effective.KNOWN_SITES`` with where its decision reaches the
record, and the listed carrier is really there. A new choosing site that
nobody listed fails this test."""

from __future__ import annotations

import ast
import re
import shutil
from pathlib import Path

from lakebench.metrics.requested_effective import KNOWN_SITES

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "src" / "lakebench"
SITE = re.compile(
    r"^(select|determine|resolve|choose)_\w*(strategy|trickle|executors?|cap|mode)\w*$"
)

#: For each "record:" carrier, text the site's file must contain: the line
#: that puts the decision in the record.
RECORD_TOKENS: dict[tuple[str, str], tuple[str, ...]] = {
    ("cli/_sustained.py", "resolve_trickle"): ('"trickle": ',),
    ("spark/scripts/gold_finalize.py", "determine_gold_strategy"): (
        "gold_strategy=strategy.value",
        "gold_strategy_source=strategy_source",
    ),
    ("spark/scripts/gold_finalize_delta.py", "determine_gold_strategy"): (
        "gold_strategy=strategy.value",
        "gold_strategy_source=strategy_source",
    ),
}


def choosing_sites(pkg: Path) -> set[tuple[str, str]]:
    """(path under the package, function name) of every choosing site."""
    out = set()
    for path in sorted(pkg.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and SITE.match(node.name):
                out.add((path.relative_to(pkg).as_posix(), node.name))
    return out


def unlisted_sites(pkg: Path) -> list[tuple[str, str]]:
    return sorted(choosing_sites(pkg) - set(KNOWN_SITES))


def _function(path: Path, name: str) -> ast.FunctionDef:
    tree = ast.parse(path.read_text())
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def _calls(fn: ast.FunctionDef) -> set[str]:
    return {
        n.func.id if isinstance(n.func, ast.Name) else getattr(n.func, "attr", "")
        for n in ast.walk(fn)
        if isinstance(n, ast.Call)
    }


def test_every_choosing_site_is_listed() -> None:
    assert unlisted_sites(PKG) == []


def test_every_listed_site_exists() -> None:
    assert sorted(set(KNOWN_SITES) - choosing_sites(PKG)) == []


def test_every_carrier_is_real() -> None:
    for (rel, name), carrier in KNOWN_SITES.items():
        kind, _, what = carrier.partition(": ")
        path = PKG / rel
        if kind == "record":
            tokens = RECORD_TOKENS[(rel, name)]
            src = path.read_text()
            for token in tokens:
                assert token in src, (rel, name, token)
        elif kind == "returns into":
            assert (rel, what) in KNOWN_SITES, (rel, name, what)
            assert name in _calls(_function(path, what)), (rel, name, what)
        elif kind == "exempt":
            assert len(what) > 20, (rel, name)
        else:
            raise AssertionError(f"{rel}:{name}: unknown carrier {carrier!r}")


def test_a_new_unlisted_site_fails(tmp_path: Path) -> None:
    """The named failing case: a stub select_gold_strategy2 in a scratch
    copy of the tree is unlisted."""
    copy = tmp_path / "lakebench"
    shutil.copytree(PKG, copy, ignore=shutil.ignore_patterns("__pycache__"))
    target = copy / "spark" / "scripts" / "gold_finalize.py"
    target.write_text(target.read_text() + "\n\ndef select_gold_strategy2(x):\n    return x\n")
    assert unlisted_sites(copy) == [("spark/scripts/gold_finalize.py", "select_gold_strategy2")]
