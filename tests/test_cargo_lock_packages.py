"""The datagen image's locked crate set is frozen with the generator: a
change to it (a new crate, another version, another source) changes the
look image and needs a Rebuild decision. The pin below is the package set
of datagen_rs/Cargo.lock when the held-out hash reader added serde_json and
ring as direct dependencies, which were already locked through object_store
(only the root package's dependency list changed)."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

LOCK = Path(__file__).resolve().parents[1] / "datagen_rs" / "Cargo.lock"
TOML = LOCK.with_name("Cargo.toml")
PACKAGES = 275
PACKAGE_SET_SHA256 = "345db4d8de0f4c21887f520a857370126cf59946b116114f0a1e1c21a5cd085d"
#: The [dependencies] table of datagen_rs/Cargo.toml (requirements and
#: features, comments and order ignored): a feature switched on changes the
#: image without changing the locked package set.
DEPENDENCIES_SHA256 = "71232c046aaa036fa7e94de54490d44ed9114b97aeb180caade5a25911ba73c6"


def _packages(text: str) -> list[str]:
    out = []
    for block in text.split("[[package]]")[1:]:
        name = re.search(r'^name = "(.*)"$', block, re.M)
        ver = re.search(r'^version = "(.*)"$', block, re.M)
        src = re.search(r'^source = "(.*)"$', block, re.M)
        assert name and ver, block[:80]
        out.append(f"{name.group(1)} {ver.group(1)} {src.group(1) if src else '-'}")
    return sorted(out)


def test_locked_crate_set_is_pinned():
    pk = _packages(LOCK.read_text())
    digest = hashlib.sha256("\n".join(pk).encode()).hexdigest()
    assert (len(pk), digest) == (PACKAGES, PACKAGE_SET_SHA256), (
        "datagen_rs/Cargo.lock's crate set changed: that changes the look image "
        "(Freeze-cost: Rebuild); update this pin only with that decision"
    )


def test_hash_reader_crates_are_direct_dependencies():
    root = LOCK.read_text().split('name = "datagen_rs"')[1].split("[[package]]")[0]
    for dep in ("ring", "serde_json"):
        assert f'"{dep}"' in root, dep


def test_direct_dependencies_and_features_are_pinned():
    table = TOML.read_text().split("[dependencies]")[1].split("\n[")[0]
    lines = sorted(
        ln.strip() for ln in table.splitlines() if ln.strip() and not ln.strip().startswith("#")
    )
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    assert digest == DEPENDENCIES_SHA256, (
        "datagen_rs/Cargo.toml's [dependencies] changed (a requirement or a feature): that "
        "changes the look image (Freeze-cost: Rebuild); update this pin only with that decision"
    )
