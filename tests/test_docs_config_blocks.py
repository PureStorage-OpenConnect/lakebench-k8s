"""Every YAML block in the user docs that looks like a lakebench config must
load under the strict schema, without deprecation warnings. Before this, the
docs taught keys the schema silently ignored (images.pull_secrets, a
top-level datagen block, platform.compute.spark.pull_policy)."""

from __future__ import annotations

import re
import warnings
from pathlib import Path

import pytest
import yaml

from lakebench.config import load_config

ROOT = Path(__file__).resolve().parents[1]

# Design specs describe proposed config, not current config.
EXEMPT_PREFIXES = ("docs/internal/",)

# Known doc defects owned elsewhere. strict=True: once the doc is fixed the
# xfail fails, so the entry gets removed.
# Doc blocks known not to load, with the reason; each entry is a strict
# xfail so the fix of the doc forces its removal here.
KNOWN_BAD: dict[str, str] = {}

_TOP = {
    "name", "recipe", "platform", "architecture", "images", "observability", "spark",
    "endpoint", "access_key", "secret_key", "scale", "namespace", "mode", "cycles",
    "spark_image", "secret_ref", "description", "version",
}  # fmt: skip
_ANCHORS = {"platform", "architecture", "recipe", "name"}


def _blocks():
    files = sorted(ROOT.glob("docs/**/*.md")) + [ROOT / "README.md", ROOT / "CONTRIBUTING.md"]
    for f in files:
        rel = str(f.relative_to(ROOT))
        if rel.startswith(EXEMPT_PREFIXES) or not f.exists():
            continue
        text = f.read_text()
        for m in re.finditer(r"```ya?ml\n(.*?)```", text, re.S):
            try:
                data = yaml.safe_load(m.group(1))
            except yaml.YAMLError:
                continue
            if not isinstance(data, dict) or "apiVersion" in data or "kind" in data:
                continue
            keys = set(data)
            if not keys & _TOP:
                continue
            if keys - _TOP and not keys & _ANCHORS:
                continue  # some other tool's YAML that shares a key name
            line = text[: m.start()].count("\n") + 1
            env = sorted(set(re.findall(r"\$\{([A-Z_][A-Z0-9_]*)\}", m.group(1))))
            block_id = f"{rel}:{line}"
            marks = []
            if block_id in KNOWN_BAD:
                marks = [pytest.mark.xfail(reason=KNOWN_BAD[block_id], strict=True)]
            yield pytest.param(data, env, id=block_id, marks=marks)


BLOCKS = list(_blocks())


def test_blocks_found():
    assert len(BLOCKS) >= 5


@pytest.mark.parametrize(("data", "env"), BLOCKS)
def test_docs_config_block_loads(data, env, tmp_path, monkeypatch):
    # ${VAR} references without a default must resolve at load time.
    for var in env:
        monkeypatch.setenv(var, "placeholder")
    data = dict(data)
    data.setdefault("name", "docs-test")
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        load_config(path)
