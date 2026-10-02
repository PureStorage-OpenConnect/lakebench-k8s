"""The root ``lakebench.yaml`` at the repo root is the CLI's default config
(picked up when no ``--config`` is passed). It has drifted from the schema in
the past: an older revision pinned ``docker.io/sillidata/lb-datagen:latest``
and ``apache/polaris:1.3.0-incubating``, both stale, and hard-coded a literal
placeholder for the Polaris client secret.

These tests pin the root file to the schema's defaults so a future schema
update either updates the file or breaks the test.
"""

from __future__ import annotations

import re
import warnings
from pathlib import Path

import yaml

from lakebench.config import load_config
from lakebench.config.schema import ImagesConfig

ROOT = Path(__file__).resolve().parents[1]
ROOT_YAML = ROOT / "lakebench.yaml"


def test_root_yaml_exists() -> None:
    """tests/test_cli.py's DEFAULT_CONFIG assertion assumes the filename."""
    assert ROOT_YAML.is_file(), (
        f"{ROOT_YAML} is missing; DEFAULT_CONFIG in cli/_helpers.py points at it"
    )


def _yaml_text() -> str:
    return ROOT_YAML.read_text()


def test_datagen_image_matches_schema_default() -> None:
    """The commented datagen image must match ImagesConfig.datagen exactly.

    The immutable tag encodes which datagen_rs commit built the image; a
    stale example teaches users to run against a different generator than
    the schema wires up.
    """
    default = ImagesConfig().datagen
    text = _yaml_text()
    match = re.search(r"^\s*#?\s*datagen:\s*(\S+)", text, re.MULTILINE)
    assert match is not None, "no `datagen:` line found in root lakebench.yaml"
    assert match.group(1) == default, (
        f"root lakebench.yaml pins datagen={match.group(1)!r}, schema default is {default!r}"
    )
    assert ":latest" not in default, (
        "ImagesConfig.datagen must not be :latest; if the schema changes to a "
        "floating tag, delete this assertion after the reviewer signs off"
    )


def test_polaris_image_matches_schema_default() -> None:
    """The commented images.polaris tag must match ImagesConfig.polaris.

    The Polaris that runs is that tag; v1.7 removed the unread
    catalog.polaris.version, so the file must not carry it either.
    """
    default = ImagesConfig().polaris
    text = _yaml_text()
    found = False
    for line in text.splitlines():
        stripped = line.lstrip("# ").strip()
        if stripped.startswith("polaris: apache/polaris:"):
            image = stripped.split(":", 1)[1].split("#", 1)[0].strip()
            assert image == default, f"images.polaris {image!r}, schema default {default!r}"
            found = True
    assert found, "no images.polaris line in root lakebench.yaml"
    in_polaris = False
    for line in text.splitlines():
        stripped = line.lstrip("# ").rstrip()
        if stripped.strip() == "polaris:":
            in_polaris = True
        elif in_polaris and stripped.strip().startswith("version:"):
            raise AssertionError("root lakebench.yaml still sets catalog.polaris.version")
        elif in_polaris and stripped and not stripped.startswith(" "):
            in_polaris = False
    assert "1.3.0-incubating" not in text, (
        "root lakebench.yaml still references the deprecated 1.3.0-incubating Polaris tag"
    )


def test_polaris_client_secret_uses_env_var() -> None:
    """The Polaris client_secret must use ${...} env-var substitution.

    A literal placeholder (``client_secret: your-secret``) trains users to
    commit real secrets into the file.
    """
    text = _yaml_text()
    match = re.search(r"^\s*client_secret:\s*(\S.*)$", text, re.MULTILINE)
    assert match is not None, "no `client_secret:` line found in root lakebench.yaml"
    value = match.group(1).strip()
    assert value.startswith("${") and value.endswith("}"), (
        f"client_secret is {value!r}; must use ${{VAR}} env-var substitution"
    )
    assert "LAKEBENCH_POLARIS_CLIENT_SECRET" in value, (
        f"client_secret {value!r} should reference LAKEBENCH_POLARIS_CLIENT_SECRET "
        "to match the other polaris-*.yaml examples"
    )


def test_no_dead_recipe_names() -> None:
    """Stale recipe or mode names must not appear in the root yaml."""
    text = _yaml_text()
    # 'sustained' at word boundary (comments referencing pipeline.sustained
    # count too; the canonical key is pipeline.continuous now).
    assert not re.search(r"\bsustained\b", text, re.IGNORECASE), (
        "'sustained' is deprecated; the canonical key is 'continuous'"
    )
    assert "iot" not in text.lower(), "no IoT workload exists"


def test_workload_at_top_level() -> None:
    """Workload block must live at the top level (D12), not under architecture."""
    data = yaml.safe_load(_yaml_text())
    assert "workload" in data, (
        "workload block must be at the top level; nesting under architecture is deprecated"
    )
    arch = data.get("architecture") or {}
    assert "workload" not in arch, (
        "workload is nested under architecture (deprecated); move it to the top level"
    )


def test_root_yaml_loads_cleanly(monkeypatch, tmp_path) -> None:
    """load_config() on the root file must succeed and use the polaris recipe."""
    monkeypatch.setenv("LAKEBENCH_POLARIS_CLIENT_SECRET", "placeholder-secret")
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        cfg = load_config(ROOT_YAML)
    assert cfg.name
    assert cfg.architecture.catalog.type.value == "polaris"
    assert cfg.architecture.catalog.polaris.client_secret == "placeholder-secret"
    assert cfg.architecture.table_format.type.value == "iceberg"
    assert cfg.architecture.query_engine.type.value == "trino"
    assert cfg.architecture.pipeline_engine.value == "spark"
