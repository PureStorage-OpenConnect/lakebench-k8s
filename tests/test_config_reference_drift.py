"""docs/configuration.md's field reference and removed-keys table are
generated from the schema by scripts/gen_config_reference.py (CC-18, C33).
A hand edit, or a schema change without a regenerate, fails here."""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest
from pydantic import BaseModel, Field

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def gen():
    spec = importlib.util.spec_from_file_location(
        "gen_config_reference", REPO / "scripts" / "gen_config_reference.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_config_reference_drift(gen):
    assert gen.drift() == [], "run: python3.11 scripts/gen_config_reference.py"


def test_check_flags_a_hand_edit(gen, tmp_path):
    (tmp_path / "docs").mkdir()
    shutil.copy(REPO / gen.DOC, tmp_path / gen.DOC)
    doc = tmp_path / gen.DOC
    doc.write_text(doc.read_text().replace("| first day |", "| advanced |", 1))
    assert any("config-reference" in p for p in gen.drift(tmp_path))
    assert gen.regenerate(tmp_path)
    assert gen.drift(tmp_path) == []


def test_every_key_is_listed_once(gen):
    from lakebench.config.schema import LakebenchConfig

    paths = [p for p, _ in gen.leaves()]
    assert len(paths) == len(set(paths))
    block = gen.block_in((REPO / gen.DOC).read_text(), "config-reference")
    for p in paths:
        assert block.count(f"| `{p}` |") == 1, p
    assert "name" in paths and "workload.datagen.scale" in paths
    assert LakebenchConfig.model_fields  # the walk starts at the root model


def test_every_removed_key_is_listed(gen):
    block = gen.block_in((REPO / gen.DOC).read_text(), "config-removed")
    removed = gen.removed_keys()
    assert removed
    for path, _ in removed:
        assert f"| `{path}` |" in block, path
    assert "workload.customer360.date_range_days" in dict(removed)


def test_first_day_tier_is_what_init_writes(gen):
    first = gen.first_day_keys()
    assert {"name", "recipe", "workload.datagen.scale", "platform.storage.s3.endpoint"} <= first
    tiers = {}
    block = gen.block_in((REPO / gen.DOC).read_text(), "config-reference")
    for line in block.splitlines():
        if line.startswith("| `"):
            cells = [c.strip() for c in line.strip("|").split(" | ")]
            tiers[cells[0].strip("`")] = cells[3]
    assert {k for k, v in tiers.items() if v == "first day"} == first & set(tiers)


def test_attribute_docstring_is_the_description():
    """The descriptions are the string literals after the fields
    (use_attribute_docstrings on ConfigModel); an explicit Field description wins."""
    from lakebench.config.schema import ConfigModel

    class Probe(ConfigModel):
        a: int = 1
        """From the docstring."""
        b: int = Field(default=2, description="From Field.")
        """Ignored."""

    assert Probe.model_fields["a"].description == "From the docstring."
    assert Probe.model_fields["b"].description == "From Field."
    assert issubclass(Probe, BaseModel)


def test_a_key_with_no_section_fails(gen, monkeypatch):
    monkeypatch.setattr(gen, "SECTIONS", gen.SECTIONS[:1])
    with pytest.raises(SystemExit, match="no section"):
        gen.render_reference()


def test_types_and_defaults_render(gen):
    from lakebench.config.schema import LakebenchConfig

    spark = LakebenchConfig.model_fields["spark"].annotation
    conf = spark.model_fields["conf"]
    assert gen.type_text(conf.annotation) == "mapping"
    assert gen.default_text(conf) == "`{}`"
    assert gen.type_text(int | None) == "integer or null"


def test_default_overrides_name_real_keys(gen):
    paths = {p for p, _ in gen.leaves()}
    assert set(gen.DEFAULT_OVERRIDES) <= paths
    block = gen.block_in((REPO / gen.DOC).read_text(), "config-reference")
    assert "| `name` | string | **(required)** |" in block
    assert "`<name>-bronze`" in block and "lakebench-bronze" not in block


def test_every_key_has_a_description(gen):
    missing = [p for p, f in gen.leaves() if not (f.description or "").strip()]
    assert missing == []
