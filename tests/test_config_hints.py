"""The did-you-mean helper's own invariants (CFG-4)."""

from __future__ import annotations

from lakebench.config._hints import FLAT_KEYS, all_paths
from lakebench.config.loader import _FLAT_FIELD_MAP


def test_flat_keys_are_known_top_level_keys():
    assert set(FLAT_KEYS) == set(_FLAT_FIELD_MAP)


def test_paths_use_the_spellings_users_write():
    paths = set(all_paths())
    assert "workload.datagen.scale" in paths
    assert "workload.schema" in paths
    assert "architecture.pipeline.continuous.run_duration" in paths
    assert not any(p.startswith("architecture.workload") for p in paths)
    assert not any(".sustained" in p for p in paths)
