"""trino.worker.spill_max_per_node must fit trino.worker.storage and be in Gi."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lakebench.config.schema import TrinoWorkerConfig


def test_defaults_valid():
    TrinoWorkerConfig()


def test_spill_above_storage_rejected():
    with pytest.raises(ValidationError, match="10%"):
        TrinoWorkerConfig(spill_max_per_node="60Gi", storage="50Gi")


def test_non_gi_spill_rejected():
    with pytest.raises(ValidationError, match="Gi"):
        TrinoWorkerConfig(spill_max_per_node="40000Mi")


def test_other_storage_units_accepted():
    TrinoWorkerConfig(storage="1Ti")
    TrinoWorkerConfig(storage="500G")


def test_spill_equal_to_storage_rejected():
    with pytest.raises(ValidationError, match="10%"):
        TrinoWorkerConfig(spill_max_per_node="50Gi", storage="50Gi")


def test_spill_disabled_skips_check():
    TrinoWorkerConfig(spill_enabled=False, spill_max_per_node="60Gi", storage="50Gi")
