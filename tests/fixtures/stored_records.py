"""The stored-record regression harness (DESIGN-v1.7 ch03 ER-1).

``tests/fixtures/records/run-<id>/metrics.json`` holds the 24 pinned stored
records of ch03 section 0.2, each taken through ``tests/fixtures/scrub.py``
(``MANIFEST.json`` names the source, its sha256 and the rewritten paths).
``tests/expected/`` holds the reviewed expected values read from those
records. Tests for EVD-1 to EVD-13 load records and expectations from here
instead of hand-building run dicts.

Run ids may be given in full (``20260929-212900-5105a0``) or by their unique
suffix (``5105a0``, ``212900-5105a0``).
"""

from __future__ import annotations

import copy
import json
import tempfile
from functools import cache
from pathlib import Path
from typing import Any

RECORDS_DIR = Path(__file__).parent / "records"
EXPECTED_DIR = Path(__file__).parents[1] / "expected"


def record_ids() -> list[str]:
    """Every fixture run id, sorted."""
    return sorted(
        p.name.removeprefix("run-")
        for p in RECORDS_DIR.iterdir()
        if p.is_dir() and p.name.startswith("run-")
    )


def resolve(run_id: str) -> str:
    """The full run id for *run_id* or a unique suffix of it."""
    matches = [r for r in record_ids() if r == run_id or r.endswith(run_id)]
    if len(matches) != 1:
        raise KeyError(f"{run_id!r} names {len(matches)} fixture records")
    return matches[0]


@cache
def _raw(run_id: str) -> str:
    return (RECORDS_DIR / f"run-{resolve(run_id)}" / "metrics.json").read_text()


def load_record(run_id: str) -> dict[str, Any]:
    """A fresh copy of the fixture's metrics.json dict (safe to mutate)."""
    return json.loads(_raw(resolve(run_id)))


def load_metrics(run_id: str):
    """The fixture loaded the way ``lakebench`` loads a stored run
    (``MetricsStorage._dict_to_metrics``)."""
    from lakebench.metrics.storage import MetricsStorage

    with tempfile.TemporaryDirectory() as tmp:
        return MetricsStorage(tmp)._dict_to_metrics(load_record(run_id))


@cache
def _expected(name: str) -> dict[str, Any]:
    return json.loads((EXPECTED_DIR / f"{name}.json").read_text())


def expected(name: str) -> dict[str, Any]:
    """A copy of ``tests/expected/<name>.json``."""
    return copy.deepcopy(_expected(name))
