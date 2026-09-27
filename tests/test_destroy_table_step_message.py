"""Destroy's table step says what it did when no engine can drop tables
(lb16 sweep: no-engine and DuckDB destroys printed an empty step)."""

from __future__ import annotations

import functools
from unittest.mock import patch

import pytest

from lakebench.deploy import destroy as destroy_mod
from lakebench.deploy.engine import DeploymentStatus

from . import test_destroy_bucket_delete as _tdb

OWNED = {"a-bronze": "MATCH", "a-silver": "MATCH", "a-gold": "MATCH"}


@pytest.fixture(autouse=True)
def _no_real_sleep():
    with patch("time.sleep"):
        yield


class _H(_tdb.TestDestroyAllBuckets):
    __test__ = False


def _destroy(**kw):
    h = _H()
    seen: list[tuple[str, DeploymentStatus, str]] = []
    original = destroy_mod.destroy_all

    def cb(component, status, message):
        seen.append((component, status, message))

    boto = _tdb.FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
    with patch.object(
        destroy_mod, "destroy_all", functools.partial(original, progress_callback=cb)
    ):
        # maint=None: find_maintenance_engine finds no engine pod.
        h._run(boto, OWNED, table_format="iceberg", **kw)
    table = [r for r in h._results if r.component == "table-cleanup"][-1]
    printed = [m for c, s, m in seen if c == "table-cleanup" and s != DeploymentStatus.IN_PROGRESS]
    return table, printed


@pytest.mark.parametrize(
    "engine_type,why",
    [
        ("none", "no query engine in this recipe"),
        ("duckdb", "DuckDB cannot drop catalog tables"),
        ("trino", "no running trino pod"),
    ],
)
def test_no_engine_table_step_is_reported(engine_type, why):
    msg = destroy_mod._no_engine_table_message(
        engine_type, "iceberg", ns_goes=True, clean_buckets=True
    )
    assert why in msg and "goes with the namespace" in msg
    kept = destroy_mod._no_engine_table_message(
        engine_type, "delta", ns_goes=False, clean_buckets=False
    )
    assert "stay registered" in kept and "bucket cleanup is off" in kept


def test_the_skipped_table_step_reaches_the_progress_callback():
    table, printed = _destroy(create_namespace=True)
    assert table.status is DeploymentStatus.SKIPPED
    assert printed == [table.message]
    assert "tables not dropped" in table.message
