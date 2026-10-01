"""The Delta c360 silver build takes its rebuild epoch from the table's log.

``resolve_txn_epoch`` in ``silver_build_delta.py`` decides the epoch of the
(txnAppId, txnVersion) key each cycle writes. The script runs a Spark job at
import, so the function is lifted out of the source and run alone. The
executed proof is ``tests/spark/test_silver_build_delta_epoch_spark.py``.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
sys.path.insert(0, str(_SCRIPTS))


class _Abort(RuntimeError):
    pass


def _load():
    src = (_SCRIPTS / "silver_build_delta.py").read_text()
    tree = ast.parse(src)
    keep = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "resolve_txn_epoch":
            keep.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in ("_TXN_APP", "_TXN_APP_ID") for t in node.targets
        ):
            keep.append(node)
    ns = {"re": re, "SilverAbort": _Abort}
    exec(compile(ast.Module(keep, []), "silver_build_delta.py", "exec"), ns)  # noqa: S102
    return ns


NS = _load()
resolve = NS["resolve_txn_epoch"]


def test_full_build_on_a_fresh_table_uses_the_configured_epoch():
    assert resolve({}, 0, False, 0) == 0
    assert resolve({}, 4, False, 0) == 4


def test_full_build_takes_an_epoch_the_log_has_never_held():
    """The counter went back to 0 over a log holding epochs 0 and 1."""
    assert resolve({0: 2, 1: 2}, 0, False, 0) == 2
    assert resolve({0: 2, 1: 2}, 1, False, 0) == 2
    assert resolve({0: 2}, 5, False, 0) == 5


def test_append_continues_the_newest_epoch_when_the_configured_one_is_stale():
    """A cycle whose ConfigMap read fell back to 0 after cycle 0 started epoch 3."""
    assert resolve({0: 2, 3: 0}, 0, True, 1) == 3


def test_append_retry_of_a_committed_cycle_keeps_its_key():
    assert resolve({3: 2}, 3, True, 2) == 3


def test_append_behind_a_later_committed_cycle_refuses():
    with pytest.raises(_Abort, match="already committed cycle 3"):
        resolve({3: 3}, 3, True, 2)


def test_append_with_a_newer_configured_epoch_uses_it():
    assert resolve({1: 2}, 2, True, 1) == 2
    assert resolve({}, 2, True, 1) == 2


def test_app_id_pattern_matches_the_written_keys_only():
    from common import delta_batch_txn_options

    pattern = NS["_TXN_APP_ID"]
    app = delta_batch_txn_options(NS["_TXN_APP"], 12, 3)["txnAppId"]
    assert pattern.match(app).group(1) == "12"
    assert pattern.match("lb-silver-stream-0b7c4c1e-rebuild-1") is None
    assert pattern.match("lb-silver-build-rebuild-1-x") is None


def test_the_writes_use_the_resolved_epoch():
    src = (_SCRIPTS / "silver_build_delta.py").read_text()
    assert src.count("rebuild_epoch=_txn_epoch,") == 2
    assert "rebuild_epoch=_rebuild_epoch" not in src
