"""The Delta c360 silver build takes its rebuild epoch from the table's log.

``resolve_txn_epoch`` in ``silver_build_delta.py`` decides the epoch of the
(txnAppId, txnVersion) key each cycle writes. The script runs a Spark job at
import, so the function is lifted out of the source and run alone. The
executed proof is ``tests/spark/test_silver_build_delta_epoch_spark.py``.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


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


@pytest.mark.parametrize(
    ("log", "configured", "append", "cycle", "epoch"),
    [
        ({}, 0, False, 0, 0),
        ({}, 4, False, 0, 4),
        # the counter went back to 0 over a log holding epochs 0 and 1
        ({0: 2, 1: 2}, 0, False, 0, 2),
        ({0: 2, 1: 2}, 1, False, 0, 2),
        ({0: 2}, 5, False, 0, 5),
        # a cycle whose ConfigMap read fell back to 0 after cycle 0 started epoch 3
        ({0: 2, 3: 0}, 0, True, 1, 3),
        ({3: 2}, 3, True, 2, 3),  # a retry of a committed cycle keeps its key
        # the run's full build started epoch 1; a later counter bump does not move it
        ({1: 0}, 2, True, 1, 1),
        ({}, 2, True, 1, 2),  # a table without build keys
    ],
)
def test_resolve_txn_epoch(log, configured, append, cycle, epoch):
    assert resolve(log, configured, append, cycle) == epoch


def test_append_behind_a_later_committed_cycle_refuses():
    with pytest.raises(_Abort):
        resolve({3: 3}, 3, True, 2)


def test_app_id_pattern_matches_the_written_keys_only(load_script):
    delta_batch_txn_options = load_script("common").delta_batch_txn_options

    pattern = NS["_TXN_APP_ID"]
    app = delta_batch_txn_options(NS["_TXN_APP"], 12, 3)["txnAppId"]
    assert pattern.match(app).group(1) == "12"
    assert pattern.match("lb-silver-stream-0b7c4c1e-rebuild-1") is None
    assert pattern.match("lb-silver-build-rebuild-1-x") is None


# --- committed_epochs: reads the log's keys, fails closed ------------------


class _Iter:
    """A py4j Scala-map iterator of (key, value) tuples."""

    def __init__(self, items):
        self._items = list(items)

    def hasNext(self):  # noqa: N802 -- py4j iterator shape
        return bool(self._items)

    def next(self):
        k, v = self._items.pop(0)
        return SimpleNamespace(_1=lambda: k, _2=lambda: v)


_LOCATION = "s3a://lb-silver/warehouse/silver.db/t"


def _fake_spark(txns=(), sql_error=None, jvm_error=None):
    """Just the surface committed_epochs touches: sql(), _jvm, _jsparkSession."""

    def sql(query):
        if sql_error:
            raise sql_error
        assert query.startswith("DESCRIBE DETAIL ")
        return SimpleNamespace(collect=lambda: [{"location": _LOCATION}])

    def for_table_with_snapshot(session, location):
        if jvm_error:
            raise jvm_error
        assert location == _LOCATION
        txn_map = SimpleNamespace(iterator=lambda: _Iter(txns))
        return SimpleNamespace(_2=lambda: SimpleNamespace(transactions=lambda: txn_map))

    delta_log = SimpleNamespace(forTableWithSnapshot=for_table_with_snapshot)
    jvm = SimpleNamespace(
        org=SimpleNamespace(
            apache=SimpleNamespace(
                spark=SimpleNamespace(
                    sql=SimpleNamespace(delta=SimpleNamespace(DeltaLog=delta_log))
                )
            )
        )
    )
    return SimpleNamespace(sql=sql, _jvm=jvm, _jsparkSession=object())


def _committed_epochs():
    src = (_SCRIPTS / "silver_build_delta.py").read_text()
    keep = [
        n
        for n in ast.parse(src).body
        if (isinstance(n, ast.FunctionDef) and n.name == "committed_epochs")
        or (
            isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "_TXN_APP_ID" for t in n.targets)
        )
    ]
    ns = {"re": re, "SilverAbort": _Abort}
    exec(compile(ast.Module(keep, []), "silver_build_delta.py", "exec"), ns)  # noqa: S102
    return ns["committed_epochs"]


def test_committed_epochs_keeps_only_the_build_keys():
    spark = _fake_spark(
        [
            ("lb-silver-build-rebuild-3", 2),
            ("lb-silver-build-rebuild-1", 4),
            ("lb-silver-stream-0b7c-rebuild-9", 7),
            ("lb-bronze-ingest-1f2e", 11),
        ]
    )
    assert _committed_epochs()(spark, "spark_catalog.silver.t") == {3: 2, 1: 4}


@pytest.mark.parametrize(
    "spark",
    [
        _fake_spark(sql_error=RuntimeError("DESCRIBE DETAIL failed")),
        _fake_spark(jvm_error=AttributeError("no forTableWithSnapshot in this Delta")),
    ],
    ids=["detail", "jvm"],
)
def test_committed_epochs_fails_closed(spark):
    """A read it cannot make is a refusal, never an empty log."""
    with pytest.raises(_Abort, match="cannot read the Delta transaction ids"):
        _committed_epochs()(spark, "spark_catalog.silver.t")
