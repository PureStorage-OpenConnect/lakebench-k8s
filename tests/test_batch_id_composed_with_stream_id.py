"""B2: silver AML stream DELETE predicate composes _stream_id with _batch_id.

A fresh silver_stream_financial checkpoint restarts batch ids at 0, so a bare
_batch_id predicate would DELETE the previous stream's batch 0 the moment
the new stream committed its own batch 0. The predicate composes the two
into a unique per-stream key so a batch 0 from stream A never collides with
batch 0 from stream B.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent / "src/lakebench/spark/scripts"))


def test_delete_predicate_composes_stream_id_and_batch_id():
    """The AML stream's DELETE runs against a (stream, batch) key, not bare batch."""
    text = (
        Path(_HERE).parent / "src/lakebench/spark/scripts/silver_stream_financial.py"
    ).read_text()
    # Two DELETE statements, one per target table; both must use both keys.
    # DOTALL because the DELETE + predicate is split across lines in an f-string.
    deletes = re.findall(r"DELETE FROM.+?_stream_id.+?AND _batch_id", text, flags=re.DOTALL)
    assert len(deletes) >= 2, (
        f"expected >=2 composed DELETE predicates in silver_stream_financial; got {len(deletes)}"
    )
    # No code path may still key on _batch_id alone. Excludes the module
    # docstring which narrates the old protocol before the B2 fix landed.
    import ast

    tree = ast.parse(text)
    body_no_doc = tree.body[1:] if isinstance(tree.body[0], ast.Expr) else tree.body
    code_only = ast.unparse(ast.Module(body_no_doc, []))
    bad = re.findall(r"DELETE FROM[^\n]+WHERE _batch_id\s*=", code_only)
    assert not bad, f"bare _batch_id DELETE remains: {bad}"


def test_stream_writes_carry_stream_id():
    """Every stream-written row projects _stream_id = streaming_query_id, not NULL."""
    text = (
        Path(_HERE).parent / "src/lakebench/spark/scripts/silver_stream_financial.py"
    ).read_text()
    assert "streaming_query_id(spark)" in text
    assert 'withColumn("_stream_id", lit(sid))' in text


def test_batch_build_writes_batch_sentinel():
    """The batch-mode build stamps the reserved 'batch' _stream_id sentinel.

    B2: batch and stream must not collide on the same (_stream_id, _batch_id)
    key, so batch-mode writes use a sentinel string streams never emit.
    """
    text = (
        Path(_HERE).parent / "src/lakebench/spark/scripts/silver_build_financial.py"
    ).read_text()
    # build_transactions and build_edges each project the sentinel.
    assert text.count('lit("batch").alias("_stream_id")') >= 2, (
        "expected _stream_id='batch' in both build_transactions and build_edges"
    )


def test_ddl_files_carry_stream_id_column():
    """Both DDL locations declare _stream_id STRING on transactions and edges."""
    ddl = (Path(_HERE).parent / "src/lakebench/deploy/financial_ddl.py").read_text()
    txn_ddl = ddl[ddl.index("SILVER_TRANSACTIONS_DDL") :][:2000]
    assert "_stream_id" in txn_ddl and "STRING" in txn_ddl
    edges_ddl = ddl[ddl.index("SILVER_COUNTERPARTY_EDGES_DDL") :][:2000]
    assert "_stream_id" in edges_ddl and "STRING" in edges_ddl

    build = (
        Path(_HERE).parent / "src/lakebench/spark/scripts/silver_build_financial.py"
    ).read_text()
    ddl_txns = build[build.index("DDL_TXNS = f") :][:2000]
    assert "_stream_id" in ddl_txns and "STRING" in ddl_txns
    ddl_edges = build[build.index("DDL_EDGES = f") :][:1500]
    assert "_stream_id" in ddl_edges and "STRING" in ddl_edges
