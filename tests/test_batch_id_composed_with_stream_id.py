"""B2: silver AML stream DELETE predicate composes _stream_id with _batch_id.

A fresh silver_stream_financial checkpoint restarts batch ids at 0, so a bare
_batch_id predicate would DELETE the previous stream's batch 0 the moment
the new stream committed its own batch 0. The predicate composes the two
into a unique per-stream key so a batch 0 from stream A never collides with
batch 0 from stream B.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent

pytestmark = pytest.mark.usefixtures("load_script")


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
