"""LB-089 lock-step: AML bronze reader path vs Rust datagen writer path.

The Rust datagen (`datagen_rs/src/bin/generate.rs`) writes pacs.008
transactions under ``{root}/bronze/pacs008/part-*.parquet``, party and
account reference tables under ``{root}/bronze/``, and the typology
manifest under ``{root}/manifest/``. The Python bronze readers
(``bronze_verify_financial.py`` and ``bronze_ingest_financial.py``)
must default to a read path that matches. Before PR-F the readers
assumed a flat layout inherited from the retired ``datagen_py``
generator, so any AML deploy against a fresh datagen invocation
failed the first Spark job with ``UNABLE_TO_INFER_SCHEMA`` -- the
first live AML end-to-end run on 2026-09-21 surfaced it.

This is a text-level check because the writer is Rust and the reader
is Python; there is no import path that would tie them together at
test time. The point of this file is that a future refactor of the
datagen layout OR a future refactor of the reader path trips a
regression test rather than shipping to a live cluster.

If a rename of these paths is intentional, update BOTH sides and this
file in the same commit.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


# Readers glob every cycle's manifest: cycle n > 0 writes
# manifest/manifest-cNNN.parquet (datagen --cycle, WORKPLAN B4).
MANIFEST_GLOB = "manifest/manifest*.parquet"


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text()


def _code_only(source: str) -> str:
    """Strip docstrings and comments from Python source so an
    assertion cannot be trivially satisfied by a docstring that
    mentions the string. Adversarial round-2 F5 finding: the
    previous version of this test asserted `in src` and a docstring
    mentioning ``bronze/pacs008/`` would satisfy it while the code
    path had reverted."""
    tree = ast.parse(source)
    # Walk and drop docstring nodes.
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body.pop(0)
    unparsed = ast.unparse(tree)
    # Comments do not survive ast.unparse; the round-trip is
    # code-only by construction.
    return unparsed


def test_bronze_verify_registers_manifest_iceberg_table():
    """LB-089 round 2: AML benchmark queries rule_precision,
    rule_recall, rule_pattern_span, and aggregate_typology_coverage all read
    `{catalog}.bronze.manifest`. Without a registration in
    bronze_verify_financial the four scoring queries fail with
    'Table does not exist' at benchmark time -- baseline recall
    stays unpopulated and the AML scorecard is silently the
    detect-only subset."""
    code = _code_only(_read("src/lakebench/spark/scripts/bronze_verify_financial.py"))
    assert "MANIFEST_TABLE" in code, (
        "bronze_verify_financial no longer references MANIFEST_TABLE. "
        "The AML benchmark's manifest reads will fail."
    )
    assert MANIFEST_GLOB in code, (
        f"bronze_verify_financial does not read {MANIFEST_GLOB!r}: later "
        "cycles' planted instances would be scored as unlabelled negatives."
    )


def test_run_scores_against_every_cycles_manifest():
    # The scoring call moved to cli/_aml_post.py (AML-6), shared by batch
    # and continuous runs.
    code = _code_only(_read("src/lakebench/cli/_aml_post.py"))
    assert MANIFEST_GLOB in code
