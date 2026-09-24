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

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


# The Rust datagen puts every AML output under these sub-paths of the
# invocation prefix. Keys mirror the format!(...) calls in
# datagen_rs/src/bin/generate.rs financial_main(). Referenced by
# `test_rust_datagen_writes_expected_sub_paths` -- adding an entry
# here without updating the reader will fail the corresponding
# assertion below.
DATAGEN_SUBPATHS = {
    "transactions_dir": "bronze/pacs008/",
    "party_file": "bronze/party.parquet",
    "account_file": "bronze/account.parquet",
    "manifest_file": "manifest/manifest.parquet",
}

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


def test_rust_datagen_writes_expected_sub_paths():
    """If the Rust datagen renames any of these paths, every reader
    that expects them needs updating in the same commit."""
    src = _read("datagen_rs/src/bin/generate.rs")
    for label, sub in DATAGEN_SUBPATHS.items():
        # transactions_dir is a directory; the actual key format is
        # "bronze/pacs008/part-{:06}.parquet". Anchor on the dir
        # prefix + "part-" so a rename to any of the pieces trips.
        needle = sub + "part-" if label == "transactions_dir" else sub
        assert needle in src, (
            f"Rust datagen no longer writes {label} to {sub}. "
            "Update readers + this test in lock-step."
        )


@pytest.mark.parametrize(
    "script",
    [
        "src/lakebench/spark/scripts/bronze_verify_financial.py",
        "src/lakebench/spark/scripts/bronze_ingest_financial.py",
    ],
)
def test_reader_derives_datagen_sub_path(script: str):
    """LB-089: the reader must default to the datagen v2 sub-path,
    not the flat layout the retired datagen_py used. Assertions run
    against code-only source (docstrings stripped) so a comment that
    mentions the string cannot satisfy the check while the code path
    has reverted."""
    code = _code_only(_read(script))
    # The derivation must anchor the reader to the transactions
    # sub-path. Two acceptable forms: a literal `bronze/pacs008/`
    # concatenated onto the root, or a helper import (not present
    # today, but forward-compatible).
    assert DATAGEN_SUBPATHS["transactions_dir"] in code, (
        f"{script} code path does not derive the transactions "
        f"sub-path {DATAGEN_SUBPATHS['transactions_dir']!r}. "
        "Datagen v2 writes there; reader must match, or LB-089 re-opens."
    )
    # And the reader must honour LB_FINANCIAL_BRONZE_PREFIX as the
    # OUTER prefix so job.py's semantic stays load-bearing.
    assert "LB_FINANCIAL_BRONZE_PREFIX" in code, (
        f"{script} code path no longer honours LB_FINANCIAL_BRONZE_PREFIX."
    )


def test_reader_default_matches_datagen_default_prefix():
    """When path_template is at its 'financial' default (config
    substitutes 'pacs008' in the deploy path), the reader's default
    ROOT must be ``pacs008/``. Any change to the default has to be
    applied to both the deployer default and the reader default in
    the same commit."""
    # ast.unparse normalises quote style to single-quotes; assert on
    # a quote-agnostic substring.
    code = _code_only(_read("src/lakebench/spark/scripts/bronze_verify_financial.py"))
    assert "'pacs008/'" in code or '"pacs008/"' in code, (
        "bronze_verify_financial default LB_FINANCIAL_BRONZE_PREFIX no "
        "longer 'pacs008/'; must match datagen.py's substitution."
    )
    depl_code = _code_only(_read("src/lakebench/deploy/datagen.py"))
    assert "'pacs008'" in depl_code or '"pacs008"' in depl_code, (
        "datagen deployer no longer substitutes 'pacs008' when "
        "path_template is the C360 default. Reader default and "
        "deployer substitution must stay lock-step."
    )


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
    code = _code_only(_read("src/lakebench/cli/_run.py"))
    assert MANIFEST_GLOB in code
