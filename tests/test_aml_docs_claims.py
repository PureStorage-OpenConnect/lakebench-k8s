"""Doc-drift guard for AML documentation (B2a).

These are cheap grep-style assertions that catch the specific regressions
the UX persona review flagged: docs describing a runtime path or a
manifest layout that does not match the code. Every assertion here has a
matching code file:line, so if the code moves the doc must move too.

Kept small on purpose; adversarial coverage of the AML measurement
meaning belongs to the AML gate suite, not here.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS = REPO_ROOT / "docs"


def _read(path: Path) -> str:
    assert path.is_file(), f"missing doc: {path}"
    return path.read_text(encoding="utf-8")


def test_aml_scoring_names_score_financial_as_runtime_path() -> None:
    """aml-scoring.md must name score_financial.py as the runtime path
    that produces the published recall and precision. The prior version
    attributed the numbers to the aml_queries.py SQL templates, which
    are only executed by the unit tests (`load_aml_queries`), not by
    `lakebench run` or `lakebench financial score`.
    """
    text = _read(DOCS / "aml-scoring.md")
    assert "score_financial.py" in text, (
        "aml-scoring.md must name spark/scripts/score_financial.py as the runtime scoring path"
    )
    # Must call out that it is authoritative / the runtime path, not one
    # of several sources of truth.
    assert "authoritative runtime path" in text or "runtime path that produces" in text, (
        "aml-scoring.md must describe score_financial.py as the authoritative "
        "runtime path, not one of several"
    )


def test_run_flow_docs_do_not_claim_a_reference_cross_check() -> None:
    """`lakebench run` does NOT invoke the reference detector against
    recall; `score_financial_reference.py` is a separate command and
    writes recall as NULL in its per-typology output. Doc claims that
    the run cross-checks recall against the reference detector are
    what tier-3 item 26 flagged.
    """
    forbidden = "cross-checked"
    getting_started = _read(DOCS / "getting-started.md")
    readme = _read(REPO_ROOT / "README.md")
    assert forbidden not in getting_started, (
        "getting-started.md must not claim `lakebench run` cross-checks recall "
        "against a reference detector; the run does not invoke it"
    )
    assert forbidden not in readme, (
        "README.md must not claim `lakebench run` cross-checks recall against a reference detector"
    )
    # And it must still describe the reference detector as a separate
    # command, so a reader does not conclude it does not exist.
    assert "reference-score" in getting_started, (
        "getting-started.md must still mention `lakebench financial "
        "reference-score` as the separate command that runs the reference detector"
    )


def test_financial_score_manifest_path_includes_pacs008_prefix() -> None:
    """The `lakebench financial score --manifest` example must include the
    `pacs008/` prefix. Without it the URI points at
    `s3://<bronze>/manifest/manifest.parquet`, which the generator does
    not write (the manifest lives under `pacs008/manifest/`, matching
    `_BRONZE_ROOT` in spark/scripts/score_financial.py).
    """
    for name in ("aml-scoring.md", "getting-started.md"):
        text = _read(DOCS / name)
        # Skip files that don't include a score command example.
        if "financial score" not in text:
            continue
        # Every `--manifest` example that names a manifest file must go
        # through `pacs008/manifest/`, not `<bronze>/manifest/`.
        assert "pacs008/manifest/manifest" in text, (
            f"{name} must document the manifest path with the pacs008/ prefix "
            "(s3a://<bronze>/pacs008/manifest/manifest.parquet)"
        )
        assert "//<bronze-bucket>/manifest/manifest" not in text, (
            f"{name} must not document the manifest path without the pacs008/ "
            "prefix; the generator writes it under pacs008/"
        )
