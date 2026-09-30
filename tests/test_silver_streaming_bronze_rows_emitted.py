"""A5 (silver-plan): STREAMING emits `bronze_rows`, never `estimated_rows`.

Before A5, ``silver_build.py`` and ``silver_build_delta.py`` published a
``estimated_rows`` metric derived from ``size_gb * 1024**3 / 4096``. The
formula (~4KB per row) is workload-specific and misses badly on real Zipf
data. A5 drops the estimate: SIMPLE always did a real bronze count, and
STREAMING now does one too (parquet-metadata footer scan, ~one S3 HEAD per
file). Both paths emit the same key: ``bronze_rows``.

These tests source-check the emission (a real live-run test lands under
Block A's local-Spark tier). We also assert the empty-bronze branch uses
``profile.total_size_gb`` rather than the removed ``profile.transaction_count``,
and that the size probe goes through the strict variant so an S3 outage does
not look like empty bronze.
"""

from __future__ import annotations

from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _strip_comments(text: str) -> str:
    """Drop full-line and trailing ``#`` comments so a source assertion is
    not fooled by a comment mentioning a removed identifier.

    Uses ``tokenize`` so a ``#`` inside a string literal (for example
    ``log("estimated_rows: not emitted #A5")``) is NOT treated as a comment.
    """
    import io
    import tokenize

    out_tokens: list = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type == tokenize.COMMENT:
                continue
            out_tokens.append(tok)
        return tokenize.untokenize(out_tokens)
    except Exception:  # noqa: BLE001
        # A partial function body may not be a complete parse; fall back to
        # a conservative line-based strip that at least catches whole-line
        # and end-of-line comments outside string literals.
        return "\n".join(line.split("#", 1)[0].rstrip() for line in text.splitlines())


def _tail(src: str) -> str:
    return _strip_comments(src.rsplit("=== JOB METRICS", 1)[1])


def _profile_body(src: str) -> str:
    """The ``profile_bronze_data`` function body with comments stripped."""
    after = src.split("def profile_bronze_data", 1)[1]
    return _strip_comments(after.split("\ndef ", 1)[0])


def _streaming_body(src: str) -> str:
    """The ``silver_streaming`` function body with comments stripped."""
    after = src.split("def silver_streaming", 1)[1]
    return _strip_comments(after.split("\ndef ", 1)[0])


def test_estimated_row_count_formula_gone():
    """The size / 4096 heuristic is dropped from profile_bronze_data."""
    for name in ("silver_build.py", "silver_build_delta.py"):
        body = _profile_body((_SCRIPTS / name).read_text())
        assert "1024 * 1024 * 1024 / 4096" not in body, (
            f"{name} still computes estimated_row_count from bytes / 4096"
        )
        assert "Estimated rows:" not in body, (
            f"{name} still logs the removed 'Estimated rows:' line"
        )


def test_metrics_block_emits_bronze_rows_not_estimated_rows():
    """The JOB METRICS block emits `bronze_rows` and never `estimated_rows`.

    This is the observable contract: the collector parses these keys per line.
    """
    for name in ("silver_build.py", "silver_build_delta.py"):
        tail = _tail((_SCRIPTS / name).read_text())
        assert "bronze_rows:" in tail, f"{name} does not emit bronze_rows"
        assert "estimated_rows" not in tail, f"{name} still emits the removed estimated_rows key"
        # `input_rows` was the SIMPLE-only label the code used before A5;
        # the unified field is bronze_rows.
        assert "input_rows:" not in tail, f"{name} still emits the pre-A5 input_rows label"


def test_streaming_counts_bronze_rows_before_transform():
    """silver_streaming counts bronze before the transform and stores it
    in ``_COUNTED['bronze_rows']`` so the metrics block emits a real value
    (matching SIMPLE) rather than falling back to an estimate."""
    for name in ("silver_build.py", "silver_build_delta.py"):
        body = _streaming_body((_SCRIPTS / name).read_text())
        assert "df_bronze.count()" in body, (
            f"{name} silver_streaming does not count bronze before the transform"
        )
        assert '_COUNTED["bronze_rows"]' in body, (
            f"{name} silver_streaming does not stash bronze_rows in _COUNTED"
        )


def test_empty_bronze_check_uses_size_not_transaction_count():
    """The empty-bronze exit uses ``profile.total_size_gb == 0`` (which the
    strict size probe from A6 makes trustworthy) rather than
    ``profile.transaction_count == 0`` (which was based on the dropped
    estimate).
    """
    for name in ("silver_build.py", "silver_build_delta.py"):
        src = (_SCRIPTS / name).read_text()
        # The old check is gone.
        assert "if profile.transaction_count == 0" not in src, (
            f"{name} still keys empty-bronze off profile.transaction_count"
        )
        # The new check is present.
        assert "profile.total_size_gb == 0" in src, (
            f"{name} does not fall through the new size-based empty check"
        )


def test_size_probe_uses_strict_variant():
    """A6 (silver-plan): get_path_size_gb calls path_size_gb_strict so a
    listing failure is a driver error, not an "empty bronze" exit-1.
    """
    for name in ("silver_build.py", "silver_build_delta.py"):
        src = (_SCRIPTS / name).read_text()
        assert "path_size_gb_strict" in src, (
            f"{name} does not import path_size_gb_strict from common"
        )
        # The non-strict variant is not called for the primary size probe.
        # (It may still be present via common.py's own module; this test
        # asserts the caller uses the strict variant here.)
        after = src.split("def get_path_size_gb", 1)[1]
        get_body = after.split("\ndef ", 1)[0]
        assert "path_size_gb_strict" in get_body, (
            f"{name} get_path_size_gb no longer routes through the strict variant"
        )
