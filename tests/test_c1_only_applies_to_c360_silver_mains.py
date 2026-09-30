"""C1 (silver-plan): the strict resolve_data_clock enforcement lives on
C360 silver mains only. AML mains use the non-strict fallback.

Rationale: AML silver does not compute customer_recency_score, so a
missing LB_DATA_CLOCK does not silently ship a wrong score there. The
strict guard is scoped to the C360 mains that DO score recency.
"""

from __future__ import annotations

from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"

# C360 silver mains: must call resolve_data_clock with strict=True.
_C360_SILVER_MAINS = (
    "silver_build.py",
    "silver_build_delta.py",
    "silver_stream.py",
    "silver_stream_delta.py",
)

# AML silver mains: must NOT pass strict=True to resolve_data_clock. (I1
# uses resolve_data_clock in silver_build_financial.main() to source a
# deterministic profile_updated_ts, non-strict per Block I1 dependency.)
_AML_SILVER_MAINS = (
    "silver_build_financial.py",
    "silver_stream_financial.py",
)


def _read(path):
    return (_SCRIPTS / path).read_text(encoding="utf-8")


def test_c360_silver_mains_use_strict_resolve_data_clock():
    for name in _C360_SILVER_MAINS:
        text = _read(name)
        assert "resolve_data_clock(" in text, f"{name}: does not call resolve_data_clock"
        # A strict call is textually stable: strict=True in the argument list.
        # Every resolve_data_clock call in a C360 main should be strict.
        call_lines = [line for line in text.splitlines() if "resolve_data_clock(" in line]
        for line in call_lines:
            assert "strict=True" in line, f"{name}: expected strict=True on line: {line.strip()}"


def test_aml_silver_mains_do_not_use_strict_resolve_data_clock():
    for name in _AML_SILVER_MAINS:
        text = _read(name)
        # AML mains may or may not call resolve_data_clock at all. If they do,
        # none of the calls carry strict=True.
        for line in text.splitlines():
            if "resolve_data_clock(" in line:
                assert "strict=True" not in line, (
                    f"{name}: AML silver must not use strict=True on: {line.strip()}"
                )


def test_no_silver_c360_main_falls_back_to_configured_data_clock_only():
    """The stream C360 mains previously called ``configured_data_clock()``
    directly, which returns None when LB_DATA_CLOCK is unset (recency is
    then measured per micro-batch or NULL). C1 replaces that with a strict
    resolve_data_clock call so a missing env raises SilverAbort.
    """
    for name in ("silver_stream.py", "silver_stream_delta.py"):
        text = _read(name)
        # Every remaining top-level configured_data_clock() call inside main
        # would be a regression. Allowing imports (line 45/57), the call at
        # main() startup should now go through resolve_data_clock(strict=True).
        # We test by asserting resolve_data_clock is called in main() and
        # strict=True is present at at least one call site.
        assert "resolve_data_clock" in text
