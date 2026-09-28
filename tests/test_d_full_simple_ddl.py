"""D-full-simple: silver.account_statements carries `_batch_id BIGINT`
and `_stream_id STRING` in all three DDL locations, and
`silver_stream_financial.main()` calls ``ensure_column`` on the reused-
catalog upgrade path for both columns.

Three DDL sites must stay in lock-step:
1. ``src/lakebench/deploy/financial_ddl.py`` -- ``SILVER_ACCOUNT_STATEMENTS_DDL``,
   used by the deployer's bootstrap.
2. ``src/lakebench/spark/scripts/silver_build_financial.py`` -- inline
   ``DDL_STATEMENTS``, used by the batch script's CREATE-if-not-exists
   loop AND by the stream script (imported from silver_build_financial).
3. ``src/lakebench/spark/scripts/silver_stream_financial.py:main()`` --
   ``ensure_column(...)`` calls that add the columns to a reused catalog
   whose statements table predates the schema bump.

Without every site, a fresh deploy and a reused-catalog deploy end up with
different silver.account_statements schemas and the stream's per-batch
DELETE + INSERT (D-full simple) fails on the first replay.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_DEPLOY_DDL = _REPO / "src/lakebench/deploy/financial_ddl.py"
_BUILD_SCRIPT = _REPO / "src/lakebench/spark/scripts/silver_build_financial.py"
_STREAM_SCRIPT = _REPO / "src/lakebench/spark/scripts/silver_stream_financial.py"


def _statements_ddl_block(text: str, constant_name: str) -> str:
    """Return the CREATE TABLE body of the named DDL string constant.

    Both files declare the constant as a triple-quoted string. Extract the
    first ``CREATE TABLE ...`` block after the constant assignment and
    stop at the closing ``TBLPROPERTIES ...`` clause (or the end of the
    string constant). Comment lines starting with ``--`` are stripped.
    """
    m = re.search(rf"^{re.escape(constant_name)}\s*=\s*(?:f?['\"]{{3}})", text, re.MULTILINE)
    assert m, f"{constant_name} not found"
    tail = text[m.end() :]
    end = tail.find('"""')
    assert end >= 0, f"unterminated triple-quoted body for {constant_name}"
    body = tail[:end]
    stripped = []
    for line in body.splitlines():
        idx = line.find("--")
        if idx >= 0:
            line = line[:idx]
        stripped.append(line)
    return "\n".join(stripped)


def _column_present(body: str, name: str, sql_type: str) -> bool:
    """True if the body declares `<name> <sql_type> ...` on some line."""
    pattern = re.compile(rf"(^|,)\s*{re.escape(name)}\s+{re.escape(sql_type)}\b", re.IGNORECASE)
    return bool(pattern.search(body))


def test_deploy_ddl_has_batch_id_and_stream_id_on_statements():
    body = _statements_ddl_block(_DEPLOY_DDL.read_text(), "SILVER_ACCOUNT_STATEMENTS_DDL")
    assert _column_present(body, "_batch_id", "BIGINT"), (
        "deploy/financial_ddl.SILVER_ACCOUNT_STATEMENTS_DDL is missing "
        "`_batch_id BIGINT`; the stream's DELETE + INSERT idempotency key "
        "cannot be written and every replay corrupts running_balance"
    )
    assert _column_present(body, "_stream_id", "STRING"), (
        "deploy/financial_ddl.SILVER_ACCOUNT_STATEMENTS_DDL is missing "
        "`_stream_id STRING`; a fresh checkpoint's batch 0 would DELETE "
        "the previous stream's batch 0 rows"
    )


def test_build_script_ddl_has_batch_id_and_stream_id_on_statements():
    body = _statements_ddl_block(_BUILD_SCRIPT.read_text(), "DDL_STATEMENTS")
    assert _column_present(body, "_batch_id", "BIGINT"), (
        "silver_build_financial.DDL_STATEMENTS is missing `_batch_id BIGINT`"
    )
    assert _column_present(body, "_stream_id", "STRING"), (
        "silver_build_financial.DDL_STATEMENTS is missing `_stream_id STRING`"
    )


def test_stream_script_ensure_column_covers_statements_id_columns():
    src = _STREAM_SCRIPT.read_text()
    # ensure_column(..., SILVER_STATEMENTS, "_batch_id", "BIGINT")
    assert re.search(
        r"ensure_column\(\s*[^,]+,\s*[^,]+SILVER_STATEMENTS[^,]*,\s*['\"]_batch_id['\"]",
        src,
    ), (
        "silver_stream_financial.main() must call ensure_column(..., "
        "SILVER_STATEMENTS, '_batch_id', 'BIGINT') on the reused-catalog "
        "upgrade path"
    )
    assert re.search(
        r"ensure_column\(\s*[^,]+,\s*[^,]+SILVER_STATEMENTS[^,]*,\s*['\"]_stream_id['\"]",
        src,
    ), (
        "silver_stream_financial.main() must call ensure_column(..., "
        "SILVER_STATEMENTS, '_stream_id', 'STRING') on the reused-catalog "
        "upgrade path"
    )


def test_build_script_writes_batch_stream_sentinel_for_statements():
    """build_statements' final projection must include both idempotency-key
    columns; batch mode writes ``_batch_id=NULL`` and ``_stream_id='batch'``
    so the stream never DELETEs batch-written rows."""
    src = _BUILD_SCRIPT.read_text()
    # Look inside build_statements for the projection.
    fn_start = src.find("def build_statements(")
    assert fn_start >= 0
    fn_end = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert re.search(r"lit\(None\).cast\(['\"]bigint['\"]\).alias\(['\"]_batch_id['\"]\)", body), (
        "build_statements does not project _batch_id in its final .select"
    )
    assert re.search(r"lit\(['\"]batch['\"]\).alias\(['\"]_stream_id['\"]\)", body), (
        "build_statements does not project _stream_id='batch' sentinel"
    )
