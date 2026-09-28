"""G4: Iceberg silver append re-asserts TBLPROPERTIES on cycles 1+.

An Iceberg table's `writeTo(...).append()` does not carry
TBLPROPERTIES: if a cycle-0 CREATE established a property set and a
later ALTER (or a rebuild by an older script) dropped one, the append
would keep writing under whichever properties currently exist. This
silently degrades the write for the lifetime of the deployment. G4
adds an ALTER TABLE ... SET TBLPROPERTIES call before each append.

Local Spark is not available in the unit tier; test via source
inspection and a stubbed exec of the helper. The full-fixture Iceberg
verification lives in tests/spark under the standard local-Spark tier.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _exec_helper_only():
    """Execute just the shared helper without importing pyspark.

    The helper uses METADATA_DELETE_AFTER_COMMIT / METADATA_PREVIOUS_VERSIONS_MAX
    from common.py. We supply those constants and let the helper's
    `spark.sql(...)` call reach a MagicMock.
    """
    src = (_SCRIPTS_DIR / "silver_build.py").read_text()
    # Slice out just the module-level helper block G4 added.
    start = src.index("_SILVER_ICEBERG_STATIC_PROPS")
    end = src.index("\ndef silver_simple", start)
    snippet = src[start:end]
    ns = {
        "METADATA_DELETE_AFTER_COMMIT": (
            "write.metadata.delete-after-commit.enabled",
            "true",
        ),
        "METADATA_PREVIOUS_VERSIONS_MAX": (
            "write.metadata.previous-versions-max",
            "50",
        ),
    }
    exec(compile(snippet, str(_SCRIPTS_DIR / "silver_build.py"), "exec"), ns)
    return ns


def _mock_spark_with_conf(dist_mode="hash", fanout="false"):
    spark = MagicMock()
    spark.conf.get.side_effect = lambda key, default=None: {
        "spark.lb.silver.distribution_mode": dist_mode,
        "spark.lb.silver.fanout_enabled": fanout,
    }.get(key, default)
    return spark


def test_helper_runs_alter_tblproperties_with_the_full_set():
    """reassert_silver_iceberg_props issues an ALTER for the property keys."""
    ns = _exec_helper_only()
    reassert = ns["reassert_silver_iceberg_props"]
    spark = _mock_spark_with_conf()
    reassert(spark, "ice.silver.customer_interactions_enriched")
    assert spark.sql.called
    sql = spark.sql.call_args.args[0]
    assert sql.startswith("ALTER TABLE ice.silver.customer_interactions_enriched SET TBLPROPERTIES")
    for key in (
        "write.format.default",
        "write.parquet.compression-codec",
        "write.metadata.delete-after-commit.enabled",
        "write.metadata.previous-versions-max",
        "write.target-file-size-bytes",
        "write.distribution-mode",
    ):
        assert key in sql, f"missing TBLPROPERTIES key: {key}"


def test_helper_honours_distribution_mode_override():
    """Operator's spark.lb.silver.distribution_mode=none is NOT overwritten.

    BUG-003 / LB-049 escape hatch for scale-5TB+ STREAMING must survive
    the cycle-1+ re-assert. Previously the helper hardcoded
    write.distribution-mode=hash, silently reverting the operator's
    choice on every append.
    """
    ns = _exec_helper_only()
    reassert = ns["reassert_silver_iceberg_props"]
    spark = _mock_spark_with_conf(dist_mode="none")
    reassert(spark, "ice.silver.customer_interactions_enriched")
    sql = spark.sql.call_args.args[0]
    assert "'write.distribution-mode' = 'none'" in sql
    assert "'write.distribution-mode' = 'hash'" not in sql


def test_helper_carries_fanout_when_enabled():
    ns = _exec_helper_only()
    reassert = ns["reassert_silver_iceberg_props"]
    spark = _mock_spark_with_conf(fanout="true")
    reassert(spark, "ice.silver.customer_interactions_enriched")
    sql = spark.sql.call_args.args[0]
    assert "write.spark.fanout.enabled" in sql
    assert "'write.spark.fanout.enabled' = 'true'" in sql


def test_silver_simple_calls_reassert_before_append():
    """The SIMPLE append path invokes the helper before writeTo(...).append()."""
    src = (_SCRIPTS_DIR / "silver_build.py").read_text()
    # `silver_simple`'s append branch runs reassert before writeTo append.
    fn = src.split("def silver_simple(", 1)[1]
    fn = fn.split("\ndef ", 1)[0]
    append_branch = fn.split("if appending:", 1)[1].split("else:", 1)[0]
    assert "reassert_silver_iceberg_props(spark, silver_tbl)" in append_branch, (
        "silver_simple's append branch must reassert TBLPROPERTIES before "
        ".append() so a drifted table cannot silently degrade the write"
    )
    # And the reassert runs BEFORE the writeTo append.
    reassert_idx = append_branch.index("reassert_silver_iceberg_props(spark, silver_tbl)")
    write_idx = append_branch.index(".append()")
    assert reassert_idx < write_idx


def test_silver_streaming_calls_reassert_before_append():
    """The STREAMING append path also invokes the helper before .append()."""
    src = (_SCRIPTS_DIR / "silver_build.py").read_text()
    fn = src.split("def silver_streaming(", 1)[1]
    fn = fn.split("\ndef ", 1)[0]
    append_branch = fn.split("if appending:", 1)[1].split("else:", 1)[0]
    assert "reassert_silver_iceberg_props(spark, silver_tbl)" in append_branch
    reassert_idx = append_branch.index("reassert_silver_iceberg_props(spark, silver_tbl)")
    write_idx = append_branch.index(".append()")
    assert reassert_idx < write_idx


def test_financial_replace_data_reasserts_props_before_overwrite():
    """AML batch's ``_replace_data`` helper runs ALTER before overwrite.

    A drifted silver.transactions or silver.entities table would take
    the next .overwrite(lit(True)) under whichever properties the ALTER
    trail left, so the DDL's declared retention/compression can silently
    stop applying (invariant 5).

    The helper lives at module scope (extracted from a main() closure in
    the A1-atomic + F2 rework) so the pre-flight tests can spy on it
    without a live Iceberg backend. The extraction changed its signature
    to ``(spark, df, table)`` but its ALTER-before-overwrite contract is
    unchanged.
    """
    src = (_SCRIPTS_DIR / "silver_build_financial.py").read_text()
    body = src.split("def _replace_data(spark, df, table):", 1)[1]
    # Cut at the end of the helper body (blank line + next `def` at column 0).
    body = body.split("\n\n\ndef ", 1)[0]
    assert "ALTER TABLE" in body
    assert "SET TBLPROPERTIES" in body
    assert "ICEBERG_V2_SNAPPY_PROPS_SQL" in body
    # The ALTER must run BEFORE the overwrite so a drifted table takes
    # the DDL properties before this cycle's write commits. The docstring
    # also mentions .overwrite(lit(True)); anchor on the executable call
    # sequence to avoid matching prose.
    alter_idx = body.index('spark.sql(f"ALTER TABLE')
    write_idx = body.index("df.writeTo(fq).overwrite(lit(True))")
    assert alter_idx < write_idx
