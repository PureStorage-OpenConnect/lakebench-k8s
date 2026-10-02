"""V16-3 (Iceberg): a repeat gold-finalize over changed silver rebuilds
every gold day, records the strategy, and refuses an incremental override
before any write. Scenarios in ``gold_repeat_scenarios``."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

import gold_repeat_scenarios as sc  # noqa: E402

pytestmark = pytest.mark.usefixtures("load_script")

CATALOG = "lbgold"


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    return tmp_path_factory.mktemp("gold-repeat-ice")


@pytest.fixture
def gold(spark_session, iceberg_catalog, warehouse, load_script, monkeypatch):
    from pyspark.sql import SparkSession

    iceberg_catalog(spark_session, CATALOG, warehouse)
    monkeypatch.setenv("LB_ICEBERG_CATALOG", CATALOG)
    monkeypatch.delenv("LB_GOLD_INCREMENTAL", raising=False)
    # main() ends with spark.stop(); the module's session must outlive it.
    monkeypatch.setattr(SparkSession, "stop", lambda self: None)
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    yield load_script("gold_finalize")
    spark_session.conf.unset("spark.lb.gold.strategy")
    spark_session.sql(f"DROP TABLE IF EXISTS {CATALOG}.{sc.GOLD}")
    spark_session.sql(f"DROP TABLE IF EXISTS {CATALOG}.{sc.SILVER}")


def _write_silver(spark, df):
    df.writeTo(f"{CATALOG}.{sc.SILVER}").createOrReplace()


@pytest.mark.requires_jars("iceberg")
def test_gold_repeat_changed_silver_reaggregates(spark_session, gold, monkeypatch, capsys):
    silver_tbl, gold_tbl = f"{CATALOG}.{sc.SILVER}", f"{CATALOG}.{sc.GOLD}"
    base = sc.silver_df(spark_session).localCheckpoint()
    _write_silver(spark_session, base)
    out = sc.run_main(gold, capsys)
    assert "gold_strategy: simple_agg" in out and "gold_strategy_source: auto" in out
    assert sc.stale_days(spark_session, silver_tbl, gold_tbl) == []

    _write_silver(spark_session, sc.changed(base).localCheckpoint())
    # At 2,000 GB with gold present the old auto choice was INCREMENTAL.
    monkeypatch.setattr(gold, "get_table_size_gb", lambda spark, tbl: 2000.0)
    out = sc.run_main(gold, capsys)
    # Every day equals a fresh aggregation; the old incremental rerun left
    # day 1 (2024-01-01) stale.
    assert sc.stale_days(spark_session, silver_tbl, gold_tbl) == []
    assert "gold_strategy: two_phase_agg" in out and "gold_strategy_source: auto" in out
    days = sc.gold_rows(spark_session, gold_tbl)
    assert sc.START in days and sc.LAST in days


@pytest.mark.requires_jars("iceberg")
def test_incremental_override_exits_before_any_write(spark_session, gold, capsys):
    spark_session.conf.set("spark.lb.gold.strategy", "incremental")
    spark_session.sql(f"DROP NAMESPACE IF EXISTS {CATALOG}.gold")
    with pytest.raises(SystemExit) as exc:
        sc.run_main(gold, capsys)
    assert exc.value.code == 1
    assert "multi-cycle cycles 2+ only" in capsys.readouterr().out
    namespaces = [r[0] for r in spark_session.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()]
    assert "gold" not in namespaces


@pytest.mark.requires_jars("iceberg")
def test_cycle_and_override_sources_are_recorded(spark_session, gold, monkeypatch, capsys):
    _write_silver(spark_session, sc.silver_df(spark_session))
    spark_session.conf.set("spark.lb.gold.strategy", "two_phase_agg")
    out = sc.run_main(gold, capsys)
    assert "gold_strategy: two_phase_agg" in out and "gold_strategy_source: override" in out
    monkeypatch.setenv("LB_GOLD_INCREMENTAL", "true")
    out = sc.run_main(gold, capsys)
    assert "gold_strategy: incremental" in out and "gold_strategy_source: cycle" in out
