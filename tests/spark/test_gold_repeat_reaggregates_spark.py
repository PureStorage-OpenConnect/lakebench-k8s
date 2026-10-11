"""A repeat gold-finalize over changed silver rebuilds every gold day, records
the strategy, and refuses an incremental override before any write, on both
table formats. Scenarios in ``gold_repeat_scenarios``."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

import gold_repeat_scenarios as sc  # noqa: E402

pytestmark = pytest.mark.usefixtures("load_script")

ICEBERG_CATALOG = "lbgold"
DELTA_CATALOG = "spark_catalog"


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    return tmp_path_factory.mktemp("gold-repeat-ice")


@pytest.fixture(
    params=[
        pytest.param("iceberg", marks=pytest.mark.requires_jars("iceberg")),
        pytest.param("delta", marks=pytest.mark.requires_jars("delta")),
    ]
)
def gold(request, spark_session, warehouse, load_script, monkeypatch):
    """The gold-finalize script for one table format, with its catalog, a
    silver writer and a gold-namespace probe."""
    from pyspark.sql import SparkSession

    monkeypatch.delenv("LB_GOLD_INCREMENTAL", raising=False)
    # main() ends with spark.stop(); the module's session must outlive it.
    monkeypatch.setattr(SparkSession, "stop", lambda self: None)
    if request.param == "iceberg":
        catalog = ICEBERG_CATALOG
        request.getfixturevalue("iceberg_catalog")(spark_session, catalog, warehouse)
        spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")

        def write_silver(df):
            df.writeTo(f"{catalog}.{sc.SILVER}").createOrReplace()

        def drop_gold_namespace():
            spark_session.sql(f"DROP NAMESPACE IF EXISTS {catalog}.gold")

        def namespaces():
            return [r[0] for r in spark_session.sql(f"SHOW NAMESPACES IN {catalog}").collect()]

        script = "gold_finalize"
    else:
        catalog = DELTA_CATALOG
        monkeypatch.delenv("LB_CATALOG_TYPE", raising=False)
        spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

        def write_silver(df):
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(f"{catalog}.{sc.SILVER}")

        def drop_gold_namespace():
            spark_session.sql(f"DROP SCHEMA IF EXISTS {catalog}.gold CASCADE")

        def namespaces():
            return [r[0] for r in spark_session.sql(f"SHOW SCHEMAS IN {catalog}").collect()]

        script = "gold_finalize_delta"
    monkeypatch.setenv("LB_ICEBERG_CATALOG", catalog)
    yield SimpleNamespace(
        mod=load_script(script),
        catalog=catalog,
        write_silver=write_silver,
        drop_gold_namespace=drop_gold_namespace,
        namespaces=namespaces,
    )
    spark_session.conf.unset("spark.lb.gold.strategy")
    spark_session.sql(f"DROP TABLE IF EXISTS {catalog}.{sc.GOLD}")
    spark_session.sql(f"DROP TABLE IF EXISTS {catalog}.{sc.SILVER}")


def _recorded(out):
    """(strategy, source) as the metrics collector reads them from the log."""
    from lakebench.metrics.collector import MetricsCollector

    extra = MetricsCollector().parse_driver_logs(out, "gold-finalize").extra_metrics
    return extra["gold_strategy"], extra["gold_strategy_source"]


def test_gold_repeat_changed_silver_reaggregates(spark_session, gold, monkeypatch, capsys):
    silver_tbl, gold_tbl = f"{gold.catalog}.{sc.SILVER}", f"{gold.catalog}.{sc.GOLD}"
    base = sc.silver_df(spark_session).localCheckpoint()
    gold.write_silver(base)
    # The size the auto choice reads is the table's own (Spark metadata
    # syntax for each format), never the 0 of a failed metadata query.
    assert gold.mod.get_table_size_gb(spark_session, silver_tbl) > 0
    assert _recorded(sc.run_main(gold.mod, capsys)) == ("simple_agg", "auto")
    assert sc.stale_days(spark_session, silver_tbl, gold_tbl) == []

    gold.write_silver(sc.changed(base).localCheckpoint())
    # At 2,000 GB with gold present the old auto choice was INCREMENTAL.
    monkeypatch.setattr(gold.mod, "get_table_size_gb", lambda spark, tbl: 2000.0)
    out = sc.run_main(gold.mod, capsys)
    # Every day equals a fresh aggregation; the old incremental rerun left
    # day 1 (2024-01-01) stale.
    assert sc.stale_days(spark_session, silver_tbl, gold_tbl) == []
    assert _recorded(out) == ("two_phase_agg", "auto")
    days = sc.gold_rows(spark_session, gold_tbl)
    assert sc.START in days and sc.LAST in days


def test_incremental_override_exits_before_any_write(spark_session, gold, capsys):
    spark_session.conf.set("spark.lb.gold.strategy", "incremental")
    gold.drop_gold_namespace()
    with pytest.raises(SystemExit) as exc:
        sc.run_main(gold.mod, capsys)
    assert exc.value.code == 1
    assert "gold" not in gold.namespaces()


def test_cycle_and_override_sources_are_recorded(spark_session, gold, monkeypatch, capsys):
    gold.write_silver(sc.silver_df(spark_session))
    spark_session.conf.set("spark.lb.gold.strategy", "two_phase_agg")
    assert _recorded(sc.run_main(gold.mod, capsys)) == ("two_phase_agg", "override")
    monkeypatch.setenv("LB_GOLD_INCREMENTAL", "true")
    assert _recorded(sc.run_main(gold.mod, capsys)) == ("incremental", "cycle")
