"""AML runs resolve every table name from one place.

The live scale-1 AML run (2026-09-24) benchmarked
``silver.customer_interactions_enriched`` because ``tables.silver`` kept its
Customer 360 default while the Spark scripts wrote ``silver.transactions``;
7 of 8 queries failed TABLE_NOT_FOUND and the run still exited 0.
Maintenance, compaction and destroy targeted the same missing tables.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakebench.spark.job import JobType, SparkJobManager
from tests.conftest import make_config


def _fin(**tables):
    arch = {"workload": {"schema": "financial", "datagen": {"scale": 1}}}
    if tables:
        arch["tables"] = tables
    return make_config(architecture=arch)


def test_financial_defaults_point_at_the_financial_tables():
    t = _fin().architecture.tables
    assert t.silver == "silver.transactions"
    assert t.gold == "gold.alerts"


def test_c360_defaults_unchanged():
    t = make_config().architecture.tables
    assert t.silver == "silver.customer_interactions_enriched"
    assert t.gold == "gold.customer_executive_dashboard"


def test_explicit_names_win():
    t = _fin(silver="s2.txns", gold="g2.alerts").architecture.tables
    assert (t.silver, t.gold) == ("s2.txns", "g2.alerts")


def test_workload_tables_cover_every_financial_table():
    t = _fin().architecture.tables
    all_tables = t.workload_tables("financial")
    for name in (
        "silver.transactions",
        "silver.counterparty_edges",
        "silver.account_statements",
        "silver.entity_profiles",
        "gold.alerts",
        "gold.detection_status",
        "bronze.manifest",
    ):
        assert name in all_tables
    assert all_tables[0] == t.bronze
    assert t.bronze not in t.workload_tables("financial", layers=("silver", "gold"))
    assert len(all_tables) == len(set(all_tables))


def test_c360_workload_tables_are_the_three_layers():
    t = make_config().architecture.tables
    assert t.workload_tables("customer360") == [t.bronze, t.silver, t.gold]


@pytest.mark.parametrize("job", [JobType.SILVER_BUILD, JobType.GOLD_FINALIZE, JobType.GOLD_REFRESH])
def test_spark_jobs_receive_configured_financial_names(job):
    cfg = _fin(silver="s2.txns", gold_alerts="g2.alerts")
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    manifest = SparkJobManager(cfg, k8s)._build_manifest(job)
    env = {e["name"]: e.get("value") for e in manifest["spec"]["driver"]["env"]}
    assert env["LB_FINANCIAL_SILVER_TRANSACTIONS"] == "s2.txns"
    assert env["LB_FINANCIAL_SILVER_TXNS"] == "s2.txns"
    assert env["LB_FINANCIAL_GOLD_ALERTS"] == "g2.alerts"


def test_delete_prefix_refuses_root():
    from lakebench.s3.client import S3Client

    c = S3Client.__new__(S3Client)
    c._client = MagicMock()
    for bad in ("", "/", "//"):
        with pytest.raises(ValueError):
            c.delete_prefix("b", bad)
    c._client.get_paginator.assert_not_called()


def test_delete_prefix_scopes_to_the_prefix():
    from lakebench.s3.client import S3Client

    c = S3Client.__new__(S3Client)
    c._client = MagicMock()
    c._client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "checkpoints/bronze-ingest/commits/0"}]}
    ]
    assert c.delete_prefix("b", "checkpoints/bronze-ingest") == 1
    c._client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="b", Prefix="checkpoints/bronze-ingest/"
    )
