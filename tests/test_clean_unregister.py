"""`lakebench clean` unregisters a layer's tables before emptying its bucket.

`clean silver|gold|bronze|data` emptied buckets and kept the catalog, so the
next run met entries with no files: Delta failed every read
(DELTA_TABLE_NOT_FOUND) and Iceberg on a Hive catalog failed to load its
missing metadata file, gold and continuous jobs included. The unregister uses
destroy's statements on the deployment's Trino or Spark Thrift pod.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

MAINT = "lakebench.modules.table_formats.iceberg.maintenance"


def _cfg(fmt="iceberg", schema="customer360"):
    from tests.conftest import make_config

    cfg = make_config(name="my-clean")
    cfg.architecture.table_format.type = type(cfg.architecture.table_format.type)(fmt)
    cfg.architecture.workload.schema_type = type(cfg.architecture.workload.schema_type)(schema)
    return cfg


def _run(cfg, layer, bucket, engine, *, exec_error=None, detail=None):
    from lakebench.deploy.unregister import unregister_layer_tables

    sent = []

    def exec_sql(eng, k8s, pod, ns, sql, timeout=30):
        sent.append(sql)
        if exec_error is not None:
            raise exec_error(sql)

    def query_sql(eng, k8s, pod, ns, sql, timeout=30):
        sent.append(sql)
        out = detail(sql) if detail else ""
        if isinstance(out, Exception):
            raise out
        return out

    with (
        patch(f"{MAINT}.find_maintenance_engine", return_value=engine),
        patch(f"{MAINT}.exec_sql", side_effect=exec_sql),
        patch(f"{MAINT}.query_sql", side_effect=query_sql),
    ):
        res = unregister_layer_tables(cfg, layer, bucket, MagicMock())
    return res, sent


def test_trino_unregisters_each_table_of_the_layer_only():
    cfg = _cfg(schema="financial")
    res, sent = _run(cfg, "silver", "my-clean-silver", ("trino", "pod", "lakehouse"))
    assert res.failed == [] and not res.skipped
    assert len(sent) == len(
        cfg.architecture.tables.workload_tables("financial", layers=("silver",))
    )
    assert all(s.startswith("CALL lakehouse.system.unregister_table(") for s in sent)
    assert all("'silver'" in s for s in sent)
    assert "lakehouse.silver.entity_profiles" in res.unregistered


def test_a_table_already_gone_is_not_a_failure():
    cfg = _cfg()
    err = lambda sql: RuntimeError(  # noqa: E731
        "exec_sql failed (rc=1): Query 1 failed: Table 'lakehouse.gold.x' does not exist"
    )
    res, _ = _run(cfg, "gold", "my-clean-gold", ("trino", "pod", "lakehouse"), exec_error=err)
    assert res.failed == [] and res.unregistered == []


def test_a_failed_statement_is_reported():
    cfg = _cfg()
    err = lambda sql: RuntimeError("exec_sql failed (rc=1): Access Denied")  # noqa: E731
    res, _ = _run(cfg, "gold", "my-clean-gold", ("trino", "pod", "lakehouse"), exec_error=err)
    assert [t for t, _ in res.failed] == ["lakehouse.gold.customer_executive_dashboard"]


def test_spark_thrift_iceberg_drops_without_purge():
    res, sent = _run(_cfg(), "silver", "my-clean-silver", ("spark-thrift", "pod", "spark_catalog"))
    assert sent == ["DROP TABLE IF EXISTS spark_catalog.silver.customer_interactions_enriched"]
    assert res.unregistered == ["spark_catalog.silver.customer_interactions_enriched"]


@pytest.mark.parametrize(
    ("detail", "dropped"),
    [
        ("location | s3a://my-clean-silver/warehouse/silver.db/t", True),
        ("location | s3a://someone-else/warehouse/silver.db/t", False),
    ],
)
def test_spark_thrift_delta_drops_only_inside_the_bucket_being_emptied(detail, dropped):
    """DROP deletes a managed Delta table's directory: never outside it."""
    res, sent = _run(
        _cfg("delta"),
        "silver",
        "my-clean-silver",
        ("spark-thrift", "pod", "spark_catalog"),
        detail=lambda sql: detail,
    )
    assert any(s.startswith("DROP TABLE") for s in sent) is dropped
    assert bool(res.unregistered) is dropped
    assert bool(res.kept) is not dropped


def test_spark_thrift_delta_with_its_files_already_gone_is_dropped():
    gone = RuntimeError("[DELTA_PATH_DOES_NOT_EXIST] s3a://my-clean-silver/x doesn't exist")
    res, sent = _run(
        _cfg("delta"),
        "silver",
        "my-clean-silver",
        ("spark-thrift", "pod", "spark_catalog"),
        detail=lambda sql: gone,
    )
    assert res.unregistered and sent[-1].startswith("DROP TABLE")


def test_no_engine_pod_skips_with_a_reason():
    res, sent = _run(_cfg(), "silver", "my-clean-silver", (None, None, None))
    assert res.skipped and sent == []


# -- the clean command ------------------------------------------------------

CFG = (
    "name: my-clean\n"
    "platform:\n"
    "  storage:\n"
    "    s3:\n"
    "      endpoint: http://minio:9000\n"
    "      access_key: k\n"
    "      secret_key: s\n"
    "      buckets:\n"
    "        bronze: my-clean-bronze\n"
    "        silver: my-clean-silver\n"
    "        gold: my-clean-gold\n"
)


def _clean(tmp_path, unregister, target="data"):
    import typer

    from lakebench.cli._clean import clean
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    cfg = tmp_path / "c.yaml"
    cfg.write_text(CFG)
    order = []
    s3 = MagicMock()
    s3._init_error = None
    s3.empty_bucket.side_effect = lambda b, **k: order.append(("empty", b)) or 0
    match = IdentityReport(
        verdict=IdentityVerdict.MATCH, resource_name="x", expected_deployment="my-clean", hint=""
    )

    def fake_unregister(cfg, layer, bucket, k8s):
        order.append(("unregister", bucket))
        return unregister(layer)

    code = None
    with (
        patch("kubernetes.client.CoreV1Api"),
        patch("kubernetes.client.CustomObjectsApi"),
        patch("kubernetes.client.BatchV1Api"),
        patch("lakebench.k8s.get_k8s_client"),
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
        patch("lakebench.deploy.ownership.build_identity_from_config"),
        patch("lakebench.deploy.ownership.verify_bucket_ownership", return_value=match),
        patch("lakebench.s3.S3Client", return_value=s3),
        patch("lakebench.deploy.unregister.unregister_layer_tables", side_effect=fake_unregister),
    ):
        try:
            clean(
                target=target,
                config_file=cfg,
                file_option=None,
                force=True,
                force_legacy=False,
                metrics_dir=Path(tmp_path / "metrics"),
            )
        except typer.Exit as e:
            code = e.exit_code
    return order, code


def test_clean_unregisters_each_layer_before_emptying_it(tmp_path):
    from lakebench.deploy.unregister import LayerUnregister

    order, code = _clean(tmp_path, lambda layer: LayerUnregister(unregistered=[f"t.{layer}"]))
    assert code in (None, 0)
    assert order == [
        ("unregister", "my-clean-bronze"),
        ("empty", "my-clean-bronze"),
        ("unregister", "my-clean-silver"),
        ("empty", "my-clean-silver"),
        ("unregister", "my-clean-gold"),
        ("empty", "my-clean-gold"),
    ]


def test_a_table_left_registered_fails_the_clean(tmp_path):
    from lakebench.deploy.unregister import LayerUnregister

    order, code = _clean(
        tmp_path,
        lambda layer: LayerUnregister(failed=[(f"t.{layer}", "Access Denied")]),
        target="silver",
    )
    assert ("empty", "my-clean-silver") in order
    assert code not in (None, 0)
