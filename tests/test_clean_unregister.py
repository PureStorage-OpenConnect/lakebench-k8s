"""`lakebench clean` unregisters a layer's tables before emptying its bucket.

`clean silver|gold|bronze|data` emptied buckets and kept the catalog, so the
next run met entries with no files: Delta failed every read
(DELTA_TABLE_NOT_FOUND) and Iceberg on a Hive catalog failed to load its
missing metadata file, gold and continuous jobs included. The unregister uses
destroy's statements on the deployment's Trino or Spark Thrift pod.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.fixtures.clean_helpers import CFG, verified_namespace

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


def test_no_engine_pod_skips_with_a_reason():
    res, sent = _run(_cfg(), "silver", "my-clean-silver", (None, None, None))
    assert res.skipped and sent == []


# -- the clean command ------------------------------------------------------


def _clean(tmp_path, unregister, target="silver"):
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
        verified_namespace(bucket_report=match),
        patch("kubernetes.client.CustomObjectsApi"),
        patch("kubernetes.client.BatchV1Api"),
        patch("lakebench.k8s.get_k8s_client"),
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
            )
        except typer.Exit as e:
            code = e.exit_code
    return order, code


def test_clean_unregisters_each_layer_before_emptying_it(tmp_path):
    from lakebench.deploy.unregister import LayerUnregister

    for target in ("silver", "gold"):
        order, code = _clean(
            tmp_path, lambda layer: LayerUnregister(unregistered=[f"t.{layer}"]), target=target
        )
        assert code in (None, 0)
        assert order == [("unregister", f"my-clean-{target}"), ("empty", f"my-clean-{target}")]


@pytest.mark.parametrize(
    ("outcome", "emptied"),
    [
        # a table left registered keeps its bucket: emptying it anyway left an
        # entry no engine could drop on a re-run
        ({"failed": [("t.silver", "Access Denied")]}, False),
        # an entry whose files are gone still empties, but the clean fails
        ({"stuck": [("t.silver", "NotFoundException")]}, True),
    ],
)
def test_unregister_problems_fail_the_clean(tmp_path, outcome, emptied):
    from lakebench.deploy.unregister import LayerUnregister

    order, code = _clean(tmp_path, lambda layer: LayerUnregister(**outcome), target="silver")
    assert (("empty", "my-clean-silver") in order) is emptied
    assert order[0] == ("unregister", "my-clean-silver")
    assert code not in (None, 0)


def test_no_engine_pod_empties_with_a_warning(tmp_path):
    from lakebench.deploy.unregister import LayerUnregister

    order, code = _clean(tmp_path, lambda layer: LayerUnregister(skipped="no pod"), target="gold")
    assert ("empty", "my-clean-gold") in order and code in (None, 0)


def test_an_unreadable_delta_location_is_a_failure_not_kept():
    res, sent = _run(
        _cfg("delta"),
        "silver",
        "my-clean-silver",
        ("spark-thrift", "pod", "spark_catalog"),
        detail=lambda sql: RuntimeError("exec_sql failed (rc=1): timed out reading"),
    )
    assert res.failed and not res.kept and not res.may_empty
    assert not any(s.startswith("DROP") for s in sent)


@pytest.mark.parametrize(
    "text",
    [
        "java.io.FileNotFoundException: s3a://my-clean-silver/x (403 Forbidden)",
        "java.lang.ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem",
    ],
)
def test_other_not_found_errors_block_the_bucket(text):
    """Only Iceberg's NotFoundException means the files are gone."""
    res, _ = _run(
        _cfg(),
        "silver",
        "my-clean-silver",
        ("spark-thrift", "pod", "spark_catalog"),
        exec_error=lambda sql: RuntimeError(text),
    )
    assert res.failed and not res.stuck and not res.may_empty


def test_an_iceberg_entry_with_its_metadata_gone_is_stuck_not_blocking():
    err = lambda sql: RuntimeError(  # noqa: E731
        "exec_sql failed (rc=1): org.apache.iceberg.exceptions.NotFoundException: "
        "Failed to open input stream for file: s3a://my-clean-silver/x/metadata/1.metadata.json"
    )
    res, _ = _run(
        _cfg(),
        "silver",
        "my-clean-silver",
        ("spark-thrift", "pod", "spark_catalog"),
        exec_error=err,
    )
    assert res.stuck and res.may_empty


def test_a_stuck_engine_stops_at_the_first_timeout():
    from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

    cfg = _cfg(schema="financial")
    res, sent = _run(
        cfg,
        "gold",
        "my-clean-gold",
        ("trino", "pod", "lakehouse"),
        exec_error=lambda sql: ExecSqlTimeout("exec_sql timed out after 120s"),
    )
    assert len(sent) == 1 and len(res.failed) == 1 and not res.may_empty


_LONG_PREFIX = (
    "query_sql failed (rc=1): Error: org.apache.hive.service.cli.HiveSQLException: "
    "Error running query: org.apache.spark.sql.delta.DeltaAnalysisException: "
)


@pytest.mark.parametrize(
    ("text", "dropped", "kept", "failed"),
    [
        # files already gone, path inside the bucket being emptied: dropped
        ("[DELTA_PATH_DOES_NOT_EXIST] s3a://my-clean-silver/x doesn't exist", True, False, False),
        # the same behind beeline's long prefix, past the reader's cut-off
        (
            _LONG_PREFIX
            + "[DELTA_PATH_DOES_NOT_EXIST] s3a://my-clean-silver/warehouse/silver.db/t "
            "doesn't exist, or is not a Delta table.",
            True,
            False,
            False,
        ),
        # elsewhere or unplaced: DROP would delete a managed directory outside the bucket
        (
            "[DELTA_PATH_DOES_NOT_EXIST] s3a://other-bucket/silver.db/t doesn't exist",
            False,
            True,
            False,
        ),
        ("[DELTA_TABLE_NOT_FOUND] Delta table `silver`.`t` doesn't exist.", False, False, True),
    ],
)
def test_a_logless_delta_entry_is_dropped_only_inside_the_bucket(text, dropped, kept, failed):
    """DROP deletes a managed table's directory even with its log gone."""
    res, sent = _run(
        _cfg("delta"),
        "silver",
        "my-clean-silver",
        ("spark-thrift", "pod", "spark_catalog"),
        detail=lambda sql: RuntimeError(text),
    )
    assert any(x.startswith("DROP TABLE") for x in sent) is dropped
    assert bool(res.unregistered) is dropped
    assert bool(res.kept) is kept and bool(res.failed) is failed
