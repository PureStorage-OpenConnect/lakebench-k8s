"""Delta table-health file counts (lb16 sweep: every Delta probe logged
"no count in output", so pre/post file counts were never recorded)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from lakebench.deploy.delta_maintenance import parse_describe_detail
from tests.conftest import make_config

DETAIL = """\
+---------+---------------------------------------+--------------------------------+-------------+-----------+----------+
| format  |                  id                   |              name              | description | numFiles  | sizeInBytes |
+---------+---------------------------------------+--------------------------------+-------------+-----------+----------+
| delta   | 4c2c7a5e-7b8e-4d7e-9a3c-1f2e3d4c5b6a  | spark_catalog.silver.enriched  | NULL        | 984       | 123456   |
+---------+---------------------------------------+--------------------------------+-------------+-----------+----------+
"""


def test_parse_describe_detail_reads_num_files():
    assert parse_describe_detail(DETAIL) == 984
    assert parse_describe_detail(DETAIL, "sizeInBytes") == 123456


def test_parse_describe_detail_without_the_column_is_none():
    assert parse_describe_detail("| a | b |\n| 1 | 2 |\n") is None
    assert parse_describe_detail("") is None
    assert parse_describe_detail("| numFiles |\n| NULL |\n") is None


def _probe(recipe, stdout):
    from lakebench.cli._sustained import _probe_table_health

    cfg = make_config(recipe=recipe, architecture={"workload": {"schema": "customer360"}})
    engine = "trino" if recipe.endswith("trino") else "spark-thrift"
    with (
        patch(
            "lakebench.deploy.iceberg.find_maintenance_engine",
            return_value=(engine, "pod-0", "spark_catalog"),
        ),
        patch("lakebench.deploy.iceberg.query_sql", return_value=stdout) as q,
    ):
        return _probe_table_health(cfg, MagicMock()), q


def test_delta_thrift_probe_records_file_counts():
    health, q = _probe("hive-delta-spark-thrift", DETAIL)
    assert health == {"silver_data_file_count": 984, "gold_data_file_count": 984}
    assert all("DESCRIBE DETAIL" in c.args[4] for c in q.call_args_list)


def test_delta_trino_probe_says_the_count_is_unavailable(caplog):
    with caplog.at_level(logging.WARNING):
        health, q = _probe("hive-delta-spark-trino", "")
    assert health == {}
    q.assert_not_called()
    assert "unavailable on trino" in caplog.text
    assert "no count in output" not in caplog.text


def test_delta_thrift_unparseable_output_says_unavailable(caplog):
    with caplog.at_level(logging.WARNING):
        health, _ = _probe("hive-delta-spark-thrift", "garbage")
    assert health == {}
    assert "count unavailable" in caplog.text
