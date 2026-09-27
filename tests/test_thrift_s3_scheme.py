"""Spark Thrift maps the s3:// scheme to S3A, as the Spark jobs do.

Polaris hands out s3:// table locations. Without fs.s3.impl the Thrift
server's remove_orphan_files failed on every table with 'No FileSystem for
scheme "s3"' (lb16 sweep, polaris-iceberg-spark-thrift).
"""

from __future__ import annotations

import pytest

from tests.test_lb148_thrift_delta import _conf, _container, _render_thrift, _thrift_cfg

S3A = "org.apache.hadoop.fs.s3a.S3AFileSystem"


@pytest.mark.parametrize(
    "recipe",
    ["polaris-iceberg-spark-thrift", "hive-iceberg-spark-thrift", "hive-delta-spark-thrift"],
)
def test_thrift_maps_s3_scheme_to_s3a(recipe):
    conf = _conf(_container(_render_thrift(_thrift_cfg(recipe))))
    assert conf["spark.hadoop.fs.s3a.impl"] == S3A
    assert conf["spark.hadoop.fs.s3.impl"] == S3A


def test_thrift_scheme_mapping_matches_spark_jobs():
    """Every fs.<scheme>.impl the Spark jobs set is also set on Thrift."""
    from lakebench.modules.pipeline_engines.spark import job as job_mod

    src = open(job_mod.__file__).read()
    job_keys = {
        k
        for k in ("spark.hadoop.fs.s3.impl", "spark.hadoop.fs.AbstractFileSystem.s3.impl")
        if f'"{k}"' in src
    }
    assert "spark.hadoop.fs.s3.impl" in job_keys
    conf = _conf(_container(_render_thrift(_thrift_cfg("polaris-iceberg-spark-thrift"))))
    for key in job_keys:
        assert key in conf, key


def test_thrift_script_has_no_broken_continuation():
    """The Jinja comment beside the mapping must not join or break lines."""
    doc = _render_thrift(_thrift_cfg("polaris-iceberg-spark-thrift"))
    script = _container(doc)["command"][-1]
    for line in script.splitlines():
        assert "{#" not in line and "#}" not in line
        assert line.strip() != "", "blank line would end the shell continuation"
