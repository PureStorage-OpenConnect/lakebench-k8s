"""Executed: a c360 batch silver rebuild keeps what it rebuilt, and a row
probe that fails does not let a populated table be rebuilt.

A later cycle of a multi-cycle run that finds no silver table rebuilds it.
On Iceberg every rebuilt row was tagged with that cycle's ``_batch_id``, so
an operator retry, which finds the table and deletes the cycle's rows before
re-appending them, deleted the whole rebuild and kept only that cycle. The
rebuild also read every file under the bronze prefix, including files an
earlier, longer run left in the bucket. And a failed ``limit(1)`` probe of
an existing table counted as "empty", so a populated table was rebuilt
without --force-rebuild.

Runs ``silver_rebuild_scenarios.py`` in fresh JVMs with the Iceberg and
Delta jars from ``LB_SPARK_TEST_JARS`` on the classpath.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")

THIS_RUN = {"0:0": 9, "0:1": 9, "0:2": 9}


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("silver-rebuild")
    script = Path(__file__).with_name("silver_rebuild_scenarios.py")
    proc = spark_subprocess(script, spark_jars.classpath, work, timeout=2400)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _why(case):
    return json.dumps([{k: e[k] for k in ("cycle", "rc")} for e in case["log"]]) + (
        "\n" + case["log"][-1]["tail"]
    )


@pytest.mark.parametrize("case", ["iceberg_simple", "iceberg_streaming", "delta_simple"])
def test_later_cycle_rebuild_reads_only_this_runs_files(result, case):
    c = result[case]
    assert c["rcs"] == [0, 0] and c["rebuild_rc"] == 0, _why(c)
    assert c["held_after_rebuild"] == THIS_RUN, _why(c)


@pytest.mark.parametrize("case", ["iceberg_simple", "iceberg_streaming", "delta_simple"])
def test_retry_of_a_later_cycle_rebuild_keeps_it(result, case):
    c = result[case]
    assert c["retry_rc"] == 0, _why(c)
    assert c["held_after_retry"] == THIS_RUN, _why(c)


@pytest.mark.parametrize("case", ["iceberg_simple", "iceberg_streaming", "delta_simple"])
def test_next_cycle_appends_after_the_rebuild(result, case):
    c = result[case]
    assert c["next_rc"] == 0, _why(c)
    assert c["held_after_next"] == {**THIS_RUN, "0:3": 9}, _why(c)


@pytest.mark.parametrize("case", ["probe_iceberg", "probe_delta"])
def test_failed_row_probe_refuses_instead_of_rebuilding(result, case):
    c = result[case]
    assert c["rcs"] == [0] and c["dropped"] > 0, _why(c)
    assert c["rc"] != 0, _why(c)
    assert c["refused"], _why(c)


@pytest.mark.parametrize("case", ["probe_iceberg", "probe_delta"])
def test_failed_row_probe_with_force_rebuild_rebuilds(result, case):
    c = result[case]
    assert c["forced_rc"] == 0, _why(c)
    assert c["held_after_forced"] == {"1:0": 9}, _why(c)


def test_unreadable_iceberg_metadata_is_not_a_missing_table(result):
    """The existence check fails; no rebuild commit is written over it.

    Also true before the existence check failed closed, on this hadoop
    catalog: createOrReplace then failed to load the same metadata. It pins
    that an unreadable table is never rebuilt over."""
    c = result["metadata_iceberg"]
    assert c["rcs"] == [0], _why(c)
    assert c["rc"] != 0, _why(c)
    assert c["metadata_after"] == c["metadata_before"], _why(c)


def test_clean_silver_then_run_builds_afresh(result):
    """The catalog entry outlived its files; it was dropped, not read."""
    c = result["cleaned_delta"]
    assert c["rcs"] == [[0, 0], [0, 0]], _why(c)
    assert c["held"] == {"1:0": 9, "1:1": 9}, _why(c)


def test_logless_entry_with_files_left_refuses_and_keeps_them(result):
    c = result["logless_delta"]
    assert c["rcs"] == [0], _why(c)
    assert c["rc"] != 0 and c["refused"], _why(c)
    assert c["files_kept"], _why(c)
