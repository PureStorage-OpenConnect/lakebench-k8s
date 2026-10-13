"""Spark storage bytes: the executors' counters become the record's io intervals.

``build_spark_io`` sums per-pod cumulative counters over time; ``build_io``
turns the series into intervals and says what was and was not measured.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from lakebench.metrics.io import (
    BASIS_SPARK_DRIVER,
    BASIS_SPARK_READ,
    BASIS_SPARK_UNSEEN,
    BASIS_SPARK_WRITE,
    REASON_EXECUTOR_IO,
    REASON_ICEBERG_READS,
    build_io,
)
from lakebench.modules.pipeline_engines.spark import jmx_exporter
from lakebench.observability.spark_io import build_spark_io, stage_of

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T1 = T0 + timedelta(seconds=60)
_E = T0.timestamp()


def _res(pod: str, points: list[tuple[int, float]]) -> dict:
    return {"metric": {"pod": pod}, "values": [[_E + t, str(v)] for t, v in points]}


def test_stage_comes_from_the_pod_name() -> None:
    assert stage_of("lb-bronze-ingest-financial-00d4-exec-3") == "bronze"
    assert stage_of("lakebench-silver-build-driver") == "silver"
    assert stage_of("lb-gold-refresh-x-exec-1") == "gold"
    assert stage_of("something-else") == "other"


def test_a_finished_pod_keeps_its_last_value_in_the_sum() -> None:
    write = [
        _res("lb-bronze-v-exec-1", [(0, 0), (15, 100), (30, 300), (45, 600), (60, 900)]),
        # exits after 30 s with 50 bytes and never reports again
        _res("lb-bronze-v-exec-2", [(0, 0), (15, 20), (30, 50)]),
    ]
    out = build_spark_io(write, [], T0, T1)
    series = out["stages"]["bronze"]["write"]
    assert [v for _, v in series] == [0.0, 120.0, 350.0, 650.0, 950.0]
    assert out["stages"]["bronze"]["pods"] == 2
    assert out["stages"]["bronze"]["read"][-1][1] == 0.0


def test_no_series_means_no_block() -> None:
    assert build_spark_io([], [], T0, T1) is None
    assert build_spark_io([{"metric": {}, "values": [[_E, "1"]]}], [], T0, T1) is None


def _record(fmt: str, spark_io: dict | None) -> dict:
    rec: dict = {
        "config_snapshot": {"table_format": fmt},
        "pipeline_benchmark": {"stages": [{"stage_name": s} for s in ("bronze", "silver", "gold")]},
    }
    if spark_io is not None:
        rec["platform_metrics"] = {"spark_io": spark_io}
    return rec


def _block() -> dict:
    write = [
        _res("lb-bronze-v-exec-1", [(0, 0), (15, 1000), (30, 3000)]),
        _res("lb-silver-b-exec-1", [(0, 0), (30, 0), (45, 500), (60, 800)]),
    ]
    read = [
        _res("lb-bronze-v-exec-1", [(0, 0), (15, 4000), (30, 9000)]),
        _res("lb-silver-b-exec-1", [(0, 0), (45, 700), (60, 1000)]),
    ]
    out = build_spark_io(write, read, T0, T1)
    assert out is not None
    return out


def test_intervals_carry_each_steps_gain_and_sum_to_the_counters() -> None:
    io = build_io(_record("delta", _block()))
    spark = [i for i in io["intervals"] if i["source"] == "spark"]
    bronze = [i for i in spark if i["stage"] == "bronze"]
    assert sum(i["write_bytes"] for i in bronze) == 3000
    assert sum(i["read_bytes"] for i in bronze) == 9000
    assert {i["write_basis"] for i in spark} == {BASIS_SPARK_WRITE}
    assert {i["read_basis"] for i in spark} == {BASIS_SPARK_READ}
    assert io["totals"]["spark"]["write_bytes"] == 3800
    assert io["totals"]["spark"]["read_bytes"] == 10000
    # only gold, which had no pod, is unmeasured
    gaps = {(n["stage"], n["direction"]): n["reason"] for n in io["not_measured"]}
    assert gaps == {("gold", "read"): REASON_EXECUTOR_IO, ("gold", "write"): REASON_EXECUTOR_IO}


def test_iceberg_table_reads_are_not_measured_never_zero() -> None:
    """Silver reads raw Parquet in a batch run, so its S3A read counter counts;
    a continuous silver reads the bronze table, which Iceberg reads bypass."""
    batch = build_io(_record("iceberg", _block()))
    silver = [i for i in batch["intervals"] if i["source"] == "spark" and i["stage"] == "silver"]
    assert sum(i["read_bytes"] for i in silver) == 1000
    gaps = {(n["stage"], n["direction"]): n["reason"] for n in batch["not_measured"]}
    assert ("silver", "read") not in gaps

    rec = _record("iceberg", _block())
    rec["pipeline_benchmark"]["pipeline_mode"] = "sustained"
    cont = build_io(rec)
    silver = [i for i in cont["intervals"] if i["source"] == "spark" and i["stage"] == "silver"]
    assert silver and all(i["read_bytes"] is None for i in silver)
    assert sum(i["write_bytes"] for i in silver) == 800
    gaps = {(n["stage"], n["direction"]): n["reason"] for n in cont["not_measured"]}
    assert gaps[("silver", "read")] == REASON_ICEBERG_READS
    assert ("silver", "write") not in gaps
    bronze = [i for i in cont["intervals"] if i["stage"] == "bronze" and i["source"] == "spark"]
    assert sum(i["read_bytes"] for i in bronze) == 9000  # raw files over S3A


def test_writes_far_below_what_landed_are_flagged_not_trusted() -> None:
    rec = _record("delta", _block())
    for s in rec["pipeline_benchmark"]["stages"]:
        if s["stage_name"] == "silver":
            s["output_size_gb"] = 2000 / 1024**3  # 800 counted of 2000 landed
        if s["stage_name"] == "bronze":
            s["output_size_gb"] = 3000 / 1024**3  # fully counted
    io = build_io(rec)
    silver = [i for i in io["intervals"] if i["stage"] == "silver" and i["source"] == "spark"]
    bronze = [i for i in io["intervals"] if i["stage"] == "bronze" and i["source"] == "spark"]
    assert silver and not any(i["complete"] for i in silver)
    assert bronze and all(i["complete"] for i in bronze)
    assert any("spark silver" in p and "40%" in p for p in io["problems"])
    assert not any("spark bronze" in p for p in io["problems"])


def test_a_record_with_no_spark_io_is_as_before() -> None:
    io = build_io(_record("iceberg", None))
    assert not [i for i in io["intervals"] if i["source"] == "spark"]
    assert {(n["stage"], n["direction"]) for n in io["not_measured"]} == {
        (s, d) for s in ("bronze", "silver", "gold") for d in ("read", "write")
    }


def test_exporter_is_added_after_the_options_already_there() -> None:
    conf = {"spark.executor.extraJavaOptions": "-XX:+UseG1GC -Djavax.net.ssl.trustStore=/t"}
    tmpl = {
        "spec": {
            "volumes": [{"name": "v"}],
            "containers": [{"name": "spark-kubernetes-executor", "volumeMounts": []}],
        }
    }
    jmx_exporter.wire(conf, tmpl, image="img:1", pull_policy="IfNotPresent")
    opts = conf["spark.executor.extraJavaOptions"]
    assert opts.startswith("-XX:+UseG1GC -Djavax.net.ssl.trustStore=/t ")
    assert opts.endswith(f"-javaagent:{jmx_exporter.JAR}={jmx_exporter.PORT}:{jmx_exporter.CONFIG}")
    assert conf["spark.executor.metrics.fileSystemSchemes"] == "file,s3a,s3"
    assert "org.apache.spark.metrics.sink.JmxSink" in conf["spark.metrics.conf.*.sink.jmx.class"]
    spec = tmpl["spec"]
    assert [v["name"] for v in spec["volumes"]] == ["v", jmx_exporter.VOLUME]
    assert spec["initContainers"][0]["image"] == "img:1"
    assert spec["containers"][0]["volumeMounts"][0]["mountPath"] == jmx_exporter.DIR


@pytest.mark.parametrize("pods", [0, 1])
def test_zero_gain_steps_add_no_interval(pods: int) -> None:
    flat = [_res("lb-bronze-v-exec-1", [(0, 5), (15, 5), (30, 5)])] if pods else []
    out = build_spark_io(flat, flat, T0, T1)
    if out is None:
        return
    io = build_io(_record("delta", out))
    assert not [i for i in io["intervals"] if i["source"] == "spark"]


def _with_driver(rec: dict, stage_job: dict[str, tuple[int, str]]) -> dict:
    """*rec* with the stage windows and the jobs' driver totals."""
    t0 = T0
    for k, s in enumerate(rec["pipeline_benchmark"]["stages"]):
        s["start_time"] = (t0 + timedelta(seconds=20 * k)).isoformat()
        s["end_time"] = (t0 + timedelta(seconds=20 * k + 18)).isoformat()
    names = {"bronze": "bronze-verify", "silver": "silver-build", "gold": "gold-finalize"}
    rec["jobs"] = [
        {
            "job_type": names[stage],
            "extra_metrics": {
                "output_bytes_written": str(total),
                "output_bytes_complete": complete,
            },
        }
        for stage, (total, complete) in stage_job.items()
    ]
    return rec


def test_the_drivers_total_makes_the_stage_exact_and_places_what_executors_missed() -> None:
    rec = _with_driver(_record("delta", _block()), {"silver": (2000, "true")})
    io = build_io(rec)
    silver = [i for i in io["intervals"] if i["source"] == "spark" and i["stage"] == "silver"]
    assert sum(i["write_bytes"] for i in silver) == 2000
    counted = [i for i in silver if i["write_basis"] == BASIS_SPARK_WRITE]
    unseen = [i for i in silver if i["write_basis"] == BASIS_SPARK_UNSEEN]
    assert sum(i["write_bytes"] for i in counted) == 800
    assert [i["write_bytes"] for i in unseen] == [1200]
    # placed in the last seconds of the stage window, not spread over the run
    assert unseen[0]["end"] == rec["pipeline_benchmark"]["stages"][1]["end_time"]
    assert not any("spark silver" in p for p in io["problems"])


def test_with_observability_off_the_drivers_total_still_gives_the_writes() -> None:
    rec = _with_driver(_record("delta", None), {"silver": (5000, "true")})
    io = build_io(rec)
    silver = [i for i in io["intervals"] if i["source"] == "spark" and i["stage"] == "silver"]
    assert len(silver) == 1 and silver[0]["write_bytes"] == 5000
    assert silver[0]["write_basis"] == BASIS_SPARK_DRIVER
    assert silver[0]["read_bytes"] is None
    gaps = {(n["stage"], n["direction"]) for n in io["not_measured"]}
    assert ("silver", "write") not in gaps and ("silver", "read") in gaps


def test_a_total_the_driver_marked_possibly_low_is_not_used() -> None:
    rec = _with_driver(_record("delta", None), {"silver": (5000, "false")})
    io = build_io(rec)
    assert not [i for i in io["intervals"] if i["source"] == "spark"]
    assert ("silver", "write") in {(n["stage"], n["direction"]) for n in io["not_measured"]}


def test_executor_counters_above_the_drivers_total_are_a_problem() -> None:
    rec = _with_driver(_record("delta", _block()), {"silver": (300, "true")})
    io = build_io(rec)
    assert any("spark silver" in p and "exceed" in p for p in io["problems"])
