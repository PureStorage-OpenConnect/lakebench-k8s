"""Unit tests for the datagen per-pod metrics aggregator."""

from __future__ import annotations

import json

from lakebench.metrics.datagen_aggregator import (
    METRICS_PREFIX,
    FleetSummary,
    PodMetrics,
    collect_from_pod_logs,
    parse_metrics_line,
)


def _emit_line(**fields) -> str:
    """Build one datagen-style log with a single LB_METRICS_JSON line."""
    return "some noise\n" + METRICS_PREFIX + json.dumps(fields) + "\ntrailing noise"


def _c360_pod(
    node_id: int,
    elapsed: float,
    bytes_w: int,
    cores: int = 8,
    cpu_request_millicores: int | None = None,
    rows_written: int | None = None,
) -> dict:
    obj = {
        "schema": "customer360",
        "node_id": node_id,
        "node_count": 8,
        "cores_used": cores,
        "bucket": "lb-bronze",
        "prefix": "customer/interactions/",
        "target_tb": 0.5,
        "customer_id_max": 500000,
        "dirty_ratio": 0.08,
        "file_size_mb": 64,
        "rows_per_file": 100_000,
        "total_files": 400,
        "files_written": 50,
        "bytes_written": bytes_w,
        "rows_written": 100_000 * 50 if rows_written is None else rows_written,
        "elapsed_s": elapsed,
        "setup_s": 0.4,
        "gen_s": elapsed - 0.4,
        "build_batch_s": elapsed * cores * 0.55,
        "encode_parquet_s": elapsed * cores * 0.4,
        "s3_put_s": elapsed * cores * 0.05,
        "throughput_mbps": bytes_w / elapsed / 1e6 if elapsed > 0 else 0,
        "cpu_seconds": elapsed * cores,
        "cpu_hr_per_tb": (elapsed * cores / 3600) / (bytes_w / 1e12) if bytes_w > 0 else 0,
    }
    if cpu_request_millicores is not None:
        obj["cpu_request_millicores"] = cpu_request_millicores
    return obj


def test_parse_metrics_line_finds_last_when_multiple():
    log = _emit_line(schema="a", node_id=0) + "\n" + _emit_line(schema="b", node_id=1)
    obj = parse_metrics_line(log)
    assert obj is not None
    assert obj["schema"] == "b"


def test_parse_metrics_line_returns_none_when_absent():
    assert parse_metrics_line("nothing here\nno metrics") is None


def test_parse_metrics_line_tolerates_malformed_json():
    log = f"{METRICS_PREFIX}not json\n{METRICS_PREFIX}" + json.dumps(
        {"schema": "c360", "node_id": 0}
    )
    obj = parse_metrics_line(log)
    assert obj is not None
    assert obj["schema"] == "c360"


def test_pod_metrics_from_json_ignores_unknown_fields():
    obj = _c360_pod(0, 100.0, 1_000_000_000)
    obj["future_field_we_dont_know"] = 42
    m = PodMetrics.from_json_obj(obj, pod_name="pod-0")
    assert m.schema == "customer360"
    assert m.node_id == 0


def test_collect_from_pod_logs_c360_fleet_math():
    """Two pods, one slow, one fast. Aggregate MB/s uses the slowest as
    the gate; sum of throughputs would overstate."""
    logs = {
        "pod-0": _emit_line(**_c360_pod(0, elapsed=100.0, bytes_w=50_000_000_000)),
        "pod-1": _emit_line(**_c360_pod(1, elapsed=120.0, bytes_w=60_000_000_000)),
    }
    fleet = collect_from_pod_logs(logs, expected_pods=2)
    assert fleet.pods_reported == 2
    assert fleet.pods_missing == 0
    assert fleet.total_bytes_written == 110_000_000_000
    assert fleet.wall_elapsed_max_s == 120.0
    assert fleet.wall_elapsed_min_s == 100.0
    # 110 GB / 120 s = 916.7 MB/s aggregate. Sum of per-pod throughputs
    # would be ~1000 MB/s -- confirm we did NOT compute that.
    assert 900 < fleet.aggregate_mbps < 930
    assert fleet.cores_total == 16
    # 16 cores * 120s? No -- sum of per-pod cpu_seconds = 8*100 + 8*120 = 1760
    assert abs(fleet.cpu_seconds_total - (8 * 100 + 8 * 120)) < 1e-3
    # CPU-hr/TB = 1760 / 3600 / 0.110 = ~4.44
    assert 4.2 < fleet.cpu_hr_per_tb < 4.7


def test_collect_from_pod_logs_missing_pods_counted():
    logs = {
        "pod-0": _emit_line(**_c360_pod(0, elapsed=50.0, bytes_w=1_000_000_000)),
        "pod-1": "died silently before emitting",
        "pod-2": "",
    }
    fleet = collect_from_pod_logs(logs, expected_pods=3)
    assert fleet.pods_reported == 1
    assert fleet.pods_missing == 2


def test_collect_from_pod_logs_empty_returns_zeroed_fleet():
    fleet = collect_from_pod_logs({}, expected_pods=8)
    assert fleet.pods_reported == 0
    assert fleet.pods_missing == 8
    assert fleet.total_bytes_written == 0
    assert fleet.cpu_hr_per_tb is None


def test_phase_pct_build_plus_encode_is_100():
    """phase_pct denominator is CPU-cost total = build + encode (matches
    the Rust emit). s3_put is I/O wait, not CPU cost, so it reports as an
    extra ratio over the same denominator rather than a share of 100."""
    logs = {
        "pod-0": _emit_line(**_c360_pod(0, elapsed=100.0, bytes_w=10_000_000_000)),
        "pod-1": _emit_line(**_c360_pod(1, elapsed=110.0, bytes_w=11_000_000_000)),
    }
    fleet = collect_from_pod_logs(logs, expected_pods=2)
    assert abs(fleet.phase_pct["build_batch"] + fleet.phase_pct["encode_parquet"] - 100.0) < 0.01
    # s3_put is small but non-zero given our synthetic ratios.
    assert fleet.phase_pct["s3_put"] > 0


def test_fleet_summary_to_dict_stable_shape():
    fleet = FleetSummary(
        schema="customer360",
        pods_expected=1,
        pods_reported=1,
        pods_missing=0,
        data_quality="complete",
        total_bytes_written=1_000_000_000,
        total_files_written=10,
        total_rows_written=1_000_000,
        aggregate_mbps=100.0,
        wall_elapsed_max_s=10.0,
        wall_elapsed_min_s=10.0,
        cores_total=4.0,
        cpu_seconds_total=40.0,
        cpu_hr_per_tb=11.11,
        phase_pct={"build_batch": 55.0, "encode_parquet": 40.0, "s3_put": 5.0},
        phase_p50_s={"build_batch": 22.0, "encode_parquet": 16.0, "s3_put": 2.0},
        phase_p95_s={"build_batch": 22.0, "encode_parquet": 16.0, "s3_put": 2.0},
        worst_pod_elapsed_s=10.0,
        best_pod_elapsed_s=10.0,
        per_pod=[],
    )
    d = fleet.to_dict()
    # Round-trip through JSON to confirm no non-serializable types crept in.
    json.dumps(d)
    assert d["schema"] == "customer360"
    assert d["pods_reported"] == 1
    assert d["aggregate_mbps"] == 100.0
    assert d["data_quality"] == "complete"
    assert set(d["phase_pct"].keys()) == {"build_batch", "encode_parquet", "s3_put"}


def test_row_total_uses_rows_written_not_total_txns():
    """Financial: total_txns is a corpus-wide constant on every pod. The
    aggregator must NEVER sum it; it must sum rows_written instead.
    """

    # Two financial pods, each reporting total_txns=100_000_000 (the same
    # corpus number). Each pod actually wrote 50_000_000 rows.
    def _pod(node_id: int) -> dict:
        return {
            "schema": "financial",
            "node_id": node_id,
            "node_count": 2,
            "cores_used": 8,
            "bucket": "b",
            "prefix": "",
            "file_size_mb": 32,
            "rows_per_file": 1_000_000,
            "total_files": 100,
            "files_written": 50,
            "bytes_written": 1_600_000_000,
            "rows_written": 50_000_000,
            "elapsed_s": 60.0,
            "setup_s": 1.0,
            "gen_s": 59.0,
            "build_batch_s": 200.0,
            "encode_parquet_s": 200.0,
            "s3_put_s": 20.0,
            "throughput_mbps": 26.6,
            "cpu_seconds": 480.0,
            "cpu_hr_per_tb": 83.3,
            "total_txns": 100_000_000,  # corpus-wide; must NOT be summed
            "scale": 1.0,
            "corpus_months": 60,
            "population": 1_000_000,
        }

    logs = {
        "pod-0": _emit_line(**_pod(0)),
        "pod-1": _emit_line(**_pod(1)),
    }
    fleet = collect_from_pod_logs(logs, expected_pods=2)
    # Correct: 50M + 50M = 100M.
    # Buggy old code would have summed total_txns: 100M + 100M = 200M.
    assert fleet.total_rows_written == 100_000_000


def test_cpu_request_millicores_preferred_over_rayon_pool():
    """K8s reserved 16 cores per pod but rayon reports 8. Cost accounting
    must use the k8s request."""
    logs = {
        "pod-0": _emit_line(
            **_c360_pod(0, 100.0, 100_000_000_000, cores=8, cpu_request_millicores=16000)
        ),
    }
    fleet = collect_from_pod_logs(logs, expected_pods=1)
    # cpu_seconds_total from effective_cores: 16 * 100 = 1600, NOT 800.
    assert abs(fleet.cpu_seconds_total - 1600.0) < 0.01
    assert fleet.cores_total == 16.0
    # cpu_hr_per_tb = (1600/3600) / 0.1 = 4.44
    assert 4.4 < fleet.cpu_hr_per_tb < 4.5


def test_partial_fleet_flags_data_quality():
    logs = {
        "pod-0": _emit_line(**_c360_pod(0, 100.0, 10_000_000_000)),
    }
    fleet = collect_from_pod_logs(logs, expected_pods=4)
    assert fleet.data_quality == "partial"
    assert fleet.pods_missing == 3


def test_complete_fleet_marked_complete():
    logs = {
        "pod-0": _emit_line(**_c360_pod(0, 100.0, 10_000_000_000)),
        "pod-1": _emit_line(**_c360_pod(1, 100.0, 10_000_000_000)),
    }
    fleet = collect_from_pod_logs(logs, expected_pods=2)
    assert fleet.data_quality == "complete"


def test_rows_written_backfilled_from_old_logs():
    """Older datagen builds emitted no rows_written; the parser must
    backfill from files_written * rows_per_file so we can still parse
    them without KeyError."""
    obj = _c360_pod(0, 100.0, 10_000_000_000)
    del obj["rows_written"]
    m = PodMetrics.from_json_obj(obj)
    assert m.rows_written == 100_000 * 50
