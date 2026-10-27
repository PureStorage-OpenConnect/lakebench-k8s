"""Platform series: per-pod cAdvisor results become sums per pod group over time."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lakebench.observability.platform_series import build_series, group_of

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T1 = T0 + timedelta(seconds=60)
_E = T0.timestamp()


def _res(pod: str, points: dict[int, float], jitter: float = 0.0) -> dict:
    return {
        "metric": {"pod": pod},
        "values": [[_E + t + jitter, str(v)] for t, v in points.items()],
    }


def test_pods_fall_into_their_group_or_are_not_the_workload() -> None:
    # Workload pods map to a stage or component group ...
    for pod, group in [
        ("lb-bronze-ingest-financial-00d4-exec-3", "bronze"),
        ("lakebench-silver-build-driver", "silver"),
        ("lakebench-datagen-2-xyz", "datagen"),
        ("lakebench-trino-worker-1", "engine"),
        ("lakebench-postgres-0", "catalog"),
        ("something-else", "other"),
    ]:
        assert group_of(pod) == group, pod
    # ... while the deployment's own tooling is None (not the workload).
    for pod in (
        "lakebench-prometheus-5d59",
        "lakebench-grafana-x",
        "lakebench-pushgateway-9",
        "lb-deps-78",
    ):
        assert group_of(pod) is None, pod


def test_sums_per_group_and_a_finished_pod_adds_nothing_afterward() -> None:
    cpu = [
        _res("lb-silver-b-exec-1", {0: 1.0, 15: 2.0, 30: 3.0, 45: 3.0, 60: 1.0}),
        _res("lb-silver-b-exec-2", {0: 1.0, 15: 1.0, 30: 1.0}),  # ends after 30 s
        _res("lakebench-trino-worker-0", {0: 0.5, 15: 0.5, 30: 0.5, 45: 0.5, 60: 0.5}),
        _res("lakebench-prometheus-x", {0: 9.0}),  # tooling: left out
    ]
    out = build_series(cpu, cpu, [], [], T0, T1)
    silver = [v for _, v in out["cpu_cores"]["silver"]]
    assert silver == [2.0, 3.0, 4.0, 3.0, 1.0]
    assert [v for _, v in out["cpu_cores"]["engine"]] == [0.5] * 5
    assert "prometheus" not in str(out["cpu_cores"].keys())
    assert [v for _, v in out["pods"]["silver"]] == [2, 2, 2, 1, 1]


def test_series_offsets_and_rounded_timestamps_land_on_their_step() -> None:
    # Prometheus-rounded timestamps still land on their 15 s step ...
    cpu = [_res("lb-gold-x-exec-1", {0: 1.0, 15: 2.0, 30: 3.0}, jitter=0.0004)]
    out = build_series(cpu, [], [], [], T0, T1)
    assert [v for _, v in out["cpu_cores"]["gold"]][:3] == [1.0, 2.0, 3.0]
    # ... and offsets are whole seconds from the start.
    assert [o for o, _ in out["cpu_cores"]["gold"]] == [0.0, 15.0, 30.0, 45.0, 60.0]
    assert out["start"] == T0.isoformat() and out["step_seconds"] == 15


def test_nothing_reported_means_no_block() -> None:
    assert build_series([], [], [], [], T0, T1) is None
    only_tooling = [_res("lakebench-grafana-x", {0: 1.0})]
    assert build_series(only_tooling, only_tooling, [], [], T0, T1) is None
    assert build_series([_res("lb-gold-x-exec-1", {0: 1.0})], [], [], [], T1, T0) is None
