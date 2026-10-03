"""One data clock for a multi-cycle Customer 360 run (CD-19, C36-1).

Every cycle's silver job gets ``LB_DATA_CLOCK`` = the exclusive end of the
event-time range the run's cycles cover, labelled ``cycle_series_end``.
Before, the ladder took each cycle's bronze-verify clock (the ConfigMap
rewritten every cycle), so one run's rows were anchored to different days.
AML at any cycle count and single-cycle Customer 360 keep the ladder.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from lakebench.spark.job import JobType, SparkJobManager
from tests.test_spark import _make_config, _mock_k8s

pytestmark = pytest.mark.usefixtures("load_script")

TODAY = datetime.now(timezone.utc).date().isoformat()


def _cfg(schema: str, cycles: int, **datagen):
    dg = {"seed": 43} if schema == "financial" else {}
    dg.update(datagen)
    return _make_config(
        architecture={
            "pipeline": {"mode": "batch", "cycles": cycles},
            "workload": {"schema": schema, "datagen": dg},
        }
    )


def _silver_env(cfg, bronze_clock: str | None) -> tuple[str, str]:
    """``(LB_DATA_CLOCK, LB_DATA_CLOCK_SOURCE)`` of the silver-build
    manifest, with the silver-state ConfigMap holding *bronze_clock*."""

    def read(name, namespace, **_kw):
        return SimpleNamespace(data={"bronze_data_clock": bronze_clock or ""})

    with patch("kubernetes.client.CoreV1Api.read_namespaced_config_map", side_effect=read):
        manifest = SparkJobManager(cfg, _mock_k8s())._build_manifest(JobType.SILVER_BUILD)
    env = {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}
    return env["LB_DATA_CLOCK"], env["LB_DATA_CLOCK_SOURCE"]


def test_c360_multicycle_single_clock():
    """The clock is the same whatever bronze-verify wrote for the cycle."""
    cfg = _cfg("customer360", 4)
    clocks = {_silver_env(cfg, day) for day in ("2024-06-30", "2025-03-30", None)}
    assert clocks == {("2025-12-31", "cycle_series_end")}
    explicit = _cfg("customer360", 3, timestamp_start="2024-01-01", timestamp_end="2024-12-31")
    assert _silver_env(explicit, "2024-05-01") == ("2024-12-31", "cycle_series_end")


#: The ladder as it stood before CD-19 (job.py _resolve_silver_data_clock):
#: (datagen window, ConfigMap clock) -> (LB_DATA_CLOCK, source).
LADDER = [
    ({"timestamp_end": "2024-12-31"}, "2024-11-30", ("2024-12-31", "datagen_timestamp_end")),
    ({}, "2024-11-30", ("2024-11-30", "bronze_data_clock")),
    ({"timestamp_start": "2024-02-01"}, None, ("2024-02-01", "datagen_timestamp_start")),
    ({}, None, (TODAY, "fallback_default")),
]


@pytest.mark.parametrize("cycles", [1, 3])
@pytest.mark.parametrize(("window", "bronze", "expected"), LADDER)
def test_aml_silver_clock_unchanged(cycles, window, bronze, expected):
    """AML keeps every rung at any cycle count: the frozen AML silver build
    anchors profile_updated_ts to this clock."""
    assert _silver_env(_cfg("financial", cycles, **window), bronze) == expected


@pytest.mark.parametrize(("window", "bronze", "expected"), LADDER)
def test_single_cycle_c360_keeps_the_ladder(window, bronze, expected):
    assert _silver_env(_cfg("customer360", 1, **window), bronze) == expected


def test_workload_version_names_the_change_and_orders_after_c360_2():
    from lakebench.metrics.compare import _workload_version_number
    from lakebench.metrics.experiment import WORKLOAD_VERSIONS

    assert WORKLOAD_VERSIONS["customer360"] == "c360-2.dev1"
    order = ["c360-1", "c360-2", "c360-2.dev1", "c360-2.dev2", "c360-3"]
    assert sorted(order, key=_workload_version_number) == order
