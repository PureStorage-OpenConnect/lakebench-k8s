"""The datagen Job keeps its pods until a continuous run has read them."""

from __future__ import annotations

import yaml

from lakebench.deploy.datagen import DatagenDeployer
from lakebench.deploy.engine import DeploymentEngine
from tests.conftest import make_config


def _ttl(continuous: bool, run_duration: int = 6480, window: int | None = None) -> int:
    cfg = make_config(
        architecture={"pipeline": {"sustained": {"run_duration": run_duration}}},
    )
    deployer = DatagenDeployer(
        DeploymentEngine(cfg, dry_run=True), continuous=continuous, window_seconds=window
    )
    ctx = deployer._build_datagen_context()
    job = yaml.safe_load(deployer.renderer.render("datagen/job.yaml.j2", ctx))
    return job["spec"]["ttlSecondsAfterFinished"]


def test_batch_datagen_pods_live_an_hour():
    assert _ttl(continuous=False) == 3600


def test_continuous_datagen_pods_outlive_the_configured_window():
    assert _ttl(continuous=True, run_duration=6480) == 6480 + 3600


def test_continuous_datagen_pods_outlive_a_duration_override():
    # `run --duration 10800` on a config whose window is 1800 s.
    assert _ttl(continuous=True, run_duration=1800, window=10800) == 10800 + 3600
