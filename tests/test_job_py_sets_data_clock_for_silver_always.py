"""C2: job.py's silver env bundle sets LB_DATA_CLOCK unconditionally, using
this resolution order:

  1. datagen.timestamp_end (if set) -> data_clock_source=datagen_timestamp_end
  2. bronze-side max(event_ts) written by bronze-verify to the deployment
     ConfigMap ``lakebench-silver-state`` -> data_clock_source=bronze_data_clock
  3. datagen.timestamp_start (if set) -> data_clock_source=datagen_timestamp_start
  4. today at 00:00 UTC -> data_clock_source=fallback_default

The Kubernetes ConfigMap read is mocked so this test does not touch a live
cluster.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.test_spark import _make_config, _mock_k8s

_SILVER_JOB_TYPES = (JobType.SILVER_BUILD, JobType.SILVER_STREAM)


def _env(config, job_type, cm_bronze_clock=None):
    """Return the driver-pod env-var dict for one silver job, with the
    ``lakebench-silver-state`` ConfigMap read patched to return
    ``cm_bronze_clock`` (or 404 when None).
    """
    from kubernetes.client.exceptions import ApiException

    def fake_read_cm(name, namespace):
        if name == "lakebench-silver-state" and cm_bronze_clock is not None:

            class CM:
                data = {"bronze_data_clock": cm_bronze_clock}

            return CM()
        raise ApiException(status=404)

    with patch(
        "kubernetes.client.CoreV1Api.read_namespaced_config_map",
        side_effect=fake_read_cm,
    ):
        manifest = SparkJobManager(config, _mock_k8s())._build_manifest(job_type)
    return {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}


def test_silver_env_uses_datagen_timestamp_end_when_set():
    """Priority 1: datagen.timestamp_end wins over ConfigMap and fallbacks."""
    cfg = _make_config(
        architecture={
            "workload": {
                "datagen": {
                    "timestamp_start": "2024-01-01",
                    "timestamp_end": "2025-07-01",
                }
            }
        }
    )
    for jt in _SILVER_JOB_TYPES:
        env = _env(cfg, jt, cm_bronze_clock="2024-12-31")
        assert env["LB_DATA_CLOCK"] == "2025-07-01", jt
        assert env["LB_DATA_CLOCK_SOURCE"] == "datagen_timestamp_end", jt


def test_silver_env_uses_bronze_data_clock_when_timestamp_end_absent():
    """Priority 2: without a configured end, use the value bronze-verify
    wrote to the ConfigMap (max(event_ts) of the verified bronze)."""
    cfg = _make_config(
        architecture={
            "workload": {"datagen": {"timestamp_start": "2024-01-01"}},
        }
    )
    for jt in _SILVER_JOB_TYPES:
        env = _env(cfg, jt, cm_bronze_clock="2025-03-15")
        assert env["LB_DATA_CLOCK"] == "2025-03-15", jt
        assert env["LB_DATA_CLOCK_SOURCE"] == "bronze_data_clock", jt


def test_silver_env_falls_back_to_timestamp_start():
    """Priority 3: no end, no bronze clock -> use timestamp_start."""
    cfg = _make_config(architecture={"workload": {"datagen": {"timestamp_start": "2024-01-01"}}})
    for jt in _SILVER_JOB_TYPES:
        env = _env(cfg, jt, cm_bronze_clock=None)
        assert env["LB_DATA_CLOCK"] == "2024-01-01", jt
        assert env["LB_DATA_CLOCK_SOURCE"] == "datagen_timestamp_start", jt


def test_silver_env_fallback_to_today_when_nothing_configured():
    """Priority 4: greenfield with nothing configured -> today at 00:00 UTC,
    labelled fallback_default so metrics.json records the choice."""
    cfg = _make_config()
    today = datetime.now(timezone.utc).date().isoformat()
    for jt in _SILVER_JOB_TYPES:
        env = _env(cfg, jt, cm_bronze_clock=None)
        assert env["LB_DATA_CLOCK"] == today, jt
        assert env["LB_DATA_CLOCK_SOURCE"] == "fallback_default", jt


def test_bronze_verify_env_never_carries_fallback_label():
    """C2 is silver-only for the resolution ladder: bronze-verify continues
    to see LB_DATA_CLOCK only when it is directly configured
    (``datagen.timestamp_end``). Bronze-verify computes ``bronze_data_clock``
    itself; it never consumes the fallback label."""
    cfg = _make_config()
    env = _env(cfg, JobType.BRONZE_VERIFY, cm_bronze_clock=None)
    # Bronze-verify does not carry the silver-only source label.
    assert "LB_DATA_CLOCK_SOURCE" not in env


def test_configmap_read_failure_does_not_break_silver_env(monkeypatch):
    """A transient ConfigMap read failure (5xx, connection error) must not
    prevent silver from getting a clock. The resolver falls through to
    timestamp_start / today and labels the source accordingly."""
    from kubernetes.client.exceptions import ApiException

    cfg = _make_config(architecture={"workload": {"datagen": {"timestamp_start": "2024-01-01"}}})

    def _raise(*_a, **_k):
        raise ApiException(status=500)

    with patch(
        "kubernetes.client.CoreV1Api.read_namespaced_config_map",
        side_effect=_raise,
    ):
        manifest = SparkJobManager(cfg, _mock_k8s())._build_manifest(JobType.SILVER_BUILD)
    env = {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}
    assert env["LB_DATA_CLOCK"] == "2024-01-01"
    assert env["LB_DATA_CLOCK_SOURCE"] == "datagen_timestamp_start"


def test_resolution_never_omits_lb_data_clock_across_matrix():
    """Belt-and-braces: LB_DATA_CLOCK is present in EVERY silver env
    combination this test parameterises, including a bare config."""
    configs = [
        _make_config(),
        _make_config(architecture={"workload": {"datagen": {"timestamp_end": "2025-07-01"}}}),
        _make_config(architecture={"workload": {"datagen": {"timestamp_start": "2024-01-01"}}}),
    ]
    for cfg in configs:
        for jt in _SILVER_JOB_TYPES:
            env = _env(cfg, jt, cm_bronze_clock=None)
            assert "LB_DATA_CLOCK" in env, (jt, cfg)
            # Parse guard: the emitted clock is a valid ISO date.
            date.fromisoformat(env["LB_DATA_CLOCK"])
            assert "LB_DATA_CLOCK_SOURCE" in env
