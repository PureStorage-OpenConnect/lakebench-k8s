"""Datagen memory model (LB-199, re-fit for LB-204): it must cover every
measured cluster peak, stay within the 16 GiB pod cap across each workload's
scale band, and agree with the band table in config/support.py.

Measured points are the cgroup memory.peak of the busiest pod of datagen-only
cluster Jobs (8 threads, 64 MB files, no thread cap), in
autosizer.DATAGEN_MEASURED_PEAK_GIB. History: the pre-LB-204 generator peaked
at 18.18 GiB (node 0) at financial scale 100 and OOMKilled at 17Gi.
"""

import importlib.util
import math
from pathlib import Path

import pytest

from lakebench.config import autosizer as a
from lakebench.config.support import DATAGEN_POD_MEMORY_CAP_GIB, DATAGEN_SCALE_BANDS

REPO = Path(__file__).resolve().parents[1]
SCHEMAS = sorted(a.DATAGEN_MEASURED_PEAK_GIB)


def _entrypoint():
    spec = importlib.util.spec_from_file_location(
        "datagen_entrypoint", REPO / "datagen_rs" / "entrypoint.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("schema", SCHEMAS)
def test_model_covers_every_measured_peak(schema):
    """Upper envelope: the modelled peak (no headroom) is at or above every
    measured busiest-pod peak."""
    for scale, measured in a.DATAGEN_MEASURED_PEAK_GIB[schema].items():
        assert a.datagen_peak_gib(schema, scale, 8) >= measured, (schema, scale)


def test_bands_cover_the_measured_workloads():
    assert set(DATAGEN_SCALE_BANDS) == set(a.DATAGEN_MEASURED_PEAK_GIB)


@pytest.mark.parametrize("schema", SCHEMAS)
def test_supported_max_is_measured_and_fits_the_cap(schema):
    """supported_max is a measured scale whose measured peak, with headroom,
    fits the pod cap."""
    supported_max, _ = DATAGEN_SCALE_BANDS[schema]
    measured = a.DATAGEN_MEASURED_PEAK_GIB[schema]
    assert int(supported_max) in measured
    assert measured[int(supported_max)] * a.DATAGEN_HEADROOM <= DATAGEN_POD_MEMORY_CAP_GIB


@pytest.mark.parametrize("schema", SCHEMAS)
def test_ceiling_fits_the_cap_and_is_bounded_by_measurement(schema):
    """The ceiling's modelled request fits 16 GiB at 8 threads, and the
    ceiling never extrapolates past twice the largest measured scale."""
    _, ceiling = DATAGEN_SCALE_BANDS[schema]
    assert a.datagen_memory_gib(schema, ceiling, 8) <= DATAGEN_POD_MEMORY_CAP_GIB
    assert ceiling <= 2 * max(a.DATAGEN_MEASURED_PEAK_GIB[schema])


@pytest.mark.parametrize("schema", SCHEMAS)
def test_every_scale_up_to_the_ceiling_runs_8_threads_within_the_cap(schema):
    """At the 8 CPU default, every scale up to the ceiling gets a request of at
    most 16 GiB, and that request admits all 8 threads (no entrypoint cut)."""
    ep = _entrypoint()
    _, ceiling = DATAGEN_SCALE_BANDS[schema]
    for scale in (1, 10, 100, ceiling / 2, ceiling):
        req = min(
            DATAGEN_POD_MEMORY_CAP_GIB, max(4, math.ceil(a.datagen_memory_gib(schema, scale, 8)))
        )
        assert ep.max_threads_for_memory(schema, float(scale), req * 2**30) >= 8, (schema, scale)


def test_fewer_threads_never_lower_the_estimate():
    """Below 8 threads nothing was measured, so the model keeps the 8-thread peak."""
    for schema in SCHEMAS:
        assert a.datagen_peak_gib(schema, 100, 2) == a.datagen_peak_gib(schema, 100, 8)
        assert a.datagen_peak_gib(schema, 100, 16) > a.datagen_peak_gib(schema, 100, 8)


def test_default_request_is_capped_at_16gi():
    """A 32-CPU override at the financial ceiling models above 16 GiB; the
    request is still the cap and the entrypoint runs fewer threads."""
    from lakebench.config import LakebenchConfig

    _, ceiling = DATAGEN_SCALE_BANDS["financial"]
    assert a.datagen_memory_gib("financial", ceiling, 32) > DATAGEN_POD_MEMORY_CAP_GIB
    cfg = LakebenchConfig(
        name="cap",
        platform={
            "storage": {"s3": {"endpoint": "http://s3:80", "access_key": "k", "secret_key": "s"}}
        },
        workload={"schema": "financial", "datagen": {"scale": ceiling}},
    )
    assert a._datagen_memory_default(cfg, "32") == f"{DATAGEN_POD_MEMORY_CAP_GIB}Gi"


def _cfg(schema, scale, **datagen):
    from lakebench.config import LakebenchConfig

    return LakebenchConfig(
        name="floor",
        platform={
            "storage": {"s3": {"endpoint": "http://s3:80", "access_key": "k", "secret_key": "s"}}
        },
        workload={"schema": schema, "datagen": {"scale": scale, **datagen}},
    )


def test_financial_above_scale_100_runs_at_least_8_pods():
    """Each pod keeps typology payloads for its own files only, so fewer pods
    means more memory per pod; the model was measured at 8 or more pods."""
    cfg = _cfg("financial", 300, parallelism=4)
    changes: list[str] = []
    a._apply_datagen_pod_floor(cfg, changes)
    assert cfg.architecture.workload.datagen.parallelism == a.DATAGEN_MIN_PODS
    assert changes and changes[0].startswith("datagen.parallelism raised")


@pytest.mark.parametrize(("schema", "scale"), [("financial", 100), ("customer360", 300)])
def test_pod_floor_leaves_small_scale_and_c360_alone(schema, scale):
    cfg = _cfg(schema, scale, parallelism=4)
    changes: list[str] = []
    a._apply_datagen_pod_floor(cfg, changes)
    assert cfg.architecture.workload.datagen.parallelism == 4
    assert changes == []


def test_clamp_note_explains_thread_cut():
    _, ceiling = DATAGEN_SCALE_BANDS["financial"]
    note = a._datagen_clamp_note(_cfg("financial", ceiling), "32")
    assert f"capped at {DATAGEN_POD_MEMORY_CAP_GIB}Gi" in note
    assert a._datagen_clamp_note(_cfg("financial", 100), "8") == ""
