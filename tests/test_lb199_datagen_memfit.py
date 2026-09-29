"""LB-199: the datagen peak-RSS model must cover the measured scale-100 peak.

Root cause: the model under-fit the per-thread term AND omitted the per-node
screening (worker) scale term, so financial scale-100 node-0 OOMKilled at the
old 17Gi default (measured working-set peak 18.18 GiB, workers 12.23 GiB;
run-20260929-000406). These tests fail if the coefficients regress below what
the measured cluster peaks require.
"""

import importlib.util
import math
from pathlib import Path

from lakebench.config import autosizer as a

REPO = Path(__file__).resolve().parents[1]

# Authoritative cluster working-set measurements (GiB), s100 / 8 threads / 128MB.
CLUSTER_NODE0_PEAK = 18.18
CLUSTER_WORKER_PEAK = 12.23


def _entrypoint():
    spec = importlib.util.spec_from_file_location(
        "datagen_entrypoint", REPO / "datagen_rs" / "entrypoint.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_scale100_request_covers_measured_node0_peak():
    """The node-0 request (with headroom) must exceed the measured 18.18 GiB."""
    req = math.ceil(a.datagen_memory_gib("financial", 100, 8, 128))
    assert req >= CLUSTER_NODE0_PEAK, req
    # and it must clear the measured peak with real headroom, not sit on it.
    assert req >= 24, f"request {req}Gi too tight over an 18.18 GiB peak"


def test_worker_scale_term_present_for_financial():
    """The missing piece: financial workers carry a per-entity screening term."""
    assert a.DATAGEN_WORKER_ENTITY_BYTES.get("financial", 0) > 0
    # A worker (no world term) must still be sized above the 12.23 GiB peak.
    entities = a.DATAGEN_ENTITIES_PER_SCALE * 100
    worker_pre = (
        a.DATAGEN_BASE_GIB["financial"]
        + entities * a.DATAGEN_WORKER_ENTITY_BYTES["financial"] / 2**30
        + 8 * (128 / 1024) * a.DATAGEN_PER_THREAD_FILE_MULTIPLIER["financial"]
    )
    assert worker_pre * a.DATAGEN_HEADROOM >= CLUSTER_WORKER_PEAK


def test_entrypoint_cap_admits_cpu_threads_at_scale100():
    """The thread cap at the new default must not throttle 8 requested threads."""
    ep = _entrypoint()
    req_gib = math.ceil(a.datagen_memory_gib("financial", 100, 8, 128))
    cap = ep.max_threads_for_memory("financial", 100.0, 128, True, req_gib * 2**30)
    assert cap >= 8, f"cap {cap} < 8 requested threads at scale 100"


def test_c360_model_unchanged():
    """The refit is financial-only; c360 coefficients must not move."""
    assert a.DATAGEN_PER_THREAD_FILE_MULTIPLIER["customer360"] == 3.0
    assert a.DATAGEN_BASE_GIB["customer360"] == 0.3
    assert "customer360" not in a.DATAGEN_WORKER_ENTITY_BYTES
