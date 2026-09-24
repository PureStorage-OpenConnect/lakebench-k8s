"""The datagen Job carries --cycle/--cycles on multi-cycle runs only, and
--customer-id-max for c360 (WORKPLAN B4, E4a)."""

from __future__ import annotations

from pathlib import Path

import jinja2
import yaml

TEMPLATE = Path(__file__).resolve().parents[1] / "src/lakebench/templates/datagen/job.yaml.j2"


def _args(**ctx):
    env = jinja2.Environment(undefined=jinja2.ChainableUndefined)
    doc = yaml.safe_load(env.from_string(TEMPLATE.read_text()).render(**ctx))
    return doc["spec"]["template"]["spec"]["containers"][0]["args"]


def test_cycle_args_only_when_multi_cycle():
    assert "--cycle" not in _args(datagen_cycles=1, datagen_cycle=0)
    a = _args(datagen_cycles=3, datagen_cycle=2)
    assert a[a.index("--cycle") + 1] == "2"
    assert a[a.index("--cycles") + 1] == "3"


def test_customer_id_max_for_c360_only():
    a = _args(datagen_customer_id_max=100000, datagen_schema="customer360")
    assert a[a.index("--customer-id-max") + 1] == "100000"
    assert "--customer-id-max" not in _args(
        datagen_customer_id_max=100000, datagen_schema="financial"
    )
