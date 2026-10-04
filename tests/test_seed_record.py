"""LB-229: a run record names a corpus seed in plaintext only when it is a
public development seed (42, the AML calibration seed); any other seed is
its salted reference, a held-out seed also by its role, and a seed that
cannot be checked is withheld. TEST VALUES ONLY: the held-out record is the
test fixture (tests/fixtures/heldout_test.json)."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from lakebench.aml import look_guard as lg
from lakebench.config import datagen_seed as ds
from lakebench.metrics import seed_record as sr
from tests.fixtures import protected_corpus as pc


@pytest.fixture
def held(monkeypatch):
    return pc.use_heldout(monkeypatch)


def _no_seed(text: str, seed: int) -> None:
    assert str(seed) not in text, f"the seed reached {text[:200]!r}"


def test_public_seeds_stay_plaintext(held):
    assert sr.recorded_seed(42) == 42
    assert sr.recorded_seed(43) == 43
    assert sr.recorded_seed(None) is None
    assert sr.seed_label(43) == "43"


def test_a_held_out_seed_is_named_by_role_and_hash(held):
    form = sr.recorded_seed(pc.EV)
    assert form == {"seed_ref": ds.seed_hash(held.salt, pc.EV), "role": "evaluation"}
    _no_seed(json.dumps(form), pc.EV)
    assert lg.recorded_seed_role(form) == "evaluation"
    assert sr.seed_label(form).startswith("evaluation seed (ref ")
    # Idempotent: a recorded form passes through.
    assert sr.recorded_seed(form) is form


def test_an_unprotected_seed_stays_plaintext(held):
    """The record still names its seed (invariant 5) and passes the
    fail-closed readers that need a seed they can check."""
    assert sr.recorded_seed(999) == 999 and sr.recorded_seed("999") == 999
    rec = {"experiment": {"workload": {"name": "financial"}, "corpus": {"seed": 999}}}
    assert lg.protected_record_reason(rec) is None
    assert sr.seed_label(999) == "999"


def test_a_spent_seed_is_a_reference(held):
    form = sr.recorded_seed(pc.SPENT)
    assert form == {"seed_ref": ds.seed_hash(held.salt, pc.SPENT), "role": None}
    _no_seed(json.dumps(form), pc.SPENT)


def test_unreadable_held_out_record_withholds(monkeypatch):
    def gone():
        raise FileNotFoundError(2, "No such file", "heldout_hashes.json")

    monkeypatch.setattr(ds, "_heldout", gone)
    assert sr.recorded_seed(999) == {"seed_ref": None}
    assert sr.recorded_seed(43) == 43  # public: no check needed
    assert sr.seed_label(999) == "withheld"
    rec = {
        "experiment": {"workload": {"name": "financial"}, "corpus": {"seed": {"seed_ref": None}}}
    }
    assert lg.protected_record_reason(rec) == "its seed is withheld"


def test_experiment_inputs_never_record_a_held_out_seed(tmp_path, held):
    from lakebench.config import LoadPurpose, load_config
    from lakebench.metrics.experiment import experiment_inputs

    path = pc.financial_config(tmp_path / "c.yaml", seed=pc.EV, role="evaluation")
    cfg = load_config(path, purpose=LoadPurpose.INSPECT, print_notes=False)
    inputs = experiment_inputs(cfg)
    _no_seed(json.dumps(inputs, default=str), pc.EV)
    corpus = inputs["corpus"]
    assert corpus["seed"]["role"] == "evaluation"
    # The corpus id still hashes the seed: a record of this corpus keeps the id
    # it had before the seed was hidden.
    assert corpus["id"]
    record = {"experiment": {"workload": {"name": "financial"}, "corpus": corpus}}
    assert lg.protected_record_reason(record) == "corpus_role evaluation"
    del corpus["corpus_role"]
    assert lg.protected_record_reason(record) == "its seed is the registered evaluation seed"


def test_fleet_record_never_carries_a_held_out_seed(held):
    from lakebench.metrics.datagen_aggregator import collect_from_pod_logs

    s = collect_from_pod_logs({"a": ""}, pod_args={"a": {"seed": pc.EV}})
    assert s.seed == pc.EV  # in memory, for the mixed-pods check only
    out = s.to_dict()
    _no_seed(json.dumps(out, default=str), pc.EV)
    assert out["seed"]["role"] == "evaluation"


def test_observed_corpus_disagreement_names_no_seed(held):
    from lakebench.metrics.experiment import _observed_corpus

    corpus = {"seed": sr.recorded_seed(pc.EV), "scale": 1.0}
    dg = {"seed": sr.recorded_seed(pc.RB), "observed": True}
    out, problems = _observed_corpus(corpus, dg)
    text = json.dumps([out, problems], default=str)
    _no_seed(text, pc.EV)
    _no_seed(text, pc.RB)
    assert problems == ["the config seed is not the seed the datagen pods ran (values withheld)"]
    # Unprotected seeds still say what differed; a block rebuilt from an
    # older record's plaintext inputs gets the same forms.
    _, problems = _observed_corpus({"seed": 42}, {"seed": 43})
    assert problems == ["config seed 42 but the datagen pods ran 43"]
    out, problems = _observed_corpus({"seed": pc.EV}, {"seed": pc.EV})
    assert problems == [] and out["seed"]["role"] == "evaluation"
    _no_seed(json.dumps(out, default=str), pc.EV)


def test_withheld_forms_never_compare_equal(monkeypatch):
    from lakebench.metrics.experiment import _observed_corpus

    def gone():
        raise FileNotFoundError(2, "No such file", "heldout_hashes.json")

    monkeypatch.setattr(ds, "_heldout", gone)
    _, problems = _observed_corpus({"seed": 999}, {"seed": 1000})
    assert problems == [
        "the config seed and the seed the datagen pods ran cannot be checked "
        "(the held-out record cannot be read)"
    ]


def test_an_old_sidecar_seed_is_recorded_by_the_rule(held):
    """A fleet sidecar written before the rule, read back by run, reaches the
    record through the rule (collector.to_dict)."""
    from datetime import datetime, timezone

    from lakebench.metrics.collector import PipelineMetrics

    m = PipelineMetrics(run_id="r", deployment_name="d", start_time=datetime.now(timezone.utc))
    m.datagen_fleet = {"seed": pc.EV, "scale": 10.0}
    out = m.to_dict()["datagen_fleet"]
    assert out["seed"]["role"] == "evaluation" and out["scale"] == 10.0
    _no_seed(json.dumps(out), pc.EV)


def test_report_shows_the_recorded_form_not_the_seed(held):
    assert sr.seed_label(pc.EV).startswith("evaluation seed (ref ")
    _no_seed(sr.seed_label(pc.EV), pc.EV)
    # A record written before the rule kept the plaintext: the page does not.
    legacy = SimpleNamespace(seed=pc.RB)
    _no_seed(sr.seed_label(legacy.seed), pc.RB)
