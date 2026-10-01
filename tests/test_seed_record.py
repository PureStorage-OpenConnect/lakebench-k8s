"""Run output names a held-out seed only by its salted hash (LB-229).

SPEC release success 5: no future look's seed in plaintext in any output.
``datagen_seed.record_seed`` is the one rule; metrics.json's experiment
block and the datagen fleet record call it. The seeds here are the test
values of ``tests/fixtures/heldout_test_seeds.py`` (never real held-out
seeds), and no assertion prints one.
"""

from __future__ import annotations

import json

import pytest

from lakebench.config import datagen_seed as ds
from lakebench.metrics.datagen_aggregator import collect_from_pod_logs
from tests.fixtures import heldout_test_seeds as ts
from tests.test_experiment import _cfg, _metrics


@pytest.fixture
def held(monkeypatch):
    """The test hash file, with the compiled floor set to the same test
    hashes (the production floor is the datagen lane's to initialise)."""
    doc = json.loads(ts.FIXTURE.read_text())
    floor = {"salt": doc["salt"], "roles": {r: tuple(h) for r, h in doc["roles"].items()}}
    monkeypatch.setattr(ds, "_HELDOUT_FLOOR", floor)
    return ts.use_fixture(monkeypatch)


def _fleet(seed, schema="financial"):
    s = collect_from_pod_logs(
        {"p0": ""}, pod_images={"p0": ("img:1", None)}, pod_args={"p0": {"seed": seed}}
    )
    s.schema = schema
    return s


def _absent(seed: int, blob) -> bool:
    text = json.dumps(blob, default=str)
    return str(seed) not in text


class TestRule:
    @pytest.mark.parametrize(
        "seed, role",
        [(ts.TEST_EVALUATION_SEED, "evaluation"), (ts.TEST_ROBUSTNESS_SEED, "robustness")],
    )
    def test_held_out_seed_is_recorded_by_its_salted_hash(self, held, seed, role):
        rec = ds.record_seed("financial", seed)
        assert rec == {"seed_ref": ds.seed_hash(held.salt, seed), "role": role}
        assert rec == ds.record_seed("financial", seed)  # equal seeds, equal records
        assert ds.record_seed("financial", rec) == rec  # idempotent
        assert _absent(seed, rec)

    @pytest.mark.parametrize("seed", [43, 42, ts.TEST_SPENT_SEED, 12345])
    def test_public_and_spent_seeds_stay_plaintext(self, held, seed):
        assert ds.record_seed("financial", seed) == seed

    def test_customer360_seed_is_plaintext(self, held):
        assert ds.record_seed("customer360", ts.TEST_EVALUATION_SEED) == ts.TEST_EVALUATION_SEED

    def test_declared_protected_role_withholds_an_unregistered_seed(self, held):
        rec = ds.record_seed("financial", 777, corpus_role="evaluation")
        assert rec["role"] == "evaluation" and rec["seed_ref"] == ds.seed_hash(held.salt, 777)
        assert ds.record_seed("financial", 43, corpus_role="evaluation") == 43

    def test_unreadable_record_withholds(self, monkeypatch):
        def broken():
            raise FileNotFoundError("heldout_hashes.json")

        monkeypatch.setattr(ds, "_heldout", broken)
        rec = ds.record_seed("financial", 43)
        assert rec == {
            "seed_ref": None,
            "role": "unknown",
            "withheld": "held-out record unreadable",
        }

    @pytest.mark.parametrize("seed", [None, True, "not a seed"])
    def test_non_seeds_pass_through(self, held, seed):
        assert ds.record_seed("financial", seed) == seed


class TestRecords:
    def test_fleet_record_carries_no_held_out_seed(self, held):
        d = _fleet(ts.TEST_EVALUATION_SEED).to_dict()
        assert d["seed"]["role"] == "evaluation" and _absent(ts.TEST_EVALUATION_SEED, d)
        assert _fleet(43).to_dict()["seed"] == 43

    def test_experiment_block_carries_no_held_out_seed(self, held, monkeypatch):
        seed = ts.TEST_EVALUATION_SEED
        monkeypatch.setattr(ds, "config_seed", lambda cfg: seed)
        cfg = _cfg("financial")
        fleet = _fleet(seed).to_dict()
        rec = _metrics(cfg, fleet=fleet).to_dict()
        corpus = rec["experiment"]["corpus"]
        assert corpus["seed"]["role"] == "evaluation"
        assert corpus["datagen"]["seed"] == corpus["seed"]
        assert not any("seed" in p for p in corpus.get("problems", []))  # forms equal
        assert _absent(seed, rec)
        # The id hashes the recorded form, so it does not give the seed away.
        assert len(corpus["id"]) == 16

    def test_mismatch_problem_never_prints_a_held_out_seed(self, held, monkeypatch):
        seed = ts.TEST_EVALUATION_SEED
        monkeypatch.setattr(ds, "config_seed", lambda cfg: seed)
        fleet = {"seed": ts.TEST_ROBUSTNESS_SEED, "schema": "financial", "image": "img:1"}
        rec = _metrics(_cfg("financial"), fleet=fleet).to_dict()
        problems = rec["experiment"]["corpus"]["problems"]
        assert any("config seed" in p for p in problems)
        assert _absent(seed, rec) and _absent(ts.TEST_ROBUSTNESS_SEED, problems)

    def test_public_seed_records_unchanged(self, held):
        """A calibration run's record is what v1.6 wrote: plaintext seed,
        same corpus id."""
        cfg = _cfg("financial", seed=43)
        corpus = _metrics(cfg, fleet={"seed": 43, "schema": "financial"}).to_dict()["experiment"][
            "corpus"
        ]
        assert corpus["seed"] == 43 and corpus["datagen"]["seed"] == 43
        assert "problems" not in corpus
