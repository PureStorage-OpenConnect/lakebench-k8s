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
        assert ds.record_seed("financial", 777) == ds.WITHHELD
        # The calibration seed is public from the pre-registration alone.
        assert ds.record_seed("financial", 43) == 43

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


class TestReviewCases:
    def test_unknown_schema_is_checked_not_plaintext(self, held):
        rec = ds.record_seed("", ts.TEST_EVALUATION_SEED)
        assert rec["role"] == "evaluation"
        d = _fleet(ts.TEST_EVALUATION_SEED, schema="").to_dict()
        assert _absent(ts.TEST_EVALUATION_SEED, d)

    def test_seeds_equal_across_the_spend(self, held):
        seed = ts.TEST_EVALUATION_SEED
        ref = ds.record_seed("financial", seed)
        assert ds.seeds_equal(ref, seed) is True and ds.seeds_equal(seed, ref) is True
        assert ds.seeds_equal(ref, ts.TEST_ROBUSTNESS_SEED) is False
        assert ds.seeds_equal(43, 43) is True and ds.seeds_equal(43, 44) is False
        assert ds.seeds_equal(ds.WITHHELD, 43) is None
        assert ds.seeds_equal(ds.WITHHELD, ds.WITHHELD) is None

    def test_record_written_before_the_spend_reads_one_seed_after(self, held, monkeypatch):
        """Config seed stored as a ref (held out at run start), fleet seed
        plaintext (written after the look spent it): one corpus, no problem."""
        seed = ts.TEST_EVALUATION_SEED
        monkeypatch.setattr(ds, "config_seed", lambda cfg: seed)
        run = _metrics(_cfg("financial"), fleet=None)
        inputs = run.config_snapshot["experiment_inputs"]
        assert inputs["corpus"]["seed"]["role"] == "evaluation"
        monkeypatch.setattr(ds, "recorded_seeds", lambda path=None: frozenset({seed}))
        run.datagen_fleet = {"seed": seed, "schema": "financial"}
        corpus = run.to_dict()["experiment"]["corpus"]
        assert not any("seed" in p for p in corpus.get("problems", []))

    def test_withheld_seed_is_a_problem_not_a_match(self, held, monkeypatch):
        monkeypatch.setattr(ds, "config_seed", lambda cfg: 777)
        fleet = {"seed": dict(ds.WITHHELD), "schema": "financial"}
        problems = _metrics(_cfg("financial"), fleet=fleet).to_dict()["experiment"]["corpus"][
            "problems"
        ]
        assert any("could not be checked" in p for p in problems)

    def test_one_spent_rule_for_scrubber_and_records(self, held, monkeypatch):
        from tests.fixtures import scrub

        seed = ts.TEST_EVALUATION_SEED
        assert scrub._protected_role(seed) == "evaluation"
        monkeypatch.setattr(ds, "recorded_seeds", lambda path=None: frozenset({seed}))
        assert scrub._protected_role(seed) is None
        assert ds.record_seed("financial", seed) == seed

    @pytest.mark.parametrize("shape", ["grouped", "underscored", "float"])
    def test_scrubber_finds_grouped_and_float_seeds(self, held, shape):
        from tests.fixtures import scrub
        from tests.fixtures import stored_records as sr

        seed = ts.TEST_EVALUATION_SEED
        rec = sr.load_record("5105a0")
        extra = rec["config_snapshot"].setdefault("extra", {})
        if shape == "grouped":
            extra["note"] = f"seed {seed:,}"
        elif shape == "underscored":
            extra["note"] = f"seed {seed:_}"
        else:
            extra["seed"] = float(seed)
        with pytest.raises(scrub.ScrubError) as exc:
            scrub.scrub_record(rec)
        assert "seed" in str(exc.value) and _absent(seed, str(exc.value))


class TestFinalPassCases:
    @pytest.mark.parametrize("value", [1e300, -1e300, float(2**63), 1.7976931348623157e308])
    def test_huge_floats_scan_quickly(self, held, value):
        import time

        from tests.fixtures import scrub

        start = time.monotonic()
        assert scrub._seed_problems({"x": value}) == []
        assert time.monotonic() - start < 1.0

    def test_float_window_is_bounded(self, held):
        from tests.fixtures import scrub

        hits = list(scrub._numbers({"x": float(2**62)}))
        assert 1 < len(hits) <= 1027

    @pytest.mark.parametrize("a, b", [(43.9, 43), (True, 1), ("43", 43.5), (float(2**60), 2**60)])
    def test_seeds_equal_needs_exact_integers(self, held, a, b):
        assert ds.seeds_equal(a, b) is None
        assert ds.seeds_equal("43", 43) is True
