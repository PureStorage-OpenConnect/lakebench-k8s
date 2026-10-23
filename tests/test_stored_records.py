"""The stored-record harness and the held-out seed refusal of the scrubber.

The fixtures under tests/fixtures/records/ are the 24 pinned stored records
taken through tests/fixtures/scrub.py. These tests pin what they say today
(tests/expected/), that every fixture is clean, that one scrub round trip
removes endpoints, bucket names and credentials without moving identity, and
that a held-out seed in a record is refused and never printed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakebench.metrics.experiment import experiment_of, identity_hash
from tests.fixtures import scrub
from tests.fixtures import stored_records as sr

EXPECTED = sr.expected("records")
DRIFT = set(EXPECTED["known_rebuild_drift"]["runs"])
FIXTURE_JSON = sorted(Path(sr.RECORDS_DIR).parent.rglob("*.json"))

LAB_ADDR = "192.0.2.15"  # RFC 5737 documentation address standing in for a lab one

SEED = 987654  # stands in for a held-out seed; stubbed, never a real one


def _put(rec, path, value):
    node = rec
    for key in path[:-1]:
        node = node.setdefault(key, {}) if isinstance(node, dict) else node[key]
    node[path[-1]] = value


def _set(path, value):
    """A mutator that writes *value* at *path* of a record."""
    return lambda rec: _put(rec, path, value)


def _extra_with(**kv):
    return _set(("config_snapshot", "extra"), kv)


_MSG = ("jobs", 0, "error_message")


# ---------------------------------------------------------------------------
# The fixture set
# ---------------------------------------------------------------------------


def _stub_heldout(monkeypatch, seed: int, role: str) -> None:
    """Make *seed* the one live held-out seed, with *role*."""
    from lakebench.config import datagen_seed

    monkeypatch.setattr(
        datagen_seed, "heldout_role", lambda n, heldout=None: role if int(n) == seed else None
    )
    monkeypatch.setattr(datagen_seed, "is_spent", lambda n, heldout=None: False)


@pytest.mark.parametrize("run_id", sorted(EXPECTED["records"]))
def test_record_matches_expected(run_id) -> None:
    want = EXPECTED["records"][run_id]
    rec = sr.load_record(run_id)
    assert rec["run_id"] == run_id
    assert rec["success"] is want["success"]
    exp = experiment_of(rec)
    if want["generation"] == "legacy":
        assert exp is None
        assert "verdict" not in rec
        return
    assert exp is not None and exp["schema"] == want["generation"]
    assert exp["workload"]["name"] == want["workload"]
    assert exp["mode"] == want["mode"]
    assert exp["architecture"]["recipe"] == want["recipe"]
    assert exp["corpus"]["scale"] == want["scale"]
    assert (rec.get("verdict") or {}).get("status") == want["stored_verdict"]
    assert exp["corpus"]["id"] == want["corpus_id"]
    assert identity_hash(exp) == want["identity_digest"]


@pytest.mark.parametrize("run_id", sorted(EXPECTED["records"]))
def test_rebuilt_block_keeps_stored_identity(run_id) -> None:
    """Loading a record and rebuilding its block must not move its identity
    A record listed in known_rebuild_drift must still drift, so the
    list empties when the drift is fixed."""
    rec = sr.load_record(run_id)
    stored = experiment_of(rec)
    rebuilt = sr.load_metrics(run_id).experiment_block()
    if stored is None:
        assert rebuilt is None
    elif run_id in DRIFT:
        assert identity_hash(rebuilt) != identity_hash(stored)
    else:
        assert identity_hash(rebuilt) == identity_hash(stored)


# ---------------------------------------------------------------------------
# Every fixture is clean, and is the scrubber's output
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", FIXTURE_JSON, ids=lambda p: p.name)
def test_fixture_is_clean(path) -> None:
    assert scrub.check_clean(json.loads(path.read_text())) == []


# ---------------------------------------------------------------------------
# One scrub round trip
# ---------------------------------------------------------------------------


def _dirty() -> dict:
    rec = sr.load_record("5105a0")
    s3 = rec["config_snapshot"]["s3"]
    s3["endpoint"] = f"http://{LAB_ADDR}:80"
    s3["buckets"] = {"bronze": "dep-x-bronze", "silver": "dep-x-silver", "gold": "dep-x-gold"}
    s3["access_key"] = "AKIA" + "ABCDEFGHIJKLMNOP"  # split so scanners do not flag the test
    s3["secret_key"] = "abc/def+ghi" * 4
    s3["secret_ref"] = "lakebench-s3-credentials"
    rec["pipeline_benchmark"]["config_snapshot"]["s3"]["endpoint"] = "http://s3.lab.internal:80"
    rec["jobs"][0]["error_message"] = f"read s3a://dep-x-bronze/customers/ from {LAB_ADDR}"
    return rec


def test_scrub_rewrites_endpoint_buckets_and_credentials() -> None:
    rec = _dirty()
    before = scrub.identity_view(rec)
    out, changed = scrub.scrub_record(rec)
    s3 = out["config_snapshot"]["s3"]
    assert s3["endpoint"] == "http://10.0.1.50:80"
    assert out["pipeline_benchmark"]["config_snapshot"]["s3"]["endpoint"] == "http://10.0.1.50:80"
    assert s3["buckets"] == {
        "bronze": "scrubbed-bronze",
        "silver": "scrubbed-silver",
        "gold": "scrubbed-gold",
    }
    assert s3["access_key"] == "${LAKEBENCH_S3_ACCESS_KEY}"
    assert s3["secret_key"] == "${LAKEBENCH_S3_SECRET_KEY}"
    assert s3["secret_ref"] == "lakebench-s3-credentials"  # a name, not a credential
    assert out["jobs"][0]["error_message"] == "read s3a://scrubbed-bronze/customers/ from 10.0.1.50"
    assert ".config_snapshot.s3.endpoint" in changed
    assert ".jobs[0].error_message" in changed
    assert ".config_snapshot.s3.secret_ref" not in changed
    assert LAB_ADDR not in json.dumps(out)
    assert "dep-x-" not in json.dumps(out)
    assert scrub.identity_view(out) == before
    assert scrub.check_clean(out) == []


def _use_record(rec, run_id):
    """Swap in a stored record; its own seed is the held-out one."""
    rec.clear()
    rec.update(sr.load_record(run_id))
    return rec["experiment"]["corpus"]["seed"]


# (mutator returning the stubbed seed or None for SEED, role)
_SEED_INJECTIONS = [
    pytest.param(lambda rec: _use_record(rec, "1320bd"), "evaluation", id="stored-seed"),
    pytest.param(_extra_with(seed="987654"), "robustness", id="string"),
    pytest.param(_extra_with(args=["generate", "--seed", "987654"]), "evaluation", id="argv-list"),
    pytest.param(_set(_MSG, 'cfg {"seed": 987654}'), "evaluation", id="dumped"),
    pytest.param(
        _extra_with(**{"987654": {"seed": 987654}, "run-987654": {"n": "x"}}),
        "robustness",
        id="seed-in-a-dict-key",
    ),
    pytest.param(
        _extra_with(**{"a" * 25 + "10.99.1.2_987654": 1}),
        "robustness",
        id="seed-cut-by-the-key-slice",
    ),
]


@pytest.mark.parametrize(("mutate", "role"), _SEED_INJECTIONS)
def test_held_out_seed_is_refused_and_never_printed(monkeypatch, mutate, role) -> None:
    """A held-out seed in any shape refuses the record, and no
    message, path or problem carries its value (or a cut of it)."""
    rec = sr.load_record("5105a0")
    seed = mutate(rec) or SEED
    _stub_heldout(monkeypatch, seed, role)

    with pytest.raises(scrub.ScrubError, match=f"{role} seed") as exc:
        scrub.scrub_record(rec)
    assert str(seed) not in str(exc.value)
    problems = scrub.check_clean(rec)
    assert problems
    assert not any(str(seed)[:5] in p for p in problems)
    try:
        scrub.scrub_text("x", rec)
    except scrub.ScrubError as e:
        assert str(seed) not in str(e)


def test_seed_in_a_bucket_name_refuses_text_scrub(monkeypatch) -> None:
    _stub_heldout(monkeypatch, SEED, "robustness")
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["buckets"]["gold"] = f"lb-{SEED}-x"
    rec["config_snapshot"]["extra"] = {"note": f"lb-{SEED}-x"}
    with pytest.raises(scrub.ScrubError) as exc:
        scrub.scrub_text("x", rec)
    assert str(SEED) not in str(exc.value)


def test_stored_seed_is_named_by_path_not_value(monkeypatch) -> None:
    rec = sr.load_record("1320bd")
    seed = rec["experiment"]["corpus"]["seed"]
    _stub_heldout(monkeypatch, seed, "evaluation")
    with pytest.raises(scrub.ScrubError) as exc:
        scrub.scrub_record(rec)
    assert ".experiment.corpus.seed" in str(exc.value)


def test_seed_in_plain_text_is_refused(monkeypatch) -> None:
    _stub_heldout(monkeypatch, SEED, "evaluation")
    with pytest.raises(scrub.ScrubError, match="evaluation seed") as exc:
        scrub.scrub_text("gen seed=987654")
    assert str(SEED) not in str(exc.value)


def test_held_out_seeds_refused_through_the_hash_record(monkeypatch) -> None:
    """The seed tests stub the lookup; this one goes through the salted hash
    record (the test fixture, whose seeds are test values), so a regression
    to an empty check fails here. A spent seed passes. No
    assertion holds a seed, so a failure cannot print it."""
    import json as _json

    from lakebench.config import datagen_seed
    from tests.fixtures import heldout_test_seeds as ts

    doc = _json.loads(ts.FIXTURE.read_text())
    floor = {"salt": doc["salt"], "roles": {r: tuple(h) for r, h in doc["roles"].items()}}
    monkeypatch.setattr(datagen_seed, "_HELDOUT_FLOOR", floor)
    ts.use_fixture(monkeypatch)
    for seed, want in (
        (ts.TEST_EVALUATION_SEED, True),
        (ts.TEST_ROBUSTNESS_SEED, True),
        (ts.TEST_SPENT_SEED, False),
    ):
        rec = sr.load_record("5105a0")
        rec["jobs"][0]["error_message"] = f"generated with seed {seed}"
        try:
            scrub.scrub_record(rec)
            refused, leaked, why = False, False, ""
        except scrub.ScrubError as exc:
            refused, leaked, why = True, str(seed) in str(exc), str(exc).split(":")[0]
        assert (refused and "seed" in why) is want, "a held-out seed check gave the wrong answer"
        assert not leaked, "the refusal names the seed value"


def test_unreadable_held_out_record_refuses(monkeypatch) -> None:
    from lakebench.config import datagen_seed

    def broken(*_a, **_k):
        raise FileNotFoundError("heldout_hashes.json")

    monkeypatch.setattr(datagen_seed, "heldout_role", broken)
    with pytest.raises(scrub.ScrubError, match="cannot be read"):
        scrub.scrub_record(sr.load_record("5105a0"))
