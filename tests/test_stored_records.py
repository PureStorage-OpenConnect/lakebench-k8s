"""ER-1: the stored-record harness and the rule-5 scrubber.

The fixtures under tests/fixtures/records/ are the 24 pinned stored records
(DESIGN-v1.7 ch03 section 0.2) taken through tests/fixtures/scrub.py. These
tests pin what they say today (tests/expected/), and that the scrubber removes
endpoints, bucket names and credentials without moving identity.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakebench.cli._compare import _build_comparison
from lakebench.metrics.experiment import experiment_of, identity_hash
from tests.fixtures import scrub
from tests.fixtures import stored_records as sr

EXPECTED = sr.expected("records")
PAIRS = sr.expected("pairs")["pairs"]
DRIFT = set(EXPECTED["known_rebuild_drift"]["runs"])
FIXTURE_JSON = sorted(Path(sr.RECORDS_DIR).parent.rglob("*.json"))

LAB_ADDR = "192.0.2.15"  # RFC 5737 documentation address standing in for a lab one


# ---------------------------------------------------------------------------
# The fixture set
# ---------------------------------------------------------------------------


def test_fixture_set_is_the_pinned_set() -> None:
    ids = sr.record_ids()
    assert len(ids) == 24
    assert set(ids) == set(EXPECTED["records"])
    assert set(ids) == set(sr.manifest()["records"])
    assert sr.manifest()["scrubber_version"] == scrub.SCRUBBER_VERSION


@pytest.mark.parametrize("run_id", sorted(EXPECTED["records"]))
def test_record_matches_expected(run_id: str) -> None:
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
def test_rebuilt_block_keeps_stored_identity(run_id: str, request) -> None:
    """Loading a record and rebuilding its block must not move its identity
    (S1). Seven v1.6 records move today; ER-10a fixes that and must empty
    known_rebuild_drift, which this strict xfail enforces."""
    if run_id in DRIFT:
        request.applymarker(pytest.mark.xfail(strict=True, reason="S1, fixed by ER-10a"))
    rec = sr.load_record(run_id)
    stored = experiment_of(rec)
    rebuilt = sr.load_metrics(run_id).experiment_block()
    if stored is None:
        assert rebuilt is None
        return
    assert identity_hash(rebuilt) == identity_hash(stored)


# ---------------------------------------------------------------------------
# Every fixture is clean, and is the scrubber's output
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path", FIXTURE_JSON, ids=lambda p: str(p.relative_to(sr.RECORDS_DIR.parent))
)
def test_fixture_is_clean(path: Path) -> None:
    assert scrub.check_clean(json.loads(path.read_text())) == []


@pytest.mark.parametrize("run_id", sorted(EXPECTED["records"]))
def test_fixture_is_scrubber_output(run_id: str) -> None:
    rec = sr.load_record(run_id)
    scrubbed, changed = scrub.scrub_record(rec)
    assert changed == []
    assert scrubbed == rec


# ---------------------------------------------------------------------------
# The scrubber rules
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


def test_check_clean_flags_each_kind() -> None:
    problems = scrub.check_clean(_dirty())
    text = "\n".join(problems)
    assert "endpoint host is not the placeholder" in text
    assert "address outside the placeholder set" in text
    assert "credential-named key with a literal value" in text
    assert "value has a credential format" in text
    assert "bucket name 'dep-x-bronze' is not scrubbed" in text


def test_credential_format_under_neutral_key_refused() -> None:
    rec = sr.load_record("5105a0")
    rec["jobs"][0]["error_message"] = "key PSFB" + "A" * 38 + " rejected"
    with pytest.raises(scrub.ScrubError, match="credential format"):
        scrub.scrub_record(rec)


def test_protected_seed_refused_without_printing_it(monkeypatch) -> None:
    from lakebench.config import datagen_seed

    rec = sr.load_record("1320bd")
    seed = rec["experiment"]["corpus"]["seed"]
    monkeypatch.setattr(datagen_seed, "protected_seeds", lambda: {seed: "evaluation"})
    with pytest.raises(scrub.ScrubError) as exc:
        scrub.scrub_record(rec)
    assert "evaluation seed" in str(exc.value)
    assert ".experiment.corpus.seed" in str(exc.value)
    assert str(seed) not in str(exc.value)


def test_rewrite_reaching_identity_refused() -> None:
    """A lab registry inside the generator image reference is identity: the
    scrubber refuses rather than move the record's identity digest."""
    rec = sr.load_record("5105a0")
    image = rec["experiment"]["corpus"]["generator_image"]
    rec["experiment"]["corpus"]["generator_image"] = f"{LAB_ADDR}:5000/{image}"
    with pytest.raises(scrub.ScrubError, match="identity"):
        scrub.scrub_record(rec)


def test_cli_check_and_scrub(tmp_path: Path, capsys) -> None:
    dirty = tmp_path / "dirty.json"
    dirty.write_text(json.dumps(_dirty()))
    assert scrub.main(["--check", str(dirty)]) == 1
    out = tmp_path / "run-x" / "metrics.json"
    assert scrub.main([str(dirty), str(out)]) == 0
    assert scrub.main(["--check", str(out)]) == 0
    assert LAB_ADDR not in capsys.readouterr().out


# ---------------------------------------------------------------------------
# The pinned pairs as v1.6 compare reads them (section 0.3, Before)
# ---------------------------------------------------------------------------


def _refusal_kinds(prov: list[str]) -> tuple[list[str], list[str]]:
    keys, kinds = [], []
    for line in prov:
        if "no provenance" in line:
            kinds.append(f"{line.split('(')[1][0]}_no_provenance")
        elif "did not pass its verdict" in line:
            kinds.append(f"{line.split()[1]}_failed")
        elif " differs (" in line:
            keys.append(line.split(" differs (")[0])
        else:
            kinds.append(line)
    return keys, kinds


@pytest.mark.parametrize("pair", sorted(PAIRS, key=lambda p: int(p[1:])))
def test_pinned_pair_before(pair: str) -> None:
    spec = PAIRS[pair]
    got = _build_comparison("A", sr.load_record(spec["a"]), "B", sr.load_record(spec["b"]))
    keys, kinds = _refusal_kinds(got["refusals"]["provenance"])
    want = spec["before"]
    assert got["verdict"] == want["verdict"]
    assert got["like_for_like"] is want["like_for_like"]
    assert keys == want["identity_differences"]
    assert kinds == want["refusals"]
    conditions = [c.split(" differs (")[0] for c in got["condition_differences"]]
    assert conditions == want["condition_differences"]
    assert len(got["refusals"]["results"]) == want["result_refusals"]
