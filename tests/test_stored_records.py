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


def test_manifest_pins_each_fixture() -> None:
    """A hand-edited fixture (a doctored row count) no longer matches the
    sha256 the import recorded."""
    for run_id, entry in sr.manifest()["records"].items():
        assert scrub.sha256_of(sr.record_path(run_id)) == entry["fixture_sha256"], run_id


_SOURCE_ROOTS = {
    "lakebench-k8s": Path("/home/repos/lakebench-k8s"),
    "lakebench-k8s-integrate": Path("/home/repos/lakebench-k8s-integrate"),
}


@pytest.mark.parametrize("run_id", sorted(EXPECTED["records"]))
def test_fixture_reproduces_from_its_source(run_id: str) -> None:
    """Rule 5 lineage, where the local sources exist (never in CI): the
    fixture is exactly the scrubber's output on the recorded source."""
    entry = sr.manifest()["records"][run_id]
    src = _SOURCE_ROOTS[entry["source"]["root"]] / entry["source"]["path"]
    if not src.exists():
        pytest.skip("source record not on this host")
    assert scrub.sha256_of(src) == entry["source_sha256"]
    scrubbed, changed = scrub.scrub_record(json.loads(src.read_text()))
    assert scrub.dump(scrubbed) == sr.record_path(run_id).read_text()
    assert changed == entry["rewritten"]


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
    assert "credential format" in text
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


def test_rewrite_inside_experiment_block_refused() -> None:
    """A lab registry inside the stored generator image reference is
    evidence: the scrubber refuses rather than edit the experiment block."""
    rec = sr.load_record("5105a0")
    image = rec["experiment"]["corpus"]["generator_image"]
    rec["experiment"]["corpus"]["generator_image"] = f"{LAB_ADDR}:5000/{image}"
    with pytest.raises(scrub.ScrubError, match="rewrite evidence"):
        scrub.scrub_record(rec)


def test_rewrite_reaching_rebuilt_identity_refused() -> None:
    """The same registry in the run-start inputs moves the identity of the
    block experiment_block() rebuilds: refused by the identity guard."""
    rec = sr.load_record("5105a0")
    corpus = rec["config_snapshot"]["experiment_inputs"]["corpus"]
    corpus["generator_image"] = f"{LAB_ADDR}:5000/{corpus['generator_image']}"
    with pytest.raises(scrub.ScrubError, match="identity"):
        scrub.scrub_record(rec)


def test_bucket_names_replaced_only_as_whole_tokens() -> None:
    """Default bucket names are prefixes of job names: a bucket
    lakebench-bronze must not turn job lakebench-bronze-verify into
    scrubbed-bronze-verify."""
    rec = sr.load_record("5105a0")
    s3 = rec["config_snapshot"]["s3"]
    s3["buckets"] = {"bronze": "lakebench-bronze", "silver": "lakebench-silver", "gold": "gb"}
    rec["jobs"][0]["job_name"] = "lakebench-bronze-verify"
    rec["jobs"][0]["error_message"] = "listing s3a://lakebench-bronze/x and lakebench-silver/y"
    with pytest.raises(scrub.ScrubError, match="shorter than any valid S3 bucket"):
        scrub.scrub_record(rec)
    s3["buckets"]["gold"] = "lakebench-gold"
    out, changed = scrub.scrub_record(rec)
    assert out["jobs"][0]["job_name"] == "lakebench-bronze-verify"
    assert out["experiment"] == rec["experiment"]
    assert out["jobs"][0]["error_message"] == (
        "listing s3a://scrubbed-bronze/x and scrubbed-silver/y"
    )


def test_bucket_named_like_a_stage_is_refused_not_rewritten() -> None:
    """A bucket literally named silver is a whole token in
    experiment.stages.executed: refuse rather than edit the stages run."""
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["buckets"]["silver"] = "silver"
    with pytest.raises(scrub.ScrubError, match=r"rewrite evidence.*stages\.executed"):
        scrub.scrub_record(rec)


def test_bucket_name_in_a_job_name_is_refused() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["buckets"]["bronze"] = "dep-x-bronze"
    rec["jobs"][0]["job_name"] = "dep-x-bronze"
    with pytest.raises(scrub.ScrubError, match=r"rewrite evidence.*jobs\[0\]\.job_name"):
        scrub.scrub_record(rec)


@pytest.mark.parametrize(
    "where",
    ["dotted_spark_conf", "camel_case", "aws_env_name", "env_list", "credentials_dict", "dict_key"],
)
def test_other_credential_and_endpoint_shapes(where: str) -> None:
    rec = sr.load_record("5105a0")
    extra: dict = rec.setdefault("config_snapshot", {}).setdefault("extra", {})
    secret = "abc/def+ghi" * 4
    if where == "dotted_spark_conf":
        extra["spark.hadoop.fs.s3a.endpoint"] = "http://fb.lab.corp:80"
        extra["spark.hadoop.fs.s3a.secret.key"] = secret
    elif where == "camel_case":
        extra["endpointOverride"] = "fb.lab.corp:80"
        extra["secretAccessKey"] = secret
    elif where == "aws_env_name":
        extra["AWS_ENDPOINT_URL_S3"] = "http://fb.lab.corp"
        extra["AWS_SECRET_ACCESS_KEY"] = secret
    elif where == "env_list":
        extra["env"] = [
            {"name": "AWS_SECRET_ACCESS_KEY", "value": secret},
            {"name": "S3_ENDPOINT", "value": "http://fb.lab.corp:80"},
        ]
    elif where == "credentials_dict":
        extra["credentials"] = {"id": "PSKEYID", "key": secret}
        extra["endpoint"] = "http://fb.lab.corp:80"
    else:
        extra[f"{LAB_ADDR}:80"] = {"endpoint": "http://fb.lab.corp:80"}
    rec["jobs"][0]["error_message"] = "connect to fb.lab.corp timed out"
    assert scrub.check_clean(rec) != []
    out, _ = scrub.scrub_record(rec)
    text = json.dumps(out)
    assert "fb.lab.corp" not in text
    assert secret not in text
    assert "PSKEYID" not in text
    assert LAB_ADDR not in text
    assert scrub.check_clean(out) == []


def test_url_userinfo_dropped() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["endpoint"] = "http://user:pw@fb.lab.corp:80"
    rec["jobs"][0]["error_message"] = "GET https://bob:hunter2@mirror.example.org/x failed"
    assert any("user-info" in p for p in scrub.check_clean(rec))
    out, _ = scrub.scrub_record(rec)
    assert out["config_snapshot"]["s3"]["endpoint"] == "http://10.0.1.50:80"
    assert out["jobs"][0]["error_message"] == "GET https://mirror.example.org/x failed"


def test_non_addresses_left_alone() -> None:
    """A four-part version is not a lab address, and a path under an
    endpoint-named key is not a host."""
    rec = sr.load_record("5105a0")
    extra = rec["config_snapshot"].setdefault("extra", {})
    extra["jdk"] = "17.0.12.7"
    extra["metrics_endpoint"] = "/metrics"
    out, changed = scrub.scrub_record(rec)
    assert out["config_snapshot"]["extra"] == {"jdk": "17.0.12.7", "metrics_endpoint": "/metrics"}
    assert not [p for p in changed if ".extra." in p]


@pytest.mark.parametrize("shape", ["string", "argv", "camel", "list"])
def test_protected_seed_in_other_shapes_refused(monkeypatch, shape: str) -> None:
    from lakebench.config import datagen_seed

    monkeypatch.setattr(datagen_seed, "protected_seeds", lambda: {987654: "robustness"})
    rec = sr.load_record("5105a0")
    extra = rec["config_snapshot"].setdefault("extra", {})
    if shape == "string":
        extra["seed"] = "987654"
    elif shape == "argv":
        extra["args"] = "generate --seed 987654 --scale 1"
    elif shape == "camel":
        extra["randomSeed"] = 987654
    else:
        extra["instance_seeds"] = [1, 987654]
    with pytest.raises(scrub.ScrubError, match="robustness seed") as exc:
        scrub.scrub_record(rec)
    assert "987654" not in str(exc.value)


def test_top_level_list_refused() -> None:
    with pytest.raises(scrub.ScrubError, match="JSON object"):
        scrub.scrub_record([{"run_id": "x"}])


def test_scrub_text_for_logs() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["endpoint"] = "http://fb.lab.corp:80"
    bucket = rec["config_snapshot"]["s3"]["buckets"]["bronze"]
    log = f"[lb] read s3a://{bucket}/a from fb.lab.corp via {LAB_ADDR}\n"
    out = scrub.scrub_text(log, rec)
    assert out == "[lb] read s3a://scrubbed-bronze/a from 10.0.1.50 via 10.0.1.50\n"
    with pytest.raises(scrub.ScrubError, match="credential format"):
        scrub.scrub_text("key AKIA" + "ABCDEFGHIJKLMNOP")


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


def test_key_rename_that_merges_keys_refused() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["by_host"] = {"10.99.0.1": 5, "10.99.0.2": 7}
    with pytest.raises(scrub.ScrubError, match="would merge"):
        scrub.scrub_record(rec)


def test_key_rename_reported_by_full_path() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["by_host"] = {"10.99.0.1": {"job_name": "x"}}
    out, changed = scrub.scrub_record(rec)
    assert out["config_snapshot"]["by_host"] == {"10.0.1.50": {"job_name": "x"}}
    assert ".config_snapshot.by_host.10.99.0.1" in changed


def test_key_rename_inside_experiment_refused() -> None:
    rec = sr.load_record("5105a0")
    rec["experiment"]["limits"]["by_host"] = {"10.99.0.1": 1}
    with pytest.raises(scrub.ScrubError, match="rename evidence key"):
        scrub.scrub_record(rec)


def test_dotless_endpoint_host_is_not_a_global_token() -> None:
    """An endpoint http://minio:9000 must not rewrite every word minio, or
    a key named minio, elsewhere in the record."""
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["endpoint"] = "http://minio:9000"
    rec["config_snapshot"]["note"] = "backend minio"
    rec["config_snapshot"]["minio"] = {"image": "x"}
    out, _ = scrub.scrub_record(rec)
    assert out["config_snapshot"]["s3"]["endpoint"] == "http://10.0.1.50:9000"
    assert out["config_snapshot"]["note"] == "backend minio"
    assert "minio" in out["config_snapshot"]


def test_endpoint_host_matched_case_insensitively() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["endpoint"] = "http://s3.lab.example:80"
    rec["jobs"][0]["error_message"] = "S3.LAB.EXAMPLE refused"
    out, _ = scrub.scrub_record(rec)
    assert out["jobs"][0]["error_message"] == "10.0.1.50 refused"


@pytest.mark.parametrize("shape", ["argv_list", "argv_ints", "seed_equals"])
def test_protected_seed_in_argv_shapes_refused(monkeypatch, shape: str) -> None:
    from lakebench.config import datagen_seed

    monkeypatch.setattr(datagen_seed, "protected_seeds", lambda: {987654: "evaluation"})
    rec = sr.load_record("5105a0")
    extra = rec["config_snapshot"].setdefault("extra", {})
    if shape == "argv_list":
        extra["args"] = ["generate", "--seed", "987654"]
    elif shape == "argv_ints":
        extra["spark_arguments"] = ["--scale", 1, "--seed", 987654]
    else:
        extra["cmd"] = "gen seed=987654"
    with pytest.raises(scrub.ScrubError, match="evaluation seed") as exc:
        scrub.scrub_record(rec)
    assert "987654" not in str(exc.value)
    if shape == "seed_equals":
        with pytest.raises(scrub.ScrubError, match="evaluation seed"):
            scrub.scrub_text("gen seed=987654")


def test_dotless_endpoint_host_rewritten_inside_urls() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["endpoint"] = "http://fb01:80"
    rec["jobs"][0]["error_message"] = "GET http://FB01:80/a failed; fb01 busy"
    out, _ = scrub.scrub_record(rec)
    assert out["jobs"][0]["error_message"] == "GET http://10.0.1.50:80/a failed; fb01 busy"


@pytest.mark.parametrize(
    "text",
    ['cfg {"seed": 987654}', "seed = 987654", "AML_SEED=987654", "datagen_seed: 987654"],
)
def test_protected_seed_in_dumped_text_refused(monkeypatch, text: str) -> None:
    from lakebench.config import datagen_seed

    monkeypatch.setattr(datagen_seed, "protected_seeds", lambda: {987654: "evaluation"})
    rec = sr.load_record("5105a0")
    rec["jobs"][0]["error_message"] = text
    with pytest.raises(scrub.ScrubError, match="evaluation seed"):
        scrub.scrub_record(rec)
    rec["jobs"][0]["error_message"] = "ok"
    rec["config_snapshot"]["args"] = ["--aml-seed", "987654"]
    with pytest.raises(scrub.ScrubError, match="evaluation seed"):
        scrub.scrub_record(rec)


def test_cgnat_address_is_private() -> None:
    rec = sr.load_record("5105a0")
    rec["jobs"][0]["error_message"] = "via 100.64.3.4 and 100.128.0.1"
    out, _ = scrub.scrub_record(rec)
    assert out["jobs"][0]["error_message"] == "via 10.0.1.50 and 100.128.0.1"


def test_key_rename_inside_verdict_refused_but_not_lookalike_keys() -> None:
    rec = sr.load_record("5105a0")
    rec["verdict"]["by_host"] = {"10.99.0.1": 1}
    with pytest.raises(scrub.ScrubError, match="rename evidence key"):
        scrub.scrub_record(rec)
    rec = sr.load_record("5105a0")
    rec["experimental"] = {"10.99.0.1": 1}
    out, changed = scrub.scrub_record(rec)
    assert out["experimental"] == {"10.0.1.50": 1}
