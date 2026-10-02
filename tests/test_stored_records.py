"""ER-1: the stored-record harness and the rule-5 scrubber.

The fixtures under tests/fixtures/records/ are the 24 pinned stored records
(DESIGN-v1.7 ch03 section 0.2) taken through tests/fixtures/scrub.py. These
tests pin what they say today (tests/expected/), and that the scrubber removes
endpoints, bucket names and credentials without moving identity.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

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
    sha256 the import recorded. A commit that edits the fixture and the
    MANIFEST together passes this; only the lineage test below, run where
    the sources exist, catches that."""
    for run_id, entry in sr.manifest()["records"].items():
        assert scrub.sha256_of(sr.record_path(run_id)) == entry["fixture_sha256"], run_id


#: Directories holding the raw source records, one environment variable per
#: MANIFEST root (LB_FIXTURE_SOURCES_MAINTAINER_EVIDENCE,
#: LB_FIXTURE_SOURCES_INTEGRATE_RUNS). Unset in CI, where the lineage test
#: skips; set on the host that holds the raw records.
_SOURCE_ROOTS = {
    root: os.environ.get("LB_FIXTURE_SOURCES_" + root.upper().replace("-", "_"), "")
    for root in ("maintainer-evidence", "integrate-runs")
}


@pytest.mark.parametrize("run_id", sorted(EXPECTED["records"]))
def test_fixture_reproduces_from_its_source(run_id: str) -> None:
    """Rule 5 lineage, where the local sources exist (never in CI): the
    fixture is exactly the scrubber's output on the recorded source."""
    entry = sr.manifest()["records"][run_id]
    root = _SOURCE_ROOTS[entry["source"]["root"]]
    src = Path(root) / entry["source"]["path"]
    if not root or not src.exists():
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
    with pytest.raises(scrub.ScrubError, match="bucket name 'silver' is a single word"):
        scrub.scrub_record(rec)


def test_legacy_bucket_named_like_a_stage_is_refused() -> None:
    """A legacy record has no experiment block for the identity guard to
    compare: a bucket named silver would rename stage_matrix.silver and
    rewrite stages[].stage_name without a refusal (review SC1)."""
    rec = sr.load_record("978622")
    assert experiment_of(rec) is None
    rec["config_snapshot"]["s3"]["buckets"]["silver"] = "silver"
    with pytest.raises(scrub.ScrubError, match="is a single word"):
        scrub.scrub_record(rec)
    rec["config_snapshot"]["s3"]["buckets"]["silver"] = "dep-x-silver"
    rec["pipeline_benchmark"]["stage_matrix"]["dep-x-silver"] = {}
    with pytest.raises(scrub.ScrubError, match="also a key in the record"):
        scrub.scrub_record(rec)


@pytest.mark.parametrize("value_at", ["table_format", "catalog", "pipeline_mode"])
def test_legacy_bucket_equal_to_an_identity_value_is_refused(value_at: str) -> None:
    """Fix-pass review: in a legacy record a bucket named like a recipe word
    rewrote table_format, catalog and pipeline_mode with no refusal."""
    rec = sr.load_record("978622")
    rec["config_snapshot"]["s3"]["buckets"]["gold"] = "dep-x-gold"
    rec["config_snapshot"][value_at] = "dep-x-gold"
    with pytest.raises(scrub.ScrubError, match="also a value outside the bucket settings"):
        scrub.scrub_record(rec)


def test_stage_name_is_evidence() -> None:
    rec = sr.load_record("978622")
    rec["config_snapshot"]["s3"]["buckets"]["bronze"] = "dep-x-bronze"
    rec["pipeline_benchmark"]["stages"][0]["stage_name"] = "verify s3a://dep-x-bronze"
    with pytest.raises(scrub.ScrubError, match=r"rewrite evidence.*stage_name"):
        scrub.scrub_record(rec)


def test_credential_named_key_inside_experiment_refused() -> None:
    """A cap such as experiment.limits.max_token reads as a credential key;
    inside the experiment block it is evidence, never a placeholder (SC2)."""
    rec = sr.load_record("5105a0")
    rec["experiment"]["limits"]["max_token"] = "4096"
    with pytest.raises(scrub.ScrubError, match=r"rewrite evidence.*max_token"):
        scrub.scrub_record(rec)


def test_address_before_a_full_stop_is_found() -> None:
    assert scrub.scrub_text("ok 10.99.7.5, 10.99.7.6.") == "ok 10.0.1.50, 10.0.1.50."
    rec = sr.load_record("5105a0")
    rec["jobs"][0]["error_message"] = "Connection refused by 10.99.7.5."
    assert any("private address" in p for p in scrub.check_clean(rec))
    # Still not part of a longer dotted run.
    assert scrub.scrub_text("jdk 1.10.99.7.5 and 10.99.7.5.1") == "jdk 1.10.99.7.5 and 10.99.7.5.1"


def test_leading_zero_address_is_private() -> None:
    assert scrub.scrub_text("via 010.099.007.005") == "via 10.0.1.50"


@pytest.mark.parametrize("shape", ["conf_argv", "key_value_env", "yaml_text"])
def test_credential_assigned_in_text(shape: str) -> None:
    """.gitleaks.toml's s3-secret-key-assignment and k8s-inline-env shapes,
    and the dotted Spark form gitleaks itself misses (review M1)."""
    fake = "Ab1x" * 10  # 40 chars, not a key
    rec = sr.load_record("5105a0")
    extra = rec["config_snapshot"].setdefault("extra", {})
    if shape == "conf_argv":
        extra["args"] = ["--conf", f"spark.hadoop.fs.s3a.secret.key={fake}"]
    elif shape == "key_value_env":
        extra["env"] = [{"key": "AWS_SECRET_ACCESS_KEY", "value": fake}]
    else:
        rec["jobs"][0]["error_message"] = f"bad config: secretKey: {fake}"
    assert scrub.check_clean(rec) != []
    out, _ = scrub.scrub_record(rec)
    text = json.dumps(out)
    assert fake not in text
    assert "${LAKEBENCH_S3_SECRET_KEY}" in text
    assert scrub.check_clean(out) == []
    # A value: line in pasted YAML refuses rather than rewrites.
    with pytest.raises(scrub.ScrubError, match="credential format"):
        scrub.scrub_text(f"env:\n- name: X\n  value: {fake}\n")


def test_subdomain_of_an_endpoint_host_rewritten() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["endpoint"] = "http://fb.lab.corp:80"
    rec["jobs"][0]["error_message"] = "GET https://mybkt.fb.lab.corp/k via ns1.fb.lab.corp"
    out, _ = scrub.scrub_record(rec)
    assert out["jobs"][0]["error_message"] == "GET https://10.0.1.50/k via 10.0.1.50"


def test_s3_url_and_endpoints_keys_are_endpoints() -> None:
    rec = sr.load_record("5105a0")
    extra = rec["config_snapshot"].setdefault("extra", {})
    extra["s3_url"] = "http://fb.lab.corp:80"
    extra["endpoints"] = "fb2.lab.corp:80"
    out, _ = scrub.scrub_record(rec)
    assert out["config_snapshot"]["extra"] == {
        "s3_url": "http://10.0.1.50:80",
        "endpoints": "10.0.1.50:80",
    }


def test_real_protected_seeds_refused_unstubbed() -> None:
    """The seed tests stub protected_seeds; this one uses the real roles, so
    a regression to an empty map fails here (review L1). The value is never
    put in an assertion, so a failure cannot print it."""
    from lakebench.config import datagen_seed

    protected = datagen_seed.protected_seeds()
    assert sorted(protected.values()) == ["evaluation", "robustness"]
    for seed in protected:
        rec = sr.load_record("5105a0")
        rec["jobs"][0]["error_message"] = f"generated with seed {seed}"
        try:
            scrub.scrub_record(rec)
            refused, leaked, why = False, False, ""
        except scrub.ScrubError as exc:
            refused, leaked, why = True, str(seed) in str(exc), str(exc).split(":")[0]
        refused = refused and "seed" in why
        assert refused, "a real held-out seed was not refused as a seed"
        assert not leaked, "the refusal names the seed value"


def test_bucket_name_in_a_job_name_is_refused() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["buckets"]["bronze"] = "dep-x-bronze"
    rec["jobs"][0]["job_name"] = "verify s3a://dep-x-bronze"
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


@pytest.mark.parametrize(
    "shape",
    [
        "string",
        "argv",
        "camel",
        "list",
        "second_number",
        "seeds_list_text",
        "hyphen_word",
        "underscore_word",
        "dict_key",
        "nested",
        "float",
    ],
)
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
    elif shape == "list":
        extra["instance_seeds"] = [1, 987654]
    elif shape == "second_number":
        # datagen_seed.aml_seed_error's own message shape.
        extra["error"] = "the corpus was generated with seed 777, not the claimed 987654"
    elif shape == "seeds_list_text":
        extra["note"] = "seeds 777, 987654"
    elif shape == "hyphen_word":
        extra["note"] = "aml-seed-987654"
    elif shape == "underscore_word":
        extra["note"] = "aml_seed_987654"
    elif shape == "dict_key":
        extra["by_seed"] = {"987654": "x"}
    elif shape == "nested":
        extra["seed"] = {"evaluation": 987654}
    else:
        extra["seed"] = 987654.0
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


def test_seed_in_a_dict_key_is_not_printed_in_the_path(monkeypatch) -> None:
    """Fix-pass review: a path built from a key holding the seed printed it."""
    from lakebench.config import datagen_seed

    monkeypatch.setattr(datagen_seed, "protected_seeds", lambda: {987654: "robustness"})
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["extra"] = {"987654": {"seed": 987654}, "run-987654": {"n": "x"}}
    with pytest.raises(scrub.ScrubError, match="robustness seed") as exc:
        scrub.scrub_record(rec)
    assert "987654" not in str(exc.value)
    assert not any("987654" in p for p in scrub.check_clean(rec))


@pytest.mark.parametrize(
    "text",
    [
        "javax.jdo.option.ConnectionPassword=hive",
        "trustStorePassword=Abcdefgh!secretTail",
        "s3SecretKey=abc",
        "rootPassword: x9",
    ],
)
def test_prefixed_credential_names_in_text(text: str) -> None:
    out = scrub.scrub_text(f"conf {text} done")
    assert out.endswith(" done") and "${LAKEBENCH_" in out
    value = text.split("=")[-1].split(": ")[-1]
    assert value not in out


def test_non_credential_pairs_in_text_left_alone() -> None:
    text = (
        "password_policy=strict-mode access_key_id=${LAKEBENCH_S3_ACCESS_KEY} "
        "fs.s3a.aws.credentials.provider=org.apache.Simple v1.10.0.0.1"
    )
    assert scrub.scrub_text(text) == text


def test_no_message_carries_a_seed(monkeypatch) -> None:
    """Second fix pass: check_clean, bucket refusals and scrub_text built
    messages from paths and keys that can hold a held-out seed."""
    from lakebench.config import datagen_seed

    monkeypatch.setattr(datagen_seed, "protected_seeds", lambda: {987654: "robustness"})
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["extra"] = {"987654": {"host": "10.99.1.2"}, "10.1.2.3-987654": 1}
    problems = scrub.check_clean(rec)
    assert problems and not any("987654" in p for p in problems)
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["buckets"]["gold"] = "lb-987654-x"
    rec["config_snapshot"]["extra"] = {"note": "lb-987654-x"}
    with pytest.raises(scrub.ScrubError) as exc:
        scrub.scrub_text("x", rec)
    assert "987654" not in str(exc.value)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("password=$3cr3tValue end", "password=${LAKEBENCH_CREDENTIAL} end"),
        ('password="my secret pass" end', 'password="${LAKEBENCH_CREDENTIAL}" end'),
        ("password=abc,def end", "password=${LAKEBENCH_CREDENTIAL} end"),
        (
            "token=secret=" + "x" * 40 + " next_field=important",
            "token=${LAKEBENCH_CREDENTIAL} next_field=important",
        ),
        ("credentials:\n  user: x", "credentials:\n  user: x"),
        # Third fix pass: a swallowed quoted pair leaked its value.
        (
            'accessKey=AKIDEXAMPLE,secretKey="abab abab" end',
            "accessKey=${LAKEBENCH_S3_ACCESS_KEY} end",
        ),
        ('password=,token="realtoken" end', "password=${LAKEBENCH_CREDENTIAL} end"),
        ("password=abc;token='tok en123' end", "password=${LAKEBENCH_CREDENTIAL} end"),
        ("password:\n  hunter22\nuser: x", "password:\n  ${LAKEBENCH_CREDENTIAL}\nuser: x"),
        (
            "password: |\n  hunter22\n  more\nuser: x",
            "password: |\n  ${LAKEBENCH_CREDENTIAL}\nuser: x",
        ),
        (
            "access_key_id=${LAKEBENCH_S3_ACCESS_KEY} ok",
            "access_key_id=${LAKEBENCH_S3_ACCESS_KEY} ok",
        ),
    ],
)
def test_text_credential_values_taken_whole(text: str, expected: str) -> None:
    assert scrub.scrub_text(text) == expected


def test_long_token_runs_scrub_in_linear_time() -> None:
    import time

    text = "a" * 50_000 + " password=x"
    t0 = time.monotonic()
    out = scrub.scrub_text(text)
    assert time.monotonic() - t0 < 2.0
    assert out.endswith("password=${LAKEBENCH_CREDENTIAL}")


def test_url_userinfo_glued_to_a_timestamp_dropped() -> None:
    out = scrub.scrub_text(
        "2026-10-01T10:00:00Z-http://AKIAFAKE:sekrit@10.99.1.2/ -https://u:pw@h/x"
    )
    assert "sekrit" not in out and "u:pw" not in out


def test_seed_cut_by_the_key_slice_is_not_printed(monkeypatch) -> None:
    from lakebench.config import datagen_seed

    monkeypatch.setattr(datagen_seed, "protected_seeds", lambda: {987654: "robustness"})
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["extra"] = {"a" * 25 + "10.99.1.2_987654": 1}
    problems = scrub.check_clean(rec)
    assert problems and not any("98765" in p for p in problems)


@pytest.mark.parametrize(
    "text",
    [
        '{\\"secretKey\\":\\"hunter2secret\\"}',
        'payload={\\"password\\": \\"hunter2secret\\"}',
        "password:\r\n  hunter2secret\r\nuser: x",
        "password: |\r\n  hunter2secret\r\n",
        "password:\n  c2VjcmV0cGFzcw==\n",
        "password:\n  hunter2:secret\n",
        'password="abc\\"hunter2secret" end',
        "password: |  # pem\n  hunter2secret\n",
        "password: |2\n  hunter2secret\n",
        "password: |\n  line1\n\n  hunter2secret\n",
        "password: |\n\n  hunter2secret\n",
        "password:\n  part1\n  hunter2secret\n",
        "run --password hunter2secret --scale 1",
    ],
)
def test_fourth_pass_text_shapes_leave_no_secret(text: str) -> None:
    """Fourth fix pass: each shape either scrubs the secret away or is
    refused; it never comes back with the secret and a clean check."""
    try:
        out = scrub.scrub_text(text)
    except scrub.ScrubError:
        return
    assert "hunter2" not in out and "c2VjcmV0" not in out and "part1" not in out


def test_backstop_refuses_what_the_rewrite_misses() -> None:
    # Header credentials are a refused format, not a rewritten pair.
    rec = sr.load_record("5105a0")
    rec["jobs"][0]["error_message"] = "Authorization: Basic aHVudGVyMnNlY3JldA=="
    assert any("credential format" in p for p in scrub.check_clean(rec))
    with pytest.raises(scrub.ScrubError, match="credential format"):
        scrub.scrub_text("Authorization: Bearer abc.def.ghi")


def test_userinfo_secret_with_a_slash_refused() -> None:
    with pytest.raises(scrub.ScrubError, match="user-info"):
        scrub.scrub_text("s3a://MYACCESS:hunter2/secret@bucket/path")


def test_partial_placeholder_leaf_is_rewritten() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["secret_key"] = "${X}hunter2secret"
    out, _ = scrub.scrub_record(rec)
    assert out["config_snapshot"]["s3"]["secret_key"] == "${LAKEBENCH_S3_SECRET_KEY}"


def test_many_text_credentials_scrub_in_linear_time() -> None:
    """Eight times the input takes about eight times the work, not sixty-four.

    CPU time of this process, not wall time, and a ratio, not a fixed bound:
    under xdist the other workers share the CPUs, and coverage tracing on the
    3.13 leg slows every line, so a 5 s wall-clock bound on 50,000 lines
    failed there with nothing wrong (2.1 s serially without coverage)."""
    import time

    def cpu_seconds(n: int) -> float:
        text = "password=x\n" * n
        t0 = time.process_time()
        out = scrub.scrub_text(text)
        elapsed = time.process_time() - t0
        assert out.count("${LAKEBENCH_CREDENTIAL}") == n
        return elapsed

    cpu_seconds(100)  # one-time costs (compiled patterns) out of the ratio
    small, big = cpu_seconds(5_000), cpu_seconds(40_000)
    # Linear measured about 8x; quadratic is 64x.
    assert big < 16 * max(small, 0.01), (small, big)


@pytest.mark.parametrize(
    "text",
    [
        "password => 'hunter2'",
        "password := hunter2",
        "password:= hunter2",
        "password: !!str hunter2",
        "password: &a hunter2",
        "password: # c\n  hunter2",
        "--secret-key \\\n    hunter2",
        'password="${X}"hunter2',
        "password: 'it''s-hunter2'",
        '\\"password\\": \\"ab\\\\\\"cd-hunter2\\"',
        '{\\\\\\"password\\\\\\": \\\\\\"hunter2\\\\\\"}',
    ],
)
def test_fifth_pass_text_shapes_leave_no_secret(text: str) -> None:
    """Final pass: a prefix replaced with the secret kept behind it, a quote
    ended early, or a doubly escaped key. Scrubbed away or refused."""
    try:
        out = scrub.scrub_text(text)
    except scrub.ScrubError:
        return
    assert "hunter2" not in out


@pytest.mark.parametrize(
    "text",
    [
        "Missing credentials:\nRetrying in 5s",
        "ERROR: invalid token:\nsee docs",
        "secret: \n\nfoo",
        "secret not found",
        "token budget exceeded",
        "password_policy=strict",
        "Invalid password: authentication failed",
        'cfg {"password": ""}',
        "credentials:\n  user: x",
    ],
)
def test_prose_is_not_refused(text: str) -> None:
    scrub.scrub_text(text)


def test_many_empty_quoted_keys_scrub_in_linear_time() -> None:
    import contextlib
    import time

    t0 = time.monotonic()
    with contextlib.suppress(scrub.ScrubError):
        scrub.scrub_text("password: ''#" * 8000)
    assert time.monotonic() - t0 < 2.0


@pytest.mark.parametrize(
    "text",
    [
        "password: [ hunter2 ]",
        '"credentials": [ "hunter2" ]',
        "password: - hunter2",
        "password: ( hunter2 )",
        "password: < hunter2 >",
        "password: ? hunter2",
        "password: *alias hunter2",
        "password: !<tag:x> hunter2",
        "token: { value: hunter2 }",
        'password: "" hunter2',
        "password: '' hunter2",
        '--password "" hunter2',
        "password:\xa0hunter2",
        "password\xa0= hunter2",
        "password: \u2028  hunter2",
        "password:\x0bhunter2",
        "password:\x0chunter2",
        "password:\n\u3000hunter2",
        "password:\r  hunter2",
        "password: #c\n  hunter2",
    ],
)
def test_last_pass_text_shapes_leave_no_secret(text: str) -> None:
    try:
        out = scrub.scrub_text(text)
    except scrub.ScrubError:
        return
    assert "hunter2" not in out


@pytest.mark.parametrize(
    "text",
    ['  PASSWORD=""\n  echo hi', 'INFO password: ""\n    retrying with default'],
)
def test_empty_quoted_value_is_not_refused(text: str) -> None:
    assert scrub.scrub_text(text) == text


@pytest.mark.parametrize("text", ["h://" + ":" * 100_000, "password: #c " * 20_000])
def test_pathological_lines_stay_linear(text: str) -> None:
    import contextlib
    import time

    t0 = time.monotonic()
    with contextlib.suppress(scrub.ScrubError):
        scrub.scrub_text(text)
    assert time.monotonic() - t0 < 3.0


@pytest.mark.parametrize("pair", sorted(PAIRS, key=lambda p: int(p[1:])))
def test_pinned_pair_verdicts(pair: str) -> None:
    """EVD-7 acceptance: P1 to P12 give the reviewed After verdict through
    the ladder. The expected file is never regenerated by code."""
    from lakebench.metrics import comparability as cmp

    spec = PAIRS[pair]
    want = spec["after"]
    got = cmp.pair_verdict([sr.load_record(spec["a"])], [sr.load_record(spec["b"])])
    assert (got.verdict, got.code, got.step) == (want["verdict"], want["code"], want["step"])
    assert got.attribution == want["attribution"]
    for group in ("workload", "corpus", "architecture", "conditions"):
        assert got.keys(group) == want[group], group
    assert got.system == want["system"]
    assert got.notes == want["notes"]
    kind = want.get("reason_kind")
    if kind:
        side, what = kind.split("_")
        assert len(got.reasons) == 1 and got.reasons[0].startswith(f"{side} run ")
        assert ("did not pass" if what == "failed" else "predates the experiment block") in (
            got.reasons[0]
        )


def test_every_changed_pinned_verdict_is_listed() -> None:
    """RR-2: a pair whose verdict class moves from Before to After is one of
    the pairs UPGRADING names (ch03 section 0.3: P1 and P3 like-for-like
    become architecture differentials with the same class; P5 moves on the
    compaction operation and P2 on the derived benchmark rounds)."""
    moved = []
    for name, spec in PAIRS.items():
        before = spec["before"]
        was = (
            "NOT COMPARABLE"
            if before["verdict"] == "not_comparable"
            else ("LIKE-FOR-LIKE" if before["like_for_like"] else "NOT LIKE-FOR-LIKE")
        )
        if was != spec["after"]["verdict"]:
            moved.append(name)
    assert moved == ["P2", "P5"]


def _with_system_identity(rec: dict, endpoint: str) -> dict:
    from lakebench.metrics.system_identity import fingerprint_of

    parts = {"api_server_ca": "c" * 12, "kubernetes": "v1.31.6", "storage_endpoint": endpoint}
    sysid = {
        "type": "cluster",
        "version": 2,
        "fingerprint": fingerprint_of(parts),
        "partial": True,
        "parts": parts,
    }
    rec["experiment"]["system_identity"] = sysid
    rec["config_snapshot"]["experiment_inputs"]["system_identity"] = json.loads(json.dumps(sysid))
    return rec


def test_scrub_recomputes_the_system_fingerprint() -> None:
    """ER-8 carry (ER-10b): a stored storage endpoint is rewritten and the
    fingerprint recomputed over the rewritten parts, in both places."""
    from lakebench.metrics.system_identity import fingerprint_of

    rec = _with_system_identity(sr.load_record("5105a0"), f"{LAB_ADDR}:80")
    source = rec["experiment"]["system_identity"]["fingerprint"]
    out, changed = scrub.scrub_record(rec)
    for where in (out["experiment"], out["config_snapshot"]["experiment_inputs"]):
        sysid = where["system_identity"]
        assert sysid["parts"]["storage_endpoint"] == "10.0.1.50:80"
        assert sysid["fingerprint"] == fingerprint_of(sysid["parts"]) != source
    assert ".experiment.system_identity.fingerprint" in changed
    assert scrub.check_clean(out) == []


def test_scrub_recomputes_the_fingerprint_inside_a_v2_identity() -> None:
    """An exp2 identity carries the system fingerprint: the recompute is the
    one identity change the guard lets through."""
    from tests.test_comparability import _fresh

    rec = _fresh().to_dict()
    _with_system_identity(rec, f"{LAB_ADDR}:80")
    rec["experiment"]["system_identity"]["parts"]["kubernetes"] = "v1.31.6"
    out, _ = scrub.scrub_record(rec)
    assert out["experiment"]["schema"] == "exp2"
    assert out["experiment"]["system_identity"]["parts"]["storage_endpoint"] == "10.0.1.50:80"


def test_scrub_refuses_an_inconsistent_source_fingerprint() -> None:
    rec = _with_system_identity(sr.load_record("5105a0"), f"{LAB_ADDR}:80")
    rec["experiment"]["system_identity"]["fingerprint"] = "0" * 16
    with pytest.raises(scrub.ScrubError, match="not the hash of its parts"):
        scrub.scrub_record(rec)


def test_scrub_refuses_a_rewrite_of_another_part() -> None:
    rec = _with_system_identity(sr.load_record("5105a0"), "10.0.1.50:80")
    from lakebench.metrics.system_identity import fingerprint_of

    # Inside the experiment block the evidence guard refuses first; the
    # run-start copy in the snapshot reaches the parts rule.
    sysid = rec["config_snapshot"]["experiment_inputs"]["system_identity"]
    sysid["parts"]["storage_server"] = f"gw {LAB_ADDR}"
    sysid["fingerprint"] = fingerprint_of(sysid["parts"])
    with pytest.raises(scrub.ScrubError, match="parts other than an endpoint"):
        scrub.scrub_record(rec)


def test_scrub_recomputes_every_system_identity_copy() -> None:
    """The snapshot copy inside pipeline_benchmark is a third copy."""
    from lakebench.metrics.system_identity import fingerprint_of

    rec = _with_system_identity(sr.load_record("5105a0"), f"{LAB_ADDR}:80")
    pb_inputs = rec["pipeline_benchmark"]["config_snapshot"].setdefault("experiment_inputs", {})
    pb_inputs["system_identity"] = json.loads(json.dumps(rec["experiment"]["system_identity"]))
    out, _ = scrub.scrub_record(rec)
    copies = scrub._system_identity_paths(out)
    assert len(copies) == 3
    for path in copies:
        sysid = scrub._at(out, path)
        assert sysid["fingerprint"] == fingerprint_of(sysid["parts"]), path
