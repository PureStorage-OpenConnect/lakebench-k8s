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

from lakebench.metrics.experiment import experiment_of, identity_hash
from tests.fixtures import scrub
from tests.fixtures import stored_records as sr

EXPECTED = sr.expected("records")
DRIFT = set(EXPECTED["known_rebuild_drift"]["runs"])
FIXTURE_JSON = sorted(Path(sr.RECORDS_DIR).parent.rglob("*.json"))

LAB_ADDR = "192.0.2.15"  # RFC 5737 documentation address standing in for a lab one

SEED = 987654  # stands in for a held-out seed; stubbed, never a real one
FAKE = "Ab1x" * 10  # 40 chars, not a key
SECRET = "abc/def+ghi" * 4


def _put(rec, path, value):
    node = rec
    for key in path[:-1]:
        node = node.setdefault(key, {}) if isinstance(node, dict) else node[key]
    node[path[-1]] = value


def _set(path, value):
    """A mutator that writes *value* at *path* of a record."""
    return lambda rec: _put(rec, path, value)


def _all(*mutators):
    def mutate(rec):
        for m in mutators:
            m(rec)

    return mutate


def _buckets(**names):
    return _all(*[_set(("config_snapshot", "s3", "buckets", k), v) for k, v in names.items()])


_S3 = ("config_snapshot", "s3")
_MSG = ("jobs", 0, "error_message")
_EXTRA = ("config_snapshot", "extra")


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
    (S1). A record listed in known_rebuild_drift must still drift, so the
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


def test_bucket_names_replaced_only_as_whole_tokens() -> None:
    """Default bucket names are prefixes of job names: a bucket
    lakebench-bronze must not turn job lakebench-bronze-verify into
    scrubbed-bronze-verify."""
    rec = sr.load_record("5105a0")
    _buckets(bronze="lakebench-bronze", silver="lakebench-silver", gold="lakebench-gold")(rec)
    rec["jobs"][0]["job_name"] = "lakebench-bronze-verify"
    rec["jobs"][0]["error_message"] = "listing s3a://lakebench-bronze/x and lakebench-silver/y"
    out, _ = scrub.scrub_record(rec)
    assert out["jobs"][0]["job_name"] == "lakebench-bronze-verify"
    assert out["experiment"] == rec["experiment"]
    assert out["jobs"][0]["error_message"] == (
        "listing s3a://scrubbed-bronze/x and scrubbed-silver/y"
    )


def _image_registry_in_experiment(rec) -> None:
    corpus = rec["experiment"]["corpus"]
    corpus["generator_image"] = f"{LAB_ADDR}:5000/{corpus['generator_image']}"


def _image_registry_in_run_inputs(rec) -> None:
    corpus = rec["config_snapshot"]["experiment_inputs"]["corpus"]
    corpus["generator_image"] = f"{LAB_ADDR}:5000/{corpus['generator_image']}"


_BY_HOST = {"192.0.2.23": 1}

# (record, mutator, match): a lab value that would have to be rewritten in
# evidence, or a rewrite that would collide or move identity, is refused.
_REFUSALS = [
    pytest.param(
        "5105a0",
        _set(_MSG, "key PSFB" + "A" * 38 + " rejected"),
        "credential format",
        id="credential-under-neutral-key",
    ),
    pytest.param(
        "5105a0", _image_registry_in_experiment, "rewrite evidence", id="registry-in-experiment"
    ),
    pytest.param(
        "5105a0", _image_registry_in_run_inputs, "identity", id="registry-in-rebuilt-identity"
    ),
    pytest.param(
        "5105a0",
        _all(
            _buckets(bronze="lakebench-bronze", silver="lakebench-silver", gold="gb"),
            _set(("jobs", 0, "job_name"), "lakebench-bronze-verify"),
        ),
        "shorter than any valid S3 bucket",
        id="bucket-shorter-than-valid",
    ),
    pytest.param(
        "5105a0",
        _buckets(silver="silver"),
        "bucket name 'silver' is a single word",
        id="bucket-named-like-a-stage",
    ),
    pytest.param(
        "978622",
        _buckets(silver="silver"),
        "is a single word",
        id="legacy-bucket-named-like-a-stage",
    ),
    pytest.param(
        "978622",
        _all(
            _buckets(silver="dep-x-silver"),
            _set(("pipeline_benchmark", "stage_matrix", "dep-x-silver"), {}),
        ),
        "also a key in the record",
        id="legacy-bucket-is-a-key",
    ),
    *[
        pytest.param(
            "978622",
            _all(_buckets(gold="dep-x-gold"), _set(("config_snapshot", value_at), "dep-x-gold")),
            "also a value outside the bucket settings",
            id=f"legacy-bucket-equals-{value_at}",
        )
        for value_at in ("table_format", "catalog", "pipeline_mode")
    ],
    pytest.param(
        "978622",
        _all(
            _buckets(bronze="dep-x-bronze"),
            _set(("pipeline_benchmark", "stages", 0, "stage_name"), "verify s3a://dep-x-bronze"),
        ),
        r"rewrite evidence.*stage_name",
        id="stage-name-is-evidence",
    ),
    pytest.param(
        "5105a0",
        _set(("experiment", "limits", "max_token"), "4096"),
        r"rewrite evidence.*max_token",
        id="credential-named-key-in-experiment",
    ),
    pytest.param(
        "5105a0",
        _all(
            _buckets(bronze="dep-x-bronze"),
            _set(("jobs", 0, "job_name"), "verify s3a://dep-x-bronze"),
        ),
        r"rewrite evidence.*jobs\[0\]\.job_name",
        id="bucket-name-in-a-job-name",
    ),
    pytest.param(
        "5105a0",
        _set(("config_snapshot", "by_host"), {"192.0.2.23": 5, "192.0.2.24": 7}),
        "would merge",
        id="key-rename-merges-keys",
    ),
    pytest.param(
        "5105a0",
        _set(("experiment", "limits", "by_host"), _BY_HOST),
        "rename evidence key",
        id="key-rename-in-experiment",
    ),
    pytest.param(
        "5105a0",
        _set(("verdict", "by_host"), _BY_HOST),
        "rename evidence key",
        id="key-rename-in-verdict",
    ),
]


@pytest.mark.parametrize(("record_id", "mutate", "match"), _REFUSALS)
def test_scrub_refuses(record_id, mutate, match) -> None:
    rec = sr.load_record(record_id)
    mutate(rec)
    with pytest.raises(scrub.ScrubError, match=match):
        scrub.scrub_record(rec)


# (record inputs, expected values, a check_clean problem the input raises):
# addresses, hosts and key names are rewritten to the placeholder; look-alikes
# are left alone.
_REWRITES = [
    pytest.param(
        {_MSG: "Connection refused by 192.0.2.25."},
        {_MSG: "Connection refused by 10.0.1.50."},
        "private address",
        id="address-before-a-full-stop",
    ),
    pytest.param(
        {_MSG: "via 100.64.3.4 and 100.128.0.1"},
        {_MSG: "via 10.0.1.50 and 100.128.0.1"},
        None,
        id="cgnat-address",
    ),
    pytest.param(
        {
            (*_S3, "endpoint"): "http://fb.lab.corp:80",
            _MSG: "GET https://mybkt.fb.lab.corp/k via ns1.fb.lab.corp",
        },
        {_MSG: "GET https://10.0.1.50/k via 10.0.1.50"},
        None,
        id="subdomain-of-an-endpoint-host",
    ),
    pytest.param(
        {
            (*_EXTRA, "s3_url"): "http://fb.lab.corp:80",
            (*_EXTRA, "endpoints"): "fb2.lab.corp:80",
        },
        {_EXTRA: {"s3_url": "http://10.0.1.50:80", "endpoints": "10.0.1.50:80"}},
        None,
        id="s3-url-and-endpoints-keys",
    ),
    pytest.param(
        {
            (*_S3, "endpoint"): "http://user:pw@fb.lab.corp:80",
            _MSG: "GET https://bob:hunter2@mirror.example.org/x failed",
        },
        {
            (*_S3, "endpoint"): "http://10.0.1.50:80",
            _MSG: "GET https://mirror.example.org/x failed",
        },
        "user-info",
        id="url-userinfo-dropped",
    ),
    pytest.param(
        {(*_EXTRA, "jdk"): "17.0.12.7", (*_EXTRA, "metrics_endpoint"): "/metrics"},
        {_EXTRA: {"jdk": "17.0.12.7", "metrics_endpoint": "/metrics"}},
        None,
        id="non-addresses-left-alone",
    ),
    pytest.param(
        {
            (*_S3, "endpoint"): "http://minio:9000",
            ("config_snapshot", "note"): "backend minio",
            ("config_snapshot", "minio"): {"image": "x"},
        },
        {
            (*_S3, "endpoint"): "http://10.0.1.50:9000",
            ("config_snapshot", "note"): "backend minio",
            ("config_snapshot", "minio"): {"image": "x"},
        },
        None,
        id="dotless-host-is-not-a-global-token",
    ),
    pytest.param(
        {(*_S3, "endpoint"): "http://s3.lab.example:80", _MSG: "S3.LAB.EXAMPLE refused"},
        {_MSG: "10.0.1.50 refused"},
        None,
        id="host-matched-case-insensitively",
    ),
    pytest.param(
        {
            (*_S3, "endpoint"): "http://fb01:80",
            _MSG: "GET http://FB01:80/a failed; fb01 busy",
        },
        {_MSG: "GET http://10.0.1.50:80/a failed; fb01 busy"},
        None,
        id="dotless-host-rewritten-inside-urls",
    ),
    pytest.param(
        {("experimental",): {"192.0.2.23": 1}},
        {("experimental",): {"10.0.1.50": 1}},
        None,
        id="experiment-lookalike-key-renamed",
    ),
]


@pytest.mark.parametrize(("inputs", "expected", "flagged"), _REWRITES)
def test_scrub_rewrites_record_values(inputs, expected, flagged) -> None:
    rec = sr.load_record("5105a0")
    for path, value in inputs.items():
        _put(rec, path, value)
    if flagged:
        assert any(flagged in p for p in scrub.check_clean(rec))
    out, _ = scrub.scrub_record(rec)
    for path, value in expected.items():
        node = out
        for key in path:
            node = node[key]
        assert node == value, path


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("ok 192.0.2.25, 192.0.2.26.", "ok 10.0.1.50, 10.0.1.50."),
        # Not part of a longer dotted run.
        ("jdk 1.192.0.2.25 and 192.0.2.25.1", "jdk 1.192.0.2.25 and 192.0.2.25.1"),
        ("via 010.099.007.005", "via 10.0.1.50"),
        ("credentials:\n  user: x", "credentials:\n  user: x"),
        ("password=$3cr3tValue end", "password=${LAKEBENCH_CREDENTIAL} end"),
        ('password="my secret pass" end', 'password="${LAKEBENCH_CREDENTIAL}" end'),
        ("password=abc,def end", "password=${LAKEBENCH_CREDENTIAL} end"),
        (
            "token=secret=" + "x" * 40 + " next_field=important",
            "token=${LAKEBENCH_CREDENTIAL} next_field=important",
        ),
        # A swallowed quoted pair must not leak its value.
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
        (
            "conf javax.jdo.option.ConnectionPassword=hive done",
            "conf javax.jdo.option.ConnectionPassword=${LAKEBENCH_CREDENTIAL} done",
        ),
        (
            "conf trustStorePassword=Abcdefgh!secretTail done",
            "conf trustStorePassword=${LAKEBENCH_CREDENTIAL} done",
        ),
        ("conf s3SecretKey=abc done", "conf s3SecretKey=${LAKEBENCH_S3_SECRET_KEY} done"),
        ("conf rootPassword: x9 done", "conf rootPassword: ${LAKEBENCH_CREDENTIAL} done"),
        (
            "2026-10-01T10:00:00Z-http://AKIAFAKE:sekrit@192.0.2.27/ -https://u:pw@h/x",
            "2026-10-01T10:00:00Z-http://10.0.1.50/ -https://h/x",
        ),
    ],
)
def test_scrub_text_rewrites_exactly(text, expected) -> None:
    assert scrub.scrub_text(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "password_policy=strict-mode access_key_id=${LAKEBENCH_S3_ACCESS_KEY} "
        "fs.s3a.aws.credentials.provider=org.apache.Simple v1.192.0.2.10",
        "Missing credentials:\nRetrying in 5s",
        "ERROR: invalid token:\nsee docs",
        "secret: \n\nfoo",
        "secret not found",
        "token budget exceeded",
        "password_policy=strict",
        'cfg {"password": ""}',
        '  PASSWORD=""\n  echo hi',
        'INFO password: ""\n    retrying with default',
    ],
)
def test_prose_is_not_refused_or_rewritten(text) -> None:
    assert scrub.scrub_text(text) == text


def test_prose_naming_a_password_is_not_refused() -> None:
    scrub.scrub_text("Invalid password: authentication failed")


_CREDENTIAL_TEXT_REFUSALS = [
    ("env:\n- name: X\n  value: " + FAKE + "\n", "credential format"),
    ("Authorization: Basic aHVudGVyMnNlY3JldA==", "credential format"),
    ("Authorization: Bearer abc.def.ghi", "credential format"),
    ("s3a://MYACCESS:hunter2/secret@bucket/path", "user-info"),
]


@pytest.mark.parametrize(("text", "match"), _CREDENTIAL_TEXT_REFUSALS)
def test_credential_text_is_refused(text, match) -> None:
    with pytest.raises(scrub.ScrubError, match=match):
        scrub.scrub_text(text)
    rec = sr.load_record("5105a0")
    _put(rec, _MSG, text)
    assert any(match in p for p in scrub.check_clean(rec))


def _extra_with(**kv):
    return _set(_EXTRA, kv)


# (mutator, values that must not survive, placeholder the scrub leaves behind)
_RECORD_SECRETS = [
    pytest.param(
        _extra_with(args=["--conf", f"spark.hadoop.fs.s3a.secret.key={FAKE}"]),
        [FAKE],
        "${LAKEBENCH_S3_SECRET_KEY}",
        id="conf-argv",
    ),
    pytest.param(
        _extra_with(env=[{"key": "AWS_SECRET_ACCESS_KEY", "value": FAKE}]),
        [FAKE],
        "${LAKEBENCH_S3_SECRET_KEY}",
        id="key-value-env",
    ),
    pytest.param(
        _set(_MSG, f"bad config: secretKey: {FAKE}"),
        [FAKE],
        "${LAKEBENCH_S3_SECRET_KEY}",
        id="yaml-text",
    ),
    *[
        pytest.param(
            _all(
                _set(_EXTRA, extra),
                _set(_MSG, "connect to fb.lab.corp timed out"),
            ),
            [SECRET, "PSKEYID", "fb.lab.corp", LAB_ADDR],
            None,
            id=name,
        )
        for name, extra in {
            "dotted-spark-conf": {
                "spark.hadoop.fs.s3a.endpoint": "http://fb.lab.corp:80",
                "spark.hadoop.fs.s3a.secret.key": SECRET,
            },
            "camel-case": {"endpointOverride": "fb.lab.corp:80", "secretAccessKey": SECRET},
            "aws-env-name": {
                "AWS_ENDPOINT_URL_S3": "http://fb.lab.corp",
                "AWS_SECRET_ACCESS_KEY": SECRET,
            },
            "env-list": {
                "env": [
                    {"name": "AWS_SECRET_ACCESS_KEY", "value": SECRET},
                    {"name": "S3_ENDPOINT", "value": "http://fb.lab.corp:80"},
                ]
            },
            "credentials-dict": {
                "credentials": {"id": "PSKEYID", "key": SECRET},
                "endpoint": "http://fb.lab.corp:80",
            },
            "dict-key": {f"{LAB_ADDR}:80": {"endpoint": "http://fb.lab.corp:80"}},
        }.items()
    ],
]


@pytest.mark.parametrize(("mutate", "leaks", "placeholder"), _RECORD_SECRETS)
def test_secret_in_a_record_never_survives(mutate, leaks, placeholder) -> None:
    """.gitleaks.toml's s3-secret-key-assignment and k8s-inline-env shapes,
    the dotted Spark form gitleaks itself misses, and credentials and hosts
    under other key spellings."""
    rec = sr.load_record("5105a0")
    mutate(rec)
    assert scrub.check_clean(rec) != []
    out, _ = scrub.scrub_record(rec)
    text = json.dumps(out)
    for leak in leaks:
        assert leak not in text
    if placeholder:
        assert placeholder in text
    assert scrub.check_clean(out) == []


_TEXT_SHAPES_LEAVE_NO_SECRET_CASES = [
    *[
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
    *[
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
    *[
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
]


@pytest.mark.parametrize(
    "text",
    _TEXT_SHAPES_LEAVE_NO_SECRET_CASES,
    ids=[f"shape-{n}" for n in range(len(_TEXT_SHAPES_LEAVE_NO_SECRET_CASES))],
)
def test_text_shape_leaves_no_secret(text) -> None:
    """Every text shape from the review passes either scrubs the secret away
    or is refused; it never comes back with the secret."""
    try:
        out = scrub.scrub_text(text)
    except scrub.ScrubError:
        return
    assert "hunter2" not in out and "c2VjcmV0" not in out and "part1" not in out


def test_userinfo_glued_to_a_timestamp_leaves_no_secret() -> None:
    out = scrub.scrub_text(
        "2026-10-01T10:00:00Z-http://AKIAFAKE:sekrit@192.0.2.27/ -https://u:pw@h/x"
    )
    assert "sekrit" not in out and "u:pw" not in out


def _use_record(rec, run_id):
    """Swap in a stored record; its own seed is the held-out one."""
    rec.clear()
    rec.update(sr.load_record(run_id))
    return rec["experiment"]["corpus"]["seed"]


# (mutator returning the stubbed seed or None for SEED, role)
_SEED_INJECTIONS = [
    pytest.param(lambda rec: _use_record(rec, "1320bd"), "evaluation", id="stored-seed"),
    *[
        pytest.param(_extra_with(**kv), "robustness", id=name)
        for name, kv in {
            "string": {"seed": "987654"},
            "argv": {"args": "generate --seed 987654 --scale 1"},
            "camel": {"randomSeed": 987654},
            "list": {"instance_seeds": [1, 987654]},
            "second-number": {
                "error": "the corpus was generated with seed 777, not the claimed 987654"
            },
            "seeds-list-text": {"note": "seeds 777, 987654"},
            "hyphen-word": {"note": "aml-seed-987654"},
            "underscore-word": {"note": "aml_seed_987654"},
            "dict-key": {"by_seed": {"987654": "x"}},
            "nested": {"seed": {"evaluation": 987654}},
            "float": {"seed": 987654.0},
        }.items()
    ],
    *[
        pytest.param(_extra_with(**kv), "evaluation", id=name)
        for name, kv in {
            "argv-list": {"args": ["generate", "--seed", "987654"]},
            "argv-ints": {"spark_arguments": ["--scale", 1, "--seed", 987654]},
            "seed-equals": {"cmd": "gen seed=987654"},
        }.items()
    ],
    *[
        pytest.param(_set(_MSG, text), "evaluation", id=f"dumped-{n}")
        for n, text in enumerate(
            [
                'cfg {"seed": 987654}',
                "seed = 987654",
                "AML_SEED=987654",
                "datagen_seed: 987654",
            ]
        )
    ],
    pytest.param(
        _set(("config_snapshot", "args"), ["--aml-seed", "987654"]),
        "evaluation",
        id="aml-seed-argv",
    ),
    pytest.param(
        _extra_with(**{"987654": {"seed": 987654}, "run-987654": {"n": "x"}}),
        "robustness",
        id="seed-in-a-dict-key",
    ),
    pytest.param(
        _extra_with(**{"987654": {"host": "192.0.2.27"}, "192.0.2.22-987654": 1}),
        "robustness",
        id="seed-in-keys-with-addresses",
    ),
    pytest.param(
        _all(
            _buckets(gold="lb-987654-x"),
            _extra_with(note="lb-987654-x"),
        ),
        "robustness",
        id="seed-in-a-bucket-name",
    ),
    pytest.param(
        _extra_with(**{"a" * 25 + "10.99.1.2_987654": 1}),
        "robustness",
        id="seed-cut-by-the-key-slice",
    ),
]


@pytest.mark.parametrize(("mutate", "role"), _SEED_INJECTIONS)
def test_held_out_seed_is_refused_and_never_printed(monkeypatch, mutate, role) -> None:
    """Invariant 7: a held-out seed in any shape refuses the record, and no
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
    to an empty check fails here (review L1). A spent seed passes. No
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


def test_key_rename_reported_by_full_path() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["by_host"] = {"192.0.2.23": {"job_name": "x"}}
    out, changed = scrub.scrub_record(rec)
    assert out["config_snapshot"]["by_host"] == {"10.0.1.50": {"job_name": "x"}}
    assert ".config_snapshot.by_host.192.0.2.23" in changed


def test_scrub_text_for_logs() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["endpoint"] = "http://fb.lab.corp:80"
    bucket = rec["config_snapshot"]["s3"]["buckets"]["bronze"]
    log = f"[lb] read s3a://{bucket}/a from fb.lab.corp via {LAB_ADDR}\n"
    out = scrub.scrub_text(log, rec)
    assert out == "[lb] read s3a://scrubbed-bronze/a from 10.0.1.50 via 10.0.1.50\n"
    with pytest.raises(scrub.ScrubError, match="credential format"):
        scrub.scrub_text("key AKIA" + "ABCDEFGHIJKLMNOP")


def test_partial_placeholder_leaf_is_rewritten() -> None:
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["s3"]["secret_key"] = "${X}hunter2secret"
    out, _ = scrub.scrub_record(rec)
    assert out["config_snapshot"]["s3"]["secret_key"] == "${LAKEBENCH_S3_SECRET_KEY}"


def _with_system_identity(rec: dict, endpoint: str) -> dict:
    from lakebench.metrics.system_identity import fingerprint_of

    parts = {"api_server_ca": "c" * 12, "kubernetes": "v1.31.6", "storage_endpoint": endpoint}
    sysid = {
        "type": "cluster",
        "version": 2,
        "fingerprint": fingerprint_of(parts, None, "cluster", 2),
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
        # The identity was stored at v2 so the recomputed fingerprint is
        # over v2 parts (raw host:port); _with_system_identity above pinned
        # that version.
        assert sysid["fingerprint"] == fingerprint_of(sysid["parts"], None, "cluster", 2) != source
    assert ".experiment.system_identity.fingerprint" in changed
    assert scrub.check_clean(out) == []


def test_scrub_recomputes_the_fingerprint_inside_a_v2_identity() -> None:
    """An exp2 identity carries the system fingerprint: the recompute is the
    one identity change the guard lets through."""
    from tests.fixtures.comparability_helpers import _fresh

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


def _system_identities(node):
    if isinstance(node, dict):
        if isinstance(node.get("system_identity"), dict):
            yield node["system_identity"]
        for v in node.values():
            yield from _system_identities(v)
    elif isinstance(node, list):
        for v in node:
            yield from _system_identities(v)


def test_scrub_recomputes_every_system_identity_copy() -> None:
    """The snapshot copy inside pipeline_benchmark is a third copy."""
    from lakebench.metrics.system_identity import fingerprint_of

    rec = _with_system_identity(sr.load_record("5105a0"), f"{LAB_ADDR}:80")
    pb_inputs = rec["pipeline_benchmark"]["config_snapshot"].setdefault("experiment_inputs", {})
    pb_inputs["system_identity"] = json.loads(json.dumps(rec["experiment"]["system_identity"]))
    out, _ = scrub.scrub_record(rec)
    copies = list(_system_identities(out))
    assert copies
    for sysid in copies:
        version = sysid.get("version") or 2
        assert sysid["parts"]["storage_endpoint"] == "10.0.1.50:80"
        assert sysid["fingerprint"] == fingerprint_of(sysid["parts"], None, "cluster", version)


def test_scrubber_accepts_a_storage_block_with_real_bucket_names() -> None:
    """The bucket name is a value in the storage-multiple block, never a key,
    so a record with real-looking bucket names scrubs and keeps every number."""
    real = {
        "bronze": "rel17-m01-d0fc03-bronze",
        "silver": "rel17-m01-d0fc03-silver",
        "gold": "rel17-m01-d0fc03-gold",
    }
    gb = 1024**3
    block = {
        "buckets": [
            {
                "bucket": real["bronze"],
                "layers": ["bronze"],
                "physical_bytes": 15 * gb,
                "unattributed_bytes": 0.0,
                "listing_error": None,
            },
            {
                "bucket": real["silver"],
                "layers": ["silver"],
                "physical_bytes": 11 * gb,
                "unattributed_bytes": gb,
                "listing_error": None,
            },
            {
                "bucket": real["gold"],
                "layers": ["gold"],
                "physical_bytes": None,
                "unattributed_bytes": None,
                "listing_error": "OSError",
            },
        ],
        "tables": [
            {"table": "silver.txn", "location": f"s3a://{real['silver']}/warehouse/silver/txn"}
        ],
        "total": {"physical_bytes": 12 * gb},
        "layers": {"silver": {"multiple": 1.5}},
    }
    rec = sr.load_record("5105a0")
    _buckets(**real)(rec)
    _put(rec, ("pipeline_benchmark", "config_snapshot", "s3", "buckets"), dict(real))
    rec["storage_multiple"] = json.loads(json.dumps(block))

    out, _ = scrub.scrub_record(rec)

    scrubbed = out["storage_multiple"]
    assert [b["bucket"] for b in scrubbed["buckets"]] == [
        "scrubbed-bronze",
        "scrubbed-silver",
        "scrubbed-gold",
    ]
    for got, want in zip(scrubbed["buckets"], block["buckets"], strict=True):
        assert {k: v for k, v in got.items() if k != "bucket"} == {
            k: v for k, v in want.items() if k != "bucket"
        }
    assert scrubbed["tables"][0]["location"] == "s3a://scrubbed-silver/warehouse/silver/txn"
    assert scrubbed["total"] == block["total"] and scrubbed["layers"] == block["layers"]
    assert not any(name in json.dumps(out) for name in real.values())
