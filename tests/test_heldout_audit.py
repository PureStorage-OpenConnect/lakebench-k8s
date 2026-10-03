"""scripts/aml_heldout_audit.py (SAF-11): read-only, ledger buckets only.

TEST VALUES ONLY: the held-out record is the test fixture; no real seed,
bucket or key is used. A fake S3 records every call, so the test proves that
only the two ledger configs' buckets are touched and a third (foreign) bucket
holding a held-out corpus is never listed or read.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from tests.conftest import exec_repo_script
from tests.fixtures import heldout_test_seeds as ts
from tests.fixtures import protected_corpus as pc

ROOT = Path(__file__).resolve().parents[1]
RECORD = ROOT / "tests/fixtures/records/run-20260927-011123-497f02/metrics.json"


@pytest.fixture
def audit_mod():
    return exec_repo_script(ROOT / "scripts/aml_heldout_audit.py", "aml_heldout_audit")


def _manifest(rows) -> bytes:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table(
        {"typology_id": [r[0] for r in rows], "seed": pa.array([r[1] for r in rows], pa.int64())}
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


class FakeS3:
    """Buckets as {bucket: {key: bytes}}; every call is recorded."""

    def __init__(self, store):
        self.store = store
        self.calls: list[tuple[str, str]] = []

    def list_objects_v2(self, Bucket, Prefix, **kw):  # noqa: N803 -- boto3's names
        self.calls.append(("list", Bucket))
        keys = sorted(k for k in self.store.get(Bucket, {}) if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def get_object(self, Bucket, Key):  # noqa: N803
        self.calls.append(("get", Bucket))
        return {"Body": io.BytesIO(self.store[Bucket][Key])}


def _config(path: Path, name: str, bronze: str, gold: str, schema="financial") -> Path:
    path.write_text(
        f"name: {name}\n"
        "platform:\n  storage:\n    s3:\n"
        "      endpoint: http://10.0.1.50\n"
        "      access_key: ${LB_TEST_AK:-placeholder}\n"
        "      secret_key: ${LB_TEST_SK:-placeholder}\n"
        f"      buckets:\n        bronze: {bronze}\n        gold: {gold}\n        silver: x\n"
        f"workload:\n  schema: {schema}\n"
    )
    return path


@pytest.fixture
def world(tmp_path, monkeypatch):
    pc.use_heldout(monkeypatch)
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "looks.jsonl"))
    monkeypatch.setenv("LB_AML_CORPORA_LEDGER", str(tmp_path / "corpora.jsonl"))
    cfgs = tmp_path / "notes" / "configs"
    cfgs.mkdir(parents=True)
    a = _config(cfgs / "a.yaml", "lb-a", "lb-a-bronze", "lb-a-gold")
    b = _config(cfgs / "b.yaml", "lb-b", "lb-b-bronze", "lb-b-gold")
    ledger = tmp_path / "notes" / "EVIDENCE-test.md"
    ledger.write_text(
        "# Evidence\n\n## Deployments ledger\n\n"
        "| Namespace | Config path | Session or lane | Scale | Deployed |\n"
        "|---|---|---|---|---|\n"
        f"| lb-a | notes/configs/{a.name} | test | 1 | 2026-10-02 |\n\n"
        "## Deployments ledger entry -- lb-b\n\n"
        "- Namespace: lb-b\n"
        f"- Config: {b}\n\n"
        "## Deployments ledger (free text)\n\nnothing here parses\n"
    )
    store = {
        "lb-a-bronze": {"pacs008/manifest/manifest.parquet": _manifest(ts.manifest_rows(43, 20))},
        "lb-a-gold": {"scoring/run-1/recall.json": b"{}"},
        # The evaluation corpus, scored: found.
        "lb-b-bronze": {
            "pacs008/manifest/manifest.parquet": _manifest(
                ts.manifest_rows(43, 300) + ts.manifest_rows(pc.EV, 3, start=300)
            )
        },
        "lb-b-gold": {"scoring/run-2/recall.json": b"{}"},
        # Foreign: never touched, though it holds a held-out corpus.
        "someone-elses-bronze": {
            "pacs008/manifest/manifest.parquet": _manifest(ts.manifest_rows(pc.RB, 5))
        },
    }
    fake = FakeS3(store)
    runs = tmp_path / "runs"
    for run_id, role in (
        ("20261002-000001-aaaaaa", None),
        ("20261002-000002-bbbbbb", "evaluation"),
    ):
        rec = json.loads(RECORD.read_text())
        rec["run_id"] = run_id
        rec["experiment"]["corpus"]["corpus_role"] = role
        (runs / f"run-{run_id}").mkdir(parents=True)
        (runs / f"run-{run_id}" / "metrics.json").write_text(json.dumps(rec))
    journals = tmp_path / "journal"
    journals.mkdir()
    (journals / "session-lb-a.jsonl").write_text(
        json.dumps(
            {
                "event_type": "session.start",
                "session_id": "s-1",
                "details": {"config_file": str(a)},
            }
        )
        + "\n"
    )
    (journals / "session-gone.jsonl").write_text(
        json.dumps(
            {
                "event_type": "session.start",
                "session_id": "s-2",
                "details": {"config_file": str(tmp_path / "deleted.yaml")},
            }
        )
        + "\n"
        + json.dumps({"event_type": "command.start", "details": {"note": f"seed {pc.RB}"}})
        + "\n"
    )
    return {"ledger": ledger, "fake": fake, "runs": runs, "journals": journals, "tmp": tmp_path}


def test_audit_touches_only_the_ledger_configs_buckets(audit_mod, world):
    audit = audit_mod.run_audit(
        [world["runs"]], [world["journals"]], [world["ledger"]], lambda s3: world["fake"]
    )
    touched = {b for _, b in world["fake"].calls}
    assert touched == {"lb-a-bronze", "lb-a-gold", "lb-b-bronze", "lb-b-gold"}
    assert "someone-elses-bronze" not in touched
    doc = audit.to_dict()
    # The planted protected-role record and the scored evaluation corpus.
    names = {r.get("run_id") or r.get("bucket_path") for r in doc["protected_scored_runs"]}
    assert "20261002-000002-bbbbbb" in names
    assert "s3://lb-b-gold/scoring/run-2/recall.json" in names
    assert not any("lb-a" in str(r) for r in doc["protected_scored_runs"])
    # The held-out seed in a journal is found by hash; the deleted config listed.
    assert any(f["kind"] == "seed_token" for f in doc["findings"])
    assert any("config unavailable" in s["reason"] for s in doc["skipped"])
    assert any("no parsable row" in s["reason"] for s in doc["skipped"])
    assert doc["counts"]["ledger_rows"] == 2


def test_scoped_client_refuses_a_foreign_bucket(audit_mod, world):
    scoped = audit_mod.ScopedS3(world["fake"], {"lb-a-bronze"})
    with pytest.raises(audit_mod.ForeignBucket):
        scoped.keys("someone-elses-bronze", "")
    with pytest.raises(audit_mod.ForeignBucket):
        scoped.read("someone-elses-bronze", "k")
    assert world["fake"].calls == []


def test_output_holds_no_seed(audit_mod, world, capsys):
    out = world["tmp"] / "audit.json"
    audit_mod.default_client_factory = lambda s3: world["fake"]
    rc = audit_mod.main(
        [
            "--runs-dir",
            str(world["runs"]),
            "--journal-dir",
            str(world["journals"]),
            "--ledger",
            str(world["ledger"]),
            "--out",
            str(out),
        ]
    )
    printed = capsys.readouterr().out
    assert rc == 1  # protected runs were found
    for text in (printed, out.read_text()):
        assert pc.seed_tokens(text) == []
        assert "placeholder" not in text
    assert {b for _, b in world["fake"].calls} <= {
        "lb-a-bronze",
        "lb-a-gold",
        "lb-b-bronze",
        "lb-b-gold",
    }


def test_clean_world_exits_zero(audit_mod, tmp_path, monkeypatch):
    pc.use_heldout(monkeypatch)
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "looks.jsonl"))
    monkeypatch.setenv("LB_AML_CORPORA_LEDGER", str(tmp_path / "corpora.jsonl"))
    runs = tmp_path / "runs"
    rec = json.loads(RECORD.read_text())
    (runs / "run-x").mkdir(parents=True)
    (runs / "run-x" / "metrics.json").write_text(json.dumps(rec))
    audit = audit_mod.run_audit([runs], [], [], lambda s3: None)
    assert audit.protected_scored_runs == [] and not audit.incomplete


def test_raw_config_fields_follow_the_loader(audit_mod, tmp_path):
    flat = tmp_path / "flat.yaml"
    flat.write_text(f"name: x\nworkload:\n  schema: financial\n  datagen:\n    seed: {pc.EV}\n")
    assert audit_mod.raw_corpus_fields(flat) == ("financial", pc.EV, None)
    nested = tmp_path / "nested.yaml"
    nested.write_text(
        "name: x\narchitecture:\n  workload:\n    schema: financial\n"
        "    datagen:\n      corpus_role: robustness\n"
    )
    assert audit_mod.raw_corpus_fields(nested) == ("financial", None, "robustness")


# -- review regressions: none of these may read as clean ---------------------


def _env(tmp_path, monkeypatch):
    pc.use_heldout(monkeypatch)
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "looks.jsonl"))
    monkeypatch.setenv("LB_AML_CORPORA_LEDGER", str(tmp_path / "corpora.jsonl"))


def _journal(dirpath: Path, config: Path | str) -> Path:
    dirpath.mkdir(parents=True, exist_ok=True)
    (dirpath / "session-x.jsonl").write_text(
        json.dumps(
            {
                "event_type": "session.start",
                "session_id": "s1",
                "details": {"config_file": str(config)},
            }
        )
        + "\n"
    )
    return dirpath


def _rc(audit_mod, tmp_path, *extra):
    """main() over tmp paths only: never the host's own journals or runs."""
    base = ["--runs-dir", str(tmp_path / "none"), "--journal-dir", str(tmp_path / "no-journal")]
    return audit_mod.main([*base, "--out", str(tmp_path / "o.json"), *extra])


def test_unreadable_record_is_not_clean(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    d = tmp_path / "runs" / "run-x"
    d.mkdir(parents=True)
    (d / "metrics.json").write_text('{"run_id": "x", "experiment": {"corpus": {"corpus_role": "ev')
    rc = audit_mod.main(
        ["--runs-dir", str(tmp_path / "runs"), "--journal-dir", str(tmp_path / "j")]
    )
    assert rc == 2


def test_split_workload_blocks_are_both_read(audit_mod, tmp_path):
    c = tmp_path / "c.yaml"
    c.write_text(
        "name: x\narchitecture:\n  workload:\n    schema: financial\n"
        "workload:\n  datagen:\n    corpus_role: evaluation\n"
    )
    assert audit_mod.raw_config_reason(audit_mod._raw_config(c)) == "corpus_role evaluation"


def test_journal_config_with_unset_variables_is_still_checked(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    monkeypatch.delenv("LB_PROBE_UNSET", raising=False)
    c = tmp_path / "c.yaml"
    c.write_text(
        "name: x\nplatform:\n  storage:\n    s3:\n      access_key: ${LB_PROBE_UNSET}\n"
        "workload:\n  schema: financial\n  datagen:\n    corpus_role: evaluation\n"
    )
    assert _rc(audit_mod, tmp_path, "--journal-dir", str(_journal(tmp_path / "j", c))) == 1


def test_journal_config_on_a_registered_prefix_is_found(audit_mod, tmp_path, monkeypatch):
    from lakebench.config import datagen_seed as ds

    _env(tmp_path, monkeypatch)
    ds.append_corpus_ledger(
        {
            "kind": "registered_corpus",
            "state": "generated",
            "role": "evaluation",
            "bronze_uri": "s3://lb-reg-bronze/pacs008/",
            "attempt": "a1",
        }
    )
    c = tmp_path / "dev.yaml"
    c.write_text(
        "name: dev\nplatform:\n  storage:\n    s3:\n      buckets:\n        bronze: lb-reg-bronze\n"
        "workload:\n  schema: financial\n"
    )
    assert _rc(audit_mod, tmp_path, "--journal-dir", str(_journal(tmp_path / "j", c))) == 1


def test_relative_journal_config_resolves_beside_lakebench_output(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    (tmp_path / "c.yaml").write_text(
        "name: x\nworkload:\n  schema: financial\n  datagen:\n    corpus_role: robustness\n"
    )
    j = _journal(tmp_path / "lakebench-output" / "journal", "c.yaml")
    assert _rc(audit_mod, tmp_path, "--journal-dir", str(j)) == 1


def test_ledger_shapes_found_in_real_ledgers(audit_mod, tmp_path):
    led = tmp_path / "E.md"
    led.write_text(
        "## pipe-aml-c1 deployment ledger entry\n"
        "- namespace: pipe-aml-c1 (destroyed), config: /x/c.yaml\n- session: s\n\n"
        "## smoke ledger\nNamespace: ds-1\nConfig: /x/d.yaml\n\n"
        "## AML batch redo\n- Namespace: lb-redo\n- Config: /x/e.yaml\n\n"
        "## Deployments ledger\n\nfree text only\n"
    )
    rows, unparsed = audit_mod.parse_ledger(led)
    assert {(r.namespace, r.config) for r in rows} == {
        ("pipe-aml-c1", "/x/c.yaml"),
        ("ds-1", "/x/d.yaml"),
        ("lb-redo", "/x/e.yaml"),
    }
    assert unparsed == ["Deployments ledger"]


def test_an_unparsable_ledger_section_is_not_clean(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    led = tmp_path / "E.md"
    led.write_text("## Deployments ledger\n\nsomething that is not a row\n")
    assert _rc(audit_mod, tmp_path, "--ledger", str(led)) == 2


def test_a_non_utf8_journal_does_not_crash(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    j = tmp_path / "j"
    j.mkdir()
    (j / "session-bin.jsonl").write_bytes(b"\xff\xfe not json\n")
    assert _rc(audit_mod, tmp_path, "--journal-dir", str(j)) in (0, 2)


def _sessions(path: Path, *sessions) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for sid, config, extra in sessions:
        lines.append(
            json.dumps(
                {
                    "event_type": "session.start",
                    "session_id": sid,
                    "details": {"config_file": str(config), **extra},
                }
            )
        )
    path.write_text("\n".join(lines) + "\n")
    return path.parent


def test_every_session_of_a_journal_is_checked(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    dev = tmp_path / "dev.yaml"
    dev.write_text("name: x\nworkload:\n  schema: financial\n")
    ev = tmp_path / "eval.yaml"
    ev.write_text(
        "name: x\nworkload:\n  schema: financial\n  datagen:\n    corpus_role: evaluation\n"
    )
    j = _sessions(tmp_path / "j" / "session-x.jsonl", ("s1", dev, {}), ("s2", ev, {}))
    assert _rc(audit_mod, tmp_path, "--journal-dir", str(j)) == 1


def test_variables_in_the_corpus_fields_are_substituted(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    c = tmp_path / "c.yaml"
    c.write_text(
        "name: x\nworkload:\n  schema: financial\n  datagen:\n    corpus_role: ${LB_T_ROLE:-evaluation}\n"
    )
    assert audit_mod.raw_config_reason(audit_mod._raw_config(c)) == "corpus_role evaluation"
    monkeypatch.setenv("LB_T_SEED", str(pc.RB))
    c.write_text("name: x\nworkload:\n  schema: financial\n  datagen:\n    seed: ${LB_T_SEED}\n")
    assert "robustness" in audit_mod.raw_config_reason(audit_mod._raw_config(c))
    monkeypatch.delenv("LB_T_SEED")
    with pytest.raises(audit_mod.Unresolved):
        audit_mod.raw_config_reason(audit_mod._raw_config(c))
    j = _sessions(tmp_path / "j" / "session-x.jsonl", ("s1", c, {}))
    assert _rc(audit_mod, tmp_path, "--journal-dir", str(j)) == 2


def test_a_ledger_row_config_naming_a_protected_role_is_found(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    c = tmp_path / "eval.yaml"
    c.write_text(
        "name: lb-eval\nworkload:\n  schema: financial\n  datagen:\n    corpus_role: evaluation\n"
    )
    led = tmp_path / "E.md"
    led.write_text(f"## Deployments ledger\n\n- Namespace: lb-eval\n- Config: {c}\n")
    audit = audit_mod.run_audit([], [], [led], lambda s3: FakeS3({}))
    assert any(f["kind"] == "protected_config" for f in audit.findings)


def test_a_config_changed_since_its_session_is_not_clean(audit_mod, tmp_path, monkeypatch):
    _env(tmp_path, monkeypatch)
    c = tmp_path / "c.yaml"
    c.write_text("name: x\nworkload:\n  schema: financial\n")
    j = _sessions(tmp_path / "j" / "session-x.jsonl", ("s1", c, {"config_hash": "0" * 16}))
    assert _rc(audit_mod, tmp_path, "--journal-dir", str(j)) == 2


def test_a_nameless_config_uses_the_session_name_for_its_bucket(audit_mod, tmp_path, monkeypatch):
    from lakebench.config import datagen_seed as ds

    _env(tmp_path, monkeypatch)
    ds.append_corpus_ledger(
        {
            "kind": "registered_corpus",
            "state": "generated",
            "role": "robustness",
            "bronze_uri": "s3://lb-reg-bronze/pacs008/",
            "attempt": "a1",
        }
    )
    c = tmp_path / "c.yaml"
    c.write_text("workload:\n  schema: financial\n")
    j = _sessions(tmp_path / "j" / "session-x.jsonl", ("s1", c, {"config_name": "lb-reg"}))
    assert _rc(audit_mod, tmp_path, "--journal-dir", str(j)) == 1
