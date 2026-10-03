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
    cfgs = tmp_path / "dev-artifacts" / "ledger-configs"
    cfgs.mkdir(parents=True)
    a = _config(cfgs / "a.yaml", "lb-a", "lb-a-bronze", "lb-a-gold")
    b = _config(cfgs / "b.yaml", "lb-b", "lb-b-bronze", "lb-b-gold")
    ledger = tmp_path / "dev-artifacts" / "EVIDENCE-test.md"
    ledger.write_text(
        "# Evidence\n\n## Deployments ledger\n\n"
        "| Namespace | Config path | Session or lane | Scale | Deployed |\n"
        "|---|---|---|---|---|\n"
        f"| lb-a | dev-artifacts/ledger-configs/{a.name} | test | 1 | 2026-10-02 |\n\n"
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
