"""The support record is generated from release-matrix run records
(EVD-12, DESIGN ch03 section 11): kept records are release evidence on a
release-matrix row at that row's Spark minor and table format version,
grouped by the versioned key; every other record is listed with its
reasons; the README table regenerates from the written record."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakebench.config import support
from lakebench.metrics import release_record as rr
from tests.test_release_record import DIGEST, FREEZE, _expected, _release, ready  # noqa: F401

REPO = Path(__file__).resolve().parents[1]


def _spark(rec: dict, image: str) -> dict:
    rec["experiment"]["architecture"]["pipeline_engine"]["image"] = image
    return rec


def _records() -> dict[str, dict]:
    """Two records that are release evidence on their matrix rows, and four
    that are not: a dirty tree, a matrix row at other versions, a row that
    is not in the matrix, and a second, different copy of a kept run."""
    polaris = _release("c360_batch")  # polaris-iceberg-spark-thrift, Spark 4.0
    aml = _spark(_release("aml_batch"), "apache/spark:4.1.1-python3")
    dirty = _release("c360_batch")
    dirty["run_id"] = "20260928-102711-d1d1d1"
    dirty["provenance"]["git_dirty"] = True
    wrong_spark = _release("aml_batch")  # hive AML batch on Spark 4.0; the matrix says 4.1
    wrong_spark["run_id"] = "20260928-130953-0b0b0b"
    off_matrix = _release("c360_batch")
    off_matrix["run_id"] = "20260928-102711-5c5c5c"
    off_matrix["experiment"]["corpus"]["scale"] = 5.0
    twin = json.loads(json.dumps(aml))
    twin["note"] = "a different copy"
    return {
        "polaris": polaris,
        "aml": aml,
        "dirty": dirty,
        "wrong_spark": wrong_spark,
        "off_matrix": off_matrix,
        "twin": twin,
    }


def _write_runs(root: Path, records: dict[str, dict]) -> list[Path]:
    a, b = root / "runs-a", root / "runs-b"
    for name, rec in records.items():
        d = (b if name == "twin" else a) / f"run-{rec['run_id']}"
        d.mkdir(parents=True)
        (d / "metrics.json").write_text(json.dumps(rec))
    return [a, b]


def _expected_file(root: Path, records: dict[str, dict]) -> Path:
    p = root / "expected.json"
    p.write_text(json.dumps(_expected(*records.values())))
    return p


def test_from_records_writes_rows(ready, tmp_path):  # noqa: F811
    records = _records()
    dirs = _write_runs(tmp_path, records)
    expected = _expected(*records.values())
    out = support.rows_from_records(dirs, FREEZE, expected, root=REPO, release_digest=DIGEST)
    assert [(e["workload"], e["recipe"], e["spark"], e["runs"]) for e in out.entries] == [
        ("customer360", "polaris-iceberg-spark-thrift", "4.0", [records["polaris"]["run_id"]]),
        ("financial", "hive-iceberg-spark-trino", "4.1", [records["aml"]["run_id"]]),
    ]
    assert all(e["tree"] == FREEZE and e["table_format_version"] == "1.11.0" for e in out.entries)
    refused = {p.parent.name: " ".join(why) for p, why in out.refused}
    assert out.read == 6 and len(refused) == 4
    assert "modified tree" in refused[f"run-{records['dirty']['run_id']}"]
    assert (
        "the release matrix runs this row on Spark 4.1, Iceberg 1.11.0; the run used "
        "Spark 4.0, Iceberg 1.11.0"
    ) in refused[f"run-{records['wrong_spark']['run_id']}"]
    assert (
        "at scale 5 is not a release-matrix row"
        in refused[f"run-{records['off_matrix']['run_id']}"]
    )
    assert "a second, different record" in refused[f"run-{records['twin']['run_id']}"]


def test_written_record_loads_and_stamps_only_its_versions(ready, tmp_path):  # noqa: F811
    records = _records()
    dirs = _write_runs(tmp_path, records)
    out = support.rows_from_records(
        dirs, FREEZE, _expected(*records.values()), root=REPO, release_digest=DIGEST
    )
    path = tmp_path / "validated_combinations.yaml"
    support.write_record(out.entries, path)
    assert path.read_text().startswith(support.RECORD_HEADER)
    rec = support.load_validation_record(path)
    assert set(rec) == {
        ("customer360", "polaris-iceberg-spark-thrift", "batch", "4.0", "1.11.0"),
        ("financial", "hive-iceberg-spark-trino", "batch", "4.1", "1.11.0"),
    }
    args = ("financial", "hive", "iceberg", "spark", "trino", "batch")
    on_41 = support.support_state(*args, record=rec, spark="4.1", table_format_version="1.11.0")
    on_40 = support.support_state(*args, record=rec, spark="4.0", table_format_version="1.11.0")
    assert on_41["state"] == support.SUPPORTED
    assert on_40["state"] == support.UNVERIFIED


def test_a_record_the_loader_refuses_is_never_written(tmp_path):
    path = tmp_path / "validated_combinations.yaml"
    path.write_text("validated: []\n")
    bad = {
        "workload": "customer360",
        "recipe": "hive-iceberg-spark-trino",
        "mode": "batch",
        "spark": "4.0",
        "table_format_version": "1.9.1",  # not runnable on Spark 4.0
        "tree": FREEZE,
        "runs": ["r1"],
    }
    with pytest.raises(support.ValidationRecordError):
        support.write_record([bad], path)
    assert path.read_text() == "validated: []\n"
    assert [p.name for p in tmp_path.iterdir()] == [path.name]


def _main(monkeypatch, *argv: str) -> int:
    monkeypatch.setattr(rr, "release_datagen_digest", lambda image=None: DIGEST)
    return support.main([str(REPO), *argv])


def test_cli_writes_the_record_and_lists_refusals(ready, tmp_path, monkeypatch, capsys):  # noqa: F811
    records = _records()
    dirs = _write_runs(tmp_path, records)
    expected = _expected_file(tmp_path, records)
    out = tmp_path / "out.yaml"
    before = sorted(p.name for p in tmp_path.iterdir())
    argv = ["--from-records", *map(str, dirs), "--tree", FREEZE, "--expected", str(expected)]
    rc = _main(monkeypatch, *argv, "--write", "--output", str(out))
    err = capsys.readouterr().err
    assert rc == 0
    assert err.count("refused ") == 4 and "6 records read, 2 kept, 4 refused; 2 entries" in err
    assert len(support.load_validation_record(out)) == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([*before, "out.yaml"])
    # Without --write the record is printed and nothing is written.
    out.unlink()
    rc = _main(monkeypatch, *argv, "--output", str(out))
    printed = capsys.readouterr().out
    assert rc == 0 and printed.startswith(support.RECORD_HEADER) and not out.exists()


def test_cli_writes_nothing_when_no_record_is_evidence(ready, tmp_path, monkeypatch, capsys):  # noqa: F811
    records = {"dirty": _records()["dirty"]}
    dirs = _write_runs(tmp_path, records)
    expected = _expected_file(tmp_path, records)
    out = tmp_path / "out.yaml"
    out.write_text("validated: []\n")
    rc = _main(
        monkeypatch,
        *["--from-records", str(dirs[0]), "--tree", FREEZE, "--expected", str(expected)],
        *["--write", "--output", str(out)],
    )
    assert rc == 1 and "nothing written" in capsys.readouterr().err
    assert out.read_text() == "validated: []\n"


@pytest.mark.parametrize(
    "argv, fragment",
    [
        (["--tree", FREEZE], "need --from-records"),
        (["--from-records", "{d}", "--tree", "abc1234", "--expected", "{e}"], "40-hex"),
        (["--from-records", "{d}", "--tree", FREEZE], "--expected is required"),
        (["--from-records", "{d}/nope", "--tree", FREEZE, "--expected", "{e}"], "not a directory"),
    ],
)
def test_cli_usage_errors_exit_2(argv, fragment, tmp_path, capsys):
    (tmp_path / "e.json").write_text('{"entries": []}')
    argv = [a.format(d=tmp_path, e=tmp_path / "e.json") for a in argv]
    with pytest.raises(SystemExit) as e:
        support.main([str(tmp_path), *argv])
    assert e.value.code == 2 and fragment in capsys.readouterr().err


def test_shipped_record_is_the_generated_header_and_an_empty_list():
    text = support.VALIDATION_RECORD.read_text()
    assert text == support.RECORD_HEADER + "validated: []\n"


def test_every_matrix_row_names_runnable_versions():
    rows = {(w, m, r) for w, m, r, _s in rr.RELEASE_MATRIX}
    assert set(rr.RELEASE_MATRIX_VERSIONS) == rows
    for (_w, _m, recipe), (spark, version) in rr.RELEASE_MATRIX_VERSIONS.items():
        fmt = support.components_of(recipe)[1]
        assert support.format_version_problem(spark, fmt, version) == "", recipe


def _full_record(tmp_path: Path) -> dict:
    entries = [
        {
            "workload": w,
            "recipe": r,
            "mode": m,
            "spark": rr.RELEASE_MATRIX_VERSIONS[(w, m, r)][0],
            "table_format_version": rr.RELEASE_MATRIX_VERSIONS[(w, m, r)][1],
            "tree": FREEZE,
            "runs": [f"20261105-0115{i:02d}-abcdef"],
        }
        for i, (w, m, r) in enumerate(sorted({row[:3] for row in rr.RELEASE_MATRIX}))
    ]
    path = tmp_path / "validated_combinations.yaml"
    support.write_record(entries, path)
    return support.load_validation_record(path)


def test_readme_regenerates_from_record(tmp_path):
    # The section 11 matrix fills 15 supported cells, each naming its
    # versions; the block differs from the empty record's, and regenerating
    # the docs from a copy of the tree writes it into README.md.
    rec = _full_record(tmp_path)
    table = support.render_support_table(rec)
    cells = [c.strip() for line in table.splitlines()[2:] for c in line.split("|")[2:-1]]
    assert sum(c.startswith("supported (Spark ") for c in cells) == 15
    assert "supported (Spark 4.0, Delta 4.0.0)" in cells
    assert table != support.render_support_table({})

    import shutil
    from unittest import mock

    for rel in support.DOCS_WITH_BLOCKS:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / rel, tmp_path / rel)
    path = tmp_path / "validated_combinations.yaml"
    with mock.patch.object(support, "VALIDATION_RECORD", path):
        changed = support.regenerate_docs(tmp_path)
    assert "README.md" in changed
    readme = (tmp_path / "README.md").read_text()
    assert "supported (Spark 4.1, Iceberg 1.11.0)" in readme
