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
    """Two records that are release evidence on their matrix rows, and
    three that are not: a dirty tree, a matrix row at other versions and a
    row that is not in the matrix."""
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
    return {
        "polaris": polaris,
        "aml": aml,
        "dirty": dirty,
        "wrong_spark": wrong_spark,
        "off_matrix": off_matrix,
    }


def _put(d: Path, rec: dict, dirname: str | None = None) -> None:
    run_dir = d / (dirname or f"run-{rec['run_id']}")
    run_dir.mkdir(parents=True)
    (run_dir / "metrics.json").write_text(json.dumps(rec))


def _write_runs(root: Path, records: dict[str, dict]) -> list[Path]:
    a, b = root / "runs-a", root / "runs-b"
    a.mkdir()
    b.mkdir()
    for rec in records.values():
        _put(a, rec)
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
    assert (out.read, out.kept, len(refused)) == (5, 2, 3)
    assert "modified tree" in refused[f"run-{records['dirty']['run_id']}"]
    assert (
        "the release matrix runs financial hive-iceberg-spark-trino batch on "
        "Spark 4.1, Iceberg 1.11.0, not Spark 4.0, Iceberg 1.11.0"
    ) in refused[f"run-{records['wrong_spark']['run_id']}"]
    assert (
        "at scale 5 is not a release-matrix row"
        in refused[f"run-{records['off_matrix']['run_id']}"]
    )
    assert "financial hive-iceberg-spark-trino batch at scale 10" in out.uncovered
    assert len(out.uncovered) == len(rr.RELEASE_MATRIX) - 2


def test_two_different_records_of_one_run_are_both_refused(ready, tmp_path):  # noqa: F811
    # Whichever copy passes, two records of one run are not evidence of
    # either; the order the directories are read in must not decide it.
    records = _records()
    dirs = _write_runs(tmp_path, records)
    twin = json.loads(json.dumps(records["aml"]))
    twin["note"] = "a different copy"
    twin["provenance"]["git_dirty"] = True  # this copy fails on its own
    _put(dirs[1], twin)
    expected = _expected(*records.values())
    for order in (dirs, dirs[::-1]):
        out = support.rows_from_records(order, FREEZE, expected, root=REPO, release_digest=DIGEST)
        assert [e["recipe"] for e in out.entries] == ["polaris-iceberg-spark-thrift"]
        reasons = [" ".join(why) for p, why in out.refused if records["aml"]["run_id"] in str(p)]
        assert len(reasons) == 2 and all("has different records" in r for r in reasons)


def test_identical_copies_count_once_and_ids_are_normalised(ready, tmp_path):  # noqa: F811
    records = _records()
    dirs = _write_runs(tmp_path, records)
    _put(dirs[1], records["polaris"])  # a byte-equal copy
    prefixed = json.loads(json.dumps(records["aml"]))
    prefixed["run_id"] = "run-" + prefixed["run_id"]  # the same run, spelled with run-
    _put(dirs[1], prefixed, f"run-{records['aml']['run_id']}")
    out = support.rows_from_records(
        dirs, FREEZE, _expected(*records.values()), root=REPO, release_digest=DIGEST
    )
    # The byte-equal copy is counted once; the run-prefixed spelling is the
    # same run id with different bytes, so that run is refused, never
    # listed twice.
    assert [r for e in out.entries for r in e["runs"]] == [records["polaris"]["run_id"]]
    assert out.duplicates == 1 and out.kept == 1
    reasons = [" ".join(w) for p, w in out.refused if records["aml"]["run_id"] in str(p)]
    assert len(reasons) == 2 and all("has different records" in r for r in reasons)


def test_a_record_outside_its_run_directory_is_refused(ready, tmp_path):  # noqa: F811
    records = {"aml": _records()["aml"]}
    dirs = _write_runs(tmp_path, {})
    _put(dirs[0], records["aml"], "run-20260101-000000-ffffff")
    out = support.rows_from_records(
        dirs, FREEZE, _expected(*records.values()), root=REPO, release_digest=DIGEST
    )
    assert not out.entries
    assert "its directory is not run-" in " ".join(out.refused[0][1])


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
    assert err.count("refused ") == 3
    assert "5 records read: 2 runs kept, 3 refused, 0 identical copies skipped; 2 entries" in err
    assert "not covered: financial hive-iceberg-spark-trino batch at scale 10" in err
    assert len(support.load_validation_record(out)) == 2
    assert out.stat().st_mode & 0o777 == 0o644
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
        support.main([str(REPO), *argv])
    assert e.value.code == 2 and fragment in capsys.readouterr().err


def test_shipped_record_is_what_the_generator_writes():
    # Holds the checked-in file to the writer's text for its own entries:
    # an edit by hand that the writer would not produce fails here.
    rec = support.load_validation_record()
    entries = [
        {
            "workload": v.workload,
            "recipe": v.recipe,
            "mode": v.mode,
            "spark": v.spark,
            "table_format_version": v.table_format_version,
            "tree": v.tree,
            "runs": list(v.runs),
        }
        for _k, v in sorted(rec.items())
    ]
    assert support.VALIDATION_RECORD.read_text() == support.record_text(entries)


def test_cli_refuses_a_lakebench_imported_from_another_tree(tmp_path, capsys):
    # The tables, the record rules and the release image come from the
    # imported code; writing ROOT's files from another tree's code would
    # put that tree's record and tables into ROOT.
    with pytest.raises(SystemExit) as e:
        support.main([str(tmp_path)])
    err = capsys.readouterr().err
    assert e.value.code == 2 and "not " + str(tmp_path.resolve() / "src") in err
    assert list(tmp_path.iterdir()) == []


def test_cli_writes_into_root_by_default(ready, tmp_path, monkeypatch, capsys):  # noqa: F811
    import shutil

    root = tmp_path / "tree"
    shutil.copytree(REPO / "src" / "lakebench", root / "src" / "lakebench")
    for rel in ("tests/fixtures/datagen_reference", "src/lakebench/config/datagen_lineage.yaml"):
        src = REPO / rel
        if src.is_dir() and not (root / rel).exists():
            shutil.copytree(src, root / rel)
    records = _records()
    dirs = _write_runs(tmp_path, records)
    expected = _expected_file(tmp_path, records)
    here = root / "src" / "lakebench" / "config" / "support.py"
    monkeypatch.setattr(support, "__file__", str(here))
    monkeypatch.setattr(rr, "release_datagen_digest", lambda image=None: DIGEST)
    rc = support.main(
        [str(root), "--from-records", *map(str, dirs), "--tree", FREEZE]
        + ["--expected", str(expected), "--write"]
    )
    assert rc == 0, capsys.readouterr().err
    written = root / "src" / "lakebench" / "config" / "validated_combinations.yaml"
    assert len(support.load_validation_record(written)) == 2
    assert support.load_validation_record() == {}  # the imported package's file is untouched


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
