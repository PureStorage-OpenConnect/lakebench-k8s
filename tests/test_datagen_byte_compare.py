"""scripts/datagen_byte_compare.py: the DAT-2 reference set and image compare
(CD-26). The podman and MinIO paths run only by hand; these tests cover the
pure parts and the checked-in reference manifests."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import datagen_byte_compare as bc  # noqa: E402


def _obj(key: str, size: int = 10, sha: str = "a" * 64) -> dict:
    return {"key": key, "size": size, "sha256": sha}


def test_compare_names_mismatching_object():
    ref = [_obj("pacs008/part-0.parquet"), _obj("manifest/manifest.parquet")]
    new = [_obj("pacs008/part-0.parquet", sha="b" * 64), _obj("manifest/manifest.parquet")]
    assert bc.compare_objects(ref, new) == ["sha256 differs: pacs008/part-0.parquet"]
    assert bc.compare_objects(ref, list(reversed(ref))) == []
    diffs = bc.compare_objects(ref, [_obj("pacs008/part-0.parquet", size=11), _obj("extra")])
    assert diffs == [
        "missing from the new image: manifest/manifest.parquet",
        "only in the new image: extra",
        "size differs: pacs008/part-0.parquet (10 vs 11)",
    ]


def test_objects_sha256_ignores_order():
    a = [_obj("x"), _obj("y", 3)]
    assert bc.objects_sha256(a) == bc.objects_sha256(list(reversed(a)))
    assert bc.objects_sha256(a) != bc.objects_sha256([_obj("x"), _obj("y", 4)])


def test_corpus_markers_are_excluded():
    assert bc.excluded("pacs008/_corpus/node-0.json")
    assert bc.excluded("_corpus/series.json")
    assert not bc.excluded("pacs008/part-0.parquet")


def test_fnv_tree_digest_matches_the_rust_definition(tmp_path):
    # FNV-1a 64 over relative path bytes then file bytes; FNV-1a("a") is the
    # published test vector 0xaf63dc4c8601ec8c.
    (tmp_path / "a").write_bytes(b"")
    assert bc.tree_digest(tmp_path) == (0xAF63DC4C8601EC8C, 1)


def test_c360_digest_sorts_paths_by_component(tmp_path):
    # As strings "a-c" < "a/b"; as paths ("a", "b") < ("a-c",). cycles.rs's
    # c360 pin sorts PathBufs, its tree digest sorts strings.
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "b").write_bytes(b"1")
    (tmp_path / "a-c").write_bytes(b"2")
    assert bc.tree_digest(tmp_path) != bc.c360_driver_digest(tmp_path)
    assert bc.c360_driver_digest(tmp_path) == bc._fnv(["a/b", "a-c"], tmp_path)


def test_pinned_digests_read_the_live_assert_not_a_comment():
    pins = bc.pinned_digests()
    src = bc.CYCLES_RS.read_text()
    for name, (digest, files) in pins.items():
        assert f"{digest:_}" in src and files > 0, name
    # The c360 pin's comment quotes the pre-LB-191 digest; it must not be read.
    old = re.search(r"Pre-fix digest\s*//\s*was \(([0-9_]+)", src)
    assert old and int(old.group(1).replace("_", "")) != pins["c360"][0]


def test_case_argv_follows_the_rendered_job():
    runs = {c: bc.render_runs(c) for c in bc.CASES}
    f0, c0 = runs["F0"][0], runs["C0"][0]
    assert f0[f0.index("--schema") + 1] == "financial" and f0[f0.index("--seed") + 1] == "43"
    assert c0[c0.index("--schema") + 1] == "customer360" and c0[c0.index("--seed") + 1] == "42"
    assert bc.nodes_of(f0) == bc.nodes_of(c0) == 4
    assert runs["F1"] == [f0 + ["--robustness-perturbation"]]
    for case in ("F2", "C2"):
        assert len(runs[case]) == 2
        for n, argv in enumerate(runs[case]):
            assert argv[argv.index("--cycle") + 1] == str(n)
            assert argv[argv.index("--cycles") + 1] == "2"
    # The two cycle windows meet and do not overlap past the boundary.
    c2 = runs["C2"]
    assert c2[0][c2[0].index("--timestamp-end") + 1] == c2[1][c2[1].index("--timestamp-start") + 1]


def _run(objects, runs=None, threads=None):
    return {
        "objects": objects,
        "threads": threads if threads is not None else [[8, 8, 8, 8]],
        "excluded_objects": 0,
        "rendered_runs": runs if runs is not None else [["--seed", "43"]],
    }


def test_compare_result_layout():
    ref = {
        "seed_ref": "43",
        "runs": [["--seed", "43"]],
        "env": {"CPU_LIMIT": "8"},
        "threads": [[8, 8, 8, 8]],
        "objects": [_obj("k")],
    }
    cases = {c: (ref, _run([_obj("k")])) for c in bc.CASES}
    cases["F2"] = (ref, _run([_obj("k", sha="c" * 64)]))
    cases["C0"] = (ref, _run([_obj("k")], threads=[[4, 4, 4, 4]]))
    doc = bc.compare_result("sha256:" + "1" * 64, "sha256:" + "2" * 64, cases)
    assert [c["case"] for c in doc["cases"]] == list(bc.CASES)
    assert {c["case"] for c in doc["cases"] if not c["equal"]} == {"F2"}
    by = {c["case"]: c for c in doc["cases"]}
    assert by["C0"]["equal"] and not by["C0"]["threads_equal"]
    for c in doc["cases"]:
        assert c["digest_a"] == doc["image_a"] and c["digest_b"] == doc["image_b"]
        assert c["excluded"] == ["_corpus/"]


def test_compare_fails_when_the_tree_renders_other_argv():
    # Same bytes, but production would now run other arguments: not equal.
    ref = {"seed_ref": "43", "runs": [["--seed", "43"]], "env": {}, "objects": [_obj("k")]}
    cases = {c: (ref, _run([_obj("k")])) for c in bc.CASES}
    cases["F1"] = (ref, _run([_obj("k")], runs=[["--seed", "43", "--workers", "2"]]))
    by = {c["case"]: c for c in bc.compare_result("a", "b", cases)["cases"]}
    assert by["F1"]["bytes_equal"] and not by["F1"]["argv_rendered_equal"]
    assert not by["F1"]["equal"] and by["F1"]["rendered_runs"]


def test_pins_cover_every_cargo_pin():
    src = bc.CYCLES_RS.read_text()
    pinned = set(re.findall(r"fn (\w+_is_pinned\w*)\(", src))
    assert pinned == {needle.removeprefix("fn ") for needle, _k, _t in bc.PINS.values()}


def _schema_release_digest() -> str:
    src = (ROOT / "src/lakebench/config/schema.py").read_text()
    m = re.search(r"Pushed digest \(1\.6\.0\): (sha256:[0-9a-f]{64})", src)
    assert m, "schema.py no longer records the 1.6.0 pushed digest"
    return m.group(1)


def test_rendered_argv_equals_the_reference():
    # A change to what Lakebench renders for a case fails here, so the
    # reference and production cannot drift apart unnoticed.
    for case in bc.CASES:
        assert bc.render_runs(case) == bc.load_reference(case)["runs"], case


def test_reference_manifests_present():
    docs = [bc.load_reference(c) for c in bc.CASES]
    assert {d["image"] for d in docs} == {_schema_release_digest()}
    for d in docs:
        assert d["objects"], d["case"]
        keys = [o["key"] for o in d["objects"]]
        assert keys == sorted(keys) and len(keys) == len(set(keys)), d["case"]
        assert not any(bc.excluded(k) for k in keys), d["case"]
        assert all(re.fullmatch(r"[0-9a-f]{64}", o["sha256"]) for o in d["objects"])
        assert d["excluded_objects"] == 0, d["case"]
        assert {k: v["digest"] for k, v in d["pin_proof"].items()} == {
            k: v[0] for k, v in bc.pinned_digests().items()
        }, d["case"]
        assert [len(t) for t in d["threads"]] == [d["nodes"]] * len(d["runs"]), d["case"]


def test_reference_manifests_carry_no_endpoint_or_key():
    for case in bc.CASES:
        text = (bc.REF_DIR / f"{case}.json").read_text()
        assert "http" not in text and "127.0.0.1" not in text, case
        assert "AWS_" not in text and "MINIO_" not in text, case
        json.loads(text)


@pytest.mark.skip(
    reason="ch03's lineage loader (ER-9L) is not in the tree yet; CD-8 adds this test"
)
def test_compare_output_matches_loader():
    pass
