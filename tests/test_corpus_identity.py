"""Corpus id v2 from the generator's own markers (SPEC v1.7 EVD-6, ER-9).

The design is ch03 section 6 ("Corpus id v2", "Series corpus identity",
"Lineage") over ch05 section 3.1's ``corpus_args_sha256`` and
``corpus_series_sha256``. The markers, series.json and compare files are
fixtures in the shapes ch05 defines; CD-7, CD-18 and CD-8 write the real
ones.
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakebench import corpus_digest as cd
from lakebench.metrics import corpus_identity as ci
from lakebench.metrics import experiment as ex
from tests.test_experiment import _cfg, _metrics

ROOT = Path(__file__).resolve().parents[1]

H1 = "a" * 64
H2 = "b" * 64
H3 = "c" * 64
D = "sha256:" + "1" * 64  # the image that generated the corpus
X = "sha256:" + "9" * 64  # another image (a stale fleet sidecar)
TAG = "docker.io/sillidata/lb-datagen:1.6.0"
SCOPE = "customer/interactions/"
BUCKET = "scrubbed-bronze"


def marker(cycle=0, node=0, total=2, cycles=1, h=H1, **kw):
    body = {
        "format": 1,
        "schema": "customer360",
        "model_version": "datagen-v2-rs-0.3",
        "build_commit": "abc1234",
        "cycle": cycle,
        "cycles": cycles,
        "node_id": node,
        "total_nodes": total,
        "delivery_mode": "batch",
        "files_written": 3,
        "rows_written": 10,
        "bytes_written": 100,
        "seed_ref": "42",
        "corpus_args": {"scale": 1.0},
        "corpus_args_sha256": h,
        "completed_utc": "2026-10-06T00:00:00Z",
    }
    body.update(kw)
    return body


def series(
    digest=D, image=TAG, seed_ref="42", cycles_total=1, updated="2026-10-06T00:01:00Z", **gen
):
    return {
        "format": 1,
        "schema": "customer360",
        "updated_utc": updated,
        "cycles_total": cycles_total,
        "cycles_complete": list(range(cycles_total)),
        "generation": {
            "seed_ref": seed_ref,
            "scale": 1.0,
            "image": image,
            "image_digest": digest,
            **({} if digest else {"image_digest_reason": "datagen pods ran different images"}),
            **gen,
        },
    }


class FakeBoto:
    """list_objects_v2 paginator (pages of 2) and get_object over a dict."""

    def __init__(self, objects: dict[str, bytes], fail_get: set[str] | None = None):
        self.objects = dict(objects)
        self.etags = {k: hashlib.md5(v).hexdigest() for k, v in objects.items()}  # noqa: S324
        self.lists = 0
        self.gets: list[str] = []
        self.fail_get = fail_get or set()

    def get_paginator(self, op):
        assert op == "list_objects_v2"
        fake = self

        class P:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                fake.lists += 1
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                for i in range(0, max(len(keys), 1), 2):
                    yield {
                        "Contents": [
                            {
                                "Key": k,
                                "Size": len(fake.objects[k]),
                                "ETag": f'"{fake.etags[k]}"',
                            }
                            for k in keys[i : i + 2]
                        ]
                    }

        return P()

    def get_object(self, Bucket, Key):  # noqa: N803
        self.gets.append(Key)
        if Key in self.fail_get:
            raise ConnectionError("endpoint unreachable")
        return {"Body": io.BytesIO(self.objects[Key])}


def bucket(markers=(), series_body=None, data=("part-0.parquet", "part-1.parquet"), scope=SCOPE):
    objs: dict[str, bytes] = {f"{scope}{name}": b"x" * 10 for name in data}
    for m in markers:
        objs[cd.marker_key(scope, m["cycle"], m["node_id"])] = json.dumps(m).encode()
    if series_body is not None:
        objs[cd.series_key(scope)] = json.dumps(series_body).encode()
    return FakeBoto(objs)


def observe(markers=(), series_body=None, lineage_path=None, **kw):
    """observe_corpus over a fake bucket, through a real config."""
    boto = bucket(markers, series_body, **kw)
    cfg = _cfg()
    obs = ci.observe_corpus(cfg, SimpleNamespace(raw_client=boto), lineage_path=lineage_path)
    return obs, boto


def table_file(tmp_path, text):
    p = tmp_path / "lineage.yaml"
    p.write_text(text)
    return p


COMMIT = "abc1234" + "0" * 33


def two_nodes(h=H1, **kw):
    return [marker(node=0, h=h, **kw), marker(node=1, h=h, **kw)]


def corpus_of(cfg=None, obs=None, inherited=None, fleet=None):
    run = _metrics(cfg or _cfg(), fleet=fleet)
    inputs = run.config_snapshot["experiment_inputs"]
    if obs is not None:
        inputs["corpus_observation"] = obs
    if inherited is not None:
        inputs["inherited_corpus"] = inherited
    return run.to_dict()["experiment"]["corpus"]


# ---------------------------------------------------------------------------
# corpus_digest: the shared hash definitions
# ---------------------------------------------------------------------------


class TestSeriesHash:
    def test_corpus_series_hash_is_array_form(self):
        one = cd.corpus_series_sha256({0: two_nodes()})
        assert one == hashlib.sha256(json.dumps([H1]).replace(" ", "").encode()).hexdigest()
        assert one != H1
        ab = {0: [marker(cycles=2, total=1, h=H1)], 1: [marker(1, cycles=2, total=1, h=H2)]}
        ba = {0: [marker(cycles=2, total=1, h=H2)], 1: [marker(1, cycles=2, total=1, h=H1)]}
        assert cd.corpus_series_sha256(ab) != cd.corpus_series_sha256(ba)

    @pytest.mark.parametrize(
        "markers",
        [
            {},
            {0: [marker(node=0)]},  # node 1 of 2 missing
            {0: [marker(node=1), marker(node=2)]},  # node 0 missing, stray node 2
            {0: [marker(node=0), marker(node=0)]},  # node 0 twice
            {0: [marker(node=0, h=H1), marker(node=1, h=H2)]},  # mixed arguments
            {0: [marker(node=0, h=None), marker(node=1, h=None)]},  # pre-DAT-3 shape
            {0: [marker(node=0, total=2), marker(node=1, total=3)]},
            {1: [marker(1, total=1, cycles=2)]},  # cycle 0 missing
            {0: [marker(total=1, cycles=1)], 1: [marker(1, total=1, cycles=2)]},
        ],
    )
    def test_incomplete_or_mixed_corpus_has_no_series_hash(self, markers):
        assert cd.corpus_series_sha256(markers) is None

    @pytest.mark.parametrize(
        "prefix", ["customer/interactions", "/customer/interactions/", "customer/interactions//"]
    )
    def test_scope_is_normalised(self, prefix):
        assert cd.datagen_scope(prefix) == SCOPE

    @pytest.mark.parametrize("prefix", ["", "/", "//"])
    def test_empty_prefix_is_never_the_whole_bucket(self, prefix):
        with pytest.raises(ValueError):
            cd.datagen_scope(prefix)

    def test_listing_excludes_a_sibling_prefix(self):
        """The raw template has no trailing slash; listing it as-is would
        take in customer/interactions_v2/ and move the digest."""
        boto = bucket(data=("a.parquet",))
        alone = cd.listing_digest(boto, BUCKET, "customer/interactions")
        boto.objects["customer/interactions_v2/b.parquet"] = b"y"
        boto.etags["customer/interactions_v2/b.parquet"] = "e"
        assert cd.listing_digest(boto, BUCKET, "customer/interactions") == alone
        ms = ci.read_corpus_markers(boto, BUCKET, "customer/interactions")
        assert ms.objects == 1 and ms.bronze_listing_sha256 == alone

    def test_listing_digest_sees_a_same_size_rewrite(self):
        boto = bucket()
        before = cd.listing_digest(boto, BUCKET, "customer/interactions")
        boto.etags[f"{SCOPE}part-0.parquet"] = "different"
        assert cd.listing_digest(boto, BUCKET, "customer/interactions") != before


# ---------------------------------------------------------------------------
# Reading the markers
# ---------------------------------------------------------------------------


class TestReadMarkers:
    def test_one_listing_gives_markers_series_and_digest(self):
        obs, boto = observe(two_nodes(), series())
        assert boto.lists == 1
        assert len(boto.gets) == 3  # two markers and series.json
        m = obs["markers"]
        assert m["error"] is None and m["problems"] == []
        assert m["cycles"][0]["nodes_found"] == [0, 1]
        assert m["corpus_series_sha256"] == cd.corpus_series_sha256({0: two_nodes()})
        # The digest covers the whole scope, the markers included.
        assert obs["bronze_listing_sha256"] == cd.listing_digest(boto, BUCKET, SCOPE)
        assert obs["series"]["generation"]["image_digest"] == D

    def test_marker_bodies_are_not_persisted(self):
        obs, _ = observe(two_nodes(), series())
        text = json.dumps(obs["markers"])
        assert 'corpus_args"' not in text and "seed_ref" not in text

    def test_first_failed_get_stops_the_read(self):
        boto = bucket(two_nodes(), series())
        boto.fail_get = {cd.marker_key(SCOPE, 0, 0)}
        ms = ci.read_corpus_markers(boto, BUCKET, SCOPE)
        assert "endpoint unreachable" in ms.error and len(boto.gets) == 1

    def test_marker_that_disagrees_with_its_key_is_a_problem(self):
        boto = bucket([marker(node=0), marker(node=1)])
        key = cd.marker_key(SCOPE, 0, 1)
        boto.objects[key] = json.dumps(marker(node=0)).encode()
        ms = ci.read_corpus_markers(boto, BUCKET, SCOPE)
        assert any("does not match its content" in p for p in ms.problems)
        assert ms.to_dict()["cycles"][0]["nodes_found"] == [0]

    def test_observation_never_raises(self):
        obs = ci.observe_corpus(_cfg(), SimpleNamespace(raw_client=None))
        assert "did not initialise" in obs["markers"]["error"]
        assert obs["bronze_listing_sha256"] is None


# ---------------------------------------------------------------------------
# The corpus block
# ---------------------------------------------------------------------------


class TestCorpusBlock:
    def test_no_observation_adds_no_v2_field(self):
        """A v1.6 record re-saved before ER-10a keeps its block's shape."""
        corpus = corpus_of()
        assert not {"id_v2", "id_v2_unavailable", "declared", "lineage"} & set(corpus)

    def test_no_marker_gives_unavailable(self):
        obs, _ = observe((), None)
        corpus = corpus_of(obs=obs)
        assert corpus["id_v2"] is None and corpus["id_v2_unavailable"] == ci.NO_MARKER
        assert "problems" not in corpus

    def test_markers_give_id_v2_with_observed_lineage(self):
        obs, _ = observe(two_nodes(), series())
        corpus = corpus_of(obs=obs)
        assert len(corpus["id_v2"]) == 16 and corpus["id_version"] == 2
        assert corpus["args_sha256"] == obs["markers"]["corpus_series_sha256"]
        assert corpus["lineage"] == D and corpus["lineage_observed"] is True
        assert "problems" not in corpus

    def test_v1_id_and_identity_never_move(self):
        cfg = _cfg()
        obs, _ = observe(two_nodes(), series())
        before = _metrics(cfg).to_dict()["experiment"]
        after = corpus_of(cfg, obs=obs)
        assert after["id"] == before["corpus"]["id"]
        run = _metrics(cfg)
        run.config_snapshot["experiment_inputs"]["corpus_observation"] = obs
        assert ex.identity_hash(run.to_dict()["experiment"]) == ex.identity_hash(before)

    def test_corpus_id_from_markers_not_config(self):
        """S4: markers written at scale 1, config edited to scale 10 before a
        --skip-generate run. The id follows the markers; the v1 id follows
        the config, which is the corruption id v2 exists to fix."""
        obs, _ = observe(two_nodes(), series())
        at1 = corpus_of(_cfg(scale=1), obs=obs)
        at10 = corpus_of(_cfg(scale=10), obs=obs)
        assert at10["id_v2"] == at1["id_v2"]
        assert at10["id"] != at1["id"]
        assert "warnings" not in at1
        assert "scale" in at10["warnings"][0] and "follows the corpus" in at10["warnings"][0]
        assert at10["declared"]["scale"] == 10

    def test_marker_disagreement_is_corpus_problem(self):
        obs, _ = observe([marker(node=0, h=H1), marker(node=1, h=H2)], series())
        corpus = corpus_of(obs=obs)
        assert corpus["id_v2"] is None and corpus["id_v2_unavailable"] == ci.NOT_ONE_CORPUS
        assert any("different arguments (cycle 0)" in p for p in corpus["problems"])
        run = _metrics(_cfg())
        run.config_snapshot["experiment_inputs"]["corpus_observation"] = obs
        rec = run.to_dict()
        prov, _, _ = ex.refusals(rec, _metrics(_cfg()).to_dict())
        assert any("different arguments" in p for p in prov)

    def test_missing_node_is_corpus_problem(self):
        obs, _ = observe([marker(node=0, total=3), marker(node=2, total=3)], series())
        corpus = corpus_of(obs=obs)
        assert "cycle 0 nodes [1] have no corpus marker" in corpus["problems"]
        assert corpus["id_v2"] is None

    def test_cycles_change_corpus_id_v2(self):
        one, _ = observe(two_nodes(), series())
        three_markers = [marker(c, n, cycles=3) for c in range(3) for n in range(2)]
        three, _ = observe(three_markers, series(cycles_total=3))
        assert corpus_of(obs=one)["id_v2"] != corpus_of(obs=three)["id_v2"]
        # cycles 1 hashes no "cycles" key at all.
        lineage = D
        expected = ex._short_hash(
            {
                "args": one["markers"]["corpus_series_sha256"],
                "model_version": None,
                "lineage": lineage,
            }
        )
        assert ci.corpus_id_v2(one, None, lineage) == (expected, None)

    def test_model_version_differs_from_the_workload(self):
        cfg = _cfg("financial")
        obs, _ = observe(
            two_nodes(model_version="datagen-v2-rs-0.2", schema="financial"),
            series(),
        )
        corpus = corpus_of(cfg, obs=obs)
        assert any("model 'datagen-v2-rs-0.2'" in p for p in corpus["problems"])


class TestLineage:
    def test_declared_without_series(self):
        obs, _ = observe(two_nodes(), None)
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"] == f"declared:{TAG}" and corpus["lineage_observed"] is False
        observed, _ = observe(two_nodes(), series())
        assert corpus["id_v2"] != corpus_of(obs=observed)["id_v2"]

    def test_declared_tag_comes_from_the_series_image(self):
        """An images.datagen edit after generation does not move the id."""
        obs, _ = observe(two_nodes(), series(digest=None, image="reg/dg:old"))
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"] == "declared:reg/dg:old"
        assert "different images" in corpus["lineage_notes"][0]

    def test_fleet_naming_another_image_gives_declared_lineage(self):
        """The fleet sidecar is never the lineage source, and when it
        contradicts series.json neither is trusted (the safe side)."""
        obs, _ = observe(two_nodes(), series(digest=D))
        fleet = {"image": TAG, "image_ids": [f"reg@{X}"]}
        corpus = corpus_of(obs=obs, fleet=fleet)
        assert corpus["lineage"] == f"declared:{TAG}" and corpus["lineage_observed"] is False
        assert any("fleet record names" in n for n in corpus["lineage_notes"])
        same = corpus_of(obs=obs, fleet={"image": TAG, "image_ids": [f"reg@{D}"]})
        assert same["lineage"] == D

    def test_series_from_another_generate_lends_no_lineage(self):
        obs, _ = observe(two_nodes(), series(seed_ref="7"))
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"].startswith("declared:")
        assert "another seed" in corpus["lineage_notes"][0]

    def test_build_commit_mismatch_is_problem(self, tmp_path):
        table = table_file(
            tmp_path,
            f'lineage:\n  - digest: {D}\n    canonical: {D}\n    build_commit: "{"f" * 40}"\n',
        )
        obs, _ = observe(two_nodes(), series(), lineage_path=table)
        corpus = corpus_of(obs=obs)
        assert any("built from ffffffffffff" in p for p in corpus["problems"])
        assert corpus["lineage_observed"] is False

    def test_build_commit_matches_by_prefix(self, tmp_path):
        table = table_file(
            tmp_path,
            f'lineage:\n  - digest: {D}\n    canonical: {D}\n    build_commit: "{COMMIT}"\n',
        )
        obs, _ = observe(two_nodes(), series(), lineage_path=table)
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"] == D and "problems" not in corpus

    def test_markers_without_a_commit_give_declared_not_a_problem(self, tmp_path):
        table = table_file(
            tmp_path,
            f'lineage:\n  - digest: {D}\n    canonical: {D}\n    build_commit: "{COMMIT}"\n',
        )
        obs, _ = observe(two_nodes(build_commit=None), series(), lineage_path=table)
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"].startswith("declared:") and "problems" not in corpus

    def test_different_builds_are_a_problem(self):
        obs, _ = observe([marker(node=0), marker(node=1, build_commit="def5678")], series())
        corpus = corpus_of(obs=obs)
        assert any("different generator builds" in p for p in corpus["problems"])
        assert corpus["lineage"].startswith("declared:")

    def test_mapped_digest_takes_its_root(self, tmp_path):
        obs, _ = observe(two_nodes(), series(), lineage_path=_mapping_table(tmp_path))
        assert corpus_of(obs=obs)["lineage"] == ROOT_DIGEST

    def test_id_v2_does_not_move_when_the_table_changes(self, tmp_path, monkeypatch):
        """The lineage is resolved at observation and persisted: a row added
        later (ER-9L) never moves the id of a record already observed, even
        while to_dict still rebuilds the block."""
        obs, _ = observe(two_nodes(), series())
        before = corpus_of(obs=obs)
        monkeypatch.setattr(ci, "LINEAGE_FILE", _mapping_table(tmp_path))
        after = corpus_of(obs=json.loads(json.dumps(obs)))
        assert after["id_v2"] == before["id_v2"] and after["lineage"] == D

    def test_unreadable_table_is_a_problem(self, tmp_path):
        table = table_file(tmp_path, "lineage: [")
        obs, _ = observe(two_nodes(), series(), lineage_path=table)
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"] == f"declared:{TAG}"
        assert any("cannot read" in p for p in corpus["problems"])

    def test_non_sha_series_digest_gives_declared(self):
        obs, _ = observe(two_nodes(), series(digest="sha256:short"))
        assert corpus_of(obs=obs)["lineage"].startswith("declared:")

    @pytest.mark.parametrize(
        "change, needle",
        [
            ({"cycles_total": 2}, "cycle count"),
            ({"customer_id_max": 5}, "customer_id_max"),
            ({"file_size_mb": 128}, "file_size_mb"),
        ],
    )
    def test_series_disagreeing_with_marker_args_lends_no_lineage(self, change, needle):
        body = series()
        if "cycles_total" in change:
            body["cycles_total"] = change["cycles_total"]
        else:
            body["generation"].update(change)
        args = {"scale": 1.0, "file_size_mb": 64, "customer_id_max": 100000}
        obs, _ = observe(two_nodes(corpus_args=args), body)
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"].startswith("declared:")
        assert needle in corpus["lineage_notes"][0]


ROOT_DIGEST = "sha256:" + "2" * 64


def _mapping_table(tmp_path):
    """A table mapping D to another root (its evidence is not checked here)."""
    return table_file(
        tmp_path,
        "lineage:\n"
        f"  - digest: {ROOT_DIGEST}\n    canonical: {ROOT_DIGEST}\n"
        f"  - digest: {D}\n    canonical: {ROOT_DIGEST}\n"
        f"    evidence: tests/fixtures/datagen_reference/compare-{'1' * 12}.json\n"
        f'    evidence_sha256: "{"0" * 64}"\n',
    )


# ---------------------------------------------------------------------------
# Series corpus identity (--repeat)
# ---------------------------------------------------------------------------


def _inherit(rep1_corpus, obs):
    """What CC-30 persists, built the one way: from repetition 1's record."""
    record = {
        "run_id": "20261006-120000-aaaaaa",
        "experiment": {"corpus": rep1_corpus},
        "config_snapshot": {"experiment_inputs": {"corpus_observation": obs}},
    }
    return ci.inherited_corpus_from(record)


class TestSeries:
    def test_series_inherited_equals_marker(self):
        """ch03 test_series_inherited_equals_marker: repetition 1 generated
        (fleet digest D), repetitions 2 and 3 did not, every observation
        holds the same markers and series.json with image_digest D. One
        id v2 and one lineage for all three."""
        obs, _ = observe(two_nodes(), series(digest=D))
        rep1 = corpus_of(obs=obs, fleet={"image": TAG, "image_ids": [f"reg@{D}"]})
        reps = [corpus_of(obs=obs, inherited=_inherit(rep1, obs)) for _ in range(2)]
        for rep in reps:
            assert rep["id_v2"] == rep1["id_v2"] and rep["lineage"] == rep1["lineage"] == D
            assert "problems" not in rep

    def test_series_marker_change_is_a_problem(self):
        obs, _ = observe(two_nodes(), series())
        rep1 = corpus_of(obs=obs)
        obs3, _ = observe(two_nodes(h=H3), series())
        inherited = _inherit(rep1, obs)
        inherited["bronze_listing_sha256"] = obs3["bronze_listing_sha256"]  # digest passed
        rep3 = corpus_of(obs=obs3, inherited=inherited)
        assert "series corpus id differs from repetition 1" in rep3["problems"]

    def test_series_digest_taken_before_save(self):
        """The pre-save digest differs from D1: the corpus problem, and no
        inherited block."""
        obs, _ = observe((), None)
        rep1 = corpus_of(obs=obs)
        changed, _ = observe((), None, data=("part-0.parquet", "part-1.parquet", "late.parquet"))
        rep2 = corpus_of(obs=changed, inherited=_inherit(rep1, obs))
        assert "bronze changed during this repetition" in rep2["problems"]
        assert "inherited_from" not in rep2 and rep2["id_v2"] is None

    @pytest.mark.parametrize(
        "d1_observed, rep_observed", [(False, False), (True, False), (False, True)]
    )
    def test_unobserved_digest_never_inherits(self, d1_observed, rep_observed):
        """None never equals None: an unobserved digest on either side is
        "not observed", never "unchanged" and never "bronze changed"."""
        obs, _ = observe((), None)
        rep1 = corpus_of(obs=obs)
        inherited = _inherit(rep1, obs)
        if not d1_observed:
            inherited["bronze_listing_sha256"] = None
        failed = ci.observe_corpus(_cfg(), SimpleNamespace(raw_client=None))
        rep2 = corpus_of(obs=obs if rep_observed else failed, inherited=inherited)
        assert "inherited_from" not in rep2
        assert any("not observed" in p for p in rep2["problems"])
        assert "bronze changed during this repetition" not in rep2["problems"]

    def test_inherited_block_copied_without_markers(self):
        obs, _ = observe((), None)
        fleet = {"image": TAG, "image_ids": [f"reg@{D}"]}
        rep1 = corpus_of(obs=obs, fleet=fleet)
        rep2 = corpus_of(obs=obs, inherited=_inherit(rep1, obs))
        assert rep2["inherited_from"] == "20261006-120000-aaaaaa"
        assert rep2["bronze_listing_sha256"] == obs["bronze_listing_sha256"]
        assert rep2["datagen"]["digest"] == D
        assert {
            k: v for k, v in rep2.items() if k not in ("inherited_from", "bronze_listing_sha256")
        } == rep1

    def test_inherited_block_keeps_repetition_1_problems(self):
        obs, _ = observe((), None)
        mixed = {"image": TAG, "image_ids": [f"reg@{D}"], "data_quality": "mixed"}
        rep1 = corpus_of(obs=obs, fleet=mixed)
        assert rep1["problems"]
        rep2 = corpus_of(obs=obs, inherited=_inherit(rep1, obs))
        assert rep2["problems"] == rep1["problems"]

    def test_bare_block_is_a_contract_problem_only(self):
        obs, _ = observe(two_nodes(), series())
        rep1 = corpus_of(obs=obs)
        rep2 = corpus_of(
            obs=obs,
            inherited={"corpus": rep1, "bronze_listing_sha256": obs["bronze_listing_sha256"]},
        )
        assert rep2["problems"] == [
            "the inherited corpus is not in the series contract shape "
            "(build it with corpus_identity.inherited_corpus_from)"
        ]
        assert rep2["id_v2"] == rep1["id_v2"]


# ---------------------------------------------------------------------------
# Lineage table and evidence
# ---------------------------------------------------------------------------

B = "sha256:" + "4" * 64  # a re-pinned image
A = "sha256:" + "5" * 64  # its canonical root


def compare_file(**changes):
    cases = [
        {
            "case": name,
            "seed_ref": seed,
            "argv_canonical": ["--scale", "1"],
            "digest_a": A,
            "digest_b": B,
            "objects": 141,
            "sha256_a": "e" * 64,
            "sha256_b": "e" * 64,
            "equal": True,
            "excluded": ["_corpus/"],
        }
        for name, seed in ci.COMPARE_CASES.items()
    ]
    data = {"format": 1, "image_a": A, "image_b": B, "cases": cases}
    for case, fields in changes.items():
        if case == "drop":
            data["cases"] = [c for c in cases if c["case"] != fields]
        elif case == "dup":
            data["cases"] = [*cases, dict(cases[0])]
        elif case == "top":
            data.update(fields)
        else:
            next(c for c in data["cases"] if c["case"] == case).update(fields)
    return data


def lineage_tree(tmp_path, data, pin=None):
    rel = f"tests/fixtures/datagen_reference/compare-{'4' * 12}.json"
    f = tmp_path / rel
    f.parent.mkdir(parents=True)
    raw = json.dumps(data).encode()
    if data is not None:
        f.write_bytes(raw)
    table = tmp_path / "lineage.yaml"
    table.write_text(
        "lineage:\n"
        f"  - digest: {A}\n    canonical: {A}\n"
        f"  - digest: {B}\n    canonical: {A}\n    evidence: {rel}\n"
        f'    evidence_sha256: "{pin or hashlib.sha256(raw).hexdigest()}"\n'
    )
    return table


class TestLineageEvidence:
    def test_tracked_table_loads_and_every_row_has_evidence(self):
        table = ci.load_lineage()
        assert "sha256:5fda9025fb9b455b390e1138d82e9f6ef16d214dfa9419815be0111d2f6fce0a" in table
        assert ci.check_lineage_evidence(table, ROOT) == []

    def test_complete_five_case_file_loads(self, tmp_path):
        table = ci.load_lineage(lineage_tree(tmp_path, compare_file()))
        assert ci.check_lineage_evidence(table, tmp_path) == []
        assert table[B].canonical == A

    @pytest.mark.parametrize(
        "changes, needle",
        [
            ({"drop": "F1"}, "expected exactly"),
            ({"dup": True}, "expected exactly"),
            ({"C2": {"equal": False}}, "not equal"),
            ({"F0": {"excluded": []}}, "_corpus/"),
            ({"F2": {"seed_ref": 44}}, "development seed"),
            ({"top": {"image_b": X}}, "image_b"),
            ({"top": {"image_a": X}}, "image_a"),
            ({"C0": {"sha256_b": "f" * 64}}, "manifests differ"),
        ],
    )
    def test_lineage_entry_requires_evidence(self, tmp_path, changes, needle):
        table = ci.load_lineage(lineage_tree(tmp_path, compare_file(**changes)))
        errors = ci.check_lineage_evidence(table, tmp_path)
        assert any(needle in e for e in errors), errors

    def test_missing_or_unpinned_file_is_refused(self, tmp_path):
        table = ci.load_lineage(lineage_tree(tmp_path, compare_file(), pin="0" * 64))
        assert any(
            "does not hash to its pin" in e for e in ci.check_lineage_evidence(table, tmp_path)
        )
        (tmp_path / "tests/fixtures/datagen_reference" / f"compare-{'4' * 12}.json").unlink()
        assert any("unreadable" in e for e in ci.check_lineage_evidence(table, tmp_path))

    @pytest.mark.parametrize(
        "row, needle",
        [
            (f"  - digest: {B}\n    canonical: {A}\n", "evidence must be"),
            (
                f"  - digest: {B}\n    canonical: {A}\n"
                "    evidence: tests/fixtures/datagen_reference/compare-000000000000.json\n"
                f'    evidence_sha256: "{"0" * 64}"\n',
                "evidence must be",
            ),
            (
                f"  - digest: {B}\n    canonical: {A}\n"
                f"    evidence: tests/fixtures/datagen_reference/compare-{'4' * 12}.json\n"
                "    evidence_sha256: nope\n",
                "evidence_sha256",
            ),
            (
                f"  - digest: {B}\n    canonical: {X}\n"
                f"    evidence: tests/fixtures/datagen_reference/compare-{'4' * 12}.json\n"
                f'    evidence_sha256: "{"0" * 64}"\n',
                "not a root row",
            ),
            (
                f"  - digest: {B}\n    canonical: {A}\n"
                f"    evidence: tests/fixtures/datagen_reference/compare-{'4' * 12}.json\n"
                f"    evidence_sha256: {'0' * 64}\n",  # unquoted: YAML reads an int
                "evidence_sha256",
            ),
            (f"  - digest: {A}\n    canonical: {A}\n", "twice"),
            (f"  - digest: {B}\n    canonical: {B}\n    colour: blue\n", "unknown keys"),
            (f"  - digest: {B}\n    canonical: {B}\n    build_commit: 1234567\n", "quoted"),
            (f"  - digest: sha256:short\n    canonical: {A}\n", "64 hex"),
        ],
    )
    def test_malformed_rows_refused(self, tmp_path, row, needle):
        table = tmp_path / "lineage.yaml"
        table.write_text(f"lineage:\n  - digest: {A}\n    canonical: {A}\n{row}")
        with pytest.raises(ci.LineageError, match=needle):
            ci.load_lineage(table)


class TestReviewCases:
    """One case per path the first review showed untested."""

    def test_unhashable_marker_values_never_raise(self):
        obs, _ = observe([marker(node=0, cycles=[1]), marker(node=1, cycles=[1])], series())
        assert obs["markers"]["corpus_series_sha256"] is None
        corpus = corpus_of(obs=obs)
        assert corpus["id_v2"] is None and corpus["problems"]

    @pytest.mark.parametrize("generation", ["not a mapping", ["x"]])
    def test_malformed_series_is_a_problem_not_a_crash(self, generation):
        body = series()
        body["generation"] = generation
        obs, _ = observe((), body)
        assert obs["series"] is None
        assert "not in the series format" in obs["markers"]["problems"][0]
        corpus = corpus_of(obs=obs)
        assert corpus["id_v2"] is None

    def test_invalid_json_marker_is_a_problem(self):
        boto = bucket(two_nodes())
        boto.objects[cd.marker_key(SCOPE, 0, 1)] = b"{not json"
        ms = cd.read_corpus_markers(boto, BUCKET, SCOPE)
        assert "c000-node-0001.json is not valid JSON" in ms.problems[0]
        assert ms.error is None and len(boto.gets) == 2

    def test_newer_marker_format_has_its_own_reason(self):
        obs, _ = observe(two_nodes(format=2), series())
        corpus = corpus_of(obs=obs)
        assert corpus["id_v2_unavailable"] == ci.UNREADABLE_MARKER
        assert any("format 2" in p for p in corpus["problems"])

    def test_too_many_markers_stops_the_read(self, monkeypatch):
        monkeypatch.setattr(cd, "MAX_MARKERS", 1)
        ms = cd.read_corpus_markers(bucket(two_nodes()), BUCKET, SCOPE)
        assert "more than 1" in ms.error and not ms.markers

    def test_cycles_key_enters_the_id_body_above_one(self):
        three = [marker(c, n, cycles=3) for c in range(3) for n in range(2)]
        obs, _ = observe(three, series(cycles_total=3))
        series_hash = obs["markers"]["corpus_series_sha256"]
        body = {"args": series_hash, "model_version": None, "lineage": D, "cycles": 3}
        assert ci.corpus_id_v2(obs, None, D) == (ex._short_hash(body), None)

    def test_extra_cycle_is_a_problem(self):
        markers = [marker(0, 0, total=1, cycles=1), marker(1, 0, total=1, cycles=1)]
        obs, _ = observe(markers, series())
        problems = corpus_of(obs=obs)["problems"]
        assert any("beyond the corpus's 1 cycles" in p for p in problems)

    def test_uppercase_hash_is_not_a_corpus_hash(self):
        obs, _ = observe(two_nodes(h="A" * 64), series())
        corpus = corpus_of(obs=obs)
        assert corpus["id_v2"] is None
        assert any("different arguments" in p for p in corpus["problems"])

    @pytest.mark.parametrize(
        "changes, needle",
        [
            ({"F0": {"digest_b": X}}, "digests differ"),
            ({"top": {"format": 2}}, "format 2"),
            ({"C0": {"objects": 0}}, "compared no objects"),
            ({"F1": {"excluded": ["_corpus/", ""]}}, "exactly"),
            ({"F2": {"argv_canonical": []}}, "argv_canonical"),
        ],
    )
    def test_compare_file_rules(self, tmp_path, changes, needle):
        table = ci.load_lineage(lineage_tree(tmp_path, compare_file(**changes)))
        errors = ci.check_lineage_evidence(table, tmp_path)
        assert any(needle in e for e in errors), errors

    def test_salted_seed_ref_accepted_when_lakebench_has_seed_ref(self, tmp_path, monkeypatch):
        from lakebench.config import datagen_seed

        monkeypatch.setattr(
            datagen_seed, "seed_ref", lambda schema, seed: f"h:{schema}:{seed}", raising=False
        )
        data = compare_file(F0={"seed_ref": "h:financial:43"})
        table = ci.load_lineage(lineage_tree(tmp_path, data))
        assert ci.check_lineage_evidence(table, tmp_path) == []

    def test_canonical_that_is_not_a_root_is_refused(self, tmp_path):
        mid = "sha256:" + "6" * 64
        rel = "tests/fixtures/datagen_reference/compare-{}.json"
        table = table_file(
            tmp_path,
            "lineage:\n"
            f"  - digest: {A}\n    canonical: {A}\n"
            f"  - digest: {mid}\n    canonical: {A}\n    evidence: {rel.format('6' * 12)}\n"
            f'    evidence_sha256: "{"0" * 64}"\n'
            f"  - digest: {B}\n    canonical: {mid}\n    evidence: {rel.format('4' * 12)}\n"
            f'    evidence_sha256: "{"0" * 64}"\n',
        )
        with pytest.raises(ci.LineageError, match="not a root row"):
            ci.load_lineage(table)


class TestFixPassCases:
    """Cases from the fix-pass review (series timing, C360 scale, fleet age,
    persisted lineage shape)."""

    def test_series_older_than_the_markers_lends_no_lineage(self):
        """A series.json from an earlier generate whose args all match (the
        new generate's series write failed): its image did not write these
        markers."""
        obs, _ = observe(two_nodes(), series(updated="2026-10-05T23:00:00Z"))
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"].startswith("declared:")
        assert "written before the corpus markers" in corpus["lineage_notes"][0]

    def test_series_within_the_clock_allowance_is_trusted(self):
        obs, _ = observe(two_nodes(), series(updated="2026-10-05T23:59:30Z"))
        assert corpus_of(obs=obs)["lineage"] == D

    def test_c360_scale_is_not_compared(self):
        """C360 pods get no --scale, so the marker records the generator's
        default while series.json records the config's."""
        body = series()
        body["generation"]["scale"] = 10.0
        obs, _ = observe(two_nodes(corpus_args={"scale": 1.0}), body)
        assert corpus_of(obs=obs)["lineage"] == D

    def test_financial_scale_compares_as_numbers(self):
        body = series()
        body["generation"]["scale"] = "1.000000"
        markers = two_nodes(schema="financial", corpus_args={"scale": 1.0})
        obs, _ = observe(markers, body)
        assert corpus_of(obs=obs)["lineage"] == D
        body["generation"]["scale"] = 2.0
        obs, _ = observe(markers, body)
        assert "another scale" in corpus_of(obs=obs)["lineage_notes"][0]

    def test_stale_fleet_sidecar_is_ignored(self):
        obs, _ = observe(two_nodes(), series(digest=D))
        fleet = {"image": TAG, "image_ids": [f"reg@{X}"], "written_at": "2026-09-01T00:00:00Z"}
        corpus = corpus_of(obs=obs, fleet=fleet)
        assert corpus["lineage"] == D
        assert any("predates this corpus" in n for n in corpus["lineage_notes"])

    def test_huge_numbers_never_raise(self):
        body = series()
        body["generation"]["scale"] = 10**400
        obs, _ = observe(two_nodes(schema="financial", corpus_args={"scale": 1.0}), body)
        corpus = corpus_of(cfg=_cfg("financial"), obs=obs)
        assert corpus["lineage"].startswith("declared:")

    @pytest.mark.parametrize(
        "lineage",
        [
            {"value": "garbage", "observed": True},
            {"value": D, "observed": False},
            "not a mapping",
        ],
    )
    def test_persisted_lineage_is_checked(self, lineage):
        obs, _ = observe(two_nodes(), series())
        obs["lineage"] = lineage
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"] == f"declared:{TAG}" and corpus["lineage_observed"] is False
