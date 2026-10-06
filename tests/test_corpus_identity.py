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
from lakebench.config.schema import ImagesConfig
from lakebench.metrics import corpus_identity as ci
from lakebench.metrics import experiment as ex
from tests.test_experiment import _cfg, _metrics

ROOT = Path(__file__).resolve().parents[1]

H1 = "a" * 64
H2 = "b" * 64
H3 = "c" * 64
D = "sha256:" + "1" * 64  # the image that generated the corpus
X = "sha256:" + "9" * 64  # another image (a stale fleet sidecar)
TAG = ImagesConfig().datagen  # the configured image (the default config)
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
    @pytest.mark.parametrize(
        "markers",
        [
            {},
            {0: [marker(node=0)]},  # node 1 of 2 missing
            {0: [marker(node=1), marker(node=2)]},  # node 0 missing, stray node 2
            {0: [marker(node=0), marker(node=0)]},  # node 0 twice
            {0: [marker(node=0, h=H1), marker(node=1, h=H2)]},  # mixed arguments
            {
                0: [marker(node=0, h=None), marker(node=1, h=None)]
            },  # shape of an image that writes no marker hash
            {0: [marker(node=0, total=2), marker(node=1, total=3)]},
            {1: [marker(1, total=1, cycles=2)]},  # cycle 0 missing
            {0: [marker(total=1, cycles=1)], 1: [marker(1, total=1, cycles=2)]},
        ],
    )
    def test_incomplete_or_mixed_corpus_has_no_series_hash(self, markers):
        assert cd.corpus_series_sha256(markers) is None

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

    def test_marker_that_disagrees_with_its_key_is_a_problem(self):
        boto = bucket([marker(node=0), marker(node=1)])
        key = cd.marker_key(SCOPE, 0, 1)
        boto.objects[key] = json.dumps(marker(node=0)).encode()
        ms = ci.read_corpus_markers(boto, BUCKET, SCOPE)
        assert any("does not match its content" in p for p in ms.problems)
        assert ms.to_dict()["cycles"][0]["nodes_found"] == [0]

# ---------------------------------------------------------------------------
# The corpus block
# ---------------------------------------------------------------------------


class TestCorpusBlock:
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
    def test_build_commit_mismatch_is_problem(self, tmp_path):
        table = table_file(
            tmp_path,
            f'lineage:\n  - digest: {D}\n    canonical: {D}\n    build_commit: "{"f" * 40}"\n',
        )
        obs, _ = observe(two_nodes(), series(), lineage_path=table)
        corpus = corpus_of(obs=obs)
        assert any("built from ffffffffffff" in p for p in corpus["problems"])
        assert corpus["lineage_observed"] is False

    def test_different_builds_are_a_problem(self):
        obs, _ = observe([marker(node=0), marker(node=1, build_commit="def5678")], series())
        corpus = corpus_of(obs=obs)
        assert any("different generator builds" in p for p in corpus["problems"])
        assert corpus["lineage"].startswith("declared:")

    def test_unreadable_table_is_a_problem(self, tmp_path):
        table = table_file(tmp_path, "lineage: [")
        obs, _ = observe(two_nodes(), series(), lineage_path=table)
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"] == f"declared:{TAG}"
        assert any("cannot read" in p for p in corpus["problems"])

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
    def test_series_marker_change_is_a_problem(self):
        obs, _ = observe(two_nodes(), series())
        rep1 = corpus_of(obs=obs)
        obs3, _ = observe(two_nodes(h=H3), series())
        inherited = _inherit(rep1, obs)
        inherited["bronze_listing_sha256"] = obs3["bronze_listing_sha256"]  # digest passed
        rep3 = corpus_of(obs=obs3, inherited=inherited)
        assert "series corpus id differs from repetition 1" in rep3["problems"]

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

class TestReviewCases:
    """One case per path the first review showed untested."""

    def test_too_many_markers_stops_the_read(self, monkeypatch):
        monkeypatch.setattr(cd, "MAX_MARKERS", 1)
        ms = cd.read_corpus_markers(bucket(two_nodes()), BUCKET, SCOPE)
        assert "more than 1" in ms.error and not ms.markers

    def test_extra_cycle_is_a_problem(self):
        markers = [marker(0, 0, total=1, cycles=1), marker(1, 0, total=1, cycles=1)]
        obs, _ = observe(markers, series())
        problems = corpus_of(obs=obs)["problems"]
        assert any("beyond the corpus's 1 cycles" in p for p in problems)

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

    def test_c360_scale_is_not_compared(self):
        """C360 pods get no --scale, so the marker records the generator's
        default while series.json records the config's."""
        body = series()
        body["generation"]["scale"] = 10.0
        obs, _ = observe(two_nodes(corpus_args={"scale": 1.0}), body)
        assert corpus_of(obs=obs)["lineage"] == D

    def test_financial_scale_compares_as_numbers(self):
        body = series()
        body["schema"] = "financial"
        body["generation"]["scale"] = "1.000000"
        markers = two_nodes(schema="financial", corpus_args={"scale": 1.0})
        obs, _ = observe(markers, body)
        assert corpus_of(obs=obs)["lineage"] == D
        body["generation"]["scale"] = 2.0
        obs, _ = observe(markers, body)
        assert "another scale" in corpus_of(obs=obs)["lineage_notes"][0]

class TestThirdPassCases:
    @pytest.mark.parametrize(
        "markers, series_schema, needle",
        [
            ([marker(node=0, schema=None), marker(node=1, schema=None)], None, "known schema"),
            ([marker(node=0), marker(node=1, schema="financial")], None, "known schema"),
            (None, "financial", "another schema"),
        ],
    )
    def test_unknown_or_mixed_schema_lends_no_lineage(self, markers, series_schema, needle):
        body = series()
        if series_schema:
            body["schema"] = series_schema
        obs, _ = observe(markers or two_nodes(), body)
        corpus = corpus_of(obs=obs)
        assert corpus["lineage"].startswith("declared:")
        assert needle in corpus["lineage_notes"][0]


# ---------------------------------------------------------------------------
# ER-9h: the observation is recorded once, before the save
# ---------------------------------------------------------------------------


class TestRecordObservation:
    def test_observation_lands_in_the_inputs_and_the_block(self):
        run = _metrics(_cfg())
        s3 = SimpleNamespace(raw_client=bucket(two_nodes(), series()))
        ci.record_corpus_observation(run, _cfg(), s3)
        obs = run.config_snapshot["experiment_inputs"]["corpus_observation"]
        assert obs["bronze_listing_sha256"] and obs["markers"]["corpus_series_sha256"]
        corpus = run.to_dict()["experiment"]["corpus"]
        assert len(corpus["id_v2"]) == 16 and corpus["lineage"] == D

    def test_failing_store_is_recorded_not_raised(self):
        class Broken:
            @property
            def raw_client(self):
                raise ConnectionError("endpoint unreachable")

        run = _metrics(_cfg())
        ci.record_corpus_observation(run, _cfg(), Broken())
        obs = run.config_snapshot["experiment_inputs"]["corpus_observation"]
        assert obs["bronze_listing_sha256"] is None
        assert "corpus observation failed" in obs["markers"]["error"]

class TestOwnerMarkerIsNotCorpus:
    pass
def test_listing_digest_of_an_empty_scope_is_none():
    assert cd.listing_digest(bucket(data=()), BUCKET, SCOPE) is None
    assert cd.is_sha256_hex(cd.listing_digest(bucket(), BUCKET, SCOPE))
