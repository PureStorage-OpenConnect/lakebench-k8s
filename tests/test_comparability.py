"""OD-2 identity groups, identity versions and the stored-block rule (EVD-7,
ER-10a; DESIGN-v1.7 ch03 sections 0.1 and 6).

Stored records come from the ER-1 harness (tests/fixtures/records/). Expected
values are derived from each record's own fields (recipe, effective
maintenance id, provenance) in the test, never from the code under test.
"""

from __future__ import annotations

import copy
import json
import tempfile
from pathlib import Path

import pytest

from lakebench.metrics import comparability as cmp
from lakebench.metrics import corpus_identity as ci
from lakebench.metrics import experiment as ex
from lakebench.metrics.storage import MetricsStorage
from tests.fixtures import stored_records as sr
from tests.test_corpus_identity import observe, two_nodes
from tests.test_corpus_identity import series as series_body
from tests.test_experiment import _cfg, _metrics

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = sr.expected("records")["records"]
EXP1_RECORDS = sorted(r for r, w in EXPECTED.items() if w["generation"] == "exp1")

SYSID = {
    "type": "cluster",
    "version": 2,
    "fingerprint": "f" * 16,
    "partial": False,
    "parts": {"api_server_ca": "c" * 12, "kubernetes": "v1.31.6"},
}


def _load(record: dict):
    with tempfile.TemporaryDirectory() as tmp:
        return MetricsStorage(tmp)._dict_to_metrics(copy.deepcopy(record))


def _fresh(*, markers: bool = True, system: bool = True):
    """A v1.7 run (identity_version 2 at run start), with or without the
    v2 inputs."""
    run = _metrics(_cfg())
    inputs = run.config_snapshot["experiment_inputs"]
    assert inputs["identity_version"] == 2
    if markers:
        obs, _ = observe(two_nodes(), series_body())
        inputs["corpus_observation"] = obs
    if system:
        inputs["system_identity"] = copy.deepcopy(SYSID)
    return run


# ---------------------------------------------------------------------------
# Stamping: exp2 only with every V2 input
# ---------------------------------------------------------------------------


class TestStamping:
    def test_all_inputs_stamp_exp2(self):
        e = _fresh().to_dict()["experiment"]
        assert e["schema"] == "exp2" and e["identity_version"] == 2
        assert "v2_unavailable" not in e
        assert e["system_identity"]["fingerprint"] == "f" * 16
        assert e["corpus"]["id_v2"]
        assert cmp.generation({"experiment": e}) == cmp.EXP2

    def test_no_marker_stamps_exp1(self):
        """The failing case of S1: without the marker hash the block must not
        read exp2 with an empty id v2."""
        obs, _ = observe((), None)
        run = _fresh(markers=False)
        run.config_snapshot["experiment_inputs"]["corpus_observation"] = obs
        e = run.to_dict()["experiment"]
        assert e["schema"] == "exp1" and "identity_version" not in e
        assert e["v2_unavailable"] == ["corpus id v2"]
        assert e["corpus"]["id_v2"] is None and e["corpus"]["id_v2_unavailable"]

    def test_no_observation_names_it(self):
        e = _fresh(markers=False).to_dict()["experiment"]
        assert e["schema"] == "exp1"
        assert e["corpus"]["id_v2_unavailable"] == ci.NOT_OBSERVED

    def test_no_system_identity_stamps_exp1(self):
        e = _fresh(system=False).to_dict()["experiment"]
        assert e["schema"] == "exp1" and e["v2_unavailable"] == ["system identity"]
        assert "system_identity" not in e

    def test_v16_inputs_never_exp2(self):
        """Inputs written before 1.7 carry no identity_version: exp1 and no
        v1.7 field, whatever else they hold."""
        run = _fresh()
        del run.config_snapshot["experiment_inputs"]["identity_version"]
        e = run.to_dict()["experiment"]
        assert e["schema"] == "exp1"
        assert not {"v2_unavailable", "identity_version"} & set(e)
        assert "access_paths" not in e["architecture"]

    def test_exp1_v17_block_keeps_the_v1_shape(self):
        """An exp1-stamped v1.7 block reads through _identity_v1 with the v1
        keys, the extra v1.7 fields notwithstanding."""
        e = _fresh(markers=False).to_dict()["experiment"]
        assert ex.identity(e) == ex._identity_v1(e)
        assert "system" in ex.identity(e) and "corpus id" in ex.identity(e)


class TestIdentityVersions:
    def test_v2_identity_keys(self):
        e = _fresh().to_dict()["experiment"]
        ident = ex.identity(e)
        assert ident["identity version"] == 2
        assert {"corpus id v2", "system fingerprint", "access paths", "dependency pinset"} <= set(
            ident
        )
        assert not {"generator image", "system", "corpus id", "query access path"} & set(ident)
        assert ident["system fingerprint"] == "f" * 16
        assert ident["access paths"] == {"pipeline": "catalog", "query": "catalog"}

    def test_v2_digest_differs_from_v1(self):
        """New runs of the same config get new ids (EVD-7 acceptance)."""
        v2 = _fresh().to_dict()["experiment"]
        v1 = _fresh(markers=False).to_dict()["experiment"]
        assert ex.identity_hash(v2) != ex.identity_hash(v1)

    @pytest.mark.parametrize("run_id", EXP1_RECORDS)
    def test_stored_v1_identity_is_the_frozen_one(self, run_id):
        exp = sr.load_record(run_id)["experiment"]
        assert ex.identity(exp) == ex._identity_v1(exp)
        assert ex.identity_hash(exp) == EXPECTED[run_id]["identity_digest"]
        assert ex.identity_digest(sr.load_record(run_id)) == EXPECTED[run_id]["identity_digest"]

    def test_v2_baseline_against_v1_run_names_both_versions(self):
        """L8: one refusal naming the versions, not per-key lines."""
        v2 = _fresh().to_dict()["experiment"]
        v1 = _fresh(markers=False).to_dict()["experiment"]
        refs = ex.stored_identity_refusals(
            ex.identity(v2), ex.result_fingerprints(v2), v1, "baseline"
        )
        assert len(refs) == 1
        assert "identity v2 and this run is v1" in refs[0]
        assert "no generator marker" in refs[0]

    def test_v1_baseline_against_v2_run_names_both_versions(self):
        v2 = _fresh().to_dict()["experiment"]
        v1 = _fresh(markers=False).to_dict()["experiment"]
        refs = ex.stored_identity_refusals(
            ex.identity(v1), ex.result_fingerprints(v1), v2, "package"
        )
        assert refs == [
            "not comparable: the package was recorded with experiment identity v1 and this run "
            "is v2; record the package again from a current run"
        ]


# ---------------------------------------------------------------------------
# A stored block is never rebuilt (d3, review d2 item 2)
# ---------------------------------------------------------------------------


def _planted():
    """A saved v1.7 record whose stored exp2 block differs from what today's
    build_experiment makes of the record."""
    d = _fresh().to_dict()
    e = d["experiment"]
    e["corpus"]["id_v2"] = "plantedidv2plant"
    e["corpus"]["id"] = "plantedcorpus1aa"
    e["limits"]["bound"] = ["planted cap"]
    e["repetitions"]["runs"] = 7
    e["support"] = {"state": "planted-state", "basis": "planted"}
    assert ex.build_experiment(_load(d)) != e
    return d


def _caller_to_dict(m):
    return m.to_dict()["experiment"]["corpus"]["id_v2"] == "plantedidv2plant"


def _caller_read_first(m):
    from lakebench.reports.generator import ReportGenerator

    with tempfile.TemporaryDirectory() as tmp:
        html = ReportGenerator(metrics_dir=tmp)._generate_read_first_panel(
            m, passed=True, warnings=[], fail_reasons=[], n_runs=1
        )
    return "plantedcorpu" in html and ex.identity_hash(m.experiment) in html


def _caller_experiment_section(m):
    from lakebench.reports.generator import ReportGenerator

    with tempfile.TemporaryDirectory() as tmp:
        html = ReportGenerator(metrics_dir=tmp)._generate_experiment_section(m)
    return "plantedcorpus1aa" in html


def _caller_caps(m):
    from lakebench.reports.formatter import caps_bound_from

    return caps_bound_from(m) == ["planted cap"]


def _caller_n_runs(m):
    from lakebench.reports.formatter import n_runs_of

    return n_runs_of(m) == 7


def _caller_support(m):
    from lakebench.reports.formatter import support_state_of

    return support_state_of(m) == "planted-state"


def _caller_reproduce(m):
    from lakebench.cli._reproduce import _run_experiment

    return (_run_experiment(m) or {}).get("corpus", {}).get("id_v2") == "plantedidv2plant"


@pytest.mark.parametrize(
    "caller",
    [
        _caller_to_dict,
        _caller_read_first,
        _caller_experiment_section,
        _caller_caps,
        _caller_n_runs,
        _caller_support,
        _caller_reproduce,
    ],
    ids=lambda f: f.__name__.removeprefix("_caller_"),
)
def test_stored_block_never_rebuilt(caller):
    """Each of the seven readers of experiment_block() sees the stored
    block. With the d2 rule (rebuild a current-schema block) every one of
    them sees the rebuilt value."""
    assert caller(_load(_planted()))


@pytest.mark.parametrize("run_id", EXP1_RECORDS)
def test_resave_keeps_exp1(run_id):
    """S1, the failing case: a v1.6 record loaded and saved by v1.7 keeps
    its block byte for byte (schema exp1, no id_v2, the same digest)."""
    rec = sr.load_record(run_id)
    saved = _load(rec).to_dict()["experiment"]
    assert saved == rec["experiment"]
    assert saved["schema"] == "exp1" and "id_v2" not in saved["corpus"]
    assert ex.identity_hash(saved) == EXPECTED[run_id]["identity_digest"]


def test_record_without_block_is_built_once():
    """A record saved before the block existed (inputs, no block) gets one
    built from its inputs, which is then stored."""
    d = _fresh().to_dict()
    del d["experiment"]
    m = _load(d)
    assert m.experiment is None
    built = m.to_dict()["experiment"]
    assert built["schema"] == "exp2"


# ---------------------------------------------------------------------------
# Classification of stored records
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("run_id", EXP1_RECORDS)
def test_no_stored_record_misses_a_required_key(run_id):
    """d2's check, kept: ladder step 0 changes no pinned pair."""
    rec = sr.load_record(run_id)
    c = cmp.classify(rec["experiment"], rec)
    assert cmp.missing_required(c) == []
    assert c.keys(cmp.CORPUS)["corpus role"] == cmp.UNDECLARED_ROLE


#: Compaction operation by recipe, from the statement builders
#: (modules/table_formats/iceberg/maintenance.py build_compaction_sql: Trino
#: optimize at 128MB, Spark Thrift rewrite_data_files defaults), for records
#: whose stored effective-maintenance id says compaction=ran.
_OP_BY_RECIPE = {
    "hive-iceberg-spark-trino": "trino_optimize:128MB",
    "polaris-iceberg-spark-trino": "trino_optimize:128MB",
    "hive-iceberg-spark-thrift": "iceberg_rewrite_data_files",
    "polaris-iceberg-spark-thrift": "iceberg_rewrite_data_files",
}


@pytest.mark.parametrize("run_id", EXP1_RECORDS)
def test_compaction_operation_of_stored_records(run_id):
    exp = sr.load_record(run_id)["experiment"]
    ran = "compaction=ran" in exp["effective_maintenance"]["id"].split(":", 1)[1].split(",")
    want = _OP_BY_RECIPE[exp["architecture"]["recipe"]] if ran else None
    assert cmp.compaction_operation(exp) == want


def test_compaction_operation_recorded_wins():
    exp = sr.load_record("130953-f8a2cf")["experiment"]
    exp["effective_maintenance"]["detail"] = {
        "operations": {
            "compaction": {"operation": "trino_optimize", "params": {"file_size_threshold": "64MB"}}
        }
    }
    assert cmp.compaction_operation(exp) == "trino_optimize:64MB"


@pytest.mark.parametrize("run_id", EXP1_RECORDS)
def test_stored_records_gain_no_optional_or_pinset_key(run_id):
    """Every stored record is 1.6, single-cycle, with null overrides: no
    derived key appears, so no stored verdict moves through them."""
    rec = sr.load_record(run_id)
    c = cmp.classify(rec["experiment"], rec)
    assert cmp.optional_keys(rec["experiment"], rec) == {}
    assert "dependency pinset" not in c.keys(cmp.ARCHITECTURE)


class TestExp1Derivations:
    def _v17(self, pinset):
        rec = sr.load_record("5105a0")
        rec["experiment"]["lakebench"]["lakebench_version"] = "1.7.0"
        if pinset is not None:
            rec.setdefault("provenance", {})["deps"] = {"pinset_sha256": pinset}
        return rec

    def test_exp1_v17_derives_pinset(self):
        a, b = self._v17("a" * 64), self._v17("b" * 64)
        ca, cb = cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)
        assert [d.key for d in cmp.diff_group(ca, cb, cmp.ARCHITECTURE)] == ["dependency pinset"]

    def test_v17_without_pinset_reads_not_recorded(self):
        rec = self._v17(None)
        c = cmp.classify(rec["experiment"], rec)
        assert c.keys(cmp.ARCHITECTURE)["dependency pinset"] == cmp.PINSET_NOT_RECORDED

    def test_v16_record_with_pinset_has_no_key(self):
        """The classifier is keyed on lakebench_version, never the schema
        string: a 1.6 record gains no key even with a provenance pinset."""
        rec = sr.load_record("5105a0")
        rec.setdefault("provenance", {})["deps"] = {"pinset_sha256": "a" * 64}
        c = cmp.classify(rec["experiment"], rec)
        assert "dependency pinset" not in c.keys(cmp.ARCHITECTURE)

    @pytest.mark.parametrize("raw,want", [("1.7.0.dev3", (1, 7)), ("v1.10", (1, 10))])
    def test_version_parse(self, raw, want):
        assert cmp.lakebench_minor({"lakebench": {"lakebench_version": raw}}) == (want, None)

    def test_unreadable_version_is_pre17_with_a_note(self):
        rec = sr.load_record("5105a0")
        rec["experiment"]["lakebench"]["lakebench_version"] = "unknown"
        c = cmp.classify(rec["experiment"], rec)
        assert "dependency pinset" not in c.keys(cmp.ARCHITECTURE)
        assert c.notes == ["lakebench version unreadable"]

    def test_exp1_derives_overrides(self):
        a = sr.load_record("5105a0")
        b = sr.load_record("5105a0")
        b["config_snapshot"]["spark"]["executor_overrides"]["silver"] = 12
        ca, cb = cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)
        diffs = cmp.diff_group(ca, cb, cmp.ARCHITECTURE)
        assert [(d.key, d.b) for d in diffs] == [("spark executor overrides", {"silver": 12})]

    def test_exp1_derives_cycles(self):
        a = sr.load_record("5105a0")
        b = sr.load_record("5105a0")
        b["cycles"] = [{}, {}, {}]
        ca, cb = cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)
        assert [(d.key, d.b) for d in cmp.diff_group(ca, cb, cmp.CORPUS)] == [("cycles", 3)]


class TestDifferences:
    def test_generator_digest_compared_only_when_both_have_one(self):
        a = sr.load_record("5105a0")["experiment"]
        b = copy.deepcopy(a)
        a["corpus"]["datagen"]["digest"] = "sha256:" + "1" * 64
        b["corpus"]["datagen"]["digest"] = None
        assert cmp.diff_group(cmp.classify(a), cmp.classify(b), cmp.CORPUS) == []
        b["corpus"]["datagen"]["digest"] = "sha256:" + "2" * 64
        assert [d.key for d in cmp.diff_group(cmp.classify(a), cmp.classify(b), cmp.CORPUS)] == [
            "generator digest"
        ]

    @pytest.mark.parametrize(
        "a,b,want",
        [
            (43, 43, True),
            (43, 44, False),
            (
                {"seed_ref": "h1", "role": "evaluation"},
                {"seed_ref": "h1", "role": "evaluation"},
                True,
            ),
            ({"seed_ref": None, "role": "unknown", "withheld": "x"}, 43, None),
            (None, 43, None),
        ],
    )
    def test_seeds_equal(self, a, b, want):
        assert cmp.seeds_equal(a, b) is want

    def test_withheld_seed_is_a_difference(self):
        a = sr.load_record("5105a0")["experiment"]
        b = copy.deepcopy(a)
        b["corpus"]["seed"] = {"seed_ref": None, "role": "unknown", "withheld": "x"}
        assert [d.key for d in cmp.diff_group(cmp.classify(a), cmp.classify(b), cmp.CORPUS)] == [
            "seed"
        ]

    def test_system_and_access_path_are_not_conditions(self):
        """OD-2: they moved to the System and Architecture groups."""
        assert "system" not in ex.CONDITION_KEYS
        assert "query access path" not in ex.CONDITION_KEYS
        a = sr.load_record("5105a0")["experiment"]
        b = copy.deepcopy(a)
        b["system"] = "local"
        b["architecture"]["query_access_path"] = "direct_storage"
        assert ex.condition_differences(a, b) == []
        assert ex.identity_differences(a, b) == []
        ca, cb = cmp.classify(a), cmp.classify(b)
        assert [d.key for d in cmp.diff_group(ca, cb, cmp.ARCHITECTURE)] == ["query access path"]
        assert cmp.system_relation(ca, cb)[0] == "different"

    def test_identity_version_mismatch_is_an_identity_difference(self):
        v2 = _fresh().to_dict()["experiment"]
        v1 = _fresh(markers=False).to_dict()["experiment"]
        assert ex.identity_differences(v1, v2) == ["identity version differs (exp1 vs exp2)"]


class TestSystemRelation:
    def _c(self, sysid=None, system="cluster"):
        exp = sr.load_record("5105a0")["experiment"]
        exp["system"] = system
        if sysid is not None:
            exp["system_identity"] = sysid
        return cmp.classify(exp)

    def test_absent_on_both_assumed_same_with_note(self):
        rel, note = cmp.system_relation(self._c(), self._c())
        assert rel == "same" and "assumed the same" in note

    def test_absent_on_one_side_is_different(self):
        """The design rule: identity absent on one side only counts as
        different (a run whose start sample timed out included)."""
        rel, note = cmp.system_relation(self._c(SYSID), self._c())
        assert rel == "different" and "one side only" in note

    def test_no_common_part_is_unknown(self):
        a = copy.deepcopy(SYSID)
        a["parts"] = {"api_server_ca": {"not_observed": "x"}}
        b = copy.deepcopy(SYSID)
        b["parts"] = {"kubernetes": {"not_observed": "x"}}
        # Nothing was compared: different, never assumed the same.
        assert cmp.system_relation(self._c(a), self._c(b))[0] == "different"

    def test_same_fingerprint_with_ca_is_same(self):
        assert cmp.system_relation(self._c(SYSID), self._c(copy.deepcopy(SYSID))) == ("same", None)

    def test_without_ca_is_unknown(self):
        """ER-8 carry: equal shapes without the CA are not one system."""
        a = copy.deepcopy(SYSID)
        a["parts"]["api_server_ca"] = {"not_observed": "no CA"}
        rel, note = cmp.system_relation(self._c(a), self._c(copy.deepcopy(SYSID)))
        assert rel == "unknown" and "API server CA" in note

    def test_different_ca_is_different(self):
        b = copy.deepcopy(SYSID)
        b["parts"]["api_server_ca"] = "d" * 12
        assert cmp.system_relation(self._c(SYSID), self._c(b))[0] == "different"


# ---------------------------------------------------------------------------
# RR-2: a default config has no optional key
# ---------------------------------------------------------------------------

EXAMPLES = sorted((ROOT / "examples").glob("*.yaml"))


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_default_config_has_no_optional_keys(path, monkeypatch):
    from lakebench.config import load_config

    for var in (
        "LAKEBENCH_POLARIS_CLIENT_SECRET",
        "LAKEBENCH_S3_ACCESS_KEY",
        "LAKEBENCH_S3_SECRET_KEY",
    ):
        monkeypatch.setenv(var, "placeholder")
    planned = ex.planned_experiment(load_config(path))
    assert cmp.optional_keys(planned) == {}
    assert not set(cmp.OPTIONAL_IDENTITY_KEYS) & set(ex._identity_v2(planned))


def test_unreadable_optional_key_is_not_the_default(monkeypatch):
    def broken(exp, record):
        raise KeyError("x")

    row = cmp.OPTIONAL_IDENTITY_KEYS["spark conf"]
    monkeypatch.setitem(
        cmp.OPTIONAL_IDENTITY_KEYS,
        "spark conf",
        cmp.OptionalKey(row.group, row.default, broken, row.owner),
    )
    exp = sr.load_record("5105a0")["experiment"]
    assert cmp.optional_keys(exp)["spark conf"] == "unreadable: KeyError"


@pytest.mark.parametrize("path", ["src/lakebench/cli/_run.py", "src/lakebench/cli/_sustained.py"])
def test_run_paths_sample_at_start_and_before_save(path):
    """Each run path that starts a record samples right after start_run
    and right before its save (the batch path is also traced by the run
    harness; the continuous one is not)."""
    src = (ROOT / path).read_text()
    starts = [i for i in range(len(src)) if src.startswith("collector.start_run(", i)]
    assert starts
    for i in starts:
        assert "sample_run_start(collector.current_run, cfg" in src[i : i + 400]
    saves = [
        i for i in range(len(src)) if src.startswith("metrics_storage.save_run(run_metrics)", i)
    ]
    for i in saves:
        assert "sample_run_end(run_metrics, cfg" in src[max(0, i - 200) : i]


def test_optional_key_table_rows_name_a_group_and_owner():
    for name, row in cmp.OPTIONAL_IDENTITY_KEYS.items():
        assert row.group in cmp.GROUPS, name
        assert row.owner, name


def test_benchmark_path_refreshes_the_stored_block():
    """``lakebench benchmark`` replaces the record's benchmark and must
    refresh the stored block's benchmark half before saving (a stored block
    is never rebuilt); CC-25 later gives it its own record."""
    src = (ROOT / "src/lakebench/cli/_query.py").read_text()
    replace = src.index("latest_run.benchmark = bench_metrics")
    save = src.index("storage.save_run(latest_run)", replace)
    assert "refresh_benchmark(latest_run)" in src[replace:save]


def test_records_json_drift_list_is_empty():
    assert sr.expected("records")["known_rebuild_drift"]["runs"] == []
    json.dumps(cmp.REQUIRED_KEYS)  # the table is plain data


# ---------------------------------------------------------------------------
# The ladder (ER-10b): constructed pairs, each edit named in the test
# ---------------------------------------------------------------------------


def _rec(run_id="5105a0", new_id=None, **edits):
    """A pinned record, optionally with a new run id; *edits* are dotted
    paths into the record (``experiment.system=local``)."""
    rec = sr.load_record(run_id)
    if new_id:
        rec["run_id"] = new_id
    for path, value in edits.items():
        node = rec
        keys = path.split("__")
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = value
    return rec


def _sys(ca="c" * 12, **parts):
    out = copy.deepcopy(SYSID)
    out["parts"]["api_server_ca"] = ca
    out["parts"].update(parts)
    from lakebench.metrics.system_identity import fingerprint_of

    out["fingerprint"] = fingerprint_of(out["parts"])
    return out


def _verdict(a, b):
    return cmp.pair_verdict(a if isinstance(a, list) else [a], b if isinstance(b, list) else [b])


class TestLadder:
    def test_a_a_is_a_repeat(self):
        v = _verdict(_rec(), _rec(new_id="x"))
        assert (v.verdict, v.step, v.attribution) == (cmp.LIKE_FOR_LIKE, "8", "repeat")
        assert v.code == 0 and v.comparable

    def test_three_by_three_repeat(self):
        side_a = [_rec(new_id=f"a{i}") for i in range(3)]
        side_b = [_rec(new_id=f"b{i}") for i in range(3)]
        v = _verdict(side_a, side_b)
        assert (v.verdict, v.attribution) == (cmp.LIKE_FOR_LIKE, "repeat")

    def test_confounded_fixture(self):
        """System fingerprint and recipe both differ: CONFOUNDED (13)."""
        a = _rec(experiment__system_identity=_sys())
        b = _rec(
            new_id="b",
            experiment__system_identity=_sys(ca="d" * 12),
        )
        b["experiment"]["architecture"]["recipe"] = "polaris-iceberg-spark-trino"
        b["experiment"]["architecture"]["catalog"] = {"type": "polaris", "version": "1.6.0"}
        v = _verdict(a, b)
        assert (v.verdict, v.code, v.step) == (cmp.CONFOUNDED, 13, "6")
        assert v.keys(cmp.ARCHITECTURE) == ["recipe", "catalog"]
        assert v.system == "different"

    def test_system_only_difference_is_a_system_differential(self):
        v = _verdict(
            _rec(experiment__system_identity=_sys()),
            _rec(new_id="b", experiment__system_identity=_sys(ca="d" * 12)),
        )
        assert (v.verdict, v.attribution) == (cmp.LIKE_FOR_LIKE, "system differential")

    def test_local_vs_cluster_with_the_local_composition_is_confounded(self):
        b = _rec(new_id="b", experiment__system="local")
        b["experiment"]["architecture"].update(
            {"recipe": "none-iceberg-spark-duckdb", "query_access_path": "direct_storage"}
        )
        v = _verdict(_rec(), b)
        assert v.verdict == cmp.CONFOUNDED
        assert v.keys(cmp.ARCHITECTURE) == ["recipe", "query access path"]

    def test_system_not_established_is_never_a_repeat(self):
        """ER-8 carry: without the CA on one side the systems are not shown
        to be one, so the pair is not called a repeat, and with an
        architecture difference it is confounded."""
        a = _rec(experiment__system_identity=_sys(ca={"not_observed": "no CA"}))
        b = _rec(new_id="b", experiment__system_identity=_sys())
        v = _verdict(a, b)
        assert (v.verdict, v.attribution, v.system) == (
            cmp.LIKE_FOR_LIKE,
            "system not established",
            "unknown",
        )
        # With an architecture difference: an architecture differential
        # with the note (the systems are not shown to differ).
        b["experiment"]["architecture"]["recipe"] = "other"
        v = _verdict(a, b)
        assert (v.verdict, v.attribution) == (cmp.LIKE_FOR_LIKE, "architecture differential")
        assert any("not established" in n for n in v.notes)

    def test_identity_on_one_side_only_counts_as_different(self):
        """A 1.6 record against a 1.7 one: a system differential, and with
        an architecture difference CONFOUNDED (never credited to the
        architecture)."""
        b = _rec(new_id="b", experiment__system_identity=_sys())
        assert _verdict(_rec(), b).attribution == "system differential"
        b["experiment"]["architecture"]["recipe"] = "other"
        assert _verdict(_rec(), b).verdict == cmp.CONFOUNDED

    def test_two_local_runs_are_never_one_system(self, monkeypatch):
        from lakebench.metrics import system_identity as si

        a = _rec(experiment__system="local", experiment__system_identity=si._local_identity(None))
        b = _rec(
            new_id="b",
            experiment__system="local",
            experiment__system_identity=si._local_identity(None),
        )
        v = _verdict(a, b)
        assert (v.verdict, v.attribution) == (cmp.LIKE_FOR_LIKE, "system not established")

    def test_version_bump_is_not_comparable(self):
        b = _rec(new_id="b")
        b["experiment"]["workload"]["version"] = "c360-2"
        v = _verdict(_rec(), b)
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "3")
        assert v.keys(cmp.WORKLOAD) == ["workload version"]

    def test_within_side_mismatch(self):
        odd = _rec(new_id="a2")
        odd["experiment"]["corpus"]["scale"] = 2.0
        v = _verdict([_rec(new_id="a1"), odd], [_rec(new_id="b")])
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "2")
        assert v.reasons[0].startswith("side A is not one experiment (a1 vs a2)")
        assert "scale differs (1.0 vs 2.0)" in v.reasons[1:] or any(
            r.startswith("scale differs") for r in v.reasons
        )

    def test_within_side_result_mismatch(self):
        odd = _rec(new_id="a2")
        fps = odd["experiment"]["results"]["fingerprints"]
        name = sorted(fps)[0]
        fps[name] = {**fps[name], "rows": (fps[name].get("rows") or 0) + 1, "exact": "0" * 16}
        v = _verdict([_rec(new_id="a1"), odd], [_rec(new_id="b")])
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "2")
        assert any(name in r for r in v.reasons)

    @pytest.mark.parametrize("order", [(4, 4, 0), (0, 4, 4)])
    def test_outcome_condition_within_a_side_refuses_in_any_order(self, order):
        """A side whose runs ran different round counts (0 is the post-stream
        estimator) is not one experiment, whatever the member order."""
        side_a = []
        for i, rounds in enumerate(order):
            rec = _rec("204941-1d17f4", new_id=f"a{i}")
            rec["experiment"]["limits"]["benchmark_rounds"] = rounds
            side_a.append(rec)
        side_b = [_rec("204941-1d17f4", new_id=f"b{i}") for i in range(3)]
        for rec in side_b:
            rec["experiment"]["limits"]["benchmark_rounds"] = 4
        v = _verdict(side_a, side_b)
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "2")

    def test_within_side_system_difference_refuses(self):
        v = _verdict(
            [
                _rec(new_id="a1", experiment__system_identity=_sys()),
                _rec(new_id="a2", experiment__system_identity=_sys(ca="d" * 12)),
            ],
            [_rec(new_id="b", experiment__system_identity=_sys())],
        )
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "2")
        assert any(r.startswith("system:") for r in v.reasons)

    def test_digest_across_members_of_a_side(self):
        """Generator digests [None, D1, D2] are not one corpus."""
        side = []
        for i, d in enumerate((None, "sha256:" + "1" * 64, "sha256:" + "2" * 64)):
            rec = _rec(new_id=f"a{i}")
            rec["experiment"]["corpus"]["datagen"]["digest"] = d
            side.append(rec)
        v = _verdict(side, [_rec(new_id="b")])
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "2")

    def test_digest_across_sides_uses_every_member(self):
        a = [_rec(new_id="a1"), _rec(new_id="a2")]
        a[1]["experiment"]["corpus"]["datagen"]["digest"] = "sha256:" + "1" * 64
        b = [_rec(new_id="b1"), _rec(new_id="b2")]
        b[1]["experiment"]["corpus"]["datagen"]["digest"] = "sha256:" + "2" * 64
        v = _verdict(a, b)
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "3")
        assert v.keys(cmp.CORPUS) == ["generator digest"]

    def test_corpus_problems_refuse_at_step_3(self):
        b = _rec(new_id="b")
        b["experiment"]["corpus"]["problems"] = ["cycle 0 nodes [3] have no marker"]
        v = _verdict(_rec(), b)
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "3")
        assert v.reasons == ["B: cycle 0 nodes [3] have no marker"]

    def test_v16_success_false_refused(self):
        b = _rec(new_id="b")
        b.pop("verdict", None)
        b["success"] = False
        assert _verdict(_rec(), b).step == "1"

    def test_refused_status_refused(self):
        b = _rec(new_id="b", verdict={"status": "REFUSED", "reasons": []})
        assert _verdict(_rec(), b).step == "1"

    def test_recomputed_verdict_never_promotes(self, monkeypatch):
        """The seam for the verdict recomputed from the record: a stored
        PASSED that recomputes FAILED is refused."""
        from lakebench.metrics import verdict as verdict_mod

        monkeypatch.setattr(
            verdict_mod, "verdict_from_record", lambda rec: {"status": "FAILED"}, raising=False
        )
        v = _verdict(_rec(), _rec(new_id="b"))
        assert v.step == "1" and "(recomputed FAILED)" in v.reasons[0]

    def test_confounded_wins_over_conditions(self):
        """Architecture, system and conditions all differ: 13, not 12."""
        a = _rec(experiment__system_identity=_sys())
        b = _rec(new_id="b", experiment__system_identity=_sys(ca="d" * 12))
        b["experiment"]["architecture"]["recipe"] = "other"
        b["experiment"]["effective_maintenance"]["id"] = "m2-2026-09-26:expire_snapshots=not_run"
        assert _verdict(a, b).code == 13

    def test_exp2_without_benchmark_is_not_established(self):
        """A v2 run with no checked results has no query set id: NOT
        ESTABLISHED (step 4), not identity incomplete."""
        a, b = _fresh().to_dict(), _fresh().to_dict()
        b["run_id"] = "b"
        for rec in (a, b):
            rec["experiment"]["results"] = {
                "query_set_id": None,
                "fingerprints": {},
                "not_checked": "no benchmark ran",
            }
        v = _verdict(a, b)
        assert (v.verdict, v.step) == (cmp.NOT_ESTABLISHED, "4")

    def test_cotenant_load_is_observational(self):
        """The constructed co-tenant-load pair: load differs, nothing else;
        the verdict is a repeat (the winner rule that reads load is v1.8)."""
        low = {"cotenant_requested": {"start": {"cpu": 10.0}, "end": {"cpu": 12.0}}}
        high = {"cotenant_requested": {"start": {"cpu": 300.0}, "end": {"cpu": 280.0}}}
        v = _verdict(_rec(experiment__observed=low), _rec(new_id="b", experiment__observed=high))
        assert (v.verdict, v.attribution) == (cmp.LIKE_FOR_LIKE, "repeat")

    def test_none_required_key_refused(self):
        """S1, the failing case: two v2 records with corpus id v2 None on
        both sides; with step 0 removed they read LIKE-FOR-LIKE."""
        a = _fresh().to_dict()
        b = _fresh().to_dict()
        b["run_id"] = "b"
        for rec in (a, b):
            rec["experiment"]["corpus"]["id_v2"] = None
            rec["success"] = True
        v = _verdict(a, b)
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "0")
        assert v.reasons[0].startswith("identity incomplete: corpus id v2 not recorded on")

    def test_none_on_one_side_refused(self):
        b = _rec(new_id="b")
        b["experiment"]["workload"]["version"] = None
        v = _verdict(_rec(), b)
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "0")
        assert v.reasons == ["identity incomplete: workload version not recorded on b"]

    def test_withheld_seed_refused(self):
        b = _rec(new_id="b")
        b["experiment"]["corpus"]["seed"] = {"seed_ref": None, "role": "unknown", "withheld": "x"}
        v = _verdict(_rec(), b)
        assert v.step == "0" and v.reasons == ["identity incomplete: seed withheld on b"]

    def test_generations_differ(self):
        v2 = _fresh().to_dict()
        v = _verdict(_rec(), v2)
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "0")
        assert v.reasons == ["A was recorded with identity v1 and B with v2"]

    def test_undeclared_role_against_exp2_role(self):
        """An exp2 record with role development against an exp1 record with
        None is refused at step 0 (the versions differ)."""
        v2 = _fresh().to_dict()
        v2["experiment"]["corpus"]["corpus_role"] = "development"
        assert _verdict(_rec(), v2).step == "0"

    def test_failed_member_refused(self):
        bad = _rec(new_id="a2", verdict={"status": "FAILED", "reasons": ["gold has 0 rows"]})
        v = _verdict([_rec(new_id="a1"), bad], [_rec(new_id="b")])
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "1")
        assert v.reasons == ["A run a2 did not pass (gold has 0 rows); fix it and re-run"]

    def test_empty_side_refused(self):
        assert _verdict([], [_rec()]).reasons == ["side A has no run"]

    def test_results_not_established(self):
        b = _rec(new_id="b")
        b["experiment"]["results"] = {"query_set_id": None, "fingerprints": {}, "not_checked": "x"}
        v = _verdict(_rec(), b)
        assert (v.verdict, v.code, v.step) == (cmp.NOT_ESTABLISHED, 11, "4")

    def test_different_results(self):
        b = _rec(new_id="b")
        fps = b["experiment"]["results"]["fingerprints"]
        name = sorted(fps)[0]
        fps[name] = {**fps[name], "rows": (fps[name].get("rows") or 0) + 1, "exact": "0" * 16}
        v = _verdict(_rec(), b)
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "5")

    def _v17(self, pinset, new_id=None):
        rec = _rec(new_id=new_id)
        rec["experiment"]["lakebench"]["lakebench_version"] = "1.7.0"
        rec["provenance"]["deps"] = {"pinset_sha256": pinset}
        return rec

    def test_pinset_only_difference_is_not_like_for_like(self):
        """S8, the owner's rule: same composition, different jars (7a)."""
        v = _verdict(self._v17("a" * 64), self._v17("b" * 64, "b"))
        assert (v.verdict, v.step) == (cmp.NOT_LIKE_FOR_LIKE, "7a")
        assert v.reasons == [
            "same composition, different dependency sets (dependency pinset differs)"
        ]

    def test_pinset_with_other_arch_difference_is_differential(self):
        b = self._v17("b" * 64, "b")
        b["experiment"]["architecture"]["catalog"] = {"type": "polaris", "version": "1.6.0"}
        v = _verdict(self._v17("a" * 64), b)
        assert (v.verdict, v.step, v.attribution) == (
            cmp.LIKE_FOR_LIKE,
            "8",
            "architecture differential",
        )

    def test_dev_build_records_carry_the_pinset(self):
        """A 1.7 development build still reports version 1.6; its blocks
        carry v2_unavailable, which only 1.7 writes, so the pinset key is
        present and a pinset-only difference is 7a, not a repeat."""
        recs = []
        for i, pin in enumerate(("a" * 64, "b" * 64)):
            rec = _rec(new_id=f"r{i}")
            rec["experiment"]["v2_unavailable"] = ["corpus id v2"]
            rec["provenance"]["deps"] = {"pinset_sha256": pin}
            recs.append(rec)
        assert recs[0]["experiment"]["lakebench"]["lakebench_version"].startswith("1.6")
        assert _verdict(recs[0], recs[1]).step == "7a"

    def test_both_not_recorded_pinsets_note(self):
        recs = [self._v17(None, f"r{i}") for i in range(2)]
        for rec in recs:
            rec["provenance"].pop("deps")
        v = _verdict(recs[0], recs[1])
        assert (v.attribution, "dependency set not recorded" in v.notes) == ("repeat", True)

    def test_pinset_v16_pair_notes_not_recorded(self):
        v = _verdict(_rec(), _rec(new_id="b"))
        assert "dependency set not recorded" in v.notes

    def test_sessions_run_is_an_outcome_condition(self):
        """S9: sessions run [8] against [3] is not like-for-like in compare
        and not a refusal for the perf gate and reproduce."""
        a = _rec(experiment__investigators={"requested": 8, "run": [8]})
        b = _rec(new_id="b", experiment__investigators={"requested": 8, "run": [3]})
        v = _verdict(a, b)
        assert (v.verdict, v.keys(cmp.CONDITIONS)) == (
            cmp.NOT_LIKE_FOR_LIKE,
            ["investigator sessions"],
        )
        assert "investigator sessions" in ex.OUTCOME_CONDITION_KEYS

    def test_to_dict_is_json(self):
        json.dumps(_verdict(_rec(), _rec(new_id="b")).to_dict())


class TestWrappers:
    def test_unknown_schema_reads_as_no_provenance(self):
        """The wrappers and the ladder agree on a block they cannot read."""
        b = _rec(new_id="b")
        b["experiment"]["schema"] = "exp3"
        prov, _, _ = ex.refusals(_rec(), b)
        assert prov and "no provenance" in prov[0]
        assert _verdict(_rec(), b).step == "1"

    def test_version_mismatch_is_one_refusal_line(self):
        v2 = _fresh().to_dict()
        prov, _, _ = ex.refusals(_rec(), v2)
        assert sum("identity v" in p for p in prov) == 1

    def test_optional_key_absent_from_the_baseline_is_a_difference(self):
        run = _fresh().to_dict()["experiment"]
        baseline = ex.identity(run)
        run["architecture"]["spark_executor_overrides"] = {"silver": 12}
        refs = ex.stored_identity_refusals(baseline, ex.result_fingerprints(run), run, "baseline")
        assert any(r.startswith("spark executor overrides differs") for r in refs), refs
        assert not any("older experiment identity" in r for r in refs)

    def test_like_for_like_lists_the_confounded_line(self):
        a = _rec(experiment__system_identity=_sys())
        b = _rec(new_id="b", experiment__system_identity=_sys(ca="d" * 12))
        b["experiment"]["architecture"]["recipe"] = "other"
        lines = ex.like_for_like(a, b)
        assert lines and lines[0].startswith("architecture and system both differ")

    def test_like_for_like_lists_the_pinset_line(self):
        t = TestLadder()
        lines = ex.like_for_like(t._v17("a" * 64), t._v17("b" * 64, "b"))
        assert lines == ["same composition, different dependency sets (dependency pinset differs)"]

    def test_refusals_carry_step_0(self):
        b = _rec(new_id="b")
        b["experiment"]["workload"]["version"] = None
        prov, _, _ = ex.refusals(_rec(), b)
        assert prov[0] == "identity incomplete: workload version not recorded on b"
