"""OD-2 identity groups, identity versions and the stored-block rule (EVD-7,
ER-10a; DESIGN-v1.7 ch03 sections 0.1 and 6).

Stored records come from the ER-1 harness (tests/fixtures/records/). Expected
values are derived from each record's own fields (recipe, effective
maintenance id, provenance) in the test, never from the code under test.
"""

from __future__ import annotations

import copy
import tempfile
from pathlib import Path

import pytest

from lakebench.metrics import comparability as cmp
from lakebench.metrics import corpus_identity as ci
from lakebench.metrics import experiment as ex
from lakebench.metrics.storage import MetricsStorage
from tests.fixtures import stored_records as sr
from tests.fixtures.comparability_helpers import SYSID as SYSID
from tests.fixtures.comparability_helpers import _fresh as _fresh
from tests.fixtures.corpus_identity_helpers import observe

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = sr.expected("records")["records"]
EXP1_RECORDS = sorted(r for r, w in EXPECTED.items() if w["generation"] == "exp1")


def _load(record: dict):
    with tempfile.TemporaryDirectory() as tmp:
        return MetricsStorage(tmp)._dict_to_metrics(copy.deepcopy(record))


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


def test_resave_keeps_exp1():
    """S1, the failing case: a v1.6 record loaded and saved by v1.7 keeps
    its block byte for byte (schema exp1, no id_v2, the same digest)."""
    for run_id in EXP1_RECORDS:
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


def test_compaction_operation_of_stored_records():
    for run_id in EXP1_RECORDS:
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


def test_stored_records_gain_no_optional_or_pinset_key():
    """Every stored record is 1.6, single-cycle, with null overrides: no
    derived key appears, so no stored verdict moves through them."""
    for run_id in EXP1_RECORDS:
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

    def test_identity_version_mismatch_is_an_identity_difference(self):
        v2 = _fresh().to_dict()["experiment"]
        v1 = _fresh(markers=False).to_dict()["experiment"]
        assert ex.identity_differences(v1, v2) == ["identity version differs (exp1 vs exp2)"]


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


def test_optional_key_table_rows_name_a_group_and_owner():
    for name, row in cmp.OPTIONAL_IDENTITY_KEYS.items():
        assert row.group in cmp.GROUPS, name
        assert row.owner, name


def test_benchmark_path_refreshes_the_stored_block():
    """``lakebench benchmark`` puts its benchmark in its own record (a copy
    of the run it measured) and must refresh the copied block's benchmark
    half before saving (a stored block is never rebuilt)."""
    src = (ROOT / "src/lakebench/cli/_query.py").read_text()
    replace = src.index("record.benchmark = bench\n")
    save = src.index("storage.save_run(record)", replace)
    assert "refresh_benchmark(record)" in src[replace:save]


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


def _condition_keys(a, b):
    """The Conditions keys that differ between two stored records, as the
    perf gate and reproduce read them (comparability.diff_group)."""
    ca, cb = cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)
    return [d.key for d in cmp.diff_group(ca, cb, cmp.CONDITIONS)]


class TestRelationFolding:
    def test_stub_identity_is_not_v2(self):
        from lakebench.metrics import system_identity as si

        run = _fresh()
        run.config_snapshot["experiment_inputs"]["system_identity"] = si._unobserved_identity(
            "cluster", "x"
        )
        e = run.to_dict()["experiment"]
        assert e["schema"] == "exp1" and "system identity" in e["v2_unavailable"]


def test_local_identity_does_not_block_exp2():
    from lakebench.metrics import system_identity as si

    run = _fresh()
    run.config_snapshot["local"] = True
    run.config_snapshot["experiment_inputs"]["system_identity"] = si._local_identity(None)
    e = run.to_dict()["experiment"]
    assert e["schema"] == "exp2" and e["system"] == "local"
    # The same identity on a cluster run is not an observed one.
    run = _fresh()
    run.config_snapshot["experiment_inputs"]["system_identity"] = si._local_identity(None)
    assert run.to_dict()["experiment"]["schema"] == "exp1"


class TestWrappers:
    def test_optional_key_absent_from_the_baseline_is_a_difference(self):
        run = _fresh().to_dict()["experiment"]
        baseline = ex.identity(run)
        run["architecture"]["spark_executor_overrides"] = {"silver": 12}
        refs = ex.stored_identity_refusals(baseline, ex.result_fingerprints(run), run, "baseline")
        assert any(r.startswith("spark executor overrides differs") for r in refs), refs
        assert not any("older experiment identity" in r for r in refs)


def test_reference_with_no_observed_system_is_refused():
    """A v2 reference whose system fingerprint is the no-part constant
    would match any unsampled run on any cluster: refused."""
    from lakebench.metrics import system_identity as si

    run = _fresh().to_dict()["experiment"]
    baseline = ex.identity(run)
    baseline["system fingerprint"] = si._unobserved_identity("cluster", "x")["fingerprint"]
    refs = ex.stored_identity_refusals(baseline, ex.result_fingerprints(run), run, "baseline")
    assert refs == [
        "not comparable: the reference observed no part of its system; "
        "the baseline cannot be matched to a system"
    ]


def test_config_only_identity_is_not_v2():
    """A cluster run whose Kubernetes client failed has only config-derived
    parts: not an observed system identity."""
    run = _fresh()
    ident = copy.deepcopy(SYSID)
    ident["parts"] = {
        "api_server_ca": {"not_observed": "no Kubernetes client"},
        "storage_endpoint": "10.0.1.50:80",
    }
    run.config_snapshot["experiment_inputs"]["system_identity"] = ident
    e = run.to_dict()["experiment"]
    assert e["schema"] == "exp1" and "system identity" in e["v2_unavailable"]


class TestDerivedBenchmarkRounds:
    """Owner decision 10-01 (EVD-7 P2): continuous records that do not store
    limits.benchmark_rounds have it derived at read time."""

    def test_stored_value_wins_and_identity_does_not_move(self):
        rec = sr.load_record("011043-e338c5")
        before = ex.identity_hash(rec["experiment"])
        assert cmp.classify(rec["experiment"], rec).keys(cmp.CONDITIONS)["benchmark rounds"] == 5
        rec["experiment"]["limits"]["benchmark_rounds"] = 3
        assert cmp.classify(rec["experiment"], rec).keys(cmp.CONDITIONS)["benchmark rounds"] == 3
        assert ex.identity_hash(sr.load_record("011043-e338c5")["experiment"]) == before

    def test_rounds_without_qph_do_not_count(self):
        rec = sr.load_record("011043-e338c5")
        rec["pipeline_benchmark"]["benchmark_rounds"][0]["qph"] = 0
        assert cmp.classify(rec["experiment"], rec).keys(cmp.CONDITIONS)["benchmark rounds"] == 4

    def test_no_record_no_derivation(self):
        exp = sr.load_record("011043-e338c5")["experiment"]
        assert cmp.classify(exp).keys(cmp.CONDITIONS)["benchmark rounds"] is None


def test_derived_rounds_zero_and_odd_values():
    rec = sr.load_record("011043-e338c5")
    rec["pipeline_benchmark"].pop("benchmark_rounds")
    assert cmp.classify(rec["experiment"], rec).keys(cmp.CONDITIONS)["benchmark rounds"] == 0
    rec["pipeline_benchmark"]["benchmark_rounds"] = [{"qph": "12.5"}, {"qph": "x"}, "bad"]
    assert cmp.classify(rec["experiment"], rec).keys(cmp.CONDITIONS)["benchmark rounds"] == 1


class TestSkippedMaintenance:
    """Owner decision 10-03 (b): maintenance skipped on both sides is the
    same maintenance, across table formats."""

    ICE = "m2-2026-09-26:expire_snapshots=skipped_by_user,remove_orphan_files=skipped_by_user,compaction=skipped_by_user"
    DELTA = "m2-2026-09-26+skipped:vacuum=skipped_by_user,compaction=skipped_by_user"

    def test_predicate(self):
        assert cmp.maintenance_equal(self.ICE, self.DELTA)
        assert cmp.maintenance_equal(self.ICE, self.ICE)
        assert cmp.maintenance_equal(None, None)
        # One operation ran: not skipped.
        assert not cmp.maintenance_equal(
            self.ICE, "m2-2026-09-26:vacuum=ran,compaction=skipped_by_user"
        )
        # The composition could not run it: not a user skip.
        assert not cmp.maintenance_equal(
            self.ICE, "m2-2026-09-26:vacuum=not_supported,compaction=not_supported"
        )
        # Another maintenance policy.
        assert not cmp.maintenance_equal(
            self.ICE, "m1-legacy:vacuum=skipped_by_user,compaction=skipped_by_user"
        )
        assert not cmp.maintenance_equal(self.ICE, None)

    def _pair(self, ice_id, delta_id):
        a = _rec(new_id="a")
        b = _rec(new_id="b")
        b["experiment"]["architecture"]["table_format"] = {"type": "delta", "version": "4.0.0"}
        b["experiment"]["architecture"]["recipe"] = "hive-delta-spark-trino"
        a["experiment"]["effective_maintenance"] = {"id": ice_id}
        b["experiment"]["effective_maintenance"] = {"id": delta_id}
        return a, b

    def test_iceberg_and_delta_both_skipped_are_one_condition(self):
        assert "effective maintenance" not in _condition_keys(*self._pair(self.ICE, self.DELTA))

    def test_settings_of_maintenance_that_never_ran_are_not_compared(self):
        a, b = self._pair(self.ICE, self.DELTA)
        a["experiment"]["maintenance_settings"] = {"retention_interval": 1800}
        b["experiment"]["maintenance_settings"] = {"retention_interval": 600}
        assert _condition_keys(a, b) == []
        ran = "m2-2026-09-26:vacuum=ran,compaction=not_supported"
        a2, b2 = self._pair(ran, ran)
        a2["experiment"]["maintenance_settings"] = {"retention_interval": 1800}
        b2["experiment"]["maintenance_settings"] = {"retention_interval": 600}
        assert "maintenance settings" in _condition_keys(a2, b2)

    def test_one_side_ran_maintenance_differs_in_conditions(self):
        ran = "m2-2026-09-26:vacuum=ran,compaction=not_supported"
        assert _condition_keys(*self._pair(self.ICE, ran))[0] == "effective maintenance"
