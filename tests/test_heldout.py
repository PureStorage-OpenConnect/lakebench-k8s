"""Held-out AML seeds as salted hashes read at run time.

Every held-out value here is a test-only seed from
``tests/fixtures/heldout_test_seeds.py``; no test reads, holds or prints a
registered evaluation or robustness seed.
"""

from __future__ import annotations

import copy
import importlib
import json
import logging
import random
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lakebench.config import datagen_seed as ds
from tests.fixtures import heldout_test_seeds as ts

ROOT = Path(__file__).resolve().parents[1]
PROD = ROOT / "src/lakebench/spark/data/aml/heldout_hashes.json"
PREREG = ROOT / "src/lakebench/spark/data/aml/aml_preregistration.json"
EV, RB, SP = ts.TEST_EVALUATION_SEED, ts.TEST_ROBUSTNESS_SEED, ts.TEST_SPENT_SEED
TEST_SEEDS = (EV, RB, SP)


def _spellings(seed: int) -> list[str]:
    return [str(seed), f"{seed:_}", hex(seed), hex(seed).upper().replace("0X", "0x")]


def _no_seed_in(text: str) -> bool:
    return not any(s in text for seed in TEST_SEEDS for s in _spellings(seed))


@pytest.fixture
def held(monkeypatch):
    return ts.use_fixture(monkeypatch)


# ---------------------------------------------------------------------------
# The file, the floor and the fixture
# ---------------------------------------------------------------------------


def test_production_file_loads_strictly():
    h = ds.load_heldout(PROD)
    doc = json.loads(PROD.read_text())
    assert doc["format"] == ds.HELDOUT_FORMAT and doc["algorithm"] == ds.HELDOUT_ALGORITHM
    assert h.absence_check in ds.ABSENCE_MODES
    assert ds.heldout_path() == PROD


def test_production_file_has_a_hash_per_role():
    doc = json.loads(PROD.read_text())
    for role in ds.PROTECTED_ROLES:
        assert doc["roles"][role], role


def test_floor_matches_file():
    # The compiled floor is the file's first entry per role, under the file's salt.
    doc = json.loads(PROD.read_text())
    assert ds._HELDOUT_FLOOR["salt"] == doc["salt"]
    for role in ds.PROTECTED_ROLES:
        assert tuple(ds._HELDOUT_FLOOR["roles"][role]) == (doc["roles"][role][0],), role


def test_floor_matches_rust():
    rs = ROOT / "datagen_rs/src/heldout.rs"
    if not rs.is_file():
        pytest.skip("datagen_rs/src/heldout.rs lands with the Rust seed check (CD-5)")
    src = rs.read_text()
    salt = re.search(r'FLOOR_SALT: &str = "([0-9a-f]{64})"', src)
    assert salt and salt.group(1) == ds._HELDOUT_FLOOR["salt"]
    for role in ds.PROTECTED_ROLES:
        found = re.findall(rf'Role::{role.capitalize()},\s*"([0-9a-f]{{64}})"', src)
        assert tuple(found) == tuple(ds._HELDOUT_FLOOR["roles"][role]), role


def test_fixture_is_not_production():
    prod = json.loads(PROD.read_text())
    fix = json.loads(ts.FIXTURE.read_text())
    assert fix["salt"] not in (prod["salt"], ds._HELDOUT_FLOOR["salt"])
    prod_hashes = {h for hs in prod["roles"].values() for h in hs}
    floor_hashes = {h for hs in ds._HELDOUT_FLOOR["roles"].values() for h in hs}
    fix_hashes = {h for hs in fix["roles"].values() for h in hs}
    assert not fix_hashes & (prod_hashes | floor_hashes)
    for s in TEST_SEEDS:
        assert s.bit_length() == 63
        assert ds.heldout_role(s, ds.load_heldout(PROD)) is None


def test_real_seeds_still_refused_under_fixture(held):
    # The fixture replaces the file, never the compiled floor: the production
    # floor hashes and salt come along unchanged.
    assert held.floor_salt == ds._HELDOUT_FLOOR["salt"] != held.salt
    for role in ds.PROTECTED_ROLES:
        assert held.floor[role] == frozenset(ds._HELDOUT_FLOOR["roles"][role])


def test_floor_checked_under_its_own_salt(monkeypatch):
    # A seed known only to the floor (made under another salt) still reads
    # as its role with a fixture file that does not list it.
    other_salt = "ab" * 32
    floor = {"salt": other_salt, "roles": {"evaluation": (ds.seed_hash(other_salt, 12345),)}}
    floor["roles"]["robustness"] = (ds.seed_hash(other_salt, 67890),)
    monkeypatch.setattr(ds, "_HELDOUT_FLOOR", floor)
    h = ts.load()
    assert ds.heldout_role(12345, h) == "evaluation"
    assert ds.heldout_role(67890, h) == "robustness"
    assert ds.heldout_role(EV, h) == "evaluation"  # the file still counts too
    assert ds.heldout_role(43, h) is None


def test_floor_survives_stripped_file(tmp_path, monkeypatch):
    other_salt = "cd" * 32
    floor = {
        "salt": other_salt,
        "roles": {
            r: (ds.seed_hash(other_salt, s),) for r, s in (("evaluation", EV), ("robustness", RB))
        },
    }
    monkeypatch.setattr(ds, "_HELDOUT_FLOOR", floor)
    doc = json.loads(PROD.read_text())
    doc["roles"] = {"evaluation": [], "robustness": []}
    p = tmp_path / "stripped.json"
    p.write_text(json.dumps(doc))
    h = ds.load_heldout(p)
    assert ds.heldout_role(EV, h) == "evaluation" and ds.heldout_role(RB, h) == "robustness"
    # Re-salted file: the floor is still checked under its own salt.
    doc["salt"] = "ef" * 32
    p.write_text(json.dumps(doc))
    assert ds.heldout_role(EV, ds.load_heldout(p)) == "evaluation"


def test_uninitialised_floor_fails_closed(monkeypatch):
    monkeypatch.setattr(
        ds, "_HELDOUT_FLOOR", {"salt": "", "roles": {"evaluation": (), "robustness": ()}}
    )
    with pytest.raises(ValueError, match="floor"):
        ds.load_heldout(PROD)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(format=2),
        lambda d: d.update(algorithm="md5"),
        lambda d: d.update(salt="xyz"),
        lambda d: d["roles"]["evaluation"].append("not-a-hash"),
        lambda d: d["roles"].update(calibration=[]),
        lambda d: d["roles"].pop("robustness"),
        lambda d: d.update(spent=["42"]),
        lambda d: d.update(absence_check="off"),
        lambda d: d.update(extra=1),
    ],
)
def test_load_heldout_is_strict(mutate, tmp_path):
    doc = json.loads(ts.FIXTURE.read_text())
    mutate(doc)
    p = tmp_path / "h.json"
    p.write_text(json.dumps(doc))
    with pytest.raises(ValueError) as e:
        ds.load_heldout(p)
    assert _no_seed_in(str(e.value))


def test_missing_file_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(ds, "_PREREG_PATH", tmp_path / "nowhere" / "prereg.json")
    monkeypatch.setattr(ds, "__file__", str(tmp_path / "datagen_seed.py"))
    monkeypatch.delenv("LB_HELDOUT_HASHES", raising=False)
    with pytest.raises(FileNotFoundError):
        ds.heldout_path()
    monkeypatch.setenv("LB_HELDOUT_HASHES", str(ts.FIXTURE))
    assert ds.heldout_path() == ts.FIXTURE


# ---------------------------------------------------------------------------
# Recovery and the corpus verdict
# ---------------------------------------------------------------------------


@pytest.fixture
def aml_features(monkeypatch):
    for mod in ("pyspark", "pyspark.sql", "pyspark.sql.functions"):
        monkeypatch.setitem(sys.modules, mod, MagicMock())
    monkeypatch.syspath_prepend(str(ROOT / "src/lakebench/spark/scripts"))
    sys.modules.pop("aml_features", None)
    af = importlib.import_module("aml_features")
    yield af
    sys.modules.pop("aml_features", None)


def test_recover_round_trip(aml_features):
    rnd = random.Random(7)
    assert ds.TID_SEED_STRIDE == aml_features._TID_SEED_STRIDE
    for _ in range(10_000):
        x = rnd.getrandbits(64)
        assert ds._splitmix64(x) == aml_features._splitmix64(x)
        assert ds.unsplitmix64(ds._splitmix64(x)) == x
        seed = rnd.getrandbits(63)
        tid, j = rnd.randrange(15), rnd.randrange(10**6)
        inner = aml_features._splitmix64(
            (0xF100 + tid * aml_features._TID_SEED_STRIDE + j) & ds._MASK64
        )
        iseed = ds._signed64(aml_features._splitmix64(seed ^ inner))
        assert ds.recover_corpus_seeds([(f"FAN_IN_{tid}_{j:07d}", iseed)]) == {seed}


def test_tid_stride_matches_the_generator():
    src = (ROOT / "datagen_rs/src/typology.rs").read_text()
    m = re.search(r"const TID_SEED_STRIDE: i64 = ([0-9_]+);", src)
    assert m and int(m.group(1).replace("_", "")) == ds.TID_SEED_STRIDE


def test_recover_handles_every_row_lazily():
    rows = iter(ts.manifest_rows(43, 500) + ts.manifest_rows(7777, 500, start=500))
    assert ds.recover_corpus_seeds(rows) == {43, 7777}


def test_screening_constants_match_the_generator():
    src = (ROOT / "datagen_rs/src/screening.rs").read_text()
    salt = re.search(r"pub const SCREEN_SALT: u64 = (0x[0-9A-Fa-f_]+);", src)
    assert salt and int(salt.group(1).replace("_", ""), 16) == ds.SCREEN_SALT
    rel = re.search(r"pub const MAX_REL: usize = ([0-9]+);", src)
    assert rel and int(rel.group(1)) == ds._SCREEN_MAX_REL
    assert "splitmix64(0x51_0000 + (k as u64) * 4 + r as u64)" in src
    assert 'format!("SANCTIONS_MATCH_{j_s:07}")' in src and 'format!("PEP_MATCH_{j_p:07}")' in src


def test_screening_rows_check_against_the_recovered_seed(held):
    rows = ts.manifest_rows(43, 60) + ts.screening_rows(43, 20)
    assert ds.recover_corpus_seeds(rows) == {43}
    # A screening row from another seed (here the fixture evaluation seed) is
    # explained by no recovered seed: the manifest is refused, never passed.
    for other in (EV, 44):
        with pytest.raises(ds.CorpusSeedError, match="screening rows") as e:
            ds.recover_corpus_seeds(ts.manifest_rows(43, 60) + ts.screening_rows(other, 1))
        assert _no_seed_in(str(e.value))
    with pytest.raises(ds.CorpusSeedError, match="typology row"):
        ds.recover_corpus_seeds(ts.screening_rows(43, 5))


@pytest.mark.parametrize(
    "rows",
    [[], [("STACK_3_0000001", None)], [(None, 5)], [("nounderscore", 5)], [("STACK_x_1", 5)]],
)
def test_unrecoverable_manifest_raises(rows):
    with pytest.raises(ds.CorpusSeedError):
        ds.recover_corpus_seeds(rows)


def test_heldout_behind_row_201_found(held, aml_features):
    # 250 calibration rows sort first by typology_id, the fixture evaluation
    # seed's 50 rows after them: a 200-row sample sees only calibration.
    cal = ts.manifest_rows(43, 250, typologies=(("GATHER_SCATTER", 0),))
    ev = ts.manifest_rows(EV, 50, typologies=(("STACK", 3),))
    rows = cal + ev
    assert [r[0] for r in sorted(rows)][:200] == [r[0] for r in cal][:200]
    v = ds.corpus_verdict(iter(rows), claimed=43, spent=[42])
    assert (v.verdict, v.role, v.seeds_found, v.matches_claim) == (
        "refused",
        "evaluation",
        2,
        False,
    )
    # The reverted behaviour: recovery capped at the first 200 sorted rows.
    capped = ds.corpus_verdict(iter(sorted(rows)[:200]), claimed=43, spent=[42])
    assert capped.verdict == "ok"


def test_corpus_verdict_roles_and_claim(held):
    ok = ds.corpus_verdict(ts.manifest_rows(43, 30), claimed=43, spent=[42])
    assert (ok.verdict, ok.role, ok.matches_claim) == ("ok", None, True)
    assert ds.corpus_verdict(ts.manifest_rows(43, 30), spent=[42]).matches_claim is None
    rb = ds.corpus_verdict(ts.manifest_rows(RB, 30), claimed=RB, spent=[42])
    assert (rb.verdict, rb.role, rb.matches_claim) == ("refused", "robustness", True)
    sp = ds.corpus_verdict(ts.manifest_rows(42, 30), spent=[42])
    assert (sp.verdict, sp.role) == ("refused", "spent")
    # A spent seed known only to the file's spent list.
    assert ds.corpus_verdict(ts.manifest_rows(SP, 30), spent=[]).role == "spent"
    mixed = ts.manifest_rows(RB, 10) + ts.manifest_rows(EV, 10, start=10)
    assert ds.corpus_verdict(mixed, claimed=RB, spent=[]).role == "evaluation"


def test_corpus_verdict_prints_no_seed(held):
    v = ds.corpus_verdict(ts.manifest_rows(EV, 20), claimed=EV, spent=[])
    assert _no_seed_in(repr(v)) and _no_seed_in(str(v))
    for bad in ([("EVIL", EV)], [("STACK_3_0000001", None), ("STACK_3_0000002", EV)]):
        with pytest.raises(ds.CorpusSeedError) as e:
            ds.corpus_verdict(bad, claimed=EV, spent=[])
        assert _no_seed_in(str(e.value))
    assert _no_seed_in(repr(held))


# ---------------------------------------------------------------------------
# The guard's messages and the scorer
# ---------------------------------------------------------------------------


def _corpora(**over):
    c = json.loads(PREREG.read_text())["corpora"]
    for k in ("evaluation_seed", "robustness_seed"):  # never held by a test
        c.pop(k, None)
    return {**c, "registered_looks_open": True, **over}


def test_guard_messages_never_print_a_heldout_seed(held):
    c = _corpora()
    msgs = [
        ds.aml_seed_error(c, EV),
        ds.aml_seed_error(c, RB, "evaluation"),
        ds.aml_seed_error(c, EV, "calibration"),
        ds.aml_seed_error(c, 7777, None, [EV]),
        ds.aml_seed_error(c, EV, "evaluation", ["evaluation"], claim_verified=False),
        ds.aml_seed_error(c, 43, None, ["robustness"], claim_verified=False),
        ds.aml_seed_error({**c, "registered_looks_open": False}, EV, "evaluation"),
    ]
    assert all(msgs) and all(_no_seed_in(m) for m in msgs), msgs


def test_guard_with_recovered_roles(held):
    c = _corpora()
    assert ds.aml_seed_error(c, EV, "evaluation", ["evaluation"], claim_verified=True) is None
    assert ds.aml_seed_error(c, None, None, ["evaluation"]) is not None
    assert "spent" in ds.aml_seed_error(c, None, None, ["spent"])
    assert ds.aml_seed_error(c, None, None, ["evaluation"], counts_only=True) is None
    assert "not only the claimed" in ds.aml_seed_error(c, EV, "evaluation", ["evaluation"])
    with pytest.raises(ValueError):
        ds.aml_seed_error(c, 43, None, ["calibration"])


def test_registered_look_needs_a_seed(held):
    with pytest.raises(ValueError, match="names its seed in datagen.seed"):
        ds.resolve_seed(None, "financial", "evaluation")
    assert ds.resolve_seed(None, "financial", "calibration") == _corpora()["calibration_seed"]


class _Manifest:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *cols):
        assert cols == ("typology_id", "seed")
        return self

    def toLocalIterator(self):  # noqa: N802 -- the pyspark name
        return iter({"typology_id": t, "seed": s} for t, s in self.rows)


class _Af:
    """What the scorer uses from aml_features, over plain rows."""

    def corpus_seed_check(self, manifest, seed):
        rows = sorted(manifest.rows)[:200]
        hit = sum(ds.recover_corpus_seeds([r]) == {seed} for r in rows)
        return {"claimed_seed": seed, "matched_share": hit / len(rows) if rows else None}

    def manifest_stamp_groups(self, _manifest, keys):
        return [(dict.fromkeys(keys), 7)]


@pytest.fixture
def scorer(monkeypatch, held):
    for mod in ("pyspark", "pyspark.sql", "pyspark.sql.functions"):
        monkeypatch.setitem(sys.modules, mod, MagicMock())
    monkeypatch.syspath_prepend(str(ROOT / "src/lakebench/spark/scripts"))
    monkeypatch.syspath_prepend(str(ROOT / "src/lakebench/aml"))
    sys.modules.pop("score_financial_reference", None)
    ref = importlib.import_module("score_financial_reference")
    import fidelity_gate

    opened = json.loads(PREREG.read_text())
    opened["corpora"]["registered_looks_open"] = True
    # Decoys for the pre-registration's plaintext keys: the scorer must not
    # read them, and a scorer that still did (the reverted one) guards only
    # these values, so a held-out fixture corpus reaches it unrefused.
    opened["corpora"]["evaluation_seed"], opened["corpora"]["robustness_seed"] = 1, 2
    monkeypatch.setattr(fidelity_gate, "load_preregistration", lambda *a, **k: (opened, "x"))
    monkeypatch.delenv("LB_DATAGEN_CORPUS_ROLE", raising=False)
    monkeypatch.delenv("LB_DATAGEN_ROBUSTNESS_PERTURBATION", raising=False)
    yield ref
    sys.modules.pop("score_financial_reference", None)


def test_misclaimed_heldout_corpus_refused(scorer, monkeypatch):
    # A corpus generated from the (fixture) evaluation seed, claimed as the
    # calibration seed: refused before anything is computed.
    monkeypatch.setenv("LB_DATAGEN_SEED", "43")
    m = _Manifest(ts.manifest_rows(EV, 40))
    with pytest.raises(SystemExit, match="refusing to score this corpus") as e:
        scorer._refuse_guarded_corpus(_Af(), m, counts_only=False)
    assert _no_seed_in(str(e.value))
    # The honest calibration corpus scores.
    assert scorer._refuse_guarded_corpus(
        _Af(), _Manifest(ts.manifest_rows(43, 40)), counts_only=False
    )


def test_scorer_registered_look_needs_every_row_from_the_claim(scorer, monkeypatch):
    monkeypatch.setenv("LB_DATAGEN_SEED", str(EV))
    monkeypatch.setenv("LB_DATAGEN_CORPUS_ROLE", "evaluation")
    assert scorer._refuse_guarded_corpus(
        _Af(), _Manifest(ts.manifest_rows(EV, 40)), counts_only=False
    )
    mixed = _Manifest(ts.manifest_rows(EV, 40) + ts.manifest_rows(43, 1, start=40))
    with pytest.raises(SystemExit, match="refusing to score"):
        scorer._refuse_guarded_corpus(_Af(), mixed, counts_only=False)


def test_scorer_refuses_an_unrecoverable_manifest(scorer, monkeypatch):
    monkeypatch.setenv("LB_DATAGEN_SEED", "43")
    bad = _Manifest(ts.manifest_rows(43, 5) + [("STACK_3_0000099", None)])
    with pytest.raises(SystemExit, match="cannot be recovered"):
        scorer._refuse_guarded_corpus(_Af(), bad, counts_only=False)


def test_scorer_refuses_without_the_hash_file(scorer, monkeypatch):
    def gone():
        raise FileNotFoundError("heldout_hashes.json not found")

    monkeypatch.setattr(ds, "_heldout", gone)
    monkeypatch.setenv("LB_DATAGEN_SEED", "43")
    with pytest.raises(SystemExit, match="refusing to score"):
        scorer._refuse_guarded_corpus(_Af(), _Manifest(ts.manifest_rows(43, 5)), counts_only=False)


def test_corpus_role_without_prereg_plaintext(held):
    from lakebench.aml.fidelity_gate import corpus_role

    p = json.loads(PREREG.read_text())
    for k in ("evaluation_seed", "robustness_seed"):
        p["corpora"].pop(k, None)
    assert corpus_role(EV, p) == "evaluation"
    assert corpus_role(str(RB), p) == "robustness"
    assert corpus_role(p["corpora"]["calibration_seed"], p) == "calibration"
    assert corpus_role(7777, p) == "other" and corpus_role("x", p) == "other"
    assert corpus_role(None, p) == "unknown"


# ---------------------------------------------------------------------------
# Append-only history
# ---------------------------------------------------------------------------


def _doc():
    return json.loads(ts.FIXTURE.read_text())


def _looks(*entries):
    return {"looks": list(entries)}


def test_history_allows_appends():
    old, new = _doc(), _doc()
    new["roles"]["evaluation"].append(ds.seed_hash(new["salt"], 99))
    new["spent"].append(43)
    new["absence_check"] = "enforce"
    assert ds.heldout_history_problems(old, new, _looks()) == []
    assert ds.heldout_history_problems(None, _doc(), None) == []


@pytest.mark.parametrize(
    ("mutate", "needle"),
    [
        (lambda d: d["roles"]["evaluation"].pop(), "roles.evaluation[0] removed"),
        (lambda d: d["roles"]["robustness"].__setitem__(0, "0" * 64), "roles.robustness[0] edited"),
        (lambda d: d.update(salt="ab" * 32), "salt changed"),
        (lambda d: d["spent"].pop(0), "spent[1] removed"),
        (lambda d: d["spent"].__setitem__(0, 41), "spent[0] edited"),
        (lambda d: d.update(_doc="rewritten"), "_doc changed"),
        (lambda d: d["roles"].pop("robustness"), "roles.robustness"),
    ],
)
def test_history_refuses_removal_and_resalt(mutate, needle):
    old, new = _doc(), _doc()
    mutate(new)
    problems = ds.heldout_history_problems(old, new, _looks())
    assert any(needle in p for p in problems), problems
    assert all(_no_seed_in(p) for p in problems)


def test_history_refuses_enforce_back_to_report():
    old = _doc()
    old["absence_check"] = "enforce"
    assert ds.heldout_history_problems(old, _doc(), _looks())


def test_spent_append_of_heldout_needs_recorded_look():
    old, new = _doc(), _doc()
    new["spent"].append(EV)
    problems = ds.heldout_history_problems(old, new, _looks())
    assert len(problems) == 1 and "evaluation" in problems[0] and _no_seed_in(problems[0])
    started = {"role": "evaluation", "seed": EV, "state": "started"}
    assert ds.heldout_history_problems(old, new, _looks(started))
    done = {**started, "state": "complete", "report_sha256": "a" * 64}
    assert ds.heldout_history_problems(old, new, _looks(done)) == []
    wrong_role = {**done, "role": "robustness"}
    assert ds.heldout_history_problems(old, new, _looks(wrong_role))
    cal = _doc()
    cal["spent"].append(43)
    assert ds.heldout_history_problems(old, cal, _looks()) == []


def test_spent_append_checked_under_the_floor_salt(monkeypatch):
    # A file re-salted so its own list misses the seed: the floor still sees it.
    floor_salt = "12" * 32
    monkeypatch.setattr(
        ds,
        "_HELDOUT_FLOOR",
        {
            "salt": floor_salt,
            "roles": {
                "evaluation": (ds.seed_hash(floor_salt, 5551212),),
                "robustness": ("0" * 64,),
            },
        },
    )
    old, new = _doc(), _doc()
    new["spent"].append(5551212)
    problems = ds.heldout_history_problems(old, new, _looks())
    assert len(problems) == 1 and "evaluation" in problems[0]


# ---------------------------------------------------------------------------
# Absence check over rendered ConfigMaps
# ---------------------------------------------------------------------------


def test_planted_token_fails(held):
    texts = {f"m/k{i}": f"x = {tok}\n" for i, tok in enumerate(_spellings(EV))}
    texts["m/rb"] = f'{{"seed": {RB}}}'
    texts["m/word"] = f"word{EV}"
    texts["m/clean"] = f"seed = 43; big = {EV + 1}"
    problems = ds.absence_problems(texts, held, exclude=[])
    named = {p.split(":")[0] for p in problems}
    assert named == {"m/k0", "m/k1", "m/k2", "m/k3", "m/rb", "m/word"}
    assert all(_no_seed_in(p) for p in problems)
    # A seed spent by a recorded look is no longer held out.
    assert ds.absence_problems({"m/k": str(EV)}, held, exclude=[EV]) == []


def _rendered_maps(cfg):
    from lakebench.modules.pipeline_engines.spark.job import SparkJobManager

    k8s = MagicMock()
    k8s.apply_manifest.return_value = True
    SparkJobManager(cfg, k8s).deploy_scripts_configmap()
    m = k8s.apply_manifest.call_args.args[0]
    return {f"{m['metadata']['name']}/{k}": v for k, v in m["data"].items()}


@pytest.fixture
def two_configs():
    from tests.conftest import make_config

    return [
        make_config(architecture={"workload": {"schema": schema}})
        for schema in ("customer360", "financial")
    ]


@pytest.mark.xfail(
    json.loads(PROD.read_text())["absence_check"] == "report",
    reason="the pre-registration keeps its plaintext seeds until the owner's OA5 commit",
    raises=AssertionError,
    strict=True,
)
def test_tip_maps_clean(two_configs):
    for cfg in two_configs:
        problems = ds.absence_problems(_rendered_maps(cfg))
        assert not problems, problems


def test_only_the_prereg_holds_a_heldout_value(two_configs):
    for cfg in two_configs:
        for p in ds.absence_problems(_rendered_maps(cfg)):
            assert p.startswith("lakebench-spark-scripts/aml_preregistration.json:"), p


def _manager(cfg):
    from lakebench.modules.pipeline_engines.spark.job import SparkJobManager

    k8s = MagicMock()
    k8s.apply_manifest.return_value = True
    return SparkJobManager(cfg, k8s), k8s


def test_scripts_map_refused_when_enforcing(two_configs, monkeypatch, caplog):
    held = ds.HeldOut(**{**ts.load().__dict__, "absence_check": "enforce"})
    monkeypatch.setattr(ds, "load_heldout", lambda *a, **k: held)
    real = ds.absence_problems
    monkeypatch.setattr(
        ds,
        "absence_problems",
        lambda texts, h=None, exclude=None: real({**texts, "m/planted": str(EV)}, h, []),
    )
    mgr, k8s = _manager(two_configs[1])
    with caplog.at_level(logging.ERROR):
        assert mgr.deploy_scripts_configmap() is False
    k8s.apply_manifest.assert_not_called()
    assert "m/planted" in caplog.text and _no_seed_in(caplog.text)


def test_scripts_map_applied_and_logged_in_report_mode(two_configs, monkeypatch, caplog):
    held = ts.load()
    monkeypatch.setattr(ds, "load_heldout", lambda *a, **k: held)
    real = ds.absence_problems
    monkeypatch.setattr(
        ds,
        "absence_problems",
        lambda texts, h=None, exclude=None: real({**texts, "m/planted": str(EV)}, h, []),
    )
    mgr, k8s = _manager(two_configs[0])
    with caplog.at_level(logging.INFO):
        assert mgr.deploy_scripts_configmap() is True
    k8s.apply_manifest.assert_called_once()
    assert "m/planted" in caplog.text and _no_seed_in(caplog.text)


def test_scripts_map_refused_without_the_hash_file(two_configs, monkeypatch):
    def gone(*a, **k):
        raise FileNotFoundError("heldout_hashes.json not found")

    monkeypatch.setattr(ds, "load_heldout", gone)
    mgr, k8s = _manager(two_configs[0])
    assert mgr.deploy_scripts_configmap() is False
    k8s.apply_manifest.assert_not_called()


def test_hash_file_ships_in_the_scripts_map(two_configs):
    maps = _rendered_maps(two_configs[1])
    shipped = json.loads(maps["lakebench-spark-scripts/heldout_hashes.json"])
    assert shipped == json.loads(PROD.read_text())


def test_seed_ref():
    h = ts.load()
    assert ds.seed_ref("customer360", 42, h) == "42"
    assert ds.seed_ref("financial", EV, h) == ds.seed_hash(h.salt, EV)
    assert _no_seed_in(ds.seed_ref("financial", EV, h))


def test_flat_copy_finds_the_hash_file_next_to_it(tmp_path):
    import subprocess

    (tmp_path / "datagen_seed.py").write_text(Path(ds.__file__).read_text())
    (tmp_path / ds.HELDOUT_FILENAME).write_text(ts.FIXTURE.read_text())
    code = (
        "import sys; sys.path.insert(0, '.');"
        "import datagen_seed as d;"
        f"assert d.heldout_role({EV}) == 'evaluation' and d.heldout_role(43) is None"
    )
    r = subprocess.run(
        [sys.executable, "-I", "-c", code], cwd=tmp_path, capture_output=True, text=True
    )
    assert r.returncode == 0, r.stderr


def test_deep_copy_of_heldout_is_harmless():
    # HeldOut is a frozen value object; copies compare equal.
    h = ts.load()
    assert copy.deepcopy(h) == h


def test_heldout_and_spent_seed_is_refused(held):
    # A held-out seed that is also spent (its look recorded, or voided) is
    # refused even as its own registered run, and a counts-only run on its
    # corpus is refused too.
    c = _corpora(spent_seeds=[*_corpora()["spent_seeds"], EV])
    err = ds.aml_seed_error(c, EV, "evaluation", ["evaluation"], claim_verified=True)
    assert err and "spent" in err and _no_seed_in(err)
    assert "spent" in ds.aml_seed_error(c, EV, "evaluation", [EV], claim_verified=True)
    v = ds.corpus_verdict(ts.manifest_rows(EV, 10), claimed=EV, spent=c["spent_seeds"])
    assert v.role == "spent"
    assert "spent" in ds.aml_seed_error(c, None, None, [v.role], counts_only=True)
    assert "spent" in ds.aml_seed_error(c, None, None, ["evaluation", "spent"], counts_only=True)


def test_absence_token_shapes(held):
    shapes = [
        f"SEED_{EV}",
        f"{EV}L",
        f"{EV}UL",
        f"{EV}_",
        f"-{EV}",
        f"{EV}.0",
        f"x={hex(EV)}u",
        f"seed_{EV}_s1",
        f"aml-{EV}_1",
        f"run{EV}",
        f"{EV}ms",
        f"{EV:_}_2",
    ]
    texts = {f"m/{i}": t for i, t in enumerate(shapes)}
    assert len(ds.absence_problems(texts, held, exclude=[])) == len(shapes)
    # Spent seeds are public: the default exclude covers the file's spent list.
    monkey_spent = ds.HeldOut(**{**held.__dict__, "spent": frozenset({EV})})
    assert ds.absence_problems({"m/k": str(EV)}, monkey_spent) == []


def test_generator_schedules_every_cycle_from_the_raw_seed():
    # Recovery assumes the manifest's instance seeds come from the raw --seed
    # in every cycle; a per-cycle seed mix would make a held-out corpus read
    # as another seed.
    src = (ROOT / "datagen_rs/src/bin/generate.rs").read_text()
    assert re.search(r"typology::schedule_p\(\s*seed,\s*seed,", src)


def _gate_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location("aml_gate_heldout", ROOT / "scripts/aml_gate.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_gate_report_records_an_unspent_heldout_seed_by_hash(held, monkeypatch):
    g = _gate_module()
    assert g.report_seed(EV) == ds.seed_hash(held.salt, EV)
    assert g.report_seed(43) == 43 and g.report_seed(None) is None
    # Once its look is recorded the seed is spent and recorded as given.
    monkeypatch.setattr(ds, "spent_seeds", lambda: frozenset({42, EV}))
    assert g.report_seed(EV) == EV


def test_look_ledger_git_failure_names_no_seed(monkeypatch, tmp_path):
    g = _gate_module()
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(tmp_path / "none.jsonl"))

    class Failed:
        returncode, stdout, stderr = 128, "", "fatal"

    monkeypatch.setattr(g.subprocess, "run", lambda *a, **k: Failed())
    with pytest.raises(OSError) as e:
        g.seed_ever_recorded(EV)
    assert _no_seed_in(str(e.value))


def test_cross_role_hash_refused(tmp_path):
    # The robustness seed's hash appended to the evaluation list would make
    # heldout_role answer "evaluation" for it.
    old, new = _doc(), _doc()
    new["roles"]["evaluation"].append(new["roles"]["robustness"][0])
    problems = ds.heldout_history_problems(old, new, _looks())
    assert any("repeats a hash" in p for p in problems), problems
    p = tmp_path / "dup.json"
    p.write_text(json.dumps(new))
    with pytest.raises(ValueError, match="repeats a hash"):
        ds.load_heldout(p)


def test_floor_hash_under_another_role_refused(tmp_path, monkeypatch):
    salt = json.loads(ts.FIXTURE.read_text())["salt"]
    other = 5551212  # test value, registered only in this monkeypatched floor
    floor = {
        "salt": salt,
        "roles": {"evaluation": (ds.seed_hash(salt, other),), "robustness": ("0" * 64,)},
    }
    monkeypatch.setattr(ds, "_HELDOUT_FLOOR", floor)
    old, new = _doc(), _doc()
    new["roles"]["robustness"].append(ds.seed_hash(salt, other))
    problems = ds.heldout_history_problems(old, new, _looks())
    assert any("floor's evaluation hash" in p for p in problems), problems
    p = tmp_path / "swap.json"
    p.write_text(json.dumps(new))
    with pytest.raises(ValueError, match="registers as evaluation"):
        ds.load_heldout(p)


def test_too_many_corpus_seeds_refused():
    rows = [r for s in range(1, 11) for r in ts.manifest_rows(s * 1000, 3, start=3 * s)]
    with pytest.raises(ds.CorpusSeedError, match="distinct corpus seeds"):
        ds.recover_corpus_seeds(rows)
    assert len(ds.recover_corpus_seeds(rows[: 3 * 8])) == 8


def test_scorer_verifies_rows_beyond_the_sample(scorer, monkeypatch):
    # 250 claimed rows, then one foreign row that sorts after row 200: the
    # 200-row sample would verify this corpus; every-row recovery does not.
    monkeypatch.setenv("LB_DATAGEN_SEED", str(EV))
    monkeypatch.setenv("LB_DATAGEN_CORPUS_ROLE", "evaluation")
    claimed = ts.manifest_rows(EV, 250, typologies=(("GATHER_SCATTER", 0),))
    foreign = ts.manifest_rows(43, 1, typologies=(("STACK", 3),))
    m = _Manifest(claimed + foreign)
    assert _Af().corpus_seed_check(m, EV)["matched_share"] == 1
    with pytest.raises(SystemExit, match="refusing to score"):
        scorer._refuse_guarded_corpus(_Af(), m, counts_only=False)


def test_resalted_file_cannot_move_a_floor_seed(tmp_path, monkeypatch):
    # The floor registers RB as robustness under its own salt; a file under
    # another salt lists RB under evaluation. The seed must not read as
    # evaluation: the conflict refuses.
    floor_salt = "34" * 32
    floor = {
        "salt": floor_salt,
        "roles": {
            "evaluation": (ds.seed_hash(floor_salt, EV),),
            "robustness": (ds.seed_hash(floor_salt, RB),),
        },
    }
    monkeypatch.setattr(ds, "_HELDOUT_FLOOR", floor)
    doc = _doc()
    doc["roles"] = {"evaluation": [ds.seed_hash(doc["salt"], RB)], "robustness": []}
    p = tmp_path / "swap.json"
    p.write_text(json.dumps(doc))
    h = ds.load_heldout(p)
    with pytest.raises(ValueError, match="different roles") as e:
        ds.heldout_role(RB, h)
    assert _no_seed_in(str(e.value))
    assert ds.heldout_role(EV, h) == "evaluation"


def test_non_string_hash_entry_is_a_value_error(tmp_path):
    doc = _doc()
    doc["roles"]["evaluation"].append(["x"])
    p = tmp_path / "bad.json"
    p.write_text(json.dumps(doc))
    with pytest.raises(ValueError):
        ds.load_heldout(p)


def test_ledger_message_names_an_unspent_heldout_seed_by_role(held, monkeypatch, tmp_path):
    g = _gate_module()
    led = tmp_path / "ledger.jsonl"
    led.write_text(json.dumps({"role": "evaluation", "seed": EV}) + "\n")
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(led))
    msg = g.seed_ever_recorded(EV)
    assert "evaluation" in msg and _no_seed_in(msg)


# ---------------------------------------------------------------------------
# Review round 2 (CD-3+4)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [EV - 2**64, EV + 2**64, -1, 2**63])
def test_spent_outside_i64_refused(bad):
    # seed - 2**64 hashes to no role, so without a range check it would pass
    # the spent rule while publishing the seed in a reversible form.
    old, new = _doc(), _doc()
    new["spent"].append(bad)
    problems = ds.heldout_history_problems(old, new, _looks())
    assert any("outside 0..2^63-1" in p for p in problems), problems
    assert all(_no_seed_in(p) for p in problems)


def test_spent_from_message_names_no_value():
    for raw in ([EV, "x"], [str(RB)], str(EV)):
        with pytest.raises(ValueError) as e:
            ds.spent_from({"spent_seeds": raw})
        assert _no_seed_in(str(e.value)), "spent_from echoed a value"


def test_absence_finds_embedded_and_grouped_seeds(held):
    shapes = [
        f"20261001{EV}1",  # inside a longer digit run
        f"0xff{EV}",  # decimal digits after a hex prefix
        f"{EV:,}",  # comma-grouped
        f"{EV:,}".replace(",", " "),  # space-grouped
        f"id={EV}{EV}",
    ]
    texts = {f"m/{i}": t for i, t in enumerate(shapes)}
    problems = ds.absence_problems(texts, held, exclude=[])
    assert {p.split(":")[0] for p in problems} == set(texts), problems
    assert all(_no_seed_in(p) for p in problems)
    # Near misses stay clean.
    clean = {"m/a": f"{EV + 1:,}", "m/b": f"1{EV + 1}2"}
    assert ds.absence_problems(clean, held, exclude=[]) == []


def _rust_seed_reads_as(role: str) -> bool | None:
    """True when the seed robustness.rs compiles in for ``role`` hashes to that
    role; None when the constant is gone (CD-5 moves the Rust check to the
    hash file). Returns a bool so a failure prints no value."""
    src = (ROOT / "datagen_rs/src/robustness.rs").read_text()
    m = re.search(rf"pub const {role.upper()}_SEED: i64 = ([0-9_]+);", src)
    if m is None:
        assert f"{role.upper()}_SEED" not in src, f"{role.upper()}_SEED in an unreadable form"
        return None
    return ds.heldout_role(int(m.group(1).replace("_", ""))) == role


@pytest.mark.parametrize("role", ds.PROTECTED_ROLES)
def test_rust_compiled_seed_hashes_to_its_role(role):
    # Until CD-5, robustness.rs compiles the two seeds in; they must be the
    # registered ones, checked by hash so no value is read into a message.
    ok = _rust_seed_reads_as(role)
    if ok is None:
        pytest.skip("robustness.rs no longer compiles the seed in (CD-5)")
    assert ok is True, f"robustness.rs {role.upper()}_SEED is not the registered {role} seed"
