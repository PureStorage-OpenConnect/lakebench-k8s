"""The AML scoring assembler (lakebench.aml.scoring_queries) builds the
financial_scoring record from integer counts returned by SQL, with every
ratio and null rule computed in Python. A FakeExecutor returns canned TSV
rows keyed by a fragment of each statement, so no cluster or Spark is needed.
"""

from __future__ import annotations

import pytest

from lakebench.aml import scoring_queries as sq

# A minimal rule vocabulary: one designated rule (W2 -> micro_structuring),
# customer-scoped, with two reason codes.
META = sq.RulesMeta(
    reason_codes={"W2_structuring": ("W2_SUB_THRESHOLD_BURST", "W2_HIGH_COUNT")},
    reason_code_vocabulary="deadbeefdeadbeef",
    customer_scoped_rules=frozenset({"W2_structuring"}),
    rule_target_typology={"W2_structuring": "micro_structuring"},
)


class FakeExecutor:
    """Returns canned rows for the statement whose text contains a key."""

    def __init__(self, by_fragment: dict[str, list[list[str | None]]]):
        self.by_fragment = by_fragment
        self.calls: list[str] = []

    def run_tsv(self, sql: str, timeout: int) -> list[list[str | None]]:
        self.calls.append(sql)
        for frag, rows in self.by_fragment.items():
            if frag in sql:
                return rows
        return []


def _covered_ids():
    return {n: i for i, (n, _) in enumerate(sq._COVERED, start=100)}


def _cov_instances(spec):
    """Per-instance coverage rows for the coverage_recall statement, from a
    {typology_type: (corpus, covered, no_participant, designated_hits,
    incidental_hits)} spec. Returns (rows, covered_typology_ids): one row per
    instance ``<typ>_t_<i>`` with the integer flags, covered instances first
    so the hit flags land on covered rows (designated/incidental hits only
    occur on covered instances, as the SQL joins them through covered_uetrs).
    """
    rows: list[list[str]] = []
    covered_ids: list[str] = []
    for typ, (corpus, covered, no_part, des, inc) in spec.items():
        for i in range(corpus):
            tid = f"{typ}_t_{i}"
            is_cov = i < covered  # first `covered` instances are covered
            # no_participant instances sit in the uncovered tail
            nop = (not is_cov) and (covered <= i < covered + no_part)
            dh = is_cov and i < des  # designated hits land on covered rows
            ih = is_cov and i < inc
            rows.append([tid, typ, str(int(is_cov)), str(int(nop)), str(int(dh)), str(int(ih))])
            if is_cov:
                covered_ids.append(tid)
    return rows, covered_ids


def _base_rows(*, manifest, status, uncoded="0"):
    """Rows common to a scored run: one designated rule ran, W2."""
    return {
        # manifest_guard: (typology_id, seed)
        "for the assembler's two guards": manifest,
        "FROM {catalog}.gold.detection_status".replace("{catalog}", "lh"): status,
        # by_code_meta uncoded/total
        "CARDINALITY(a.reason_codes)": [[uncoded, "10"]],
    }


def test_covered_scored_run_mixes_states():
    # Three typologies: micro_structuring (scored, W2 ran), cycle (no_rule),
    # random (control). coverage_recall returns integer counts.
    ids = _covered_ids()
    _cov_rows, _covered = _cov_instances(
        {
            "micro_structuring": (10, 8, 0, 6, 7),
            "cycle": (4, 2, 1, 0, 1),
            "random": (6, 6, 0, 0, 1),
        }
    )
    exe = FakeExecutor(
        {
            "for the assembler's two guards": [
                ["micro_structuring_1_0", "43"],
                ["cycle_2_0", "43"],
                ["random_9_0", "43"],
            ],
            "FROM lh.gold.detection_status": [
                ["W2_structuring", "run-x", "ran", "micro_structuring", "5", ""],
            ],
            # coverage_recall: one row per instance (typ_id, typ, is_covered,
            # no_participant, designated_hit, incidental_hit).
            "Coverage and covered-recall": _cov_rows,
            "Per-rule false positives": [["W2_structuring", "20", "5", "30", "40"]],
            "Global alert counts": [["25", "18"]],
            "Per-rule chance": [["6", "W2_structuring", "1"]],
            "an evidence cap cut": [],
            "Distinct instance count per typology_type": [
                ["micro_structuring", "10"],
                ["cycle", "4"],
                ["random", "6"],
            ],
            "MAX(expected_workload)": [
                ["micro_structuring", "W2_structuring"],
                ["cycle", "W3_round_tripping"],
                ["random", "random"],
            ],
            "Per-reason-code counts": [
                ["W2_structuring", "W2_SUB_THRESHOLD_BURST", "20", "18", "6", "16"],
            ],
            "CARDINALITY(a.reason_codes)": [["0", "25"]],
            # subject check chunk + manifest subject rows (typology_id first;
            # micro_structuring_t_0 is covered, so the covered pass keeps it).
            "Resolve a chunk of subject": [["1001", "1", "1"]],
            "participant_entity_ids": [
                ["micro_structuring_t_0", "micro_structuring", "1001", "43"]
            ],
            "detectable_by": [],
        }
    )
    rec = sq.score(
        exe.run_tsv,
        catalog="lh",
        snapshot_ids=ids,
        run_id="run-x",
        meta=META,
        alert_set={"rows": 25, "digest": "abc"},
    )
    assert rec["mode"] == "covered"
    assert rec["status"] == "scored"
    assert rec["computed_by"] == "lb-score-query"
    cov = rec["covered"]
    typ = {t["typology_type"]: t for t in cov["typologies"]}
    # recall_covered = designated_hits / covered_instances, scored.
    assert typ["micro_structuring"]["recall_covered"] == pytest.approx(6 / 8)
    # cycle has no designated rule: recall None.
    assert typ["cycle"]["recall_covered"] is None
    assert typ["cycle"]["no_participant_txns"] == 1
    # coverage = covered / corpus.
    assert typ["random"]["coverage"] == pytest.approx(1.0)
    # workload_category comes from the manifest, not null.
    assert typ["micro_structuring"]["workload_category"] == "W2_structuring"
    # fp over the full manifest: fp_alerts = 25 - 18 = 7, rate 7/25.
    assert cov["fp_alerts"] == 7
    assert cov["fp_rate"] == pytest.approx(7 / 25)
    # random control floor = covered incidental of random = 1/6.
    assert cov["random_control_floor"] == pytest.approx(1 / 6)
    # chance_by_rule = rule random hits / n_random.
    assert cov["chance_by_rule"]["W2_structuring"] == pytest.approx(1 / 6)


def test_batch_run():
    exe = FakeExecutor(
        {
            "for the assembler's two guards": [["micro_structuring_1_0", "43"]],
            "FROM lh.gold.detection_status": [
                ["W2_structuring", "run-x", "ran", "micro_structuring", "5", ""],
            ],
            "Coverage and covered-recall": _cov_instances({"micro_structuring": (10, 10, 0, 6, 7)})[
                0
            ],
            "Per-rule false positives": [["W2_structuring", "20", "5", "30", "40"]],
            "Global alert counts": [["25", "18"]],
            "Per-rule chance": [],
            "an evidence cap cut": [],
            "Distinct instance count per typology_type": [["micro_structuring", "10"]],
            "MAX(expected_workload)": [["micro_structuring", "W2_structuring"]],
            "Per-reason-code counts": [
                ["W2_structuring", "W2_SUB_THRESHOLD_BURST", "20", "18", "6", "16"],
            ],
            "CARDINALITY(a.reason_codes)": [["0", "25"]],
            "Resolve a chunk of subject": [["1001", "1", "1"]],
            "participant_entity_ids": [
                ["micro_structuring_t_0", "micro_structuring", "1001", "43"]
            ],
            "detectable_by": [],
            "WHERE is_customer": [["4321"]],
        }
    )
    rec = sq.score(
        exe.run_tsv,
        catalog="lh",
        snapshot_ids=None,
        run_id="run-x",
        meta=META,
    )
    assert "mode" not in rec  # batch has no mode key
    assert rec["run_id"] == "run-x"
    assert rec["computed_by"] == "lb-score-query"
    t = {x["typology_type"]: x for x in rec["typologies"]}["micro_structuring"]
    # batch recall = designated_hits / corpus_instances.
    assert t["recall"] == pytest.approx(6 / 10)
    assert t["instance_count"] == 10
    assert t["workload_category"] == "W2_structuring"  # from the manifest, not null
    # The per-customer screening rate's denominator (scripts/aml_screen_rates.py).
    assert rec["customer_count"] == 4321
    assert rec["fp_rate"] == pytest.approx(7 / 25)


def test_batch_ignores_coverage():
    # Batch scoring has no coverage gate (compute_scores scores the full
    # manifest), so the assembler must run coverage_recall without the covered
    # join. The FakeExecutor returns the hit for an instance whose is_covered
    # flag is 0; batch recall must still count it. In the SQL this is the
    # {hit_gate} placeholder rendering empty for batch; here we assert the
    # assembler divides designated_hits by the full corpus regardless of the
    # is_covered flag on the rows.
    rows = [
        # 3 instances, 1 covered, but 2 designated hits (one on an uncovered
        # instance): a batch score counts both; a covered score would not.
        ["micro_structuring_t_0", "micro_structuring", "1", "0", "1", "1"],
        ["micro_structuring_t_1", "micro_structuring", "0", "0", "1", "1"],
        ["micro_structuring_t_2", "micro_structuring", "0", "0", "0", "0"],
    ]
    exe = FakeExecutor(
        {
            "for the assembler's two guards": [["micro_structuring_1_0", "43"]],
            "FROM lh.gold.detection_status": [
                ["W2_structuring", "run-x", "ran", "micro_structuring", "5", ""],
            ],
            "Coverage and covered-recall": rows,
            "Per-rule false positives": [["W2_structuring", "20", "5", "30", "40"]],
            "Global alert counts": [["25", "18"]],
            "Per-rule chance": [],
            "an evidence cap cut": [],
            "Distinct instance count per typology_type": [["micro_structuring", "3"]],
            "MAX(expected_workload)": [["micro_structuring", "W2_structuring"]],
            "Per-reason-code counts": [],
            "CARDINALITY(a.reason_codes)": [["0", "25"]],
            "Resolve a chunk of subject": [],
            "participant_entity_ids": [],
            "detectable_by": [],
        }
    )
    rec = sq.score(
        exe.run_tsv,
        catalog="lh",
        snapshot_ids=None,
        run_id="run-x",
        meta=META,
    )
    t = {x["typology_type"]: x for x in rec["typologies"]}["micro_structuring"]
    # 2 designated hits over 3 corpus instances, the uncovered hit included.
    assert t["recall"] == pytest.approx(2 / 3)
    assert t["instance_count"] == 3


def test_not_scored_on_protected_seed(monkeypatch):
    # manifest_protected_reason returns a reason -> NotScored.
    monkeypatch.setattr(
        sq.ds, "manifest_protected_reason", lambda rows: "a held-out evaluation seed"
    )
    exe = FakeExecutor({"for the assembler's two guards": [["micro_structuring_1_0", "50000042"]]})
    with pytest.raises(sq.NotScored) as e:
        sq.score(
            exe.run_tsv,
            catalog="lh",
            snapshot_ids=None,
            run_id="run-x",
            meta=META,
        )
    assert "refusing to score" in str(e.value)


def test_not_scored_on_duplicate_typology_id(monkeypatch):
    monkeypatch.setattr(sq.ds, "manifest_protected_reason", lambda rows: None)
    exe = FakeExecutor(
        {
            "for the assembler's two guards": [
                ["micro_structuring_1_0", "43"],
                ["micro_structuring_1_0", "43"],
            ]
        }
    )
    with pytest.raises(sq.NotScored) as e:
        sq.score(
            exe.run_tsv,
            catalog="lh",
            snapshot_ids=None,
            run_id="run-x",
            meta=META,
        )
    assert "repeat typology_id" in str(e.value)


def test_subject_chunking_boundary(monkeypatch):
    # More distinct subject ids than one chunk: every chunk is sent and the
    # results are merged. Use a small chunk via monkeypatch.
    monkeypatch.setattr(sq, "SUBJECT_CHUNK", 2)
    seen_chunks = []

    def run_tsv(sql, timeout):
        if "Resolve a chunk of subject" in sql:
            import re

            seen_chunks.append(sql)
            ids = re.findall(r"\((\d+)\)", sql)
            return [[i, "1", "1"] for i in ids]
        return []

    rows = [("micro_structuring", [1000 + k], 43) for k in range(5)]
    check = sq._subject_check(
        run_tsv,
        "lh",
        sq._pins(None),
        rows,
        {"micro_structuring"},
        900,
    )
    # 5 distinct ids, chunk size 2 -> 3 chunks.
    assert len(seen_chunks) == 3
    assert check["subjects"] == 5
    assert check["status"] == "ok"
    assert check["not_customer"] == 0


def test_zero_alerts_fp_rate_none():
    exe = FakeExecutor(
        {
            "for the assembler's two guards": [["micro_structuring_1_0", "43"]],
            "FROM lh.gold.detection_status": [
                ["W2_structuring", "run-x", "ran", "micro_structuring", "0", ""],
            ],
            "Coverage and covered-recall": _cov_instances({"micro_structuring": (10, 0, 0, 0, 0)})[
                0
            ],
            "Per-rule false positives": [],
            "Global alert counts": [["0", "0"]],
            "Per-rule chance": [],
            "an evidence cap cut": [],
            "Distinct instance count per typology_type": [["micro_structuring", "10"]],
            "MAX(expected_workload)": [["micro_structuring", "W2_structuring"]],
            "Per-reason-code counts": [],
            "CARDINALITY(a.reason_codes)": [["0", "0"]],
            "Resolve a chunk of subject": [],
            "participant_entity_ids": [],
            "detectable_by": [],
        }
    )
    rec = sq.score(
        exe.run_tsv,
        catalog="lh",
        snapshot_ids=None,
        run_id="run-x",
        meta=META,
    )
    assert rec["fp_rate"] is None
    assert rec["total_alerts"] == 0
    assert rec["fp_alerts"] == 0


def test_empty_values_relation_names_its_columns():
    # precision and coverage_recall reference t.rule_id / p.typology_type even
    # when no designated rule ran, so the empty relation must carry the names.
    sql = sq._values([], ["rule_id", "target"])
    assert "AS rule_id" in sql and "AS target" in sql and "WHERE 1 = 0" in sql


def test_engine_failure_names_the_statement():
    def run_tsv(sql, timeout):
        raise RuntimeError("Query exceeded distributed total memory limit")

    with pytest.raises(RuntimeError, match=r"^manifest_guard\.sql\.tmpl: Query exceeded"):
        sq.score(run_tsv, catalog="lh", snapshot_ids=None, run_id="run-x", meta=META)


def _covered_rescreen_exe(rescreen_rows, rescreen_error=None):
    """A covered W5 run: sanctions_match has 10 instances, 8 covered, 6
    designated and 7 incidental hits (on covered instances t_0..t_5 and
    t_0..t_6). ``rescreen_rows`` are the manifest's detectable_by=rescreen
    instances."""
    rows, _ = _cov_instances({"sanctions_match": (10, 8, 0, 6, 7)})
    canned = {
        "for the assembler's two guards": [["sanctions_match_1_0", "43"]],
        "FROM lh.gold.detection_status": [
            ["W5_sanctions_match", "run-x", "ran", "sanctions_match", "5", ""],
        ],
        "Coverage and covered-recall": rows,
        "Per-rule false positives": [["W5_sanctions_match", "20", "5", "30", "40"]],
        "Global alert counts": [["25", "18"]],
        "Per-rule chance": [],
        "an evidence cap cut": [],
        "Distinct instance count per typology_type": [["sanctions_match", "10"]],
        "MAX(expected_workload)": [["sanctions_match", "W5_sanctions"]],
        "Per-reason-code counts": [],
        "CARDINALITY(a.reason_codes)": [["0", "25"]],
        "Resolve a chunk of subject": [["1002", "1", "1"]],
        "participant_entity_ids": [
            ["sanctions_match_t_0", "sanctions_match", "1001", "43"],
            ["sanctions_match_t_2", "sanctions_match", "1002", "43"],
        ],
        "detectable_by": rescreen_rows,
    }

    class Exe(FakeExecutor):
        def run_tsv(self, sql, timeout):
            if rescreen_error and "detectable_by" in sql:
                raise RuntimeError(rescreen_error)
            return super().run_tsv(sql, timeout)

    return Exe(canned)


W5_META = sq.RulesMeta(
    reason_codes={},
    reason_code_vocabulary="deadbeefdeadbeef",
    customer_scoped_rules=frozenset({"W5_sanctions_match"}),
    rule_target_typology={"W5_sanctions_match": "sanctions_match"},
)


def test_covered_rescreen_only_instances_leave_recall_and_the_subject_check():
    # t_0 and t_1 are covered rescreen-only instances, both hit; t_9 is a
    # rescreen-only instance that is not covered. Covered recall and
    # incidental recall drop the two covered ones from numerator and
    # denominator; covered_instances keeps them; the subject check reads
    # t_2 and never t_0.
    exe = _covered_rescreen_exe(
        [
            ["sanctions_match_t_0", "sanctions_match"],
            ["sanctions_match_t_1", "sanctions_match"],
            ["sanctions_match_t_9", "sanctions_match"],
        ]
    )
    rec = sq.score(
        exe.run_tsv,
        catalog="lh",
        snapshot_ids=_covered_ids(),
        run_id="run-x",
        meta=W5_META,
        alert_set={"rows": 25, "digest": "abc"},
    )
    t = rec["covered"]["typologies"][0]
    assert t["covered_instances"] == 8
    assert t["rescreen_only_excluded"] == 2
    assert t["recall_covered"] == pytest.approx((6 - 2) / (8 - 2))
    assert t["incidental_recall_covered"] == pytest.approx((7 - 2) / (8 - 2))
    subject_sql = [s for s in exe.calls if "Resolve a chunk of subject" in s]
    assert subject_sql and all("1002" in s and "1001" not in s for s in subject_sql)


def test_covered_recall_is_null_when_every_covered_instance_is_rescreen_only():
    rows = [[f"sanctions_match_t_{i}", "sanctions_match"] for i in range(8)]
    rec = sq.score(
        _covered_rescreen_exe(rows).run_tsv,
        catalog="lh",
        snapshot_ids=_covered_ids(),
        run_id="run-x",
        meta=W5_META,
        alert_set={"rows": 25, "digest": "abc"},
    )
    t = rec["covered"]["typologies"][0]
    assert t["covered_instances"] == 8
    assert t["rescreen_only_excluded"] == 8
    assert t["recall_covered"] is None
    assert t["incidental_recall_covered"] is None


@pytest.mark.parametrize(
    ("error", "scored"),
    [
        ("Column 'injection_parameters' cannot be resolved", True),
        ("Query exceeded distributed total memory limit", False),
    ],
)
def test_rescreen_lookup_failure_is_never_read_as_zero(error, scored):
    exe = _covered_rescreen_exe([], rescreen_error=error)
    kwargs = {
        "catalog": "lh",
        "snapshot_ids": _covered_ids(),
        "run_id": "run-x",
        "meta": W5_META,
        "alert_set": {"rows": 25, "digest": "abc"},
    }
    if scored:
        t = sq.score(exe.run_tsv, **kwargs)["covered"]["typologies"][0]
        assert t["rescreen_only_excluded"] == 0
        assert t["recall_covered"] == pytest.approx(6 / 8)
    else:
        with pytest.raises(RuntimeError, match="memory limit"):
            sq.score(exe.run_tsv, **kwargs)
