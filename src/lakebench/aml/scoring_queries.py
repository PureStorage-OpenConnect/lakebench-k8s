"""AML recall and precision scoring as queries, assembled on the CLI side.

This is the covered and batch scorer that replaces the ``score_financial``
Spark job: the counts come from SQL run through the benchmark query engine,
and every ratio, null rule and the per-code vocabulary are assembled here,
from integer counts, so the figures do not depend on a Spark double's last
bit. The assembled ``financial_scoring`` record matches the Spark job field
for field (``docs/benchmarks/aml/scoring.md`` 8.4).

Pure: the SQL runs through an injected callable ``run_tsv(sql, timeout) ->
list[list[str | None]]`` (one list per data row, an empty cell is ``None``),
so the module imports no pyspark and no kubectl. The CLI unit supplies an
adapter over the query executor that returns TSV rows; the tests supply a
fake. ``alert_set`` is the Spark xxhash64 fingerprint and is never recomputed
here: covered mode takes it as an argument (the gold tick records it).

The subject-customer check resolves each instance's subject role in Python
(``datagen_seed.subject_index``) and pushes the distinct subject ids back in
chunked ``IN`` lists, because the ``corridor_high_risk`` role is a wrapping
64-bit multiply Trino cannot evaluate and the full id set would overflow the
kubectl-exec argv limit.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lakebench.config import datagen_seed as ds

#: Distinct subject datagen ids per ``subject_check`` statement. Each id is a
#: signed 64-bit integer, at most 20 characters plus a comma; 2000 ids is well
#: under 64 KiB of VALUES text, which keeps the whole kubectl-exec argv (SQL
#: plus flags) under 100 KiB even with the CTE boilerplate.
SUBJECT_CHUNK = 2000

RunTsv = Callable[[str, int], list[list[str | None]]]

_TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "benchmark/queries/aml/trino/scoring"

#: The scorer's computed_by stamp (was lb-score-financial for the Spark job).
COMPUTED_BY = "lb-score-query"

#: Tick snapshot suffixes, by table, as covered mode names them.
_COVERED = (
    ("txns", "silver.transactions"),
    ("entities", "silver.entities"),
    ("accounts", "silver.accounts"),
    ("versions", "silver.silver_batch_versions"),
    ("alerts", "gold.alerts"),
    ("status", "gold.detection_status"),
)

#: Which pin placeholder each table reference uses.
_PINS = ("txns", "entities", "accounts", "versions", "alerts", "status", "manifest")


@dataclass(frozen=True)
class RulesMeta:
    """The rule/reason-code vocabulary the assembler needs, injected so this
    module stays importable without the Spark scripts. The CLI unit builds it
    from the pure rule-metadata module (the home the reason-code vocabulary,
    HIGH_PRIORITY_CUTOFFS, CUSTOMER_SCOPED_RULES and RULE_TARGET_TYPOLOGY move
    to beside subject_index); the tests build it with fixed values.

    - ``reason_codes``: rule_id -> ordered tuple of the codes it can write,
      base first (aml_reason_codes.REASON_CODES).
    - ``reason_code_vocabulary``: the vocabulary digest string
      (aml_reason_codes.vocabulary_digest(HIGH_PRIORITY_CUTOFFS)).
    - ``customer_scoped_rules``: the rules whose subject must be a customer.
    - ``rule_target_typology``: rule_id -> its target typology (or None).
    """

    reason_codes: dict[str, tuple[str, ...]]
    reason_code_vocabulary: str
    customer_scoped_rules: frozenset[str]
    rule_target_typology: dict[str, str | None]


class NotScored(Exception):
    """Scoring cannot run; the message is the reason, as the Spark job's was."""


def _template(name: str) -> str:
    return (_TEMPLATE_DIR / name).read_text()


def _pins(snapshot_ids: dict[str, int] | None) -> dict[str, str]:
    """{<name>_pin: 'FOR VERSION AS OF <id>' | ''} for each table reference.

    Covered mode pins every table at the tick snapshot; the manifest is read
    at its current snapshot in both modes (it is a registered copy, not a tick
    table), so manifest_pin is always ''.
    """
    out = {f"{name}_pin": "" for name in _PINS}
    if snapshot_ids is not None:
        for name, _ in _COVERED:
            out[f"{name}_pin"] = f"FOR VERSION AS OF {int(snapshot_ids[name])}"
    return out


def _f(cell: str | None) -> float | None:
    return None if cell is None or cell == "" else float(cell)


def _i(cell: str | None) -> int:
    return 0 if cell is None or cell == "" else int(cell)


def _s(cell: str | None) -> str:
    """A cell the SQL guarantees non-null (an id, a typology_type, a rule_id).
    A NULL where one is not expected is a contract break, not a 0."""
    if cell is None:
        raise NotScored("a scoring query returned NULL in a key column")
    return cell


def _one(rows: list[list[str | None]]) -> list[str | None]:
    if not rows:
        raise NotScored("a scoring query returned no rows")
    return rows[0]


def _quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _values(pairs: Sequence[tuple[str, ...]], columns: Sequence[str]) -> str:
    """A ``SELECT ... FROM (VALUES ...) AS t(cols)`` body, or a typed empty
    relation when there are no pairs (so a downstream JOIN yields nothing)."""
    cols = ", ".join(columns)
    if not pairs:
        nulls = ", ".join("CAST(NULL AS VARCHAR)" for _ in columns)
        return f"SELECT {nulls} WHERE 1 = 0"  # typed empty
    tuples = ", ".join("(" + ", ".join(_quote(v) for v in p) + ")" for p in pairs)
    return f"SELECT * FROM (VALUES {tuples}) AS t({cols})"


# --- status and guards -------------------------------------------------------


def _status_rows(run_tsv: RunTsv, catalog: str, pins: dict[str, str], timeout: int) -> list[dict]:
    sql = _template("status.sql.tmpl").format(catalog=catalog, status_pin=pins["status_pin"])
    rows = run_tsv(sql, timeout)
    out = []
    for r in rows:
        out.append(
            {
                "rule_id": r[0],
                "run_id": r[1],
                "status": r[2],
                "target_typology": r[3],
                "alert_count": _i(r[4]) if r[4] not in (None, "") else None,
                "reason": r[5] if r[5] not in (None, "") else None,
            }
        )
    return out


def _guard_manifest(run_tsv: RunTsv, catalog: str, pins: dict[str, str], timeout: int) -> None:
    """The protected-corpus refusal and the duplicate-typology_id check, both
    before any scoring (score_financial.refuse_protected_corpus and
    aml_features.check_manifest). Raises NotScored with the reason."""
    sql = _template("manifest_guard.sql.tmpl").format(
        catalog=catalog, manifest_pin=pins["manifest_pin"]
    )
    rows = run_tsv(sql, timeout)
    if not rows:
        raise NotScored("bronze.manifest is empty: cannot score without ground truth")
    pairs = [(_s(r[0]), 0 if r[1] in (None, "") else int(str(r[1]))) for r in rows]
    reason = ds.manifest_protected_reason(iter(pairs))
    if reason is not None:
        raise NotScored(f"refusing to score this corpus: {reason}")
    ids = [p[0] for p in pairs]
    if len(set(ids)) != len(ids):
        raise NotScored(
            f"manifest files repeat typology_id ({len(ids)} rows, {len(set(ids))} distinct): "
            "stale cycle manifests in the prefix?"
        )


# --- the count statements ----------------------------------------------------


def _designated(status_rows: list[dict]) -> dict[str, list[str]]:
    designated: dict[str, list[str]] = {}
    for r in status_rows:
        if r.get("target_typology"):
            designated.setdefault(r["target_typology"], []).append(r["rule_id"])
    return designated


def _typology_state(typ: str, designated: dict[str, list[str]], rule_status: dict[str, str]) -> str:
    rules = designated.get(typ, [])
    if not rules:
        return "no_rule"
    states = {rule_status.get(rid) for rid in rules}
    if "ran" in states and states != {"ran"}:
        return "partial"
    if "ran" in states:
        return "scored"
    if "error" in states:
        return "rule_error"
    return "rule_skipped"


def _coverage_recall(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    run_id: str,
    account_parquet: str,
    pairs_sql: str,
    covered_pass: bool,
    timeout: int,
) -> tuple[dict[str, dict[str, int]], set[str]]:
    # Covered mode counts hits over covered instances only; batch mode counts
    # over the whole manifest (compute_scores has no coverage gate).
    hit_scope = (
        "JOIN covered c ON c.typology_id = mu.typology_id AND c.is_covered" if covered_pass else ""
    )
    sql = _template("coverage_recall.sql.tmpl").format(
        catalog=catalog,
        run_id=run_id,
        account_parquet=account_parquet,
        pairs_values=pairs_sql,
        hit_scope=hit_scope,
        **pins,
    )
    rows = run_tsv(sql, timeout)
    # One row per instance: (typology_id, typology_type, is_covered,
    # no_participant, designated_hit, incidental_hit). Aggregate to per-type
    # counts here, and keep the covered typology_ids for the subject check and
    # the rescreen-only exclusion, which score_covered applies at instance
    # grain (not per typology_type).
    out: dict[str, dict[str, int]] = {}
    covered_ids: set[str] = set()
    for r in rows:
        tid, typ = _s(r[0]), _s(r[1])
        is_cov, no_part, dh, ih = _i(r[2]), _i(r[3]), _i(r[4]), _i(r[5])
        c = out.setdefault(
            typ,
            {
                "corpus_instances": 0,
                "covered_instances": 0,
                "no_participant_txns": 0,
                "designated_hits": 0,
                "incidental_hits": 0,
            },
        )
        c["corpus_instances"] += 1
        c["covered_instances"] += is_cov
        c["no_participant_txns"] += no_part
        c["designated_hits"] += dh
        c["incidental_hits"] += ih
        if is_cov:
            covered_ids.add(tid)
    return out, covered_ids


def _precision(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    run_id: str,
    targets_sql: str,
    timeout: int,
) -> dict[str, dict[str, int]]:
    sql = _template("precision.sql.tmpl").format(
        catalog=catalog,
        run_id=run_id,
        targets_values=targets_sql,
        alerts_pin=pins["alerts_pin"],
        manifest_pin=pins["manifest_pin"],
    )
    rows = run_tsv(sql, timeout)
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        out[_s(r[0])] = {
            "rule_alerts": _i(r[1]),
            "off_alerts": _i(r[2]),
            "on_target_txn_pairs": _i(r[3]),
            "txn_pairs": _i(r[4]),
        }
    return out


def _global_fp(
    run_tsv: RunTsv, catalog: str, pins: dict[str, str], run_id: str, timeout: int
) -> tuple[int, int]:
    sql = _template("global_fp.sql.tmpl").format(
        catalog=catalog,
        run_id=run_id,
        alerts_pin=pins["alerts_pin"],
        manifest_pin=pins["manifest_pin"],
    )
    r = _one(run_tsv(sql, timeout))
    return _i(r[0]), _i(r[1])


def _chance(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    run_id: str,
    account_parquet: str,
    covered_pass: bool,
    timeout: int,
) -> tuple[int, dict[str, int]]:
    # Covered mode counts the chance floor over covered random instances;
    # batch over every random instance (compute_scores n_random).
    random_scope = (
        "COALESCE(bu.n_uetr, 0) > 0 AND COALESCE(bu.missing_uetr, 0) = 0 "
        "AND COALESCE(be.missing_entity, 0) = 0"
        if covered_pass
        else "TRUE"
    )
    sql = _template("chance.sql.tmpl").format(
        catalog=catalog,
        run_id=run_id,
        account_parquet=account_parquet,
        random_scope=random_scope,
        **pins,
    )
    rows = run_tsv(sql, timeout)
    n_random = 0
    per_rule: dict[str, int] = {}
    for r in rows:
        n_random = _i(r[0])
        if r[1] not in (None, ""):
            per_rule[_s(r[1])] = _i(r[2])
    return n_random, per_rule


def _evidence_cap(
    run_tsv: RunTsv, catalog: str, pins: dict[str, str], run_id: str, timeout: int
) -> dict[str, int]:
    sql = _template("evidence_cap.sql.tmpl").format(
        catalog=catalog, run_id=run_id, alerts_pin=pins["alerts_pin"]
    )
    return {_s(r[0]): _i(r[1]) for r in run_tsv(sql, timeout)}


def _n_inst(run_tsv: RunTsv, catalog: str, pins: dict[str, str], timeout: int) -> dict[str, int]:
    sql = _template("n_inst.sql.tmpl").format(catalog=catalog, manifest_pin=pins["manifest_pin"])
    return {_s(r[0]): _i(r[1]) for r in run_tsv(sql, timeout)}


def _workload_categories(
    run_tsv: RunTsv, catalog: str, pins: dict[str, str], timeout: int
) -> dict[str, str | None]:
    """typology_type -> expected_workload (the generator's AML category), for
    the workload_category field compute_scores carries (score_financial.py
    agg groups by typology_type and expected_workload together, so one
    workload per type)."""
    sql = (
        f"SELECT typology_type, MAX(expected_workload) "
        f"FROM {catalog}.bronze.manifest {pins['manifest_pin']} GROUP BY typology_type"
    )
    return {_s(r[0]): (None if r[1] in (None, "") else _s(r[1])) for r in run_tsv(sql, timeout)}


def _by_code(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    run_id: str,
    targets_sql: str,
    ran_targets: dict[str, str],
    n_inst: dict[str, int],
    meta: RulesMeta,
    timeout: int,
) -> dict[str, Any]:
    """_scores_by_code: per-rule, per-code recall/fp/alert counts."""
    REASON_CODES = meta.reason_codes

    out: dict[str, Any] = {
        "recall_by_code": {},
        "fp_by_code": {},
        "alerts_by_code": {},
        "reason_code_vocabulary": meta.reason_code_vocabulary,
    }
    meta_row = _one(
        run_tsv(
            _template("by_code_meta.sql.tmpl").format(
                catalog=catalog, run_id=run_id, alerts_pin=pins["alerts_pin"]
            ),
            timeout,
        )
    )
    uncoded = _i(meta_row[0])
    if uncoded:
        out["by_code_status"] = f"not_scored: {uncoded} alerts carry no reason code"
        return out
    rules = sorted(ran_targets)
    if not rules:
        out["by_code_status"] = "not_scored: no designated rule ran"
        return out
    sql = _template("by_code.sql.tmpl").format(
        catalog=catalog,
        run_id=run_id,
        targets_values=targets_sql,
        alerts_pin=pins["alerts_pin"],
        manifest_pin=pins["manifest_pin"],
    )
    n_by: dict[tuple[str, str], int] = {}
    n_with_txn: dict[tuple[str, str], int] = {}
    hit_inst: dict[tuple[str, str], int] = {}
    hit_alerts: dict[tuple[str, str], int] = {}
    for r in run_tsv(sql, timeout):
        key = (_s(r[0]), _s(r[1]))
        n_by[key] = _i(r[2])
        n_with_txn[key] = _i(r[3])
        hit_inst[key] = _i(r[4])
        hit_alerts[key] = _i(r[5])
    recall: dict[str, dict[str, float | None]] = {}
    fp: dict[str, dict[str, float | None]] = {}
    counts: dict[str, dict[str, int]] = {}
    for rule in rules:
        seen = {c for (rr, c) in n_by if rr == rule and c is not None}
        base = list(REASON_CODES.get(rule, ()))
        vocab = base + sorted(seen - set(base))
        total = n_inst.get(ran_targets[rule], 0)
        recall[rule] = {c: (hit_inst.get((rule, c), 0) / total if total else None) for c in vocab}
        fp[rule] = {
            c: (
                1.0 - hit_alerts.get((rule, c), 0) / n_with_txn[(rule, c)]
                if n_with_txn.get((rule, c))
                else None
            )
            for c in vocab
        }
        counts[rule] = {c: n_by.get((rule, c), 0) for c in vocab}
    out.update(recall_by_code=recall, fp_by_code=fp, alerts_by_code=counts, by_code_status="scored")
    return out


# --- subject-customer check --------------------------------------------------


def _subject_check(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    account_parquet: str,
    manifest_rows: list[tuple[str, list[int] | None, int | None]],
    scoped_typologies: set[str],
    timeout: int,
) -> dict[str, Any]:
    """subject_customer_check: resolve each instance's subject in Python, then
    ask silver (chunked) whether it maps and is a customer."""
    subjects: list[tuple[str, int]] = []  # (typology_type, dg_id)
    unresolved: set[str] = set()
    for typ, ents, seed in manifest_rows:
        ids = ents or []
        if not ids or seed is None:
            unresolved.add(typ)
            continue
        idx = ds.subject_index(typ, len(ids), int(seed))
        subjects.append((typ, int(ids[idx])))
    if not subjects:
        return {"status": "unchecked", "reason": "no manifest row names its participants"}
    distinct_ids = sorted({dg for _, dg in subjects})
    resolved: dict[int, tuple[int, int]] = {}  # dg_id -> (mapped, is_customer)
    for i in range(0, len(distinct_ids), SUBJECT_CHUNK):
        chunk = distinct_ids[i : i + SUBJECT_CHUNK]
        dg_list = ", ".join(f"({d})" for d in chunk)
        sql = _template("subject_check.sql.tmpl").format(
            catalog=catalog,
            account_parquet=account_parquet,
            accounts_pin=pins["accounts_pin"],
            entities_pin=pins["entities_pin"],
            dg_ids=dg_list,
        )
        for r in run_tsv(sql, timeout):
            resolved[int(_s(r[0]))] = (_i(r[1]), _i(r[2]))
    by_typology: dict[str, dict[str, int]] = {}
    for typ, dg in subjects:
        mapped, is_cust = resolved.get(dg, (0, 0))
        b = by_typology.setdefault(typ, {"subjects": 0, "unmapped": 0, "not_customer": 0})
        b["subjects"] += 1
        if not mapped:
            b["unmapped"] += 1
        elif not is_cust:
            b["not_customer"] += 1
    scoped = {t: v for t, v in by_typology.items() if t in scoped_typologies}
    bad = {t for t, v in scoped.items() if v["not_customer"]}
    gaps = {t for t, v in scoped.items() if v["unmapped"]}
    if bad:
        status = "fail"
    elif gaps:
        status = "incomplete"
    elif unresolved & set(scoped_typologies):
        status = "unchecked"
    else:
        status = "ok"
    return {
        "status": status,
        "subjects": sum(v["subjects"] for v in by_typology.values()),
        "unmapped": sum(v["unmapped"] for v in by_typology.values()),
        "not_customer": sum(v["not_customer"] for v in by_typology.values()),
        "failing_typologies": sorted(bad | gaps),
        "unresolved_typologies": sorted(unresolved),
        "by_typology": by_typology,
    }


# --- assembly ----------------------------------------------------------------


def _summary_from_counts(
    *,
    cov: dict[str, dict[str, int]],
    prec: dict[str, dict[str, int]],
    total_alerts: int,
    tp_global: int,
    n_random: int,
    chance_hits: dict[str, int],
    status_rows: list[dict],
    designated: dict[str, list[str]],
    rule_status: dict[str, str],
    by_code: dict[str, Any],
    capped_by_rule: dict[str, int],
    covered_pass: bool,
) -> dict[str, Any]:
    """The compute_scores summary, from the integer counts of one pass. When
    ``covered_pass`` the recall denominators are covered instances; otherwise
    the full corpus. fp/precision are always the full-manifest counts (the
    caller passes the full-pass prec/global for covered mode)."""
    fp_alerts = total_alerts - tp_global
    fp_rate = (fp_alerts / total_alerts) if total_alerts > 0 else None
    fp_by_rule: dict[str, float] = {}
    nonplanted_by_rule: dict[str, int] = {}
    txn_precision_by_rule: dict[str, float] = {}
    targeted = {rid: typ for typ, rids in designated.items() for rid in rids}
    if total_alerts > 0:
        nonplanted_by_rule.update(dict.fromkeys(targeted, 0))
        for rule, p in prec.items():
            if p["rule_alerts"]:
                fp_by_rule[rule] = p["off_alerts"] / p["rule_alerts"]
                nonplanted_by_rule[rule] = p["off_alerts"]
            if p["txn_pairs"]:
                txn_precision_by_rule[rule] = p["on_target_txn_pairs"] / p["txn_pairs"]
    chance_by_rule = {r: n / n_random for r, n in chance_hits.items()} if n_random else {}

    # typology_counts is over the typology_types present in the manifest (the
    # Spark state_rows universe is the agg's typology_types, i.e. cov keys).
    counts = {
        k: sum(1 for typ in set(cov) if _typology_state(typ, designated, rule_status) == k)
        for k in ("scored", "partial", "no_rule", "rule_skipped", "rule_error")
    }
    rules = [
        {
            "rule_id": r["rule_id"],
            "status": r.get("status"),
            "reason": r.get("reason"),
            "target_typology": r.get("target_typology"),
            "alert_count": r.get("alert_count"),
        }
        for r in sorted(status_rows, key=lambda x: x["rule_id"])
    ]
    capped_typologies = {
        typ: sorted(r for r in rids if capped_by_rule.get(r))
        for typ, rids in sorted(designated.items())
        if any(capped_by_rule.get(r) for r in rids)
    }
    random_floor = None
    if "random" in cov and cov["random"]["covered_instances"]:
        random_floor = cov["random"]["incidental_hits"] / cov["random"]["covered_instances"]
    elif "random" in cov and not covered_pass and cov["random"]["corpus_instances"]:
        random_floor = cov["random"]["incidental_hits"] / cov["random"]["corpus_instances"]
    summary = {
        **by_code,
        "evidence_capped_alerts_by_rule": dict(sorted(capped_by_rule.items())),
        "recall_bounded_by_evidence_cap": capped_typologies,
        "typology_counts": counts,
        "rules": rules,
        "total_alerts": int(total_alerts),
        "fp_alerts": int(fp_alerts),
        "fp_rate": fp_rate,
        "fp_rate_by_rule": fp_by_rule,
        "nonplanted_alerts_by_rule": dict(sorted(nonplanted_by_rule.items())),
        "txn_precision_by_rule": txn_precision_by_rule,
        "chance_by_rule": chance_by_rule,
        "random_control_floor": random_floor,
    }
    return summary


def _recall_of(counts: dict[str, int], denom_key: str, state: str) -> float | None:
    denom = counts[denom_key]
    if not denom or state not in ("scored", "partial"):
        return None
    return counts["designated_hits"] / denom


def score(
    run_tsv: RunTsv,
    *,
    catalog: str,
    snapshot_ids: dict[str, int] | None,
    run_id: str,
    account_parquet: str,
    meta: RulesMeta,
    alert_set: dict | None = None,
    timeout: int = 900,
) -> dict[str, Any]:
    """Assemble the financial_scoring record.

    ``snapshot_ids`` None is batch mode; a six-key dict is covered mode.
    ``run_id`` is the scoring run's LB_RUN_ID. ``alert_set`` is the gold
    tick's fingerprint, required and only used in covered mode. ``meta`` is the
    rule/reason-code vocabulary. Raises NotScored with a reason a caller writes
    as ``status: not_scored``.
    """
    pins = _pins(snapshot_ids)
    _guard_manifest(run_tsv, catalog, pins, timeout)
    status_rows = _status_rows(run_tsv, catalog, pins, timeout)
    run_ids = sorted({r["run_id"] for r in status_rows})
    if len(run_ids) != 1:
        raise NotScored(f"gold.detection_status holds {len(run_ids)} run ids, not one")
    status_run_id = run_ids[0]
    _check_status_run(status_run_id, run_id, catalog)
    pending = sorted(r["rule_id"] for r in status_rows if r.get("status") == "pending")
    if pending:
        raise NotScored(f"rules {pending} still pending at the last completed tick")

    rule_status = {r["rule_id"]: r["status"] for r in status_rows}
    designated = _designated(status_rows)
    ran_targets = {
        rid: typ
        for typ, rids in designated.items()
        for rid in rids
        if rule_status.get(rid) == "ran"
    }
    pairs = sorted((typ, rid) for rid, typ in ran_targets.items())
    pairs_sql = _values([(typ, rid) for typ, rid in pairs], ["typology_type", "rule_id"])
    targets = sorted((rid, typ) for typ, rids in designated.items() for rid in rids)
    targets_sql = _values(targets, ["rule_id", "target"])

    cov, covered_ids = _coverage_recall(
        run_tsv,
        catalog,
        pins,
        status_run_id,
        account_parquet,
        pairs_sql,
        covered_pass=snapshot_ids is not None,
        timeout=timeout,
    )
    prec = _precision(run_tsv, catalog, pins, status_run_id, targets_sql, timeout)
    total_alerts, tp_global = _global_fp(run_tsv, catalog, pins, status_run_id, timeout)
    n_random, chance_hits = _chance(
        run_tsv,
        catalog,
        pins,
        status_run_id,
        account_parquet,
        covered_pass=snapshot_ids is not None,
        timeout=timeout,
    )
    capped = _evidence_cap(run_tsv, catalog, pins, status_run_id, timeout)
    n_inst = _n_inst(run_tsv, catalog, pins, timeout)
    workload = _workload_categories(run_tsv, catalog, pins, timeout)
    by_code = _by_code(
        run_tsv, catalog, pins, status_run_id, targets_sql, ran_targets, n_inst, meta, timeout
    )

    summary = _summary_from_counts(
        cov=cov,
        prec=prec,
        total_alerts=total_alerts,
        tp_global=tp_global,
        n_random=n_random,
        chance_hits=chance_hits,
        status_rows=status_rows,
        designated=designated,
        rule_status=rule_status,
        by_code=by_code,
        capped_by_rule=capped,
        covered_pass=snapshot_ids is not None,
    )
    scoped: set[str] = {
        t
        for r in meta.customer_scoped_rules & {rid for rid, t in ran_targets.items()}
        if (t := meta.rule_target_typology.get(r)) is not None
    }

    if snapshot_ids is None:
        return _batch_record(
            run_tsv,
            catalog,
            pins,
            account_parquet,
            status_run_id,
            cov,
            covered_ids,
            summary,
            scoped,
            workload,
            timeout,
        )
    return _covered_record(
        run_tsv,
        catalog,
        pins,
        account_parquet,
        snapshot_ids,
        status_run_id,
        cov,
        covered_ids,
        summary,
        designated,
        rule_status,
        status_rows,
        scoped,
        alert_set,
        workload,
        timeout,
    )


def _check_status_run(status_run_id: str, own_run_id: str, catalog: str) -> None:
    if not own_run_id:
        return
    if status_run_id == own_run_id or status_run_id.startswith(own_run_id + "-c"):
        return
    raise NotScored(
        f"{catalog}.gold.detection_status names run {status_run_id}, not this run "
        f"({own_run_id}): gold still holds another run's alerts"
    )


def _manifest_subject_rows(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    timeout: int,
    covered_ids: set[str] | None,
) -> list[tuple[str, list[int] | None, int | None]]:
    """(typology_type, participant_entity_ids, seed) for the subject check, from
    bronze.manifest. ``covered_ids`` restricts to those instances at instance
    grain (the covered pass checks covered subjects only, as score_covered's
    covered_manifest does); None reads every instance (batch). Filtering is in
    Python against typology_id, so a large covered set is not pushed through an
    IN-list argv."""
    # ARRAY_JOIN renders the id array as a comma string; a raw ARRAY column
    # prints as a bracketed literal in Trino TSV, which would not parse.
    sql = (
        f"SELECT typology_id, typology_type, "
        f"ARRAY_JOIN(participant_entity_ids, ','), CAST(seed AS BIGINT) "
        f"FROM {catalog}.bronze.manifest {pins['manifest_pin']}"
    )
    rows = run_tsv(sql, timeout)
    out: list[tuple[str, list[int] | None, int | None]] = []
    for r in rows:
        if covered_ids is not None and r[0] not in covered_ids:
            continue
        ents = None
        if r[2] not in (None, ""):
            ents = [int(x) for x in str(r[2]).split(",") if x != ""]
        out.append((_s(r[1]), ents, None if r[3] in (None, "") else int(str(r[3]))))
    return out


def _typology_row_state(
    typ: str, designated: dict[str, list[str]], rule_status: dict[str, str]
) -> tuple[str, str | None]:
    """(detection_status, designated_rules) for a typology, as compute_scores
    state_rows carries them."""
    return (
        _typology_state(typ, designated, rule_status),
        ",".join(sorted(designated.get(typ, []))) or None,
    )


def _batch_record(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    account_parquet: str,
    run_id: str,
    cov: dict[str, dict[str, int]],
    covered_ids: set[str],
    summary: dict[str, Any],
    scoped: set[str],
    workload: dict[str, str | None],
    timeout: int,
) -> dict[str, Any]:
    """The batch financial_scoring record: the compute_scores summary plus the
    per-typology recall rows, the subject-customer check and customer_count.
    recall is designated_hits / corpus_instances, null unless scored/partial.
    """
    designated: dict[str, list[str]] = {}
    rule_status: dict[str, str] = {}
    for r in summary["rules"]:
        rule_status[r["rule_id"]] = r["status"]
        if r.get("target_typology"):
            designated.setdefault(r["target_typology"], []).append(r["rule_id"])

    # Batch scores the whole manifest: every instance's subject is checked.
    subj_rows = _manifest_subject_rows(run_tsv, catalog, pins, timeout, None)
    check = _subject_check(run_tsv, catalog, pins, account_parquet, subj_rows, scoped, timeout)
    typologies = []
    for typ in sorted(cov):
        c = cov[typ]
        state, des_rules = _typology_row_state(typ, designated, rule_status)
        corpus = c["corpus_instances"]
        recall = (
            (c["designated_hits"] / corpus) if (corpus and state in ("scored", "partial")) else None
        )
        incidental = (c["incidental_hits"] / corpus) if corpus else None
        typologies.append(
            {
                "typology_type": typ,
                "workload_category": workload.get(typ),
                "designated_rules": des_rules,
                "recall": recall,
                "incidental_recall": incidental,
                "instance_count": corpus,
                "detection_status": state,
                "subjects_not_customer": ((check.get("by_typology") or {}).get(typ, {})).get(
                    "not_customer"
                ),
                "bounded_by_evidence_cap": summary["recall_bounded_by_evidence_cap"].get(typ, []),
            }
        )
    record = dict(summary)
    record["run_id"] = run_id
    record["subject_customer_check"] = check
    record["typologies"] = typologies
    record["computed_by"] = COMPUTED_BY
    return record


def _covered_record(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    account_parquet: str,
    snapshot_ids: dict[str, int],
    run_id: str,
    cov: dict[str, dict[str, int]],
    covered_ids: set[str],
    summary: dict[str, Any],
    designated: dict[str, list[str]],
    rule_status: dict[str, str],
    status_rows: list[dict],
    scoped: set[str],
    alert_set: dict | None,
    workload: dict[str, str | None],
    timeout: int,
) -> dict[str, Any]:
    """The covered financial_scoring record (mode: covered)."""
    excluded = {
        typ: "mode-excluded"
        for typ in designated
        if all(
            r.get("status") == "skipped" and r.get("reason") == "mode-excluded"
            for r in status_rows
            if r.get("target_typology") == typ
        )
    }
    # Rescreen-only exclusion is applied over the covered manifest (instance
    # grain): covered instances whose injection_parameters['detectable_by'] is
    # 'rescreen', counted per typology.
    rescreen = _rescreen_only(run_tsv, catalog, pins, timeout, covered_ids)
    # Covered subject check: subjects of covered instances only.
    subj_rows = _manifest_subject_rows(run_tsv, catalog, pins, timeout, covered_ids)
    check = _subject_check(run_tsv, catalog, pins, account_parquet, subj_rows, scoped, timeout)

    typologies = []
    for typ in sorted(cov):
        c = cov[typ]
        state, des_rules = _typology_row_state(typ, designated, rule_status)
        covn = c["covered_instances"]
        corpus = c["corpus_instances"]
        recall_covered = (
            (c["designated_hits"] / covn) if (covn and state in ("scored", "partial")) else None
        )
        incidental_covered = (c["incidental_hits"] / covn) if covn else None
        typologies.append(
            {
                "typology_type": typ,
                "recall_covered": recall_covered,
                "incidental_recall_covered": incidental_covered,
                "covered_instances": covn,
                "corpus_instances": corpus,
                "coverage": (covn / corpus) if corpus else None,
                "no_participant_txns": c["no_participant_txns"],
                "rescreen_only_excluded": rescreen.get(typ, 0),
                "detection_status": state,
                "designated_rules": des_rules,
                "workload_category": workload.get(typ),
            }
        )
    covered_block = {
        **{f"{name}_snapshot": snapshot_ids[name] for name, _ in _COVERED},
        "typologies": typologies,
        "excluded_typologies": [
            {"typology_type": t, "reason": why} for t, why in sorted(excluded.items())
        ],
        "covered_instances": sum(int(t["covered_instances"] or 0) for t in typologies),
        "corpus_instances": sum(int(t["corpus_instances"] or 0) for t in typologies),
        "total_alerts": summary["total_alerts"],
        "fp_alerts": summary["fp_alerts"],
        "fp_rate": summary["fp_rate"],
        "fp_rate_by_rule": summary["fp_rate_by_rule"],
        "txn_precision_by_rule": summary["txn_precision_by_rule"],
        "chance_by_rule": summary["chance_by_rule"],
        "random_control_floor": summary["random_control_floor"],
        "typology_counts": summary["typology_counts"],
        "rules": summary["rules"],
        "evidence_capped_alerts_by_rule": summary["evidence_capped_alerts_by_rule"],
        "recall_bounded_by_evidence_cap": summary["recall_bounded_by_evidence_cap"],
        "subject_customer_check": check,
    }
    return {
        "mode": "covered",
        "status": "scored",
        "run_id": run_id,
        "covered": covered_block,
        "alert_set": alert_set,
        "computed_by": COMPUTED_BY,
    }


def _rescreen_only(
    run_tsv: RunTsv,
    catalog: str,
    pins: dict[str, str],
    timeout: int,
    covered_ids: set[str],
) -> dict[str, int]:
    """Covered instances whose injection_parameters.detectable_by = 'rescreen',
    per typology; left out of covered recall and counted (score_covered). The
    rescreen rows are selected in SQL, then restricted to covered instances in
    Python at instance grain (a partially-covered typology must not count its
    uncovered rescreen instances)."""
    sql = (
        f"SELECT typology_id, typology_type FROM {catalog}.bronze.manifest "
        f"{pins['manifest_pin']} "
        f"WHERE ELEMENT_AT(injection_parameters, 'detectable_by') = 'rescreen'"
    )
    try:
        rows = run_tsv(sql, timeout)
    except Exception:  # noqa: BLE001 -- a manifest without the column
        return {}
    out: dict[str, int] = {}
    for tid, typ in rows:
        if tid in covered_ids:
            out[_s(typ)] = out.get(_s(typ), 0) + 1
    return out
