"""The expected-results file, written from reference runs.

``uat/expected-results-<version>.json`` is what the release gate holds every
matrix record's results to (``release_record.record_problems`` item 6, read
through ``release_record.load_expected``). It is written once, from
reference runs made before the freeze, reviewed by a second person, and
committed before the freeze commit (the gate's ``expected-results`` check).
``harness.py expected`` is the command; a rehearsal writes a draft with the
same code to its ``--out``.

Shape (the reader's)::

    {"version": "X.Y.Z",
     "entries": [{"workload", "workload_version", "corpus_id_v2", "scale",
                  "mode": "batch", "query_set_id", "fingerprints": {query: fp},
                  "alert_set": {...} (AML only), "from_runs": [run ids]}],
     "continuous": [{"workload", "workload_version", "query_set_ids": [...],
                     "fingerprints": {query: fp} (Customer 360 only),
                     "from_runs": [run ids]}]}

A batch entry is keyed like the reader matches it: workload, workload
version, corpus id v2, scale. A continuous entry by workload and workload
version. ``from_runs`` is for the reviewer; the reader ignores it.

Every input record must be exp2 (corpus id v2), a PASSED run on a corpus that
is not held out, has no corpus problems and came from the release datagen
image (``release_record._image_problems``, the check the gate later applies).
Its results must hold a usable fingerprint for every query of today's
benchmark registry and no other (batch with a query engine, and the Customer
360 continuous result check), none of them empty unless the query allows an
empty result, under the registry's full query set id; an AML batch run must
carry a non-empty alert set. A continuous run's rounds must have executed the
registry's sets: the full set for Customer 360, the pre-case and the full set
for AML, at the release matrix's continuous scale (the gate matches
continuous entries by workload and workload version only). Runs of one entry
must share corpus id v2 and scale, every pair of them must match as the gate
matches fingerprints (``fingerprint.mismatch``) and their alert sets must be
equal; a batch entry needs at least one run with a query engine;
each approximate column's expected sums are the midpoint of the runs'
range. Any refusal writes nothing. Before returning, the file is checked
against every input record through the gate's own reader
(``release_record._results_problems``), so the file accepts the runs it was
written from.

Nothing here prints or writes a seed: the corpus is named only by its id v2,
and corpus problems (which can name seeds) are counted, not quoted.
"""

from __future__ import annotations

import copy
import importlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

#: Fingerprint keys that are evidence of how a result was read, not part of
#: the match (``benchmark/fingerprint.py``): left out of the expected value,
#: which stands for every engine.
_EVIDENCE_KEYS = ("engine", "adapted_sql_sha")

VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")


class ExpectedRefused(Exception):
    """The records cannot make an expected-results file; ``problems`` says why."""

    def __init__(self, problems: Sequence[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


def file_name(version: str) -> str:
    """The name the release gate reads (``release_gate.EXPECTED_RESULTS``)."""
    return f"expected-results-{version}.json"


def load_records(dirs: Sequence[Path]) -> list[tuple[str, dict[str, Any]]]:
    """``(run id, record)`` for every ``run-*/metrics.json`` under *dirs*
    (the ``uat/runs/`` layout), sorted by run id. Refuses a directory with
    no records, an unreadable record and a run id found twice."""
    problems: list[str] = []
    found: dict[str, dict[str, Any]] = {}
    for d in dirs:
        paths = sorted(Path(d).glob("run-*/metrics.json"))
        if not paths:
            problems.append(f"{d}: no run-*/metrics.json")
        for p in paths:
            rid = p.parent.name
            if rid in found:
                problems.append(f"{rid}: found twice")
                continue
            try:
                data = json.loads(p.read_text())
            except (OSError, ValueError) as e:
                problems.append(f"{rid}: unreadable: {e}")
                continue
            if not isinstance(data, dict):
                problems.append(f"{rid}: not a JSON object")
                continue
            found[rid] = data
    if problems:
        raise ExpectedRefused(problems)
    return sorted(found.items())


def _mode(value: Any) -> str:
    from lakebench.metrics.release_record import _mode as mode

    return mode(value)


def _round_set(rnd: Mapping[str, Any]) -> str:
    """The query set a round counts for, by the release record's own rule
    (``release_record.round_query_set``): a round whose only failures are the
    tolerated Q9 counts for the set it listed, so writer and reader agree."""
    from lakebench.metrics.release_record import round_query_set

    return str(round_query_set(rnd) or rnd.get("executed_query_set_id") or "")


def _rounds(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The in-stream rounds the gate reads (``release_record._results_problems``):
    those that measured a QpH."""
    return [
        r
        for r in (
            (record.get("pipeline_benchmark") or {}).get("benchmark_rounds")
            or record.get("benchmark_rounds")
            or []
        )
        if isinstance(r, dict) and isinstance(r.get("qph"), (int, float)) and r["qph"] > 0
    ]


def _alert_shape(alert: Any) -> str | None:
    """Why *alert* is not a well-formed alert set (``metrics.alert_set``'s
    check when that module is present), else None."""
    if not isinstance(alert, Mapping) or not alert:
        return "not an object"
    try:
        mod = importlib.import_module("lakebench.metrics.alert_set")
    except ImportError:
        return None
    problem: str | None = mod.shape_problem(alert)
    return problem


def registry(workload: str) -> dict[str, Any] | None:
    """The workload's benchmark queries as the registry holds them today:
    ``names``, ``allow_empty`` (queries whose result may be empty), ``full``
    (the query set id of every query) and ``pre_case`` (the id of the set an
    AML continuous round runs before a case exists: every query but the
    investigator class). None for a workload with no query set of its own."""
    from lakebench.benchmark.queries import BENCHMARK_QUERIES_BY_DOMAIN, query_set_id
    from lakebench.config.schema import WorkloadSchema

    try:
        queries = BENCHMARK_QUERIES_BY_DOMAIN[WorkloadSchema(workload)]
    except (KeyError, ValueError):
        return None
    names = sorted(q.name for q in queries)
    return {
        "names": names,
        "allow_empty": {q.name for q in queries if q.allow_empty},
        "full": query_set_id(names),
        "pre_case": query_set_id([q.name for q in queries if q.query_class != "investigator"]),
    }


def _expected_sets(workload: str) -> list[str] | None:
    """The query sets a continuous reference's rounds must have executed:
    Customer 360 the full set; AML the pre-case set and, once a case
    exists, the full set."""
    reg = registry(workload)
    if reg is None:
        return None
    if workload == "financial":
        return sorted({reg["pre_case"], reg["full"]})
    return [reg["full"]]


def _fingerprint_problems(rid: str, fps: Any, reg: Mapping[str, Any] | None) -> list[str]:
    """Missing, unusable or empty fingerprints, and a query list that is not
    the registry's (a reference that skipped or added a query)."""
    from lakebench.benchmark.fingerprint import describe, usable

    if not isinstance(fps, Mapping) or not fps:
        return [f"{rid}: no result fingerprints"]
    problems = []
    allow_empty = set((reg or {}).get("allow_empty") or ())
    for q, fp in sorted(fps.items()):
        if not usable(fp):
            problems.append(f"{rid}: query {q} has no usable fingerprint ({describe(fp)})")
        elif int(fp.get("rows") or 0) == 0 and q not in allow_empty:
            problems.append(
                f"{rid}: query {q} returned no rows (it does not allow an empty result)"
            )
    if reg is not None and sorted(fps) != reg["names"]:
        problems.append(
            f"{rid}: queries are not the registry's (missing "
            f"{sorted(set(reg['names']) - set(fps))}, extra {sorted(set(fps) - set(reg['names']))})"
        )
    return problems


def record_refusals(
    rid: str,
    record: Mapping[str, Any],
    release_digest: str | None = None,
    root: Path | None = None,
) -> list[str]:
    """Why one record cannot be a reference run (empty when it can).
    *release_digest* and *root* are ``release_record._image_problems``'s: the
    corpus must come from the release datagen image, as the gate later
    requires of every matrix record."""
    from lakebench.config.datagen_seed import PROTECTED_ROLES
    from lakebench.metrics import release_record as rr
    from lakebench.metrics.experiment import experiment_of
    from lakebench.metrics.verdict import passed

    exp = experiment_of(dict(record))
    if not isinstance(exp, Mapping) or exp.get("schema") not in ("exp1", "exp2"):
        return [f"{rid}: no experiment block"]
    problems: list[str] = []
    if exp.get("schema") != "exp2":
        problems.append(f"{rid}: exp1 record: expected results need corpus id v2 (exp2)")
    kind = record.get("record_kind") or "run"
    if kind != "run":
        problems.append(f"{rid}: a {kind} record, not a run")
    if not passed(dict(record)):
        problems.append(f"{rid}: the run did not pass")
    corpus = exp.get("corpus") or {}
    role = corpus.get("corpus_role")
    if role in PROTECTED_ROLES:
        problems.append(f"{rid}: the corpus is the held-out {role} corpus")
    if corpus.get("problems"):
        # Not quoted: a corpus problem can name the configured and observed
        # seeds.
        problems.append(
            f"{rid}: the record lists {len(corpus['problems'])} corpus problem(s) "
            "(experiment.corpus.problems)"
        )
    elif exp.get("schema") == "exp2":
        if not corpus.get("id_v2"):
            problems.append(f"{rid}: no corpus id v2")
        digest = release_digest if release_digest is not None else rr.release_datagen_digest()
        problems += [f"{rid}: {p}" for p in rr._image_problems(record, exp, digest, root)]
    workload = exp.get("workload") or {}
    name, version = workload.get("name"), workload.get("version")
    if not name or not version:
        problems.append(f"{rid}: no workload name and version")
    reg = registry(str(name)) if name else None
    if name and reg is None:
        problems.append(f"{rid}: workload {name} has no benchmark query set of its own")
    results = exp.get("results") or {}
    recipe = str((exp.get("architecture") or {}).get("recipe") or "")
    mode = _mode(exp.get("mode"))
    if mode == "batch":
        if not recipe.endswith("-none"):
            if results.get("not_checked"):
                problems.append(f"{rid}: results not checked: {results['not_checked']}")
            problems += _fingerprint_problems(rid, results.get("fingerprints"), reg)
            qs = results.get("query_set_id")
            if reg is not None and qs != reg["full"]:
                problems.append(
                    f"{rid}: query set {qs} is not the registry's full set {reg['full']}"
                )
        if name == "financial":
            alert = results.get("alert_set")
            if alert is None:
                why = results.get("alert_set_unavailable")
                problems.append(
                    f"{rid}: no alert set" + (f" ({why})" if why else " (AML batch needs one)")
                )
            elif shape := _alert_shape(alert):
                problems.append(f"{rid}: alert set is not well formed: {shape}")
            elif not alert.get("rows"):
                problems.append(f"{rid}: the alert set holds no alerts")
    elif mode == "continuous":
        rounds = _rounds(record)
        if not rounds:
            problems.append(f"{rid}: no in-stream round measured a QpH")
        elif any(not r.get("executed_query_set_id") for r in rounds):
            problems.append(f"{rid}: rounds do not record the query set they executed")
        else:
            sets = sorted({_round_set(r) for r in rounds})
            want = _expected_sets(str(name))
            if want is not None and sets != want:
                problems.append(
                    f"{rid}: rounds ran query set(s) {', '.join(sets)}; a {name} continuous "
                    f"reference runs {', '.join(want)}"
                    + (" (the pre-case and the post-case set)" if len(want) == 2 else "")
                )
            scales = {
                float(s) for w, m, _r, s in rr.RELEASE_MATRIX if w == name and m == "continuous"
            }
            scale = float(corpus.get("scale") or 0)
            if scales and scale not in scales:
                # The gate matches continuous entries by workload alone.
                problems.append(
                    f"{rid}: a continuous reference at scale {scale:g}; the release rows run at "
                    f"scale {', '.join(f'{x:g}' for x in sorted(scales))}"
                )
        if name == "customer360":
            if results.get("not_checked"):
                problems.append(f"{rid}: results not checked: {results['not_checked']}")
            problems += _fingerprint_problems(rid, results.get("fingerprints"), reg)
    else:
        problems.append(f"{rid}: unknown mode {mode}")
    return problems


def _strip(fp: Mapping[str, Any]) -> dict[str, Any]:
    return {k: copy.deepcopy(v) for k, v in fp.items() if k not in _EVIDENCE_KEYS}


def _merge_fingerprints(
    label: str, members: Sequence[tuple[str, Mapping[str, Any]]]
) -> tuple[dict[str, Any], list[str]]:
    """One expected fingerprint per query from *members* (``(run id,
    fingerprints)``, sorted), provided every pair of runs matches as the
    gate matches (``fingerprint.mismatch``) and every run answered the same
    queries. The value is the first run's, with each approximate
    column's sums at the midpoint of the runs' range, so a release run is
    held to the centre of the references rather than to one end."""
    from lakebench.benchmark.fingerprint import mismatch

    ref_id, ref = members[0]
    problems = []
    for rid, fps in members[1:]:
        if set(fps) != set(ref):
            problems.append(
                f"{label}: {rid} answered other queries than {ref_id} "
                f"(missing {sorted(set(ref) - set(fps))}, extra {sorted(set(fps) - set(ref))})"
            )
            continue
    if problems:
        return {}, problems
    # Every pair, not only each run against the first: the gate's rule is
    # pairwise, and tolerance is not transitive (A, A+T and A-T each match A
    # but the last two differ by 2T).
    for i, (rid, fps) in enumerate(members):
        for other_id, other in members[i + 1 :]:
            for q in sorted(ref):
                why = mismatch(other[q], fps[q])
                if why:
                    problems.append(
                        f"{label}: query {q} differs between {other_id} and {rid}: {why}"
                    )
    merged = {}
    for q in sorted(ref):
        fp = _strip(ref[q])
        for key in ("approx", "approx_w"):
            sums = fp.get(key)
            if not isinstance(sums, dict):
                continue
            for col in sums:
                values: list[float] = []
                for _rid, fps in members:
                    theirs: Any = (fps.get(q) or {}).get(key) or {}
                    values.append(float(theirs.get(col, sums[col])))
                sums[col] = (min(values) + max(values)) / 2
        merged[q] = fp
    return merged, problems


def _one(label: str, what: str, values: Mapping[str, Any]) -> list[str]:
    """A problem when the runs of one entry disagree on *what*."""
    distinct = {json.dumps(v, sort_keys=True) for v in values.values()}
    if len(distinct) <= 1:
        return []
    shown = ", ".join(f"{rid} {v}" for rid, v in sorted(values.items()))
    return [f"{label}: runs differ in {what} ({shown})"]


def build_expected(
    records: Sequence[tuple[str, Mapping[str, Any]]],
    version: str,
    *,
    release_digest: str | None = None,
    root: Path | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """``(expected-results dict, notes)`` from ``(run id, record)`` pairs;
    raises ``ExpectedRefused`` with every problem found. *release_digest*
    (default ``release_record.release_datagen_digest()``) and *root* (the
    repository, for the lineage evidence file) go to the image check."""
    from lakebench.metrics.experiment import experiment_of
    from lakebench.metrics.release_record import RELEASE_MATRIX, _results_problems

    if not VERSION_RE.match(version):
        raise ExpectedRefused([f"version {version!r} is not X.Y.Z"])
    if not records:
        raise ExpectedRefused(["no reference records"])
    problems: list[str] = []
    for rid, rec in records:
        problems += record_refusals(rid, rec, release_digest, root)
    if problems:
        raise ExpectedRefused(problems)

    batch: dict[tuple[str, str, float], list[tuple[str, Mapping[str, Any]]]] = {}
    cont: dict[tuple[str, str], list[tuple[str, Mapping[str, Any]]]] = {}
    exps: dict[str, Mapping[str, Any]] = {}
    for rid, rec in sorted(records, key=lambda x: x[0]):
        exp = experiment_of(dict(rec))
        assert exp is not None  # record_refusals checked it
        exps[rid] = exp
        w = exp.get("workload") or {}
        key2 = (str(w["name"]), str(w["version"]))
        if _mode(exp.get("mode")) == "batch":
            scale = float((exp.get("corpus") or {}).get("scale") or 0)
            batch.setdefault((*key2, scale), []).append((rid, rec))
        else:
            cont.setdefault(key2, []).append((rid, rec))

    def res(rid: str) -> Mapping[str, Any]:
        return exps[rid].get("results") or {}

    def recipe(rid: str) -> str:
        return str((exps[rid].get("architecture") or {}).get("recipe") or "")

    entries: list[dict[str, Any]] = []
    for (name, wver, scale), members in sorted(batch.items()):
        label = f"{name} {wver} batch scale {scale:g}"
        ids = [rid for rid, _ in members]
        corpus_ids = {rid: (exps[rid].get("corpus") or {}).get("id_v2") for rid in ids}
        problems += _one(label, "corpus id v2", corpus_ids)
        engine = [rid for rid in ids if not recipe(rid).endswith("-none")]
        if not engine:
            problems.append(f"{label}: no reference run with a query engine")
            continue
        problems += _one(label, "query set", {rid: res(rid).get("query_set_id") for rid in engine})
        fps, why = _merge_fingerprints(
            label, [(rid, dict(res(rid).get("fingerprints") or {})) for rid in engine]
        )
        problems += why
        entry: dict[str, Any] = {
            "workload": name,
            "workload_version": wver,
            "corpus_id_v2": corpus_ids[ids[0]],
            "scale": scale,
            "mode": "batch",
            "query_set_id": res(engine[0]).get("query_set_id"),
            "fingerprints": fps,
        }
        if name == "financial":
            alerts = {rid: res(rid).get("alert_set") for rid in ids}
            problems += _one(label, "alert set", alerts)
            entry["alert_set"] = copy.deepcopy(alerts[ids[0]])
        entry["from_runs"] = ids
        entries.append(entry)

    continuous: list[dict[str, Any]] = []
    for (name, wver), members in sorted(cont.items()):
        label = f"{name} {wver} continuous"
        ids = [rid for rid, _ in members]
        problems += _one(
            label,
            "corpus id v2",
            {rid: (exps[rid].get("corpus") or {}).get("id_v2") for rid in ids},
        )
        problems += _one(
            label,
            "scale",
            {rid: float((exps[rid].get("corpus") or {}).get("scale") or 0) for rid in ids},
        )
        sets = {rid: sorted({_round_set(r) for r in _rounds(rec)}) for rid, rec in members}
        problems += _one(label, "executed query sets", sets)
        centry: dict[str, Any] = {
            "workload": name,
            "workload_version": wver,
            "scale": float((exps[ids[0]].get("corpus") or {}).get("scale") or 0),
            "query_set_ids": sets[ids[0]],
        }
        if name == "customer360":
            fps, why = _merge_fingerprints(
                label, [(rid, dict(res(rid).get("fingerprints") or {})) for rid in ids]
            )
            problems += why
            centry["fingerprints"] = fps
        centry["from_runs"] = ids
        continuous.append(centry)

    if problems:
        raise ExpectedRefused(problems)
    expected: dict[str, Any] = {"version": version, "entries": entries, "continuous": continuous}

    # The gate's own reader must accept every reference run against the
    # file: writer and reader cannot drift apart unnoticed.
    for rid, rec in records:
        problems += [
            f"{rid}: the written file does not accept its own reference run: {p}"
            for p in _results_problems(rec, exps[rid], expected)
        ]
    if problems:
        raise ExpectedRefused(problems)

    covered = {(e["workload"], "batch", float(e["scale"])) for e in entries} | {
        (c["workload"], "continuous") for c in continuous
    }
    notes = []
    for w, m, _recipe, s in sorted(set(RELEASE_MATRIX)):
        key = (w, m, float(s)) if m == "batch" else (w, m)
        if key not in covered:
            notes.append(
                f"no entry for release rows {w} {m}"
                + (f" scale {s:g}" if m == "batch" else "")
                + "; their records fail the gate until one is added"
            )
    return expected, sorted(set(notes))


def dump(expected: Mapping[str, Any]) -> str:
    return json.dumps(expected, indent=2, sort_keys=True) + "\n"


def _rows(fps: Mapping[str, Any]) -> str:
    return ", ".join(f"{q} {fp.get('rows')}" for q, fp in sorted(fps.items()))


def summary(expected: Mapping[str, Any]) -> list[str]:
    """What the reviewer reads: per entry, its runs, rows per query and
    alerts per rule (no seed)."""
    lines = []
    for e in expected.get("entries") or []:
        lines.append(
            f"batch {e['workload']} {e['workload_version']} scale {float(e['scale']):g} "
            f"corpus {e['corpus_id_v2']} query set {e['query_set_id']}, "
            f"from {', '.join(e['from_runs'])}"
        )
        lines.append(f"  rows per query: {_rows(e['fingerprints'])}")
        alert = e.get("alert_set")
        if isinstance(alert, Mapping):
            by_rule = alert.get("by_rule") or {}
            lines.append(
                f"  alerts: {alert.get('rows')}; per rule: "
                + ", ".join(f"{r} {(p or {}).get('rows')}" for r, p in sorted(by_rule.items()))
            )
    for c in expected.get("continuous") or []:
        lines.append(
            f"continuous {c['workload']} {c['workload_version']} scale {float(c['scale']):g}: "
            f"query sets {', '.join(c['query_set_ids'])}, from {', '.join(c['from_runs'])}"
        )
        if c.get("fingerprints"):
            lines.append(f"  rows per query (result check): {_rows(c['fingerprints'])}")
    return lines


def write(path: Path, expected: Mapping[str, Any], *, replace: bool = False) -> None:
    """Write *expected* to *path*; refuses an existing file unless *replace*
    (a reviewed file is never regenerated over)."""
    if path.exists() and not replace:
        raise ExpectedRefused([f"{path} exists; a reviewed expected-results file is not rewritten"])
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(dump(expected))
    tmp.replace(path)
