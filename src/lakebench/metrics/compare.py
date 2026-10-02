"""Compare two sides of stored run records (``lakebench compare``).

Pure: this module reads metrics.json files and series manifests and builds
the comparison; it makes no cluster call and writes nothing (resolving a
config ref loads the config, which imports cluster client modules without
calling them). ``cli/_compare.py`` is the thin command over it.

A side is a comma-separated list of refs, each tried in this order:

1. ``series:<id>``: the members of a ``run --repeat`` series manifest
   (``<output dir>/series/<id>.json``, schema ``lb-series/1``);
2. a run id ``YYYYMMDD-HHMMSS-xxxxxx`` (``run-`` prefix allowed), looked up
   in every runs directory;
3. a directory holding ``metrics.json``, or a path to a ``metrics.json``;
4. a config file (``.yaml``/``.yml``): the latest run record of its
   deployment name, and when that record belongs to a series, every member
   of the series its manifest names.

A member whose verdict did not pass is excluded and listed. The
pair is decided by ``comparability.pair_verdict`` over the passed members;
``missing_condition`` turns the ladder's structured cause into the one
condition the pair lacks and the command that supplies it; ``assess`` says
per metric what may be read from the numbers. No code path here names a
winner: the winner rule is not in this release (``WINNER_RULE``).
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lakebench.metrics import comparability as cmp

#: The cmp2 document ``--format json`` and ``-o`` write.
SCHEMA = "cmp2"
#: The series manifest schema ``run --repeat`` writes (metrics/series.py).
MANIFEST_SCHEMA = "lb-series/1"
SERIES_PREFIX = "series:"
#: False in this release: every directional metric of a comparable pair is
#: ``not_assessed`` and no winner is named.
WINNER_RULE = False

RUN_ID = re.compile(r"^(?:run-)?(\d{8}-\d{6}-[0-9a-f]{6})$")
SERIES_ID = re.compile(r"^s-\d{8}-\d{6}-[0-9a-f]{6}$")
_RECORD_KINDS = (None, "run")
_CONFIG_SUFFIXES = (".yaml", ".yml")

#: Assessment outcomes (``assess``).
WITHHELD = "withheld"
NOT_DIRECTIONAL = "not_directional"
CONFOUNDED_ROW = "confounded"
NOT_ASSESSED = "not_assessed"
CAPPED = "capped"


class CompareError(ValueError):
    """A side that cannot be resolved; *path* is the exit path id
    (``compare.bad_ref``, ``compare.same_runs``, ``compare.equal_names``,
    ``compare.unreadable_record``)."""

    def __init__(self, path: str, message: str) -> None:
        super().__init__(message)
        self.path = path


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


@dataclass
class Member:
    run_id: str
    path: Path
    record: dict[str, Any]
    sha256: str
    passed: bool = True
    status: str | None = None
    excluded: str | None = None


@dataclass
class Side:
    label: str
    refs: list[str]
    members: list[Member] = field(default_factory=list)
    #: Series runs that are not members (``not_member_reason``), as
    #: ``(run id or index, reason)``.
    non_members: list[tuple[str, str]] = field(default_factory=list)
    deployment: str | None = None
    series: str | None = None
    #: The config refs and the deployment name each resolved to.
    configs: list[tuple[Path, str, str]] = field(default_factory=list)
    #: Every file read for this side (``-o`` may not overwrite one).
    inputs: list[Path] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def kept(self) -> list[Member]:
        """The members not excluded (passed, or from before the experiment
        block, which the ladder refuses itself)."""
        return [m for m in self.members if m.excluded is None]

    @property
    def passed(self) -> list[Member]:
        """The members whose verdict passed: what n counts and the metrics
        read."""
        return [m for m in self.kept if m.passed and cmp.generation(m.record) != cmp.LEGACY]

    @property
    def ladder_records(self) -> list[Mapping[str, Any]]:
        """What the ladder sees: the members not excluded, or, when every
        member was, all of them (so step 1 names the failure)."""
        chosen = self.kept or self.members
        return [m.record for m in chosen]


def side_of_records(label: str, records: Sequence[Mapping[str, Any]]) -> Side:
    """A side built from records already in memory (no files read), with
    the same exclusions as ``resolve_side``."""
    side = Side(label, [str(r.get("run_id") or f"record {i}") for i, r in enumerate(records)])
    for i, r in enumerate(records):
        raw = json.dumps(r, sort_keys=True, default=str).encode()
        side.members.append(
            Member(
                str(r.get("run_id") or f"record-{label}-{i}"),
                Path(f"<memory:{label}:{i}>"),
                dict(r),
                hashlib.sha256(raw).hexdigest(),
            )
        )
    _mark_exclusions(side)
    side.deployment = (records[0].get("deployment_name") if records else None) or None
    return side


def compare_records(
    records_a: Sequence[Mapping[str, Any]], records_b: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    """The cmp2 document for two sides of in-memory records."""
    return build_comparison(side_of_records("A", records_a), side_of_records("B", records_b))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_record(path: Path) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
        data = json.loads(raw)
    except (OSError, ValueError) as e:
        raise CompareError("compare.unreadable_record", f"cannot read {path}: {e}") from None
    if not isinstance(data, dict):
        raise CompareError("compare.unreadable_record", f"{path} is not a run record")
    return data, hashlib.sha256(raw).hexdigest()


def _member(path: Path, expect_id: str | None = None) -> Member:
    record, digest = _read_record(path)
    rid = record.get("run_id")
    if not isinstance(rid, str) or not rid:
        raise CompareError("compare.unreadable_record", f"{path} has no run_id")
    dir_id = RUN_ID.match(path.parent.name)
    if dir_id and dir_id.group(1) != rid:
        raise CompareError(
            "compare.unreadable_record",
            f"{path} is in run-{dir_id.group(1)} but records run {rid}",
        )
    if expect_id is not None and rid != expect_id:
        raise CompareError(
            "compare.unreadable_record", f"{path} records run {rid}, not {expect_id}"
        )
    return Member(rid, path, record, digest)


def _dirs_text(runs_dirs: Sequence[Path]) -> str:
    return ", ".join(str(d) for d in runs_dirs)


def _find_run(run_id: str, runs_dirs: Sequence[Path]) -> Member:
    """The record of *run_id* in the union of *runs_dirs*; refused when two
    directories hold different files for it."""
    found: list[Member] = []
    for d in runs_dirs:
        p = d / f"run-{run_id}" / "metrics.json"
        if p.is_file():
            found.append(_member(p, run_id))
    if not found:
        raise CompareError(
            "compare.bad_ref",
            f"no record for {run_id} in {_dirs_text(runs_dirs)}; run it first: "
            "lakebench run <config>",
        )
    digests = {m.sha256 for m in found}
    if len(digests) > 1:
        raise CompareError(
            "compare.unreadable_record",
            f"run {run_id} has different records in "
            + ", ".join(str(m.path) for m in found)
            + "; keep one",
        )
    return found[0]


def _series_dirs(runs_dirs: Sequence[Path]) -> list[Path]:
    out: list[Path] = []
    for d in runs_dirs:
        s = d.resolve().parent / "series"
        if s not in out:
            out.append(s)
    return out


def _load_manifest(series_id: str, runs_dirs: Sequence[Path]) -> tuple[dict[str, Any], Path]:
    if not SERIES_ID.match(series_id):
        raise CompareError("compare.bad_ref", f"{series_id!r} is not a series id")
    hits = [p for p in (s / f"{series_id}.json" for s in _series_dirs(runs_dirs)) if p.is_file()]
    if not hits:
        raise CompareError(
            "compare.bad_ref",
            f"no manifest for series {series_id} in "
            + ", ".join(str(s) for s in _series_dirs(runs_dirs))
            + "; pass the run ids instead",
        )
    datas = []
    for p in hits:
        try:
            data = json.loads(p.read_text())
        except (OSError, ValueError) as e:
            raise CompareError("compare.unreadable_record", f"cannot read {p}: {e}") from None
        if not isinstance(data, dict) or data.get("schema") != MANIFEST_SCHEMA:
            raise CompareError(
                "compare.unreadable_record", f"{p} is not an {MANIFEST_SCHEMA} series manifest"
            )
        if data.get("series_id") != series_id:
            raise CompareError(
                "compare.unreadable_record", f"{p} is the manifest of {data.get('series_id')}"
            )
        datas.append(data)
    if any(d != datas[0] for d in datas[1:]):
        raise CompareError(
            "compare.unreadable_record",
            f"series {series_id} has different manifests in " + ", ".join(map(str, hits)),
        )
    return datas[0], hits[0]


def _add_series(side: Side, series_id: str, runs_dirs: Sequence[Path]) -> None:
    manifest, path = _load_manifest(series_id, runs_dirs)
    side.inputs.append(path)
    side.series = series_id
    side.deployment = side.deployment or manifest.get("deployment_name")
    runs = manifest.get("runs")
    if not isinstance(runs, list):
        raise CompareError("compare.unreadable_record", f"{path} lists no runs")
    for entry in runs:
        if not isinstance(entry, Mapping):
            raise CompareError("compare.unreadable_record", f"{path} has a malformed run entry")
        rid = entry.get("run_id")
        label = str(rid or f"repetition {entry.get('index')}")
        if entry.get("member") is not True or not rid:
            side.non_members.append(
                (label, str(entry.get("not_member_reason") or "not a member of the series"))
            )
            continue
        try:
            m = _find_run(str(rid), runs_dirs)
        except CompareError as e:
            if e.path != "compare.bad_ref":
                raise
            raise CompareError(
                "compare.bad_ref",
                f"series {series_id} lists run {rid}, which has no record in "
                f"{_dirs_text(runs_dirs)}; pass --runs-dir for the directory that holds it",
            ) from None
        stamp = m.record.get("series")
        stamp_id = stamp.get("id") if isinstance(stamp, Mapping) else None
        if stamp_id != series_id:
            raise CompareError(
                "compare.unreadable_record",
                f"series {series_id} lists run {rid}, whose record says series {stamp_id}",
            )
        _add_member(side, m)


def _add_member(side: Side, m: Member) -> None:
    for prior in side.members:
        if prior.run_id == m.run_id:
            if prior.sha256 != m.sha256:
                raise CompareError(
                    "compare.unreadable_record",
                    f"run {m.run_id} is given twice with different records "
                    f"({prior.path}, {m.path})",
                )
            return
    side.members.append(m)
    side.inputs.append(m.path)


def _config_name(path: Path) -> str:
    """The deployment name config *path* resolves to under
    ``LoadPurpose.COMPARE`` (which also resolves a v1.6 nameless config
    through its state). When the file cannot be loaded (an unset ``${VAR}``
    elsewhere in it), its literal ``name:`` still names the deployment."""
    import yaml

    from lakebench.config import ConfigError, LoadPurpose, load_config

    try:
        return str(load_config(path, purpose=LoadPurpose.COMPARE).name)
    except ConfigError as e:
        try:
            raw = yaml.safe_load(path.read_text())
        except (OSError, yaml.YAMLError):
            raw = None
        raw_name = raw.get("name") if isinstance(raw, Mapping) else None
        if isinstance(raw_name, str) and raw_name and "${" not in raw_name:
            return raw_name
        raise CompareError("compare.bad_ref", f"cannot resolve the name of {path}: {e}") from None


def _records_of(name: str, runs_dirs: Sequence[Path], skipped: list[Path]) -> list[Member]:
    """Every run record of deployment *name*; files that cannot be read are
    appended to *skipped* (the caller warns: one of them may be the
    deployment's latest run)."""
    out: dict[str, Member] = {}
    for d in runs_dirs:
        if not d.is_dir():
            continue
        for p in sorted(d.glob("run-*/metrics.json")):
            try:
                data = json.loads(p.read_text())
            except (OSError, ValueError):
                skipped.append(p)
                continue
            if not isinstance(data, dict) or data.get("deployment_name") != name:
                continue
            if data.get("record_kind") not in _RECORD_KINDS:
                continue
            m = _member(p)
            prior = out.get(m.run_id)
            if prior is not None and prior.sha256 != m.sha256:
                raise CompareError(
                    "compare.unreadable_record",
                    f"run {m.run_id} has different records in {prior.path}, {m.path}; keep one",
                )
            out.setdefault(m.run_id, m)
    return list(out.values())


def _recorded_config_sha(record: Mapping[str, Any]) -> str | None:
    for block in (record.get("config_snapshot"), record.get("provenance")):
        if isinstance(block, Mapping) and isinstance(block.get("config_sha256"), str):
            return str(block["config_sha256"])
    return None


def _add_config(side: Side, path: Path, runs_dirs: Sequence[Path]) -> None:
    name = _config_name(path)
    side.inputs.append(path)
    side.configs.append((path, name, _sha256(path)))
    side.deployment = side.deployment or name
    skipped: list[Path] = []
    candidates = _records_of(name, runs_dirs, skipped)
    for p in skipped:
        side.warnings.append(
            f"{side.label}: skipped unreadable record {p}; if it is {name}'s latest run, "
            "pass the run ids instead"
        )
    if not candidates:
        raise CompareError(
            "compare.bad_ref",
            f"no record for {path} (deployment {name}) in {_dirs_text(runs_dirs)}; "
            f"run it first: lakebench run {path}",
        )
    from lakebench.metrics.storage import _sort_instant

    latest = max(candidates, key=lambda m: (_sort_instant(m.record.get("start_time")), m.run_id))
    stamp = latest.record.get("series")
    series_id = stamp.get("id") if isinstance(stamp, Mapping) else None
    if series_id:
        _add_series(side, str(series_id), runs_dirs)
    else:
        _add_member(side, latest)
    file_sha = side.configs[-1][2]
    for m in side.members:
        rec_sha = _recorded_config_sha(m.record)
        if rec_sha and rec_sha != file_sha:
            side.warnings.append(
                f"{side.label}: {path} has changed since run {m.run_id} was recorded"
            )
            break


def resolve_side(label: str, text: str, runs_dirs: Sequence[Path]) -> Side:
    """The members of one side (module docstring for the ref forms)."""
    refs = [r.strip() for r in text.split(",") if r.strip()]
    if not refs:
        raise CompareError("compare.bad_ref", f"side {label} names no run")
    side = Side(label, refs)
    for ref in refs:
        if ref.startswith(SERIES_PREFIX):
            _add_series(side, ref[len(SERIES_PREFIX) :], runs_dirs)
            continue
        rid = RUN_ID.match(ref)
        if rid and not Path(ref).exists():
            _add_member(side, _find_run(rid.group(1), runs_dirs))
            continue
        p = Path(ref)
        if p.is_dir():
            if not (p / "metrics.json").is_file():
                raise CompareError("compare.bad_ref", f"{p} holds no metrics.json")
            _add_member(side, _member(p / "metrics.json"))
        elif p.is_file() and p.suffix.lower() in _CONFIG_SUFFIXES:
            _add_config(side, p, runs_dirs)
        elif p.is_file():
            _add_member(side, _member(p))
        else:
            raise CompareError(
                "compare.bad_ref",
                f"no record for {ref} in {_dirs_text(runs_dirs)}; run it first: "
                f"lakebench run {ref if ref.endswith(_CONFIG_SUFFIXES) else '<config>'}",
            )
    if not side.members:
        raise CompareError(
            "compare.bad_ref",
            f"side {label} ({text}) resolves to no run"
            + (f": series {side.series} has no member" if side.series else ""),
        )
    _mark_exclusions(side)
    if side.deployment is None and side.members:
        side.deployment = side.members[0].record.get("deployment_name")
    return side


def _mark_exclusions(side: Side) -> None:
    """A member whose verdict did not pass is excluded (and listed). A
    record from before the experiment block is not excluded: the ladder
    refuses it at step 1."""
    for m in side.members:
        ok, status = cmp._member_passed(m.record)
        m.passed, m.status = ok, status
        if not ok and cmp.generation(m.record) != cmp.LEGACY:
            reason = cmp._first_reason(m.record)
            m.excluded = f"{status or 'did not pass'}" + (f": {reason}" if reason else "")


def resolve(side_a: str, side_b: str, runs_dirs: Sequence[Path]) -> tuple[Side, Side]:
    """Both sides, refused (``CompareError``) when they cannot be a pair."""
    a = resolve_side("A", side_a, runs_dirs)
    b = resolve_side("B", side_b, runs_dirs)
    for pa, na, sa in a.configs:
        for pb, nb, sb in b.configs:
            if na == nb and sa != sb:
                raise CompareError(
                    "compare.equal_names",
                    f"A and B both resolve to {na}; a name is one deployment "
                    f"({pa} and {pb} differ)",
                )
    ids_a = {m.run_id for m in a.members}
    ids_b = {m.run_id for m in b.members}
    if ids_a == ids_b:
        raise CompareError(
            "compare.same_runs", f"A and B resolve to the same runs ({', '.join(sorted(ids_a))})"
        )
    shared = ids_a & ids_b
    if shared:
        raise CompareError(
            "compare.same_runs",
            f"A and B share runs ({', '.join(sorted(shared))}); a run is on one side",
        )
    return a, b


def resolution_line(side: Side) -> str:
    """``A: a.yaml -> deployment d, series s, 3 runs: r1 passed, r3 FAILED
    (excluded)``."""
    head = f"{side.label}: {', '.join(side.refs)}"
    deployments: list[str] = []
    series: list[str] = []
    for m in side.members:
        dep = m.record.get("deployment_name")
        if dep and str(dep) not in deployments:
            deployments.append(str(dep))
        stamp = m.record.get("series")
        sid = stamp.get("id") if isinstance(stamp, Mapping) else None
        if sid and str(sid) not in series:
            series.append(str(sid))
    if not deployments and side.deployment:
        deployments.append(side.deployment)
    if not series and side.series:
        series.append(side.series)
    where = []
    if deployments:
        where.append(
            ("deployments " if len(deployments) > 1 else "deployment ") + ", ".join(deployments)
        )
    if series:
        where.append("series " + ", ".join(series))
    runs = []
    for m in side.members:
        if m.excluded is not None:
            runs.append(f"{m.run_id} {m.excluded} (excluded)")
        elif cmp.generation(m.record) == cmp.LEGACY:
            runs.append(f"{m.run_id} predates the experiment block")
        else:
            runs.append(f"{m.run_id} passed")
    runs += [f"{rid} not a member ({why})" for rid, why in side.non_members]
    n = len(side.members)
    body = f"{n} run{'s' if n != 1 else ''}: " + ", ".join(runs)
    return f"{head} -> " + (", ".join(where) + ", " if where else "") + body


# ---------------------------------------------------------------------------
# The missing condition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MissingCondition:
    condition: str
    command: str | None
    hint: str

    def to_dict(self) -> dict[str, Any]:
        return {"condition": self.condition, "command": self.command, "hint": self.hint}


def _first(side: Side) -> Mapping[str, Any]:
    chosen = side.passed or side.kept or side.members
    return chosen[0].record if chosen else {}


def _cfg(side: Side) -> str:
    """``provenance.config_path`` of the side's first member, else a
    placeholder naming its deployment."""
    rec = _first(side)
    path = (rec.get("provenance") or {}).get("config_path")
    if isinstance(path, str) and path:
        return path
    return f"<the config of deployment {side.deployment or rec.get('deployment_name') or '?'}>"


def _mode(side: Side) -> str | None:
    from lakebench.metrics.metric_registry import canonical_mode

    rec = _first(side)
    raw = (rec.get("pipeline_benchmark") or {}).get("pipeline_mode") or (
        rec.get("experiment") or {}
    ).get("mode")
    try:
        return canonical_mode(raw) if isinstance(raw, str) else None
    except ValueError:
        return None


def _continuous(side: Side) -> bool:
    return _mode(side) == "sustained"


def _hidden_seeds() -> frozenset[int] | None:
    """The protected and spent AML seeds, never printed; None when they
    cannot be read (then no integer seed is printed)."""
    try:
        from lakebench.config import datagen_seed

        return frozenset(
            int(x)
            for x in (
                *datagen_seed.protected_seeds(),
                *datagen_seed.spent_seeds(),
                *datagen_seed.recorded_seeds(),
            )
        )
    except Exception:  # noqa: BLE001 -- unreadable: hide every integer seed
        return None


def _seed_out(v: Any, hidden: frozenset[int] | None) -> Any:
    """*v* as a recorded seed may be shown: a protected, spent or recorded
    look seed (or any integer seed when that list cannot be read) reads
    ``<protected seed>``."""
    if isinstance(v, list | tuple):
        return [_seed_out(x, hidden) for x in v]
    if isinstance(v, str) and v.strip().isdigit():
        return v if hidden is not None and int(v) not in hidden else "<protected seed>"
    if isinstance(v, bool) or not isinstance(v, int):
        return v
    if hidden is None or v in hidden:
        return "<protected seed>"
    return v


_RUN_ID_TOKEN = re.compile(r"\d{8}-\d{6}-[0-9a-f]{6}")
_INT_TOKEN = re.compile(r"(?<![\w.-])\d+(?!\w|\.\d)")
_DIGIT_TO_LETTER = str.maketrans("0123456789", "abcdefghij")


def _scrub_text(text: str, hidden: frozenset[int] | None) -> str:
    """*text* with any hidden seed in it replaced. Only text that names a
    seed is touched. With the seed list unreadable, every integer in it
    except run ids is replaced."""
    if "seed" not in text.lower():
        return text
    masked: list[str] = []

    def mask(m: re.Match[str]) -> str:
        masked.append(m.group(0))
        return "\x00" + str(len(masked) - 1).translate(_DIGIT_TO_LETTER) + "\x00"

    out = _RUN_ID_TOKEN.sub(mask, text)
    out = _INT_TOKEN.sub(
        lambda m: "<protected seed>" if hidden is None or int(m.group(0)) in hidden else m.group(0),
        out,
    )
    return re.sub(
        "\x00([a-j]+)\x00",
        lambda m: masked[int(m.group(1).translate(str.maketrans("abcdefghij", "0123456789")))],
        out,
    )


def redact(node: Any, hidden: frozenset[int] | None) -> Any:
    """A copy of *node* (a comparison document, or any part of one) with
    every protected seed hidden: a ``seed`` value, the ``a`` and ``b`` of an
    entry whose ``key`` is ``seed``, and a hidden seed inside text that
    names a seed. *hidden* empty hides nothing."""
    if hidden is not None and not hidden:
        return node
    if isinstance(node, Mapping):
        seed_entry = node.get("key") == "seed"
        out = {}
        for k, v in node.items():
            if k == "seed" or (seed_entry and k in ("a", "b")):
                out[k] = _seed_out(v, hidden)
            else:
                out[k] = redact(v, hidden)
        return out
    if isinstance(node, list | tuple):
        return [redact(v, hidden) for v in node]
    if isinstance(node, str):
        return _scrub_text(node, hidden)
    return node


def _is_aml(record: Mapping[str, Any]) -> bool:
    exp = record.get("experiment") or {}
    snap = record.get("config_snapshot") or {}
    return "financial" in (
        (exp.get("workload") or {}).get("name"),
        (exp.get("corpus") or {}).get("schema"),
        snap.get("workload_schema"),
        snap.get("schema_type"),
    )


def hidden_for(*sides: Side) -> frozenset[int] | None:
    """The seeds a comparison of *sides* must not print: the protected,
    spent and recorded AML seeds, on every pair. A pair with no AML member
    may print the public development seeds of the other workloads (the
    Customer 360 byte-compare seed), which the AML spent list can also
    hold."""
    hidden = _hidden_seeds()
    if hidden is None:
        return None
    if any(_is_aml(m.record) for side in sides for m in side.members):
        return hidden
    from lakebench.metrics.corpus_identity import COMPARE_CASES

    public = {seed for case, seed in COMPARE_CASES.items() if case.startswith("C")}
    return frozenset(hidden - public)


def _val(v: Any) -> str:
    if v is None:
        return "not recorded"
    if isinstance(v, Mapping) and "seed_ref" in v:
        ref = v.get("seed_ref")
        return f"seed ref {str(ref)[:12]}" if ref else "withheld"
    if isinstance(v, list | tuple):
        return ", ".join(_val(x) for x in v) if v else "none"
    if isinstance(v, Mapping):
        return json.dumps(v, sort_keys=True, default=str)
    return str(v)


def _repeat_command(side: Side, other: Side, *, keep: list[str] | None = None) -> str:
    """``--repeat 3`` for a batch side; for a continuous side (which cannot
    repeat) the run-it-k-more-times form with the run ids to pass."""
    cfg = _cfg(side)
    if not _continuous(side):
        return f"lakebench run {cfg} --repeat 3"
    have = keep if keep is not None else [m.run_id for m in side.passed]
    k = max(3 - len(have), 1)
    ids = have + ["<new run id>"] * k
    other_ids = ",".join(m.run_id for m in other.passed) or "<other side's run ids>"
    mine = ",".join(ids)
    pair = f"{mine} {other_ids}" if side.label == "A" else f"{other_ids} {mine}"
    times = "time" if k == 1 else "times"
    return (
        f"run `lakebench run {cfg} --continuous` {k} more {times} and pass the run ids: "
        f"`lakebench compare {pair}`"
    )


def _sides(a: Side, b: Side, label: str | None) -> tuple[Side, Side]:
    return (b, a) if label == "B" else (a, b)


def _workload_version_number(v: Any) -> int | None:
    m = re.search(r"-(\d+)$", str(v or ""))
    return int(m.group(1)) if m else None


#: Corpus keys in the order a hint picks them (the corpus id follows from
#: the others, so it is named only when nothing else differs).
_CORPUS_HINT_ORDER = (
    "scale",
    "seed",
    "corpus role",
    "cycles",
    "generator image",
    "generator digest",
    "corpus id v2",
    "corpus id",
)

_CORPUS_SETTING = {
    "scale": "architecture.workload.datagen.scale",
    "seed": "architecture.workload.datagen.seed",
    "corpus role": "architecture.workload.datagen.corpus_role",
    "cycles": "architecture.pipeline.cycles",
}

_WORKLOAD_SETTING = {
    "workload": "architecture.workload.schema",
    "mode": "architecture.pipeline.mode",
}


def _regen(side: Side) -> str:
    """The run that regenerates a side's corpus: a batch run needs
    ``--generate --regenerate``; a continuous run regenerates its own data
    and refuses ``--regenerate``."""
    cfg = _cfg(side)
    if _continuous(side):
        return f"lakebench run {cfg} --continuous"
    return f"lakebench run {cfg} --generate --regenerate"


def _also(corpus: Mapping[str, cmp.Difference], key: str) -> str:
    """`` (also differs: ...)`` naming the other settable corpus keys that
    differ, so one re-run is not followed by another refusal."""
    rest = [
        k
        for k in _CORPUS_HINT_ORDER
        if k in corpus and k != key and k not in ("corpus id", "corpus id v2")
    ]
    return f" (also differs: {', '.join(rest)})" if rest else ""


def _identity_hint(diffs: Mapping[str, list[cmp.Difference]], a: Side, b: Side) -> MissingCondition:
    workload = {d.key: d for d in diffs.get(cmp.WORKLOAD, [])}
    corpus = {d.key: d for d in diffs.get(cmp.CORPUS, [])}
    cfg_b = _cfg(b)
    for key in ("workload", "mode"):
        if key in workload:
            d = workload[key]
            setting = _WORKLOAD_SETTING[key]
            cmd = f"lakebench run {cfg_b}"
            return MissingCondition(
                "the same workload and mode",
                cmd,
                f"different workloads or modes ({_val(d.a)} vs {_val(d.b)}). Missing: the same "
                f"workload and mode. Set `{setting}` in {cfg_b} to `{_val(d.a)}` and run `{cmd}`",
            )
    if "workload version" in workload:
        d = workload["workload version"]
        va, vb = _workload_version_number(d.a), _workload_version_number(d.b)
        if va is not None and vb is not None and va != vb:
            older, newer, who = (a, d.b, "baseline") if va < vb else (b, d.a, "candidate")
            cmd = f"lakebench run {_cfg(older)}"
            return MissingCondition(
                "the same workload version",
                cmd,
                f"{who} predates {newer}; re-run it: `{cmd}`",
            )
        cmd = f"lakebench run {cfg_b}"
        return MissingCondition(
            "the same workload version",
            cmd,
            f"workload versions differ ({_val(d.a)} vs {_val(d.b)}). Missing: the same workload "
            f"version. Re-run both sides on one lakebench version: `{cmd}`",
        )
    if workload:
        d = next(iter(workload.values()))
        cmd = f"lakebench run {cfg_b}"
        return MissingCondition(
            "the same workload",
            cmd,
            f"{d.key} differs ({_val(d.a)} vs {_val(d.b)}). Missing: the same workload. Set the "
            f"same workload settings in both configs and run `{cmd}`",
        )
    for key in _CORPUS_HINT_ORDER:
        if key not in corpus:
            continue
        d = corpus[key]
        also = _also(corpus, key)
        cmd = _regen(b)
        if key in _CORPUS_SETTING:
            setting = _CORPUS_SETTING[key]
            return MissingCondition(
                "the same corpus",
                cmd,
                f"corpus {key} differs ({_val(d.a)} vs {_val(d.b)}){also}. Missing: the same "
                f"corpus. Set `{setting}: {_val(d.a)}` in {cfg_b}, then `{cmd}`",
            )
        if key in ("generator image", "generator digest"):
            return MissingCondition(
                "one generator",
                cmd,
                f"the corpora came from different generators ({_val(d.a)} vs {_val(d.b)}){also}. "
                f"Missing: one generator. Set `images.datagen` in {cfg_b} to A's image and run "
                f"`{cmd}`, or record the re-pin's neutrality in config/datagen_lineage.yaml",
            )
        return MissingCondition(
            "one corpus",
            cmd,
            f"the corpora differ ({key} {_val(d.a)} vs {_val(d.b)}) though their recorded "
            f"settings match. Missing: one corpus. Regenerate B: `{cmd}`",
        )
    if corpus:
        d = next(iter(corpus.values()))
        cmd = _regen(b)
        return MissingCondition(
            "the same corpus",
            cmd,
            f"corpus {d.key} differs ({_val(d.a)} vs {_val(d.b)}). Missing: the same corpus. "
            f"Set it equal in both configs, then `{cmd}`",
        )
    return MissingCondition("the same workload and corpus", None, "the workload or corpus differs")


def _not_established_hint(cause: cmp.Cause, a: Side, b: Side) -> MissingCondition:
    side, _ = _sides(a, b, cause.side)
    cfg = _cfg(side)
    detail = cause.detail or ""
    rec = _first(side)
    exp = rec.get("experiment") or {}
    engine = (exp.get("architecture") or {}).get("query_engine")
    workload = (exp.get("workload") or {}).get("name")
    lb = side.label
    if engine in (None, "", "none") and not detail.startswith("continuous"):
        return MissingCondition(
            "checked results",
            None,
            f"{lb}'s recipe has no query engine, so its results cannot be checked. Missing: "
            "checked results. Use a recipe with trino, spark-thrift or duckdb",
        )
    if detail.startswith("continuous") or _continuous(side):
        if workload == "financial":
            return MissingCondition(
                "checked results",
                None,
                "AML continuous records no end-of-run result check; compare batch runs of "
                "this corpus instead",
            )
        cmd = f"lakebench run {cfg} --continuous"
        return MissingCondition(
            "checked results",
            cmd,
            f"{lb} has no end-of-run result check ({detail}). Missing: checked results. "
            f"Re-run: `{cmd}`",
        )
    if "benchmark" in detail:
        cmd = f"lakebench benchmark {cfg}"
        return MissingCondition(
            "checked results",
            cmd,
            f"{lb} ran no benchmark. Missing: checked results. Run: `{cmd}`",
        )
    cmd = f"lakebench run {cfg}"
    return MissingCondition(
        "checked results",
        cmd,
        f"{lb}'s results were not checked ({detail}). Missing: checked results. Re-run: `{cmd}`",
    )


def _bound_side(cause: cmp.Cause) -> tuple[str, str] | None:
    a = set(cause.a or []) if isinstance(cause.a, list) else set()
    b = set(cause.b or []) if isinstance(cause.b, list) else set()
    if b - a:
        return "B", sorted(b - a)[0]
    if a - b:
        return "A", sorted(a - b)[0]
    return None


def _bound_line(side: Side, kind: str) -> str | None:
    from lakebench.metrics.bounds import binding_caps

    for m in side.passed or side.members:
        try:
            lines = binding_caps(m.record)
        except Exception:  # noqa: BLE001 -- the kind alone still names the bound
            return None
        prefix = kind.split("*")[0].strip()
        for line in lines:
            if prefix and prefix in line:
                return line
    return None


def _table_format(side: Side) -> str | None:
    tf = ((_first(side).get("experiment") or {}).get("architecture") or {}).get("table_format")
    if isinstance(tf, Mapping):
        tf = tf.get("type")
    return str(tf) if tf else None


def _maintenance_setting(side: Side) -> bool | None:
    """``pre_benchmark_maintenance`` as the side's first member ran it."""
    snap = _first(side).get("config_snapshot") or {}
    for block in (
        snap.get("maintenance"),
        (snap.get("experiment_inputs") or {}).get("maintenance_config"),
    ):
        if isinstance(block, Mapping) and isinstance(block.get("pre_benchmark_maintenance"), bool):
            return bool(block["pre_benchmark_maintenance"])
    return None


def _condition_hint(cause: cmp.Cause, a: Side, b: Side) -> MissingCondition:
    key = cause.key or ""
    cfg_b = _cfg(b)
    va, vb = _val(cause.a), _val(cause.b)
    continuous = _continuous(a) or _continuous(b)
    off = (
        "`--skip-maintenance` on both runs"
        if continuous
        else "`architecture.pipeline.pre_benchmark_maintenance: false` in both configs"
    )
    if key == "compaction operation":
        return MissingCondition(
            "the same maintenance",
            None,
            f"compaction differs by engine ({va} vs {vb}). Missing: the same maintenance. No "
            f"setting aligns them; compare with maintenance off: {off}, then "
            "re-run both",
        )
    if key in ("effective maintenance", "maintenance settings"):
        fa, fb = _table_format(a), _table_format(b)
        if key == "effective maintenance" and fa and fb and fa != fb:
            return MissingCondition(
                "the same maintenance",
                None,
                f"effective maintenance differs ({va} vs {vb}): {fa.capitalize()} and "
                f"{fb.capitalize()} run different "
                "maintenance operations. Missing: the same maintenance. No setting aligns "
                "them (with maintenance off the two still record different operations); "
                "compare compositions of one table format",
            )
        want_a, want_b = _maintenance_setting(a), _maintenance_setting(b)
        if not continuous and want_a is not None and want_a != want_b:
            flag = str(want_a).lower()
            return MissingCondition(
                "the same maintenance",
                f"lakebench run {cfg_b}",
                f"{key} differs ({va} vs {vb}). Missing: the same maintenance. Set "
                f"`architecture.pipeline.pre_benchmark_maintenance: {flag}` in {cfg_b} and "
                "re-run",
            )
        return MissingCondition(
            "the same maintenance",
            None,
            f"{key} differs ({va} vs {vb}). Missing: the same maintenance. Set the same "
            f"maintenance in both configs, or compare with maintenance off: {off}, then "
            "re-run both",
        )
    if key == "benchmark iterations":
        cmd = f"lakebench benchmark {cfg_b}"
        return MissingCondition(
            "the same benchmark iterations",
            cmd,
            f"iterations differ ({va} vs {vb}). Missing: the same iterations. Set "
            f"`architecture.benchmark.iterations: {va}` in {cfg_b} and run `{cmd}`",
        )
    if key == "benchmark mode":
        cmd = f"lakebench benchmark {cfg_b}"
        return MissingCondition(
            "the same benchmark mode",
            cmd,
            f"benchmark mode differs ({va} vs {vb}). Missing: the same benchmark mode. Set "
            f"`architecture.benchmark.mode: {va}` in {cfg_b} and run `{cmd}`",
        )
    if key == "Lakebench limits that bound":
        which = _bound_side(cause)
        if which is not None:
            lb, kind = which
            side = b if lb == "B" else a
            line = _bound_line(side, kind)
            return MissingCondition(
                "the same bounds",
                None,
                f"{lb} was bounded by `{kind}`" + (f" ({line})" if line else "") + ". Missing: "
                f"the same bounds. Re-run {lb} when the cluster can grant its request, or set "
                "the same override on both sides",
            )
        return MissingCondition(
            "the same bounds",
            None,
            f"the Lakebench limits that bound differ ({va} vs {vb}). Missing: the same bounds. "
            "Re-run when the cluster can grant both requests, or set the same override on both "
            "sides",
        )
    if key in cmp.OUTCOME_CONDITION_KEYS:
        what = "in-stream rounds" if key == "benchmark rounds" else key
        return MissingCondition(
            f"equal {what}",
            None,
            f"{what} differ ({va} vs {vb}), an outcome of speed. Missing: equal {what}. No "
            "setting makes them equal; the figures are medians over different numbers of "
            "rounds",
        )
    return MissingCondition(
        "the same execution conditions",
        f"lakebench run {cfg_b}",
        f"{key} differs ({va} vs {vb}). Missing: the same execution conditions. Set the same "
        f"value in both configs and run `lakebench run {cfg_b}`",
    )


def _side_not_one_hint(cause: cmp.Cause, a: Side, b: Side) -> MissingCondition:
    side, other = _sides(a, b, cause.side)
    lb = side.label
    key = cause.key or "identity"
    if key in cmp.OUTCOME_CONDITION_KEYS:
        what = "in-stream rounds" if key == "benchmark rounds" else key
        return MissingCondition(
            "one experiment per side",
            None,
            f"{what} differ inside side {lb} ({_val(cause.a)} vs {_val(cause.b)}), an outcome "
            "of speed. Missing: one experiment per side. Compare single runs",
        )
    if cause.a is None and cause.b is None and cause.detail:
        what = f"{key} ({cause.detail})"
    else:
        what = f"{key} ({_val(cause.a)} vs {_val(cause.b)})"
    first = cause.run or (side.passed[0].run_id if side.passed else "?")
    cmd = _repeat_command(side, other, keep=[first])
    differs = (
        f"run {cause.other_run} differs from {cause.run} in"
        if cause.other_run
        else ("its runs differ in")
    )
    return MissingCondition(
        "one experiment per side",
        cmd,
        f"{lb}: {differs} {what}. Missing: one experiment per side. Compare them separately "
        + (f"or re-run: `{cmd}`" if cmd.startswith("lakebench") else f"or {cmd}"),
    )


def _older(a: Side, b: Side) -> Side:
    from lakebench.metrics.storage import _sort_instant

    ta = _sort_instant(_first(a).get("start_time"))
    tb = _sort_instant(_first(b).get("start_time"))
    return a if ta <= tb else b


def _endpoint(side: Side) -> str | None:
    snap = _first(side).get("config_snapshot") or {}
    s3 = snap.get("s3") if isinstance(snap, Mapping) else None
    ep = s3.get("endpoint") if isinstance(s3, Mapping) else None
    return str(ep) if ep else None


def missing_condition(pair: cmp.PairVerdict, a: Side, b: Side) -> MissingCondition:
    """The one condition the pair lacks for its first failed ladder step,
    and the command that supplies it (``command`` None when no command
    does)."""
    c = pair.cause
    kind = c.kind
    if pair.verdict == cmp.LIKE_FOR_LIKE:
        return MissingCondition(
            "the winner rule (not in this release)",
            None,
            "like-for-like; no winner is named in this release",
        )
    side, other = _sides(a, b, c.side)
    cfg = _cfg(side)
    lb = side.label
    if kind == "legacy":
        cmd = f"lakebench run {cfg}"
        return MissingCondition(
            "a v1.6+ record",
            cmd,
            f"{lb}: run {c.run} predates the experiment block. Missing: a v1.6+ record. "
            f"Re-run it: `{cmd}`",
        )
    if kind == "newer_schema":
        return MissingCondition(
            "a record this lakebench reads",
            "pip install --upgrade lakebench",
            f"{lb}: run {c.run} has record schema {c.detail}, newer than this lakebench. "
            "Missing: a record this version reads. Upgrade: `pip install --upgrade lakebench`",
        )
    if kind == "generation":
        v1_side = a if c.a == 1 else b
        v2_side = b if v1_side is a else a
        why = list((_first(v1_side).get("experiment") or {}).get("v2_unavailable") or [])
        because = f" ({', '.join(map(str, why))} not recorded)" if why else ""
        cmd = _regen(v1_side)
        return MissingCondition(
            "one identity version",
            cmd,
            f"{v1_side.label} was recorded with experiment identity v1{because} and "
            f"{v2_side.label} with v2. Missing: one identity version. Re-run "
            f"{v1_side.label} on the current datagen image: `{cmd}`",
        )
    if kind == "mixed_generation":
        return MissingCondition(
            "one identity version per side",
            None,
            f"side {lb} mixes identity v1 and v2 records. Missing: one identity version per "
            "side. Compare the v1 and v2 runs separately",
        )
    if kind == "required_key":
        cmd = f"lakebench run {cfg}"
        return MissingCondition(
            "a complete identity",
            cmd,
            f"{c.run}: {c.key} was not recorded, so it cannot be shown equal. Missing: a "
            f"complete identity. Re-run it: `{cmd}`",
        )
    if kind == "seed_withheld":
        return MissingCondition(
            "a recorded seed",
            None,
            f"{c.run}: its seed is withheld (a protected corpus), so it cannot be shown equal. "
            "Missing: a recorded seed. Compare runs of a development corpus instead",
        )
    if kind == "no_run":
        cmd = f"lakebench run {cfg}"
        return MissingCondition(
            "a passed run", cmd, f"side {lb} has no run. Missing: a passed run. Run: `{cmd}`"
        )
    if kind == "failed":
        cmd = f"lakebench run {cfg}"
        return MissingCondition(
            "a passed run",
            cmd,
            f"{lb}: run {c.run} did not pass ({c.detail}). Missing: a passed run. Fix it and "
            f"re-run: `{cmd}`",
        )
    if kind == "side_not_one":
        return _side_not_one_hint(c, a, b)
    if kind == "identity":
        return _identity_hint(pair.differences, a, b)
    if kind == "corpus_problem":
        cmd = _regen(side)
        return MissingCondition(
            "one corpus",
            cmd,
            f"{lb}'s corpus is not one corpus ({c.detail}). Missing: one corpus. Regenerate it: "
            f"`{cmd}`",
        )
    if kind == "not_established":
        return _not_established_hint(c, a, b)
    if kind == "results":
        chosen = b.passed or b.members
        member = chosen[0] if chosen else None
        rid = member.run_id if member else "?"
        cmd = f"lakebench report --run {rid}"
        from lakebench._constants import DEFAULT_OUTPUT_DIR

        if member is not None and member.path.parent.name.startswith("run-"):
            runs_dir = member.path.parent.parent
            if runs_dir.resolve() != (Path(DEFAULT_OUTPUT_DIR) / "runs").resolve():
                cmd += f" --metrics {runs_dir}"
        return MissingCondition(
            "equal results (invariant 2)",
            cmd,
            f"the runs returned different results ({c.detail}). Missing: equal results "
            "(invariant 2). No re-run makes this pair comparable: the combination that "
            f"disagrees is excluded. Inspect: `{cmd}`",
        )
    if kind == "confounded":
        ep = _endpoint(a)
        target = f"`platform.storage.s3.endpoint: {ep}`" if ep else "A's storage endpoint"
        return MissingCondition(
            "one factor group held equal",
            None,
            f"architecture and system both differ ({c.key}; {c.detail}). Missing: one factor "
            f"group held equal. Run {_cfg(b)} against A's system ({target} on A's cluster) or "
            "A's composition on B's system",
        )
    if kind == "condition":
        return _condition_hint(c, a, b)
    if kind == "pinset":
        older = _older(a, b)
        cmd = f"lakebench deploy {_cfg(older)}"
        return MissingCondition(
            "one dependency set",
            cmd,
            f"same composition, different dependency sets (pinset {str(c.a)[:12]} vs "
            f"{str(c.b)[:12]}). Missing: one dependency set. Redeploy the older side so it "
            f"resolves the current lock: `{cmd}`, then re-run it",
        )
    return MissingCondition("a comparable pair", None, "; ".join(pair.reasons) or pair.verdict)


# ---------------------------------------------------------------------------
# Metrics and assessment
# ---------------------------------------------------------------------------


def _scores(record: Mapping[str, Any]) -> dict[str, float]:
    from lakebench.metrics.metric_registry import ALIASES

    pb = record.get("pipeline_benchmark")
    raw = pb.get("scores") if isinstance(pb, Mapping) else None
    if not isinstance(raw, Mapping):
        raw = record.get("scorecard")
    out: dict[str, float] = {}
    for k, v in (raw or {}).items() if isinstance(raw, Mapping) else ():
        if isinstance(v, bool) or not isinstance(v, int | float):
            continue
        if v != v:  # NaN
            continue
        out[ALIASES.get(str(k), str(k))] = float(v)
    return out


def _summary(values: list[float]) -> dict[str, Any] | None:
    if not values:
        return None
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "values": values,
        "n": len(values),
    }


def _bound_kinds(record: Mapping[str, Any]) -> list[str]:
    from lakebench.metrics.bounds import TRICKLE_LINE_PREFIX

    limits = (record.get("experiment") or {}).get("limits") or {}
    kinds = limits.get("bound_kinds")
    if not isinstance(kinds, list):
        kinds = [b for b in limits.get("bound") or [] if not str(b).startswith(TRICKLE_LINE_PREFIX)]
    return [str(k) for k in kinds]


def _trickle_held(record: Mapping[str, Any]) -> bool:
    from lakebench.metrics.bounds import record_trickle_bound

    try:
        return bool(record_trickle_bound(record))
    except Exception:  # noqa: BLE001 -- unreadable on a continuous run reads as held
        return _mode_of_record(record) == "sustained"


def _mode_of_record(record: Mapping[str, Any]) -> str | None:
    from lakebench.metrics.metric_registry import canonical_mode

    raw = (record.get("pipeline_benchmark") or {}).get("pipeline_mode")
    try:
        return canonical_mode(raw) if isinstance(raw, str) else None
    except ValueError:
        return None


@dataclass
class Assessment:
    outcome: str
    missing: str | None
    hint: str | None
    capped_by: list[str] = field(default_factory=list)


def row_caps(
    metric: str, mode: str | None, bound_kinds: Iterable[str], trickle_held: bool
) -> list[str]:
    """The Lakebench limits that bound *metric* on either side: a bound
    kind it depends on, or the trickle of a continuous run that held
    intake. Every row carries them whatever its assessment."""
    from lakebench.metrics.bounds import BOUND_TRICKLE
    from lakebench.metrics.metric_registry import ModeRequired, capped_by

    try:
        return capped_by(
            metric, list(bound_kinds), mode, extra=[BOUND_TRICKLE] if trickle_held else []
        )
    except (ModeRequired, ValueError):
        # A mode-split key on a record without a mode: every mode's caps.
        return capped_by(
            metric, list(bound_kinds), None, extra=[BOUND_TRICKLE] if trickle_held else []
        )


def assess(
    metric: str,
    mode: str | None,
    pair: cmp.PairVerdict,
    pair_missing: MissingCondition,
    bound_kinds: Iterable[str],
    trickle_held: bool,
) -> Assessment:
    """What may be read from *metric*'s numbers (design section 8, the
    release default: no step past the caps names a winner). The outcome
    follows the design's order; the caps that bound the row are in
    ``capped_by`` whatever the outcome."""
    from lakebench.metrics.metric_registry import ModeRequired, lookup

    caps = row_caps(metric, mode, bound_kinds, trickle_held)

    if pair.verdict in (cmp.NOT_COMPARABLE, cmp.NOT_ESTABLISHED):
        return Assessment(WITHHELD, pair_missing.condition, pair_missing.hint, caps)
    try:
        meta = lookup(metric, mode)
    except (ModeRequired, ValueError):
        meta = None
    if meta is None or not meta.directional:
        return Assessment(NOT_DIRECTIONAL, None, None, caps)
    if meta.group == "ml_loop":
        return Assessment(
            NOT_ASSESSED,
            "both sides ran the ML loop with the same loop definition, Spark version, "
            "executor counts and write-mode pair",
            "ML loop metrics are not assessed in this release",
            caps,
        )
    if pair.verdict == cmp.CONFOUNDED:
        return Assessment(CONFOUNDED_ROW, pair_missing.condition, pair_missing.hint, caps)
    if pair.verdict == cmp.NOT_LIKE_FOR_LIKE:
        return Assessment(NOT_ASSESSED, "a like-for-like pair", pair_missing.hint, caps)
    if caps:
        return Assessment(
            CAPPED,
            "a metric not bounded by a Lakebench cap",
            f"this figure measures {', '.join(caps)}, not the system; no winner is named on it",
            caps,
        )
    return Assessment(NOT_ASSESSED, "the winner rule", "winner rule not in this release", caps)


# ---------------------------------------------------------------------------
# The comparison
# ---------------------------------------------------------------------------


def _side_doc(side: Side) -> dict[str, Any]:
    from lakebench.metrics.bounds import binding_caps
    from lakebench.metrics.experiment import identity_digest, support_of

    first = _first(side)
    try:
        bound = binding_caps(first) if first else []
    except Exception:  # noqa: BLE001 -- shown as recorded
        bound = list(((first.get("experiment") or {}).get("limits") or {}).get("bound") or [])
    try:
        support = support_of(first) if first else None
    except Exception:  # noqa: BLE001
        support = None
    members = []
    for m in side.members:
        try:
            digest = identity_digest(m.record)
        except Exception:  # noqa: BLE001 -- a block the identity cannot read
            digest = None
        members.append(
            {
                "run_id": m.run_id,
                "verdict": m.status,
                "digest": digest,
                "excluded": m.excluded is not None,
                "reason": m.excluded,
            }
        )
    return {
        "refs": list(side.refs),
        "deployment": side.deployment,
        "series": side.series,
        "mode": _mode(side),
        "members": members,
        "non_members": [{"run": r, "reason": why} for r, why in side.non_members],
        "n_attempted": len(side.members) + len(side.non_members),
        "n_passed": len(side.passed),
        "experiment": side.passed[0].record.get("experiment") if side.passed else None,
        "support": support,
        "bound": bound,
    }


def _warnings(a: Side, b: Side) -> list[str]:
    out = list(a.warnings) + list(b.warnings)
    fa, fb = _first(a), _first(b)
    if fa and fb:
        try:
            from lakebench.metrics.maintenance_policy import policy_mismatch, recorded_policy

            problem = policy_mismatch(recorded_policy(fa), recorded_policy(fb))
        except Exception:  # noqa: BLE001 -- a policy that cannot be read is not a warning
            problem = None
        if problem:
            out.append(problem)
    return out


def build_comparison(a: Side, b: Side) -> dict[str, Any]:
    """The cmp2 document for resolved sides *a* and *b*."""
    pair = cmp.pair_verdict(a.ladder_records, b.ladder_records, a.label, b.label)
    missing = missing_condition(pair, a, b)
    hidden = hidden_for(a, b)
    seed_diff = next((d for ds in pair.differences.values() for d in ds if d.key == "seed"), None)
    if (
        seed_diff is not None
        and missing.hint.startswith("corpus seed differs")
        and (
            _seed_out(seed_diff.a, hidden) != seed_diff.a
            or _seed_out(seed_diff.b, hidden) != seed_diff.b
        )
    ):
        # A protected seed is never something to set by hand.
        missing = MissingCondition(
            "the same corpus",
            None,
            "corpus seed differs (a protected seed). Missing: the same corpus. A protected "
            "seed is not set by hand; compare runs of a development corpus instead",
        )
    mode = _mode(a) or _mode(b)
    withheld = pair.verdict in (cmp.NOT_COMPARABLE, cmp.NOT_ESTABLISHED)
    kinds: list[str] = []
    trickle = False
    for side in (a, b):
        for m in side.passed:
            for k in _bound_kinds(m.record):
                if k not in kinds:
                    kinds.append(k)
            trickle = trickle or _trickle_held(m.record)
    per_side: dict[str, dict[str, list[float]]] = {"A": {}, "B": {}}
    for side in (a, b):
        for m in side.passed:
            for k, v in _scores(m.record).items():
                per_side[side.label].setdefault(k, []).append(v)
    from lakebench.metrics.metric_registry import ModeRequired, lookup

    rows = []
    for key in sorted(set(per_side["A"]) | set(per_side["B"])):
        sa = _summary(per_side["A"].get(key, []))
        sb = _summary(per_side["B"].get(key, []))
        delta = None
        if not withheld and sa and sb and sa["median"] != 0:
            delta = round((sb["median"] - sa["median"]) / abs(sa["median"]) * 100, 2)
        try:
            meta = lookup(key, mode)
        except (ModeRequired, ValueError):
            meta = None
        asm = assess(key, mode, pair, missing, kinds, trickle)
        rows.append(
            {
                "metric": key,
                "unit": meta.unit if meta else None,
                "direction": meta.direction if meta else None,
                "a": sa,
                "b": sb,
                "delta_pct": delta,
                "assessment": asm.outcome,
                "winner": None,
                "missing": {"condition": asm.missing, "command": None},
                "hint": asm.hint,
                "capped_by": asm.capped_by,
            }
        )
    doc = {
        "schema": SCHEMA,
        "verdict": pair.verdict,
        "exit_code": pair.code,
        "step": pair.step,
        "attribution": pair.attribution,
        "system": pair.system,
        "missing": missing.to_dict(),
        "cause": pair.to_dict()["cause"],
        "reasons": list(pair.reasons),
        "notes": list(pair.notes),
        "winner_rule": WINNER_RULE,
        "sides": {"a": _side_doc(a), "b": _side_doc(b)},
        "groups": {
            g: [{"key": d.key, "a": d.a, "b": d.b} for d in ds]
            for g, ds in pair.differences.items()
            if ds
        },
        "warnings": _warnings(a, b),
        "metrics": rows,
    }
    # One pass over the finished document, so no field can carry a
    # protected seed past it.
    return redact(json.loads(json.dumps(doc, default=str)), hidden)


def to_json(doc: Mapping[str, Any]) -> str:
    return json.dumps(doc, indent=2, default=str)


CSV_COLUMNS = (
    "metric",
    "a_median",
    "a_min",
    "a_max",
    "b_median",
    "b_min",
    "b_max",
    "delta_pct",
    "verdict",
    "attribution",
    "n_a",
    "n_b",
    "assessment",
    "bound_by",
)


def to_csv(doc: Mapping[str, Any]) -> str:
    buf = io.StringIO()
    missing = doc.get("missing") or {}
    sides = doc.get("sides") or {}
    head = [
        ("schema", doc.get("schema")),
        ("verdict", doc.get("verdict")),
        ("exit_code", doc.get("exit_code")),
        ("attribution", doc.get("attribution") or ""),
        ("missing", missing.get("condition") or ""),
        ("command", missing.get("command") or ""),
        ("n_a", (sides.get("a") or {}).get("n_passed")),
        ("n_b", (sides.get("b") or {}).get("n_passed")),
    ]
    for k, v in head:
        buf.write(f"# {k}: {v}\n")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(CSV_COLUMNS)
    for r in doc.get("metrics") or []:
        sa, sb = r.get("a") or {}, r.get("b") or {}
        w.writerow(
            [
                r["metric"],
                sa.get("median"),
                sa.get("min"),
                sa.get("max"),
                sb.get("median"),
                sb.get("min"),
                sb.get("max"),
                r.get("delta_pct"),
                doc.get("verdict"),
                doc.get("attribution") or "",
                sa.get("n", 0),
                sb.get("n", 0),
                r.get("assessment"),
                ";".join(r.get("capped_by") or []),
            ]
        )
    return buf.getvalue()
