"""Corpus id v2: the corpus as the generator wrote it (SPEC v1.7 EVD-6).

``corpus.id`` (v1) hashes the config's datagen block and is never
recomputed, so stored ids do not move. Corpus id v2 is taken from what the
generator itself received: each datagen node writes a marker
``<scope>_corpus/c{cycle:03}-node-{node:04}.json`` whose
``corpus_args_sha256`` hashes the resolved generator arguments (ch05
section 3.1 is the one definition), and ``lakebench.corpus_digest`` folds
the per-cycle hashes into ``corpus_series_sha256``. A config edited after
generation therefore changes nothing about the id.

The flow, per run:

1. ``observe_corpus(cfg, s3)`` runs once, immediately before ``save_run``
   (the ER-9h call site). It lists the datagen scope once, reads the
   markers and ``series.json`` from that listing, and returns a plain dict
   that the run persists as ``config_snapshot.experiment_inputs.
   corpus_observation``. It never raises: a failed read is recorded as an
   ``error``.
2. ``build_experiment`` calls ``corpus_v2_fields`` with that persisted dict
   (and, for repetitions 2 to N of a ``--repeat`` series, the inherited
   block CC-30 persists as ``experiment_inputs.inherited_corpus``). Nothing
   here reads S3 at build time, so a later build sees what the run saw.

``corpus_v2_fields`` applies ch03 section 6 "Series corpus identity" in
order: the inherited block is accepted only when both listing digests are
observed and equal (else the corpus problem "bronze changed during this
repetition" or "not observed"); node markers win over the inherited block;
the inherited block is copied verbatim only when there are no markers;
otherwise ``id_v2`` is None with the reason in ``id_v2_unavailable``.

Lineage is the image digest that generated the corpus, read from
``series.json``'s ``generation.image_digest`` (written next to the bytes by
every generate) and mapped through ``config/datagen_lineage.yaml`` to the
first digest of an output-neutral re-pin chain. It is resolved once, by
``observe_corpus``, and persisted with the observation, so the id never
depends on the table installed when a record is re-read. A series.json that
disagrees with the markers (seed, cycle count, scale, file size, customer
id space) lends no lineage. The datagen fleet record is never the source:
it is a per-namespace sidecar that ``run`` loads whether or not it
generated (``cli/_run.py:_load_latest_datagen_fleet``); when it names
another image than series.json, the lineage is declared (the safe side).
No observed digest gives ``declared:<image tag>``, which never equals an
observed lineage.

``corpus.observed`` keeps its v1 meaning (the datagen pods' seed and scale
were observed); whether the generator image was observed is
``corpus.lineage_observed``.

Known limits:

* The id binds the generator arguments and lineage, not the objects. An
  object deleted or added under the scope by hand leaves every marker in
  place, so the id is unchanged; ``bronze_listing_sha256`` in the
  observation changes, and DAT-4 (CD-9, v1.8) is where object completeness
  is checked against the markers.
* Two images under one mutable tag, both without an observed digest, read
  as one ``declared:<tag>`` lineage; ``lineage_observed`` false says so.
* A record without an observation (every v1.6 record, and v1.7 records
  written before the ER-9h call site) gets no v2 fields at all, so a
  re-saved v1.6 block keeps its shape.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakebench.corpus_digest import (
    MarkerSet,
    distinct,
    is_sha256_hex,
    read_corpus_markers,
)

__all__ = [
    "CORPUS_ID_VERSION",
    "CorpusV2",
    "LineageError",
    "LineageRow",
    "MarkerSet",
    "check_lineage_evidence",
    "corpus_id_v2",
    "corpus_v2_fields",
    "inherited_corpus_from",
    "load_lineage",
    "marker_problems",
    "observe_corpus",
    "read_corpus_markers",
    "resolve_lineage",
    "validate_compare_file",
]

CORPUS_ID_VERSION = 2

#: ``experiment_inputs.inherited_corpus`` format (``inherited_corpus_from``).
INHERITED_FORMAT = 1

LINEAGE_FILE = Path(__file__).resolve().parents[1] / "config" / "datagen_lineage.yaml"

#: Where CD-8's compare result for an image lives, relative to the repo root
#: (ch05 section 4.1). Not shipped in the wheel: only the CI test opens it.
EVIDENCE_PATTERN = "tests/fixtures/datagen_reference/compare-{digest12}.json"

#: The five byte-compare cases and the public development seed each runs on
#: (ch05 section 4.1: 43 financial, 42 Customer 360; never a held-out seed).
COMPARE_CASES: dict[str, int] = {"F0": 43, "F1": 43, "F2": 43, "C0": 42, "C2": 42}

NOT_OBSERVED = "corpus not observed at run end (no corpus observation in this record)"
NO_MARKER = "corpus has no generator marker hash (datagen image before DAT-3)"
UNREADABLE_MARKER = "corpus markers are in a format this Lakebench does not read"
NOT_ONE_CORPUS = "corpus markers do not describe one complete corpus (see corpus.problems)"

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHORT_COMMIT = re.compile(r"^[0-9a-f]{7,40}$")


def _short_hash(obj: Any) -> str:
    from lakebench.metrics.experiment import _short_hash as short

    return short(obj)


def _distinct(values: Sequence[Any]) -> list[Any]:
    return distinct(values)


# ---------------------------------------------------------------------------
# The run-end observation (S3, once per run)
# ---------------------------------------------------------------------------


def observe_corpus(cfg: Any, s3: Any, *, lineage_path: Path | None = None) -> dict[str, Any]:
    """The run-end corpus observation (ch03 section 0.1), as persisted in
    ``experiment_inputs.corpus_observation``: the marker set, series.json,
    the listing digest and the lineage resolved now, against the lineage
    table this Lakebench ships. Resolving it here and persisting it keeps
    id v2 a function of the record: a later build, or a later lineage row,
    never moves a stored id. *s3* is an ``S3Client``. Never raises."""
    now = datetime.now(timezone.utc).isoformat()
    config_image = None
    try:
        from lakebench.deploy.datagen import bronze_datagen_prefix

        config_image = cfg.images.datagen
        bucket = cfg.platform.storage.s3.buckets.bronze
        prefix = bronze_datagen_prefix(cfg)
        client = s3.raw_client
        if client is None:
            raise RuntimeError("the S3 client did not initialise")
        ms = read_corpus_markers(client, bucket, prefix)
    except Exception as e:  # noqa: BLE001 -- recorded, never raised from a save
        ms = MarkerSet(error=f"corpus observation failed: {_reason(e)}")
    try:
        markers = ms.to_dict()
    except Exception as e:  # noqa: BLE001
        markers = MarkerSet(error=f"corpus markers unusable: {_reason(e)}").to_dict()
    obs: dict[str, Any] = {
        "format": 1,
        "markers": markers,
        "series": ms.series,
        "bronze_listing_sha256": ms.bronze_listing_sha256,
        "observed_at": now,
    }
    try:
        obs["lineage"] = resolve_lineage(obs, config_image=config_image, path=lineage_path)
    except Exception as e:  # noqa: BLE001
        obs["lineage"] = {
            "value": f"declared:{config_image}",
            "observed": False,
            "problems": [f"lineage could not be resolved: {_reason(e)}"],
            "notes": [],
        }
    return obs


def _reason(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:200]}"


# ---------------------------------------------------------------------------
# Problems and id (from the persisted observation only)
# ---------------------------------------------------------------------------


def _ints(values: Sequence[Any]) -> list[int]:
    return [v for v in values if isinstance(v, int) and not isinstance(v, bool)]


def marker_problems(markers: Mapping[str, Any], model_version: str | None) -> list[str]:
    """Why the persisted marker set is not one complete corpus written by
    one generator given one set of arguments. Each is a corpus problem
    (``corpus.problems``), which compare reads as NOT COMPARABLE."""
    if markers.get("error"):
        return []
    problems = list(markers.get("problems") or [])
    cycles = list(markers.get("cycles") or [])
    if not cycles:
        return problems
    declared = _distinct([v for c in cycles for v in c.get("cycles") or []])
    if len(declared) != 1 or len(_ints(declared)) != 1 or declared[0] < 1:
        problems.append(f"the corpus markers disagree on the cycle count ({declared})")
    else:
        n = declared[0]
        present = {c["cycle"] for c in cycles}
        missing = sorted(set(range(n)) - present)
        extra = sorted(present - set(range(n)))
        if missing:
            problems.append(f"cycles {missing} have no corpus marker")
        if extra:
            problems.append(f"corpus markers for cycles {extra} beyond the corpus's {n} cycles")
    all_hashes = [h for c in cycles for h in c.get("corpus_args_sha256") or []]
    any_hash = any(h is not None for h in all_hashes)
    for c in cycles:
        cyc = c["cycle"]
        totals = c.get("total_nodes") or []
        if len(totals) != 1 or len(_ints(totals)) != 1 or totals[0] < 1:
            problems.append(f"cycle {cyc} corpus markers disagree on the node count ({totals})")
        else:
            found = set(c.get("nodes_found") or [])
            missing = sorted(set(range(totals[0])) - found)
            extra = sorted(found - set(range(totals[0])))
            if missing:
                problems.append(f"cycle {cyc} nodes {missing} have no corpus marker")
            if extra:
                problems.append(
                    f"cycle {cyc} has corpus markers for nodes {extra} beyond its {totals[0]} nodes"
                )
        hashes = c.get("corpus_args_sha256") or []
        if any_hash and (len(hashes) != 1 or not is_sha256_hex(hashes[0])):
            problems.append(
                f"the corpus was written by generators given different arguments (cycle {cyc})"
            )
    versions = _distinct([v for c in cycles for v in c.get("model_version") or []])
    if len(versions) > 1:
        problems.append(f"corpus markers report different generator model versions ({versions})")
    elif model_version is not None and versions and versions[0] != model_version:
        problems.append(
            f"the corpus was generated by model {versions[0]!r}, but this workload "
            f"version expects {model_version!r}"
        )
    commits = _distinct([v for c in cycles for v in c.get("build_commit") or []])
    if len(commits) > 1:
        problems.append(f"corpus nodes report different generator builds ({commits})")
    return problems


def corpus_id_v2(
    obs: Mapping[str, Any] | None, model_version: str | None, lineage: str
) -> tuple[str | None, str | None]:
    """``(id, None)``, or ``(None, why it is unavailable)``.

    id = ``_short_hash({"args": corpus_series_sha256, "model_version": ...,
    "lineage": ..., "cycles": N})``, with ``cycles`` present only when the
    markers' cycle count is above 1 (K7)."""
    if not obs:
        return None, NOT_OBSERVED
    markers = obs.get("markers") or {}
    if markers.get("error"):
        return None, f"corpus markers could not be read: {markers['error']}"
    cycles = list(markers.get("cycles") or [])
    hashes = [h for c in cycles for h in c.get("corpus_args_sha256") or []]
    if not cycles:
        return None, UNREADABLE_MARKER if markers.get("unreadable_format") else NO_MARKER
    if all(h is None for h in hashes):
        return None, NO_MARKER
    series = markers.get("corpus_series_sha256")
    if not is_sha256_hex(series):
        return None, NOT_ONE_CORPUS
    n = (cycles[0].get("cycles") or [1])[0]
    body: dict[str, Any] = {"args": series, "model_version": model_version, "lineage": lineage}
    if isinstance(n, int) and n > 1:
        body["cycles"] = n
    return _short_hash(body), None


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


class LineageError(ValueError):
    """The lineage table or a compare file breaks the evidence rules."""


@dataclass(frozen=True)
class LineageRow:
    digest: str
    canonical: str
    evidence: str | None = None
    evidence_sha256: str | None = None
    build_commit: str | None = None


_ROW_KEYS = {"digest", "canonical", "evidence", "evidence_sha256", "build_commit"}


def load_lineage(path: Path | None = None) -> dict[str, LineageRow]:
    """Read ``config/datagen_lineage.yaml``: ``{digest, canonical,
    evidence, evidence_sha256, build_commit?}`` rows.

    A row whose ``digest`` equals its ``canonical`` is a root and needs no
    evidence. Any other row must name its compare file at
    ``EVIDENCE_PATTERN`` (first 12 hex of ``digest``) and pin that file's
    sha256, and its ``canonical`` must itself be a root. ``build_commit``,
    when given, is the 40-hex commit the image was built from. The loader
    never opens the compare file, because the wheel does not ship
    ``tests/``: ``check_lineage_evidence`` (run by the unit suite on every
    commit) proves each pinned file exists, hashes to the pin and passes
    ``validate_compare_file``. So a source checkout and an installed
    Lakebench read one table and give one id. Raises LineageError.
    """
    import yaml

    p = path or LINEAGE_FILE
    try:
        data = yaml.safe_load(p.read_text())
    except (OSError, yaml.YAMLError) as e:
        raise LineageError(f"cannot read {p.name}: {e}") from e
    rows = data.get("lineage") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise LineageError(f"{p.name} has no 'lineage' list")
    table: dict[str, LineageRow] = {}
    for i, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise LineageError(f"{p.name} row {i} is not a mapping")
        unknown = set(raw) - _ROW_KEYS
        if unknown:
            raise LineageError(f"{p.name} row {i} has unknown keys {sorted(unknown)}")
        for name in ("digest", "canonical"):
            if not isinstance(raw.get(name), str) or not _DIGEST.match(raw[name]):
                raise LineageError(f"{p.name} row {i}: {name} is not sha256:<64 hex>")
        commit = raw.get("build_commit")
        if commit is not None and not (isinstance(commit, str) and _COMMIT.match(commit)):
            raise LineageError(f"{p.name} row {i}: build_commit must be a quoted 40-hex commit")
        row = LineageRow(
            digest=raw["digest"],
            canonical=raw["canonical"],
            evidence=raw.get("evidence"),
            evidence_sha256=raw.get("evidence_sha256"),
            build_commit=commit,
        )
        if row.digest in table:
            raise LineageError(f"{p.name} lists {row.digest} twice")
        if row.digest != row.canonical:
            want = EVIDENCE_PATTERN.format(digest12=row.digest.split(":", 1)[1][:12])
            if row.evidence != want:
                raise LineageError(f"{p.name} row {i}: evidence must be {want}")
            if not is_sha256_hex(row.evidence_sha256):
                raise LineageError(f"{p.name} row {i}: evidence_sha256 is not quoted 64 hex")
        elif row.evidence is not None or row.evidence_sha256 is not None:
            raise LineageError(f"{p.name} row {i}: a root row carries no evidence")
        table[row.digest] = row
    for row in table.values():
        root = table.get(row.canonical)
        if root is None or root.canonical != root.digest:
            raise LineageError(
                f"{p.name}: {row.digest} maps to {row.canonical}, which is not a root row"
            )
    return table


def _dev_seed_refs(case: str) -> set[str]:
    """The accepted ``seed_ref`` spellings of a compare case's development
    seed: the plaintext seed, and ``datagen_seed.seed_ref`` of it when this
    Lakebench has that function (the financial form is a salted hash)."""
    seed = COMPARE_CASES[case]
    refs = {str(seed)}
    try:
        from lakebench.config import datagen_seed

        ref = getattr(datagen_seed, "seed_ref", None)
        if ref is not None:
            refs.add(str(ref("financial" if case.startswith("F") else "customer360", seed)))
    except Exception:  # noqa: BLE001 -- the plaintext form still applies
        pass
    return refs


def validate_compare_file(data: Any, digest: str, canonical: str) -> list[str]:
    """Why *data* (a parsed ``compare-<digest12>.json``, ch05 section 4.1)
    is not evidence that *digest* writes the same bytes as *canonical*:
    format 1, ``image_a`` the canonical and ``image_b`` the digest, exactly
    the cases F0, F1, C0, F2, C2 once each, and per case ``equal: true``,
    ``excluded == ["_corpus/"]``, a non-empty ``argv_canonical``, a
    positive object count, equal sha256 manifests, the file's digests, and
    the case's development seed."""
    if not isinstance(data, dict):
        return ["the compare file is not a JSON object"]
    errors = []
    if data.get("format") != 1:
        errors.append(f"format {data.get('format')!r}, expected 1")
    if data.get("image_a") != canonical:
        errors.append(f"image_a {data.get('image_a')!r} is not the row's canonical {canonical}")
    if data.get("image_b") != digest:
        errors.append(f"image_b {data.get('image_b')!r} is not the row's digest {digest}")
    cases = data.get("cases")
    if not isinstance(cases, list):
        return [*errors, "no cases list"]
    names = [c.get("case") if isinstance(c, dict) else None for c in cases]
    if sorted(map(str, names)) != sorted(COMPARE_CASES):
        errors.append(f"cases {names}, expected exactly {sorted(COMPARE_CASES)}")
    for c in cases:
        if not isinstance(c, dict) or c.get("case") not in COMPARE_CASES:
            continue
        name = c["case"]
        if c.get("equal") is not True:
            errors.append(f"case {name} is not equal: true")
        if c.get("excluded") != ["_corpus/"]:
            errors.append(f"case {name} must exclude exactly ['_corpus/']")
        argv = c.get("argv_canonical")
        if not isinstance(argv, list) or not argv:
            errors.append(f"case {name} has no argv_canonical")
        objects = c.get("objects")
        if not isinstance(objects, int) or isinstance(objects, bool) or objects < 1:
            errors.append(f"case {name} compared no objects")
        if c.get("digest_a") != data.get("image_a") or c.get("digest_b") != data.get("image_b"):
            errors.append(f"case {name} digests differ from the file's images")
        if str(c.get("seed_ref")) not in _dev_seed_refs(name):
            errors.append(f"case {name} is not on development seed {COMPARE_CASES[name]}")
        if not is_sha256_hex(c.get("sha256_a")) or c.get("sha256_a") != c.get("sha256_b"):
            errors.append(f"case {name} manifests differ or are not sha256")
    return errors


def check_lineage_evidence(table: Mapping[str, LineageRow], root: Path) -> list[str]:
    """For every non-root row: the pinned compare file exists under *root*
    (the repo), hashes to ``evidence_sha256`` and passes
    ``validate_compare_file``. The unit suite runs this over the tracked
    table, which is what lets ``load_lineage`` trust the pin at run time."""
    errors = []
    for row in table.values():
        if row.digest == row.canonical:
            continue
        p = root / str(row.evidence)
        try:
            raw = p.read_bytes()
        except OSError as e:
            errors.append(f"{row.digest}: evidence {row.evidence} unreadable ({e})")
            continue
        if hashlib.sha256(raw).hexdigest() != row.evidence_sha256:
            errors.append(f"{row.digest}: evidence {row.evidence} does not hash to its pin")
            continue
        try:
            data = json.loads(raw)
        except ValueError as e:
            errors.append(f"{row.digest}: evidence is not JSON ({e})")
            continue
        errors += [
            f"{row.digest}: {e}" for e in validate_compare_file(data, row.digest, row.canonical)
        ]
    return errors


def resolve_lineage(
    obs: Mapping[str, Any], *, config_image: str | None, path: Path | None = None
) -> dict[str, Any]:
    """The lineage of the corpus in *obs* (ch03 section 6 "Lineage"), as
    persisted in the observation: ``{value, observed, digest, tag,
    problems, notes}``.

    The digest is ``series.json``'s ``generation.image_digest``. The tag in
    a declared lineage is ``generation.image`` when series.json exists (the
    image that wrote the corpus, as configured then), else *config_image*,
    so an ``images.datagen`` edit after generation does not move the id.
    An unreadable lineage table is a corpus problem (it is tracked and
    tested, so this means a broken install), and the lineage is declared.
    """
    series = obs.get("series") if isinstance(obs.get("series"), Mapping) else None
    gen = (series or {}).get("generation")
    gen = gen if isinstance(gen, Mapping) else {}
    tag = gen.get("image") or config_image
    markers = obs.get("markers") or {}
    commits = _distinct(
        [v for c in markers.get("cycles") or [] for v in c.get("build_commit") or []]
    )

    def declared(note: str, problems: Sequence[str] = ()) -> dict[str, Any]:
        return {
            "value": f"declared:{tag}",
            "observed": False,
            "digest": None,
            "tag": tag,
            "problems": list(problems),
            "notes": [note],
        }

    try:
        table = load_lineage(path)
    except LineageError as e:
        return declared("lineage table unreadable; lineage is declared", [str(e)])
    if series is None:
        return declared("no series marker (series.json) next to the corpus; lineage is declared")
    if markers.get("series_check"):
        return declared(f"{markers['series_check']}; lineage is declared")
    digest = gen.get("image_digest")
    if not digest:
        reason = gen.get("image_digest_reason") or "series.json records no image digest"
        return declared(f"{reason}; lineage is declared")
    if not isinstance(digest, str) or not _DIGEST.match(digest):
        return declared(f"series.json image digest {digest!r} is not sha256:<64 hex>")
    if len(commits) > 1:
        return declared("corpus nodes report different generator builds; lineage is declared")
    value = digest
    row = table.get(digest)
    if row is not None:
        if row.build_commit:
            seen = commits[0] if commits else None
            if not (isinstance(seen, str) and _SHORT_COMMIT.match(seen)):
                return declared(
                    f"the corpus markers name no build commit to check against "
                    f"{row.build_commit[:12]}; lineage is declared"
                )
            if not (row.build_commit.startswith(seen) or seen.startswith(row.build_commit)):
                return declared(
                    "lineage is declared",
                    [
                        f"image {digest[:19]} was built from {row.build_commit[:12]}, but the "
                        f"corpus markers name build {seen[:12]}"
                    ],
                )
        value = row.canonical
    return {
        "value": value,
        "observed": True,
        "digest": digest,
        "tag": tag,
        "problems": [],
        "notes": [],
    }


def _utc(value: Any) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()


def _lineage_at_build(
    obs: Mapping[str, Any],
    declared_image: str | None,
    fleet: Mapping[str, Any] | None,
    fleet_digest: str | None,
) -> dict[str, Any]:
    """The persisted lineage, checked for shape, and downgraded to declared
    when a datagen fleet record from this corpus's generate (written no
    earlier than the first marker) names another image than series.json.
    An older fleet record is a stale per-namespace sidecar and is ignored."""
    raw = obs.get("lineage")
    series = obs.get("series") if isinstance(obs.get("series"), Mapping) else {}
    gen = (series or {}).get("generation")
    fallback_tag = (gen.get("image") if isinstance(gen, Mapping) else None) or declared_image

    def declared(tag: Any, note: str, problems: Sequence[str] = ()) -> dict[str, Any]:
        return {
            "value": f"declared:{tag or '<no image tag>'}",
            "observed": False,
            "problems": list(problems),
            "notes": [note],
        }

    if not isinstance(raw, Mapping):
        return declared(fallback_tag, "the observation carries no resolved lineage")
    value, observed = raw.get("value"), raw.get("observed") is True
    problems = [str(p) for p in raw.get("problems") or []]
    notes = [str(n) for n in raw.get("notes") or []]
    tag = raw.get("tag") or fallback_tag
    if observed and not (isinstance(value, str) and _DIGEST.match(value)):
        return declared(tag, "the persisted lineage is not a digest", problems)
    if not observed and not (isinstance(value, str) and value.startswith("declared:")):
        return declared(tag, "the persisted lineage is not a declared lineage", problems)
    out: dict[str, Any] = {
        "value": value,
        "observed": observed,
        "problems": problems,
        "notes": notes,
    }
    digest = raw.get("digest")
    if observed and fleet_digest and digest and fleet_digest != digest:
        first = _utc((obs.get("markers") or {}).get("completed_first"))
        written = _utc((fleet or {}).get("written_at"))
        if first is not None and written is not None and written + 60.0 < first:
            notes.append(
                f"the datagen fleet record ({fleet_digest[:19]}) predates this corpus; ignored"
            )
        else:
            return declared(
                tag,
                f"the datagen fleet record names {fleet_digest[:19]} but series.json "
                f"{str(digest)[:19]}; lineage is declared",
                problems,
            )
    return out


# ---------------------------------------------------------------------------
# The corpus block's v2 fields
# ---------------------------------------------------------------------------


@dataclass
class CorpusV2:
    """What ``build_experiment`` applies to ``experiment.corpus``."""

    fields: dict[str, Any] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)
    #: Case (b): the inherited block, which replaces the corpus block whole.
    replace: dict[str, Any] | None = None


def inherited_corpus_from(record: Mapping[str, Any]) -> dict[str, Any]:
    """The ``experiment_inputs.inherited_corpus`` a ``--repeat`` series
    (CC-30) persists for repetitions 2 to N, built from repetition 1's saved
    metrics.json dict: its whole ``experiment.corpus`` block, its run id and
    D1, the ``bronze_listing_sha256`` of the same record's run-end
    observation. Taking both from one saved record ties the block to the
    digest that verified it."""
    exp = record.get("experiment") if isinstance(record.get("experiment"), Mapping) else {}
    inputs = (record.get("config_snapshot") or {}).get("experiment_inputs") or {}
    obs = inputs.get("corpus_observation") if isinstance(inputs, Mapping) else None
    return {
        "format": INHERITED_FORMAT,
        "corpus": copy.deepcopy((exp or {}).get("corpus")),
        "from_run_id": record.get("run_id"),
        "bronze_listing_sha256": (obs or {}).get("bronze_listing_sha256")
        if isinstance(obs, Mapping)
        else None,
    }


#: Declared corpus keys compared with ``series.json``'s ``generation`` for
#: the display warning. CD-18's ``--skip-generate`` rule 2 refuses a run
#: whose config disagrees with the whole generation block; this warning
#: covers the keys the experiment block declares, for the runs rule 2 does
#: not see (a single cycle with no series.json is not compared at all).
_DECLARED_VS_SERIES = (
    ("scale", "scale"),
    ("generator_image", "image"),
    ("timestamp_start", "timestamp_start"),
    ("timestamp_end", "timestamp_end"),
)


def _declared_differs(declared: Mapping[str, Any], series: Any) -> list[str]:
    gen = series.get("generation") if isinstance(series, Mapping) else None
    if not isinstance(gen, Mapping):
        return []
    out = []
    for dkey, skey in _DECLARED_VS_SERIES:
        a, b = declared.get(dkey), gen.get(skey)
        if a is None or b is None:
            continue
        if dkey == "scale":
            try:
                if abs(float(a) - float(b)) <= 1e-6:
                    continue
            except (TypeError, ValueError, ArithmeticError):
                pass
        elif str(a) == str(b):
            continue
        out.append(dkey)
    return out


def _inheritance(inherited: Mapping[str, Any], digest: Any) -> tuple[bool, list[str]]:
    """Step 3: the inherited block is usable only when it is in the contract
    shape and both listing digests are observed and equal."""
    if inherited.get("format") != INHERITED_FORMAT or not isinstance(
        inherited.get("corpus"), Mapping
    ):
        return False, [
            "the inherited corpus is not in the series contract shape "
            "(build it with corpus_identity.inherited_corpus_from)"
        ]
    d1 = inherited.get("bronze_listing_sha256")
    if not is_sha256_hex(d1):
        return False, [
            "repetition 1's corpus was not observed (no bronze listing digest); "
            "this repetition does not inherit it"
        ]
    if not is_sha256_hex(digest):
        return False, [
            "bronze was not observed before this repetition was saved; "
            "it does not inherit repetition 1's corpus"
        ]
    if digest != d1:
        return False, ["bronze changed during this repetition"]
    return True, []


def corpus_v2_fields(
    declared: Mapping[str, Any],
    *,
    obs: Mapping[str, Any] | None,
    inherited: Mapping[str, Any] | None,
    model_version: str | None,
    fleet_digest: str | None = None,
    fleet: Mapping[str, Any] | None = None,
) -> CorpusV2 | None:
    """ch03 section 6 "Series corpus identity" steps 3 and 4, from persisted
    inputs only.

    *declared* is the config's corpus block (``experiment_inputs.corpus``);
    *obs* the persisted ``corpus_observation``; *inherited* the persisted
    ``inherited_corpus`` of a series repetition 2 to N
    (``inherited_corpus_from``); *fleet_digest* this record's datagen fleet
    digest, used only to downgrade a lineage it contradicts. Returns None
    when neither *obs* nor *inherited* is present (a record from before the
    observation), so such a block gains no v2 field.
    """
    if not obs and not inherited:
        return None
    out = CorpusV2()
    digest = (obs or {}).get("bronze_listing_sha256")
    inherit_ok, contract_ok = False, False
    if inherited:
        inherit_ok, problems = _inheritance(inherited, digest)
        out.problems += problems
        contract_ok = inherit_ok or (
            inherited.get("format") == INHERITED_FORMAT
            and isinstance(inherited.get("corpus"), Mapping)
        )

    markers = (obs or {}).get("markers") or {}
    has_markers = bool(markers.get("cycles")) and not markers.get("error")
    if inherit_ok and not has_markers:
        assert inherited is not None
        block = copy.deepcopy(dict(inherited["corpus"]))
        block["inherited_from"] = inherited.get("from_run_id")
        block["bronze_listing_sha256"] = digest
        out.replace = block
        return out

    if obs:
        out.problems += marker_problems(markers, model_version)
    lineage = _lineage_at_build(obs or {}, declared.get("generator_image"), fleet, fleet_digest)
    if has_markers:
        out.problems += lineage["problems"]
    id_v2, unavailable = corpus_id_v2(obs, model_version, lineage["value"])
    if inherited and contract_ok and has_markers:
        if id_v2 is None or inherited["corpus"].get("id_v2") != id_v2:
            out.problems.append("series corpus id differs from repetition 1")

    f = out.fields
    f["id_v2"] = id_v2
    if id_v2 is not None:
        f["id_version"] = CORPUS_ID_VERSION
        f["args_sha256"] = markers.get("corpus_series_sha256")
    else:
        f["id_v2_unavailable"] = unavailable
    if has_markers:
        f["lineage"] = lineage["value"]
        f["lineage_observed"] = lineage["observed"]
        if lineage["notes"]:
            f["lineage_notes"] = list(lineage["notes"])
    f["declared"] = {k: v for k, v in declared.items() if k != "id"}
    differs = _declared_differs(declared, (obs or {}).get("series"))
    if differs:
        f["warnings"] = [
            "config datagen settings differ from the corpus this run read "
            f"({', '.join(differs)}); the corpus id follows the corpus"
        ]
    return out
