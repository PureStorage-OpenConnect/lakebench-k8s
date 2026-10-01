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
first digest of an output-neutral re-pin chain. The datagen fleet record is
not used for lineage: it is a per-namespace sidecar that ``run`` loads
whether or not it generated (``cli/_run.py:_load_latest_datagen_fleet``),
so a stale sidecar would name an image that did not write these bytes. No
observed digest gives ``declared:<image tag>``, which never equals an
observed lineage.

Known limits:

* The id binds the generator arguments and lineage, not the objects. An
  object deleted or added under the scope by hand leaves every marker in
  place, so the id is unchanged; ``bronze_listing_sha256`` in the
  observation changes, and DAT-4 (CD-9, v1.8) is where object completeness
  is checked against the markers.
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
    MARKER_DIR,
    MARKER_NAME,
    SERIES_NAME,
    corpus_series_sha256,
    datagen_scope,
    is_sha256_hex,
    list_scope,
    listing_sha256,
)

CORPUS_ID_VERSION = 2

#: Marker file format this reader understands (ch05 section 3.1).
MARKER_FORMAT = 1

#: More markers than this is not a datagen corpus; reading stops.
MAX_MARKERS = 10_000

LINEAGE_FILE = Path(__file__).resolve().parents[1] / "config" / "datagen_lineage.yaml"

#: Where CD-8's compare result for an image lives, relative to the repo root
#: (ch05 section 4.1). Not shipped in the wheel: only the CI test opens it.
EVIDENCE_PATTERN = "tests/fixtures/datagen_reference/compare-{digest12}.json"

#: The five byte-compare cases and the public development seed each runs on
#: (ch05 section 4.1: 43 financial, 42 Customer 360; never a held-out seed).
COMPARE_CASES: dict[str, int] = {"F0": 43, "F1": 43, "F2": 43, "C0": 42, "C2": 42}

NOT_OBSERVED = "corpus not observed at run end (no corpus observation in this record)"
NO_MARKER = "corpus has no generator marker hash (datagen image before DAT-3)"
NOT_ONE_CORPUS = "corpus markers do not describe one complete corpus (see corpus.problems)"

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def _short_hash(obj: Any) -> str:
    from lakebench.metrics.experiment import _short_hash as short

    return short(obj)


def _distinct(values: Sequence[Any]) -> list[Any]:
    """Distinct JSON values in a stable order."""
    seen: dict[str, Any] = {}
    for v in values:
        seen.setdefault(json.dumps(v, sort_keys=True, default=str), v)
    return [seen[k] for k in sorted(seen)]


# ---------------------------------------------------------------------------
# Reading the markers (S3, once per run)
# ---------------------------------------------------------------------------


@dataclass
class MarkerSet:
    """What one listing of a datagen scope found."""

    scope: str = ""
    markers: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    series: dict[str, Any] | None = None
    bronze_listing_sha256: str | None = None
    objects: int = 0
    problems: list[str] = field(default_factory=list)
    series_check: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """The persisted form (ch03 section 0.1): per cycle the nodes found
        and the distinct values of the fields identity reads. Marker bodies
        (``corpus_args``, ``seed_ref``) are never persisted."""
        cycles = []
        for cycle in sorted(self.markers):
            ms = self.markers[cycle]
            cycles.append(
                {
                    "cycle": cycle,
                    "nodes_found": sorted(m["node_id"] for m in ms),
                    "total_nodes": _distinct([m.get("total_nodes") for m in ms]),
                    "cycles": _distinct([m.get("cycles") for m in ms]),
                    "corpus_args_sha256": _distinct([m.get("corpus_args_sha256") for m in ms]),
                    "model_version": _distinct([m.get("model_version") for m in ms]),
                    "build_commit": _distinct([m.get("build_commit") for m in ms]),
                }
            )
        return {
            "format": 1,
            "scope": self.scope,
            "cycles": cycles,
            "corpus_series_sha256": corpus_series_sha256(self.markers),
            "objects": self.objects,
            "problems": list(self.problems),
            "series_check": self.series_check,
            "error": self.error,
        }


def _get_json(client: Any, bucket: str, key: str) -> Any:
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def _reason(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:200]}"


def read_corpus_markers(client: Any, bucket: str, prefix: str) -> MarkerSet:
    """One ``list_objects_v2`` pass over ``datagen_scope(prefix)`` in
    *bucket* (a boto3 client), then a GET per marker and of ``series.json``.

    The listing digest covers every object in the scope, ``_corpus/``
    included. The first failed S3 call stops the read and is recorded as
    ``error``, so an outage costs one call's retries, not one per marker.
    Never raises.
    """
    out = MarkerSet()
    try:
        out.scope = datagen_scope(prefix)
    except ValueError as e:
        out.error = str(e)
        return out
    try:
        objects = list_scope(client, bucket, out.scope)
    except Exception as e:  # noqa: BLE001 -- recorded, never raised from a save
        out.error = f"listing {out.scope} failed: {_reason(e)}"
        return out
    out.objects = len(objects)
    out.bronze_listing_sha256 = listing_sha256(objects)

    marker_dir = f"{out.scope}{MARKER_DIR}/"
    keys = [str(o["Key"]) for o in objects if str(o["Key"]).startswith(marker_dir)]
    marker_keys: list[tuple[str, int, int]] = []
    series_key = None
    for key in keys:
        name = key[len(marker_dir) :]
        if name == SERIES_NAME:
            series_key = key
            continue
        m = MARKER_NAME.match(name)
        if m:
            marker_keys.append((key, int(m.group(1)), int(m.group(2))))
    if len(marker_keys) > MAX_MARKERS:
        out.error = (
            f"{len(marker_keys)} corpus markers under {marker_dir} (more than {MAX_MARKERS})"
        )
        return out

    for key, cycle, node in sorted(marker_keys):
        name = key[len(marker_dir) :]
        try:
            body = _get_json(client, bucket, key)
        except json.JSONDecodeError:
            out.problems.append(f"corpus marker {name} is not valid JSON")
            continue
        except Exception as e:  # noqa: BLE001
            out.error = f"reading corpus marker {name} failed: {_reason(e)}"
            return out
        if not isinstance(body, dict):
            out.problems.append(f"corpus marker {name} is not a JSON object")
            continue
        if body.get("format") != MARKER_FORMAT:
            out.problems.append(
                f"corpus marker {name} has format {body.get('format')!r}, "
                f"this Lakebench reads {MARKER_FORMAT}"
            )
            continue
        if body.get("cycle") != cycle or body.get("node_id") != node:
            out.problems.append(
                f"corpus marker {name} does not match its content "
                f"(cycle {body.get('cycle')!r}, node {body.get('node_id')!r})"
            )
            continue
        out.markers.setdefault(cycle, []).append(body)

    if series_key is not None:
        try:
            series = _get_json(client, bucket, series_key)
        except json.JSONDecodeError:
            out.problems.append("the series marker series.json is not valid JSON")
            series = None
        except Exception as e:  # noqa: BLE001
            out.error = f"reading series.json failed: {_reason(e)}"
            return out
        if series is not None and not isinstance(series, dict):
            out.problems.append("the series marker series.json is not a JSON object")
            series = None
        out.series = series
    out.series_check = _series_check(out.series, out.markers)
    return out


def _series_check(series: Mapping[str, Any] | None, markers: Mapping[int, Sequence[Mapping]]):
    """Why series.json does not describe the generate the node markers came
    from (None when it does, or when either is absent). A series written by
    an earlier generate than the markers must not lend them its lineage."""
    if not series or not markers:
        return None
    gen = series.get("generation") or {}
    refs = {str(m.get("seed_ref")) for ms in markers.values() for m in ms}
    if "seed_ref" in gen and refs != {str(gen.get("seed_ref"))}:
        return "series.json names another seed than the corpus markers"
    cycles = {m.get("cycles") for ms in markers.values() for m in ms}
    if "cycles_total" in series and cycles != {series.get("cycles_total")}:
        return "series.json names another cycle count than the corpus markers"
    return None


def observe_corpus(cfg: Any, s3: Any) -> dict[str, Any]:
    """The run-end corpus observation (ch03 section 0.1), as persisted in
    ``experiment_inputs.corpus_observation``. *s3* is an ``S3Client``.
    Never raises."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        from lakebench.deploy.datagen import bronze_datagen_prefix

        bucket = cfg.platform.storage.s3.buckets.bronze
        prefix = bronze_datagen_prefix(cfg)
        client = s3.raw_client
        if client is None:
            raise RuntimeError("the S3 client did not initialise")
        ms = read_corpus_markers(client, bucket, prefix)
    except Exception as e:  # noqa: BLE001
        ms = MarkerSet(error=f"corpus observation failed: {_reason(e)}")
    return {
        "format": 1,
        "markers": ms.to_dict(),
        "series": ms.series,
        "bronze_listing_sha256": ms.bronze_listing_sha256,
        "observed_at": now,
    }


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
    markers' cycle count is above 1."""
    if not obs:
        return None, NOT_OBSERVED
    markers = obs.get("markers") or {}
    if markers.get("error"):
        return None, f"corpus markers could not be read: {markers['error']}"
    cycles = list(markers.get("cycles") or [])
    hashes = [h for c in cycles for h in c.get("corpus_args_sha256") or []]
    if not cycles or all(h is None for h in hashes):
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


def load_lineage(path: Path | None = None) -> dict[str, LineageRow]:
    """Read ``config/datagen_lineage.yaml``: ``{digest, canonical,
    evidence, evidence_sha256, build_commit?}`` rows.

    A row whose ``digest`` equals its ``canonical`` is a root and needs no
    evidence. Any other row must name its compare file at
    ``EVIDENCE_PATTERN`` (first 12 hex of ``digest``) and pin that file's
    sha256, and its ``canonical`` must itself be a root. The loader never
    opens the compare file, because the wheel does not ship ``tests/``:
    ``check_lineage_evidence`` (run by the unit suite on every commit)
    proves each pinned file exists, hashes to the pin and passes
    ``validate_compare_file``. So a source checkout and an installed
    Lakebench read one table and give one id. Raises LineageError.
    """
    import yaml

    p = path or LINEAGE_FILE
    try:
        data = yaml.safe_load(p.read_text())
    except (OSError, yaml.YAMLError) as e:
        raise LineageError(f"cannot read {p.name}: {e}") from e
    rows = (data or {}).get("lineage") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise LineageError(f"{p.name} has no 'lineage' list")
    table: dict[str, LineageRow] = {}
    for i, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise LineageError(f"{p.name} row {i} is not a mapping")
        unknown = set(raw) - {"digest", "canonical", "evidence", "evidence_sha256", "build_commit"}
        if unknown:
            raise LineageError(f"{p.name} row {i} has unknown keys {sorted(unknown)}")
        row = LineageRow(
            digest=str(raw.get("digest") or ""),
            canonical=str(raw.get("canonical") or ""),
            evidence=raw.get("evidence"),
            evidence_sha256=raw.get("evidence_sha256"),
            build_commit=raw.get("build_commit"),
        )
        for name in ("digest", "canonical"):
            if not _DIGEST.match(getattr(row, name)):
                raise LineageError(f"{p.name} row {i}: {name} is not sha256:<64 hex>")
        if row.build_commit is not None and not isinstance(row.build_commit, str):
            raise LineageError(f"{p.name} row {i}: build_commit must be a quoted string")
        if row.digest in table:
            raise LineageError(f"{p.name} lists {row.digest} twice")
        if row.digest != row.canonical:
            want = EVIDENCE_PATTERN.format(digest12=row.digest.split(":", 1)[1][:12])
            if row.evidence != want:
                raise LineageError(f"{p.name} row {i}: evidence must be {want}")
            if not is_sha256_hex(row.evidence_sha256):
                raise LineageError(f"{p.name} row {i}: evidence_sha256 is not 64 hex")
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


def validate_compare_file(data: Any, digest: str, canonical: str) -> list[str]:
    """Why *data* (a parsed ``compare-<digest12>.json``, ch05 section 4.1)
    is not evidence that *digest* writes the same bytes as *canonical*."""
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
        if "_corpus/" not in (c.get("excluded") or []):
            errors.append(f"case {name} does not exclude _corpus/")
        if c.get("digest_a") != data.get("image_a") or c.get("digest_b") != data.get("image_b"):
            errors.append(f"case {name} digests differ from the file's images")
        if str(c.get("seed_ref")) != str(COMPARE_CASES[name]):
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
        except json.JSONDecodeError as e:
            errors.append(f"{row.digest}: evidence is not JSON ({e})")
            continue
        errors += [
            f"{row.digest}: {e}" for e in validate_compare_file(data, row.digest, row.canonical)
        ]
    return errors


@dataclass
class Lineage:
    value: str
    observed: bool
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def generator_lineage(
    obs: Mapping[str, Any],
    *,
    config_image: str | None,
    fleet_digest: str | None,
    table: Mapping[str, LineageRow] | None,
    table_error: str | None = None,
) -> Lineage:
    """The lineage of the corpus in *obs* (ch03 section 6 "Lineage").

    The digest is ``series.json``'s ``generation.image_digest``. The tag in
    a declared lineage is ``generation.image`` when series.json exists (the
    image that wrote the corpus, as configured then), else *config_image*,
    so an ``images.datagen`` edit after generation does not move the id.
    """
    series = obs.get("series") if isinstance(obs.get("series"), Mapping) else None
    gen = (series or {}).get("generation") or {}
    tag = gen.get("image") or config_image
    markers = obs.get("markers") or {}
    commits = _distinct(
        [v for c in markers.get("cycles") or [] for v in c.get("build_commit") or []]
    )

    def declared(note: str) -> Lineage:
        return Lineage(f"declared:{tag}", False, notes=[note])

    if table_error:
        return declared(f"lineage table unreadable ({table_error}); lineage is declared")
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
    out = Lineage(digest, True)
    row = (table or {}).get(digest)
    if row is not None:
        if row.build_commit and commits and commits[0] != row.build_commit:
            return Lineage(
                f"declared:{tag}",
                False,
                problems=[
                    f"image {digest[:19]} was built from {row.build_commit}, but the "
                    f"corpus markers name build {commits[0]}"
                ],
            )
        out.value = row.canonical
    if fleet_digest and fleet_digest != digest:
        out.notes.append(
            f"the datagen fleet record names {fleet_digest[:19]}, not the series image; "
            "lineage follows series.json"
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


#: Declared corpus keys compared with ``series.json``'s ``generation`` for
#: the display warning (CD-18's ``--skip-generate`` rule 2 already refuses
#: a run whose config disagrees with the series; this catches the rest).
_DECLARED_VS_SERIES = (
    ("scale", "scale"),
    ("generator_image", "image"),
    ("timestamp_start", "timestamp_start"),
    ("timestamp_end", "timestamp_end"),
)


def _declared_differs(declared: Mapping[str, Any], series: Mapping[str, Any] | None) -> list[str]:
    gen = (series or {}).get("generation") or {}
    out = []
    for dkey, skey in _DECLARED_VS_SERIES:
        a, b = declared.get(dkey), gen.get(skey)
        if a is None or b is None:
            continue
        if dkey == "scale":
            try:
                if abs(float(a) - float(b)) <= 1e-6:
                    continue
            except (TypeError, ValueError):
                pass
        elif str(a) == str(b):
            continue
        out.append(dkey)
    return out


def corpus_v2_fields(
    declared: Mapping[str, Any],
    *,
    obs: Mapping[str, Any] | None,
    inherited: Mapping[str, Any] | None,
    model_version: str | None,
    fleet_digest: str | None = None,
    lineage_path: Path | None = None,
) -> CorpusV2 | None:
    """ch03 section 6 "Series corpus identity" steps 3 and 4.

    *declared* is the config's corpus block (``experiment_inputs.corpus``);
    *obs* the persisted ``corpus_observation``; *inherited* the persisted
    ``inherited_corpus`` of a series repetition 2 to N, ``{corpus:
    <repetition 1's experiment.corpus>, from_run_id, bronze_listing_sha256:
    D1}``. Returns None when neither is present (a record from before the
    observation), so such a block gains no v2 field.
    """
    if not obs and not inherited:
        return None
    out = CorpusV2()
    digest = (obs or {}).get("bronze_listing_sha256")
    inherit_ok = False
    if inherited:
        d1 = inherited.get("bronze_listing_sha256")
        if not is_sha256_hex(d1):
            out.problems.append(
                "repetition 1's corpus was not observed (no bronze listing digest); "
                "this repetition does not inherit it"
            )
        elif not is_sha256_hex(digest):
            out.problems.append(
                "bronze was not observed before this repetition was saved; "
                "it does not inherit repetition 1's corpus"
            )
        elif digest != d1:
            out.problems.append("bronze changed during this repetition")
        elif not isinstance(inherited.get("corpus"), Mapping):
            out.problems.append("the inherited corpus block is missing")
        else:
            inherit_ok = True

    markers = (obs or {}).get("markers") or {}
    has_markers = bool(markers.get("cycles")) and not markers.get("error")
    if inherit_ok and not has_markers:
        block = copy.deepcopy(dict(inherited["corpus"]))  # type: ignore[index]
        block["inherited_from"] = inherited.get("from_run_id")  # type: ignore[union-attr]
        block["bronze_listing_sha256"] = digest
        out.replace = block
        return out

    if obs:
        out.problems += marker_problems(markers, model_version)
    table: dict[str, LineageRow] | None = None
    table_error = None
    try:
        table = load_lineage(lineage_path)
    except LineageError as e:
        table_error = str(e)
    lineage = generator_lineage(
        obs or {},
        config_image=declared.get("generator_image"),
        fleet_digest=fleet_digest,
        table=table,
        table_error=table_error,
    )
    out.problems += lineage.problems
    id_v2, unavailable = corpus_id_v2(obs, model_version, lineage.value)
    if inherited and has_markers:
        inherited_id = (inherited.get("corpus") or {}).get("id_v2")
        if id_v2 is None or inherited_id != id_v2:
            out.problems.append("series corpus id differs from repetition 1")

    f = out.fields
    f["id_v2"] = id_v2
    if id_v2 is not None:
        f["id_version"] = CORPUS_ID_VERSION
        f["args_sha256"] = markers.get("corpus_series_sha256")
    else:
        f["id_v2_unavailable"] = unavailable
    if has_markers:
        f["lineage"] = lineage.value
        f["lineage_observed"] = lineage.observed
        if lineage.notes:
            f["lineage_notes"] = list(lineage.notes)
        if not lineage.observed:
            f["observed"] = False
            f["observed_note"] = "generator image not observed: " + "; ".join(lineage.notes)
    f["declared"] = {k: v for k, v in declared.items() if k != "id"}
    differs = _declared_differs(declared, (obs or {}).get("series"))
    if differs:
        f["warnings"] = [
            "config datagen settings differ from the corpus this run read "
            f"({', '.join(differs)}); the corpus id follows the corpus"
        ]
    return out
